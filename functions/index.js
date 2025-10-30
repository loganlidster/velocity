const { HttpsError, onCall } = require("firebase-functions/v2/https");
const { setGlobalOptions } = require("firebase-functions/v2");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require("firebase-admin");
const { Connector } = require("@google-cloud/cloud-sql-connector");
const pg = require("pg");
const { randomUUID } = require("crypto");

// ----------------------------------------------------------------------------
// Global config
// ----------------------------------------------------------------------------
setGlobalOptions({
  region: "us-west1",
  secrets: ["pg-appuser-password"],
});

admin.initializeApp();

// Cloud SQL instance name
const INSTANCE_CONNECTION_NAME = "trade-socket:us-west1:trade-socket-sql";

// ----------------------------------------------------------------------------
// Helpers: env + DB pool
// ----------------------------------------------------------------------------
function getEnv(nameUpper, nameHyphen) {
  return process.env[nameUpper] || process.env[nameHyphen] || "";
}
function requireSecret(value, label) {
  if (!value) throw new HttpsError("failed-precondition", `${label} secret is not set.`);
  return value;
}

let pool;
async function getPool() {
  if (pool) return pool;

  const dbPassword = requireSecret(
    getEnv("PG_APPUSER_PASSWORD", "pg-appuser-password"),
    "PG_APPUSER_PASSWORD"
  );

  const connector = new Connector();
  const clientOpts = await connector.getOptions({
    instanceConnectionName: INSTANCE_CONNECTION_NAME,
    ipType: "PUBLIC",
  });

  pool = new pg.Pool({
    ...clientOpts,
    user: "appuser",
    password: dbPassword,
    database: "tradiac",
    max: 5,
  });

  await pool.query("SELECT NOW()");
  return pool;
}

// ----------------------------------------------------------------------------
// Schema Helpers - Updated for actual database schema
// ----------------------------------------------------------------------------
async function ensureUserApiKeysTable(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS user_api_keys (
      user_id TEXT PRIMARY KEY,
      polygon_key TEXT,
      alpaca_paper_key TEXT,
      alpaca_paper_secret TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
}

async function ensureWalletSchema(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS wallets (
      wallet_id   TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL,
      env         TEXT NOT NULL CHECK (env IN ('paper','live')),
      name        TEXT NOT NULL,
      enabled     BOOLEAN NOT NULL DEFAULT FALSE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await client.query(`CREATE INDEX IF NOT EXISTS ix_wallets_user ON wallets(user_id, env);`);

  await client.query(`
    CREATE TABLE IF NOT EXISTS wallet_settings (
      wallet_id       TEXT PRIMARY KEY REFERENCES wallets(wallet_id) ON DELETE CASCADE,
      budget_usd      NUMERIC,
      trading_window  TEXT,
      custom_start_et TEXT,
      custom_end_et   TEXT,
      cooldown_min    INT,
      order_policy    TEXT,
      method_rth      TEXT,
      method_ah       TEXT,
      buy_pct_rth     NUMERIC,
      sell_pct_rth    NUMERIC,
      buy_pct_ah      NUMERIC,
      sell_pct_ah     NUMERIC,
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // wallet_symbols table - this is the main symbol configuration table
  await client.query(`
    CREATE TABLE IF NOT EXISTS wallet_symbols (
      wallet_id       TEXT NOT NULL REFERENCES wallets(wallet_id) ON DELETE CASCADE,
      symbol          TEXT NOT NULL,
      buy_budget_usd  NUMERIC,
      buy_pct_rth     NUMERIC,
      sell_pct_rth    NUMERIC,
      buy_pct_ah      NUMERIC,
      sell_pct_ah     NUMERIC,
      method_rth      TEXT,
      method_ah       TEXT,
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      budget_mode     TEXT,
      percent_budget  NUMERIC,
      enabled         BOOLEAN DEFAULT TRUE,
      PRIMARY KEY (wallet_id, symbol)
    );
  `);
  await client.query(`CREATE INDEX IF NOT EXISTS ix_wallet_symbols_wallet ON wallet_symbols(wallet_id);`);
}

async function ensureBaselineSchema(client) {
  // baseline_daily - global baseline data (NO wallet_id)
  await client.query(`
    CREATE TABLE IF NOT EXISTS baseline_daily (
      trading_day DATE NOT NULL,
      symbol      TEXT NOT NULL,
      session     TEXT NOT NULL,
      method      TEXT NOT NULL,
      baseline    NUMERIC NOT NULL,
      sample_count INTEGER NOT NULL DEFAULT 0,
      source      TEXT NOT NULL DEFAULT 'computed',
      computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (trading_day, symbol, session, method)
    );
  `);
  await client.query(`CREATE INDEX IF NOT EXISTS ix_baseline_symbol ON baseline_daily(symbol, trading_day DESC);`);
}

async function ensureAlpacaLogSchema(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS alpaca_io_log (
      id         BIGSERIAL PRIMARY KEY,
      ts_utc     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      user_id    TEXT NOT NULL,
      env        TEXT NOT NULL,
      method     TEXT NOT NULL,
      path       TEXT NOT NULL,
      req_body   TEXT,
      status     INT,
      resp_body  TEXT,
      wallet_id  TEXT
    );
  `);
  await client.query(`CREATE INDEX IF NOT EXISTS ix_alpaca_log_user ON alpaca_io_log(user_id, ts_utc DESC);`);
}

async function ensureWalletRunLogSchema(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS wallet_run_log (
      id         BIGSERIAL PRIMARY KEY,
      wallet_id  TEXT NOT NULL,
      env        TEXT NOT NULL,
      ts_utc     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      summary    JSONB
    );
  `);
  await client.query(`CREATE INDEX IF NOT EXISTS ix_wallet_run_log ON wallet_run_log(wallet_id, ts_utc DESC);`);
}

// ----------------------------------------------------------------------------
// Helper: Get Wallet Env and Keys (with fallback for paper)
// ----------------------------------------------------------------------------
async function getWalletEnvAndKeys(client, walletId, uid) {
  // 1. Get wallet's environment and user ID
  const { rows: walletRows } = await client.query(
    `SELECT user_id, env FROM wallets WHERE wallet_id=$1`, [walletId]
  );
  if (walletRows.length === 0) throw new HttpsError("not-found", "Wallet not found.");
  const { user_id: userId, env } = walletRows[0];

  // 2. Try to get wallet-specific keys based on environment
  const { rows: walletKeyRows } = await client.query(
    `SELECT alpaca_paper_key, alpaca_paper_secret, alpaca_live_key, alpaca_live_secret FROM wallet_api_keys WHERE wallet_id=$1`, [walletId]
  );
  
  if (walletKeyRows.length > 0) {
    if (env === 'paper' && walletKeyRows[0].alpaca_paper_key && walletKeyRows[0].alpaca_paper_secret) {
      console.log(`[getWalletEnvAndKeys] Using wallet-specific PAPER keys for wallet ${walletId}`);
      return { env, alpacaKey: walletKeyRows[0].alpaca_paper_key, alpacaSecret: walletKeyRows[0].alpaca_paper_secret, userId };
    }
    if (env === 'live' && walletKeyRows[0].alpaca_live_key && walletKeyRows[0].alpaca_live_secret) {
      console.log(`[getWalletEnvAndKeys] Using wallet-specific LIVE keys for wallet ${walletId}`);
      return { env, alpacaKey: walletKeyRows[0].alpaca_live_key, alpacaSecret: walletKeyRows[0].alpaca_live_secret, userId };
    }
  }

  // 3. Fall back to user-level keys based on environment
  if (env === 'paper') {
    const { rows: userKeyRows } = await client.query(
      `SELECT alpaca_paper_key, alpaca_paper_secret FROM user_api_keys WHERE user_id=$1`, [userId]
    );
    if (userKeyRows.length > 0 && userKeyRows[0].alpaca_paper_key && userKeyRows[0].alpaca_paper_secret) {
      console.log(`[getWalletEnvAndKeys] Using user-level PAPER keys for wallet ${walletId}`);
      return { env, alpacaKey: userKeyRows[0].alpaca_paper_key, alpacaSecret: userKeyRows[0].alpaca_paper_secret, userId };
    }
  }
  
  if (env === 'live') {
    const { rows: userKeyRows } = await client.query(
      `SELECT alpaca_live_key, alpaca_live_secret FROM user_api_keys WHERE user_id=$1`, [userId]
    );
    if (userKeyRows.length > 0 && userKeyRows[0].alpaca_live_key && userKeyRows[0].alpaca_live_secret) {
      console.log(`[getWalletEnvAndKeys] Using user-level LIVE keys for wallet ${walletId}`);
      return { env, alpacaKey: userKeyRows[0].alpaca_live_key, alpacaSecret: userKeyRows[0].alpaca_live_secret, userId };
    }
  }

  // 4. If we're here, no valid keys were found for the environment.
  throw new HttpsError("failed-precondition", `Alpaca keys for '${env}' environment are not set for this wallet.`);
}

// ----------------------------------------------------------------------------
// Helper: Assert wallet ownership
// ----------------------------------------------------------------------------
async function assertWalletOwnership(client, walletId, userId) {
  const { rows } = await client.query(
    `SELECT 1 FROM wallets WHERE wallet_id=$1 AND user_id=$2`,
    [walletId, userId]
  );
  if (rows.length === 0) {
    throw new HttpsError("permission-denied", "Wallet not found or access denied.");
  }
}

// ----------------------------------------------------------------------------
// Helper: Get Polygon key (wallet-level or user-level fallback)
// ----------------------------------------------------------------------------
async function getPolygonKey(client, walletId, uid) {
  const r1 = await client.query(`SELECT polygon_key FROM wallet_api_keys WHERE wallet_id=$1`, [walletId]);
  if (r1.rows.length > 0 && r1.rows[0].polygon_key) return r1.rows[0].polygon_key;
  const r2 = await client.query(`SELECT polygon_key FROM user_api_keys WHERE user_id=$1`, [uid]);
  if (r2.rows.length > 0 && r2.rows[0].polygon_key) return r2.rows[0].polygon_key;
  throw new HttpsError("failed-precondition", "No Polygon key found.");
}

// ----------------------------------------------------------------------------
// Helper: Alpaca GET with logging
// ----------------------------------------------------------------------------
async function alpacaGET(client, userId, walletId, env, path, alpacaKey, alpacaSecret) {
  const baseUrl = env === "paper"
    ? "https://paper-api.alpaca.markets"
    : "https://api.alpaca.markets";
  const url = `${baseUrl}${path}`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "APCA-API-KEY-ID": alpacaKey,
      "APCA-API-SECRET-KEY": alpacaSecret,
    },
  });

  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }

  await client.query(
    `INSERT INTO alpaca_io_log(user_id, env, method, path, req_body, status, resp_body, wallet_id)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
    [userId, env, 'GET', path, JSON.stringify({}), response.status, JSON.stringify(data), walletId]
  );

  if (!response.ok) {
    throw new HttpsError("internal", `Alpaca error: ${response.status} ${text}`);
  }

  return data;
}

// ============================================================================
// EXECUTION ENGINE HELPER FUNCTIONS
// ============================================================================

// Get Current BTC Price from Polygon
async function getBTCPrice(client, userId, walletId, polygonKey) {
  try {
    const url = `https://api.polygon.io/v2/last/trade/X:BTCUSD?apiKey=${polygonKey}`;
    
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Polygon API error: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    
    await client.query(
      `INSERT INTO alpaca_io_log (user_id, env, method, path, req_body, status, resp_body, wallet_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [userId, 'paper', 'GET', '/v2/last/trade/X:BTCUSD', JSON.stringify({}), response.status, JSON.stringify(data), walletId]
    );
    
    if (data.results && data.results.p) {
      return parseFloat(data.results.p);
    }
    
    throw new Error('Invalid BTC price data from Polygon');
  } catch (error) {
    console.error('Error fetching BTC price:', error);
    
    await client.query(
      `INSERT INTO execution_errors (user_id, wallet_id, error_type, error_message, error_stack, function_name)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [userId, walletId, 'API_ERROR', error.message, error.stack, 'getBTCPrice']
    );
    
    throw error;
  }
}

// Get Alpaca Positions with Cost Basis
async function getAlpacaPositions(client, userId, walletId, alpacaKey, alpacaSecret, env) {
  try {
    const baseUrl = env === 'paper' ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
    const url = `${baseUrl}/v2/positions`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'APCA-API-KEY-ID': alpacaKey,
        'APCA-API-SECRET-KEY': alpacaSecret
      }
    });
    
    if (!response.ok) {
      throw new Error(`Alpaca API error: ${response.status} ${response.statusText}`);
    }
    
    const positions = await response.json();
    
    await client.query(
      `INSERT INTO alpaca_io_log (user_id, env, method, path, req_body, status, resp_body, wallet_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [userId, env, 'GET', '/v2/positions', JSON.stringify({}), response.status, JSON.stringify(positions), walletId]
    );
    
    const positionMap = {};
    for (const pos of positions) {
      positionMap[pos.symbol] = {
        qty: parseInt(pos.qty),
        cost_basis: parseFloat(pos.cost_basis),
        current_price: parseFloat(pos.current_price),
        market_value: parseFloat(pos.market_value),
        unrealized_pl: parseFloat(pos.unrealized_pl || 0),
        unrealized_plpc: parseFloat(pos.unrealized_plpc || 0)
      };
    }
    
    return positionMap;
  } catch (error) {
    console.error('Error fetching Alpaca positions:', error);
    
    await client.query(
      `INSERT INTO execution_errors (user_id, wallet_id, error_type, error_message, error_stack, function_name)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [userId, walletId, 'API_ERROR', error.message, error.stack, 'getAlpacaPositions']
    );
    
    throw error;
  }
}

// Get Account Cash Balance
async function getAccountCash(client, userId, walletId, alpacaKey, alpacaSecret, env) {
  try {
    const baseUrl = env === 'paper' ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
    const url = `${baseUrl}/v2/account`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'APCA-API-KEY-ID': alpacaKey,
        'APCA-API-SECRET-KEY': alpacaSecret
      }
    });
    
    if (!response.ok) {
      throw new Error(`Alpaca API error: ${response.status} ${response.statusText}`);
    }
    
    const account = await response.json();
    
    await client.query(
      `INSERT INTO alpaca_io_log (user_id, env, method, path, req_body, status, resp_body, wallet_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [userId, env, 'GET', '/v2/account', JSON.stringify({}), response.status, JSON.stringify(account), walletId]
    );
    
    return parseFloat(account.cash || 0);
  } catch (error) {
    console.error('Error fetching account cash:', error);
    
    await client.query(
      `INSERT INTO execution_errors (user_id, wallet_id, error_type, error_message, error_stack, function_name)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [userId, walletId, 'API_ERROR', error.message, error.stack, 'getAccountCash']
    );
    
    throw error;
  }
}

// Get All Open Orders
async function getOpenOrders(client, userId, walletId, alpacaKey, alpacaSecret, env) {
  try {
    const baseUrl = env === 'paper' ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
    const url = `${baseUrl}/v2/orders?status=open`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'APCA-API-KEY-ID': alpacaKey,
        'APCA-API-SECRET-KEY': alpacaSecret
      }
    });
    
    if (!response.ok) {
      throw new Error(`Alpaca API error: ${response.status} ${response.statusText}`);
    }
    
    const orders = await response.json();
    
    await client.query(
      `INSERT INTO alpaca_io_log (user_id, env, method, path, req_body, status, resp_body, wallet_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [userId, 'paper', 'GET', '/v2/orders?status=open', JSON.stringify({}), response.status, JSON.stringify(orders), walletId]
    );
    
    return orders;
  } catch (error) {
    console.error('Error fetching open orders:', error);
    
    await client.query(
      `INSERT INTO execution_errors (user_id, wallet_id, error_type, error_message, error_stack, function_name)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [userId, walletId, 'API_ERROR', error.message, error.stack, 'getOpenOrders']
    );
    
    throw error;
  }
}

// Cancel All Open Orders
async function cancelAllOrders(client, userId, walletId, alpacaKey, alpacaSecret, env) {
  try {
    const openOrders = await getOpenOrders(client, userId, walletId, alpacaKey, alpacaSecret, env);
    
    if (openOrders.length === 0) {
      console.log('No open orders to cancel');
      return { cancelled: 0, errors: [] };
    }
    
    console.log(`Cancelling ${openOrders.length} open orders...`);
    
    const results = { cancelled: 0, errors: [] };
    
    for (const order of openOrders) {
      try {
        const baseUrl = env === 'paper' ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
          const url = `${baseUrl}/v2/orders/${order.id}`;
        
        const response = await fetch(url, {
          method: 'DELETE',
          headers: {
            'APCA-API-KEY-ID': alpacaKey,
            'APCA-API-SECRET-KEY': alpacaSecret
          }
        });
        
        await client.query(
          `INSERT INTO alpaca_io_log (user_id, env, method, path, req_body, status, resp_body, wallet_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
          [userId, 'paper', 'DELETE', `/v2/orders/${order.id}`, JSON.stringify({}), response.status, JSON.stringify({ order_id: order.id }), walletId]
        );
        
        if (response.ok) {
          results.cancelled++;
          
          await client.query(
            `INSERT INTO execution_cancellations (user_id, wallet_id, symbol, alpaca_order_id, cancellation_reason)
             VALUES ($1, $2, $3, $4, $5)`,
            [userId, walletId, order.symbol, order.id, 'Pre-execution cleanup']
          );
        } else {
          const errorText = await response.text();
          results.errors.push({ order_id: order.id, error: errorText });
          console.error(`Failed to cancel order ${order.id}:`, errorText);
        }
      } catch (error) {
        results.errors.push({ order_id: order.id, error: error.message });
        console.error(`Error cancelling order ${order.id}:`, error);
      }
    }
    
    console.log(`Cancelled ${results.cancelled} orders, ${results.errors.length} errors`);
    return results;
  } catch (error) {
    console.error('Error in cancelAllOrders:', error);
    
    await client.query(
      `INSERT INTO execution_errors (user_id, wallet_id, error_type, error_message, error_stack, function_name)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [userId, walletId, 'API_ERROR', error.message, error.stack, 'cancelAllOrders']
    );
    
    throw error;
  }
}

// Calculate Execution Prices
// Note: buy_pct and sell_pct are percentages (e.g., 1.0 = 1%)
// buy_multiplier = 1 + (buy_pct / 100)
// sell_multiplier = 1 - (sell_pct / 100)
function calculateExecutionPrices(btcPrice, baseline, buyPct, sellPct) {
  const buyMultiplier = 1 + (buyPct / 100);
  const sellMultiplier = 1 - (sellPct / 100);
  
  const buyRatio = baseline * buyMultiplier;
  const sellRatio = baseline * sellMultiplier;
  
  const buyPrice = btcPrice / buyRatio;
  const sellPrice = btcPrice / sellRatio;
  
  return {
    buyRatio,
    sellRatio,
    buyPrice: parseFloat(buyPrice.toFixed(4)),
    sellPrice: parseFloat(sellPrice.toFixed(4))
  };
}

// Determine Execution Decision

// ============================================
// TRADE LOGIC IMPROVEMENTS - Helper Functions
// ============================================

// Symbol cooldown tracking (in-memory)
const symbolCooldowns = new Map();

/**
 * Check if we can place an order for this symbol
 * Prevents: 1) Conflicting orders, 2) Orders within cooldown period
 */
async function canPlaceOrder(client, userId, env, walletId, symbol, side, alpacaKey, alpacaSecret) {
  try {
    // Check 1: Cooldown (60 seconds since last fill)
    const cooldownKey = `${walletId}_${symbol}`;
    const lastFillTime = symbolCooldowns.get(cooldownKey);
    if (lastFillTime) {
      const timeSinceLastFill = Date.now() - lastFillTime;
      if (timeSinceLastFill < 60000) {
        const remainingSeconds = Math.ceil((60000 - timeSinceLastFill) / 1000);
        console.log(`[${symbol}] ⏸️  Cooldown active: ${remainingSeconds}s remaining`);
        return { canPlace: false, reason: `Cooldown: ${remainingSeconds}s remaining` };
      }
    }
    
    // Check 2: Conflicting open orders
    const openOrders = await getOpenOrders(client, userId, walletId, alpacaKey, alpacaSecret, env);
    const symbolOrders = openOrders.filter(o => o.symbol === symbol);
    
    const hasBuyOrder = symbolOrders.some(o => o.side === 'buy');
    const hasSellOrder = symbolOrders.some(o => o.side === 'sell');
    
    if (side === 'buy' && hasSellOrder) {
      console.log(`[${symbol}] 🚫 Cannot place BUY: SELL order already exists`);
      return { canPlace: false, reason: 'SELL order already exists' };
    }
    
    if (side === 'sell' && hasBuyOrder) {
      console.log(`[${symbol}] 🚫 Cannot place SELL: BUY order already exists`);
      return { canPlace: false, reason: 'BUY order already exists' };
    }
    
    return { canPlace: true, reason: 'OK' };
    
  } catch (error) {
    console.error(`[${symbol}] Error checking order eligibility:`, error.message);
      // On error, DENY the order (fail closed for safety)
      return { canPlace: false, reason: 'Eligibility check failed' };
  }
}

/**
 * Check if order price is within acceptable range of current market price
 * Threshold: MIN of 10% or $0.50
 */
function isOrderPriceReasonable(symbol, orderPrice, currentPrice, side) {
  if (!currentPrice || currentPrice <= 0) {
    console.log(`[${symbol}] No current price available, allowing order`);
    return { reasonable: true, reason: 'No current price to compare' };
  }
  
  // Threshold: MIN of 10% or $0.50
  const threshold = Math.min(currentPrice * 0.10, 0.50);
  const distance = Math.abs(orderPrice - currentPrice);
  
  if (distance > threshold) {
    console.log(`[${symbol}] 📊 Order too far from market: ${side} at $${orderPrice.toFixed(2)} vs market $${currentPrice.toFixed(2)} (distance: $${distance.toFixed(2)}, threshold: $${threshold.toFixed(2)})`);
    return { 
      reasonable: false, 
      reason: `Price $${orderPrice.toFixed(2)} is $${distance.toFixed(2)} from market $${currentPrice.toFixed(2)} (threshold: $${threshold.toFixed(2)})` 
    };
  }
  
  console.log(`[${symbol}] ✅ Order price reasonable: ${side} at $${orderPrice.toFixed(2)} vs market $${currentPrice.toFixed(2)} (distance: $${distance.toFixed(2)}, threshold: $${threshold.toFixed(2)})`);
  return { reasonable: true, reason: 'Within threshold' };
}

/**
 * Record that an order was filled (start cooldown)
 */
function recordOrderFill(walletId, symbol) {
  const cooldownKey = `${walletId}_${symbol}`;
  symbolCooldowns.set(cooldownKey, Date.now());
  console.log(`[${symbol}] ⏱️  Cooldown started (60 seconds)`);
}

// ============================================
// END: Trade Logic Improvements
// ============================================

function determineExecutionDecision(position, budgetAvailable) {
  const hasShares = position && position.qty > 0;
  const hasBudget = budgetAvailable > 0;
  
  let decision = 'HOLD';
  let reason = '';
  
  if (hasShares && hasBudget) {
    decision = 'BOTH';
    reason = `Has ${position.qty} shares (cost basis: $${position.cost_basis.toFixed(2)}) and $${budgetAvailable.toFixed(2)} budget available`;
  } else if (hasShares) {
    decision = 'SELL';
    reason = `Has ${position.qty} shares (cost basis: $${position.cost_basis.toFixed(2)}), no budget available`;
  } else if (hasBudget) {
    decision = 'BUY';
    reason = `No shares, $${budgetAvailable.toFixed(2)} budget available`;
  } else {
    decision = 'HOLD';
    reason = 'No shares and no budget available';
  }
  
  return { decision, reason };
}

// Place Limit Order
async function placeLimitOrder(client, userId, walletId, symbol, side, qty, limitPrice, alpacaKey, alpacaSecret, env) {
  try {
    // Determine if we're in after-hours
    const now = new Date();
    const etTime = new Date(now.toLocaleString("en-US", { timeZone: "America/New_York" }));
    const currentMinute = etTime.getHours() * 60 + etTime.getMinutes();
    const isRTH = currentMinute >= 570 && currentMinute < 960; // 9:30 AM - 4:00 PM ET
    
    const baseUrl = env === 'paper' ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
      const url = `${baseUrl}/v2/orders`;
    
    const orderData = {
      symbol: symbol,
      qty: qty,
      side: side,
      type: 'limit',
      limit_price: limitPrice.toFixed(2),
      time_in_force: 'day',
      extended_hours: !isRTH  // TRUE for after-hours, FALSE for RTH
    };
    
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'APCA-API-KEY-ID': alpacaKey,
        'APCA-API-SECRET-KEY': alpacaSecret,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(orderData)
    });
    
    const responseData = await response.json();
    
    await client.query(
      `INSERT INTO alpaca_io_log (user_id, env, method, path, req_body, status, resp_body, wallet_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [userId, 'paper', 'POST', '/v2/orders', JSON.stringify(orderData), response.status, JSON.stringify(responseData), walletId]
    );
    
    if (!response.ok) {
      throw new Error(`Alpaca order error: ${JSON.stringify(responseData)}`);
    }
    
    await client.query(
      `INSERT INTO execution_orders (user_id, wallet_id, symbol, side, qty, limit_price, status, alpaca_order_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [userId, walletId, symbol, side, qty, limitPrice, responseData.status, responseData.id]
    );
    
    console.log(`✓ Placed ${side} order: ${qty} ${symbol} @ $${limitPrice.toFixed(2)} (Session: ${isRTH ? 'RTH' : 'AH'}, extended_hours: ${!isRTH}, Order ID: ${responseData.id})`);
    
    return responseData;
  } catch (error) {
    console.error(`Error placing ${side} order for ${symbol}:`, error);
    
    await client.query(
      `INSERT INTO execution_errors (user_id, wallet_id, symbol, error_type, error_message, error_stack, function_name)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [userId, walletId, symbol, 'ORDER_ERROR', error.message, error.stack, 'placeLimitOrder']
    );
    
    throw error;
  }
}

// Log Execution Snapshot
async function logExecutionSnapshot(client, snapshotData) {
  try {
    await client.query(
      `INSERT INTO execution_snapshots (
        user_id, wallet_id, symbol,
        btc_price, stock_price, current_ratio,
        baseline_value, baseline_method,
        buy_price, sell_price,
        decision, decision_reason,
        shares_owned, budget_available
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
      [
        snapshotData.userId,
        snapshotData.walletId,
        snapshotData.symbol,
        snapshotData.btcPrice,
        snapshotData.stockPrice,
        snapshotData.currentRatio,
        snapshotData.baselineValue,
        snapshotData.baselineMethod,
        snapshotData.buyPrice,
        snapshotData.sellPrice,
        snapshotData.decision,
        snapshotData.decisionReason,
        snapshotData.sharesOwned,
        snapshotData.budgetAvailable
      ]
    );
  } catch (error) {
    console.error('Error logging execution snapshot:', error);
  }
}

// ============================================================================
// MAIN EXECUTION FUNCTION
// ============================================================================
async function executeWallet(userId, walletId) {
  const pool = await getPool();
  const client = await pool.connect();
  
  try {
    console.log(`\n========================================`);
    console.log(`Executing wallet: ${walletId}`);
    console.log(`========================================`);
    
    // NO TRANSACTION HERE - we'll use per-symbol transactions
    
    const walletResult = await client.query(
      `SELECT * FROM wallets WHERE wallet_id = $1 AND user_id = $2`,
      [walletId, userId]
    );
    
    if (walletResult.rows.length === 0) {
      throw new Error('Wallet not found');
    }
    
    const wallet = walletResult.rows[0];
    
    // Check if wallet is enabled in wallets table
    const settingsResult = await client.query(
      `SELECT enabled FROM wallets WHERE wallet_id = $1`,
      [walletId]
    );
    
    if (settingsResult.rows.length === 0 || !settingsResult.rows[0].enabled) {
      console.log('Wallet is disabled or has no settings, skipping execution');
      return { success: true, message: 'Wallet disabled' };
    }
    
    console.log('Wallet is enabled, proceeding with execution');
    
    // Get wallet environment and keys using the helper
    const { env, alpacaKey, alpacaSecret } = await getWalletEnvAndKeys(client, walletId, userId);
    
    // Get Polygon key
    const keysResult = await client.query(
      `SELECT polygon_key FROM user_api_keys WHERE user_id = $1`,
      [userId]
    );
    
    if (keysResult.rows.length === 0 || !keysResult.rows[0].polygon_key) {
      throw new Error('Polygon API key not found');
    }
    
    const polygon_key = keysResult.rows[0].polygon_key;
    
    console.log('Fetching BTC price...');
    const btcPrice = await getBTCPrice(client, userId, walletId, polygon_key);
    console.log(`BTC Price: $${btcPrice.toFixed(2)}`);
    
    console.log('Fetching Alpaca positions...');
    const positions = await getAlpacaPositions(client, userId, walletId, alpacaKey, alpacaSecret, env);
    console.log(`Found ${Object.keys(positions).length} positions`);
    
    console.log('Fetching account cash...');
    const accountCash = await getAccountCash(client, userId, walletId, alpacaKey, alpacaSecret, env);
    console.log(`Account Cash: $${accountCash.toFixed(2)}`);
      
      // Get account equity for percentage budget calculations
      const baseUrl = env === 'paper' ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
      const accountResponse = await fetch(`${baseUrl}/v2/account`, {
        method: 'GET',
        headers: {
          'APCA-API-KEY-ID': alpacaKey,
          'APCA-API-SECRET-KEY': alpacaSecret
        }
      });
      const accountData = await accountResponse.json();
      const accountEquity = parseFloat(accountData.equity || 0);
      console.log(`Account Equity: $${accountEquity.toFixed(2)}`);
    
    console.log('Cancelling all open orders...');
    const cancelResult = await cancelAllOrders(client, userId, walletId, alpacaKey, alpacaSecret, env);
    console.log(`Cancelled ${cancelResult.cancelled} orders`);
    
    // Get wallet symbols with baselines - CORRECTED QUERY
    const symbolsResult = await client.query(
      `SELECT ws.*, 
              bd_rth.baseline as rth_baseline,
              bd_ah.baseline as ah_baseline
       FROM wallet_symbols ws
       LEFT JOIN baseline_daily bd_rth ON ws.symbol = bd_rth.symbol 
         AND ws.method_rth = bd_rth.method 
         AND bd_rth.session = 'RTH'
         AND bd_rth.trading_day = (SELECT MAX(trading_day) FROM baseline_daily WHERE symbol = ws.symbol AND session = 'RTH')
       LEFT JOIN baseline_daily bd_ah ON ws.symbol = bd_ah.symbol 
         AND ws.method_ah = bd_ah.method 
         AND bd_ah.session = 'AH'
         AND bd_ah.trading_day = (SELECT MAX(trading_day) FROM baseline_daily WHERE symbol = ws.symbol AND session = 'AH')
       WHERE ws.wallet_id = $1 AND ws.enabled = true`,
      [walletId]
    );
    
    console.log(`Found ${symbolsResult.rows.length} enabled symbols`);
    
    const results = [];
    
    // CRITICAL FIX: Process each symbol in its own transaction

      // ============================================================================
      // PHASE 1: SMART BUDGET ALLOCATION
      // ============================================================================
      console.log('\n=== PHASE 1: CALCULATE BUDGETS ===');
      
      // Step 1: Calculate requested budgets for each symbol
      const symbolBudgets = [];
      let totalFixedBudgetNeeded = 0;
      let totalPercentRequested = 0;
      
      for (const symbolData of symbolsResult.rows) {
        const position = positions[symbolData.symbol];
        const costBasis = position ? position.cost_basis : 0;
        
        const budgetMode = symbolData.budget_mode || 'fixed';
        let requestedBudget = 0;
        
        if (budgetMode === 'percent') {
          const percentBudget = parseFloat(symbolData.percent_budget) || 0;
          requestedBudget = (accountEquity * percentBudget) / 100;
          totalPercentRequested += Math.max(0, requestedBudget - costBasis);
          console.log(`[${symbolData.symbol}] PERCENT: ${percentBudget}% of $${accountEquity.toFixed(2)} = $${requestedBudget.toFixed(2)}`);
        } else {
          requestedBudget = parseFloat(symbolData.buy_budget_usd) || 0;
          totalFixedBudgetNeeded += Math.max(0, requestedBudget - costBasis);
          console.log(`[${symbolData.symbol}] FIXED: $${requestedBudget.toFixed(2)}`);
        }
        
        symbolBudgets.push({
          symbol: symbolData.symbol,
          mode: budgetMode,
          requestedBudget: requestedBudget,
          costBasis: costBasis,
          requestedRemaining: Math.max(0, requestedBudget - costBasis)
        });
      }
      
      console.log(`\nTotal Fixed Budget Needed: $${totalFixedBudgetNeeded.toFixed(2)}`);
      console.log(`Total Percent Budget Requested: $${totalPercentRequested.toFixed(2)}`);
      console.log(`Available Cash: $${accountCash.toFixed(2)}`);
      
      // Step 2: Calculate available cash for percentage budgets
      const cashForPercent = Math.max(0, accountCash - totalFixedBudgetNeeded);
      console.log(`Cash Available for Percent Budgets: $${cashForPercent.toFixed(2)}`);
      
      // Step 3: Allocate budgets proportionally if needed
      const allocatedBudgets = {};
      
      for (const sb of symbolBudgets) {
        let allocatedBudget = 0;
        
        if (sb.mode === 'fixed') {
          // Fixed budgets get their full amount
          allocatedBudget = sb.requestedBudget;
        } else {
          // Percentage budgets: allocate proportionally if total requested > available
          if (totalPercentRequested > cashForPercent) {
            // Proportional allocation
            const proportion = cashForPercent / totalPercentRequested;
            allocatedBudget = sb.requestedBudget * proportion;
            console.log(`[${sb.symbol}] SCALED DOWN: $${sb.requestedBudget.toFixed(2)} → $${allocatedBudget.toFixed(2)} (${(proportion * 100).toFixed(1)}%)`);
          } else {
            // Enough cash for all percentage budgets
            allocatedBudget = sb.requestedBudget;
          }
        }
        
        allocatedBudgets[sb.symbol] = {
          totalBudget: allocatedBudget,
          costBasis: sb.costBasis,
          remaining: Math.max(0, allocatedBudget - sb.costBasis)
        };
        
        console.log(`[${sb.symbol}] ALLOCATED: Total=$${allocatedBudget.toFixed(2)}, Remaining=$${allocatedBudgets[sb.symbol].remaining.toFixed(2)}`);
      }
      
      // Step 4: Track cumulative spending to prevent over-spending
      let cumulativeSpent = 0;
      
      console.log('\n=== PHASE 2: EXECUTE TRADES ===');
      
    for (const symbolData of symbolsResult.rows) {
      // Start a new transaction for THIS symbol only
      await client.query('BEGIN');
      
      try {
        console.log(`\n--- Processing ${symbolData.symbol} ---`);
        
        // Determine current session
        const now = new Date();
        const etTime = new Date(now.toLocaleString("en-US", { timeZone: "America/New_York" }));
        const currentMinute = etTime.getHours() * 60 + etTime.getMinutes();
        const isRTH = currentMinute >= 570 && currentMinute < 960; // 9:30 AM - 4:00 PM ET
        const currentSession = isRTH ? 'RTH' : 'AH';
        
        // Use correct baseline for current session
        const baseline = isRTH ? symbolData.rth_baseline : symbolData.ah_baseline;
        const baselineMethod = isRTH ? symbolData.method_rth : symbolData.method_ah;
        
        // CRITICAL FIX: Handle NULL percentages with defaults
        const buyPct = parseFloat(isRTH ? (symbolData.buy_pct_rth || 1.0) : (symbolData.buy_pct_ah || 1.0));
        const sellPct = parseFloat(isRTH ? (symbolData.sell_pct_rth || 2.0) : (symbolData.sell_pct_ah || 2.0));
        
        console.log(`Session: ${currentSession}, Method: ${baselineMethod}`);
        console.log(`Buy %: ${buyPct}, Sell %: ${sellPct}`);
        
        // Validate baseline exists
        if (!baseline) {
          throw new Error(`No ${currentSession} baseline found for ${symbolData.symbol}`);
        }
        
        // Calculate execution prices
        const { buyRatio, sellRatio, buyPrice, sellPrice } = calculateExecutionPrices(
          btcPrice,
          parseFloat(baseline),
          buyPct,
          sellPct
        );
        
        console.log(`Buy Ratio: ${buyRatio.toFixed(2)}, Buy Price: $${buyPrice.toFixed(4)}`);
        console.log(`Sell Ratio: ${sellRatio.toFixed(2)}, Sell Price: $${sellPrice.toFixed(4)}`);
        
        const position = positions[symbolData.symbol];
        const costBasis = position ? position.cost_basis : 0;
        
          // Get allocated budget for this symbol (calculated in Phase 1)
          const allocation = allocatedBudgets[symbolData.symbol];
          const symbolBudget = allocation.totalBudget;
          const symbolBudgetRemaining = allocation.remaining;
          
          // CRITICAL: Check remaining cash after previous orders
          const remainingCash = Math.max(0, accountCash - cumulativeSpent);
          const budgetAvailable = Math.max(0, Math.min(symbolBudgetRemaining, remainingCash));
        
          // Log budget constraints
          console.log(`[${symbolData.symbol}] Budget: $${symbolBudget.toFixed(2)}, Remaining: $${symbolBudgetRemaining.toFixed(2)}`);
          console.log(`[${symbolData.symbol}] Cumulative Spent: $${cumulativeSpent.toFixed(2)}, Remaining Cash: $${remainingCash.toFixed(2)}`);
          console.log(`[${symbolData.symbol}] Budget Available: $${budgetAvailable.toFixed(2)}`);
        
        if (accountCash < symbolBudgetRemaining) {
          console.log(`⚠️  Account cash limits budget for ${symbolData.symbol}`);
        }
        
        const { decision, reason } = determineExecutionDecision(position, budgetAvailable);
        console.log(`Decision: ${decision} - ${reason}`);
        
        const stockPrice = position ? position.current_price : null;
        const currentRatio = stockPrice ? (btcPrice / stockPrice) : null;
        
        await logExecutionSnapshot(client, {
          userId,
          walletId,
          symbol: symbolData.symbol,
          btcPrice,
          stockPrice,
          currentRatio,
          baselineValue: baseline,
          baselineMethod,
          buyPrice,
          sellPrice,
          decision,
          decisionReason: reason,
          sharesOwned: position ? position.qty : 0,
          budgetAvailable
        });
        
        const orders = [];
        
        // WASH TRADE PREVENTION: Never place both buy AND sell orders at same time
        // Only place the order most likely to execute based on current price
        
        if (decision === 'BOTH') {
          // Get current stock price
          const currentPrice = position ? position.current_price : null;
          
          if (currentPrice) {
            console.log(`Current Price: $${currentPrice.toFixed(4)}`);
            console.log(`Buy Price: $${buyPrice.toFixed(4)}, Sell Price: $${sellPrice.toFixed(4)}`);
            
            // If current price <= buy price, place BUY order (price is attractive)
            // If current price > buy price, place SELL order (price is high)
            if (currentPrice <= buyPrice) {
              console.log('⚠️  WASH TRADE PREVENTION: Price at/below buy price, placing BUY order only');
              
              const qtyToBuy = Math.floor(budgetAvailable / buyPrice);
              if (qtyToBuy > 0) {
                // Check eligibility and price
                const eligibility = await canPlaceOrder(client, userId, env, walletId, symbolData.symbol, 'buy', alpacaKey, alpacaSecret);
                const priceCheck = isOrderPriceReasonable(symbolData.symbol, buyPrice, currentPrice, 'buy');
                
                if (!eligibility.canPlace) {
                  console.log(`[${symbolData.symbol}] ⏭️  Skipping BUY order: ${eligibility.reason}`);
                } else if (!priceCheck.reasonable) {
                  console.log(`[${symbolData.symbol}] ⏭️  Skipping BUY order: ${priceCheck.reason}`);
                } else {
                  const buyOrder = await placeLimitOrder(
                    client, userId, walletId,
                    symbolData.symbol, 'buy', qtyToBuy, buyPrice,
                    alpacaKey, alpacaSecret, env
                  );
                  orders.push(buyOrder);
                  recordOrderFill(walletId, symbolData.symbol);
                }
              }
            } else {
              console.log('⚠️  WASH TRADE PREVENTION: Price above buy price, placing SELL order only');
              
              if (position && position.qty > 0) {
                // Check eligibility and price
                const eligibility = await canPlaceOrder(client, userId, env, walletId, symbolData.symbol, 'sell', alpacaKey, alpacaSecret);
                const priceCheck = isOrderPriceReasonable(symbolData.symbol, sellPrice, currentPrice, 'sell');
                
                if (!eligibility.canPlace) {
                  console.log(`[${symbolData.symbol}] ⏭️  Skipping SELL order: ${eligibility.reason}`);
                } else if (!priceCheck.reasonable) {
                  console.log(`[${symbolData.symbol}] ⏭️  Skipping SELL order: ${priceCheck.reason}`);
                } else {
                  const sellOrder = await placeLimitOrder(
                    client, userId, walletId,
                    symbolData.symbol, 'sell', position.qty, sellPrice,
                    alpacaKey, alpacaSecret, env
                  );
                  orders.push(sellOrder);
                  recordOrderFill(walletId, symbolData.symbol);
                }
              }
            }
          } else {
            // No current price, default to SELL if we have shares
            console.log('⚠️  No current price, defaulting to SELL order');
            if (position && position.qty > 0) {
              const eligibility = await canPlaceOrder(client, userId, env, walletId, symbolData.symbol, 'sell', alpacaKey, alpacaSecret);
              
              if (!eligibility.canPlace) {
                console.log(`[${symbolData.symbol}] ⏭️  Skipping SELL order: ${eligibility.reason}`);
              } else {
                const sellOrder = await placeLimitOrder(
                  client, userId, walletId,
                  symbolData.symbol, 'sell', position.qty, sellPrice,
                  alpacaKey, alpacaSecret, env
                );
                orders.push(sellOrder);
                recordOrderFill(walletId, symbolData.symbol);
              }
            }
          }
        } else if (decision === 'SELL') {
          // Only sell order
          if (position && position.qty > 0) {
            // Check if we can place this order (cooldown + conflicts)
            const eligibility = await canPlaceOrder(client, userId, env, walletId, symbolData.symbol, 'sell', alpacaKey, alpacaSecret);
            
            if (!eligibility.canPlace) {
              console.log(`[${symbolData.symbol}] ⏭️  Skipping SELL order: ${eligibility.reason}`);
            } else {
              // Check if price is reasonable (within threshold)
              const currentPrice = position.current_price;
              const priceCheck = isOrderPriceReasonable(symbolData.symbol, sellPrice, currentPrice, 'sell');
              
              if (!priceCheck.reasonable) {
                console.log(`[${symbolData.symbol}] ⏭️  Skipping SELL order: ${priceCheck.reason}`);
              } else {
                // All checks passed - place the order
                const sellOrder = await placeLimitOrder(
                  client, userId, walletId,
                  symbolData.symbol, 'sell', position.qty, sellPrice,
                  alpacaKey, alpacaSecret, env
                );
                orders.push(sellOrder);
                
                // Record fill for cooldown tracking
                recordOrderFill(walletId, symbolData.symbol);
              }
            }
          } else {
            console.log(`[${symbolData.symbol}] No shares to sell`);
          }
        } else if (decision === 'BUY') {
          // Only buy order
          const qtyToBuy = Math.floor(budgetAvailable / buyPrice);
          
          if (qtyToBuy > 0) {
            // Check if we can place this order (cooldown + conflicts)
            const eligibility = await canPlaceOrder(client, userId, env, walletId, symbolData.symbol, 'buy', alpacaKey, alpacaSecret);
            
            if (!eligibility.canPlace) {
              console.log(`[${symbolData.symbol}] ⏭️  Skipping BUY order: ${eligibility.reason}`);
            } else {
              // Check if price is reasonable (within threshold)
              const currentPrice = position ? position.current_price : null;
              const priceCheck = isOrderPriceReasonable(symbolData.symbol, buyPrice, currentPrice, 'buy');
              
              if (!priceCheck.reasonable) {
                console.log(`[${symbolData.symbol}] ⏭️  Skipping BUY order: ${priceCheck.reason}`);
              } else {
                // All checks passed - place the order
                const buyOrder = await placeLimitOrder(
                  client, userId, walletId,
                  symbolData.symbol, 'buy', qtyToBuy, buyPrice,
                  alpacaKey, alpacaSecret, env
                );
                orders.push(buyOrder);
                
                // Record fill for cooldown tracking
                recordOrderFill(walletId, symbolData.symbol);
                
                // Track cumulative spending
                const orderValue = qtyToBuy * buyPrice;
                cumulativeSpent += orderValue;
                console.log(`[${symbolData.symbol}] Order placed: ${qtyToBuy} shares @ $${buyPrice.toFixed(2)} = $${orderValue.toFixed(2)}`);
                console.log(`[${symbolData.symbol}] Total Spent So Far: $${cumulativeSpent.toFixed(2)}`);
              }
            }
          } else {
            console.log('Insufficient budget to buy even 1 share');
          }
        }
        
        // COMMIT this symbol's transaction
        await client.query('COMMIT');
        console.log(`✓ ${symbolData.symbol} transaction committed`);
        
        results.push({
          symbol: symbolData.symbol,
          decision,
          orders
        });
        
      } catch (error) {
        // ROLLBACK only this symbol's transaction
        await client.query('ROLLBACK');
        console.error(`✗ Error processing ${symbolData.symbol}:`, error.message);
        
        // Log error in a separate transaction
        try {
          await client.query('BEGIN');
          await client.query(
            `INSERT INTO execution_errors (user_id, wallet_id, symbol, error_type, error_message, error_stack, function_name)
             VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [userId, walletId, symbolData.symbol, 'EXECUTION_ERROR', error.message, error.stack, 'executeWallet']
          );
          await client.query('COMMIT');
        } catch (logError) {
          await client.query('ROLLBACK');
          console.error('Failed to log error:', logError);
        }
        
        results.push({
          symbol: symbolData.symbol,
          error: error.message
        });
        
        // CONTINUE to next symbol - don't let one failure stop the rest
      }
    }
    
    console.log(`\n========================================`);
    console.log(`Execution complete for wallet ${walletId}`);
    console.log(`Processed ${results.length} symbols`);
    console.log(`Successful: ${results.filter(r => !r.error).length}`);
    console.log(`Failed: ${results.filter(r => r.error).length}`);
    console.log(`========================================\n`);
    
    return {
      success: true,
      walletId,
      results
    };
    
  } catch (error) {
    console.error('Critical error in executeWallet:', error);
    
    // Log critical error in a separate transaction
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO execution_errors (user_id, wallet_id, error_type, error_message, error_stack, function_name, severity)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [userId, walletId, 'CRITICAL_ERROR', error.message, error.stack, 'executeWallet', 'CRITICAL']
      );
      await client.query('COMMIT');
    } catch (logError) {
      await client.query('ROLLBACK');
      console.error('Failed to log critical error:', logError);
    }
    
    // Return error details instead of throwing (prevents 500 error to frontend)
    return {
      success: false,
      error: error.message,
      walletId,
      stack: error.stack
    };
  } finally {
    client.release();
  }
}

// ============================================================================
// CALLABLE FUNCTIONS
// ============================================================================

// Save API Keys
exports.saveApiKeys = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { polygonKey, alpacaPaperKey, alpacaPaperSecret } = request.data || {};

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureUserApiKeysTable(client);

    await client.query(
      `INSERT INTO user_api_keys (user_id, polygon_key, alpaca_paper_key, alpaca_paper_secret, updated_at)
       VALUES ($1, $2::TEXT, $3::TEXT, $4::TEXT, NOW())
       ON CONFLICT (user_id)
       DO UPDATE SET
         polygon_key = EXCLUDED.polygon_key,
         alpaca_paper_key = EXCLUDED.alpaca_paper_key,
         alpaca_paper_secret = EXCLUDED.alpaca_paper_secret,
         updated_at = NOW()`,
      [uid, polygonKey || null, alpacaPaperKey || null, alpacaPaperSecret || null]
    );

    return { success: true };
  } catch (e) {
    console.error("saveApiKeys error:", e);
    throw new HttpsError("internal", "Failed to save API keys.");
  } finally {
    client.release();
  }
});

// Load API Keys
exports.loadApiKeys = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureUserApiKeysTable(client);

    const { rows } = await client.query(
      `SELECT polygon_key, alpaca_paper_key, alpaca_paper_secret FROM user_api_keys WHERE user_id=$1`, [uid]
    );

    if (rows.length === 0) {
      return { success: true, data: { polygonKey: null, alpacaPaperKey: null, alpacaPaperSecret: null } };
    }

    const row = rows[0];
    return {
      success: true,
      data: {
        polygonKey: row.polygon_key || null,
        alpacaPaperKey: row.alpaca_paper_key || null,
        alpacaPaperSecret: row.alpaca_paper_secret || null,
      },
    };
  } catch (e) {
    console.error("loadApiKeys error:", e);
    throw new HttpsError("internal", "Failed to load API keys.");
  } finally {
    client.release();
  }
});

// Save API Keys to wallet_api_keys table
exports.saveWalletApiKeys = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const userId = request.auth.uid;
  const { walletId, polygonKey, alpacaPaperKey, alpacaPaperSecret } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required");

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    // Verify wallet belongs to user
    const walletCheck = await client.query(
      'SELECT wallet_id FROM wallets WHERE wallet_id = $1 AND user_id = $2',
      [walletId, userId]
    );
    
    if (walletCheck.rows.length === 0) {
      throw new HttpsError('permission-denied', 'Wallet not found or access denied');
    }

    // Ensure wallet_api_keys table exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS wallet_api_keys (
        wallet_id UUID PRIMARY KEY REFERENCES wallets(wallet_id) ON DELETE CASCADE,
        polygon_key TEXT,
        alpaca_paper_key TEXT,
        alpaca_paper_secret TEXT,
        alpaca_live_key TEXT,
        alpaca_live_secret TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Insert or update keys
    await client.query(
      `INSERT INTO wallet_api_keys (wallet_id, polygon_key, alpaca_paper_key, alpaca_paper_secret, updated_at)
       VALUES ($1, $2::TEXT, $3::TEXT, $4::TEXT, NOW())
       ON CONFLICT (wallet_id)
       DO UPDATE SET
         polygon_key = EXCLUDED.polygon_key,
         alpaca_paper_key = EXCLUDED.alpaca_paper_key,
         alpaca_paper_secret = EXCLUDED.alpaca_paper_secret,
         updated_at = NOW()`,
      [walletId, polygonKey || null, alpacaPaperKey || null, alpacaPaperSecret || null]
    );

    console.log(`[saveWalletApiKeys] Keys saved for wallet ${walletId}`);
    return { success: true };
  } catch (e) {
    console.error("saveWalletApiKeys error:", e);
    throw new HttpsError("internal", "Failed to save API keys: " + e.message);
  } finally {
    client.release();
  }
});

// Load API Keys from wallet_api_keys table (with fallback to user_api_keys)
exports.loadWalletApiKeys = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const userId = request.auth.uid;
  const { walletId } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required");

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    // Verify wallet belongs to user
    const walletCheck = await client.query(
      'SELECT wallet_id FROM wallets WHERE wallet_id = $1 AND user_id = $2',
      [walletId, userId]
    );
    
    if (walletCheck.rows.length === 0) {
      throw new HttpsError('permission-denied', 'Wallet not found or access denied');
    }

    // Try to load from wallet_api_keys first
    let keys = null;
    try {
      const walletKeysResult = await client.query(
        `SELECT polygon_key, alpaca_paper_key, alpaca_paper_secret FROM wallet_api_keys WHERE wallet_id=$1`,
        [walletId]
      );
      if (walletKeysResult.rows.length > 0) {
        keys = walletKeysResult.rows[0];
        console.log(`[loadWalletApiKeys] Loaded keys from wallet_api_keys for wallet ${walletId}`);
      }
    } catch (e) {
      console.log(`[loadWalletApiKeys] wallet_api_keys table doesn't exist or error: ${e.message}`);
    }

    // Fallback to user_api_keys if no wallet-specific keys found
    if (!keys || (!keys.polygon_key && !keys.alpaca_paper_key)) {
      console.log(`[loadWalletApiKeys] No wallet-specific keys, falling back to user_api_keys`);
      const userKeysResult = await client.query(
        `SELECT polygon_key, alpaca_paper_key, alpaca_paper_secret FROM user_api_keys WHERE user_id=$1`,
        [userId]
      );
      if (userKeysResult.rows.length > 0) {
        keys = userKeysResult.rows[0];
        console.log(`[loadWalletApiKeys] Loaded keys from user_api_keys for user ${userId}`);
      }
    }

    return {
      success: true,
      data: {
        polygonKey: keys?.polygon_key || "",
        alpacaPaperKey: keys?.alpaca_paper_key || "",
        alpacaPaperSecret: keys?.alpaca_paper_secret || ""
      }
    };
  } catch (e) {
    console.error("loadWalletApiKeys error:", e);
    throw new HttpsError("internal", "Failed to load API keys: " + e.message);
  } finally {
    client.release();
  }
});


// Create Wallet
exports.createWallet = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { env, name } = request.data || {};

  if (!env || !name) {
    throw new HttpsError("invalid-argument", "env and name are required.");
  }
  if (!["paper", "live"].includes(env)) {
    throw new HttpsError("invalid-argument", "env must be 'paper' or 'live'.");
  }

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);

    const walletId = randomUUID();
    await client.query(
      `INSERT INTO wallets(wallet_id, user_id, env, name, enabled, created_at, updated_at)
       VALUES($1,$2,$3,$4,false,NOW(),NOW())`,
      [walletId, uid, env, name]
    );

    await client.query(
      `INSERT INTO wallet_settings(wallet_id, updated_at) VALUES($1, NOW())`,
      [walletId]
    );

    return { success: true, walletId };
  } catch (e) {
    console.error("createWallet error:", e);
    throw new HttpsError("internal", "Failed to create wallet.");
  } finally {
    client.release();
  }
});

// List Wallets
exports.listWallets = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);

    const { rows } = await client.query(
      `SELECT wallet_id, env, name, enabled, created_at, updated_at
       FROM wallets WHERE user_id=$1 ORDER BY created_at DESC`,
      [uid]
    );

    return { success: true, wallets: rows };
  } catch (e) {
    console.error("listWallets error:", e);
    throw new HttpsError("internal", "Failed to list wallets.");
  } finally {
    client.release();
  }
});

// Update Wallet
exports.updateWallet = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, name, enabled } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    const updates = [];
    const values = [];
    let idx = 1;

    if (name !== undefined) {
      updates.push(`name=$${idx++}`);
      values.push(name);
    }
    if (enabled !== undefined) {
      updates.push(`enabled=$${idx++}`);
      values.push(enabled);
    }
    updates.push(`updated_at=NOW()`);
    values.push(walletId);

    if (updates.length > 1) {
      await client.query(
        `UPDATE wallets SET ${updates.join(", ")} WHERE wallet_id=$${idx}`,
        values
      );
    }

    return { success: true };
  } catch (e) {
    console.error("updateWallet error:", e);
    throw new HttpsError("internal", "Failed to update wallet.");
  } finally {
    client.release();
  }
});

// Delete Wallet
exports.deleteWallet = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    await client.query(`DELETE FROM wallets WHERE wallet_id=$1`, [walletId]);

    return { success: true };
  } catch (e) {
    console.error("deleteWallet error:", e);
    throw new HttpsError("internal", "Failed to delete wallet.");
  } finally {
    client.release();
  }
});

// List Wallet Symbols - CORRECTED to use wallet_symbols table
exports.listWalletSymbols = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    const { rows } = await client.query(
      `SELECT * FROM wallet_symbols WHERE wallet_id=$1 ORDER BY symbol`,
      [walletId]
    );

    return { success: true, symbols: rows };
  } catch (e) {
    console.error("listWalletSymbols error:", e);
    throw new HttpsError("internal", "Failed to list symbols.");
  } finally {
    client.release();
  }
});

// Upsert Wallet Symbol - CORRECTED to use wallet_symbols table
exports.upsertWalletSymbol = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  
  // Accept both camelCase (from frontend) and snake_case
  const data = request.data || {};
  const walletId = data.walletId;
  const symbol = data.symbol;
  const buy_budget_usd = data.buyBudgetUsd || data.buy_budget_usd;
  const budget_mode = data.budgetMode || data.budget_mode || 'fixed';
  const percent_budget = data.percentBudget || data.percent_budget;
  const buy_pct_rth = data.buyPctRth || data.buy_pct_rth;
  const sell_pct_rth = data.sellPctRth || data.sell_pct_rth;
  const buy_pct_ah = data.buyPctAh || data.buy_pct_ah;
  const sell_pct_ah = data.sellPctAh || data.sell_pct_ah;
  const method_rth = data.methodRth || data.method_rth;
  const method_ah = data.methodAh || data.method_ah;
  const enabled = data.enabled;

  if (!walletId || !symbol) {
    throw new HttpsError("invalid-argument", "walletId and symbol are required.");
  }

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    console.log(`Upserting symbol ${symbol}: budget=${buy_budget_usd}, budget_mode=${budget_mode}, percent_budget=${percent_budget}, buy_rth=${buy_pct_rth}, sell_rth=${sell_pct_rth}, method_rth=${method_rth}, method_ah=${method_ah}`);

    await client.query(
      `INSERT INTO wallet_symbols(wallet_id, symbol, buy_budget_usd, budget_mode, percent_budget, buy_pct_rth, sell_pct_rth, buy_pct_ah, sell_pct_ah, method_rth, method_ah, enabled, updated_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
       ON CONFLICT (wallet_id, symbol)
       DO UPDATE SET
         buy_budget_usd = EXCLUDED.buy_budget_usd,
         budget_mode = EXCLUDED.budget_mode,
         percent_budget = EXCLUDED.percent_budget,
         buy_pct_rth = EXCLUDED.buy_pct_rth,
         sell_pct_rth = EXCLUDED.sell_pct_rth,
         buy_pct_ah = EXCLUDED.buy_pct_ah,
         sell_pct_ah = EXCLUDED.sell_pct_ah,
         method_rth = EXCLUDED.method_rth,
         method_ah = EXCLUDED.method_ah,
         enabled = EXCLUDED.enabled,
         updated_at = NOW()`,
      [walletId, symbol, buy_budget_usd, budget_mode, percent_budget, buy_pct_rth, sell_pct_rth, buy_pct_ah, sell_pct_ah, method_rth, method_ah, enabled !== false]
    );

    return { success: true };
  } catch (e) {
    console.error("upsertWalletSymbol error:", e);
    throw new HttpsError("internal", "Failed to upsert symbol.");
  } finally {
    client.release();
  }
});

// Delete Wallet Symbol - CORRECTED to use wallet_symbols table
exports.deleteWalletSymbol = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, symbol } = request.data || {};

  if (!walletId || !symbol) {
    throw new HttpsError("invalid-argument", "walletId and symbol are required.");
  }

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    await client.query(
      `DELETE FROM wallet_symbols WHERE wallet_id=$1 AND symbol=$2`,
      [walletId, symbol]
    );

    return { success: true };
  } catch (e) {
    console.error("deleteWalletSymbol error:", e);
    throw new HttpsError("internal", "Failed to delete symbol.");
  } finally {
    client.release();
  }
});

// Get Wallet Snapshot
exports.getWalletSnapshot = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);
    await ensureUserApiKeysTable(client);
    await assertWalletOwnership(client, walletId, uid);

    // Use the new helper to get the correct keys for the wallet's environment
    const { env, alpacaKey, alpacaSecret, userId } = await getWalletEnvAndKeys(client, walletId, uid);

    const account = await alpacaGET(client, userId, walletId, env, "/v2/account", alpacaKey, alpacaSecret);

    const snapshot = {
      equity: parseFloat(account.equity || 0),
      cash: parseFloat(account.cash || 0),
      positionsValue: parseFloat(account.long_market_value || 0),
    };

    return { success: true, snapshot };
  } catch (e) {
    console.error("getWalletSnapshot error:", e);
    throw new HttpsError("internal", "Failed to load wallet snapshot.");
  } finally {
    client.release();
  }
});

// Get Wallet Baselines - CORRECTED to use wallet_symbols and baseline_daily properly
exports.getWalletBaselines = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId } = request.data || {};
  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);
    await ensureBaselineSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    // Fetch baselines for wallet's symbols - CORRECTED QUERY
    const { rows } = await client.query(
      `SELECT bd.symbol, bd.session, bd.method, bd.trading_day, bd.baseline
       FROM baseline_daily bd
       JOIN wallet_symbols ws ON bd.symbol = ws.symbol
       WHERE ws.wallet_id = $1
       ORDER BY bd.symbol, bd.session, bd.trading_day DESC`,
      [walletId]
    );

    // Group by symbol and session, taking the most recent baseline for each
    const baselines = {};
    for (const row of rows) {
      const sym = row.symbol;
      const session = row.session;
      if (!baselines[sym]) baselines[sym] = {};
      if (!baselines[sym][session]) {
        baselines[sym][session] = {
          value: Number(row.baseline),
          method: row.method,
          // Convert DATE to ISO string format to avoid timezone issues
             as_of_date: row.trading_day ? new Date(row.trading_day).toISOString().split('T')[0] : null
        };
      }
    }

    return { success: true, baselines };
  } catch (e) {
    console.error("getWalletBaselines error:", e);
    throw new HttpsError("internal", "Failed to get baselines.");
  } finally {
    client.release();
  }
});

// ============================================================================
// BASELINE COMPUTATION FUNCTIONS
// ============================================================================

// Helper: Fetch Polygon Minute Bars
async function fetchPolygonBars(symbol, date, polygonKey) {
  const dateStr = date.toISOString().split('T')[0];
  const url = `https://api.polygon.io/v2/aggs/ticker/${symbol}/range/1/minute/${dateStr}/${dateStr}?adjusted=true&sort=asc&limit=50000&apiKey=${polygonKey}`;
  
  console.log(`Fetching Polygon data: ${symbol} on ${dateStr}`);
  
  const response = await fetch(url);
  
  console.log(`Polygon response for ${symbol}: ${response.status} ${response.statusText}`);
  
  if (!response.ok) {
    const errorText = await response.text();
    console.error(`Polygon API error for ${symbol}:`, errorText);
    throw new Error(`Polygon API error for ${symbol}: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json();
  
  console.log(`Polygon data for ${symbol}: resultsCount=${data.resultsCount}, queryCount=${data.queryCount}, status=${data.status}`);
  
  if (!data.results || data.results.length === 0) {
    console.error(`No data returned for ${symbol} on ${dateStr}. Response:`, JSON.stringify(data));
    throw new Error(`No data returned for ${symbol} on ${dateStr}. Status: ${data.status}`);
  }
  
  console.log(`Successfully fetched ${data.results.length} bars for ${symbol}`);
  
  return data.results.map(bar => ({
    timestamp: bar.t,
    open: bar.o,
    high: bar.h,
    low: bar.l,
    close: bar.c,
    volume: bar.v
  }));
}

// Helper: Split Bars into RTH and AH Sessions
   function splitIntoSessions(bars) {
     const rthBars = [];
     const ahBars = [];
     
     for (const bar of bars) {
       const date = new Date(bar.timestamp);
       
       // Use Intl API to get ET time (handles DST automatically)
       const formatter = new Intl.DateTimeFormat('en-US', {
         timeZone: 'America/New_York',
         hour: '2-digit',
         minute: '2-digit',
         hour12: false
       });
       
       const parts = formatter.formatToParts(date);
       const hour = parseInt(parts.find(p => p.type === 'hour').value);
       const minute = parseInt(parts.find(p => p.type === 'minute').value);
       const timeInMinutes = hour * 60 + minute;
       
       // RTH: 9:30 AM - 4:00 PM ET (570 - 960 minutes)
       if (timeInMinutes >= 570 && timeInMinutes < 960) {
         rthBars.push(bar);
       } 
       // AH: 4:00 PM - 8:00 PM ET (960 - 1200 minutes)
       else if (timeInMinutes >= 960 && timeInMinutes < 1200) {
         ahBars.push(bar);
       }
     }
     
     return { rthBars, ahBars };
   }
   
// Helper: Align BTC and Stock Bars
function alignBars(btcBars, stockBars) {
  const aligned = [];
  const stockMap = new Map();
  
  for (const bar of stockBars) {
    stockMap.set(bar.timestamp, bar);
  }
  
  for (const btcBar of btcBars) {
    const stockBar = stockMap.get(btcBar.timestamp);
    if (stockBar && stockBar.close > 0 && btcBar.close > 0) {
      aligned.push({
        timestamp: btcBar.timestamp,
        btc_close: btcBar.close,
        btc_volume: btcBar.volume,
        stock_close: stockBar.close,
        stock_volume: stockBar.volume,
        ratio: btcBar.close / stockBar.close
      });
    }
  }
  
  return aligned;
}

// Method 1: EQUAL_MEAN
function computeEqualMean(alignedBars) {
  if (alignedBars.length === 0) return null;
  const sum = alignedBars.reduce((acc, bar) => acc + bar.ratio, 0);
  return sum / alignedBars.length;
}

// Method 2: MEDIAN
function computeMedian(alignedBars) {
  if (alignedBars.length === 0) return null;
  const ratios = alignedBars.map(bar => bar.ratio).sort((a, b) => a - b);
  const mid = Math.floor(ratios.length / 2);
  return ratios.length % 2 === 0 ? (ratios[mid - 1] + ratios[mid]) / 2 : ratios[mid];
}

// Method 3: VWAP_RATIO
function computeVWAPRatio(alignedBars) {
  if (alignedBars.length === 0) return null;
  
  // Calculate VWAP for BTC
  let btcVWAPSum = 0;
  let btcVolumeSum = 0;
  for (const bar of alignedBars) {
    btcVWAPSum += bar.btc_close * bar.btc_volume;
    btcVolumeSum += bar.btc_volume;
  }
  const btcVWAP = btcVolumeSum === 0 ? 0 : btcVWAPSum / btcVolumeSum;
  
  // Calculate VWAP for Stock
  let stockVWAPSum = 0;
  let stockVolumeSum = 0;
  for (const bar of alignedBars) {
    stockVWAPSum += bar.stock_close * bar.stock_volume;
    stockVolumeSum += bar.stock_volume;
  }
  const stockVWAP = stockVolumeSum === 0 ? 0 : stockVWAPSum / stockVolumeSum;
  
  // Return ratio of VWAPs
  return stockVWAP === 0 ? null : btcVWAP / stockVWAP;
}

// Method 4: VOL_WEIGHTED
function computeVolWeighted(alignedBars) {
  if (alignedBars.length === 0) return null;
  let weightedSum = 0;
  let totalWeight = 0;
  
  for (const bar of alignedBars) {
    weightedSum += bar.ratio * bar.stock_volume;
    totalWeight += bar.stock_volume;
  }
  
  return totalWeight === 0 ? null : weightedSum / totalWeight;
}

// Method 5: WINSORIZED
function computeWinsorized(alignedBars) {
  if (alignedBars.length === 0) return null;
  const ratios = alignedBars.map(bar => bar.ratio).sort((a, b) => a - b);
  
  const p5Index = Math.floor(ratios.length * 0.05);
  const p95Index = Math.floor(ratios.length * 0.95);
  const p5Value = ratios[p5Index];
  const p95Value = ratios[p95Index];
  
  const winsorized = ratios.map(r => {
    if (r < p5Value) return p5Value;
    if (r > p95Value) return p95Value;
    return r;
  });
  
  const sum = winsorized.reduce((acc, val) => acc + val, 0);
  return sum / winsorized.length;
}

// Main: Compute Baselines for a Symbol
async function computeBaselinesForSymbol(client, symbol, date, polygonKey, userId, walletId) {
  try {
    console.log(`Computing baselines for ${symbol} on ${date.toISOString().split('T')[0]}`);
    
    const btcBars = await fetchPolygonBars('X:BTCUSD', date, polygonKey);
    const stockBars = await fetchPolygonBars(symbol, date, polygonKey);
    
    const { rthBars: btcRTH, ahBars: btcAH } = splitIntoSessions(btcBars);
    const { rthBars: stockRTH, ahBars: stockAH } = splitIntoSessions(stockBars);
    
    const rthAligned = alignBars(btcRTH, stockRTH);
    const ahAligned = alignBars(btcAH, stockAH);
    
    console.log(`Aligned: ${rthAligned.length} RTH bars, ${ahAligned.length} AH bars`);
    
    if (rthAligned.length === 0 && ahAligned.length === 0) {
      throw new Error(`No aligned data for ${symbol}`);
    }
    
    const results = [];
    const dateStr = date.toISOString().split('T')[0];
    
    // RTH Session
    if (rthAligned.length > 0) {
      const methods = {
        'EQUAL_MEAN': computeEqualMean(rthAligned),
        'MEDIAN': computeMedian(rthAligned),
        'VWAP_RATIO': computeVWAPRatio(rthAligned),
        'VOL_WEIGHTED': computeVolWeighted(rthAligned),
        'WINSORIZED': computeWinsorized(rthAligned)
      };
      
      for (const [method, baseline] of Object.entries(methods)) {
        if (baseline !== null && baseline > 0) {
          await client.query(
            `INSERT INTO baseline_daily (trading_day, symbol, session, method, baseline, sample_count, source, computed_at)
             VALUES ($1, $2, 'RTH', $3, $4, $5, 'polygon', NOW())
             ON CONFLICT (trading_day, symbol, session, method)
             DO UPDATE SET baseline = EXCLUDED.baseline, sample_count = EXCLUDED.sample_count, computed_at = NOW()`,
            [dateStr, symbol, method, baseline, rthAligned.length]
          );
          
          results.push({ session: 'RTH', method, baseline: baseline.toFixed(4), sample_count: rthAligned.length });
          console.log(`  RTH ${method}: ${baseline.toFixed(4)}`);
        }
      }
    }
    
    // AH Session
    if (ahAligned.length > 0) {
      const methods = {
        'EQUAL_MEAN': computeEqualMean(ahAligned),
        'MEDIAN': computeMedian(ahAligned),
        'VWAP_RATIO': computeVWAPRatio(ahAligned),
        'VOL_WEIGHTED': computeVolWeighted(ahAligned),
        'WINSORIZED': computeWinsorized(ahAligned)
      };
      
      for (const [method, baseline] of Object.entries(methods)) {
        if (baseline !== null && baseline > 0) {
          await client.query(
            `INSERT INTO baseline_daily (trading_day, symbol, session, method, baseline, sample_count, source, computed_at)
             VALUES ($1, $2, 'AH', $3, $4, $5, 'polygon', NOW())
             ON CONFLICT (trading_day, symbol, session, method)
             DO UPDATE SET baseline = EXCLUDED.baseline, sample_count = EXCLUDED.sample_count, computed_at = NOW()`,
            [dateStr, symbol, method, baseline, ahAligned.length]
          );
          
          results.push({ session: 'AH', method, baseline: baseline.toFixed(4), sample_count: ahAligned.length });
          console.log(`  AH ${method}: ${baseline.toFixed(4)}`);
        }
      }
    }
    
    return { success: true, symbol, date: dateStr, results };
    
  } catch (error) {
    console.error(`Error computing baselines for ${symbol}:`, error);
    
    await client.query(
      `INSERT INTO execution_errors (user_id, wallet_id, symbol, error_type, error_message, error_stack, function_name)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [userId, walletId, symbol, 'BASELINE_ERROR', error.message, error.stack, 'computeBaselinesForSymbol']
    );
    
    return { success: false, symbol, error: error.message };
  }
}

// Compute Wallet Baselines
exports.computeWalletBaselines = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, date } = request.data || {};
  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    await ensureWalletSchema(client);
    await ensureBaselineSchema(client);
    await ensureUserApiKeysTable(client);
    await assertWalletOwnership(client, walletId, uid);

    // Get Polygon key
    const polygonKey = await getPolygonKey(client, walletId, uid);
    
    // Get wallet symbols
    const { rows: symbols } = await client.query(
      `SELECT DISTINCT symbol FROM wallet_symbols WHERE wallet_id = $1 AND enabled = true`,
      [walletId]
    );
    
    if (symbols.length === 0) {
      throw new HttpsError("invalid-argument", "No enabled symbols in wallet");
    }
    
    // Use trading_calendar to find the last open trading day
    let targetDate;
    if (date) {
      targetDate = new Date(date);
    } else {
      // Get today's date
      const today = new Date();
      const todayStr = today.toISOString().split('T')[0];
      
      // Query trading_calendar to find the previous open trading day
      const calResult = await client.query(
        `SELECT prev_open_date 
         FROM trading_calendar 
         WHERE cal_date::date = $1::date 
         AND prev_open_date IS NOT NULL`,
        [todayStr]
      );
      
      if (calResult.rows.length > 0 && calResult.rows[0].prev_open_date) {
        targetDate = new Date(calResult.rows[0].prev_open_date);
        console.log(`Using trading_calendar: today=${todayStr}, prev_open_date=${targetDate.toISOString().split('T')[0]}`);
      } else {
        // Fallback: use yesterday if trading_calendar doesn't have data
        targetDate = new Date(Date.now() - 24 * 60 * 60 * 1000);
        console.log(`Trading_calendar has no data for ${todayStr}, using yesterday as fallback`);
      }
    }
    
    console.log(`Target date for baseline computation: ${targetDate.toISOString().split('T')[0]}`);
    
    console.log(`Computing baselines for ${symbols.length} symbols on ${targetDate.toISOString().split('T')[0]}`);
    
    // Compute baselines for each symbol
    const results = [];
    for (const { symbol } of symbols) {
      const result = await computeBaselinesForSymbol(client, symbol, targetDate, polygonKey, uid, walletId);
      results.push(result);
    }
    
    const successful = results.filter(r => r.success).length;
    const failed = results.filter(r => !r.success).length;
    
    console.log(`\n========================================`);
    console.log(`Baseline Computation Complete`);
    console.log(`Date: ${targetDate.toISOString().split('T')[0]}`);
    console.log(`Symbols: ${symbols.length}`);
    console.log(`Successful: ${successful}`);
    console.log(`Failed: ${failed}`);
    console.log(`========================================\n`);
    
    return { 
      success: true, 
      date: targetDate.toISOString().split('T')[0],
      computed: successful,
      failed: failed,
      totalSymbols: symbols.length,
      results: results
    };
  } catch (e) {
    console.error("computeWalletBaselines error:", e);
    throw new HttpsError("internal", e.message || "Failed to compute baselines.");
  } finally {
    client.release();
  }
});

// Manual execution trigger
exports.runWalletExecute = onCall({ cors: true }, async (request) => {
  const userId = request.auth?.uid;
  if (!userId) throw new HttpsError("unauthenticated", "User not authenticated");

  const { walletId } = request.data;
  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required");

  try {
    const result = await executeWallet(userId, walletId);
    return result;
  } catch (error) {
    console.error("runWalletExecute error:", error);
    throw new HttpsError("internal", "Failed to execute wallet");
  }
});
// ============================================================================
// BASELINE BACKFILL SYSTEM - Backend Functions
// ============================================================================

// Ensure backfill tables exist
async function ensureBackfillSchema(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS backfill_jobs (
      id SERIAL PRIMARY KEY,
      user_id VARCHAR(100) NOT NULL,
      wallet_id VARCHAR(100) NOT NULL,
      job_name VARCHAR(100) NOT NULL,
      start_date DATE NOT NULL,
      end_date DATE NOT NULL,
      symbols TEXT[] NOT NULL,
      store_minute_bars BOOLEAN DEFAULT false,
      status VARCHAR(20) DEFAULT 'pending',
      total_days INTEGER NOT NULL,
      completed_days INTEGER DEFAULT 0,
      failed_days INTEGER DEFAULT 0,
      total_baselines INTEGER DEFAULT 0,
      started_at TIMESTAMP,
      completed_at TIMESTAMP,
      error_message TEXT,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE IF NOT EXISTS backfill_progress (
      id SERIAL PRIMARY KEY,
      job_id INTEGER NOT NULL REFERENCES backfill_jobs(id) ON DELETE CASCADE,
      trading_day DATE NOT NULL,
      symbol VARCHAR(10) NOT NULL,
      status VARCHAR(20) DEFAULT 'pending',
      baselines_computed INTEGER DEFAULT 0,
      bars_fetched INTEGER DEFAULT 0,
      error_message TEXT,
      started_at TIMESTAMP,
      completed_at TIMESTAMP,
      CONSTRAINT unique_job_day_symbol UNIQUE(job_id, trading_day, symbol)
    );
    
    CREATE INDEX IF NOT EXISTS idx_backfill_jobs_user_id ON backfill_jobs(user_id);
    CREATE INDEX IF NOT EXISTS idx_backfill_jobs_wallet_id ON backfill_jobs(wallet_id);
    CREATE INDEX IF NOT EXISTS idx_backfill_jobs_status ON backfill_jobs(status);
    CREATE INDEX IF NOT EXISTS idx_backfill_progress_job_id ON backfill_progress(job_id);
  `);
}

exports.computeBaselinesForDateRange = onCall({ cors: true, timeoutSeconds: 540 }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, startDate, endDate, symbols, storeMinuteBars = false, jobName } = request.data || {};
  
  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");
  if (!startDate || !endDate) throw new HttpsError("invalid-argument", "startDate and endDate are required.");
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  
  try {
    await ensureWalletSchema(client);
    await ensureBaselineSchema(client);
    await ensureBackfillSchema(client);
    await assertWalletOwnership(client, walletId, uid);
    
    const polygonKey = await getPolygonKey(client, walletId, uid);
    if (!polygonKey) throw new HttpsError("failed-precondition", "Polygon API key not configured.");
    
    const { rows: tradingDays } = await client.query(
      `SELECT cal_date FROM trading_calendar 
       WHERE cal_date BETWEEN $1 AND $2 
       AND is_open = true 
       ORDER BY cal_date`,
      [startDate, endDate]
    );
    
    if (tradingDays.length === 0) {
      throw new HttpsError("invalid-argument", "No trading days found in the specified date range.");
    }
    
    let symbolsToProcess = symbols;
    if (!symbolsToProcess || symbolsToProcess.length === 0) {
      const { rows: walletSymbols } = await client.query(
        `SELECT symbol FROM wallet_symbols WHERE wallet_id = $1 AND enabled = true`,
        [walletId]
      );
      symbolsToProcess = walletSymbols.map(r => r.symbol);
    }
    
    if (symbolsToProcess.length === 0) {
      throw new HttpsError("invalid-argument", "No symbols to process.");
    }
    
    const { rows: [job] } = await client.query(
      `INSERT INTO backfill_jobs 
       (user_id, wallet_id, job_name, start_date, end_date, symbols, store_minute_bars, total_days, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'running')
       RETURNING id`,
      [uid, walletId, jobName || `Backfill ${startDate} to ${endDate}`, startDate, endDate, symbolsToProcess, storeMinuteBars, tradingDays.length]
    );
    
    const jobId = job.id;
    
    await client.query(
      `UPDATE backfill_jobs SET started_at = NOW() WHERE id = $1`,
      [jobId]
    );
    
    for (const day of tradingDays) {
      for (const symbol of symbolsToProcess) {
        await client.query(
          `INSERT INTO backfill_progress (job_id, trading_day, symbol, status)
           VALUES ($1, $2, $3, 'pending')
           ON CONFLICT (job_id, trading_day, symbol) DO NOTHING`,
          [jobId, day.cal_date, symbol]
        );
      }
    }
    
    let completedDays = 0;
    let failedDays = 0;
    let totalBaselines = 0;
    
    for (const day of tradingDays) {
      const dateStr = day.cal_date.toISOString().split('T')[0];
      console.log(`Processing ${dateStr}...`);
      
      let daySuccess = true;
      
      for (const symbol of symbolsToProcess) {
        try {
          await client.query(
            `UPDATE backfill_progress 
             SET status = 'running', started_at = NOW() 
             WHERE job_id = $1 AND trading_day = $2 AND symbol = $3`,
            [jobId, dateStr, symbol]
          );
          
          const result = await computeBaselinesForSymbol(client, symbol, new Date(dateStr), polygonKey, uid, walletId);
          
          if (result.success) {
            const baselinesCount = result.results.length;
            totalBaselines += baselinesCount;
            
            await client.query(
              `UPDATE backfill_progress 
               SET status = 'success', baselines_computed = $1, completed_at = NOW()
               WHERE job_id = $2 AND trading_day = $3 AND symbol = $4`,
              [baselinesCount, jobId, dateStr, symbol]
            );
            
            console.log(`  ${symbol}: ${baselinesCount} baselines computed`);
          }
          
          await new Promise(resolve => setTimeout(resolve, 10)); // 10ms for premium Polygon plan (100 req/sec)
          
        } catch (error) {
          console.error(`  ${symbol}: Error - ${error.message}`);
          daySuccess = false;
          
          await client.query(
            `UPDATE backfill_progress 
             SET status = 'failed', error_message = $1, completed_at = NOW()
             WHERE job_id = $2 AND trading_day = $3 AND symbol = $4`,
            [error.message, jobId, dateStr, symbol]
          );
        }
      }
      
      if (daySuccess) {
        completedDays++;
      } else {
        failedDays++;
      }
      
      await client.query(
        `UPDATE backfill_jobs 
         SET completed_days = $1, failed_days = $2, total_baselines = $3, updated_at = NOW()
         WHERE id = $4`,
        [completedDays, failedDays, totalBaselines, jobId]
      );
    }
    
    await client.query(
      `UPDATE backfill_jobs 
       SET status = 'completed', completed_at = NOW(), updated_at = NOW()
       WHERE id = $1`,
      [jobId]
    );
    
    console.log(`Backfill job ${jobId} completed: ${completedDays} days successful, ${failedDays} days failed, ${totalBaselines} baselines computed`);
    
    return {
      success: true,
      jobId,
      completedDays,
      failedDays,
      totalBaselines,
      message: `Backfill completed: ${completedDays}/${tradingDays.length} days processed, ${totalBaselines} baselines computed`
    };
    
  } catch (e) {
    console.error("computeBaselinesForDateRange error:", e);
    throw new HttpsError("internal", `Failed to start backfill: ${e.message}`);
  } finally {
    client.release();
  }
});

exports.getBackfillProgress = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { jobId } = request.data || {};
  
  if (!jobId) throw new HttpsError("invalid-argument", "jobId is required.");
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  
  try {
    const { rows: [job] } = await client.query(
      `SELECT * FROM backfill_jobs WHERE id = $1 AND user_id = $2`,
      [jobId, uid]
    );
    
    if (!job) throw new HttpsError("not-found", "Job not found.");
    
    const { rows: progress } = await client.query(
      `SELECT trading_day, symbol, status, baselines_computed, error_message, completed_at
       FROM backfill_progress
       WHERE job_id = $1
       ORDER BY trading_day DESC, symbol`,
      [jobId]
    );
    
    const { rows: errors } = await client.query(
      `SELECT trading_day, symbol, error_message
       FROM backfill_progress
       WHERE job_id = $1 AND status = 'failed'
       ORDER BY trading_day DESC, symbol
       LIMIT 20`,
      [jobId]
    );
    
    return {
      success: true,
      job: {
        id: job.id,
        jobName: job.job_name,
        startDate: job.start_date,
        endDate: job.end_date,
        symbols: job.symbols,
        status: job.status,
        totalDays: job.total_days,
        completedDays: job.completed_days,
        failedDays: job.failed_days,
        totalBaselines: job.total_baselines,
        startedAt: job.started_at,
        completedAt: job.completed_at
      },
      progress,
      errors
    };
    
  } catch (e) {
    console.error("getBackfillProgress error:", e);
    throw new HttpsError("internal", "Failed to get backfill progress.");
  } finally {
    client.release();
  }
});

exports.listBackfillJobs = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, limit = 20 } = request.data || {};
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  
  try {
    let query = `SELECT * FROM backfill_jobs WHERE user_id = $1`;
    const params = [uid];
    
    if (walletId) {
      query += ` AND wallet_id = $2`;
      params.push(walletId);
    }
    
    query += ` ORDER BY created_at DESC LIMIT $${params.length + 1}`;
    params.push(limit);
    
    const { rows: jobs } = await client.query(query, params);
    
    return {
      success: true,
      jobs: jobs.map(job => ({
        id: job.id,
        jobName: job.job_name,
        startDate: job.start_date,
        endDate: job.end_date,
        symbols: job.symbols,
        status: job.status,
        totalDays: job.total_days,
        completedDays: job.completed_days,
        failedDays: job.failed_days,
        totalBaselines: job.total_baselines,
        createdAt: job.created_at,
        startedAt: job.started_at,
        completedAt: job.completed_at
      }))
    };
    
  } catch (e) {
    console.error("listBackfillJobs error:", e);
    throw new HttpsError("internal", "Failed to list backfill jobs.");
  } finally {
    client.release();
  }
});

exports.cancelBackfillJob = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { jobId } = request.data || {};
  
  if (!jobId) throw new HttpsError("invalid-argument", "jobId is required.");
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  
  try {
    const { rows: [job] } = await client.query(
      `UPDATE backfill_jobs 
       SET status = 'cancelled', completed_at = NOW(), updated_at = NOW()
       WHERE id = $1 AND user_id = $2 AND status = 'running'
       RETURNING id`,
      [jobId, uid]
    );
    
    if (!job) {
      throw new HttpsError("not-found", "Job not found or not running.");
    }
    
    return {
      success: true,
      message: "Backfill job cancelled"
    };
    
  } catch (e) {
    console.error("cancelBackfillJob error:", e);
    throw new HttpsError("internal", "Failed to cancel backfill job.");
  } finally {
    client.release();
  }
});

/**
 * Get real-time dashboard data for a wallet
 * Returns current prices, baselines, positions, and trading signals
 */
exports.getDashboardData = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError('unauthenticated', 'User must be authenticated');
  const userId = request.auth.uid;

  const { walletId } = request.data;
  if (!walletId) throw new HttpsError('invalid-argument', 'walletId is required');

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    console.log(`[getDashboardData] Starting for wallet ${walletId}`);

    // 1. Get wallet info and verify ownership
    const walletResult = await client.query(
      'SELECT * FROM wallets WHERE wallet_id = $1 AND user_id = $2',
      [walletId, userId]
    );
    if (walletResult.rows.length === 0) {
      throw new HttpsError('not-found', 'Wallet not found');
    }
    const wallet = walletResult.rows[0];

    // 2. Get API keys
    const { env, alpacaKey, alpacaSecret } = await getWalletEnvAndKeys(client, walletId, userId);
    const polygonKey = await getPolygonKey(client, walletId, userId);
    if (!polygonKey) throw new HttpsError('failed-precondition', 'Polygon API key not configured');
    if (!alpacaKey || !alpacaSecret) throw new HttpsError('failed-precondition', 'Alpaca API keys not configured');

    // 3. Get current BTC price
    const btcPrice = await getBTCPrice(client, userId, walletId, polygonKey);
    console.log(`[getDashboardData] BTC Price: $${btcPrice}`);

    // 4. Get wallet snapshot (equity, cash, positions) from Alpaca
    const alpacaBaseUrl = env === 'paper' ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
    const accountResponse = await fetch(`${alpacaBaseUrl}/v2/account`, {
      method: 'GET',
      headers: {
        'APCA-API-KEY-ID': alpacaKey,
        'APCA-API-SECRET-KEY': alpacaSecret
      }
    });
    const accountData = await accountResponse.json();
    const snapshot = {
      equity: parseFloat(accountData.equity),
      cash: parseFloat(accountData.cash),
      positionsValue: parseFloat(accountData.long_market_value || 0)
    };
    console.log(`[getDashboardData] Snapshot - Equity: $${snapshot.equity}, Cash: $${snapshot.cash}`);

    // 5. Get wallet symbols with their settings
    const symbolsResult = await client.query(`
      SELECT *
      FROM wallet_symbols
      WHERE wallet_id = $1
      ORDER BY symbol
    `, [walletId]);

    if (symbolsResult.rows.length === 0) {
      return {
        success: true,
        btcPrice,
        snapshot,
        signals: [],
        message: 'No symbols configured for this wallet'
      };
    }

    // 6. Determine current session (RTH or AH)
    const now = new Date();
    const etTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const hour = etTime.getHours();
    const minute = etTime.getMinutes();
    const currentMinute = hour * 60 + minute;
    
    // RTH: 9:30 AM - 4:00 PM (570-960 minutes) - Regular Trading Hours
       // AH: Everything else - After Hours (stocks trade 24/7 globally)
       const isRTH = currentMinute >= 570 && currentMinute < 960;
       const currentSession = isRTH ? 'RTH' : 'AH';
    
    console.log(`[getDashboardData] Current session: ${currentSession} (ET time: ${etTime.toLocaleTimeString()})`);

    // 7. Get yesterday's baselines for all symbols
    const symbolNames = symbolsResult.rows.map(r => r.symbol);
    const baselinesResult = await client.query(`
      SELECT 
        symbol,
        session,
        method,
        baseline,
        trading_day
      FROM baseline_daily
      WHERE symbol = ANY($1)
        AND trading_day = (
          SELECT MAX(trading_day) 
          FROM baseline_daily 
          WHERE symbol = ANY($1)
        )
      ORDER BY symbol, session, method
    `, [symbolNames]);

    // Organize baselines by symbol
    const baselinesBySymbol = {};
    baselinesResult.rows.forEach(row => {
      if (!baselinesBySymbol[row.symbol]) {
        baselinesBySymbol[row.symbol] = { RTH: {}, AH: {}, trading_day: row.trading_day };
      }
      baselinesBySymbol[row.symbol][row.session][row.method] = row.baseline;
    });

    // 8. Get current stock prices from Polygon
    const stockPrices = {};
    for (const symbol of symbolNames) {
      try {
        const priceData = await getPolygonPrice(polygonKey, symbol);
        stockPrices[symbol] = priceData;
        console.log(`[getDashboardData] ${symbol} Price: $${priceData.price}`);
      } catch (error) {
        console.error(`[getDashboardData] Error fetching price for ${symbol}:`, error.message);
        stockPrices[symbol] = { price: null, error: error.message };
      }
    }

    // 9. Get current positions from Alpaca
    const positionsBySymbol = await getAlpacaPositions(client, userId, walletId, alpacaKey, alpacaSecret, env);

    // 10. Generate trading signals for each symbol
    const signals = [];
    for (const symbolRow of symbolsResult.rows) {
      const symbol = symbolRow.symbol;
      const stockPrice = stockPrices[symbol]?.price;
      
      if (!stockPrice) {
        signals.push({
          symbol,
          error: 'Price unavailable',
          signal: 'HOLD'
        });
        continue;
      }

      // Get the appropriate baseline based on current session
      const baselines = baselinesBySymbol[symbol];
      if (!baselines) {
        signals.push({
          symbol,
          stockPrice,
          error: 'No baseline data',
          signal: 'HOLD'
        });
        continue;
      }

      // Select method based on current session
      // RTH: Use method_rth, AH: Use method_ah
      const method = currentSession === 'RTH' ? symbolRow.method_rth : symbolRow.method_ah;
      const sessionBaselines = baselines[currentSession];
      const baseline = sessionBaselines?.[method];

      if (!baseline) {
        signals.push({
          symbol,
          stockPrice,
          session: currentSession,
          method,
          error: 'Baseline not available for this session/method',
          signal: 'HOLD'
        });
        continue;
      }

      // Calculate current ratio
      const currentRatio = btcPrice / stockPrice;

      // Get multipliers (stored as percentages, convert to multipliers)
      const buyPctRTH = symbolRow.buy_pct_rth || 1.0;
      const sellPctRTH = symbolRow.sell_pct_rth || 1.0;
      const buyPctAH = symbolRow.buy_pct_ah || 1.0;
      const sellPctAH = symbolRow.sell_pct_ah || 1.0;

      const buyPct = currentSession === 'RTH' ? buyPctRTH : buyPctAH;
      const sellPct = currentSession === 'RTH' ? sellPctRTH : sellPctAH;

      const buyMultiplier = 1 + (buyPct / 100);
      const sellMultiplier = 1 - (sellPct / 100);

      // Calculate execution prices
      const buyPrice = btcPrice / (baseline * buyMultiplier);
      const sellPrice = btcPrice / (baseline * sellMultiplier);

      // Get position info
      const position = positionsBySymbol[symbol];
      const sharesOwned = position ? parseFloat(position.qty) : 0;
      const costBasis = position ? parseFloat(position.cost_basis) : 0;

      // Get budget - handle both fixed and percentage modes
      let budget = 0;
      const budgetMode = symbolRow.budget_mode || 'fixed';
      
      if (budgetMode === 'percent') {
        // Calculate budget as percentage of wallet equity
        const percentBudget = parseFloat(symbolRow.percent_budget) || 0;
        budget = (snapshot.equity * percentBudget) / 100;
        console.log(`[getDashboardData] ${symbol} using ${percentBudget}% of equity ($${snapshot.equity}) = $${budget.toFixed(2)}`);
      } else {
        // Use fixed budget
        budget = parseFloat(symbolRow.buy_budget_usd) || 0;
        console.log(`[getDashboardData] ${symbol} using fixed budget: $${budget.toFixed(2)}`);
      }
      
      const availableBudget = Math.max(0, budget - costBasis);

      // Calculate shares to buy
      const sharesToBuy = availableBudget > 0 ? Math.floor(availableBudget / buyPrice) : 0;

       // Determine signal - Simple logic based on price only
       let signal = 'HOLD';
       if (stockPrice <= buyPrice) {
         signal = 'BUY';
       } else if (stockPrice >= sellPrice) {
         signal = 'SELL';
       } else {
         signal = 'HOLD';
       }

      signals.push({
        symbol,
        baseline,
        baselineDate: baselines.trading_day,
        session: currentSession,
        method,
        stockPrice,
        currentRatio: currentRatio.toFixed(2),
        buyPrice: buyPrice.toFixed(2),
        sellPrice: sellPrice.toFixed(2),
        sharesOwned,
        costBasis: costBasis.toFixed(2),
        budget: budget.toFixed(2),
        availableBudget: availableBudget.toFixed(2),
        sharesToBuy,
        signal,
        buyMultiplier: buyMultiplier.toFixed(4),
        sellMultiplier: sellMultiplier.toFixed(4)
      });
    }

    console.log(`[getDashboardData] Generated ${signals.length} signals`);

    return {
      success: true,
      btcPrice,
      snapshot,
      signals,
      session: currentSession,
      timestamp: new Date().toISOString()
    };

  } catch (error) {
    console.error('[getDashboardData] Error:', error);
    throw new HttpsError('internal', error.message);
  } finally {
    client.release();
  }
});

/**
 * Helper: Get current price from Polygon
 */
async function getPolygonPrice(apiKey, symbol) {
  const url = `https://api.polygon.io/v2/last/trade/${symbol}?apiKey=${apiKey}`;
  const response = await fetch(url);
  
  if (!response.ok) {
    throw new Error(`Polygon API error: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json();
  
  if (data.status !== 'OK') {
    throw new Error(`Polygon API error: ${data.status}`);
  }

  return {
    price: data.results.p,
    timestamp: data.results.t
  };
}

/**
 * Get recent execution activity for dashboard
 */
exports.getRecentActivity = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError('unauthenticated', 'User must be authenticated');
  const userId = request.auth.uid;

  const { walletId, limit = 20 } = request.data;
  if (!walletId) throw new HttpsError('invalid-argument', 'walletId is required');

  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    // Get recent orders (execution tables may not exist yet)
    const ordersResult = await client.query(`
      SELECT *
      FROM execution_orders
      WHERE wallet_id = $1
      ORDER BY created_at DESC
      LIMIT $2
    `, [walletId, limit]).catch(() => ({ rows: [] }));

    return {
      success: true,
      activity: ordersResult.rows
    };

  } catch (error) {
    console.error('[getRecentActivity] Error:', error);
    throw new HttpsError('internal', error.message);
  } finally {
    client.release();
  }
});

// ============================================================================
// SYSTEM SETTINGS MANAGEMENT
// ============================================================================

/**
 * Get system settings
 */
exports.getSystemSettings = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError('unauthenticated', 'User must be authenticated');
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    const result = await client.query('SELECT * FROM system_settings');
    const settings = {};
    result.rows.forEach(row => {
      settings[row.setting_key] = row.setting_value;
    });
    return { success: true, settings };
  } catch (error) {
    console.error('[getSystemSettings] Error:', error);
    throw new HttpsError('internal', error.message);
  } finally {
    client.release();
  }
});

/**
 * Update system settings
 */
exports.updateSystemSettings = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError('unauthenticated', 'User must be authenticated');
  const userId = request.auth.uid;
  const { settingKey, settingValue } = request.data;
  
  if (!settingKey) throw new HttpsError('invalid-argument', 'settingKey is required');
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
      // Use UPSERT to insert if not exists, update if exists
      await client.query(
        `INSERT INTO system_settings (setting_key, setting_value, updated_at, updated_by)
         VALUES ($1, $2, NOW(), $3)
         ON CONFLICT (setting_key) 
         DO UPDATE SET setting_value = $2, updated_at = NOW(), updated_by = $3`,
        [settingKey, settingValue, userId]
      );
    
    console.log(`[updateSystemSettings] Updated ${settingKey} = ${settingValue} by ${userId}`);
    
    return { success: true };
  } catch (error) {
    console.error('[updateSystemSettings] Error:', error);
    throw new HttpsError('internal', error.message);
  } finally {
    client.release();
  }
});

/**
 * Update wallet enabled status
 */
exports.updateWalletEnabled = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError('unauthenticated', 'User must be authenticated');
  const userId = request.auth.uid;
  const { walletId, enabled } = request.data;
  
  if (!walletId) throw new HttpsError('invalid-argument', 'walletId is required');
  if (typeof enabled !== 'boolean') throw new HttpsError('invalid-argument', 'enabled must be boolean');
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    // Verify wallet belongs to user
    const walletCheck = await client.query(
      'SELECT wallet_id FROM wallets WHERE wallet_id = $1 AND user_id = $2',
      [walletId, userId]
    );
    
    if (walletCheck.rows.length === 0) {
      throw new HttpsError('permission-denied', 'Wallet not found or access denied');
    }
    
    await client.query(
      `UPDATE wallets SET enabled = $1, updated_at = NOW() WHERE wallet_id = $2`,
      [enabled, walletId]
    );
    
    console.log(`[updateWalletEnabled] Wallet ${walletId} enabled = ${enabled} by ${userId}`);
    
    return { success: true };
  } catch (error) {
    console.error('[updateWalletEnabled] Error:', error);
    throw new HttpsError('internal', error.message);
  } finally {
    client.release();
  }
});

/**
 * Get wallet execution status
 */
exports.getWalletStatus = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError('unauthenticated', 'User must be authenticated');
  const userId = request.auth.uid;
  const { walletId } = request.data;
  
  if (!walletId) throw new HttpsError('invalid-argument', 'walletId is required');
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  try {
    // Get wallet settings
    const settingsResult = await client.query(
      `SELECT enabled FROM wallets WHERE wallet_id = $1`,
      [walletId]
    );
    
    if (settingsResult.rows.length === 0) {
      return { success: true, enabled: false, lastExecution: null, orderCount: 0 };
    }
    
    // Get last execution time
    const lastExecResult = await client.query(
      `SELECT created_at FROM execution_snapshots WHERE wallet_id = $1 ORDER BY created_at DESC LIMIT 1`,
      [walletId]
    );
    
    // Get order count
    const orderCountResult = await client.query(
      `SELECT COUNT(*) as count FROM execution_orders WHERE wallet_id = $1`,
      [walletId]
    );
    
    return {
      success: true,
      enabled: settingsResult.rows[0].enabled,
      lastExecution: lastExecResult.rows.length > 0 ? lastExecResult.rows[0].created_at : null,
      orderCount: parseInt(orderCountResult.rows[0].count)
    };
  } catch (error) {
    console.error('[getWalletStatus] Error:', error);
    throw new HttpsError('internal', error.message);
  } finally {
    client.release();
  }
});

// ============================================================================
// SCHEDULED EXECUTION - Runs every minute
// ============================================================================

/**
 * Scheduled function that executes all enabled wallets every minute
 * This is triggered by Cloud Scheduler
 */
exports.tradiacEngine = onSchedule("every 1 minutes", async (event) => {
  console.log('tradiacEngine: Starting 1-minute run');
  
  let pool;
  let client;
  
  try {
    console.log('tradiacEngine: Getting database pool...');
    pool = await getPool();
    console.log('tradiacEngine: Pool obtained, connecting client...');
    
    client = await pool.connect();
    console.log('tradiacEngine: Client connected, checking system settings...');
    
    // Check global system settings
    const systemSettingsResult = await client.query(`
      SELECT setting_key, setting_value
      FROM system_settings
      WHERE setting_key IN ('system_enabled', 'global_trading_start_et', 'global_trading_end_et')
    `);
    
    const systemSettings = {};
    systemSettingsResult.rows.forEach(row => {
      systemSettings[row.setting_key] = row.setting_value;
    });
    
    // Check if system is enabled
    if (systemSettings.system_enabled === 'false') {
      console.log('System is DISABLED. Skipping all wallets.');
      return;
    }
    
    console.log('System is ENABLED');
    
    // Check if within global trading hours
    const now = new Date();
    const etTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const currentHour = etTime.getHours();
    const currentMinute = etTime.getMinutes();
    const currentTimeMinutes = currentHour * 60 + currentMinute;
    
    console.log(`Current ET Time: ${etTime.toLocaleTimeString('en-US', { timeZone: 'America/New_York' })}`);
    
    // Parse global trading hours
    if (systemSettings.global_trading_start_et && systemSettings.global_trading_end_et) {
      const [startHour, startMin] = systemSettings.global_trading_start_et.split(':').map(Number);
      const [endHour, endMin] = systemSettings.global_trading_end_et.split(':').map(Number);
      
      const startTimeMinutes = startHour * 60 + startMin;
      const endTimeMinutes = endHour * 60 + endMin;
      
      console.log(`Global Trading Hours: ${systemSettings.global_trading_start_et} - ${systemSettings.global_trading_end_et} ET`);
      
      if (currentTimeMinutes < startTimeMinutes || currentTimeMinutes >= endTimeMinutes) {
        console.log(`Outside global trading hours. Skipping all wallets.`);
        return;
      }
      
      console.log('Within global trading hours');
    }
    
    console.log('tradiacEngine: Querying enabled wallets...');
    
    // Get all enabled wallets - CHECK WALLETS TABLE, NOT WALLET_SETTINGS
    const walletsResult = await client.query(`
      SELECT w.wallet_id, w.user_id, w.name as wallet_name
      FROM wallets w
      WHERE w.enabled = true
    `);
    
    console.log(`tradiacEngine: Found ${walletsResult.rows.length} enabled wallets`);
    
    if (walletsResult.rows.length === 0) {
      console.log('tradiacEngine: No enabled wallets found. Exiting.');
      return;
    }
    
    // Execute each enabled wallet
    for (const wallet of walletsResult.rows) {
      console.log(`\n========================================`);
      console.log(`Executing wallet: ${wallet.wallet_name} (${wallet.wallet_id})`);
      console.log(`========================================`);
      
      try {
        await executeWallet(wallet.user_id, wallet.wallet_id);
        console.log(`✓ Wallet ${wallet.wallet_name} executed successfully`);
      } catch (error) {
        console.error(`✗ Error executing wallet ${wallet.wallet_name}:`, error.message);
        console.error('Error stack:', error.stack);
        
        // Log error but continue with other wallets
        try {
          await client.query(
            `INSERT INTO execution_errors (user_id, wallet_id, error_type, error_message, error_stack, function_name, severity)
             VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [wallet.user_id, wallet.wallet_id, 'SCHEDULED_EXECUTION_ERROR', error.message, error.stack, 'tradiacEngine', 'ERROR']
          );
        } catch (logError) {
          console.error('Failed to log execution error:', logError);
        }
      }
    }
    
    console.log('\ntradiacEngine: Run completed successfully');
    
  } catch (error) {
    console.error('CRITICAL ERROR in tradiacEngine:', error.message);
    console.error('Error stack:', error.stack);
    
    // Log critical error
    if (client) {
      try {
        await client.query(
          `INSERT INTO execution_errors (user_id, wallet_id, error_type, error_message, error_stack, function_name, severity)
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [null, null, 'SCHEDULER_CRITICAL_ERROR', error.message, error.stack, 'tradiacEngine', 'CRITICAL']
        );
      } catch (logError) {
        console.error('Failed to log critical error:', logError);
      }
    }
  } finally {
    if (client) {
      console.log('tradiacEngine: Releasing database client');
      client.release();
    }
  }
});

// ============================================================================
// NIGHTLY DATA COLLECTION JOB
// Runs at 1 AM EST to fetch previous trading day's minute bar data
// ============================================================================

exports.collectDailyMarketData = onSchedule({
  schedule: "0 6 * * *",  // 1 AM EST = 6 AM UTC (during EST)
  timeZone: "America/New_York",
  timeoutSeconds: 540
}, async (event) => {
  console.log('[DataCollection] Starting nightly data collection');
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  
  try {
    // Get previous trading day
    const result = await client.query(`
      SELECT prev_open_date 
      FROM trading_calendar 
      WHERE cal_date = CURRENT_DATE
      LIMIT 1
    `);
    
    if (result.rows.length === 0) {
      throw new Error('No trading calendar entry for today');
    }
    
    const tradingDay = result.rows[0].prev_open_date;
    console.log(`[DataCollection] Collecting data for trading day: ${tradingDay}`);
    
    // Get Polygon API key
    const keyResult = await client.query(`SELECT polygon_key FROM user_api_keys LIMIT 1`);
    
    if (keyResult.rows.length === 0 || !keyResult.rows[0].polygon_key) {
      throw new Error('No Polygon API key found');
    }
    
    const polygonKey = keyResult.rows[0].polygon_key;
    
    // Helper: Fetch from Polygon
    async function fetchPolygonBars(symbol, date) {
      const dateStr = date.toISOString().split('T')[0];
      const url = `https://api.polygon.io/v2/aggs/ticker/${symbol}/range/1/minute/${dateStr}/${dateStr}?adjusted=true&sort=asc&limit=50000&apiKey=${polygonKey}`;
      
      console.log(`[DataCollection] Fetching ${symbol} for ${dateStr}`);
      
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error(`Polygon API error for ${symbol}: ${response.status}`);
      }
      
      const data = await response.json();
      
      if (!data.results || data.results.length === 0) {
        console.warn(`[DataCollection] No data for ${symbol} on ${dateStr}`);
        return [];
      }
      
      console.log(`[DataCollection] Fetched ${data.results.length} bars for ${symbol}`);
      return data.results;
    }
    
    // Helper: Determine session
function determineSession(timestamp) {
        const date = new Date(timestamp);
        const formatter = new Intl.DateTimeFormat('en-US', {
          timeZone: 'America/New_York',
          hour: '2-digit',
          minute: '2-digit',
          hour12: false
        });
        
        const parts = formatter.formatToParts(date);
        const hour = parseInt(parts.find(p => p.type === 'hour').value);
        const minute = parseInt(parts.find(p => p.type === 'minute').value);
        const timeInMinutes = hour * 60 + minute;
        
        // RTH: 9:30 AM - 4:00 PM EST
        if (timeInMinutes >= 570 && timeInMinutes < 960) {
          return 'RTH';
        } else {
          // Everything else is AH (includes pre-market, after-hours, overnight)
          return 'AH';
        }
      }    
    // Fetch and store BTC data
    console.log('[DataCollection] Fetching BTC data...');
    const btcBars = await fetchPolygonBars('X:BTCUSD', new Date(tradingDay));
    
    let btcCount = 0;
    for (const bar of btcBars) {
      const session = determineSession(bar.t);
      const barTime = new Date(bar.t);
      
      const etFormatter = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
      });
      let etTimeStr = etFormatter.format(barTime);
         
         // Fix: PostgreSQL doesn't accept 24:xx:xx format, convert to 00:xx:xx
         if (etTimeStr.startsWith('24:')) {
           etTimeStr = '00' + etTimeStr.substring(2);
         }
      
      try {
        await client.query(`
          INSERT INTO minute_btc (et_date, et_time, ts_utc, o, h, l, c, v, vw, session, source)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'polygon')
          ON CONFLICT (et_date, et_time) DO UPDATE SET
            o = EXCLUDED.o, h = EXCLUDED.h, l = EXCLUDED.l, c = EXCLUDED.c,
            v = EXCLUDED.v, vw = EXCLUDED.vw, session = EXCLUDED.session
        `, [tradingDay, etTimeStr, barTime, bar.o, bar.h, bar.l, bar.c, bar.v, bar.vw || null, session]);
        btcCount++;
      } catch (error) {
        console.error(`[DataCollection] Error inserting BTC bar:`, error.message);
      }
    }
    
    console.log(`[DataCollection] Stored ${btcCount} BTC bars`);
    
    // Fetch and store stock data
    const symbols = ['BTDR', 'CAN', 'CIFR', 'CLSK', 'CORZ', 'HIVE', 'HUT', 'MARA', 'RIOT'];
    const stockCounts = {};
    
    for (const symbol of symbols) {
      console.log(`[DataCollection] Fetching ${symbol} data...`);
      
      try {
        const stockBars = await fetchPolygonBars(symbol, new Date(tradingDay));
        let count = 0;
        
        for (const bar of stockBars) {
          const session = determineSession(bar.t);
          const barTime = new Date(bar.t);
          
          const etFormatter = new Intl.DateTimeFormat('en-US', {
            timeZone: 'America/New_York',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
          });
          let etTimeStr = etFormatter.format(barTime);
             
             // Fix: PostgreSQL doesn't accept 24:xx:xx format, convert to 00:xx:xx
             if (etTimeStr.startsWith('24:')) {
               etTimeStr = '00' + etTimeStr.substring(2);
             }
          
          try {
            await client.query(`
              INSERT INTO minute_stock (symbol, et_date, et_time, ts_utc, o, h, l, c, v, vw, session, source)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'polygon')
              ON CONFLICT (symbol, et_date, et_time) DO UPDATE SET
                o = EXCLUDED.o, h = EXCLUDED.h, l = EXCLUDED.l, c = EXCLUDED.c,
                v = EXCLUDED.v, vw = EXCLUDED.vw, session = EXCLUDED.session
            `, [symbol, tradingDay, etTimeStr, barTime, bar.o, bar.h, bar.l, bar.c, bar.v, bar.vw || null, session]);
            count++;
          } catch (error) {
            console.error(`[DataCollection] Error inserting ${symbol} bar:`, error.message);
          }
        }
        
        stockCounts[symbol] = count;
        console.log(`[DataCollection] Stored ${count} ${symbol} bars`);
        
        // Rate limiting
        await new Promise(resolve => setTimeout(resolve, 10));
      } catch (error) {
        console.error(`[DataCollection] Error fetching ${symbol}:`, error.message);
        stockCounts[symbol] = 0;
      }
    }
    
    console.log('[DataCollection] Data collection complete');
    console.log(`  Trading Day: ${tradingDay}`);
    console.log(`  BTC bars: ${btcCount}`);
    for (const [symbol, count] of Object.entries(stockCounts)) {
      console.log(`  ${symbol} bars: ${count}`);
    }
    
    return { success: true, tradingDay, btcBars: btcCount, stockBars: stockCounts };
    
  } catch (error) {
    console.error('[DataCollection] Error:', error);
    throw error;
  } finally {
    client.release();
  }
});

// ============================================================================
// NIGHTLY BASELINE CALCULATION JOB
// Runs at 1:15 AM EST to calculate baselines from stored data
// ============================================================================

exports.calculateDailyBaselines = onSchedule({
  schedule: "15 6 * * *",  // 1:15 AM EST = 6:15 AM UTC (during EST)
  timeZone: "America/New_York",
  timeoutSeconds: 540
}, async (event) => {
  console.log('[BaselineCalc] Starting nightly baseline calculation');
  
  const dbPool = await getPool();
  const client = await dbPool.connect();
  
  try {
    // Get previous trading day
    const result = await client.query(`
      SELECT prev_open_date 
      FROM trading_calendar 
      WHERE cal_date = CURRENT_DATE
      LIMIT 1
    `);
    
    if (result.rows.length === 0) {
      throw new Error('No trading calendar entry for today');
    }
    
    const tradingDay = result.rows[0].prev_open_date;
    console.log(`[BaselineCalc] Calculating baselines for trading day: ${tradingDay}`);
    
    // Helper functions for baseline calculation
    function alignBars(btcBars, stockBars) {
      const aligned = [];
      const stockMap = new Map();
      
      for (const bar of stockBars) {
        stockMap.set(bar.timestamp, bar);
      }
      
      for (const btcBar of btcBars) {
        const stockBar = stockMap.get(btcBar.timestamp);
        if (stockBar && stockBar.close > 0 && btcBar.close > 0) {
          aligned.push({
            timestamp: btcBar.timestamp,
            btc_close: btcBar.close,
            btc_volume: btcBar.volume,
            stock_close: stockBar.close,
            stock_volume: stockBar.volume,
            ratio: btcBar.close / stockBar.close
          });
        }
      }
      
      return aligned;
    }
    
    function computeEqualMean(aligned) {
      if (aligned.length === 0) return null;
      const sum = aligned.reduce((acc, bar) => acc + bar.ratio, 0);
      return sum / aligned.length;
    }
    
    function computeMedian(aligned) {
      if (aligned.length === 0) return null;
      const sorted = aligned.map(b => b.ratio).sort((a, b) => a - b);
      const mid = Math.floor(sorted.length / 2);
      return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
    }
    
    function computeVWAPRatio(aligned) {
      if (aligned.length === 0) return null;
      let btcVWAPNum = 0, btcVWAPDen = 0, stockVWAPNum = 0, stockVWAPDen = 0;
      
      for (const bar of aligned) {
        btcVWAPNum += bar.btc_close * bar.btc_volume;
        btcVWAPDen += bar.btc_volume;
        stockVWAPNum += bar.stock_close * bar.stock_volume;
        stockVWAPDen += bar.stock_volume;
      }
      
      if (btcVWAPDen === 0 || stockVWAPDen === 0) return null;
      const btcVWAP = btcVWAPNum / btcVWAPDen;
      const stockVWAP = stockVWAPNum / stockVWAPDen;
      return stockVWAP > 0 ? btcVWAP / stockVWAP : null;
    }
    
    function computeVolWeighted(aligned) {
      if (aligned.length === 0) return null;
      let numerator = 0, denominator = 0;
      
      for (const bar of aligned) {
        const weight = bar.btc_volume + bar.stock_volume;
        numerator += bar.ratio * weight;
        denominator += weight;
      }
      
      return denominator > 0 ? numerator / denominator : null;
    }
    
    function computeWinsorized(aligned) {
      if (aligned.length === 0) return null;
      const ratios = aligned.map(b => b.ratio).sort((a, b) => a - b);
      const p5 = Math.floor(ratios.length * 0.05);
      const p95 = Math.floor(ratios.length * 0.95);
      const winsorized = ratios.slice(p5, p95 + 1);
      if (winsorized.length === 0) return null;
      const sum = winsorized.reduce((acc, val) => acc + val, 0);
      return sum / winsorized.length;
    }
    
    const symbols = ['BTDR', 'CAN', 'CIFR', 'CLSK', 'CORZ', 'HIVE', 'HUT', 'MARA', 'RIOT'];
    const sessions = ['RTH', 'AH'];
    let totalBaselines = 0;
    const results = {};
    
    for (const symbol of symbols) {
      results[symbol] = { RTH: 0, AH: 0 };
      
      for (const session of sessions) {
        try {
          console.log(`[BaselineCalc] Calculating ${symbol} ${session}`);
          
          // Fetch BTC bars
          const btcResult = await client.query(`
            SELECT ts_utc, c as close, v as volume
            FROM minute_btc
            WHERE et_date = $1 AND session = $2
            ORDER BY ts_utc
          `, [tradingDay, session]);
          
          const btcBars = btcResult.rows.map(row => ({
            timestamp: new Date(row.ts_utc).getTime(),
            close: parseFloat(row.close),
            volume: parseInt(row.volume) || 0
          }));
          
          // Fetch stock bars
          const stockResult = await client.query(`
            SELECT ts_utc, c as close, v as volume
            FROM minute_stock
            WHERE symbol = $1 AND et_date = $2 AND session = $3
            ORDER BY ts_utc
          `, [symbol, tradingDay, session]);
          
          const stockBars = stockResult.rows.map(row => ({
            timestamp: new Date(row.ts_utc).getTime(),
            close: parseFloat(row.close),
            volume: parseInt(row.volume) || 0
          }));
          
          console.log(`[BaselineCalc] ${symbol} ${session}: ${btcBars.length} BTC, ${stockBars.length} stock bars`);
          
          if (btcBars.length === 0 || stockBars.length === 0) {
            console.warn(`[BaselineCalc] No data for ${symbol} ${session}`);
            continue;
          }
          
          // Align bars
          const aligned = alignBars(btcBars, stockBars);
          console.log(`[BaselineCalc] ${symbol} ${session}: ${aligned.length} aligned bars`);
          
          if (aligned.length === 0) {
            console.warn(`[BaselineCalc] No aligned data for ${symbol} ${session}`);
            continue;
          }
          
          // Calculate all 5 methods
          const methods = {
            'EQUAL_MEAN': computeEqualMean(aligned),
            'MEDIAN': computeMedian(aligned),
            'VWAP_RATIO': computeVWAPRatio(aligned),
            'VOL_WEIGHTED': computeVolWeighted(aligned),
            'WINSORIZED': computeWinsorized(aligned)
          };
          
          // Store baselines
          for (const [method, baseline] of Object.entries(methods)) {
            if (baseline !== null && baseline > 0) {
              await client.query(`
                INSERT INTO baseline_daily (trading_day, symbol, session, method, baseline, sample_count, source, computed_at)
                VALUES ($1, $2, $3, $4, $5, $6, 'database', NOW())
                ON CONFLICT (trading_day, symbol, session, method)
                DO UPDATE SET baseline = EXCLUDED.baseline, sample_count = EXCLUDED.sample_count, computed_at = NOW()
              `, [tradingDay, symbol, session, method, baseline, aligned.length]);
              
              console.log(`[BaselineCalc]   ${method}: ${baseline.toFixed(4)}`);
              results[symbol][session]++;
              totalBaselines++;
            }
          }
        } catch (error) {
          console.error(`[BaselineCalc] Error calculating ${symbol} ${session}:`, error.message);
        }
      }
    }
    
    console.log('[BaselineCalc] Baseline calculation complete');
    console.log(`  Trading Day: ${tradingDay}`);
    console.log(`  Total baselines: ${totalBaselines}`);
    for (const [symbol, counts] of Object.entries(results)) {
      console.log(`  ${symbol}: RTH=${counts.RTH}, AH=${counts.AH}`);
    }
    
    return { success: true, tradingDay, totalBaselines, results };
    
  } catch (error) {
    console.error('[BaselineCalc] Error:', error);
    throw error;
  } finally {
    client.release();
  }
});


// ============================================================================
// KPI DASHBOARD BACKEND FUNCTIONS
// Add these to your index.js file
// ============================================================================

// Get Account Info (Portfolio Summary)
exports.getAccountInfo = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    // Get wallet info and API keys
    const { rows: walletRows } = await client.query(
      `SELECT w.env, wak.alpaca_paper_key, wak.alpaca_paper_secret, wak.alpaca_live_key, wak.alpaca_live_secret
       FROM wallets w
       LEFT JOIN wallet_api_keys wak ON w.wallet_id = wak.wallet_id
       WHERE w.wallet_id = $1`,
      [walletId]
    );

    if (!walletRows.length) throw new HttpsError("not-found", "Wallet not found.");

    const wallet = walletRows[0];
    const env = wallet.env;
    const alpacaKey = env === 'paper' ? wallet.alpaca_paper_key : wallet.alpaca_live_key;
    const alpacaSecret = env === 'paper' ? wallet.alpaca_paper_secret : wallet.alpaca_live_secret;

    if (!alpacaKey || !alpacaSecret) {
      throw new HttpsError("failed-precondition", "Alpaca API keys not configured.");
    }

    // Call Alpaca API
    const baseUrl = env === 'paper' ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
    const response = await fetch(`${baseUrl}/v2/account`, {
      headers: {
        'APCA-API-KEY-ID': alpacaKey,
        'APCA-API-SECRET-KEY': alpacaSecret
      }
    });

    if (!response.ok) {
      throw new HttpsError("internal", `Alpaca API error: ${response.statusText}`);
    }

    const account = await response.json();

    return {
      success: true,
      account: {
        equity: parseFloat(account.equity),
        cash: parseFloat(account.cash),
        buying_power: parseFloat(account.buying_power),
        portfolio_value: parseFloat(account.portfolio_value),
        last_equity: parseFloat(account.last_equity),
        long_market_value: parseFloat(account.long_market_value),
        short_market_value: parseFloat(account.short_market_value)
      }
    };

  } catch (e) {
    console.error("getAccountInfo error:", e);
    throw new HttpsError("internal", e.message || "Failed to get account info.");
  } finally {
    client.release();
  }
});

// Get Current Positions
exports.getPositions = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    // Get wallet info and API keys
    const { rows: walletRows } = await client.query(
      `SELECT w.env, wak.alpaca_paper_key, wak.alpaca_paper_secret, wak.alpaca_live_key, wak.alpaca_live_secret
       FROM wallets w
       LEFT JOIN wallet_api_keys wak ON w.wallet_id = wak.wallet_id
       WHERE w.wallet_id = $1`,
      [walletId]
    );

    if (!walletRows.length) throw new HttpsError("not-found", "Wallet not found.");

    const wallet = walletRows[0];
    const env = wallet.env;
    const alpacaKey = env === 'paper' ? wallet.alpaca_paper_key : wallet.alpaca_live_key;
    const alpacaSecret = env === 'paper' ? wallet.alpaca_paper_secret : wallet.alpaca_live_secret;

    if (!alpacaKey || !alpacaSecret) {
      throw new HttpsError("failed-precondition", "Alpaca API keys not configured.");
    }

    // Call Alpaca API
    const baseUrl = env === 'paper' ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
    const response = await fetch(`${baseUrl}/v2/positions`, {
      headers: {
        'APCA-API-KEY-ID': alpacaKey,
        'APCA-API-SECRET-KEY': alpacaSecret
      }
    });

    if (!response.ok) {
      throw new HttpsError("internal", `Alpaca API error: ${response.statusText}`);
    }

    const positions = await response.json();

    return {
      success: true,
      positions: positions.map(p => ({
        symbol: p.symbol,
        qty: parseFloat(p.qty),
        current_price: parseFloat(p.current_price),
        market_value: parseFloat(p.market_value),
        cost_basis: parseFloat(p.cost_basis),
        unrealized_pl: parseFloat(p.unrealized_pl),
        unrealized_plpc: parseFloat(p.unrealized_plpc),
        change_today: parseFloat(p.change_today || 0)
      }))
    };

  } catch (e) {
    console.error("getPositions error:", e);
    throw new HttpsError("internal", e.message || "Failed to get positions.");
  } finally {
    client.release();
  }
});

// Get Recent Orders
exports.getRecentOrders = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, limit = 10 } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    const { rows } = await client.query(
      `SELECT 
        order_id,
        created_at,
        symbol,
        side,
        qty,
        limit_price,
        status,
        alpaca_order_id
       FROM execution_orders
       WHERE wallet_id = $1
       ORDER BY created_at DESC
       LIMIT $2`,
      [walletId, limit]
    );

    return { success: true, orders: rows };

  } catch (e) {
    console.error("getRecentOrders error:", e);
    throw new HttpsError("internal", "Failed to get recent orders.");
  } finally {
    client.release();
  }
});

// Get Performance Data
exports.getPerformanceData = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, days = 30 } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    const { rows } = await client.query(
      `SELECT 
        es.symbol,
        COUNT(*) as total_trades,
        COUNT(CASE WHEN es.decision = 'BUY' THEN 1 END) as buy_signals,
        COUNT(CASE WHEN es.decision = 'SELL' THEN 1 END) as sell_signals,
        AVG(es.stock_price) as avg_price,
        -- Calculate simple ROI based on price changes
        CASE 
          WHEN MIN(es.stock_price) > 0 THEN
            ((MAX(es.stock_price) - MIN(es.stock_price)) / MIN(es.stock_price) * 100)
          ELSE 0
        END as roi_percent,
        -- Calculate win rate (simplified)
        CASE 
          WHEN COUNT(*) > 0 THEN
            (COUNT(CASE WHEN es.decision IN ('BUY', 'BOTH') THEN 1 END)::numeric / COUNT(*)::numeric * 100)
          ELSE 0
        END as win_rate
       FROM execution_snapshots es
       WHERE es.wallet_id = $1
         AND es.created_at >= NOW() - INTERVAL '1 day' * $2
       GROUP BY es.symbol
       ORDER BY es.symbol`,
      [walletId, days]
    );

    return { success: true, performance: rows };

  } catch (e) {
    console.error("getPerformanceData error:", e);
    throw new HttpsError("internal", "Failed to get performance data.");
  } finally {
    client.release();
  }
});

// Get Trading Activity
exports.getTradingActivity = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, days = 7 } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    const { rows } = await client.query(
      `SELECT 
        EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/New_York') as hour_et,
        COUNT(*) as order_count
       FROM execution_orders
       WHERE wallet_id = $1
         AND created_at >= NOW() - INTERVAL '1 day' * $2
       GROUP BY EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/New_York')
       ORDER BY hour_et`,
      [walletId, days]
    );

    return { success: true, activity: rows };

  } catch (e) {
    console.error("getTradingActivity error:", e);
    throw new HttpsError("internal", "Failed to get trading activity.");
  } finally {
    client.release();
  }
});

// ============================================================================
// KPI DASHBOARD - FULL ALPACA API INTEGRATION
// ============================================================================
// New Cloud Functions to add comprehensive Alpaca data




// ============================================================================
// 1. GET PORTFOLIO HISTORY - Account equity & P/L over time
// ============================================================================
exports.getPortfolioHistory = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, period = '1M', timeframe = '1D' } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    // Get wallet API keys
    const { rows } = await client.query(
      'SELECT alpaca_key, alpaca_secret, env FROM wallets WHERE wallet_id = $1',
      [walletId]
    );

    if (rows.length === 0) {
      throw new HttpsError("not-found", "Wallet not found.");
    }

    const { alpaca_key, alpaca_secret, env } = rows[0];
    const isPaper = env === 'paper';

    // Initialize Alpaca client
    const alpaca = new Alpaca({
      keyId: alpaca_key,
      secretKey: alpaca_secret,
      paper: isPaper
    });

    // Get portfolio history
    // period: 1D, 5D, 1M, 3M, 6M, 1Y, all
    // timeframe: 1Min, 5Min, 15Min, 1H, 1D
    const history = await alpaca.getPortfolioHistory({
      period: period,
      timeframe: timeframe,
      extended_hours: true
    });

    // Format response
    const formatted = {
      timestamps: history.timestamp || [],
      equity: history.equity || [],
      profit_loss: history.profit_loss || [],
      profit_loss_pct: history.profit_loss_pct || [],
      base_value: history.base_value || 0,
      timeframe: history.timeframe || timeframe
    };

    return formatted;

  } catch (e) {
    console.error("getPortfolioHistory error:", e);
    throw new HttpsError("internal", `Failed to get portfolio history: ${e.message}`);
  } finally {
    client.release();
  }
});

// ============================================================================
// 2. GET ACCOUNT ACTIVITIES - Fills, dividends, fees, transfers
// ============================================================================
exports.getAccountActivities = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, activityTypes = null, pageSize = 100, pageToken = null } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    // Get wallet API keys
    const { rows } = await client.query(
      'SELECT alpaca_key, alpaca_secret, env FROM wallets WHERE wallet_id = $1',
      [walletId]
    );

    if (rows.length === 0) {
      throw new HttpsError("not-found", "Wallet not found.");
    }

    const { alpaca_key, alpaca_secret, env } = rows[0];
    const isPaper = env === 'paper';

    // Initialize Alpaca client
    const alpaca = new Alpaca({
      keyId: alpaca_key,
      secretKey: alpaca_secret,
      paper: isPaper
    });

    // Activity types: FILL, TRANS, MISC, ACATC, ACATS, CSD, CSW, DIV, DIVCGL, 
    // DIVCGS, DIVFEE, DIVFT, DIVNRA, DIVROC, DIVTXEX, INT, JNLC, JNLS, MA, 
    // NC, OPASN, OPEXP, OPXRC, PTC, PTR, REORG, SSO, SSP
    
    const params = {
      page_size: pageSize
    };

    if (pageToken) {
      params.page_token = pageToken;
    }

    if (activityTypes) {
      params.activity_types = activityTypes;
    }

    // Get activities
    const activities = await alpaca.getAccountActivities(params);

    // Format response
    const formatted = activities.map(activity => ({
      id: activity.id,
      activity_type: activity.activity_type,
      date: activity.date || activity.transaction_time,
      net_amount: parseFloat(activity.net_amount || 0),
      symbol: activity.symbol || null,
      qty: parseFloat(activity.qty || 0),
      price: parseFloat(activity.price || 0),
      side: activity.side || null,
      order_id: activity.order_id || null,
      description: activity.description || null
    }));

    return { activities: formatted };

  } catch (e) {
    console.error("getAccountActivities error:", e);
    throw new HttpsError("internal", `Failed to get account activities: ${e.message}`);
  } finally {
    client.release();
  }
});

// ============================================================================
// 3. GET CLOSED POSITIONS - Reconstructed from fills
// ============================================================================
exports.getClosedPositions = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, days = 90 } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    // Get wallet API keys
    const { rows: walletRows } = await client.query(
      'SELECT alpaca_key, alpaca_secret, env FROM wallets WHERE wallet_id = $1',
      [walletId]
    );

    if (walletRows.length === 0) {
      throw new HttpsError("not-found", "Wallet not found.");
    }

    const { alpaca_key, alpaca_secret, env } = walletRows[0];
    const isPaper = env === 'paper';

    // Initialize Alpaca client
    const alpaca = new Alpaca({
      keyId: alpaca_key,
      secretKey: alpaca_secret,
      paper: isPaper
    });

    // Get all FILL activities
    const fills = await alpaca.getAccountActivities({
      activity_types: 'FILL',
      page_size: 500
    });

    // Group fills by symbol and reconstruct positions
    const positionsBySymbol = {};

    fills.forEach(fill => {
      const symbol = fill.symbol;
      if (!positionsBySymbol[symbol]) {
        positionsBySymbol[symbol] = {
          symbol: symbol,
          buys: [],
          sells: []
        };
      }

      const fillData = {
        date: fill.transaction_time,
        qty: Math.abs(parseFloat(fill.qty)),
        price: parseFloat(fill.price),
        amount: Math.abs(parseFloat(fill.net_amount))
      };

      if (fill.side === 'buy') {
        positionsBySymbol[symbol].buys.push(fillData);
      } else if (fill.side === 'sell') {
        positionsBySymbol[symbol].sells.push(fillData);
      }
    });

    // Calculate closed positions using FIFO
    const closedPositions = [];

    Object.keys(positionsBySymbol).forEach(symbol => {
      const { buys, sells } = positionsBySymbol[symbol];
      
      // Sort by date
      buys.sort((a, b) => new Date(a.date) - new Date(b.date));
      sells.sort((a, b) => new Date(a.date) - new Date(b.date));

      let buyQueue = [...buys];
      let totalRealized = 0;
      let totalInvested = 0;
      let totalReturned = 0;

      sells.forEach(sell => {
        let remainingSellQty = sell.qty;

        while (remainingSellQty > 0 && buyQueue.length > 0) {
          const buy = buyQueue[0];
          const matchQty = Math.min(remainingSellQty, buy.qty);

          const costBasis = matchQty * buy.price;
          const proceeds = matchQty * sell.price;
          const realized = proceeds - costBasis;

          totalRealized += realized;
          totalInvested += costBasis;
          totalReturned += proceeds;

          remainingSellQty -= matchQty;
          buy.qty -= matchQty;

          if (buy.qty <= 0) {
            buyQueue.shift();
          }
        }
      });

      if (sells.length > 0) {
        const roi = totalInvested > 0 ? (totalRealized / totalInvested) * 100 : 0;
        
        closedPositions.push({
          symbol: symbol,
          total_buys: buys.length,
          total_sells: sells.length,
          total_invested: totalInvested,
          total_returned: totalReturned,
          realized_pl: totalRealized,
          roi_percent: roi,
          first_buy: buys[0]?.date || null,
          last_sell: sells[sells.length - 1]?.date || null
        });
      }
    });

    // Sort by realized P&L
    closedPositions.sort((a, b) => b.realized_pl - a.realized_pl);

    return { closedPositions };

  } catch (e) {
    console.error("getClosedPositions error:", e);
    throw new HttpsError("internal", `Failed to get closed positions: ${e.message}`);
  } finally {
    client.release();
  }
});

// ============================================================================
// 4. GET ORDER EXECUTION METRICS - Fill rates, latencies, slippage
// ============================================================================
exports.getOrderMetrics = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId, days = 30 } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    // Get order metrics from our database
    const { rows } = await client.query(
      `SELECT 
        symbol,
        COUNT(*) as total_orders,
        COUNT(CASE WHEN status = 'filled' THEN 1 END) as filled_orders,
        COUNT(CASE WHEN status = 'canceled' THEN 1 END) as canceled_orders,
        COUNT(CASE WHEN status = 'rejected' THEN 1 END) as rejected_orders,
        AVG(CASE WHEN fill_rate IS NOT NULL THEN fill_rate ELSE 0 END) as avg_fill_rate,
        AVG(CASE WHEN time_to_fill_seconds IS NOT NULL THEN time_to_fill_seconds ELSE 0 END) as avg_time_to_fill,
        AVG(CASE WHEN time_to_accept_seconds IS NOT NULL THEN time_to_accept_seconds ELSE 0 END) as avg_time_to_accept,
        AVG(CASE WHEN slippage_pct IS NOT NULL THEN slippage_pct ELSE 0 END) as avg_slippage_pct,
        MIN(CASE WHEN time_to_fill_seconds IS NOT NULL THEN time_to_fill_seconds END) as fastest_fill,
        MAX(CASE WHEN time_to_fill_seconds IS NOT NULL THEN time_to_fill_seconds END) as slowest_fill
      FROM execution_orders
      WHERE wallet_id = $1
        AND created_at >= NOW() - INTERVAL '1 day' * $2
      GROUP BY symbol
      ORDER BY total_orders DESC`,
      [walletId, days]
    );

    const metrics = rows.map(row => ({
      symbol: row.symbol,
      total_orders: parseInt(row.total_orders),
      filled_orders: parseInt(row.filled_orders),
      canceled_orders: parseInt(row.canceled_orders),
      rejected_orders: parseInt(row.rejected_orders),
      fill_rate_pct: parseFloat(row.avg_fill_rate) || 0,
      avg_time_to_fill_sec: parseFloat(row.avg_time_to_fill) || 0,
      avg_time_to_accept_sec: parseFloat(row.avg_time_to_accept) || 0,
      avg_slippage_pct: parseFloat(row.avg_slippage_pct) || 0,
      fastest_fill_sec: parseFloat(row.fastest_fill) || 0,
      slowest_fill_sec: parseFloat(row.slowest_fill) || 0
    }));

    // Calculate overall metrics
    const totals = {
      total_orders: metrics.reduce((sum, m) => sum + m.total_orders, 0),
      filled_orders: metrics.reduce((sum, m) => sum + m.filled_orders, 0),
      canceled_orders: metrics.reduce((sum, m) => sum + m.canceled_orders, 0),
      rejected_orders: metrics.reduce((sum, m) => sum + m.rejected_orders, 0),
      avg_fill_rate_pct: metrics.length > 0 ? 
        metrics.reduce((sum, m) => sum + m.fill_rate_pct, 0) / metrics.length : 0,
      avg_time_to_fill_sec: metrics.length > 0 ?
        metrics.reduce((sum, m) => sum + m.avg_time_to_fill_sec, 0) / metrics.length : 0,
      avg_slippage_pct: metrics.length > 0 ?
        metrics.reduce((sum, m) => sum + m.avg_slippage_pct, 0) / metrics.length : 0
    };

    return { metrics, totals };

  } catch (e) {
    console.error("getOrderMetrics error:", e);
    throw new HttpsError("internal", `Failed to get order metrics: ${e.message}`);
  } finally {
    client.release();
  }
});

// ============================================================================
// 5. GET ACCOUNT SNAPSHOT - Current account state with gain/loss
// ============================================================================
exports.getAccountSnapshot = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "You must be logged in.");
  const uid = request.auth.uid;
  const { walletId } = request.data || {};

  if (!walletId) throw new HttpsError("invalid-argument", "walletId is required.");

  const dbPool = await getPool();
  const client = await dbPool.connect();

  try {
    await ensureWalletSchema(client);
    await assertWalletOwnership(client, walletId, uid);

    // Get wallet API keys
    const { rows } = await client.query(
      'SELECT alpaca_key, alpaca_secret, env FROM wallets WHERE wallet_id = $1',
      [walletId]
    );

    if (rows.length === 0) {
      throw new HttpsError("not-found", "Wallet not found.");
    }

    const { alpaca_key, alpaca_secret, env } = rows[0];
    const isPaper = env === 'paper';

    // Initialize Alpaca client
    const alpaca = new Alpaca({
      keyId: alpaca_key,
      secretKey: alpaca_secret,
      paper: isPaper
    });

    // Get account info
    const account = await alpaca.getAccount();

    // Format response with all useful fields
    const snapshot = {
      // Account identifiers
      account_number: account.account_number,
      status: account.status,
      
      // Equity values
      equity: parseFloat(account.equity),
      last_equity: parseFloat(account.last_equity),
      cash: parseFloat(account.cash),
      buying_power: parseFloat(account.buying_power),
      portfolio_value: parseFloat(account.portfolio_value),
      
      // P&L calculations
      equity_change: parseFloat(account.equity) - parseFloat(account.last_equity),
      equity_change_pct: parseFloat(account.last_equity) > 0 ?
        ((parseFloat(account.equity) - parseFloat(account.last_equity)) / parseFloat(account.last_equity)) * 100 : 0,
      
      // Trading status
      trading_blocked: account.trading_blocked,
      transfers_blocked: account.transfers_blocked,
      account_blocked: account.account_blocked,
      
      // Pattern day trader
      pattern_day_trader: account.pattern_day_trader,
      daytrade_count: parseInt(account.daytrade_count) || 0,
      
      // Multipliers
      multiplier: parseFloat(account.multiplier),
      
      // Timestamps
      created_at: account.created_at,
      updated_at: new Date().toISOString()
    };

    return snapshot;

  } catch (e) {
    console.error("getAccountSnapshot error:", e);
    throw new HttpsError("internal", `Failed to get account snapshot: ${e.message}`);
  } finally {
    client.release();
  }
});