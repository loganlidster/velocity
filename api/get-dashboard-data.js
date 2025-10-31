// Vercel serverless function to get dashboard data
const { Pool } = require('pg');

module.exports = async (req, res) => {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const walletId = req.query.wallet_id || req.body?.walletId || req.body?.data?.walletId;
  
  if (!walletId) {
    return res.status(400).json({ success: false, error: 'wallet_id is required' });
  }

  let pool;
  let client;
  
  try {
    pool = new Pool({
      host: '34.168.157.63',
      database: 'tradiac_v2',
      user: 'appuser',
      password: 'Fu3lth3j3t!',
      ssl: { rejectUnauthorized: false },
      max: 5,
      connectionTimeoutMillis: 10000
    });
    
    client = await pool.connect();
    
    // Get wallet info
    const walletResult = await client.query(`
      SELECT * FROM wallets WHERE wallet_id = $1
    `, [walletId]);
    
    if (walletResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Wallet not found' });
    }
    
    const wallet = walletResult.rows[0];
    
    // Get symbols for this wallet
    const symbolsResult = await client.query(`
      SELECT * FROM wallet_symbols WHERE wallet_id = $1 AND enabled = true
    `, [walletId]);
    
    // Get API keys for this wallet
    const keysResult = await client.query(`
      SELECT * FROM wallet_api_keys WHERE wallet_id = $1
    `, [walletId]);
    
    let accountData = null;
    let btcPrice = null;
    
    // Fetch account data from Alpaca if keys exist
    if (keysResult.rows.length > 0) {
      const keys = keysResult.rows[0];
      const alpacaKey = wallet.env === 'paper' ? keys.alpaca_paper_key : keys.alpaca_live_key;
      const alpacaSecret = wallet.env === 'paper' ? keys.alpaca_paper_secret : keys.alpaca_live_secret;
      
      if (alpacaKey && alpacaSecret) {
        try {
          const alpacaBaseUrl = wallet.env === 'paper' 
            ? 'https://paper-api.alpaca.markets'
            : 'https://api.alpaca.markets';
          
          // Get account info
          const accountResponse = await fetch(`${alpacaBaseUrl}/v2/account`, {
            headers: {
              'APCA-API-KEY-ID': alpacaKey,
              'APCA-API-SECRET-KEY': alpacaSecret
            }
          });
          
          if (accountResponse.ok) {
            accountData = await accountResponse.json();
          }
        } catch (error) {
          console.error('Error fetching Alpaca data:', error);
        }
      }
      
      // Fetch BTC price from Polygon
      if (keys.polygon_key) {
        try {
          const btcResponse = await fetch(
            `https://api.polygon.io/v2/last/trade/X:BTCUSD?apiKey=${keys.polygon_key}`
          );
          
          if (btcResponse.ok) {
            const btcData = await btcResponse.json();
            btcPrice = btcData.results?.p || null;
          }
        } catch (error) {
          console.error('Error fetching BTC price:', error);
        }
      }
    }
    
    const dashboardData = {
      success: true,
      wallet: {
        id: wallet.wallet_id,
        name: wallet.name,
        env: wallet.env,
        enabled: wallet.enabled
      },
      symbols: symbolsResult.rows,
      positions: [],
      account: accountData ? {
        cash: parseFloat(accountData.cash || 0),
        equity: parseFloat(accountData.equity || 0),
        buying_power: parseFloat(accountData.buying_power || 0)
      } : {
        cash: 0,
        equity: 0,
        buying_power: 0
      },
      btc_price: btcPrice,
      performance: {
        daily_pnl: accountData ? parseFloat(accountData.equity) - parseFloat(accountData.last_equity) : 0,
        total_pnl: 0,
        daily_return: 0,
        total_return: 0
      },
      timestamp: new Date().toISOString()
    };
    
    return res.json(dashboardData);
  } catch (error) {
    console.error('Error getting dashboard data:', error);
    return res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  } finally {
    if (client) client.release();
    if (pool) await pool.end();
  }
};