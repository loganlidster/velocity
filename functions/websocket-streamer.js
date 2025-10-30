/**
 * VELOCITY V2 - REAL-TIME WEBSOCKET STREAMER
 * 
 * Connects to Polygon WebSocket and streams real-time tick data
 * for BTC and 9 crypto mining stocks, storing in TimescaleDB
 */

const { onSchedule } = require("firebase-functions/v2/scheduler");
const { Connector } = require("@google-cloud/cloud-sql-connector");
const pg = require("pg");
const WebSocket = require('ws');

// Database connection
const INSTANCE_CONNECTION_NAME = "trade-socket:us-west1:trade-socket-sql";

let pool;
async function getPool() {
    if (pool) return pool;

    const dbPassword = process.env.PG_APPUSER_PASSWORD || "Fu3lth3j3t!";
    
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
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 10000,
    });

    return pool;
}

// Symbols to track
const SYMBOLS = ['BTDR', 'CAN', 'CIFR', 'CLSK', 'CORZ', 'HIVE', 'HUT', 'MARA', 'RIOT', 'WULF', 'APLD'];
const BTC_SYMBOL = 'X:BTCUSD';

// Polygon API configuration
const POLYGON_API_KEY = 'K_hSDwyuUSqRmD57vOlUmYqZGdcZsoG0';
const POLYGON_WS_URL = `wss://socket.polygon.io/stocks`;

// In-memory cache for latest prices
let latestPrices = {
    btc: null,
    stocks: {}
};

// Aggregation buffer (0.5 second intervals)
let aggregationBuffer = {};

/**
 * Connect to Polygon WebSocket and stream data
 */
function connectWebSocket() {
    const ws = new WebSocket(POLYGON_WS_URL);
    
    ws.on('open', () => {
        console.log('✅ Connected to Polygon WebSocket');
        
        // Authenticate
        ws.send(JSON.stringify({
            action: 'auth',
            params: POLYGON_API_KEY
        }));
        
        // Subscribe to BTC
        ws.send(JSON.stringify({
            action: 'subscribe',
            params: `T.${BTC_SYMBOL}`
        }));
        
        // Subscribe to all stocks
        SYMBOLS.forEach(symbol => {
            ws.send(JSON.stringify({
                action: 'subscribe',
                params: `T.${symbol}`
            }));
        });
        
        console.log(`📡 Subscribed to BTC + ${SYMBOLS.length} stocks`);
    });
    
    ws.on('message', async (data) => {
        try {
            const messages = JSON.parse(data);
            
            // Handle array of messages
            if (Array.isArray(messages)) {
                for (const msg of messages) {
                    await handleMessage(msg);
                }
            } else {
                await handleMessage(messages);
            }
        } catch (error) {
            console.error('Error processing message:', error);
        }
    });
    
    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
    });
    
    ws.on('close', () => {
        console.log('❌ WebSocket closed, reconnecting in 5 seconds...');
        setTimeout(connectWebSocket, 5000);
    });
    
    return ws;
}

/**
 * Handle incoming WebSocket message
 */
async function handleMessage(msg) {
    // Trade message
    if (msg.ev === 'T') {
        const symbol = msg.sym;
        const price = msg.p;
        const volume = msg.s;
        const timestamp = new Date(msg.t);
        
        // Update cache
        if (symbol === BTC_SYMBOL) {
            latestPrices.btc = price;
        } else if (SYMBOLS.includes(symbol)) {
            latestPrices.stocks[symbol] = price;
        }
        
        // Calculate ratio if we have both BTC and stock price
        if (latestPrices.btc && symbol !== BTC_SYMBOL && SYMBOLS.includes(symbol)) {
            const ratio = price / latestPrices.btc;
            
            // Store tick
            await storeTick({
                time: timestamp,
                symbol: symbol,
                btc_price: latestPrices.btc,
                stock_price: price,
                ratio: ratio,
                btc_volume: null,
                stock_volume: volume
            });
            
            // Add to aggregation buffer
            addToAggregationBuffer(symbol, ratio, timestamp);
        }
    }
    
    // Status messages
    if (msg.ev === 'status') {
        console.log('Status:', msg.message);
    }
}

/**
 * Store tick data in database
 */
async function storeTick(tick) {
    try {
        const pool = await getPool();
        
        await pool.query(
            `INSERT INTO ratio_ticks_live 
             (time, symbol, btc_price, stock_price, ratio, btc_volume, stock_volume)
             VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [
                tick.time,
                tick.symbol,
                tick.btc_price,
                tick.stock_price,
                tick.ratio,
                tick.btc_volume,
                tick.stock_volume
            ]
        );
    } catch (error) {
        console.error('Error storing tick:', error);
    }
}

/**
 * Add tick to aggregation buffer
 */
function addToAggregationBuffer(symbol, ratio, timestamp) {
    // Round to nearest 0.5 second
    const roundedTime = new Date(Math.floor(timestamp.getTime() / 500) * 500);
    const key = `${symbol}_${roundedTime.getTime()}`;
    
    if (!aggregationBuffer[key]) {
        aggregationBuffer[key] = {
            symbol: symbol,
            time: roundedTime,
            ratios: [],
            open: ratio,
            high: ratio,
            low: ratio,
            close: ratio
        };
    }
    
    const agg = aggregationBuffer[key];
    agg.ratios.push(ratio);
    agg.high = Math.max(agg.high, ratio);
    agg.low = Math.min(agg.low, ratio);
    agg.close = ratio;
}

/**
 * Flush aggregation buffer every 0.5 seconds
 */
async function flushAggregationBuffer() {
    const now = Date.now();
    const keysToFlush = [];
    
    for (const key in aggregationBuffer) {
        const agg = aggregationBuffer[key];
        
        // Flush if older than 0.5 seconds
        if (now - agg.time.getTime() >= 500) {
            keysToFlush.push(key);
            
            // Calculate average
            const avg = agg.ratios.reduce((a, b) => a + b, 0) / agg.ratios.length;
            
            // Store aggregate
            await storeAggregate({
                time: agg.time,
                symbol: agg.symbol,
                open_ratio: agg.open,
                high_ratio: agg.high,
                low_ratio: agg.low,
                close_ratio: agg.close,
                avg_ratio: avg,
                tick_count: agg.ratios.length
            });
        }
    }
    
    // Remove flushed keys
    keysToFlush.forEach(key => delete aggregationBuffer[key]);
}

/**
 * Store aggregate data in database
 */
async function storeAggregate(agg) {
    try {
        const pool = await getPool();
        
        await pool.query(
            `INSERT INTO ratio_aggregates_500ms 
             (time, symbol, open_ratio, high_ratio, low_ratio, close_ratio, avg_ratio, tick_count)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [
                agg.time,
                agg.symbol,
                agg.open_ratio,
                agg.high_ratio,
                agg.low_ratio,
                agg.close_ratio,
                agg.avg_ratio,
                agg.tick_count
            ]
        );
    } catch (error) {
        console.error('Error storing aggregate:', error);
    }
}

/**
 * Main streaming function - runs continuously
 */
const streamRealTimeData = onSchedule({
    schedule: "every 1 minutes",
    timeoutSeconds: 540,
    memory: "512MiB",
    secrets: ["PG_APPUSER_PASSWORD"]
}, async (event) => {
    console.log('🚀 Starting real-time data streaming...');
    
    // Connect to WebSocket
    const ws = connectWebSocket();
    
    // Start aggregation flusher
    const flushInterval = setInterval(flushAggregationBuffer, 500);
    
    // Keep alive for 9 minutes (function timeout is 9 minutes)
    await new Promise(resolve => setTimeout(resolve, 540000));
    
    // Cleanup
    clearInterval(flushInterval);
    ws.close();
    
    console.log('✅ Streaming session complete');
});

/**
 * Initialize TimescaleDB tables (run once)
 */
const initializeTimescaleDB = onSchedule({
    schedule: "every 24 hours",
    timeoutSeconds: 60,
    memory: "256MiB",
    secrets: ["PG_APPUSER_PASSWORD"]
}, async (event) => {
    console.log('🔧 Initializing TimescaleDB tables...');
    
    try {
        const pool = await getPool();
        
        // Create TimescaleDB extension
        await pool.query(`CREATE EXTENSION IF NOT EXISTS timescaledb;`);
        
        // Create tick data table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ratio_ticks_live (
                time TIMESTAMPTZ NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                btc_price DECIMAL(12,2) NOT NULL,
                stock_price DECIMAL(12,2) NOT NULL,
                ratio DECIMAL(12,6) NOT NULL,
                btc_volume DECIMAL(20,8),
                stock_volume BIGINT
            );
        `);
        
        // Convert to hypertable (ignore if already exists)
        try {
            await pool.query(`SELECT create_hypertable('ratio_ticks_live', 'time', if_not_exists => TRUE);`);
        } catch (e) {
            console.log('Hypertable already exists or error:', e.message);
        }
        
        // Create aggregates table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ratio_aggregates_500ms (
                time TIMESTAMPTZ NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                open_ratio DECIMAL(12,6) NOT NULL,
                high_ratio DECIMAL(12,6) NOT NULL,
                low_ratio DECIMAL(12,6) NOT NULL,
                close_ratio DECIMAL(12,6) NOT NULL,
                avg_ratio DECIMAL(12,6) NOT NULL,
                tick_count INTEGER NOT NULL
            );
        `);
        
        // Convert to hypertable
        try {
            await pool.query(`SELECT create_hypertable('ratio_aggregates_500ms', 'time', if_not_exists => TRUE);`);
        } catch (e) {
            console.log('Hypertable already exists or error:', e.message);
        }
        
        // Create indexes
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ratio_ticks_symbol_time ON ratio_ticks_live (symbol, time DESC);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ratio_agg_symbol_time ON ratio_aggregates_500ms (symbol, time DESC);`);
        
        // Grant permissions
        await pool.query(`GRANT ALL ON ratio_ticks_live TO appuser;`);
        await pool.query(`GRANT ALL ON ratio_aggregates_500ms TO appuser;`);
        
        // Add retention policies
        try {
            await pool.query(`SELECT add_retention_policy('ratio_ticks_live', INTERVAL '7 days', if_not_exists => TRUE);`);
            await pool.query(`SELECT add_retention_policy('ratio_aggregates_500ms', INTERVAL '90 days', if_not_exists => TRUE);`);
        } catch (e) {
            console.log('Retention policy already exists or error:', e.message);
        }
        
        console.log('✅ TimescaleDB initialization complete!');
        
    } catch (error) {
        console.error('❌ TimescaleDB initialization failed:', error);
        throw error;
    }
});

// Export functions
module.exports = {
    streamRealTimeData,
    initializeTimescaleDB
};