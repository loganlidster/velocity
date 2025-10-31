// Vercel serverless function to get wallet baselines
const { Pool } = require('pg');

module.exports = async (req, res) => {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const walletId = req.query.wallet_id || req.body?.wallet_id;
  
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
    
    // Get symbols for this wallet
    const symbolsResult = await client.query(`
      SELECT symbol FROM wallet_symbols WHERE wallet_id = $1
    `, [walletId]);
    
    const symbols = symbolsResult.rows.map(r => r.symbol);
    
    if (symbols.length === 0) {
      return res.json({ success: true, baselines: [] });
    }
    
    // Get latest baselines for these symbols
    const baselinesResult = await client.query(`
      SELECT 
        symbol,
        session,
        method,
        baseline_value,
        date,
        sample_count
      FROM baseline_daily
      WHERE symbol = ANY($1)
        AND date = (SELECT MAX(date) FROM baseline_daily WHERE symbol = ANY($1))
      ORDER BY symbol, session, method
    `, [symbols]);
    
    return res.json({ success: true, baselines: baselinesResult.rows });
  } catch (error) {
    console.error('Error getting wallet baselines:', error);
    return res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  } finally {
    if (client) client.release();
    if (pool) await pool.end();
  }
};