// Vercel serverless function to list wallet symbols
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
    
    const { rows } = await client.query(`
      SELECT *
      FROM wallet_symbols
      WHERE wallet_id = $1
      ORDER BY symbol
    `, [walletId]);
    
    return res.json({ success: true, symbols: rows });
  } catch (error) {
    console.error('Error listing wallet symbols:', error);
    return res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  } finally {
    if (client) client.release();
    if (pool) await pool.end();
  }
};