// Vercel serverless function to get wallet snapshot
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
    
    // For now, return a basic snapshot structure
    // This would normally query execution_snapshots table
    const snapshot = {
      wallet_id: walletId,
      timestamp: new Date().toISOString(),
      positions: [],
      cash: 0,
      equity: 0,
      message: 'Snapshot functionality to be implemented'
    };
    
    return res.json({ success: true, snapshot });
  } catch (error) {
    console.error('Error getting wallet snapshot:', error);
    return res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  } finally {
    if (client) client.release();
    if (pool) await pool.end();
  }
};