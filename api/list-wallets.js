// Vercel serverless function to list wallets
const { Pool } = require('pg');

module.exports = async (req, res) => {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  let pool;
  let client;
  
  try {
    // Create connection pool
    pool = new Pool({
      host: '136.117.225.96',
      database: 'tradiac_v2',
      user: 'appuser',
      password: 'Fu3lth3j3t!',
      ssl: {
        rejectUnauthorized: false
      },
      max: 5,
      connectionTimeoutMillis: 10000
    });
    
    client = await pool.connect();
    
    // Get user_id from query parameter (for now, return all wallets if not provided)
    const userId = req.query.user_id;
    
    let query, params;
    if (userId) {
      query = `
        SELECT wallet_id, user_id, env, name, enabled, created_at, updated_at
        FROM wallets
        WHERE enabled = true AND user_id = $1
        ORDER BY created_at DESC
      `;
      params = [userId];
    } else {
      // For now, return all wallets if no user_id provided (for testing)
      query = `
        SELECT wallet_id, user_id, env, name, enabled, created_at, updated_at
        FROM wallets
        WHERE enabled = true
        ORDER BY created_at DESC
      `;
      params = [];
    }
    
    const { rows } = await client.query(query, params);
    
    return res.json({ success: true, wallets: rows, user_id: userId });
  } catch (error) {
    console.error('Error listing wallets:', error);
    return res.status(500).json({ 
      success: false, 
      error: error.message,
      stack: error.stack
    });
  } finally {
    if (client) client.release();
    if (pool) await pool.end();
  }
};