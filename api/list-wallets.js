// Vercel serverless function to list wallets
const { Pool } = require('pg');

// Create database connection pool
const getPool = () => {
  return new Pool({
    host: '136.117.225.96',
    database: 'tradiac_v2',
    user: 'appuser',
    password: 'Fu3lth3j3t!',
    ssl: false,
    max: 5
  });
};

module.exports = async (req, res) => {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const pool = getPool();
  const client = await pool.connect();
  
  try {
    // For now, just return all wallets (we'll add auth later)
    const { rows } = await client.query(`
      SELECT wallet_id, user_id, env, name, enabled, created_at, updated_at
      FROM wallets
      WHERE enabled = true
      ORDER BY created_at DESC
    `);
    
    res.json({ success: true, wallets: rows });
  } catch (error) {
    console.error('Error listing wallets:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  } finally {
    client.release();
    await pool.end();
  }
};