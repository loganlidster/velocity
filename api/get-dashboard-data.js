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
    
    // For now, return basic dashboard data
    // This would normally include positions, P&L, etc.
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
      account: {
        cash: 0,
        equity: 0,
        buying_power: 0
      },
      performance: {
        daily_pnl: 0,
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