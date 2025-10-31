const { Pool } = require('pg');

const pool = new Pool({
  host: '136.117.225.96',
  database: 'trade_socket',
  user: 'appuser',
  password: 'Fu3lth3j3t!',
  ssl: false
});

module.exports = async (req, res) => {
  const client = await pool.connect();
  
  try {
    const userId = 'iot3lLLI2TbYe5lIdSFL6l7ygb83';
    const symbols = ['BTDR', 'CAN', 'CIFR', 'CLSK', 'CORZ', 'HIVE', 'HUT', 'MARA', 'RIOT', 'WULF', 'APLD'];
    
    // Create wallet
    const walletResult = await client.query(`
      INSERT INTO wallets (wallet_id, user_id, env, name, enabled, created_at, updated_at)
      VALUES (
        'logan-paper1-' || substr(md5(random()::text), 1, 8),
        $1, 'paper', 'Logan Paper1', true, NOW(), NOW()
      )
      RETURNING wallet_id
    `, [userId]);
    
    const walletId = walletResult.rows[0].wallet_id;
    
    // Add symbols
    for (const symbol of symbols) {
      await client.query(`
        INSERT INTO wallet_symbols (
          wallet_id, symbol, budget_mode, percent_budget,
          buy_pct_rth, sell_pct_rth, buy_pct_ah, sell_pct_ah,
          method_rth, method_ah, enabled, updated_at
        )
        VALUES ($1, $2, 'percent', 9.0, 0.3, 0.4, 0.1, 0.3, 'MEDIAN', 'WINSORIZED', true, NOW())
      `, [walletId, symbol]);
    }
    
    // Add API keys
    await client.query(`
      INSERT INTO user_api_keys (user_id, alpaca_paper_key, alpaca_paper_secret, polygon_key, updated_at)
      VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT (user_id) DO UPDATE SET
        alpaca_paper_key = EXCLUDED.alpaca_paper_key,
        alpaca_paper_secret = EXCLUDED.alpaca_paper_secret,
        polygon_key = EXCLUDED.polygon_key
    `, [userId, 'PKM9CGRKTW3SVUT19YQB', 'XVGrnhMlsnE83QO1UYLgteUeOsoQ830Ha93xliE7', 'K_hSDwyuUSqRmD57vOlUmYqZGdcZsoG0']);
    
    res.json({
      success: true,
      walletId: walletId,
      message: 'Wallet created with 11 symbols'
    });
    
  } catch (error) {
    res.status(500).json({ error: error.message });
  } finally {
    client.release();
  }
};
