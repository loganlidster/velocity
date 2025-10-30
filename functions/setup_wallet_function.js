// Add this to index.js - Cloud Function to create wallet

exports.setupLoganWallet = onCall(
  {
    secrets: ["pg-appuser-password"],
  },
  async (request) => {
    const pool = await getPool();
    const client = await pool.connect();
    
    try {
      const userId = 'iot3lLLI2TbYe5lIdSFL6l7ygb83';
      const symbols = ['BTDR', 'CAN', 'CIFR', 'CLSK', 'CORZ', 'HIVE', 'HUT', 'MARA', 'RIOT', 'WULF', 'APLD'];
      
      // Create wallet
      const walletResult = await client.query(`
        INSERT INTO wallets (wallet_id, user_id, env, name, enabled, created_at, updated_at)
        VALUES (
          'logan-paper1-' || substr(md5(random()::text), 1, 8),
          $1,
          'paper',
          'Logan Paper1',
          true,
          NOW(),
          NOW()
        )
        RETURNING wallet_id
      `, [userId]);
      
      const walletId = walletResult.rows[0].wallet_id;
      console.log('Created wallet:', walletId);
      
      // Add each symbol
      for (const symbol of symbols) {
        await client.query(`
          INSERT INTO wallet_symbols (
            wallet_id, symbol, budget_mode, percent_budget, buy_budget_usd,
            buy_pct_rth, sell_pct_rth, buy_pct_ah, sell_pct_ah,
            method_rth, method_ah, enabled, updated_at
          )
          VALUES ($1, $2, 'percent', 9.0, NULL, 0.3, 0.4, 0.1, 0.3, 'MEDIAN', 'WINSORIZED', true, NOW())
        `, [walletId, symbol]);
        console.log('Added symbol:', symbol);
      }
      
      // Add API keys
      await client.query(`
        INSERT INTO user_api_keys (user_id, alpaca_paper_key, alpaca_paper_secret, polygon_key, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
          alpaca_paper_key = EXCLUDED.alpaca_paper_key,
          alpaca_paper_secret = EXCLUDED.alpaca_paper_secret,
          polygon_key = EXCLUDED.polygon_key,
          updated_at = NOW()
      `, [userId, 'PKM9CGRKTW3SVUT19YQB', 'XVGrnhMlsnE83QO1UYLgteUeOsoQ830Ha93xliE7', 'K_hSDwyuUSqRmD57vOlUmYqZGdcZsoG0']);
      
      console.log('API keys added');
      
      return {
        success: true,
        walletId: walletId,
        message: 'Wallet created successfully with 11 symbols'
      };
      
    } catch (error) {
      console.error('Error creating wallet:', error);
      throw new HttpsError('internal', error.message);
    } finally {
      client.release();
    }
  }
);
