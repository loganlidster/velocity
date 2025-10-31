const { Client } = require('pg');

async function checkSchema() {
  const client = new Client({
    host: '34.168.157.63',
    port: 5432,
    user: 'appuser',
    password: 'Tradiac2024!',
    database: 'tradiac',
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 10000,
  });

  try {
    await client.connect();
    console.log('Connected to database');

    // Check if baseline_daily table exists
    const tableCheck = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name LIKE '%baseline%'
    `);
    console.log('\nBaseline tables:', tableCheck.rows);

    // Check baseline_daily columns if it exists
    if (tableCheck.rows.some(r => r.table_name === 'baseline_daily')) {
      const columns = await client.query(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'baseline_daily'
        ORDER BY ordinal_position
      `);
      console.log('\nbaseline_daily columns:', columns.rows);
    }

    // Check baseline_data_rth columns if it exists
    if (tableCheck.rows.some(r => r.table_name === 'baseline_data_rth')) {
      const columns = await client.query(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'baseline_data_rth'
        ORDER BY ordinal_position
      `);
      console.log('\nbaseline_data_rth columns:', columns.rows);
    }

    // Check baseline_data_ah columns if it exists
    if (tableCheck.rows.some(r => r.table_name === 'baseline_data_ah')) {
      const columns = await client.query(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'baseline_data_ah'
        ORDER BY ordinal_position
      `);
      console.log('\nbaseline_data_ah columns:', columns.rows);
    }

  } catch (error) {
    console.error('Error:', error.message);
  } finally {
    await client.end();
  }
}

checkSchema();
