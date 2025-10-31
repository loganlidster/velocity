const fs = require('fs');

// Read the functions/index.js file
let content = fs.readFileSync('functions/index.js', 'utf8');

// Replace the baseline_daily table creation to use 'date' instead of 'trading_day'
content = content.replace(
  /CREATE TABLE IF NOT EXISTS baseline_daily \(\s*trading_day DATE NOT NULL,/,
  'CREATE TABLE IF NOT EXISTS baseline_daily (\n        date DATE NOT NULL,'
);

// Replace PRIMARY KEY definition
content = content.replace(
  /PRIMARY KEY \(trading_day, symbol, session, method\)/,
  'PRIMARY KEY (date, symbol, session, method)'
);

// Replace index creation
content = content.replace(
  /CREATE INDEX IF NOT EXISTS ix_baseline_symbol ON baseline_daily\(symbol, trading_day DESC\)/,
  'CREATE INDEX IF NOT EXISTS ix_baseline_symbol ON baseline_daily(symbol, date DESC)'
);

// Replace all SELECT queries that reference trading_day from baseline_daily
content = content.replace(
  /SELECT bd\.symbol, bd\.session, bd\.method, bd\.trading_day, bd\.baseline/g,
  'SELECT bd.symbol, bd.session, bd.method, bd.date as trading_day, bd.baseline'
);

// Replace INSERT INTO baseline_daily with trading_day
content = content.replace(
  /INSERT INTO baseline_daily \(trading_day,/g,
  'INSERT INTO baseline_daily (date,'
);

// Replace ON CONFLICT for baseline_daily
content = content.replace(
  /ON CONFLICT \(trading_day, symbol, session, method\)/g,
  'ON CONFLICT (date, symbol, session, method)'
);

// Write back
fs.writeFileSync('functions/index.js', content);
console.log('Fixed baseline_daily schema references');