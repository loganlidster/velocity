# Velocity - Trading System V2

Real-time cryptocurrency mining stock trading system with WebSocket streaming and velocity-based adaptive strategies.

## Architecture

- **Frontend:** Firebase Hosting (HTML/CSS/JS)
- **Backend:** Firebase Functions (Node.js)
- **Database:** PostgreSQL with TimescaleDB
- **Real-time Data:** Polygon WebSocket streaming
- **Deployment:** Automated via GitHub Actions

## Project Structure

```
velocity/
├── functions/          # Firebase Functions (backend)
│   ├── index.js       # Main trading engine
│   └── package.json   # Dependencies
├── public/            # Frontend files
│   ├── index.html     # Dashboard UI
│   └── app.js         # Frontend logic
├── .github/
│   └── workflows/     # GitHub Actions for auto-deploy
├── firebase.json      # Firebase configuration
└── README.md
```

## Features

### V1 (Current - Production)
- BTC/Stock ratio-based trading
- Dual-feed BTC price verification (Polygon + Alpaca)
- RTH and AH session support
- 5 baseline calculation methods
- Real-time position tracking

### V2 (In Development)
- Real-time WebSocket streaming (0.5s intervals)
- TimescaleDB for tick data storage
- Enhanced baselines (120x more data points)
- Velocity-based adaptive trading
- Regime detection (RISING_FAST, FALLING_FAST, VOLATILE, STABLE)

## Deployment

Automated deployment via GitHub Actions:
- Push to `main` branch → Auto-deploys to Firebase
- Separate environments: production (tradiac-live) and development (trade-socket)

## Development

```bash
# Install dependencies
cd functions
npm install

# Deploy to Firebase
firebase deploy

# View logs
firebase functions:log --only executeTrading
```

## Database

PostgreSQL with TimescaleDB extension for time-series data:
- `ratio_ticks_live` - Raw tick data (7 day retention)
- `ratio_aggregates_500ms` - 0.5s OHLC aggregates (90 day retention)
- `baseline_daily` - Daily baseline calculations
- `execution_orders` - Order tracking
- `btc_price_comparison` - Dual-feed verification logs

## Monitored Symbols

**BTC/USD** vs 11 crypto mining stocks:
- BTDR, CAN, CIFR, CLSK, CORZ, HIVE, HUT, MARA, RIOT, WULF, APLD

## License

Private - All Rights Reserved

---

**Status:** WebSocket streaming active! 🚀