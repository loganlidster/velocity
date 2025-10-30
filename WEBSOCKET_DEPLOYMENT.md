# 🚀 WebSocket Real-Time Data Streaming - READY TO DEPLOY

## What This Does

**Real-time data collection system** that streams BTC and 9 crypto mining stocks from Polygon WebSocket and stores tick data in TimescaleDB.

### Features:
- ✅ Connects to Polygon WebSocket
- ✅ Streams BTC/USD + 9 stocks (BTDR, CAN, CIFR, CLSK, CORZ, HIVE, HUT, MARA, RIOT)
- ✅ Calculates BTC/Stock ratios in real-time
- ✅ Stores raw ticks in `ratio_ticks_live` table
- ✅ Aggregates to 0.5 second intervals in `ratio_aggregates_500ms` table
- ✅ Auto-reconnects on disconnect
- ✅ 7-day retention for ticks, 90-day for aggregates

## How It Works

### 1. WebSocket Connection
- Connects to Polygon's real-time WebSocket
- Subscribes to BTC + 9 stocks
- Receives trade updates as they happen

### 2. Data Processing
- Calculates BTC/Stock ratio for each trade
- Stores raw tick data
- Aggregates into 0.5 second OHLC intervals

### 3. Database Storage
- **Raw Ticks:** Every trade stored with timestamp, prices, ratio
- **Aggregates:** OHLC data every 0.5 seconds (open, high, low, close, average)

## Deployment

### Files Created:
1. `functions/websocket-streamer.js` - WebSocket streaming service
2. Updated `functions/index.js` - Exports streaming functions
3. Updated `functions/package.json` - Added `ws` dependency

### Functions Deployed:
1. **`streamRealTimeData`** - Runs every minute, streams for 9 minutes
2. **`initializeTimescaleDB`** - Runs once daily, sets up tables

## Next Steps

1. **Deploy to Firebase:**
   ```bash
   cd velocity
   git add .
   git commit -m "Add WebSocket real-time data streaming"
   git push origin main
   ```

2. **Initialize Database:**
   - First deployment will create TimescaleDB tables automatically
   - Or manually trigger: `firebase functions:call initializeTimescaleDB`

3. **Start Streaming:**
   - Streaming starts automatically every minute
   - Or manually trigger: `firebase functions:call streamRealTimeData`

4. **Monitor:**
   ```bash
   firebase functions:log --only streamRealTimeData
   ```

## Data Collection Rate

**Expected data points per day:**
- Raw ticks: ~50,000-100,000 (depends on market activity)
- 0.5s aggregates: ~46,800 per symbol (2 per second × 6.5 hours × 9 stocks)
- **Total:** ~500,000 data points per day

**vs Current System:**
- Current: 390 data points per day
- New: 46,800 data points per day
- **Improvement: 120x more data!**

## Database Tables

### `ratio_ticks_live`
```sql
time          | TIMESTAMPTZ  | Trade timestamp
symbol        | VARCHAR(10)  | Stock symbol
btc_price     | DECIMAL      | BTC price at trade
stock_price   | DECIMAL      | Stock price
ratio         | DECIMAL      | Stock/BTC ratio
btc_volume    | DECIMAL      | BTC volume
stock_volume  | BIGINT       | Stock volume
```

### `ratio_aggregates_500ms`
```sql
time         | TIMESTAMPTZ  | Aggregate timestamp
symbol       | VARCHAR(10)  | Stock symbol
open_ratio   | DECIMAL      | Opening ratio
high_ratio   | DECIMAL      | Highest ratio
low_ratio    | DECIMAL      | Lowest ratio
close_ratio  | DECIMAL      | Closing ratio
avg_ratio    | DECIMAL      | Average ratio
tick_count   | INTEGER      | Number of ticks
```

## Cost Estimate

**Polygon WebSocket:**
- Free tier: Unlimited WebSocket connections
- Cost: $0

**Firebase Functions:**
- Streaming: 9 minutes × 60 times/hour × 24 hours = ~13,000 minutes/month
- Cost: ~$5-10/month

**Database Storage:**
- ~500K rows/day × 30 days = 15M rows/month
- With compression: ~2-3 GB
- Cost: ~$5/month

**Total: ~$10-15/month**

## Ready to Deploy! 🚀

Everything is set up and ready. Just push to GitHub and it will auto-deploy!