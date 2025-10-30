# 🚀 Quick Start Guide - Get Everything Running

## Current Status

✅ **Code Deployed:** WebSocket streaming + TimescaleDB setup
✅ **Symbols:** 11 stocks (BTDR, CAN, CIFR, CLSK, CORZ, HIVE, HUT, MARA, RIOT, WULF, APLD)
✅ **Auto-deployment:** GitHub → Firebase

## Step 1: Deploy to Vercel (5 minutes)

**Option A: Vercel CLI**
```bash
cd C:\Users\logan\OneDrive\Desktop\velocity
npm install -g vercel
vercel login
vercel
```

**Option B: Vercel Dashboard**
1. Go to: https://vercel.com/new
2. Import: `loganlidster/velocity`
3. Root Directory: Leave blank (it will use public folder)
4. Deploy!

Then add custom domain `lidster.co` in project settings.

---

## Step 2: Initialize Database (1 minute)

Run this in PowerShell:
```powershell
cd C:\Users\logan\OneDrive\Desktop\velocity
firebase use trade-socket
firebase functions:call initializeTimescaleDB
```

This creates the TimescaleDB tables for tick data.

---

## Step 3: Start Data Streaming (1 minute)

Trigger the first streaming session:
```powershell
firebase functions:call streamRealTimeData
```

After this, it runs automatically every minute!

---

## Step 4: Verify Data Collection (5 minutes)

Check the logs:
```powershell
firebase functions:log --only streamRealTimeData --limit 100
```

Look for:
- ✅ "Connected to Polygon WebSocket"
- ✅ "Subscribed to BTC + 11 stocks"
- ✅ Trade messages coming in

---

## Step 5: Check Database (Optional)

If you want to verify data is being stored, connect to your PostgreSQL database and run:

```sql
-- See recent ticks
SELECT * FROM ratio_ticks_live 
ORDER BY time DESC LIMIT 100;

-- Count by symbol
SELECT symbol, COUNT(*) as ticks
FROM ratio_ticks_live
GROUP BY symbol
ORDER BY symbol;
```

---

## What Happens Next?

**Automatic:**
- WebSocket streams data every minute (9 minutes per session)
- Data stored in TimescaleDB
- 46,800+ data points per day collected
- Retention: 7 days for ticks, 90 days for aggregates

**Manual (when ready):**
- Build enhanced baselines from 0.5s data
- Implement velocity-based trading
- Add live wallet (currently using paper)

---

## Troubleshooting

**Functions not deploying?**
- Check: https://github.com/loganlidster/velocity/actions
- Should see green checkmarks

**No data in database?**
- Check logs: `firebase functions:log --only streamRealTimeData`
- Verify TimescaleDB initialized: `firebase functions:log --only initializeTimescaleDB`

**Vercel not working?**
- Make sure you're deploying the `public` folder
- Check build logs in Vercel dashboard

---

## URLs

- **GitHub:** https://github.com/loganlidster/velocity
- **Firebase Console:** https://console.firebase.google.com/project/trade-socket
- **Vercel:** (will be `lidster.co` after setup)

---

**Ready to go? Start with Step 1!** 🚀