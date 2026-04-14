# PolyBot — Polymarket Market Making Bot

## Overview

Replicates the strategy of the `0x6E1d...D0F` account:
- Reads BTC price from Binance in real-time
- Calculates **fair probability** using log-normal pricing (80% vol)
- Compares vs Polymarket CLOB midpoint
- Places two-sided quotes (bid + ask) when edge ≥ 1.5%
- Captures the spread on every fill
- Dashboard tracks all trades, PnL, and logs

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run in paper trading mode (no keys needed)
```bash
# Terminal 1 — start the bot
python bot/market_maker.py

# Terminal 2 — start the dashboard
python dashboard/server.py
# Open http://localhost:5050
```

### 3. Configure for live trading
Create a `.env` file:
```
POLY_PRIVATE_KEY=0x_your_polygon_wallet_private_key
POLY_API_KEY=your_clob_api_key
POLY_API_SECRET=your_clob_api_secret
POLY_PASSPHRASE=your_clob_passphrase
```

Get CLOB credentials from: https://docs.polymarket.com/developers/CLOB/authentication

Install the live trading client:
```bash
pip install py-clob-client
```

---

## Strategy

```
Every 30 seconds:
  1. GET /api/v3/ticker/price?symbol=BTCUSDT  → BTC spot
  2. GET gamma-api.polymarket.com/markets?tag=crypto → active BTC markets
  3. For each market:
       strike = parse "$X" from question
       dte    = days until expiry
       fair   = log_normal_cdf(btc_price, strike, dte, vol=80%)
       mid    = CLOB midpoint for YES token
       edge   = abs(fair - mid)
       if edge >= 1.5%:
         POST bid @ fair - 2%
         POST ask @ fair + 2%
```

The spread capture comes from:
- Market participants who trade at taker (market order)
- We fill at our limit, capturing ~4% gross spread
- Net after fees: ~3.5%+ per round trip

---

## Files

```
polybot/
├── bot/
│   └── market_maker.py     ← Main trading bot
├── dashboard/
│   ├── server.py           ← Flask API server
│   └── static/
│       └── index.html      ← Dashboard UI
├── data/
│   └── bot.db              ← SQLite (auto-created)
├── logs/
│   └── bot_YYYYMMDD.log    ← Daily log files
└── requirements.txt
```

---

## Config Tuning

Edit `bot/market_maker.py` → `Config` class:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SPREAD_PCT` | 0.04 (4%) | Total spread width |
| `MIN_EDGE` | 0.015 (1.5%) | Minimum edge to quote |
| `ORDER_SIZE` | $50 | USDC per side |
| `MAX_POSITION` | $500 | Max per market |
| `QUOTE_REFRESH_SEC` | 30 | Cycle interval |
| `MAX_DAILY_LOSS` | $200 | Kill switch threshold |

---

## Risk Warnings

1. **Start in paper trading mode** — verify logic before going live
2. **Prediction markets resolve to 0 or 1** — adverse selection risk if your fair price is wrong
3. **Inventory risk** — if you fill on one side and market moves against you, PnL can be negative
4. **Expiry risk** — don't hold positions into resolution; bot auto-skips markets within 12h of expiry
5. **Polymarket ToS** — ensure you comply with applicable laws in your jurisdiction

---

## Dashboard

Open `http://localhost:5050` after running `server.py`:

- **Cumulative PnL curve** — 30-day history
- **BTC price** — 24h Binance feed
- **Recent trades** — last 50 fills with PnL per trade
- **Market breakdown** — PnL by market
- **Hourly PnL bars** — 48h granular view
- **Win rate ring** — live win/loss ratio
- **Live log** — tailed bot log output
- **Quote monitor** — last 30 quotes with edge stats
