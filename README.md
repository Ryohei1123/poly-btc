# PolyBot — Polymarket Market Making Bot

## Overview

Replicates the strategy of the `0x6E1d...D0F` account:
- Reads BTC price from multiple exchanges (Binance/Coinbase/Kraken) and uses a median reference
- Uses Binance websocket for low-latency BTC updates with periodic reference re-anchoring
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

### 2. Configure PostgreSQL
```bash
# Example local DB (adjust credentials/host as needed)
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/polybot
```

### 3. Run in paper trading mode (no keys needed)
```bash
# Terminal 1 — start the bot
python market_maker.py

# Terminal 2 — start the dashboard
python server.py
# Open http://localhost:5050
```

Paper mode defaults:
- `POLY_FORCE_PAPER=1` (enabled by default)
- `POLY_PAPER_INITIAL_BALANCE=500` (starting paper equity)

### 4. Configure for live trading
Create a `.env` file:
```
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/polybot
POLY_FORCE_PAPER=0
POLY_EXECUTION_MODE=live
POLY_ENABLE_LIVE_TRADING=1
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
  1. GET BTC spot from Binance + Coinbase + Kraken  → median BTC reference
  2. GET gamma-api.polymarket.com/markets?tag=crypto → active BTC markets
  3. For each supported BTC terminal-price market:
       strike = parse "$X" from question
       dte    = days until expiry
       fair   = log_normal_cdf(btc_price, strike, dte, vol=80%)
       mid    = CLOB midpoint for YES token
       edge   = abs(fair - mid)
       if edge >= 1.5%:
         if market exposure + order_size <= max_position:
         POST bid @ fair - 2%
         POST ask @ fair + 2%
```

Supported question styles:
- terminal-style (e.g. "be above/below $X on/by date")
- barrier-style (e.g. "hit/reach/touch $X by date")
- comparative-event BTC barriers with explicit 50-50 fallback (e.g. "BTC hits $X before Y"; modeled with BTC hit hazard vs anchor-event hazard)

Unsupported/complex market structures that do not match the three models above are skipped by design to avoid model mismatch.

The spread capture comes from:
- Market participants who trade at taker (market order)
- We fill at our limit, capturing ~4% gross spread
- Net after fees: ~3.5%+ per round trip

---

## Files

```
poly-btc/
├── market_maker.py         ← Main trading bot
├── server.py               ← Flask API server
├── index.html              ← Dashboard UI
├── logs/
│   └── bot_YYYYMMDD.log    ← Daily log files
└── requirements.txt
```

---

## Config Tuning

Edit `market_maker.py` → `Config` class:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SPREAD_PCT` | 0.04 (4%) | Total spread width |
| `MIN_EDGE` | 0.015 (1.5%) | Minimum edge to quote |
| `ORDER_SIZE` | $50 | USDC per side |
| `MAX_POSITION` | $500 | Max per market |
| `QUOTE_REFRESH_SEC` | 30 | Cycle interval |
| `MAX_DAILY_LOSS` | $200 | Kill switch threshold |

Runtime env vars:
- `DATABASE_URL=postgresql://...` sets PostgreSQL connection
- `POLY_EXECUTION_MODE=paper|live` chooses execution mode
- `POLY_FORCE_PAPER=1` forces paper mode even if live mode is configured
- `POLY_ENABLE_LIVE_TRADING=1` is required for real orders in live mode
- `POLY_PAPER_INITIAL_BALANCE=500` sets paper account starting balance
- `POLY_STRATEGY_PROFILE=conservative|balanced|target_clone|aggressive` applies preset defaults for major strategy/risk knobs (explicit per-key env vars always override)
  - `target_clone` is tuned for broad BTC market-maker coverage (faster cycle, wider market set, moderate spread/edge)
- `POLY_SPREAD_PCT` quote spread width (e.g. `0.04` = 4%)
- `POLY_MIN_EDGE` minimum fair-vs-mid edge to quote
- `POLY_ORDER_SIZE` USDC notional per side
- `POLY_MAX_POSITION` max USDC exposure per market
- `POLY_QUOTE_REFRESH_SEC` quote cycle cadence
- `POLY_MARKETS_WATCHED` breadth of BTC markets scanned each cycle
- `POLY_MARKETS_FETCH_LIMIT` raw Gamma fetch size before model filtering
- `POLY_MIN_MARKET_LIQUIDITY` minimum market liquidity required to place quotes
- `POLY_MAX_DAILY_LOSS` kill-switch threshold
- `POLY_MAX_OPEN_ORDERS` safety cap on outstanding orders
- `POLY_INVENTORY_SOFT_LIMIT_PCT` one-sided quoting threshold (inventory rebalance)
- `POLY_INVENTORY_HARD_LIMIT_PCT` hard block threshold for risk-increasing side
- `POLY_INVENTORY_SKEW_PCT` quote skew magnitude used to mean-revert inventory
- `POLY_PASSIVE_BUFFER_TICKS` top-of-book passive buffer (post-only style distance)
- `POLY_MIN_MID_DISTANCE_PCT` minimum distance from midpoint to avoid taker-like quotes
- `POLY_MAX_MID_DISTANCE_PCT` maximum distance from midpoint to avoid too-far stale quotes
- `POLY_EXEC_QHIT_GOOD` / `POLY_EXEC_QHIT_WARN` execution panel thresholds for quote-hit rate
- `POLY_EXEC_ACK_GOOD` / `POLY_EXEC_ACK_WARN` execution panel thresholds for ack rate
- `POLY_EXEC_FILL_GOOD` / `POLY_EXEC_FILL_WARN` execution panel thresholds for fill rate
- `POLY_EXEC_SCORE_W_QHIT`, `POLY_EXEC_SCORE_W_ACK`, `POLY_EXEC_SCORE_W_FILL` score weighting
- `POLY_EXEC_SCORE_GOOD` / `POLY_EXEC_SCORE_WARN` score color bands in dashboard
- `POLY_MIN_ORDER_SHARES` minimum share size guard
- `POLY_MAX_ORDERS_PER_CYCLE` per-cycle order throttle
- `POLY_CANCEL_BEFORE_REQUOTE=1` cancel stale live orders before each quote cycle
- `POLY_BTC_REFERENCE_REFRESH_SEC=120` reference median refresh cadence in seconds
- `POLY_ANCHOR_EVENT_HAZARD_PER_DAY` generic anchor-event hazard for comparative-event model
- `POLY_GTA_RELEASE_HAZARD_PER_DAY` GTA-specific anchor-event hazard override

## Run 24/7 (Linux)

Use `tmux` so bot and dashboard stay alive after you disconnect:

```bash
sudo apt-get update && sudo apt-get install -y tmux
cd /root/works/poly-btc
pip install -r requirements.txt

# Bot session
tmux new -d -s polybot 'export POLY_FORCE_PAPER=1 POLY_PAPER_INITIAL_BALANCE=500; python market_maker.py'

# Dashboard session
tmux new -d -s polydash 'python server.py'

# Check logs / status
tmux ls
tmux attach -t polybot
```

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
- **Quote model visibility** — quote stream shows model type (`terminal`, `barrier`, `comparative_5050`)
- **Header telemetry** — bot status, websocket health, per-cycle and 10-cycle average orders/latency with simple trend markers (`+` improving/increasing, `-` worsening/decreasing, `=` unchanged) and inline legend (hover, click/tap, keyboard accessible, viewport-aware tooltip placement)
- **Execution telemetry API** — `/api/execution_telemetry` exposes quote hit/ack/fill rates, edge quality, and quote distance (cycle + rolling averages)
