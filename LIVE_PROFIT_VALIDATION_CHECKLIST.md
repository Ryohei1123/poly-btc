# Live Profit Validation Checklist (First 7 Days)

Use this checklist to decide whether your market maker is ready to scale.

## 0) Pre-Flight (Must Pass Before Day 1)

- [ ] `.env` has a real `POLY_PRIVATE_KEY` (not placeholder text).
- [ ] `POLY_ENABLE_LIVE_TRADING=1`.
- [ ] Bot starts without startup credential errors.
- [ ] Logs show `[LIVE]` orders (not `[PAPER]`).
- [ ] Dashboard/API shows bot connected (`/api/status` running true).

If any item fails, do not evaluate profitability yet.

## 1) Daily Data Capture

At the same UTC time each day:

```bash
python scripts/append_daily_metrics.py
```

This appends one row to `data/daily_metrics.csv` with key fields:

- `mode`
- `filled_orders_24h`
- `fill_notional_24h_usd`
- `realized_pnl_24h_usd`
- `win_rate_24h_pct`
- `quote_hit_rate_avg_pct`
- `ack_rate_avg_pct`
- `fill_rate_avg_pct`
- `cycle_latency_avg_ms`
- `errors_snapshot`

## 2) Hard Stop Conditions (Any Day)

Stop live trading immediately and investigate if any condition is true:

- [ ] `mode != live`
- [ ] `errors_snapshot > 0` and keeps increasing across 2+ days
- [ ] `ack_rate_avg_pct < 85`
- [ ] Daily kill-switch triggers repeatedly
- [ ] Missing or stale runtime updates in dashboard status

## 3) Day-Level Pass Thresholds

For each day, mark pass/fail:

- [ ] `filled_orders_24h >= 20`
- [ ] `fill_notional_24h_usd >= 10 x POLY_ORDER_SIZE`
- [ ] `ack_rate_avg_pct >= 90`
- [ ] `fill_rate_avg_pct >= 15`
- [ ] `quote_hit_rate_avg_pct >= 20`
- [ ] `cycle_latency_avg_ms <= 3000`

Notes:

- Low `filled_orders_24h` means your parameters may be too strict or market coverage too narrow.
- High latency and low ack usually point to infra/API issues, not strategy edge.

## 4) Profit Decision Rules (After 7 Days)

Use rolling 7-day totals from `data/daily_metrics.csv`:

- [ ] Sum of `realized_pnl_24h_usd` is positive.
- [ ] At least 4 of 7 days have positive `realized_pnl_24h_usd`.
- [ ] Worst single day loss is less than `2 x POLY_MAX_DAILY_LOSS`.
- [ ] No repeated hard-stop condition from section 2.

If all pass: strategy is operationally healthy enough for small scaling.
If any fail: keep size small and tune before increasing risk.

## 5) Safe Scaling Plan

Only scale one knob at a time, then re-observe for 48 hours:

1. Increase `POLY_ORDER_SIZE` by 25%.
2. Keep `POLY_MAX_POSITION` proportional (about 8-12x order size).
3. Do not relax `POLY_MAX_DAILY_LOSS` until 7-day PnL is stable.
4. Increase `POLY_MARKETS_WATCHED` only after execution quality remains healthy.

## 6) Quick Interpretation Guide

- Positive PnL + strong ack/fill -> good candidate for cautious scale-up.
- Positive PnL + weak ack/fill -> likely fragile; fix execution first.
- Negative PnL + strong fill -> model/edge issue; tighten market filters and edge rules.
- Negative PnL + weak fill -> execution and strategy both need work.

## 7) Current Baseline Reminder

Your current `.env` is a conservative live starter:

- `POLY_ORDER_SIZE=2`
- `POLY_MAX_POSITION=20`
- `POLY_MAX_DAILY_LOSS=5`
- `POLY_MARKETS_WATCHED=8`
- `POLY_MIN_MARKET_LIQUIDITY=1000`

This is appropriate for a first live validation phase.
