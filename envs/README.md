## Environment

This project runs **live Polymarket CLOB orders only** (paper simulation was removed).

```bash
cp envs/live-template.env .env
# Edit .env: DATABASE_URL, POLY_ENABLE_LIVE_TRADING=1, and the four POLY_* CLOB credentials.
```

Then:

```bash
python market_maker.py
```

For 24/7 operation with auto-restart:

```bash
bash scripts/install-systemd-user.sh
```

### Files

- `live-template.env` — starting point for production (fill in credentials).

- Keep API keys only in local `.env` (never commit secrets).
