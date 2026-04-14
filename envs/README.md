## Environment Profiles

Use one of these files as your active `.env`:

```bash
cp envs/paper-balanced.env .env
```

Then start the bot:

```bash
python market_maker.py
```

For 24/7 operation with auto-restart:

```bash
bash scripts/install-systemd-user.sh
```

Pause/resume during debugging:

```bash
bash scripts/stop-systemd-user.sh
bash scripts/start-systemd-user.sh
```

### Files

- `paper-conservative.env` - lower risk paper profile
- `paper-balanced.env` - default paper profile
- `live-template.env` - live-trading template (fill in credentials manually)

### Notes

- Keep live credentials only in local `.env` (never commit secrets).
- `live-template.env` ships with placeholder values and safety defaults.
