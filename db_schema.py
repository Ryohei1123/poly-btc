"""
Shared database schema and migrations for the bot and dashboard.
"""

from psycopg import Connection


SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS trades (
        id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ, market_id TEXT,
        market_slug TEXT, event_slug TEXT, market_q TEXT, side TEXT, price REAL, size REAL,
        order_id TEXT, status TEXT DEFAULT 'open', pnl REAL DEFAULT 0, fill_price REAL
    )""",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS market_slug TEXT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS event_slug TEXT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS mode TEXT DEFAULT 'paper'",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS notional_usdc REAL",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS size_shares REAL",
    """CREATE TABLE IF NOT EXISTS quotes (
        id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ, market_id TEXT,
        bid REAL, ask REAL, fair_price REAL, mid REAL, edge REAL, placed INTEGER DEFAULT 0,
        model_type TEXT DEFAULT 'terminal'
    )""",
    "ALTER TABLE quotes ADD COLUMN IF NOT EXISTS model_type TEXT DEFAULT 'terminal'",
    """CREATE TABLE IF NOT EXISTS btc_prices (ts TIMESTAMPTZ PRIMARY KEY, price REAL)""",
    """CREATE TABLE IF NOT EXISTS bot_stats (
        ts TIMESTAMPTZ PRIMARY KEY, total_trades INTEGER DEFAULT 0,
        open_positions INTEGER DEFAULT 0, realized_pnl REAL DEFAULT 0,
        unrealized_pnl REAL DEFAULT 0, daily_pnl REAL DEFAULT 0,
        balance REAL DEFAULT 0, active_markets INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS runtime_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        updated_at TIMESTAMPTZ NOT NULL,
        running INTEGER DEFAULT 1,
        kill_switch INTEGER DEFAULT 0,
        paper_mode INTEGER DEFAULT 1,
        btc_price REAL DEFAULT 0,
        btc_source TEXT DEFAULT 'ref',
        ws_connected INTEGER DEFAULT 0,
        ws_tick_age_sec REAL DEFAULT 0,
        cycle_latency_ms REAL DEFAULT 0,
        orders_placed_cycle INTEGER DEFAULT 0,
        cycle_latency_avg_ms REAL DEFAULT 0,
        orders_placed_avg REAL DEFAULT 0,
        last_cycle TEXT DEFAULT '',
        errors INTEGER DEFAULT 0
    )""",
    "ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS btc_source TEXT DEFAULT 'ref'",
    "ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS ws_connected INTEGER DEFAULT 0",
    "ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS ws_tick_age_sec REAL DEFAULT 0",
    "ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS cycle_latency_ms REAL DEFAULT 0",
    "ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS orders_placed_cycle INTEGER DEFAULT 0",
    "ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS cycle_latency_avg_ms REAL DEFAULT 0",
    "ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS orders_placed_avg REAL DEFAULT 0",
    # Historical fix: normalize share count to notional/price when legacy rows were
    # written with midpoint-derived shares (causes impossible UI values).
    """UPDATE trades
       SET size_shares = ROUND((COALESCE(notional_usdc, size) / GREATEST(price, 0.01))::numeric, 6)
       WHERE price IS NOT NULL
         AND price > 0
         AND COALESCE(notional_usdc, size) IS NOT NULL
         AND COALESCE(notional_usdc, size) > 0
         AND (
              size_shares IS NULL
              OR size_shares <= 0
              OR ABS((size_shares * price) - COALESCE(notional_usdc, size)) > GREATEST(0.5, COALESCE(notional_usdc, size) * 0.05)
         )""",
]


def apply_schema(con: Connection) -> None:
    """Apply base schema plus additive migrations."""
    for stmt in SCHEMA_STATEMENTS:
        con.execute(stmt)
