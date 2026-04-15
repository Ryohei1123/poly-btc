#!/usr/bin/env python3
"""
Drop poly-btc application tables and reapply schema from db_schema.py.

Uses DATABASE_URL from the environment, or loads .env from the project root.

Usage:
  cd /path/to/poly-btc && python scripts/reset_db.py
  DATABASE_URL=postgresql://... python scripts/reset_db.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load .env from project root when present (same pattern as typical local dev).
try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import psycopg

from db_schema import apply_schema

# Must match tables created in db_schema.SCHEMA_STATEMENTS.
_APP_TABLES = ("trades", "quotes", "btc_prices", "bot_stats", "runtime_state")


def main() -> None:
    url = os.getenv("DATABASE_URL")
    if not url:
        print("DATABASE_URL is not set. Set it or add it to .env in the project root.", file=sys.stderr)
        sys.exit(1)

    drops = ", ".join(_APP_TABLES)
    sql = f"DROP TABLE IF EXISTS {drops} CASCADE"

    with psycopg.connect(url, autocommit=True) as con:
        con.execute(sql)
        print(f"Dropped tables: {', '.join(_APP_TABLES)}")

    with psycopg.connect(url, autocommit=False) as con:
        apply_schema(con)
        con.commit()
        print("Schema reapplied (db_schema.apply_schema).")


if __name__ == "__main__":
    main()
