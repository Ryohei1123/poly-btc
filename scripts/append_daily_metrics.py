#!/usr/bin/env python3
"""
Append one daily evaluation row to data/daily_metrics.csv.

Usage:
  python scripts/append_daily_metrics.py
"""

from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional helper
    def load_dotenv(*_args, **_kwargs):  # type: ignore[override]
        return False


REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env", override=True)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/polybot")
OUTPUT_CSV = REPO_ROOT / "data" / "daily_metrics.csv"


CSV_COLUMNS = [
    "date_utc",
    "mode",
    "total_trades_all_time",
    "filled_orders_24h",
    "wins_24h",
    "win_rate_24h_pct",
    "fill_notional_24h_usd",
    "realized_pnl_24h_usd",
    "daily_pnl_snapshot_usd",
    "balance_snapshot_usd",
    "quote_hit_rate_avg_pct",
    "ack_rate_avg_pct",
    "fill_rate_avg_pct",
    "avg_edge_avg_pct",
    "avg_order_distance_avg_pct",
    "cycle_latency_avg_ms",
    "errors_snapshot",
]


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def fetch_metrics() -> dict[str, Any]:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "psycopg is not installed for this Python interpreter. "
            "Run with the project virtualenv, e.g. '.venv/bin/python scripts/append_daily_metrics.py'."
        ) from e

    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as con:
        agg = con.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE status = 'filled' AND ts > NOW() - INTERVAL '24 hours') AS filled_24h,
              COUNT(*) FILTER (WHERE status = 'filled' AND ts > NOW() - INTERVAL '24 hours' AND pnl > 0) AS wins_24h,
              COALESCE(
                SUM(
                  CASE
                    WHEN status = 'filled' AND ts > NOW() - INTERVAL '24 hours'
                    THEN COALESCE(fill_price * size_shares, notional_usdc, size, 0)
                    ELSE 0
                  END
                ),
                0
              ) AS fill_notional_24h,
              COALESCE(SUM(CASE WHEN status = 'filled' AND ts > NOW() - INTERVAL '24 hours' THEN pnl ELSE 0 END), 0) AS realized_pnl_24h
            FROM trades
            """
        ).fetchone() or {}

        stats = con.execute(
            """
            SELECT total_trades, daily_pnl, balance
            FROM bot_stats
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchone() or {}

        runtime = con.execute(
            """
            SELECT
              paper_mode,
              quote_hit_rate_avg,
              ack_rate_avg,
              fill_rate_avg,
              avg_edge_avg,
              avg_order_distance_avg,
              cycle_latency_avg_ms,
              errors
            FROM runtime_state
            WHERE id = 1
            """
        ).fetchone() or {}

    filled_24h = _to_int(agg.get("filled_24h"))
    wins_24h = _to_int(agg.get("wins_24h"))
    win_rate_24h_pct = (wins_24h / filled_24h * 100.0) if filled_24h else 0.0

    mode = "paper" if _to_int(runtime.get("paper_mode")) == 1 else "live"

    return {
        "date_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "mode": mode,
        "total_trades_all_time": _to_int(stats.get("total_trades")),
        "filled_orders_24h": filled_24h,
        "wins_24h": wins_24h,
        "win_rate_24h_pct": round(win_rate_24h_pct, 2),
        "fill_notional_24h_usd": round(_to_float(agg.get("fill_notional_24h")), 4),
        "realized_pnl_24h_usd": round(_to_float(agg.get("realized_pnl_24h")), 4),
        "daily_pnl_snapshot_usd": round(_to_float(stats.get("daily_pnl")), 4),
        "balance_snapshot_usd": round(_to_float(stats.get("balance")), 4),
        "quote_hit_rate_avg_pct": round(_to_float(runtime.get("quote_hit_rate_avg")) * 100.0, 2),
        "ack_rate_avg_pct": round(_to_float(runtime.get("ack_rate_avg")) * 100.0, 2),
        "fill_rate_avg_pct": round(_to_float(runtime.get("fill_rate_avg")) * 100.0, 2),
        "avg_edge_avg_pct": round(_to_float(runtime.get("avg_edge_avg")) * 100.0, 4),
        "avg_order_distance_avg_pct": round(_to_float(runtime.get("avg_order_distance_avg")) * 100.0, 4),
        "cycle_latency_avg_ms": round(_to_float(runtime.get("cycle_latency_avg_ms")), 2),
        "errors_snapshot": _to_int(runtime.get("errors")),
    }


def append_csv(row: dict[str, Any]) -> None:
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    file_exists = OUTPUT_CSV.exists()
    with OUTPUT_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})


def main() -> None:
    row = fetch_metrics()
    append_csv(row)
    print(f"Appended daily metrics to {OUTPUT_CSV}")
    print(row)


if __name__ == "__main__":
    main()

