"""
Polymarket Bot Dashboard — Backend API
Serves real-time stats from the PostgreSQL database.
Run: python server.py
Access: http://localhost:5050
"""

import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen
import psycopg
from psycopg.rows import dict_row
from flask import Flask, jsonify, send_from_directory, g
from dotenv import load_dotenv
from db_schema import apply_schema

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/polybot")
DEFAULT_STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR = str(DEFAULT_STATIC_DIR if DEFAULT_STATIC_DIR.exists() else Path(__file__).parent)

app = Flask(__name__, static_folder=STATIC_DIR)
EVENT_SLUG_CACHE: dict[str, str] = {}

SQL_COUNT_TRADES = "SELECT COUNT(*) AS cnt FROM trades"
SQL_SUM_FILLED_PNL = "SELECT COALESCE(SUM(pnl), 0) AS pnl FROM trades WHERE status='filled'"
SQL_SUM_TRADE_VOLUME = "SELECT COALESCE(SUM(COALESCE(notional_usdc, size)), 0) AS volume FROM trades"
SQL_COUNT_OPEN_ORDERS = "SELECT COUNT(*) AS cnt FROM trades WHERE status IN ('open','submitted')"
SQL_LATEST_BALANCE = "SELECT balance FROM bot_stats ORDER BY ts DESC LIMIT 1"
SQL_LATEST_BTC_PRICE = "SELECT price FROM btc_prices ORDER BY ts DESC LIMIT 1"
SQL_RUNTIME_SUMMARY = "SELECT updated_at, running, kill_switch, paper_mode, btc_price FROM runtime_state WHERE id=1"
SQL_ACTIVE_MARKETS_LAST_HOUR = "SELECT COUNT(DISTINCT market_id) AS active_markets FROM quotes WHERE ts >= %s"
SQL_DAILY_PNL = "SELECT COALESCE(SUM(pnl), 0) AS pnl FROM trades WHERE ts >= %s AND status='filled'"
SQL_COUNT_WIN_TRADES = "SELECT COUNT(*) AS cnt FROM trades WHERE pnl > 0 AND status='filled'"
SQL_COUNT_FILLED_TRADES = "SELECT COUNT(*) AS cnt FROM trades WHERE status='filled'"
SQL_PNL_CURVE = """SELECT ts, realized_pnl FROM bot_stats
           ORDER BY ts DESC LIMIT 720"""
SQL_FILLED_TRADES_FOR_PNL = """SELECT ts, pnl FROM trades
               WHERE status='filled'
               ORDER BY ts ASC LIMIT 5000"""
SQL_RECENT_TRADES = """SELECT ts, market_id, market_slug, event_slug, market_q, mode, side, price, size, notional_usdc, size_shares, pnl, status
           FROM trades ORDER BY ts DESC LIMIT 75"""
SQL_MARKET_BREAKDOWN = """SELECT market_q, COUNT(*) as cnt, COALESCE(SUM(pnl), 0) as total_pnl,
                  AVG(price) as avg_price, SUM(COALESCE(notional_usdc, size)) as volume
           FROM trades WHERE status='filled'
           GROUP BY market_q ORDER BY total_pnl DESC"""
SQL_BTC_HISTORY = "SELECT ts, price FROM btc_prices ORDER BY ts DESC LIMIT 288"
SQL_RECENT_QUOTES = """SELECT ts, market_id, bid, ask, fair_price, mid, edge, placed
           FROM quotes ORDER BY ts DESC LIMIT 30"""
SQL_HOURLY_PNL = """SELECT to_char(date_trunc('hour', ts), 'YYYY-MM-DD HH24:00') as hour,
                  COUNT(*) as trades, COALESCE(SUM(pnl), 0) as pnl
           FROM trades WHERE status='filled'
           GROUP BY hour ORDER BY hour DESC LIMIT 48"""

def get_db():
    if "db" not in g:
        g.db = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return g.db

@app.teardown_appcontext
def close_db(_exc):
    con = g.pop("db", None)
    if con is not None:
        con.close()

def ensure_db():
    """Create DB schema. Demo data is optional via POLYBOT_SEED_DEMO=1."""
    con = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    apply_schema(con)

    # Seed demo data only when explicitly requested.
    count = con.execute("SELECT COUNT(*) AS cnt FROM trades").fetchone()["cnt"]
    if count == 0 and os.getenv("POLYBOT_SEED_DEMO", "0") == "1":
        _seed_demo_data(con)
    con.commit()
    con.close()

def _seed_demo_data(con):
    """Insert realistic demo data so the dashboard renders on first launch."""
    import random, math
    random.seed(42)
    now = datetime.now(timezone.utc)

    markets = [
        ("mkt_001", "Will BTC be above $90,000 on April 30, 2026?"),
        ("mkt_002", "Will BTC be above $95,000 on May 15, 2026?"),
        ("mkt_003", "Will Bitcoin exceed $100,000 by end of April?"),
        ("mkt_004", "Will BTC close above $88,000 this week?"),
        ("mkt_005", "Will Bitcoin drop below $80,000 in April 2026?"),
    ]

    # 645 demo trades over 30 days
    total_pnl = 0
    for i in range(645):
        ts = (now - timedelta(minutes=i*67)).isoformat()
        mid, mq = random.choice(markets)
        side = random.choice(["BUY", "SELL"])
        price = round(random.uniform(0.35, 0.72), 3)
        size = round(random.uniform(30, 120), 2)
        pnl = round(random.gauss(0.8, 1.2), 2)  # Positive edge mean
        total_pnl += pnl
        con.execute(
            "INSERT INTO trades (ts,market_id,market_slug,event_slug,market_q,side,price,size,order_id,status,pnl,fill_price) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (ts, mid, "", "", mq, side, price, size, f"ord_{i:04d}", "filled", pnl, round(price + 0.005 * (1 if side=="BUY" else -1), 3)),
        )

    # BTC prices last 24hrs
    for j in range(288):  # 5-min intervals
        ts = (now - timedelta(minutes=j*5)).isoformat()
        base = 87500
        price = base + math.sin(j/20)*800 + random.gauss(0, 200)
        con.execute(
            "INSERT INTO btc_prices VALUES (%s,%s) ON CONFLICT (ts) DO NOTHING",
            (ts, round(price, 2)),
        )

    # Quotes log
    for k in range(200):
        ts = (now - timedelta(minutes=k*10)).isoformat()
        mid_v, mq = random.choice(markets)
        fair = round(random.uniform(0.40, 0.70), 3)
        mid = round(fair + random.gauss(0, 0.04), 3)
        edge = round(abs(fair - mid), 3)
        con.execute(
            "INSERT INTO quotes (ts,market_id,bid,ask,fair_price,mid,edge,placed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (ts, mid_v, round(fair-0.02,3), round(fair+0.02,3), fair, mid, edge, int(edge>0.015)),
        )

    # Bot stats snapshots
    pnl_acc = 0
    for h in range(720):  # hourly for 30 days
        ts = (now - timedelta(hours=h)).isoformat()
        pnl_acc += random.gauss(380, 80)  # ~$272k over month
        con.execute(
            "INSERT INTO bot_stats (ts,total_trades,open_positions,realized_pnl,daily_pnl,active_markets) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (ts) DO NOTHING",
            (ts, max(0,645-h), random.randint(3,8), round(pnl_acc,2), round(random.gauss(9100,1200),2), random.randint(5,10)),
        )

def fmt_ts(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M")
    return str(value)[:16]

def market_url_from_slugs(market_slug: str, event_slug: str):
    market_slug = (market_slug or "").strip()
    event_slug = (event_slug or "").strip()
    if market_slug:
        return f"https://polymarket.com/market/{market_slug}"
    if event_slug:
        return f"https://polymarket.com/event/{event_slug}"
    return None

def rows_with_formatted_ts(rows):
    out = []
    for r in rows:
        d = dict(r)
        d["ts"] = fmt_ts(d.get("ts"))
        out.append(d)
    return out

def timeseries(rows, value_key: str, out_key: str):
    return [{"ts": fmt_ts(r["ts"]), out_key: r[value_key]} for r in rows]

def query_value(con, sql: str, key: str, params=None, default=0):
    row = con.execute(sql, params or ()).fetchone()
    if not row:
        return default
    value = row.get(key)
    return default if value is None else value

def query_latest_value(con, sql: str, key: str, params=None):
    row = con.execute(sql, params or ()).fetchone()
    if not row:
        return None
    return row.get(key)

def resolve_event_slug(market_slug: str) -> str:
    """
    Resolve market slug -> event slug via Gamma API.
    Cache in-memory to avoid repeated network calls on dashboard refresh.
    """
    slug = (market_slug or "").strip()
    if not slug:
        return ""
    cached = EVENT_SLUG_CACHE.get(slug)
    if cached is not None:
        return cached
    try:
        query = urlencode({"slug": slug})
        with urlopen(f"https://gamma-api.polymarket.com/markets?{query}", timeout=3) as resp:
            import json
            payload = json.loads(resp.read().decode("utf-8"))
        if isinstance(payload, list) and payload:
            events = payload[0].get("events")
            if isinstance(events, list) and events:
                event_slug = str(events[0].get("slug", "")).strip()
                EVENT_SLUG_CACHE[slug] = event_slug
                return event_slug
    except Exception:
        pass
    EVENT_SLUG_CACHE[slug] = ""
    return ""

# ─── API endpoints ─────────────────────────────────────────────────────────────

@app.route("/api/summary")
def summary():
    con = get_db()
    paper_initial_balance = float(os.getenv("POLY_PAPER_INITIAL_BALANCE", "500"))
    total_trades = query_value(con, SQL_COUNT_TRADES, "cnt", default=0)
    realized_pnl = query_value(
        con,
        SQL_SUM_FILLED_PNL,
        "pnl",
        default=0.0,
    )
    total_volume = query_value(
        con,
        SQL_SUM_TRADE_VOLUME,
        "volume",
        default=0.0,
    )
    open_t = query_value(
        con,
        SQL_COUNT_OPEN_ORDERS,
        "cnt",
        default=0,
    )
    latest_balance = query_latest_value(
        con,
        SQL_LATEST_BALANCE,
        "balance",
    )
    equity = float(latest_balance) if latest_balance is not None else (paper_initial_balance + realized_pnl)
    roi_pct = ((equity - paper_initial_balance) / paper_initial_balance * 100.0) if paper_initial_balance > 0 else 0.0

    btc_price = query_latest_value(con, SQL_LATEST_BTC_PRICE, "price")
    runtime = con.execute(SQL_RUNTIME_SUMMARY).fetchone()

    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    markets_active = query_value(
        con,
        SQL_ACTIVE_MARKETS_LAST_HOUR,
        "active_markets",
        params=(one_hour_ago,),
        default=0,
    )

    one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)
    daily_pnl = query_value(
        con,
        SQL_DAILY_PNL,
        "pnl",
        params=(one_day_ago,),
        default=0.0,
    )
    win_trades = query_value(
        con,
        SQL_COUNT_WIN_TRADES,
        "cnt",
        default=0,
    )
    total_filled = query_value(
        con,
        SQL_COUNT_FILLED_TRADES,
        "cnt",
        default=0,
    )
    win_rate = round((win_trades / total_filled * 100), 1) if total_filled > 0 else 0.0

    return jsonify({
        "total_trades":    total_trades,
        "realized_pnl":    round(realized_pnl, 2),
        "total_volume":    round(total_volume, 2),
        "open_orders":     open_t,
        "btc_price":       float(runtime["btc_price"]) if runtime and runtime["btc_price"] else (btc_price or 0),
        "active_markets":  markets_active,
        "daily_pnl":       round(daily_pnl, 2),
        "win_rate":        win_rate,
        "kill_switch":     bool(runtime["kill_switch"]) if runtime else False,
        "paper_mode":      bool(runtime["paper_mode"]) if runtime else True,
        "starting_balance": round(paper_initial_balance, 2),
        "equity":          round(equity, 2),
        "roi_pct":         round(roi_pct, 2),
    })

@app.route("/api/status")
def status():
    con = get_db()
    row = con.execute(
        "SELECT updated_at, running, kill_switch, paper_mode, btc_price, btc_source, ws_connected, ws_tick_age_sec, cycle_latency_ms, orders_placed_cycle, last_cycle, errors FROM runtime_state WHERE id=1"
    ).fetchone()
    if not row:
        return jsonify({
            "running": False,
            "healthy": False,
            "kill_switch": False,
            "paper_mode": True,
            "btc_price": 0.0,
            "btc_source": "ref",
            "ws_connected": False,
            "ws_tick_age_sec": None,
            "cycle_latency_ms": 0.0,
            "orders_placed_cycle": 0,
            "last_cycle": "",
            "errors": 0,
            "updated_at": None,
        })

    updated_at = row["updated_at"]
    healthy = False
    if updated_at:
        try:
            ts = updated_at if isinstance(updated_at, datetime) else datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
            healthy = (datetime.now(timezone.utc) - ts).total_seconds() <= 120
        except Exception:
            healthy = False

    return jsonify({
        "running": bool(row["running"]),
        "healthy": healthy,
        "kill_switch": bool(row["kill_switch"]),
        "paper_mode": bool(row["paper_mode"]),
        "btc_price": float(row["btc_price"] or 0),
        "btc_source": str(row["btc_source"] or "ref"),
        "ws_connected": bool(row["ws_connected"]),
        "ws_tick_age_sec": (None if row["ws_tick_age_sec"] is None else float(row["ws_tick_age_sec"])),
        "cycle_latency_ms": float(row["cycle_latency_ms"] or 0.0),
        "orders_placed_cycle": int(row["orders_placed_cycle"] or 0),
        "last_cycle": row["last_cycle"] or "",
        "errors": int(row["errors"] or 0),
        "updated_at": updated_at.isoformat() if isinstance(updated_at, datetime) else updated_at,
    })

@app.route("/api/pnl_curve")
def pnl_curve():
    con = get_db()
    rows = con.execute(SQL_PNL_CURVE).fetchall()
    if not rows:
        trades = con.execute(SQL_FILLED_TRADES_FOR_PNL).fetchall()
        acc = 0.0
        points = []
        for t in trades:
            acc += float(t["pnl"] or 0)
            points.append({"ts": fmt_ts(t["ts"]), "pnl": round(acc, 4)})
        return jsonify(points)

    rows = list(reversed(rows))
    return jsonify(timeseries(rows, "realized_pnl", "pnl"))

@app.route("/api/trades/recent")
def recent_trades():
    con = get_db()
    rows = con.execute(SQL_RECENT_TRADES).fetchall()
    out = rows_with_formatted_ts(rows)
    for d in out:
        event_slug = (d.get("event_slug") or "").strip()
        market_slug = (d.get("market_slug") or "").strip()
        if not event_slug and market_slug:
            # Backfill for old rows that only stored market slug.
            event_slug = resolve_event_slug(market_slug)
            if event_slug:
                try:
                    con.execute(
                        "UPDATE trades SET event_slug=%s WHERE market_slug=%s AND (event_slug IS NULL OR event_slug='')",
                        (event_slug, market_slug),
                    )
                    con.commit()
                except Exception:
                    pass
        d["market_url"] = market_url_from_slugs(market_slug, event_slug)
        price = float(d.get("price") or 0.0)
        size = float(d.get("notional_usdc") or d.get("size") or 0.0)
        shares = float(d.get("size_shares") or 0.0)
        if shares <= 0:
            shares = round(size / max(price, 0.01), 2) if size > 0 else 0.0
        pnl = float(d.get("pnl") or 0.0)
        # Market maker currently quotes YES token, so this indicates YES share flow.
        d["outcome"] = "YES"
        d["price_cents"] = round(price * 100.0, 2)
        d["contracts"] = round(shares, 2)
        d["notional_usdc"] = round(size, 4)
        d["return_pct"] = round((pnl / size * 100.0), 2) if size > 0 else 0.0
    return jsonify(out)

@app.route("/api/trades/markets")
def market_breakdown():
    con = get_db()
    rows = con.execute(SQL_MARKET_BREAKDOWN).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/btc")
def btc_history():
    con = get_db()
    rows = con.execute(SQL_BTC_HISTORY).fetchall()
    rows = list(reversed(rows))
    return jsonify(timeseries(rows, "price", "price"))

@app.route("/api/quotes/recent")
def recent_quotes():
    con = get_db()
    rows = con.execute(SQL_RECENT_QUOTES).fetchall()
    return jsonify(rows_with_formatted_ts(rows))

@app.route("/api/hourly_pnl")
def hourly_pnl():
    con = get_db()
    rows = con.execute(SQL_HOURLY_PNL).fetchall()
    return jsonify([dict(r) for r in reversed(rows)])

@app.route("/api/logs")
def get_logs():
    log_dir = Path(__file__).parent / "logs"
    today = datetime.now().strftime("%Y%m%d")
    log_file = log_dir / f"bot_{today}.log"
    lines = []
    if log_file.exists():
        with open(log_file, encoding="utf-8") as f:
            lines = f.readlines()[-60:]
    return jsonify({"lines": [l.rstrip() for l in lines]})

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve(path):
    if path and (Path(STATIC_DIR) / path).exists():
        return send_from_directory(STATIC_DIR, path)
    return send_from_directory(STATIC_DIR, "index.html")

if __name__ == "__main__":
    ensure_db()
    host = os.getenv("POLY_DASHBOARD_HOST", "0.0.0.0")
    port = int(os.getenv("POLY_DASHBOARD_PORT", "5050"))
    threads = int(os.getenv("POLY_DASHBOARD_THREADS", "12"))
    print("\n  🤖  Polymarket Bot Dashboard")
    print("  ─────────────────────────────")
    print(f"  http://localhost:{port}\n")
    try:
        from waitress import serve as waitress_serve
        print(f"  Serving with Waitress on {host}:{port} (threads={threads})")
        waitress_serve(app, host=host, port=port, threads=threads)
    except Exception:
        # Fallback for local development if waitress is unavailable.
        print("  Waitress not available, falling back to Flask dev server")
        app.run(host=host, port=port, debug=False)
