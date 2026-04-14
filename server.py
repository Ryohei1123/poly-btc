"""
Polymarket Bot Dashboard — Backend API
Serves real-time stats from the SQLite database.
Run: python server.py
Access: http://localhost:5050
"""

import sqlite3
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from flask import Flask, jsonify, send_from_directory
from dotenv import load_dotenv

load_dotenv()

DB_PATH = str(Path(__file__).parent / "data" / "bot.db")
DEFAULT_STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR = str(DEFAULT_STATIC_DIR if DEFAULT_STATIC_DIR.exists() else Path(__file__).parent)

app = Flask(__name__, static_folder=STATIC_DIR)

def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def ensure_db():
    """Create DB schema. Demo data is optional via POLYBOT_SEED_DEMO=1."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, market_id TEXT,
        market_q TEXT, side TEXT, price REAL, size REAL,
        order_id TEXT, status TEXT DEFAULT 'open', pnl REAL DEFAULT 0, fill_price REAL
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS quotes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, market_id TEXT,
        bid REAL, ask REAL, fair_price REAL, mid REAL, edge REAL, placed INTEGER DEFAULT 0
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS btc_prices (ts TEXT PRIMARY KEY, price REAL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS bot_stats (
        ts TEXT PRIMARY KEY, total_trades INTEGER DEFAULT 0,
        open_positions INTEGER DEFAULT 0, realized_pnl REAL DEFAULT 0,
        unrealized_pnl REAL DEFAULT 0, daily_pnl REAL DEFAULT 0,
        balance REAL DEFAULT 0, active_markets INTEGER DEFAULT 0
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS runtime_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        updated_at TEXT NOT NULL,
        running INTEGER DEFAULT 1,
        kill_switch INTEGER DEFAULT 0,
        paper_mode INTEGER DEFAULT 1,
        btc_price REAL DEFAULT 0,
        last_cycle TEXT DEFAULT '',
        errors INTEGER DEFAULT 0
    )""")

    # Seed demo data only when explicitly requested.
    count = con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    if count == 0 and os.getenv("POLYBOT_SEED_DEMO", "0") == "1":
        _seed_demo_data(con)
    con.commit()
    return con

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
            "INSERT INTO trades (ts,market_id,market_q,side,price,size,order_id,status,pnl,fill_price) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (ts, mid, mq, side, price, size, f"ord_{i:04d}", "filled", pnl, round(price + 0.005 * (1 if side=="BUY" else -1), 3))
        )

    # BTC prices last 24hrs
    for j in range(288):  # 5-min intervals
        ts = (now - timedelta(minutes=j*5)).isoformat()
        base = 87500
        price = base + math.sin(j/20)*800 + random.gauss(0, 200)
        con.execute("INSERT OR IGNORE INTO btc_prices VALUES (?,?)", (ts, round(price, 2)))

    # Quotes log
    for k in range(200):
        ts = (now - timedelta(minutes=k*10)).isoformat()
        mid_v, mq = random.choice(markets)
        fair = round(random.uniform(0.40, 0.70), 3)
        mid = round(fair + random.gauss(0, 0.04), 3)
        edge = round(abs(fair - mid), 3)
        con.execute(
            "INSERT INTO quotes (ts,market_id,bid,ask,fair_price,mid,edge,placed) VALUES (?,?,?,?,?,?,?,?)",
            (ts, mid_v, round(fair-0.02,3), round(fair+0.02,3), fair, mid, edge, int(edge>0.015))
        )

    # Bot stats snapshots
    pnl_acc = 0
    for h in range(720):  # hourly for 30 days
        ts = (now - timedelta(hours=h)).isoformat()
        pnl_acc += random.gauss(380, 80)  # ~$272k over month
        con.execute(
            "INSERT OR IGNORE INTO bot_stats (ts,total_trades,open_positions,realized_pnl,daily_pnl,active_markets) VALUES (?,?,?,?,?,?)",
            (ts, max(0,645-h), random.randint(3,8), round(pnl_acc,2), round(random.gauss(9100,1200),2), random.randint(5,10))
        )

# ─── API endpoints ─────────────────────────────────────────────────────────────

@app.route("/api/summary")
def summary():
    con = get_db()
    paper_initial_balance = float(os.getenv("POLY_PAPER_INITIAL_BALANCE", "500"))
    total_trades = con.execute("SELECT COUNT(*) FROM trades").fetchone()[0] or 0
    realized_pnl = con.execute("SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE status='filled'").fetchone()[0] or 0
    total_volume = con.execute("SELECT COALESCE(SUM(size), 0) FROM trades").fetchone()[0] or 0
    open_t = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0] or 0
    last_balance_row = con.execute(
        "SELECT balance FROM bot_stats ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    equity = float(last_balance_row["balance"]) if last_balance_row and last_balance_row["balance"] is not None else (paper_initial_balance + realized_pnl)
    roi_pct = ((equity - paper_initial_balance) / paper_initial_balance * 100.0) if paper_initial_balance > 0 else 0.0

    btc = con.execute("SELECT price FROM btc_prices ORDER BY ts DESC LIMIT 1").fetchone()
    runtime = con.execute(
        "SELECT updated_at, running, kill_switch, paper_mode, btc_price FROM runtime_state WHERE id=1"
    ).fetchone()

    one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    markets_active = con.execute(
        "SELECT COUNT(DISTINCT market_id) FROM quotes WHERE ts >= ?",
        (one_hour_ago,)
    ).fetchone()[0] or 0

    one_day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    daily_pnl = con.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE ts >= ? AND status='filled'",
        (one_day_ago,)
    ).fetchone()[0] or 0
    win_trades = con.execute("SELECT COUNT(*) FROM trades WHERE pnl > 0 AND status='filled'").fetchone()[0] or 0
    total_filled = con.execute("SELECT COUNT(*) FROM trades WHERE status='filled'").fetchone()[0] or 0
    win_rate = round((win_trades / total_filled * 100), 1) if total_filled > 0 else 0.0

    return jsonify({
        "total_trades":    total_trades,
        "realized_pnl":    round(realized_pnl, 2),
        "total_volume":    round(total_volume, 2),
        "open_orders":     open_t,
        "btc_price":       float(runtime["btc_price"]) if runtime and runtime["btc_price"] else (btc[0] if btc else 0),
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
        "SELECT updated_at, running, kill_switch, paper_mode, btc_price, last_cycle, errors FROM runtime_state WHERE id=1"
    ).fetchone()
    if not row:
        return jsonify({
            "running": False,
            "healthy": False,
            "kill_switch": False,
            "paper_mode": True,
            "btc_price": 0.0,
            "last_cycle": "",
            "errors": 0,
            "updated_at": None,
        })

    updated_at = row["updated_at"]
    healthy = False
    if updated_at:
        try:
            ts = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            healthy = (datetime.now(timezone.utc) - ts).total_seconds() <= 120
        except Exception:
            healthy = False

    return jsonify({
        "running": bool(row["running"]),
        "healthy": healthy,
        "kill_switch": bool(row["kill_switch"]),
        "paper_mode": bool(row["paper_mode"]),
        "btc_price": float(row["btc_price"] or 0),
        "last_cycle": row["last_cycle"] or "",
        "errors": int(row["errors"] or 0),
        "updated_at": updated_at,
    })

@app.route("/api/pnl_curve")
def pnl_curve():
    con = get_db()
    rows = con.execute(
        """SELECT ts, realized_pnl FROM bot_stats
           ORDER BY ts DESC LIMIT 720"""
    ).fetchall()
    if not rows:
        trades = con.execute(
            """SELECT ts, pnl FROM trades
               WHERE status='filled'
               ORDER BY ts ASC LIMIT 5000"""
        ).fetchall()
        acc = 0.0
        points = []
        for t in trades:
            acc += float(t["pnl"] or 0)
            points.append({"ts": t["ts"][:16], "pnl": round(acc, 4)})
        return jsonify(points)

    rows = list(reversed(rows))
    return jsonify([{"ts": r["ts"][:16], "pnl": r["realized_pnl"]} for r in rows])

@app.route("/api/trades/recent")
def recent_trades():
    con = get_db()
    rows = con.execute(
        """SELECT ts, market_q, side, price, size, pnl, status
           FROM trades ORDER BY ts DESC LIMIT 75"""
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/trades/markets")
def market_breakdown():
    con = get_db()
    rows = con.execute(
        """SELECT market_q, COUNT(*) as cnt, COALESCE(SUM(pnl), 0) as total_pnl,
                  AVG(price) as avg_price, SUM(size) as volume
           FROM trades WHERE status='filled'
           GROUP BY market_q ORDER BY total_pnl DESC"""
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/btc")
def btc_history():
    con = get_db()
    rows = con.execute(
        "SELECT ts, price FROM btc_prices ORDER BY ts DESC LIMIT 288"
    ).fetchall()
    rows = list(reversed(rows))
    return jsonify([{"ts": r["ts"][:16], "price": r["price"]} for r in rows])

@app.route("/api/quotes/recent")
def recent_quotes():
    con = get_db()
    rows = con.execute(
        """SELECT ts, market_id, bid, ask, fair_price, mid, edge, placed
           FROM quotes ORDER BY ts DESC LIMIT 30"""
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/hourly_pnl")
def hourly_pnl():
    con = get_db()
    rows = con.execute(
        """SELECT strftime('%Y-%m-%d %H:00', ts) as hour,
                  COUNT(*) as trades, COALESCE(SUM(pnl), 0) as pnl
           FROM trades WHERE status='filled'
           GROUP BY hour ORDER BY hour DESC LIMIT 48"""
    ).fetchall()
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
    print("\n  🤖  Polymarket Bot Dashboard")
    print("  ─────────────────────────────")
    print(f"  http://localhost:{port}\n")
    try:
        from waitress import serve as waitress_serve
        print(f"  Serving with Waitress on {host}:{port}")
        waitress_serve(app, host=host, port=port)
    except Exception:
        # Fallback for local development if waitress is unavailable.
        print("  Waitress not available, falling back to Flask dev server")
        app.run(host=host, port=port, debug=False)
