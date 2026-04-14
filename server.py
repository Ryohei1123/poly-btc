"""
Polymarket Bot Dashboard — Backend API
Serves real-time stats from the PostgreSQL database.
Run: python server.py
Access: http://localhost:5050
"""

import os
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import psycopg
from psycopg.rows import dict_row
from flask import Flask, jsonify, send_from_directory, g
from dotenv import load_dotenv
from db_schema import apply_schema

load_dotenv()

def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/polybot")
DEFAULT_STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR = str(DEFAULT_STATIC_DIR if DEFAULT_STATIC_DIR.exists() else Path(__file__).parent)

app = Flask(__name__, static_folder=STATIC_DIR)
EVENT_SLUG_CACHE: dict[str, str] = {}
GAMMA_API = "https://gamma-api.polymarket.com"
BINANCE_API = "https://api.binance.com"
LIVE_CACHE: dict[str, dict[str, object]] = {
    "markets": {"ts": 0.0, "data": []},
    "btc_history": {"ts": 0.0, "data": []},
}
EXEC_THRESHOLDS = {
    "qhit_good": env_float("POLY_EXEC_QHIT_GOOD", 0.45),
    "qhit_warn": env_float("POLY_EXEC_QHIT_WARN", 0.20),
    "ack_good": env_float("POLY_EXEC_ACK_GOOD", 0.75),
    "ack_warn": env_float("POLY_EXEC_ACK_WARN", 0.50),
    "fill_good": env_float("POLY_EXEC_FILL_GOOD", 0.30),
    "fill_warn": env_float("POLY_EXEC_FILL_WARN", 0.12),
}
EXEC_SCORE_WEIGHTS = {
    "qhit": env_float("POLY_EXEC_SCORE_W_QHIT", 0.35),
    "ack": env_float("POLY_EXEC_SCORE_W_ACK", 0.40),
    "fill": env_float("POLY_EXEC_SCORE_W_FILL", 0.25),
}
EXEC_SCORE_THRESHOLDS = {
    "good": env_float("POLY_EXEC_SCORE_GOOD", 55.0),
    "warn": env_float("POLY_EXEC_SCORE_WARN", 35.0),
}

SQL_COUNT_TRADES = "SELECT COUNT(*) AS cnt FROM trades"
SQL_SUM_FILLED_PNL = "SELECT COALESCE(SUM(pnl), 0) AS pnl FROM trades WHERE status='filled'"
SQL_SUM_TRADE_VOLUME = "SELECT COALESCE(SUM(COALESCE(notional_usdc, size)), 0) AS volume FROM trades"
SQL_COUNT_OPEN_ORDERS = "SELECT COUNT(*) AS cnt FROM trades WHERE status IN ('open','submitted')"
SQL_LATEST_BALANCE = "SELECT balance FROM bot_stats ORDER BY ts DESC LIMIT 1"
SQL_LATEST_BTC_PRICE = "SELECT price FROM btc_prices ORDER BY ts DESC LIMIT 1"
SQL_RUNTIME_SUMMARY = """
SELECT
    updated_at,
    running,
    kill_switch,
    paper_mode,
    btc_price,
    btc_source,
    ws_connected,
    ws_tick_age_sec,
    cycle_latency_ms,
    orders_placed_cycle,
    cycle_latency_avg_ms,
    orders_placed_avg
FROM runtime_state
WHERE id=1
"""
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
SQL_RECENT_QUOTES = """SELECT ts, market_id, bid, ask, fair_price, mid, edge, placed, model_type
           FROM quotes ORDER BY ts DESC LIMIT 30"""
SQL_HOURLY_PNL = """SELECT to_char(date_trunc('hour', ts), 'YYYY-MM-DD HH24:00') as hour,
                  COUNT(*) as trades, COALESCE(SUM(pnl), 0) as pnl
           FROM trades WHERE status='filled'
           GROUP BY hour ORDER BY hour DESC LIMIT 48"""
SQL_EXEC_TELEMETRY = """
SELECT
    quotes_considered_cycle,
    quotes_eligible_cycle,
    order_attempts_cycle,
    order_acks_cycle,
    fills_cycle,
    quote_hit_rate_cycle,
    ack_rate_cycle,
    fill_rate_cycle,
    avg_edge_cycle,
    avg_order_distance_cycle,
    quote_hit_rate_avg,
    ack_rate_avg,
    fill_rate_avg,
    avg_edge_avg,
    avg_order_distance_avg,
    updated_at
FROM runtime_state
WHERE id=1
"""

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

def _safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)

def _parse_list_field(raw):
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []

def _fetch_json(url: str, params=None, timeout: int = 8):
    try:
        full_url = f"{url}?{urlencode(params)}" if params else url
        req = Request(
            full_url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; polybot/1.0)",
                "Accept": "application/json",
            },
        )
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None

def _iso_to_ts(iso: str) -> str:
    if not iso:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M")
    except Exception:
        return str(iso)[:16]

def get_live_btc_markets():
    now = time.time()
    cached = LIVE_CACHE["markets"]
    if now - float(cached["ts"]) < 10:
        return cached["data"]

    payload = _fetch_json(
        f"{GAMMA_API}/markets",
        params={
            "active": "true",
            "closed": "false",
            "limit": 2000,
            "_order": "volume",
        },
        timeout=10,
    )
    markets_out = []
    if isinstance(payload, list):
        for m in payload:
            q = str(m.get("question", "") or "")
            q_lower = q.lower()
            if "bitcoin" not in q_lower and "btc" not in q_lower:
                continue
            if not m.get("active"):
                continue

            outcomes = [str(x).strip().lower() for x in _parse_list_field(m.get("outcomes"))]
            outcome_prices = _parse_list_field(m.get("outcomePrices"))
            yes_idx = outcomes.index("yes") if "yes" in outcomes else 0
            no_idx = outcomes.index("no") if "no" in outcomes else (1 if yes_idx == 0 else 0)
            if not outcome_prices:
                outcome_prices = [0.5, 0.5]
            yes_idx = max(0, min(yes_idx, len(outcome_prices) - 1))
            no_idx = max(0, min(no_idx, len(outcome_prices) - 1))
            yes_price = _safe_float(outcome_prices[yes_idx], 0.5)
            no_price = _safe_float(outcome_prices[no_idx], 0.5)

            best_bid = _safe_float(m.get("bestBid"), 0.0)
            best_ask = _safe_float(m.get("bestAsk"), 0.0)
            mid = ((best_bid + best_ask) / 2.0) if best_bid > 0 and best_ask > 0 else yes_price
            spread = (best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 0.0

            one_day_change = _safe_float(m.get("oneDayPriceChange"), 0.0)
            if abs(one_day_change) <= 1.5:
                one_day_change *= 100.0

            events = m.get("events") if isinstance(m.get("events"), list) else []
            event_slug = ""
            if events:
                event_slug = str(events[0].get("slug", "") or "").strip()
            market_slug = str(m.get("slug", "") or "").strip()

            markets_out.append({
                "ts": _iso_to_ts(m.get("updatedAt") or m.get("endDate")),
                "question": q,
                "market_slug": market_slug,
                "event_slug": event_slug,
                "market_url": market_url_from_slugs(market_slug, event_slug),
                "yes_price": max(0.0, min(1.0, yes_price)),
                "no_price": max(0.0, min(1.0, no_price)),
                "mid_price": max(0.0, min(1.0, mid)),
                "best_bid": max(0.0, min(1.0, best_bid)),
                "best_ask": max(0.0, min(1.0, best_ask)),
                "spread": max(0.0, spread),
                "one_day_change_pct": one_day_change,
                "volume": _safe_float(m.get("volume"), 0.0),
                "liquidity": _safe_float(m.get("liquidity"), 0.0),
            })
    markets_out.sort(key=lambda x: x["volume"], reverse=True)
    LIVE_CACHE["markets"] = {"ts": now, "data": markets_out}
    return markets_out

def get_live_btc_history():
    now = time.time()
    cached = LIVE_CACHE["btc_history"]
    if now - float(cached["ts"]) < 15:
        return cached["data"]

    payload = _fetch_json(
        f"{BINANCE_API}/api/v3/klines",
        params={
            "symbol": "BTCUSDT",
            "interval": "5m",
            "limit": 288,
        },
        timeout=10,
    )
    out = []
    if isinstance(payload, list):
        for row in payload:
            if not isinstance(row, list) or len(row) < 5:
                continue
            ts_ms = int(_safe_float(row[0], 0.0))
            dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            out.append({
                "ts": dt.strftime("%Y-%m-%dT%H:%M"),
                "price": _safe_float(row[4], 0.0),
            })
    LIVE_CACHE["btc_history"] = {"ts": now, "data": out}
    return out

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
    markets = get_live_btc_markets()
    btc_hist = get_live_btc_history()
    btc_price = btc_hist[-1]["price"] if btc_hist else 0.0

    active_markets = len(markets)
    total_volume = sum(m["volume"] for m in markets)
    avg_yes = (sum(m["yes_price"] for m in markets) / active_markets) if active_markets else 0.0
    weighted_yes = (sum(m["yes_price"] * m["volume"] for m in markets) / total_volume) if total_volume > 0 else avg_yes
    yes_ge_50 = (sum(1 for m in markets if m["yes_price"] >= 0.5) / active_markets * 100.0) if active_markets else 0.0
    top = markets[0] if markets else None

    return jsonify({
        "active_markets": active_markets,
        "total_market_volume": round(total_volume, 2),
        "avg_yes_cents": round(avg_yes * 100.0, 2),
        "weighted_yes_cents": round(weighted_yes * 100.0, 2),
        "yes_ge_50_pct": round(yes_ge_50, 1),
        "btc_price": round(btc_price, 2),
        "top_market_question": top["question"] if top else "",
        "top_market_yes_cents": round((top["yes_price"] if top else 0.0) * 100.0, 2),
        "top_market_volume": round(top["volume"], 2) if top else 0.0,
    })

@app.route("/api/status")
def status():
    markets = get_live_btc_markets()
    btc_hist = get_live_btc_history()
    now = time.time()
    market_age = max(0.0, now - float(LIVE_CACHE["markets"]["ts"]))
    btc_age = max(0.0, now - float(LIVE_CACHE["btc_history"]["ts"]))
    healthy = bool(markets) and bool(btc_hist)
    return jsonify({
        "running": True,
        "healthy": healthy,
        "paper_mode": False,
        "source": "LIVE_MARKET",
        "market_count": len(markets),
        "market_age_sec": round(market_age, 1),
        "btc_age_sec": round(btc_age, 1),
    })

@app.route("/api/execution_telemetry")
def execution_telemetry():
    con = get_db()
    row = con.execute(SQL_EXEC_TELEMETRY).fetchone()
    if not row:
        return jsonify({
            "quotes_considered_cycle": 0,
            "quotes_eligible_cycle": 0,
            "order_attempts_cycle": 0,
            "order_acks_cycle": 0,
            "fills_cycle": 0,
            "quote_hit_rate_cycle": 0.0,
            "ack_rate_cycle": 0.0,
            "fill_rate_cycle": 0.0,
            "avg_edge_cycle": 0.0,
            "avg_order_distance_cycle": 0.0,
            "quote_hit_rate_avg": 0.0,
            "ack_rate_avg": 0.0,
            "fill_rate_avg": 0.0,
            "avg_edge_avg": 0.0,
            "avg_order_distance_avg": 0.0,
            "updated_at": None,
            "thresholds": EXEC_THRESHOLDS,
            "score_weights": EXEC_SCORE_WEIGHTS,
            "score_thresholds": EXEC_SCORE_THRESHOLDS,
        })
    return jsonify({
        "quotes_considered_cycle": int(row["quotes_considered_cycle"] or 0),
        "quotes_eligible_cycle": int(row["quotes_eligible_cycle"] or 0),
        "order_attempts_cycle": int(row["order_attempts_cycle"] or 0),
        "order_acks_cycle": int(row["order_acks_cycle"] or 0),
        "fills_cycle": int(row["fills_cycle"] or 0),
        "quote_hit_rate_cycle": float(row["quote_hit_rate_cycle"] or 0.0),
        "ack_rate_cycle": float(row["ack_rate_cycle"] or 0.0),
        "fill_rate_cycle": float(row["fill_rate_cycle"] or 0.0),
        "avg_edge_cycle": float(row["avg_edge_cycle"] or 0.0),
        "avg_order_distance_cycle": float(row["avg_order_distance_cycle"] or 0.0),
        "quote_hit_rate_avg": float(row["quote_hit_rate_avg"] or 0.0),
        "ack_rate_avg": float(row["ack_rate_avg"] or 0.0),
        "fill_rate_avg": float(row["fill_rate_avg"] or 0.0),
        "avg_edge_avg": float(row["avg_edge_avg"] or 0.0),
        "avg_order_distance_avg": float(row["avg_order_distance_avg"] or 0.0),
        "updated_at": (
            row["updated_at"].isoformat()
            if isinstance(row["updated_at"], datetime) else row["updated_at"]
        ),
        "thresholds": EXEC_THRESHOLDS,
        "score_weights": EXEC_SCORE_WEIGHTS,
        "score_thresholds": EXEC_SCORE_THRESHOLDS,
    })

@app.route("/api/pnl_curve")
def pnl_curve():
    markets = get_live_btc_markets()[:20]
    return jsonify([
        {
            "label": (m["question"][:22] + "…") if len(m["question"]) > 22 else m["question"],
            "value": round(m["volume"], 2),
        }
        for m in markets
    ])

@app.route("/api/trades/recent")
def recent_trades():
    markets = get_live_btc_markets()[:75]
    out = []
    for m in markets:
        out.append({
            "ts": m["ts"],
            "market_q": m["question"],
            "yes_cents": round(m["yes_price"] * 100.0, 2),
            "no_cents": round(m["no_price"] * 100.0, 2),
            "mid_cents": round(m["mid_price"] * 100.0, 2),
            "spread_cents": round(m["spread"] * 100.0, 2),
            "volume_usd": round(m["volume"], 2),
            "change_24h_pct": round(m["one_day_change_pct"], 2),
            "market_url": m["market_url"],
        })
    return jsonify(out)

@app.route("/api/trades/markets")
def market_breakdown():
    markets = get_live_btc_markets()[:12]
    return jsonify([
        {
            "market_q": m["question"],
            "volume": round(m["volume"], 2),
            "yes_cents": round(m["yes_price"] * 100.0, 2),
            "no_cents": round(m["no_price"] * 100.0, 2),
        }
        for m in markets
    ])

@app.route("/api/btc")
def btc_history():
    return jsonify(get_live_btc_history())

@app.route("/api/quotes/recent")
def recent_quotes():
    markets = get_live_btc_markets()[:30]
    return jsonify([
        {
            "model_type": "live",
            "market_id": m["market_slug"] or m["event_slug"] or m["question"][:24],
            "fair_price": m["yes_price"],
            "bid": m["best_bid"] if m["best_bid"] > 0 else m["yes_price"],
            "ask": m["best_ask"] if m["best_ask"] > 0 else m["no_price"],
            "edge": m["spread"],
            "placed": 1 if m["best_bid"] > 0 and m["best_ask"] > 0 else 0,
        }
        for m in markets
    ])

@app.route("/api/hourly_pnl")
def hourly_pnl():
    markets = get_live_btc_markets()[:24]
    return jsonify([
        {
            "hour": (m["question"][:10] + "…") if len(m["question"]) > 10 else m["question"],
            "trades": 0,
            "pnl": round(m["one_day_change_pct"], 2),
        }
        for m in markets
    ])

@app.route("/api/logs")
def get_logs():
    markets = get_live_btc_markets()
    top = markets[0] if markets else None
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        f"{now} [INFO] LIVE_DATA source=gamma+binance btc_markets={len(markets)}",
    ]
    if top:
        lines.append(
            f"{now} [INFO] TOP_MARKET yes={top['yes_price']*100:.2f}c "
            f"no={top['no_price']*100:.2f}c volume=${top['volume']:.0f} q={top['question'][:80]}"
        )
    return jsonify({"lines": lines})

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
