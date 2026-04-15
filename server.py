"""
Polymarket Bot Dashboard — Backend API
PostgreSQL bot tables when the market maker is active; Gamma/Binance for market health
and fallbacks. Run: python server.py — http://localhost:5050
"""

import os
import json
import time
from typing import Optional
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import psycopg
from psycopg.rows import dict_row
from flask import Flask, jsonify, send_from_directory, g
from dotenv import load_dotenv
from db_schema import apply_schema

_REPO_ROOT = Path(__file__).resolve().parent
load_dotenv(_REPO_ROOT / ".env", override=True)

def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_any_nonempty(*keys: str) -> bool:
    return any((os.getenv(k) or "").strip() for k in keys)


def _live_credentials_configured() -> bool:
    pk_ok = _env_any_nonempty("POLY_PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY")
    if not pk_ok:
        return False
    if env_bool("POLY_CLOB_AUTO_DERIVE_API_CREDS", True):
        return True
    return (
        _env_any_nonempty("POLY_API_KEY", "POLYMARKET_API_KEY")
        and _env_any_nonempty("POLY_API_SECRET", "POLYMARKET_API_SECRET")
        and _env_any_nonempty("POLY_PASSPHRASE", "POLYMARKET_API_PASSPHRASE", "POLYMARKET_PASSPHRASE")
    )


def _execution_env_flags() -> dict:
    """CLOB trading is live-only in this repo; flags reflect whether the server .env is armed."""
    armed = env_bool("POLY_ENABLE_LIVE_TRADING", False)
    creds = _live_credentials_configured()
    return {
        "enable_live_trading": armed,
        "live_credentials_configured": creds,
        "env_ready_for_live_orders": bool(armed and creds),
    }

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
    """Create DB schema (no synthetic seed data)."""
    con = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    apply_schema(con)
    con.commit()
    con.close()

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


def _bot_log_file_path() -> Optional[Path]:
    """Path to today's bot log from market_maker.py, or explicit POLY_DASHBOARD_BOT_LOG."""
    raw = (os.getenv("POLY_DASHBOARD_BOT_LOG") or "").strip()
    if raw:
        p = Path(raw).expanduser()
        return p if p.is_file() else None
    log_dir = Path(__file__).resolve().parent / "logs"
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    candidate = log_dir / f"bot_{day}.log"
    return candidate if candidate.is_file() else None


def _tail_text_file(path: Path, max_lines: int) -> list[str]:
    """Read the last max_lines lines without loading the whole file."""
    if max_lines <= 0:
        return []
    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            block = 8192
            data = b""
            nl_count = 0
            while size > 0 and nl_count <= max_lines + 1:
                read_sz = min(block, size)
                size -= read_sz
                f.seek(size)
                data = f.read(read_sz) + data
                nl_count = data.count(b"\n")
        text = data.decode("utf-8", errors="replace")
        lines = text.splitlines()
        return lines[-max_lines:]
    except OSError:
        return []


def _dashboard_mode_raw() -> str:
    raw = (os.getenv("POLY_DASHBOARD_DATA_SOURCE", "auto") or "auto").strip().lower()
    return raw if raw in ("auto", "db", "gamma") else "auto"


def _db_has_recent_bot_activity(con) -> bool:
    try:
        r1 = con.execute(
            "SELECT 1 AS x FROM trades WHERE ts > NOW() - INTERVAL '72 hours' LIMIT 1"
        ).fetchone()
        if r1:
            return True
        r2 = con.execute(
            "SELECT 1 AS x FROM bot_stats WHERE ts > NOW() - INTERVAL '72 hours' LIMIT 1"
        ).fetchone()
        return bool(r2)
    except Exception:
        return False


def dashboard_uses_bot_db(con) -> bool:
    """Use PostgreSQL bot tables for dashboard panels (auto if recent bot rows exist)."""
    mode = _dashboard_mode_raw()
    if mode == "gamma":
        return False
    if mode == "db":
        return True
    return _db_has_recent_bot_activity(con)


def _env_credential_any(*keys: str) -> str:
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return ""


def _dash_normalize_private_key(raw: str) -> str:
    s = (raw or "").strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1].strip()
    for ch in ("\ufeff", "\u200b", "\u200c", "\u200d"):
        s = s.replace(ch, "")
    return s.strip()


def _clob_raw_balance_to_usdc(raw) -> float:
    """CLOB returns collateral balance as integer micro-USDC (6 dp) or a decimal string."""
    if raw is None:
        return 0.0
    s = str(raw).strip()
    if not s:
        return 0.0
    if "." in s or "e" in s.lower():
        return float(s)
    return int(s) / 1e6


_CLOB_BAL_CACHE: dict = {"ts": 0.0, "bal": None, "err": None}
_CLOB_BAL_TTL_SEC = 15.0


def _fetch_clob_collateral_usdc_fresh() -> tuple[Optional[float], Optional[str]]:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams
        from py_clob_client.constants import POLYGON
    except ImportError:
        return None, "py-clob-client not installed on this host"

    pk = _dash_normalize_private_key(
        _env_credential_any("POLY_PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY")
    )
    if not pk:
        return None, None

    host = (os.getenv("POLY_CLOB_API") or "https://clob.polymarket.com").strip().rstrip("/")
    sig_type_str = (os.getenv("POLY_CLOB_SIGNATURE_TYPE") or "").strip()
    signature_type = None
    if sig_type_str:
        try:
            signature_type = int(sig_type_str)
        except ValueError:
            return None, "invalid POLY_CLOB_SIGNATURE_TYPE (expected integer)"

    funder = (os.getenv("POLY_CLOB_FUNDER") or "").strip() or None
    auto_derive = env_bool("POLY_CLOB_AUTO_DERIVE_API_CREDS", True)

    try:
        if auto_derive:
            client = ClobClient(
                host=host,
                chain_id=POLYGON,
                key=pk,
                creds=None,
                signature_type=signature_type,
                funder=funder,
            )
            creds = client.create_or_derive_api_creds()
            if creds is None:
                creds = client.derive_api_key()
            if creds is None:
                return None, "CLOB API creds derive failed (None)"
            client.set_api_creds(creds)
        else:
            api_key = _env_credential_any("POLY_API_KEY", "POLYMARKET_API_KEY")
            api_secret = _env_credential_any("POLY_API_SECRET", "POLYMARKET_API_SECRET")
            api_pass = _env_credential_any(
                "POLY_PASSPHRASE", "POLYMARKET_API_PASSPHRASE", "POLYMARKET_PASSPHRASE"
            )
            if not api_key or not api_secret or not api_pass:
                return None, None
            client = ClobClient(
                host=host,
                chain_id=POLYGON,
                key=pk,
                creds=ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_pass,
                ),
                signature_type=signature_type,
                funder=funder,
            )
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=-1)
        data = client.get_balance_allowance(params)
    except Exception as e:
        return None, str(e)[:240]

    if not isinstance(data, dict):
        return None, "unexpected CLOB balance response"
    bal = _clob_raw_balance_to_usdc(data.get("balance"))
    return round(bal, 6), None


def _cached_clob_usdc_balance() -> tuple[Optional[float], Optional[str]]:
    global _CLOB_BAL_CACHE
    if not _live_credentials_configured():
        return None, None
    now = time.time()
    if (now - float(_CLOB_BAL_CACHE["ts"])) < _CLOB_BAL_TTL_SEC:
        return _CLOB_BAL_CACHE["bal"], _CLOB_BAL_CACHE["err"]
    bal, err = _fetch_clob_collateral_usdc_fresh()
    _CLOB_BAL_CACHE = {"ts": now, "bal": bal, "err": err}
    return bal, err


def _merge_clob_balance_into(payload: dict) -> dict:
    bal, err = _cached_clob_usdc_balance()
    payload["clob_collateral_usdc"] = bal
    payload["clob_collateral_usdc_error"] = err
    return payload


def _wrap_rows(rows: list, source: str) -> dict:
    return {"rows": rows, "source": source}


def _summary_from_db(con, markets: list, btc_price: float) -> dict:
    stats = con.execute(
        "SELECT realized_pnl, balance, total_trades, active_markets, daily_pnl "
        "FROM bot_stats ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    agg = con.execute(
        """SELECT
            COALESCE(SUM(COALESCE(notional_usdc, size, 0)), 0) AS vol,
            COUNT(*) FILTER (WHERE status = 'filled') AS n_filled,
            COUNT(*) FILTER (WHERE status = 'filled' AND pnl > 0) AS n_wins
        FROM trades"""
    ).fetchone()
    dm = con.execute(
        "SELECT COUNT(DISTINCT market_id) AS c FROM trades WHERE ts > NOW() - INTERVAL '24 hours'"
    ).fetchone()
    n_filled = int(agg["n_filled"] or 0) if agg else 0
    n_wins = int(agg["n_wins"] or 0) if agg else 0
    win_rate = (n_wins / n_filled * 100.0) if n_filled else 0.0
    distinct_24h = int(dm["c"] or 0) if dm else 0
    top = markets[0] if markets else None
    return {
        "dashboard_data_source": "db",
        "btc_price": round(btc_price, 2),
        "top_market_question": top["question"] if top else "",
        "top_market_yes_cents": round((top["yes_price"] if top else 0.0) * 100.0, 2),
        "top_market_volume": round(top["volume"], 2) if top else 0.0,
        "bot_realized_pnl": float(stats["realized_pnl"] or 0) if stats else 0.0,
        "bot_balance": float(stats["balance"] or 0) if stats else 0.0,
        "bot_daily_pnl": float(stats["daily_pnl"] or 0) if stats else 0.0,
        "bot_total_trades": int(stats["total_trades"] or 0) if stats else 0,
        "bot_fill_volume": float(agg["vol"] or 0) if agg else 0.0,
        "bot_filled_count": n_filled,
        "bot_win_rate_pct": round(win_rate, 1),
        "bot_distinct_markets_24h": distinct_24h,
    }


def _pnl_curve_from_db(con) -> list[dict]:
    rows = con.execute(SQL_PNL_CURVE).fetchall()
    rows = list(reversed(rows or []))
    out = []
    for r in rows:
        ts = r.get("ts")
        label = fmt_ts(ts) if ts is not None else "—"
        out.append({"label": label, "value": round(float(r.get("realized_pnl") or 0), 2)})
    return out


def _btc_from_db(con) -> list[dict]:
    rows = con.execute(SQL_BTC_HISTORY).fetchall()
    rows = list(reversed(rows or []))
    out = []
    for r in rows:
        ts = r.get("ts")
        label = fmt_ts(ts) if ts is not None else "—"
        out.append({"ts": label, "price": float(r.get("price") or 0)})
    return out


def _recent_trades_from_db(con) -> list[dict]:
    rows = con.execute(SQL_RECENT_TRADES).fetchall() or []
    out = []
    for r in rows:
        price = float(r.get("price") or 0)
        slug = (r.get("market_slug") or "").strip()
        ev = (r.get("event_slug") or "").strip()
        url = market_url_from_slugs(slug, ev)
        notional = r.get("notional_usdc")
        if notional is None:
            notional = r.get("size")
        out.append(
            {
                "ts": fmt_ts(r.get("ts")),
                "market_q": r.get("market_q") or "",
                "side": (r.get("side") or "").upper(),
                "price": price,
                "price_cents": round(price * 100.0, 2),
                "status": r.get("status") or "",
                "mode": r.get("mode") or "",
                "volume_usd": round(float(notional or 0), 2),
                "pnl": round(float(r.get("pnl") or 0), 4),
                "size_shares": float(r.get("size_shares") or 0),
                "market_url": url,
            }
        )
    return out


def _market_breakdown_from_db(con) -> list[dict]:
    rows = con.execute(SQL_MARKET_BREAKDOWN).fetchall() or []
    out = []
    for r in rows:
        out.append(
            {
                "market_q": r.get("market_q") or "",
                "volume": round(float(r.get("volume") or 0), 2),
                "total_pnl": round(float(r.get("total_pnl") or 0), 2),
                "cnt": int(r.get("cnt") or 0),
                "yes_cents": round(float(r.get("avg_price") or 0) * 100.0, 2),
            }
        )
    return out


def _quotes_from_db(con) -> list[dict]:
    rows = con.execute(SQL_RECENT_QUOTES).fetchall() or []
    out = []
    for r in rows:
        mid = float(r.get("mid") or 0)
        edge = float(r.get("edge") or 0)
        out.append(
            {
                "model_type": r.get("model_type") or "terminal",
                "market_id": (r.get("market_id") or "")[:28],
                "fair_price": float(r.get("fair_price") or 0),
                "bid": float(r.get("bid") or 0),
                "ask": float(r.get("ask") or 0),
                "edge": edge,
                "placed": int(r.get("placed") or 0),
                "mid": mid,
                "spread": abs(float(r.get("ask") or 0) - float(r.get("bid") or 0)),
            }
        )
    return out


def _hourly_pnl_from_db(con) -> list[dict]:
    rows = con.execute(SQL_HOURLY_PNL).fetchall() or []
    out = []
    for r in rows:
        out.append(
            {
                "hour": r.get("hour") or "—",
                "trades": int(r.get("trades") or 0),
                "pnl": round(float(r.get("pnl") or 0), 2),
            }
        )
    return out

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


def _gamma_market_array(payload) -> list:
    """Gamma usually returns a JSON array; tolerate wrapped shapes."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        inner = payload.get("data")
        if isinstance(inner, list):
            return inner
    return []


def _btc_rows_from_gamma_markets(items: list) -> list:
    """Filter to BTC-ish questions and normalize fields (same idea as market_maker.get_btc_markets)."""
    markets_out = []
    for m in items:
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
    return markets_out


def get_live_btc_markets():
    now = time.time()
    cached = LIVE_CACHE["markets"]
    if now - float(cached["ts"]) < 10:
        return cached["data"]

    # Match market_maker.py: crypto-tagged fetch surfaces BTC markets; global top-N can omit them.
    raw_limit = int(os.getenv("POLY_MARKETS_FETCH_LIMIT", "1000") or "1000")
    fetch_limit = max(100, min(raw_limit, 3000))
    tag = (os.getenv("POLY_GAMMA_TAG_SLUG", "crypto") or "").strip()

    base_params = {
        "active": "true",
        "closed": "false",
        "limit": fetch_limit,
        "_order": "volume",
    }
    if tag:
        base_params["tag_slug"] = tag

    payload = _fetch_json(f"{GAMMA_API}/markets", params=base_params, timeout=10)
    markets_out = _btc_rows_from_gamma_markets(_gamma_market_array(payload))

    if not markets_out:
        payload_all = _fetch_json(
            f"{GAMMA_API}/markets",
            params={
                "active": "true",
                "closed": "false",
                "limit": fetch_limit,
                "_order": "volume",
            },
            timeout=10,
        )
        markets_out = _btc_rows_from_gamma_markets(_gamma_market_array(payload_all))

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

    try:
        con = get_db()
        use_db = dashboard_uses_bot_db(con)
    except Exception:
        con = None
        use_db = False

    if use_db and con is not None:
        return jsonify(_merge_clob_balance_into(_summary_from_db(con, markets, btc_price)))

    active_markets = len(markets)
    total_volume = sum(m["volume"] for m in markets)
    avg_yes = (sum(m["yes_price"] for m in markets) / active_markets) if active_markets else 0.0
    weighted_yes = (sum(m["yes_price"] * m["volume"] for m in markets) / total_volume) if total_volume > 0 else avg_yes
    yes_ge_50 = (sum(1 for m in markets if m["yes_price"] >= 0.5) / active_markets * 100.0) if active_markets else 0.0
    top = markets[0] if markets else None

    return jsonify(
        _merge_clob_balance_into(
            {
                "dashboard_data_source": "gamma",
                "active_markets": active_markets,
                "total_market_volume": round(total_volume, 2),
                "avg_yes_cents": round(avg_yes * 100.0, 2),
                "weighted_yes_cents": round(weighted_yes * 100.0, 2),
                "yes_ge_50_pct": round(yes_ge_50, 1),
                "btc_price": round(btc_price, 2),
                "top_market_question": top["question"] if top else "",
                "top_market_yes_cents": round((top["yes_price"] if top else 0.0) * 100.0, 2),
                "top_market_volume": round(top["volume"], 2) if top else 0.0,
            }
        )
    )

@app.route("/api/status")
def status():
    markets = get_live_btc_markets()
    btc_hist = get_live_btc_history()
    now = time.time()
    market_age = max(0.0, now - float(LIVE_CACHE["markets"]["ts"]))
    btc_age = max(0.0, now - float(LIVE_CACHE["btc_history"]["ts"]))
    healthy = bool(markets) and bool(btc_hist)

    env_flags = _execution_env_flags()
    bot_running = False
    bot_stale = True
    kill_switch = False
    runtime_updated_at = None
    try:
        con = get_db()
        row = con.execute(
            "SELECT running, kill_switch, updated_at FROM runtime_state WHERE id=1"
        ).fetchone()
        if row:
            bot_running = bool(int(row["running"] or 0))
            kill_switch = bool(int(row["kill_switch"] or 0))
            u = row.get("updated_at")
            if isinstance(u, datetime):
                if u.tzinfo is None:
                    u = u.replace(tzinfo=timezone.utc)
                bot_stale = (datetime.now(timezone.utc) - u).total_seconds() > 120.0
                runtime_updated_at = u.isoformat()
            else:
                bot_stale = True
    except Exception:
        bot_running = False
        bot_stale = True
        kill_switch = False

    bot_connected = bot_running and not bot_stale

    return jsonify({
        "running": bot_connected,
        "healthy": healthy,
        "kill_switch": kill_switch,
        "bot_connected": bot_connected,
        "runtime_updated_at": runtime_updated_at,
        "source": "LIVE_MARKET",
        "market_count": len(markets),
        "market_age_sec": round(market_age, 1),
        "btc_age_sec": round(btc_age, 1),
        **env_flags,
    })

def _execution_telemetry_empty_payload():
    return {
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
    }


@app.route("/api/execution_telemetry")
def execution_telemetry():
    try:
        con = get_db()
        row = con.execute(SQL_EXEC_TELEMETRY).fetchone()
    except Exception:
        return jsonify(_execution_telemetry_empty_payload())
    if not row:
        return jsonify(_execution_telemetry_empty_payload())
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
    try:
        con = get_db()
        if dashboard_uses_bot_db(con):
            return jsonify(_wrap_rows(_pnl_curve_from_db(con), "db"))
    except Exception:
        pass
    markets = get_live_btc_markets()[:20]
    rows = [
        {
            "label": (m["question"][:22] + "…") if len(m["question"]) > 22 else m["question"],
            "value": round(m["volume"], 2),
        }
        for m in markets
    ]
    return jsonify(_wrap_rows(rows, "gamma"))


@app.route("/api/trades/recent")
def recent_trades():
    try:
        con = get_db()
        if dashboard_uses_bot_db(con):
            return jsonify(_wrap_rows(_recent_trades_from_db(con), "db"))
    except Exception:
        pass
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
    return jsonify(_wrap_rows(out, "gamma"))


@app.route("/api/trades/markets")
def market_breakdown():
    try:
        con = get_db()
        if dashboard_uses_bot_db(con):
            return jsonify(_wrap_rows(_market_breakdown_from_db(con), "db"))
    except Exception:
        pass
    markets = get_live_btc_markets()[:12]
    rows = [
        {
            "market_q": m["question"],
            "volume": round(m["volume"], 2),
            "yes_cents": round(m["yes_price"] * 100.0, 2),
            "no_cents": round(m["no_price"] * 100.0, 2),
        }
        for m in markets
    ]
    return jsonify(_wrap_rows(rows, "gamma"))


@app.route("/api/btc")
def btc_history():
    try:
        con = get_db()
        if dashboard_uses_bot_db(con):
            return jsonify(_wrap_rows(_btc_from_db(con), "db"))
    except Exception:
        pass
    return jsonify(_wrap_rows(get_live_btc_history(), "gamma"))


@app.route("/api/quotes/recent")
def recent_quotes():
    try:
        con = get_db()
        if dashboard_uses_bot_db(con):
            return jsonify(_wrap_rows(_quotes_from_db(con), "db"))
    except Exception:
        pass
    markets = get_live_btc_markets()[:30]
    rows = [
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
    ]
    return jsonify(_wrap_rows(rows, "gamma"))


@app.route("/api/hourly_pnl")
def hourly_pnl():
    try:
        con = get_db()
        if dashboard_uses_bot_db(con):
            return jsonify(_wrap_rows(_hourly_pnl_from_db(con), "db"))
    except Exception:
        pass
    markets = get_live_btc_markets()[:24]
    rows = [
        {
            "hour": (m["question"][:10] + "…") if len(m["question"]) > 10 else m["question"],
            "trades": 0,
            "pnl": round(m["one_day_change_pct"], 2),
        }
        for m in markets
    ]
    return jsonify(_wrap_rows(rows, "gamma"))

@app.route("/api/logs")
def get_logs():
    """Tail market_maker.py log (poly-btc/logs/bot_YYYYMMDD.log) when present."""
    try:
        max_lines = int(os.getenv("POLY_DASHBOARD_LOG_LINES", "120") or "120")
    except (TypeError, ValueError):
        max_lines = 120
    max_lines = max(20, min(max_lines, 500))

    log_path = _bot_log_file_path()
    if log_path:
        tailed = _tail_text_file(log_path, max_lines)
        if tailed:
            return jsonify({"lines": tailed, "source": str(log_path.name)})

    return jsonify(
        {
            "lines": [
                "No bot log file yet. Start market_maker.py to create poly-btc/logs/bot_YYYYMMDD.log"
            ],
            "source": "none",
        }
    )

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
