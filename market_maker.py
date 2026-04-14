"""
Polymarket Market Making Bot
============================
Strategy: BTC-linked binary prediction markets
- Reads BTC price from Binance
- Calculates fair probability from price/strike
- Compares vs Polymarket CLOB midpoint
- Places two-sided quotes when edge >= threshold
- Captures spread, earns maker rebates
"""

import asyncio
import json
import logging
import os
import time
import math
import random
import statistics
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from typing import Optional
import aiohttp
import psycopg
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────

def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}

def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default

def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default

@dataclass
class Config:
    # API endpoints
    GAMMA_API: str = "https://gamma-api.polymarket.com"
    CLOB_API: str  = "https://clob.polymarket.com"
    BINANCE_API: str = "https://api.binance.com"
    COINBASE_API: str = "https://api.exchange.coinbase.com"
    KRAKEN_API: str = "https://api.kraken.com"
    BINANCE_WS: str = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    BTC_REFERENCE_REFRESH_SEC: int = field(default_factory=lambda: env_int("POLY_BTC_REFERENCE_REFRESH_SEC", 120))

    # Trading parameters
    SPREAD_PCT: float = 0.04       # Quote ±2% around fair price (captures 4% spread)
    MIN_EDGE: float   = 0.015      # Minimum edge over midpoint to place order
    ORDER_SIZE: float = 50.0       # USDC notional per side per quote
    MAX_POSITION: float = 500.0    # Max USDC in any single market
    QUOTE_REFRESH_SEC: int = 30    # How often to refresh quotes
    MARKETS_WATCHED: int = 10      # How many BTC markets to watch
    PAPER_FILL_BASE: float = 0.20  # Base fill probability in paper mode
    PAPER_INITIAL_BALANCE: float = field(default_factory=lambda: env_float("POLY_PAPER_INITIAL_BALANCE", 500.0))  # Paper account starting balance (USDC)
    FORCE_PAPER: bool = field(default_factory=lambda: env_bool("POLY_FORCE_PAPER", True))
    EXECUTION_MODE: str = field(default_factory=lambda: os.getenv("POLY_EXECUTION_MODE", "paper").strip().lower())
    ENABLE_LIVE_TRADING: bool = field(default_factory=lambda: env_bool("POLY_ENABLE_LIVE_TRADING", False))
    MIN_ORDER_SHARES: float = field(default_factory=lambda: env_float("POLY_MIN_ORDER_SHARES", 5.0))
    MAX_ORDERS_PER_CYCLE: int = field(default_factory=lambda: env_int("POLY_MAX_ORDERS_PER_CYCLE", 20))
    CANCEL_BEFORE_REQUOTE: bool = field(default_factory=lambda: env_bool("POLY_CANCEL_BEFORE_REQUOTE", True))

    # Risk
    MAX_DAILY_LOSS: float = 200.0  # Kill switch: stop if daily PnL < -$200
    MAX_OPEN_ORDERS: int = 40      # Cancel all if exceeded

    # Auth (set via env or .env file)
    PRIVATE_KEY: str = field(default_factory=lambda: os.getenv("POLY_PRIVATE_KEY", ""))
    API_KEY: str     = field(default_factory=lambda: os.getenv("POLY_API_KEY", ""))
    API_SECRET: str  = field(default_factory=lambda: os.getenv("POLY_API_SECRET", ""))
    API_PASSPHRASE: str = field(default_factory=lambda: os.getenv("POLY_PASSPHRASE", ""))

    # Database
    DATABASE_URL: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5432/polybot",
        )
    )

config = Config()
live_client = None

def is_live_mode() -> bool:
    mode = config.EXECUTION_MODE
    if mode not in {"paper", "live"}:
        mode = "paper"
    if config.FORCE_PAPER:
        return False
    return mode == "live"

def validate_runtime_config():
    if is_live_mode() and not config.ENABLE_LIVE_TRADING:
        raise RuntimeError(
            "Live mode requested but POLY_ENABLE_LIVE_TRADING is not enabled. "
            "Set POLY_ENABLE_LIVE_TRADING=1 to allow real order placement."
        )
    if is_live_mode():
        missing = []
        for env_name, value in {
            "POLY_PRIVATE_KEY": config.PRIVATE_KEY,
            "POLY_API_KEY": config.API_KEY,
            "POLY_API_SECRET": config.API_SECRET,
            "POLY_PASSPHRASE": config.API_PASSPHRASE,
        }.items():
            if not value:
                missing.append(env_name)
        if missing:
            raise RuntimeError(f"Missing required live trading credentials: {', '.join(missing)}")

def notional_to_shares(notional_usdc: float, price: float) -> float:
    if notional_usdc <= 0:
        return 0.0
    return round(notional_usdc / max(price, 0.01), 6)

# ─── Logging ──────────────────────────────────────────────────────────────────

log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_dir / f"bot_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger("polybot")

# ─── Database ─────────────────────────────────────────────────────────────────

def init_db(database_url: str):
    con = psycopg.connect(database_url, autocommit=False)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id          BIGSERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL,
            market_id   TEXT    NOT NULL,
            market_slug TEXT,
            event_slug  TEXT,
            market_q    TEXT,
            mode        TEXT    DEFAULT 'paper',
            side        TEXT    NOT NULL,
            price       REAL    NOT NULL,
            size        REAL    NOT NULL,
            notional_usdc REAL,
            size_shares REAL,
            order_id    TEXT,
            status      TEXT    DEFAULT 'open',
            pnl         REAL    DEFAULT 0,
            fill_price  REAL
        )
    """)
    con.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS market_slug TEXT")
    con.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS event_slug TEXT")
    con.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS mode TEXT DEFAULT 'paper'")
    con.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS notional_usdc REAL")
    con.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS size_shares REAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            id          BIGSERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL,
            market_id   TEXT    NOT NULL,
            bid         REAL,
            ask         REAL,
            fair_price  REAL,
            mid         REAL,
            edge        REAL,
            placed      INTEGER DEFAULT 0
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS btc_prices (
            ts    TIMESTAMPTZ PRIMARY KEY,
            price REAL NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bot_stats (
            ts              TIMESTAMPTZ PRIMARY KEY,
            total_trades    INTEGER DEFAULT 0,
            open_positions  INTEGER DEFAULT 0,
            realized_pnl    REAL    DEFAULT 0,
            unrealized_pnl  REAL    DEFAULT 0,
            daily_pnl       REAL    DEFAULT 0,
            balance         REAL    DEFAULT 0,
            active_markets  INTEGER DEFAULT 0
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS runtime_state (
            id              INTEGER PRIMARY KEY CHECK (id = 1),
            updated_at      TIMESTAMPTZ NOT NULL,
            running         INTEGER DEFAULT 1,
            kill_switch     INTEGER DEFAULT 0,
            paper_mode      INTEGER DEFAULT 1,
            btc_price       REAL DEFAULT 0,
            btc_source      TEXT DEFAULT 'ref',
            ws_connected    INTEGER DEFAULT 0,
            ws_tick_age_sec REAL DEFAULT 0,
            cycle_latency_ms REAL DEFAULT 0,
            orders_placed_cycle INTEGER DEFAULT 0,
            last_cycle      TEXT DEFAULT '',
            errors          INTEGER DEFAULT 0
        )
    """)
    con.execute("ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS btc_source TEXT DEFAULT 'ref'")
    con.execute("ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS ws_connected INTEGER DEFAULT 0")
    con.execute("ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS ws_tick_age_sec REAL DEFAULT 0")
    con.execute("ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS cycle_latency_ms REAL DEFAULT 0")
    con.execute("ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS orders_placed_cycle INTEGER DEFAULT 0")
    con.commit()
    return con

db = init_db(config.DATABASE_URL)

# ─── Data Models ──────────────────────────────────────────────────────────────

@dataclass
class Market:
    condition_id: str
    slug: str
    event_slug: str
    question: str
    yes_token: str
    no_token: str
    yes_price: float
    no_price: float
    volume: float
    liquidity: float
    end_date_iso: str
    active: bool

@dataclass
class Quote:
    market: Market
    fair_price: float   # 0–1 probability
    mid: float          # current Polymarket midpoint
    bid: float          # our bid
    ask: float          # our ask
    edge: float         # how much we beat the mid
    should_place: bool

@dataclass
class BotState:
    running: bool = True
    btc_price: float = 0.0
    total_trades: int = 0
    realized_pnl: float = 0.0
    daily_pnl: float = 0.0
    open_positions: dict = field(default_factory=dict)
    active_markets: list = field(default_factory=list)
    last_cycle: str = ""
    kill_switch: bool = False
    errors: list = field(default_factory=list)
    paper_mode: bool = True
    starting_balance: float = config.PAPER_INITIAL_BALANCE
    current_balance: float = config.PAPER_INITIAL_BALANCE
    equity: float = config.PAPER_INITIAL_BALANCE
    ws_btc_price: float = 0.0
    ws_connected: bool = False
    ws_last_tick: float = 0.0
    reference_btc_price: float = 0.0
    reference_last_update: float = 0.0
    cycle_latency_ms: float = 0.0
    orders_placed_cycle: int = 0

state = BotState()

class BinanceBtcWsFeed:
    """Maintains a real-time BTC ticker stream from Binance websocket."""
    def __init__(self):
        self._running = True

    async def run(self):
        backoff_sec = 1
        while self._running and state.running:
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_read=60)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(config.BINANCE_WS, heartbeat=20) as ws:
                        state.ws_connected = True
                        log.info("BTC websocket connected (Binance)")
                        backoff_sec = 1
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                    tick = float(payload.get("c", 0.0))
                                    if tick > 0:
                                        state.ws_btc_price = tick
                                        state.ws_last_tick = time.time()
                                except Exception:
                                    continue
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"BTC websocket disconnected: {e}")
            finally:
                state.ws_connected = False
                if self._running and state.running:
                    await asyncio.sleep(backoff_sec)
                    backoff_sec = min(backoff_sec * 2, 20)

    def stop(self):
        self._running = False

def get_total_exposure() -> float:
    """Approximate gross inventory notional in USDC."""
    exposure = 0.0
    for pos in state.open_positions.values():
        qty = abs(float(pos.get("qty", 0.0)))
        avg = max(0.01, float(pos.get("avg_price", 0.0)))
        exposure += qty * avg
    return round(exposure, 4)

def get_market_exposure(market_id: str) -> float:
    """Approximate per-market inventory notional in USDC."""
    pos = state.open_positions.get(market_id)
    if not pos:
        return 0.0
    qty = abs(float(pos.get("qty", 0.0)))
    avg = max(0.01, float(pos.get("avg_price", 0.0)))
    return round(qty * avg, 4)

def refresh_account_state():
    """Refresh balance/equity fields for paper-mode accounting."""
    if state.paper_mode:
        state.current_balance = round(state.starting_balance + state.realized_pnl, 4)
        # For now, equity mirrors realized balance until mark-to-market is added.
        state.equity = state.current_balance

# ─── Market Data ──────────────────────────────────────────────────────────────

async def get_btc_price(session: aiohttp.ClientSession) -> float:
    """Use websocket BTC feed when fresh, with periodic multi-exchange reference refresh."""
    async def fetch_binance() -> Optional[float]:
        try:
            async with session.get(
                f"{config.BINANCE_API}/api/v3/ticker/price",
                params={"symbol": "BTCUSDT"},
                timeout=aiohttp.ClientTimeout(total=4)
            ) as r:
                d = await r.json()
                return float(d["price"])
        except Exception:
            return None

    async def fetch_coinbase() -> Optional[float]:
        try:
            async with session.get(
                f"{config.COINBASE_API}/products/BTC-USD/ticker",
                timeout=aiohttp.ClientTimeout(total=4)
            ) as r:
                d = await r.json()
                return float(d["price"])
        except Exception:
            return None

    async def fetch_kraken() -> Optional[float]:
        try:
            async with session.get(
                f"{config.KRAKEN_API}/0/public/Ticker",
                params={"pair": "XBTUSD"},
                timeout=aiohttp.ClientTimeout(total=4)
            ) as r:
                d = await r.json()
                result = d.get("result", {})
                pair_key = next(iter(result.keys()), None)
                if not pair_key:
                    return None
                last_trade = result[pair_key].get("c", [])
                if not last_trade:
                    return None
                return float(last_trade[0])
        except Exception:
            return None

    now_ts = time.time()
    ws_fresh = state.ws_btc_price > 0 and (now_ts - state.ws_last_tick) <= 15
    refresh_due = (now_ts - state.reference_last_update) >= config.BTC_REFERENCE_REFRESH_SEC

    if refresh_due or state.reference_btc_price <= 0:
        prices = {}
        binance, coinbase, kraken = await asyncio.gather(
            fetch_binance(),
            fetch_coinbase(),
            fetch_kraken(),
        )
        if binance is not None:
            prices["binance"] = binance
        if coinbase is not None:
            prices["coinbase"] = coinbase
        if kraken is not None:
            prices["kraken"] = kraken
        if prices:
            state.reference_btc_price = float(statistics.median(prices.values()))
            state.reference_last_update = now_ts
            sources = ",".join(sorted(prices.keys()))
            log.info(f"BTC reference median from {sources}: ${state.reference_btc_price:,.2f}")

    if ws_fresh:
        price = state.ws_btc_price
    elif state.reference_btc_price > 0:
        price = state.reference_btc_price
    else:
        log.warning("BTC price unavailable from websocket/reference; using last known price")
        price = state.btc_price or 85000.0

    state.btc_price = price
    db.execute(
        """
        INSERT INTO btc_prices (ts, price)
        VALUES (%s, %s)
        ON CONFLICT (ts) DO UPDATE SET price = EXCLUDED.price
        """,
        (datetime.now(timezone.utc), price),
    )
    db.commit()
    return price

async def get_btc_markets(session: aiohttp.ClientSession) -> list[Market]:
    """
    Fetch active BTC prediction markets from Gamma API.
    These are markets with questions like "Will BTC be above $X on date?"
    """
    try:
        async with session.get(
            f"{config.GAMMA_API}/markets",
            params={
                "active": "true",
                "closed": "false",
                "tag_slug": "crypto",
                "limit": 500,
                "_order": "volume",
            },
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            markets_raw = await r.json()
    except Exception as e:
        log.error(f"Gamma API error: {e}")
        return []

    results = []
    for m in markets_raw:
        q = m.get("question", "").lower()
        # Filter for BTC price prediction markets
        if "bitcoin" not in q and "btc" not in q:
            continue
        if not m.get("active"):
            continue

        # Gamma API shape changed over time. Support both:
        # 1) legacy "tokens" objects
        # 2) modern "clobTokenIds" + "outcomes"/"outcomePrices"
        yes_token = ""
        no_token = ""
        tokens = m.get("tokens", [])
        if tokens and len(tokens) >= 2:
            yes_tok = next((t for t in tokens if t.get("outcome", "").lower() == "yes"), tokens[0])
            no_tok = next((t for t in tokens if t.get("outcome", "").lower() == "no"), tokens[1])
            yes_token = yes_tok.get("token_id", "")
            no_token = no_tok.get("token_id", "")
        else:
            clob_ids = m.get("clobTokenIds") or []
            outcomes_raw = m.get("outcomes")
            outcomes = []
            if isinstance(outcomes_raw, list):
                outcomes = [str(x).lower() for x in outcomes_raw]
            elif isinstance(outcomes_raw, str):
                try:
                    outcomes = [str(x).lower() for x in json.loads(outcomes_raw)]
                except Exception:
                    outcomes = []

            if len(clob_ids) >= 2:
                yes_idx = outcomes.index("yes") if "yes" in outcomes else 0
                no_idx = outcomes.index("no") if "no" in outcomes else (1 if yes_idx == 0 else 0)
                yes_token = str(clob_ids[yes_idx])
                no_token = str(clob_ids[no_idx])

        if not yes_token or not no_token:
            continue

        # Parse prices from outcomePrices field
        outcome_prices = m.get("outcomePrices", ["0.5", "0.5"])
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except Exception:
                outcome_prices = ["0.5", "0.5"]
        try:
            yes_price = float(outcome_prices[0])
            no_price  = float(outcome_prices[1])
        except (IndexError, ValueError):
            yes_price, no_price = 0.5, 0.5

        results.append(Market(
            condition_id=m.get("conditionId", m.get("id", "")),
            slug=m.get("slug", ""),
            event_slug=(m.get("events")[0].get("slug", "") if isinstance(m.get("events"), list) and m.get("events") else ""),
            question=m.get("question", ""),
            yes_token=yes_token,
            no_token=no_token,
            yes_price=yes_price,
            no_price=no_price,
            volume=float(m.get("volume", 0) or 0),
            liquidity=float(m.get("liquidity", 0) or 0),
            end_date_iso=m.get("endDate", ""),
            active=True
        ))

    # Sort by volume, take top N
    results.sort(key=lambda x: x.volume, reverse=True)
    state.active_markets = results[:config.MARKETS_WATCHED]
    return state.active_markets

async def get_order_book_mid(session: aiohttp.ClientSession, token_id: str) -> Optional[float]:
    """Get midpoint from CLOB order book."""
    try:
        async with session.get(
            f"{config.CLOB_API}/midpoint",
            params={"token_id": token_id},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            d = await r.json()
            mid = d.get("mid")
            if mid is not None:
                return float(mid)
    except Exception as e:
        log.debug(f"Midpoint fetch failed for {token_id[:16]}: {e}")
    return None

# ─── Fair Probability Calculator ──────────────────────────────────────────────

def extract_strike_from_question(question: str) -> Optional[float]:
    """
    Parse strike price from questions like:
    "Will Bitcoin be above $90,000 on Dec 31?"
    "BTC above $95k by end of month?"
    """
    import re
    # Match patterns like $90,000 or $95k or $90000
    patterns = [
        r'\$([0-9]{1,3}(?:,[0-9]{3})+)',  # $90,000
        r'\$([0-9]+)k\b',                  # $95k
        r'\$([0-9]+(?:\.[0-9]+)?)m\b',     # $1m, $1.5m
        r'\$([0-9]{4,6})\b',               # $90000
    ]
    for pat in patterns:
        m = re.search(pat, question, re.IGNORECASE)
        if m:
            val = m.group(1).replace(",", "")
            token = question[m.start():m.end()].lower()
            mult = 1
            if "k" in token:
                mult = 1000
            elif "m" in token:
                mult = 1_000_000
            return float(val) * mult
    return None

def calc_fair_probability(btc_price: float, strike: float, days_to_expiry: float) -> float:
    """
    Log-normal probability: P(BTC > strike at expiry).
    Uses annualised vol of ~80% (crypto standard).
    """
    if strike <= 0 or days_to_expiry <= 0 or btc_price <= 0:
        return 0.5

    sigma_annual = 0.80
    T = days_to_expiry / 365.0
    sigma_T = sigma_annual * math.sqrt(T)

    # Log-normal CDF: P(S_T > K)
    # = N(d2) where d2 = [ln(S/K) + (mu - 0.5*sigma^2)*T] / (sigma*sqrt(T))
    # Using risk-neutral drift mu=0 (prediction market, no drift assumption)
    from_log = math.log(btc_price / strike)
    d2 = (from_log - 0.5 * sigma_T**2) / sigma_T

    # Standard normal CDF approximation
    def norm_cdf(x: float) -> float:
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    return norm_cdf(d2)

def get_days_to_expiry(end_date_iso: str) -> float:
    """Calculate days until market expiry."""
    try:
        end = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = (end - now).total_seconds() / 86400
        return max(delta, 0.1)
    except Exception:
        return 7.0  # Default 1 week

# ─── Quoting Logic ────────────────────────────────────────────────────────────

async def compute_quote(session: aiohttp.ClientSession, market: Market) -> Optional[Quote]:
    """
    Compute a two-sided quote for a BTC market.
    Only quotes if edge >= MIN_EDGE vs current midpoint.
    """
    btc = state.btc_price
    if btc <= 0:
        return None

    strike = extract_strike_from_question(market.question)
    if strike is None:
        log.debug(f"Could not parse strike from: {market.question}")
        return None

    dte = get_days_to_expiry(market.end_date_iso)
    fair = calc_fair_probability(btc, strike, dte)

    # Clamp to valid range
    fair = max(0.02, min(0.98, fair))

    # Get current market midpoint from CLOB
    mid = await get_order_book_mid(session, market.yes_token)
    if mid is None:
        mid = market.yes_price  # Fall back to Gamma price

    half_spread = config.SPREAD_PCT / 2.0
    bid = round(fair - half_spread, 2)
    ask = round(fair + half_spread, 2)

    # Clamp to valid tick range
    bid = max(0.01, min(0.99, bid))
    ask = max(0.01, min(0.99, ask))
    if bid >= ask:
        return None

    # Edge = how much our fair price beats the current mid
    edge = abs(fair - mid)
    should_place = (
        edge >= config.MIN_EDGE
        and dte > 0.5          # Don't trade within 12hrs of expiry
        and market.liquidity > 1000  # Require at least $1k liquidity
        and not state.kill_switch
    )

    q = Quote(
        market=market,
        fair_price=fair,
        mid=mid,
        bid=bid,
        ask=ask,
        edge=edge,
        should_place=should_place,
    )

    # Log to DB
    db.execute(
        """INSERT INTO quotes (ts,market_id,bid,ask,fair_price,mid,edge,placed)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        (datetime.now(timezone.utc),
         market.condition_id, bid, ask, fair, mid, edge, int(should_place))
    )
    db.commit()

    return q

# ─── Order Placement (Authenticated) ──────────────────────────────────────────

def get_live_client():
    global live_client
    if live_client is not None:
        return live_client
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON

    live_client = ClobClient(
        host=config.CLOB_API,
        chain_id=POLYGON,
        key=config.PRIVATE_KEY,
        creds={
            "api_key": config.API_KEY,
            "api_secret": config.API_SECRET,
            "api_passphrase": config.API_PASSPHRASE,
        }
    )
    return live_client

async def place_order(
    session: aiohttp.ClientSession,
    token_id: str,
    side: str,        # "BUY" or "SELL"
    price: float,
    size: float,
    market: Market,
    edge: float = 0.0,
    mid: Optional[float] = None,
) -> Optional[str]:
    """
    Place a limit order via the CLOB API.
    Requires POLY_PRIVATE_KEY, POLY_API_KEY, POLY_API_SECRET, POLY_PASSPHRASE
    to be set in environment variables.

    In PAPER TRADING mode (no keys set), this simulates order placement.
    """
    paper_mode = not is_live_mode()
    state.paper_mode = paper_mode

    order_id = f"sim_{int(time.time()*1000)}_{side[:1]}"
    notional_usdc = float(size)
    reference_price = mid if mid is not None else price
    size_shares = notional_to_shares(notional_usdc, reference_price)
    if size_shares < config.MIN_ORDER_SHARES:
        log.debug(
            f"Skip tiny order size_shares={size_shares:.6f} below minimum {config.MIN_ORDER_SHARES:.6f}"
        )
        return None

    def clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    def apply_fill_pnl(market_id: str, fill_side: str, fill_price: float, shares: float) -> float:
        """
        Inventory accounting on YES shares:
        shares = usdc_notional / price
        realized PnL appears when reducing an existing long/short.
        """
        qty = shares
        pos = state.open_positions.get(market_id, {"qty": 0.0, "avg_price": 0.0})
        cur_qty = float(pos["qty"])
        cur_avg = float(pos["avg_price"])
        realized = 0.0

        if fill_side == "BUY":
            if cur_qty >= 0:
                new_qty = cur_qty + qty
                new_avg = ((cur_avg * cur_qty) + (fill_price * qty)) / new_qty if new_qty else 0.0
                pos["qty"], pos["avg_price"] = new_qty, new_avg
            else:
                close_qty = min(qty, abs(cur_qty))
                realized += (cur_avg - fill_price) * close_qty
                remaining_buy = qty - close_qty
                new_qty = cur_qty + close_qty
                if abs(new_qty) < 1e-9:
                    if remaining_buy > 0:
                        pos["qty"], pos["avg_price"] = remaining_buy, fill_price
                    else:
                        pos["qty"], pos["avg_price"] = 0.0, 0.0
                else:
                    pos["qty"] = new_qty
                if remaining_buy > 0 and new_qty > 0:
                    pos["qty"], pos["avg_price"] = remaining_buy, fill_price
        else:  # SELL
            if cur_qty <= 0:
                short_qty = abs(cur_qty)
                new_short = short_qty + qty
                new_avg = ((cur_avg * short_qty) + (fill_price * qty)) / new_short if new_short else 0.0
                pos["qty"], pos["avg_price"] = -new_short, new_avg
            else:
                close_qty = min(qty, cur_qty)
                realized += (fill_price - cur_avg) * close_qty
                remaining_sell = qty - close_qty
                new_qty = cur_qty - close_qty
                if abs(new_qty) < 1e-9:
                    if remaining_sell > 0:
                        pos["qty"], pos["avg_price"] = -remaining_sell, fill_price
                    else:
                        pos["qty"], pos["avg_price"] = 0.0, 0.0
                else:
                    pos["qty"] = new_qty
                if remaining_sell > 0 and new_qty < 0:
                    pos["qty"], pos["avg_price"] = -remaining_sell, fill_price

        if abs(pos["qty"]) < 1e-9:
            state.open_positions.pop(market_id, None)
        else:
            state.open_positions[market_id] = pos
        return round(realized, 4)

    if paper_mode:
        ref_mid = mid if mid is not None else market.yes_price
        dist = (ref_mid - price) if side == "BUY" else (price - ref_mid)
        fill_prob = clamp(config.PAPER_FILL_BASE + (edge * 5.0) - (max(0.0, dist) * 1.8), 0.05, 0.90)
        is_filled = random.random() < fill_prob

        status = "filled" if is_filled else "cancelled"
        fill_price = price if is_filled else None
        trade_pnl = 0.0
        if is_filled:
            trade_pnl = apply_fill_pnl(market.condition_id, side, price, size_shares)
            state.realized_pnl += trade_pnl
            refresh_account_state()

        log.info(
            f"[PAPER] {side} ${notional_usdc:.2f} ({size_shares:.4f} shares) "
            f"{market.question[:40]}... @ {price:.3f} status={status} fill_prob={fill_prob:.2f} pnl={trade_pnl:+.2f}"
        )
        state.total_trades += 1
        db.execute(
            """INSERT INTO trades (ts,market_id,market_slug,event_slug,market_q,mode,side,price,size,notional_usdc,size_shares,order_id,status,pnl,fill_price)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (datetime.now(timezone.utc),
             market.condition_id, market.slug, market.event_slug, market.question,
             "paper", side, price, notional_usdc, notional_usdc, size_shares, order_id, status, trade_pnl, fill_price)
        )
        db.commit()
        return order_id

    # --- Live trading via py-clob-client ---
    # Install: pip install py-clob-client
    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        client = get_live_client()

        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size_shares,
            side=side,
            order_type=OrderType.GTC,
        )
        resp = client.create_and_post_order(order_args)
        real_id = resp.get("orderID", order_id)

        state.total_trades += 1
        db.execute(
            """INSERT INTO trades (ts,market_id,market_slug,event_slug,market_q,mode,side,price,size,notional_usdc,size_shares,order_id,status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (datetime.now(timezone.utc),
             market.condition_id, market.slug, market.event_slug, market.question,
             "live", side, price, notional_usdc, notional_usdc, size_shares, real_id, "open")
        )
        db.commit()
        log.info(
            f"[LIVE] {side} ${notional_usdc:.2f} ({size_shares:.4f} shares) "
            f"@ {price:.3f} order_id={real_id[:16]}"
        )
        return real_id

    except ImportError:
        log.critical("py-clob-client not installed. Live trading cannot run.")
        if is_live_mode():
            state.kill_switch = True
        return None
    except Exception as e:
        log.error(f"Order placement failed: {e}")
        state.errors.append(f"{datetime.now().isoformat()} | {e}")
        return None

# ─── Risk / Kill Switch ────────────────────────────────────────────────────────

def cancel_live_orders_best_effort() -> int:
    """
    Try to cancel stale live orders before re-quoting.
    Uses whichever client methods are available in installed py-clob-client version.
    """
    if not is_live_mode():
        return 0
    try:
        client = get_live_client()

        for method_name in ("cancel_all", "cancel_all_orders"):
            method = getattr(client, method_name, None)
            if callable(method):
                resp = method()
                if isinstance(resp, dict):
                    count = int(resp.get("cancelled") or resp.get("count") or 0)
                elif isinstance(resp, list):
                    count = len(resp)
                else:
                    count = 0
                return count

        get_orders = getattr(client, "get_orders", None)
        cancel = getattr(client, "cancel", None)
        if callable(get_orders) and callable(cancel):
            orders = get_orders() or []
            cancelled = 0
            for order in orders:
                oid = None
                if isinstance(order, dict):
                    oid = order.get("orderID") or order.get("id")
                if not oid:
                    continue
                try:
                    cancel(oid)
                    cancelled += 1
                except Exception:
                    continue
            return cancelled
    except Exception as e:
        log.warning(f"Unable to cancel stale live orders before re-quote: {e}")
    return 0

def check_kill_switch():
    """Halt trading if daily loss exceeds threshold."""
    if state.daily_pnl < -config.MAX_DAILY_LOSS:
        if not state.kill_switch:
            log.critical(f"KILL SWITCH: daily PnL {state.daily_pnl:.2f} < -{config.MAX_DAILY_LOSS}")
            state.kill_switch = True

def update_stats():
    """Persist bot stats snapshot to DB."""
    state.daily_pnl = compute_daily_pnl()
    refresh_account_state()
    db.execute(
        """INSERT INTO bot_stats
           (ts,total_trades,open_positions,realized_pnl,unrealized_pnl,daily_pnl,balance,active_markets)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (ts) DO UPDATE
           SET total_trades = EXCLUDED.total_trades,
               open_positions = EXCLUDED.open_positions,
               realized_pnl = EXCLUDED.realized_pnl,
               unrealized_pnl = EXCLUDED.unrealized_pnl,
               daily_pnl = EXCLUDED.daily_pnl,
               balance = EXCLUDED.balance,
               active_markets = EXCLUDED.active_markets""",
        (
            datetime.now(timezone.utc),
            state.total_trades,
            len(state.open_positions),
            state.realized_pnl,
            0.0,
            state.daily_pnl,
            state.equity,
            len(state.active_markets),
        )
    )
    db.commit()

# ─── Main Loop ────────────────────────────────────────────────────────────────

async def main_loop():
    log.info("=" * 60)
    log.info("  Polymarket Market Making Bot  |  Starting up...")
    log.info("=" * 60)
    validate_runtime_config()
    state.paper_mode = not is_live_mode()
    state.starting_balance = config.PAPER_INITIAL_BALANCE
    refresh_account_state()
    log.info(
        f"Config: spread={config.SPREAD_PCT*100:.1f}%  min_edge={config.MIN_EDGE*100:.1f}%  "
        f"notional=${config.ORDER_SIZE} mode={'paper' if state.paper_mode else 'live'} "
        f"starting_balance=${state.starting_balance:.2f}"
    )

    ws_feed = BinanceBtcWsFeed()
    ws_task = asyncio.create_task(ws_feed.run())
    async with aiohttp.ClientSession() as session:
        cycle = 0
        while state.running:
            try:
                cycle_started = time.perf_counter()
                cycle += 1
                state.last_cycle = datetime.now().strftime("%H:%M:%S")
                log.info(f"─── Cycle {cycle} ───────────────────────────────────────")

                # 1. Refresh BTC price
                btc = await get_btc_price(session)
                btc_src = "ws" if (time.time() - state.ws_last_tick) <= 15 and state.ws_btc_price > 0 else "ref"
                log.info(f"BTC/USDT: ${btc:,.2f} ({btc_src})")

                # 2. Fetch active BTC markets
                markets = await get_btc_markets(session)
                log.info(f"Found {len(markets)} active BTC markets")

                # 3. Check kill switch
                check_kill_switch()
                placed_count = 0
                if state.kill_switch:
                    log.warning("Kill switch active — skipping order placement")
                else:
                    if is_live_mode() and config.CANCEL_BEFORE_REQUOTE:
                        cancelled = cancel_live_orders_best_effort()
                        if cancelled:
                            log.info(f"Cancelled {cancelled} stale live orders before re-quote")
                    # 4. Quote each market
                    for market in markets:
                        quote = await compute_quote(session, market)
                        if quote is None:
                            continue

                        log.info(
                            f"  {market.question[:55]:55s}  "
                            f"fair={quote.fair_price:.3f}  "
                            f"mid={quote.mid:.3f}  "
                            f"edge={quote.edge:.3f}  "
                            f"{'✓ QUOTE' if quote.should_place else '✗ skip'}"
                        )

                        if quote.should_place:
                            if state.paper_mode:
                                exposure = get_total_exposure()
                                projected = exposure + (2.0 * config.ORDER_SIZE)
                                if projected > state.equity:
                                    log.warning(
                                        f"Paper balance guard: projected exposure ${projected:.2f} > equity ${state.equity:.2f}. "
                                        "Skipping new quote."
                                    )
                                    continue
                            market_exposure = get_market_exposure(market.condition_id)
                            projected_market = market_exposure + config.ORDER_SIZE
                            if projected_market > config.MAX_POSITION:
                                log.info(
                                    f"Max position guard for market {market.condition_id[:12]}: "
                                    f"${projected_market:.2f} > ${config.MAX_POSITION:.2f}, skipping quote"
                                )
                                continue
                            if placed_count >= config.MAX_ORDERS_PER_CYCLE:
                                log.info(
                                    f"Max orders per cycle reached ({config.MAX_ORDERS_PER_CYCLE}), stopping quote placement"
                                )
                                break
                            # Place bid + ask
                            await place_order(
                                session,
                                market.yes_token,
                                "BUY",
                                quote.bid,
                                config.ORDER_SIZE,
                                market,
                                edge=quote.edge,
                                mid=quote.mid,
                            )
                            await place_order(
                                session,
                                market.yes_token,
                                "SELL",
                                quote.ask,
                                config.ORDER_SIZE,
                                market,
                                edge=quote.edge,
                                mid=quote.mid,
                            )
                            placed_count += 1
                            await asyncio.sleep(0.3)  # Rate limit

                    log.info(f"Placed quotes on {placed_count} markets this cycle")
                    if state.paper_mode:
                        log.info(
                            f"Paper account: start=${state.starting_balance:.2f} "
                            f"equity=${state.equity:.2f} realized={state.realized_pnl:+.2f} exposure=${get_total_exposure():.2f}"
                        )

                state.orders_placed_cycle = placed_count
                state.cycle_latency_ms = round((time.perf_counter() - cycle_started) * 1000.0, 2)
                log.info(
                    f"Cycle telemetry: orders={state.orders_placed_cycle} latency_ms={state.cycle_latency_ms:.2f}"
                )

                # 5. Update stats
                update_stats()
                update_runtime_state()

                # 6. Wait for next cycle
                log.info(f"Sleeping {config.QUOTE_REFRESH_SEC}s until next cycle...")
                await asyncio.sleep(config.QUOTE_REFRESH_SEC)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception(f"Cycle error: {e}")
                state.errors.append(f"{datetime.now().isoformat()} | Cycle error: {e}")
                await asyncio.sleep(10)

    ws_feed.stop()
    ws_task.cancel()
    try:
        await ws_task
    except Exception:
        pass
    log.info("Bot stopped.")

def compute_daily_pnl() -> float:
    row = db.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE status='filled' AND ts > (NOW() - INTERVAL '1 day')"
    ).fetchone()
    return float(row[0] or 0.0)

def update_runtime_state():
    now_ts = time.time()
    ws_age = (now_ts - state.ws_last_tick) if state.ws_last_tick > 0 else 9999.0
    btc_source = "ws" if (state.ws_connected and ws_age <= 15 and state.ws_btc_price > 0) else "ref"
    db.execute(
        """INSERT INTO runtime_state
           (id,updated_at,running,kill_switch,paper_mode,btc_price,btc_source,ws_connected,ws_tick_age_sec,cycle_latency_ms,orders_placed_cycle,last_cycle,errors)
           VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (id) DO UPDATE
           SET updated_at = EXCLUDED.updated_at,
               running = EXCLUDED.running,
               kill_switch = EXCLUDED.kill_switch,
               paper_mode = EXCLUDED.paper_mode,
               btc_price = EXCLUDED.btc_price,
               btc_source = EXCLUDED.btc_source,
               ws_connected = EXCLUDED.ws_connected,
               ws_tick_age_sec = EXCLUDED.ws_tick_age_sec,
               cycle_latency_ms = EXCLUDED.cycle_latency_ms,
               orders_placed_cycle = EXCLUDED.orders_placed_cycle,
               last_cycle = EXCLUDED.last_cycle,
               errors = EXCLUDED.errors""",
        (
            datetime.now(timezone.utc),
            int(state.running),
            int(state.kill_switch),
            int(state.paper_mode),
            state.btc_price,
            btc_source,
            int(state.ws_connected),
            float(round(ws_age, 3)),
            float(state.cycle_latency_ms),
            int(state.orders_placed_cycle),
            state.last_cycle,
            len(state.errors),
        ),
    )
    db.commit()

if __name__ == "__main__":
    asyncio.run(main_loop())
