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
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from typing import Optional
import aiohttp
import psycopg
from pathlib import Path

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

@dataclass
class Config:
    # API endpoints
    GAMMA_API: str = "https://gamma-api.polymarket.com"
    CLOB_API: str  = "https://clob.polymarket.com"
    BINANCE_API: str = "https://api.binance.com"

    # Trading parameters
    SPREAD_PCT: float = 0.04       # Quote ±2% around fair price (captures 4% spread)
    MIN_EDGE: float   = 0.015      # Minimum edge over midpoint to place order
    ORDER_SIZE: float = 50.0       # USDC per side per quote
    MAX_POSITION: float = 500.0    # Max USDC in any single market
    QUOTE_REFRESH_SEC: int = 30    # How often to refresh quotes
    MARKETS_WATCHED: int = 10      # How many BTC markets to watch
    PAPER_FILL_BASE: float = 0.20  # Base fill probability in paper mode
    PAPER_INITIAL_BALANCE: float = field(default_factory=lambda: env_float("POLY_PAPER_INITIAL_BALANCE", 500.0))  # Paper account starting balance (USDC)
    FORCE_PAPER: bool = field(default_factory=lambda: env_bool("POLY_FORCE_PAPER", True))

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
            side        TEXT    NOT NULL,
            price       REAL    NOT NULL,
            size        REAL    NOT NULL,
            order_id    TEXT,
            status      TEXT    DEFAULT 'open',
            pnl         REAL    DEFAULT 0,
            fill_price  REAL
        )
    """)
    con.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS market_slug TEXT")
    con.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS event_slug TEXT")
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
            last_cycle      TEXT DEFAULT '',
            errors          INTEGER DEFAULT 0
        )
    """)
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

state = BotState()

def get_total_exposure() -> float:
    """Approximate gross inventory notional in USDC."""
    exposure = 0.0
    for pos in state.open_positions.values():
        qty = abs(float(pos.get("qty", 0.0)))
        avg = max(0.01, float(pos.get("avg_price", 0.0)))
        exposure += qty * avg
    return round(exposure, 4)

def refresh_account_state():
    """Refresh balance/equity fields for paper-mode accounting."""
    if state.paper_mode:
        state.current_balance = round(state.starting_balance + state.realized_pnl, 4)
        # For now, equity mirrors realized balance until mark-to-market is added.
        state.equity = state.current_balance

# ─── Market Data ──────────────────────────────────────────────────────────────

async def get_btc_price(session: aiohttp.ClientSession) -> float:
    """Fetch spot BTC/USDT from Binance."""
    try:
        async with session.get(
            f"{config.BINANCE_API}/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            d = await r.json()
            price = float(d["price"])
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
    except Exception as e:
        log.warning(f"BTC price fetch failed: {e}")
        return state.btc_price or 85000.0

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
    paper_mode = config.FORCE_PAPER or not config.PRIVATE_KEY
    state.paper_mode = paper_mode

    order_id = f"sim_{int(time.time()*1000)}_{side[:1]}"

    def clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    def apply_fill_pnl(market_id: str, fill_side: str, fill_price: float, usdc_size: float) -> float:
        """
        Inventory accounting on YES shares:
        shares = usdc_notional / price
        realized PnL appears when reducing an existing long/short.
        """
        qty = usdc_size / max(fill_price, 0.01)
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
            trade_pnl = apply_fill_pnl(market.condition_id, side, price, size)
            state.realized_pnl += trade_pnl
            refresh_account_state()

        log.info(
            f"[PAPER] {side} {size:.1f} {market.question[:40]}... @ {price:.3f} "
            f"status={status} fill_prob={fill_prob:.2f} pnl={trade_pnl:+.2f}"
        )
        state.total_trades += 1
        db.execute(
            """INSERT INTO trades (ts,market_id,market_slug,event_slug,market_q,side,price,size,order_id,status,pnl,fill_price)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (datetime.now(timezone.utc),
             market.condition_id, market.slug, market.event_slug, market.question,
             side, price, size, order_id, status, trade_pnl, fill_price)
        )
        db.commit()
        return order_id

    # --- Live trading via py-clob-client ---
    # Install: pip install py-clob-client
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.constants import POLYGON

        client = ClobClient(
            host=config.CLOB_API,
            chain_id=POLYGON,
            key=config.PRIVATE_KEY,
            creds={
                "api_key": config.API_KEY,
                "api_secret": config.API_SECRET,
                "api_passphrase": config.API_PASSPHRASE,
            }
        )

        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=side,
            order_type=OrderType.GTC,
        )
        resp = client.create_and_post_order(order_args)
        real_id = resp.get("orderID", order_id)

        state.total_trades += 1
        db.execute(
            """INSERT INTO trades (ts,market_id,market_slug,event_slug,market_q,side,price,size,order_id,status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (datetime.now(timezone.utc),
             market.condition_id, market.slug, market.event_slug, market.question,
             side, price, size, real_id, "open")
        )
        db.commit()
        log.info(f"[LIVE] {side} {size:.1f} @ {price:.3f} order_id={real_id[:16]}")
        return real_id

    except ImportError:
        log.warning("py-clob-client not installed. Run: pip install py-clob-client")
        return None
    except Exception as e:
        log.error(f"Order placement failed: {e}")
        state.errors.append(f"{datetime.now().isoformat()} | {e}")
        return None

# ─── Risk / Kill Switch ────────────────────────────────────────────────────────

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
    state.paper_mode = config.FORCE_PAPER or not bool(config.PRIVATE_KEY)
    state.starting_balance = config.PAPER_INITIAL_BALANCE
    refresh_account_state()
    log.info(
        f"Config: spread={config.SPREAD_PCT*100:.1f}%  min_edge={config.MIN_EDGE*100:.1f}%  "
        f"size=${config.ORDER_SIZE}  paper={state.paper_mode}  starting_balance=${state.starting_balance:.2f}"
    )

    async with aiohttp.ClientSession() as session:
        cycle = 0
        while state.running:
            try:
                cycle += 1
                state.last_cycle = datetime.now().strftime("%H:%M:%S")
                log.info(f"─── Cycle {cycle} ───────────────────────────────────────")

                # 1. Refresh BTC price
                btc = await get_btc_price(session)
                log.info(f"BTC/USDT: ${btc:,.2f}")

                # 2. Fetch active BTC markets
                markets = await get_btc_markets(session)
                log.info(f"Found {len(markets)} active BTC markets")

                # 3. Check kill switch
                check_kill_switch()
                if state.kill_switch:
                    log.warning("Kill switch active — skipping order placement")
                else:
                    # 4. Quote each market
                    placed_count = 0
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
                                projected = exposure + config.ORDER_SIZE
                                if projected > state.equity:
                                    log.warning(
                                        f"Paper balance guard: projected exposure ${projected:.2f} > equity ${state.equity:.2f}. "
                                        "Skipping new quote."
                                    )
                                    continue
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
                    log.info(
                        f"Paper account: start=${state.starting_balance:.2f} "
                        f"equity=${state.equity:.2f} realized={state.realized_pnl:+.2f} exposure=${get_total_exposure():.2f}"
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

    log.info("Bot stopped.")

def compute_daily_pnl() -> float:
    row = db.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE status='filled' AND ts > (NOW() - INTERVAL '1 day')"
    ).fetchone()
    return float(row[0] or 0.0)

def update_runtime_state():
    db.execute(
        """INSERT INTO runtime_state
           (id,updated_at,running,kill_switch,paper_mode,btc_price,last_cycle,errors)
           VALUES (1,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (id) DO UPDATE
           SET updated_at = EXCLUDED.updated_at,
               running = EXCLUDED.running,
               kill_switch = EXCLUDED.kill_switch,
               paper_mode = EXCLUDED.paper_mode,
               btc_price = EXCLUDED.btc_price,
               last_cycle = EXCLUDED.last_cycle,
               errors = EXCLUDED.errors""",
        (
            datetime.now(timezone.utc),
            int(state.running),
            int(state.kill_switch),
            int(state.paper_mode),
            state.btc_price,
            state.last_cycle,
            len(state.errors),
        ),
    )
    db.commit()

if __name__ == "__main__":
    asyncio.run(main_loop())
