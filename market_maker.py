"""
Polymarket Market Making Bot
============================
Strategy: BTC-linked binary prediction markets
- Reads BTC price from Binance websocket with multi-exchange reference fallback
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
import statistics
import re
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from typing import Dict, Optional
import aiohttp
import psycopg
from pathlib import Path
from dotenv import load_dotenv
from db_schema import apply_schema

load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────

def env_value(name: str, default, parser):
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return parser(raw)
    except (TypeError, ValueError):
        return default

def env_bool(name: str, default: bool) -> bool:
    return env_value(name, default, lambda v: v.strip().lower() in {"1", "true", "yes", "on"})

def env_float(name: str, default: float) -> float:
    return env_value(name, default, float)

def env_int(name: str, default: int) -> int:
    return env_value(name, default, int)

STRATEGY_PRESETS: dict[str, dict[str, float | int]] = {
    # Lower risk / slower turnover.
    "conservative": {
        "POLY_SPREAD_PCT": 0.05,
        "POLY_MIN_EDGE": 0.02,
        "POLY_ORDER_SIZE": 30.0,
        "POLY_MAX_POSITION": 300.0,
        "POLY_QUOTE_REFRESH_SEC": 40,
        "POLY_MARKETS_WATCHED": 8,
        "POLY_MARKETS_FETCH_LIMIT": 800,
        "POLY_MIN_MARKET_LIQUIDITY": 2000.0,
        "POLY_MAX_DAILY_LOSS": 120.0,
        "POLY_MAX_OPEN_ORDERS": 30,
        "POLY_INVENTORY_SOFT_LIMIT_PCT": 0.50,
        "POLY_INVENTORY_HARD_LIMIT_PCT": 0.75,
        "POLY_INVENTORY_SKEW_PCT": 0.015,
        "POLY_PASSIVE_BUFFER_TICKS": 2,
        "POLY_MIN_MID_DISTANCE_PCT": 0.008,
        "POLY_MAX_MID_DISTANCE_PCT": 0.25,
        "POLY_MAX_ORDERS_PER_CYCLE": 14,
    },
    # Current default behavior.
    "balanced": {},
    # Target-wallet style: broad market coverage, moderate spread, fast cadence.
    "target_clone": {
        "POLY_SPREAD_PCT": 0.035,
        "POLY_MIN_EDGE": 0.012,
        "POLY_ORDER_SIZE": 60.0,
        "POLY_MAX_POSITION": 700.0,
        "POLY_QUOTE_REFRESH_SEC": 20,
        "POLY_MARKETS_WATCHED": 18,
        "POLY_MARKETS_FETCH_LIMIT": 2000,
        "POLY_MIN_MARKET_LIQUIDITY": 1000.0,
        "POLY_MAX_DAILY_LOSS": 350.0,
        "POLY_MAX_OPEN_ORDERS": 70,
        "POLY_INVENTORY_SOFT_LIMIT_PCT": 0.65,
        "POLY_INVENTORY_HARD_LIMIT_PCT": 0.92,
        "POLY_INVENTORY_SKEW_PCT": 0.01,
        "POLY_PASSIVE_BUFFER_TICKS": 1,
        "POLY_MIN_MID_DISTANCE_PCT": 0.004,
        "POLY_MAX_MID_DISTANCE_PCT": 0.30,
        "POLY_MAX_ORDERS_PER_CYCLE": 28,
    },
    # Higher throughput / higher inventory risk.
    "aggressive": {
        "POLY_SPREAD_PCT": 0.03,
        "POLY_MIN_EDGE": 0.01,
        "POLY_ORDER_SIZE": 75.0,
        "POLY_MAX_POSITION": 900.0,
        "POLY_QUOTE_REFRESH_SEC": 20,
        "POLY_MARKETS_WATCHED": 20,
        "POLY_MARKETS_FETCH_LIMIT": 2000,
        "POLY_MIN_MARKET_LIQUIDITY": 800.0,
        "POLY_MAX_DAILY_LOSS": 450.0,
        "POLY_MAX_OPEN_ORDERS": 80,
        "POLY_INVENTORY_SOFT_LIMIT_PCT": 0.70,
        "POLY_INVENTORY_HARD_LIMIT_PCT": 0.95,
        "POLY_INVENTORY_SKEW_PCT": 0.008,
        "POLY_PASSIVE_BUFFER_TICKS": 1,
        "POLY_MIN_MID_DISTANCE_PCT": 0.003,
        "POLY_MAX_MID_DISTANCE_PCT": 0.35,
        "POLY_MAX_ORDERS_PER_CYCLE": 36,
    },
}

def selected_profile() -> str:
    raw = (os.getenv("POLY_STRATEGY_PROFILE", "balanced") or "").strip().lower()
    return raw if raw in STRATEGY_PRESETS else "balanced"

def env_float_profile(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is not None:
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default
    profile_defaults = STRATEGY_PRESETS.get(selected_profile(), {})
    if name in profile_defaults:
        try:
            return float(profile_defaults[name])
        except (TypeError, ValueError):
            return default
    return default

def env_int_profile(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return default
    profile_defaults = STRATEGY_PRESETS.get(selected_profile(), {})
    if name in profile_defaults:
        try:
            return int(profile_defaults[name])
        except (TypeError, ValueError):
            return default
    return default


def normalize_private_key(raw: str) -> str:
    """Strip .env junk (quotes, whitespace) so eth_account accepts the key."""
    s = (raw or "").strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1].strip()
    for ch in ("\ufeff", "\u200b", "\u200c", "\u200d"):
        s = s.replace(ch, "")
    return s.strip()


def assert_valid_hex_private_key(s: str) -> None:
    """ethereum keys: 32 bytes = 64 hex chars, optional 0x prefix."""
    body = s[2:] if s.startswith(("0x", "0X")) else s
    if len(body) != 64:
        raise RuntimeError(
            "POLY_PRIVATE_KEY must be exactly 64 hex characters (or 0x + 64 hex). "
            f"After trimming quotes/whitespace, got {len(body)} hex characters."
        )
    try:
        int(body, 16)
    except ValueError as e:
        raise RuntimeError(
            "POLY_PRIVATE_KEY contains invalid hex (wrong character or stray spaces). "
            "Use only 0-9 and a-f, no spaces inside the key."
        ) from e


@dataclass
class Config:
    STRATEGY_PROFILE: str = field(default_factory=selected_profile)
    # API endpoints
    GAMMA_API: str = "https://gamma-api.polymarket.com"
    CLOB_API: str  = "https://clob.polymarket.com"
    BINANCE_API: str = "https://api.binance.com"
    COINBASE_API: str = "https://api.exchange.coinbase.com"
    KRAKEN_API: str = "https://api.kraken.com"
    BINANCE_WS: str = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    BTC_REFERENCE_REFRESH_SEC: int = field(default_factory=lambda: env_int("POLY_BTC_REFERENCE_REFRESH_SEC", 120))

    # Trading parameters
    SPREAD_PCT: float = field(default_factory=lambda: env_float_profile("POLY_SPREAD_PCT", 0.04))         # Quote ±2% around fair price (captures 4% spread)
    MIN_EDGE: float   = field(default_factory=lambda: env_float_profile("POLY_MIN_EDGE", 0.015))          # Minimum edge over midpoint to place order
    ORDER_SIZE: float = field(default_factory=lambda: env_float_profile("POLY_ORDER_SIZE", 50.0))         # USDC notional per side per quote
    MAX_POSITION: float = field(default_factory=lambda: env_float_profile("POLY_MAX_POSITION", 500.0))    # Max USDC in any single market
    QUOTE_REFRESH_SEC: int = field(default_factory=lambda: env_int_profile("POLY_QUOTE_REFRESH_SEC", 30)) # How often to refresh quotes
    MARKETS_WATCHED: int = field(default_factory=lambda: env_int_profile("POLY_MARKETS_WATCHED", 10))     # How many BTC markets to watch
    MARKETS_FETCH_LIMIT: int = field(default_factory=lambda: env_int_profile("POLY_MARKETS_FETCH_LIMIT", 1000)) # Raw market fetch size before filtering
    MIN_MARKET_LIQUIDITY: float = field(default_factory=lambda: env_float_profile("POLY_MIN_MARKET_LIQUIDITY", 1000.0)) # Minimum market liquidity to quote
    ENABLE_LIVE_TRADING: bool = field(default_factory=lambda: env_bool("POLY_ENABLE_LIVE_TRADING", False))
    MIN_ORDER_SHARES: float = field(default_factory=lambda: env_float("POLY_MIN_ORDER_SHARES", 5.0))
    MAX_ORDERS_PER_CYCLE: int = field(default_factory=lambda: env_int_profile("POLY_MAX_ORDERS_PER_CYCLE", 20))
    CANCEL_BEFORE_REQUOTE: bool = field(default_factory=lambda: env_bool("POLY_CANCEL_BEFORE_REQUOTE", True))
    ANCHOR_EVENT_HAZARD_PER_DAY: float = field(default_factory=lambda: env_float("POLY_ANCHOR_EVENT_HAZARD_PER_DAY", 0.001))
    GTA_RELEASE_HAZARD_PER_DAY: float = field(default_factory=lambda: env_float("POLY_GTA_RELEASE_HAZARD_PER_DAY", 0.0002))

    # Risk
    MAX_DAILY_LOSS: float = field(default_factory=lambda: env_float_profile("POLY_MAX_DAILY_LOSS", 200.0)) # Kill switch: stop if daily PnL < -$200
    MAX_OPEN_ORDERS: int = field(default_factory=lambda: env_int_profile("POLY_MAX_OPEN_ORDERS", 40))      # Cancel all if exceeded
    INVENTORY_SOFT_LIMIT_PCT: float = field(default_factory=lambda: env_float_profile("POLY_INVENTORY_SOFT_LIMIT_PCT", 0.60)) # One-sided quoting starts beyond this ratio
    INVENTORY_HARD_LIMIT_PCT: float = field(default_factory=lambda: env_float_profile("POLY_INVENTORY_HARD_LIMIT_PCT", 0.90)) # Strictly block risk-increasing side
    INVENTORY_SKEW_PCT: float = field(default_factory=lambda: env_float_profile("POLY_INVENTORY_SKEW_PCT", 0.01))             # Midpoint skew to rebalance inventory
    PASSIVE_BUFFER_TICKS: int = field(default_factory=lambda: env_int_profile("POLY_PASSIVE_BUFFER_TICKS", 1))               # Keep quotes at least N ticks away from opposite top-of-book
    MIN_MID_DISTANCE_PCT: float = field(default_factory=lambda: env_float_profile("POLY_MIN_MID_DISTANCE_PCT", 0.005))       # Minimum bid/ask distance from mid to avoid crossing/taking
    MAX_MID_DISTANCE_PCT: float = field(default_factory=lambda: env_float_profile("POLY_MAX_MID_DISTANCE_PCT", 0.30))        # Maximum distance from mid (too far quotes are clipped)

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

def validate_runtime_config():
    """This codebase runs CLOB orders only; paper simulation has been removed."""
    if not config.ENABLE_LIVE_TRADING:
        raise RuntimeError(
            "POLY_ENABLE_LIVE_TRADING=1 is required. This bot places real Polymarket CLOB orders."
        )
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
        raise RuntimeError(f"Missing required CLOB credentials: {', '.join(missing)}")

    config.PRIVATE_KEY = normalize_private_key(config.PRIVATE_KEY)
    assert_valid_hex_private_key(config.PRIVATE_KEY)
    config.API_KEY = (config.API_KEY or "").strip()
    config.API_SECRET = (config.API_SECRET or "").strip()
    config.API_PASSPHRASE = (config.API_PASSPHRASE or "").strip()

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
    apply_schema(con)
    con.commit()
    return con

db = init_db(config.DATABASE_URL)

SQL_UPSERT_BTC_PRICE = """
        INSERT INTO btc_prices (ts, price)
        VALUES (%s, %s)
        ON CONFLICT (ts) DO UPDATE SET price = EXCLUDED.price
        """
SQL_INSERT_QUOTE = """INSERT INTO quotes (ts,market_id,bid,ask,fair_price,mid,edge,placed,model_type)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
SQL_INSERT_TRADE = """INSERT INTO trades
           (ts,market_id,market_slug,event_slug,market_q,mode,side,price,size,notional_usdc,size_shares,order_id,status,pnl,fill_price)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
SQL_UPSERT_BOT_STATS = """INSERT INTO bot_stats
           (ts,total_trades,open_positions,realized_pnl,unrealized_pnl,daily_pnl,balance,active_markets)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (ts) DO UPDATE
           SET total_trades = EXCLUDED.total_trades,
               open_positions = EXCLUDED.open_positions,
               realized_pnl = EXCLUDED.realized_pnl,
               unrealized_pnl = EXCLUDED.unrealized_pnl,
               daily_pnl = EXCLUDED.daily_pnl,
               balance = EXCLUDED.balance,
               active_markets = EXCLUDED.active_markets"""
SQL_DAILY_PNL = "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE status='filled' AND ts > (NOW() - INTERVAL '1 day')"
SQL_UPSERT_RUNTIME_STATE = """INSERT INTO runtime_state
           (id,updated_at,running,kill_switch,paper_mode,btc_price,btc_source,ws_connected,ws_tick_age_sec,cycle_latency_ms,orders_placed_cycle,cycle_latency_avg_ms,orders_placed_avg,quotes_considered_cycle,quotes_eligible_cycle,order_attempts_cycle,order_acks_cycle,fills_cycle,quote_hit_rate_cycle,ack_rate_cycle,fill_rate_cycle,avg_edge_cycle,avg_order_distance_cycle,quote_hit_rate_avg,ack_rate_avg,fill_rate_avg,avg_edge_avg,avg_order_distance_avg,last_cycle,errors)
           VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
               cycle_latency_avg_ms = EXCLUDED.cycle_latency_avg_ms,
               orders_placed_avg = EXCLUDED.orders_placed_avg,
               quotes_considered_cycle = EXCLUDED.quotes_considered_cycle,
               quotes_eligible_cycle = EXCLUDED.quotes_eligible_cycle,
               order_attempts_cycle = EXCLUDED.order_attempts_cycle,
               order_acks_cycle = EXCLUDED.order_acks_cycle,
               fills_cycle = EXCLUDED.fills_cycle,
               quote_hit_rate_cycle = EXCLUDED.quote_hit_rate_cycle,
               ack_rate_cycle = EXCLUDED.ack_rate_cycle,
               fill_rate_cycle = EXCLUDED.fill_rate_cycle,
               avg_edge_cycle = EXCLUDED.avg_edge_cycle,
               avg_order_distance_cycle = EXCLUDED.avg_order_distance_cycle,
               quote_hit_rate_avg = EXCLUDED.quote_hit_rate_avg,
               ack_rate_avg = EXCLUDED.ack_rate_avg,
               fill_rate_avg = EXCLUDED.fill_rate_avg,
               avg_edge_avg = EXCLUDED.avg_edge_avg,
               avg_order_distance_avg = EXCLUDED.avg_order_distance_avg,
               last_cycle = EXCLUDED.last_cycle,
               errors = EXCLUDED.errors"""

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def db_write(query: str, params: tuple) -> None:
    db.execute(query, params)
    db.commit()

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
    best_bid: float
    best_ask: float
    volume: float
    liquidity: float
    end_date_iso: str
    rules_text: str
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
    starting_balance: float = 0.0
    current_balance: float = 0.0
    equity: float = 0.0
    ws_btc_price: float = 0.0
    ws_connected: bool = False
    ws_last_tick: float = 0.0
    reference_btc_price: float = 0.0
    reference_last_update: float = 0.0
    cycle_latency_ms: float = 0.0
    orders_placed_cycle: int = 0
    cycle_latency_avg_ms: float = 0.0
    orders_placed_avg: float = 0.0
    cycle_latency_window: list[float] = field(default_factory=list)
    orders_placed_window: list[int] = field(default_factory=list)
    quotes_considered_cycle: int = 0
    quotes_eligible_cycle: int = 0
    order_attempts_cycle: int = 0
    order_acks_cycle: int = 0
    fills_cycle: int = 0
    quote_hit_rate_cycle: float = 0.0
    ack_rate_cycle: float = 0.0
    fill_rate_cycle: float = 0.0
    avg_edge_cycle: float = 0.0
    avg_order_distance_cycle: float = 0.0
    quote_hit_rate_avg: float = 0.0
    ack_rate_avg: float = 0.0
    fill_rate_avg: float = 0.0
    avg_edge_avg: float = 0.0
    avg_order_distance_avg: float = 0.0
    quote_hit_rate_window: list[float] = field(default_factory=list)
    ack_rate_window: list[float] = field(default_factory=list)
    fill_rate_window: list[float] = field(default_factory=list)
    avg_edge_window: list[float] = field(default_factory=list)
    avg_order_distance_window: list[float] = field(default_factory=list)
    # Cumulative matched YES shares already applied via ledger (live partial fills).
    live_matched_shares_by_order: dict[str, float] = field(default_factory=dict)

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

def get_market_position_qty(market_id: str) -> float:
    """Signed YES share inventory for a market (+long / -short)."""
    pos = state.open_positions.get(market_id)
    if not pos:
        return 0.0
    return float(pos.get("qty", 0.0))

def refresh_account_state():
    """Equity from realized PnL ledger (starting reference balance + cumulative realized)."""
    state.current_balance = round(state.starting_balance + state.realized_pnl, 4)
    state.equity = state.current_balance

def update_cycle_telemetry(orders_placed: int, cycle_started: float) -> None:
    """Track instantaneous and 10-cycle average telemetry."""
    state.orders_placed_cycle = int(orders_placed)
    state.cycle_latency_ms = round((time.perf_counter() - cycle_started) * 1000.0, 2)

    state.orders_placed_window.append(state.orders_placed_cycle)
    state.cycle_latency_window.append(state.cycle_latency_ms)
    if len(state.orders_placed_window) > 10:
        state.orders_placed_window.pop(0)
    if len(state.cycle_latency_window) > 10:
        state.cycle_latency_window.pop(0)

    state.orders_placed_avg = round(
        sum(state.orders_placed_window) / max(1, len(state.orders_placed_window)),
        2,
    )
    state.cycle_latency_avg_ms = round(
        sum(state.cycle_latency_window) / max(1, len(state.cycle_latency_window)),
        2,
    )

def update_execution_telemetry(
    quotes_considered: int,
    quotes_eligible: int,
    order_attempts: int,
    order_acks: int,
    fills: int,
    edge_sum: float,
    edge_count: int,
    order_distance_sum: float,
    order_distance_count: int,
) -> None:
    state.quotes_considered_cycle = int(quotes_considered)
    state.quotes_eligible_cycle = int(quotes_eligible)
    state.order_attempts_cycle = int(order_attempts)
    state.order_acks_cycle = int(order_acks)
    state.fills_cycle = int(fills)
    state.quote_hit_rate_cycle = round((quotes_eligible / quotes_considered), 4) if quotes_considered > 0 else 0.0
    state.ack_rate_cycle = round((order_acks / order_attempts), 4) if order_attempts > 0 else 0.0
    state.fill_rate_cycle = round((fills / order_acks), 4) if order_acks > 0 else 0.0
    state.avg_edge_cycle = round((edge_sum / edge_count), 6) if edge_count > 0 else 0.0
    state.avg_order_distance_cycle = round((order_distance_sum / order_distance_count), 6) if order_distance_count > 0 else 0.0

    for window, value in (
        (state.quote_hit_rate_window, state.quote_hit_rate_cycle),
        (state.ack_rate_window, state.ack_rate_cycle),
        (state.fill_rate_window, state.fill_rate_cycle),
        (state.avg_edge_window, state.avg_edge_cycle),
        (state.avg_order_distance_window, state.avg_order_distance_cycle),
    ):
        window.append(float(value))
        if len(window) > 10:
            window.pop(0)

    state.quote_hit_rate_avg = round(sum(state.quote_hit_rate_window) / max(1, len(state.quote_hit_rate_window)), 4)
    state.ack_rate_avg = round(sum(state.ack_rate_window) / max(1, len(state.ack_rate_window)), 4)
    state.fill_rate_avg = round(sum(state.fill_rate_window) / max(1, len(state.fill_rate_window)), 4)
    state.avg_edge_avg = round(sum(state.avg_edge_window) / max(1, len(state.avg_edge_window)), 6)
    state.avg_order_distance_avg = round(
        sum(state.avg_order_distance_window) / max(1, len(state.avg_order_distance_window)),
        6,
    )

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
    db_write(
        SQL_UPSERT_BTC_PRICE,
        (utcnow(), price),
    )
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
                "limit": max(100, min(config.MARKETS_FETCH_LIMIT, 3000)),
                "_order": "volume",
            },
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            markets_raw = await r.json()
    except Exception as e:
        log.error(f"Gamma API error: {e}")
        return []

    results = []
    skipped_unsupported = 0
    for m in markets_raw:
        q = m.get("question", "").lower()
        # Filter for BTC price prediction markets
        if "bitcoin" not in q and "btc" not in q:
            continue
        if not m.get("active"):
            continue
        rules_text = " ".join(
            str(m.get(k, "") or "")
            for k in ("description", "rules", "resolutionSource", "resolution")
        )
        model_type = classify_market_model(m.get("question", ""), rules_text)
        if model_type == "unsupported":
            skipped_unsupported += 1
            continue

        # Gamma API shape changed over time. Support both:
        # 1) legacy "tokens" objects
        # 2) modern "clobTokenIds" + "outcomes"/"outcomePrices"
        yes_token = ""
        no_token = ""
        yes_idx = 0
        no_idx = 1
        tokens = m.get("tokens", [])
        if tokens and len(tokens) >= 2:
            yes_tok = next((t for t in tokens if t.get("outcome", "").lower() == "yes"), tokens[0])
            no_tok = next((t for t in tokens if t.get("outcome", "").lower() == "no"), tokens[1])
            yes_token = yes_tok.get("token_id", "")
            no_token = no_tok.get("token_id", "")
        else:
            clob_ids_raw = m.get("clobTokenIds")
            clob_ids = []
            if isinstance(clob_ids_raw, list):
                clob_ids = [str(x) for x in clob_ids_raw]
            elif isinstance(clob_ids_raw, str):
                try:
                    parsed_clob_ids = json.loads(clob_ids_raw)
                    if isinstance(parsed_clob_ids, list):
                        clob_ids = [str(x) for x in parsed_clob_ids]
                except Exception:
                    clob_ids = []
            outcomes_raw = m.get("outcomes")
            outcomes = []
            if isinstance(outcomes_raw, list):
                outcomes = [str(x).lower() for x in outcomes_raw]
            elif isinstance(outcomes_raw, str):
                try:
                    outcomes = [str(x).lower() for x in json.loads(outcomes_raw)]
                except Exception:
                    outcomes = []

            if "yes" in outcomes:
                yes_idx = outcomes.index("yes")
            if "no" in outcomes:
                no_idx = outcomes.index("no")
            elif yes_idx == 0:
                no_idx = 1
            else:
                no_idx = 0

            if len(clob_ids) >= 2:
                yes_idx = max(0, min(yes_idx, len(clob_ids) - 1))
                no_idx = max(0, min(no_idx, len(clob_ids) - 1))
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
            yes_idx_price = max(0, min(yes_idx, len(outcome_prices) - 1))
            no_idx_price = max(0, min(no_idx, len(outcome_prices) - 1))
            yes_price = float(outcome_prices[yes_idx_price])
            no_price  = float(outcome_prices[no_idx_price])
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
            best_bid=float(m.get("bestBid", 0) or 0),
            best_ask=float(m.get("bestAsk", 0) or 0),
            volume=float(m.get("volume", 0) or 0),
            liquidity=float(m.get("liquidity", 0) or 0),
            end_date_iso=m.get("endDate", ""),
            rules_text=rules_text,
            active=True
        ))

    def market_priority_score(market: Market) -> float:
        # Favor deep/liquid markets with balanced probabilities and usable horizon.
        vol_score = math.log1p(max(0.0, market.volume))
        liq_score = math.log1p(max(0.0, market.liquidity))
        balance_score = max(0.0, 1.0 - abs(market.yes_price - 0.5) * 2.0)
        dte = get_days_to_expiry(market.end_date_iso)
        horizon_score = 1.0 if 0.5 < dte <= 45 else (0.5 if dte > 45 else 0.0)
        return (0.55 * vol_score) + (0.35 * liq_score) + (2.0 * balance_score) + horizon_score

    # Rank by priority score (not raw volume only) to broaden actionable coverage.
    results.sort(key=market_priority_score, reverse=True)
    state.active_markets = results[:config.MARKETS_WATCHED]
    if skipped_unsupported:
        log.info(f"Skipped {skipped_unsupported} unsupported BTC market structures")
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

def extract_price_levels(text: str) -> list[float]:
    """
    Extract ordered dollar-denominated levels from free text.
    Supports formats like $90,000, $95k, $1m, $90000.
    """
    if not text:
        return []
    matches = re.finditer(
        r"\$([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.[0-9]+)?)([km]?)\b",
        text,
        re.IGNORECASE,
    )
    out: list[float] = []
    for m in matches:
        base_raw = m.group(1).replace(",", "")
        suffix = (m.group(2) or "").lower()
        try:
            value = float(base_raw)
        except ValueError:
            continue
        if suffix == "k":
            value *= 1_000.0
        elif suffix == "m":
            value *= 1_000_000.0
        out.append(value)
    return out

def extract_strike_from_question(question: str, rules_text: str = "") -> Optional[float]:
    """
    Parse strike price from questions like:
    "Will Bitcoin be above $90,000 on Dec 31?"
    "BTC above $95k by end of month?"
    """
    levels_q = extract_price_levels(question)
    if levels_q:
        return levels_q[0]
    levels_rules = extract_price_levels(rules_text)
    if levels_rules:
        return levels_rules[0]
    return None

def extract_price_range(question: str, rules_text: str = "") -> Optional[tuple[float, float]]:
    levels = extract_price_levels(question)
    if len(levels) < 2 and rules_text:
        levels = levels + extract_price_levels(rules_text)
    if len(levels) < 2:
        return None
    a, b = levels[0], levels[1]
    lo, hi = (a, b) if a <= b else (b, a)
    return (lo, hi)

def has_explicit_time_cue(question: str, rules_text: str = "") -> bool:
    q = f"{(question or '').lower()} {(rules_text or '').lower()}".strip()
    if not q:
        return False
    month_names = (
        "jan", "feb", "mar", "apr", "may", "jun",
        "jul", "aug", "sep", "oct", "nov", "dec",
    )
    if any(m in q for m in month_names):
        return True
    if re.search(r"\b20[2-9][0-9]\b", q):
        return True
    time_phrases = ("by end", "end of", "this week", "this month", "this year", " by ", " on ")
    return any(p in q for p in time_phrases)

def classify_market_model(question: str, rules_text: str = "") -> str:
    """
    Supported models:
      - terminal: price relation at expiry (above/below/close/settle)
      - barrier: first-touch style (hit/reach/touch) with explicit time cue
      - unsupported: everything else (including comparative-event questions)
    """
    q = (question or "").strip().lower()
    rules = (rules_text or "").strip().lower()
    if not q:
        return "unsupported"

    strike = extract_strike_from_question(question, rules_text)
    if strike is None:
        return "unsupported"

    if ("between" in q or "range" in q) and extract_price_range(question, rules_text):
        if has_explicit_time_cue(q, rules):
            return "range_terminal"

    terminal_patterns = (
        r"\bbe above\b",
        r"\bbe below\b",
        r"\bclose above\b",
        r"\bclose below\b",
        r"\bsettle above\b",
        r"\bsettle below\b",
        r"\bover \$",
        r"\bunder \$",
        r"\bat or above\b",
        r"\bat or below\b",
    )
    if any(re.search(pat, q) for pat in terminal_patterns):
        return "terminal"

    barrier_patterns = (
        r"\bhit\b",
        r"\bhits\b",
        r"\breach\b",
        r"\breaches\b",
        r"\btouch\b",
        r"\btouches\b",
        r"\bdip to\b",
        r"\bdrop to\b",
        r"\bfall to\b",
    )
    has_barrier_verb = any(re.search(pat, q) for pat in barrier_patterns)
    if has_barrier_verb:
        if " before " in q and ("50-50" in rules or "50 50" in rules):
            return "comparative_5050"
        # Exclude comparative-event structures like "before GTA VI".
        if " before " in q and not has_explicit_time_cue(q.replace(" before ", " "), rules):
            return "unsupported"
        if not has_explicit_time_cue(q, rules):
            return "unsupported"
        return "barrier"

    return "unsupported"

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

def calc_range_terminal_probability(
    btc_price: float,
    low: float,
    high: float,
    days_to_expiry: float,
) -> float:
    """
    Approximate terminal probability that BTC finishes in [low, high].
    """
    if low <= 0 or high <= 0 or high <= low:
        return 0.0
    p_gt_low = calc_fair_probability(btc_price, low, days_to_expiry)
    p_gt_high = calc_fair_probability(btc_price, high, days_to_expiry)
    return max(0.0, min(1.0, p_gt_low - p_gt_high))

def calc_barrier_hit_probability(btc_price: float, barrier: float, days_to_expiry: float) -> float:
    """
    Approximate first-touch probability under GBM with zero price drift.
    Works for upper and lower barriers.
    """
    if barrier <= 0 or days_to_expiry <= 0 or btc_price <= 0:
        return 0.5
    if abs(barrier - btc_price) / btc_price < 1e-9:
        return 1.0

    sigma = 0.80
    T = max(days_to_expiry / 365.0, 1e-6)
    sigma_sqrt_t = sigma * math.sqrt(T)
    if sigma_sqrt_t <= 0:
        return 0.5

    # Under dS/S = sigma dW, log-price drift is nu = -0.5*sigma^2.
    nu = -0.5 * sigma * sigma

    def norm_cdf(x: float) -> float:
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    if barrier > btc_price:
        a = math.log(barrier / btc_price)
        z1 = (nu * T - a) / sigma_sqrt_t
        z2 = (-nu * T - a) / sigma_sqrt_t
        p = norm_cdf(z1) + math.exp((2.0 * nu * a) / (sigma * sigma)) * norm_cdf(z2)
    else:
        # Lower barrier via mirrored process -log(S_t).
        a = math.log(btc_price / barrier)
        nu_m = -nu
        z1 = (nu_m * T - a) / sigma_sqrt_t
        z2 = (-nu_m * T - a) / sigma_sqrt_t
        p = norm_cdf(z1) + math.exp((2.0 * nu_m * a) / (sigma * sigma)) * norm_cdf(z2)

    return max(0.0, min(1.0, p))

def infer_anchor_event_hazard(question: str, rules_text: str) -> float:
    q = (question or "").lower()
    rules = (rules_text or "").lower()
    if "gta vi" in q or "gta vi" in rules:
        return max(1e-7, config.GTA_RELEASE_HAZARD_PER_DAY)
    return max(1e-7, config.ANCHOR_EVENT_HAZARD_PER_DAY)

def calc_comparative_5050_probability(
    btc_price: float,
    strike: float,
    days_to_expiry: float,
    question: str,
    rules_text: str,
) -> float:
    """
    For "A before B" markets with explicit 50-50 fallback if neither by deadline:
      payout = P(A occurs before min(B, D)) + 0.5 * P(A>D and B>D)
    We model A=BTC barrier hit using implied hazard from barrier probability,
    and B (anchor event) as exponential hazard inferred from question/rules.
    """
    if days_to_expiry <= 0:
        return 0.5

    p_hit = calc_barrier_hit_probability(btc_price, strike, days_to_expiry)
    T = max(days_to_expiry, 1e-6)
    lambda_b = max(1e-9, -math.log(max(1e-9, 1.0 - p_hit)) / T)
    lambda_g = infer_anchor_event_hazard(question, rules_text)

    lam_sum = lambda_b + lambda_g
    p_yes_before_deadline = (lambda_b / lam_sum) * (1.0 - math.exp(-lam_sum * T))
    p_none = math.exp(-lam_sum * T)
    fair = p_yes_before_deadline + 0.5 * p_none
    return max(0.0, min(1.0, fair))

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
    model = classify_market_model(market.question, market.rules_text)
    if model == "unsupported":
        return None

    strike = extract_strike_from_question(market.question, market.rules_text)
    if strike is None:
        log.debug(f"Could not parse strike from: {market.question}")
        return None

    dte = get_days_to_expiry(market.end_date_iso)
    if model == "range_terminal":
        range_bounds = extract_price_range(market.question, market.rules_text)
        if not range_bounds:
            return None
        low, high = range_bounds
        fair = calc_range_terminal_probability(btc, low, high, dte)
    elif model == "barrier":
        fair = calc_barrier_hit_probability(btc, strike, dte)
    elif model == "comparative_5050":
        fair = calc_comparative_5050_probability(
            btc,
            strike,
            dte,
            market.question,
            market.rules_text,
        )
    else:
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

    tick = 0.01
    passive_buffer = max(1, int(config.PASSIVE_BUFFER_TICKS)) * tick

    # Post-only style guard: keep our bid below opposite best ask and our ask above
    # opposite best bid whenever top-of-book is available from Gamma.
    top_bid = max(0.0, float(market.best_bid))
    top_ask = max(0.0, float(market.best_ask))
    if top_ask > 0:
        bid = min(bid, max(0.01, round(top_ask - passive_buffer, 2)))
    if top_bid > 0:
        ask = max(ask, min(0.99, round(top_bid + passive_buffer, 2)))

    # Distance guards around midpoint: avoid too-aggressive (taker-like) and too-far quotes.
    min_mid_dist = max(0.0, float(config.MIN_MID_DISTANCE_PCT))
    max_mid_dist = max(min_mid_dist + tick, float(config.MAX_MID_DISTANCE_PCT))
    bid = min(bid, round(max(0.01, mid - min_mid_dist), 2))
    ask = max(ask, round(min(0.99, mid + min_mid_dist), 2))
    bid = max(bid, round(max(0.01, mid - max_mid_dist), 2))
    ask = min(ask, round(min(0.99, mid + max_mid_dist), 2))

    bid = max(0.01, min(0.99, round(bid, 2)))
    ask = max(0.01, min(0.99, round(ask, 2)))
    if bid >= ask:
        return None

    # Edge = how much our fair price beats the current mid
    edge = abs(fair - mid)
    should_place = (
        edge >= config.MIN_EDGE
        and dte > 0.5          # Don't trade within 12hrs of expiry
        and market.liquidity > config.MIN_MARKET_LIQUIDITY
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
    db_write(
        SQL_INSERT_QUOTE,
        (utcnow(),
         market.condition_id, bid, ask, fair, mid, edge, int(should_place), model)
    )

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

def persist_trade(
    market: Market,
    mode: str,
    side: str,
    price: float,
    notional_usdc: float,
    size_shares: float,
    order_id: str,
    status: str,
    pnl: float = 0.0,
    fill_price: Optional[float] = None,
) -> None:
    """Persist a CLOB trade row to PostgreSQL."""
    db_write(
        SQL_INSERT_TRADE,
        (
            utcnow(),
            market.condition_id,
            market.slug,
            market.event_slug,
            market.question,
            mode,
            side,
            price,
            notional_usdc,
            notional_usdc,
            size_shares,
            order_id,
            status,
            pnl,
            fill_price,
        ),
    )


def ledger_apply_yes_fill(market_id: str, fill_side: str, fill_price: float, shares: float) -> float:
    """
    Inventory accounting on YES shares (live partial fills).
    shares = conditional token size filled.
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


SQL_UPDATE_LIVE_TRADE_TERMINAL = """
UPDATE trades SET status=%s, fill_price=%s, pnl=%s
WHERE order_id=%s AND COALESCE(mode,'')='live' AND status IN ('open','submitted')
"""
SQL_UPDATE_LIVE_ACCUMULATE_PNL = """
UPDATE trades SET pnl = COALESCE(pnl, 0) + %s, fill_price = %s
WHERE order_id=%s AND COALESCE(mode,'')='live' AND status IN ('open','submitted')
"""
SQL_UPDATE_LIVE_TRADE_FILLED_NO_PNL_RESET = """
UPDATE trades SET status='filled', fill_price=%s
WHERE order_id=%s AND COALESCE(mode,'')='live' AND status IN ('open','submitted')
"""


def _clob_order_id(obj: dict) -> str:
    return str(obj.get("orderID") or obj.get("id") or obj.get("order_id") or "")


def _terminal_order_status(detail: object) -> Optional[str]:
    """Map CLOB get_order payload to filled | cancelled | open | None (unknown)."""
    if not isinstance(detail, dict):
        return None
    status = str(detail.get("status") or detail.get("orderStatus") or "").lower()
    if status in ("filled", "matched", "fully_filled", "closed"):
        return "filled"
    if status in ("cancelled", "canceled", "expired", "rejected"):
        return "cancelled"
    if status in ("live", "open", "pending", "submitted"):
        return "open"
    try:
        orig = float(detail.get("original_size") or detail.get("size") or 0)
        matched = float(
            detail.get("size_matched")
            or detail.get("filled_size")
            or detail.get("filledSize")
            or detail.get("sizeMatched")
            or 0
        )
        if orig > 0 and matched >= orig * 0.999:
            return "filled"
    except (TypeError, ValueError):
        pass
    return None


def _fill_price_from_clob_detail(detail: dict, fallback: float) -> float:
    for key in ("avg_price", "avgPrice", "price", "last_fill_price", "lastFillPrice"):
        v = detail.get(key)
        if v is not None:
            try:
                p = float(v)
                if p > 0:
                    return p
            except (TypeError, ValueError):
                continue
    return max(0.01, float(fallback))


def _fill_size_from_clob_detail(detail: dict, fallback: float) -> float:
    for key in ("size_matched", "sizeMatched", "filled_size", "filledSize", "matched"):
        v = detail.get(key)
        if v is not None:
            try:
                s = float(v)
                if s > 0:
                    return s
            except (TypeError, ValueError):
                continue
    return max(0.0, float(fallback))


def _original_size_clob(detail: dict, fallback_shares: float) -> float:
    for key in ("original_size", "originalSize", "size", "base_size", "baseSize"):
        v = detail.get(key)
        if v is not None:
            try:
                s = float(v)
                if s > 0:
                    return s
            except (TypeError, ValueError):
                continue
    return max(0.0, float(fallback_shares))


def _matched_size_clob(detail: dict, fallback: float) -> float:
    m = _fill_size_from_clob_detail(detail, 0.0)
    if m > 0:
        return m
    return max(0.0, float(fallback))


def sync_live_trades_with_exchange() -> Dict[str, int]:
    """
    Reconcile DB rows (live, open/submitted) with CLOB get_orders / get_order.
    Applies incremental fills (partial + full) using live_matched_shares_by_order watermarks,
    then terminal status (filled / cancelled).
    """
    stats: Dict[str, int] = {"filled": 0, "cancelled": 0, "skipped": 0, "fill_ticks": 0}
    try:
        client = get_live_client()
    except Exception as e:
        log.warning(f"[LIVE] sync: CLOB client init failed: {e}")
        return stats
    try:
        open_list = client.get_orders() or []
    except Exception as e:
        log.warning(f"[LIVE] sync: get_orders failed: {e}")
        return stats

    open_ids = {_clob_order_id(o) for o in open_list if isinstance(o, dict)}
    open_ids.discard("")

    try:
        cur = db.execute(
            "SELECT order_id, market_id, side, price, size_shares, notional_usdc "
            "FROM trades WHERE COALESCE(mode,'')='live' AND status IN ('open','submitted') "
            "ORDER BY ts DESC LIMIT 400"
        )
        rows = cur.fetchall() or []
    except Exception as e:
        log.warning(f"[LIVE] sync: DB read failed: {e}")
        return stats

    for row in rows:
        oid = str(row[0] or "")
        if not oid:
            continue
        market_id = str(row[1] or "")
        side = str(row[2] or "BUY").upper()
        row_price = float(row[3] or 0)
        row_shares = float(row[4] or 0)
        notional = float(row[5] or 0)

        detail = None
        try:
            detail = client.get_order(oid)
        except Exception as e:
            log.debug(f"[LIVE] sync: get_order {oid[:16]}… {e}")

        on_book = oid in open_ids

        if detail is None:
            if on_book:
                stats["skipped"] += 1
                continue
            state.live_matched_shares_by_order.pop(oid, None)
            db_write(
                SQL_UPDATE_LIVE_TRADE_TERMINAL,
                ("cancelled", None, 0.0, oid),
            )
            stats["cancelled"] += 1
            continue

        if not isinstance(detail, dict):
            stats["skipped"] += 1
            continue

        term = _terminal_order_status(detail)

        if term == "cancelled":
            state.live_matched_shares_by_order.pop(oid, None)
            db_write(
                SQL_UPDATE_LIVE_TRADE_TERMINAL,
                ("cancelled", None, 0.0, oid),
            )
            stats["cancelled"] += 1
            continue

        orig = _original_size_clob(detail, row_shares)
        matched = _matched_size_clob(detail, 0.0)
        if matched <= 0 and row_price > 0 and notional > 0:
            matched = notional_to_shares(notional, max(0.01, row_price))
        if orig <= 0 and matched > 0:
            orig = matched
        if term == "filled" and matched <= 0 and orig > 0:
            matched = orig

        wm = float(state.live_matched_shares_by_order.get(oid, 0.0))
        delta = max(0.0, matched - wm)
        fill_px = _fill_price_from_clob_detail(detail, row_price)

        if delta > 1e-8:
            trade_pnl = ledger_apply_yes_fill(market_id, side, fill_px, delta)
            state.realized_pnl += trade_pnl
            refresh_account_state()
            db_write(SQL_UPDATE_LIVE_ACCUMULATE_PNL, (trade_pnl, fill_px, oid))
            state.live_matched_shares_by_order[oid] = matched
            stats["fill_ticks"] += 1

        fully_filled = (orig > 0 and matched >= orig * 0.999) or (term == "filled")

        if fully_filled:
            state.live_matched_shares_by_order.pop(oid, None)
            db_write(SQL_UPDATE_LIVE_TRADE_FILLED_NO_PNL_RESET, (fill_px, oid))
            stats["filled"] += 1
            continue

        if term == "open" or on_book:
            stats["skipped"] += 1
            continue

        # Off book, not cancelled/open/filled-complete: unknown terminal
        log.debug(
            f"[LIVE] sync: order {oid[:16]}… off open list, unknown status "
            f"{detail.get('status')!r} — skipping DB status update"
        )
        stats["skipped"] += 1

    return stats


async def place_order(
    session: aiohttp.ClientSession,
    token_id: str,
    side: str,        # "BUY" or "SELL"
    price: float,
    size: float,
    market: Market,
    edge: float = 0.0,
    mid: Optional[float] = None,
) -> Optional[tuple[str, str]]:
    """Place a limit order via the Polymarket CLOB (py-clob-client)."""
    notional_usdc = float(size)
    size_shares = notional_to_shares(notional_usdc, price)
    if size_shares < config.MIN_ORDER_SHARES:
        log.debug(
            f"Skip tiny order size_shares={size_shares:.6f} below minimum {config.MIN_ORDER_SHARES:.6f}"
        )
        return None

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
        real_id = resp.get("orderID") or ""

        state.total_trades += 1
        persist_trade(
            market=market,
            mode="live",
            side=side,
            price=price,
            notional_usdc=notional_usdc,
            size_shares=size_shares,
            order_id=real_id,
            status="open",
        )
        log.info(
            f"[LIVE] {side} ${notional_usdc:.2f} ({size_shares:.4f} shares) "
            f"@ {price:.3f} order_id={real_id[:16] if real_id else '?'}"
        )
        return (real_id, "open")

    except ImportError:
        log.critical("py-clob-client not installed. Install with: pip install py-clob-client")
        state.kill_switch = True
        return None
    except Exception as e:
        log.error(f"Order placement failed: {e}")
        state.errors.append(f"{datetime.now().isoformat()} | {e}")
        return None

# ─── Risk / Kill Switch ────────────────────────────────────────────────────────

def count_live_open_orders() -> int:
    """
    Resting live orders for MAX_OPEN_ORDERS enforcement.
    Prefer CLOB get_orders(); fall back to DB if the client call fails.
    """
    try:
        client = get_live_client()
        orders = client.get_orders()
        if isinstance(orders, list):
            return len(orders)
    except Exception as e:
        log.debug(f"get_orders count unavailable: {e}")
    try:
        row = db.execute(
            "SELECT COUNT(*) AS c FROM trades WHERE COALESCE(mode, '') = 'live' "
            "AND status IN ('open', 'submitted')"
        ).fetchone()
        return int(row[0] if row and row[0] is not None else 0)
    except Exception:
        return 0


def cancel_live_orders_best_effort() -> int:
    """
    Try to cancel stale live orders before re-quoting.
    Uses whichever client methods are available in installed py-clob-client version.
    """
    try:
        client = get_live_client()
    except Exception as e:
        log.warning(f"Unable to cancel stale orders (CLOB client init): {e}")
        return 0
    try:
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
    db_write(
        SQL_UPSERT_BOT_STATS,
        (
            utcnow(),
            state.total_trades,
            len(state.open_positions),
            state.realized_pnl,
            0.0,
            state.daily_pnl,
            state.equity,
            len(state.active_markets),
        )
    )

# ─── Main Loop ────────────────────────────────────────────────────────────────

async def main_loop():
    log.info("=" * 60)
    log.info("  Polymarket Market Making Bot  |  Starting up...")
    log.info("=" * 60)
    validate_runtime_config()
    state.starting_balance = 0.0
    refresh_account_state()
    log.info(
        f"Config: profile={config.STRATEGY_PROFILE} spread={config.SPREAD_PCT*100:.1f}%  min_edge={config.MIN_EDGE*100:.1f}%  "
        f"notional=${config.ORDER_SIZE} CLOB=live"
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
                buy_orders = 0
                sell_orders = 0
                quotes_considered = 0
                quotes_eligible = 0
                order_attempts = 0
                order_acks = 0
                fills = 0
                edge_sum = 0.0
                edge_count = 0
                order_distance_sum = 0.0
                order_distance_count = 0
                if state.kill_switch:
                    log.warning("Kill switch active — skipping order placement")
                    update_execution_telemetry(
                        quotes_considered=0,
                        quotes_eligible=0,
                        order_attempts=0,
                        order_acks=0,
                        fills=0,
                        edge_sum=0.0,
                        edge_count=0,
                        order_distance_sum=0.0,
                        order_distance_count=0,
                    )
                else:
                    sync_pre = sync_live_trades_with_exchange()
                    if config.CANCEL_BEFORE_REQUOTE:
                        cancelled = cancel_live_orders_best_effort()
                        if cancelled:
                            log.info(f"Cancelled {cancelled} stale live orders before re-quote")
                    sync_st = sync_live_trades_with_exchange()
                    merged = {
                        "filled": sync_pre.get("filled", 0) + sync_st.get("filled", 0),
                        "fill_ticks": sync_pre.get("fill_ticks", 0) + sync_st.get("fill_ticks", 0),
                        "cancelled": sync_pre.get("cancelled", 0) + sync_st.get("cancelled", 0),
                        "skipped": sync_st.get("skipped", 0),
                    }
                    if merged.get("filled") or merged.get("cancelled") or merged.get("fill_ticks"):
                        log.info(
                            f"[LIVE] exchange sync: orders_filled={merged['filled']} "
                            f"fill_ticks={merged.get('fill_ticks', 0)} "
                            f"cancelled={merged['cancelled']} "
                            f"resting_skipped={merged.get('skipped', 0)}"
                        )
                    fills += merged.get("fill_ticks", 0)
                    open_order_cap_blocked = False
                    n_open = count_live_open_orders()
                    if n_open >= config.MAX_OPEN_ORDERS:
                        log.warning(
                            f"Open orders {n_open} >= POLY_MAX_OPEN_ORDERS ({config.MAX_OPEN_ORDERS}) "
                            "— skipping new placements this cycle"
                        )
                        open_order_cap_blocked = True
                    # 4. Quote each market
                    for market in markets:
                        quote = await compute_quote(session, market)
                        if quote is None:
                            continue
                        quotes_considered += 1

                        log.info(
                            f"  {market.question[:55]:55s}  "
                            f"fair={quote.fair_price:.3f}  "
                            f"mid={quote.mid:.3f}  "
                            f"edge={quote.edge:.3f}  "
                            f"{'✓ QUOTE' if quote.should_place else '✗ skip'}"
                        )

                        if quote.should_place:
                            if open_order_cap_blocked:
                                continue
                            quotes_eligible += 1
                            edge_sum += float(quote.edge)
                            edge_count += 1
                            inv_qty = get_market_position_qty(market.condition_id)
                            max_qty_hint = config.MAX_POSITION / max(quote.mid, 0.01)
                            inv_ratio = inv_qty / max(max_qty_hint, 1e-6)

                            # Inventory-aware skew: long YES inventory shifts both quotes down;
                            # short inventory shifts both quotes up.
                            skew = max(-1.0, min(1.0, inv_ratio)) * config.INVENTORY_SKEW_PCT
                            bid_px = max(0.01, min(0.99, round(quote.bid - skew, 2)))
                            ask_px = max(0.01, min(0.99, round(quote.ask - skew, 2)))
                            if bid_px >= ask_px:
                                ask_px = min(0.99, round(bid_px + 0.01, 2))

                            allow_buy = True
                            allow_sell = True
                            if inv_ratio >= config.INVENTORY_SOFT_LIMIT_PCT:
                                allow_buy = False
                            if inv_ratio <= -config.INVENTORY_SOFT_LIMIT_PCT:
                                allow_sell = False
                            if inv_ratio >= config.INVENTORY_HARD_LIMIT_PCT:
                                allow_buy = False
                            if inv_ratio <= -config.INVENTORY_HARD_LIMIT_PCT:
                                allow_sell = False

                            if not allow_buy and not allow_sell:
                                log.info(
                                    f"Inventory guard for {market.condition_id[:12]}: "
                                    f"qty={inv_qty:.2f} ratio={inv_ratio:.2f}, skipping both sides"
                                )
                                continue

                            market_exposure = get_market_exposure(market.condition_id)
                            projected_market = market_exposure + (max(int(allow_buy), int(allow_sell)) * config.ORDER_SIZE)
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

                            # Place one or both sides depending on inventory risk.
                            if allow_buy:
                                order_attempts += 1
                                order_distance_sum += abs(bid_px - quote.mid)
                                order_distance_count += 1
                                buy_res = await place_order(
                                    session,
                                    market.yes_token,
                                    "BUY",
                                    bid_px,
                                    config.ORDER_SIZE,
                                    market,
                                    edge=quote.edge,
                                    mid=quote.mid,
                                )
                                if buy_res:
                                    buy_id, buy_status = buy_res
                                    buy_orders += 1
                                    order_acks += 1
                                    if buy_status == "filled":
                                        fills += 1
                            if allow_sell:
                                order_attempts += 1
                                order_distance_sum += abs(ask_px - quote.mid)
                                order_distance_count += 1
                                sell_res = await place_order(
                                    session,
                                    market.yes_token,
                                    "SELL",
                                    ask_px,
                                    config.ORDER_SIZE,
                                    market,
                                    edge=quote.edge,
                                    mid=quote.mid,
                                )
                                if sell_res:
                                    sell_id, sell_status = sell_res
                                    sell_orders += 1
                                    order_acks += 1
                                    if sell_status == "filled":
                                        fills += 1

                            placed_count += 1
                            await asyncio.sleep(0.3)  # Rate limit

                    update_execution_telemetry(
                        quotes_considered=quotes_considered,
                        quotes_eligible=quotes_eligible,
                        order_attempts=order_attempts,
                        order_acks=order_acks,
                        fills=fills,
                        edge_sum=edge_sum,
                        edge_count=edge_count,
                        order_distance_sum=order_distance_sum,
                        order_distance_count=order_distance_count,
                    )
                    log.info(
                        f"Placed quotes on {placed_count} markets this cycle "
                        f"(buy_orders={buy_orders}, sell_orders={sell_orders})"
                    )
                    log.info(
                        f"Account: equity=${state.equity:.2f} realized={state.realized_pnl:+.2f} "
                        f"exposure=${get_total_exposure():.2f}"
                    )

                update_cycle_telemetry(placed_count, cycle_started)
                log.info(
                    f"Cycle telemetry: orders={state.orders_placed_cycle} "
                    f"orders_avg10={state.orders_placed_avg:.2f} "
                    f"latency_ms={state.cycle_latency_ms:.2f} "
                    f"latency_avg10_ms={state.cycle_latency_avg_ms:.2f} "
                    f"q_hit={state.quote_hit_rate_cycle:.2%}/{state.quote_hit_rate_avg:.2%} "
                    f"ack={state.ack_rate_cycle:.2%}/{state.ack_rate_avg:.2%} "
                    f"fill={state.fill_rate_cycle:.2%}/{state.fill_rate_avg:.2%} "
                    f"edge={state.avg_edge_cycle:.4f}/{state.avg_edge_avg:.4f} "
                    f"dist={state.avg_order_distance_cycle:.4f}/{state.avg_order_distance_avg:.4f}"
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
    row = db.execute(SQL_DAILY_PNL).fetchone()
    return float(row[0] or 0.0)

def update_runtime_state():
    now_ts = time.time()
    ws_age = (now_ts - state.ws_last_tick) if state.ws_last_tick > 0 else 9999.0
    btc_source = "ws" if (state.ws_connected and ws_age <= 15 and state.ws_btc_price > 0) else "ref"
    db_write(
        SQL_UPSERT_RUNTIME_STATE,
        (
            utcnow(),
            int(state.running),
            int(state.kill_switch),
            0,
            state.btc_price,
            btc_source,
            int(state.ws_connected),
            float(round(ws_age, 3)),
            float(state.cycle_latency_ms),
            int(state.orders_placed_cycle),
            float(state.cycle_latency_avg_ms),
            float(state.orders_placed_avg),
            int(state.quotes_considered_cycle),
            int(state.quotes_eligible_cycle),
            int(state.order_attempts_cycle),
            int(state.order_acks_cycle),
            int(state.fills_cycle),
            float(state.quote_hit_rate_cycle),
            float(state.ack_rate_cycle),
            float(state.fill_rate_cycle),
            float(state.avg_edge_cycle),
            float(state.avg_order_distance_cycle),
            float(state.quote_hit_rate_avg),
            float(state.ack_rate_avg),
            float(state.fill_rate_avg),
            float(state.avg_edge_avg),
            float(state.avg_order_distance_avg),
            state.last_cycle,
            len(state.errors),
        ),
    )

if __name__ == "__main__":
    asyncio.run(main_loop())
