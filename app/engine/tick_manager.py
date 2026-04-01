"""TickDataManager — connects to Angel One SmartWebSocketV2 and distributes ticks.

Pipeline:
  Angel One WS → TickDataManager → CandleBuilder (per token)
                                  → TickEvaluator (entry/exit check)
                                  → ws_manager.broadcast (→ frontend dashboard)
"""

import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Set

from app.engine.candle_builder import CandleBuilder
from app.engine.time_utils import market_now, parse_exchange_timestamp

logger = logging.getLogger(__name__)

# Map exchange string to SmartWebSocketV2 exchange type integer
EXCHANGE_TYPE_MAP = {
    "NSE": 1,   # NSE_CM
    "NFO": 2,   # NSE_FO
    "BSE": 3,   # BSE_CM
    "BFO": 4,   # BSE_FO
    "MCX": 5,   # MCX_FO
}

# SmartWebSocketV2 subscription modes
MODE_LTP = 1         # LTP only — minimal bandwidth
MODE_QUOTE = 2       # LTP + OHLC + volume
MODE_SNAP_QUOTE = 3  # Full quote with OI
DEFAULT_SUBSCRIPTION_MODE = MODE_QUOTE


class TickDataManager:
    """
    Central manager for Angel One SmartWebSocketV2 tick streaming.

    One instance per user. Runs the WS in a daemon thread, updates a
    thread-safe LTP dict, feeds per-token CandleBuilders, evaluates
    entry/exit via TickEvaluator, and broadcasts to frontend dashboard.
    """

    def __init__(
        self,
        auth_token: str,
        feed_token: str,
        api_key: str,
        client_code: str,
        candle_interval: str = "FIVE_MINUTE",
        tick_evaluator=None,
        event_loop: Optional[asyncio.AbstractEventLoop] = None,
        candle_close_callback=None,
        subscription_mode: int = DEFAULT_SUBSCRIPTION_MODE,
    ):
        self._auth_token = auth_token
        self._feed_token = feed_token
        self._api_key = api_key
        self._client_code = client_code
        self._candle_interval = candle_interval
        self._tick_evaluator = tick_evaluator
        self._loop = event_loop
        self._candle_close_callback = candle_close_callback
        self._connection_open_callback = None
        self._connection_close_callback = None
        self._subscription_mode = subscription_mode

        self._lock = threading.Lock()
        self._ltp_data: Dict[str, float] = {}
        self._candle_builders: Dict[str, CandleBuilder] = {}
        self._subscribed_tokens: List[dict] = []
        self._subscribed_set: Set[str] = set()
        self._last_tick_at: Dict[str, datetime] = {}
        self._clock_drift_warned_at: Dict[str, float] = {}

        # token → symbol name mapping for broadcasts
        self._token_symbols: Dict[str, str] = {}

        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False

    def set_candle_close_callback(self, callback):
        self._candle_close_callback = callback

    def set_connection_callbacks(self, on_open=None, on_close=None):
        self._connection_open_callback = on_open
        self._connection_close_callback = on_close

    def update_credentials(
        self,
        auth_token: str,
        feed_token: str,
        api_key: str,
        client_code: str,
    ):
        self._auth_token = auth_token
        self._feed_token = feed_token
        self._api_key = api_key
        self._client_code = client_code

    def restart_with_credentials(
        self,
        auth_token: str,
        feed_token: str,
        api_key: str,
        client_code: str,
    ):
        """Swap session credentials and reconnect the market WebSocket."""
        self.stop()
        self.update_credentials(auth_token, feed_token, api_key, client_code)
        self.start()

    @property
    def is_connected(self) -> bool:
        return self._connected

    def start(self):
        """Launch WebSocket connection in a daemon thread."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_ws, daemon=True, name="tick-ws")
        self._thread.start()
        logger.info("TickDataManager started for client %s", self._client_code)

    def stop(self):
        """Close WebSocket cleanly."""
        self._running = False
        if self._ws:
            try:
                self._ws.close_connection()
            except Exception:
                pass
        self._connected = False
        logger.info("TickDataManager stopped for client %s", self._client_code)

    def subscribe(self, tokens: List[dict]):
        """
        Subscribe to instrument tokens.

        Args:
            tokens: list of {"exchange": "NSE", "token": "3045", "symbol": "SBIN-EQ"}
        """
        new_set = {f"{t.get('exchange', 'NSE')}:{t['token']}" for t in tokens}

        with self._lock:
            if new_set == self._subscribed_set:
                return  # No change

        # Group tokens by exchange type for the SDK
        exchange_groups: Dict[int, List[str]] = {}
        for t in tokens:
            ex_type = EXCHANGE_TYPE_MAP.get(t.get("exchange", "NSE"), 1)
            exchange_groups.setdefault(ex_type, []).append(str(t["token"]))
            # Store symbol mapping
            self._token_symbols[str(t["token"])] = t.get("symbol", str(t["token"]))

        token_list = [
            {"exchangeType": ex, "tokens": toks}
            for ex, toks in exchange_groups.items()
        ]

        with self._lock:
            self._subscribed_tokens = token_list
            self._subscribed_set = new_set
            for t in tokens:
                tok = str(t["token"])
                if tok not in self._candle_builders:
                    self._candle_builders[tok] = CandleBuilder(self._candle_interval)

        if self._connected and self._ws:
            try:
                self._ws.subscribe("nebula_tick", self._subscription_mode, token_list)
                logger.info("Subscribed to %d tokens via WebSocket", len(tokens))
            except Exception as e:
                logger.warning("Subscribe failed: %s", e)

    def unsubscribe(self, tokens: List[dict]):
        """Unsubscribe from instrument tokens."""
        exchange_groups: Dict[int, List[str]] = {}
        for t in tokens:
            ex_type = EXCHANGE_TYPE_MAP.get(t.get("exchange", "NSE"), 1)
            exchange_groups.setdefault(ex_type, []).append(str(t["token"]))

        token_list = [
            {"exchangeType": ex, "tokens": toks}
            for ex, toks in exchange_groups.items()
        ]

        remove_set = {f"{t.get('exchange', 'NSE')}:{t['token']}" for t in tokens}
        with self._lock:
            self._subscribed_set -= remove_set

        if self._connected and self._ws:
            try:
                self._ws.unsubscribe("nebula_unsub", self._subscription_mode, token_list)
            except Exception as e:
                logger.warning("Unsubscribe failed: %s", e)

    def get_ltp(self, token: str) -> Optional[float]:
        """Thread-safe read of last traded price."""
        with self._lock:
            return self._ltp_data.get(str(token))

    def get_all_ltp(self) -> Dict[str, float]:
        """Thread-safe snapshot of all LTP values."""
        with self._lock:
            return dict(self._ltp_data)

    def get_candle_builder(self, token: str) -> Optional[CandleBuilder]:
        """Get the CandleBuilder for a token."""
        with self._lock:
            return self._candle_builders.get(str(token))

    def seed_candle_builder(self, token: str, raw_candles: list):
        """Seed a token's CandleBuilder with historical candles."""
        tok = str(token)
        with self._lock:
            if tok not in self._candle_builders:
                self._candle_builders[tok] = CandleBuilder(self._candle_interval)
            self._candle_builders[tok].seed_candles(raw_candles)

    def merge_candle_builder_history(self, token: str, raw_candles: list):
        """Merge historical finalized candles into an existing builder."""
        tok = str(token)
        with self._lock:
            if tok not in self._candle_builders:
                self._candle_builders[tok] = CandleBuilder(self._candle_interval)
            self._candle_builders[tok].merge_historical_candles(raw_candles)

    def get_last_tick_times(self) -> Dict[str, datetime]:
        with self._lock:
            return dict(self._last_tick_at)

    # ── WebSocket connection ──────────────────────────────────────────────

    def _run_ws(self):
        """WebSocket connection loop with reconnection."""
        try:
            from SmartApi.smartWebSocketV2 import SmartWebSocketV2
        except ImportError:
            logger.error("SmartWebSocketV2 not available — tick streaming disabled")
            self._running = False
            return

        while self._running:
            try:
                self._ws = SmartWebSocketV2(
                    auth_token=self._auth_token,
                    api_key=self._api_key,
                    client_code=self._client_code,
                    feed_token=self._feed_token,
                    max_retry_attempt=3,
                    retry_strategy=1,
                    retry_delay=5,
                    retry_multiplier=2,
                    retry_duration=60,
                )

                self._ws.on_open = self._on_open
                self._ws.on_data = self._on_data
                self._ws.on_error = self._on_error
                self._ws.on_close = self._on_close

                logger.info("Connecting to Angel One tick WebSocket...")
                self._ws.connect()  # Blocks until disconnect
            except Exception as e:
                logger.error("Tick WS connection error: %s", e)
                self._connected = False

            if self._running:
                logger.warning("Tick WS disconnected, reconnecting in 10s...")
                self._connected = False
                time.sleep(10)

    def _on_open(self, wsapp):
        """Called when WebSocket connects. Re-subscribe to saved tokens."""
        self._connected = True
        logger.info("Tick WS connected for client %s", self._client_code)

        with self._lock:
            token_list = self._subscribed_tokens

        if token_list:
            try:
                self._ws.subscribe("nebula_tick", self._subscription_mode, token_list)
                logger.info("Re-subscribed to %d groups on reconnect", len(token_list))
            except Exception as e:
                logger.warning("Re-subscribe on open failed: %s", e)
        if self._connection_open_callback and self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._connection_open_callback(), self._loop)

    def _on_data(self, wsapp, data):
        """
        Called on each tick. This is the HOT PATH.

        1. Update LTP in memory
        2. Feed CandleBuilder
        3. Evaluate entry/exit via TickEvaluator
        4. Broadcast to dashboard frontend
        """
        if not isinstance(data, dict):
            return

        token = str(data.get("token", "")).strip()
        if not token:
            return

        # Angel One sends prices in paisa (integer) — divide by 100
        raw_ltp = data.get("last_traded_price")
        if raw_ltp is None:
            return

        ltp = raw_ltp / 100.0
        exchange_time = self._extract_tick_timestamp(data)
        now = exchange_time or market_now()
        self._monitor_clock_drift(token, exchange_time)

        # 1. Update LTP + feed candle builder
        finalized_candle = None
        with self._lock:
            self._ltp_data[token] = ltp
            self._last_tick_at[token] = now

            builder = self._candle_builders.get(token)
            if builder:
                finalized_candle = builder.on_tick(ltp, now)

        # 2. Tick-level entry/exit evaluation (outside data lock)
        if self._tick_evaluator:
            try:
                action = self._tick_evaluator.on_tick(token, ltp)
                if action:
                    self._dispatch_tick_action(action)
            except Exception as e:
                logger.error("Tick evaluator error for token %s: %s", token, e)

        # 3. Broadcast to frontend dashboard
        symbol = self._token_symbols.get(token, token)
        if finalized_candle and self._candle_close_callback and self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self._candle_close_callback(token, symbol, finalized_candle),
                self._loop,
            )
        self._broadcast_tick(token, symbol, ltp, finalized_candle)

    def _broadcast_tick(self, token: str, symbol: str, ltp: float, finalized_candle=None):
        """Push tick data to frontend via WebSocket broadcast."""
        from app.engine.broadcast import ws_manager

        tick_data = {
            "type": "tick",
            "token": token,
            "symbol": symbol,
            "ltp": ltp,
            "timestamp": market_now().isoformat(),
        }

        if finalized_candle:
            tick_data["candle_closed"] = True
            tick_data["candle"] = {
                "time": finalized_candle.time,
                "open": finalized_candle.open,
                "high": finalized_candle.high,
                "low": finalized_candle.low,
                "close": finalized_candle.close,
            }

        # Schedule broadcast on the event loop (we're in a thread)
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(
                ws_manager.broadcast("market-data", tick_data), self._loop
            )

    def _dispatch_tick_action(self, action):
        """Dispatch tick-triggered entry/exit to the engine."""
        from app.engine.broadcast import ws_manager

        if action.action_type == "ENTRY":
            logger.info(
                "TICK ENTRY triggered: %s gap_id=%d @ %.2f",
                action.symbol, action.gap_id, action.trigger_price,
            )
            # Schedule async entry handling
            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._handle_tick_entry(action), self._loop
                )
        elif action.action_type == "EXIT":
            logger.info(
                "TICK EXIT triggered: %s pos_id=%d reason=%s @ %.2f",
                action.symbol, action.position_id, action.exit_reason,
                action.trigger_price,
            )
            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._handle_tick_exit(action), self._loop
                )

    async def _handle_tick_entry(self, action):
        """Handle a tick-triggered entry (runs on the event loop)."""
        from app.database import async_session_factory
        from app.engine.executor import execute_entry
        from app.models.engine import FVGGap
        from app.models.stock import GlobalConfig, WatchlistItem
        from sqlalchemy import select

        try:
            async with async_session_factory() as db:
                gap = (await db.execute(
                    select(FVGGap).where(FVGGap.id == action.gap_id)
                )).scalar_one_or_none()
                if not gap or not gap.is_active:
                    return

                wi = (await db.execute(
                    select(WatchlistItem).where(WatchlistItem.id == action.watchlist_item_id)
                )).scalar_one_or_none()
                if not wi:
                    return

                config = (await db.execute(
                    select(GlobalConfig).where(GlobalConfig.user_id == action.user_id)
                )).scalar_one_or_none()
                if not config:
                    return

                from app.broker.client import get_broker_client
                broker = get_broker_client()

                await execute_entry(
                    db, action.user_id, gap, wi, broker, config, action.trigger_price
                )
                await db.commit()
        except Exception as e:
            logger.error("Tick entry handler error: %s", e)

    async def _handle_tick_exit(self, action):
        """Handle a tick-triggered exit (runs on the event loop)."""
        from app.database import async_session_factory
        from app.engine.executor import execute_exit
        from app.models.engine import OpenPosition
        from app.models.stock import GlobalConfig, WatchlistItem
        from sqlalchemy import select

        try:
            async with async_session_factory() as db:
                position = (await db.execute(
                    select(OpenPosition).where(OpenPosition.id == action.position_id)
                )).scalar_one_or_none()
                if not position or not position.is_open:
                    return

                wi = (await db.execute(
                    select(WatchlistItem).where(
                        WatchlistItem.id == position.watchlist_item_id
                    )
                )).scalar_one_or_none()
                if not wi:
                    return

                config = (await db.execute(
                    select(GlobalConfig).where(GlobalConfig.user_id == action.user_id)
                )).scalar_one_or_none()
                if not config:
                    return

                from app.broker.client import get_broker_client
                broker = get_broker_client()

                await execute_exit(
                    db, action.user_id, position, wi, broker, config,
                    action.exit_price, action.exit_reason, action.trigger_price,
                )
                await db.commit()
        except Exception as e:
            logger.error("Tick exit handler error: %s", e)

    def _on_error(self, *args):
        self._connected = False
        error_msg = " ".join(str(a) for a in args)
        logger.error("Tick WS error: %s", error_msg)
        if self._connection_close_callback and self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self._connection_close_callback("ws_error", error_msg),
                self._loop,
            )

    def _on_close(self, wsapp):
        self._connected = False
        logger.warning("Tick WS closed for client %s", self._client_code)
        if self._connection_close_callback and self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self._connection_close_callback("ws_closed", "WebSocket closed"),
                self._loop,
            )

    def _extract_tick_timestamp(self, data: dict) -> datetime | None:
        for key in (
            "exchange_timestamp",
            "exchange_time",
            "last_traded_timestamp",
            "ltt",
            "timestamp",
            "exchangeTime",
            "last_traded_time",
        ):
            parsed = parse_exchange_timestamp(data.get(key))
            if parsed is not None:
                return parsed
        return None

    def _monitor_clock_drift(self, token: str, exchange_time: datetime | None):
        if exchange_time is None:
            return

        drift_seconds = abs((market_now() - exchange_time).total_seconds())
        if drift_seconds < 2.0:
            return

        now_mono = time.monotonic()
        last_warn = self._clock_drift_warned_at.get(token, 0.0)
        if now_mono - last_warn < 60:
            return

        self._clock_drift_warned_at[token] = now_mono
        logger.warning(
            "Clock drift detected for token %s: local vs exchange time differs by %.2fs",
            token,
            drift_seconds,
        )
