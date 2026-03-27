"""Phase 5 tests — TickDataManager, recovery helpers, and broadcast."""

import asyncio
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.engine.broadcast import ConnectionManager
from app.engine.runtime import _format_recovery_time, _recover_missing_candles
from app.engine.tick_manager import TickDataManager
from app.engine.time_utils import IST, parse_exchange_timestamp
from app.models.stock import GlobalConfig, WatchlistItem


class TestTickDataManager:
    """Unit tests for TickDataManager (no real WS connection needed)."""

    def test_subscribe_tracks_tokens(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        mgr.subscribe([
            {"exchange": "NSE", "token": "3045", "symbol": "SBIN-EQ"},
            {"exchange": "NSE", "token": "2885", "symbol": "RELIANCE-EQ"},
        ])
        assert mgr.get_ltp("3045") is None  # No ticks yet
        assert mgr.get_candle_builder("3045") is not None
        assert mgr.get_candle_builder("2885") is not None

    def test_subscribe_idempotent(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        tokens = [{"exchange": "NSE", "token": "3045", "symbol": "SBIN-EQ"}]
        mgr.subscribe(tokens)
        mgr.subscribe(tokens)  # Should not error — idempotent
        assert mgr.get_candle_builder("3045") is not None

    def test_seed_candle_builder(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        raw = [
            ["2024-01-01T09:15:00", 100, 105, 95, 102, 1000],
            ["2024-01-01T09:20:00", 102, 108, 100, 106, 2000],
        ]
        mgr.seed_candle_builder("3045", raw)
        builder = mgr.get_candle_builder("3045")
        assert builder is not None
        assert len(builder.get_all_candles()) == 2

    def test_get_all_ltp_empty(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        assert mgr.get_all_ltp() == {}

    def test_on_data_updates_ltp(self):
        """Simulate _on_data being called with tick data."""
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        mgr.subscribe([{"exchange": "NSE", "token": "3045", "symbol": "SBIN-EQ"}])

        # Simulate a tick (prices in paisa)
        mgr._on_data(None, {"token": "3045", "last_traded_price": 25050})

        assert mgr.get_ltp("3045") == 250.50

    def test_on_data_feeds_candle_builder(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        mgr.subscribe([{"exchange": "NSE", "token": "3045", "symbol": "SBIN-EQ"}])

        # Send multiple ticks
        mgr._on_data(None, {"token": "3045", "last_traded_price": 25050})
        mgr._on_data(None, {"token": "3045", "last_traded_price": 25100})
        mgr._on_data(None, {"token": "3045", "last_traded_price": 24900})

        builder = mgr.get_candle_builder("3045")
        latest = builder.get_latest_candle()
        assert latest is not None
        assert latest.high == 251.0
        assert latest.low == 249.0

    def test_unsubscribe(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        mgr.subscribe([{"exchange": "NSE", "token": "3045", "symbol": "SBIN-EQ"}])
        mgr.unsubscribe([{"exchange": "NSE", "token": "3045"}])
        # Subscribed set should be empty now
        assert len(mgr._subscribed_set) == 0

    def test_merge_candle_builder_history(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        mgr.seed_candle_builder("3045", [
            ["2024-01-01T09:15:00", 100, 105, 95, 102, 1000],
        ])
        mgr.merge_candle_builder_history("3045", [
            ["2024-01-01T09:20:00", 102, 108, 100, 106, 2000],
        ])
        builder = mgr.get_candle_builder("3045")
        assert len(builder.get_all_candles()) == 2

    def test_connection_callbacks(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        mgr.set_connection_callbacks(
            on_open=AsyncMock(),
            on_close=AsyncMock(),
        )
        mgr._loop = MagicMock()
        mgr._loop.is_running.return_value = True

        with patch("asyncio.run_coroutine_threadsafe") as run_threadsafe:
            mgr._on_open(None)
            mgr._on_close(None)
            assert run_threadsafe.call_count == 2
            for call in run_threadsafe.call_args_list:
                call.args[0].close()

    def test_exchange_timestamp_is_used_for_tick(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        mgr.subscribe([{"exchange": "NSE", "token": "3045", "symbol": "SBIN-EQ"}])
        mgr._on_data(None, {
            "token": "3045",
            "last_traded_price": 25050,
            "exchange_timestamp": "2024-01-01T09:15:01+05:30",
        })
        last_tick = mgr.get_last_tick_times()["3045"]
        assert last_tick == datetime.datetime(2024, 1, 1, 9, 15, 1, tzinfo=IST)

    def test_default_subscription_mode_is_quote(self):
        mgr = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        assert mgr._subscription_mode == 2


class TestRecoveryHelpers:
    def test_format_recovery_time_rounds_down(self):
        ts = datetime.datetime(2024, 1, 1, 9, 18, 42)
        assert _format_recovery_time(ts) == "2024-01-01 09:15"

    @pytest.mark.asyncio
    async def test_recover_missing_candles_merges_history(self):
        class Broker:
            def get_candle_data(self, **kwargs):
                return [
                    ["2024-01-01T09:20:00", 102, 108, 100, 106, 2000],
                ]

        tick_manager = TickDataManager(
            auth_token="x", feed_token="x", api_key="x", client_code="C1",
        )
        tick_manager.seed_candle_builder("3045", [
            ["2024-01-01T09:15:00", 100, 105, 95, 102, 1000],
        ])

        runtime = MagicMock()
        runtime.user_id = 1
        runtime.config = GlobalConfig(user_id=1)
        runtime.watchlist_by_token = {
            "3045": WatchlistItem(
                user_id=1, symbol="SBIN-EQ", token="3045", exchange="NSE", quantity=1
            )
        }
        runtime.tick_manager = tick_manager
        runtime.broker = Broker()
        runtime.disconnected_at = datetime.datetime(2024, 1, 1, 9, 18, 0)
        runtime.refresh_thresholds = AsyncMock()

        db = MagicMock()
        ok = await _recover_missing_candles(db, runtime)
        assert ok is True
        candles = tick_manager.get_candle_builder("3045").get_all_candles()
        assert len(candles) == 2


class TestTimeParsing:
    def test_parse_exchange_timestamp_epoch_ms(self):
        parsed = parse_exchange_timestamp(1704080701000)
        assert parsed is not None
        assert parsed.tzinfo == IST

    def test_parse_exchange_timestamp_iso(self):
        parsed = parse_exchange_timestamp("2024-01-01T09:15:01+05:30")
        assert parsed == datetime.datetime(2024, 1, 1, 9, 15, 1, tzinfo=IST)


class TestConnectionManager:
    """Tests for the WebSocket broadcast manager."""

    @pytest.mark.asyncio
    async def test_broadcast_no_connections(self):
        """Broadcasting with no connections should not error."""
        manager = ConnectionManager()
        await manager.broadcast("test-channel", {"msg": "hello"})

    @pytest.mark.asyncio
    async def test_broadcast_scan_update(self):
        manager = ConnectionManager()
        # Should not error even with no connections
        await manager.broadcast_scan_update("SBIN-EQ", {"ltp": 250.5, "trend": "UP"})

    @pytest.mark.asyncio
    async def test_broadcast_trade_signal(self):
        manager = ConnectionManager()
        await manager.broadcast_trade_signal({
            "signal": "LONG_ENTRY", "symbol": "SBIN-EQ", "price": 250.5,
        })

    @pytest.mark.asyncio
    async def test_broadcast_engine_event(self):
        manager = ConnectionManager()
        await manager.broadcast_engine_event("auto_exit_complete", {"closed": 2})
