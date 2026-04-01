"""Phase 5 tests — TickDataManager, recovery helpers, and broadcast."""

import asyncio
import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.broker.smart_client import AngelOneClient
from app.config import settings
from app.engine.broadcast import ConnectionManager
from app.engine.runtime import (
    _RUNTIMES,
    _format_recovery_time,
    _recover_missing_candles,
    refresh_runtime_broker_session,
)
from app.engine.tick_manager import TickDataManager
from app.engine.time_utils import IST, parse_exchange_timestamp
from app.models.stock import GlobalConfig, WatchlistItem
from app.models.user import BrokerCredential


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

    def test_restart_with_credentials_updates_session_values(self):
        mgr = TickDataManager(
            auth_token="old-auth", feed_token="old-feed", api_key="old-key", client_code="OLD",
        )
        with patch.object(mgr, "stop") as stop_mock, patch.object(mgr, "start") as start_mock:
            mgr.restart_with_credentials("new-auth", "new-feed", "new-key", "NEW")

        stop_mock.assert_called_once()
        start_mock.assert_called_once()
        assert mgr._auth_token == "new-auth"
        assert mgr._feed_token == "new-feed"
        assert mgr._api_key == "new-key"
        assert mgr._client_code == "NEW"


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

    @pytest.mark.asyncio
    async def test_refresh_runtime_broker_session_restarts_live_runtime(
        self, db_session, test_user, monkeypatch
    ):
        old_backend = settings.NEBULA_BROKER_BACKEND
        monkeypatch.setattr(settings, "NEBULA_BROKER_BACKEND", "live")

        cred = BrokerCredential(user_id=test_user.id)
        cred.api_key = "KEY"
        cred.client_id = "CID"
        cred.password = "PIN"
        cred.totp_secret = "JBSWY3DPEHPK3PXP"
        db_session.add(cred)
        await db_session.commit()

        old_order_updates = MagicMock()
        old_polling_task = MagicMock()
        tick_manager = MagicMock()
        tick_manager._api_key = "OLDKEY"
        tick_manager._client_code = "OLDCID"

        broker = MagicMock()
        broker._session_listener = object()
        broker.login.return_value = {
            "status": True,
            "data": {
                "jwtToken": "JWT2",
                "feedToken": "FEED2",
                "refreshToken": "REF2",
                "clientcode": "CID",
            },
        }
        broker.session_tokens = {
            "auth_token": "JWT2",
            "feed_token": "FEED2",
            "refresh_token": "REF2",
            "api_key": "KEY",
            "client_code": "CID",
        }

        runtime = SimpleNamespace(
            user_id=test_user.id,
            broker=broker,
            tick_manager=tick_manager,
            order_updates=old_order_updates,
            polling_task=old_polling_task,
            session_refresh_in_progress=False,
            _handle_order_update=AsyncMock(),
        )
        _RUNTIMES[test_user.id] = runtime

        class FakeOrderUpdateManager:
            def __init__(self):
                self.is_running = True
                self.started = None

            def start(self, *args, **kwargs):
                self.started = (args, kwargs)

            def stop(self):
                return None

        try:
            with patch("app.engine.runtime.OrderUpdateManager", FakeOrderUpdateManager):
                refreshed = await refresh_runtime_broker_session(
                    db_session,
                    test_user.id,
                    restart_streams=True,
                )

            assert refreshed is True
            assert cred.jwt_token == "JWT2"
            assert cred.feed_token == "FEED2"
            assert cred.refresh_token == "REF2"
            broker.login.assert_called_once_with("CID", "PIN", "JBSWY3DPEHPK3PXP")
            old_order_updates.stop.assert_called_once()
            old_polling_task.cancel.assert_called_once()
            tick_manager.restart_with_credentials.assert_called_once_with(
                auth_token="JWT2",
                feed_token="FEED2",
                api_key="KEY",
                client_code="CID",
            )
            assert runtime.order_updates is not None
            assert isinstance(runtime.order_updates, FakeOrderUpdateManager)
            assert runtime.order_updates.started is not None
        finally:
            _RUNTIMES.pop(test_user.id, None)
            monkeypatch.setattr(settings, "NEBULA_BROKER_BACKEND", old_backend)


class TestTimeParsing:
    def test_parse_exchange_timestamp_epoch_ms(self):
        parsed = parse_exchange_timestamp(1704080701000)
        assert parsed is not None
        assert parsed.tzinfo == IST

    def test_parse_exchange_timestamp_iso(self):
        parsed = parse_exchange_timestamp("2024-01-01T09:15:01+05:30")
        assert parsed == datetime.datetime(2024, 1, 1, 9, 15, 1, tzinfo=IST)


class TestSmartClientReauth:
    def test_invalid_session_reauthenticates_and_retries(self):
        class DummySmart:
            def __init__(self):
                self.access_token = ""
                self.feed_token = ""
                self.refresh_token = ""
                self.userId = ""
                self.generate_calls = 0
                self.candle_calls = 0

            def generateSession(self, client_id, password, totp):
                self.generate_calls += 1
                self.access_token = f"JWT{self.generate_calls}"
                self.feed_token = f"FEED{self.generate_calls}"
                self.refresh_token = f"REF{self.generate_calls}"
                self.userId = client_id
                return {
                    "status": True,
                    "data": {
                        "jwtToken": self.access_token,
                        "feedToken": self.feed_token,
                        "refreshToken": self.refresh_token,
                        "clientcode": client_id,
                    },
                }

            def getCandleData(self, params):
                self.candle_calls += 1
                if self.candle_calls == 1:
                    return {
                        "status": False,
                        "message": "Invalid Session or Session is Expired Please Re-login",
                        "errorcode": "AB1010",
                        "data": None,
                    }
                return {
                    "status": True,
                    "data": [["2024-01-01T09:15:00", 100, 101, 99, 100, 1000]],
                }

        client = AngelOneClient(api_key="KEY")
        client.smart = DummySmart()
        client.login("CID", "PIN", "JBSWY3DPEHPK3PXP")

        candles = client.get_candle_data(
            symbol_token="3045",
            interval="FIVE_MINUTE",
            from_date="2024-01-01 09:15",
            to_date="2024-01-01 09:20",
        )

        assert len(candles) == 1
        assert client.smart.generate_calls == 2
        assert client.smart.candle_calls == 2
        assert client.session_tokens["auth_token"] == "JWT2"


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
