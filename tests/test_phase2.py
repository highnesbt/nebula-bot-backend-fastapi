"""Phase 2 tests — stock and engine API endpoints."""

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.engine import FVGGap, OpenPosition, PendingOrder, Signal
from app.models.stock import WatchlistItem
from app.models.user import BrokerCredential, User
from app.engine.runtime import get_runtime


# ── Config tests ──────────────────────────────────────────────────────────────


class TestConfigAPI:
    @pytest.mark.asyncio
    async def test_get_config_creates_default(self, client, auth_headers):
        resp = await client.get("/api/config/", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["candle_interval"] == "FIVE_MINUTE"
        assert data["atr_multiplier"] == 1.5
        assert data["entry_order_timeout"] == 60
        assert data["otr_price_band_pct"] == 0.75
        assert data["is_active"] is False

    @pytest.mark.asyncio
    async def test_update_config(self, client, auth_headers):
        await client.get("/api/config/", headers=auth_headers)
        resp = await client.put(
            "/api/config/",
            json={"atr_multiplier": 2.0, "entry_order_timeout": 45},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["atr_multiplier"] == 2.0
        assert data["entry_order_timeout"] == 45
        assert data["candle_interval"] == "FIVE_MINUTE"

    @pytest.mark.asyncio
    async def test_update_config_time_fields(self, client, auth_headers):
        await client.get("/api/config/", headers=auth_headers)
        resp = await client.put(
            "/api/config/",
            json={"auto_exit_time": "15:20", "no_entry_after": "14:45"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["auto_exit_time"] == "15:20"
        assert data["no_entry_after"] == "14:45"


# ── Watchlist tests ───────────────────────────────────────────────────────────


class TestWatchlistAPI:
    @pytest.mark.asyncio
    async def test_add_and_list(self, client, auth_headers):
        resp = await client.post(
            "/api/watchlist/",
            json={"symbol": "SBIN-EQ", "token": "3045", "quantity": 10},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["symbol"] == "SBIN-EQ"
        assert data["quantity"] == 10

        resp = await client.get("/api/watchlist/", headers=auth_headers)
        assert resp.status_code == 200
        items = resp.json()
        assert len(items) == 1
        assert items[0]["symbol"] == "SBIN-EQ"

    @pytest.mark.asyncio
    async def test_add_duplicate(self, client, auth_headers):
        await client.post(
            "/api/watchlist/",
            json={"symbol": "SBIN-EQ", "token": "3045", "quantity": 5},
            headers=auth_headers,
        )
        resp = await client.post(
            "/api/watchlist/",
            json={"symbol": "SBIN-EQ", "token": "3045", "quantity": 5},
            headers=auth_headers,
        )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_update_watchlist_item(self, client, auth_headers):
        resp = await client.post(
            "/api/watchlist/",
            json={"symbol": "RELIANCE-EQ", "token": "2885", "quantity": 1},
            headers=auth_headers,
        )
        item_id = resp.json()["id"]
        resp = await client.put(
            f"/api/watchlist/{item_id}",
            json={"quantity": 25},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["quantity"] == 25

    @pytest.mark.asyncio
    async def test_delete_watchlist_item(self, client, auth_headers):
        resp = await client.post(
            "/api/watchlist/",
            json={"symbol": "TCS-EQ", "token": "11536", "quantity": 2},
            headers=auth_headers,
        )
        item_id = resp.json()["id"]
        resp = await client.delete(f"/api/watchlist/{item_id}", headers=auth_headers)
        assert resp.status_code == 200
        resp = await client.get("/api/watchlist/", headers=auth_headers)
        assert len(resp.json()) == 0

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, client, auth_headers):
        resp = await client.get("/api/watchlist/9999", headers=auth_headers)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_add_watchlist_item_backfills_and_places_manual_entry(
        self, client, auth_headers, db_session, test_user
    ):
        from datetime import timedelta

        from app.engine.time_utils import market_now
        from app.models.stock import GlobalConfig
        from app.engine.runtime import start_runtime, stop_runtime

        db_session.add(GlobalConfig(
            user_id=test_user.id,
            is_active=True,
            trend_ema_period=2,
            trend_slope_lookback=1,
            trend_slope_threshold=0.01,
            atr_period=2,
            min_gap_percent=0.0,
        ))

        cred = BrokerCredential(user_id=test_user.id)
        cred.api_key = "KEY"
        cred.client_id = "CID"
        cred.password = "PW"
        cred.totp_secret = "TS"
        db_session.add(cred)
        await db_session.commit()

        runtime = await start_runtime(db_session, test_user.id)

        now = market_now().replace(second=0, microsecond=0)
        base = now - timedelta(minutes=25)
        base = base.replace(minute=(base.minute // 5) * 5)
        candles = [
            [base.isoformat(), 100, 101, 99, 100, 1000],
            [(base + timedelta(minutes=5)).isoformat(), 100, 102, 99.5, 101, 1100],
            [(base + timedelta(minutes=10)).isoformat(), 101, 104, 103, 103.5, 1200],
        ]

        runtime.broker.get_candle_data = lambda **kwargs: candles
        runtime.broker.get_ltp = lambda exchange, symbol, token: 101.55

        resp = await client.post(
            "/api/watchlist/",
            json={"symbol": "SBIN-EQ", "token": "3045", "quantity": 1},
            headers=auth_headers,
        )
        assert resp.status_code == 200

        result = await db_session.execute(select(FVGGap).where(FVGGap.user_id == test_user.id))
        gaps = list(result.scalars())
        assert len(gaps) == 1
        assert gaps[0].gap_type == "LONG"

        pending_result = await db_session.execute(
            select(PendingOrder).where(
                PendingOrder.user_id == test_user.id,
                PendingOrder.order_type == "ENTRY",
                PendingOrder.status == "OPEN",
            )
        )
        pending = pending_result.scalar_one_or_none()
        assert pending is not None

        builder = runtime.tick_manager.get_candle_builder("3045")
        assert builder is not None
        assert len(builder.get_all_candles()) == 3

        await stop_runtime(db_session, test_user.id, cancel_pending_orders=False)

    @pytest.mark.asyncio
    async def test_retry_backrun_after_failed_watchlist_add(
        self, client, auth_headers, db_session, test_user
    ):
        from app.models.stock import GlobalConfig
        from app.engine.runtime import start_runtime, stop_runtime

        db_session.add(GlobalConfig(user_id=test_user.id, is_active=True))
        cred = BrokerCredential(user_id=test_user.id)
        cred.api_key = "KEY"
        cred.client_id = "CID"
        cred.password = "PW"
        cred.totp_secret = "TS"
        db_session.add(cred)
        await db_session.commit()

        runtime = await start_runtime(db_session, test_user.id)
        runtime.broker.get_candle_data = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("historical unavailable"))

        add_resp = await client.post(
            "/api/watchlist/",
            json={"symbol": "SBIN-EQ", "token": "3045", "quantity": 1},
            headers=auth_headers,
        )
        assert add_resp.status_code == 200
        added = add_resp.json()
        assert added["backrun_status"] == "FAILED"
        assert "Backrun failed" in added["backrun_error"]

        runtime.broker.get_candle_data = lambda **kwargs: [
            ["2026-03-30T09:15:00+05:30", 100, 101, 99, 100, 1000],
            ["2026-03-30T09:20:00+05:30", 100, 102, 99.5, 101, 1100],
            ["2026-03-30T09:25:00+05:30", 101, 104, 103, 103.5, 1200],
        ]
        runtime.broker.get_ltp = lambda exchange, symbol, token: 101.55

        retry_resp = await client.post(
            f"/api/watchlist/{added['id']}/retry-backrun",
            headers=auth_headers,
        )
        assert retry_resp.status_code == 200
        retried = retry_resp.json()
        assert retried["backrun_status"] == "SUCCESS"
        assert retried["backrun_error"] == ""

        await stop_runtime(db_session, test_user.id, cancel_pending_orders=False)


# ── Engine Status tests ───────────────────────────────────────────────────────


class TestEngineAPI:
    @pytest.mark.asyncio
    async def test_engine_status(self, client, auth_headers):
        resp = await client.get("/api/engine/status", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["is_active"] is False
        assert data["open_positions"] == 0
        assert data["active_gaps"] == 0

    @pytest.mark.asyncio
    async def test_engine_status_clears_stale_active_flag(
        self, client, auth_headers, db_session, test_user
    ):
        from app.models.stock import GlobalConfig

        db_session.add(GlobalConfig(user_id=test_user.id, is_active=True))
        await db_session.commit()

        resp = await client.get("/api/engine/status", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["is_active"] is False

        result = await db_session.execute(
            select(GlobalConfig).where(GlobalConfig.user_id == test_user.id)
        )
        config = result.scalar_one()
        assert config.is_active is False

    @pytest.mark.asyncio
    async def test_start_stop_engine(self, client, auth_headers, db_session, test_user):
        cred = BrokerCredential(user_id=test_user.id)
        cred.api_key = "KEY"
        cred.client_id = "CID"
        cred.password = "PW"
        cred.totp_secret = "TS"
        db_session.add(cred)
        await db_session.commit()

        resp = await client.post("/api/engine/start", headers=auth_headers)
        assert resp.status_code == 200
        assert "started" in resp.json()["message"]
        assert get_runtime(1) is not None

        resp = await client.post("/api/engine/start", headers=auth_headers)
        assert resp.status_code == 400

        resp = await client.post("/api/engine/stop", headers=auth_headers)
        assert resp.status_code == 200
        assert "stopped" in resp.json()["message"]
        assert get_runtime(1) is None

        resp = await client.post("/api/engine/stop", headers=auth_headers)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_exit_all_positions(self, client, auth_headers, db_session, test_user):
        cred = BrokerCredential(user_id=test_user.id)
        cred.api_key = "KEY"
        cred.client_id = "CID"
        cred.password = "PW"
        cred.totp_secret = "TS"
        db_session.add(cred)

        wi = WatchlistItem(
            user_id=test_user.id, symbol="INFY-EQ", token="1594", quantity=5,
        )
        db_session.add(wi)
        await db_session.flush()

        pos = OpenPosition(
            user_id=test_user.id,
            watchlist_item_id=wi.id,
            symbol="INFY-EQ",
            position_type="LONG",
            quantity=5,
            entry_price=90.0,
            stop_loss=88.0,
            take_profit=100.0,
            is_open=True,
        )
        db_session.add(pos)
        await db_session.commit()

        resp = await client.post("/api/engine/exit-all", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["closed"] == 1
        assert data["failed"] == 0
        assert data["total_pnl"] == 50.0


# ── Gaps tests ────────────────────────────────────────────────────────────────


class TestGapsAPI:
    @pytest.mark.asyncio
    async def test_list_gaps_empty(self, client, auth_headers):
        resp = await client.get("/api/engine/gaps", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == []

    @pytest.mark.asyncio
    async def test_list_gaps_with_data(self, client, auth_headers, db_session, test_user):
        wi = WatchlistItem(
            user_id=test_user.id, symbol="SBIN-EQ", token="3045", quantity=10,
        )
        db_session.add(wi)
        await db_session.flush()

        gap = FVGGap(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="SBIN-EQ",
            gap_type="LONG", gap_high=100.5, gap_low=99.0,
            entry_price=99.45, stop_loss=98.5, take_profit=101.5, is_active=True,
        )
        db_session.add(gap)
        await db_session.commit()

        resp = await client.get("/api/engine/gaps", headers=auth_headers)
        assert resp.status_code == 200
        gaps = resp.json()
        assert len(gaps) == 1
        assert gaps[0]["symbol"] == "SBIN-EQ"
        assert gaps[0]["gap_type"] == "LONG"


# ── Positions tests ───────────────────────────────────────────────────────────


class TestPositionsAPI:
    @pytest.mark.asyncio
    async def test_list_positions_empty(self, client, auth_headers):
        resp = await client.get("/api/engine/positions", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == []

    @pytest.mark.asyncio
    async def test_positions_with_data(self, client, auth_headers, db_session, test_user):
        wi = WatchlistItem(
            user_id=test_user.id, symbol="INFY-EQ", token="1594", quantity=5,
        )
        db_session.add(wi)
        await db_session.flush()

        pos = OpenPosition(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="INFY-EQ",
            position_type="LONG", quantity=5, entry_price=1500.0,
            stop_loss=1480.0, take_profit=1540.0, is_open=True,
        )
        db_session.add(pos)
        await db_session.commit()

        resp = await client.get("/api/engine/positions", headers=auth_headers)
        assert resp.status_code == 200
        positions = resp.json()
        assert len(positions) == 1
        assert positions[0]["symbol"] == "INFY-EQ"
        assert positions[0]["entry_price"] == 1500.0

    @pytest.mark.asyncio
    async def test_position_history(self, client, auth_headers, db_session, test_user):
        wi = WatchlistItem(
            user_id=test_user.id, symbol="TCS-EQ", token="11536", quantity=2,
        )
        db_session.add(wi)
        await db_session.flush()

        db_session.add(OpenPosition(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="TCS-EQ",
            position_type="LONG", quantity=2, entry_price=3500.0, is_open=True,
        ))
        db_session.add(OpenPosition(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="TCS-EQ",
            position_type="SHORT", quantity=2, entry_price=3400.0,
            is_open=False, pnl=100.0,
        ))
        await db_session.commit()

        resp = await client.get("/api/engine/positions/history", headers=auth_headers)
        assert resp.status_code == 200
        assert len(resp.json()) == 2


class TestCandlesAPI:
    @pytest.mark.asyncio
    async def test_completed_candles_requires_running_engine(self, client, auth_headers):
        resp = await client.get("/api/engine/candles", params={"token": "3045"}, headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == []

    @pytest.mark.asyncio
    async def test_completed_candles_returns_empty_when_builder_not_ready(
        self, client, auth_headers, db_session, test_user
    ):
        from app.engine.runtime import start_runtime, stop_runtime
        from app.models.stock import GlobalConfig, WatchlistItem
        from app.models.user import BrokerCredential

        db_session.add(GlobalConfig(user_id=test_user.id, is_active=True))
        cred = BrokerCredential(user_id=test_user.id)
        cred.api_key = "KEY"
        cred.client_id = "CID"
        cred.password = "PW"
        cred.totp_secret = "TS"
        db_session.add(cred)

        wi = WatchlistItem(
            user_id=test_user.id, symbol="SBIN-EQ", token="3045", quantity=1,
        )
        db_session.add(wi)
        await db_session.commit()

        await start_runtime(db_session, test_user.id)

        resp = await client.get("/api/engine/candles", params={"token": "9999"}, headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == []

        await stop_runtime(db_session, test_user.id, cancel_pending_orders=False)

    @pytest.mark.asyncio
    async def test_completed_candles_returns_built_candles(
        self, client, auth_headers, db_session, test_user
    ):
        from app.engine.runtime import start_runtime, stop_runtime
        from app.models.user import BrokerCredential

        cred = BrokerCredential(user_id=test_user.id)
        cred.api_key = "KEY"
        cred.client_id = "CID"
        cred.password = "PW"
        cred.totp_secret = "TS"
        db_session.add(cred)

        wi = WatchlistItem(
            user_id=test_user.id, symbol="SBIN-EQ", token="3045", quantity=1,
        )
        db_session.add(wi)
        await db_session.commit()

        runtime = await start_runtime(db_session, test_user.id)
        runtime.tick_manager.seed_candle_builder("3045", [
            ["2024-01-01T09:15:00", 100, 105, 95, 102, 1000],
            ["2024-01-01T09:20:00", 102, 108, 100, 106, 2000],
        ])

        resp = await client.get("/api/engine/candles", params={"token": "3045"}, headers=auth_headers)
        assert resp.status_code == 200
        candles = resp.json()
        assert len(candles) == 2
        assert candles[0]["open"] == 100.0
        assert candles[1]["close"] == 106.0

        await stop_runtime(db_session, test_user.id, cancel_pending_orders=False)


# ── Signals tests ─────────────────────────────────────────────────────────────


class TestSignalsAPI:
    @pytest.mark.asyncio
    async def test_list_signals_empty(self, client, auth_headers):
        resp = await client.get("/api/engine/signals", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["results"] == []

    @pytest.mark.asyncio
    async def test_signals_with_data(self, client, auth_headers, db_session, test_user):
        signal = Signal(
            user_id=test_user.id, symbol="SBIN-EQ", signal_type="LONG_ENTRY",
            price=250.0, order_id="TEST001", order_status="complete",
        )
        db_session.add(signal)
        await db_session.commit()

        resp = await client.get("/api/engine/signals", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["results"][0]["signal_type"] == "LONG_ENTRY"

    @pytest.mark.asyncio
    async def test_signal_detail(self, client, auth_headers, db_session, test_user):
        signal = Signal(
            user_id=test_user.id, symbol="SBIN-EQ", signal_type="SHORT_EXIT",
            price=248.0, order_id="TEST002", order_status="complete",
            pnl=50.0, exit_reason="TSL_HIT",
        )
        db_session.add(signal)
        await db_session.commit()
        await db_session.refresh(signal)

        resp = await client.get(
            f"/api/engine/signals/{signal.id}", headers=auth_headers
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["exit_reason"] == "TSL_HIT"
        assert data["pnl"] == 50.0

    @pytest.mark.asyncio
    async def test_signal_not_found(self, client, auth_headers):
        resp = await client.get("/api/engine/signals/9999", headers=auth_headers)
        assert resp.status_code == 404
