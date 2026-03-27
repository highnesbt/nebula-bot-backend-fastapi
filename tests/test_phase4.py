"""Phase 4 tests — Order lifecycle management."""

import asyncio

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.mock_client import MockBrokerClient
from app.engine.order_manager import OrderManager
from app.engine.pricing import round_to_tick_size
from app.engine.rate_limiter import OrderRateLimiter
from app.models.engine import AuditLog, FVGGap, OpenPosition, PendingOrder, Signal
from app.models.stock import GlobalConfig, WatchlistItem
from app.models.user import User


# ── Pricing tests ─────────────────────────────────────────────────────────────


class TestPricing:
    def test_round_to_tick_size_basic(self):
        assert round_to_tick_size(100.03) == 100.05
        assert round_to_tick_size(100.07) == 100.05
        assert round_to_tick_size(100.08) == 100.10
        assert round_to_tick_size(100.00) == 100.00

    def test_round_to_tick_size_zero(self):
        assert round_to_tick_size(0) == 0.0
        assert round_to_tick_size(-5) == 0.0


# ── Rate limiter tests ────────────────────────────────────────────────────────


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_acquire_within_limit(self):
        limiter = OrderRateLimiter(max_per_second=5)
        for _ in range(5):
            await limiter.acquire()
        assert limiter.tokens_remaining == 0

    @pytest.mark.asyncio
    async def test_tokens_remaining(self):
        limiter = OrderRateLimiter(max_per_second=3)
        assert limiter.tokens_remaining == 3
        await limiter.acquire()
        assert limiter.tokens_remaining == 2


# ── OrderManager tests ────────────────────────────────────────────────────────


class TestOrderManager:
    @pytest.mark.asyncio
    async def test_entry_fill_creates_position(self, db_session: AsyncSession, test_user: User):
        """When an entry order fills, an OpenPosition should be created."""
        broker = MockBrokerClient()
        manager = OrderManager(broker)

        # Setup: watchlist item + gap + pending order
        wi = WatchlistItem(
            user_id=test_user.id, symbol="SBIN-EQ", token="3045",
            quantity=10, exchange="NSE",
        )
        db_session.add(wi)
        await db_session.flush()

        gap = FVGGap(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="SBIN-EQ",
            gap_type="LONG", gap_high=252.0, gap_low=250.0,
            entry_price=250.6, stop_loss=248.0, take_profit=254.0,
            is_active=True,
        )
        db_session.add(gap)
        await db_session.flush()

        pending = PendingOrder(
            user_id=test_user.id, watchlist_item_id=wi.id,
            order_id="MOCK001", order_type="ENTRY", direction="BUY",
            gap_id=gap.id, placed_price=250.6, total_quantity=10,
            status="OPEN",
        )
        db_session.add(pending)
        await db_session.commit()

        # Act: simulate fill
        await manager.on_order_complete(db_session, pending, fill_price=250.5, filled_qty=10)
        await db_session.commit()

        # Assert: PendingOrder is COMPLETE
        assert pending.status == "COMPLETE"
        assert pending.filled_price == 250.5

        # Assert: OpenPosition was created
        from sqlalchemy import select
        pos_result = await db_session.execute(
            select(OpenPosition).where(OpenPosition.user_id == test_user.id)
        )
        position = pos_result.scalar_one()
        assert position.entry_price == 250.5
        assert position.position_type == "LONG"
        assert position.is_open is True
        assert position.stop_loss == 248.0

        # Assert: Gap is deactivated
        await db_session.refresh(gap)
        assert gap.is_active is False

    @pytest.mark.asyncio
    async def test_exit_fill_closes_position(self, db_session: AsyncSession, test_user: User):
        """When an exit order fills, the OpenPosition should be closed."""
        broker = MockBrokerClient()
        manager = OrderManager(broker)

        wi = WatchlistItem(
            user_id=test_user.id, symbol="SBIN-EQ", token="3045",
            quantity=10, exchange="NSE",
        )
        db_session.add(wi)
        await db_session.flush()

        pos = OpenPosition(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="SBIN-EQ",
            position_type="LONG", quantity=10, entry_price=250.5,
            stop_loss=248.0, take_profit=254.0, is_open=True,
        )
        db_session.add(pos)
        await db_session.flush()

        pending = PendingOrder(
            user_id=test_user.id, watchlist_item_id=wi.id,
            order_id="MOCK002", order_type="EXIT", direction="SELL",
            position_id=pos.id, placed_price=254.0, total_quantity=10,
            status="OPEN",
        )
        db_session.add(pending)
        await db_session.commit()

        # Act: simulate fill
        await manager.on_order_complete(db_session, pending, fill_price=253.8, filled_qty=10)
        await db_session.commit()

        # Assert
        assert pending.status == "COMPLETE"
        await db_session.refresh(pos)
        assert pos.is_open is False
        assert pos.exit_price == 253.8
        assert pos.pnl == round((253.8 - 250.5) * 10, 2)

    @pytest.mark.asyncio
    async def test_entry_cancel_reactivates_gap(self, db_session: AsyncSession, test_user: User):
        """When an entry order is cancelled, the gap should be reactivated."""
        broker = MockBrokerClient()
        manager = OrderManager(broker)

        wi = WatchlistItem(
            user_id=test_user.id, symbol="SBIN-EQ", token="3045",
            quantity=10, exchange="NSE",
        )
        db_session.add(wi)
        await db_session.flush()

        gap = FVGGap(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="SBIN-EQ",
            gap_type="LONG", gap_high=252.0, gap_low=250.0,
            entry_price=250.6, stop_loss=248.0, take_profit=254.0,
            is_active=False,  # was deactivated when order was placed
        )
        db_session.add(gap)
        await db_session.flush()

        pending = PendingOrder(
            user_id=test_user.id, watchlist_item_id=wi.id,
            order_id="MOCK003", order_type="ENTRY", direction="BUY",
            gap_id=gap.id, placed_price=250.6, total_quantity=10,
            status="OPEN",
        )
        db_session.add(pending)
        await db_session.commit()

        # Act: cancel
        await manager.on_order_cancelled(db_session, pending, reason="ENTRY_TIMEOUT")
        await db_session.commit()

        # Assert
        assert pending.status == "CANCELLED"
        await db_session.refresh(gap)
        assert gap.is_active is True  # reactivated!

    @pytest.mark.asyncio
    async def test_exit_cancel_triggers_retry(self, db_session: AsyncSession, test_user: User):
        """When an exit order is cancelled, a retry order should be placed."""
        broker = MockBrokerClient()
        manager = OrderManager(broker)

        wi = WatchlistItem(
            user_id=test_user.id, symbol="SBIN-EQ", token="3045",
            quantity=10, exchange="NSE",
        )
        db_session.add(wi)
        await db_session.flush()

        config = GlobalConfig(user_id=test_user.id)
        db_session.add(config)
        await db_session.flush()

        pos = OpenPosition(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="SBIN-EQ",
            position_type="LONG", quantity=10, entry_price=250.5,
            is_open=True, exit_pending=True,
        )
        db_session.add(pos)
        await db_session.flush()

        pending = PendingOrder(
            user_id=test_user.id, watchlist_item_id=wi.id,
            order_id="MOCK004", order_type="EXIT", direction="SELL",
            position_id=pos.id, placed_price=254.0, total_quantity=10,
            status="OPEN",
        )
        db_session.add(pending)
        await db_session.commit()

        # Act: cancel exit
        await manager.on_order_cancelled(db_session, pending, reason="EXIT_TIMEOUT")
        await db_session.commit()

        # Assert: a new retry pending order was created
        from sqlalchemy import select
        result = await db_session.execute(
            select(PendingOrder).where(
                PendingOrder.position_id == pos.id,
                PendingOrder.status == "OPEN",
            )
        )
        retry = result.scalar_one_or_none()
        assert retry is not None
        assert retry.order_id != "MOCK004"  # new order
        assert retry.placed_price < 254.0  # SELL at lower (worse) price

    @pytest.mark.asyncio
    async def test_poll_order_status(self, db_session: AsyncSession, test_user: User):
        """REST polling should detect completed orders."""
        broker = MockBrokerClient()
        manager = OrderManager(broker)

        wi = WatchlistItem(
            user_id=test_user.id, symbol="SBIN-EQ", token="3045",
            quantity=10, exchange="NSE",
        )
        db_session.add(wi)
        await db_session.flush()

        gap = FVGGap(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="SBIN-EQ",
            gap_type="SHORT", gap_high=200.0, gap_low=198.0,
            entry_price=199.4, stop_loss=201.0, take_profit=196.0,
            is_active=True,
        )
        db_session.add(gap)
        await db_session.flush()

        pending = PendingOrder(
            user_id=test_user.id, watchlist_item_id=wi.id,
            order_id="MOCK005", order_type="ENTRY", direction="SELL",
            gap_id=gap.id, placed_price=199.4, total_quantity=10,
            status="OPEN",
        )
        db_session.add(pending)
        await db_session.commit()

        # Mock returns complete status
        await manager.poll_order_status(db_session, pending)
        await db_session.commit()

        assert pending.status == "COMPLETE"

    @pytest.mark.asyncio
    async def test_entry_partial_fill_creates_partial_position(
        self, db_session: AsyncSession, test_user: User
    ):
        broker = MockBrokerClient()
        manager = OrderManager(broker)

        wi = WatchlistItem(
            user_id=test_user.id, symbol="SBIN-EQ", token="3045",
            quantity=10, exchange="NSE",
        )
        db_session.add(wi)
        await db_session.flush()

        gap = FVGGap(
            user_id=test_user.id, watchlist_item_id=wi.id, symbol="SBIN-EQ",
            gap_type="LONG", gap_high=252.0, gap_low=250.0,
            entry_price=250.6, stop_loss=248.0, take_profit=254.0,
            is_active=True,
        )
        db_session.add(gap)
        await db_session.flush()

        pending = PendingOrder(
            user_id=test_user.id, watchlist_item_id=wi.id,
            order_id="MOCK006", order_type="ENTRY", direction="BUY",
            gap_id=gap.id, placed_price=250.6, total_quantity=10,
            status="OPEN",
        )
        db_session.add(pending)
        await db_session.commit()

        await manager.on_order_partial(db_session, pending, fill_price=250.5, filled_qty=4)
        await db_session.commit()

        assert pending.status == "PARTIAL"
        assert pending.filled_quantity == 4

        from sqlalchemy import select
        pos_result = await db_session.execute(
            select(OpenPosition).where(OpenPosition.user_id == test_user.id)
        )
        position = pos_result.scalar_one()
        assert position.quantity == 4
        assert position.entry_price == 250.5

        await db_session.refresh(gap)
        assert gap.is_active is False
