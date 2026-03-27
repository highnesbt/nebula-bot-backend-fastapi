"""OrderManager — state machine for PendingOrder lifecycle.

Handles transitions:
  OPEN → COMPLETE (filled)
  OPEN → CANCELLED (timeout / manual)
  OPEN → REJECTED (broker rejected)
  
For ENTRY orders: creates OpenPosition on fill.
For EXIT orders: closes OpenPosition on fill, retries on timeout/cancel.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.broadcast import ws_manager
from app.engine.rate_limiter import rate_limiter
from app.models.engine import AuditLog, FVGGap, OpenPosition, PendingOrder, Signal
from app.models.stock import GlobalConfig, WatchlistItem

logger = logging.getLogger(__name__)


class OrderManager:
    """Manages PendingOrder state transitions."""

    def __init__(self, broker_client):
        self.broker = broker_client

    async def on_order_complete(
        self, db: AsyncSession, pending: PendingOrder, fill_price: float, filled_qty: int,
    ):
        """Handle a filled order — create/close position as appropriate."""
        pending.status = "COMPLETE"
        pending.filled_price = fill_price
        pending.filled_quantity = filled_qty
        pending.resolved_at = datetime.now(timezone.utc)

        if pending.order_type == "ENTRY":
            await self._handle_entry_fill(db, pending, fill_price, filled_qty)
        elif pending.order_type == "EXIT":
            await self._handle_exit_fill(db, pending, fill_price, filled_qty)

        # Audit
        db.add(AuditLog(
            user_id=pending.user_id, action="ORDER_FILLED",
            order_id=pending.order_id,
            symbol=await self._get_symbol(db, pending),
            direction=pending.direction,
            price=fill_price, quantity=filled_qty,
            status="COMPLETE",
        ))
        await self._refresh_runtime_state(db, pending)

    async def on_order_partial(
        self, db: AsyncSession, pending: PendingOrder, fill_price: float, filled_qty: int,
    ):
        """Handle a partial fill by managing the filled quantity immediately."""
        pending.status = "PARTIAL"
        pending.filled_price = fill_price
        pending.filled_quantity = filled_qty
        pending.resolved_at = datetime.now(timezone.utc)

        if pending.order_type == "ENTRY":
            await self._handle_entry_fill(db, pending, fill_price, filled_qty)
        elif pending.order_type == "EXIT":
            await self._handle_exit_partial_fill(db, pending, fill_price, filled_qty)

        try:
            self.broker.cancel_order(pending.order_id)
        except Exception:
            pass

        db.add(AuditLog(
            user_id=pending.user_id,
            action="ORDER_PARTIAL",
            order_id=pending.order_id,
            symbol=await self._get_symbol(db, pending),
            direction=pending.direction,
            price=fill_price,
            quantity=filled_qty,
            status="PARTIAL",
            details="Remainder cancelled after partial fill",
        ))
        await self._refresh_runtime_state(db, pending)

    async def on_order_cancelled(
        self, db: AsyncSession, pending: PendingOrder, reason: str = "TIMEOUT",
    ):
        """Handle a cancelled order."""
        pending.status = "CANCELLED"
        pending.cancel_reason = reason
        pending.resolved_at = datetime.now(timezone.utc)

        symbol = await self._get_symbol(db, pending)

        if pending.order_type == "ENTRY":
            # Re-activate the gap so it can be filled again
            if pending.gap_id:
                gap_result = await db.execute(
                    select(FVGGap).where(FVGGap.id == pending.gap_id)
                )
                gap = gap_result.scalar_one_or_none()
                if gap:
                    gap.is_active = True

            db.add(Signal(
                user_id=pending.user_id,
                watchlist_item_id=pending.watchlist_item_id,
                symbol=symbol,
                signal_type=f"{pending.direction}_ENTRY",
                price=pending.placed_price,
                order_id=pending.order_id,
                order_status="CANCELLED",
                error_message=f"Entry cancelled: {reason}",
            ))

        elif pending.order_type == "EXIT":
            # Exit cancelled — need retry
            await self._handle_exit_retry(db, pending, reason)

        # Audit
        db.add(AuditLog(
            user_id=pending.user_id, action="ORDER_CANCELLED",
            order_id=pending.order_id, symbol=symbol,
            direction=pending.direction,
            price=pending.placed_price,
            status="CANCELLED", details=reason,
        ))

        await ws_manager.broadcast_trade_signal({
            "signal": "ORDER_CANCELLED",
            "order_type": pending.order_type,
            "symbol": symbol,
            "reason": reason,
        })
        await self._refresh_runtime_state(db, pending)

    async def on_order_rejected(
        self, db: AsyncSession, pending: PendingOrder, reason: str = "",
    ):
        """Handle a rejected order."""
        pending.status = "REJECTED"
        pending.cancel_reason = reason
        pending.resolved_at = datetime.now(timezone.utc)

        symbol = await self._get_symbol(db, pending)

        if pending.order_type == "EXIT":
            # Exit rejected — need retry with adjusted price
            await self._handle_exit_retry(db, pending, f"REJECTED: {reason}")

        db.add(AuditLog(
            user_id=pending.user_id, action="ORDER_REJECTED",
            order_id=pending.order_id, symbol=symbol,
            direction=pending.direction,
            price=pending.placed_price,
            status="REJECTED", details=reason,
        ))
        await self._refresh_runtime_state(db, pending)

    async def check_entry_timeout(self, db: AsyncSession, config: GlobalConfig):
        """Cancel entry orders that have exceeded the timeout."""
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=config.entry_order_timeout)

        result = await db.execute(
            select(PendingOrder).where(
                PendingOrder.user_id == config.user_id,
                PendingOrder.order_type == "ENTRY",
                PendingOrder.status == "OPEN",
                PendingOrder.placed_at < cutoff,
            )
        )
        timed_out = list(result.scalars())

        for po in timed_out:
            logger.info(f"Entry timeout: cancelling order #{po.order_id}")
            try:
                await rate_limiter.acquire()
                self.broker.cancel_order(po.order_id)
            except Exception as e:
                logger.error(f"Failed to cancel timed-out order #{po.order_id}: {e}")
            await self.on_order_cancelled(db, po, reason="ENTRY_TIMEOUT")

        return len(timed_out)

    async def check_exit_timeout(self, db: AsyncSession, config: GlobalConfig):
        """Check exit orders that have exceeded timeout — retry with worse price."""
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=config.exit_order_timeout)

        result = await db.execute(
            select(PendingOrder).where(
                PendingOrder.user_id == config.user_id,
                PendingOrder.order_type == "EXIT",
                PendingOrder.status == "OPEN",
                PendingOrder.placed_at < cutoff,
            )
        )
        timed_out = list(result.scalars())

        for po in timed_out:
            logger.info(f"Exit timeout: cancelling order #{po.order_id}")
            try:
                await rate_limiter.acquire()
                self.broker.cancel_order(po.order_id)
            except Exception as e:
                logger.error(f"Failed to cancel timed-out exit #{po.order_id}: {e}")
            await self.on_order_cancelled(db, po, reason="EXIT_TIMEOUT")

        return len(timed_out)

    async def poll_order_status(self, db: AsyncSession, pending: PendingOrder):
        """REST polling fallback — check order status via API."""
        status_data = self.broker.get_order_status(pending.order_id)
        order_status = status_data.get("orderstatus", "").lower()

        if order_status == "complete":
            fill_price = float(status_data.get("averageprice", 0))
            filled_qty = int(status_data.get("filledshares", 0))
            if fill_price > 0 and filled_qty > 0:
                await self.on_order_complete(db, pending, fill_price, filled_qty)
        elif order_status == "cancelled":
            await self.on_order_cancelled(db, pending, "BROKER_CANCELLED")
        elif order_status == "rejected":
            reason = status_data.get("text", "Unknown rejection")
            await self.on_order_rejected(db, pending, reason)
        elif order_status == "partial":
            fill_price = float(status_data.get("averageprice", 0))
            filled_qty = int(status_data.get("filledshares", 0))
            if fill_price > 0 and filled_qty > 0:
                await self.on_order_partial(db, pending, fill_price, filled_qty)

    # ── Internal helpers ──────────────────────────────────────────────────

    async def _handle_entry_fill(
        self, db: AsyncSession, pending: PendingOrder,
        fill_price: float, filled_qty: int,
    ):
        """Entry order filled → create OpenPosition."""
        # Determine position type from direction
        position_type = "LONG" if pending.direction == "BUY" else "SHORT"

        # Get gap for SL/TP
        gap = None
        if pending.gap_id:
            gap_result = await db.execute(
                select(FVGGap).where(FVGGap.id == pending.gap_id)
            )
            gap = gap_result.scalar_one_or_none()

        stop_loss = gap.stop_loss if gap else 0.0
        take_profit = gap.take_profit if gap else 0.0

        # Mark gap as filled
        if gap:
            gap.is_active = False
            gap.filled_at = datetime.now(timezone.utc)

        position = OpenPosition(
            user_id=pending.user_id,
            watchlist_item_id=pending.watchlist_item_id,
            symbol=await self._get_symbol(db, pending),
            position_type=position_type,
            quantity=filled_qty,
            entry_price=fill_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            is_open=True,
        )
        db.add(position)
        await db.flush()

        # Link pending order to position
        pending.position_id = position.id

        symbol = await self._get_symbol(db, pending)

        # Signal log
        db.add(Signal(
            user_id=pending.user_id,
            watchlist_item_id=pending.watchlist_item_id,
            symbol=symbol,
            signal_type=f"{position_type}_ENTRY",
            price=fill_price,
            order_id=pending.order_id,
            order_status="complete",
        ))

        logger.info(f"[{symbol}] Entry filled @ {fill_price} → Position #{position.id}")

        await ws_manager.broadcast_trade_signal({
            "signal": f"{position_type}_ENTRY_FILLED",
            "symbol": symbol,
            "price": fill_price,
            "position_id": position.id,
        })

    async def _handle_exit_fill(
        self, db: AsyncSession, pending: PendingOrder,
        fill_price: float, filled_qty: int,
    ):
        """Exit order filled → close OpenPosition."""
        if not pending.position_id:
            return

        pos_result = await db.execute(
            select(OpenPosition).where(OpenPosition.id == pending.position_id)
        )
        position = pos_result.scalar_one_or_none()
        if position is None:
            return

        # Calculate PnL
        if position.position_type == "LONG":
            pnl = (fill_price - position.entry_price) * position.quantity
        else:
            pnl = (position.entry_price - fill_price) * position.quantity

        position.is_open = False
        position.exit_price = fill_price
        position.exit_pending = False
        position.pnl = round(pnl, 2)
        position.closed_at = datetime.now(timezone.utc)

        symbol = await self._get_symbol(db, pending)

        # Signal log
        db.add(Signal(
            user_id=pending.user_id,
            watchlist_item_id=pending.watchlist_item_id,
            symbol=symbol,
            signal_type=f"{position.position_type}_EXIT",
            price=fill_price,
            order_id=pending.order_id,
            order_status="complete",
            pnl=round(pnl, 2),
            exit_reason="ORDER_FILLED",
        ))

        logger.info(f"[{symbol}] Exit filled @ {fill_price} PnL={pnl:.2f}")

        await ws_manager.broadcast_trade_signal({
            "signal": f"{position.position_type}_EXIT_FILLED",
            "symbol": symbol,
            "price": fill_price,
            "pnl": round(pnl, 2),
        })

    async def _handle_exit_partial_fill(
        self, db: AsyncSession, pending: PendingOrder,
        fill_price: float, filled_qty: int,
    ):
        """Reduce the live position for the filled quantity and keep managing the rest."""
        if not pending.position_id:
            return

        pos_result = await db.execute(
            select(OpenPosition).where(OpenPosition.id == pending.position_id)
        )
        position = pos_result.scalar_one_or_none()
        if position is None or not position.is_open:
            return

        actual_qty = min(filled_qty, position.quantity)
        if position.position_type == "LONG":
            partial_pnl = (fill_price - position.entry_price) * actual_qty
        else:
            partial_pnl = (position.entry_price - fill_price) * actual_qty

        remaining_qty = position.quantity - actual_qty
        position.exit_pending = False
        position.pnl = round(position.pnl + partial_pnl, 2)

        if remaining_qty <= 0:
            position.quantity = actual_qty
            await self._handle_exit_fill(db, pending, fill_price, actual_qty)
            return

        position.quantity = remaining_qty
        symbol = await self._get_symbol(db, pending)
        db.add(Signal(
            user_id=pending.user_id,
            watchlist_item_id=pending.watchlist_item_id,
            symbol=symbol,
            signal_type=f"{position.position_type}_EXIT",
            price=fill_price,
            order_id=pending.order_id,
            order_status="partial",
            pnl=round(partial_pnl, 2),
            exit_reason="PARTIAL_FILL",
        ))

        await ws_manager.broadcast_trade_signal({
            "signal": f"{position.position_type}_EXIT_PARTIAL",
            "symbol": symbol,
            "price": fill_price,
            "filled_qty": actual_qty,
            "remaining_qty": remaining_qty,
        })

    async def _handle_exit_retry(
        self, db: AsyncSession, pending: PendingOrder, reason: str,
    ):
        """Exit order failed — retry with increasingly aggressive price."""
        if not pending.position_id:
            return

        pos_result = await db.execute(
            select(OpenPosition).where(OpenPosition.id == pending.position_id)
        )
        position = pos_result.scalar_one_or_none()
        if position is None or not position.is_open:
            return

        # Get config for retry settings
        config_result = await db.execute(
            select(GlobalConfig).where(GlobalConfig.user_id == pending.user_id)
        )
        config = config_result.scalar_one_or_none()
        if config is None:
            return

        # Count existing retries for this position
        retry_result = await db.execute(
            select(PendingOrder).where(
                PendingOrder.position_id == pending.position_id,
                PendingOrder.order_type == "EXIT",
            )
        )
        retry_count = len(list(retry_result.scalars()))

        if retry_count >= config.exit_retry_max:
            logger.error(
                f"Exit retry exhausted for position #{pending.position_id} "
                f"({retry_count} attempts). Manual intervention required."
            )
            await ws_manager.broadcast_engine_event("EXIT_RETRY_EXHAUSTED", {
                "position_id": pending.position_id,
                "symbol": await self._get_symbol(db, pending),
                "attempts": retry_count,
            })
            return

        # Calculate worse price: slip by exit_retry_price_slip_pct each retry
        slip_pct = config.exit_retry_price_slip_pct * retry_count
        if position.position_type == "LONG":
            # Selling — lower the price
            new_price = pending.placed_price * (1 - slip_pct / 100)
        else:
            # Buying back — raise the price
            new_price = pending.placed_price * (1 + slip_pct / 100)

        new_price = round(new_price, 2)

        # Get watchlist item
        wi_result = await db.execute(
            select(WatchlistItem).where(WatchlistItem.id == pending.watchlist_item_id)
        )
        wi = wi_result.scalar_one_or_none()
        if wi is None:
            return

        logger.info(
            f"Exit retry #{retry_count} for position #{pending.position_id}: "
            f"new price {new_price} (slip={slip_pct:.1f}%)"
        )

        # Place a new exit order
        try:
            await rate_limiter.acquire()
            order_id = self.broker.place_order(
                symbol=wi.symbol, symbol_token=wi.token,
                transaction_type=pending.direction,
                quantity=position.quantity, exchange=wi.exchange,
                price=new_price,
            )
        except Exception as e:
            logger.error(f"Exit retry order failed: {e}")
            return

        # New pending order for the retry
        new_pending = PendingOrder(
            user_id=pending.user_id,
            watchlist_item_id=pending.watchlist_item_id,
            order_id=str(order_id),
            order_type="EXIT",
            direction=pending.direction,
            position_id=pending.position_id,
            placed_price=new_price,
            total_quantity=position.quantity,
            status="OPEN",
        )
        db.add(new_pending)

        db.add(AuditLog(
            user_id=pending.user_id, action="EXIT_RETRY",
            order_id=str(order_id),
            symbol=await self._get_symbol(db, pending),
            direction=pending.direction, price=new_price,
            quantity=position.quantity, status="OPEN",
            details=f"retry={retry_count}, slip={slip_pct:.1f}%",
        ))

    async def _refresh_runtime_state(self, db: AsyncSession, pending: PendingOrder):
        from app.engine.runtime import get_runtime

        runtime = get_runtime(pending.user_id)
        if runtime is None:
            return

        if pending.gap_id:
            runtime.tick_evaluator.clear_triggered_gap(pending.gap_id)
        if pending.position_id:
            runtime.tick_evaluator.clear_triggered_position(pending.position_id)
        await runtime.refresh_thresholds(db)

    async def _get_symbol(self, db: AsyncSession, pending: PendingOrder) -> str:
        """Get symbol for a pending order."""
        if pending.watchlist_item_id:
            result = await db.execute(
                select(WatchlistItem).where(
                    WatchlistItem.id == pending.watchlist_item_id
                )
            )
            wi = result.scalar_one_or_none()
            if wi:
                return wi.symbol
        return "UNKNOWN"
