"""Auto-exit — force close all positions at configured time."""

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.broadcast import ws_manager
from app.models.engine import AuditLog, OpenPosition, PendingOrder, Signal
from app.models.stock import GlobalConfig, WatchlistItem

logger = logging.getLogger(__name__)


async def auto_exit_all_positions(
    db: AsyncSession,
    user_id: int,
    broker_client,
    config: GlobalConfig,
) -> dict:
    """
    Force-close all open positions.

    Called by APScheduler at auto_exit_time (default 15:14 IST).
    Also cancels all pending orders.

    Returns {"closed": int, "failed": int, "total_pnl": float}
    """
    from app.engine.executor import execute_exit

    # 1. Cancel all pending orders first
    pending_result = await db.execute(
        select(PendingOrder).where(
            PendingOrder.user_id == user_id,
            PendingOrder.status == "OPEN",
        )
    )
    pending_orders = list(pending_result.scalars())

    for po in pending_orders:
        try:
            broker_client.cancel_order(po.order_id)
            po.status = "CANCELLED"
            po.cancel_reason = "AUTO_EXIT"
            po.resolved_at = datetime.now(timezone.utc)
            logger.info(f"Auto-exit: cancelled pending order #{po.order_id}")
        except Exception as e:
            logger.error(f"Auto-exit: failed to cancel order #{po.order_id}: {e}")

        # Re-activate gap if it was an entry order
        if po.order_type == "ENTRY" and po.gap_id:
            from app.models.engine import FVGGap
            gap_result = await db.execute(
                select(FVGGap).where(FVGGap.id == po.gap_id)
            )
            gap = gap_result.scalar_one_or_none()
            if gap:
                gap.is_active = True

    # 2. Close all open positions
    pos_result = await db.execute(
        select(OpenPosition).where(
            OpenPosition.user_id == user_id,
            OpenPosition.is_open == True,
        )
    )
    positions = list(pos_result.scalars())

    closed = 0
    failed = 0
    total_pnl = 0.0

    for pos in positions:
        # Get watchlist item for broker call
        wi_result = await db.execute(
            select(WatchlistItem).where(WatchlistItem.id == pos.watchlist_item_id)
        )
        wi = wi_result.scalar_one_or_none()
        if wi is None:
            failed += 1
            continue

        try:
            # Get current LTP for exit price
            ltp = broker_client.get_ltp(wi.exchange, wi.symbol, wi.token)

            direction = "SELL" if pos.position_type == "LONG" else "BUY"
            order_id = broker_client.place_order(
                symbol=wi.symbol,
                symbol_token=wi.token,
                transaction_type=direction,
                quantity=pos.quantity,
                exchange=wi.exchange,
                price=round(ltp, 2),
            )

            # For auto-exit, we directly close the position
            # (don't go through full PendingOrder lifecycle)
            if pos.position_type == "LONG":
                pnl = (ltp - pos.entry_price) * pos.quantity
            else:
                pnl = (pos.entry_price - ltp) * pos.quantity

            pos.is_open = False
            pos.exit_price = ltp
            pos.pnl = round(pnl, 2)
            pos.exit_reason = "AUTO_EXIT"
            pos.closed_at = datetime.now(timezone.utc)
            pos.exit_pending = False

            total_pnl += pnl
            closed += 1

            # Signal log
            db.add(Signal(
                user_id=user_id, watchlist_item_id=wi.id, symbol=wi.symbol,
                signal_type=f"{pos.position_type}_EXIT",
                price=ltp, order_id=str(order_id),
                order_status="complete", pnl=round(pnl, 2),
                exit_reason="AUTO_EXIT",
            ))

            # Audit log
            db.add(AuditLog(
                user_id=user_id, action="AUTO_EXIT",
                order_id=str(order_id), symbol=wi.symbol,
                direction=direction, price=ltp,
                quantity=pos.quantity, status="COMPLETE",
                details=f"pnl={round(pnl, 2)}",
            ))

            logger.info(f"Auto-exit: closed {wi.symbol} @ {ltp} PnL={pnl:.2f}")

        except Exception as e:
            failed += 1
            logger.error(f"Auto-exit: failed to close {wi.symbol}: {e}")
            db.add(AuditLog(
                user_id=user_id, action="AUTO_EXIT_FAILED",
                symbol=wi.symbol, status="FAILED", details=str(e),
            ))

    # Broadcast summary
    await ws_manager.broadcast_engine_event("auto_exit_complete", {
        "closed": closed,
        "failed": failed,
        "total_pnl": round(total_pnl, 2),
    })

    return {"closed": closed, "failed": failed, "total_pnl": round(total_pnl, 2)}
