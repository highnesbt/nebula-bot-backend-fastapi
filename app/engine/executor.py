"""Order executor — places orders via broker client and logs signals.

REWRITTEN for order lifecycle: does NOT create OpenPosition immediately.
Instead creates a PendingOrder and delegates to OrderManager for confirmation.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.broadcast import ws_manager
from app.models.engine import AuditLog, FVGGap, OpenPosition, PendingOrder, Signal
from app.models.stock import GlobalConfig, WatchlistItem

logger = logging.getLogger(__name__)


async def execute_entry(
    db: AsyncSession,
    user_id: int,
    gap: FVGGap,
    watchlist_item: WatchlistItem,
    broker_client,
    config: GlobalConfig,
    ltp: float,
) -> PendingOrder | None:
    """
    Place a LIMIT entry order and create a PendingOrder.

    Does NOT create OpenPosition — that happens when order fills
    (via OrderManager / smart-order-update WebSocket).

    Returns the PendingOrder or None if order placement failed.
    """
    # OTR price band compliance check
    if config.otr_price_band_pct > 0:
        deviation_pct = abs(gap.entry_price - ltp) / ltp * 100
        if deviation_pct > config.otr_price_band_pct:
            logger.warning(
                f"[{watchlist_item.symbol}] Entry price {gap.entry_price} is "
                f"{deviation_pct:.2f}% from LTP {ltp} — exceeds OTR band "
                f"({config.otr_price_band_pct}%). Skipping."
            )
            return None

    direction = "BUY" if gap.gap_type == "LONG" else "SELL"

    try:
        order_id = broker_client.place_order(
            symbol=watchlist_item.symbol,
            symbol_token=watchlist_item.token,
            transaction_type=direction,
            quantity=watchlist_item.quantity,
            exchange=watchlist_item.exchange,
            price=gap.entry_price,
        )
    except Exception as e:
        logger.error(f"[{watchlist_item.symbol}] Entry order failed: {e}")
        # Log the error signal
        signal = Signal(
            user_id=user_id,
            watchlist_item_id=watchlist_item.id,
            symbol=watchlist_item.symbol,
            signal_type=f"{gap.gap_type}_ENTRY",
            price=gap.entry_price,
            order_status="FAILED",
            error_message=str(e),
        )
        db.add(signal)

        # Audit log
        db.add(AuditLog(
            user_id=user_id, action="ORDER_FAILED",
            order_id="", symbol=watchlist_item.symbol,
            direction=direction, price=gap.entry_price,
            quantity=watchlist_item.quantity,
            status="FAILED", details=str(e),
        ))
        return None

    # Create PendingOrder — NOT OpenPosition
    pending = PendingOrder(
        user_id=user_id,
        watchlist_item_id=watchlist_item.id,
        order_id=str(order_id),
        order_type="ENTRY",
        direction=direction,
        gap_id=gap.id,
        placed_price=gap.entry_price,
        total_quantity=watchlist_item.quantity,
        status="OPEN",
    )
    db.add(pending)

    # Audit log
    db.add(AuditLog(
        user_id=user_id, action="ORDER_PLACED",
        order_id=str(order_id), symbol=watchlist_item.symbol,
        direction=direction, price=gap.entry_price,
        quantity=watchlist_item.quantity, status="OPEN",
    ))

    logger.info(
        f"[{watchlist_item.symbol}] {gap.gap_type} entry order placed: "
        f"#{order_id} @ {gap.entry_price}"
    )

    # Broadcast
    await ws_manager.broadcast_trade_signal({
        "signal": f"{gap.gap_type}_ENTRY_PENDING",
        "symbol": watchlist_item.symbol,
        "price": gap.entry_price,
        "order_id": str(order_id),
    })

    return pending


async def execute_exit(
    db: AsyncSession,
    user_id: int,
    position: OpenPosition,
    watchlist_item: WatchlistItem,
    broker_client,
    config: GlobalConfig,
    exit_price: float,
    exit_reason: str,
    ltp: float | None = None,
) -> PendingOrder | None:
    """
    Place a LIMIT exit order and create a PendingOrder.

    For exit retries (price adjustment), this is called again with adjusted price.
    """
    if position.exit_pending:
        logger.warning(f"[{watchlist_item.symbol}] Exit already pending, skipping duplicate")
        return None

    direction = "SELL" if position.position_type == "LONG" else "BUY"

    # For exits, use aggressive pricing if LTP is available
    actual_exit_price = exit_price
    if ltp is not None:
        # Place limit slightly more aggressive than exit_price
        if direction == "SELL":
            actual_exit_price = min(exit_price, ltp * 0.999)  # Slightly below LTP
        else:
            actual_exit_price = max(exit_price, ltp * 1.001)  # Slightly above LTP

    try:
        order_id = broker_client.place_order(
            symbol=watchlist_item.symbol,
            symbol_token=watchlist_item.token,
            transaction_type=direction,
            quantity=position.quantity,
            exchange=watchlist_item.exchange,
            price=round(actual_exit_price, 2),
        )
    except Exception as e:
        logger.error(f"[{watchlist_item.symbol}] Exit order failed: {e}")
        db.add(AuditLog(
            user_id=user_id, action="EXIT_ORDER_FAILED",
            symbol=watchlist_item.symbol, direction=direction,
            price=actual_exit_price, quantity=position.quantity,
            status="FAILED", details=str(e),
        ))
        return None

    # Mark position as exit pending
    position.exit_pending = True

    # Create PendingOrder for exit
    pending = PendingOrder(
        user_id=user_id,
        watchlist_item_id=watchlist_item.id,
        order_id=str(order_id),
        order_type="EXIT",
        direction=direction,
        position_id=position.id,
        placed_price=round(actual_exit_price, 2),
        total_quantity=position.quantity,
        status="OPEN",
    )
    db.add(pending)

    # Audit log
    db.add(AuditLog(
        user_id=user_id, action="EXIT_ORDER_PLACED",
        order_id=str(order_id), symbol=watchlist_item.symbol,
        direction=direction, price=actual_exit_price,
        quantity=position.quantity, status="OPEN",
        details=f"reason={exit_reason}",
    ))

    logger.info(
        f"[{watchlist_item.symbol}] Exit order placed: #{order_id} "
        f"@ {actual_exit_price:.2f} reason={exit_reason}"
    )

    await ws_manager.broadcast_trade_signal({
        "signal": f"{position.position_type}_EXIT_PENDING",
        "symbol": watchlist_item.symbol,
        "price": actual_exit_price,
        "order_id": str(order_id),
        "reason": exit_reason,
    })

    return pending
