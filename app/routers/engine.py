"""Engine router — status, start/stop, exit-all, gaps, positions, signals."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.database import get_db
from app.models.engine import FVGGap, OpenPosition, Signal
from app.models.stock import GlobalConfig
from app.models.user import User
from app.schemas.engine import (
    EngineStatusOut,
    ExitAllOut,
    FVGGapOut,
    MessageOut,
    OpenPositionOut,
    SignalListOut,
    SignalOut,
)
from app.engine.runtime import exit_all_with_runtime, start_runtime, stop_runtime

router = APIRouter(prefix="/engine", tags=["Engine"])


# ── Engine Status ─────────────────────────────────────────────────────────────


@router.get("/status", response_model=EngineStatusOut)
async def engine_status(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get engine running status."""
    config_result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.user_id == user.id)
    )
    config = config_result.scalar_one_or_none()
    is_active = config.is_active if config else False

    pos_result = await db.execute(
        select(func.count()).select_from(OpenPosition).where(
            OpenPosition.user_id == user.id, OpenPosition.is_open == True
        )
    )
    open_positions = pos_result.scalar()

    gap_result = await db.execute(
        select(func.count()).select_from(FVGGap).where(
            FVGGap.user_id == user.id, FVGGap.is_active == True
        )
    )
    active_gaps = gap_result.scalar()

    return EngineStatusOut(
        is_active=is_active,
        open_positions=open_positions,
        active_gaps=active_gaps,
    )


@router.post("/start", response_model=MessageOut)
async def start_engine(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Start the live scanner (sets is_active=True)."""
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.user_id == user.id)
    )
    config = result.scalar_one_or_none()
    if config is None:
        config = GlobalConfig(user_id=user.id)
        db.add(config)
        await db.flush()

    if config.is_active:
        raise HTTPException(400, "Engine is already running.")

    await start_runtime(db, user.id)
    config.is_active = True
    return MessageOut(message="Engine started.")


@router.post("/stop", response_model=MessageOut)
async def stop_engine(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Stop the live scanner (sets is_active=False). Acts as kill switch."""
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.user_id == user.id)
    )
    config = result.scalar_one_or_none()
    if config is None:
        config = GlobalConfig(user_id=user.id)
        db.add(config)
        await db.flush()

    if not config.is_active:
        raise HTTPException(400, "Engine is not running.")

    await stop_runtime(db, user.id, cancel_pending_orders=True)
    config.is_active = False

    return MessageOut(message="Engine stopped.")


# ── Exit All ──────────────────────────────────────────────────────────────────


@router.post("/exit-all", response_model=ExitAllOut)
async def exit_all_positions(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually exit all open positions."""
    pos_result = await db.execute(
        select(func.count()).select_from(OpenPosition).where(
            OpenPosition.user_id == user.id, OpenPosition.is_open == True
        )
    )
    if pos_result.scalar() == 0:
        raise HTTPException(400, "No open positions to exit.")

    result = await exit_all_with_runtime(db, user.id)
    return ExitAllOut(**result)


# ── FVG Gaps ──────────────────────────────────────────────────────────────────


@router.get("/gaps", response_model=list[FVGGapOut])
async def list_gaps(
    active_only: bool = Query(True),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List FVG gaps. Pass ?active_only=false to include filled gaps."""
    query = select(FVGGap).where(FVGGap.user_id == user.id)
    if active_only:
        query = query.where(FVGGap.is_active == True)
    query = query.order_by(FVGGap.detected_at.desc())

    result = await db.execute(query)
    return [
        FVGGapOut(
            id=g.id,
            symbol=g.symbol or (g.watchlist_item.symbol if g.watchlist_item else "—"),
            gap_type=g.gap_type,
            entry_price=g.entry_price,
            stop_loss=g.stop_loss,
            take_profit=g.take_profit,
            gap_high=g.gap_high,
            gap_low=g.gap_low,
            is_active=g.is_active,
            detected_at=g.detected_at.isoformat(),
            filled_at=g.filled_at.isoformat() if g.filled_at else None,
        )
        for g in result.scalars()
    ]


# ── Open Positions ────────────────────────────────────────────────────────────


def _position_to_dict(p: OpenPosition) -> dict:
    return {
        "id": p.id,
        "symbol": p.symbol or (p.watchlist_item.symbol if p.watchlist_item else "—"),
        "position_type": p.position_type,
        "quantity": p.quantity,
        "entry_price": p.entry_price,
        "stop_loss": p.stop_loss,
        "take_profit": p.take_profit,
        "max_price_reached": p.max_price_reached,
        "min_price_reached": p.min_price_reached,
        "current_tsl": p.current_tsl,
        "is_open": p.is_open,
        "pnl": p.pnl,
        "exit_price": p.exit_price,
        "opened_at": p.opened_at.isoformat(),
        "closed_at": p.closed_at.isoformat() if p.closed_at else None,
    }


@router.get("/positions", response_model=list[OpenPositionOut])
async def list_positions(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List open positions with TSL state."""
    result = await db.execute(
        select(OpenPosition).where(
            OpenPosition.user_id == user.id, OpenPosition.is_open == True
        )
    )
    return [_position_to_dict(p) for p in result.scalars()]


@router.get("/positions/history", response_model=list[OpenPositionOut])
async def list_position_history(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List all positions (open and closed)."""
    result = await db.execute(
        select(OpenPosition)
        .where(OpenPosition.user_id == user.id)
        .order_by(OpenPosition.opened_at.desc())
        .limit(50)
    )
    return [_position_to_dict(p) for p in result.scalars()]


# ── Signals ───────────────────────────────────────────────────────────────────


def _signal_to_dict(s: Signal) -> dict:
    return {
        "id": s.id,
        "symbol": s.symbol or (s.watchlist_item.symbol if s.watchlist_item else "—"),
        "signal_type": s.signal_type,
        "price": s.price,
        "order_id": s.order_id,
        "order_status": s.order_status,
        "pnl": s.pnl,
        "exit_reason": s.exit_reason,
        "error_message": s.error_message,
        "created_at": s.created_at.isoformat(),
    }


@router.get("/signals", response_model=SignalListOut)
async def list_signals(
    signal_type: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    limit: int = Query(20),
    offset: int = Query(0),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List signal history with filtering and server-side pagination."""
    query = select(Signal).where(Signal.user_id == user.id)

    if signal_type:
        if signal_type in ("ENTRY", "EXIT"):
            query = query.where(Signal.signal_type.endswith(signal_type))
        else:
            query = query.where(Signal.signal_type == signal_type)

    if date_from:
        query = query.where(func.date(Signal.created_at) >= date_from)
    if date_to:
        query = query.where(func.date(Signal.created_at) <= date_to)

    # Count
    from sqlalchemy import func as sqla_func
    count_query = select(sqla_func.count()).select_from(query.subquery())
    count_result = await db.execute(count_query)
    count = count_result.scalar()

    # Paginated results
    query = query.order_by(Signal.created_at.desc()).offset(offset).limit(limit)
    result = await db.execute(query)
    signals = [_signal_to_dict(s) for s in result.scalars()]

    return SignalListOut(count=count, results=signals)


@router.get("/signals/{signal_id}", response_model=SignalOut)
async def get_signal(
    signal_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get signal detail."""
    result = await db.execute(
        select(Signal).where(Signal.id == signal_id, Signal.user_id == user.id)
    )
    s = result.scalar_one_or_none()
    if s is None:
        raise HTTPException(404, "Signal not found.")
    return _signal_to_dict(s)
