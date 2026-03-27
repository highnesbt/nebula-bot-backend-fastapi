"""Helpers to sync DB-backed gaps and positions into the in-memory TickEvaluator."""

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.atr import compute_atr
from app.engine.tick_evaluator import EntryThreshold, ExitThreshold, TickEvaluator
from app.models.engine import FVGGap, OpenPosition
from app.models.stock import GlobalConfig, WatchlistItem


def _estimate_atr_from_position(position: OpenPosition, config: GlobalConfig) -> float:
    stop_distance = abs(position.entry_price - position.stop_loss)
    if config.atr_multiplier > 0 and stop_distance > 0:
        return round(stop_distance / config.atr_multiplier, 4)
    return max(round(position.entry_price * 0.005, 4), 0.05)


async def sync_entry_thresholds(
    db: AsyncSession,
    evaluator: TickEvaluator,
    user_id: int,
):
    """Replace all in-memory entry thresholds from active DB gaps."""
    result = await db.execute(
        select(FVGGap, WatchlistItem)
        .join(WatchlistItem, WatchlistItem.id == FVGGap.watchlist_item_id)
        .where(
            FVGGap.user_id == user_id,
            FVGGap.is_active == True,
            WatchlistItem.is_active == True,
        )
    )

    token_map: dict[str, list[EntryThreshold]] = defaultdict(list)
    active_gap_ids: set[int] = set()

    for gap, item in result.all():
        active_gap_ids.add(gap.id)
        tolerance = max((gap.gap_high - gap.gap_low) * 0.1, 0.01)
        token_map[item.token].append(
            EntryThreshold(
                gap_id=gap.id,
                gap_type=gap.gap_type,
                entry_price=gap.entry_price,
                tolerance=tolerance,
                watchlist_item_id=item.id,
                user_id=user_id,
                symbol=item.symbol,
            )
        )

    evaluator.replace_entry_thresholds(dict(token_map))
    evaluator.retain_triggered_gaps(active_gap_ids)


async def sync_exit_thresholds(
    db: AsyncSession,
    evaluator: TickEvaluator,
    user_id: int,
    config: GlobalConfig,
    tick_manager=None,
):
    """Replace all in-memory exit thresholds from open DB positions."""
    result = await db.execute(
        select(OpenPosition, WatchlistItem)
        .join(WatchlistItem, WatchlistItem.id == OpenPosition.watchlist_item_id)
        .where(
            OpenPosition.user_id == user_id,
            OpenPosition.is_open == True,
            WatchlistItem.is_active == True,
        )
    )

    token_map: dict[str, ExitThreshold] = {}
    active_position_ids: set[int] = set()

    for position, item in result.all():
        active_position_ids.add(position.id)

        atr_value = None
        if tick_manager is not None:
            builder = tick_manager.get_candle_builder(item.token)
            if builder is not None:
                atr_value = compute_atr(builder.get_all_candles(), config.atr_period)
        if atr_value is None:
            atr_value = _estimate_atr_from_position(position, config)

        token_map[item.token] = ExitThreshold(
            position_id=position.id,
            position_type=position.position_type,
            entry_price=position.entry_price,
            stop_loss=position.stop_loss,
            take_profit=position.take_profit,
            current_tsl=position.current_tsl,
            max_price_reached=position.max_price_reached or position.entry_price,
            min_price_reached=position.min_price_reached or position.entry_price,
            tp_crossed=False,
            atr_value=atr_value,
            atr_multiplier=config.atr_multiplier,
            tight_multiplier=config.atr_tight_multiplier,
            max_profit_per_trade=config.max_profit_per_trade,
            quantity=position.quantity,
            watchlist_item_id=item.id,
            user_id=user_id,
            symbol=item.symbol,
        )

    evaluator.replace_exit_thresholds(token_map)
    evaluator.retain_triggered_positions(active_position_ids)


async def sync_all_thresholds(
    db: AsyncSession,
    evaluator: TickEvaluator,
    user_id: int,
    config: GlobalConfig,
    tick_manager=None,
):
    """Refresh both entry and exit thresholds for a user."""
    await sync_entry_thresholds(db, evaluator, user_id)
    await sync_exit_thresholds(db, evaluator, user_id, config, tick_manager=tick_manager)
