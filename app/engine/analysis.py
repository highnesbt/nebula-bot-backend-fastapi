"""Candle-close analysis for the tick-first runtime."""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.atr import compute_atr
from app.engine.broadcast import ws_manager
from app.engine.fvg_detector import detect_fvg_gaps
from app.engine.thresholds import sync_all_thresholds
from app.engine.trend import detect_trend_detailed
from app.models.engine import FVGGap
from app.models.stock import WatchlistItem


async def analyze_finalized_candle(
    db: AsyncSession,
    runtime,
    token: str,
    symbol: str,
    finalized_candle,
):
    """Run strategy analysis when a 5-minute candle is finalized."""
    builder = runtime.tick_manager.get_candle_builder(token)
    if builder is None:
        return

    candles = builder.get_all_candles()
    if len(candles) < 3:
        return

    watchlist_item = runtime.watchlist_by_token.get(token)
    if watchlist_item is None:
        result = await db.execute(
            select(WatchlistItem).where(
                WatchlistItem.user_id == runtime.user_id,
                WatchlistItem.token == token,
            )
        )
        watchlist_item = result.scalar_one_or_none()
        if watchlist_item is None:
            return
        runtime.watchlist_by_token[token] = watchlist_item

    trend = detect_trend_detailed(
        builder.get_close_prices(),
        ema_period=runtime.config.trend_ema_period,
        slope_lookback=runtime.config.trend_slope_lookback,
        slope_threshold=runtime.config.trend_slope_threshold,
    )
    atr_value = compute_atr(candles, runtime.config.atr_period)

    detected_gaps = detect_fvg_gaps(
        candles,
        trend.direction,
        tp_multiplier=runtime.config.tp_multiplier,
        start_index=max(len(candles) - 1, 2),
        min_gap_percent=runtime.config.min_gap_percent,
    )

    created = 0
    for gap in detected_gaps:
        detected_at = _parse_candle_time(gap.detected_at)
        exists_result = await db.execute(
            select(FVGGap).where(
                FVGGap.user_id == runtime.user_id,
                FVGGap.watchlist_item_id == watchlist_item.id,
                FVGGap.gap_type == gap.gap_type,
                FVGGap.detected_at == detected_at,
            )
        )
        if exists_result.scalar_one_or_none() is not None:
            continue

        db.add(
            FVGGap(
                user_id=runtime.user_id,
                watchlist_item_id=watchlist_item.id,
                symbol=watchlist_item.symbol,
                gap_type=gap.gap_type,
                gap_high=gap.gap_high,
                gap_low=gap.gap_low,
                entry_price=gap.entry_price,
                stop_loss=gap.stop_loss,
                take_profit=gap.take_profit,
                is_active=True,
                detected_at=detected_at,
            )
        )
        created += 1

    if created:
        await db.flush()

    await sync_all_thresholds(
        db,
        runtime.tick_evaluator,
        runtime.user_id,
        runtime.config,
        tick_manager=runtime.tick_manager,
    )

    await ws_manager.broadcast_scan_update(
        symbol,
        {
            "trend": trend.direction,
            "ema_value": trend.ema_value,
            "slope": trend.slope,
            "atr": round(atr_value, 4) if atr_value is not None else None,
            "new_gaps": created,
            "candle_time": finalized_candle.time,
            "entries_paused": runtime.entries_paused,
        },
    )


def _parse_candle_time(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.utcnow()
