"""Fair Value Gap detection logic — ported as-is (pure Python)."""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Candle:
    """OHLC candle data."""
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class FVGSetup:
    """A detected Fair Value Gap setup."""
    gap_type: str           # "LONG" or "SHORT"
    entry_price: float      # Price level to enter
    stop_loss: float        # SL at other side of gap
    take_profit: float      # TP at multiplier × gap_size
    gap_high: float         # Upper boundary of gap zone
    gap_low: float          # Lower boundary of gap zone
    detected_at: str        # Candle time when detected
    is_active: bool = True


def detect_fvg_gaps(
    candles: List[Candle],
    trend: str,
    tp_multiplier: float = 2.0,
    start_index: int = 2,
    min_gap_percent: float = 0.0,
) -> List[FVGSetup]:
    """
    Scan candles for Fair Value Gaps.

    Bullish FVG (only in UP trend):
        candle[i].low > candle[i-2].high  →  gap between them

    Bearish FVG (only in DOWN trend):
        candle[i].high < candle[i-2].low  →  gap between them

    SIDEWAYS trend → no gaps detected (dead zone).
    """
    if trend == "SIDEWAYS":
        return []

    gaps = []

    for i in range(max(start_index, 2), len(candles)):
        curr = candles[i]
        first = candles[i - 2]

        # Bullish FVG
        if trend == "UP" and curr.low > first.high:
            gap_size = curr.low - first.high
            if min_gap_percent > 0 and (gap_size / first.high * 100) < min_gap_percent:
                continue
            gaps.append(FVGSetup(
                gap_type="LONG",
                entry_price=first.high + gap_size * 0.3,
                stop_loss=first.low,
                take_profit=first.high + (gap_size * tp_multiplier),
                gap_high=curr.low,
                gap_low=first.high,
                detected_at=curr.time,
            ))

        # Bearish FVG
        elif trend == "DOWN" and curr.high < first.low:
            gap_size = first.low - curr.high
            if min_gap_percent > 0 and (gap_size / first.low * 100) < min_gap_percent:
                continue
            gaps.append(FVGSetup(
                gap_type="SHORT",
                entry_price=curr.high + gap_size * 0.3,
                stop_loss=first.high,
                take_profit=first.low - (gap_size * tp_multiplier),
                gap_high=first.low,
                gap_low=curr.high,
                detected_at=curr.time,
            ))

    return gaps


def check_gap_fill(gap: FVGSetup, candle: Candle) -> bool:
    """Check if a candle triggers entry into an FVG gap."""
    if not gap.is_active:
        return False

    tolerance = (gap.gap_high - gap.gap_low) * 0.1
    if gap.gap_type == "LONG" and candle.low <= gap.entry_price + tolerance:
        return True
    if gap.gap_type == "SHORT" and candle.high >= gap.entry_price - tolerance:
        return True

    return False
