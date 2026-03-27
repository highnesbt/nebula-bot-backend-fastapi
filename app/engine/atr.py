"""ATR (Average True Range) computation — ported as-is (pure Python)."""

from typing import List, Optional

from app.engine.fvg_detector import Candle


def compute_atr(candles: List[Candle], period: int = 14) -> Optional[float]:
    """
    Compute ATR from the last `period` candles.

    Returns the mean of (high - low) for the most recent `period` candles,
    or None if fewer than `period` candles are available.
    """
    if len(candles) < period:
        return None

    tr_values = [c.high - c.low for c in candles[-period:]]
    return sum(tr_values) / period
