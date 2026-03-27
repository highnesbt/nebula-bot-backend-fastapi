"""EMA-based trend detection — ported as-is (pure Python)."""

from dataclasses import dataclass
from typing import List


@dataclass
class TrendResult:
    """Detailed trend detection result."""
    direction: str      # "UP", "DOWN", or "SIDEWAYS"
    ema_value: float
    slope: float        # Percentage slope


def compute_ema(prices: List[float], period: int) -> List[float]:
    """Compute Exponential Moving Average."""
    if not prices or period < 1:
        return []

    k = 2.0 / (period + 1)
    ema_values = []

    if len(prices) < period:
        sma = sum(prices) / len(prices)
        ema_values.append(sma)
        for i in range(1, len(prices)):
            ema_values.append(prices[i] * k + ema_values[-1] * (1 - k))
        return ema_values

    sma = sum(prices[:period]) / period
    for i in range(period - 1):
        ema_values.append(None)
    ema_values.append(sma)

    for i in range(period, len(prices)):
        ema_val = prices[i] * k + ema_values[-1] * (1 - k)
        ema_values.append(ema_val)

    return ema_values


def detect_trend(
    close_prices: List[float],
    ema_period: int = 15,
    slope_lookback: int = 20,
    slope_threshold: float = 0.5,
) -> str:
    """Detect market trend using normalized EMA slope."""
    ema = compute_ema(close_prices, ema_period)
    valid_ema = [v for v in ema if v is not None]
    if len(valid_ema) < 2:
        return "SIDEWAYS"

    lookback = min(slope_lookback, len(valid_ema) - 1)
    base = valid_ema[-lookback]
    if base == 0:
        return "SIDEWAYS"
    slope_pct = (valid_ema[-1] - base) / base * 100

    if slope_pct > slope_threshold:
        return "UP"
    elif slope_pct < -slope_threshold:
        return "DOWN"
    return "SIDEWAYS"


def detect_trend_detailed(
    close_prices: List[float],
    ema_period: int = 15,
    slope_lookback: int = 20,
    slope_threshold: float = 0.5,
) -> TrendResult:
    """Detect market trend — returns full details for dashboard."""
    ema = compute_ema(close_prices, ema_period)
    valid_ema = [v for v in ema if v is not None]

    if len(valid_ema) < 2:
        return TrendResult(
            direction="SIDEWAYS",
            ema_value=round(valid_ema[-1], 4) if valid_ema else 0.0,
            slope=0.0,
        )

    lookback = min(slope_lookback, len(valid_ema) - 1)
    base = valid_ema[-lookback]
    if base == 0:
        return TrendResult(direction="SIDEWAYS", ema_value=round(valid_ema[-1], 4), slope=0.0)
    slope_pct = (valid_ema[-1] - base) / base * 100

    if slope_pct > slope_threshold:
        direction = "UP"
    elif slope_pct < -slope_threshold:
        direction = "DOWN"
    else:
        direction = "SIDEWAYS"

    return TrendResult(
        direction=direction,
        ema_value=round(valid_ema[-1], 4),
        slope=round(slope_pct, 4),
    )
