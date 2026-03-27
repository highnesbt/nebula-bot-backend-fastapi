"""Position management — ATR trailing stop loss — ported as-is (pure Python)."""

from dataclasses import dataclass
from typing import Optional

from app.engine.fvg_detector import Candle

DEFAULT_TIGHT_MULTIPLIER = 0.5


@dataclass
class Position:
    """Tracks an open position's state for exit management."""
    position_type: str          # "LONG" or "SHORT"
    entry_price: float
    stop_loss: float
    take_profit: float
    quantity: int
    symbol: str
    token: str

    max_price_reached: float = 0.0
    min_price_reached: float = float("inf")
    current_tsl: float = 0.0
    tp_crossed: bool = False

    def __post_init__(self):
        if self.position_type == "LONG":
            self.max_price_reached = self.entry_price
        else:
            self.min_price_reached = self.entry_price


@dataclass
class ExitResult:
    """Result of an exit check."""
    should_exit: bool
    exit_price: float = 0.0
    exit_reason: str = ""
    pnl: float = 0.0


def update_position_and_check_exit(
    position: Position,
    candle: Candle,
    atr_value: float,
    atr_multiplier: float = 1.5,
    max_profit_per_trade: float = 2000.0,
    tight_multiplier: float = DEFAULT_TIGHT_MULTIPLIER,
) -> ExitResult:
    """Update ATR trailing stop and check exit conditions."""
    if position.position_type == "LONG":
        return _check_long_exit(position, candle, atr_value, atr_multiplier, max_profit_per_trade, tight_multiplier)
    else:
        return _check_short_exit(position, candle, atr_value, atr_multiplier, max_profit_per_trade, tight_multiplier)


def _check_long_exit(
    pos: Position, candle: Candle, atr_value: float,
    atr_multiplier: float, max_profit: float,
    tight_multiplier: float = DEFAULT_TIGHT_MULTIPLIER,
) -> ExitResult:
    pos.max_price_reached = max(pos.max_price_reached, candle.high)

    if not pos.tp_crossed and candle.high >= pos.take_profit:
        pos.tp_crossed = True

    effective_multiplier = tight_multiplier if pos.tp_crossed else atr_multiplier
    new_tsl = pos.max_price_reached - (atr_value * effective_multiplier)
    pos.current_tsl = max(pos.current_tsl, new_tsl)
    effective_sl = max(pos.stop_loss, pos.current_tsl)

    if candle.low <= effective_sl:
        exit_price = effective_sl
        pnl = (exit_price - pos.entry_price) * pos.quantity
        return ExitResult(
            should_exit=True, exit_price=round(exit_price, 2),
            exit_reason="TSL_HIT", pnl=round(pnl, 2),
        )

    unrealized = (candle.high - pos.entry_price) * pos.quantity
    if unrealized >= max_profit:
        exit_price = pos.entry_price + (max_profit / pos.quantity)
        return ExitResult(
            should_exit=True, exit_price=round(exit_price, 2),
            exit_reason="MAX_PROFIT", pnl=max_profit,
        )

    return ExitResult(should_exit=False)


def _check_short_exit(
    pos: Position, candle: Candle, atr_value: float,
    atr_multiplier: float, max_profit: float,
    tight_multiplier: float = DEFAULT_TIGHT_MULTIPLIER,
) -> ExitResult:
    pos.min_price_reached = min(pos.min_price_reached, candle.low)

    if not pos.tp_crossed and candle.low <= pos.take_profit:
        pos.tp_crossed = True

    effective_multiplier = tight_multiplier if pos.tp_crossed else atr_multiplier
    new_tsl = pos.min_price_reached + (atr_value * effective_multiplier)
    if pos.current_tsl > 0:
        pos.current_tsl = min(pos.current_tsl, new_tsl)
    else:
        pos.current_tsl = new_tsl
    effective_sl = min(pos.stop_loss, pos.current_tsl)

    if candle.high >= effective_sl:
        exit_price = effective_sl
        pnl = (pos.entry_price - exit_price) * pos.quantity
        return ExitResult(
            should_exit=True, exit_price=round(exit_price, 2),
            exit_reason="TSL_HIT", pnl=round(pnl, 2),
        )

    unrealized = (pos.entry_price - candle.low) * pos.quantity
    if unrealized >= max_profit:
        exit_price = pos.entry_price - (max_profit / pos.quantity)
        return ExitResult(
            should_exit=True, exit_price=round(exit_price, 2),
            exit_reason="MAX_PROFIT", pnl=max_profit,
        )

    return ExitResult(should_exit=False)
