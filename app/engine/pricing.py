"""Price normalization helpers for broker-facing order prices."""

from decimal import Decimal, ROUND_HALF_UP

NSE_TICK_SIZE = Decimal("0.05")


def round_to_tick_size(price: float, tick_size: Decimal = NSE_TICK_SIZE) -> float:
    """Round a price to the nearest allowed exchange tick size."""
    if price <= 0:
        return 0.0

    ticks = (Decimal(str(price)) / tick_size).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    normalized = ticks * tick_size
    return float(normalized.quantize(Decimal("0.00")))
