"""Timezone-aware time utilities for Indian market hours."""

from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

MARKET_OPEN = datetime.strptime("09:15", "%H:%M").time()
MARKET_CLOSE = datetime.strptime("15:30", "%H:%M").time()


def market_now() -> datetime:
    """Return current time in IST."""
    return datetime.now(IST)


def is_market_hours() -> bool:
    """Check if current time is within NSE market hours."""
    now = market_now().time()
    return MARKET_OPEN <= now <= MARKET_CLOSE


def parse_exchange_timestamp(value) -> datetime | None:
    """Parse common broker/exchange timestamp formats into IST."""
    if value in (None, ""):
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=IST)
        return value.astimezone(IST)

    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=IST)

    text = str(value).strip()
    if not text:
        return None

    if text.isdigit():
        return parse_exchange_timestamp(int(text))

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=IST)
        return parsed.astimezone(IST)
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=IST)
        except ValueError:
            continue

    return None
