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
