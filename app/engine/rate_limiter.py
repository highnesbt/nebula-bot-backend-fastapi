"""Token-bucket rate limiter for order placement — SEBI compliance.

April 2026 regulations cap algo trading at ≤9 orders/second.
This enforcer blocks callers until a token is available.
"""

import asyncio
import time
from collections import deque


class OrderRateLimiter:
    """
    Token-bucket rate limiter.

    Default: max 8 orders/second (leaves 1 headroom below the 9/s cap).
    """

    def __init__(self, max_per_second: int = 8):
        self._max = max_per_second
        self._timestamps: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Wait until a token is available, then consume it."""
        async with self._lock:
            now = time.monotonic()
            # Purge timestamps older than 1 second
            while self._timestamps and now - self._timestamps[0] > 1.0:
                self._timestamps.popleft()

            if len(self._timestamps) >= self._max:
                # Wait until the oldest token expires
                sleep_time = 1.0 - (now - self._timestamps[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                # Purge again after sleep
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] > 1.0:
                    self._timestamps.popleft()

            self._timestamps.append(time.monotonic())

    @property
    def tokens_remaining(self) -> int:
        """Number of tokens available right now."""
        now = time.monotonic()
        while self._timestamps and now - self._timestamps[0] > 1.0:
            self._timestamps.popleft()
        return max(0, self._max - len(self._timestamps))


# Singleton
rate_limiter = OrderRateLimiter()
