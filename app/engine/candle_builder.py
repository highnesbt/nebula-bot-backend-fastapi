"""Build OHLC candles from tick data — ported with updated imports."""

import datetime
from typing import List, Optional

from app.engine.fvg_detector import Candle
from app.broker.constants import INTERVAL_MINUTES


class CandleBuilder:
    """
    Builds OHLC candles from tick prices.

    Each candle spans `interval_minutes`. When a tick arrives outside the
    current candle's window, the current candle is finalized and a new one starts.
    """

    def __init__(self, interval: str = "FIVE_MINUTE"):
        self.interval_minutes = INTERVAL_MINUTES.get(interval, 5)
        self.candles: List[Candle] = []
        self._current: Optional[dict] = None

    def seed_candles(self, raw_candles: list):
        """
        Seed with historical candle data from getCandleData().
        Each item: [time_str, open, high, low, close, volume]
        """
        self.candles = []
        sorted_raw = sorted(raw_candles, key=lambda x: x[0])
        for row in sorted_raw:
            self.candles.append(Candle(
                time=str(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]) if len(row) > 5 else 0.0,
            ))

    def merge_historical_candles(self, raw_candles: list):
        """Merge finalized historical candles into existing candle history by time."""
        merged = {c.time: c for c in self.candles}
        for row in raw_candles:
            candle = Candle(
                time=str(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]) if len(row) > 5 else 0.0,
            )
            merged[candle.time] = candle
        self.candles = [merged[key] for key in sorted(merged)]

    def on_tick(self, price: float, timestamp: datetime.datetime) -> Optional[Candle]:
        """
        Process a tick. Returns a finalized Candle if the interval boundary
        was crossed, otherwise None.
        """
        candle_start = self._get_candle_start(timestamp)
        finalized = None

        if self._current is None:
            self._current = {
                "start": candle_start,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
            }
        elif candle_start != self._current["start"]:
            # New interval — finalize current candle
            finalized = Candle(
                time=str(self._current["start"]),
                open=self._current["open"],
                high=self._current["high"],
                low=self._current["low"],
                close=self._current["close"],
            )
            self.candles.append(finalized)

            self._current = {
                "start": candle_start,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
            }
        else:
            self._current["high"] = max(self._current["high"], price)
            self._current["low"] = min(self._current["low"], price)
            self._current["close"] = price

        return finalized

    def get_latest_candle(self) -> Optional[Candle]:
        """Return the in-progress candle (not yet finalized)."""
        if self._current is None:
            return None
        return Candle(
            time=str(self._current["start"]),
            open=self._current["open"],
            high=self._current["high"],
            low=self._current["low"],
            close=self._current["close"],
        )

    def get_all_candles(self) -> List[Candle]:
        """Return all finalized candles."""
        return list(self.candles)

    def get_close_prices(self) -> List[float]:
        """Return close prices from all finalized candles."""
        return [c.close for c in self.candles]

    def _get_candle_start(self, timestamp: datetime.datetime) -> datetime.datetime:
        """Round down timestamp to the nearest candle interval boundary."""
        minutes = timestamp.hour * 60 + timestamp.minute
        interval_start = (minutes // self.interval_minutes) * self.interval_minutes
        return timestamp.replace(
            hour=interval_start // 60,
            minute=interval_start % 60,
            second=0,
            microsecond=0,
        )
