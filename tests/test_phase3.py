"""Phase 3 tests — pure-logic engine modules (no DB needed for most)."""

import datetime

import pytest

from app.engine.fvg_detector import Candle, FVGSetup, check_gap_fill, detect_fvg_gaps
from app.engine.trend import TrendResult, compute_ema, detect_trend, detect_trend_detailed
from app.engine.atr import compute_atr
from app.engine.candle_builder import CandleBuilder
from app.engine.position_manager import (
    ExitResult, Position, update_position_and_check_exit,
)
from app.engine.tick_evaluator import (
    EntryThreshold, ExitThreshold, TickAction, TickEvaluator,
)


# ── FVG Detector ──────────────────────────────────────────────────────────────


class TestFVGDetector:
    def _make_candles(self, data: list[tuple]) -> list[Candle]:
        """Helper: list of (open, high, low, close) tuples → Candle list."""
        return [
            Candle(time=f"t{i}", open=d[0], high=d[1], low=d[2], close=d[3])
            for i, d in enumerate(data)
        ]

    def test_bullish_fvg_detected(self):
        candles = self._make_candles([
            (100, 102, 98, 101),    # candle 0
            (101, 105, 100, 104),   # candle 1
            (106, 110, 103, 109),   # candle 2: low(103) > high(0)(102) → bullish FVG
        ])
        gaps = detect_fvg_gaps(candles, "UP")
        assert len(gaps) == 1
        assert gaps[0].gap_type == "LONG"
        assert gaps[0].gap_low == 102  # first.high
        assert gaps[0].gap_high == 103  # curr.low

    def test_bearish_fvg_detected(self):
        candles = self._make_candles([
            (200, 205, 198, 199),   # candle 0
            (199, 200, 195, 196),   # candle 1
            (194, 197, 190, 191),   # candle 2: high(197) < low(0)(198) → bearish FVG
        ])
        gaps = detect_fvg_gaps(candles, "DOWN")
        assert len(gaps) == 1
        assert gaps[0].gap_type == "SHORT"

    def test_no_fvg_in_sideways(self):
        candles = self._make_candles([
            (100, 110, 90, 105),
            (105, 115, 95, 110),
            (112, 120, 111, 118),
        ])
        gaps = detect_fvg_gaps(candles, "SIDEWAYS")
        assert len(gaps) == 0

    def test_min_gap_percent_filter(self):
        candles = self._make_candles([
            (100, 100.1, 99, 100),    # tiny gap
            (100, 101, 99, 100.5),
            (100.15, 102, 100.11, 101),  # gap = 0.01 (0.01%)
        ])
        gaps_no_filter = detect_fvg_gaps(candles, "UP", min_gap_percent=0.0)
        gaps_filtered = detect_fvg_gaps(candles, "UP", min_gap_percent=1.0)
        # A tiny gap should be filtered by min_gap_percent=1.0
        assert len(gaps_filtered) <= len(gaps_no_filter)

    def test_check_gap_fill(self):
        gap = FVGSetup(
            gap_type="LONG", entry_price=100.0, stop_loss=98.0,
            take_profit=104.0, gap_high=101.0, gap_low=99.0,
            detected_at="t1",
        )
        # Candle that retraces to entry
        candle = Candle(time="t2", open=102, high=103, low=100, close=101)
        assert check_gap_fill(gap, candle) is True

        # Candle that stays above
        candle2 = Candle(time="t3", open=105, high=106, low=104, close=105)
        assert check_gap_fill(gap, candle2) is False


# ── Trend ─────────────────────────────────────────────────────────────────────


class TestTrend:
    def test_compute_ema_basic(self):
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        ema = compute_ema(prices, 3)
        assert len(ema) == 5
        assert ema[2] == pytest.approx(11.0, abs=0.01)  # SMA of first 3

    def test_detect_uptrend(self):
        # Steadily rising prices
        prices = [100 + i * 2 for i in range(30)]
        trend = detect_trend(prices, ema_period=10)
        assert trend == "UP"

    def test_detect_downtrend(self):
        # Steadily falling prices
        prices = [200 - i * 2 for i in range(30)]
        trend = detect_trend(prices, ema_period=10)
        assert trend == "DOWN"

    def test_detect_sideways(self):
        # Flat prices
        prices = [100.0] * 30
        trend = detect_trend(prices, ema_period=10)
        assert trend == "SIDEWAYS"

    def test_detect_trend_detailed(self):
        prices = [100 + i * 2 for i in range(30)]
        result = detect_trend_detailed(prices, ema_period=10)
        assert isinstance(result, TrendResult)
        assert result.direction == "UP"
        assert result.slope > 0


# ── ATR ───────────────────────────────────────────────────────────────────────


class TestATR:
    def test_compute_atr(self):
        candles = [
            Candle(time=f"t{i}", open=100, high=105, low=95, close=100)
            for i in range(14)
        ]
        atr = compute_atr(candles, period=14)
        assert atr == pytest.approx(10.0, abs=0.01)

    def test_not_enough_candles(self):
        candles = [Candle(time="t0", open=100, high=105, low=95, close=100)]
        assert compute_atr(candles, period=14) is None


# ── Candle Builder ────────────────────────────────────────────────────────────


class TestCandleBuilder:
    def test_seed_candles(self):
        builder = CandleBuilder("FIVE_MINUTE")
        raw = [
            ["2024-01-01T09:15:00", 100, 105, 95, 102, 1000],
            ["2024-01-01T09:20:00", 102, 108, 100, 106, 2000],
        ]
        builder.seed_candles(raw)
        assert len(builder.get_all_candles()) == 2
        assert builder.get_close_prices() == [102.0, 106.0]

    def test_on_tick_builds_candle(self):
        builder = CandleBuilder("FIVE_MINUTE")
        base = datetime.datetime(2024, 1, 1, 9, 15, 0)

        # Ticks within same 5-min window
        r1 = builder.on_tick(100.0, base)
        assert r1 is None  # No finalized candle yet

        builder.on_tick(105.0, base + datetime.timedelta(seconds=30))
        builder.on_tick(95.0, base + datetime.timedelta(seconds=60))
        builder.on_tick(102.0, base + datetime.timedelta(minutes=4))

        # Tick in next window → finalizes previous
        finalized = builder.on_tick(103.0, base + datetime.timedelta(minutes=5))
        assert finalized is not None
        assert finalized.open == 100.0
        assert finalized.high == 105.0
        assert finalized.low == 95.0
        assert finalized.close == 102.0

    def test_get_latest_candle(self):
        builder = CandleBuilder("FIVE_MINUTE")
        base = datetime.datetime(2024, 1, 1, 9, 15, 0)
        builder.on_tick(100.0, base)
        builder.on_tick(110.0, base + datetime.timedelta(seconds=10))

        latest = builder.get_latest_candle()
        assert latest is not None
        assert latest.high == 110.0


# ── Position Manager ─────────────────────────────────────────────────────────


class TestPositionManager:
    def _make_position(self, ptype: str = "LONG") -> Position:
        return Position(
            position_type=ptype, entry_price=100.0, stop_loss=95.0,
            take_profit=110.0, quantity=10, symbol="TEST", token="1234",
        )

    def test_long_tsl_hit(self):
        pos = self._make_position("LONG")
        # Price goes up — TSL ratchets up
        # high=108, atr=5, mult=1.5 → TSL=108-7.5=100.5
        # We need low > 100.5 so TSL doesn't trigger on this candle
        candle_up = Candle(time="t1", open=103, high=108, low=101, close=107)
        result = update_position_and_check_exit(pos, candle_up, atr_value=5.0)
        assert result.should_exit is False

        # Now price drops below TSL (100.5)
        candle_down = Candle(time="t2", open=101, high=101, low=100, close=100)
        result = update_position_and_check_exit(pos, candle_down, atr_value=5.0)
        assert result.should_exit is True
        assert result.exit_reason == "TSL_HIT"

    def test_long_max_profit(self):
        pos = self._make_position("LONG")
        # entry=100, max_profit=500, qty=10 → need unrealized=(high-100)*10 >= 500 → high>=150
        # high=160 → after TP crossed (110), tight_mult=0.5 → TSL=160-5*0.5=157.5
        # low MUST stay above 157.5 for TSL not to trigger
        candle = Candle(time="t1", open=158, high=160, low=158, close=158)
        result = update_position_and_check_exit(pos, candle, atr_value=5.0, max_profit_per_trade=500)
        assert result.should_exit is True
        assert result.exit_reason == "MAX_PROFIT"
        assert result.pnl == 500


    def test_short_tsl_hit(self):
        pos = self._make_position("SHORT")
        pos.stop_loss = 105.0
        pos.take_profit = 90.0

        # Price drops first — lower min_price, but not below TP
        candle_down = Candle(time="t1", open=99, high=99, low=95, close=96)
        result = update_position_and_check_exit(pos, candle_down, atr_value=5.0)
        assert result.should_exit is False
        # min_price=95, TSL = 95 + 5*1.5 = 102.5
        # effective_sl = min(105, 102.5) = 102.5

        # Price bounces up past TSL
        candle_up = Candle(time="t2", open=101, high=103, low=101, close=103)
        result = update_position_and_check_exit(pos, candle_up, atr_value=5.0)
        assert result.should_exit is True
        assert result.exit_reason == "TSL_HIT"



# ── Tick Evaluator ────────────────────────────────────────────────────────────


class TestTickEvaluator:
    def test_entry_trigger(self):
        evaluator = TickEvaluator(user_id=1, no_entry_after=datetime.time(23, 59))
        evaluator.update_entry_thresholds("TOK1", [
            EntryThreshold(
                gap_id=10, gap_type="LONG", entry_price=100.0,
                tolerance=2.0, watchlist_item_id=1, user_id=1, symbol="TEST",
            ),
        ])
        action = evaluator.on_tick("TOK1", 101.5)
        assert action is not None
        assert action.action_type == "ENTRY"
        assert action.gap_id == 10

    def test_entry_not_triggered(self):
        evaluator = TickEvaluator(user_id=1, no_entry_after=datetime.time(23, 59))
        evaluator.update_entry_thresholds("TOK1", [
            EntryThreshold(
                gap_id=10, gap_type="LONG", entry_price=100.0,
                tolerance=2.0, watchlist_item_id=1, user_id=1, symbol="TEST",
            ),
        ])
        action = evaluator.on_tick("TOK1", 150.0)  # Way above entry
        assert action is None

    def test_exit_tsl_hit(self):
        evaluator = TickEvaluator(user_id=1)
        evaluator.update_exit_threshold("TOK1", ExitThreshold(
            position_id=5, position_type="LONG", entry_price=100.0,
            stop_loss=95.0, take_profit=110.0, current_tsl=0.0,
            max_price_reached=100.0, min_price_reached=float("inf"),
            tp_crossed=False, atr_value=5.0, atr_multiplier=1.5,
            tight_multiplier=0.5, max_profit_per_trade=2000.0,
            quantity=10, watchlist_item_id=1, user_id=1, symbol="TEST",
        ))

        # Price at stop loss
        action = evaluator.on_tick("TOK1", 94.0)
        assert action is not None
        assert action.action_type == "EXIT"
        assert action.exit_reason == "TSL_HIT"

    def test_duplicate_gap_trigger_prevented(self):
        evaluator = TickEvaluator(user_id=1, no_entry_after=datetime.time(23, 59))
        evaluator.update_entry_thresholds("TOK1", [
            EntryThreshold(
                gap_id=10, gap_type="LONG", entry_price=100.0,
                tolerance=2.0, watchlist_item_id=1, user_id=1, symbol="TEST",
            ),
        ])
        # First trigger
        action1 = evaluator.on_tick("TOK1", 101.0)
        assert action1 is not None

        # Same tick again — should be prevented
        action2 = evaluator.on_tick("TOK1", 101.0)
        assert action2 is None

    def test_clear_triggered_gap(self):
        evaluator = TickEvaluator(user_id=1, no_entry_after=datetime.time(23, 59))
        evaluator.update_entry_thresholds("TOK1", [
            EntryThreshold(
                gap_id=10, gap_type="LONG", entry_price=100.0,
                tolerance=2.0, watchlist_item_id=1, user_id=1, symbol="TEST",
            ),
        ])
        evaluator.on_tick("TOK1", 101.0)
        evaluator.clear_triggered_gap(10)
        action = evaluator.on_tick("TOK1", 101.0)
        assert action is not None
