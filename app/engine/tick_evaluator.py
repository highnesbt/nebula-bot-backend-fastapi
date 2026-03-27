"""Tick-level entry/exit evaluation — ported with updated imports.

Runs on every WebSocket tick. Holds precomputed thresholds in memory
and compares each tick price against them. No DB queries in the hot path.
"""

import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from datetime import time as dt_time
from typing import Dict, List, Optional, Set

from app.engine.time_utils import market_now

logger = logging.getLogger(__name__)


@dataclass
class EntryThreshold:
    """Precomputed entry trigger for an active FVG gap."""
    gap_id: int
    gap_type: str
    entry_price: float
    tolerance: float
    watchlist_item_id: int
    user_id: int
    symbol: str


@dataclass
class ExitThreshold:
    """Precomputed exit trigger for an open position."""
    position_id: int
    position_type: str
    entry_price: float
    stop_loss: float
    take_profit: float
    current_tsl: float
    max_price_reached: float
    min_price_reached: float
    tp_crossed: bool
    atr_value: float
    atr_multiplier: float
    tight_multiplier: float
    max_profit_per_trade: float
    quantity: int
    watchlist_item_id: int
    user_id: int
    symbol: str


@dataclass
class TickAction:
    """Trigger returned by on_tick when an entry/exit condition fires."""
    action_type: str        # "ENTRY" or "EXIT"
    gap_id: int = 0
    gap_type: str = ""
    position_id: int = 0
    exit_price: float = 0.0
    exit_reason: str = ""
    pnl: float = 0.0
    user_id: int = 0
    watchlist_item_id: int = 0
    symbol: str = ""
    trigger_price: float = 0.0


class TickEvaluator:
    """
    Evaluates ticks against precomputed entry/exit thresholds.

    Thread-safe: thresholds are read/written under a lock.
    One instance per user, stored alongside TickDataManager.
    """

    def __init__(self, user_id: int, no_entry_after: dt_time = dt_time(15, 0),
                 signal_cooldown_minutes: int = 15):
        self._user_id = user_id
        self._no_entry_after = no_entry_after
        self._signal_cooldown_minutes = signal_cooldown_minutes
        self._lock = threading.Lock()

        self._entry_thresholds: Dict[str, List[EntryThreshold]] = {}
        self._exit_thresholds: Dict[str, ExitThreshold] = {}
        self._triggered_gaps: Set[int] = set()
        self._triggered_positions: Set[int] = set()
        self._cooldown_until: Dict[str, datetime] = {}
        self._entries_paused = False

    def update_entry_thresholds(self, token: str, thresholds: List[EntryThreshold]):
        with self._lock:
            self._entry_thresholds[token] = list(thresholds)

    def update_exit_threshold(self, token: str, threshold: ExitThreshold):
        with self._lock:
            self._exit_thresholds[token] = threshold

    def clear_entry_thresholds(self, token: str):
        with self._lock:
            self._entry_thresholds.pop(token, None)

    def clear_exit_threshold(self, token: str):
        with self._lock:
            self._exit_thresholds.pop(token, None)

    def clear_triggered_gap(self, gap_id: int):
        with self._lock:
            self._triggered_gaps.discard(gap_id)

    def clear_triggered_position(self, position_id: int):
        with self._lock:
            self._triggered_positions.discard(position_id)

    def replace_entry_thresholds(self, token_map: Dict[str, List[EntryThreshold]]):
        with self._lock:
            self._entry_thresholds = {
                token: list(thresholds) for token, thresholds in token_map.items()
            }

    def replace_exit_thresholds(self, token_map: Dict[str, ExitThreshold]):
        with self._lock:
            self._exit_thresholds = dict(token_map)

    def retain_triggered_gaps(self, active_gap_ids: Set[int]):
        with self._lock:
            self._triggered_gaps.intersection_update(active_gap_ids)

    def retain_triggered_positions(self, active_position_ids: Set[int]):
        with self._lock:
            self._triggered_positions.intersection_update(active_position_ids)

    def set_entries_paused(self, paused: bool):
        with self._lock:
            self._entries_paused = paused

    def set_cooldown(self, symbol: str, until):
        with self._lock:
            self._cooldown_until[symbol] = until

    def get_exit_threshold(self, token: str) -> Optional[ExitThreshold]:
        with self._lock:
            t = self._exit_thresholds.get(token)
            if t is None:
                return None
            return ExitThreshold(
                position_id=t.position_id, position_type=t.position_type,
                entry_price=t.entry_price, stop_loss=t.stop_loss,
                take_profit=t.take_profit, current_tsl=t.current_tsl,
                max_price_reached=t.max_price_reached,
                min_price_reached=t.min_price_reached,
                tp_crossed=t.tp_crossed, atr_value=t.atr_value,
                atr_multiplier=t.atr_multiplier,
                tight_multiplier=t.tight_multiplier,
                max_profit_per_trade=t.max_profit_per_trade,
                quantity=t.quantity, watchlist_item_id=t.watchlist_item_id,
                user_id=t.user_id, symbol=t.symbol,
            )

    def on_tick(self, token: str, ltp: float) -> Optional[TickAction]:
        """HOT PATH — no DB queries, no broker calls. Must be fast."""
        with self._lock:
            action = self._check_exit_locked(token, ltp)
            if action:
                return action
            return self._check_entry_locked(token, ltp)

    def _check_exit_locked(self, token: str, ltp: float) -> Optional[TickAction]:
        thresh = self._exit_thresholds.get(token)
        if thresh is None:
            return None
        if thresh.position_id in self._triggered_positions:
            return None

        if thresh.position_type == "LONG":
            return self._check_long_exit(thresh, ltp)
        else:
            return self._check_short_exit(thresh, ltp)

    def _check_long_exit(self, t: ExitThreshold, ltp: float) -> Optional[TickAction]:
        if ltp > t.max_price_reached:
            t.max_price_reached = ltp
        if not t.tp_crossed and ltp >= t.take_profit:
            t.tp_crossed = True

        multiplier = t.tight_multiplier if t.tp_crossed else t.atr_multiplier
        new_tsl = t.max_price_reached - (t.atr_value * multiplier)
        t.current_tsl = max(t.current_tsl, new_tsl)
        effective_sl = max(t.stop_loss, t.current_tsl)

        if ltp <= effective_sl:
            exit_price = round(effective_sl, 2)
            pnl = round((exit_price - t.entry_price) * t.quantity, 2)
            self._triggered_positions.add(t.position_id)
            return TickAction(
                action_type="EXIT", position_id=t.position_id,
                exit_price=exit_price, exit_reason="TSL_HIT", pnl=pnl,
                user_id=t.user_id, watchlist_item_id=t.watchlist_item_id,
                symbol=t.symbol, trigger_price=ltp,
            )

        unrealized = (ltp - t.entry_price) * t.quantity
        if unrealized >= t.max_profit_per_trade:
            exit_price = round(t.entry_price + (t.max_profit_per_trade / t.quantity), 2)
            self._triggered_positions.add(t.position_id)
            return TickAction(
                action_type="EXIT", position_id=t.position_id,
                exit_price=exit_price, exit_reason="MAX_PROFIT",
                pnl=t.max_profit_per_trade, user_id=t.user_id,
                watchlist_item_id=t.watchlist_item_id,
                symbol=t.symbol, trigger_price=ltp,
            )
        return None

    def _check_short_exit(self, t: ExitThreshold, ltp: float) -> Optional[TickAction]:
        if ltp < t.min_price_reached:
            t.min_price_reached = ltp
        if not t.tp_crossed and ltp <= t.take_profit:
            t.tp_crossed = True

        multiplier = t.tight_multiplier if t.tp_crossed else t.atr_multiplier
        new_tsl = t.min_price_reached + (t.atr_value * multiplier)
        if t.current_tsl > 0:
            t.current_tsl = min(t.current_tsl, new_tsl)
        else:
            t.current_tsl = new_tsl
        effective_sl = min(t.stop_loss, t.current_tsl)

        if ltp >= effective_sl:
            exit_price = round(effective_sl, 2)
            pnl = round((t.entry_price - exit_price) * t.quantity, 2)
            self._triggered_positions.add(t.position_id)
            return TickAction(
                action_type="EXIT", position_id=t.position_id,
                exit_price=exit_price, exit_reason="TSL_HIT", pnl=pnl,
                user_id=t.user_id, watchlist_item_id=t.watchlist_item_id,
                symbol=t.symbol, trigger_price=ltp,
            )

        unrealized = (t.entry_price - ltp) * t.quantity
        if unrealized >= t.max_profit_per_trade:
            exit_price = round(t.entry_price - (t.max_profit_per_trade / t.quantity), 2)
            self._triggered_positions.add(t.position_id)
            return TickAction(
                action_type="EXIT", position_id=t.position_id,
                exit_price=exit_price, exit_reason="MAX_PROFIT",
                pnl=t.max_profit_per_trade, user_id=t.user_id,
                watchlist_item_id=t.watchlist_item_id,
                symbol=t.symbol, trigger_price=ltp,
            )
        return None

    def _check_entry_locked(self, token: str, ltp: float) -> Optional[TickAction]:
        if self._entries_paused:
            return None
        thresholds = self._entry_thresholds.get(token)
        if not thresholds:
            return None

        now = market_now()
        if now.time() >= self._no_entry_after:
            return None

        for t in thresholds:
            if t.gap_id in self._triggered_gaps:
                continue

            cooldown_end = self._cooldown_until.get(t.symbol)
            if cooldown_end and now < cooldown_end:
                continue

            filled = False
            if t.gap_type == "LONG" and ltp <= t.entry_price + t.tolerance:
                filled = True
            elif t.gap_type == "SHORT" and ltp >= t.entry_price - t.tolerance:
                filled = True

            if filled:
                self._triggered_gaps.add(t.gap_id)
                return TickAction(
                    action_type="ENTRY", gap_id=t.gap_id, gap_type=t.gap_type,
                    user_id=t.user_id, watchlist_item_id=t.watchlist_item_id,
                    symbol=t.symbol, trigger_price=ltp,
                )

        return None
