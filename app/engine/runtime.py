"""Per-user engine runtime registry for the tick-first FastAPI backend."""

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from math import floor

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.client import get_broker_client
from app.broker.constants import INTERVALS
from app.config import settings
from app.engine.analysis import analyze_finalized_candle
from app.engine.auto_exit import auto_exit_all_positions
from app.engine.broadcast import ws_manager
from app.engine.executor import execute_entry
from app.engine.order_manager import OrderManager
from app.engine.order_ws import OrderUpdateManager
from app.engine.thresholds import sync_all_thresholds
from app.engine.tick_evaluator import TickEvaluator
from app.engine.tick_manager import TickDataManager
from app.engine.time_utils import market_now, parse_exchange_timestamp
from app.models.engine import AuditLog, FVGGap, OpenPosition, PendingOrder
from app.models.stock import GlobalConfig, WatchlistItem
from app.models.user import BrokerCredential


@dataclass
class EngineRuntime:
    user_id: int
    config: GlobalConfig
    broker: object
    tick_evaluator: TickEvaluator
    tick_manager: TickDataManager
    order_updates: OrderUpdateManager | None = None
    polling_task: asyncio.Task | None = None
    entries_paused: bool = False
    recovery_in_progress: bool = False
    recovery_failed: bool = False
    disconnected_at: object = None
    watchlist_by_token: dict[str, WatchlistItem] = field(default_factory=dict)

    async def refresh_thresholds(self, db: AsyncSession):
        self.tick_evaluator.set_entries_paused(self.entries_paused)
        await sync_all_thresholds(
            db,
            self.tick_evaluator,
            self.user_id,
            self.config,
            tick_manager=self.tick_manager,
        )

    async def pause_entries(self, db: AsyncSession, reason: str):
        self.entries_paused = True
        self.tick_evaluator.set_entries_paused(True)
        await self.refresh_thresholds(db)
        await ws_manager.broadcast_engine_event(
            "entries_paused",
            {"user_id": self.user_id, "reason": reason},
        )

    async def resume_entries(self, db: AsyncSession):
        self.entries_paused = False
        self.tick_evaluator.set_entries_paused(False)
        await self.refresh_thresholds(db)
        await ws_manager.broadcast_engine_event(
            "entries_resumed",
            {"user_id": self.user_id},
        )


_RUNTIMES: dict[int, EngineRuntime] = {}


def get_runtime(user_id: int) -> EngineRuntime | None:
    return _RUNTIMES.get(user_id)


def is_runtime_active(user_id: int) -> bool:
    return user_id in _RUNTIMES


async def start_runtime(db: AsyncSession, user_id: int) -> EngineRuntime:
    """Create and start the live engine runtime for a user."""
    if user_id in _RUNTIMES:
        return _RUNTIMES[user_id]

    config = await _load_config(db, user_id)
    cred = await _load_credentials(db, user_id)

    broker = get_broker_client(cred.api_key)
    try:
        broker.login(cred.client_id, cred.password, cred.totp_secret)
    except Exception as exc:
        raise HTTPException(400, f"Broker login failed: {exc}")

    evaluator = TickEvaluator(
        user_id=user_id,
        no_entry_after=config.no_entry_after,
        signal_cooldown_minutes=config.signal_cooldown_minutes,
    )

    loop = asyncio.get_running_loop()
    tick_manager = TickDataManager(
        auth_token="",
        feed_token="",
        api_key=cred.api_key,
        client_code=cred.client_id,
        candle_interval=config.candle_interval,
        tick_evaluator=evaluator,
        event_loop=loop,
        candle_close_callback=None,
    )

    runtime = EngineRuntime(
        user_id=user_id,
        config=config,
        broker=broker,
        tick_evaluator=evaluator,
        tick_manager=tick_manager,
    )

    tick_manager.set_candle_close_callback(runtime._handle_candle_close)

    watchlist_items = await _load_watchlist(db, user_id)
    runtime.watchlist_by_token = {item.token: item for item in watchlist_items}
    _prepare_tick_manager(runtime, watchlist_items)
    await _seed_warmup_candles(runtime, watchlist_items)
    await runtime.refresh_thresholds(db)
    await _start_market_streams(runtime, cred)

    _RUNTIMES[user_id] = runtime
    await ws_manager.broadcast_engine_event(
        "engine_started",
        {"user_id": user_id, "tokens": sorted(runtime.watchlist_by_token)},
    )
    return runtime


async def stop_runtime(
    db: AsyncSession,
    user_id: int,
    cancel_pending_orders: bool = True,
):
    """Stop and remove the runtime for a user."""
    runtime = _RUNTIMES.pop(user_id, None)

    broker = runtime.broker if runtime else None
    if broker is None:
        cred = await _load_credentials(db, user_id, required=False)
        if cred is not None:
            broker = get_broker_client(cred.api_key)
            try:
                broker.login(cred.client_id, cred.password, cred.totp_secret)
            except Exception:
                broker = None

    if cancel_pending_orders and broker is not None:
        await _cancel_open_pending_orders(db, user_id, broker, reason="ENGINE_STOP")

    if runtime is not None:
        if runtime.polling_task is not None:
            runtime.polling_task.cancel()
        if runtime.order_updates is not None:
            runtime.order_updates.stop()
        runtime.tick_manager.stop()

    await ws_manager.broadcast_engine_event(
        "engine_stopped",
        {"user_id": user_id},
    )


async def refresh_runtime_thresholds(db: AsyncSession, user_id: int):
    runtime = _RUNTIMES.get(user_id)
    if runtime is not None:
        await runtime.refresh_thresholds(db)


async def onboard_watchlist_item(
    db: AsyncSession,
    user_id: int,
    item: WatchlistItem,
):
    """Attach a newly added watchlist item to an active runtime, if one exists."""
    runtime = _RUNTIMES.get(user_id)
    if runtime is None or not item.is_active:
        return

    runtime.watchlist_by_token[item.token] = item
    _prepare_tick_manager(runtime, list(runtime.watchlist_by_token.values()))

    candles = await _fetch_historical_candles(runtime, item)
    finalized = _filter_finalized_candles(candles, runtime.config.candle_interval)
    if finalized:
        await _replay_watchlist_history(db, runtime, item, finalized)

    await runtime.refresh_thresholds(db)
    await _maybe_execute_watchlist_entry(db, runtime, item)


async def exit_all_with_runtime(db: AsyncSession, user_id: int) -> dict:
    """Use the active runtime broker if present, else create a temporary broker session."""
    runtime = _RUNTIMES.get(user_id)
    if runtime is not None:
        result = await auto_exit_all_positions(db, user_id, runtime.broker, runtime.config)
        await runtime.refresh_thresholds(db)
        return result

    config = await _load_config(db, user_id)
    cred = await _load_credentials(db, user_id)
    broker = get_broker_client(cred.api_key)
    try:
        broker.login(cred.client_id, cred.password, cred.totp_secret)
    except Exception as exc:
        raise HTTPException(400, f"Broker login failed: {exc}")
    return await auto_exit_all_positions(db, user_id, broker, config)


async def _cancel_open_pending_orders(
    db: AsyncSession,
    user_id: int,
    broker,
    reason: str,
):
    result = await db.execute(
        select(PendingOrder).where(
            PendingOrder.user_id == user_id,
            PendingOrder.status == "OPEN",
        )
    )
    manager = OrderManager(broker)
    for pending in result.scalars():
        try:
            broker.cancel_order(pending.order_id)
        except Exception:
            pass
        await manager.on_order_cancelled(db, pending, reason=reason)


async def _load_config(db: AsyncSession, user_id: int) -> GlobalConfig:
    result = await db.execute(select(GlobalConfig).where(GlobalConfig.user_id == user_id))
    config = result.scalar_one_or_none()
    if config is None:
        config = GlobalConfig(user_id=user_id)
        db.add(config)
        await db.flush()
    return config


async def _load_credentials(
    db: AsyncSession,
    user_id: int,
    required: bool = True,
) -> BrokerCredential | None:
    result = await db.execute(
        select(BrokerCredential).where(BrokerCredential.user_id == user_id)
    )
    cred = result.scalar_one_or_none()
    if cred is None and required:
        raise HTTPException(400, "Broker credentials not configured.")
    return cred


async def _load_watchlist(db: AsyncSession, user_id: int) -> list[WatchlistItem]:
    result = await db.execute(
        select(WatchlistItem).where(
            WatchlistItem.user_id == user_id,
            WatchlistItem.is_active == True,
        )
    )
    return list(result.scalars())


def _prepare_tick_manager(runtime: EngineRuntime, watchlist_items: list[WatchlistItem]):
    if not watchlist_items:
        return

    runtime.tick_manager.subscribe(
        [
            {
                "exchange": item.exchange,
                "token": item.token,
                "symbol": item.symbol,
            }
            for item in watchlist_items
        ]
    )


async def _seed_warmup_candles(runtime: EngineRuntime, watchlist_items: list[WatchlistItem]):
    if not watchlist_items:
        return

    to_dt = market_now()
    from_dt = to_dt - timedelta(days=5)
    interval = INTERVALS.get(runtime.config.candle_interval, runtime.config.candle_interval)

    for item in watchlist_items:
        try:
            candles = runtime.broker.get_candle_data(
                symbol_token=item.token,
                interval=interval,
                from_date=from_dt.strftime("%Y-%m-%d %H:%M"),
                to_date=to_dt.strftime("%Y-%m-%d %H:%M"),
                exchange=item.exchange,
            )
        except Exception:
            candles = []
        if candles:
            runtime.tick_manager.seed_candle_builder(item.token, candles[-60:])


async def _fetch_historical_candles(runtime: EngineRuntime, item: WatchlistItem) -> list:
    to_dt = market_now()
    from_dt = to_dt - timedelta(days=5)
    interval = INTERVALS.get(runtime.config.candle_interval, runtime.config.candle_interval)

    try:
        return runtime.broker.get_candle_data(
            symbol_token=item.token,
            interval=interval,
            from_date=from_dt.strftime("%Y-%m-%d %H:%M"),
            to_date=to_dt.strftime("%Y-%m-%d %H:%M"),
            exchange=item.exchange,
        ) or []
    except Exception:
        return []


def _filter_finalized_candles(raw_candles: list, candle_interval: str) -> list:
    if not raw_candles:
        return []

    now = market_now()
    interval_minutes = {
        "ONE_MINUTE": 1,
        "THREE_MINUTE": 3,
        "FIVE_MINUTE": 5,
        "TEN_MINUTE": 10,
        "FIFTEEN_MINUTE": 15,
        "THIRTY_MINUTE": 30,
        "ONE_HOUR": 60,
    }.get(candle_interval, 5)
    minute = floor(now.minute / interval_minutes) * interval_minutes
    current_candle_start = now.replace(minute=minute, second=0, microsecond=0)

    filtered = []
    for row in sorted(raw_candles, key=lambda value: value[0]):
        candle_time = parse_exchange_timestamp(row[0])
        if candle_time is None:
            continue
        if candle_time >= current_candle_start:
            continue
        filtered.append(row)
    return filtered


async def _replay_watchlist_history(
    db: AsyncSession,
    runtime: EngineRuntime,
    item: WatchlistItem,
    finalized_candles: list,
):
    today = market_now().date()

    for index in range(len(finalized_candles)):
        runtime.tick_manager.seed_candle_builder(item.token, finalized_candles[: index + 1])
        candle_time = parse_exchange_timestamp(finalized_candles[index][0])
        if candle_time is None or candle_time.date() != today:
            continue

        builder = runtime.tick_manager.get_candle_builder(item.token)
        if builder is None or len(builder.get_all_candles()) < 3:
            continue

        await analyze_finalized_candle(
            db,
            runtime,
            item.token,
            item.symbol,
            builder.get_all_candles()[-1],
            broadcast_scan=False,
        )


async def _maybe_execute_watchlist_entry(
    db: AsyncSession,
    runtime: EngineRuntime,
    item: WatchlistItem,
):
    if runtime.entries_paused:
        return

    open_position = await db.execute(
        select(OpenPosition).where(
            OpenPosition.user_id == runtime.user_id,
            OpenPosition.watchlist_item_id == item.id,
            OpenPosition.is_open == True,
        )
    )
    if open_position.scalar_one_or_none() is not None:
        return

    open_pending = await db.execute(
        select(PendingOrder).where(
            PendingOrder.user_id == runtime.user_id,
            PendingOrder.watchlist_item_id == item.id,
            PendingOrder.order_type == "ENTRY",
            PendingOrder.status == "OPEN",
        )
    )
    if open_pending.scalar_one_or_none() is not None:
        return

    ltp = runtime.tick_manager.get_ltp(item.token)
    if ltp is None:
        try:
            ltp = runtime.broker.get_ltp(item.exchange, item.symbol, item.token)
        except Exception:
            ltp = None
    if ltp is None:
        return

    action = runtime.tick_evaluator.on_tick(item.token, ltp)
    if action is None or action.action_type != "ENTRY" or action.watchlist_item_id != item.id:
        return

    gap_result = await db.execute(
        select(FVGGap).where(
            FVGGap.id == action.gap_id,
            FVGGap.user_id == runtime.user_id,
            FVGGap.is_active == True,
        )
    )
    gap = gap_result.scalar_one_or_none()
    if gap is None:
        return

    await execute_entry(
        db,
        runtime.user_id,
        gap,
        item,
        runtime.broker,
        runtime.config,
        ltp,
    )


async def _start_market_streams(runtime: EngineRuntime, cred: BrokerCredential):
    runtime.tick_manager.set_connection_callbacks(
        on_open=lambda: runtime._handle_market_reconnect(),
        on_close=lambda code, reason: runtime._handle_market_disconnect(code, reason),
    )
    ws_credentials = getattr(runtime.broker, "ws_credentials", None)
    if settings.NEBULA_BROKER_BACKEND == "live" and ws_credentials:
        runtime.tick_manager._auth_token = ws_credentials.get("auth_token", "")
        runtime.tick_manager._feed_token = ws_credentials.get("feed_token", "")
        runtime.tick_manager._api_key = ws_credentials.get("api_key", cred.api_key)
        runtime.tick_manager._client_code = ws_credentials.get("client_code", cred.client_id)
        runtime.tick_manager.start()

        runtime.order_updates = OrderUpdateManager()
        runtime.order_updates.start(
            ws_credentials.get("auth_token", ""),
            ws_credentials.get("api_key", cred.api_key),
            ws_credentials.get("client_code", cred.client_id),
            ws_credentials.get("feed_token", ""),
            callback=lambda data: runtime._handle_order_update(data),
            loop=asyncio.get_running_loop(),
        )
        if not runtime.order_updates.is_running:
            runtime.polling_task = asyncio.create_task(_poll_open_orders_loop(runtime))
    else:
        runtime.polling_task = asyncio.create_task(_poll_open_orders_loop(runtime))


async def _handle_order_update_impl(runtime: EngineRuntime, data: dict):
    from app.database import async_session_factory

    async with async_session_factory() as db:
        result = await db.execute(
            select(PendingOrder).where(
                PendingOrder.user_id == runtime.user_id,
                PendingOrder.order_id == data["orderid"],
                PendingOrder.status == "OPEN",
            )
        )
        pending = result.scalar_one_or_none()
        if pending is None:
            return

        manager = OrderManager(runtime.broker)
        status = data.get("orderstatus", "").lower()
        fill_price = float(data.get("averageprice") or 0)
        filled_qty = int(data.get("filledshares") or 0)

        if status == "complete":
            await manager.on_order_complete(db, pending, fill_price, filled_qty)
        elif status == "partial":
            await manager.on_order_partial(db, pending, fill_price, filled_qty)
        elif status == "cancelled":
            await manager.on_order_cancelled(db, pending, reason=data.get("text") or "ORDER_CANCELLED")
        elif status == "rejected":
            await manager.on_order_rejected(db, pending, reason=data.get("text") or "ORDER_REJECTED")

        await runtime.refresh_thresholds(db)
        await db.commit()


async def _handle_candle_close_impl(runtime: EngineRuntime, token: str, symbol: str, finalized_candle):
    from app.database import async_session_factory

    async with async_session_factory() as db:
        await analyze_finalized_candle(db, runtime, token, symbol, finalized_candle)
        await db.commit()


async def _handle_market_disconnect_impl(runtime: EngineRuntime, code: str, reason: str):
    from app.database import async_session_factory

    runtime.disconnected_at = market_now()
    runtime.recovery_in_progress = True
    runtime.recovery_failed = False

    async with async_session_factory() as db:
        await runtime.pause_entries(db, f"{code}: {reason}")
        await db.commit()


async def _handle_market_reconnect_impl(runtime: EngineRuntime):
    from app.database import async_session_factory

    if runtime.disconnected_at is None:
        return

    async with async_session_factory() as db:
        ok = await _recover_missing_candles(db, runtime)
        runtime.recovery_in_progress = False
        runtime.recovery_failed = not ok
        if ok:
            runtime.disconnected_at = None
            await runtime.resume_entries(db)
        else:
            await runtime.pause_entries(db, "recovery_failed")
            await ws_manager.broadcast_engine_event(
                "recovery_failed",
                {"user_id": runtime.user_id},
            )
        await db.commit()


async def _recover_missing_candles(db: AsyncSession, runtime: EngineRuntime) -> bool:
    from_dt = runtime.disconnected_at or market_now()
    to_dt = market_now()
    interval = INTERVALS.get(runtime.config.candle_interval, runtime.config.candle_interval)
    all_ok = True

    for token, item in runtime.watchlist_by_token.items():
        try:
            candles = runtime.broker.get_candle_data(
                symbol_token=item.token,
                interval=interval,
                from_date=_format_recovery_time(from_dt),
                to_date=to_dt.strftime("%Y-%m-%d %H:%M"),
                exchange=item.exchange,
            )
        except Exception:
            candles = None

        if not candles:
            all_ok = False
            continue
        runtime.tick_manager.merge_candle_builder_history(token, candles)

    await runtime.refresh_thresholds(db)
    await ws_manager.broadcast_engine_event(
        "recovery_complete" if all_ok else "recovery_partial",
        {"user_id": runtime.user_id},
    )
    return all_ok


def _format_recovery_time(value) -> str:
    minute = floor(value.minute / 5) * 5
    rounded = value.replace(minute=minute, second=0, microsecond=0)
    return rounded.strftime("%Y-%m-%d %H:%M")


async def _poll_open_orders_loop(runtime: EngineRuntime):
    from app.database import async_session_factory

    while True:
        try:
            await asyncio.sleep(3)
            async with async_session_factory() as db:
                result = await db.execute(
                    select(PendingOrder).where(
                        PendingOrder.user_id == runtime.user_id,
                        PendingOrder.status == "OPEN",
                    )
                )
                open_orders = list(result.scalars())
                if not open_orders:
                    continue

                manager = OrderManager(runtime.broker)
                for pending in open_orders:
                    await manager.poll_order_status(db, pending)
                await runtime.refresh_thresholds(db)
                await db.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(3)


async def audit_runtime_candles(db: AsyncSession, runtime: EngineRuntime):
    """Compare recent local finalized candles with broker candles and log drift."""
    interval = INTERVALS.get(runtime.config.candle_interval, runtime.config.candle_interval)
    to_dt = market_now()
    from_dt = to_dt - timedelta(minutes=20)

    for token, item in runtime.watchlist_by_token.items():
        builder = runtime.tick_manager.get_candle_builder(token)
        if builder is None:
            continue

        local_candles = builder.get_all_candles()
        if not local_candles:
            continue

        try:
            broker_candles = runtime.broker.get_candle_data(
                symbol_token=item.token,
                interval=interval,
                from_date=from_dt.strftime("%Y-%m-%d %H:%M"),
                to_date=to_dt.strftime("%Y-%m-%d %H:%M"),
                exchange=item.exchange,
            )
        except Exception as exc:
            db.add(AuditLog(
                user_id=runtime.user_id,
                action="CANDLE_AUDIT_FAILED",
                symbol=item.symbol,
                status="FAILED",
                details=str(exc),
            ))
            continue

        broker_map = {
            str(row[0]): {
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
            }
            for row in broker_candles
        }

        local = local_candles[-1]
        broker = broker_map.get(local.time)
        if broker is None:
            continue

        drift = {
            "open": round(local.open - broker["open"], 4),
            "high": round(local.high - broker["high"], 4),
            "low": round(local.low - broker["low"], 4),
            "close": round(local.close - broker["close"], 4),
        }
        status = "MATCH" if all(abs(value) < 0.0001 for value in drift.values()) else "DRIFT"
        db.add(AuditLog(
            user_id=runtime.user_id,
            action="CANDLE_AUDIT",
            symbol=item.symbol,
            status=status,
            details=(
                f"time={local.time};"
                f" local=({local.open},{local.high},{local.low},{local.close});"
                f" broker=({broker['open']},{broker['high']},{broker['low']},{broker['close']});"
                f" drift={drift}"
            ),
        ))


EngineRuntime._handle_order_update = _handle_order_update_impl
EngineRuntime._handle_candle_close = _handle_candle_close_impl
EngineRuntime._handle_market_disconnect = _handle_market_disconnect_impl
EngineRuntime._handle_market_reconnect = _handle_market_reconnect_impl
