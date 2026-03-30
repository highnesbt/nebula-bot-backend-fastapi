"""Stock router — config, watchlist, search, LTP endpoints."""

import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.database import get_db
from app.models.stock import GlobalConfig, WatchlistItem
from app.models.user import BrokerCredential, User
from app.engine.runtime import onboard_watchlist_item, refresh_runtime_thresholds, get_runtime
from app.schemas.stock import (
    GlobalConfigIn,
    GlobalConfigOut,
    LTPOut,
    MessageOut,
    SearchScripOut,
    WatchlistItemIn,
    WatchlistItemOut,
    WatchlistItemUpdate,
)

router = APIRouter(tags=["Stocks"])


# ── Config endpoints ──────────────────────────────────────────────────────────


def _config_to_dict(config: GlobalConfig) -> dict:
    return {
        "candle_interval": config.candle_interval,
        "scan_interval_seconds": config.scan_interval_seconds,
        "signal_cooldown_minutes": config.signal_cooldown_minutes,
        "trend_ema_period": config.trend_ema_period,
        "trend_slope_lookback": config.trend_slope_lookback,
        "trend_slope_threshold": config.trend_slope_threshold,
        "atr_period": config.atr_period,
        "atr_multiplier": config.atr_multiplier,
        "atr_tight_multiplier": config.atr_tight_multiplier,
        "max_profit_per_trade": config.max_profit_per_trade,
        "tp_multiplier": config.tp_multiplier,
        "min_gap_percent": config.min_gap_percent,
        "auto_exit_time": config.auto_exit_time.strftime("%H:%M"),
        "no_entry_after": config.no_entry_after.strftime("%H:%M"),
        "entry_order_timeout": config.entry_order_timeout,
        "exit_order_timeout": config.exit_order_timeout,
        "exit_retry_max": config.exit_retry_max,
        "exit_retry_price_slip_pct": config.exit_retry_price_slip_pct,
        "otr_price_band_pct": config.otr_price_band_pct,
        "webhook_url": config.webhook_url,
        "is_active": config.is_active,
    }


@router.get("/config/", response_model=GlobalConfigOut)
async def get_config(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get current global config (creates default if missing)."""
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.user_id == user.id)
    )
    config = result.scalar_one_or_none()
    if config is None:
        config = GlobalConfig(user_id=user.id)
        db.add(config)
        await db.flush()
    return _config_to_dict(config)


@router.put("/config/", response_model=GlobalConfigOut)
async def update_config(
    payload: GlobalConfigIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update global config. Only provided fields are changed."""
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.user_id == user.id)
    )
    config = result.scalar_one_or_none()
    if config is None:
        config = GlobalConfig(user_id=user.id)
        db.add(config)
        await db.flush()

    data = payload.model_dump(exclude_unset=True)

    # Parse time strings into time objects
    if "auto_exit_time" in data and data["auto_exit_time"]:
        parts = data["auto_exit_time"].split(":")
        data["auto_exit_time"] = datetime.time(int(parts[0]), int(parts[1]))
    if "no_entry_after" in data and data["no_entry_after"]:
        parts = data["no_entry_after"].split(":")
        data["no_entry_after"] = datetime.time(int(parts[0]), int(parts[1]))

    for key, value in data.items():
        if value is not None:
            setattr(config, key, value)

    runtime = get_runtime(user.id)
    if runtime is not None:
        for key, value in data.items():
            if value is not None:
                setattr(runtime.config, key, value)
        await refresh_runtime_thresholds(db, user.id)

    return _config_to_dict(config)


# ── Watchlist endpoints ───────────────────────────────────────────────────────


def _watchlist_to_dict(item: WatchlistItem) -> dict:
    return {
        "id": item.id,
        "symbol": item.symbol,
        "token": item.token,
        "exchange": item.exchange,
        "quantity": item.quantity,
        "ema_low": item.ema_low,
        "ema_high": item.ema_high,
        "is_active": item.is_active,
        "backrun_status": item.backrun_status,
        "backrun_error": item.backrun_error,
    }


async def _run_backrun_for_item(
    db: AsyncSession,
    user_id: int,
    item: WatchlistItem,
):
    try:
        await onboard_watchlist_item(db, user_id, item)
        runtime = get_runtime(user_id)
        item.backrun_status = "SUCCESS" if runtime is not None and item.is_active else "IDLE"
        item.backrun_error = ""
    except Exception as exc:
        item.backrun_status = "FAILED"
        item.backrun_error = f"Backrun failed: {exc}"


@router.get("/watchlist/", response_model=list[WatchlistItemOut])
async def list_watchlist(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List all watchlist items for the current user."""
    result = await db.execute(
        select(WatchlistItem)
        .where(WatchlistItem.user_id == user.id)
        .order_by(WatchlistItem.created_at.desc())
    )
    return [_watchlist_to_dict(item) for item in result.scalars()]


@router.post("/watchlist/", response_model=WatchlistItemOut)
async def add_to_watchlist(
    payload: WatchlistItemIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Add a stock to the watchlist."""
    result = await db.execute(
        select(WatchlistItem).where(
            WatchlistItem.user_id == user.id,
            WatchlistItem.symbol == payload.symbol,
        )
    )
    if result.scalar_one_or_none():
        raise HTTPException(400, f"{payload.symbol} already in your watchlist.")

    item = WatchlistItem(
        user_id=user.id,
        symbol=payload.symbol,
        token=payload.token,
        exchange="NSE",
        quantity=payload.quantity,
        ema_low=payload.ema_low,
        ema_high=payload.ema_high,
    )
    db.add(item)
    await db.flush()
    item.backrun_status = "IDLE"
    item.backrun_error = ""
    await db.refresh(item)
    await _run_backrun_for_item(db, user.id, item)
    await db.flush()
    await db.refresh(item)
    return _watchlist_to_dict(item)


@router.get("/watchlist/{item_id}", response_model=WatchlistItemOut)
async def get_watchlist_item(
    item_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a single watchlist item."""
    result = await db.execute(
        select(WatchlistItem).where(
            WatchlistItem.id == item_id, WatchlistItem.user_id == user.id
        )
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(404, "Watchlist item not found.")
    return _watchlist_to_dict(item)


@router.put("/watchlist/{item_id}", response_model=WatchlistItemOut)
async def update_watchlist_item(
    item_id: int,
    payload: WatchlistItemUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a watchlist item."""
    result = await db.execute(
        select(WatchlistItem).where(
            WatchlistItem.id == item_id, WatchlistItem.user_id == user.id
        )
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(404, "Watchlist item not found.")

    data = payload.model_dump(exclude_unset=True)
    for key, value in data.items():
        if value is not None:
            setattr(item, key, value)

    await db.flush()
    await db.refresh(item)
    return _watchlist_to_dict(item)


@router.post("/watchlist/{item_id}/retry-backrun", response_model=WatchlistItemOut)
async def retry_watchlist_backrun(
    item_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Retry historical backrun for an existing watchlist item."""
    result = await db.execute(
        select(WatchlistItem).where(
            WatchlistItem.id == item_id,
            WatchlistItem.user_id == user.id,
        )
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(404, "Watchlist item not found.")

    await _run_backrun_for_item(db, user.id, item)
    await db.flush()
    await db.refresh(item)
    return _watchlist_to_dict(item)


@router.delete("/watchlist/{item_id}", response_model=MessageOut)
async def delete_watchlist_item(
    item_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a stock from the watchlist."""
    result = await db.execute(
        select(WatchlistItem).where(
            WatchlistItem.id == item_id, WatchlistItem.user_id == user.id
        )
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(404, "Watchlist item not found.")

    await db.delete(item)
    return MessageOut(message="Removed from watchlist.")


# ── Search ────────────────────────────────────────────────────────────────────


@router.get("/search/", response_model=list[SearchScripOut])
async def search_scrip(
    query: str = Query(...),
    exchange: str = Query("NSE"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Search for a stock symbol and its token from the broker."""
    from app.config import settings

    result = await db.execute(
        select(BrokerCredential).where(BrokerCredential.user_id == user.id)
    )
    cred = result.scalar_one_or_none()
    if cred is None:
        raise HTTPException(400, "Broker credentials not configured.")

    # Lazy import to avoid circular deps at module scope
    from app.broker.client import get_broker_client

    client = get_broker_client(cred.api_key)
    try:
        client.login(cred.client_id, cred.password, cred.totp_secret)
        results = client.search_scrip(exchange, query)
        return [
            SearchScripOut(
                exchange=r.get("exchange", exchange),
                tradingsymbol=r.get("tradingsymbol", ""),
                symboltoken=r.get("symboltoken", ""),
            )
            for r in results
        ]
    except Exception as e:
        raise HTTPException(400, f"Search failed: {e}")


# ── LTP ───────────────────────────────────────────────────────────────────────


@router.get("/ltp/", response_model=list[LTPOut])
async def get_watchlist_ltp(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get last traded price for all watchlist items."""
    result = await db.execute(
        select(WatchlistItem)
        .where(WatchlistItem.user_id == user.id)
        .order_by(WatchlistItem.symbol)
    )
    items = list(result.scalars())
    if not items:
        return []

    # Try to get broker client
    cred_result = await db.execute(
        select(BrokerCredential).where(BrokerCredential.user_id == user.id)
    )
    cred = cred_result.scalar_one_or_none()

    if cred is None:
        return [
            LTPOut(symbol=i.symbol, token=i.token, exchange=i.exchange,
                   ltp=None, is_active=i.is_active)
            for i in items
        ]

    from app.broker.client import get_broker_client

    client = get_broker_client(cred.api_key)
    try:
        client.login(cred.client_id, cred.password, cred.totp_secret)
    except Exception:
        return [
            LTPOut(symbol=i.symbol, token=i.token, exchange=i.exchange,
                   ltp=None, is_active=i.is_active)
            for i in items
        ]

    results = []
    for item in items:
        try:
            ltp = client.get_ltp(item.exchange, item.symbol, item.token)
        except Exception:
            ltp = None
        results.append(LTPOut(
            symbol=item.symbol, token=item.token, exchange=item.exchange,
            ltp=ltp, is_active=item.is_active,
        ))
    return results
