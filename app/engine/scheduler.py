"""APScheduler setup — scan dispatch, auto-exit, gap cleanup, order timeouts, midnight re-auth."""

import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def _scan_cycle():
    """Run one scan cycle for all active watchlist items."""
    # Imported lazily to avoid circular imports at module scope
    from app.database import async_session_factory
    from app.models.stock import GlobalConfig
    from sqlalchemy import select

    async with async_session_factory() as db:
        result = await db.execute(
            select(GlobalConfig).where(GlobalConfig.is_active == True)
        )
        configs = list(result.scalars())
        for config in configs:
            try:
                await _run_scan_for_user(db, config)
            except Exception as e:
                logger.error(f"Scan cycle error for user {config.user_id}: {e}")
        await db.commit()


async def _run_scan_for_user(db, config):
    """Placeholder for per-user scan cycle — will be wired in integration."""
    logger.debug(f"Scan cycle for user {config.user_id}")


async def _check_order_timeouts():
    """Check and cancel timed-out entry/exit orders."""
    from app.broker.client import get_broker_client
    from app.database import async_session_factory
    from app.engine.order_manager import OrderManager
    from app.models.stock import GlobalConfig
    from app.models.user import BrokerCredential
    from sqlalchemy import select

    async with async_session_factory() as db:
        result = await db.execute(
            select(GlobalConfig).where(GlobalConfig.is_active == True)
        )
        configs = list(result.scalars())

        for config in configs:
            cred_result = await db.execute(
                select(BrokerCredential).where(BrokerCredential.user_id == config.user_id)
            )
            cred = cred_result.scalar_one_or_none()
            if cred is None:
                continue

            broker = get_broker_client(cred.api_key)
            manager = OrderManager(broker)

            try:
                await manager.check_entry_timeout(db, config)
                await manager.check_exit_timeout(db, config)
            except Exception as e:
                logger.error(f"Order timeout check error for user {config.user_id}: {e}")

        await db.commit()


async def _auto_exit():
    """Auto-exit all positions at configured time."""
    from app.broker.client import get_broker_client
    from app.database import async_session_factory
    from app.engine.auto_exit import auto_exit_all_positions
    from app.models.stock import GlobalConfig
    from app.models.user import BrokerCredential
    from sqlalchemy import select

    async with async_session_factory() as db:
        result = await db.execute(select(GlobalConfig))
        configs = list(result.scalars())

        for config in configs:
            cred_result = await db.execute(
                select(BrokerCredential).where(BrokerCredential.user_id == config.user_id)
            )
            cred = cred_result.scalar_one_or_none()
            if cred is None:
                continue

            broker = get_broker_client(cred.api_key)
            try:
                broker.login(cred.client_id, cred.password, cred.totp_secret)
                result = await auto_exit_all_positions(db, config.user_id, broker, config)
                logger.info(f"Auto-exit user {config.user_id}: {result}")
            except Exception as e:
                logger.error(f"Auto-exit error for user {config.user_id}: {e}")

        await db.commit()


async def _midnight_reauth():
    """Midnight re-authentication — refresh broker sessions (SEBI compliance)."""
    from app.broker.client import get_broker_client
    from app.database import async_session_factory
    from app.models.user import BrokerCredential
    from sqlalchemy import select

    logger.info("Midnight re-auth starting...")

    async with async_session_factory() as db:
        result = await db.execute(select(BrokerCredential))
        creds = list(result.scalars())

        for cred in creds:
            try:
                broker = get_broker_client(cred.api_key)
                login_data = broker.login(cred.client_id, cred.password, cred.totp_secret)
                # Store the new JWT token
                if login_data and login_data.get("data"):
                    cred.jwt_token = login_data["data"].get("jwtToken", "")
                    cred.feed_token = login_data["data"].get("feedToken", "")
                logger.info(f"Re-auth success for user {cred.user_id}")
            except Exception as e:
                logger.error(f"Re-auth failed for user {cred.user_id}: {e}")

        await db.commit()


async def _cleanup_stale_gaps():
    """Deactivate FVG gaps older than today."""
    from app.database import async_session_factory
    from app.models.engine import FVGGap
    from sqlalchemy import select, func

    async with async_session_factory() as db:
        today = datetime.utcnow().date()
        result = await db.execute(
            select(FVGGap).where(
                FVGGap.is_active == True,
                func.date(FVGGap.detected_at) < today,
            )
        )
        stale = list(result.scalars())
        for gap in stale:
            gap.is_active = False
        if stale:
            logger.info(f"Cleaned up {len(stale)} stale FVG gaps")
        await db.commit()


def setup_scheduler(scan_interval_seconds: int = 30):
    """Configure and start the APScheduler."""
    # Scan cycle — every N seconds during market hours
    scheduler.add_job(
        _scan_cycle,
        IntervalTrigger(seconds=scan_interval_seconds),
        id="scan_cycle",
        replace_existing=True,
    )

    # Order timeout check — every 15 seconds
    scheduler.add_job(
        _check_order_timeouts,
        IntervalTrigger(seconds=15),
        id="order_timeouts",
        replace_existing=True,
    )

    # Auto-exit — 15:14 IST (Mon-Fri)
    scheduler.add_job(
        _auto_exit,
        CronTrigger(hour=15, minute=14, day_of_week="mon-fri", timezone="Asia/Kolkata"),
        id="auto_exit",
        replace_existing=True,
    )

    # Midnight re-auth — 00:01 IST daily
    scheduler.add_job(
        _midnight_reauth,
        CronTrigger(hour=0, minute=1, timezone="Asia/Kolkata"),
        id="midnight_reauth",
        replace_existing=True,
    )

    # Gap cleanup — 09:10 IST (before market open)
    scheduler.add_job(
        _cleanup_stale_gaps,
        CronTrigger(hour=9, minute=10, day_of_week="mon-fri", timezone="Asia/Kolkata"),
        id="gap_cleanup",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(f"Scheduler started with {scan_interval_seconds}s scan interval")


def stop_scheduler():
    """Shutdown the scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
