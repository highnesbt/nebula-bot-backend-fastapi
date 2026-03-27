"""Nebula v2 — FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.config import settings
from app.database import async_session_factory

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    from app.engine.scheduler import setup_scheduler, stop_scheduler

    logger.info("Nebula v2 starting up...")
    setup_scheduler(scan_interval_seconds=30)
    yield
    stop_scheduler()
    logger.info("Nebula v2 shut down.")


app = FastAPI(
    title="Nebula",
    version="2.0.0",
    description="FVG Intraday Trading Engine",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
from app.routers.auth import router as auth_router  # noqa: E402
from app.routers.stock import router as stock_router  # noqa: E402
from app.routers.engine import router as engine_router  # noqa: E402
from app.routers.ws import router as ws_router  # noqa: E402

app.include_router(auth_router, prefix="/api")
app.include_router(stock_router, prefix="/api")
app.include_router(engine_router, prefix="/api")
app.include_router(ws_router)


@app.get("/healthz")
async def healthcheck():
    """Basic process, scheduler, and DB health for nginx/systemd checks."""
    from app.engine.scheduler import scheduler

    db_ok = False
    async with async_session_factory() as session:
        await session.execute(text("SELECT 1"))
        db_ok = True

    return {
        "status": "ok",
        "service": "nebula-fastapi",
        "database": "ok" if db_ok else "error",
        "scheduler_running": scheduler.running,
    }
