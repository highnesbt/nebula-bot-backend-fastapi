"""GlobalConfig and WatchlistItem models — ported from Django stocks app."""

import datetime
from datetime import time as dt_time
from datetime import timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class GlobalConfig(Base):
    """Per-user engine configuration — strategy parameters, timing, risk limits."""

    __tablename__ = "global_configs"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), unique=True)

    # ── Candle / Scan ─────────────────────────────────────────────────────
    candle_interval: Mapped[str] = mapped_column(String(20), default="FIVE_MINUTE")
    scan_interval_seconds: Mapped[int] = mapped_column(Integer, default=30)
    signal_cooldown_minutes: Mapped[int] = mapped_column(Integer, default=15)

    # ── Trend (EMA) ───────────────────────────────────────────────────────
    trend_ema_period: Mapped[int] = mapped_column(Integer, default=15)
    trend_slope_lookback: Mapped[int] = mapped_column(Integer, default=20)
    trend_slope_threshold: Mapped[float] = mapped_column(Float, default=0.2)

    # ── ATR / Risk ────────────────────────────────────────────────────────
    atr_period: Mapped[int] = mapped_column(Integer, default=14)
    atr_multiplier: Mapped[float] = mapped_column(Float, default=1.5)
    atr_tight_multiplier: Mapped[float] = mapped_column(Float, default=0.5)
    max_profit_per_trade: Mapped[float] = mapped_column(Float, default=2000.0)
    tp_multiplier: Mapped[float] = mapped_column(Float, default=2.0)
    min_gap_percent: Mapped[float] = mapped_column(Float, default=0.0)

    # ── Timing ────────────────────────────────────────────────────────────
    auto_exit_time: Mapped[dt_time] = mapped_column(Time, default=dt_time(15, 14))
    no_entry_after: Mapped[dt_time] = mapped_column(Time, default=dt_time(15, 0))

    # ── Order lifecycle ───────────────────────────────────────────────────
    entry_order_timeout: Mapped[int] = mapped_column(Integer, default=60)  # seconds
    exit_order_timeout: Mapped[int] = mapped_column(Integer, default=15)   # seconds
    exit_retry_max: Mapped[int] = mapped_column(Integer, default=3)
    exit_retry_price_slip_pct: Mapped[float] = mapped_column(Float, default=1.0)  # 1%

    # ── OTR compliance ────────────────────────────────────────────────────
    otr_price_band_pct: Mapped[float] = mapped_column(Float, default=0.75)  # ±0.75%

    # ── Webhook / Misc ────────────────────────────────────────────────────
    webhook_url: Mapped[str] = mapped_column(Text, default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)

    # Relationship
    user: Mapped["User"] = relationship(back_populates="config")

    def __repr__(self) -> str:
        return f"<GlobalConfig user_id={self.user_id} active={self.is_active}>"


class WatchlistItem(Base):
    """A stock in the user's watchlist — scanned by the engine."""

    __tablename__ = "watchlist_items"
    __table_args__ = (
        UniqueConstraint("user_id", "symbol", name="uq_user_symbol"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    symbol: Mapped[str] = mapped_column(String(50))  # e.g. "SBIN-EQ"
    token: Mapped[str] = mapped_column(String(20))    # Angel One token
    exchange: Mapped[str] = mapped_column(String(10), default="NSE")
    quantity: Mapped[int] = mapped_column(Integer, default=1)

    # Optional per-stock EMA overrides
    ema_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    ema_high: Mapped[float | None] = mapped_column(Float, nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.datetime.now(timezone.utc)
    )

    def __repr__(self) -> str:
        return f"<WatchlistItem {self.symbol} qty={self.quantity}>"


# Forward reference
from app.models.user import User  # noqa: E402, F401
