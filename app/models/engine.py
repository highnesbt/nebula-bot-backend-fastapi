"""Engine models — FVGGap, Signal, OpenPosition, PendingOrder, AuditLog.

Ported from Django engine app with added PendingOrder and AuditLog
for order lifecycle management and April 2026 compliance.
"""

import datetime
from datetime import timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class FVGGap(Base):
    """A detected Fair Value Gap — waiting for price to fill it."""

    __tablename__ = "fvg_gaps"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    watchlist_item_id: Mapped[int] = mapped_column(ForeignKey("watchlist_items.id"))
    symbol: Mapped[str] = mapped_column(String(50), default="")

    gap_type: Mapped[str] = mapped_column(String(10))  # "LONG" or "SHORT"
    gap_high: Mapped[float] = mapped_column(Float)
    gap_low: Mapped[float] = mapped_column(Float)
    entry_price: Mapped[float] = mapped_column(Float)
    stop_loss: Mapped[float] = mapped_column(Float)
    take_profit: Mapped[float] = mapped_column(Float)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    detected_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.datetime.now(timezone.utc)
    )
    filled_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    watchlist_item: Mapped["WatchlistItem"] = relationship(lazy="selectin")

    def __repr__(self) -> str:
        return f"<FVGGap {self.symbol} {self.gap_type} active={self.is_active}>"


class Signal(Base):
    """Audit log of every order placed — entry, exit, auto-exit."""

    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    watchlist_item_id: Mapped[int | None] = mapped_column(
        ForeignKey("watchlist_items.id"), nullable=True
    )
    symbol: Mapped[str] = mapped_column(String(50), default="")

    signal_type: Mapped[str] = mapped_column(String(30))  # LONG_ENTRY, SHORT_EXIT, etc.
    price: Mapped[float] = mapped_column(Float, default=0.0)
    order_id: Mapped[str] = mapped_column(String(50), default="")
    order_status: Mapped[str] = mapped_column(String(30), default="")
    pnl: Mapped[float] = mapped_column(Float, default=0.0)
    exit_reason: Mapped[str] = mapped_column(String(30), default="")
    error_message: Mapped[str] = mapped_column(Text, default="")

    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.datetime.now(timezone.utc)
    )

    watchlist_item: Mapped["WatchlistItem"] = relationship(lazy="selectin")

    def __repr__(self) -> str:
        return f"<Signal {self.signal_type} {self.symbol}>"


class OpenPosition(Base):
    """Tracks an active (or closed) position with TSL state."""

    __tablename__ = "open_positions"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "watchlist_item_id", "is_open",
            name="uq_user_watchlist_open",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    watchlist_item_id: Mapped[int] = mapped_column(ForeignKey("watchlist_items.id"))
    symbol: Mapped[str] = mapped_column(String(50), default="")

    position_type: Mapped[str] = mapped_column(String(10))  # "LONG" or "SHORT"
    quantity: Mapped[int] = mapped_column(Integer)
    entry_price: Mapped[float] = mapped_column(Float)
    stop_loss: Mapped[float] = mapped_column(Float, default=0.0)
    take_profit: Mapped[float] = mapped_column(Float, default=0.0)

    # TSL state
    max_price_reached: Mapped[float] = mapped_column(Float, default=0.0)
    min_price_reached: Mapped[float] = mapped_column(Float, default=0.0)
    current_tsl: Mapped[float] = mapped_column(Float, default=0.0)

    # Lifecycle
    is_open: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    exit_pending: Mapped[bool] = mapped_column(Boolean, default=False)
    exit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    pnl: Mapped[float] = mapped_column(Float, default=0.0)
    exit_reason: Mapped[str] = mapped_column(String(30), default="")

    opened_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.datetime.now(timezone.utc)
    )
    closed_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    watchlist_item: Mapped["WatchlistItem"] = relationship(lazy="selectin")

    def __repr__(self) -> str:
        return f"<OpenPosition {self.symbol} {self.position_type} open={self.is_open}>"


class PendingOrder(Base):
    """Tracks a limit order from placement to fill/cancel/reject.

    OpenPosition is only created once a PendingOrder reaches COMPLETE.
    """

    __tablename__ = "pending_orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    watchlist_item_id: Mapped[int] = mapped_column(ForeignKey("watchlist_items.id"))

    order_id: Mapped[str] = mapped_column(String(50), index=True)  # Angel One order ID
    order_type: Mapped[str] = mapped_column(String(10))  # "ENTRY" or "EXIT"
    direction: Mapped[str] = mapped_column(String(10))   # "BUY" or "SELL"

    # Links to related records
    gap_id: Mapped[int | None] = mapped_column(ForeignKey("fvg_gaps.id"), nullable=True)
    position_id: Mapped[int | None] = mapped_column(
        ForeignKey("open_positions.id"), nullable=True
    )

    placed_price: Mapped[float] = mapped_column(Float)
    filled_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_quantity: Mapped[int] = mapped_column(Integer, default=0)
    total_quantity: Mapped[int] = mapped_column(Integer)

    # State machine: OPEN → COMPLETE | CANCELLED | REJECTED
    status: Mapped[str] = mapped_column(String(20), default="OPEN", index=True)
    cancel_reason: Mapped[str] = mapped_column(String(50), default="")
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str] = mapped_column(Text, default="")

    placed_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.datetime.now(timezone.utc)
    )
    resolved_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    watchlist_item: Mapped["WatchlistItem"] = relationship(lazy="selectin")

    def __repr__(self) -> str:
        return f"<PendingOrder {self.order_id} {self.status}>"


class AuditLog(Base):
    """Immutable audit trail for all trading actions — 5-year retention compliance."""

    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)

    action: Mapped[str] = mapped_column(String(50))  # ORDER_PLACED, ORDER_FILLED, etc.
    order_id: Mapped[str] = mapped_column(String(50), default="")
    symbol: Mapped[str] = mapped_column(String(50), default="")
    direction: Mapped[str] = mapped_column(String(10), default="")
    price: Mapped[float] = mapped_column(Float, default=0.0)
    quantity: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(30), default="")
    details: Mapped[str] = mapped_column(Text, default="")  # JSON extra info

    timestamp: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.datetime.now(timezone.utc),
        index=True,
    )

    def __repr__(self) -> str:
        return f"<AuditLog {self.action} {self.symbol} @ {self.timestamp}>"


# Forward reference
from app.models.stock import WatchlistItem  # noqa: E402, F401
