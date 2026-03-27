"""Engine-related Pydantic schemas — gaps, positions, signals, engine status."""

from pydantic import BaseModel


class EngineStatusOut(BaseModel):
    is_active: bool
    open_positions: int
    active_gaps: int


class FVGGapOut(BaseModel):
    id: int
    symbol: str
    gap_type: str
    entry_price: float
    stop_loss: float
    take_profit: float
    gap_high: float
    gap_low: float
    is_active: bool
    detected_at: str
    filled_at: str | None = None


class OpenPositionOut(BaseModel):
    id: int
    symbol: str
    position_type: str
    quantity: int
    entry_price: float
    stop_loss: float
    take_profit: float
    max_price_reached: float
    min_price_reached: float
    current_tsl: float
    is_open: bool
    pnl: float
    exit_price: float | None = None
    opened_at: str
    closed_at: str | None = None


class SignalOut(BaseModel):
    id: int
    symbol: str
    signal_type: str
    price: float
    order_id: str
    order_status: str
    pnl: float
    exit_reason: str
    error_message: str
    created_at: str


class SignalListOut(BaseModel):
    count: int
    results: list[SignalOut]


class ExitAllOut(BaseModel):
    closed: int
    failed: int
    total_pnl: float


class MessageOut(BaseModel):
    message: str
