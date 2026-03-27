"""Stock-related Pydantic schemas — config, watchlist, search, LTP."""

from pydantic import BaseModel


class GlobalConfigIn(BaseModel):
    candle_interval: str | None = None
    scan_interval_seconds: int | None = None
    signal_cooldown_minutes: int | None = None
    trend_ema_period: int | None = None
    trend_slope_lookback: int | None = None
    trend_slope_threshold: float | None = None
    atr_period: int | None = None
    atr_multiplier: float | None = None
    atr_tight_multiplier: float | None = None
    max_profit_per_trade: float | None = None
    tp_multiplier: float | None = None
    min_gap_percent: float | None = None
    auto_exit_time: str | None = None
    no_entry_after: str | None = None
    entry_order_timeout: int | None = None
    exit_order_timeout: int | None = None
    exit_retry_max: int | None = None
    exit_retry_price_slip_pct: float | None = None
    otr_price_band_pct: float | None = None
    webhook_url: str | None = None
    is_active: bool | None = None


class GlobalConfigOut(BaseModel):
    candle_interval: str
    scan_interval_seconds: int
    signal_cooldown_minutes: int
    trend_ema_period: int
    trend_slope_lookback: int
    trend_slope_threshold: float
    atr_period: int
    atr_multiplier: float
    atr_tight_multiplier: float
    max_profit_per_trade: float
    tp_multiplier: float
    min_gap_percent: float
    auto_exit_time: str
    no_entry_after: str
    entry_order_timeout: int
    exit_order_timeout: int
    exit_retry_max: int
    exit_retry_price_slip_pct: float
    otr_price_band_pct: float
    webhook_url: str
    is_active: bool


class WatchlistItemIn(BaseModel):
    symbol: str
    token: str
    quantity: int = 1
    ema_low: float | None = None
    ema_high: float | None = None


class WatchlistItemUpdate(BaseModel):
    quantity: int | None = None
    ema_low: float | None = None
    ema_high: float | None = None
    is_active: bool | None = None


class WatchlistItemOut(BaseModel):
    id: int
    symbol: str
    token: str
    exchange: str
    quantity: int
    ema_low: float | None = None
    ema_high: float | None = None
    is_active: bool


class SearchScripOut(BaseModel):
    exchange: str
    tradingsymbol: str
    symboltoken: str


class LTPOut(BaseModel):
    symbol: str
    token: str
    exchange: str
    ltp: float | None = None
    is_active: bool


class MessageOut(BaseModel):
    message: str
