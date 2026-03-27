"""Angel One API constants."""

EXCHANGE_NSE = "NSE"

# Candle interval mapping for getCandleData
INTERVALS = {
    "ONE_MINUTE": "ONE_MINUTE",
    "THREE_MINUTE": "THREE_MINUTE",
    "FIVE_MINUTE": "FIVE_MINUTE",
    "TEN_MINUTE": "TEN_MINUTE",
    "FIFTEEN_MINUTE": "FIFTEEN_MINUTE",
    "THIRTY_MINUTE": "THIRTY_MINUTE",
    "ONE_HOUR": "ONE_HOUR",
    "ONE_DAY": "ONE_DAY",
}

# Interval duration in minutes (for candle builder)
INTERVAL_MINUTES = {
    "ONE_MINUTE": 1,
    "THREE_MINUTE": 3,
    "FIVE_MINUTE": 5,
    "TEN_MINUTE": 10,
    "FIFTEEN_MINUTE": 15,
    "THIRTY_MINUTE": 30,
    "ONE_HOUR": 60,
}

ORDER_TYPE_LIMIT = "LIMIT"
PRODUCT_INTRADAY = "INTRADAY"
VARIETY_NORMAL = "NORMAL"
DURATION_DAY = "DAY"
