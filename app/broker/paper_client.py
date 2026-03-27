"""Paper trading client — simulates orders without real money."""

import uuid
from datetime import datetime, timezone


class PaperTradingClient:
    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        self._logged_in = False
        self._orders: dict = {}

    def login(self, client_id: str, password: str, totp_secret: str):
        self._logged_in = True

    def get_ltp(self, exchange: str, symbol: str, token: str) -> float:
        return 100.0  # Paper mode returns a fixed LTP

    def get_candle_data(self, symbol_token: str, interval: str,
                        from_date: str, to_date: str, exchange: str = "NSE") -> list:
        return []

    def place_order(self, symbol: str, symbol_token: str, transaction_type: str,
                    quantity: int, exchange: str = "NSE", price: float = 0.0) -> str:
        order_id = f"PAPER_{uuid.uuid4().hex[:8].upper()}"
        self._orders[order_id] = {
            "orderstatus": "complete",
            "averageprice": str(price),
            "filledshares": str(quantity),
            "symbol": symbol,
            "placed_at": datetime.now(timezone.utc).isoformat(),
        }
        return order_id

    def get_order_fill_price(self, order_id: str, max_attempts: int = 5) -> float | None:
        order = self._orders.get(order_id)
        if order:
            return float(order["averageprice"])
        return None

    def cancel_order(self, order_id: str, variety: str = "NORMAL") -> bool:
        if order_id in self._orders:
            self._orders[order_id]["orderstatus"] = "cancelled"
            return True
        return False

    def modify_order(self, order_id: str, new_price: float, **kwargs) -> bool:
        if order_id in self._orders:
            self._orders[order_id]["averageprice"] = str(new_price)
            return True
        return False

    def get_order_status(self, order_id: str) -> dict:
        return self._orders.get(order_id, {"orderstatus": "unknown"})

    def search_scrip(self, exchange: str, query: str) -> list:
        return [
            {"exchange": exchange, "tradingsymbol": f"{query}-EQ", "symboltoken": "99999"},
        ]
