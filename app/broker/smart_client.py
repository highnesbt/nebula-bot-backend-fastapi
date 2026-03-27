"""Live Angel One broker client — wraps SmartConnect with order lifecycle methods."""

import time as _time
from typing import Optional

import pyotp
from SmartApi import SmartConnect

from app.broker.constants import (
    DURATION_DAY,
    EXCHANGE_NSE,
    ORDER_TYPE_LIMIT,
    PRODUCT_INTRADAY,
    VARIETY_NORMAL,
)
from app.broker.exceptions import BrokerAuthError, BrokerDataError, BrokerOrderError
from app.engine.pricing import round_to_tick_size


class AngelOneClient:
    """Production broker client wrapping SmartConnect."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.smart = SmartConnect(api_key=api_key)
        self._authenticated = False

    def login(self, client_id: str, password: str, totp_secret: str) -> dict:
        """Generate session with Angel One."""
        try:
            totp = pyotp.TOTP(totp_secret).now()
            data = self.smart.generateSession(client_id, password, totp)
            if data.get("status"):
                self._authenticated = True
                return data
            raise BrokerAuthError(data.get("message", "Login failed"))
        except BrokerAuthError:
            raise
        except Exception as e:
            raise BrokerAuthError(f"Login failed: {e}")

    @property
    def ws_credentials(self) -> Optional[dict]:
        """Return WebSocket credentials if authenticated."""
        if self._authenticated and self.smart.access_token and self.smart.feed_token:
            return {
                "auth_token": self.smart.access_token,
                "feed_token": self.smart.feed_token,
                "api_key": self.api_key,
                "client_code": self.smart.userId or "",
            }
        return None

    def get_candle_data(
        self, symbol_token: str, interval: str,
        from_date: str, to_date: str, exchange: str = EXCHANGE_NSE,
    ) -> list:
        """Fetch historical OHLC candle data."""
        params = {
            "exchange": exchange,
            "symboltoken": symbol_token,
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date,
        }
        try:
            result = self.smart.getCandleData(params)
            if result.get("status") and result.get("data"):
                return result["data"]
            raise BrokerDataError(result.get("message", "No data"))
        except BrokerDataError:
            raise
        except Exception as e:
            raise BrokerDataError(f"Candle data fetch failed: {e}")

    def place_order(
        self, symbol: str, symbol_token: str, transaction_type: str,
        quantity: int, exchange: str = EXCHANGE_NSE, price: float = 0.0,
    ) -> str:
        """Place an INTRADAY LIMIT order. Returns order ID."""
        if price <= 0:
            raise BrokerOrderError("Limit order requires a positive price.")

        normalized_price = round_to_tick_size(price)
        params = {
            "variety": VARIETY_NORMAL,
            "tradingsymbol": symbol,
            "symboltoken": symbol_token,
            "transactiontype": transaction_type,
            "exchange": exchange,
            "ordertype": ORDER_TYPE_LIMIT,
            "producttype": PRODUCT_INTRADAY,
            "duration": DURATION_DAY,
            "quantity": str(quantity),
            "price": str(normalized_price),
        }
        try:
            response = self.smart._postRequest("api.order.place", params)
            if response and response.get("status"):
                data = response.get("data")
                if data and "orderid" in data:
                    return data["orderid"]
                raise BrokerOrderError(
                    f"Order placed but no order ID in response: {response}"
                )
            message = (response or {}).get("message", "Unknown rejection reason")
            error_code = (response or {}).get("errorcode", "")
            detail = f"Angel One rejected order: {message}"
            if error_code:
                detail += f" (code: {error_code})"
            raise BrokerOrderError(detail)
        except BrokerOrderError:
            raise
        except Exception as e:
            raise BrokerOrderError(f"Order failed: {e}")

    def cancel_order(self, order_id: str, variety: str = VARIETY_NORMAL) -> bool:
        """Cancel a pending order."""
        try:
            params = {"variety": variety, "orderid": order_id}
            response = self.smart._postRequest("api.order.cancel", params)
            return bool(response and response.get("status"))
        except Exception:
            return False

    def modify_order(
        self, order_id: str, new_price: float,
        variety: str = VARIETY_NORMAL, **kwargs,
    ) -> bool:
        """Modify a pending order's price."""
        try:
            params = {
                "variety": variety,
                "orderid": order_id,
                "price": str(round_to_tick_size(new_price)),
                **kwargs,
            }
            response = self.smart._postRequest("api.order.modify", params)
            return bool(response and response.get("status"))
        except Exception:
            return False

    def get_order_status(self, order_id: str) -> dict:
        """Get current status of an order. Returns latest status entry."""
        try:
            result = self.smart.individual_order_details(order_id)
            if result and result.get("status") and result.get("data"):
                orders = result["data"]
                for entry in reversed(orders):
                    return {
                        "orderstatus": (entry.get("orderstatus") or "").lower(),
                        "averageprice": entry.get("averageprice", "0"),
                        "filledshares": entry.get("filledshares", "0"),
                        "text": entry.get("text", ""),
                    }
            return {"orderstatus": "unknown"}
        except Exception:
            return {"orderstatus": "unknown"}

    def get_order_fill_price(self, order_id: str, max_attempts: int = 5) -> Optional[float]:
        """Poll for order fill price."""
        for attempt in range(max_attempts):
            try:
                result = self.smart.individual_order_details(order_id)
                if result and result.get("status") and result.get("data"):
                    for entry in reversed(result["data"]):
                        status = (entry.get("orderstatus") or "").lower()
                        avg_price = entry.get("averageprice")
                        if status == "complete" and avg_price:
                            price = float(avg_price)
                            if price > 0:
                                return price
            except Exception:
                pass
            if attempt < max_attempts - 1:
                _time.sleep(1)
        return None

    def get_ltp(self, exchange: str, symbol: str, token: str) -> float:
        """Get last traded price."""
        try:
            result = self.smart.ltpData(exchange, symbol, token)
            if result.get("status") and result.get("data"):
                return float(result["data"]["ltp"])
            raise BrokerDataError("LTP fetch failed")
        except BrokerDataError:
            raise
        except Exception as e:
            raise BrokerDataError(f"LTP failed: {e}")

    def search_scrip(self, exchange: str, query: str) -> list:
        """Search for scrips by name."""
        try:
            result = self.smart.searchScrip(exchange, query)
            if result.get("status") and result.get("data"):
                return result["data"]
            return []
        except Exception:
            return []

    def logout(self, client_id: str):
        """Terminate session."""
        try:
            self.smart.terminateSession(client_id)
        except Exception:
            pass
