"""Live Angel One broker client — wraps SmartConnect with order lifecycle methods."""

import logging
import time as _time
from typing import Any, Callable, Optional

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
from app.broker.totp import normalize_totp_secret
from app.engine.pricing import round_to_tick_size

logger = logging.getLogger(__name__)

_SESSION_ERROR_CODES = {"AB1010", "AG8001"}
_SESSION_ERROR_SNIPPETS = (
    "invalid session",
    "session is expired",
    "please re-login",
    "invalid token",
)


class AngelOneClient:
    """Production broker client wrapping SmartConnect."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.smart = SmartConnect(api_key=api_key)
        self._authenticated = False
        self._client_id = ""
        self._password = ""
        self._totp_secret = ""
        self._session_listener: Callable[[dict], None] | None = None

    def set_session_listener(self, callback: Callable[[dict], None] | None):
        """Register a callback for successful session refreshes."""
        self._session_listener = callback

    def login(self, client_id: str, password: str, totp_secret: str) -> dict:
        """Generate session with Angel One."""
        try:
            normalized_secret = normalize_totp_secret(totp_secret)
            self._client_id = client_id
            self._password = password
            self._totp_secret = normalized_secret
            totp = pyotp.TOTP(normalized_secret).now()
            data = self.smart.generateSession(client_id, password, totp)
            if data.get("status"):
                self._authenticated = True
                self._notify_session_listener(data)
                return data
            raise BrokerAuthError(data.get("message", "Login failed"))
        except ValueError as exc:
            raise BrokerAuthError(f"Invalid TOTP secret format: {exc}") from exc
        except BrokerAuthError:
            raise
        except Exception as e:
            raise BrokerAuthError(f"Login failed: {e}")

    @property
    def ws_credentials(self) -> Optional[dict]:
        """Return WebSocket credentials if authenticated."""
        access_token = getattr(self.smart, "access_token", "")
        feed_token = getattr(self.smart, "feed_token", "")
        user_id = getattr(self.smart, "userId", "")
        if self._authenticated and access_token and feed_token:
            return {
                "auth_token": access_token,
                "feed_token": feed_token,
                "api_key": self.api_key,
                "client_code": user_id or "",
            }
        return None

    @property
    def session_tokens(self) -> dict:
        """Return the currently cached session token set."""
        return {
            "auth_token": getattr(self.smart, "access_token", "") or "",
            "feed_token": getattr(self.smart, "feed_token", "") or "",
            "refresh_token": getattr(self.smart, "refresh_token", "") or "",
            "api_key": self.api_key,
            "client_code": getattr(self.smart, "userId", "") or self._client_id,
        }

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
            result = self._request_with_auto_reauth(
                lambda: self.smart.getCandleData(params),
                operation_name="get_candle_data",
            )
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
            response = self._request_with_auto_reauth(
                lambda: self.smart._postRequest("api.order.place", params),
                operation_name="place_order",
            )
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
            response = self._request_with_auto_reauth(
                lambda: self.smart._postRequest("api.order.cancel", params),
                operation_name="cancel_order",
            )
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
            response = self._request_with_auto_reauth(
                lambda: self.smart._postRequest("api.order.modify", params),
                operation_name="modify_order",
            )
            return bool(response and response.get("status"))
        except Exception:
            return False

    def get_order_status(self, order_id: str) -> dict:
        """Get current status of an order. Returns latest status entry."""
        try:
            result = self._request_with_auto_reauth(
                lambda: self.smart.individual_order_details(order_id),
                operation_name="get_order_status",
            )
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
                result = self._request_with_auto_reauth(
                    lambda: self.smart.individual_order_details(order_id),
                    operation_name="get_order_fill_price",
                )
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
            result = self._request_with_auto_reauth(
                lambda: self.smart.ltpData(exchange, symbol, token),
                operation_name="get_ltp",
            )
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
            result = self._request_with_auto_reauth(
                lambda: self.smart.searchScrip(exchange, query),
                operation_name="search_scrip",
            )
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

    def _request_with_auto_reauth(
        self,
        operation: Callable[[], Any],
        operation_name: str,
        allow_retry: bool = True,
    ):
        try:
            response = operation()
        except Exception as exc:
            if allow_retry and self._is_session_error(exc):
                self._reauthenticate(operation_name)
                return self._request_with_auto_reauth(
                    operation,
                    operation_name,
                    allow_retry=False,
                )
            raise

        if allow_retry and self._is_session_payload(response):
            self._reauthenticate(operation_name)
            return self._request_with_auto_reauth(
                operation,
                operation_name,
                allow_retry=False,
            )

        return response

    def _reauthenticate(self, operation_name: str):
        if not self._client_id or not self._password or not self._totp_secret:
            raise BrokerAuthError(
                f"{operation_name} failed: broker session expired and no cached credentials are available."
            )

        logger.warning(
            "SmartAPI session expired during %s; attempting automatic re-auth.",
            operation_name,
        )
        self.login(self._client_id, self._password, self._totp_secret)

    def _notify_session_listener(self, login_data: dict):
        if self._session_listener is None:
            return

        payload = login_data.get("data") or {}
        session = {
            "auth_token": getattr(self.smart, "access_token", "") or payload.get("jwtToken", ""),
            "feed_token": getattr(self.smart, "feed_token", "") or payload.get("feedToken", ""),
            "refresh_token": getattr(self.smart, "refresh_token", "") or payload.get("refreshToken", ""),
            "api_key": self.api_key,
            "client_code": getattr(self.smart, "userId", "") or payload.get("clientcode", self._client_id),
        }
        self._session_listener(session)

    def _is_session_payload(self, payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False

        code = str(payload.get("errorcode") or payload.get("errorCode") or "").upper()
        message = str(payload.get("message") or payload.get("errorMessage") or "").lower()
        return code in _SESSION_ERROR_CODES or any(
            snippet in message for snippet in _SESSION_ERROR_SNIPPETS
        )

    def _is_session_error(self, exc: Exception) -> bool:
        text = str(exc).lower()
        return any(snippet in text for snippet in _SESSION_ERROR_SNIPPETS)
