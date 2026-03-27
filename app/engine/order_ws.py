"""Angel One Order-Update WebSocket — real-time order status stream.

Listens for order status changes (open → complete/cancelled/rejected)
and dispatches to OrderManager for state transitions.

Uses the smartapi-python WebSocket client with mode=1 (order updates).
Falls back to REST polling if WebSocket connection fails.
"""

import asyncio
import json
import logging
import threading
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class OrderUpdateManager:
    """
    Manages the Angel One order-update WebSocket connection.

    On each status update, calls the registered callback with:
        {
            "orderid": str,
            "orderstatus": str,  # "open", "complete", "cancelled", "rejected"
            "averageprice": str,
            "filledshares": str,
            "text": str,
        }
    """

    def __init__(self):
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._callback: Optional[Callable] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def start(self, auth_token: str, api_key: str, client_code: str,
              feed_token: str, callback: Callable,
              loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        Start WebSocket in a background thread.

        callback receives a dict with order update fields.
        If loop is provided, callback will be scheduled on that event loop.
        """
        self._callback = callback
        self._loop = loop
        self._running = True

        def _run():
            try:
                from SmartApi.smartWebSocketOrderUpdate import SmartWebSocketOrderUpdate

                ws = SmartWebSocketOrderUpdate(
                    auth_token, api_key, client_code, feed_token
                )

                def on_data(wsapp, message):
                    try:
                        data = json.loads(message) if isinstance(message, str) else message
                        if data and isinstance(data, dict):
                            self._dispatch(data)
                    except Exception as e:
                        logger.error(f"Order WS on_data error: {e}")

                def on_error(wsapp, error):
                    logger.error(f"Order WS error: {error}")

                def on_close(wsapp):
                    logger.info("Order WS closed")
                    self._running = False

                def on_open(wsapp):
                    logger.info("Order WS connected")

                ws.on_data = on_data
                ws.on_error = on_error
                ws.on_close = on_close
                ws.on_open = on_open

                self._ws = ws
                ws.connect()

            except ImportError:
                logger.warning(
                    "SmartWebSocketOrderUpdate not available. "
                    "Using REST polling fallback."
                )
                self._running = False
            except Exception as e:
                logger.error(f"Order WS failed to start: {e}")
                self._running = False

        self._thread = threading.Thread(target=_run, daemon=True, name="order-ws")
        self._thread.start()
        logger.info("Order update WebSocket thread started")

    def stop(self):
        """Stop the WebSocket connection."""
        self._running = False
        if self._ws:
            try:
                self._ws.close_connection()
            except Exception:
                pass
        self._ws = None
        logger.info("Order update WebSocket stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    def _dispatch(self, data: dict):
        """Dispatch order update to callback."""
        if self._callback is None:
            return

        normalized = {
            "orderid": str(data.get("orderid", data.get("order_id", ""))),
            "orderstatus": (data.get("orderstatus", data.get("order_status", ""))).lower(),
            "averageprice": str(data.get("averageprice", data.get("average_price", "0"))),
            "filledshares": str(data.get("filledshares", data.get("filled_shares", "0"))),
            "text": data.get("text", ""),
        }

        if not normalized["orderid"]:
            return

        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self._callback(normalized), self._loop
            )
        else:
            # Sync fallback
            try:
                self._callback(normalized)
            except Exception as e:
                logger.error(f"Order WS callback error: {e}")


# Singleton
order_update_manager = OrderUpdateManager()
