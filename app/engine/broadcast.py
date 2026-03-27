"""WebSocket broadcast manager for FastAPI."""

import asyncio
import logging
from typing import Dict, Set

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and broadcasts messages."""

    def __init__(self):
        # {channel_name: set of websockets}
        self._channels: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, channel: str = "default"):
        await websocket.accept()
        async with self._lock:
            if channel not in self._channels:
                self._channels[channel] = set()
            self._channels[channel].add(websocket)
        logger.info(f"WS connected to channel '{channel}'")

    async def disconnect(self, websocket: WebSocket, channel: str = "default"):
        async with self._lock:
            if channel in self._channels:
                self._channels[channel].discard(websocket)

    async def broadcast(self, channel: str, data: dict):
        """Send data to all connections in a channel."""
        async with self._lock:
            connections = self._channels.get(channel, set()).copy()

        dead = []
        for ws in connections:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)

        # Clean up dead connections
        if dead:
            async with self._lock:
                for ws in dead:
                    self._channels.get(channel, set()).discard(ws)

    async def broadcast_scan_update(self, symbol: str, data: dict):
        """Broadcast scan cycle update to the notifications channel."""
        await self.broadcast("notifications", {
            "type": "scan_update",
            "symbol": symbol,
            **data,
        })

    async def broadcast_trade_signal(self, data: dict):
        """Broadcast a trade signal (entry/exit) event."""
        await self.broadcast("notifications", {
            "type": "trade_signal",
            **data,
        })

    async def broadcast_engine_event(self, event: str, data: dict = None):
        """Broadcast engine lifecycle events (started, stopped, killed)."""
        await self.broadcast("notifications", {
            "type": "engine_event",
            "event": event,
            **(data or {}),
        })


# Singleton instance
ws_manager = ConnectionManager()
