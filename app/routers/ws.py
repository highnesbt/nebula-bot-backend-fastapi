"""FastAPI WebSocket endpoints for real-time notifications and market data."""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.engine.broadcast import ws_manager

router = APIRouter(tags=["WebSocket"])


@router.websocket("/ws/notifications")
async def notifications_ws(websocket: WebSocket):
    """
    Real-time notifications: scan updates, trade signals, engine events.

    Frontend connects once on mount. All push events flow through here.
    """
    await ws_manager.connect(websocket, "notifications")
    try:
        while True:
            # Keep connection alive — client can send pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket, "notifications")


@router.websocket("/ws/market-data")
async def market_data_ws(websocket: WebSocket):
    """
    Real-time market data: LTP ticks for watchlist items.

    Broadcasts tick updates from the Angel One tick WebSocket.
    """
    await ws_manager.connect(websocket, "market-data")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket, "market-data")
