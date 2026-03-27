"""FastAPI WebSocket endpoints for real-time notifications and market data."""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, WebSocketException, status

from app.auth import get_user_from_token
from app.database import async_session_factory
from app.engine.broadcast import ws_manager

router = APIRouter(tags=["WebSocket"])


async def _authenticate_websocket(websocket: WebSocket):
    """Validate the JWT passed in the `token` query parameter."""
    token = websocket.query_params.get("token", "").strip()
    if not token:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)

    async with async_session_factory() as db:
        try:
            return await get_user_from_token(token, db)
        except Exception as exc:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION) from exc


@router.websocket("/ws/notifications")
async def notifications_ws(websocket: WebSocket):
    """
    Real-time notifications: scan updates, trade signals, engine events.

    Frontend connects once on mount. All push events flow through here.
    """
    await _authenticate_websocket(websocket)
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
    await _authenticate_websocket(websocket)
    await ws_manager.connect(websocket, "market-data")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket, "market-data")
