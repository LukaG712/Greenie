"""Route module: websocket."""

import json
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, Request, WebSocket, WebSocketDisconnect

from hub.api.core import _api_keys, _api_keys_lock, _rate_limits, _rate_limits_lock

# WebSocket connections
_ws_connections: Dict[str, set] = {}
_ws_lock = threading.Lock()


def _get_api_key() -> str:
    """Get the primary API key."""
    from hub.config import load_config
    config = load_config()
    return config.get("api_key", "")


def _emit_event(event_type: str, data: Dict[str, Any]) -> None:
    """Emit an event."""
    pass  # Placeholder


def _track_run(agent: str) -> None:
    """Track a run."""
    pass  # Placeholder


def _track_error(error_type: str) -> None:
    """Track an error."""
    pass  # Placeholder


async def api_websocket(
    channel: str = "all",
    request: Request = None,
):
    """WebSocket endpoint for real-time event streaming.

    Query parameters:
    - channel: Channel to subscribe to (all, runs, schedules)

    Authentication via query parameter: ?token=YOUR_API_KEY
    (WebSocket doesn't support custom headers)
    """
    try:
        from fastapi.websockets import WebSocket, WebSocketDisconnect
    except ImportError:
        raise HTTPException(status_code=501, detail="WebSocket not supported")

    # This endpoint is a placeholder; actual WebSocket handling requires
    # a WebSocket endpoint decorator. See /api/ws/connect for the real endpoint.
    raise HTTPException(status_code=405, detail="Use WebSocket connection: ws://host/api/ws/connect")


from fastapi.websockets import WebSocket, WebSocketDisconnect


async def websocket_endpoint(ws: WebSocket, channel: str = "all", token: str = ""):
    """WebSocket connection for real-time event streaming.

    Query parameters:
    - channel: Channel to subscribe to (all, runs, schedules)
    - token: API key for authentication
    """
    # Authenticate
    expected = _get_api_key()
    if token != expected:
        # Try API key store
        with _api_keys_lock:
            key_data = _api_keys.get(token)
        if not key_data or not key_data.get("active", True):
            await ws.close(code=4001, reason="Invalid API key")
            return

    await ws.accept()

    # Register connection
    with _ws_lock:
        if channel not in _ws_connections:
            _ws_connections[channel] = set()
        _ws_connections[channel].add(ws)

    try:
        # Send connection confirmation
        await ws.send(json.dumps({
            "event": "connected",
            "channel": channel,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))

        # Keep alive loop
        while True:
            data = await ws.receive_text()
            # Handle client messages (e.g., ping)
            if data == "ping":
                await ws.send(json.dumps({"event": "pong", "timestamp": datetime.now(timezone.utc).isoformat()}))
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        # Unregister connection
        with _ws_lock:
            _ws_connections.get(channel, set()).discard(ws)
            _ws_connections.get("all", set()).discard(ws)


# ─── Event Emitter Integration ──────────────────────────────────────────────

# Override the run completion callbacks to emit events
_original_on_complete = None


def _enhanced_on_complete(result: Any) -> None:
    """Enhanced run completion handler that emits events."""
    event_type = "run.completed" if result.state == "done" else "run.failed"
    _emit_event(event_type, {
        "run_id": result.id,
        "agent": result.agent_name,
        "state": result.state,
        "task": result.task[:200] if result.task else "",
    })
    _track_run(result.agent_name or "unknown")
    if result.state == "failed":
        _track_error("run_failed")


# Wire up event emission to existing endpoints
# (Events are emitted from the appropriate endpoints below)


def register(app):
    """Register websocket routes with the FastAPI app."""
    app.get("/api/ws")(api_websocket)
    app.websocket("/api/ws/connect")(websocket_endpoint)
