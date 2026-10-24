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


# ─── Rate Limiting ──────────────────────────────────────────────────────────

# Per-key rate limit configuration
_rate_limits: Dict[str, Dict[str, Any]] = {}
_rate_limits_lock = threading.Lock()

# Per-key request counters: key -> list of timestamps
_rate_counters: Dict[str, List[float]] = {}
_rate_counters_lock = threading.Lock()

# Default rate limit: 60 requests per minute
_DEFAULT_RATE_LIMIT = 60
_DEFAULT_RATE_WINDOW = 60


def _check_rate_limit(key_id: str) -> None:
    """Check and enforce rate limit for an API key. Raises 429 if exceeded."""
    now = time.time()
    with _rate_limits_lock:
        limit_cfg = _rate_limits.get(key_id)
        if limit_cfg is None:
            limit_cfg = {"requests": _DEFAULT_RATE_LIMIT, "window": _DEFAULT_RATE_WINDOW}
        max_requests = limit_cfg.get("requests", _DEFAULT_RATE_LIMIT)
        window = limit_cfg.get("window", _DEFAULT_RATE_WINDOW)

    with _rate_counters_lock:
        if key_id not in _rate_counters:
            _rate_counters[key_id] = []
        # Prune old entries
        cutoff = now - window
        _rate_counters[key_id] = [t for t in _rate_counters[key_id] if t > cutoff]
        counters = _rate_counters[key_id]
        if len(counters) >= max_requests:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded ({max_requests} requests per {window}s). "
                       f"Try again in {int(cutoff + window - now)}s.",
            )
        counters.append(now)


def _get_rate_limit_info(key_id: str) -> Dict[str, Any]:
    """Get current rate limit usage for a key."""
    now = time.time()
    with _rate_limits_lock:
        limit_cfg = _rate_limits.get(key_id, {
            "requests": _DEFAULT_RATE_LIMIT,
            "window": _DEFAULT_RATE_WINDOW,
        })
    max_requests = limit_cfg.get("requests", _DEFAULT_RATE_LIMIT)
    window = limit_cfg.get("window", _DEFAULT_RATE_WINDOW)

    with _rate_counters_lock:
        cutoff = now - window
        counters = _rate_counters.get(key_id, [])
        active = [t for t in counters if t > cutoff]
        _rate_counters[key_id] = active

    return {
        "limit": max_requests,
        "window": window,
        "used": len(active),
        "remaining": max(0, max_requests - len(active)),
        "reset_at": datetime.fromtimestamp(cutoff + window).isoformat(),
    }



def register(app):
    """Register websocket routes with the FastAPI app."""
    app.get("/api/ws")(api_websocket)
    app.websocket("/api/ws/connect")(websocket_endpoint)
