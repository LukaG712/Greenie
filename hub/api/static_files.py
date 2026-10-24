"""Route module: static_files."""

import asyncio
import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException
from starlette.responses import FileResponse

from hub.api.core import _STATIC_DIR, _webhooks, _webhooks_lock


async def serve_app():
    app_index = _STATIC_DIR / "app" / "index.html"
    if app_index.exists():
        return FileResponse(str(app_index))
    raise HTTPException(status_code=404, detail="App not found")


async def serve_index():
    index = _STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"message": "Agent Hub", "status": "running"}


async def serve_dashboard():
    """Serve the web dashboard UI."""
    dashboard_index = _STATIC_DIR / "dashboard" / "index.html"
    if dashboard_index.exists():
        return FileResponse(str(dashboard_index))
    raise HTTPException(status_code=404, detail="Dashboard not found")


async def serve_static(path: str):
    file_path = _STATIC_DIR / path
    if file_path.exists() and file_path.is_file():
        return FileResponse(str(file_path))
    raise HTTPException(status_code=404, detail="File not found")


# ─── Webhook Event System ───────────────────────────────────────────────────

# In-memory webhook registry
_webhooks: Dict[str, Dict[str, Any]] = {}
_webhooks_lock = threading.Lock()

# Event types that can trigger webhooks
_EVENT_TYPES = {
    "run.completed", "run.failed", "run.started",
    "schedule.executed", "schedule.failed",
    "pipeline.completed", "pipeline.failed",
    "batch.completed", "batch.failed",
    "agent.created", "agent.deleted",
    "session.created", "session.deleted",
    "approval.pending", "approval.approved", "approval.denied",
}


def _emit_event(event_type: str, data: Dict[str, Any]) -> None:
    """Emit an event to all matching webhooks in the background."""
    import httpx

    async def _dispatch() -> None:
        with _webhooks_lock:
            targets = [
                wh for wh in _webhooks.values()
                if wh.get("enabled", True) and (
                    event_type in wh.get("events", []) or "*" in wh.get("events", [])
                )
            ]
        payload = {
            "event": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            for target in targets:
                try:
                    headers = {"Content-Type": "application/json"}
                    if target.get("secret"):
                        # Simple HMAC signature
                        import hashlib
                        import hmac
                        body = json.dumps(payload)
                        sig = hmac.new(
                            target["secret"].encode(),
                            body.encode(),
                            hashlib.sha256,
                        ).hexdigest()
                        headers["X-Webhook-Signature"] = sig
                    await client.post(
                        target["url"],
                        json=payload,
                        headers=headers,
                    )
                except Exception as e:
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Webhook delivery failed for {target['url']}: {e}")

    asyncio.create_task(_dispatch())



def register(app):
    """Register static_files routes with the FastAPI app."""
    app.get("/app/")(serve_app)
    app.get("/")(serve_index)
    app.get("/dashboard/")(serve_dashboard)
    app.get("/static/{path:path}")(serve_static)
