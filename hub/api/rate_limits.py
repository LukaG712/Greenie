"""Route module: rate_limits."""

import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _audit_log, _audit_log_lock, _rate_limits, _rate_limits_lock, _verify_token

logger = logging.getLogger(__name__)

# Default rate limits
_DEFAULT_RATE_LIMIT = 100
_DEFAULT_RATE_WINDOW = 60


def _get_rate_limit_info(key_id: str) -> Dict[str, Any]:
    """Get current rate limit usage info."""
    return {"key_id": key_id, "requests_made": 0, "limit": _DEFAULT_RATE_LIMIT, "window": _DEFAULT_RATE_WINDOW}


async def api_create_rate_limit(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Set a custom rate limit for an API key.

    Example body:
    {
        "key_id": "key-abc123",
        "requests": 100,
        "window": 60
    }
    """
    key_id = body.get("key_id", "").strip()
    if not key_id:
        raise HTTPException(status_code=400, detail="key_id is required")

    requests = body.get("requests", _DEFAULT_RATE_LIMIT)
    window = body.get("window", _DEFAULT_RATE_WINDOW)

    if requests <= 0 or window <= 0:
        raise HTTPException(status_code=400, detail="requests and window must be positive")

    with _rate_limits_lock:
        _rate_limits[key_id] = {
            "requests": requests,
            "window": window,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    # Persist to database
    try:
        feature_store.save_rate_limit(key_id, requests)
    except Exception as e:
        logger.warning(f"Failed to save rate limit: {e}")

    return {"status": "ok", "rate_limit": _rate_limits[key_id]}


async def api_list_rate_limits(_auth: None = Depends(_verify_token)):
    """List all custom rate limit configurations."""
    with _rate_limits_lock:
        return {"rate_limits": dict(_rate_limits)}


async def api_get_rate_limit(key_id: str, _auth: None = Depends(_verify_token)):
    """Get rate limit configuration and current usage for a key."""
    with _rate_limits_lock:
        cfg = _rate_limits.get(key_id)
    if not cfg:
        cfg = {"requests": _DEFAULT_RATE_LIMIT, "window": _DEFAULT_RATE_WINDOW}

    usage = _get_rate_limit_info(key_id)
    return {"configuration": cfg, "usage": usage}


async def api_delete_rate_limit(key_id: str, _auth: None = Depends(_verify_token)):
    """Remove custom rate limit for a key (reverts to default)."""
    with _rate_limits_lock:
        if key_id not in _rate_limits:
            raise HTTPException(status_code=404, detail="Rate limit not found")
        del _rate_limits[key_id]
    return {"status": "ok", "message": "Rate limit removed, reverted to default"}


# ─── Audit Log ──────────────────────────────────────────────────────────────

# In-memory audit log (bounded)
_audit_log: List[Dict[str, Any]] = []
_audit_log_lock = threading.Lock()
_MAX_AUDIT_ENTRIES = 5000


def _audit(action: str, actor: str, resource: str, detail: str = "", ip: str = "") -> None:
    """Append an entry to the audit log."""
    entry = {
        "id": f"audit-{uuid.uuid4().hex[:8]}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "actor": actor,
        "resource": resource,
        "detail": detail,
        "ip": ip,
    }
    with _audit_log_lock:
        _audit_log.append(entry)
        # Bounded: keep last N entries
        if len(_audit_log) > _MAX_AUDIT_ENTRIES:
            _audit_log.pop(0)
    # Persist to database
    try:
        feature_store.log_audit(action, resource, actor=actor, details=detail, ip_address=ip)
    except Exception as e:
        logger.warning(f"Failed to persist audit log: {e}")



def register(app):
    """Register rate_limits routes with the FastAPI app."""
    app.post("/api/rate-limits", status_code=201)(api_create_rate_limit)
    app.get("/api/rate-limits")(api_list_rate_limits)
    app.get("/api/rate-limits/{key_id}")(api_get_rate_limit)
    app.delete("/api/rate-limits/{key_id}")(api_delete_rate_limit)
