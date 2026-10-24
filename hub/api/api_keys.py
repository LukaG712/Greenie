"""Route module: api_keys."""

import logging
import secrets
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _api_keys, _api_keys_lock, _service_health, _service_health_lock, _verify_token

logger = logging.getLogger(__name__)


async def api_create_api_key(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Create a new API key with optional permissions and expiration.

    Example body:
    {
        "name": "CI/CD Pipeline",
        "permissions": ["read", "runs"],
        "expires_in_days": 30
    }

    Permissions: "*" (all), "read", "runs", "agents", "sessions", "webhooks", "analytics"
    """
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    permissions = body.get("permissions", ["*"])
    expires_in_days = body.get("expires_in_days")

    # Generate key using secrets module for cryptographic strength
    key_value = secrets.token_hex(24)

    expires_at = None
    if expires_in_days:
        from datetime import timedelta
        expires_at = (datetime.now(timezone.utc) + timedelta(days=expires_in_days)).isoformat()

    key_id = f"key-{uuid.uuid4().hex[:8]}"
    key_data = {
        "id": key_id,
        "name": name,
        "key": key_value,  # Only shown once at creation
        "permissions": permissions,
        "active": True,
        "expires_at": expires_at,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "last_used": None,
        "usage_count": 0,
    }

    with _api_keys_lock:
        _api_keys[key_value] = key_data
    # Persist to database
    try:
        feature_store.save_api_key(key_data)
    except Exception as e:
        logger.warning(f"Failed to save API key: {e}")

    return key_data


async def api_list_api_keys(_auth: None = Depends(_verify_token)):
    """List all API keys (without the actual key values)."""
    with _api_keys_lock:
        # Create copies to avoid mutating the original dicts
        keys = [dict(v) for v in _api_keys.values()]
    # Strip key values from copies
    for k in keys:
        k.pop("key", None)
    return {"keys": keys}


async def api_get_api_key(key_id: str, _auth: None = Depends(_verify_token)):
    """Get an API key detail by ID."""
    with _api_keys_lock:
        key_data = next((k for k in _api_keys.values() if k["id"] == key_id), None)
    if not key_data:
        raise HTTPException(status_code=404, detail="API key not found")
    result = dict(key_data)
    result.pop("key", None)
    return result


async def api_update_api_key(key_id: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Update an API key's metadata."""
    with _api_keys_lock:
        key_data = next((k for k in _api_keys.values() if k["id"] == key_id), None)
        if not key_data:
            raise HTTPException(status_code=404, detail="API key not found")
        if "name" in body:
            key_data["name"] = body["name"].strip()
        if "permissions" in body:
            key_data["permissions"] = body["permissions"]
        if "active" in body:
            key_data["active"] = body["active"]
        # Persist to database
        try:
            feature_store.save_api_key(key_data)
        except Exception as e:
            logger.warning(f"Failed to save API key update: {e}")
        return {k: v for k, v in key_data.items() if k != "key"}


async def api_delete_api_key(key_id: str, _auth: None = Depends(_verify_token)):
    """Delete (revoke) an API key."""
    key_val = None
    with _api_keys_lock:
        key_data = next((k for k, v in _api_keys.items() if v["id"] == key_id), None)
        if not key_data:
            raise HTTPException(status_code=404, detail="API key not found")
        # Find and remove the key
        for key_val, val in list(_api_keys.items()):
            if val["id"] == key_id:
                del _api_keys[key_val]
                break
    # Persist to database
    if key_val:
        try:
            feature_store.delete_api_key(key_val)
        except Exception as e:
            logger.warning(f"Failed to delete API key: {e}")
    return {"status": "ok", "message": "API key revoked"}


async def api_rotate_api_key(key_id: str, _auth: None = Depends(_verify_token)):
    """Rotate an API key (generate a new key value, invalidate the old one)."""
    with _api_keys_lock:
        key_data = next((k for k, v in _api_keys.items() if v["id"] == key_id), None)
        if not key_data:
            raise HTTPException(status_code=404, detail="API key not found")

        # Find the old key value
        old_key = None
        for key_val, val in _api_keys.items():
            if val["id"] == key_id:
                old_key = key_val
                break

        if not old_key:
            raise HTTPException(status_code=500, detail="Key value not found")

        # Generate new key using secrets module for cryptographic strength
        new_key = secrets.token_hex(24)
        key_data["key"] = new_key
        key_data["rotated_at"] = datetime.now(timezone.utc).isoformat()
        key_data["usage_count"] = 0
        key_data["last_used"] = None

        # Replace old key with new one
        del _api_keys[old_key]
        _api_keys[new_key] = key_data
    # Persist to database
    try:
        feature_store.delete_api_key(old_key)
        feature_store.save_api_key(key_data)
    except Exception as e:
        logger.warning(f"Failed to persist API key rotation: {e}")

    return {"status": "ok", "message": "API key rotated", "new_key": new_key}


# ─── Service Auto-Discovery & Health Monitoring ─────────────────────────────

# Service health history
_service_health: Dict[str, List[Dict[str, Any]]] = {
    "Slinker": [],
    "Trapper": [],
}
_service_health_lock = threading.Lock()



def register(app):
    """Register api_keys routes with the FastAPI app."""
    app.post("/api/keys", status_code=201)(api_create_api_key)
    app.get("/api/keys")(api_list_api_keys)
    app.get("/api/keys/{key_id}")(api_get_api_key)
    app.put("/api/keys/{key_id}")(api_update_api_key)
    app.delete("/api/keys/{key_id}")(api_delete_api_key)
    app.post("/api/keys/{key_id}/rotate")(api_rotate_api_key)
