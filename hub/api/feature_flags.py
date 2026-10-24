"""Route module: feature_flags."""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _feature_flags, _feature_flags_lock, _verify_token

logger = logging.getLogger(__name__)

# Flag evaluation history
_flag_evaluations: List[Dict[str, Any]] = []
_flag_evaluations_lock = threading.Lock()


def _evaluate_flag(flag_name: str, context: Dict[str, Any]) -> bool:
    """Evaluate a feature flag for a given context."""
    with _feature_flags_lock:
        flag = _feature_flags.get(flag_name)
    if not flag:
        return False
    if not flag.get("enabled", False):
        return flag.get("default_value", False)
    # Simple percentage-based evaluation
    for rule in flag.get("rules", []):
        if rule.get("type") == "percentage":
            import hashlib
            user_id = context.get("user_id", "anonymous")
            hash_val = int(hashlib.md5(f"{flag_name}:{user_id}".encode()).hexdigest(), 16) % 100
            return hash_val < rule.get("percentage", 0)
        elif rule.get("type") == "user":
            user_id = context.get("user_id", "")
            if user_id in rule.get("users", []):
                return rule.get("enabled", False)
    return flag.get("default_value", False)


async def api_create_feature_flag(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Create a new feature flag.

    Example body:
    {
        "name": "new_pipeline_ui",
        "description": "New pipeline UI for beta testers",
        "enabled": true,
        "rules": [
            {"type": "percentage", "percentage": 50, "enabled": true},
            {"type": "user", "users": ["admin"], "enabled": true}
        ]
    }
    """
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    flag = {
        "name": name,
        "description": body.get("description", ""),
        "enabled": body.get("enabled", True),
        "rules": body.get("rules", []),
        "default_value": body.get("default_value", False),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    with _feature_flags_lock:
        _feature_flags[name] = flag
    # Persist to database
    try:
        feature_store.save_feature_flag(flag)
    except Exception as e:
        logger.warning(f"Failed to save feature flag: {e}")

    return flag


async def api_list_feature_flags(_auth: None = Depends(_verify_token)):
    """List all feature flags."""
    with _feature_flags_lock:
        flags = list(_feature_flags.values())
    return {"flags": flags}


async def api_get_feature_flag(flag_name: str, _auth: None = Depends(_verify_token)):
    """Get a feature flag by name."""
    with _feature_flags_lock:
        flag = _feature_flags.get(flag_name)
    if not flag:
        raise HTTPException(status_code=404, detail="Flag not found")
    return flag


async def api_update_feature_flag(flag_name: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Update a feature flag."""
    with _feature_flags_lock:
        flag = _feature_flags.get(flag_name)
        if not flag:
            raise HTTPException(status_code=404, detail="Flag not found")
        if "enabled" in body:
            flag["enabled"] = body["enabled"]
        if "description" in body:
            flag["description"] = body["description"]
        if "rules" in body:
            flag["rules"] = body["rules"]
        if "default_value" in body:
            flag["default_value"] = body["default_value"]
        flag["updated_at"] = datetime.now(timezone.utc).isoformat()
    # Persist to database
    try:
        feature_store.save_feature_flag(flag)
    except Exception as e:
        logger.warning(f"Failed to save feature flag update: {e}")
    return dict(flag)


async def api_delete_feature_flag(flag_name: str, _auth: None = Depends(_verify_token)):
    """Delete a feature flag."""
    with _feature_flags_lock:
        if flag_name not in _feature_flags:
            raise HTTPException(status_code=404, detail="Flag not found")
        del _feature_flags[flag_name]
    # Persist to database
    try:
        feature_store.delete_feature_flag(flag_name)
    except Exception as e:
        logger.warning(f"Failed to delete feature flag: {e}")
    return {"status": "ok", "message": "Flag deleted"}


async def api_evaluate_feature_flag(
    flag_name: str,
    body: Optional[Dict[str, Any]] = None,
    _auth: None = Depends(_verify_token),
):
    """Evaluate a feature flag for a specific context.

    Example body:
    {
        "user_id": "u-123",
        "agent": "vault",
        "environment": "production"
    }
    """
    context = body or {}
    result = _evaluate_flag(flag_name, context)

    # Track evaluation
    eval_entry = {
        "flag": flag_name,
        "context": context,
        "result": result,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with _flag_evaluations_lock:
        _flag_evaluations.append(eval_entry)
        if len(_flag_evaluations) > 1000:
            _flag_evaluations.pop(0)

    return {"flag": flag_name, "enabled": result, "context": context}


async def api_flag_evaluations(limit: int = 50, _auth: None = Depends(_verify_token)):
    """Get recent flag evaluation history."""
    with _flag_evaluations_lock:
        evaluations = _flag_evaluations[-limit:]
    evaluations.reverse()
    return {"evaluations": evaluations, "count": len(evaluations)}


def register(app):
    """Register feature_flags routes with the FastAPI app."""
    app.post("/api/flags", status_code=201)(api_create_feature_flag)
    app.get("/api/flags")(api_list_feature_flags)
    app.get("/api/flags/{flag_name}")(api_get_feature_flag)
    app.put("/api/flags/{flag_name}")(api_update_feature_flag)
    app.delete("/api/flags/{flag_name}")(api_delete_feature_flag)
    app.post("/api/flags/{flag_name}/evaluate")(api_evaluate_feature_flag)
    app.get("/api/flags/evaluations")(api_flag_evaluations)
