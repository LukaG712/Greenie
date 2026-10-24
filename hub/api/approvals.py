"""Route module: approvals."""

import logging
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store, memory
from hub.api.core import _approval_policies, _approval_policies_lock, _verify_token

logger = logging.getLogger(__name__)

# In-memory approval store (mirrors feature_store)
_approvals: Dict[str, Dict[str, Any]] = {}
_approvals_lock = threading.Lock()


def _emit_event(event: str, data: Dict[str, Any]) -> None:
    """Emit a webhook event for approval state changes."""
    logger.info(f"Approval event: {event} - {data}")


async def api_list_approvals(
    status: Optional[str] = None,
    agent: Optional[str] = None,
    _auth: None = Depends(_verify_token),
):
    """List approval requests, optionally filtered by status or agent."""
    # Fetch from memory module (primary source)
    try:
        approvals = memory.list_approvals(status=status)
    except Exception:
        approvals = []

    # Also include in-memory approvals not in memory module
    mem_ids = {a.get("id") for a in approvals}
    with _approvals_lock:
        for a in _approvals.values():
            if a.get("id") not in mem_ids:
                approvals.append(a)

    if status:
        approvals = [a for a in approvals if a.get("status") == status]
    if agent:
        approvals = [a for a in approvals if a.get("agent") == agent or a.get("bot_id") == agent]

    # Check for expired approvals
    now = datetime.now(timezone.utc)
    for a in approvals:
        if a.get("status") == "pending":
            created_str = a.get("created_at", "")
            if created_str:
                try:
                    created = datetime.fromisoformat(created_str)
                    timeout = a.get("timeout_minutes", 30)
                    if (now - created).total_seconds() > timeout * 60:
                        a["status"] = "expired"
                        a["resolved_at"] = now.isoformat()
                except (ValueError, TypeError):
                    pass

    return {"approvals": approvals}


async def api_approve(approval_id: str, body: Optional[Dict[str, Any]] = None, _auth: None = Depends(_verify_token)):
    """Approve a pending approval request."""
    body = body or {}
    # Try memory module first, then in-memory store
    updated = memory.update_approval_status(approval_id, "approved")
    if updated is not None:
        return updated
    with _approvals_lock:
        approval = _approvals.get(approval_id)
        if not approval:
            raise HTTPException(status_code=404, detail="Approval not found")
        if approval["status"] != "pending":
            raise HTTPException(status_code=400, detail=f"Approval is {approval['status']}")

        approval["status"] = "approved"
        approval["resolved_at"] = datetime.now(timezone.utc).isoformat()
        approval["resolved_by"] = body.get("user", "api")
        approval["notes"] = body.get("notes", "")

    try:
        feature_store.save_approval_request(approval)
    except Exception as e:
        logger.warning(f"Failed to save approval decision: {e}")

    _emit_event("approval.approved", {"approval_id": approval_id, "action": approval.get("action", "")})
    return approval


async def api_deny(approval_id: str, body: Optional[Dict[str, Any]] = None, _auth: None = Depends(_verify_token)):
    """Deny a pending approval request."""
    body = body or {}
    # Try memory module first, then in-memory store
    updated = memory.update_approval_status(approval_id, "denied")
    if updated is not None:
        return updated
    with _approvals_lock:
        approval = _approvals.get(approval_id)
        if not approval:
            raise HTTPException(status_code=404, detail="Approval not found")
        if approval["status"] != "pending":
            raise HTTPException(status_code=400, detail=f"Approval is {approval['status']}")

        approval["status"] = "denied"
        approval["resolved_at"] = datetime.now(timezone.utc).isoformat()
        approval["resolved_by"] = body.get("user", "api")
        approval["notes"] = body.get("notes", "")

    try:
        feature_store.save_approval_request(approval)
    except Exception as e:
        logger.warning(f"Failed to save approval decision: {e}")

    _emit_event("approval.denied", {"approval_id": approval_id, "action": approval.get("action", "")})
    return approval


# --- Auto Review Settings ---


async def api_set_approval_policy(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Configure which actions require human approval."""
    with _approval_policies_lock:
        _approval_policies["enabled"] = body.get("enabled", _approval_policies["enabled"])
        _approval_policies["require_for"] = body.get("require_for", [])
        _approval_policies["auto_approve_agents"] = body.get("auto_approve_agents", [])
    try:
        feature_store.save_approval_policy(_approval_policies)
    except Exception as e:
        logger.warning(f"Failed to save approval policy: {e}")
    return dict(_approval_policies)


async def api_get_approval_policy(_auth: None = Depends(_verify_token)):
    """Get current approval policy configuration."""
    with _approval_policies_lock:
        return dict(_approval_policies)


async def api_request_approval(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Create a new approval request for a pending action."""
    action = body.get("action", "").strip()
    if not action:
        raise HTTPException(status_code=400, detail="action is required")

    approval_id = f"appr-{uuid.uuid4().hex[:8]}"
    approval = {
        "id": approval_id,
        "action": action,
        "description": body.get("description", ""),
        "agent": body.get("agent", ""),
        "details": body.get("details", {}),
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "resolved_at": None,
        "resolved_by": None,
        "timeout_minutes": body.get("timeout_minutes", 30),
    }

    with _approvals_lock:
        _approvals[approval_id] = approval

    try:
        feature_store.save_approval_request(approval)
    except Exception as e:
        logger.warning(f"Failed to save approval request: {e}")

    _emit_event("approval.pending", {"approval_id": approval_id, "action": action})

    return approval


async def api_approval_stats(_auth: None = Depends(_verify_token)):
    """Get approval statistics: counts by status and action."""
    with _approvals_lock:
        approvals = list(_approvals.values())

    from collections import Counter
    by_status = Counter(a["status"] for a in approvals)
    by_action = Counter(a["action"] for a in approvals)

    return {
        "total": len(approvals),
        "pending": by_status.get("pending", 0),
        "by_status": dict(by_status),
        "by_action": dict(by_action),
    }


# ─── File Storage ───────────────────────────────────────────────────────────

_file_store: Dict[str, Dict[str, Any]] = {}
_file_store_lock = threading.Lock()

# Upload directory - use environment variable or default to user's data directory
_UPLOAD_DIR = Path(os.environ.get("EVERSHADE_UPLOAD_DIR", str(Path.home() / ".evershade" / "data" / "uploads")))
_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

_MAX_FILE_SIZE = 50 * 1024 * 1024


def register(app):
    """Register approvals routes with the FastAPI app."""
    app.get("/api/approvals")(api_list_approvals)
    app.post("/api/approvals/{approval_id}/approve")(api_approve)
    app.post("/api/approvals/{approval_id}/deny")(api_deny)
    app.post("/api/approvals/policy")(api_set_approval_policy)
    app.get("/api/approvals/policy")(api_get_approval_policy)
    app.post("/api/approvals/request", status_code=201)(api_request_approval)
    app.get("/api/approvals/stats")(api_approval_stats)
