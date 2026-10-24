"""Route module: audit."""

from typing import Any, Dict, List, Optional

from fastapi import Depends

from hub.api.core import _audit_log, _audit_log_lock, _verify_token


async def api_list_audit(
    action: Optional[str] = None,
    actor: Optional[str] = None,
    resource: Optional[str] = None,
    limit: int = 100,
    _auth: None = Depends(_verify_token),
):
    """Query the audit log with optional filters.

    Query parameters:
    - action: Filter by action type (e.g. "run.created", "agent.deleted")
    - actor: Filter by actor (API key name or user ID)
    - resource: Filter by resource type (e.g. "run", "agent", "webhook")
    - limit: Max entries to return (default 100, max 1000)
    """
    limit = min(limit, 1000)
    with _audit_log_lock:
        entries = list(_audit_log)

    # Apply filters
    if action:
        entries = [e for e in entries if e["action"] == action]
    if actor:
        entries = [e for e in entries if e["actor"] == actor]
    if resource:
        entries = [e for e in entries if e["resource"] == resource]

    # Return most recent first, up to limit
    entries = entries[-limit:]
    entries.reverse()

    return {"entries": entries, "total": len(entries)}


async def api_audit_stats(_auth: None = Depends(_verify_token)):
    """Get audit log statistics: counts by action, actor, and resource."""
    with _audit_log_lock:
        entries = list(_audit_log)

    from collections import Counter
    actions = Counter(e["action"] for e in entries)
    actors = Counter(e["actor"] for e in entries)
    resources = Counter(e["resource"] for e in entries)

    return {
        "total_entries": len(entries),
        "by_action": dict(actions),
        "by_actor": dict(actors),
        "by_resource": dict(resources),
    }


async def api_clear_audit(_auth: None = Depends(_verify_token)):
    """Clear all audit log entries."""
    with _audit_log_lock:
        _audit_log.clear()
    return {"status": "ok", "message": "Audit log cleared"}


def register(app):
    """Register audit routes with the FastAPI app."""
    app.get("/api/audit")(api_list_audit)
    app.get("/api/audit/stats")(api_audit_stats)
    app.post("/api/audit/clear")(api_clear_audit)
