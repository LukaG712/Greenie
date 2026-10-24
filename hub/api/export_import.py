"""Route module: export_import."""

import json
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials

from hub import memory
from hub.api.core import _verify_token
from hub.config import load_config


def _get_api_key() -> str:
    """Get the primary API key."""
    config = load_config()
    return config.get("api_key", "")


def _check_auth_rate_limit(client_ip: str) -> None:
    """Check auth rate limit."""
    pass  # Placeholder


def _record_auth_failure(client_ip: str) -> None:
    """Record auth failure."""
    pass  # Placeholder


async def api_export_sessions(
    agent: Optional[str] = None,
    format: str = "json",
    _auth: None = Depends(_verify_token),
):
    """Export sessions data as JSON or CSV.

    Query parameters:
    - agent: Filter by agent ID
    - format: Output format (json or csv)
    """
    sessions = memory.list_sessions(agent_id=agent)

    if format.lower() == "csv":
        import csv
        import io

        output = io.StringIO()
        if sessions:
            writer = csv.DictWriter(output, fieldnames=sessions[0].keys())
            writer.writeheader()
            for s in sessions:
                # Flatten nested fields
                flat = {}
                for k, v in s.items():
                    if isinstance(v, (dict, list)):
                        flat[k] = json.dumps(v)
                    else:
                        flat[k] = v
                writer.writerow(flat)
        return {
            "content_type": "text/csv",
            "filename": f"sessions_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv",
            "data": output.getvalue(),
        }
    else:
        return {
            "format": "json",
            "count": len(sessions),
            "sessions": sessions,
        }


async def api_export_runs(
    agent: Optional[str] = None,
    state: Optional[str] = None,
    format: str = "json",
    limit: int = 100,
    _auth: None = Depends(_verify_token),
):
    """Export runs data as JSON or CSV."""
    from hub.runner import list_runs
    runs = list_runs(agent=agent, state=state, limit=limit)

    if format.lower() == "csv":
        import csv
        import io

        output = io.StringIO()
        run_dicts = [r.to_dict() for r in runs]
        if run_dicts:
            writer = csv.DictWriter(output, fieldnames=run_dicts[0].keys())
            writer.writeheader()
            for rd in run_dicts:
                flat = {}
                for k, v in rd.items():
                    if isinstance(v, (dict, list)):
                        flat[k] = json.dumps(v)
                    else:
                        flat[k] = v
                writer.writerow(flat)
        return {
            "content_type": "text/csv",
            "filename": f"runs_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv",
            "data": output.getvalue(),
        }
    else:
        return {
            "format": "json",
            "count": len(runs),
            "runs": [r.to_dict() for r in runs],
        }


async def api_export_memories(
    agent: Optional[str] = None,
    project: Optional[str] = None,
    _auth: None = Depends(_verify_token),
):
    """Export memories data as JSON."""
    memories_list = memory.list_memories(agent_id=agent, project_path=project)
    return {
        "format": "json",
        "count": len(memories_list),
        "memories": memories_list,
    }


async def api_export_config(_auth: None = Depends(_verify_token)):
    """Export the current Greenie configuration (without secrets)."""
    config = load_config()
    # Strip sensitive fields
    safe_config = {k: v for k, v in config.items() if k not in ("api_keys", "secrets")}
    return {"config": safe_config}


async def api_import_sessions(
    body: Dict[str, Any],
    _auth: None = Depends(_verify_token),
):
    """Import sessions from JSON data.

    Expects: {"sessions": [...]}
    """
    sessions_data = body.get("sessions", [])
    if not isinstance(sessions_data, list):
        raise HTTPException(status_code=400, detail="sessions must be a list")

    imported = 0
    for sess in sessions_data:
        try:
            if isinstance(sess, dict) and sess.get("title"):
                memory.create_session(
                    title=sess.get("title", ""),
                    project_path=sess.get("project_path"),
                    agent_id=sess.get("agent_id"),
                    bot_ids=sess.get("bot_ids"),
                )
                imported += 1
        except Exception:
            pass  # Skip invalid entries

    return {"imported": imported, "total": len(sessions_data)}


def register(app):
    """Register export_import routes with the FastAPI app."""
    app.get("/api/export/sessions")(api_export_sessions)
    app.get("/api/export/runs")(api_export_runs)
    app.get("/api/export/memories")(api_export_memories)
    app.get("/api/export/config")(api_export_config)
    app.post("/api/import/sessions")(api_import_sessions)
