"""Route module: api_versions."""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _api_versions, _api_versions_lock, _verify_token

logger = logging.getLogger(__name__)

# Deprecated endpoints store
_deprecated_endpoints: Dict[str, Any] = {}
_deprecated_endpoints_lock = threading.Lock()


async def api_list_versions(_auth: None = Depends(_verify_token)):
    """List all API versions and their status."""
    with _api_versions_lock:
        versions = list(_api_versions.values())
    return {"versions": versions}


async def api_create_version(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Register a new API version.

    Example body:
    {
        "version": "v2",
        "status": "current",
        "description": "New API version with enhanced features"
    }
    """
    version = body.get("version", "").strip()
    if not version:
        raise HTTPException(status_code=400, detail="version is required")

    status = body.get("status", "current")
    if status not in ("current", "deprecated", "sunset"):
        raise HTTPException(status_code=400, detail="status must be current, deprecated, or sunset")

    version_data = {
        "version": version,
        "status": status,
        "release_date": body.get("release_date", datetime.now(timezone.utc).strftime("%Y-%m-%d")),
        "deprecation_date": body.get("deprecation_date"),
        "sunset_date": body.get("sunset_date"),
        "description": body.get("description", ""),
    }

    with _api_versions_lock:
        _api_versions[version] = version_data
    # Persist to database
    try:
        feature_store.save_api_version(version_data)
    except Exception as e:
        logger.warning(f"Failed to save API version: {e}")

    return version_data


async def api_deprecate_version(version: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Mark an API version as deprecated.

    Example body:
    {
        "deprecation_date": "2025-06-01",
        "sunset_date": "2026-01-01",
        "migration_guide": "See /docs/migration/v1-to-v2"
    }
    """
    with _api_versions_lock:
        if version not in _api_versions:
            raise HTTPException(status_code=404, detail="Version not found")
        _api_versions[version]["status"] = "deprecated"
        _api_versions[version]["deprecation_date"] = body.get("deprecation_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        if "sunset_date" in body:
            _api_versions[version]["sunset_date"] = body["sunset_date"]
        if "migration_guide" in body:
            _api_versions[version]["migration_guide"] = body["migration_guide"]
        # Persist to database
        try:
            feature_store.save_api_version(_api_versions[version])
        except Exception as e:
            logger.warning(f"Failed to save API version update: {e}")
        return _api_versions[version]


async def api_register_deprecated_endpoint(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Register a deprecated endpoint with a migration path.

    Example body:
    {
        "old_path": "/api/v1/runs",
        "new_path": "/api/v2/runs",
        "message": "Use /api/v2/runs instead",
        "version": "v2"
    }
    """
    old_path = body.get("old_path", "").strip()
    new_path = body.get("new_path", "").strip()

    if not old_path or not new_path:
        raise HTTPException(status_code=400, detail="old_path and new_path are required")

    entry = {
        "old_path": old_path,
        "new_path": new_path,
        "message": body.get("message", f"Use {new_path} instead"),
        "version": body.get("version", ""),
        "registered_at": datetime.now(timezone.utc).isoformat(),
    }

    with _deprecated_endpoints_lock:
        _deprecated_endpoints[old_path] = entry

    return entry


async def api_list_deprecated_endpoints(_auth: None = Depends(_verify_token)):
    """List all deprecated endpoints and their migration paths."""
    with _deprecated_endpoints_lock:
        endpoints = list(_deprecated_endpoints.values())
    return {"endpoints": endpoints}


# ─── Task Dependencies / DAG Execution ──────────────────────────────────────

# DAG task store
_dag_tasks: Dict[str, Dict[str, Any]] = {}
_dag_tasks_lock = threading.Lock()

# DAG execution history
_dag_executions: List[Dict[str, Any]] = []
_dag_executions_lock = threading.Lock()



def register(app):
    """Register api_versions routes with the FastAPI app."""
    app.get("/api/versions")(api_list_versions)
    app.post("/api/versions", status_code=201)(api_create_version)
    app.post("/api/versions/{version}/deprecate")(api_deprecate_version)
    app.post("/api/deprecated-endpoints", status_code=201)(api_register_deprecated_endpoint)
    app.get("/api/deprecated-endpoints")(api_list_deprecated_endpoints)
