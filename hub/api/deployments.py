"""Route module: deployments."""

import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _deployments, _deployments_lock, _verify_token

logger = logging.getLogger(__name__)

# Current deployment
_current_deployment: Optional[Dict[str, Any]] = None
_current_deployment_lock = threading.Lock()


async def api_create_deployment(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Record a new deployment.

    Example body:
    {
        "version": "1.2.0",
        "environment": "production",
        "commit": "abc123",
        "branch": "main",
        "deployer": "ci-pipeline",
        "notes": "Added caching and message bus"
    }
    """
    version = body.get("version", "").strip()
    if not version:
        raise HTTPException(status_code=400, detail="version is required")

    deployment = {
        "id": f"deploy-{uuid.uuid4().hex[:8]}",
        "version": version,
        "environment": body.get("environment", "production"),
        "commit": body.get("commit", ""),
        "branch": body.get("branch", ""),
        "deployer": body.get("deployer", "manual"),
        "notes": body.get("notes", ""),
        "status": "deploying",  # deploying, deployed, rolled_back
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "duration_seconds": None,
    }

    with _deployments_lock:
        _deployments.append(deployment)

    with _current_deployment_lock:
        _current_deployment = deployment

    # Persist to database
    try:
        feature_store.save_deployment(deployment)
    except Exception as e:
        logger.warning(f"Failed to save deployment: {e}")

    return deployment


async def api_list_deployments(
    environment: Optional[str] = None,
    limit: int = 20,
    _auth: None = Depends(_verify_token),
):
    """List deployment history, optionally filtered by environment."""
    with _deployments_lock:
        deployments = list(_deployments)

    if environment:
        deployments = [d for d in deployments if d["environment"] == environment]

    deployments = deployments[-limit:]
    deployments.reverse()

    return {"deployments": deployments, "count": len(deployments)}


async def api_current_deployment(_auth: None = Depends(_verify_token)):
    """Get the current active deployment."""
    with _current_deployment_lock:
        if _current_deployment is None:
            raise HTTPException(status_code=404, detail="No active deployment")
        return _current_deployment


async def api_complete_deployment(deployment_id: str, _auth: None = Depends(_verify_token)):
    """Mark a deployment as completed."""
    with _deployments_lock:
        deployment = None
        for d in _deployments:
            if d["id"] == deployment_id:
                deployment = d
                break
        if not deployment:
            raise HTTPException(status_code=404, detail="Deployment not found")
        if deployment["status"] != "deploying":
            raise HTTPException(status_code=400, detail=f"Deployment is {deployment['status']}")

        deployment["status"] = "deployed"
        deployment["completed_at"] = datetime.now(timezone.utc).isoformat()
        start = datetime.fromisoformat(deployment["started_at"])
        end = datetime.fromisoformat(deployment["completed_at"])
        deployment["duration_seconds"] = (end - start).total_seconds()

    # Persist to database
    try:
        feature_store.save_deployment(deployment)
    except Exception as e:
        logger.warning(f"Failed to save deployment completion: {e}")

    return deployment


async def api_rollback_deployment(deployment_id: str, _auth: None = Depends(_verify_token)):
    """Mark a deployment as rolled back."""
    with _deployments_lock:
        deployment = None
        for d in _deployments:
            if d["id"] == deployment_id:
                deployment = d
                break
        if not deployment:
            raise HTTPException(status_code=404, detail="Deployment not found")

        deployment["status"] = "rolled_back"
        deployment["completed_at"] = datetime.now(timezone.utc).isoformat()
        start = datetime.fromisoformat(deployment["started_at"])
        end = datetime.fromisoformat(deployment["completed_at"])
        deployment["duration_seconds"] = (end - start).total_seconds()

    # Persist to database
    try:
        feature_store.save_deployment(deployment)
    except Exception as e:
        logger.warning(f"Failed to save deployment rollback: {e}")

    return deployment


async def api_deployment_stats(_auth: None = Depends(_verify_token)):
    """Get deployment statistics."""
    with _deployments_lock:
        deployments = list(_deployments)

    from collections import Counter
    by_env = Counter(d["environment"] for d in deployments)
    by_status = Counter(d["status"] for d in deployments)

    durations = [d["duration_seconds"] for d in deployments if d["duration_seconds"] is not None]
    avg_duration = sum(durations) / len(durations) if durations else 0

    return {
        "total_deployments": len(deployments),
        "by_environment": dict(by_env),
        "by_status": dict(by_status),
        "avg_duration_seconds": round(avg_duration, 2),
        "last_30_days": len([d for d in deployments if (datetime.now(timezone.utc) - datetime.fromisoformat(d["started_at"])).days < 30]),
    }


def register(app):
    """Register deployments routes with the FastAPI app."""
    app.post("/api/deployments", status_code=201)(api_create_deployment)
    app.get("/api/deployments")(api_list_deployments)
    app.get("/api/deployments/current")(api_current_deployment)
    app.post("/api/deployments/{deployment_id}/complete")(api_complete_deployment)
    app.post("/api/deployments/{deployment_id}/rollback")(api_rollback_deployment)
    app.get("/api/deployments/stats")(api_deployment_stats)
