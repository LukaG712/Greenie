"""Route module: deployments."""

import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _circuit_breakers, _circuit_breakers_lock, _deployments, _deployments_lock, _verify_token

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


# ─── Circuit Breaker ────────────────────────────────────────────────────────

# Circuit breaker state per service
_circuit_breakers: Dict[str, Dict[str, Any]] = {}
_circuit_breakers_lock = threading.Lock()

# Default circuit breaker config
_CB_DEFAULT_FAILURE_THRESHOLD = 5
_CB_DEFAULT_RECOVERY_TIMEOUT = 60  # seconds
_CB_DEFAULT_SUCCESS_THRESHOLD = 3


def _get_circuit_state(service: str) -> Dict[str, Any]:
    """Get or create circuit breaker state for a service."""
    with _circuit_breakers_lock:
        if service not in _circuit_breakers:
            _circuit_breakers[service] = {
                "service": service,
                "state": "closed",  # closed, open, half-open
                "failure_count": 0,
                "success_count": 0,
                "last_failure_at": None,
                "last_state_change": datetime.now(timezone.utc).isoformat(),
                "failure_threshold": _CB_DEFAULT_FAILURE_THRESHOLD,
                "recovery_timeout": _CB_DEFAULT_RECOVERY_TIMEOUT,
                "success_threshold": _CB_DEFAULT_SUCCESS_THRESHOLD,
            }
            # Persist to database
            try:
                feature_store.save_circuit_breaker(service, "closed", 0)
            except Exception as e:
                logger.warning(f"Failed to save circuit breaker: {e}")
        return _circuit_breakers[service]


def _record_success(service: str) -> str:
    """Record a successful call and update circuit state."""
    cb = _get_circuit_state(service)
    with _circuit_breakers_lock:
        if cb["state"] == "half-open":
            cb["success_count"] += 1
            if cb["success_count"] >= cb["success_threshold"]:
                cb["state"] = "closed"
                cb["failure_count"] = 0
                cb["success_count"] = 0
                cb["last_state_change"] = datetime.now(timezone.utc).isoformat()
        elif cb["state"] == "closed":
            cb["failure_count"] = max(0, cb["failure_count"] - 1)
        return cb["state"]


def _record_failure(service: str) -> str:
    """Record a failed call and update circuit state."""
    cb = _get_circuit_state(service)
    with _circuit_breakers_lock:
        cb["failure_count"] += 1
        cb["last_failure_at"] = datetime.now(timezone.utc).isoformat()

        if cb["state"] == "half-open":
            cb["state"] = "open"
            cb["last_state_change"] = datetime.now(timezone.utc).isoformat()
        elif cb["state"] == "closed" and cb["failure_count"] >= cb["failure_threshold"]:
            cb["state"] = "open"
            cb["last_state_change"] = datetime.now(timezone.utc).isoformat()

        return cb["state"]


def _can_execute(service: str) -> bool:
    """Check if a service call is allowed by the circuit breaker."""
    cb = _get_circuit_state(service)
    if cb["state"] == "closed":
        return True
    if cb["state"] == "open":
        # Check if recovery timeout has elapsed
        last_change = datetime.fromisoformat(cb["last_state_change"])
        elapsed = (datetime.now(timezone.utc) - last_change).total_seconds()
        if elapsed >= cb["recovery_timeout"]:
            with _circuit_breakers_lock:
                cb["state"] = "half-open"
                cb["success_count"] = 0
                cb["last_state_change"] = datetime.now(timezone.utc).isoformat()
            return True
        return False
    # half-open: allow limited calls
    return True



def register(app):
    """Register deployments routes with the FastAPI app."""
    app.post("/api/deployments", status_code=201)(api_create_deployment)
    app.get("/api/deployments")(api_list_deployments)
    app.get("/api/deployments/current")(api_current_deployment)
    app.post("/api/deployments/{deployment_id}/complete")(api_complete_deployment)
    app.post("/api/deployments/{deployment_id}/rollback")(api_rollback_deployment)
    app.get("/api/deployments/stats")(api_deployment_stats)
