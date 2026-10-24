"""Route module: circuit_breakers."""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _circuit_breakers, _circuit_breakers_lock, _verify_token

logger = logging.getLogger(__name__)

# Circuit breaker defaults
_CB_DEFAULT_FAILURE_THRESHOLD = 5
_CB_DEFAULT_RECOVERY_TIMEOUT = 60
_CB_DEFAULT_SUCCESS_THRESHOLD = 3


def _get_circuit_state(service: str) -> Dict[str, Any]:
    """Get or create circuit breaker state for a service."""
    with _circuit_breakers_lock:
        if service not in _circuit_breakers:
            _circuit_breakers[service] = {
                "service": service,
                "state": "closed",
                "failure_count": 0,
                "success_count": 0,
                "last_failure_at": None,
                "last_state_change": datetime.now(timezone.utc).isoformat(),
                "failure_threshold": _CB_DEFAULT_FAILURE_THRESHOLD,
                "recovery_timeout": _CB_DEFAULT_RECOVERY_TIMEOUT,
                "success_threshold": _CB_DEFAULT_SUCCESS_THRESHOLD,
            }
        return _circuit_breakers[service]


def _record_success(service: str) -> str:
    """Record a success for a service."""
    with _circuit_breakers_lock:
        cb = _circuit_breakers.get(service)
        if not cb:
            return "closed"
        cb["success_count"] += 1
        if cb["state"] == "half-open" and cb["success_count"] >= cb.get("success_threshold", 3):
            cb["state"] = "closed"
            cb["failure_count"] = 0
            cb["success_count"] = 0
            cb["last_state_change"] = datetime.now(timezone.utc).isoformat()
        return cb["state"]


def _record_failure(service: str) -> str:
    """Record a failure for a service."""
    with _circuit_breakers_lock:
        cb = _circuit_breakers.get(service)
        if not cb:
            return "closed"
        cb["failure_count"] += 1
        cb["last_failure_at"] = datetime.now(timezone.utc).isoformat()
        if cb["failure_count"] >= cb.get("failure_threshold", 5):
            cb["state"] = "open"
            cb["last_state_change"] = datetime.now(timezone.utc).isoformat()
        return cb["state"]


def _can_execute(service: str) -> bool:
    """Check if a service call is allowed."""
    with _circuit_breakers_lock:
        cb = _circuit_breakers.get(service)
        if not cb or cb["state"] in ("closed", "half-open"):
            return True
        # Open state - check if recovery timeout has passed
        if cb["last_failure_at"]:
            last_fail = datetime.fromisoformat(cb["last_failure_at"])
            if (datetime.now(timezone.utc) - last_fail).total_seconds() > cb.get("recovery_timeout", 60):
                cb["state"] = "half-open"
                cb["last_state_change"] = datetime.now(timezone.utc).isoformat()
                cb["success_count"] = 0
                return True
        return False


async def api_list_circuit_breakers(_auth: None = Depends(_verify_token)):
    """Get circuit breaker state for all services."""
    with _circuit_breakers_lock:
        breakers = {}
        for service, cb in _circuit_breakers.items():
            breakers[service] = {
                "state": cb["state"],
                "failure_count": cb["failure_count"],
                "success_count": cb["success_count"],
                "last_failure_at": cb["last_failure_at"],
                "last_state_change": cb["last_state_change"],
            }
    return {"circuit_breakers": breakers}


async def api_record_circuit_result(
    service: str,
    body: Dict[str, Any],
    _auth: None = Depends(_verify_token),
):
    """Record a success or failure for a service's circuit breaker.

    Example body:
    {
        "success": true
    }
    """
    success = body.get("success", True)
    if success:
        state = _record_success(service)
    else:
        state = _record_failure(service)

    return {"service": service, "new_state": state}


async def api_check_circuit(service: str, _auth: None = Depends(_verify_token)):
    """Check if a service call is allowed by the circuit breaker."""
    allowed = _can_execute(service)
    cb = _get_circuit_state(service)
    return {
        "service": service,
        "allowed": allowed,
        "state": cb["state"],
    }


async def api_reset_circuit_breaker(service: str, _auth: None = Depends(_verify_token)):
    """Reset a circuit breaker to closed state."""
    with _circuit_breakers_lock:
        if service not in _circuit_breakers:
            raise HTTPException(status_code=404, detail="Service circuit breaker not found")
        _circuit_breakers[service] = {
            "service": service,
            "state": "closed",
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
        logger.warning(f"Failed to reset circuit breaker: {e}")
    return {"status": "ok", "service": service, "state": "closed"}


async def api_set_circuit_config(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Configure circuit breaker thresholds for a service.

    Example body:
    {
        "service": "Trapper",
        "failure_threshold": 5,
        "recovery_timeout": 60,
        "success_threshold": 3
    }
    """
    service = body.get("service", "").strip()
    if not service:
        raise HTTPException(status_code=400, detail="service is required")

    cb = _get_circuit_state(service)
    with _circuit_breakers_lock:
        if "failure_threshold" in body:
            cb["failure_threshold"] = body["failure_threshold"]
        if "recovery_timeout" in body:
            cb["recovery_timeout"] = body["recovery_timeout"]
        if "success_threshold" in body:
            cb["success_threshold"] = body["success_threshold"]

    return {"status": "ok", "config": cb}


def register(app):
    """Register circuit_breakers routes with the FastAPI app."""
    app.get("/api/circuit-breakers")(api_list_circuit_breakers)
    app.post("/api/circuit-breakers/{service}/record")(api_record_circuit_result)
    app.post("/api/circuit-breakers/{service}/check")(api_check_circuit)
    app.post("/api/circuit-breakers/{service}/reset")(api_reset_circuit_breaker)
    app.post("/api/circuit-breakers/config")(api_set_circuit_config)
