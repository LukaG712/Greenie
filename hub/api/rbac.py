"""Route module: rbac."""

import asyncio
import json
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _verify_token
from hub.runner import Run, agent_status, get_run, list_runs, recover_stale_runs, run_agent, stop_run

logger = logging.getLogger(__name__)

# Role store
_roles: Dict[str, Dict[str, Any]] = {
    "admin": {"name": "admin", "permissions": ["*"], "description": "Full access"},
    "developer": {"name": "developer", "permissions": ["read", "write", "runs"], "description": "Developer access"},
    "viewer": {"name": "viewer", "permissions": ["read"], "description": "Read-only access"},
}
_roles_lock = threading.Lock()

# User store
_users: Dict[str, Dict[str, Any]] = {}
_users_lock = threading.Lock()


def _audit(action: str, user: str, resource_type: str, resource_id: str, details: str) -> None:
    """Record an audit log entry."""
    pass  # Placeholder


def _emit_event(event_type: str, data: Dict[str, Any]) -> None:
    """Emit an event to the event bus."""
    pass  # Placeholder


async def _execute_template_pipeline(steps: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Execute a template pipeline."""
    return {"status": "completed"}


async def api_create_role(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Create a new role with custom permissions.

    Example body:
    {
        "name": "analyst",
        "permissions": ["read", "analytics", "export"],
        "description": "Can view analytics and export data"
    }
    """
    role_name = body.get("name", "").strip().lower()
    if not role_name:
        raise HTTPException(status_code=400, detail="name is required")

    permissions = body.get("permissions", [])
    if not isinstance(permissions, list) or not permissions:
        raise HTTPException(status_code=400, detail="permissions must be a non-empty list")

    role = {
        "name": role_name,
        "permissions": permissions,
        "description": body.get("description", ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    with _roles_lock:
        _roles[role_name] = role

    # Persist to database
    try:
        role["id"] = role_name
        feature_store.save_rbac_role(role)
    except Exception as e:
        logger.warning(f"Failed to save role: {e}")

    _audit("role.created", _auth or "anonymous", "role", f"Created role {role_name}", "")
    return role


async def api_list_roles(_auth: None = Depends(_verify_token)):
    """List all available roles."""
    with _roles_lock:
        return {"roles": dict(_roles)}


async def api_get_role(role_name: str, _auth: None = Depends(_verify_token)):
    """Get a role by name."""
    with _roles_lock:
        role = _roles.get(role_name)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    return role


async def api_update_role(role_name: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Update a role's permissions."""
    with _roles_lock:
        role = _roles.get(role_name)
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        if "permissions" in body:
            role["permissions"] = body["permissions"]
        if "description" in body:
            role["description"] = body["description"]
    # Persist to database
    try:
        role["id"] = role_name
        feature_store.save_rbac_role(role)
    except Exception as e:
        logger.warning(f"Failed to save role update: {e}")
    return dict(role)


async def api_delete_role(role_name: str, _auth: None = Depends(_verify_token)):
    """Delete a custom role (built-in roles cannot be deleted)."""
    if role_name in ("admin", "developer", "viewer"):
        raise HTTPException(status_code=403, detail="Cannot delete built-in role")

    with _roles_lock:
        if role_name not in _roles:
            raise HTTPException(status_code=404, detail="Role not found")
        del _roles[role_name]

    # Persist to database
    try:
        feature_store.delete_rbac_role(role_name)
    except Exception as e:
        logger.warning(f"Failed to delete role: {e}")

    _audit("role.deleted", _auth or "anonymous", "role", f"Deleted role {role_name}", "")
    return {"status": "ok", "message": "Role deleted"}


async def api_create_user(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Create a new user with an assigned role.

    Example body:
    {
        "username": "alice",
        "role": "developer",
        "api_key": "key-abc123",
        "email": "alice@example.com"
    }
    """
    username = body.get("username", "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username is required")

    role = body.get("role", "viewer")
    with _roles_lock:
        if role not in _roles:
            raise HTTPException(status_code=400, detail=f"Role '{role}' does not exist")

    user = {
        "username": username,
        "role": role,
        "api_key": body.get("api_key", ""),
        "email": body.get("email", ""),
        "active": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "last_login": None,
    }

    with _users_lock:
        _users[username] = user

    # Persist to database
    try:
        user["id"] = username
        user["role_id"] = user.get("role", "viewer")
        feature_store.save_rbac_user(user)
    except Exception as e:
        logger.warning(f"Failed to save user: {e}")

    _audit("user.created", _auth or "anonymous", "user", f"Created user {username} with role {role}", "")
    return user


async def api_list_users(_auth: None = Depends(_verify_token)):
    """List all users."""
    with _users_lock:
        users = list(_users.values())
    return {"users": users}


async def api_get_user(username: str, _auth: None = Depends(_verify_token)):
    """Get a user by username."""
    with _users_lock:
        user = _users.get(username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


async def api_update_user(username: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Update a user's role or status."""
    with _users_lock:
        user = _users.get(username)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if "role" in body:
            with _roles_lock:
                if body["role"] not in _roles:
                    raise HTTPException(status_code=400, detail=f"Role '{body['role']}' does not exist")
            user["role"] = body["role"]
        if "active" in body:
            user["active"] = body["active"]
        if "email" in body:
            user["email"] = body["email"]
    # Persist to database
    try:
        user["id"] = username
        user["role_id"] = user.get("role", "viewer")
        feature_store.save_rbac_user(user)
    except Exception as e:
        logger.warning(f"Failed to save user update: {e}")
    return dict(user)


async def api_delete_user(username: str, _auth: None = Depends(_verify_token)):
    """Delete a user."""
    with _users_lock:
        if username not in _users:
            raise HTTPException(status_code=404, detail="User not found")
        del _users[username]

    # Persist to database
    try:
        feature_store.delete_rbac_user(username)
    except Exception as e:
        logger.warning(f"Failed to delete user: {e}")

    _audit("user.deleted", _auth or "anonymous", "user", f"Deleted user {username}", "")
    return {"status": "ok", "message": "User deleted"}


def _check_permission(key_id: str, permission: str) -> bool:
    """Check if an API key holder has a specific permission via RBAC."""
    # Find user associated with this key
    with _users_lock:
        user = None
        for u in _users.values():
            if u.get("api_key") == key_id:
                user = u
                break

    if not user:
        return True  # Primary key has full access

    with _roles_lock:
        role = _roles.get(user.get("role", "viewer"))
        if not role:
            return False
        perms = role.get("permissions", [])
        return "*" in perms or permission in perms


# ─── Background Job Queue ───────────────────────────────────────────────────

# Job states
JOB_PENDING = "pending"
JOB_RUNNING = "running"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"
JOB_RETRYING = "retrying"
JOB_DEAD = "dead"

# Job queue
_job_queue: Dict[str, Dict[str, Any]] = {}
_job_queue_lock = threading.Lock()
_job_order: List[str] = []  # FIFO ordering
_job_order_lock = threading.Lock()

# Dead letter queue for permanently failed jobs
_dead_letter_queue: List[Dict[str, Any]] = []
_dead_letter_lock = threading.Lock()

# Job worker task
_job_worker_task: Optional[asyncio.Task] = None


async def _job_worker():
    """Background worker that processes jobs from the queue."""
    while True:
        job_id = None
        with _job_queue_lock:
            with _job_order_lock:
                for jid in _job_order:
                    job = _job_queue.get(jid)
                    if job and job["state"] == JOB_PENDING:
                        job_id = jid
                        job["state"] = JOB_RUNNING
                        job["started_at"] = datetime.now(timezone.utc).isoformat()
                        break

        if job_id:
            await _process_job(job_id)
        else:
            await asyncio.sleep(0.5)


async def _process_job(job_id: str):
    """Process a single job from the queue."""
    with _job_queue_lock:
        job = _job_queue.get(job_id)
        if not job:
            return

    try:
        task_type = job.get("type", "")
        payload = job.get("payload", {})

        if task_type == "run_agent":
            agent = payload.get("agent", "")
            task = payload.get("task", "")
            if agent and task:
                run = run_agent(agent, task)
                with _job_queue_lock:
                    job["run_id"] = run.id
                    job["result"] = {"run_id": run.id}

        elif task_type == "pipeline":
            steps = payload.get("steps", [])
            result = await _execute_template_pipeline(steps)
            with _job_queue_lock:
                job["result"] = result

        elif task_type == "Trapper":
            from hub.Trapper_provider import TrapperProvider
            provider = TrapperProvider()
            task = payload.get("task", "")
            result = await provider.execute(task)
            with _job_queue_lock:
                job["result"] = result

        elif task_type == "Slinker":
            from hub.Slinker_provider import SlinkerProvider
            provider = SlinkerProvider()
            task = payload.get("task", "")
            result = await provider.execute(task)
            with _job_queue_lock:
                job["result"] = result

        elif task_type == "webhook":
            import httpx
            url = payload.get("url", "")
            data = payload.get("data", {})
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json=data)
                with _job_queue_lock:
                    job["result"] = {"status_code": resp.status_code}

        else:
            with _job_queue_lock:
                job["result"] = {"message": f"Unknown job type: {task_type}"}

        with _job_queue_lock:
            job["state"] = JOB_COMPLETED
            job["completed_at"] = datetime.now(timezone.utc).isoformat()

        _emit_event("job.completed", {"job_id": job_id, "type": task_type})

    except Exception as e:
        _handle_job_failure(job_id, str(e))


def _handle_job_failure(job_id: str, error: str):
    """Handle a job failure with retry logic."""
    with _job_queue_lock:
        job = _job_queue.get(job_id)
        if not job:
            return

        retries = job.get("retries", 0)
        max_retries = job.get("max_retries", 3)

        if retries < max_retries:
            job["retries"] = retries + 1
            job["state"] = JOB_RETRYING
            job["last_error"] = error
            job["next_retry_at"] = datetime.fromtimestamp(
                time.time() + (2 ** retries) * 5  # Exponential backoff
            ).isoformat()

            # Re-queue for retry
            asyncio.create_task(_retry_job(job_id, (2 ** retries) * 5))
        else:
            job["state"] = JOB_DEAD
            job["last_error"] = error
            job["failed_at"] = datetime.now(timezone.utc).isoformat()

            # Move to dead letter queue
            with _dead_letter_lock:
                _dead_letter_queue.append(dict(job))

    _emit_event("job.failed", {"job_id": job_id, "error": error, "retries": retries})


async def _retry_job(job_id: str, delay: float):
    """Wait and re-queue a job for retry."""
    await asyncio.sleep(delay)
    with _job_queue_lock:
        job = _job_queue.get(job_id)
        if job and job["state"] == JOB_RETRYING:
            job["state"] = JOB_PENDING



def register(app):
    """Register rbac routes with the FastAPI app."""
    app.post("/api/roles", status_code=201)(api_create_role)
    app.get("/api/roles")(api_list_roles)
    app.get("/api/roles/{role_name}")(api_get_role)
    app.put("/api/roles/{role_name}")(api_update_role)
    app.delete("/api/roles/{role_name}")(api_delete_role)
    app.post("/api/users", status_code=201)(api_create_user)
    app.get("/api/users")(api_list_users)
    app.get("/api/users/{username}")(api_get_user)
    app.put("/api/users/{username}")(api_update_user)
    app.delete("/api/users/{username}")(api_delete_user)
