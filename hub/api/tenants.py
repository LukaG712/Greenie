"""Route module: tenants."""

import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import (
    _api_keys,
    _tenant_resources,
    _tenant_resources_lock,
    _tenants,
    _tenants_lock,
    _verify_token,
)

logger = logging.getLogger(__name__)


async def api_create_tenant(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Create a new tenant (workspace) for resource isolation.

    Example body:
    {
        "name": "Acme Corp",
        "description": "Acme Corporation workspace",
        "limits": {
            "max_agents": 10,
            "max_runs_per_day": 1000,
            "max_api_keys": 5
        },
        "allowed_services": ["Trapper", "Slinker", "agent"]
    }
    """
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    tenant_id = f"tenant-{uuid.uuid4().hex[:8]}"
    tenant = {
        "id": tenant_id,
        "name": name,
        "description": body.get("description", ""),
        "limits": body.get("limits", {}),
        "allowed_services": body.get("allowed_services", ["Trapper", "Slinker", "agent"]),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "active": True,
        "resource_count": 0,
    }

    with _tenants_lock:
        _tenants[tenant_id] = tenant
    # Persist to database
    try:
        feature_store.save_tenant(tenant)
    except Exception as e:
        logger.warning(f"Failed to save tenant: {e}")
    with _tenant_resources_lock:
        _tenant_resources[tenant_id] = []

    return tenant


async def api_list_tenants(_auth: None = Depends(_verify_token)):
    """List all tenants."""
    with _tenants_lock:
        tenants = list(_tenants.values())
    return {"tenants": tenants}


async def api_get_tenant(tenant_id: str, _auth: None = Depends(_verify_token)):
    """Get a tenant by ID."""
    with _tenants_lock:
        tenant = _tenants.get(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return tenant


async def api_update_tenant(tenant_id: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Update a tenant's configuration."""
    with _tenants_lock:
        tenant = _tenants.get(tenant_id)
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")
        if "name" in body:
            tenant["name"] = body["name"].strip()
        if "description" in body:
            tenant["description"] = body["description"]
        if "limits" in body:
            tenant["limits"] = body["limits"]
        if "allowed_services" in body:
            tenant["allowed_services"] = body["allowed_services"]
        if "active" in body:
            tenant["active"] = body["active"]
    # Persist to database
    try:
        feature_store.save_tenant(tenant)
    except Exception as e:
        logger.warning(f"Failed to save tenant update: {e}")
    return dict(tenant)


async def api_delete_tenant(tenant_id: str, _auth: None = Depends(_verify_token)):
    """Delete a tenant and all its associated resources."""
    with _tenants_lock:
        if tenant_id not in _tenants:
            raise HTTPException(status_code=404, detail="Tenant not found")
        del _tenants[tenant_id]
    # Persist to database
    try:
        feature_store.delete_tenant(tenant_id)
    except Exception as e:
        logger.warning(f"Failed to delete tenant: {e}")

    with _tenant_resources_lock:
        _tenant_resources.pop(tenant_id, None)

    return {"status": "ok", "message": "Tenant deleted"}


async def api_assign_resource_to_tenant(tenant_id: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Assign a resource (agent, run, etc.) to a tenant.

    Example body:
    {
        "resource_type": "agent",
        "resource_id": "vault"
    }
    """
    resource_type = body.get("resource_type", "").strip()
    resource_id = body.get("resource_id", "").strip()

    if not resource_type or not resource_id:
        raise HTTPException(status_code=400, detail="resource_type and resource_id are required")

    with _tenants_lock:
        tenant = _tenants.get(tenant_id)
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

        # Check limits
        limits = tenant.get("limits", {})
        if resource_type == "agent" and "max_agents" in limits:
            with _tenant_resources_lock:
                current = len([r for r in _tenant_resources.get(tenant_id, []) if r.startswith("agent:")])
                if current >= limits["max_agents"]:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Tenant limit reached: max {limits['max_agents']} agents"
                    )

    resource_key = f"{resource_type}:{resource_id}"
    with _tenant_resources_lock:
        if tenant_id not in _tenant_resources:
            _tenant_resources[tenant_id] = []
        if resource_key not in _tenant_resources[tenant_id]:
            _tenant_resources[tenant_id].append(resource_key)

    # Persist to database
    try:
        feature_store.save_tenant_resource({
            "tenant_id": tenant_id,
            "resource_type": resource_type,
            "resource_id": resource_id,
        })
    except Exception as e:
        logger.warning(f"Failed to save tenant resource: {e}")

    with _tenants_lock:
        tenant["resource_count"] = len(_tenant_resources.get(tenant_id, []))

    return {"status": "ok", "tenant_id": tenant_id, "resource": resource_key}


async def api_tenant_resources(tenant_id: str, _auth: None = Depends(_verify_token)):
    """List all resources assigned to a tenant."""
    with _tenants_lock:
        tenant = _tenants.get(tenant_id)
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

    with _tenant_resources_lock:
        resources = list(_tenant_resources.get(tenant_id, []))

    return {"tenant_id": tenant_id, "resources": resources, "count": len(resources)}


async def api_unassign_resource(tenant_id: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Remove a resource from a tenant."""
    resource_type = body.get("resource_type", "").strip()
    resource_id = body.get("resource_id", "").strip()

    if not resource_type or not resource_id:
        raise HTTPException(status_code=400, detail="resource_type and resource_id are required")

    resource_key = f"{resource_type}:{resource_id}"
    with _tenant_resources_lock:
        resources = _tenant_resources.get(tenant_id, [])
        if resource_key in resources:
            resources.remove(resource_key)
            _tenant_resources[tenant_id] = resources

    # Persist to database
    try:
        feature_store.delete_tenant_resource(tenant_id, resource_type, resource_id)
    except Exception as e:
        logger.warning(f"Failed to delete tenant resource: {e}")

    return {"status": "ok", "tenant_id": tenant_id, "resource": resource_key}


async def api_tenant_usage(tenant_id: str, _auth: None = Depends(_verify_token)):
    """Get resource usage statistics for a tenant."""
    with _tenants_lock:
        tenant = _tenants.get(tenant_id)
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

    with _tenant_resources_lock:
        resources = _tenant_resources.get(tenant_id, [])

    # Count by type
    from collections import Counter
    by_type = Counter(r.split(":")[0] for r in resources if ":" in r)

    limits = tenant.get("limits", {})
    usage = {}
    for limit_key, limit_val in limits.items():
        resource_type = limit_key.replace("max_", "").replace("_per_day", "")
        current = by_type.get(resource_type, 0)
        usage[limit_key] = {
            "current": current,
            "limit": limit_val,
            "remaining": max(0, limit_val - current),
            "percentage": round(current / max(limit_val, 1) * 100, 1),
        }

    return {
        "tenant_id": tenant_id,
        "total_resources": len(resources),
        "by_type": dict(by_type),
        "limits_usage": usage,
    }


# ─── Database Backup & Restore ──────────────────────────────────────────────

_BACKUP_DIR = Path(__file__).resolve().parent.parent / "data" / "backups"



def register(app):
    """Register tenants routes with the FastAPI app."""
    app.post("/api/tenants", status_code=201)(api_create_tenant)
    app.get("/api/tenants")(api_list_tenants)
    app.get("/api/tenants/{tenant_id}")(api_get_tenant)
    app.put("/api/tenants/{tenant_id}")(api_update_tenant)
    app.delete("/api/tenants/{tenant_id}")(api_delete_tenant)
    app.post("/api/tenants/{tenant_id}/assign")(api_assign_resource_to_tenant)
    app.get("/api/tenants/{tenant_id}/resources")(api_tenant_resources)
    app.post("/api/tenants/{tenant_id}/unassign")(api_unassign_resource)
    app.get("/api/tenants/{tenant_id}/usage")(api_tenant_usage)
