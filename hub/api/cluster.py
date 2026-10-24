"""Route module: cluster management.

API endpoints for multi-instance cluster management:
- GET /api/cluster/status — Cluster status and instance list
- GET /api/cluster/instances — List all registered instances
- POST /api/cluster/heartbeat — Manual heartbeat update
"""

import logging
from typing import Any, Dict

from fastapi import Depends, HTTPException

from hub.api.core import _verify_token

logger = logging.getLogger(__name__)


async def api_cluster_status(_auth: None = Depends(_verify_token)):
    """Get cluster status including Redis connection and instance count."""
    try:
        from hub import cluster as cluster_module
    except ImportError:
        return {
            "available": False,
            "message": "Cluster module not available",
        }

    return await cluster_module.cluster.status()


async def api_list_instances(_auth: None = Depends(_verify_token)):
    """List all registered cluster instances."""
    try:
        from hub import cluster as cluster_module
    except ImportError:
        raise HTTPException(status_code=503, detail="Cluster module not available")

    if not cluster_module.cluster.available:
        return {
            "instances": [],
            "message": "Cluster mode disabled — running in single-instance mode",
        }

    instances = await cluster_module.cluster.instance_registry.get_instances()
    active = [i for i in instances if i["status"] == "active"]
    stale = [i for i in instances if i["status"] == "stale"]

    return {
        "total": len(instances),
        "active": len(active),
        "stale": len(stale),
        "instances": instances,
        "active_instances": active,
    }


async def api_heartbeat(_auth: None = Depends(_verify_token)):
    """Send a manual heartbeat for the current instance.

    Useful for health check probes that want to confirm the instance is alive.
    """
    try:
        from hub import cluster as cluster_module
    except ImportError:
        raise HTTPException(status_code=503, detail="Cluster module not available")

    if not cluster_module.cluster.available:
        return {"status": "disabled", "message": "Cluster mode not active"}

    instance_id = cluster_module.cluster.instance_id
    if not instance_id:
        raise HTTPException(status_code=500, detail="Instance not registered")

    updated = await cluster_module.cluster.instance_registry.heartbeat(instance_id)
    return {
        "status": "ok" if updated else "error",
        "instance_id": instance_id,
    }


async def api_job_queue_status(_auth: None = Depends(_verify_token)):
    """Get shared job queue status (Redis-based)."""
    try:
        from hub import cluster as cluster_module
    except ImportError:
        return {
            "available": False,
            "message": "Cluster module not available",
        }

    if not cluster_module.cluster.available or cluster_module.cluster.job_queue is None:
        return {
            "available": False,
            "message": "Shared job queue not available",
        }

    jq = cluster_module.cluster.job_queue
    pending = await jq.size()
    claimed = await jq.claimed_count()
    dlq = await jq.dlq_count()

    return {
        "available": True,
        "pending": pending,
        "claimed": claimed,
        "dead_letter": dlq,
        "total": pending + claimed + dlq,
    }


def register(app):
    """Register cluster routes with the FastAPI app."""
    app.get("/api/cluster/status")(api_cluster_status)
    app.get("/api/cluster/instances")(api_list_instances)
    app.post("/api/cluster/heartbeat")(api_heartbeat)
    app.get("/api/cluster/jobs/status")(api_job_queue_status)
