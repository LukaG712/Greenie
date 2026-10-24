"""Route module: jobs."""

import asyncio
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _response_cache, _response_cache_lock, _verify_token
from hub.runner import Run, agent_status, get_run, list_runs, recover_stale_runs, run_agent, stop_run

logger = logging.getLogger(__name__)

# Job states
JOB_PENDING = "pending"
JOB_RUNNING = "running"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"
JOB_DEAD = "dead"

# Job queue
_job_queue: Dict[str, Dict[str, Any]] = {}
_job_queue_lock = threading.Lock()

# Job ordering
_job_order: List[str] = []
_job_order_lock = threading.Lock()

# Dead letter queue
_dead_letter_queue: List[Dict[str, Any]] = []
_dead_letter_lock = threading.Lock()

# Worker task
_job_worker_task: Optional[asyncio.Task] = None


def _audit(action: str, user: str, resource_type: str, resource_id: str, details: str) -> None:
    """Record an audit log entry."""
    pass  # Placeholder - audit logging not implemented yet


async def _job_worker() -> None:
    """Background worker that processes jobs from the queue."""
    while True:
        job_id = None
        with _job_order_lock:
            if _job_order:
                job_id = _job_order.pop(0)
        
        if not job_id:
            await asyncio.sleep(1)
            continue
        
        with _job_queue_lock:
            job = _job_queue.get(job_id)
            if not job or job["state"] != JOB_PENDING:
                continue
            job["state"] = JOB_RUNNING
            job["started_at"] = datetime.now(timezone.utc).isoformat()
        
        try:
            # Process job based on type
            job_type = job["type"]
            payload = job.get("payload", {})
            
            if job_type == "run_agent":
                agent = payload.get("agent", "")
                task = payload.get("task", "")
                if agent and task:
                    run = run_agent(agent, task)
                    job["result"] = {"run_id": run.id, "answer": run.answer}
            
            job["state"] = JOB_COMPLETED
            job["completed_at"] = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            job["retries"] = job.get("retries", 0) + 1
            job["last_error"] = str(e)
            if job["retries"] >= job.get("max_retries", 3):
                job["state"] = JOB_DEAD
                with _dead_letter_lock:
                    _dead_letter_queue.append(job)
            else:
                job["state"] = JOB_FAILED
        
        with _job_queue_lock:
            _job_queue[job_id] = job
        
        await asyncio.sleep(0.1)


async def api_submit_job(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Submit a new job to the background queue.

    Example body:
    {
        "type": "run_agent",
        "payload": {
            "agent": "vault",
            "task": "Summarize the latest news"
        },
        "max_retries": 3,
        "priority": 0,
        "metadata": {"source": "webhook"}
    }

    Job types: run_agent, pipeline, Trapper, Slinker, webhook
    """
    job_type = body.get("type", "").strip()
    if not job_type:
        raise HTTPException(status_code=400, detail="type is required")

    job_id = f"job-{uuid.uuid4().hex[:8]}"
    job = {
        "id": job_id,
        "type": job_type,
        "payload": body.get("payload", {}),
        "state": JOB_PENDING,
        "priority": body.get("priority", 0),
        "max_retries": body.get("max_retries", 3),
        "retries": 0,
        "metadata": body.get("metadata", {}),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": None,
        "completed_at": None,
        "result": None,
        "last_error": None,
    }

    with _job_queue_lock:
        _job_queue[job_id] = job
    with _job_order_lock:
        _job_order.append(job_id)

    # Persist to database
    try:
        feature_store.save_background_job(job)
    except Exception as e:
        logger.warning(f"Failed to save job: {e}")

    # Start worker if not running
    global _job_worker_task
    if _job_worker_task is None or _job_worker_task.done():
        _job_worker_task = asyncio.create_task(_job_worker())

    _audit("job.submitted", _auth or "anonymous", "job", f"Submitted job {job_id} ({job_type})", "")
    return job


async def api_list_jobs(
    state: Optional[str] = None,
    _auth: None = Depends(_verify_token),
):
    """List all jobs, optionally filtered by state."""
    with _job_queue_lock:
        jobs = list(_job_queue.values())

    if state:
        jobs = [j for j in jobs if j["state"] == state]

    # Sort by priority (higher first) then by creation time
    jobs.sort(key=lambda j: (-j.get("priority", 0), j.get("created_at", "")))

    return {"jobs": jobs}


async def api_get_job(job_id: str, _auth: None = Depends(_verify_token)):
    """Get a job by ID."""
    with _job_queue_lock:
        job = _job_queue.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


async def api_cancel_job(job_id: str, _auth: None = Depends(_verify_token)):
    """Cancel a pending job."""
    with _job_queue_lock:
        job = _job_queue.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job["state"] in (JOB_RUNNING, JOB_COMPLETED):
            raise HTTPException(status_code=400, detail=f"Cannot cancel job in {job['state']} state")
        job["state"] = "cancelled"
        job["cancelled_at"] = datetime.now(timezone.utc).isoformat()

    with _job_order_lock:
        if job_id in _job_order:
            _job_order.remove(job_id)

    # Persist to database
    try:
        feature_store.save_background_job(job)
    except Exception as e:
        logger.warning(f"Failed to save job cancellation: {e}")

    return {"status": "ok", "message": "Job cancelled"}


async def api_retry_job(job_id: str, _auth: None = Depends(_verify_token)):
    """Manually retry a failed or dead job."""
    with _job_queue_lock:
        job = _job_queue.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job["state"] not in (JOB_FAILED, JOB_DEAD, "cancelled"):
            raise HTTPException(status_code=400, detail=f"Cannot retry job in {job['state']} state")
        job["state"] = JOB_PENDING
        job["retries"] = 0
        job["last_error"] = None

    with _job_order_lock:
        _job_order.append(job_id)

    # Start worker if not running
    global _job_worker_task
    if _job_worker_task is None or _job_worker_task.done():
        _job_worker_task = asyncio.create_task(_job_worker())

    return {"status": "ok", "message": "Job re-queued for retry"}


async def api_list_dead_letter(_auth: None = Depends(_verify_token)):
    """List jobs in the dead letter queue (permanently failed)."""
    with _dead_letter_lock:
        jobs = list(_dead_letter_queue)
    return {"jobs": jobs, "count": len(jobs)}


async def api_replay_dead_letter(job_id: str, _auth: None = Depends(_verify_token)):
    """Replay a job from the dead letter queue."""
    with _dead_letter_lock:
        job = None
        for j in _dead_letter_queue:
            if j["id"] == job_id:
                job = j
                break
        if not job:
            raise HTTPException(status_code=404, detail="Job not found in dead letter queue")

    # Re-create as new job
    new_job_id = f"job-{uuid.uuid4().hex[:8]}"
    new_job = {
        "id": new_job_id,
        "type": job["type"],
        "payload": job["payload"],
        "state": JOB_PENDING,
        "priority": job.get("priority", 0),
        "max_retries": job.get("max_retries", 3),
        "retries": 0,
        "metadata": {**job.get("metadata", {}), "replayed_from": job_id},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": None,
        "completed_at": None,
        "result": None,
        "last_error": None,
    }

    with _job_queue_lock:
        _job_queue[new_job_id] = new_job
    with _job_order_lock:
        _job_order.append(new_job_id)

    # Remove from dead letter queue
    with _dead_letter_lock:
        _dead_letter_queue.remove(job)

    # Start worker if not running
    global _job_worker_task
    if _job_worker_task is None or _job_worker_task.done():
        _job_worker_task = asyncio.create_task(_job_worker())

    return {"status": "ok", "new_job_id": new_job_id}


async def api_job_stats(_auth: None = Depends(_verify_token)):
    """Get job queue statistics."""
    with _job_queue_lock:
        jobs = list(_job_queue.values())
    with _dead_letter_lock:
        dead_count = len(_dead_letter_queue)

    from collections import Counter
    by_state = Counter(j["state"] for j in jobs)
    by_type = Counter(j["type"] for j in jobs)

    return {
        "total": len(jobs),
        "by_state": dict(by_state),
        "by_type": dict(by_type),
        "dead_letter_count": dead_count,
        "pending": by_state.get(JOB_PENDING, 0),
        "running": by_state.get(JOB_RUNNING, 0),
        "completed": by_state.get(JOB_COMPLETED, 0),
        "failed": by_state.get(JOB_FAILED, 0) + by_state.get(JOB_DEAD, 0),
    }


# ─── Response Caching ───────────────────────────────────────────────────────

# In-memory response cache: key -> {data, expires_at, hits}
_response_cache: Dict[str, Dict[str, Any]] = {}
_response_cache_lock = threading.Lock()

# Per-endpoint cache TTL configuration (seconds)
_cache_ttl_config: Dict[str, int] = {
    "/api/dashboard": 30,
    "/api/search": 60,
    "/api/analytics/overview": 120,
    "/api/services/status": 30,
    "/api/Slinker/tasks": 10,
    "/api/agents": 300,
}
_cache_ttl_lock = threading.Lock()


def _cache_key(method: str, path: str, params: str = "") -> str:
    """Generate a cache key from request method, path, and params."""
    return f"{method}:{path}:{params}"


def _get_cached(key: str) -> Optional[Any]:
    """Get a cached response if it exists and hasn't expired."""
    now = time.time()
    with _response_cache_lock:
        entry = _response_cache.get(key)
        if entry is None:
            return None
        if entry["expires_at"] < now:
            del _response_cache[key]
            return None
        entry["hits"] += 1
        return entry["data"]


def _set_cached(key: str, data: Any, ttl: int = 60) -> None:
    """Store a response in the cache with the given TTL."""
    now = time.time()
    with _response_cache_lock:
        _response_cache[key] = {
            "data": data,
            "expires_at": now + ttl,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "hits": 0,
        }
        # Evict expired entries if cache grows too large
        if len(_response_cache) > 500:
            expired = [k for k, v in _response_cache.items() if v["expires_at"] < now]
            for k in expired[:100]:
                del _response_cache[k]



def register(app):
    """Register jobs routes with the FastAPI app."""
    app.post("/api/jobs", status_code=201)(api_submit_job)
    app.get("/api/jobs")(api_list_jobs)
    app.get("/api/jobs/{job_id}")(api_get_job)
    app.delete("/api/jobs/{job_id}")(api_cancel_job)
    app.post("/api/jobs/{job_id}/retry")(api_retry_job)
    app.get("/api/jobs/dead-letter")(api_list_dead_letter)
    app.post("/api/jobs/dead-letter/{job_id}/replay")(api_replay_dead_letter)
    app.get("/api/jobs/stats")(api_job_stats)
