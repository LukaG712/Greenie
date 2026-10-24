"""Route module: schedules."""

import asyncio
import contextlib
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _scheduled_pipelines, _scheduled_pipelines_lock, _verify_token

logger = logging.getLogger(__name__)


async def _scheduled_pipeline_runner(pipeline_id: str) -> None:
    """Background runner for scheduled pipelines."""
    while True:
        with _scheduled_pipelines_lock:
            pipeline = _scheduled_pipelines.get(pipeline_id)
        if not pipeline or not pipeline.get("enabled", True):
            await asyncio.sleep(60)
            continue
        await asyncio.sleep(60)


async def api_create_schedule(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Create a scheduled pipeline that runs on a cron-like schedule.

    Example body:
    {
        "name": "Daily research digest",
        "schedule": "0 9 * * *",  // Every day at 9 AM
        "steps": [
            {"service": "Trapper", "task": "SEARCH: latest AI news"},
            {"service": "Slinker", "task": "Summarize the research findings"}
        ]
    }
    """
    name = body.get("name", f"schedule-{uuid.uuid4().hex[:8]}")
    schedule = body.get("schedule", "")
    steps = body.get("steps", [])

    if not schedule:
        raise HTTPException(status_code=400, detail="schedule is required (cron expression)")
    if not steps:
        raise HTTPException(status_code=400, detail="steps are required")

    # Validate cron expression
    parts = schedule.strip().split()
    if len(parts) != 5:
        raise HTTPException(status_code=400, detail="Invalid cron expression (expected 5 fields)")

    pipeline_id = f"sched-{uuid.uuid4().hex[:8]}"
    pipeline_data = {
        "id": pipeline_id,
        "name": name,
        "schedule": schedule,
        "steps": steps,
        "enabled": body.get("enabled", True),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_count": 0,
        "last_run": None,
        "last_result": None,
    }

    with _scheduled_pipelines_lock:
        _scheduled_pipelines[pipeline_id] = pipeline_data
        # Persist to database
        try:
            feature_store.save_scheduled_pipeline(pipeline_data)
        except Exception as e:
            logger.warning(f"Failed to save scheduled pipeline: {e}")
        # Start background runner
        task = asyncio.create_task(_scheduled_pipeline_runner(pipeline_id))
        _scheduled_tasks[pipeline_id] = task

    return pipeline_data


async def api_list_schedules(_auth: None = Depends(_verify_token)):
    """List all scheduled pipelines."""
    with _scheduled_pipelines_lock:
        return {"schedules": list(_scheduled_pipelines.values())}


async def api_get_schedule(schedule_id: str, _auth: None = Depends(_verify_token)):
    """Get a scheduled pipeline by ID."""
    with _scheduled_pipelines_lock:
        pipeline = _scheduled_pipelines.get(schedule_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return pipeline


async def api_update_schedule(schedule_id: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Update a scheduled pipeline."""
    with _scheduled_pipelines_lock:
        pipeline = _scheduled_pipelines.get(schedule_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Schedule not found")
        if "name" in body:
            pipeline["name"] = body["name"]
        if "schedule" in body:
            pipeline["schedule"] = body["schedule"]
        if "steps" in body:
            pipeline["steps"] = body["steps"]
        if "enabled" in body:
            pipeline["enabled"] = body["enabled"]
        return pipeline


async def api_delete_schedule(schedule_id: str, _auth: None = Depends(_verify_token)):
    """Delete a scheduled pipeline."""
    with _scheduled_pipelines_lock:
        if schedule_id not in _scheduled_pipelines:
            raise HTTPException(status_code=404, detail="Schedule not found")
        del _scheduled_pipelines[schedule_id]
        # Persist to database
        try:
            feature_store.delete_scheduled_pipeline(schedule_id)
        except Exception as e:
            logger.warning(f"Failed to delete scheduled pipeline: {e}")
        task = _scheduled_tasks.pop(schedule_id, None)
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return {"status": "ok", "message": "Schedule deleted"}


# ─── Unified Cross-Service Search ───────────────────────────────────────────



def register(app):
    """Register schedules routes with the FastAPI app."""
    app.post("/api/schedules", status_code=201)(api_create_schedule)
    app.get("/api/schedules")(api_list_schedules)
    app.get("/api/schedules/{schedule_id}")(api_get_schedule)
    app.put("/api/schedules/{schedule_id}")(api_update_schedule)
    app.delete("/api/schedules/{schedule_id}")(api_delete_schedule)
