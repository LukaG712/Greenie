"""Route module: reports."""

import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Request

from hub import agents, feature_store
from hub.api.core import _analytics, _scheduled_reports, _scheduled_reports_lock, _verify_token

logger = logging.getLogger(__name__)

# Analytics data
_analytics_api_calls: List[Dict[str, Any]] = []
_analytics_task_durations: List[Dict[str, Any]] = []
_runs: Dict[str, Any] = {}


async def api_create_scheduled_report(request: Request, _auth: None = Depends(_verify_token)):
    """Create a scheduled report.
    
    Body: {
        "name": "daily summary",
        "cron": "0 9 * * *",
        "type": "analytics|runs|agents|custom",
        "format": "json|csv|text",
        "recipients": ["email@example.com"],
        "config": {...}
    }
    """
    body = await request.json()
    
    report_id = str(uuid.uuid4())[:8]
    report = {
        "id": report_id,
        "name": body.get("name", "untitled"),
        "cron": body.get("cron", "0 9 * * *"),
        "type": body.get("type", "analytics"),
        "format": body.get("format", "json"),
        "recipients": body.get("recipients", []),
        "config": body.get("config", {}),
        "enabled": body.get("enabled", True),
        "last_run": None,
        "last_result": None,
        "run_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    
    with _scheduled_reports_lock:
        _scheduled_reports[report_id] = report
    
    # Persist to database
    try:
        feature_store.save_scheduled_report(report)
    except Exception as e:
        logger.warning(f"Failed to save scheduled report: {e}")
    
    return report


async def api_list_scheduled_reports(_auth: None = Depends(_verify_token)):
    """List all scheduled reports."""
    with _scheduled_reports_lock:
        return list(_scheduled_reports.values())


async def api_get_scheduled_report(report_id: str, _auth: None = Depends(_verify_token)):
    """Get a scheduled report's details."""
    with _scheduled_reports_lock:
        report = _scheduled_reports.get(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    return report


async def api_update_scheduled_report(report_id: str, request: Request, _auth: None = Depends(_verify_token)):
    """Update a scheduled report."""
    with _scheduled_reports_lock:
        report = _scheduled_reports.get(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    body = await request.json()
    with _scheduled_reports_lock:
        if "name" in body:
            report["name"] = body["name"]
        if "cron" in body:
            report["cron"] = body["cron"]
        if "type" in body:
            report["type"] = body["type"]
        if "format" in body:
            report["format"] = body["format"]
        if "recipients" in body:
            report["recipients"] = body["recipients"]
        if "config" in body:
            report["config"] = body["config"]
        if "enabled" in body:
            report["enabled"] = body["enabled"]
        report["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    # Persist to database
    try:
        feature_store.save_scheduled_report(report)
    except Exception as e:
        logger.warning(f"Failed to save report update: {e}")
    
    return report


async def api_delete_scheduled_report(report_id: str, _auth: None = Depends(_verify_token)):
    """Delete a scheduled report."""
    with _scheduled_reports_lock:
        if report_id not in _scheduled_reports:
            raise HTTPException(status_code=404, detail="Report not found")
        del _scheduled_reports[report_id]
    
    # Persist to database
    try:
        feature_store.delete_scheduled_report(report_id)
    except Exception as e:
        logger.warning(f"Failed to delete report: {e}")
    
    return {"status": "ok", "message": "Report deleted"}


async def api_run_report_now(report_id: str, _auth: None = Depends(_verify_token)):
    """Run a scheduled report immediately."""
    with _scheduled_reports_lock:
        report = _scheduled_reports.get(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    # Generate report content
    report_type = report.get("type", "analytics")
    report_format = report.get("format", "json")
    
    content = _generate_report_content(report_type)
    
    # Update report stats
    with _scheduled_reports_lock:
        report["last_run"] = datetime.now(timezone.utc).isoformat()
        report["last_result"] = {"status": "ok", "format": report_format}
        report["run_count"] = report.get("run_count", 0) + 1
    
    # Persist to database
    try:
        feature_store.save_scheduled_report(report)
    except Exception as e:
        logger.warning(f"Failed to save report run result: {e}")
    
    return {
        "status": "ok",
        "report_id": report_id,
        "content": content,
        "format": report_format,
    }


def _generate_report_content(report_type: str) -> Dict[str, Any]:
    """Generate report content based on type."""
    if report_type == "analytics":
        return {
            "api_calls": len(_analytics_api_calls),
            "task_durations": dict(_analytics_task_durations[-100:] if _analytics_task_durations else {}),
            "runs_by_agent": {},
        }
    elif report_type == "runs":
        return {
            "total_runs": len(_runs),
            "runs": [
                {
                    "id": r.id[:8],
                    "agent": r.agent,
                    "state": r.state,
                    "task": r.task[:100],
                }
                for r in list(_runs.values())[:50]
            ],
        }
    elif report_type == "agents":
        return {
            "agents": list(agents.list_agents().keys()),
            "count": len(agents.list_agents()),
        }
    else:
        return {"type": report_type, "message": "Custom report type"}



def register(app):
    """Register reports routes with the FastAPI app."""
    app.post("/api/reports")(api_create_scheduled_report)
    app.get("/api/reports")(api_list_scheduled_reports)
    app.get("/api/reports/{report_id}")(api_get_scheduled_report)
    app.put("/api/reports/{report_id}")(api_update_scheduled_report)
    app.delete("/api/reports/{report_id}")(api_delete_scheduled_report)
    app.post("/api/reports/{report_id}/run")(api_run_report_now)
