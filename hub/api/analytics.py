"""Route module: analytics."""

from typing import Any, Dict, List, Optional

from fastapi import Depends

from hub.api.core import _analytics, _analytics_lock, _verify_token


async def api_analytics_overview(_auth: None = Depends(_verify_token)):
    """Get an overview of platform usage analytics.

    Returns API call counts, task durations, runs by agent, and error rates.
    """
    with _analytics_lock:
        api_calls = dict(_analytics["api_calls"])
        runs_by_agent = dict(_analytics["runs_by_agent"])
        errors_by_type = dict(_analytics["errors_by_type"])
        daily_stats = dict(_analytics["daily_stats"])

        # Calculate task duration stats
        durations = _analytics["task_durations"]
        if durations:
            all_durations = [d["duration_ms"] for d in durations]
            avg_duration = sum(all_durations) / len(all_durations)
            max_duration = max(all_durations)
            min_duration = min(all_durations)
            p95_duration = sorted(all_durations)[int(len(all_durations) * 0.95)]
        else:
            avg_duration = max_duration = min_duration = p95_duration = 0

        # Task stats by service
        by_service = {}
        for d in durations:
            svc = d["service"]
            if svc not in by_service:
                by_service[svc] = {"count": 0, "total_ms": 0, "errors": 0}
            by_service[svc]["count"] += 1
            by_service[svc]["total_ms"] += d["duration_ms"]
            if d["status"] == "error":
                by_service[svc]["errors"] += 1

        for svc in by_service:
            total = by_service[svc]["total_ms"]
            count = by_service[svc]["count"]
            by_service[svc]["avg_ms"] = total / count if count else 0
            by_service[svc]["error_rate"] = by_service[svc]["errors"] / count if count else 0

    return {
        "api_calls": api_calls,
        "runs_by_agent": runs_by_agent,
        "errors_by_type": errors_by_type,
        "daily_stats": daily_stats,
        "task_durations": {
            "total_tasks": len(durations),
            "avg_ms": round(avg_duration, 2),
            "max_ms": round(max_duration, 2),
            "min_ms": round(min_duration, 2),
            "p95_ms": round(p95_duration, 2),
            "by_service": by_service,
        },
    }


async def api_analytics_api_calls(
    limit: int = 50,
    _auth: None = Depends(_verify_token),
):
    """Get API call counts, sorted by frequency."""
    with _analytics_lock:
        calls = dict(_analytics["api_calls"])
    sorted_calls = sorted(calls.items(), key=lambda x: x[1], reverse=True)[:limit]
    return {"api_calls": [{"endpoint": ep, "count": cnt} for ep, cnt in sorted_calls]}


async def api_analytics_tasks(
    service: Optional[str] = None,
    limit: int = 100,
    _auth: None = Depends(_verify_token),
):
    """Get recent task execution records."""
    with _analytics_lock:
        durations = list(_analytics["task_durations"])

    if service:
        durations = [d for d in durations if d["service"] == service]
    durations = durations[-limit:]
    return {"tasks": durations}


async def api_analytics_errors(
    limit: int = 50,
    _auth: None = Depends(_verify_token),
):
    """Get error counts by type."""
    with _analytics_lock:
        errors = dict(_analytics["errors_by_type"])
    sorted_errors = sorted(errors.items(), key=lambda x: x[1], reverse=True)[:limit]
    return {"errors": [{"type": et, "count": cnt} for et, cnt in sorted_errors]}


async def api_analytics_daily(
    days: int = 30,
    _auth: None = Depends(_verify_token),
):
    """Get daily stats for the last N days."""
    with _analytics_lock:
        daily = dict(_analytics["daily_stats"])

    # Get last N days
    dates = sorted(daily.keys(), reverse=True)[:days]
    return {"daily": [{"date": d, **daily[d]} for d in dates]}


async def api_reset_analytics(_auth: None = Depends(_verify_token)):
    """Reset all analytics data (useful for testing)."""
    with _analytics_lock:
        _analytics["api_calls"] = {}
        _analytics["task_durations"] = []
        _analytics["runs_by_agent"] = {}
        _analytics["errors_by_type"] = {}
        _analytics["daily_stats"] = {}
    return {"status": "ok", "message": "Analytics data reset"}


def register(app):
    """Register analytics routes with the FastAPI app."""
    app.get("/api/analytics/overview")(api_analytics_overview)
    app.get("/api/analytics/api-calls")(api_analytics_api_calls)
    app.get("/api/analytics/tasks")(api_analytics_tasks)
    app.get("/api/analytics/errors")(api_analytics_errors)
    app.get("/api/analytics/daily")(api_analytics_daily)
    app.post("/api/analytics/reset")(api_reset_analytics)
