"""Route module: notifications."""

import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _analytics, _analytics_lock, _notification_channels, _notification_channels_lock, _verify_token

logger = logging.getLogger(__name__)


def _send_notification(channel_id: str, message: str) -> None:
    """Send a notification to a channel."""
    pass  # Placeholder


def _broadcast_notification(message: str) -> None:
    """Broadcast a notification to all channels."""
    pass  # Placeholder


async def api_create_notification_channel(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Register a notification channel (Slack, Discord, or generic webhook).

    Example body:
    {
        "name": "DevOps Alerts",
        "type": "slack",
        "webhook_url": "https://hooks.slack.com/services/...",
        "enabled": true
    }
    """
    name = body.get("name", "").strip()
    ch_type = body.get("type", "").lower()
    webhook_url = body.get("webhook_url", "").strip()

    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    if ch_type not in ("slack", "discord", "generic"):
        raise HTTPException(status_code=400, detail="type must be 'slack', 'discord', or 'generic'")
    if not webhook_url:
        raise HTTPException(status_code=400, detail="webhook_url is required")

    channel_id = f"ch-{uuid.uuid4().hex[:8]}"
    channel_data = {
        "id": channel_id,
        "name": name,
        "type": ch_type,
        "webhook_url": webhook_url,
        "enabled": body.get("enabled", True),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    with _notification_channels_lock:
        _notification_channels[channel_id] = channel_data
    # Persist to database
    try:
        feature_store.save_notification_channel(channel_data)
    except Exception as e:
        logger.warning(f"Failed to save notification channel: {e}")

    # Return without webhook_url
    return {
        "id": channel_id,
        "name": name,
        "type": ch_type,
        "enabled": channel_data["enabled"],
        "has_webhook_url": True,
    }


async def api_list_notification_channels(_auth: None = Depends(_verify_token)):
    """List all notification channels."""
    with _notification_channels_lock:
        channels = list(_notification_channels.values())
    for ch in channels:
        ch["has_webhook_url"] = bool(ch.get("webhook_url", ""))
        ch.pop("webhook_url", None)
    return {"channels": channels}


async def api_get_notification_channel(channel_id: str, _auth: None = Depends(_verify_token)):
    """Get a notification channel by ID."""
    with _notification_channels_lock:
        channel = _notification_channels.get(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    result = dict(channel)
    result["has_webhook_url"] = bool(result.pop("webhook_url", ""))
    return result


async def api_update_notification_channel(channel_id: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Update a notification channel."""
    with _notification_channels_lock:
        channel = _notification_channels.get(channel_id)
        if not channel:
            raise HTTPException(status_code=404, detail="Channel not found")
        if "name" in body:
            channel["name"] = body["name"].strip()
        if "webhook_url" in body:
            channel["webhook_url"] = body["webhook_url"].strip()
        if "enabled" in body:
            channel["enabled"] = body["enabled"]
        # Persist to database
        try:
            feature_store.save_notification_channel(channel)
        except Exception as e:
            logger.warning(f"Failed to save notification channel update: {e}")
        return {
            "id": channel_id,
            "name": channel["name"],
            "type": channel["type"],
            "enabled": channel["enabled"],
            "has_webhook_url": True,
        }


async def api_delete_notification_channel(channel_id: str, _auth: None = Depends(_verify_token)):
    """Delete a notification channel."""
    with _notification_channels_lock:
        if channel_id not in _notification_channels:
            raise HTTPException(status_code=404, detail="Channel not found")
        del _notification_channels[channel_id]
    # Persist to database
    try:
        feature_store.delete_notification_channel(channel_id)
    except Exception as e:
        logger.warning(f"Failed to delete notification channel: {e}")
    return {"status": "ok", "message": "Channel deleted"}


async def api_test_notification(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Send a test notification to a channel."""
    channel_id = body.get("channel_id", "")
    if not channel_id:
        raise HTTPException(status_code=400, detail="channel_id is required")

    title = body.get("title", "Test Notification")
    message = body.get("message", "This is a test notification from Greenie.")
    level = body.get("level", "info")

    success = await _send_notification(channel_id, title, message, level)
    return {"success": success, "channel_id": channel_id}


async def api_broadcast_notification(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Broadcast a notification to all enabled channels."""
    title = body.get("title", "Notification")
    message = body.get("message", "")
    level = body.get("level", "info")

    if not message:
        raise HTTPException(status_code=400, detail="message is required")

    results = await _broadcast_notification(title, message, level)
    return {"results": results}


# ─── Analytics & Usage Tracking ─────────────────────────────────────────────

# In-memory analytics store
_analytics: Dict[str, Any] = {
    "api_calls": {},           # endpoint -> count
    "task_durations": [],      # list of (service, duration_ms, status)
    "runs_by_agent": {},       # agent -> count
    "errors_by_type": {},      # error_type -> count
    "daily_stats": {},         # date -> {calls, tasks, errors}
}
_analytics_lock = threading.Lock()


def _track_api_call(endpoint: str) -> None:
    """Track an API call for analytics."""
    with _analytics_lock:
        _analytics["api_calls"][endpoint] = _analytics["api_calls"].get(endpoint, 0) + 1
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today not in _analytics["daily_stats"]:
            _analytics["daily_stats"][today] = {"calls": 0, "tasks": 0, "errors": 0}
        _analytics["daily_stats"][today]["calls"] += 1
    # Persist to database
    try:
        feature_store.track_api_call(endpoint)
        feature_store.update_daily_stats(today, "Greenie", calls=1)
    except Exception as e:
        logger.warning(f"Failed to persist analytics: {e}")


def _track_task_duration(service: str, duration_ms: float, status: str) -> None:
    """Track a task's execution duration."""
    with _analytics_lock:
        _analytics["task_durations"].append({
            "service": service,
            "duration_ms": duration_ms,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        # Keep only last 1000 entries
        if len(_analytics["task_durations"]) > 1000:
            _analytics["task_durations"] = _analytics["task_durations"][-1000:]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today not in _analytics["daily_stats"]:
            _analytics["daily_stats"][today] = {"calls": 0, "tasks": 0, "errors": 0}
        _analytics["daily_stats"][today]["tasks"] += 1
        if status == "error":
            _analytics["daily_stats"][today]["errors"] += 1
    # Persist to database
    try:
        feature_store.track_task_duration(service, duration_ms, status)
        errors = 1 if status == "error" else 0
        feature_store.update_daily_stats(today, service, tasks=1, errors=errors)
    except Exception as e:
        logger.warning(f"Failed to persist task duration: {e}")


def _track_run(agent: str) -> None:
    """Track a run for analytics."""
    with _analytics_lock:
        _analytics["runs_by_agent"][agent] = _analytics["runs_by_agent"].get(agent, 0) + 1


def _track_error(error_type: str) -> None:
    """Track an error for analytics."""
    with _analytics_lock:
        _analytics["errors_by_type"][error_type] = _analytics["errors_by_type"].get(error_type, 0) + 1



def register(app):
    """Register notifications routes with the FastAPI app."""
    app.post("/api/notifications/channels", status_code=201)(api_create_notification_channel)
    app.get("/api/notifications/channels")(api_list_notification_channels)
    app.get("/api/notifications/channels/{channel_id}")(api_get_notification_channel)
    app.put("/api/notifications/channels/{channel_id}")(api_update_notification_channel)
    app.delete("/api/notifications/channels/{channel_id}")(api_delete_notification_channel)
    app.post("/api/notifications/test")(api_test_notification)
    app.post("/api/notifications/broadcast")(api_broadcast_notification)
