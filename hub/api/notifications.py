"""Route module: notifications."""

import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _notification_channels, _notification_channels_lock, _verify_token

logger = logging.getLogger(__name__)


async def _send_notification(channel_id: str, title: str, message: str, level: str = "info") -> bool:
    """Send a notification to a specific channel."""
    with _notification_channels_lock:
        channel = _notification_channels.get(channel_id)
    if not channel or not channel.get("enabled", True):
        return False

    try:
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as client:
            payload = {"title": title, "message": message, "level": level}
            resp = await client.post(channel["webhook_url"], json=payload)
            return resp.status_code in (200, 201, 204)
    except Exception:
        return False


async def _broadcast_notification(title: str, message: str, level: str = "info") -> list[dict[str, Any]]:
    """Broadcast a notification to all enabled channels."""
    with _notification_channels_lock:
        channels = list(_notification_channels.values())

    results = []
    for ch in channels:
        success = await _send_notification(ch["id"], title, message, level)
        results.append({"channel": ch["id"], "type": ch["type"], "success": success})
    return results


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


def register(app):
    """Register notifications routes with the FastAPI app."""
    app.post("/api/notifications/channels", status_code=201)(api_create_notification_channel)
    app.get("/api/notifications/channels")(api_list_notification_channels)
    app.get("/api/notifications/channels/{channel_id}")(api_get_notification_channel)
    app.put("/api/notifications/channels/{channel_id}")(api_update_notification_channel)
    app.delete("/api/notifications/channels/{channel_id}")(api_delete_notification_channel)
    app.post("/api/notifications/test")(api_test_notification)
    app.post("/api/notifications/broadcast")(api_broadcast_notification)
