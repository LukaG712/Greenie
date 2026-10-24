"""Route module: webhooks."""

import json
import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import (
    _notification_channels,
    _notification_channels_lock,
    _scheduled_reports,
    _scheduled_reports_lock,
    _verify_token,
    _webhook_secrets,
    _webhook_secrets_lock,
    _webhooks,
    _webhooks_lock,
)

logger = logging.getLogger(__name__)

# Event types that can trigger webhooks
_EVENT_TYPES = {
    "run.completed", "run.failed", "run.started",
    "schedule.executed", "schedule.failed",
    "pipeline.completed", "pipeline.failed",
    "batch.completed", "batch.failed",
    "agent.created", "agent.deleted",
    "session.created", "session.deleted",
    "approval.pending", "approval.approved", "approval.denied",
}


async def api_create_webhook(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Register a webhook endpoint for event notifications.

    Example body:
    {
        "url": "https://your-server.com/webhook",
        "events": ["run.completed", "run.failed"],
        "secret": "your-hmac-secret",
        "enabled": true
    }

    Use "*" for events to receive all event types.
    """
    url = body.get("url", "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="url is required")

    events = body.get("events", ["*"])
    if not isinstance(events, list) or not events:
        raise HTTPException(status_code=400, detail="events must be a non-empty list")

    # Validate event types
    for ev in events:
        if ev != "*" and ev not in _EVENT_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown event type: {ev}. Available: {list(_EVENT_TYPES)}",
            )

    webhook_id = f"wh-{uuid.uuid4().hex[:8]}"
    webhook_data = {
        "id": webhook_id,
        "url": url,
        "events": events,
        "secret": body.get("secret", ""),
        "enabled": body.get("enabled", True),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "delivery_count": 0,
        "last_delivery": None,
        "last_error": None,
    }

    with _webhooks_lock:
        _webhooks[webhook_id] = webhook_data
    # Persist to database
    try:
        feature_store.save_webhook(webhook_data)
    except Exception as e:
        logger.warning(f"Failed to save webhook to database: {e}")

    # Return without secret
    result = {k: v for k, v in webhook_data.items() if k != "secret"}
    result["has_secret"] = bool(webhook_data.get("secret"))
    return result


async def api_list_webhooks(_auth: None = Depends(_verify_token)):
    """List all registered webhooks."""
    with _webhooks_lock:
        webhooks = list(_webhooks.values())
    # Strip secrets from response
    for wh in webhooks:
        wh["has_secret"] = bool(wh.pop("secret", ""))
    return {"webhooks": webhooks}


async def api_get_webhook(webhook_id: str, _auth: None = Depends(_verify_token)):
    """Get a webhook by ID."""
    with _webhooks_lock:
        webhook = _webhooks.get(webhook_id)
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    result = {k: v for k, v in webhook.items() if k != "secret"}
    result["has_secret"] = bool(webhook.get("secret"))
    return result


async def api_update_webhook(webhook_id: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Update a webhook configuration."""
    with _webhooks_lock:
        webhook = _webhooks.get(webhook_id)
        if not webhook:
            raise HTTPException(status_code=404, detail="Webhook not found")
        if "url" in body:
            webhook["url"] = body["url"].strip()
        if "events" in body:
            events = body["events"]
            if not isinstance(events, list) or not events:
                raise HTTPException(status_code=400, detail="events must be a non-empty list")
            for ev in events:
                if ev != "*" and ev not in _EVENT_TYPES:
                    raise HTTPException(status_code=400, detail=f"Unknown event type: {ev}")
            webhook["events"] = events
        if "secret" in body:
            webhook["secret"] = body["secret"]
        if "enabled" in body:
            webhook["enabled"] = body["enabled"]
        # Persist to database
        try:
            feature_store.save_webhook(webhook)
        except Exception as e:
            logger.warning(f"Failed to save webhook update to database: {e}")
        return {k: v for k, v in webhook.items() if k != "secret"}


async def api_delete_webhook(webhook_id: str, _auth: None = Depends(_verify_token)):
    """Delete a webhook."""
    with _webhooks_lock:
        if webhook_id not in _webhooks:
            raise HTTPException(status_code=404, detail="Webhook not found")
        del _webhooks[webhook_id]
    # Persist to database
    try:
        feature_store.delete_webhook(webhook_id)
    except Exception as e:
        logger.warning(f"Failed to delete webhook from database: {e}")
    return {"status": "ok", "message": "Webhook deleted"}


async def api_test_webhook(webhook_id: str, _auth: None = Depends(_verify_token)):
    """Send a test event to a webhook to verify it works."""
    import httpx

    with _webhooks_lock:
        webhook = _webhooks.get(webhook_id)
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")

    test_payload = {
        "event": "webhook.test",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {"message": "This is a test webhook delivery", "webhook_id": webhook_id},
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            headers = {"Content-Type": "application/json"}
            if webhook.get("secret"):
                import hashlib
                import hmac
                body_str = json.dumps(test_payload)
                sig = hmac.new(
                    webhook["secret"].encode(),
                    body_str.encode(),
                    hashlib.sha256,
                ).hexdigest()
                headers["X-Webhook-Signature"] = sig
            resp = await client.post(webhook["url"], json=test_payload, headers=headers)
            return {
                "status": "delivered",
                "status_code": resp.status_code,
                "response": resp.text[:500],
            }
    except Exception as e:
        return {"status": "failed", "error": str(e)}


async def api_list_event_types(_auth: None = Depends(_verify_token)):
    """List all available event types for webhooks."""
    return {"event_types": list(_EVENT_TYPES)}


# ─── Notification Channels ──────────────────────────────────────────────────

# In-memory notification channel registry
_notification_channels: Dict[str, Dict[str, Any]] = {}
_notification_channels_lock = threading.Lock()


async def _send_notification(channel_id: str, title: str, message: str, level: str = "info") -> bool:
    """Send a notification to a specific channel."""
    import httpx

    with _notification_channels_lock:
        channel = _notification_channels.get(channel_id)
    if not channel or not channel.get("enabled", True):
        return False

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            ch_type = channel.get("type", "").lower()

            if ch_type == "slack":
                payload = {"text": f"*{title}*\n{message}"}
                if level == "error":
                    payload["attachments"] = [{"color": "danger", "text": message}]
                elif level == "warning":
                    payload["attachments"] = [{"color": "warning", "text": message}]
                resp = await client.post(channel["webhook_url"], json=payload)
            elif ch_type == "discord":
                payload = {
                    "content": None,
                    "embeds": [{
                        "title": title,
                        "description": message[:2000],
                        "color": {"error": 0xFF0000, "warning": 0xFFAA00, "success": 0x00FF00}.get(level, 0x0099FF),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }],
                }
                resp = await client.post(channel["webhook_url"], json=payload)
            elif ch_type == "generic":
                payload = {"title": title, "message": message, "level": level, "timestamp": datetime.now(timezone.utc).isoformat()}
                resp = await client.post(channel["webhook_url"], json=payload)
            else:
                return False

            return resp.status_code in (200, 201, 204)
    except Exception:
        return False


async def _broadcast_notification(title: str, message: str, level: str = "info") -> List[Dict[str, Any]]:
    """Broadcast a notification to all enabled channels."""
    with _notification_channels_lock:
        channels = list(_notification_channels.values())

    results = []
    for ch in channels:
        success = await _send_notification(ch["id"], title, message, level)
        results.append({"channel": ch["id"], "type": ch["type"], "success": success})
    return results


async def api_list_webhook_configs(_auth: None = Depends(_verify_token)):
    """List all webhook configurations (secrets redacted)."""
    with _webhook_secrets_lock:
        configs = {}
        for name, cfg in _webhook_secrets.items():
            configs[name] = {
                "pipelines": cfg.get("pipelines", []),
                "has_secret": bool(cfg.get("secret")),
            }
    return configs


# ─── Scheduled Reports Endpoints ─────────────────────────────────────────────

# In-memory store for scheduled reports
_scheduled_reports: Dict[str, Dict[str, Any]] = {}
_scheduled_reports_lock = threading.Lock()



def register(app):
    """Register webhooks routes with the FastAPI app."""
    app.post("/api/webhooks", status_code=201)(api_create_webhook)
    app.get("/api/webhooks")(api_list_webhooks)
    app.get("/api/webhooks/{webhook_id}")(api_get_webhook)
    app.put("/api/webhooks/{webhook_id}")(api_update_webhook)
    app.delete("/api/webhooks/{webhook_id}")(api_delete_webhook)
    app.post("/api/webhooks/{webhook_id}/test")(api_test_webhook)
    app.get("/api/events/types")(api_list_event_types)
    app.get("/api/webhooks/config")(api_list_webhook_configs)
