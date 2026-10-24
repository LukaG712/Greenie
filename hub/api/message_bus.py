"""Route module: message_bus."""

import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _feature_flags, _feature_flags_lock, _message_bus, _message_bus_lock, _verify_token

logger = logging.getLogger(__name__)

# Message subscribers
_message_subscribers: Dict[str, List[Dict[str, Any]]] = {}
_message_subscribers_lock = threading.Lock()


async def _publish_message(topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Publish a message to a topic."""
    message = {
        "id": f"msg-{uuid.uuid4().hex[:8]}",
        "topic": topic,
        "payload": payload,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with _message_bus_lock:
        if topic not in _message_bus:
            _message_bus[topic] = []
        _message_bus[topic].append(message)
    return {"message": message}


async def api_publish_message(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Publish a message to a topic on the internal message bus.

    Example body:
    {
        "topic": "events.user.created",
        "payload": {"user_id": "u-123", "name": "Alice"}
    }
    """
    topic = body.get("topic", "").strip()
    if not topic:
        raise HTTPException(status_code=400, detail="topic is required")

    payload = body.get("payload", {})
    result = await _publish_message(topic, payload)
    return result


async def api_list_topics(_auth: None = Depends(_verify_token)):
    """List all topics with message counts."""
    with _message_bus_lock:
        topics = {}
        for topic, messages in _message_bus.items():
            topics[topic] = {
                "message_count": len(messages),
                "latest": messages[-1]["timestamp"] if messages else None,
            }
    return {"topics": topics}


async def api_get_messages(
    topic: str,
    limit: int = 50,
    offset: int = 0,
    _auth: None = Depends(_verify_token),
):
    """Get messages from a specific topic.

    Query parameters:
    - limit: Max messages to return (default 50, max 500)
    - offset: Number of messages to skip from the end
    """
    limit = min(limit, 500)
    with _message_bus_lock:
        messages = _message_bus.get(topic, [])

    # Return most recent first
    messages = messages[offset:]
    messages = messages[-limit:]
    messages.reverse()

    return {"topic": topic, "messages": messages, "count": len(messages)}


async def api_subscribe_topic(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Subscribe to a topic via webhook notifications.

    Example body:
    {
        "topic": "events.*",
        "webhook_url": "https://your-server.com/messages"
    }

    Use wildcard patterns like 'events.*' to match all sub-topics.
    """
    topic = body.get("topic", "").strip()
    if not topic:
        raise HTTPException(status_code=400, detail="topic is required")

    webhook_url = body.get("webhook_url", "").strip()
    if not webhook_url:
        raise HTTPException(status_code=400, detail="webhook_url is required")

    subscriber_id = f"sub-{uuid.uuid4().hex[:8]}"
    subscriber = {
        "id": subscriber_id,
        "topic": topic,
        "url": webhook_url,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "delivery_count": 0,
    }

    with _message_subscribers_lock:
        if topic not in _message_subscribers:
            _message_subscribers[topic] = []
        _message_subscribers[topic].append(subscriber)

    # Persist to database
    try:
        feature_store.save_message_subscription(subscriber)
    except Exception as e:
        logger.warning(f"Failed to save message subscription: {e}")

    return {"id": subscriber_id, "topic": topic}


async def api_list_subscriptions(_auth: None = Depends(_verify_token)):
    """List all topic subscriptions."""
    with _message_subscribers_lock:
        all_subs = []
        for topic, subs in _message_subscribers.items():
            all_subs.extend(subs)
    return {"subscriptions": all_subs}


async def api_unsubscribe(subscriber_id: str, _auth: None = Depends(_verify_token)):
    """Remove a topic subscription."""
    with _message_subscribers_lock:
        for topic, subs in _message_subscribers.items():
            for i, sub in enumerate(subs):
                if sub["id"] == subscriber_id:
                    subs.pop(i)
                    # Persist to database
                    try:
                        feature_store.delete_message_subscription(subscriber_id)
                    except Exception as e:
                        logger.warning(f"Failed to delete message subscription: {e}")
                    return {"status": "ok", "message": "Subscription removed"}
    raise HTTPException(status_code=404, detail="Subscription not found")


async def api_clear_messages(
    topic: Optional[str] = None,
    _auth: None = Depends(_verify_token),
):
    """Clear messages from a topic or all topics."""
    with _message_bus_lock:
        if topic:
            if topic in _message_bus:
                count = len(_message_bus[topic])
                _message_bus[topic].clear()
                return {"status": "ok", "cleared": count}
            return {"status": "ok", "cleared": 0}
        else:
            total = sum(len(msgs) for msgs in _message_bus.values())
            _message_bus.clear()
            return {"status": "ok", "cleared": total}


# ─── Feature Flags ──────────────────────────────────────────────────────────

# Feature flag store: flag_name -> {enabled, description, rules, created_at}
_feature_flags: Dict[str, Dict[str, Any]] = {}
_feature_flags_lock = threading.Lock()

# Flag evaluation history
_flag_evaluations: List[Dict[str, Any]] = []
_flag_evaluations_lock = threading.Lock()


def _evaluate_flag(flag_name: str, context: Optional[Dict[str, Any]] = None) -> bool:
    """Evaluate a feature flag for the given context."""
    with _feature_flags_lock:
        flag = _feature_flags.get(flag_name)
    if not flag:
        return False

    # Global override
    if not flag.get("enabled", True):
        return False

    context = context or {}

    # Check target rules
    rules = flag.get("rules", [])
    if not rules:
        return True

    for rule in rules:
        rule_type = rule.get("type", "")

        if rule_type == "user":
            user_id = context.get("user_id", "")
            target_users = rule.get("users", [])
            if target_users and user_id not in target_users:
                continue
            return rule.get("enabled", True)

        elif rule_type == "agent":
            agent_name = context.get("agent", "")
            target_agents = rule.get("agents", [])
            if target_agents and agent_name not in target_agents:
                continue
            return rule.get("enabled", True)

        elif rule_type == "percentage":
            import hashlib
            percentage = rule.get("percentage", 100)
            identifier = context.get("user_id", context.get("agent", "default"))
            hash_val = int(hashlib.md5(f"{flag_name}:{identifier}".encode()).hexdigest(), 16)
            if (hash_val % 100) < percentage:
                return rule.get("enabled", True)
            continue

        elif rule_type == "environment":
            env = context.get("environment", os.environ.get("ENVIRONMENT", "development"))
            target_envs = rule.get("environments", [])
            if target_envs and env not in target_envs:
                continue
            return rule.get("enabled", True)

    # No matching rule found
    return flag.get("default_value", False)



def register(app):
    """Register message_bus routes with the FastAPI app."""
    app.post("/api/messages/publish", status_code=201)(api_publish_message)
    app.get("/api/messages/topics")(api_list_topics)
    app.get("/api/messages/{topic}")(api_get_messages)
    app.post("/api/messages/subscribe", status_code=201)(api_subscribe_topic)
    app.get("/api/messages/subscriptions")(api_list_subscriptions)
    app.delete("/api/messages/subscriptions/{subscriber_id}")(api_unsubscribe)
    app.post("/api/messages/clear")(api_clear_messages)
