"""Route module: cache."""

import json
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import Depends, HTTPException

from hub.api.core import _message_bus, _message_bus_lock, _response_cache, _response_cache_lock, _verify_token
from hub.config import load_config, save_config, set_primary_bot, validate_name

# Cache TTL configuration
_cache_ttl_config: Dict[str, int] = {}
_cache_ttl_lock = threading.Lock()


async def api_cache_stats(_auth: None = Depends(_verify_token)):
    """Get cache usage statistics."""
    now = time.time()
    with _response_cache_lock:
        total = len(_response_cache)
        expired = sum(1 for v in _response_cache.values() if v["expires_at"] < now)
        active = total - expired
        total_hits = sum(v["hits"] for v in _response_cache.values())

    return {
        "total_entries": total,
        "active_entries": active,
        "expired_entries": expired,
        "total_hits": total_hits,
        "hit_rate": round(total_hits / max(active, 1), 2),
    }


async def api_cache_clear(
    endpoint: Optional[str] = None,
    _auth: None = Depends(_verify_token),
):
    """Clear cached responses.

    If endpoint is provided, only clear entries matching that endpoint prefix.
    Otherwise, clear the entire cache.
    """
    with _response_cache_lock:
        if endpoint:
            keys_to_remove = [k for k in _response_cache if endpoint in k]
            for k in keys_to_remove:
                del _response_cache[k]
            return {"status": "ok", "cleared": len(keys_to_remove)}
        else:
            count = len(_response_cache)
            _response_cache.clear()
            return {"status": "ok", "cleared": count}


async def api_cache_entries(_auth: None = Depends(_verify_token)):
    """List all active cache entries with metadata."""
    now = time.time()
    with _response_cache_lock:
        entries = []
        for key, val in _response_cache.items():
            if val["expires_at"] >= now:
                ttl_remaining = int(val["expires_at"] - now)
                entries.append({
                    "key": key,
                    "ttl_remaining": ttl_remaining,
                    "hits": val["hits"],
                    "created_at": val["created_at"],
                })
    return {"entries": entries, "count": len(entries)}


async def api_set_cache_ttl(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Set custom TTL for a specific endpoint.

    Example body:
    {
        "endpoint": "/api/dashboard",
        "ttl_seconds": 60
    }
    """
    endpoint = body.get("endpoint", "").strip()
    if not endpoint:
        raise HTTPException(status_code=400, detail="endpoint is required")

    ttl = body.get("ttl_seconds", 60)
    if ttl < 0:
        raise HTTPException(status_code=400, detail="ttl_seconds must be non-negative")

    with _cache_ttl_lock:
        if ttl == 0:
            _cache_ttl_config.pop(endpoint, None)
        else:
            _cache_ttl_config[endpoint] = ttl

    return {"status": "ok", "endpoint": endpoint, "ttl_seconds": ttl}


async def api_get_cache_ttl(_auth: None = Depends(_verify_token)):
    """Get current cache TTL configuration for all endpoints."""
    with _cache_ttl_lock:
        return {"ttl_config": dict(_cache_ttl_config)}


async def api_delete_cache_ttl(endpoint: str, _auth: None = Depends(_verify_token)):
    """Remove custom TTL for an endpoint (reverts to no caching)."""
    with _cache_ttl_lock:
        _cache_ttl_config.pop(endpoint, None)
    return {"status": "ok", "message": f"TTL config removed for {endpoint}"}


# ─── Internal Message Bus (Pub/Sub) ─────────────────────────────────────────

# Topic -> list of message entries
_message_bus: Dict[str, List[Dict[str, Any]]] = {}
_message_bus_lock = threading.Lock()

# Topic -> list of subscriber callbacks (webhook URLs)
_message_subscribers: Dict[str, List[Dict[str, Any]]] = {}
_message_subscribers_lock = threading.Lock()

# Max messages per topic
_MAX_MESSAGES_PER_TOPIC = 500


async def _publish_message(topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Publish a message to a topic and notify subscribers."""
    import httpx

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
        # Bounded: keep last N messages per topic
        if len(_message_bus[topic]) > _MAX_MESSAGES_PER_TOPIC:
            _message_bus[topic] = _message_bus[topic][-_MAX_MESSAGES_PER_TOPIC:]

    # Notify subscribers
    with _message_subscribers_lock:
        subscribers = list(_message_subscribers.get(topic, []))
        # Also check wildcard subscribers
        for wild_topic, wild_subs in _message_subscribers.items():
            if wild_topic.endswith("*"):
                base = wild_topic[:-1]
                if topic.startswith(base):
                    subscribers.extend(wild_subs)

    delivery_results = []
    if subscribers:
        async with httpx.AsyncClient(timeout=10.0) as client:
            for sub in subscribers:
                try:
                    resp = await client.post(sub["url"], json=message)
                    delivery_results.append({
                        "subscriber": sub["id"],
                        "status": "delivered" if resp.status_code in (200, 201) else "failed",
                        "status_code": resp.status_code,
                    })
                except Exception as e:
                    delivery_results.append({
                        "subscriber": sub["id"],
                        "status": "error",
                        "error": str(e),
                    })

    return {"message": message, "deliveries": delivery_results}



def register(app):
    """Register cache routes with the FastAPI app."""
    app.get("/api/cache/stats")(api_cache_stats)
    app.post("/api/cache/clear")(api_cache_clear)
    app.get("/api/cache/entries")(api_cache_entries)
    app.post("/api/cache/ttl")(api_set_cache_ttl)
    app.get("/api/cache/ttl")(api_get_cache_ttl)
    app.delete("/api/cache/ttl/{endpoint:path}")(api_delete_cache_ttl)
