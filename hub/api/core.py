"""Core infrastructure for the Greenie API.

This module contains shared infrastructure used by all route modules:
- In-memory stores with thread-safe locks
- SSE event store for streaming responses
- Authentication helpers
- App lifespan (startup/shutdown)
- Middleware (CORS, logging, metrics)
- Background tasks (scheduled pipelines, job queue)

Route modules import from this module to access stores and helpers.
"""

import asyncio
import contextlib
import hmac
import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from hub import agents, coordinator, feature_store, memory, obsidian, ollama, providers, remote, skills
from hub import routines as routines_engine
from hub.config import load_config, save_config, set_primary_bot, validate_name
from hub.gobber_client import gobber_client
from hub.logging_config import RequestIDFilter, get_logger, setup_structured_logging
from hub.runner import (
    Run,
    agent_status,
    get_run,
    list_runs,
    recover_stale_runs,
    run_agent,
    stop_run,
)

# Observability: tracing and metrics (optional, graceful fallback if deps missing)
try:
    from hub import tracing as tracing_module
except ImportError:
    tracing_module = None
try:
    from hub import metrics as metrics_module
except ImportError:
    metrics_module = None
try:
    from hub import plugins as plugins_module
except ImportError:
    plugins_module = None
try:
    from hub import cluster as cluster_module
except ImportError:
    cluster_module = None
try:
    from hub import marketplace as marketplace_module
except ImportError:
    marketplace_module = None

# Initialize structured logging
log_level = os.environ.get("LOG_LEVEL", "INFO")
setup_structured_logging(level=log_level)
logger = get_logger(__name__)

security = HTTPBearer(auto_error=False)

# ─── Static files directory ─────────────────────────────────────────────────

_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

# ─── Rate limiting for authentication failures ──────────────────────────────

_auth_failure_store: Dict[str, List[float]] = {}
_auth_failure_lock = threading.Lock()
_AUTH_MAX_FAILURES = 10
_AUTH_WINDOW_SECONDS = 60


def _check_auth_rate_limit(client_ip: str) -> None:
    """Rate limit authentication failures per IP address."""
    now = time.time()
    with _auth_failure_lock:
        if client_ip not in _auth_failure_store:
            _auth_failure_store[client_ip] = []
        failures = _auth_failure_store[client_ip]
        _auth_failure_store[client_ip] = [t for t in failures if now - t < _AUTH_WINDOW_SECONDS]
        failures = _auth_failure_store[client_ip]
        if len(failures) >= _AUTH_MAX_FAILURES:
            raise HTTPException(
                status_code=429,
                detail="Too many authentication failures. Try again later."
            )


def _record_auth_failure(client_ip: str) -> None:
    """Record an authentication failure for rate limiting."""
    now = time.time()
    with _auth_failure_lock:
        if client_ip not in _auth_failure_store:
            _auth_failure_store[client_ip] = []
        _auth_failure_store[client_ip].append(now)


def _cleanup_auth_failure_store() -> None:
    """Periodically clean up stale entries from the auth failure store to prevent memory leaks."""
    now = time.time()
    with _auth_failure_lock:
        stale_ips = []
        for ip, timestamps in _auth_failure_store.items():
            _auth_failure_store[ip] = [t for t in timestamps if now - t < _AUTH_WINDOW_SECONDS]
            if not _auth_failure_store[ip]:
                stale_ips.append(ip)
        for ip in stale_ips:
            del _auth_failure_store[ip]


async def _auth_failure_cleanup_task() -> None:
    """Background task that periodically cleans up the auth failure store."""
    while True:
        await asyncio.sleep(60)
        _cleanup_auth_failure_store()


# ─── SSE event store ────────────────────────────────────────────────────────

_sse_event_store: Dict[str, "SSERunStore"] = {}
_sse_event_store_lock = threading.Lock()
_SSE_MAX_EVENTS = 1000
_TERMINAL_EVENTS = {"complete", "error", "end"}
_sse_next_id: int = 0


class SSERunStore:
    """Bounded, append-only event store for one run."""

    __slots__ = ("events", "evicted_before_id")

    def __init__(self) -> None:
        self.events: list = []
        self.evicted_before_id: Optional[int] = None


def _sse_buffer_for_run(run_id: str) -> SSERunStore:
    """Get or create an SSE event store for a run."""
    with _sse_event_store_lock:
        if run_id not in _sse_event_store:
            _sse_event_store[run_id] = SSERunStore()
        return _sse_event_store[run_id]


def _sse_remove_buffer(run_id: str) -> None:
    """Remove an SSE event store after completion."""
    with _sse_event_store_lock:
        _sse_event_store.pop(run_id, None)


def _sse_store_core(run_id: str, event: str, sse_event: str, is_terminal: bool) -> int:
    """Core logic for storing an SSE event. Must be called with lock held.
    
    Raises AssertionError if called without the lock to prevent race conditions.
    """
    global _sse_next_id
    assert _sse_event_store_lock.locked(), "_sse_store_core must be called with _sse_event_store_lock held"
    if run_id not in _sse_event_store:
        _sse_event_store[run_id] = SSERunStore()
    store = _sse_event_store[run_id]

    _sse_next_id += 1
    event_id = _sse_next_id

    if len(store.events) >= _SSE_MAX_EVENTS:
        if is_terminal:
            removed = False
            for i in range(len(store.events)):
                ev_id, ev_str = store.events[i]
                if not any(f"event: {t}" in ev_str for t in _TERMINAL_EVENTS):
                    oldest_evicted = ev_id
                    store.events.pop(i)
                    removed = True
                    if store.evicted_before_id is None or oldest_evicted > store.evicted_before_id:
                        store.evicted_before_id = oldest_evicted
                    break
            if not removed and store.events:
                oldest_evicted = store.events[0][0]
                store.events.pop(0)
                if store.evicted_before_id is None or oldest_evicted > store.evicted_before_id:
                    store.evicted_before_id = oldest_evicted
        else:
            oldest_evicted = store.events[0][0]
            store.events.pop(0)
            if store.evicted_before_id is None or oldest_evicted > store.evicted_before_id:
                store.evicted_before_id = oldest_evicted

    store.events.append((event_id, sse_event))
    return event_id


def _sse_send_event(run_id: str, event: str, data: str) -> None:
    """Send an SSE event to the store for a run.

    Uses a single threading.Lock for both async and sync contexts to prevent
    concurrent corruption from dual-lock design.
    """
    sse_event = f"event: {event}\ndata: {data}\n\n"
    is_terminal = event in _TERMINAL_EVENTS

    with _sse_event_store_lock:
        _sse_store_core(run_id, event, sse_event, is_terminal)


# ─── Authentication helpers ─────────────────────────────────────────────────

def _get_api_key() -> str:
    key = os.environ.get("HUB_API_KEY", "")
    if not key:
        raise RuntimeError("HUB_API_KEY environment variable is not set. Refusing to start.")
    return key


def _verify_token(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> None:
    client_ip = request.client.host if request else "unknown"
    _check_auth_rate_limit(client_ip)
    if credentials is None:
        _record_auth_failure(client_ip)
        raise HTTPException(status_code=401, detail="Authorization header missing")
    expected = _get_api_key()
    if not hmac.compare_digest(credentials.credentials, expected):
        _record_auth_failure(client_ip)
        raise HTTPException(status_code=401, detail="Invalid API key")


def _verify_token_or_query(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> None:
    """Verify token from Authorization header or cookie."""
    client_ip = request.client.host if request else "unknown"
    _check_auth_rate_limit(client_ip)
    expected = _get_api_key()
    if credentials is not None:
        if hmac.compare_digest(credentials.credentials, expected):
            return
    cookie_token = request.cookies.get("hub_api_key") if request else None
    if cookie_token is not None and hmac.compare_digest(cookie_token, expected):
        return
    _record_auth_failure(client_ip)
    raise HTTPException(status_code=401, detail="Invalid API key")


# ─── In-memory stores ───────────────────────────────────────────────────────

# Scheduled pipelines
_scheduled_pipelines: Dict[str, Dict[str, Any]] = {}
_scheduled_pipelines_lock = threading.Lock()
_scheduled_tasks: Dict[str, asyncio.Task] = {}

# Webhooks
_webhooks: Dict[str, Dict[str, Any]] = {}
_webhooks_lock = threading.Lock()

# Notification channels
_notification_channels: Dict[str, Dict[str, Any]] = {}
_notification_channels_lock = threading.Lock()

# Analytics
_analytics: Dict[str, Any] = {
    "api_calls": {},
    "task_durations": [],
    "daily_stats": {},
}
_analytics_lock = threading.Lock()

# API keys
_api_keys: Dict[str, Dict[str, Any]] = {}
_api_keys_lock = threading.Lock()

# Service health
_service_health: Dict[str, Dict[str, Any]] = {}
_service_health_lock = threading.Lock()

# Rate limits
_rate_limits: Dict[str, Dict[str, Any]] = {}
_rate_limits_lock = threading.Lock()

# Audit log
_audit_log: List[Dict[str, Any]] = []
_audit_log_lock = threading.Lock()

# Templates
_templates: Dict[str, Dict[str, Any]] = {}
_templates_lock = threading.Lock()

# Approval workflows
_approval_policies: Dict[str, Dict[str, Any]] = {}
_approval_policies_lock = threading.Lock()
_approval_requests: Dict[str, Dict[str, Any]] = {}
_approval_requests_lock = threading.Lock()

# RBAC
_rbac_roles: Dict[str, Dict[str, Any]] = {}
_rbac_roles_lock = threading.Lock()
_rbac_users: Dict[str, Dict[str, Any]] = {}
_rbac_users_lock = threading.Lock()

# Background jobs
_background_jobs: Dict[str, Dict[str, Any]] = {}
_background_jobs_lock = threading.Lock()

# Response cache
_response_cache: Dict[str, Dict[str, Any]] = {}
_response_cache_lock = threading.Lock()

# Message bus
_message_bus: Dict[str, List[Dict[str, Any]]] = {}
_message_bus_lock = threading.Lock()
_message_subscriptions: Dict[str, List[Dict[str, Any]]] = {}
_message_subscriptions_lock = threading.Lock()

# Feature flags
_feature_flags: Dict[str, Dict[str, Any]] = {}
_feature_flags_lock = threading.Lock()

# Deployments
_deployments: Dict[str, Dict[str, Any]] = {}
_deployments_lock = threading.Lock()

# Circuit breakers
_circuit_breakers: Dict[str, Dict[str, Any]] = {}
_circuit_breakers_lock = threading.Lock()

# API versions
_api_versions: Dict[str, Dict[str, Any]] = {}
_api_versions_lock = threading.Lock()

# DAG definitions
_dag_definitions: Dict[str, Dict[str, Any]] = {}
_dag_definitions_lock = threading.Lock()

# Multi-tenancy
_tenants: Dict[str, Dict[str, Any]] = {}
_tenants_lock = threading.Lock()
_tenant_resources: Dict[str, List[str]] = {}
_tenant_resources_lock = threading.Lock()

# Scheduled reports
_scheduled_reports: Dict[str, Dict[str, Any]] = {}
_scheduled_reports_lock = threading.Lock()

# Webhook configs (GitHub/GitLab)
_webhook_secrets: Dict[str, Dict[str, Any]] = {}
_webhook_secrets_lock = threading.Lock()

# File storage metadata
_file_storage: Dict[str, Dict[str, Any]] = {}
_file_storage_lock = threading.Lock()


# ─── Hybrid stores (Gobber-first with local fallback) ───────────────────────
# These async helpers try Gobber first, then fall back to local in-memory
# stores when Gobber is unavailable. This allows Greenie to operate
# independently while preferring persistent storage when available.

async def hybrid_cache_get(key: str) -> Optional[Any]:
    """Get from cache: try Gobber first, then local."""
    result = await gobber_client.get_cache(key)
    if result is not None:
        return result
    with _response_cache_lock:
        entry = _response_cache.get(key)
        if entry:
            return entry.get("value")
    return None


async def hybrid_cache_set(key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
    """Set cache: try Gobber, always update local."""
    await gobber_client.set_cache(key, value, ttl_seconds)
    now = time.time()
    with _response_cache_lock:
        _response_cache[key] = {
            "value": value,
            "expires_at": now + (ttl_seconds or 300),
            "hits": 0,
            "created_at": now,
        }


async def hybrid_cache_delete(key: str) -> None:
    """Delete from cache: try Gobber, always update local."""
    await gobber_client.delete_cache(key)
    with _response_cache_lock:
        _response_cache.pop(key, None)


async def hybrid_flag_get(name: str) -> Optional[Dict[str, Any]]:
    """Get feature flag: try Gobber first, then local."""
    result = await gobber_client.get_flag(name)
    if result is not None:
        return result
    with _feature_flags_lock:
        return _feature_flags.get(name)


async def hybrid_flag_list() -> list[Dict[str, Any]]:
    """List feature flags: try Gobber first, then local."""
    result = await gobber_client.list_flags()
    if result:
        return result
    with _feature_flags_lock:
        return list(_feature_flags.values())


async def hybrid_flag_set(name: str, is_enabled: bool, description: str = "", targeting: Optional[dict] = None) -> None:
    """Set feature flag: try Gobber, always update local."""
    await gobber_client.set_flag(name, is_enabled, description, targeting)
    with _feature_flags_lock:
        _feature_flags[name] = {
            "name": name,
            "is_enabled": is_enabled,
            "description": description,
            "targeting": targeting,
        }


async def hybrid_flag_delete(name: str) -> None:
    """Delete feature flag: try Gobber, always update local."""
    await gobber_client.delete_flag(name)
    with _feature_flags_lock:
        _feature_flags.pop(name, None)


async def hybrid_webhook_list(active_only: bool = False) -> list[Dict[str, Any]]:
    """List webhooks: try Gobber first, then local."""
    result = await gobber_client.list_webhooks(active_only)
    if result:
        return result
    with _webhooks_lock:
        return list(_webhooks.values())


async def hybrid_webhook_create(name: str, url: str, events: list[str], secret: Optional[str] = None) -> Optional[str]:
    """Create webhook: try Gobber, always update local."""
    webhook_id = await gobber_client.create_webhook(name, url, events, secret)
    with _webhooks_lock:
        local_id = f"wh-{uuid.uuid4().hex[:8]}"
        _webhooks[local_id] = {
            "id": local_id,
            "name": name,
            "url": url,
            "events": events,
            "secret": secret,
            "is_active": True,
        }
    return webhook_id or local_id


async def hybrid_webhook_delete(webhook_id: str) -> None:
    """Delete webhook: try Gobber, always update local."""
    await gobber_client.delete_webhook(webhook_id)
    with _webhooks_lock:
        _webhooks.pop(webhook_id, None)


async def hybrid_analytics_record(event_type: str, properties: Optional[dict] = None, service: Optional[str] = None) -> None:
    """Record analytics event: try Gobber, always update local."""
    await gobber_client.record_event(event_type, properties, service)
    with _analytics_lock:
        if event_type not in _analytics["api_calls"]:
            _analytics["api_calls"][event_type] = 0
        _analytics["api_calls"][event_type] += 1


async def hybrid_analytics_query(event_type: Optional[str] = None, service: Optional[str] = None, limit: int = 100) -> list[Dict[str, Any]]:
    """Query analytics: try Gobber first, then local."""
    result = await gobber_client.query_events(event_type, service, limit)
    if result:
        return result
    with _analytics_lock:
        return list(_analytics.get("api_calls", {}).items())[:limit]


async def hybrid_role_list() -> list[Dict[str, Any]]:
    """List RBAC roles: try Gobber first, then local."""
    result = await gobber_client.list_roles()
    if result:
        return result
    with _rbac_roles_lock:
        return list(_rbac_roles.values())


async def hybrid_role_create(name: str, permissions: list[str], description: str = "") -> Optional[str]:
    """Create role: try Gobber, always update local."""
    role_id = await gobber_client.create_role(name, permissions, description)
    with _rbac_roles_lock:
        local_id = f"role-{uuid.uuid4().hex[:8]}"
        _rbac_roles[local_id] = {
            "id": local_id,
            "name": name,
            "permissions": permissions,
            "description": description,
        }
    return role_id or local_id


async def hybrid_role_delete(role_id: str) -> None:
    """Delete role: try Gobber, always update local."""
    await gobber_client.delete_role(role_id)
    with _rbac_roles_lock:
        _rbac_roles.pop(role_id, None)


async def hybrid_role_assign(user_id: str, role_id: str) -> None:
    """Assign role: try Gobber, always update local."""
    await gobber_client.assign_role(user_id, role_id)
    with _rbac_users_lock:
        if user_id not in _rbac_users:
            _rbac_users[user_id] = []
        _rbac_users[user_id].append(role_id)


async def hybrid_role_revoke(user_id: str, role_id: str) -> None:
    """Revoke role: try Gobber, always update local."""
    await gobber_client.revoke_role(user_id, role_id)
    with _rbac_users_lock:
        if user_id in _rbac_users:
            _rbac_users[user_id] = [r for r in _rbac_users[user_id] if r != role_id]


# ─── CORS configuration ─────────────────────────────────────────────────────

_default_cors_origins = [
    "http://localhost:*",
    "http://127.0.0.1:*",
]


def _get_cors_origins() -> List[str]:
    """Get CORS allowed origins from config, falling back to defaults."""
    try:
        config = load_config()
        cors_cfg = config.get("cors", {})
        if cors_cfg.get("enabled", True):
            origins = cors_cfg.get("origins", _default_cors_origins)
            return origins
    except Exception:
        pass
    return _default_cors_origins


# ─── Startup validation ─────────────────────────────────────────────────────

def _validate_startup() -> Dict[str, Any]:
    """Validate configuration and return a status report."""
    issues: List[Dict[str, Any]] = []
    config = load_config()

    for agent_name, agent_cfg in config.get("agents", {}).items():
        runner_name = agent_cfg.get("runner", "junie")
        provider = providers.get_provider(runner_name)
        if provider is None:
            issues.append({
                "agent": agent_name,
                "severity": "error",
                "message": f"Provider '{runner_name}' not registered",
            })
        elif not provider.check_available():
            issues.append({
                "agent": agent_name,
                "severity": "warning",
                "message": f"Provider '{runner_name}' binary not found on PATH",
            })

        project_dir = agent_cfg.get("project_dir", "")
        if project_dir:
            p = Path(project_dir).expanduser().resolve()
            if not p.exists():
                issues.append({
                    "agent": agent_name,
                    "severity": "warning",
                    "message": f"Project directory does not exist: {project_dir}",
                })

        remote_cfg = remote.get_agent_remote_config(config, agent_name)
        if remote_cfg:
            target = remote.RemoteTarget(remote_cfg)
            if not target.is_available():
                issues.append({
                    "agent": agent_name,
                    "severity": "warning",
                    "message": f"Remote target '{remote_cfg.host}' is not reachable",
                })

    available_providers = []
    for pname in providers.list_providers():
        p = providers.get_provider(pname)
        if p:
            available_providers.append({
                "name": pname,
                "available": p.check_available(),
            })

    return {
        "issues": issues,
        "providers": available_providers,
        "healthy": len([i for i in issues if i["severity"] == "error"]) == 0,
    }


# ─── App factory ────────────────────────────────────────────────────────────

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup and shutdown event handler."""
    recover_stale_runs()
    providers.ensure_default_providers()
    routines_engine.start_scheduler()
    # Validate API key at startup to fail fast
    try:
        _get_api_key()
        logger.info("API key validated successfully")
    except RuntimeError as e:
        logger.error(f"API key validation failed: {e}")
        raise
    # Start periodic auth failure store cleanup (store reference for cleanup on shutdown)
    app.state._auth_cleanup_task = asyncio.create_task(_auth_failure_cleanup_task())
    try:
        from hub import queue_manager
        queue_manager.recover_queue()
    except Exception:
        pass
    try:
        from hub.runner import _build_run_index
        _build_run_index()
    except Exception:
        pass
    # Initialize persistent feature stores from SQLite
    try:
        _fs = feature_store.init_all()
        _webhooks.clear()
        _webhooks.update(_fs["webhooks"])
        _notification_channels.clear()
        _notification_channels.update(_fs["notification_channels"])
        _analytics["api_calls"].clear()
        _analytics["api_calls"].update(_fs["analytics"]["api_calls"])
        _analytics["task_durations"] = _fs["analytics"]["task_durations"]
        _analytics["daily_stats"].clear()
        _analytics["daily_stats"].update(_fs["analytics"]["daily_stats"])
        _api_keys.clear()
        _api_keys.update(_fs["api_keys"])
        _scheduled_pipelines.clear()
        _scheduled_pipelines.update(_fs["scheduled_pipelines"])
        _rate_limits.clear()
        _rate_limits.update(_fs["rate_limits"])
        _templates.clear()
        _templates.update(_fs["templates"])
        _approval_policies.clear()
        _approval_policies.update(_fs["approval_policies"])
        _approval_requests.clear()
        _approval_requests.update(_fs["approval_requests"])
        _rbac_roles.clear()
        _rbac_roles.update(_fs["rbac_roles"])
        _rbac_users.clear()
        _rbac_users.update(_fs["rbac_users"])
        _background_jobs.clear()
        _background_jobs.update(_fs["background_jobs"])
        _message_subscriptions.clear()
        _message_subscriptions.update(_fs["message_subscriptions"])
        _feature_flags.clear()
        _feature_flags.update(_fs["feature_flags"])
        _circuit_breakers.clear()
        _circuit_breakers.update(_fs["circuit_breakers"])
        _api_versions.clear()
        _api_versions.update(_fs["api_versions"])
        _dag_definitions.clear()
        _dag_definitions.update(_fs["dag_definitions"])
        _tenants.clear()
        _tenants.update(_fs["tenants"])
        _tenant_resources.clear()
        _tenant_resources.update(_fs["tenant_resources"])
        _scheduled_reports.clear()
        _scheduled_reports.update(_fs["scheduled_reports"])
        _webhook_secrets.clear()
        for platform, cfg in _fs.get("webhook_configs", {}).items():
            _webhook_secrets[platform] = {
                "secret": cfg.get("secret", ""),
                "pipelines": cfg.get("pipelines", []),
            }
        # Load additional stores that were missing
        _audit_log.clear()
        _audit_log.extend(_fs.get("audit_log", []))
        _file_storage.clear()
        _file_storage.update(_fs.get("file_storage", {}))
        _response_cache.clear()
        _response_cache.update(_fs.get("response_cache", {}))
        _deployments.clear()
        _deployments.update(_fs.get("deployments", {}))
        _message_bus.clear()
        for topic, messages in _fs.get("message_bus", {}).items():
            _message_bus[topic] = messages
        # Load auth users and sessions
        try:
            from hub.api import auth as auth_module
            auth_users = _fs.get("auth_users", {})
            if auth_users:
                auth_module.load_users_from_store(auth_users)
            auth_sessions = _fs.get("auth_sessions", {})
            if auth_sessions:
                auth_module.load_sessions_from_store(auth_sessions)
            # Initialize default admin if no users exist
            if not auth_users:
                admin_password = os.environ.get("ADMIN_PASSWORD", "")
                admin_user = auth_module.initialize_default_admin(password=admin_password)
                if admin_user:
                    logger.info("Default admin user initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize auth: {e}")
        logger.info("Feature stores loaded from SQLite database")
    except Exception as e:
        logger.warning(f"Failed to load feature stores from database: {e}")
    # Initialize OpenTelemetry tracing
    if tracing_module is not None:
        try:
            tracing_module.setup_tracing(app)
            logger.info("OpenTelemetry tracing initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize tracing: {e}")
    # Load plugins from plugins directory
    if plugins_module is not None:
        try:
            loaded = plugins_module.load_plugins()
            if loaded:
                logger.info(f"Loaded {len(loaded)} plugins: {', '.join(loaded)}")
        except Exception as e:
            logger.warning(f"Failed to load plugins: {e}")
    # Initialize cluster support (Redis-based shared state)
    if cluster_module is not None:
        try:
            available = await cluster_module.cluster.initialize()
            if available:
                logger.info("Cluster mode enabled: multi-instance support active")
            else:
                logger.info("Cluster mode disabled: running in single-instance mode")
        except Exception as e:
            logger.warning(f"Failed to initialize cluster: {e}")
    # Initialize marketplace registry
    if marketplace_module is not None:
        try:
            marketplace_module.registry.initialize()
            logger.info("Marketplace registry initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize marketplace: {e}")
    yield
    # Cancel background tasks on shutdown
    if hasattr(app.state, '_auth_cleanup_task'):
        app.state._auth_cleanup_task.cancel()
        try:
            await app.state._auth_cleanup_task
        except asyncio.CancelledError:
            pass
    routines_engine.stop_scheduler()
    # Shutdown cluster support
    if cluster_module is not None:
        try:
            await cluster_module.cluster.shutdown()
        except Exception as e:
            logger.warning(f"Failed to shutdown cluster: {e}")
    # Shutdown marketplace registry
    if marketplace_module is not None:
        try:
            marketplace_module.registry.shutdown()
        except Exception as e:
            logger.warning(f"Failed to shutdown marketplace: {e}")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Agent Hub",
        description="Unified Workflow orchestration platform — central API for agents, pipelines, and cross-service integration with Slinker and Trapper.",
        version="1.0.3",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Authentication routes (user sessions — separate from API key auth)
    try:
        from hub.api.auth_routes import router as auth_router
        app.include_router(auth_router)
        logger.info("Authentication routes enabled")
    except Exception as e:
        logger.warning(f"Failed to enable authentication routes: {e}")

    # Readiness endpoint (separate from health/liveness)
    @app.get("/ready", tags=["Health"])
    async def readiness_check():
        """Check if the service is ready to accept requests.
        
        Unlike /health (liveness), this checks that dependencies are available.
        Returns 503 if any required dependency is unhealthy.
        """
        checks = {
            "database": True,
            "dependencies": {},
        }
        all_ready = True
        
        # Check database connectivity
        try:
            from hub.feature_store import _get_connection
            conn = _get_connection()
            conn.execute("SELECT 1")
            conn.close()
        except Exception as e:
            checks["database"] = False
            all_ready = False
            logger.warning(f"Database readiness check failed: {e}")
        
        # Check optional dependencies using a shared client
        import httpx as httpx_lib
        timeout = httpx_lib.Timeout(2.0)
        
        async with httpx_lib.AsyncClient(timeout=timeout) as client:
            # Check Gobber (optional)
            try:
                gobber_url = os.environ.get("GOBBER_URL", "http://gobber:8085")
                if not gobber_url.startswith("http://localhost"):
                    resp = await client.get(f"{gobber_url}/health")
                    checks["dependencies"]["gobber"] = resp.status_code == 200
            except Exception:
                checks["dependencies"]["gobber"] = False
            
            # Check Hider (optional)
            try:
                hider_url = os.environ.get("HIDER_URL", "http://hider:8087")
                if not hider_url.startswith("http://localhost"):
                    resp = await client.get(f"{hider_url}/health")
                    checks["dependencies"]["hider"] = resp.status_code == 200
            except Exception:
                checks["dependencies"]["hider"] = False
        
        status_code = 200 if all_ready else 503
        return JSONResponse(
            status_code=status_code,
            content={
                "status": "ready" if all_ready else "not_ready",
                "checks": checks,
            },
        )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_get_cors_origins(),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Requested-With", "X-CSRF-Token", "X-XSRF-Token"],
    )

    # CSRF protection middleware
    try:
        from hub.api.csrf import add_csrf_middleware
        add_csrf_middleware(app)
        logger.info("CSRF protection middleware enabled")
    except Exception as e:
        logger.warning(f"Failed to enable CSRF middleware: {e}")

    # Rate limiting middleware for expensive operations
    try:
        from hub.api.rate_limit_middleware import add_rate_limit_middleware
        add_rate_limit_middleware(app)
        logger.info("Rate limiting middleware enabled")
    except Exception as e:
        logger.warning(f"Failed to enable rate limiting middleware: {e}")

    # Idempotency middleware for retryable operations
    try:
        from hub.api.idempotency import add_idempotency_middleware
        add_idempotency_middleware(app)
        logger.info("Idempotency middleware enabled")
    except Exception as e:
        logger.warning(f"Failed to enable idempotency middleware: {e}")

    # Prometheus metrics middleware
    if metrics_module is not None:
        app.add_middleware(metrics_module.create_metrics_middleware())

    # Plugin routes
    if plugins_module is not None:
        try:
            plugins_module.create_plugin_routes(app)
        except Exception as e:
            logger.warning(f"Failed to register plugin routes: {e}")

    # Request logging middleware
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        """Log all requests with timing and correlation ID."""
        request_id = str(uuid.uuid4())[:8]
        start_time = time.time()

        response = await call_next(request)
        duration_ms = (time.time() - start_time) * 1000

        logger.info(
            f"{request.method} {request.url.path} {response.status_code} "
            f"{duration_ms:.1f}ms",
            extra={"request_id": request_id, "method": request.method,
                   "path": request.url.path, "status": response.status_code,
                   "duration_ms": round(duration_ms, 1)},
        )

        response.headers["X-Request-ID"] = request_id
        return response

    return app
