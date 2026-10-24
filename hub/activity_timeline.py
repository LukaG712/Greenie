"""Unified Activity Timeline and Audit Trail.

Implements an event history that makes activity traceable across services.
For a workflow or task, users can see:
- Who or what initiated it (including which trigger)
- Which services participated
- Which model was selected
- Which tools were invoked
- Which workstation executed the work
- Which approval gates were passed
- Which files were modified
- What failed and why
- Execution duration
- Usage and estimated cost
- The final outcome

Uses SQLite persistence with WAL mode for performance.
"""

import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

DATA_DIR = os.getenv("GREENIE_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
TIMELINE_DB_PATH = os.getenv("GREENIE_TIMELINE_DB_PATH", os.path.join(DATA_DIR, "timeline.db"))


def _get_connection():
    """Get a connection to the timeline database."""
    os.makedirs(os.path.dirname(TIMELINE_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(TIMELINE_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_tables():
    """Create timeline tables if they don't exist."""
    conn = _get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS timeline_events (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT 'system',
                correlation_id TEXT,
                workflow_id TEXT,
                task_id TEXT,
                agent_id TEXT,
                service TEXT,
                user_id TEXT,
                actor TEXT,
                summary TEXT,
                details TEXT,
                cost_usd REAL DEFAULT 0,
                duration_ms INTEGER,
                status TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ', 'now'))
            );
            
            CREATE INDEX IF NOT EXISTS idx_timeline_timestamp ON timeline_events(timestamp);
            CREATE INDEX IF NOT EXISTS idx_timeline_correlation ON timeline_events(correlation_id);
            CREATE INDEX IF NOT EXISTS idx_timeline_workflow ON timeline_events(workflow_id);
            CREATE INDEX IF NOT EXISTS idx_timeline_task ON timeline_events(task_id);
            CREATE INDEX IF NOT EXISTS idx_timeline_agent ON timeline_events(agent_id);
            CREATE INDEX IF NOT EXISTS idx_timeline_service ON timeline_events(service);
            CREATE INDEX IF NOT EXISTS idx_timeline_category ON timeline_events(category);
            
            CREATE TABLE IF NOT EXISTS audit_events (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                user_id TEXT,
                actor TEXT,
                resource_type TEXT,
                resource_id TEXT,
                action TEXT,
                old_value TEXT,
                new_value TEXT,
                ip_address TEXT,
                details TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ', 'now'))
            );
            
            CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_events(timestamp);
            CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_events(user_id);
            CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_events(resource_type, resource_id);
        """)
        conn.commit()
    finally:
        conn.close()


# Initialize tables on import
_ensure_tables()


def record_event(
    event: dict,
    *,
    event_type: Optional[str] = None,
    category: str = "system",
    correlation_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
    task_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    service: Optional[str] = None,
    user_id: Optional[str] = None,
    actor: Optional[str] = None,
    summary: Optional[str] = None,
    cost_usd: float = 0,
    duration_ms: Optional[int] = None,
    status: Optional[str] = None,
) -> str:
    """Record an event in the activity timeline.
    
    Args:
        event: Either a dict with event data or a string event type.
        event_type: Type of event (e.g., 'workflow_started', 'tool_called').
        category: Category for grouping (system, workflow, agent, security, etc.).
        correlation_id: Cross-service correlation ID.
        workflow_id: Associated workflow ID.
        task_id: Associated task ID.
        agent_id: Associated agent ID.
        service: Service name that generated the event.
        user_id: User who triggered the event.
        actor: Actor (user, system, trigger name, etc.).
        summary: Human-readable summary.
        cost_usd: Estimated cost in USD.
        duration_ms: Duration in milliseconds.
        status: Status (success, failed, pending, etc.).
        
    Returns:
        The event ID.
    """
    if isinstance(event, dict):
        event_type = event_type or event.get("event", event.get("event_type", "unknown"))
        summary = summary or event.get("summary", "")
        details = json.dumps(event)
    else:
        event_type = event_type or str(event)
        details = json.dumps({"event": event}) if event else "{}"
    
    event_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    
    conn = _get_connection()
    try:
        conn.execute("""
            INSERT INTO timeline_events 
            (id, timestamp, event_type, category, correlation_id, workflow_id, task_id,
             agent_id, service, user_id, actor, summary, details, cost_usd, duration_ms, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, timestamp, event_type, category, correlation_id, workflow_id,
            task_id, agent_id, service, user_id, actor, summary or "", details,
            cost_usd, duration_ms, status,
        ))
        conn.commit()
        return event_id
    finally:
        conn.close()


def record_audit_event(
    event_type: str,
    *,
    user_id: Optional[str] = None,
    actor: Optional[str] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    action: Optional[str] = None,
    old_value: Optional[str] = None,
    new_value: Optional[str] = None,
    ip_address: Optional[str] = None,
    details: Optional[dict] = None,
) -> str:
    """Record a security audit event.
    
    Separate from operational timeline events for security compliance.
    """
    event_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    
    conn = _get_connection()
    try:
        conn.execute("""
            INSERT INTO audit_events
            (id, timestamp, event_type, user_id, actor, resource_type, resource_id,
             action, old_value, new_value, ip_address, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, timestamp, event_type, user_id, actor, resource_type,
            resource_id, action, old_value, new_value, ip_address,
            json.dumps(details) if details else None,
        ))
        conn.commit()
        return event_id
    finally:
        conn.close()


def query_timeline(
    *,
    correlation_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
    task_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    service: Optional[str] = None,
    category: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Query the activity timeline with filters."""
    conditions = []
    params = []
    
    if correlation_id:
        conditions.append("correlation_id = ?")
        params.append(correlation_id)
    if workflow_id:
        conditions.append("workflow_id = ?")
        params.append(workflow_id)
    if task_id:
        conditions.append("task_id = ?")
        params.append(task_id)
    if agent_id:
        conditions.append("agent_id = ?")
        params.append(agent_id)
    if service:
        conditions.append("service = ?")
        params.append(service)
    if category:
        conditions.append("category = ?")
        params.append(category)
    if start_time:
        conditions.append("timestamp >= ?")
        params.append(start_time)
    if end_time:
        conditions.append("timestamp <= ?")
        params.append(end_time)
    
    where_clause = f"WHERE {' AND '.join(conditions)} " if conditions else ""
    
    conn = _get_connection()
    try:
        rows = conn.execute(f"""
            SELECT * FROM timeline_events
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchall()
        
        return [dict(r) for r in rows]
    finally:
        conn.close()


def query_audit(
    *,
    user_id: Optional[str] = None,
    resource_type: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Query the security audit trail."""
    conditions = []
    params = []
    
    if user_id:
        conditions.append("user_id = ?")
        params.append(user_id)
    if resource_type:
        conditions.append("resource_type = ?")
        params.append(resource_type)
    if start_time:
        conditions.append("timestamp >= ?")
        params.append(start_time)
    if end_time:
        conditions.append("timestamp <= ?")
        params.append(end_time)
    
    where_clause = f"WHERE {' AND '.join(conditions)} " if conditions else ""
    
    conn = _get_connection()
    try:
        rows = conn.execute(f"""
            SELECT * FROM audit_events
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchall()
        
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_workflow_timeline(workflow_id: str) -> list[dict]:
    """Get the complete timeline for a specific workflow run."""
    return query_timeline(workflow_id=workflow_id, limit=1000)


def get_task_timeline(task_id: str) -> list[dict]:
    """Get the complete timeline for a specific task."""
    return query_timeline(task_id=task_id, limit=1000)


def get_correlation_timeline(correlation_id: str) -> list[dict]:
    """Get all events across services for a correlation ID."""
    return query_timeline(correlation_id=correlation_id, limit=1000)


def timeline_stats() -> dict:
    """Get timeline statistics."""
    conn = _get_connection()
    try:
        total = conn.execute("SELECT COUNT(*) as count FROM timeline_events").fetchone()["count"]
        audit_total = conn.execute("SELECT COUNT(*) as count FROM audit_events").fetchone()["count"]
        
        by_category = {}
        for row in conn.execute("SELECT category, COUNT(*) as count FROM timeline_events GROUP BY category").fetchall():
            by_category[row["category"]] = row["count"]
        
        by_service = {}
        for row in conn.execute("SELECT service, COUNT(*) as count FROM timeline_events WHERE service IS NOT NULL GROUP BY service").fetchall():
            by_service[row["service"]] = row["count"]
        
        total_cost = conn.execute("SELECT SUM(cost_usd) as total FROM timeline_events").fetchone()["total"] or 0
        
        return {
            "total_events": total,
            "total_audit_events": audit_total,
            "by_category": by_category,
            "by_service": by_service,
            "total_cost_usd": round(total_cost, 4),
        }
    finally:
        conn.close()


def prune_old_events(retain_days: int = 90):
    """Remove events older than retain_days to manage disk usage."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retain_days)).isoformat()
    conn = _get_connection()
    try:
        deleted = conn.execute(
            "DELETE FROM timeline_events WHERE timestamp < ?",
            (cutoff,),
        ).rowcount
        conn.commit()
        logger.info(f"Pruned {deleted} old timeline events")
        return deleted
    finally:
        conn.close()
