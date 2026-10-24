"""Hooks System — event-driven lifecycle hooks.

Inspired by Factory Droid's hooks, this module provides event-driven automation
for mission lifecycle events. Hooks can be scripts or webhooks that execute
when specific events occur.

Supported events:
- PreToolUse: Before tool execution (can block)
- PostToolUse: After tool completion
- SessionStart: Session start/resume (context injection)
- SessionEnd: Session termination (cleanup)
- MissionStart: Mission begins execution
- MissionEnd: Mission completes or fails
- MilestoneComplete: Milestone finishes
- ValidationFail: Validation finds issues
"""

import json
import os
import sqlite3
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

# ─── Models ──────────────────────────────────────────────────────────────────


class HookEvent(str, Enum):
    """Supported hook events."""
    PRE_TOOL_USE = "pre_tool_use"
    POST_TOOL_USE = "post_tool_use"
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    MISSION_START = "mission_start"
    MISSION_END = "mission_end"
    MILESTONE_COMPLETE = "milestone_complete"
    VALIDATION_FAIL = "validation_fail"


class HookType(str, Enum):
    """Hook execution type."""
    SCRIPT = "script"  # Local script execution
    WEBHOOK = "webhook"  # HTTP POST to external endpoint


@dataclass
class HookDefinition:
    """A hook definition."""
    id: str = field(default_factory=lambda: f"hook-{uuid4().hex[:8]}")
    name: str = ""
    description: str = ""
    event: str = HookEvent.MISSION_START.value
    hook_type: str = HookType.SCRIPT.value
    # Script path or webhook URL
    target: str = ""
    # Environment variables for the hook
    env_vars: dict[str, str] = field(default_factory=dict)
    # Whether the hook can block execution (for pre-* events)
    can_block: bool = False
    # Timeout in seconds
    timeout: int = 30
    # Whether the hook is enabled
    enabled: bool = True
    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HookDefinition":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class HookExecutionResult:
    """Result of a hook execution."""
    id: str = field(default_factory=lambda: f"hook-exec-{uuid4().hex[:8]}")
    hook_id: str = ""
    event: str = ""
    success: bool = False
    output: str = ""
    error: str = ""
    duration_ms: int = 0
    blocked: bool = False
    executed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HookExecutionResult":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ─── Storage ─────────────────────────────────────────────────────────────────

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_HOOKS_DB_PATH = os.environ.get("GREENIE_HOOKS_DB_PATH", str(_DATA_DIR / "hooks.db"))

_db_lock = threading.Lock()


def _get_connection() -> sqlite3.Connection:
    """Get a new database connection with WAL mode."""
    db_dir = Path(_HOOKS_DB_PATH).parent
    db_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_HOOKS_DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _init_schema() -> None:
    """Initialize the hooks database schema."""
    conn = _get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS hooks (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                event TEXT NOT NULL,
                hook_type TEXT NOT NULL DEFAULT 'script',
                target TEXT NOT NULL DEFAULT '',
                env_vars_json TEXT NOT NULL DEFAULT '{}',
                can_block INTEGER NOT NULL DEFAULT 0,
                timeout INTEGER NOT NULL DEFAULT 30,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS hook_executions (
                id TEXT PRIMARY KEY,
                hook_id TEXT NOT NULL,
                event TEXT NOT NULL,
                success INTEGER NOT NULL DEFAULT 0,
                output TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                duration_ms INTEGER NOT NULL DEFAULT 0,
                blocked INTEGER NOT NULL DEFAULT 0,
                executed_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_hooks_event ON hooks(event);
            CREATE INDEX IF NOT EXISTS idx_hooks_enabled ON hooks(enabled);
            CREATE INDEX IF NOT EXISTS idx_hook_executions_hook ON hook_executions(hook_id);
        """)
        conn.commit()
    finally:
        conn.close()


with _db_lock:
    _init_schema()


def _row_to_hook(row: sqlite3.Row) -> HookDefinition:
    """Convert a database row to a HookDefinition."""
    return HookDefinition(
        id=row["id"],
        name=row["name"],
        description=row["description"],
        event=row["event"],
        hook_type=row["hook_type"],
        target=row["target"],
        env_vars=json.loads(row["env_vars_json"]),
        can_block=bool(row["can_block"]),
        timeout=row["timeout"],
        enabled=bool(row["enabled"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_hook_result(row: sqlite3.Row) -> HookExecutionResult:
    """Convert a database row to a HookExecutionResult."""
    return HookExecutionResult(
        id=row["id"],
        hook_id=row["hook_id"],
        event=row["event"],
        success=bool(row["success"]),
        output=row["output"],
        error=row["error"],
        duration_ms=row["duration_ms"],
        blocked=bool(row["blocked"]),
        executed_at=row["executed_at"],
    )


def save_hook(hook: HookDefinition) -> HookDefinition:
    """Save or update a hook."""
    with _db_lock:
        conn = _get_connection()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO hooks
                   (id, name, description, event, hook_type, target, env_vars_json,
                    can_block, timeout, enabled, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    hook.id, hook.name, hook.description, hook.event, hook.hook_type,
                    hook.target, json.dumps(hook.env_vars), int(hook.can_block),
                    hook.timeout, int(hook.enabled), hook.created_at, hook.updated_at,
                ),
            )
            conn.commit()
            return hook
        finally:
            conn.close()


def get_hook(hook_id: str) -> Optional[HookDefinition]:
    """Get a hook by ID."""
    conn = _get_connection()
    try:
        row = conn.execute("SELECT * FROM hooks WHERE id = ?", (hook_id,)).fetchone()
        return _row_to_hook(row) if row else None
    finally:
        conn.close()


def list_hooks(event: Optional[str] = None, enabled_only: bool = True) -> list[HookDefinition]:
    """List hooks with optional filters."""
    conn = _get_connection()
    try:
        query = "SELECT * FROM hooks WHERE 1=1"
        params: list[Any] = []

        if event:
            query += " AND event = ?"
            params.append(event)
        if enabled_only:
            query += " AND enabled = 1"

        query += " ORDER BY created_at DESC"
        rows = conn.execute(query, params).fetchall()
        return [_row_to_hook(r) for r in rows]
    finally:
        conn.close()


def delete_hook(hook_id: str) -> bool:
    """Delete a hook."""
    with _db_lock:
        conn = _get_connection()
        try:
            cursor = conn.execute("DELETE FROM hooks WHERE id = ?", (hook_id,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()


def save_hook_execution(result: HookExecutionResult) -> HookExecutionResult:
    """Save a hook execution result."""
    with _db_lock:
        conn = _get_connection()
        try:
            conn.execute(
                """INSERT INTO hook_executions
                   (id, hook_id, event, success, output, error, duration_ms, blocked, executed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    result.id, result.hook_id, result.event, int(result.success),
                    result.output, result.error, result.duration_ms, int(result.blocked),
                    result.executed_at,
                ),
            )
            conn.commit()
            return result
        finally:
            conn.close()


def get_hook_executions(hook_id: str, limit: int = 50) -> list[HookExecutionResult]:
    """Get execution history for a hook."""
    conn = _get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM hook_executions WHERE hook_id = ? ORDER BY executed_at DESC LIMIT ?",
            (hook_id, limit),
        ).fetchall()
        return [_row_to_hook_result(r) for r in rows]
    finally:
        conn.close()


# ─── Controller ──────────────────────────────────────────────────────────────


def create_hook(
    name: str,
    event: str,
    hook_type: str,
    target: str,
    description: str = "",
    env_vars: Optional[dict[str, str]] = None,
    can_block: bool = False,
    timeout: int = 30,
) -> HookDefinition:
    """Create a new hook.

    Args:
        name: Hook name.
        event: Event to trigger on.
        hook_type: 'script' or 'webhook'.
        target: Script path or webhook URL.
        description: Optional description.
        env_vars: Optional environment variables.
        can_block: Whether the hook can block execution.
        timeout: Timeout in seconds.

    Returns:
        The created HookDefinition.
    """
    if not name.strip():
        raise ValueError("name is required")
    if event not in [e.value for e in HookEvent]:
        raise ValueError(f"Invalid event: {event}")
    if hook_type not in [t.value for t in HookType]:
        raise ValueError(f"Invalid hook_type: {hook_type}")
    if not target.strip():
        raise ValueError("target is required")

    hook = HookDefinition(
        name=name.strip(),
        description=description.strip(),
        event=event,
        hook_type=hook_type,
        target=target.strip(),
        env_vars=env_vars or {},
        can_block=can_block,
        timeout=timeout,
    )
    save_hook(hook)
    return hook


def execute_hooks(
    event: str,
    context: Optional[dict[str, Any]] = None,
) -> list[HookExecutionResult]:
    """Execute all enabled hooks for an event.

    Args:
        event: The event that occurred.
        context: Optional context data for the hooks.

    Returns:
        List of execution results.
    """
    hooks = list_hooks(event=event, enabled_only=True)
    results = []

    for hook in hooks:
        result = HookExecutionResult(
            hook_id=hook.id,
            event=event,
        )

        try:
            if hook.hook_type == HookType.WEBHOOK.value:
                # Webhook execution
                import httpx
                payload = {"event": event, "context": context or {}, "hook": hook.to_dict()}
                with httpx.Client(timeout=hook.timeout) as client:
                    resp = client.post(hook.target, json=payload)
                    result.success = resp.status_code in (200, 201, 202, 204)
                    result.output = resp.text[:1000]  # Truncate long output
                    result.blocked = hook.can_block and not result.success

            elif hook.hook_type == HookType.SCRIPT.value:
                # Script execution
                import subprocess
                env = os.environ.copy()
                env.update(hook.env_vars)
                if context:
                    env["HOOK_CONTEXT"] = json.dumps(context)

                proc = subprocess.run(
                    [hook.target],
                    capture_output=True,
                    text=True,
                    timeout=hook.timeout,
                    env=env,
                )
                result.success = proc.returncode == 0
                result.output = proc.stdout[:1000]
                result.error = proc.stderr[:500] if proc.stderr else ""
                result.blocked = hook.can_block and proc.returncode == 2

        except Exception as e:
            result.success = False
            result.error = str(e)

        result.duration_ms = 0  # Would be calculated with proper timing
        save_hook_execution(result)
        results.append(result)

    return results


def test_hook(hook_id: str, context: Optional[dict[str, Any]] = None) -> HookExecutionResult:
    """Test a hook (dry-run).

    Args:
        hook_id: The hook to test.
        context: Optional test context.

    Returns:
        Execution result.
    """
    hook = get_hook(hook_id)
    if hook is None:
        raise ValueError(f"Hook {hook_id} not found")

    results = execute_hooks(hook.event, context)
    # Find the result for the specific hook being tested
    for result in results:
        if result.hook_id == hook_id:
            return result
    return HookExecutionResult(
        hook_id=hook_id,
        event=hook.event,
        success=False,
        error="No hooks executed",
    )
