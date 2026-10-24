"""Environments — reusable sandbox definitions.

Inspired by Factory's Environments, this module provides reusable definitions
of the sandbox where agents run. An environment describes the repositories,
tools, dependencies, and setup commands needed for a project.

Key concepts:
- Environment: Reusable definition of a sandbox.
- Health check: Verify the environment is ready.
- Reproducible setup: Same environment produces same sandbox.
"""

import json
import os
import sqlite3
import subprocess
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

# ─── Models ──────────────────────────────────────────────────────────────────


@dataclass
class EnvironmentDefinition:
    """An environment (sandbox) definition."""
    id: str = field(default_factory=lambda: f"env-{uuid4().hex[:8]}")
    name: str = ""
    description: str = ""
    # Target repository
    repository: str = ""
    # Branch or tag
    branch: str = "main"
    # Runtime and toolchain versions
    python_version: str = ""
    node_version: str = ""
    # Dependencies to install
    dependencies: list[str] = field(default_factory=list)
    # Setup commands
    setup_commands: list[str] = field(default_factory=list)
    # Build command
    build_command: str = ""
    # Test command
    test_command: str = ""
    # Environment variables (keys only, values stored securely)
    env_var_keys: list[str] = field(default_factory=list)
    # Required services
    required_services: list[str] = field(default_factory=list)
    # Health check command
    health_check_command: str = ""
    # Whether the environment is validated
    validated: bool = False
    # Last validation timestamp
    last_validated_at: Optional[str] = None
    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EnvironmentDefinition":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ─── Storage ─────────────────────────────────────────────────────────────────

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_ENVS_DB_PATH = os.environ.get("GREENIE_ENVS_DB_PATH", str(_DATA_DIR / "environments.db"))

_db_lock = threading.Lock()


def _get_connection() -> sqlite3.Connection:
    """Get a new database connection with WAL mode."""
    db_dir = Path(_ENVS_DB_PATH).parent
    db_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_ENVS_DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _init_schema() -> None:
    """Initialize the environments database schema."""
    conn = _get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS environments (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                repository TEXT NOT NULL DEFAULT '',
                branch TEXT NOT NULL DEFAULT 'main',
                python_version TEXT NOT NULL DEFAULT '',
                node_version TEXT NOT NULL DEFAULT '',
                dependencies_json TEXT NOT NULL DEFAULT '[]',
                setup_commands_json TEXT NOT NULL DEFAULT '[]',
                build_command TEXT NOT NULL DEFAULT '',
                test_command TEXT NOT NULL DEFAULT '',
                env_var_keys_json TEXT NOT NULL DEFAULT '[]',
                required_services_json TEXT NOT NULL DEFAULT '[]',
                health_check_command TEXT NOT NULL DEFAULT '',
                validated INTEGER NOT NULL DEFAULT 0,
                last_validated_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_envs_name ON environments(name);
            CREATE INDEX IF NOT EXISTS idx_envs_repo ON environments(repository);
        """)
        conn.commit()
    finally:
        conn.close()


with _db_lock:
    _init_schema()


def _row_to_env(row: sqlite3.Row) -> EnvironmentDefinition:
    """Convert a database row to an EnvironmentDefinition."""
    return EnvironmentDefinition(
        id=row["id"],
        name=row["name"],
        description=row["description"],
        repository=row["repository"],
        branch=row["branch"],
        python_version=row["python_version"],
        node_version=row["node_version"],
        dependencies=json.loads(row["dependencies_json"]),
        setup_commands=json.loads(row["setup_commands_json"]),
        build_command=row["build_command"],
        test_command=row["test_command"],
        env_var_keys=json.loads(row["env_var_keys_json"]),
        required_services=json.loads(row["required_services_json"]),
        health_check_command=row["health_check_command"],
        validated=bool(row["validated"]),
        last_validated_at=row["last_validated_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def save_environment(env: EnvironmentDefinition) -> EnvironmentDefinition:
    """Save or update an environment."""
    with _db_lock:
        conn = _get_connection()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO environments
                   (id, name, description, repository, branch, python_version, node_version,
                    dependencies_json, setup_commands_json, build_command, test_command,
                    env_var_keys_json, required_services_json, health_check_command,
                    validated, last_validated_at, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    env.id, env.name, env.description, env.repository, env.branch,
                    env.python_version, env.node_version,
                    json.dumps(env.dependencies), json.dumps(env.setup_commands),
                    env.build_command, env.test_command,
                    json.dumps(env.env_var_keys), json.dumps(env.required_services),
                    env.health_check_command, int(env.validated), env.last_validated_at,
                    env.created_at, env.updated_at,
                ),
            )
            conn.commit()
            return env
        finally:
            conn.close()


def get_environment(env_id: str) -> Optional[EnvironmentDefinition]:
    """Get an environment by ID."""
    conn = _get_connection()
    try:
        row = conn.execute("SELECT * FROM environments WHERE id = ?", (env_id,)).fetchone()
        return _row_to_env(row) if row else None
    finally:
        conn.close()


def list_environments() -> list[EnvironmentDefinition]:
    """List all environments."""
    conn = _get_connection()
    try:
        rows = conn.execute("SELECT * FROM environments ORDER BY name").fetchall()
        return [_row_to_env(r) for r in rows]
    finally:
        conn.close()


def delete_environment(env_id: str) -> bool:
    """Delete an environment."""
    with _db_lock:
        conn = _get_connection()
        try:
            cursor = conn.execute("DELETE FROM environments WHERE id = ?", (env_id,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()


# ─── Controller ──────────────────────────────────────────────────────────────


def create_environment(
    name: str,
    repository: str,
    description: str = "",
    branch: str = "main",
    python_version: str = "",
    node_version: str = "",
    dependencies: Optional[list[str]] = None,
    setup_commands: Optional[list[str]] = None,
    build_command: str = "",
    test_command: str = "",
    env_var_keys: Optional[list[str]] = None,
    required_services: Optional[list[str]] = None,
    health_check_command: str = "",
) -> EnvironmentDefinition:
    """Create a new environment definition.

    Args:
        name: Environment name.
        repository: Repository path or URL.
        description: Optional description.
        branch: Git branch or tag.
        python_version: Python version (e.g., "3.12").
        node_version: Node.js version (e.g., "20").
        dependencies: List of dependencies to install.
        setup_commands: List of setup commands.
        build_command: Build command.
        test_command: Test command.
        env_var_keys: Environment variable keys.
        required_services: Required services.
        health_check_command: Health check command.

    Returns:
        The created EnvironmentDefinition.
    """
    if not name.strip():
        raise ValueError("name is required")
    if not repository.strip():
        raise ValueError("repository is required")

    env = EnvironmentDefinition(
        name=name.strip(),
        description=description.strip(),
        repository=repository.strip(),
        branch=branch,
        python_version=python_version,
        node_version=node_version,
        dependencies=dependencies or [],
        setup_commands=setup_commands or [],
        build_command=build_command,
        test_command=test_command,
        env_var_keys=env_var_keys or [],
        required_services=required_services or [],
        health_check_command=health_check_command,
    )
    save_environment(env)
    return env


def validate_environment(env_id: str) -> dict[str, Any]:
    """Validate an environment by running its health check.

    Args:
        env_id: The environment to validate.

    Returns:
        Dict with validation results.
    """
    env = get_environment(env_id)
    if env is None:
        raise ValueError(f"Environment {env_id} not found")

    result = {
        "environment_id": env_id,
        "checks": [],
        "passed": True,
    }

    # Check repository exists
    repo_path = Path(env.repository)
    if repo_path.exists() and repo_path.is_dir():
        result["checks"].append({"check": "repository_exists", "passed": True})
    else:
        result["checks"].append({"check": "repository_exists", "passed": False})
        result["passed"] = False

    # Check Python version if specified
    if env.python_version:
        try:
            proc = subprocess.run(
                ["python", "--version"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            version_output = proc.stdout.strip()
            passed = env.python_version.split(".")[0] in version_output
            result["checks"].append({
                "check": "python_version",
                "expected": env.python_version,
                "actual": version_output,
                "passed": passed,
            })
            if not passed:
                result["passed"] = False
        except Exception as e:
            result["checks"].append({"check": "python_version", "passed": False, "error": str(e)})
            result["passed"] = False

    # Run health check command if specified
    if env.health_check_command:
        try:
            import shlex
            proc = subprocess.run(
                shlex.split(env.health_check_command),
                shell=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            passed = proc.returncode == 0
            result["checks"].append({
                "check": "health_check",
                "command": env.health_check_command,
                "passed": passed,
                "output": proc.stdout[:500],
                "error": proc.stderr[:500] if proc.stderr else "",
            })
            if not passed:
                result["passed"] = False
        except Exception as e:
            result["checks"].append({"check": "health_check", "passed": False, "error": str(e)})
            result["passed"] = False

    # Update environment validation status
    env.validated = result["passed"]
    env.last_validated_at = datetime.now(timezone.utc).isoformat()
    save_environment(env)

    return result
