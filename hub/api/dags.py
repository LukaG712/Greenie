"""Route module: dags."""

import asyncio
import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _tenant_resources, _tenant_resources_lock, _tenants, _tenants_lock, _verify_token
from hub.runner import Run, agent_status, get_run, list_runs, recover_stale_runs, run_agent, stop_run

logger = logging.getLogger(__name__)

# DAG task store
_dag_tasks: Dict[str, Dict[str, Any]] = {}
_dag_tasks_lock = threading.Lock()

# DAG execution history
_dag_executions: Dict[str, Dict[str, Any]] = {}
_dag_executions_lock = threading.Lock()


async def api_create_dag(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Create a Directed Acyclic Graph of dependent tasks.

    Tasks are executed respecting their dependencies. Independent tasks
    run in parallel; dependent tasks wait for their prerequisites.

    Example body:
    {
        "name": "Research Pipeline",
        "tasks": {
            "scrape": {"service": "Trapper", "task": "SCRAPE:https://example.com"},
            "analyze": {"service": "Slinker", "task": "Analyze content", "depends_on": ["scrape"]},
            "summarize": {"service": "agent", "agent": "vault", "task": "Summarize", "depends_on": ["analyze"]}
        }
    }
    """
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    tasks = body.get("tasks", {})
    if not tasks:
        raise HTTPException(status_code=400, detail="tasks are required")

    # Validate: check for cycles
    task_ids = set(tasks.keys())
    for task_id, task_def in tasks.items():
        deps = task_def.get("depends_on", [])
        for dep in deps:
            if dep not in task_ids:
                raise HTTPException(status_code=400, detail=f"Task '{task_id}' depends on unknown task '{dep}'")

    # Simple cycle detection via DFS
    visited = set()
    in_stack = set()

    def has_cycle(node: str) -> bool:
        if node in in_stack:
            return True
        if node in visited:
            return False
        visited.add(node)
        in_stack.add(node)
        for dep in tasks.get(node, {}).get("depends_on", []):
            if has_cycle(dep):
                return True
        in_stack.discard(node)
        return False

    for task_id in task_ids:
        if has_cycle(task_id):
            raise HTTPException(status_code=400, detail="Cycle detected in task dependencies")

    dag_id = f"dag-{uuid.uuid4().hex[:8]}"
    dag = {
        "id": dag_id,
        "name": name,
        "tasks": tasks,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "execution_count": 0,
    }

    with _dag_tasks_lock:
        _dag_tasks[dag_id] = dag

    # Persist to database
    try:
        feature_store.save_dag_definition(dag)
    except Exception as e:
        logger.warning(f"Failed to save DAG definition: {e}")

    return dag


async def api_list_dags(_auth: None = Depends(_verify_token)):
    """List all DAG definitions."""
    with _dag_tasks_lock:
        dags = list(_dag_tasks.values())
    return {"dags": dags}


async def api_get_dag(dag_id: str, _auth: None = Depends(_verify_token)):
    """Get a DAG definition by ID."""
    with _dag_tasks_lock:
        dag = _dag_tasks.get(dag_id)
    if not dag:
        raise HTTPException(status_code=404, detail="DAG not found")
    return dag


async def api_delete_dag(dag_id: str, _auth: None = Depends(_verify_token)):
    """Delete a DAG definition."""
    with _dag_tasks_lock:
        if dag_id not in _dag_tasks:
            raise HTTPException(status_code=404, detail="DAG not found")
        del _dag_tasks[dag_id]

    # Persist to database
    try:
        feature_store.delete_dag_definition(dag_id)
    except Exception as e:
        logger.warning(f"Failed to delete DAG definition: {e}")

    return {"status": "ok", "message": "DAG deleted"}


async def api_execute_dag(dag_id: str, _auth: None = Depends(_verify_token)):
    """Execute a DAG, running tasks respecting their dependencies.

    Independent tasks run in parallel. Dependent tasks wait for all
    prerequisites to complete before starting.
    """
    with _dag_tasks_lock:
        dag = _dag_tasks.get(dag_id)
        if not dag:
            raise HTTPException(status_code=404, detail="DAG not found")

    tasks = dag["tasks"]
    execution_id = f"dagexec-{uuid.uuid4().hex[:8]}"
    results: Dict[str, Any] = {}
    errors: Dict[str, str] = {}

    # Topological sort to determine execution order
    def get_execution_order() -> List[List[str]]:
        """Return layers of tasks that can execute in parallel."""
        in_degree = {tid: len(td.get("depends_on", [])) for tid, td in tasks.items()}
        layers = []
        remaining = dict(in_degree)

        while remaining:
            # Find tasks with no remaining dependencies
            layer = [tid for tid, deg in remaining.items() if deg == 0]
            if not layer:
                break  # Cycle (shouldn't happen after validation)
            layers.append(layer)
            for tid in layer:
                del remaining[tid]
                # Reduce in-degree for dependents
                for other_id, other_def in tasks.items():
                    if tid in other_def.get("depends_on", []) and other_id in remaining:
                        remaining[other_id] -= 1

        return layers

    layers = get_execution_order()

    async def _execute_single_task(task_id: str, task_def: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single task and return its result."""
        service = task_def.get("service", "agent")
        task = task_def.get("task", "")

        # Substitute results from dependencies
        for dep_id in task_def.get("depends_on", []):
            dep_result = results.get(dep_id)
            if dep_result:
                task = task.replace(f"{{{{{dep_id}}}}}", str(dep_result)[:2000])

        try:
            if service == "Trapper":
                from hub.Trapper_provider import TrapperProvider
                provider = TrapperProvider()
                result_data = await provider.execute(task)
                return {"status": "completed", "result": result_data}
            elif service == "Slinker":
                from hub.Slinker_provider import SlinkerProvider
                provider = SlinkerProvider()
                result_data = await provider.execute(task)
                return {"status": "completed", "result": result_data}
            elif service == "agent":
                agent = task_def.get("agent", "")
                if agent:
                    run = run_agent(agent, task)
                    return {"status": "completed", "run_id": run.id, "answer": run.answer}
                return {"status": "error", "error": "agent name required"}
            else:
                return {"status": "error", "error": f"Unknown service: {service}"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # Execute layer by layer
    for layer in layers:
        # Check if any dependency in previous layers failed
        blocked = False
        for task_id in layer:
            for dep_id in tasks[task_id].get("depends_on", []):
                if dep_id in errors:
                    errors[task_id] = f"Blocked: dependency '{dep_id}' failed: {errors[dep_id]}"
                    blocked = True

        # Execute unblocked tasks in parallel
        unblocked = [tid for tid in layer if tid not in errors]
        if unblocked:
            coros = [_execute_single_task(tid, tasks[tid]) for tid in unblocked]
            layer_results = await asyncio.gather(*coros)
            for tid, result in zip(unblocked, layer_results):
                if result["status"] == "error":
                    errors[tid] = result["error"]
                else:
                    results[tid] = result.get("result", result.get("answer", ""))

    # Track execution
    execution = {
        "id": execution_id,
        "dag_id": dag_id,
        "dag_name": dag["name"],
        "results": results,
        "errors": errors,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "succeeded": len(errors) == 0,
    }

    with _dag_executions_lock:
        _dag_executions[execution_id] = execution

    with _dag_tasks_lock:
        dag["execution_count"] += 1

    return {
        "execution_id": execution_id,
        "dag_id": dag_id,
        "results": results,
        "errors": errors,
        "succeeded": len(errors) == 0,
    }


async def api_list_dag_executions(limit: int = 20, _auth: None = Depends(_verify_token)):
    """List recent DAG executions."""
    with _dag_executions_lock:
        executions = list(_dag_executions.values())

    executions = executions[-limit:]
    executions.reverse()

    return {"executions": executions, "count": len(executions)}


# ─── Multi-Tenancy / Workspace Isolation ────────────────────────────────────

# Tenant registry
_tenants: Dict[str, Dict[str, Any]] = {}
_tenants_lock = threading.Lock()

# Tenant -> resource mapping (which agents/runs belong to which tenant)
_tenant_resources: Dict[str, List[str]] = {}
_tenant_resources_lock = threading.Lock()

# Default tenant
_DEFAULT_TENANT = "default"



def register(app):
    """Register dags routes with the FastAPI app."""
    app.post("/api/dags", status_code=201)(api_create_dag)
    app.get("/api/dags")(api_list_dags)
    app.get("/api/dags/{dag_id}")(api_get_dag)
    app.delete("/api/dags/{dag_id}")(api_delete_dag)
    app.post("/api/dags/{dag_id}/execute", status_code=201)(api_execute_dag)
    app.get("/api/dags/executions")(api_list_dag_executions)
