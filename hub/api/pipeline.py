"""Route module: pipeline."""

import asyncio
import json
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx
from fastapi import Depends, HTTPException

from hub.api.core import _verify_token
from hub.config import load_config, save_config, set_primary_bot, validate_name
from hub.runner import Run, agent_status, get_run, list_runs, recover_stale_runs, run_agent, stop_run


async def api_dashboard(_auth: None = Depends(_verify_token)):
    """Unified dashboard aggregating status from all three services.

    Returns a single view of Greenie agents, Slinker workers and queue,
    and Trapper health — useful for monitoring the entire platform.
    """
    import asyncio
    import os

    import httpx

    async def _Slinker_dashboard():
        base_url = os.environ.get("SLINKER_URL", "http://localhost:8000").rstrip("/")
        api_key = os.environ.get("SLINKER_API_KEY", "")
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                health_resp = await client.get(f"{base_url}/health")
                queue_resp = await client.get(f"{base_url}/api/v1/queue", headers=headers)
                workers_resp = await client.get(f"{base_url}/api/v1/workers", headers=headers)
                tasks_resp = await client.get(
                    f"{base_url}/api/v1/tasks", headers=headers, params={"limit": 10}
                )
                return {
                    "health": "ok" if health_resp.status_code == 200 else "error",
                    "queue": queue_resp.json() if queue_resp.status_code == 200 else None,
                    "workers": workers_resp.json() if workers_resp.status_code == 200 else [],
                    "recent_tasks": tasks_resp.json() if tasks_resp.status_code == 200 else [],
                }
        except Exception as e:
            return {"health": "unreachable", "error": str(e)}

    async def _Trapper_dashboard():
        base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{base_url}/health")
                return {
                    "health": "ok" if resp.status_code == 200 else "error",
                    "details": resp.json() if resp.status_code == 200 else None,
                }
        except Exception as e:
            return {"health": "unreachable", "error": str(e)}

    async def _evershade_valley_dashboard():
        config = load_config()
        agent_count = len(config.get("agents", {}))
        recent_runs = list_runs(limit=10)
        return {
            "agents_count": agent_count,
            "recent_runs": [r.to_dict() for r in recent_runs],
        }

    Slinker, Trapper, evershade_valley = await asyncio.gather(
        _Slinker_dashboard(), _Trapper_dashboard(), _evershade_valley_dashboard()
    )

    return {
        "Greenie": evershade_valley,
        "Slinker": Slinker,
        "Trapper": Trapper,
    }


# ─── Slinker Streaming Chat ────────────────────────────────────────────────


async def api_create_pipeline(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Create and execute a cross-service pipeline.

    A pipeline chains multiple steps that can use different services:
    - "Trapper" steps: web research via Trapper
    - "Slinker" steps: coding tasks via Slinker
    - "agent" steps: local Greenie agents

    Example body:
    {
        "steps": [
            {
                "service": "Trapper",
                "task": "SEARCH: latest Python async best practices"
            },
            {
                "service": "Slinker",
                "task": "Create a summary document of the research findings"
            }
        ]
    }
    """
    import os

    import httpx

    steps = body.get("steps", [])
    if not steps:
        raise HTTPException(status_code=400, detail="steps are required")

    results = []
    context = ""  # Accumulated context passed between steps

    for i, step in enumerate(steps):
        service = step.get("service", "").lower()
        task = step.get("task", "")
        if not task:
            results.append({"step": i, "service": service, "error": "task is required"})
            continue

        # Prepend context from previous steps
        effective_task = task
        if context and not task.upper().startswith(("SCRAPE:", "CRAWL:", "SEARCH:", "MAP:",
                                                     "BATCH:", "INTERACT:", "EXTRACT:", "AGENT:",
                                                     "TASK:", "STATUS:", "WORKERS:", "QUEUE:")):
            effective_task = f"## Context from previous steps\n\n{context}\n\n---\n\n## Current Task\n\n{task}"

        step_result = {"step": i, "service": service}

        if service == "Trapper":
            base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
            api_key = os.environ.get("TRAPPER_API_KEY", "")
            headers = {"Content-Type": "application/json"}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"

            # Parse operation from task
            op, target = _parse_Trapper_task(task)

            try:
                async with httpx.AsyncClient(timeout=120.0) as client:
                    if op in ("scrape",):
                        resp = await client.post(f"{base_url}/scrape", json={"url": target}, headers=headers)
                    elif op in ("search",):
                        resp = await client.post(f"{base_url}/search", json={"query": target}, headers=headers)
                    elif op in ("map",):
                        resp = await client.post(f"{base_url}/map", json={"url": target}, headers=headers)
                    elif op in ("crawl",):
                        resp = await client.post(f"{base_url}/crawl", json={"url": target}, headers=headers)
                    elif op in ("agent",):
                        payload = {"prompt": target} if not target.startswith("{") else json.loads(target)
                        resp = await client.post(f"{base_url}/agent", json=payload, headers=headers)
                    else:
                        # Default to search for unknown ops
                        resp = await client.post(f"{base_url}/search", json={"query": target}, headers=headers)

                    if resp.status_code in (200, 201):
                        data = resp.json()
                        step_result["status"] = "ok"
                        step_result["data"] = data.get("data", data)
                        # Extract text for context
                        if isinstance(data.get("data"), dict):
                            step_result["context"] = str(data.get("data", ""))[:2000]
                        elif isinstance(data.get("data"), list):
                            step_result["context"] = str(data.get("data", ""))[:2000]
                        else:
                            step_result["context"] = str(data)[:2000]
                    else:
                        step_result["status"] = "error"
                        step_result["error"] = resp.text
            except Exception as e:
                step_result["status"] = "error"
                step_result["error"] = str(e)

        elif service == "Slinker":
            base_url = os.environ.get("SLINKER_URL", "http://localhost:8000").rstrip("/")
            api_key = os.environ.get("SLINKER_API_KEY", "")
            headers = {"Content-Type": "application/json"}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"

            messages = [{"role": "user", "content": effective_task}]
            payload = {"messages": messages, "stream": False}

            try:
                async with httpx.AsyncClient(timeout=1800.0) as client:
                    resp = await client.post(f"{base_url}/v1/chat/completions", json=payload, headers=headers)
                    if resp.status_code == 200:
                        data = resp.json()
                        choices = data.get("choices", [])
                        answer = choices[0].get("message", {}).get("content", "") if choices else ""
                        step_result["status"] = "ok"
                        step_result["answer"] = answer
                        step_result["context"] = answer[:2000]
                    else:
                        step_result["status"] = "error"
                        step_result["error"] = resp.text
            except Exception as e:
                step_result["status"] = "error"
                step_result["error"] = str(e)

        elif service == "agent":
            agent_name = step.get("agent", "")
            if not agent_name:
                step_result["error"] = "agent name is required for agent service"
                step_result["status"] = "error"
            else:
                try:
                    run = run_agent(agent_name=agent_name, task=effective_task)
                    step_result["status"] = "ok" if run.state == "done" else "error"
                    step_result["answer"] = run.answer
                    step_result["context"] = run.answer[:2000]
                    step_result["run_id"] = run.id
                    if run.state != "done":
                        step_result["error"] = run.fail_reason
                except Exception as e:
                    step_result["status"] = "error"
                    step_result["error"] = str(e)
        else:
            step_result["status"] = "error"
            step_result["error"] = f"Unknown service: {service}"

        # Accumulate context for next steps
        if step_result.get("context"):
            context += f"\n\n### Step {i} ({service})\n\n{step_result['context']}"
            context = context[-4000:]  # Keep last 4000 chars to avoid bloating

        results.append(step_result)

    return {
        "pipeline_id": f"pipe-{uuid.uuid4().hex[:8]}",
        "steps": results,
        "completed": all(r.get("status") in ("ok", "error") for r in results),
    }


def _parse_Trapper_task(task: str) -> tuple[str, str]:
    """Parse an Trapper task string to determine the operation and target."""
    task_upper = task.upper().strip()
    prefixes = ["SCRAPE:", "CRAWL:", "SEARCH:", "MAP:", "BATCH:", "INTERACT:", "EXTRACT:", "AGENT:"]
    for prefix in prefixes:
        if task_upper.startswith(prefix):
            return prefix[:-1].lower(), task[len(prefix):].strip()
    # Default to search
    return "search", task


# ─── Trapper SSE Job Streaming ────────────────────────────────────────────


async def api_unified_search(q: str, _auth: None = Depends(_verify_token)):
    """Unified search across all three services.

    Searches Trapper for web content, Slinker for tasks and projects,
    and Greenie for agents and runs.

    Query parameters:
    - q: Search query string
    - services: Comma-separated list of services to search (default: all)
    """
    import asyncio
    import os

    import httpx

    services_param = os.environ.get("SEARCH_SERVICES", "all").lower()
    search_Trapper = services_param in ("all", "Trapper")
    search_Slinker = services_param in ("all", "Slinker")
    search_evershade_valley = services_param in ("all", "Greenie")

    async def _search_Trapper():
        if not search_Trapper:
            return {"service": "Trapper", "results": []}
        base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
        api_key = os.environ.get("TRAPPER_API_KEY", "")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(f"{base_url}/search", json={"query": q}, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    return {"service": "Trapper", "results": data.get("data", [])}
                return {"service": "Trapper", "error": "Search failed"}
        except Exception as e:
            return {"service": "Trapper", "error": str(e)}

    async def _search_Slinker():
        if not search_Slinker:
            return {"service": "Slinker", "results": []}
        base_url = os.environ.get("SLINKER_URL", "http://localhost:8000").rstrip("/")
        api_key = os.environ.get("SLINKER_API_KEY", "")
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                tasks_resp = await client.get(f"{base_url}/api/v1/tasks", headers=headers, params={"limit": 50})
                projects_resp = await client.get(f"{base_url}/api/v1/projects", headers=headers)
                results = []
                if tasks_resp.status_code == 200:
                    tasks = tasks_resp.json()
                    q_lower = q.lower()
                    for t in tasks:
                        task_str = json.dumps(t).lower()
                        if q_lower in task_str:
                            results.append({"type": "task", "data": t})
                if projects_resp.status_code == 200:
                    projects = projects_resp.json()
                    q_lower = q.lower()
                    for p in projects:
                        project_str = json.dumps(p).lower()
                        if q_lower in project_str:
                            results.append({"type": "project", "data": p})
                return {"service": "Slinker", "results": results}
        except Exception as e:
            return {"service": "Slinker", "error": str(e)}

    async def _search_evershade_valley():
        if not search_evershade_valley:
            return {"service": "Greenie", "results": []}
        results = []
        q_lower = q.lower()
        try:
            config = load_config()
            for name, data in config.get("agents", {}).items():
                if q_lower in name.lower() or q_lower in json.dumps(data).lower():
                    results.append({"type": "agent", "name": name, "data": data})
            runs = list_runs(limit=50)
            for r in runs:
                rd = r.to_dict()
                if q_lower in json.dumps(rd).lower():
                    results.append({"type": "run", "data": rd})
        except Exception as e:
            pass
        return {"service": "Greenie", "results": results}

    Trapper_results, Slinker_results, evershade_valley_results = await asyncio.gather(
        _search_Trapper(), _search_Slinker(), _search_evershade_valley()
    )

    return {
        "query": q,
        "results": [Trapper_results, Slinker_results, evershade_valley_results],
    }


# ─── Parallel Batch Execution ───────────────────────────────────────────────


async def api_batch_execute(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Execute multiple tasks in parallel across services.

    Unlike pipelines (sequential), batch executes all tasks concurrently.
    Each task specifies its target service and task definition.

    Example body:
    {
        "tasks": [
            {"service": "Trapper", "task": "SEARCH: latest Python news"},
            {"service": "Trapper", "task": "SEARCH: latest Rust news"},
            {"service": "Slinker", "task": "Compare Python vs Rust performance"}
        ]
    }
    """
    import os

    import httpx

    tasks = body.get("tasks", [])
    if not tasks:
        raise HTTPException(status_code=400, detail="tasks are required")

    batch_id = f"batch-{uuid.uuid4().hex[:8]}"

    async def _execute_task(idx: int, task_def: Dict[str, Any]) -> Dict[str, Any]:
        service = task_def.get("service", "").lower()
        task = task_def.get("task", "")
        result = {"index": idx, "service": service}

        if not task:
            result["error"] = "task is required"
            return result

        try:
            if service == "Trapper":
                base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
                api_key = os.environ.get("TRAPPER_API_KEY", "")
                headers = {"Content-Type": "application/json"}
                if api_key:
                    headers["Authorization"] = f"Bearer {api_key}"
                op, target = _parse_Trapper_task(task)
                async with httpx.AsyncClient(timeout=120.0) as client:
                    if op in ("scrape",):
                        resp = await client.post(f"{base_url}/scrape", json={"url": target}, headers=headers)
                    elif op in ("search",):
                        resp = await client.post(f"{base_url}/search", json={"query": target}, headers=headers)
                    elif op in ("map",):
                        resp = await client.post(f"{base_url}/map", json={"url": target}, headers=headers)
                    elif op in ("crawl",):
                        resp = await client.post(f"{base_url}/crawl", json={"url": target}, headers=headers)
                    elif op in ("agent",):
                        payload = {"prompt": target} if not target.startswith("{") else json.loads(target)
                        resp = await client.post(f"{base_url}/agent", json=payload, headers=headers)
                    else:
                        resp = await client.post(f"{base_url}/search", json={"query": target}, headers=headers)
                    if resp.status_code in (200, 201):
                        data = resp.json()
                        result["status"] = "ok"
                        result["data"] = data.get("data", data)
                    else:
                        result["status"] = "error"
                        result["error"] = resp.text
            elif service == "Slinker":
                base_url = os.environ.get("SLINKER_URL", "http://localhost:8000").rstrip("/")
                api_key = os.environ.get("SLINKER_API_KEY", "")
                headers = {"Content-Type": "application/json"}
                if api_key:
                    headers["Authorization"] = f"Bearer {api_key}"
                messages = [{"role": "user", "content": task}]
                payload = {"messages": messages, "stream": False}
                async with httpx.AsyncClient(timeout=1800.0) as client:
                    resp = await client.post(f"{base_url}/v1/chat/completions", json=payload, headers=headers)
                    if resp.status_code == 200:
                        data = resp.json()
                        choices = data.get("choices", [])
                        answer = choices[0].get("message", {}).get("content", "") if choices else ""
                        result["status"] = "ok"
                        result["answer"] = answer
                    else:
                        result["status"] = "error"
                        result["error"] = resp.text
            elif service == "agent":
                agent_name = task_def.get("agent", "")
                if agent_name:
                    run = run_agent(agent_name=agent_name, task=task)
                    result["status"] = "ok" if run.state == "done" else "error"
                    result["answer"] = run.answer
                    result["run_id"] = run.id
                else:
                    result["status"] = "error"
                    result["error"] = "agent name is required"
            else:
                result["status"] = "error"
                result["error"] = f"Unknown service: {service}"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    # Execute all tasks in parallel
    results = await asyncio.gather(
        *[_execute_task(i, t) for i, t in enumerate(tasks)]
    )

    return {
        "batch_id": batch_id,
        "results": list(results),
        "completed": all(r.get("status") in ("ok", "error") for r in results),
    }


# ─── Slinker Task Output Stream ────────────────────────────────────────────



def register(app):
    """Register pipeline routes with the FastAPI app."""
    app.get("/api/dashboard")(api_dashboard)
    app.post("/api/pipeline", status_code=201)(api_create_pipeline)
    app.get("/api/search")(api_unified_search)
    app.post("/api/batch", status_code=201)(api_batch_execute)
