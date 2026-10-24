"""Route module: streaming."""

import asyncio
import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import Depends, HTTPException
from starlette.responses import StreamingResponse

from hub.api.core import _scheduled_pipelines, _scheduled_pipelines_lock, _scheduled_tasks, _verify_token
from hub.runner import Run, agent_status, get_run, list_runs, recover_stale_runs, run_agent, stop_run


def _parse_Trapper_task(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a Trapper task result."""
    return task_data


async def api_Slinker_chat_stream(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Proxy: Execute a task via Slinker's chat endpoint with SSE streaming."""
    import os

    import httpx
    from fastapi.responses import StreamingResponse

    base_url = os.environ.get("Slinker_URL", "http://localhost:8000").rstrip("/")
    api_key = os.environ.get("Slinker_API_KEY", "")
    messages = body.get("messages")
    if not messages:
        raise HTTPException(status_code=400, detail="messages are required")
    payload = {"messages": messages, "stream": True}
    payload.update({k: v for k, v in body.items() if k != "messages"})
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    async def event_generator():
        try:
            async with httpx.AsyncClient(timeout=1800.0) as client:
                async with client.stream(
                    "POST", f"{base_url}/v1/chat/completions", json=payload, headers=headers
                ) as resp:
                    if resp.status_code == 503:
                        yield "data: {\"error\": \"Slinker is busy. Please try again later.\"}\n\n"
                        return
                    if resp.status_code != 200:
                        err_text = await resp.aread()
                        yield f"data: {{\"error\": \"{err_text.decode('utf-8', errors='replace')}\"}}\n\n"
                        return
                    async for line in resp.aiter_lines():
                        if line:
                            yield f"{line}\n\n"
        except httpx.ConnectError:
            yield f'data: {{"error": "Cannot connect to Slinker at {base_url}"}}\n\n'

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ─── Trapper Health Proxy ─────────────────────────────────────────────────


async def api_crawl_stream(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Start an Trapper job and stream its progress via SSE.

    Supports crawl, agent, extract, and batch_scrape operations.
    The client receives real-time updates as the job progresses.

    Example body:
    {
        "operation": "crawl",
        "payload": {"url": "https://example.com"}
    }
    """
    import os

    import httpx
    from fastapi.responses import StreamingResponse

    base_url = os.environ.get("Trapper_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("Trapper_API_KEY", "")
    operation = body.get("operation", "crawl").lower()
    payload = body.get("payload", {})

    # Map operation to endpoint
    op_endpoints = {
        "crawl": "crawl",
        "agent": "agent",
        "extract": "extract",
        "batch_scrape": "batch_scrape",
        "batch": "batch_scrape",
    }
    endpoint = op_endpoints.get(operation)
    if not endpoint:
        raise HTTPException(status_code=400, detail=f"Unsupported operation: {operation}")

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    async def event_generator():
        job_id = None
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                # Start the job
                resp = await client.post(f"{base_url}/{endpoint}", json=payload, headers=headers)
                if resp.status_code not in (200, 201):
                    yield f'data: {{"event": "error", "data": "{resp.text}"}}\n\n'
                    return

                data = resp.json()
                job_id = data.get("jobId") or data.get("job_id")
                if not job_id:
                    # Job completed immediately, return result
                    yield f'data: {{"event": "complete", "data": {json.dumps(data)}}}\n\n'
                    return

                yield f'data: {{"event": "started", "job_id": "{job_id}", "operation": "{operation}"}}\n\n'

                # Determine status endpoint
                status_endpoint = endpoint  # Same as create endpoint
                poll_interval = 2.0

                # Poll for job status
                max_polls = 600  # 20 minutes max
                for _ in range(max_polls):
                    await asyncio.sleep(poll_interval)
                    status_resp = await client.get(
                        f"{base_url}/{status_endpoint}/{job_id}", headers=headers
                    )
                    if status_resp.status_code == 200:
                        status_data = status_resp.json()
                        status = status_data.get("status", "unknown")
                        yield f'data: {json.dumps({"event": "progress", "job_id": job_id, "status": status, "data": status_data})}\n\n'

                        if status in ("completed", "success", "done"):
                            yield f'data: {json.dumps({"event": "complete", "job_id": job_id, "data": status_data})}\n\n'
                            return
                        elif status in ("failed", "error", "cancelled"):
                            yield f'data: {json.dumps({"event": "error", "job_id": job_id, "status": status, "data": status_data})}\n\n'
                            return

                        # Adjust poll interval based on nextPollMs
                        next_poll = status_data.get("nextPollMs")
                        if next_poll:
                            poll_interval = next_poll / 1000.0
                    else:
                        yield f'data: {{"event": "error", "job_id": "{job_id}", "message": "Failed to poll status"}}\n\n'
                        return

                # Max polls reached
                yield f'data: {{"event": "timeout", "job_id": "{job_id}"}}\n\n'

        except httpx.ConnectError:
            yield f'data: {{"event": "error", "message": "Cannot connect to Trapper at {base_url}"}}\n\n'
        except Exception as e:
            yield f'data: {{"event": "error", "message": "{str(e)}"}}\n\n'

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ─── Scheduled Pipelines ────────────────────────────────────────────────────

# In-memory store for scheduled pipelines
_scheduled_pipelines: Dict[str, Dict[str, Any]] = {}
_scheduled_pipelines_lock = threading.Lock()
_scheduled_tasks: Dict[str, asyncio.Task] = {}


def _cron_simple_match(cron_expr: str, now: datetime) -> bool:
    """Simple cron matcher supporting minute, hour, day-of-month, month, day-of-week.

    Supports: *, N, N-M, N/M patterns.
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return False

    fields = [
        (now.minute, "minute", 0, 59),
        (now.hour, "hour", 0, 23),
        (now.day, "day", 1, 31),
        (now.month, "month", 1, 12),
        (now.weekday(), "dow", 0, 6),
    ]

    for part, (value, name, min_val, max_val) in zip(parts, fields):
        if part == "*":
            continue
        for segment in part.split(","):
            if "/" in segment:
                range_part, step = segment.split("/", 1)
                step = int(step)
                if range_part == "*":
                    start = min_val
                elif "-" in range_part:
                    start = int(range_part.split("-")[0])
                else:
                    start = int(range_part)
                if (value - start) % step == 0:
                    return True  # Match found in this field, move to next
            elif "-" in segment:
                start, end = segment.split("-", 1)
                if int(start) <= value <= int(end):
                    continue  # Match, but check next segment
            else:
                if int(segment) == value:
                    continue  # Match
        # If we get here, no match in this field's segments
        # But we need to check if ANY segment matched
        # Re-check with proper logic
        matched = False
        for segment in part.split(","):
            if "/" in segment:
                range_part, step = segment.split("/", 1)
                step = int(step)
                if range_part == "*":
                    start = min_val
                elif "-" in range_part:
                    start = int(range_part.split("-")[0])
                else:
                    start = int(range_part)
                if (value - start) % step == 0:
                    matched = True
                    break
            elif "-" in segment:
                start, end = segment.split("-", 1)
                if int(start) <= value <= int(end):
                    matched = True
                    break
            else:
                if int(segment) == value:
                    matched = True
                    break
        if not matched:
            return False

    return True


async def _scheduled_pipeline_runner(pipeline_id: str):
    """Background task that runs a scheduled pipeline at its cron interval."""
    import httpx

    with _scheduled_pipelines_lock:
        pipeline = _scheduled_pipelines.get(pipeline_id)
    if not pipeline:
        return

    cron_expr = pipeline["schedule"]
    steps = pipeline["steps"]
    run_count = 0

    while True:
        now = datetime.now(timezone.utc)
        if _cron_simple_match(cron_expr, now):
            with _scheduled_pipelines_lock:
                pipeline = _scheduled_pipelines.get(pipeline_id)
            if not pipeline or not pipeline.get("enabled", True):
                break

            # Execute the pipeline steps
            results = []
            context = ""
            for i, step in enumerate(steps):
                service = step.get("service", "").lower()
                task = step.get("task", "")
                if not task:
                    results.append({"step": i, "service": service, "error": "task is required"})
                    continue

                effective_task = task
                if context and not task.upper().startswith(("SCRAPE:", "CRAWL:", "SEARCH:", "MAP:",
                                                             "BATCH:", "INTERACT:", "EXTRACT:", "AGENT:",
                                                             "TASK:", "STATUS:", "WORKERS:", "QUEUE:")):
                    effective_task = f"## Context from previous steps\n\n{context}\n\n---\n\n## Current Task\n\n{task}"

                step_result = {"step": i, "service": service}

                try:
                    if service == "Trapper":
                        base_url = os.environ.get("Trapper_URL", "http://localhost:3000").rstrip("/")
                        api_key = os.environ.get("Trapper_API_KEY", "")
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
                                step_result["status"] = "ok"
                                step_result["data"] = data.get("data", data)
                                step_result["context"] = str(data.get("data", ""))[:2000]
                            else:
                                step_result["status"] = "error"
                                step_result["error"] = resp.text
                    elif service == "Slinker":
                        base_url = os.environ.get("Slinker_URL", "http://localhost:8000").rstrip("/")
                        api_key = os.environ.get("Slinker_API_KEY", "")
                        headers = {"Content-Type": "application/json"}
                        if api_key:
                            headers["Authorization"] = f"Bearer {api_key}"
                        messages = [{"role": "user", "content": effective_task}]
                        payload = {"messages": messages, "stream": False}
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
                    elif service == "agent":
                        agent_name = step.get("agent", "")
                        if agent_name:
                            run = run_agent(agent_name=agent_name, task=effective_task)
                            step_result["status"] = "ok" if run.state == "done" else "error"
                            step_result["answer"] = run.answer
                            step_result["context"] = run.answer[:2000]
                            step_result["run_id"] = run.id
                        else:
                            step_result["status"] = "error"
                            step_result["error"] = "agent name is required"
                    else:
                        step_result["status"] = "error"
                        step_result["error"] = f"Unknown service: {service}"
                except Exception as e:
                    step_result["status"] = "error"
                    step_result["error"] = str(e)

                if step_result.get("context"):
                    context += f"\n\n### Step {i} ({service})\n\n{step_result['context']}"
                    context = context[-4000:]
                results.append(step_result)

            run_count += 1
            with _scheduled_pipelines_lock:
                pipeline["run_count"] = run_count
                pipeline["last_run"] = now.isoformat()
                pipeline["last_result"] = results

        await asyncio.sleep(10)  # Check every 10 seconds


async def api_Slinker_task_stream(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Submit a task to Slinker and stream its output via SSE.

    The task is submitted, then polled until completion with real-time
    progress updates streamed to the client.

    Example body:
    {
        "task": "Refactor the authentication module",
        "project": "my-project"
    }
    """
    import os

    import httpx
    from fastapi.responses import StreamingResponse

    base_url = os.environ.get("Slinker_URL", "http://localhost:8000").rstrip("/")
    api_key = os.environ.get("Slinker_API_KEY", "")
    task_content = body.get("task", "")
    project = body.get("project", "")

    if not task_content:
        raise HTTPException(status_code=400, detail="task is required")

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {"task": task_content}
    if project:
        payload["project"] = project

    async def event_generator():
        task_id = None
        try:
            async with httpx.AsyncClient(timeout=1800.0) as client:
                # Submit the task
                resp = await client.post(f"{base_url}/api/v1/tasks", json=payload, headers=headers)
                if resp.status_code not in (200, 201):
                    yield f'data: {{"event": "error", "data": "{resp.text}"}}\n\n'
                    return

                data = resp.json()
                task_id = data.get("task_id")
                if not task_id:
                    yield f'data: {json.dumps({"event": "complete", "data": data})}\n\n'
                    return

                yield f'data: {json.dumps({"event": "started", "task_id": task_id})}\n\n'

                # Poll for task completion
                max_polls = 900  # 30 minutes max
                for _ in range(max_polls):
                    await asyncio.sleep(2)
                    status_resp = await client.get(f"{base_url}/api/v1/tasks/{task_id}", headers=headers)
                    if status_resp.status_code == 200:
                        status_data = status_resp.json()
                        status = status_data.get("status", "unknown")
                        yield f'data: {json.dumps({"event": "progress", "task_id": task_id, "status": status, "data": status_data})}\n\n'

                        if status in ("completed", "done", "success"):
                            output_resp = await client.get(f"{base_url}/api/v1/tasks/{task_id}/output", headers=headers)
                            output_data = output_resp.json() if output_resp.status_code == 200 else {}
                            yield f'data: {json.dumps({"event": "complete", "task_id": task_id, "output": output_data})}\n\n'
                            return
                        elif status in ("failed", "error", "cancelled"):
                            yield f'data: {json.dumps({"event": "error", "task_id": task_id, "status": status, "data": status_data})}\n\n'
                            return

                yield f'data: {{"event": "timeout", "task_id": "{task_id}"}}\n\n'

        except httpx.ConnectError:
            yield f'data: {{"event": "error", "message": "Cannot connect to Slinker at {base_url}"}}\n\n'
        except Exception as e:
            yield f'data: {{"event": "error", "message": "{str(e)}"}}\n\n'

    return StreamingResponse(event_generator(), media_type="text/event-stream")



def register(app):
    """Register streaming routes with the FastAPI app."""
    app.post("/api/Slinker/chat/stream")(api_Slinker_chat_stream)
    app.post("/api/crawl/stream")(api_crawl_stream)
    app.post("/api/Slinker/task/stream")(api_Slinker_task_stream)
