"""Workflow executor — runs workflows step-by-step with approval gates.

The executor takes a WorkflowDefinition and creates a WorkflowRun, executing
each step in order. It supports:
- Agent steps: runs a Greenie agent via the runner
- Service steps: calls Trapper, Slinker, or other services via HTTP
- Approval steps: pauses execution waiting for human approval
- Condition steps: evaluates simple conditions to skip/include subsequent steps
- Notify steps: sends notifications via Poltergeist

Execution state is persisted to SQLite after each step for recovery.
"""

import asyncio
import json
import logging
import os
import re
import shlex
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from hub.workflow import storage
from hub.workflow.models import (
    ApprovalStatus,
    RunStatus,
    StepStatus,
    StepType,
    WorkflowApproval,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStep,
    WorkflowStepRun,
)

logger = logging.getLogger("hub.workflow.executor")


class WorkflowExecutor:
    """Executes workflow definitions step-by-step."""

    def __init__(self):
        # Pending approvals: run_id -> event for resuming
        self._pending_events: dict[str, asyncio.Event] = {}
        self._pending_results: dict[str, str] = {}  # run_id -> "approved" | "denied"
        # Background tasks for workflow execution
        self._background_tasks: set[asyncio.Task] = set()

    async def execute(
        self,
        workflow: WorkflowDefinition,
        trigger_type: str = "manual",
        trigger_data: Optional[dict[str, Any]] = None,
        started_by: str = "api",
    ) -> WorkflowRun:
        """Start executing a workflow and return the run.

        This method creates the run, saves it, and schedules the background
        execution task. The returned run object will be updated as execution
        progresses.
        """
        # Create the run
        now = datetime.now(timezone.utc).isoformat()
        run = WorkflowRun(
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            status=RunStatus.RUNNING.value,
            trigger_type=trigger_type,
            trigger_data=trigger_data or {},
            started_at=now,
            started_by=started_by,
        )

        # Create step run records
        for step in workflow.steps:
            step_run = WorkflowStepRun(
                step_id=step.id,
                step_name=step.name or step.step_type,
            )
            run.step_runs.append(step_run)

        # Save initial state
        storage.save_workflow_run(run)
        logger.info(f"Workflow run started: {run.id} for workflow {workflow.id}")

        # Execute in background - store task reference to prevent silent failures
        task = asyncio.create_task(self._run_steps(workflow, run))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return run

    async def _run_steps(self, workflow: WorkflowDefinition, run: WorkflowRun) -> None:
        """Execute all steps in the workflow.

        If steps have milestone_id set, execute worker steps in parallel within
        each milestone, then run the validator after all workers complete.
        Otherwise fall back to sequential execution.
        """
        # Check if parallel execution is enabled (milestones indicate parallel intent)
        has_milestones = any(s.milestone_id for s in workflow.steps)

        if has_milestones:
            await self._run_parallel(workflow, run)
        else:
            await self._run_sequential(workflow, run)

    async def _run_sequential(self, workflow: WorkflowDefinition, run: WorkflowRun) -> None:
        """Execute all steps in the workflow sequentially (original behavior)."""
        start_time = time.time()
        context: dict[str, Any] = {}  # Accumulated context from previous steps

        try:
            for i, step in enumerate(workflow.steps):
                # Check if run was cancelled
                run = storage.get_workflow_run(run.id)
                if run is None or run.status == RunStatus.CANCELLED.value:
                    logger.info(f"Run {run.id} was cancelled, stopping execution")
                    break

                # Check dependencies
                if step.depends_on:
                    dep_statuses = []
                    for sr in run.step_runs:
                        if sr.step_id in step.depends_on:
                            dep_statuses.append((sr.step_id, sr.status))
                    # Skip if any dependency failed
                    if any(s == StepStatus.FAILED.value for _, s in dep_statuses):
                        run.step_runs[i].status = StepStatus.SKIPPED.value
                        run.step_runs[i].completed_at = datetime.now(timezone.utc).isoformat()
                        storage.save_workflow_run(run)
                        continue

                # Check condition
                if step.step_type == StepType.CONDITION.value and step.condition:
                    if not self._evaluate_condition(step.condition, context):
                        run.step_runs[i].status = StepStatus.SKIPPED.value
                        run.step_runs[i].completed_at = datetime.now(timezone.utc).isoformat()
                        storage.save_workflow_run(run)
                        continue

                # Execute step
                run.current_step_index = i
                run.step_runs[i].status = StepStatus.RUNNING.value
                run.step_runs[i].started_at = datetime.now(timezone.utc).isoformat()
                storage.save_workflow_run(run)

                # Determine max retries and timeout for this step
                max_retries = step.config.get("max_retries", 0)
                step_timeout = step.config.get("timeout_seconds", 0)  # 0 = no timeout

                last_exception = None
                executed = False

                for attempt in range(max_retries + 1):
                    try:
                        async def _execute_step():
                            if step.step_type == StepType.APPROVAL.value:
                                result = await self._wait_for_approval(run, step)
                            elif step.step_type == StepType.AGENT.value:
                                result = await self._execute_agent(step, context)
                            elif step.step_type == StepType.SERVICE.value:
                                result = await self._execute_service(step, context)
                            elif step.step_type == StepType.NOTIFY.value:
                                result = await self._execute_notify(step, context)
                            elif step.step_type == StepType.WORKSTATION.value:
                                result = await self._execute_workstation(step, run, context)
                            elif step.step_type == StepType.CONDITION.value:
                                result = {"evaluated": True}
                            else:
                                result = {"error": f"Unknown step type: {step.step_type}"}
                            return result

                        # Apply timeout if configured
                        if step_timeout and step_timeout > 0:
                            result = await asyncio.wait_for(_execute_step(), timeout=step_timeout)
                        else:
                            result = await _execute_step()

                        executed = True

                        # Step completed
                        run.step_runs[i].status = StepStatus.COMPLETED.value
                        run.step_runs[i].output = json.dumps(result, default=str)[:10000]
                        run.step_runs[i].completed_at = datetime.now(timezone.utc).isoformat()

                        # Accumulate context
                        context[f"step_{i}"] = result
                        context[f"step_{step.name}"] = result
                        break  # Success, exit retry loop

                    except asyncio.TimeoutError:
                        last_exception = asyncio.TimeoutError(f"Step {step.name} timed out after {step_timeout}s")
                        logger.warning(f"Step {i} ({step.name}) attempt {attempt + 1}/{max_retries + 1} timed out")
                        if attempt < max_retries:
                            wait_time = min(2 ** attempt, 30)  # Exponential backoff, max 30s
                            logger.info(f"Retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                        continue

                    except Exception as e:
                        last_exception = e
                        logger.warning(f"Step {i} ({step.name}) attempt {attempt + 1}/{max_retries + 1} failed: {e}")
                        if attempt < max_retries:
                            wait_time = min(2 ** attempt, 30)  # Exponential backoff, max 30s
                            logger.info(f"Retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                        continue

                # If we get here and didn't execute successfully
                if not executed and last_exception is not None:
                    run.step_runs[i].status = StepStatus.FAILED.value
                    run.step_runs[i].error = str(last_exception)[:2000]
                    run.step_runs[i].completed_at = datetime.now(timezone.utc).isoformat()

                    # Check on_failure policy
                    if step.on_failure == "stop":
                        run.status = RunStatus.FAILED.value
                        run.error = f"Step {step.name} failed: {last_exception}"
                        break

                step_duration = time.time() - start_time
                run.step_runs[i].duration_seconds = round(step_duration, 2)
                storage.save_workflow_run(run)

            # Mark run as completed if not already failed/cancelled
            if run.status == RunStatus.RUNNING.value:
                run.status = RunStatus.COMPLETED.value

        except Exception as e:
            logger.error(f"Workflow run {run.id} error: {e}")
            if run.status == RunStatus.RUNNING.value:
                run.status = RunStatus.FAILED.value
                run.error = str(e)[:2000]

        finally:
            run.completed_at = datetime.now(timezone.utc).isoformat()
            run.duration_seconds = round(time.time() - start_time, 2)
            storage.save_workflow_run(run)
            logger.info(
                f"Workflow run {run.id} finished with status {run.status} "
                f"in {run.duration_seconds:.1f}s"
            )

    async def _run_parallel(self, workflow: WorkflowDefinition, run: WorkflowRun) -> None:
        """Execute steps grouped by milestone with parallel workers.

        Within each milestone, worker steps run concurrently. The validator
        (if present) runs only after all workers complete.
        """
        start_time = time.time()
        context: dict[str, Any] = {}

        # Group steps by milestone
        milestones: dict[str, list[tuple[int, WorkflowStep]]] = {}
        for i, step in enumerate(workflow.steps):
            ms_id = step.milestone_id or "default"
            milestones.setdefault(ms_id, []).append((i, step))

        try:
            for ms_id, steps in milestones.items():
                # Check cancellation
                run = storage.get_workflow_run(run.id)
                if run is None or run.status == RunStatus.CANCELLED.value:
                    break

                # Separate workers from validators
                workers = [(i, s) for i, s in steps if s.role != "validator"]
                validators = [(i, s) for i, s in steps if s.role == "validator"]

                # Run workers in parallel
                if workers:
                    tasks = [self._execute_single_step(workflow, run, i, s, context) for i, s in workers]
                    await asyncio.gather(*tasks, return_exceptions=True)

                    # Check if any worker failed fatally
                    for i, s in workers:
                        sr = run.step_runs[i]
                        if sr.status == StepStatus.FAILED.value and s.on_failure == "stop":
                            run.status = RunStatus.FAILED.value
                            run.error = f"Worker {s.name} failed"
                            storage.save_workflow_run(run)
                            return

                # Run validators sequentially after workers
                for i, s in validators:
                    run = storage.get_workflow_run(run.id)
                    if run is None or run.status == RunStatus.CANCELLED.value:
                        break
                    await self._execute_single_step(workflow, run, i, s, context)

            # Mark run as completed
            if run.status == RunStatus.RUNNING.value:
                run.status = RunStatus.COMPLETED.value

        except Exception as e:
            logger.error(f"Workflow run {run.id} parallel error: {e}")
            if run.status == RunStatus.RUNNING.value:
                run.status = RunStatus.FAILED.value
                run.error = str(e)[:2000]

        finally:
            run.completed_at = datetime.now(timezone.utc).isoformat()
            run.duration_seconds = round(time.time() - start_time, 2)
            storage.save_workflow_run(run)
            logger.info(
                f"Workflow run {run.id} finished (parallel) with status {run.status} "
                f"in {run.duration_seconds:.1f}s"
            )

    async def _execute_single_step(
        self, workflow: WorkflowDefinition, run: WorkflowRun, i: int, step: WorkflowStep, context: dict[str, Any]
    ) -> None:
        """Execute a single step within parallel or sequential mode."""
        # Check dependencies
        if step.depends_on:
            dep_statuses = []
            for sr in run.step_runs:
                if sr.step_id in step.depends_on:
                    dep_statuses.append((sr.step_id, sr.status))
            if any(s == StepStatus.FAILED.value for _, s in dep_statuses):
                run.step_runs[i].status = StepStatus.SKIPPED.value
                run.step_runs[i].completed_at = datetime.now(timezone.utc).isoformat()
                storage.save_workflow_run(run)
                return

        # Check condition
        if step.step_type == StepType.CONDITION.value and step.condition:
            if not self._evaluate_condition(step.condition, context):
                run.step_runs[i].status = StepStatus.SKIPPED.value
                run.step_runs[i].completed_at = datetime.now(timezone.utc).isoformat()
                storage.save_workflow_run(run)
                return

        # Execute
        run.step_runs[i].status = StepStatus.RUNNING.value
        run.step_runs[i].started_at = datetime.now(timezone.utc).isoformat()
        storage.save_workflow_run(run)

        # Determine max retries and timeout for this step
        max_retries = step.config.get("max_retries", 0)
        step_timeout = step.config.get("timeout_seconds", 0)  # 0 = no timeout

        last_exception = None
        executed = False

        for attempt in range(max_retries + 1):
            try:
                async def _execute_step():
                    if step.step_type == StepType.APPROVAL.value:
                        result = await self._wait_for_approval(run, step)
                    elif step.step_type == StepType.AGENT.value:
                        result = await self._execute_agent(step, context)
                    elif step.step_type == StepType.SERVICE.value:
                        result = await self._execute_service(step, context)
                    elif step.step_type == StepType.NOTIFY.value:
                        result = await self._execute_notify(step, context)
                    elif step.step_type == StepType.WORKSTATION.value:
                        result = await self._execute_workstation(step, run, context)
                    elif step.step_type in (StepType.PLANNER.value, StepType.WORKER.value, StepType.VALIDATOR.value):
                        # Mission control steps — delegate to agent or service
                        if step.agent:
                            result = await self._execute_agent(step, context)
                        elif step.service:
                            result = await self._execute_service(step, context)
                        else:
                            result = {"role": step.role, "status": "completed"}
                    elif step.step_type == StepType.MILESTONE.value:
                        result = {"milestone": step.milestone_id, "status": "passed"}
                    elif step.step_type == StepType.CONDITION.value:
                        result = {"evaluated": True}
                    else:
                        result = {"error": f"Unknown step type: {step.step_type}"}
                    return result

                # Apply timeout if configured
                if step_timeout and step_timeout > 0:
                    result = await asyncio.wait_for(_execute_step(), timeout=step_timeout)
                else:
                    result = await _execute_step()

                executed = True
                run.step_runs[i].status = StepStatus.COMPLETED.value
                run.step_runs[i].output = json.dumps(result, default=str)[:10000]
                run.step_runs[i].completed_at = datetime.now(timezone.utc).isoformat()

                # Accumulate context
                context[f"step_{i}"] = result
                context[f"step_{step.name}"] = result
                break  # Success, exit retry loop

            except asyncio.TimeoutError:
                last_exception = asyncio.TimeoutError(f"Step {step.name} timed out after {step_timeout}s")
                logger.warning(f"Step {i} ({step.name}) attempt {attempt + 1}/{max_retries + 1} timed out")
                if attempt < max_retries:
                    wait_time = min(2 ** attempt, 30)
                    logger.info(f"Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                continue

            except Exception as e:
                last_exception = e
                logger.warning(f"Step {i} ({step.name}) attempt {attempt + 1}/{max_retries + 1} failed: {e}")
                if attempt < max_retries:
                    wait_time = min(2 ** attempt, 30)
                    logger.info(f"Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                continue

        # If we get here and didn't execute successfully
        if not executed and last_exception is not None:
            run.step_runs[i].status = StepStatus.FAILED.value
            run.step_runs[i].error = str(last_exception)[:2000]
            run.step_runs[i].completed_at = datetime.now(timezone.utc).isoformat()

        step_duration = time.time() - (getattr(run, "_start_time", None) or time.time())
        run.step_runs[i].duration_seconds = round(step_duration, 2)
        storage.save_workflow_run(run)

    async def _execute_agent(self, step: WorkflowStep, context: dict[str, Any]) -> dict[str, Any]:
        """Execute an agent step by running a Greenie agent."""
        agent_name = step.agent
        if not agent_name:
            raise ValueError("Agent step requires 'agent' field")

        task = step.task
        if not task:
            raise ValueError("Agent step requires 'task' field")

        # Prepend context from previous steps
        if context:
            context_text = json.dumps(context, default=str, indent=2)[:4000]
            task = f"## Context from previous steps\n\n{context_text}\n\n---\n\n## Current Task\n\n{task}"

        # Import runner lazily
        from hub.runner import run_agent
        run = run_agent(agent_name=agent_name, task=task)
        return {
            "agent": agent_name,
            "run_id": run.id,
            "state": run.state,
            "answer": run.answer[:5000] if run.answer else "",
        }

    async def _execute_service(self, step: WorkflowStep, context: dict[str, Any]) -> dict[str, Any]:
        """Execute a service step by calling an external service."""
        service = step.service.lower()
        task = step.task
        if not task:
            raise ValueError("Service step requires 'task' field")

        # Prepend context
        if context:
            context_text = json.dumps(context, default=str, indent=2)[:4000]
            task = f"## Context\n\n{context_text}\n\n---\n\n## Task\n\n{task}"

        import httpx

        async with httpx.AsyncClient(timeout=120.0) as client:
            if service == "trapper":
                return await self._call_trapper(client, task)
            elif service == "slinker":
                return await self._call_slinker(client, task)
            elif service == "poltergeist":
                return await self._call_poltergeist(client, task)
            elif service == "shrewd":
                return await self._call_shrewd(client, task)
            else:
                raise ValueError(f"Unknown service: {service}")

    async def _call_trapper(self, client: "httpx.AsyncClient", task: str) -> dict[str, Any]:
        """Call Trapper service."""
        base_url = os.environ.get("TRAPPER_URL", "http://trapper-api:3000").rstrip("/")
        api_key = os.environ.get("TRAPPER_API_KEY", "")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        # Parse operation
        op, target = self._parse_trapper_task(task)
        if op in ("scrape", "crawl", "map"):
            resp = await client.post(f"{base_url}/{op}", json={"url": target}, headers=headers)
        elif op == "agent":
            payload = {"prompt": target} if not target.startswith("{") else json.loads(target)
            resp = await client.post(f"{base_url}/agent", json=payload, headers=headers)
        else:
            resp = await client.post(f"{base_url}/search", json={"query": target}, headers=headers)

        if resp.status_code in (200, 201):
            return resp.json()
        else:
            raise RuntimeError(f"Trapper returned {resp.status_code}: {resp.text[:500]}")

    async def _call_slinker(self, client: "httpx.AsyncClient", task: str) -> dict[str, Any]:
        """Call Slinker service."""
        base_url = os.environ.get("SLINKER_URL", "http://slinker:8000").rstrip("/")
        api_key = os.environ.get("SLINKER_API_KEY", "")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        resp = await client.post(
            f"{base_url}/v1/chat/completions",
            json={"messages": [{"role": "user", "content": task}], "stream": False},
            headers=headers,
        )
        if resp.status_code == 200:
            data = resp.json()
            choices = data.get("choices", [])
            answer = choices[0].get("message", {}).get("content", "") if choices else ""
            return {"answer": answer}
        else:
            raise RuntimeError(f"Slinker returned {resp.status_code}: {resp.text[:500]}")

    async def _call_poltergeist(self, client: "httpx.AsyncClient", task: str) -> dict[str, Any]:
        """Call Poltergeist notification service."""
        base_url = os.environ.get("POLTERGEIST_URL", "http://poltergeist:8088").rstrip("/")
        api_key = os.environ.get("POLTERGEIST_API_KEY", "")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        # Parse notification payload
        if task.startswith("{"):
            payload = json.loads(task)
        else:
            payload ={"message": task}

        resp = await client.post(f"{base_url}/api/notifications", json=payload, headers=headers)
        if resp.status_code in (200, 201):
            return resp.json()
        else:
            raise RuntimeError(f"Poltergeist returned {resp.status_code}: {resp.text[:500]}")

    async def _call_shrewd(self, client: "httpx.AsyncClient", task: str) -> dict[str, Any]:
        """Call Shrewd LLM service."""
        base_url = os.environ.get("SHREWD_URL", "http://shrewd:8089").rstrip("/")
        api_key = os.environ.get("SHREWD_API_KEY", "")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        resp = await client.post(
            f"{base_url}/api/chat",
            json={"messages": [{"role": "user", "content": task}]},
            headers=headers,
        )
        if resp.status_code == 200:
            return resp.json()
        else:
            raise RuntimeError(f"Shrewd returned {resp.status_code}: {resp.text[:500]}")

    async def _execute_notify(self, step: WorkflowStep, context: dict[str, Any]) -> dict[str, Any]:
        """Execute a notify step via Poltergeist. Gracefully degrades when unavailable."""
        channel = step.notify_channel
        message = step.notify_message

        # Interpolate context variables
        for key, value in context.items():
            placeholder = f"{{{key}}}"
            if placeholder in message:
                message = message.replace(placeholder, str(value)[:2000])

        import httpx
        base_url = os.environ.get("POLTERGEIST_URL", "http://poltergeist:8088").rstrip("/")
        api_key = os.environ.get("POLTERGEIST_API_KEY", "")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                payload = {
                    "channel": channel,
                    "message": message,
                }
                resp = await client.post(f"{base_url}/api/notifications", json=payload, headers=headers)
                if resp.status_code in (200, 201):
                    return resp.json()
                else:
                    logger.warning(f"Notification failed: {resp.status_code} {resp.text[:200]}")
                    return {"status": "simulated", "channel": channel, "message": message[:200]}
        except (httpx.ConnectError, httpx.TimeoutException, OSError) as e:
            logger.warning(f"Poltergeist unavailable, simulating notification: {e}")
            return {"status": "simulated", "channel": channel, "message": message[:200]}

    async def _execute_workstation(
        self, step: WorkflowStep, run: WorkflowRun, context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute a workstation step: create, start, exec, or destroy a workstation.

        The step's config field contains the workstation configuration:
        {
            "action": "create" | "start" | "exec" | "stop" | "destroy",
            "name": "workstation name",
            "image": "python:3.11-slim",
            "command": "pip install requests && python script.py",  # for exec action
            "allowed_hosts": ["api.openai.com"],
            ...
        }
        """
        config = step.config or {}
        action = config.get("action", "create").lower()

        # Import workstation module
        from hub.workstation import (
            WorkstationConfig,
        )
        from hub.workstation import (
            create_workstation as ws_create,
        )
        from hub.workstation import (
            destroy_workstation as ws_destroy,
        )
        from hub.workstation import (
            exec_in_workstation as ws_exec,
        )
        from hub.workstation import (
            get_workstation as ws_get,
        )
        from hub.workstation import (
            start_workstation as ws_start,
        )
        from hub.workstation import (
            stop_workstation as ws_stop,
        )

        ws_name = config.get("name", step.name or f"workflow-ws-{run.id}")
        ws_config_data = {k: v for k, v in config.items()
                          if k not in ("action", "name", "command")}

        ws_config = WorkstationConfig(
            image=ws_config_data.get("image", "python:3.11-slim"),
            cpu_limit=ws_config_data.get("cpu_limit", 1.0),
            memory_limit=ws_config_data.get("memory_limit", "512m"),
            disk_limit=ws_config_data.get("disk_limit", "10g"),
            env_vars=ws_config_data.get("env_vars", {}),
            volumes=ws_config_data.get("volumes", []),
            network_name=ws_config_data.get("network_name", "evershade-workstations"),
            allowed_hosts=ws_config_data.get("allowed_hosts", []),
            dns_servers=ws_config_data.get("dns_servers", []),
            enable_terminal=ws_config_data.get("enable_terminal", True),
            enable_desktop=ws_config_data.get("enable_desktop", False),
            desktop_type=ws_config_data.get("desktop_type", "novnc"),
            labels=ws_config_data.get("labels", {}),
            extra_hosts=ws_config_data.get("extra_hosts", {}),
            command=ws_config_data.get("command"),
        )

        if action == "create":
            ws = ws_create(
                name=ws_name,
                description=config.get("description", ""),
                config=ws_config,
                created_by=f"workflow-{run.id}",
                agent_name=step.agent or "",
                workflow_run_id=run.id,
                tags=config.get("tags", []),
            )
            if ws is None:
                raise RuntimeError("Failed to create workstation")
            return {
                "action": "create",
                "workstation_id": ws.id,
                "status": ws.status,
                "container_id": ws.container_id[:12] if ws.container_id else "",
            }

        elif action == "start":
            # Find existing workstation by workflow run ID
            ws_id = config.get("workstation_id", "")
            if not ws_id:
                # Try to find by workflow run ID
                from hub.workstation import list_workstations as ws_list
                wss = ws_list(status=None, limit=100)
                for w in wss:
                    if w.workflow_run_id == run.id:
                        ws_id = w.id
                        break
            if not ws_id:
                raise RuntimeError("No workstation_id provided and none found for this workflow run")

            ws = ws_start(ws_id)
            if ws is None:
                raise RuntimeError(f"Workstation {ws_id} not found")
            return {
                "action": "start",
                "workstation_id": ws.id,
                "status": ws.status,
            }

        elif action == "exec":
            ws_id = config.get("workstation_id", "")
            if not ws_id:
                from hub.workstation import list_workstations as ws_list
                wss = ws_list(status=None, limit=100)
                for w in wss:
                    if w.workflow_run_id == run.id:
                        ws_id = w.id
                        break
            if not ws_id:
                raise RuntimeError("No workstation_id provided for exec action")

            command = config.get("command", step.task or "echo 'No command specified'")
            # Interpolate context variables, sanitizing values to prevent command injection
            for key, value in context.items():
                placeholder = f"{{{key}}}"
                if placeholder in command:
                    sanitized = shlex.quote(str(value)[:2000])
                    command = command.replace(placeholder, sanitized)

            result = ws_exec(ws_id, command)
            if result is None:
                raise RuntimeError(f"Workstation {ws_id} not found or not running")
            return {
                "action": "exec",
                "workstation_id": ws_id,
                "exit_code": result.get("exit_code"),
                "stdout": result.get("stdout", "")[:5000],
                "stderr": result.get("stderr", "")[:2000],
            }

        elif action == "stop":
            ws_id = config.get("workstation_id", "")
            if not ws_id:
                from hub.workstation import list_workstations as ws_list
                wss = ws_list(status=None, limit=100)
                for w in wss:
                    if w.workflow_run_id == run.id:
                        ws_id = w.id
                        break
            if not ws_id:
                raise RuntimeError("No workstation_id provided for stop action")

            ws = ws_stop(ws_id)
            if ws is None:
                raise RuntimeError(f"Workstation {ws_id} not found")
            return {
                "action": "stop",
                "workstation_id": ws.id,
                "status": ws.status,
            }

        elif action == "destroy":
            ws_id = config.get("workstation_id", "")
            if not ws_id:
                from hub.workstation import list_workstations as ws_list
                wss = ws_list(status=None, limit=100)
                for w in wss:
                    if w.workflow_run_id == run.id:
                        ws_id = w.id
                        break
            if not ws_id:
                raise RuntimeError("No workstation_id provided for destroy action")

            ws = ws_destroy(ws_id, force=config.get("force", False))
            if ws is None:
                raise RuntimeError(f"Workstation {ws_id} not found")
            return {
                "action": "destroy",
                "workstation_id": ws.id,
                "status": ws.status,
            }

        else:
            raise ValueError(f"Unknown workstation action: {action}")

    async def _wait_for_approval(self, run: WorkflowRun, step: WorkflowStep) -> dict[str, Any]:
        """Pause execution waiting for human approval."""
        # Create approval request
        approval = WorkflowApproval(
            run_id=run.id,
            step_id=step.id,
            description=step.approval_description or f"Approve step: {step.name}",
            timeout_minutes=step.approval_timeout_minutes,
            context={"step_type": step.step_type, "task": step.task},
        )
        storage.save_workflow_approval(approval)

        # Update run and step status
        run.status = RunStatus.WAITING_APPROVAL.value
        run.step_runs[run.current_step_index].status = StepStatus.WAITING_APPROVAL.value
        storage.save_workflow_run(run)

        # Create event for resuming
        event = asyncio.Event()
        self._pending_events[run.id] = event

        try:
            # Wait for approval with timeout
            timeout = step.approval_timeout_minutes * 60
            await asyncio.wait_for(event.wait(), timeout=timeout)

            result = self._pending_results.pop(run.id, "denied")

            # Update approval
            approval.status = ApprovalStatus.APPROVED.value if result == "approved" else ApprovalStatus.DENIED.value
            approval.resolved_at = datetime.now(timezone.utc).isoformat()
            storage.save_workflow_approval(approval)

            # Update step run
            idx = run.current_step_index
            if result == "approved":
                run.step_runs[idx].status = StepStatus.COMPLETED.value
                run.step_runs[idx].approval_result = "approved"
                run.step_runs[idx].completed_at = datetime.now(timezone.utc).isoformat()
                return {"approved": True}
            else:
                run.step_runs[idx].status = StepStatus.FAILED.value
                run.step_runs[idx].approval_result = "denied"
                run.step_runs[idx].error = "Approval denied"
                run.step_runs[idx].completed_at = datetime.now(timezone.utc).isoformat()
                raise RuntimeError("Approval denied")

        except TimeoutError:
            approval.status = ApprovalStatus.EXPIRED.value
            approval.resolved_at = datetime.now(timezone.utc).isoformat()
            storage.save_workflow_approval(approval)

            idx = run.current_step_index
            run.step_runs[idx].status = StepStatus.FAILED.value
            run.step_runs[idx].error = "Approval timed out"
            run.step_runs[idx].completed_at = datetime.now(timezone.utc).isoformat()
            raise RuntimeError("Approval timed out")
        finally:
            self._pending_events.pop(run.id, None)

    async def resolve_approval(self, run_id: str, approved: bool) -> bool:
        """Resolve a pending approval, resuming workflow execution.

        Returns True if the approval was found and resolved.
        """
        event = self._pending_events.get(run_id)
        if event is None:
            return False

        self._pending_results[run_id] = "approved" if approved else "denied"
        event.set()
        return True

    def _evaluate_condition(self, condition: dict[str, Any], context: dict[str, Any]) -> bool:
        """Evaluate a simple condition against the execution context.

        Condition format: {"key": "step_0.status", "operator": "eq", "value": "ok"}
        Supported operators: eq, ne, contains, gt, lt
        """
        key = condition.get("key", "")
        operator = condition.get("operator", "eq")
        expected = condition.get("value", "")

        # Resolve key from context (supports dot notation)
        actual = self._resolve_context_key(key, context)
        if actual is None:
            return False

        # Compare
        if operator == "eq":
            return str(actual) == str(expected)
        elif operator == "ne":
            return str(actual) != str(expected)
        elif operator == "contains":
            return str(expected) in str(actual)
        elif operator == "gt":
            try:
                return float(actual) > float(expected)
            except (ValueError, TypeError):
                return False
        elif operator == "lt":
            try:
                return float(actual) < float(expected)
            except (ValueError, TypeError):
                return False
        return False

    def _resolve_context_key(self, key: str, context: dict[str, Any]) -> Any:
        """Resolve a dot-notation key from the context dict."""
        parts = key.split(".")
        value = context
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
            if value is None:
                return None
        return value

    @staticmethod
    def _parse_trapper_task(task: str) -> tuple[str, str]:
        """Parse a Trapper task string to determine the operation and target."""
        task_upper = task.upper().strip()
        prefixes = ["SCRAPE:", "CRAWL:", "SEARCH:", "MAP:", "BATCH:", "INTERACT:", "EXTRACT:", "AGENT:"]
        for prefix in prefixes:
            if task_upper.startswith(prefix):
                return prefix[:-1].lower(), task[len(prefix):].strip()
        return "search", task


# Global executor instance
_executor = WorkflowExecutor()


async def execute_workflow(
    workflow: WorkflowDefinition,
    trigger_type: str = "manual",
    trigger_data: Optional[dict[str, Any]] = None,
    started_by: str = "api",
) -> WorkflowRun:
    """Execute a workflow (convenience function)."""
    return await _executor.execute(workflow, trigger_type, trigger_data, started_by)


async def resolve_approval(run_id: str, approved: bool) -> bool:
    """Resolve a pending approval (convenience function)."""
    return await _executor.resolve_approval(run_id, approved)
