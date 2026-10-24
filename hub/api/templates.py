"""Route module: templates."""

import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from hub import feature_store
from hub.api.core import _approval_policies, _approval_policies_lock, _templates, _templates_lock, _verify_token
from hub.runner import Run, agent_status, get_run, list_runs, recover_stale_runs, run_agent, stop_run

logger = logging.getLogger(__name__)


async def api_list_templates(_auth: None = Depends(_verify_token)):
    """List all available Bot templates."""
    from hub import templates as templates_module
    templates_list = templates_module.list_templates()
    return {"templates": templates_list}


async def api_get_template(name: str, _auth: None = Depends(_verify_token)):
    """Get a single template by name."""
    from hub import templates as templates_module
    template = templates_module.get_template(name)
    if template is None:
        raise HTTPException(status_code=404, detail=f"Template '{name}' not found")
    return template


async def api_apply_template(
    name: str,
    body: Optional[Dict[str, Any]] = None,
    _auth: None = Depends(_verify_token),
):
    """Create an agent from a template."""
    from hub import templates as templates_module
    body = body or {}
    agent_name = body.get("agent_name")
    try:
        result = templates_module.create_agent_from_template(name, agent_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return result


# --- Teach a Task ---


async def api_create_template(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Save a reusable pipeline or task template.

    Example body:
    {
        "name": "Research & Summarize",
        "description": "Crawl a URL and summarize the content",
        "type": "pipeline",
        "steps": [
            {"service": "Trapper", "task": "SCRAPE:{{url}}"},
            {"service": "agent", "agent": "vault", "task": "Summarize: {{result}}"}
        ],
        "variables": ["url"],
        "tags": ["research", "summary"]
    }
    """
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    template_type = body.get("type", "pipeline")
    if template_type not in ("pipeline", "task", "schedule"):
        raise HTTPException(status_code=400, detail="type must be pipeline, task, or schedule")

    template_id = f"tpl-{uuid.uuid4().hex[:8]}"
    template = {
        "id": template_id,
        "name": name,
        "description": body.get("description", ""),
        "type": template_type,
        "steps": body.get("steps", []),
        "variables": body.get("variables", []),
        "tags": body.get("tags", []),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "usage_count": 0,
    }

    with _templates_lock:
        _templates[template_id] = template
    # Persist to database
    try:
        feature_store.save_template(template)
    except Exception as e:
        logger.warning(f"Failed to save template: {e}")

    return template


async def api_list_templates(
    tag: Optional[str] = None,
    _auth: None = Depends(_verify_token),
):
    """List all templates, optionally filtered by tag."""
    with _templates_lock:
        templates = list(_templates.values())

    if tag:
        templates = [t for t in templates if tag in t.get("tags", [])]

    return {"templates": templates}


async def api_get_template(template_id: str, _auth: None = Depends(_verify_token)):
    """Get a template by ID."""
    with _templates_lock:
        template = _templates.get(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    return template


async def api_update_template(template_id: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Update a template."""
    with _templates_lock:
        template = _templates.get(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")
        if "name" in body:
            template["name"] = body["name"].strip()
        if "description" in body:
            template["description"] = body["description"]
        if "steps" in body:
            template["steps"] = body["steps"]
        if "variables" in body:
            template["variables"] = body["variables"]
        if "tags" in body:
            template["tags"] = body["tags"]
        template["updated_at"] = datetime.now(timezone.utc).isoformat()
    # Persist to database
    try:
        feature_store.save_template(template)
    except Exception as e:
        logger.warning(f"Failed to save template update: {e}")
    return dict(template)


async def api_delete_template(template_id: str, _auth: None = Depends(_verify_token)):
    """Delete a template."""
    with _templates_lock:
        if template_id not in _templates:
            raise HTTPException(status_code=404, detail="Template not found")
        del _templates[template_id]
    # Persist to database
    try:
        feature_store.delete_template(template_id)
    except Exception as e:
        logger.warning(f"Failed to delete template: {e}")
    return {"status": "ok", "message": "Template deleted"}


async def api_execute_template(template_id: str, body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Execute a template with variable substitutions.

    Example body:
    {
        "variables": {"url": "https://example.com"}
    }
    """
    with _templates_lock:
        template = _templates.get(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")

    variables = body.get("variables", {})
    steps = template.get("steps", [])

    # Substitute variables in steps
    resolved_steps = []
    for step in steps:
        resolved = {}
        for k, v in step.items():
            if isinstance(v, str):
                for var_key, var_val in variables.items():
                    v = v.replace(f"{{{{{var_key}}}}}", str(var_val))
            resolved[k] = v
        resolved_steps.append(resolved)

    # Track usage
    with _templates_lock:
        template["usage_count"] += 1

    # If it's a pipeline template, execute via pipeline endpoint
    if template["type"] == "pipeline":
        # Reuse pipeline logic
        result = await _execute_template_pipeline(resolved_steps)
        return {"template_id": template_id, "status": "completed", "result": result}
    elif template["type"] == "task":
        # Single task execution
        if not resolved_steps:
            raise HTTPException(status_code=400, detail="No steps in template")
        step = resolved_steps[0]
        service = step.get("service", "agent")
        task = step.get("task", "")
        agent = step.get("agent", "")

        if service == "agent" and agent:
            run = run_agent(agent, task)
            return {"template_id": template_id, "run_id": run.id, "status": "started"}
        elif service == "Trapper":
            from hub.Trapper_provider import TrapperProvider
            provider = TrapperProvider()
            result = await provider.execute(task)
            return {"template_id": template_id, "status": "completed", "result": result}
        elif service == "Slinker":
            from hub.Slinker_provider import SlinkerProvider
            provider = SlinkerProvider()
            result = await provider.execute(task)
            return {"template_id": template_id, "status": "completed", "result": result}
        else:
            raise HTTPException(status_code=400, detail=f"Unknown service: {service}")
    else:
        raise HTTPException(status_code=400, detail=f"Cannot execute template type: {template['type']}")


async def _execute_template_pipeline(steps: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Execute a list of pipeline steps sequentially."""
    results = []
    context = {}
    for i, step in enumerate(steps):
        service = step.get("service", "agent")
        task = step.get("task", "")
        agent = step.get("agent", "")

        # Substitute context variables
        for k, v in context.items():
            task = task.replace(f"{{{{{k}}}}}", str(v))

        if service == "agent" and agent:
            run = run_agent(agent, task)
            result = {"step": i, "service": service, "run_id": run.id}
        elif service == "Trapper":
            from hub.Trapper_provider import TrapperProvider
            provider = TrapperProvider()
            result_data = await provider.execute(task)
            result = {"step": i, "service": service, "result": result_data}
            context[f"step{i}"] = str(result_data)[:2000]
        elif service == "Slinker":
            from hub.Slinker_provider import SlinkerProvider
            provider = SlinkerProvider()
            result_data = await provider.execute(task)
            result = {"step": i, "service": service, "result": result_data}
            context[f"step{i}"] = str(result_data)[:2000]
        else:
            result = {"step": i, "error": f"Unknown service: {service}"}

        results.append(result)

    return {"steps": results, "context": context}


# ─── Approval Workflow ──────────────────────────────────────────────────────

# Pending approvals queue
_approvals: Dict[str, Dict[str, Any]] = {}
_approvals_lock = threading.Lock()

# Approval policies: which actions require approval
_approval_policies: Dict[str, Any] = {
    "enabled": False,
    "require_for": [],  # e.g. ["external_api", "file_write", "agent_create"]
    "auto_approve_agents": [],  # agents that bypass approval
}
_approval_policies_lock = threading.Lock()



def register(app):
    """Register templates routes with the FastAPI app."""
    app.get("/api/templates")(api_list_templates)
    app.get("/api/templates/{name}")(api_get_template)
    app.post("/api/templates/{name}/apply", status_code=201)(api_apply_template)
    app.post("/api/templates", status_code=201)(api_create_template)
    app.get("/api/templates")(api_list_templates)
    app.get("/api/templates/{template_id}")(api_get_template)
    app.put("/api/templates/{template_id}")(api_update_template)
    app.delete("/api/templates/{template_id}")(api_delete_template)
    app.post("/api/templates/{template_id}/execute")(api_execute_template)
