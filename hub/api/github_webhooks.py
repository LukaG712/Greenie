"""Route module: github_webhooks."""

import asyncio
import hashlib
import hmac
import json
import logging
import weakref

from fastapi import Depends, HTTPException, Request

from hub import feature_store
from hub.api.core import _verify_token, _webhook_secrets, _webhook_secrets_lock
from hub.config import load_config, save_config, set_primary_bot, validate_name

logger = logging.getLogger(__name__)

# Background tasks for webhook pipelines
_webhook_background_tasks: set = set()


def _verify_github_signature(payload: bytes, signature: str, secret: str) -> bool:
    """Verify GitHub webhook HMAC signature."""
    if not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        secret.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def _verify_gitlab_signature(payload: bytes, token: str, secret: str) -> bool:
    """Verify GitLab webhook secret token."""
    return hmac.compare_digest(token.encode(), secret.encode()) if secret else True


async def api_github_webhook(request: Request):
    """Receive GitHub webhooks and trigger pipelines.
    
    Supports push, pull_request, and issues events.
    Configure via /api/webhooks/github/config.
    """
    body = await request.body()
    payload_str = body.decode("utf-8", errors="replace")
    
    # Get config
    with _webhook_secrets_lock:
        config = _webhook_secrets.get("github", {})
    
    secret = config.get("secret", "")
    
    # Verify signature
    signature = request.headers.get("x-hub-signature-256", "")
    if secret and signature:
        if not _verify_github_signature(body, signature, secret):
            logger.warning("GitHub webhook signature verification failed")
            raise HTTPException(status_code=401, detail="Invalid signature")
    
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    
    event_type = request.headers.get("x-github-event", "unknown")
    
    # Extract useful info
    repo = ""
    branch = ""
    action = ""
    commit_msg = ""
    pr_url = ""
    
    if event_type == "push":
        repo = payload.get("repository", {}).get("full_name", "")
        branch = payload.get("ref", "").replace("refs/heads/", "")
        for commit in payload.get("commits", [])[:1]:
            commit_msg = commit.get("message", "")
        action = "push"
    elif event_type == "pull_request":
        repo = payload.get("repository", {}).get("full_name", "")
        action = payload.get("action", "opened")
        pr = payload.get("pull_request", {})
        pr_url = pr.get("html_url", "")
        branch = pr.get("head", {}).get("ref", "")
        commit_msg = pr.get("title", "")
    elif event_type == "issues":
        repo = payload.get("repository", {}).get("full_name", "")
        action = payload.get("action", "opened")
        issue = payload.get("issue", {})
        commit_msg = issue.get("title", "")
        pr_url = issue.get("html_url", "")
    
    # Check for auto-triggered pipelines
    auto_pipelines = config.get("pipelines", [])
    triggered = []
    for pipeline_config in auto_pipelines:
        match_events = pipeline_config.get("events", [])
        match_actions = pipeline_config.get("actions", [])
        if event_type in match_events and (not match_actions or action in match_actions):
            pipeline_id = pipeline_config.get("pipeline_id")
            steps = pipeline_config.get("steps", [])
            
            # Build context from webhook
            context = {
                "event": event_type,
                "action": action,
                "repo": repo,
                "branch": branch,
                "commit_message": commit_msg,
                "url": pr_url,
            }
            
            # Run pipeline in background
            async def _run_webhook_pipeline(steps, ctx):
                try:
                    results = []
                    accumulated_context = json.dumps(ctx)
                    for step in steps:
                        service = step.get("service", "junie")
                        task = step.get("task", "")
                        # Substitute variables
                        task = task.replace("{{repo}}", ctx.get("repo", ""))
                        task = task.replace("{{branch}}", ctx.get("branch", ""))
                        task = task.replace("{{commit_message}}", ctx.get("commit_message", ""))
                        task = task.replace("{{url}}", ctx.get("url", ""))
                        task = task.replace("{{context}}", accumulated_context[:1000])
                        
                        # Execute step
                        if service == "Trapper":
                            from hub.Trapper_provider import TrapperProvider
                            provider = TrapperProvider()
                            result = await provider.run(task)
                        elif service in ("Slinker", "junie"):
                            from hub.Slinker_provider import SlinkerProvider
                            provider = SlinkerProvider()
                            result = await provider.run(task)
                        else:
                            result = {"status": "error", "message": f"Unknown service: {service}"}
                        
                        results.append({"step": step, "result": result})
                        if isinstance(result, dict):
                            output = result.get("output", result.get("answer", ""))
                            if output:
                                accumulated_context = (accumulated_context + "\n" + output)[:4000]
                    
                    logger.info(f"Webhook pipeline completed: {len(results)} steps")
                except Exception as e:
                    logger.error(f"Webhook pipeline failed: {e}")
            
            task = asyncio.create_task(_run_webhook_pipeline(steps, context))
            _webhook_background_tasks.add(task)
            task.add_done_callback(_webhook_background_tasks.discard)
            triggered.append(pipeline_id or "inline")
    
    logger.info(
        f"GitHub webhook received: {event_type} {action} from {repo}",
        extra={"event": event_type, "action": action, "repo": repo},
    )
    
    return {
        "status": "ok",
        "event": event_type,
        "action": action,
        "triggered_pipelines": triggered,
    }


async def api_gitlab_webhook(request: Request):
    """Receive GitLab webhooks and trigger pipelines.
    
    Supports push, merge_request, and issue events.
    Configure via /api/webhooks/gitlab/config.
    """
    body = await request.body()
    payload_str = body.decode("utf-8", errors="replace")
    
    # Get config
    with _webhook_secrets_lock:
        config = _webhook_secrets.get("gitlab", {})
    
    secret = config.get("secret", "")
    
    # Verify token
    token = request.headers.get("x-gitlab-token", "")
    if secret and not _verify_gitlab_signature(body, token, secret):
        logger.warning("GitLab webhook token verification failed")
        raise HTTPException(status_code=401, detail="Invalid token")
    
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    
    event_type = request.headers.get("x-gitlab-event", "unknown")
    
    # Extract useful info
    repo = payload.get("project", {}).get("path_with_namespace", "")
    branch = ""
    action = ""
    commit_msg = ""
    mr_url = ""
    
    if event_type in ("push", "Push Hook"):
        ref = payload.get("ref", "")
        branch = ref.replace("refs/heads/", "") if ref else ""
        action = "push"
        commit_msg = payload.get("commit_message", "")
    elif event_type == "Merge Request Hook":
        action = payload.get("object_attributes", {}).get("action", "open")
        mr = payload.get("object_attributes", {})
        mr_url = mr.get("url", "")
        branch = mr.get("source_branch", "")
        commit_msg = mr.get("title", "")
    elif event_type == "Issue Hook":
        action = payload.get("object_attributes", {}).get("action", "open")
        issue = payload.get("object_attributes", {})
        commit_msg = issue.get("title", "")
        mr_url = issue.get("url", "")
    
    # Check for auto-triggered pipelines
    auto_pipelines = config.get("pipelines", [])
    triggered = []
    for pipeline_config in auto_pipelines:
        match_events = pipeline_config.get("events", [])
        match_actions = pipeline_config.get("actions", [])
        if event_type in match_events and (not match_actions or action in match_actions):
            steps = pipeline_config.get("steps", [])
            
            context = {
                "event": event_type,
                "action": action,
                "repo": repo,
                "branch": branch,
                "commit_message": commit_msg,
                "url": mr_url,
            }
            
            async def _run_webhook_pipeline(steps, ctx):
                try:
                    accumulated_context = json.dumps(ctx)
                    for step in steps:
                        service = step.get("service", "junie")
                        task = step.get("task", "")
                        task = task.replace("{{repo}}", ctx.get("repo", ""))
                        task = task.replace("{{branch}}", ctx.get("branch", ""))
                        task = task.replace("{{commit_message}}", ctx.get("commit_message", ""))
                        task = task.replace("{{url}}", ctx.get("url", ""))
                        task = task.replace("{{context}}", accumulated_context[:1000])
                        
                        if service == "Trapper":
                            from hub.Trapper_provider import TrapperProvider
                            provider = TrapperProvider()
                            result = await provider.run(task)
                        elif service in ("Slinker", "junie"):
                            from hub.Slinker_provider import SlinkerProvider
                            provider = SlinkerProvider()
                            result = await provider.run(task)
                        else:
                            result = {"status": "error", "message": f"Unknown service: {service}"}
                        
                        if isinstance(result, dict):
                            output = result.get("output", result.get("answer", ""))
                            if output:
                                accumulated_context = (accumulated_context + "\n" + output)[:4000]
                except Exception as e:
                    logger.error(f"GitLab webhook pipeline failed: {e}")
            
            task = asyncio.create_task(_run_webhook_pipeline(steps, context))
            _webhook_background_tasks.add(task)
            task.add_done_callback(_webhook_background_tasks.discard)
            triggered.append("inline")
    
    logger.info(
        f"GitLab webhook received: {event_type} {action} from {repo}",
        extra={"event": event_type, "action": action, "repo": repo},
    )
    
    return {
        "status": "ok",
        "event": event_type,
        "action": action,
        "triggered_pipelines": triggered,
    }


async def api_github_webhook_config(request: Request, _auth: None = Depends(_verify_token)):
    """Configure GitHub webhook integration.
    
    Body: {
        "secret": "your-webhook-secret",
        "pipelines": [
            {
                "events": ["push", "pull_request"],
                "actions": ["opened", "synchronized"],
                "steps": [{"service": "Slinker", "task": "Review {{commit_message}}"}]
            }
        ]
    }
    """
    body = await request.json()
    
    with _webhook_secrets_lock:
        _webhook_secrets["github"] = {
            "secret": body.get("secret", ""),
            "pipelines": body.get("pipelines", []),
        }
    
    # Persist to database
    try:
        feature_store.save_webhook_config({
            "id": "github",
            "secret": body.get("secret", ""),
            "pipelines": body.get("pipelines", []),
        })
    except Exception as e:
        logger.warning(f"Failed to save GitHub webhook config: {e}")
    
    return {"status": "ok", "message": "GitHub webhook configured"}


async def api_gitlab_webhook_config(request: Request, _auth: None = Depends(_verify_token)):
    """Configure GitLab webhook integration.
    
    Body: {
        "secret": "your-webhook-token",
        "pipelines": [
            {
                "events": ["Merge Request Hook"],
                "actions": ["open", "update"],
                "steps": [{"service": "Slinker", "task": "Review {{commit_message}}"}]
            }
        ]
    }
    """
    body = await request.json()
    
    with _webhook_secrets_lock:
        _webhook_secrets["gitlab"] = {
            "secret": body.get("secret", ""),
            "pipelines": body.get("pipelines", []),
        }
    
    # Persist to database
    try:
        feature_store.save_webhook_config({
            "id": "gitlab",
            "secret": body.get("secret", ""),
            "pipelines": body.get("pipelines", []),
        })
    except Exception as e:
        logger.warning(f"Failed to save GitLab webhook config: {e}")
    
    return {"status": "ok", "message": "GitLab webhook configured"}



def register(app):
    """Register github_webhooks routes with the FastAPI app."""
    app.post("/api/webhooks/github")(api_github_webhook)
    app.post("/api/webhooks/gitlab")(api_gitlab_webhook)
    app.post("/api/webhooks/github/config")(api_github_webhook_config)
    app.post("/api/webhooks/gitlab/config")(api_gitlab_webhook_config)
