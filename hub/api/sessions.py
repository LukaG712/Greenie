"""Route module: sessions."""

import asyncio
import json
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException
from starlette.responses import StreamingResponse

from hub import agents, memory, ollama
from hub.api.core import (
    _sse_buffer_for_run,
    _sse_event_store,
    _sse_event_store_lock,
    _sse_remove_buffer,
    _sse_send_event,
    _verify_token,
    _verify_token_or_query,
)
from hub.config import load_config, save_config, set_primary_bot, validate_name
from hub.runner import Run, agent_status, get_run, list_runs, recover_stale_runs, run_agent, stop_run


async def api_list_sessions(
    agent: Optional[str] = None,
    _auth: None = Depends(_verify_token),
):
    sessions = memory.list_sessions(agent_id=agent)
    # Add unread count to each session
    for s in sessions:
        s["unread_count"] = memory.get_unread_count(s["id"])
    return {"sessions": sessions}


async def api_create_session(
    body: Dict[str, Any],
    _auth: None = Depends(_verify_token),
):
    title = body.get("title", "")
    project_path = body.get("project_path")
    agent_id = body.get("agent_id")
    bot_ids = body.get("bot_ids")
    session = memory.create_session(
        title=title,
        project_path=project_path,
        agent_id=agent_id,
        bot_ids=bot_ids,
    )
    return session


async def api_get_session(
    session_id: str,
    _auth: None = Depends(_verify_token),
):
    session = memory.get_session_with_messages(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    # Mark session as read and include unread count
    memory.mark_session_read(session_id)
    session["unread_count"] = 0
    return session


async def api_delete_session(
    session_id: str,
    _auth: None = Depends(_verify_token),
):
    deleted = memory.delete_session(session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    return {"deleted": session_id}


# --- Message dispatch ---


async def api_dispatch_message(
    session_id: str,
    body: Dict[str, Any],
    _auth: None = Depends(_verify_token),
):
    # Validate session exists
    session = memory.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")

    content = body.get("content", "")
    if not content:
        raise HTTPException(status_code=400, detail="content is required")

    # Allow overriding agent_id from request body
    agent_id = body.get("agent_id") or session.get("agent_id")
    if not agent_id:
        raise HTTPException(status_code=400, detail="Session has no agent_id set")

    # Validate agent exists
    try:
        agent_cfg = agents.get_agent(agent_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")

    # Update session's agent_id if it changed
    if session.get("agent_id") != agent_id:
        conn = memory._get_connection()
        conn.execute(
            "UPDATE sessions SET agent_id = ? WHERE id = ?",
            (agent_id, session_id),
        )
        conn.commit()

    # Optional prompt refinement via Ollama
    refine_prompt = body.get("refine_prompt", False)
    refined_content = content
    if refine_prompt:
        ollama_client = ollama.get_ollama_client()
        if ollama_client and ollama_client.is_available():
            project_path = session.get("project_path", "")
            context = None
            if project_path:
                context = memory.assemble_context(
                    session_id=session_id,
                    agent_id=agent_id,
                    project_path=project_path,
                )
            refined_content = ollama_client.refine_prompt(
                prompt=content,
                context=context,
            )

    # Persist user message (original content, not refined)
    memory.add_message(
        session_id=session_id,
        role="user",
        content=content,
        bot_id=agent_id,
    )

    # Pre-create a run ID and SSE buffer BEFORE starting the run.
    # This ensures output_callback has a buffer to write to from the very first line.
    pre_run_id = str(uuid.uuid4())
    _sse_buffer_for_run(pre_run_id)

    def _output_callback(line_type: str, text: str) -> None:
        """Called for each line of agent output during execution."""
        clean = text.rstrip()
        if clean:
            _sse_send_event(pre_run_id, "output", json.dumps({
                "event": "output",
                "run_id": pre_run_id,
                "type": line_type,
                "text": clean,
            }))

    def _on_start(rid: str) -> None:
        _sse_send_event(rid, "start", json.dumps({"event": "start", "run_id": rid, "state": "working"}))

    def _on_complete(result: Run) -> None:
        if result.state == "done":
            _sse_send_event(result.id, "complete", json.dumps({
                "event": "complete",
                "run_id": result.id,
                "state": "done",
                "answer": result.answer,
            }))
        elif result.state == "failed":
            _sse_send_event(result.id, "error", json.dumps({
                "event": "error",
                "run_id": result.id,
                "state": "failed",
                "reason": result.fail_reason,
            }))
        else:
            _sse_send_event(result.id, "complete", json.dumps({
                "event": "complete",
                "run_id": result.id,
                "state": result.state,
            }))
        _sse_send_event(result.id, "end", "")

        def _delayed_cleanup():
            time.sleep(30)
            _sse_remove_buffer(result.id)

        threading.Thread(target=_delayed_cleanup, daemon=True).start()

    from hub import queue_manager
    queue_manager.enqueue_task(
        bot_id=agent_id,
        task=refined_content,
        session_id=session_id,
        output_callback=_output_callback,
        on_start=_on_start,
        on_complete=_on_complete,
        run_id=pre_run_id,
    )

    # Return immediately with the run ID; client connects to SSE stream
    return {
        "status": "queued",
        "session_id": session_id,
        "agent": agent_id,
        "run_id": pre_run_id,
    }


# --- Bot Queue & Pause/Resume ---


async def api_stream_session(
    session_id: str,
    run_id: Optional[str] = None,
    _auth: None = Depends(_verify_token_or_query),
):
    """SSE endpoint for live run output.

    Streams events for a specific run (via ?run_id=...) or for the latest run
    in the session.  If the run is already finished, replays all stored events
    so the client does not hang indefinitely.

    Each connected client receives its own ordered copy of the full event stream
    — multiple clients do not compete for events.
    """
    session = memory.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")

    # Resolve the target run
    all_runs = list_runs()
    session_runs = [r for r in all_runs if r.session_id == session_id]
    session_runs.sort(key=lambda r: r.started or "", reverse=True)

    if run_id:
        # Stream a specific run
        target_run = next((r for r in session_runs if r.id == run_id), None)
        if target_run is None:
            # Run not found in this session — check all runs
            target_run = next((r for r in all_runs if r.id == run_id), None)
            if target_run is None:
                raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    else:
        # Stream the latest run in the session
        target_run = session_runs[0] if session_runs else None

    async def event_generator():
        if target_run is None:
            # No runs at all — send idle and close
            yield "event: idle\ndata: no active run\n\n"
            return

        rid = target_run.id
        store = _sse_buffer_for_run(rid)

        if target_run.state == "working":
            # Run is in progress — stream events using event ID cursors.
            # Each client tracks its own last_event_id so multiple clients
            # get independent ordered streams that survive evictions.
            last_event_id = 0
            received_end = False
            gap_sent = False
            while True:
                # Acquire lock only to snapshot; release before yielding.
                with _sse_event_store_lock:
                    s = _sse_event_store.get(rid)
                    if s is None:
                        # Store was cleaned up; fall through to final-state replay
                        break
                    # Check for retention gap: client is behind when its
                    # cursor is less than the highest evicted ID.
                    has_gap = (
                        s.evicted_before_id is not None
                        and s.evicted_before_id > last_event_id
                    )
                    # Snapshot (id, string) pairs after the client's cursor
                    new_events = [
                        (ev_id, ev_str)
                        for ev_id, ev_str in s.events
                        if ev_id > last_event_id
                    ]
                # Yield outside the lock so slow clients don't block producers.
                if has_gap and not gap_sent:
                    yield (
                        f"event: retention_gap\ndata: "
                        f"{json.dumps({'run_id': rid, 'evicted_before_id': s.evicted_before_id})}\n\n"
                    )
                    gap_sent = True
                if new_events:
                    for ev_id, ev_str in new_events:
                        yield ev_str
                    # Advance cursor using the IDs of events in this snapshot,
                    # not from the shared store, so new events arriving between
                    # snapshot and cursor update are never skipped.
                    last_event_id = new_events[-1][0]
                    # Check for end event
                    if any("event: end" in ev_str for _, ev_str in new_events):
                        received_end = True
                        break
                else:
                    try:
                        await asyncio.sleep(0.2)
                    except asyncio.CancelledError:
                        break

            # If we broke out without receiving an end event, replay final state
            if not received_end:
                if target_run.state == "done":
                    yield f"event: complete\ndata: {json.dumps({'run_id': rid, 'state': 'done', 'answer': target_run.answer})}\n\n"
                elif target_run.state == "failed":
                    yield f"event: error\ndata: {json.dumps({'run_id': rid, 'state': 'failed', 'reason': target_run.fail_reason})}\n\n"
                yield "event: end\n\n"
        else:
            # Run already finished — replay all stored events
            with _sse_event_store_lock:
                s = _sse_event_store.get(rid)
                if s is not None:
                    events = [ev_str for _, ev_str in s.events]
                else:
                    events = []
            # Notify about retention gap if events were evicted
            with _sse_event_store_lock:
                s = _sse_event_store.get(rid)
                evicted_before = s.evicted_before_id if s else None
            if evicted_before is not None:
                yield (
                    f"event: retention_gap\ndata: "
                    f"{json.dumps({'run_id': rid, 'evicted_before_id': evicted_before})}\n\n"
                )
            for ev in events:
                yield ev
            # If no stored events, replay the final state from the run record
            if not events:
                if target_run.state == "done":
                    yield f"event: complete\ndata: {json.dumps({'run_id': rid, 'state': 'done', 'answer': target_run.answer})}\n\n"
                elif target_run.state == "failed":
                    yield f"event: error\ndata: {json.dumps({'run_id': rid, 'state': 'failed', 'reason': target_run.fail_reason})}\n\n"
                else:
                    yield f"event: complete\ndata: {json.dumps({'run_id': rid, 'state': target_run.state})}\n\n"
            yield "event: end\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Memories ---


async def api_handoff(
    session_id: str,
    body: Dict[str, Any],
    _auth: None = Depends(_verify_token),
):
    """Hand off a task from one Bot to another within the same session.

    Expects: from_bot, to_bot, message.
    Prevents infinite loops by tracking hop count.
    """
    from_bot = body.get("from_bot", "")
    to_bot = body.get("to_bot", "")
    message = body.get("message", "")
    if not from_bot or not to_bot:
        raise HTTPException(status_code=400, detail="from_bot and to_bot are required")

    # Validate session exists
    session = memory.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")

    # Validate to_bot exists
    try:
        agents.get_agent(to_bot)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Bot '{to_bot}' not found")

    # Check handoff loop prevention
    max_hops = 5  # Default max hops
    config = load_config()
    max_hops = config.get("max_handoff_hops", 5)

    # Count existing handoffs in this session
    messages = memory.get_session_messages(session_id)
    hop_count = sum(1 for m in messages if m.get("msg_type") == "handoff")
    if hop_count >= max_hops:
        raise HTTPException(
            status_code=400,
            detail=f"Max handoff hops ({max_hops}) reached. Preventing infinite loop."
        )

    # Record handoff message
    now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    memory.add_message(
        session_id=session_id,
        role="assistant",
        content=f"Handing off to @{to_bot}: {message}",
        bot_id=from_bot,
        msg_type="handoff",
        metadata=f'{{"from": "{from_bot}", "to": "{to_bot}", "hop": {hop_count + 1}, "at": "{now_iso}"}}',
    )

    # Dispatch to the receiving bot via queue manager
    from hub import queue_manager

    def _on_handoff_complete(res):
        finished_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        memory.add_message(
            session_id=session_id,
            role="assistant",
            content=res.raw_output or "Handoff task completed.",
            bot_id=to_bot,
            msg_type="handoff_reply",
            metadata=f'{{"from": "{to_bot}", "run_id": "{res.id}", "at": "{finished_iso}"}}',
        )
        _sse_send_event(res.id, "complete", res.state)

    run = queue_manager.enqueue_task(
        bot_id=to_bot,
        task=message,
        session_id=session_id,
        on_complete=_on_handoff_complete,
    )

    return {
        "status": "dispatched",
        "run_id": run.id,
        "from_bot": from_bot,
        "to_bot": to_bot,
        "session_id": session_id,
        "hop": hop_count + 1,
    }


# --- Templates / Marketplace ---



def register(app):
    """Register sessions routes with the FastAPI app."""
    app.get("/api/sessions")(api_list_sessions)
    app.post("/api/sessions", status_code=201)(api_create_session)
    app.get("/api/sessions/{session_id}")(api_get_session)
    app.delete("/api/sessions/{session_id}")(api_delete_session)
    app.post("/api/sessions/{session_id}/messages", status_code=201)(api_dispatch_message)
    app.get("/api/sessions/{session_id}/stream")(api_stream_session)
    app.post("/api/sessions/{session_id}/handoff", status_code=201)(api_handoff)
