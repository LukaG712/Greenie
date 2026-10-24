"""Multi-agent task coordinator with dependencies, concurrency, and worktree isolation.

Manages a plan of tasks that can run concurrently when their dependencies are
satisfied. Each coding worker gets its own Git worktree to avoid conflicts.
Requires explicit approval before merging worker changes to the main tree.
"""

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from hub.runner import Run, run_agent

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """Status of a coordinated task."""
    PENDING = "pending"
    WAITING = "waiting"      # Waiting on dependencies
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NEEDS_APPROVAL = "needs_approval"  # Worker finished, awaiting review


@dataclass
class CoordinatedTask:
    """A single task in a coordinated plan."""
    id: str
    name: str
    description: str
    agent: str               # Agent name from config
    dependencies: List[str] = field(default_factory=list)  # Task IDs this task depends on
    status: str = TaskStatus.PENDING
    worker_run_id: Optional[str] = None  # Run ID if currently executing
    worktree_path: Optional[str] = None  # Isolated worktree for this worker
    result: Optional[str] = None         # Final answer/output
    error: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    output_lines: List[str] = field(default_factory=list)  # Collected output
    approved: bool = False              # Whether changes were approved for merge

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WorkerOutput:
    """Callback data for incremental worker output."""
    task_id: str
    line_type: str  # "stdout" or "stderr"
    text: str


class TaskCoordinator:
    """Coordinates multiple agent tasks with dependency resolution and worktree isolation.

    Usage:
        coord = TaskCoordinator(project_dir="/path/to/repo", max_concurrency=3)
        coord.add_task("fix-login", "Fix login bug", agent="vault")
        coord.add_task("fix-nav", "Fix navigation", agent="vault", dependencies=["fix-login"])
        coord.execute()  # Runs tasks respecting dependencies and concurrency limits
    """

    def __init__(
        self,
        project_dir: str,
        max_concurrency: int = 3,
        worktree_base: Optional[str] = None,
    ):
        """Initialize the coordinator.

        Args:
            project_dir: Root project directory (must be a Git repo for worktree isolation).
            max_concurrency: Maximum number of concurrent workers.
            worktree_base: Base directory for worktrees. Defaults to project_dir/.worktrees.
        """
        self.project_dir = str(Path(project_dir).resolve())
        self.max_concurrency = max_concurrency
        self.worktree_base = worktree_base or str(Path(self.project_dir) / ".worktrees")
        self._tasks: Dict[str, CoordinatedTask] = {}
        self._lock = threading.RLock()
        self._running_count = 0
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []
        self._output_callback: Optional[Callable[[WorkerOutput], None]] = None

    def add_task(
        self,
        name: str,
        description: str,
        agent: str,
        dependencies: Optional[List[str]] = None,
    ) -> str:
        """Add a task to the coordination plan.

        Args:
            name: Short name for the task.
            description: Task prompt sent to the agent.
            agent: Agent name from hub config.
            dependencies: List of task names this task depends on.

        Returns:
            The task ID.
        """
        task_id = str(uuid.uuid4())[:8]
        dep_ids = []
        if dependencies:
            for dep_name in dependencies:
                dep_id = self._find_task_by_name(dep_name)
                if dep_id is None:
                    raise ValueError(f"Dependency '{dep_name}' not found in task plan")
                dep_ids.append(dep_id)

        task = CoordinatedTask(
            id=task_id,
            name=name,
            description=description,
            agent=agent,
            dependencies=dep_ids,
        )
        with self._lock:
            self._tasks[task_id] = task
        return task_id

    def remove_task(self, task_id: str) -> bool:
        """Remove a pending task from the plan."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if task.status not in (TaskStatus.PENDING, TaskStatus.WAITING):
                return False
            # Remove this task from other tasks' dependencies
            for t in self._tasks.values():
                if task_id in t.dependencies:
                    t.dependencies.remove(task_id)
            del self._tasks[task_id]
            return True

    def get_task(self, task_id: str) -> Optional[CoordinatedTask]:
        """Get a task by ID."""
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> List[CoordinatedTask]:
        """Get all tasks in the plan."""
        with self._lock:
            return list(self._tasks.values())

    def cancel_all(self) -> None:
        """Cancel all pending and running tasks."""
        self._stop_event.set()
        with self._lock:
            for task in self._tasks.values():
                if task.status in (TaskStatus.PENDING, TaskStatus.WAITING, TaskStatus.RUNNING):
                    task.status = TaskStatus.CANCELLED
                    task.finished_at = datetime.now(timezone.utc).isoformat()

    def set_output_callback(self, callback: Callable[[WorkerOutput], None]) -> None:
        """Set a callback for incremental worker output."""
        self._output_callback = callback

    def execute(self) -> None:
        """Start executing the task plan in a background thread.

        Tasks are scheduled respecting dependencies and concurrency limits.
        """
        self._stop_event.clear()
        t = threading.Thread(target=self._execution_loop, daemon=True)
        t.start()
        self._threads.append(t)

    def is_running(self) -> bool:
        """Check if the coordinator is currently executing tasks."""
        with self._lock:
            return self._running_count > 0

    def is_complete(self) -> bool:
        """Check if all tasks have reached a terminal state."""
        with self._lock:
            if not self._tasks:
                return True
            return all(
                t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)
                for t in self._tasks.values()
            )

    def _find_task_by_name(self, name: str) -> Optional[str]:
        """Find a task ID by its name."""
        for tid, task in self._tasks.items():
            if task.name == name:
                return tid
        return None

    def _execution_loop(self) -> None:
        """Main execution loop: schedule and monitor tasks."""
        while not self._stop_event.is_set():
            # Check for completion
            with self._lock:
                if self.is_complete():
                    break

            # Schedule ready tasks
            self._schedule_ready_tasks()

            # Update waiting tasks
            self._update_waiting_statuses()

            # Brief sleep to avoid busy-waiting
            time.sleep(0.5)

        # Clean up worktrees for completed/failed/cancelled tasks
        self._cleanup_worktrees()

    def _schedule_ready_tasks(self) -> None:
        """Start tasks whose dependencies are satisfied, respecting concurrency limits."""
        with self._lock:
            if self._running_count >= self.max_concurrency:
                return

            for task in self._tasks.values():
                if task.status != TaskStatus.PENDING:
                    continue
                if not self._dependencies_met(task):
                    task.status = TaskStatus.WAITING
                    continue

                # Start this task
                self._launch_task(task)

    def _dependencies_met(self, task: CoordinatedTask) -> bool:
        """Check if all dependencies for a task are completed."""
        for dep_id in task.dependencies:
            dep = self._tasks.get(dep_id)
            if dep is None:
                return False
            if dep.status != TaskStatus.COMPLETED:
                return False
        return True

    def _update_waiting_statuses(self) -> None:
        """Update WAITING tasks: if all deps met, mark as PENDING for scheduling."""
        with self._lock:
            for task in self._tasks.values():
                if task.status == TaskStatus.WAITING:
                    if self._dependencies_met(task):
                        task.status = TaskStatus.PENDING

    def _launch_task(self, task: CoordinatedTask) -> None:
        """Launch a task in a background thread with worktree isolation."""
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(timezone.utc).isoformat()
        self._running_count += 1

        # Create isolated worktree
        worktree_path = self._create_worktree(task)
        if worktree_path:
            task.worktree_path = worktree_path

        t = threading.Thread(
            target=self._run_task,
            args=(task,),
            daemon=True,
        )
        t.start()
        self._threads.append(t)

    def _run_task(self, task: CoordinatedTask) -> None:
        """Execute a single task, handling output and cleanup."""
        try:
            # Build the task description with worktree context
            effective_task = task.description
            if task.worktree_path:
                effective_task = (
                    f"Work in this directory (isolated worktree): {task.worktree_path}\n\n"
                    + effective_task
                )

            def output_callback(line_type: str, text: str) -> None:
                # Notify coordinator callback
                if self._output_callback is not None:
                    try:
                        self._output_callback(WorkerOutput(task.id, line_type, text))
                    except Exception:
                        pass
                # Collect output lines
                with self._lock:
                    clean = text.rstrip()
                    if clean:
                        task.output_lines.append(clean)

            # Run the agent
            run = run_agent(
                agent_name=task.agent,
                task=effective_task,
                output_callback=output_callback,
            )
            task.worker_run_id = run.id

            with self._lock:
                if run.state == "done":
                    task.result = run.answer
                    # Mark as needing approval if there's a worktree
                    if task.worktree_path:
                        task.status = TaskStatus.NEEDS_APPROVAL
                    else:
                        task.status = TaskStatus.COMPLETED
                elif run.state == "failed":
                    task.status = TaskStatus.FAILED
                    task.error = run.fail_reason or "Agent run failed"
                else:
                    task.status = TaskStatus.FAILED
                    task.error = f"Unexpected run state: {run.state}"

                task.finished_at = datetime.now(timezone.utc).isoformat()

        except Exception as e:
            logger.exception("Task %s failed with exception", task.id)
            with self._lock:
                task.status = TaskStatus.FAILED
                task.error = str(e)
                task.finished_at = datetime.now(timezone.utc).isoformat()
        finally:
            with self._lock:
                self._running_count -= 1
                # M6: Clean up worktree on failure
                if task.status == TaskStatus.FAILED and task.worktree_path:
                    self._remove_worktree(task.worktree_path)
                    task.worktree_path = None

    def approve_task(self, task_id: str) -> bool:
        """Approve a task's changes, marking it as completed.

        The actual merge is NOT performed automatically — the caller must
        handle merging the worktree changes to the main branch.

        Args:
            task_id: The task to approve.

        Returns:
            True if the task was approved, False if not found or not in NEEDS_APPROVAL state.
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if task.status != TaskStatus.NEEDS_APPROVAL:
                return False
            task.status = TaskStatus.COMPLETED
            task.approved = True
            task.finished_at = datetime.now(timezone.utc).isoformat()
            return True

    def reject_task(self, task_id: str) -> bool:
        """Reject a task's changes, marking it as failed and cleaning up its worktree.

        Args:
            task_id: The task to reject.

        Returns:
            True if the task was rejected, False if not found or not in NEEDS_APPROVAL state.
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if task.status != TaskStatus.NEEDS_APPROVAL:
                return False
            task.status = TaskStatus.FAILED
            task.error = "Changes rejected by reviewer"
            task.finished_at = datetime.now(timezone.utc).isoformat()
            # Clean up worktree
            if task.worktree_path:
                self._remove_worktree(task.worktree_path)
                task.worktree_path = None
            return True

    def _create_worktree(self, task: CoordinatedTask) -> Optional[str]:
        """Create an isolated worktree for a task.

        Returns the worktree path or None if worktree creation failed.
        """
        try:
            from hub import git_service
            if not git_service.is_git_repo(self.project_dir):
                return None

            # Create a unique branch name
            branch_name = f"agent/{task.name}-{task.id}"
            wt_path = Path(self.worktree_base) / task.id
            wt_path.mkdir(parents=True, exist_ok=True)

            # Get current branch as the base
            status = git_service.get_git_status(self.project_dir)
            base_branch = status.branch or "HEAD"

            if git_service.create_worktree(self.project_dir, base_branch, str(wt_path)):
                return str(wt_path)
            return None
        except Exception as e:
            logger.warning("Failed to create worktree for task %s: %s", task.id, e)
            return None

    def _remove_worktree(self, path: str) -> bool:
        """Remove a worktree."""
        try:
            from hub import git_service
            return git_service.remove_worktree(path)
        except Exception as e:
            logger.warning("Failed to remove worktree %s: %s", path, e)
            return False

    def _cleanup_worktrees(self) -> None:
        """Clean up worktrees for non-approved tasks."""
        for task in self._tasks.values():
            if task.worktree_path and not task.approved:
                self._remove_worktree(task.worktree_path)
                task.worktree_path = None

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the coordination plan."""
        with self._lock:
            tasks = list(self._tasks.values())

        status_counts: Dict[str, int] = {}
        for t in tasks:
            status_counts[t.status] = status_counts.get(t.status, 0) + 1

        return {
            "project_dir": self.project_dir,
            "max_concurrency": self.max_concurrency,
            "total_tasks": len(tasks),
            "running": self._running_count,
            "status_counts": status_counts,
            "is_running": self.is_running(),
            "is_complete": self.is_complete(),
            "tasks": [t.to_dict() for t in tasks],
        }


# --- Global coordinator registry ---

_active_coordinators: Dict[str, TaskCoordinator] = {}
_coordinators_lock = threading.Lock()


def create_coordinator(
    coordinator_id: str,
    project_dir: str,
    max_concurrency: int = 3,
    worktree_base: Optional[str] = None,
) -> TaskCoordinator:
    """Create and register a new coordinator."""
    coord = TaskCoordinator(
        project_dir=project_dir,
        max_concurrency=max_concurrency,
        worktree_base=worktree_base,
    )
    with _coordinators_lock:
        _active_coordinators[coordinator_id] = coord
    return coord


def get_coordinator(coordinator_id: str) -> Optional[TaskCoordinator]:
    """Get a registered coordinator by ID."""
    return _active_coordinators.get(coordinator_id)


def list_coordinators() -> Dict[str, Dict[str, Any]]:
    """List all active coordinators with their summaries."""
    with _coordinators_lock:
        result = {}
        for cid, coord in _active_coordinators.items():
            result[cid] = coord.get_summary()
    return result


def remove_coordinator(coordinator_id: str) -> bool:
    """Remove a coordinator after it completes."""
    with _coordinators_lock:
        coord = _active_coordinators.pop(coordinator_id, None)
        if coord is None:
            return False
        # Clean up any remaining worktrees
        coord._cleanup_worktrees()
        return True
