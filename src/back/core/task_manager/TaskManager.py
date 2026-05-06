"""Async Task Manager — in-memory task tracking for long-running operations."""

import uuid
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.core.task_manager.models import Task, TaskStatus, TaskStep

logger = get_logger(__name__)


def _format_duration(seconds: Optional[float]) -> str:
    """Human-friendly duration used in log lines and notifications."""
    if seconds is None:
        return "n/a"
    if seconds < 1:
        return f"{int(seconds * 1000)}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, rem = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {rem:.1f}s"
    hours, rem_min = divmod(minutes, 60)
    return f"{int(hours)}h {int(rem_min)}m"


class TaskManager:
    """Manages async tasks in memory."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._tasks: Dict[str, Task] = {}
        self._max_tasks = 100
        self._initialized = True

    def create_task(
        self, name: str, task_type: str, steps: List[Dict[str, str]] = None
    ) -> Task:
        task_id = str(uuid.uuid4())[:8]
        task_steps = []
        if steps:
            for step_def in steps:
                task_steps.append(
                    TaskStep(
                        name=step_def.get("name", ""),
                        description=step_def.get("description", ""),
                    )
                )
        task = Task(id=task_id, name=name, task_type=task_type, steps=task_steps)
        self._tasks[task_id] = task
        self._cleanup_old_tasks()
        logger.info("Created task %s: %s", task_id, name)
        return task

    def get_task(self, task_id: str) -> Optional[Task]:
        return self._tasks.get(task_id)

    def get_all_tasks(self, include_completed: bool = True) -> List[Task]:
        tasks = list(self._tasks.values())
        if not include_completed:
            tasks = [
                t for t in tasks if t.status in (TaskStatus.PENDING, TaskStatus.RUNNING)
            ]
        tasks.sort(key=lambda t: t.created_at, reverse=True)
        return tasks

    def get_active_tasks(self) -> List[Task]:
        return self.get_all_tasks(include_completed=False)

    def start_task(self, task_id: str, message: str = "Starting...") -> bool:
        task = self._tasks.get(task_id)
        if not task:
            return False
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now().isoformat()
        task.message = message
        task.progress = 0
        if task.steps:
            task.steps[0].status = "running"
            task.steps[0].started_at = datetime.now().isoformat()
            task.current_step = 0
            logger.info(
                "START task %s [%s] — %s (step 1/%s: %s)",
                task_id,
                task.task_type,
                task.name,
                len(task.steps),
                task.steps[0].name,
            )
        else:
            logger.info(
                "START task %s [%s] — %s: %s",
                task_id,
                task.task_type,
                task.name,
                message,
            )
        return True

    def update_progress(self, task_id: str, progress: int, message: str = None) -> bool:
        task = self._tasks.get(task_id)
        if not task:
            return False
        clamped = min(100, max(0, progress))
        task.progress = max(task.progress, clamped)
        if message:
            task.message = message
        return True

    def advance_step(self, task_id: str, message: str = None) -> bool:
        task = self._tasks.get(task_id)
        if not task or not task.steps:
            return False
        if task.current_step < len(task.steps):
            task.steps[task.current_step].status = "completed"
            task.steps[task.current_step].completed_at = datetime.now().isoformat()
        task.current_step += 1
        if task.current_step < len(task.steps):
            task.steps[task.current_step].status = "running"
            task.steps[task.current_step].started_at = datetime.now().isoformat()
            if message:
                task.message = message
            else:
                task.message = task.steps[task.current_step].description
            logger.info(
                "Task %s step %s/%s: %s",
                task_id,
                task.current_step + 1,
                len(task.steps),
                task.steps[task.current_step].name,
            )
        step_progress = int((task.current_step / len(task.steps)) * 100)
        task.progress = max(task.progress, step_progress)
        return True

    def skip_step(self, task_id: str, message: str = None) -> bool:
        """Mark the current step as skipped and advance to the next one.

        Use this when a workflow phase isn't executed for the current run
        (e.g. *Checking source tables* when the user forced a full rebuild).
        Without this, the seeded step list and the actual execution drift
        because the next ``advance_step`` call lands a "creating view"
        message inside a row labelled "checking source tables".
        """
        task = self._tasks.get(task_id)
        if not task or not task.steps:
            return False
        now = datetime.now().isoformat()
        if task.current_step < len(task.steps):
            step = task.steps[task.current_step]
            step.status = "skipped"
            step.started_at = step.started_at or now
            step.completed_at = now
            logger.info(
                "Task %s step %s/%s skipped: %s",
                task_id,
                task.current_step + 1,
                len(task.steps),
                step.name,
            )
        task.current_step += 1
        if task.current_step < len(task.steps):
            task.steps[task.current_step].status = "running"
            task.steps[task.current_step].started_at = datetime.now().isoformat()
            if message:
                task.message = message
            else:
                task.message = task.steps[task.current_step].description
        step_progress = int((task.current_step / len(task.steps)) * 100)
        task.progress = max(task.progress, step_progress)
        return True

    def complete_task(
        self, task_id: str, result: Any = None, message: str = "Completed"
    ) -> bool:
        task = self._tasks.get(task_id)
        if not task:
            return False
        task.status = TaskStatus.COMPLETED
        task.completed_at = datetime.now().isoformat()
        task.progress = 100
        task.message = message
        task.result = result
        for step in task.steps:
            if step.status not in ("completed", "skipped"):
                step.status = "completed"
                step.completed_at = datetime.now().isoformat()
        logger.info(
            "END task %s [%s] completed in %s — %s",
            task_id,
            task.task_type,
            _format_duration(task.duration_seconds()),
            message,
        )
        return True

    def fail_task(self, task_id: str, error: str) -> bool:
        task = self._tasks.get(task_id)
        if not task:
            return False
        task.status = TaskStatus.FAILED
        task.completed_at = datetime.now().isoformat()
        task.error = error
        task.message = f"Failed: {error[:100]}"
        if task.steps and task.current_step < len(task.steps):
            task.steps[task.current_step].status = "failed"
        duration = _format_duration(task.duration_seconds())
        logger.error("Task %s failed after %s: %s", task_id, duration, error)
        logger.info(
            "END task %s [%s] failed in %s",
            task_id,
            task.task_type,
            duration,
        )
        return True

    def cancel_task(self, task_id: str) -> bool:
        task = self._tasks.get(task_id)
        if not task:
            return False
        if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now().isoformat()
            task.message = "Cancelled"
            logger.info(
                "END task %s [%s] cancelled in %s",
                task_id,
                task.task_type,
                _format_duration(task.duration_seconds()),
            )
            return True
        return False

    def delete_task(self, task_id: str) -> bool:
        if task_id in self._tasks:
            del self._tasks[task_id]
            return True
        return False

    def clear_completed(self) -> int:
        to_remove = [
            tid
            for tid, task in self._tasks.items()
            if task.status
            in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)
        ]
        for tid in to_remove:
            del self._tasks[tid]
        return len(to_remove)

    def _cleanup_old_tasks(self):
        if len(self._tasks) <= self._max_tasks:
            return
        sorted_tasks = sorted(
            self._tasks.items(),
            key=lambda x: (
                x[1].status in (TaskStatus.PENDING, TaskStatus.RUNNING),
                x[1].created_at,
            ),
        )
        while len(self._tasks) > self._max_tasks:
            task_id, task = sorted_tasks.pop(0)
            if task.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
                del self._tasks[task_id]

    def run_background_task(
        self,
        name: str,
        task_type: str,
        target: Any,
        *args: Any,
        steps: Optional[List[Dict[str, str]]] = None,
        **kwargs: Any,
    ) -> Task:
        """Create a tracked task, run *target* in a daemon thread, return the task."""
        task = self.create_task(name, task_type, steps=steps)
        thread = threading.Thread(
            target=target, args=(task, *args), kwargs=kwargs, daemon=True
        )
        thread.start()
        return task
