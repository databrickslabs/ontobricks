"""Async Task Manager — in-memory task tracking for long-running operations."""

import uuid
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.core.task_manager.models import Task, TaskStatus, TaskStep

logger = get_logger(__name__)


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
                "Task %s started, step 1/%s: %s",
                task_id,
                len(task.steps),
                task.steps[0].name,
            )
        else:
            logger.info("Task %s started: %s", task_id, message)
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
            if step.status != "completed":
                step.status = "completed"
                step.completed_at = datetime.now().isoformat()
        logger.info("Task %s completed: %s", task_id, message)
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
        logger.error("Task %s failed: %s", task_id, error)
        return True

    def cancel_task(self, task_id: str) -> bool:
        task = self._tasks.get(task_id)
        if not task:
            return False
        if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now().isoformat()
            task.message = "Cancelled"
            logger.info("Task %s cancelled", task_id)
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
