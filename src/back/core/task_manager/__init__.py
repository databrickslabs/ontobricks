"""Async Task Manager — in-memory task tracking for long-running operations."""

from back.core.task_manager.models import TaskStatus, TaskStep, Task  # noqa: F401
from back.core.task_manager.TaskManager import TaskManager  # noqa: F401

# Global singleton instance
task_manager = TaskManager()


def get_task_manager() -> TaskManager:
    """Get the global task manager instance."""
    return task_manager


def run_background_task(name, task_type, target, *args, steps=None, **kwargs) -> Task:
    """Backward-compatible wrapper — delegates to TaskManager.run_background_task."""
    return task_manager.run_background_task(
        name, task_type, target, *args, steps=steps, **kwargs
    )


__all__ = [
    "TaskStatus",
    "TaskStep",
    "Task",
    "TaskManager",
    "get_task_manager",
    "run_background_task",
    "task_manager",
]
