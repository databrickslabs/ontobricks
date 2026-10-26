"""Async Task Manager — in-memory task tracking for long-running operations."""

from back.core.task_manager.TaskStatus import TaskStatus  # noqa: F401
from back.core.task_manager.Task import Task, TaskLogEntry, TaskStep  # noqa: F401
from back.core.task_manager.TaskManager import TaskManager  # noqa: F401

# Global singleton instance
task_manager = TaskManager()


def get_task_manager() -> TaskManager:
    """Get the global task manager instance."""
    return task_manager


__all__ = [
    "TaskStatus",
    "TaskStep",
    "TaskLogEntry",
    "Task",
    "TaskManager",
    "get_task_manager",
    "task_manager",
]
