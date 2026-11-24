"""
Internal API -- Task management JSON endpoints.

Moved from app/frontend/tasks/routes.py during the front/back split.
Tasks are scoped to their owner; admins see and manage every task.
"""

from typing import Tuple

from fastapi import APIRouter, Depends, Request

from api.routers.internal._guards import require
from back.core.errors import AuthorizationError, ConflictError, NotFoundError
from back.core.task_manager import get_task_manager, TaskStatus
from back.objects.registry import ROLE_ADMIN, role_level

router = APIRouter(prefix="/tasks", tags=["Tasks"])


def _caller(request: Request) -> Tuple[str, bool]:
    """Return ``(email, is_admin)`` for the current request."""
    email = getattr(request.state, "user_email", "") or ""
    role = getattr(request.state, "user_role", "") or ""
    return email, role_level(role) >= role_level(ROLE_ADMIN)


@router.get("/")
async def list_tasks(request: Request, include_completed: bool = True):
    """List the caller's tasks (every task for admins)."""
    email, is_admin = _caller(request)
    tasks = [
        t
        for t in get_task_manager().get_all_tasks(include_completed=include_completed)
        if t.visible_to(email, is_admin)
    ]

    return {
        "success": True,
        "tasks": [t.to_dict() for t in tasks],
        "active_count": len(
            [t for t in tasks if t.status in (TaskStatus.PENDING, TaskStatus.RUNNING)]
        ),
    }


@router.get("/{task_id}")
async def get_task(task_id: str, request: Request):
    """Get a specific task by ID."""
    task = get_task_manager().get_task(task_id)
    if not task or not task.visible_to(*_caller(request)):
        raise NotFoundError("Task not found")

    return {"success": True, "task": task.to_dict()}


@router.post("/{task_id}/cancel")
async def cancel_task(task_id: str, request: Request):
    """Cancel a running task."""
    tm = get_task_manager()
    task = tm.get_task(task_id)
    if not task or not task.visible_to(*_caller(request)):
        raise NotFoundError("Task not found")
    if not task.cancellable_by(*_caller(request)):
        raise AuthorizationError("Only the task owner or an admin can cancel this task")

    if tm.cancel_task(task_id):
        return {"success": True, "message": "Task cancelled"}
    raise ConflictError("Cannot cancel task (already completed)")


@router.post("/clear-completed", dependencies=[Depends(require(ROLE_ADMIN))])
async def clear_completed_tasks():
    """Clear all completed/failed/cancelled tasks (admin only: affects every user)."""
    count = get_task_manager().clear_completed()

    return {
        "success": True,
        "message": f"Cleared {count} tasks",
        "cleared_count": count,
    }
