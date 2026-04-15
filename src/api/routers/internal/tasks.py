"""
Internal API -- Task management JSON endpoints.

Moved from app/frontend/tasks/routes.py during the front/back split.
"""
from fastapi import APIRouter

from back.core.task_manager import get_task_manager, TaskStatus

router = APIRouter(prefix="/tasks", tags=["Tasks"])


@router.get("/")
async def list_tasks(include_completed: bool = True):
    """List all tasks."""
    tm = get_task_manager()
    tasks = tm.get_all_tasks(include_completed=include_completed)
    
    return {
        'success': True,
        'tasks': [t.to_dict() for t in tasks],
        'active_count': len([t for t in tasks if t.status in (TaskStatus.PENDING, TaskStatus.RUNNING)])
    }


@router.get("/{task_id}")
async def get_task(task_id: str):
    """Get a specific task by ID."""
    tm = get_task_manager()
    task = tm.get_task(task_id)
    
    if not task:
        return {'success': False, 'message': 'Task not found'}
    
    return {
        'success': True,
        'task': task.to_dict()
    }


@router.post("/{task_id}/cancel")
async def cancel_task(task_id: str):
    """Cancel a running task."""
    tm = get_task_manager()
    
    if tm.cancel_task(task_id):
        return {'success': True, 'message': 'Task cancelled'}
    else:
        return {'success': False, 'message': 'Cannot cancel task (not found or already completed)'}


@router.post("/clear-completed")
async def clear_completed_tasks():
    """Clear all completed/failed/cancelled tasks."""
    tm = get_task_manager()
    count = tm.clear_completed()
    
    return {
        'success': True,
        'message': f'Cleared {count} tasks',
        'cleared_count': count
    }
