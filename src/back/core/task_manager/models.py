"""Task data models for the async task manager."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum
from dataclasses import dataclass, field, asdict


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskStep:
    """A step within a task."""

    name: str
    description: str
    status: str = "pending"
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class Task:
    """Represents an async task."""

    id: str
    name: str
    task_type: str
    status: TaskStatus = TaskStatus.PENDING
    progress: int = 0
    message: str = ""
    result: Any = None
    error: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    steps: List[TaskStep] = field(default_factory=list)
    current_step: int = 0

    def duration_seconds(self) -> Optional[float]:
        """Wall-clock duration in seconds.

        For terminal tasks: ``completed_at - started_at`` (falls back to
        ``created_at`` if the worker never called ``start_task``).
        For running/pending tasks: elapsed time until *now* so the UI can
        show a live timer. ``None`` only if no usable timestamp exists.
        """
        start_iso = self.started_at or self.created_at
        if not start_iso:
            return None
        try:
            start = datetime.fromisoformat(start_iso)
        except ValueError:
            return None
        if self.completed_at:
            try:
                end = datetime.fromisoformat(self.completed_at)
            except ValueError:
                return None
        else:
            end = datetime.now()
        return max(0.0, (end - start).total_seconds())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "task_type": self.task_type,
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds(),
            "steps": [s.to_dict() for s in self.steps],
            "current_step": self.current_step,
        }
