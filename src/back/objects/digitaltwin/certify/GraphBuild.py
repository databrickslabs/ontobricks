"""One Build: the Original Graph, then the Certified Graph, in a single task.

The Original build (gateway VIEW, ``..._data`` VIEW, search cache) runs first;
when it produced triples, the certified build composes the Certified Graph and
loads its backend. Both phases keep their own build-run record. A read-only
(IN-REVIEW or PUBLISHED) version is refused before anything is rebuilt (D9).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.digitaltwin.build import TwinLakehouseBuild
from back.objects.digitaltwin.certify.CertifiedBuild import CertifiedBuild
from back.objects.digitaltwin.models import DomainSnapshot


class GraphBuild:
    """Run the Original build then the certified build as one tracked task."""

    @classmethod
    def start(cls, domain: Any, settings: Any, *, build_kind: str = "session") -> str:
        """Start the build of the loaded version; return the task id.

        Raises before any task is created when the version is read-only, the
        mapping is not buildable or a warehouse is missing.
        """
        from back.core.helpers import get_build_sql_credentials
        from back.core.task_manager import get_task_manager

        CertifiedBuild.ensure_editable(domain, settings)
        original = TwinLakehouseBuild.prepare(domain, settings)
        builder = GraphDBFactory.get_builder(domain, settings, engine="delta")
        host, token, _warehouse_id = get_build_sql_credentials(domain, settings)
        snapshot = DomainSnapshot(domain, host=host, token=token)
        steps = list(builder.build_steps()) + CertifiedBuild.task_steps(domain, settings)
        tm = get_task_manager()
        task = tm.run_background_task(
            "Graph Build",
            "graph_build",
            cls._run,
            tm,
            original,
            builder,
            snapshot,
            settings,
            build_kind,
            steps=steps,
        )
        return task.id

    @staticmethod
    def _run(
        task: Any,
        tm: Any,
        original: Any,
        builder: Any,
        snapshot: Any,
        settings: Any,
        build_kind: str,
    ) -> None:
        chained = _ChainedTaskManager(tm)
        original._run(task, chained, builder, build_kind)
        if chained.failed:
            return
        if not (chained.original_result or {}).get("triple_count"):
            tm.complete_task(
                task.id, result=chained.original_result, message=chained.original_message
            )
            return
        chained.enter_certified_phase()
        CertifiedBuild(chained, task.id, snapshot, settings).run()


class _ChainedTaskManager:
    """Task manager each phase reports to: one start and one completion for both.

    The Original phase's completion is kept (not reported); the certified
    phase's start becomes the next step and its completion ends the task with
    both results.
    """

    def __init__(self, tm: Any) -> None:
        self._tm = tm
        self._certified = False
        self.failed = False
        self.original_result: Optional[Dict[str, Any]] = None
        self.original_message = ""

    def __getattr__(self, name: str) -> Any:
        return getattr(self._tm, name)

    def enter_certified_phase(self) -> None:
        self._certified = True

    def start_task(self, task_id: str, message: str = "Starting...") -> bool:
        if self._certified:
            return self._tm.advance_step(task_id, message)
        return self._tm.start_task(task_id, message)

    def complete_task(
        self, task_id: str, result: Any = None, message: str = "Completed"
    ) -> bool:
        if not self._certified:
            self.original_result = dict(result or {})
            self.original_message = message
            return True
        merged = {**(result or {}), "original": self.original_result}
        return self._tm.complete_task(
            task_id, result=merged, message=f"{self.original_message}; {message}"
        )

    def fail_task(self, task_id: str, error: str) -> bool:
        self.failed = True
        return self._tm.fail_task(task_id, error)
