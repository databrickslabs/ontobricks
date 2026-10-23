"""Background execution of one simulation run (TaskManager thread body)."""

from __future__ import annotations

import datetime as _dt
import time
from typing import Any, Dict, Optional

from back.core.errors import InfrastructureError, OntoBricksError
from back.core.helpers import effective_graph_name
from back.core.logging import get_logger
from back.objects.simulation.SimulationDiff import SimulationDiff
from back.objects.simulation.SimulationReasoningService import SimulationReasoningService
from back.objects.simulation.SimulationSandbox import SimulationSandbox
from back.objects.simulation.SimulationSpec import SimulationSpec

logger = get_logger(__name__)


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


class SimulationRunner:
    """Scope → seed sandbox → apply patches → reason → diff → persist.

    Everything except the initial scope read happens inside the sandbox
    table; the live graph is never written.
    """

    def __init__(
        self,
        *,
        tm: Any,
        task_id: str,
        settings: Any,
        domain_snap: Any,
        registry: Any,
        folder: str,
        version: str,
        spec: SimulationSpec,
        owner: str = "",
        cohort_rule: Optional[Dict[str, Any]] = None,
        get_store: Any = None,
    ) -> None:
        self._tm = tm
        self._task_id = task_id
        self._settings = settings
        self._domain = domain_snap
        self._registry = registry
        self._folder = folder
        self._version = version
        self._spec = spec
        self._owner = owner
        self._cohort_rule = cohort_rule
        self._get_store = get_store

    def _entry(self, status: str, **extra: Any) -> Dict[str, Any]:
        entry = {
            "id": self._spec.id,
            "label": self._spec.label,
            "status": status,
            "spec": self._spec.to_dict(),
            "owner": self._owner,
            "task_id": self._task_id,
        }
        entry.update(extra)
        return entry

    def _progress(self, pct: int, msg: str) -> None:
        self._tm.update_progress(self._task_id, pct, msg)

    def _live_store(self) -> Any:
        if self._get_store is not None:
            return self._get_store(self._domain, self._settings)
        from back.core.graphdb import get_graphdb

        return get_graphdb(self._domain, self._settings)

    def run(self) -> None:
        started_at = _now_iso()
        t0 = time.time()
        sim_id = self._spec.id
        tm, task_id = self._tm, self._task_id
        try:
            tm.start_task(task_id, "Preparing simulation sandbox")
            self._registry.save_simulation(
                self._folder, self._version, self._entry("running")
            )
            result = self._execute()
            duration = round(time.time() - t0, 3)
            summary = dict(result["summary"])
            summary["duration_s"] = duration
            self._registry.save_simulation(
                self._folder,
                self._version,
                self._entry(
                    "ready",
                    diff_summary=summary,
                    diff={"added": result["added"], "removed": result["removed"]},
                    last_run_at=_now_iso(),
                ),
            )
            self._registry.record_simulation_run(
                self._folder,
                self._version,
                {
                    "simulation_id": sim_id,
                    "status": "success",
                    "message": (
                        f"+{summary['added_count']} / -{summary['removed_count']} triples"
                    ),
                    "task_id": task_id,
                    "started_at": started_at,
                    "finished_at": _now_iso(),
                    "duration_s": duration,
                    "stats": summary,
                    "owner": self._owner,
                },
            )
            tm.complete_task(
                task_id,
                result={"simulation_id": sim_id, "summary": summary},
                message=(
                    f"Simulation complete: +{summary['added_count']} / "
                    f"-{summary['removed_count']} triples"
                ),
            )
        except Exception as exc:  # noqa: BLE001
            message = (
                exc.message
                if isinstance(exc, OntoBricksError)
                else "Simulation failed unexpectedly — check the application logs"
            )
            logger.error("Simulation %s failed: %s", sim_id, exc, exc_info=True)
            try:
                self._registry.save_simulation(
                    self._folder,
                    self._version,
                    self._entry("failed", error=message[:2000], last_run_at=_now_iso()),
                )
            except Exception as save_exc:  # noqa: BLE001
                logger.warning("Could not persist failed simulation %s: %s", sim_id, save_exc)
            self._registry.record_simulation_run(
                self._folder,
                self._version,
                {
                    "simulation_id": sim_id,
                    "status": "error",
                    "error": message[:2000],
                    "task_id": task_id,
                    "started_at": started_at,
                    "finished_at": _now_iso(),
                    "duration_s": round(time.time() - t0, 3),
                    "owner": self._owner,
                },
            )
            tm.fail_task(task_id, message)

    def _execute(self) -> Dict[str, Any]:
        from back.objects.digitaltwin import CohortService

        self._progress(5, "Opening graph backend")
        live = self._live_store()
        if live is None:
            raise InfrastructureError("Graph backend is not configured")
        graph_name = effective_graph_name(self._domain)

        self._progress(10, "Resolving simulation scope")
        entities = self._spec.scope.resolve_entities(
            live,
            graph_name,
            cohort_service=CohortService(self._domain),
            cohort_rule=self._cohort_rule,
        )

        sandbox = SimulationSandbox(live, graph_name, self._spec.id)
        self._progress(20, f"Seeding sandbox with {len(entities)} entities")
        sandbox.reset()
        sandbox.seed(entities)
        before = sandbox.triples()

        self._progress(40, f"Applying {len(self._spec.patches)} patches")
        asserted = sandbox.apply(self._spec.patches)

        self._progress(50, "Running reasoning in the sandbox")
        svc = SimulationReasoningService(self._domain, sandbox.store, sandbox.table)

        def _swrl_progress(idx: int, total: int, rule_name: str) -> None:
            self._progress(50 + int((idx / max(total, 1)) * 30), f"SWRL {idx + 1}/{total}: {rule_name}")

        reasoning = svc.run_full_reasoning(
            self._spec.reasoning_options(), progress_callback=_swrl_progress
        )
        provenance = {
            SimulationDiff.key(
                {"subject": t.subject, "predicate": t.predicate, "object": t.object}
            ): t.provenance
            for t in reasoning.inferred_triples
        }
        self._progress(85, f"Materialising {len(reasoning.inferred_triples)} inferred triples")
        svc.materialize_inferred(reasoning)

        self._progress(92, "Computing before/after diff")
        diff = SimulationDiff.compute(
            before, sandbox.triples(), patch_added=asserted, provenance=provenance
        )
        stats = reasoning.stats or {}
        diff["summary"].update(
            {
                "entity_count": len(entities),
                "patch_count": len(self._spec.patches),
                "inferred_total": len(reasoning.inferred_triples),
                "phase_errors": {
                    k[: -len("_error")]: v for k, v in stats.items() if k.endswith("_error")
                },
                "sandbox_table": sandbox.table,
            }
        )
        return diff
