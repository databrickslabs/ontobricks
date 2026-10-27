"""Background execution of one simulation run (TaskManager thread body)."""

from __future__ import annotations

import datetime as _dt
import time
from typing import Any, Dict, Optional

from back.core.errors import InfrastructureError, OntoBricksError
from back.core.helpers import effective_graph_name, extract_local_name, is_uri
from back.core.logging import get_logger
from back.objects.simulation.SimulationDiff import SimulationDiff
from back.objects.simulation.SimulationPatch import SimulationPatch
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

    def _is_current_run(self) -> bool:
        row = self._registry.load_simulation(self._folder, self._version, self._spec.id)
        return bool(row) and str(row.get("task_id") or "") == str(self._task_id)

    def _live_store(self) -> Any:
        if self._get_store is not None:
            return self._get_store(self._domain, self._settings)
        from back.core.graphdb import get_graphdb

        return get_graphdb(self._domain, self._settings)

    def _hypothesis_uris(self) -> list:
        return list(
            dict.fromkeys(
                uri
                for p in self._spec.patches
                for uri in (p.subject, p.object)
                if uri and (uri == p.subject or is_uri(uri))
            )
        )

    def _sandbox_patches(self) -> list[SimulationPatch]:
        """Resolve ontology predicate URIs to the R2RML data namespace.

        The fact picker supplies ontology URIs (normally ``...#hascontract``),
        while mapped triples use ``.../hascontract``. Applying the raw URI
        creates a disconnected sandbox edge that SWRL cannot join.
        """
        ontology = getattr(self._domain, "ontology", None) or {}
        base_uri = str(ontology.get("base_uri") or "")
        data_ns = base_uri.rstrip("#/") + "/" if base_uri else ""
        aliases: Dict[str, str] = {}
        for prop in ontology.get("properties") or []:
            if not isinstance(prop, dict):
                continue
            name = str(prop.get("name") or prop.get("localName") or "").strip()
            uri = str(prop.get("uri") or "").strip()
            if not name and not uri:
                continue
            canonical = data_ns + extract_local_name(uri or name) if data_ns else (uri or name)
            for key in (name, uri, extract_local_name(uri) if uri else ""):
                if key:
                    aliases[key.lower()] = canonical
        return [
            SimulationPatch(
                op=patch.op,
                subject=patch.subject,
                predicate=aliases.get(patch.predicate.lower(), patch.predicate),
                object=patch.object,
                datatype=patch.datatype,
            )
            for patch in self._spec.patches
        ]

    def _seed_hypothesis(self, sandbox: Any, skip: Any = None) -> None:
        """Copy patched entities not already in the scope, plus one live hop.

        Skip the scope slice (already copied). One hop plus a sandbox object
        walk is enough for ``hasContract`` → ``hasMeter``; two hops from the
        customer re-traverses the live graph and freezes the 30% progress bar.
        """
        ignored = set(skip or [])
        uris = [u for u in self._hypothesis_uris() if u not in ignored]
        if not uris:
            return
        self._progress(30, f"Seeding {len(uris)} hypothesis entities and neighbours")
        seed_nb = getattr(sandbox, "seed_neighbourhood", None)
        if callable(seed_nb):
            seed_nb(uris, hops=1)
        else:
            sandbox.seed(uris)

    def _require_swrl(self, stats: Dict[str, Any]) -> None:
        """Fail the run when the SWRL phase was requested but did not execute."""
        if not self._spec.phases.get("swrl", True):
            return
        if stats.get("swrl_error"):
            raise InfrastructureError(f"SWRL failed: {stats['swrl_error']}")
        if stats.get("swrl_skipped"):
            reason = stats.get("swrl_reason") or "SWRL did not run"
            raise InfrastructureError(f"SWRL did not run: {reason}")
        errors = stats.get("swrl_errors") or 0
        if not errors:
            return
        details = stats.get("swrl_error_details") or ""
        suffix = f": {details}" if details else ""
        raise InfrastructureError(f"SWRL failed on {errors} rule(s){suffix}")

    def _relink_hypothesis(self, sandbox: Any) -> None:
        """After patches, copy objects of new links only (not the patched subjects).

        Re-seeding a patched subject from live would restore values that a
        Set/Remove hypothesis just replaced.
        """
        subjects = {
            p.subject for p in self._spec.patches if p.subject
        }
        uri_objects = getattr(sandbox, "uri_objects_of", None)
        if not subjects or not callable(uri_objects):
            return
        extra = [
            obj
            for subject in subjects
            for obj in uri_objects(subject)
            if obj and obj not in subjects
        ]
        extra = list(dict.fromkeys(extra))
        if not extra:
            return
        self._progress(42, f"Copying {len(extra)} linked entities for reasoning")
        seed_nb = getattr(sandbox, "seed_neighbourhood", None)
        if callable(seed_nb):
            seed_nb(extra, hops=1)
        else:
            sandbox.seed(extra)

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
            if not self._is_current_run():
                logger.warning(
                    "Simulation %s run %s was aborted; dropping result",
                    sim_id,
                    task_id,
                )
                return
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
            if not self._is_current_run():
                logger.warning(
                    "Simulation %s run %s was aborted during failure handling",
                    sim_id,
                    task_id,
                )
                return
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
        self._seed_hypothesis(sandbox, skip=entities)
        self._progress(38, "Recording the before-picture")
        before = sandbox.triples()

        self._progress(40, f"Applying {len(self._spec.patches)} patches")
        asserted = sandbox.apply(self._sandbox_patches())
        self._relink_hypothesis(sandbox)

        self._progress(50, "Running reasoning in the sandbox")
        svc = SimulationReasoningService(self._domain, sandbox.store, sandbox.table)

        def _swrl_progress(idx: int, total: int, rule_name: str) -> None:
            self._progress(50 + int((idx / max(total, 1)) * 30), f"SWRL {idx + 1}/{total}: {rule_name}")

        reasoning = svc.run_full_reasoning(
            self._spec.reasoning_options(), progress_callback=_swrl_progress
        )
        self._require_swrl(reasoning.stats or {})
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
                "swrl_inferred_count": stats.get("swrl_inferred_count"),
                "swrl_rules_count": stats.get("swrl_rules_count"),
                "swrl_skipped": stats.get("swrl_skipped"),
                "swrl_reason": stats.get("swrl_reason"),
                "swrl_errors": stats.get("swrl_errors"),
                "swrl_error_details": stats.get("swrl_error_details"),
                "phase_errors": {
                    k[: -len("_error")]: v for k, v in stats.items() if k.endswith("_error")
                },
                "sandbox_table": sandbox.table,
            }
        )
        ensure = getattr(sandbox, "ensure_explorer_views", None)
        if callable(ensure):
            from back.core.helpers import effective_graph_query_table

            live_union = effective_graph_query_table(
                self._domain, self._settings, include_inferred=True, store=live
            )
            live_asserted = (
                live.synced_table_name(graph_name)
                if hasattr(live, "synced_table_name")
                else live_union
            )
            ensure(live_union, live_asserted)
        return diff
