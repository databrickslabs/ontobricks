"""Facade for what-if simulations of the active domain."""

from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.graphdb.GraphDBFactory import normalize_graph_backend
from back.core.helpers import effective_graph_name, effective_graph_query_table
from back.core.logging import get_logger
from back.objects.simulation.SimulationArtifacts import SimulationArtifacts
from back.objects.simulation.SimulationDiff import SimulationDiff
from back.objects.simulation.SimulationRunner import SimulationRunner
from back.objects.simulation.SimulationSpec import SimulationSpec

logger = get_logger(__name__)


class Simulation:
    """CRUD, run orchestration and diff paging for a domain's simulations.

    Scenario metadata lives in the registry (``simulations`` /
    ``simulation_runs``); sandbox triples live in ``<graph>__sim_<id>``.
    """

    DEFAULT_HOPS = 2
    DEFAULT_CLASS_CAP = 5000
    SUPPORTED_BACKENDS = frozenset({"lakebase", "databricks", "neo4j"})

    def __init__(self, domain: Any, settings: Any, *, registry: Any = None) -> None:
        self._domain = domain
        self._settings = settings
        self._registry = registry

    # -- Capability ---------------------------------------------------------

    @classmethod
    def is_backend_supported(cls, domain: Any) -> bool:
        info = getattr(domain, "info", None) or {}
        return normalize_graph_backend(info.get("graph_backend")) in cls.SUPPORTED_BACKENDS

    @property
    def supported(self) -> bool:
        return self.is_backend_supported(self._domain)

    def require_supported(self) -> None:
        if not self.supported:
            raise ValidationError(
                "Simulations require a Lakebase, Databricks Delta, or Neo4j graph backend"
            )

    # -- Context ------------------------------------------------------------

    @property
    def folder(self) -> str:
        folder = getattr(self._domain, "uc_domain_folder", "") or ""
        if not folder:
            raise ValidationError("Save the domain to the registry before simulating")
        return folder

    @property
    def version(self) -> str:
        return str(getattr(self._domain, "current_version", "") or "1")

    @property
    def registry(self) -> Any:
        """Registry store holding the ``simulations`` / ``simulation_runs`` rows."""
        if self._registry is None:
            from back.objects.registry import RegistryService

            self._registry = RegistryService.from_context(self._domain, self._settings).store
        return self._registry

    def parse_spec(self, body: Dict[str, Any], sim_id: Optional[str] = None) -> SimulationSpec:
        return SimulationSpec.from_dict(
            body,
            default_hops=self.DEFAULT_HOPS,
            default_cap=self.DEFAULT_CLASS_CAP,
            sim_id=sim_id,
        )

    # -- CRUD ---------------------------------------------------------------

    def list_all(self) -> List[Dict[str, Any]]:
        return [
            self._reap_stale_run(row)
            for row in self.registry.list_simulations(self.folder, self.version)
        ]

    def get(self, sim_id: str) -> Dict[str, Any]:
        row = self.registry.load_simulation(self.folder, self.version, sim_id)
        if not row:
            raise NotFoundError(f"Simulation '{sim_id}' was not found")
        return self._with_artifacts(self._reap_stale_run(row))

    def _with_artifacts(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if not row:
            return row
        row["artifacts"] = SimulationArtifacts.describe(self._domain, row)
        return row

    def sandbox_relation(self, sim_id: str, *, store: Any = None) -> str:
        """Physical Lakebase table of a *ready* simulation sandbox.

        Explorer reads the union view from :meth:`explorer_relation`, not this
        table. Draft / running / failed scenarios have no guaranteed snapshot.
        """
        self.require_supported()
        row = self.get(sim_id)
        if row.get("status") != "ready":
            raise ValidationError(
                "Run the simulation before exploring its sandbox"
            )
        backend = normalize_graph_backend(
            (getattr(self._domain, "info", None) or {}).get("graph_backend")
        )
        if backend == "lakebase":
            from back.core.graphdb.lakebase import LakebaseSimulationStore

            return LakebaseSimulationStore.sandbox_table_name(
                effective_graph_name(self._domain), sim_id
            )
        from back.objects.simulation.SimulationSandbox import SimulationSandbox

        if store is None:
            from back.core.graphdb import get_graphdb

            store = get_graphdb(self._domain, self._settings)
        if store is None:
            raise InfrastructureError("Graph backend is not configured")
        return SimulationSandbox(store, effective_graph_name(self._domain), sim_id).table

    def explorer_relation(
        self,
        sim_id: str,
        *,
        include_inferred: bool = True,
        store: Any = None,
    ) -> str:
        """Live graph UNION sandbox after-picture for Explorer reads.

        Sandbox predicates overlay live ones; other live facts of the same
        entity stay visible. The view is created lazily.
        """
        graph_name = effective_graph_name(self._domain)
        graph_store = store
        if graph_store is None:
            from back.core.graphdb import get_graphdb

            graph_store = get_graphdb(self._domain, self._settings)
        if graph_store is None:
            raise InfrastructureError("Graph backend is not configured")
        backend = normalize_graph_backend(
            (getattr(self._domain, "info", None) or {}).get("graph_backend")
        )
        if backend == "lakebase":
            from back.core.graphdb.lakebase import LakebaseSimulationUnion, _companion_ddl

            view = _companion_ddl.sim_union_phy(
                graph_name, sim_id, include_inferred=include_inferred
            )
            LakebaseSimulationUnion(graph_store).ensure(
                view,
                effective_graph_query_table(
                    self._domain,
                    self._settings,
                    include_inferred=include_inferred,
                    store=graph_store,
                ),
                self.sandbox_relation(sim_id, store=graph_store),
            )
            return view
        sandbox = self.sandbox_relation(sim_id, store=graph_store)
        from back.objects.simulation.SimulationSandbox import SimulationSandbox

        simulation_sandbox = SimulationSandbox(graph_store, graph_name, sim_id)
        simulation_sandbox.ensure_explorer_views(
            effective_graph_query_table(
                self._domain, self._settings, include_inferred=True, store=graph_store
            ),
            effective_graph_query_table(
                self._domain, self._settings, include_inferred=False, store=graph_store
            ),
        )
        # Neo4j does not have SQL views. Its Explorer overlay is performed by
        # GraphFilter from the sandbox marker graph.
        if backend == "neo4j":
            return sandbox
        if hasattr(simulation_sandbox, "_view_name"):
            return simulation_sandbox._view_name(include_inferred)
        return sandbox

    def create(self, body: Dict[str, Any], owner: str = "") -> Dict[str, Any]:
        self.require_supported()
        spec = self.parse_spec(body)
        self.registry.save_simulation(
            self.folder,
            self.version,
            {"id": spec.id, "label": spec.label, "status": "draft",
             "spec": spec.to_dict(), "owner": owner},
        )
        return self.get(spec.id)

    def update(self, sim_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get(sim_id)
        if current.get("status") == "running" and self._run_task_is_active(
            current.get("task_id")
        ):
            raise ValidationError("Simulation is running — wait for it to finish")
        spec = self.parse_spec(body, sim_id=sim_id)
        self.registry.save_simulation(
            self.folder,
            self.version,
            {"id": sim_id, "label": spec.label, "status": "draft",
             "spec": spec.to_dict(), "owner": current.get("owner", "")},
        )
        return self.get(sim_id)

    def delete(self, sim_id: str) -> bool:
        self.get(sim_id)
        self._drop_sandbox(sim_id)
        return self.registry.delete_simulation(self.folder, self.version, sim_id)

    def purge(self, sim_id: str) -> Dict[str, Any]:
        """Drop the sandbox after-picture; keep the scenario spec as a draft."""
        current = self.get(sim_id)
        if current.get("status") == "running" and self._run_task_is_active(
            current.get("task_id")
        ):
            raise ValidationError("Simulation is running — wait for it to finish")
        self._drop_sandbox(sim_id)
        self.registry.save_simulation(
            self.folder,
            self.version,
            {
                "id": sim_id,
                "label": current.get("label", ""),
                "status": "draft",
                "spec": current.get("spec") or {},
                "owner": current.get("owner", ""),
                "diff_summary": {},
                "diff": {},
                "task_id": "",
                "error": "",
            },
        )
        return self.get(sim_id)

    def _drop_sandbox(self, sim_id: str) -> None:
        """Best-effort removal of the sandbox table (registry row is the source of truth)."""
        if not self.is_backend_supported(self._domain):
            return
        try:
            from back.core.graphdb import get_graphdb
            from back.objects.simulation.SimulationSandbox import SimulationSandbox

            store = get_graphdb(self._domain, self._settings)
            if store is not None:
                SimulationSandbox(store, effective_graph_name(self._domain), sim_id).drop()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not drop sandbox for simulation %s: %s", sim_id, exc)

    # -- Runs ---------------------------------------------------------------

    def start_run(self, sim_id: str, owner: str = "") -> str:
        """Launch the simulation in a background task; return the task id."""
        from back.core.task_manager import get_task_manager
        from back.objects.digitaltwin import CohortService, DomainSnapshot

        self.require_supported()
        row = self.get(sim_id)
        if row.get("status") == "running" and self._run_task_is_active(row.get("task_id")):
            raise ValidationError("Simulation is already running")
        spec = self.parse_spec(row.get("spec") or {}, sim_id=sim_id)

        cohort_rule = None
        if spec.scope.type == "cohort":
            rules = CohortService(self._domain).list_rules()
            cohort_rule = next((r for r in rules if r.get("id") == spec.scope.focus), None)
            if not cohort_rule:
                raise NotFoundError(f"Cohort rule '{spec.scope.focus}' was not found")

        self._domain.ensure_generated_content()
        domain_snap = DomainSnapshot(self._domain)
        tm = get_task_manager()
        task = tm.create_task(
            name=f"Simulation: {spec.label}",
            task_type="simulation",
            steps=[{"name": "running", "description": "Running what-if simulation"}],
        )
        runner = SimulationRunner(
            tm=tm,
            task_id=task.id,
            settings=self._settings,
            domain_snap=domain_snap,
            registry=self.registry,
            folder=self.folder,
            version=self.version,
            spec=spec,
            owner=owner or row.get("owner", ""),
            cohort_rule=cohort_rule,
        )
        self.registry.save_simulation(
            self.folder,
            self.version,
            {
                "id": sim_id,
                "label": spec.label,
                "status": "running",
                "spec": spec.to_dict(),
                "owner": owner or row.get("owner", ""),
                "task_id": task.id,
            },
        )
        tm.merge_result(task.id, {"simulation_id": spec.id})
        threading.Thread(target=runner.run, daemon=True).start()
        return task.id

    def abort(self, sim_id: str) -> Dict[str, Any]:
        """Mark a run failed and clear its task so the scenario can be edited."""
        row = self.get(sim_id)
        tid = str(row.get("task_id") or "").strip()
        if tid:
            try:
                from back.core.task_manager import get_task_manager

                get_task_manager().cancel_task(tid)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Could not cancel simulation task %s: %s", tid, exc)
        self.registry.save_simulation(
            self.folder,
            self.version,
            {
                "id": sim_id,
                "label": row.get("label", ""),
                "status": "failed",
                "spec": row.get("spec") or {},
                "owner": row.get("owner", ""),
                "task_id": "",
                "error": "Run aborted",
            },
        )
        return self.get(sim_id)

    @staticmethod
    def _run_task_is_active(task_id: Any) -> bool:
        tid = str(task_id or "").strip()
        if not tid:
            return False
        try:
            from back.core.task_manager import get_task_manager

            task = get_task_manager().get_task(tid)
        except Exception:  # noqa: BLE001
            return False
        if task is None:
            return False
        status = getattr(task.status, "value", task.status)
        return str(status).lower() in ("pending", "running")

    def _reap_stale_run(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Clear ``running`` when the worker task is gone (restart / crash)."""
        if not row or row.get("status") != "running":
            return row
        if self._run_task_is_active(row.get("task_id")):
            return row
        sim_id = row.get("id")
        if not sim_id:
            return row
        self.registry.save_simulation(
            self.folder,
            self.version,
            {
                "id": sim_id,
                "label": row.get("label", ""),
                "status": "failed",
                "spec": row.get("spec") or {},
                "owner": row.get("owner", ""),
                "task_id": "",
                "error": "Run interrupted — the worker is no longer active",
            },
        )
        return self.registry.load_simulation(self.folder, self.version, sim_id) or row

    def runs(self, sim_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        return self.registry.load_simulation_runs(
            self.folder, self.version, sim_id=sim_id, limit=limit
        )

    def diff_page(
        self, sim_id: str, kind: str, page: int, page_size: int, search: str = ""
    ) -> Dict[str, Any]:
        row = self.get(sim_id)
        out = SimulationDiff.page(row.get("diff") or {}, kind, page, page_size, search)
        out["summary"] = row.get("diff_summary") or {}
        return out
