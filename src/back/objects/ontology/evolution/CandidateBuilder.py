"""Build the parent and candidate arms of one Evolve run into run-owned graphs.

Each arm is a session-free ``DomainSession`` made from a deep copy of the
parent version document (the candidate arm with the run's candidate
ontology and assignment swapped in). Its display name is rewritten to
``<name>__evo<run8><p|c>`` so every relation the Delta build derives from
the name (``triplestore_<safe>_V<n>``, ``…_data``, ``…_inferred``,
``…_graph``, search companions) and the process-wide GraphQL schema cache
key belong to the run, never to the parent domain.

Builds run on the Build warehouse in materialized mode with build history
switched off, so the parent version's ``build_runs`` and ``last_build`` are
never written. Nothing here calls ``DomainSession.save`` against a real
session or writes a registry version.
"""

from __future__ import annotations

import copy
import re
from typing import Any, Callable, Dict, List, Optional

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger
from back.objects.ontology.evolution.CandidateFactory import CandidateFactory

logger = get_logger(__name__)

ARMS = ("parent", "candidate")
_OWNED = re.compile(r"__evo[0-9a-f]{8}[pc]_v[0-9]+")


class CandidateBuilder:
    """Create, build and drop the two run-owned arm graphs."""

    def __init__(
        self,
        settings: Any,
        session_settings: Dict[str, Any],
        *,
        task_manager: Any = None,
        builder_factory: Optional[Callable[[Any], Any]] = None,
    ):
        self._settings = settings
        self._session_settings = copy.deepcopy(session_settings or {})
        self._tm = task_manager
        self._builder_factory = builder_factory

    # -- arm domains ----------------------------------------------------

    @staticmethod
    def arm_name(name: str, run_id: str, arm: str) -> str:
        if arm not in ARMS:
            raise ValidationError(f"Unknown arm '{arm}'")
        return f"{name}__evo{run_id[:8]}{arm[0]}"

    def arm_domain(
        self, document: Dict[str, Any], run: Dict[str, Any], arm: str, folder: str
    ):
        """A fresh, isolated domain for *arm*; the parent document is not mutated."""
        domain = CandidateFactory.session_for(document, run["parent_version"])
        if arm == "candidate":
            candidate = run.get("candidate") or {}
            if not candidate.get("ontology"):
                raise ValidationError("The run has no candidate to build")
            domain._data["ontology"] = copy.deepcopy(candidate["ontology"])
            domain._data["assignment"] = copy.deepcopy(candidate["assignment"])
        name = domain.info.get("name") or folder
        domain.info["name"] = self.arm_name(name, run["id"], arm)
        domain.info["lakehouse_materialization"] = "table"
        # Arms are read through the plain graph view; search companions would
        # be extra run-owned tables to build and drop for no benefit.
        domain.info["graph_cache_enabled"] = False
        domain._data["settings"] = copy.deepcopy(self._session_settings)
        domain.domain_folder = folder
        domain.clear_generated_content()
        domain.ensure_generated_content()
        return domain

    @staticmethod
    def require_delta_backend(domain: Any) -> None:
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        backend = GraphDBFactory._resolve_triple_store_backend(domain)
        if backend != "databricks":
            raise ValidationError(
                "Ontology Evolve evaluates on the Lakehouse (Delta) graph backend only",
                detail=backend,
            )

    # -- build ----------------------------------------------------------

    def _builder(self, domain: Any):
        if self._builder_factory is not None:
            return self._builder_factory(domain)
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        return GraphDBFactory.get_builder(domain, self._settings, engine="delta")

    def _task_manager(self):
        if self._tm is None:
            from back.core.task_manager import get_task_manager

            self._tm = get_task_manager()
        return self._tm

    def build(self, domain: Any, run_id: str, arm: str) -> Dict[str, Any]:
        """Materialize *domain*'s graph; returns relation names and triple count."""
        from back.core.helpers.DatabricksHelpers import DatabricksHelpers
        from back.objects.digitaltwin.models import DomainSnapshot
        from shared.config.constants import DEFAULT_BASE_URI

        self.require_delta_backend(domain)
        builder = self._builder(domain)
        data_table = builder.data_table_fqn()
        view_table = data_table[: -len("_data")] if data_table else ""
        if len(view_table.split(".")) != 3 or not self.is_owned(data_table):
            raise ValidationError(
                "Could not derive a run-owned graph location", detail=data_table
            )
        r2rml = domain.get_r2rml()
        if not r2rml:
            raise ValidationError("The arm has no R2RML mapping to build")
        host, token, warehouse_id = DatabricksHelpers.get_triplestore_sql_credentials(
            domain, self._settings
        )
        tm = self._task_manager()
        task = tm.create_task(
            name=f"Evolve {run_id}: {arm} graph",
            task_type="evolution_arm_build",
            steps=builder.build_steps("table"),
        )
        builder.run_triplestore_build(
            tm=tm,
            task_id=task.id,
            domain_snap=DomainSnapshot(domain),
            host=host,
            token=token,
            warehouse_id=warehouse_id,
            view_table=view_table,
            data_table=data_table,
            r2rml_content=r2rml,
            mapping_config=domain.assignment,
            ontology_config=domain.ontology,
            base_uri=domain.ontology.get("base_uri", DEFAULT_BASE_URI),
            build_kind="evolution",
            record_history=False,
        )
        done = tm.get_task(task.id)
        status = getattr(getattr(done, "status", None), "value", "")
        result = getattr(done, "result", None) or {}
        if status != "completed" or not int(result.get("triple_count") or 0):
            raise InfrastructureError(
                f"The {arm} graph build did not produce triples",
                detail=getattr(done, "error", "") or getattr(done, "message", ""),
            )
        return {
            "arm": arm,
            "task_id": task.id,
            "view_table": view_table,
            "data_table": data_table,
            "inferred_table": builder.inferred_table_fqn(),
            "graph_view": builder.graph_view_fqn(),
            "triple_count": int(result["triple_count"]),
        }

    # -- cleanup --------------------------------------------------------

    @staticmethod
    def is_owned(fqn: str) -> bool:
        """True only for relations named by an Evolve arm."""
        leaf = (fqn or "").rsplit(".", 1)[-1].lower()
        return leaf.startswith("triplestore_") and bool(_OWNED.search(leaf))

    @staticmethod
    def owned_relations(manifest_builds: List[Dict[str, Any]]) -> List[str]:
        names = []
        for build in manifest_builds or []:
            for key in ("graph_view", "view_table", "inferred_table", "data_table"):
                fqn = build.get(key) or ""
                if fqn and CandidateBuilder.is_owned(fqn) and fqn not in names:
                    names.append(fqn)
        return names

    def drop(self, domain: Any, manifest_builds: List[Dict[str, Any]]) -> List[str]:
        """Drop the run-owned relations recorded in the manifest; returns what was dropped."""
        builder = self._builder(domain)
        dropped = []
        for fqn in self.owned_relations(manifest_builds):
            kind = "table" if fqn.endswith(("_data", "_inferred")) else "view"
            try:
                builder.drop_relation(fqn, kind=kind)
                dropped.append(fqn)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolve cleanup could not drop %s: %s", fqn, exc)
        return dropped
