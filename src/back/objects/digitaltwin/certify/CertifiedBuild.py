"""Certified build: compose the Certified Graph in Delta, then load its backend.

The composition (``…_cert_base`` over the Original ``…_data``,
``…_cert_enriched``, ``…_certified``) is built for every backend. A Lakehouse
domain serves ``…_certified`` (over the base view when virtual, over a copy of
it when materialized); Lakebase and Neo4j domains get the base loaded through
the same backend load the Original build uses (:class:`GraphLoad`, or
:class:`SyncedGraphLoad` for Lakebase managed_synced).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.core.helpers.SQLHelpers import SQLHelpers
from back.core.logging import get_logger
from back.objects.digitaltwin.build import GraphLoad, SyncedGraphLoad

logger = get_logger(__name__)


class CertifiedBuild:
    """One run of the certified build for a domain version."""

    BUILD_KIND = "certified"
    LAKEHOUSE_VIRTUAL = "lakehouse_virtual"
    LAKEHOUSE_MATERIALIZED = "lakehouse_materialized"

    def __init__(self, tm: Any, task_id: str, domain: Any, settings: Any) -> None:
        self.tm = tm
        self.task_id = task_id
        self.domain = domain
        self.settings = settings
        self.backend = ""
        self.graph_name = ""
        self.triple_count = 0
        self.start_time = 0.0

    def run(self) -> None:
        """Run the build; never raises (errors flow through ``tm.fail_task``)."""
        self.start_time = time.time()
        self.tm.start_task(self.task_id, "Composing the certified graph...")
        try:
            target = self._target()
            builder = GraphDBFactory.get_builder(
                self.domain, self.settings, engine="delta"
            )
            builder.ensure_certified_composition()
            if target in (self.LAKEHOUSE_VIRTUAL, self.LAKEHOUSE_MATERIALIZED):
                if target == self.LAKEHOUSE_MATERIALIZED:
                    builder.materialize_certified_base()
                self.graph_name = builder.certified_view_fqn()
                self.triple_count = builder.count_triples(self.graph_name)
            elif not self._load_backend(builder):
                self._record("error")
                return
        except OntoBricksError as exc:
            self._fail(str(exc))
            return
        except Exception as exc:  # noqa: BLE001 — orchestrator final guard
            logger.exception("[CERT-BUILD %s] unexpected failure", self.task_id)
            self._fail(f"Certified build failed: {exc}")
            return
        message = f"Certified graph built ({self.triple_count} triples)"
        self.tm.complete_task(
            self.task_id,
            result={
                "triple_count": self.triple_count,
                "graph_name": self.graph_name,
                "graph_engine": self.backend,
                "duration_seconds": time.time() - self.start_time,
            },
            message=message,
        )
        self._record("success", message=message)

    def _target(self) -> str:
        """Return the serving target, or raise when the build cannot run."""
        self.backend = GraphDBFactory._resolve_graph_backend(self.domain)
        if self.backend == "none":
            raise ValidationError(
                "This domain has no graph backend: there is no certified graph to build"
            )
        if self.backend != "databricks":
            return self.backend
        mode = GraphDBFactory.resolve_lakehouse_materialization(
            self.domain, self.settings
        )
        if mode == "view":
            return self.LAKEHOUSE_VIRTUAL
        return self.LAKEHOUSE_MATERIALIZED

    def _load_backend(self, builder: Any) -> bool:
        """Load the certified base into the domain's graph engine (Lakebase, Neo4j)."""
        store = GraphDBFactory.get_graphdb(self.domain, self.settings)
        if store is None:
            raise InfrastructureError("Could not initialize the graph backend")
        self.graph_name = SQLHelpers.effective_graph_name(self.domain)
        load_cls = GraphLoad
        source_relation = builder.cert_base_fqn()
        if getattr(store, "is_synced", False) is True:
            load_cls = SyncedGraphLoad
            source_relation = self._lakeflow_source(builder, source_relation)
        load = load_cls(
            tm=self.tm,
            task_id=self.task_id,
            is_api=False,
            domain=self.domain,
            settings=self.settings,
            store=store,
            source_client=self._source_client(),
            graph_name=self.graph_name,
            source_relation=source_relation,
            delta_cfg=getattr(self.domain, "delta", None) or {},
            cancel_check=self._is_cancelled,
        )
        outcome = load.run()
        self.triple_count = load.triple_count
        if outcome == GraphLoad.EMPTY:
            self._fail_task(
                "The Original Graph has no triple: build the Original Graph first"
            )
            return False
        return outcome == GraphLoad.LOADED

    def _lakeflow_source(self, builder: Any, base_fqn: str) -> str:
        """Expose the base with ``object_hash``, the Lakeflow synced-table key."""
        lakebase = GraphDBFactory.get_builder(
            self.domain, self.settings, engine="lakebase"
        )
        source_fqn = builder.cert_sync_source_fqn()
        builder.create_view(
            source_fqn,
            lakebase.wrap_view_sql_for_lakeflow(
                f"SELECT subject, predicate, object FROM {base_fqn}"
            ),
        )
        return source_fqn

    def _source_client(self) -> Any:
        """Build-warehouse client the load reads the composition from."""
        from back.core.databricks import DatabricksClient
        from back.core.helpers import get_build_sql_credentials, resolve_build_use_sea

        host, token, warehouse_id = get_build_sql_credentials(
            self.domain, self.settings
        )
        return DatabricksClient(
            host=host,
            token=token,
            warehouse_id=warehouse_id,
            use_sea=resolve_build_use_sea(self.domain, self.settings),
        )

    def _is_cancelled(self) -> bool:
        try:
            return bool(self.tm.is_cancelled(self.task_id))
        except Exception:  # noqa: BLE001
            return False

    def _fail(self, message: str) -> None:
        self._fail_task(message)
        self._record("error", error=message)

    def _fail_task(self, message: str) -> None:
        logger.error("[CERT-BUILD %s] %s", self.task_id, message)
        self.tm.fail_task(self.task_id, message)

    def _record(self, status: str, *, message: str = "", error: str = "") -> None:
        """Persist this run to the registry ``build_runs`` trace (best-effort)."""
        try:
            from back.objects.registry.RegistryService import RegistryService
            from back.objects.session import sanitize_domain_folder

            folder = getattr(self.domain, "uc_domain_folder", "") or (
                sanitize_domain_folder((self.domain.info or {}).get("name", ""))
            )
            started = datetime.fromtimestamp(self.start_time, tz=timezone.utc)
            entry = {
                "version": str(getattr(self.domain, "current_version", "") or ""),
                "build_kind": self.BUILD_KIND,
                "status": status,
                "message": message,
                "error": error,
                "started_at": started.isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "duration_s": time.time() - self.start_time,
                "triple_count": int(self.triple_count or 0),
                "graph_engine": self.backend,
                "graph_name": self.graph_name,
                "task_id": self.task_id,
            }
            RegistryService.from_context(self.domain, self.settings).record_build_run(
                folder, entry
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[CERT-BUILD %s] could not record build-run trace (non-fatal): %s",
                self.task_id,
                exc,
            )
