"""Internal helper that drives a single Digital Twin build.

Extracted from :class:`back.objects.digitaltwin.DigitalTwin.run_build_task`
(formerly an 839-line method) to make each phase — prepare → view →
apply → cache → archive — a named, focused method that shares state via
``self`` instead of a closure.

This module is **private** to the ``digitaltwin`` package; it is not
re-exported from ``__init__.py`` and external callers must keep using
``DigitalTwin.run_build_task`` (which is now a thin delegator).

Incremental diff / Delta-snapshot management was removed in v0.4.1.
All builds are full rebuilds; when the graph engine is ``lakebase``
in ``managed_synced`` mode the data-plane Lakeflow pipeline handles
the refresh automatically.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from back.core.errors import OntoBricksError, OperationCancelledError


def _raise_if_cancelled(cancel_check) -> None:
    """Raise OperationCancelledError if the task has been cancelled."""
    try:
        if cancel_check():
            raise OperationCancelledError("Build cancelled by user")
    except OperationCancelledError:
        raise
    except Exception:  # noqa: BLE001
        pass
from back.core.logging import get_logger
from back.objects.digitaltwin.models import DomainSnapshot

logger = get_logger(__name__)


class _BuildPipeline:
    """One run of the Digital Twin build/sync pipeline.

    Constructed once per build with the same arguments as the legacy
    :meth:`DigitalTwin.run_build_task`. Call :meth:`run` to execute the
    pipeline; it never raises (errors flow through ``tm.fail_task``).
    """

    def __init__(
        self,
        tm,
        task_id: str,
        domain,
        settings,
        domain_snap: DomainSnapshot,
        host: str,
        token: str,
        warehouse_id: str,
        view_table: str,
        graph_name: str,
        r2rml_content: str,
        base_uri: str,
        mapping_config,
        ontology_config,
        delta_cfg: dict,
        *,
        build_kind: str = "session",
    ) -> None:
        self.tm = tm
        self.task_id = task_id
        self.domain = domain
        self.settings = settings
        self.domain_snap = domain_snap
        self.host = host
        self.token = token
        self.warehouse_id = warehouse_id
        self.view_table = view_table
        self.graph_name = graph_name
        self.r2rml_content = r2rml_content
        self.base_uri = base_uri
        self.mapping_config = mapping_config
        self.ontology_config = ontology_config
        self.delta_cfg = delta_cfg
        self.build_kind = build_kind

        self.is_api = build_kind == "api"
        self.start_time = time.time()
        self.phase_times: Dict[str, float] = {}
        self.parts = view_table.split(".")

        self.domain_name = (domain.info or {}).get("name", "<unknown>")

        # Lazy-initialised across phases.
        self.source_client = None
        self.store = None
        self.entity_mappings: list = []
        self.relationship_mappings: list = []
        self.spark_sql: str = ""
        self.triple_count: int = 0
        # Lakebase managed-synced mode flag, resolved once before _open_store.
        self._lakebase_engine_config: Dict[str, Any] = {}
        self._is_lakebase_synced: bool = False

    # ------------------------------------------------------------------
    # Phase utilities
    # ------------------------------------------------------------------

    def _log_phase(self, name: str, t0_phase: float) -> None:
        elapsed = time.time() - t0_phase
        self.phase_times[name] = elapsed
        logger.info(
            "[DT-BUILD %s] phase [%s]: %.2fs", self.task_id, name, elapsed
        )

    def _is_cancelled(self) -> bool:
        """Cancel-check hook passed to long-running workers.

        Returns ``True`` once the user has flipped this build's task to
        ``cancelled``. Wired into :class:`SyncedTableManager` so the
        Lakeflow wait loops bail out promptly instead of observing a
        stale FAILED state from a pipeline the user already abandoned.
        """
        try:
            return self.tm.is_cancelled(self.task_id)
        except Exception:  # noqa: BLE001
            return False

    def _resolve_lakebase_mode(self) -> None:
        """Resolve graph engine + engine_config once, before ``_open_store``."""
        from back.core.triplestore.TripleStoreFactory import TripleStoreFactory

        logger.debug("[DT-BUILD %s] resolving graph engine mode…", self.task_id)
        try:
            engine = TripleStoreFactory._resolve_graph_engine(
                self.domain, self.settings
            )
            cfg = TripleStoreFactory._resolve_graph_engine_config(
                self.domain, self.settings
            ) or {}
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DT-BUILD %s] could not resolve lakebase mode — defaulting to "
                "lakebase/app_managed: %s",
                self.task_id,
                exc,
            )
            engine = "lakebase"
            cfg = {}
        self._lakebase_engine_config = cfg
        self._is_lakebase_synced = (
            engine == "lakebase" and cfg.get("sync_mode") == "managed_synced"
        )
        cfg_summary = {k: v for k, v in cfg.items() if k != "schema"}
        logger.info(
            "[DT-BUILD %s] graph engine resolved: engine=%s sync_mode=%s config=%s",
            self.task_id,
            engine,
            cfg.get("sync_mode", "app_managed"),
            cfg_summary or "{}",
        )
        if self._is_lakebase_synced:
            logger.info(
                "[DT-BUILD %s] managed_synced active — bulk data movement runs "
                "on the data plane via Lakeflow (sync_table_mode=%s timeout=%ss)",
                self.task_id,
                cfg.get("sync_table_mode", "snapshot"),
                cfg.get("sync_timeout_s", 600),
            )

    def _lakebase_managed_synced(self) -> bool:
        """Return ``True`` when bulk data movement should be delegated to Lakeflow."""
        return self._is_lakebase_synced

    def _count_view_triples(self) -> int:
        """Return the number of triples in the VIEW (server-side COUNT)."""
        logger.debug(
            "[DT-BUILD %s] counting triples in VIEW %s", self.task_id, self.view_table
        )
        try:
            rows = self.source_client.execute_query(
                f"SELECT COUNT(*) AS cnt FROM {self.view_table}"
            )
            count = int(rows[0].get("cnt", 0)) if rows else 0
            logger.debug(
                "[DT-BUILD %s] VIEW %s contains %d triple(s)",
                self.task_id,
                self.view_table,
                count,
            )
            return count
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DT-BUILD %s] could not count triples in VIEW %s: %s",
                self.task_id,
                self.view_table,
                exc,
            )
            return 0

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Drive the build through every phase, reporting progress to ``tm``."""
        self._log_start()
        try:
            t_phase = time.time()
            if not self._prepare_translation():
                return
            self._log_phase("prepare", t_phase)
            self.tm.update_progress(self.task_id, 10, "SQL generated")

            self._resolve_lakebase_mode()

            t_phase = time.time()
            if not self._create_view():
                return
            self._log_phase("create_view", t_phase)
            self._post_create_view_progress()

            t_phase = time.time()
            self._announce_apply_step()

            if not self._open_store():
                return

            if not self._apply_full_rebuild():
                return

            self._log_phase("apply_graph", t_phase)

            if not self.is_api:
                self._populate_session_cache()

            self._complete_task()

        except OperationCancelledError as exc:
            # Cooperative cancel from a wait loop bubbled up past the
            # phase-level handler — task is already CANCELLED, just log.
            logger.info(
                "[DT-BUILD %s] aborted by cancel: %s", self.task_id, exc
            )
        except Exception as exc:  # noqa: BLE001 — orchestrator final guard
            self._fail_unexpected(exc)

    # ------------------------------------------------------------------
    # Phases
    # ------------------------------------------------------------------

    def _log_start(self) -> None:
        logger.info(
            "[DT-BUILD %s] START kind=%s domain=%s view=%s graph=%s "
            "warehouse=%s",
            self.task_id,
            self.build_kind,
            self.domain_name,
            self.view_table,
            self.graph_name,
            self.warehouse_id,
        )

    def _prepare_translation(self) -> bool:
        """Parse R2RML, augment mappings, build the Spark SQL union query."""
        from back.core.databricks import DatabricksClient
        from back.core.w3c import sparql

        from back.objects.digitaltwin.DigitalTwin import DigitalTwin

        self.tm.start_task(self.task_id, "Preparing mappings...")
        self.source_client = DatabricksClient(
            host=self.host, token=self.token, warehouse_id=self.warehouse_id
        )

        entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(
            self.r2rml_content
        )
        logger.info(
            "[DT-BUILD %s] R2RML parsed: %d entity mapping(s), "
            "%d relationship mapping(s)",
            self.task_id,
            len(entity_mappings or []),
            len(relationship_mappings or []),
        )
        entity_mappings = DigitalTwin.augment_mappings_from_config(
            entity_mappings, self.mapping_config, self.base_uri, self.ontology_config
        )
        relationship_mappings = DigitalTwin.augment_relationships_from_config(
            relationship_mappings,
            self.mapping_config,
            self.base_uri,
            self.ontology_config,
        )
        logger.info(
            "[DT-BUILD %s] mappings augmented from config: %d entity, "
            "%d relationship (base_uri=%s)",
            self.task_id,
            len(entity_mappings or []),
            len(relationship_mappings or []),
            self.base_uri,
        )
        self.entity_mappings = entity_mappings
        self.relationship_mappings = relationship_mappings

        if not entity_mappings and not relationship_mappings:
            logger.warning(
                "[DT-BUILD %s] aborting: no valid mappings found "
                "(entities=%s, relationships=%s)",
                self.task_id,
                bool(entity_mappings),
                bool(relationship_mappings),
            )
            self.tm.fail_task(self.task_id, "No valid mappings found")
            return False

        all_data_sparql = (
            f"PREFIX : <{self.base_uri}>\n"
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\n"
            "SELECT DISTINCT ?subject ?predicate ?object\n"
            "WHERE {\n"
            "    ?subject ?predicate ?object .\n"
            "}"
        )

        try:
            result = sparql.translate_sparql_to_spark(
                all_data_sparql,
                entity_mappings,
                None,
                relationship_mappings,
                dialect="spark",
            )
        except OntoBricksError as exc:
            logger.error(
                "[DT-BUILD %s] SPARQL→Spark translation failed: %s",
                self.task_id,
                exc.message,
            )
            self.tm.fail_task(self.task_id, exc.message)
            return False

        if self.is_api and not result.get("success"):
            logger.error(
                "[DT-BUILD %s] SPARQL→Spark translation returned failure: %s",
                self.task_id,
                result.get("message", "Translation failed"),
            )
            self.tm.fail_task(
                self.task_id, result.get("message", "Translation failed")
            )
            return False

        self.spark_sql = result["sql"]
        logger.info(
            "[DT-BUILD %s] SPARQL→Spark translation OK (sql_chars=%d)",
            self.task_id,
            len(self.spark_sql or ""),
        )
        return True

    def _create_view(self) -> bool:
        """Create or replace the Spark VIEW. Returns ``False`` on failure."""
        from back.objects.digitaltwin.DigitalTwin import DigitalTwin

        self.tm.advance_step(self.task_id, f"Creating VIEW {self.view_table}...")
        logger.info(
            "[DT-BUILD %s] creating VIEW %s on warehouse %s",
            self.task_id,
            self.view_table,
            self.warehouse_id,
        )
        try:
            catalog, schema, vname = self.parts
            view_ok, view_msg = self.source_client.create_or_replace_view(
                catalog, schema, vname, self.spark_sql
            )
            if not view_ok:
                if self.is_api:
                    logger.error(
                        "[DT-BUILD %s] failed to create VIEW %s: %s",
                        self.task_id,
                        self.view_table,
                        view_msg,
                    )
                    self.tm.fail_task(
                        self.task_id, f"Failed to create VIEW: {view_msg}"
                    )
                else:
                    detail = DigitalTwin.diagnose_view_error(
                        view_msg, self.entity_mappings, self.relationship_mappings
                    )
                    logger.error(
                        "[DT-BUILD %s] failed to create VIEW %s:\n%s",
                        self.task_id,
                        self.view_table,
                        detail,
                    )
                    self.tm.fail_task(
                        self.task_id, f"Failed to create VIEW: {detail}"
                    )
                return False
            logger.info(
                "[DT-BUILD %s] VIEW %s created", self.task_id, self.view_table
            )
            return True
        except Exception as exc:  # noqa: BLE001
            if self.is_api:
                logger.exception(
                    "[DT-BUILD %s] VIEW creation raised: %s", self.task_id, exc
                )
                self.tm.fail_task(self.task_id, str(exc))
                return False
            detail = DigitalTwin.diagnose_view_error(
                str(exc), self.entity_mappings, self.relationship_mappings
            )
            logger.exception(
                "[DT-BUILD %s] failed to create VIEW %s:\n%s",
                self.task_id,
                self.view_table,
                detail,
            )
            self.tm.fail_task(self.task_id, f"Failed to create VIEW: {detail}")
            return False

    def _post_create_view_progress(self) -> None:
        if self.is_api:
            self.tm.update_progress(self.task_id, 25, "VIEW created")
        else:
            self.tm.update_progress(
                self.task_id, 25, f"VIEW {self.view_table} created"
            )

    def _announce_apply_step(self) -> None:
        apply_msg = (
            "Applying changes to graph..."
            if self.is_api
            else "Applying changes to the knowledge graph..."
        )
        self.tm.advance_step(self.task_id, apply_msg)

    def _open_store(self) -> bool:
        """Initialise the graph backend. Returns ``False`` on failure."""
        from back.core.triplestore import get_triplestore as _get_ts

        logger.debug(
            "[DT-BUILD %s] opening graph backend store (domain=%s)",
            self.task_id,
            self.domain_name,
        )
        self.store = _get_ts(self.domain_snap, self.settings, backend="graph")
        if not self.store:
            logger.error(
                "[DT-BUILD %s] could not initialize graph backend "
                "(domain=%s) — check graph_engine_config in Settings",
                self.task_id,
                self.domain_name,
            )
            self.tm.fail_task(self.task_id, "Could not initialize graph backend")
            return False
        schema = getattr(self.store, "graph_schema", "?")
        sync_mode = getattr(self.store, "sync_mode", "?")
        store_cls = type(self.store).__name__
        logger.info(
            "[DT-BUILD %s] graph backend opened: class=%s schema=%s sync_mode=%s",
            self.task_id,
            store_cls,
            schema,
            sync_mode,
        )
        return True

    def _apply_full_rebuild(self) -> bool:
        """Drop, recreate, and bulk-insert all triples (or trigger Lakeflow sync).

        In ``managed_synced`` mode the entire branch is replaced by
        :meth:`_apply_via_synced_pipeline` — the Lakeflow snapshot pipeline
        rewrites the synced PG table and the app does not iterate triples.
        """
        if self._lakebase_managed_synced():
            return self._apply_via_synced_pipeline()

        t_fetch = time.time()
        logger.info(
            "[DT-BUILD %s] full rebuild: reading all triples from VIEW %s",
            self.task_id,
            self.view_table,
        )
        if not self.is_api:
            self.tm.update_progress(self.task_id, 40, "Reading all triples from VIEW...")

        triple_count = self._count_view_triples()
        self._log_phase("fetch_triples", t_fetch)

        self.triple_count = triple_count
        logger.info(
            "[DT-BUILD %s] VIEW reports %d triples to ingest",
            self.task_id,
            triple_count,
        )
        if triple_count == 0:
            logger.warning(
                "[DT-BUILD %s] VIEW %s returned 0 triples — "
                "possible causes: (1) R2RML mappings do not match source table "
                "columns, (2) source tables are empty, (3) SQL query filters "
                "out all rows. The VIEW was created successfully but the graph "
                "will be empty until mappings are corrected.",
                self.task_id,
                self.view_table,
            )
            empty_msg = (
                "VIEW created but no triples generated (check your mappings)"
                if not self.is_api
                else "VIEW created but no triples generated"
            )
            self.tm.complete_task(
                self.task_id,
                result={
                    "triple_count": 0,
                    "view_table": self.view_table,
                    "graph_name": self.graph_name,
                    "build_mode": "full",
                    "duration_seconds": time.time() - self.start_time,
                },
                message=empty_msg,
            )
            return False

        t_insert = time.time()
        if not self.is_api:
            self.tm.update_progress(
                self.task_id, 50, f"Full rebuild: writing {triple_count} triples..."
            )
        logger.info(
            "[DT-BUILD %s] dropping & recreating graph table %s",
            self.task_id,
            self.graph_name,
        )
        self.store.drop_table(self.graph_name)
        self.store.create_table(self.graph_name)

        is_api_local = self.is_api
        tm_local = self.tm
        task_id_local = self.task_id
        total_local = triple_count

        def _on_progress_full(written: int, total: int) -> None:
            denom = total_local or total or 1
            progress = 50 + int(written / denom * 40)
            if is_api_local:
                tm_local.update_progress(
                    task_id_local,
                    progress,
                    f"Written {written}/{denom} triples...",
                )
            else:
                tm_local.update_progress(
                    task_id_local,
                    min(progress, 90),
                    f"Written {written}/{denom} triples...",
                )

        logger.info(
            "[DT-BUILD %s] streaming %d triples into %s (batch_size=5000)",
            self.task_id,
            triple_count,
            self.graph_name,
        )
        select_sql = (
            f"SELECT subject, predicate, object FROM {self.view_table}"
        )
        if hasattr(self.store, "bulk_load_into_sync"):
            # Lakebase app_managed: warehouse data goes into *_sync; app writes
            # (reasoning / materialise) target the companion (*__app) via
            # _writable_table_id.  Non-Lakebase backends fall through to the
            # legacy single-table path below.
            triple_iter = self.source_client.iter_rows(
                select_sql, batch_size=5000
            )
            written = self.store.bulk_load_into_sync(
                self.graph_name,
                triple_iter,
                batch_size=5000,
                on_progress=_on_progress_full,
            )
            logger.info(
                "[DT-BUILD %s] bulk_load_into_sync wrote %d rows into %s_sync",
                self.task_id,
                written,
                self.graph_name,
            )
        else:
            self._stream_triples_into_store(
                select_sql,
                insert_batch_size=5000,
                on_progress=_on_progress_full,
            )
        logger.info(
            "[DT-BUILD %s] optimizing graph table %s",
            self.task_id,
            self.graph_name,
        )
        self.store.optimize_table(self.graph_name)
        self._log_phase("graph_insert", t_insert)
        return True

    def _stream_triples_into_store(
        self,
        select_sql: str,
        *,
        insert_batch_size: int = 5000,
        on_progress: Optional[Any] = None,
    ) -> int:
        """Stream warehouse rows into the graph store via the bulk insert iterator."""
        use_iter = hasattr(self.store, "bulk_insert_iter")
        logger.debug(
            "[DT-BUILD %s] streaming triples into %s "
            "(batch_size=%d method=%s)",
            self.task_id,
            self.graph_name,
            insert_batch_size,
            "bulk_insert_iter" if use_iter else "insert_triples",
        )
        triple_iter = self.source_client.iter_rows(
            select_sql, batch_size=insert_batch_size
        )
        if use_iter:
            written = self.store.bulk_insert_iter(
                self.graph_name,
                triple_iter,
                batch_size=insert_batch_size,
                on_progress=on_progress,
            )
            logger.info(
                "[DT-BUILD %s] bulk_insert_iter wrote %d rows into %s",
                self.task_id,
                written,
                self.graph_name,
            )
            return written
        triples = list(triple_iter)
        logger.debug(
            "[DT-BUILD %s] fetched %d triples from warehouse, inserting…",
            self.task_id,
            len(triples),
        )
        written = self.store.insert_triples(
            self.graph_name,
            triples,
            batch_size=min(insert_batch_size, 500),
            on_progress=on_progress,
        )
        logger.info(
            "[DT-BUILD %s] insert_triples wrote %d rows into %s",
            self.task_id,
            written,
            self.graph_name,
        )
        return written

    def _apply_via_synced_pipeline(self) -> bool:
        """Lakebase managed-synced apply path -- triples never enter the app.

        Steps:

        1. Build the synced UC FQN from ``engine_config.sync_uc_catalog``.
        2. ``CREATE SCHEMA IF NOT EXISTS`` in Unity Catalog.
        3. ``SyncedTableManager.ensure`` — registers the synced table.
        4. ``ensure_synced_companion`` — Postgres companion table.
        5. ``trigger_and_wait`` — wait until sync reaches an online state.
        6. ``ensure_synced_union_view`` — union view over ``_sync`` ∪ companion.
        7. ``TRUNCATE`` the companion for a clean reasoning slate.
        """
        from back.core.errors import InfrastructureError
        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_sync_uc_fallback_catalog,
        )

        t0 = time.time()
        try:
            mgr = self.store.synced_manager()
            fallback_cat = resolve_sync_uc_fallback_catalog(
                self.domain, self.settings, self.delta_cfg
            )
            synced_uc = self.store.synced_uc_name(
                self.graph_name, fallback_catalog=fallback_cat
            )
            logger.info(
                "[DT-BUILD %s] Managed-sync registers UC synced table at %s "
                "(graph_engine_config.sync_uc_catalog=%r; fallback_catalog=%r; "
                "UC schema segment=%s)",
                self.task_id,
                synced_uc,
                (self.store.sync_uc_catalog or "").strip() or None,
                fallback_cat or None,
                self.store.graph_schema,
            )
        except InfrastructureError as exc:
            logger.error(
                "[DT-BUILD %s] managed_synced setup failed: %s",
                self.task_id,
                exc,
            )
            self.tm.fail_task(self.task_id, str(exc))
            return False

        def _upd(pct: int, msg: str) -> None:
            if not self.is_api:
                self.tm.update_progress(self.task_id, pct, msg)

        def _adv() -> None:
            """Advance to the next named task step (no-op in API mode)."""
            if not self.is_api:
                self.tm.advance_step(self.task_id)

        # Step 2 is already active (set by _announce_apply_step → advance_step).
        _upd(45, f"Ensuring UC schema for {synced_uc}…")
        try:
            from back.core.graphdb.lakebase._sync_uc_schema import (
                ensure_uc_schema_for_synced_table_fqn,
            )

            # Step 2 — ensure the UC schema exists (warehouse DDL).
            t_step = time.time()
            logger.debug(
                "[DT-BUILD %s] step 2/7: ensuring UC schema for %s",
                self.task_id,
                synced_uc,
            )
            ensure_uc_schema_for_synced_table_fqn(
                self.source_client,
                synced_uc,
                task_log_prefix=f"[DT-BUILD {self.task_id}]",
            )
            logger.info(
                "[DT-BUILD %s] step 2/7 done: UC schema verified in %.2fs",
                self.task_id,
                time.time() - t_step,
            )

            _raise_if_cancelled(self._is_cancelled)
            _adv()  # → "Registering synced table in Unity Catalog"

            # Step 3 — register/reuse the Lakebase synced table.
            t_step = time.time()
            logger.debug(
                "[DT-BUILD %s] step 3/7: registering synced table %s "
                "(source=%s sync_mode=%s)",
                self.task_id,
                synced_uc,
                self.view_table,
                self.store.sync_table_mode,
            )
            _synced_obj = mgr.ensure(
                synced_uc,
                source_table_full_name=self.view_table,
                primary_key_columns=["subject", "predicate", "object"],
                sync_mode=self.store.sync_table_mode,
            )
            # ensure() may have used a fallback name (ghost control-plane state);
            # extract the actual UC FQN from the returned object so downstream
            # steps (trigger_and_wait, ensure_synced_union_view) use the right name.
            actual_synced_uc = (
                getattr(_synced_obj, "name", None) or synced_uc
            )
            if actual_synced_uc != synced_uc:
                logger.warning(
                    "[DT-BUILD %s] synced table registered under fallback UC name %s "
                    "(requested %s); downstream steps will use the fallback.",
                    self.task_id,
                    actual_synced_uc,
                    synced_uc,
                )
            logger.info(
                "[DT-BUILD %s] step 3/7 done: synced table registered in %.2fs",
                self.task_id,
                time.time() - t_step,
            )

            _raise_if_cancelled(self._is_cancelled)
            _adv()  # → "Creating companion table"

            # Step 4 — create companion table in Postgres.
            t_step = time.time()
            logger.debug(
                "[DT-BUILD %s] step 4/7: creating companion table for %s "
                "(pg schema=%s)",
                self.task_id,
                self.graph_name,
                getattr(self.store, "graph_schema", "?"),
            )
            self.store.ensure_synced_companion(self.graph_name)
            logger.info(
                "[DT-BUILD %s] step 4/7 done: companion table ready in %.2fs",
                self.task_id,
                time.time() - t_step,
            )
        except OperationCancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "[DT-BUILD %s] failed to register synced table %s: %s "
                "(source_view=%s pg_schema=%s sync_table_mode=%s)",
                self.task_id,
                synced_uc,
                exc,
                self.view_table,
                getattr(self.store, "graph_schema", "?"),
                getattr(self.store, "sync_table_mode", "?"),
            )
            self.tm.fail_task(
                self.task_id,
                f"Could not register Lakebase synced table: {exc}",
            )
            return False

        _adv()  # → "Syncing data from Delta (Lakeflow)"

        # Step 5 — trigger Lakeflow snapshot and wait for ONLINE.
        logger.debug(
            "[DT-BUILD %s] step 5/7: triggering Lakeflow sync for %s "
            "(timeout=%ss)",
            self.task_id,
            synced_uc,
            self.store.sync_timeout_s,
        )
        _human = {
            "PROVISIONING": "provisioning pipeline…",
            "PROVISIONING_PIPELINE_RESOURCES": "provisioning pipeline resources…",
            "PROVISIONING_INITIAL_SNAPSHOT": "initial snapshot in progress…",
            "ONLINE_TRIGGERED_UPDATE": "snapshot update running…",
            "ONLINE_CONTINUOUS_UPDATE": "continuous update running…",
            "ONLINE_PIPELINE_FAILED": "pipeline error — retrying…",
            "OFFLINE": "pipeline offline — waiting…",
        }

        def _on_sync_state(pipeline_state: str) -> None:
            label = _human.get(pipeline_state, pipeline_state.lower().replace("_", " "))
            logger.info(
                "[DT-BUILD %s] Lakeflow pipeline state → %s",
                self.task_id,
                pipeline_state,
            )
            if not self.is_api:
                self.tm.update_progress(
                    self.task_id,
                    58,
                    f"Lakebase sync — {label}",
                )

        t_sync = time.time()
        try:
            state = mgr.trigger_and_wait(
                actual_synced_uc,
                timeout_s=self.store.sync_timeout_s,
                cancel_check=self._is_cancelled,
                on_state_change=_on_sync_state,
            )
            logger.info(
                "[DT-BUILD %s] step 5/7 done: Lakeflow pipeline reached %s "
                "in %.1fs (total elapsed from build start=%.1fs)",
                self.task_id,
                state,
                time.time() - t_sync,
                time.time() - t0,
            )
        except OperationCancelledError as exc:
            # User cancelled the task while we were polling Lakeflow. The
            # task is already in CANCELLED — do NOT flip it to FAILED.
            logger.info(
                "[DT-BUILD %s] step 5/7 aborted by user cancel for %s (%s)",
                self.task_id,
                synced_uc,
                exc,
            )
            return False
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "[DT-BUILD %s] step 5/7 FAILED: Lakeflow sync for %s "
                "did not complete after %.1fs — state may be stuck in "
                "provisioning; check the Lakebase synced-table pipeline "
                "in the Databricks UI. Error: %s",
                self.task_id,
                synced_uc,
                time.time() - t_sync,
                exc,
            )
            self.tm.fail_task(
                self.task_id, f"Lakebase sync did not complete: {exc}"
            )
            return False

        _adv()  # → "Creating knowledge graph union view"

        # Step 6 — create/refresh the union view.
        t_step = time.time()
        logger.debug(
            "[DT-BUILD %s] step 6/7: creating union view for %s",
            self.task_id,
            self.graph_name,
        )
        # If ensure() used a fallback name, derive the actual Postgres table name
        # from the last component of actual_synced_uc and pass it as an override.
        _actual_synced_phy: Optional[str] = None
        if actual_synced_uc != synced_uc:
            _actual_synced_phy = actual_synced_uc.split(".")[-1]

        try:
            self.store.ensure_synced_union_view(
                self.graph_name,
                synced_phy_override=_actual_synced_phy,
            )
            logger.info(
                "[DT-BUILD %s] step 6/7 done: union view created/refreshed "
                "in %.2fs",
                self.task_id,
                time.time() - t_step,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "[DT-BUILD %s] step 6/7 FAILED: could not create union view "
                "for graph=%s synced_uc=%s actual_synced_uc=%s pg_schema=%s — "
                "the _sync table may not yet be visible in Postgres: %s",
                self.task_id,
                self.graph_name,
                synced_uc,
                actual_synced_uc,
                getattr(self.store, "graph_schema", "?"),
                exc,
            )
            self.tm.fail_task(
                self.task_id,
                f"Could not create Lakebase union view after sync: {exc}",
            )
            return False

        _adv()  # → "Finalizing knowledge graph"

        # Step 7 — truncate companion for a clean reasoning slate.
        t_step = time.time()
        logger.debug(
            "[DT-BUILD %s] step 7/7: truncating companion table for %s",
            self.task_id,
            self.graph_name,
        )
        try:
            self.store.truncate_companion(self.graph_name)
            logger.info(
                "[DT-BUILD %s] step 7/7 done: companion truncated in %.2fs "
                "(full rebuild — reasoning starts clean)",
                self.task_id,
                time.time() - t_step,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DT-BUILD %s] step 7/7 WARNING: could not truncate companion "
                "for %s (non-fatal — reasoning data may be stale): %s",
                self.task_id,
                self.graph_name,
                exc,
            )

        self.triple_count = self._count_view_triples()
        if self.triple_count == 0:
            logger.warning(
                "[DT-BUILD %s] post-sync VIEW %s contains 0 triples — "
                "the Lakeflow pipeline may have synced an empty source or "
                "the VIEW SQL produces no rows; check mappings and source data",
                self.task_id,
                self.view_table,
            )

        if not self.is_api:
            self.tm.update_progress(
                self.task_id, 90, f"Synced {self.triple_count} triples"
            )
        logger.info(
            "[DT-BUILD %s] _apply_via_synced_pipeline complete: "
            "triples=%d total_elapsed=%.1fs",
            self.task_id,
            self.triple_count,
            time.time() - t0,
        )
        return True

    def _populate_session_cache(self) -> None:
        from back.objects.digitaltwin.DigitalTwin import DigitalTwin

        logger.debug(
            "[DT-BUILD %s] populating session cache (triples=%d)",
            self.task_id,
            self.triple_count,
        )
        try:
            final_count = self.triple_count
            build_stamp = self.domain.triplestore.get("build_last_update")

            status_cache = {
                "success": True,
                "has_data": final_count > 0,
                "count": final_count,
                "view_table": self.view_table,
                "graph_name": self.graph_name,
            }
            if build_stamp and final_count > 0:
                status_cache["last_modified"] = build_stamp

            try:
                graph_engine = DigitalTwin.resolve_graph_engine(
                    self.domain, self.settings
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[DT-BUILD %s] could not resolve graph engine for cache "
                    "— defaulting to 'lakebase': %s",
                    self.task_id,
                    exc,
                )
                graph_engine = "lakebase"

            graph_has_data = final_count > 0

            dt = DigitalTwin(self.domain)

            existence_cache = {
                "view_exists": True,
                "view_table": self.view_table,
                "graph_name": self.graph_name,
                "graph_engine": graph_engine,
                "graph_has_data": graph_has_data,
                "lakebase_table_exists": graph_has_data,
                "graph_display": "",
                "last_built": self.domain.last_build,
                "last_update": self.domain.last_update,
            }

            dt.set_ts_cache("status", status_cache)
            dt.set_ts_cache("dt_existence", existence_cache)
            logger.info(
                "[DT-BUILD %s] session cache populated: "
                "triples=%d engine=%s has_data=%s graph=%s",
                self.task_id,
                final_count,
                graph_engine,
                graph_has_data,
                self.graph_name,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DT-BUILD %s] could not populate DT session cache "
                "(non-fatal — DT status may be stale until next page load): %s",
                self.task_id,
                exc,
            )

    def _complete_task(self) -> None:
        duration = time.time() - self.start_time
        logger.info(
            "[DT-BUILD %s] DONE kind=%s domain=%s triples=%d "
            "duration=%.2fs phases={%s}",
            self.task_id,
            self.build_kind,
            self.domain_name,
            self.triple_count,
            duration,
            ", ".join(f"{k}={v:.2f}s" for k, v in self.phase_times.items())
            or "n/a",
        )

        result_data: Dict[str, Any] = {
            "triple_count": self.triple_count,
            "view_table": self.view_table,
            "graph_name": self.graph_name,
            "build_mode": "full",
            "duration_seconds": duration,
        }
        if not self.is_api:
            result_data["phase_times"] = self.phase_times

        msg = f"Full rebuild: {self.triple_count} triples in {duration:.1f}s"
        self.tm.complete_task(self.task_id, result=result_data, message=msg)

    def _fail_unexpected(self, exc: Exception) -> None:
        duration = time.time() - self.start_time
        logger.exception(
            "[DT-BUILD %s] FAILED kind=%s domain=%s after %.2fs: %s",
            self.task_id,
            self.build_kind,
            self.domain_name,
            duration,
            exc,
        )
        if self.is_api:
            self.tm.fail_task(self.task_id, str(exc))
        else:
            self.tm.fail_task(self.task_id, "Triple store sync failed")
