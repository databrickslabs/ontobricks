"""Incremental and microbatch builds of the ``…_data`` relation.

Extracted collaborator of
:class:`~back.objects.digitaltwin.build._build_pipeline._BuildPipeline`, in the
style of :class:`TwinBuildPostMaterialize`: static methods that take the
running pipeline (*pipe*) and read/write its state by attribute; the pipeline
keeps one-line hooks.

For a domain whose ``materialization_mode`` resolves to ``incremental`` or
``microbatch`` this replaces the ``…_data`` CTAS:

a. compile the row-local mappings into a slot pipeline spec, and the batch
   mappings into the translator's ``incremental=True`` SQL;
b. create/update the per-version Lakeflow slot pipeline (continuous for
   ``microbatch``) — skipped when no mapping is row-local;
c. run an update and wait for it (b and c: :class:`TwinSlotPipeline`). A full
   refresh (new pipeline or changed SQL) always completes as a *triggered*
   update first, so ``…_data`` is never built over an empty ``…_slots``; a
   ``microbatch`` pipeline then switches to continuous and is waited for until
   ``RUNNING``;
d. create ``…_data`` as a materialized view over ``…_slots`` and the batch
   branch (``…_graph`` stays the union VIEW) — or keep it when its definition
   fingerprint is unchanged — and refresh it.

Any :class:`OntoBricksError` in a–d records ``incremental_failed:<Error>`` on
the mode decision and hands the build back to the ``full`` path, which drops
whatever materialized view was left behind. Whenever the build does not use
this version's slot pipeline it stops it if it is running. A cooperative
cancel is never swallowed.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from back.core.errors import (
    InfrastructureError,
    OntoBricksError,
    OperationCancelledError,
    ValidationError,
)
from back.core.graphdb.ModeDecision import ModeDecision
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildPostMaterialize import (
    TwinBuildPostMaterialize,
)
from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory
from back.objects.digitaltwin.build.TwinSlotPipeline import TwinSlotPipeline

logger = get_logger(__name__)

# Dependencies are imported inside the methods, as elsewhere in this package:
# ``back.core.graphdb.delta`` imports this build package back (through
# ``DeltaTripleStoreBuildPipeline``) and ``back.core.streaming`` reaches it
# through the SPARQL translator, so module-level imports are circular.

# The translator's all-triples query; the incremental flag turns it into the
# Enzyme-refreshable shape (UNION ALL of single-predicate SELECTs, GROUP BY).
_BATCH_SPARQL = (
    "SELECT DISTINCT ?subject ?predicate ?object WHERE { ?subject ?predicate ?object . }"
)


class TwinIncrementalBuild:
    """Slot pipeline + materialized ``…_data`` for incremental / microbatch domains."""

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    @staticmethod
    def apply(pipe: Any) -> bool:
        """Build ``…_data`` incrementally; ``False`` means "take the full path".

        Re-decides the mode once the mappings are compiled, because only then
        are the source tables known for the row-tracking probe.
        """
        from back.core.graphdb.GraphDBFactory import (
            GraphDBFactory,
            normalize_materialization_mode,
        )

        pipe.slots_table, pipe.slot_pipeline_id, pipe.refresh_technique = None, "", ""
        requested = normalize_materialization_mode(
            GraphDBFactory.resolve_materialization_mode(pipe.domain, pipe.settings)
        )
        if requested == "full":
            TwinIncrementalBuild._leave(pipe, only_if_slots_exist=True)
            return False
        # The Lakebase sync schedule follows the resolver, never a runtime fallback
        # (_fall_back keeps it): the gate first, so a failed probe keeps it too.
        gate = TwinBuildPostMaterialize.gate_decision(pipe.domain, pipe.settings)
        pipe.sync_schedule_mode = gate.effective
        try:
            spec = TwinIncrementalBuild.compile_for(
                pipe.domain,
                pipe.settings,
                pipe.entity_mappings,
                pipe.relationship_mappings,
                base_uri=getattr(pipe, "base_uri", ""),
            )
            try:
                pipe.mode_decision = TwinBuildPostMaterialize.decide_mode(
                    pipe, source_tables=spec.source_tables
                )
            except InfrastructureError:
                TwinIncrementalBuild._keep_schedule_without_probe(pipe, gate.effective)
                raise
            pipe.sync_schedule_mode = pipe.mode_decision.effective
            if pipe.mode_decision.effective == "full":
                TwinIncrementalBuild._leave(pipe)
                return False
            TwinIncrementalBuild._build(pipe, spec)
        except OperationCancelledError:
            raise
        except OntoBricksError as exc:
            TwinIncrementalBuild._fall_back(pipe, requested, exc)
            return False
        return True

    @staticmethod
    def _keep_schedule_without_probe(pipe: Any, gate_mode: str) -> None:
        """The probe failed (no resolver decision): keep the last good build's schedule.

        For a Lakeflow-synced store only (the schedule matters nowhere else):
        the base run's registered schedule, else *gate_mode*. Neither a
        genuinely untracked domain (snapshot) nor a tracked one (triggered)
        is then re-registered because the probe could not answer once.
        """
        from back.objects.digitaltwin.build.TwinLakebaseSync import TwinLakebaseSync

        if pipe._uses_synced_pipeline():
            pipe.sync_schedule_mode = TwinLakebaseSync.previous_schedule_mode(pipe) or gate_mode

    @staticmethod
    def _leave(pipe: Any, *, only_if_slots_exist: bool = False) -> None:
        """Hand the build to the full path: clear ``…_graph``, stop the slot pipeline.

        A ``…_graph`` MATERIALIZED VIEW (``with_graph_mv``) would block the
        union VIEW the full path creates, so it is dropped (best-effort; a VIEW
        or nothing makes the drop fail harmlessly). A ``…_data`` MV is dropped
        by ``materialize.apply_data_relation`` itself.

        This version's slot pipeline is stopped if it is running. A requested
        ``full`` build only looks it up (*only_if_slots_exist*) when the
        ``…_slots`` table exists, so domains that never ran incrementally make
        no pipelines API call.
        """
        from back.core.graphdb.delta import _table_naming, materialize

        graph = _table_naming.graph_view_fqn(pipe.domain, pipe.settings)
        if graph:
            materialize.drop_relation(pipe.source_client, graph, kind="materialized_view")
        if only_if_slots_exist and not materialize.relation_exists(
            pipe.source_client, _table_naming.slots_table_fqn(pipe.domain, pipe.settings)
        ):
            return
        TwinSlotPipeline.stop_if_active(pipe)

    @staticmethod
    def skips_graph_stream(pipe: Any) -> bool:
        """True for the Delta engine, whose ``…_data`` / ``…_graph`` already are the graph.

        Streaming into its store would route every triple into ``…_inferred``
        (``DeltaFlatStore.insert_triples``) right after the build reset it.
        Records the trace's ``apply_mode``: ``none`` here, ``full_stream`` otherwise.
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        backend = GraphDBFactory._resolve_triple_store_backend(pipe.domain, pipe.settings)
        pipe.apply_mode = "none" if backend == "databricks" else "full_stream"
        if backend != "databricks":
            return False
        logger.info(
            "[DT-BUILD %s] Delta engine: %s is the graph; nothing to stream into the store "
            "(%d triples)",
            pipe.task_id,
            TwinBuildPostMaterialize.triples_source_relation(pipe),
            pipe.triple_count,
        )
        return True

    # ------------------------------------------------------------------
    # Steps a–d
    # ------------------------------------------------------------------

    @staticmethod
    def compile_for(
        domain: Any,
        settings: Any,
        entity_mappings: Any,
        relationship_mappings: Any,
        *,
        base_uri: str,
    ):
        """The slot pipeline spec of the mappings, writing *domain*'s ``…_slots`` table.

        Shared by the main build, the MERGE fingerprint and the scheduled tick's
        row-tracking probe, so all three see the same row-local / batch split.
        Raises :class:`ValidationError` when the slots table name cannot be
        resolved or a mapping cannot be compiled.
        """
        from back.core.graphdb.delta import _table_naming
        from back.core.streaming import SlotProjectionCompiler

        slots_fqn = _table_naming.slots_table_fqn(domain, settings)
        if not slots_fqn:
            raise ValidationError("Could not resolve the slots table name")
        return SlotProjectionCompiler.compile(
            entity_mappings or {},
            relationship_mappings or [],
            base_uri=base_uri or "",
            slots_fqn=slots_fqn,
        )

    @staticmethod
    def _build(pipe: Any, spec: Any) -> None:
        mode = pipe.mode_decision.effective
        definition = TwinIncrementalBuild.data_definition_for(
            spec, pipe.entity_mappings, pipe.relationship_mappings, mode
        )
        if definition["refresh_clause"]:
            logger.info(
                "[DT-BUILD %s] microbatch data view refreshes with %s",
                pipe.task_id,
                definition["refresh_clause"],
            )
        slots = definition["slots_fqn"]
        if slots:
            rewritten = TwinIncrementalBuild._rewritten_sources(pipe, spec)
            drifted = TwinBuildRunHistory.reconcile_drift_pending(pipe)
            if drifted:
                logger.info(
                    "[DT-BUILD %s] a scheduled reconciliation found drift: full refresh of "
                    "the slot pipeline",
                    pipe.task_id,
                )
            TwinSlotPipeline.run(pipe, spec, force_full_refresh=bool(rewritten) or drifted)
            pipe.source_versions = TwinIncrementalBuild._source_versions(pipe, spec)
        TwinIncrementalBuild._apply_views(pipe, definition)
        pipe.slots_table = slots
        logger.info(
            "[DT-BUILD %s] %s build of %s done: %d row-local, %d batch mapping(s), "
            "refresh technique %s",
            pipe.task_id,
            pipe.mode_decision.effective,
            pipe.data_table,
            len(spec.row_local_map_ids),
            len(spec.batch_mappings),
            pipe.refresh_technique,
        )

    @staticmethod
    def _rewritten_sources(pipe: Any, spec: Any) -> list:
        """Sources rewritten since the base run recorded their version (``DESCRIBE HISTORY``).

        An ``INSERT OVERWRITE`` / ``CREATE OR REPLACE`` / ``TRUNCATE`` /
        ``RESTORE`` breaks the change feed the slot pipeline reads: the build
        then runs a full refresh of the slot pipeline in the same mode and
        records ``source_overwritten`` as the mode's reason. A history that
        cannot be read raises (the incremental build falls back, as for the
        row-tracking probe).
        """
        from back.core.streaming import SourceReadiness

        base = TwinBuildRunHistory.previous_materialization_stats(pipe)
        recorded = base.get("source_versions")
        if not isinstance(recorded, dict):
            return []
        rewritten = [
            table
            for table in spec.source_tables
            if isinstance(recorded.get(table), int)
            and SourceReadiness.source_overwritten_since(
                pipe.source_client, table, recorded[table]
            )
        ]
        if rewritten:
            decision = pipe.mode_decision
            pipe.mode_decision = ModeDecision(
                decision.requested, decision.effective, "source_overwritten"
            )
            logger.info(
                "[DT-BUILD %s] %s rewritten since the last build: full refresh of the slot "
                "pipeline",
                pipe.task_id,
                ", ".join(rewritten),
            )
        return rewritten

    @staticmethod
    def _source_versions(pipe: Any, spec: Any) -> dict:
        """``{source table: latest Delta version}`` for the next build's overwrite check.

        Best-effort: a table whose history cannot be read is left out (the next
        build then cannot tell, and does not force a refresh for it).
        """
        from back.core.graphdb.delta import ChangeFeedReader

        versions = {}
        for table in spec.source_tables:
            try:
                versions[table] = ChangeFeedReader.latest_version(pipe.source_client, table)
            except InfrastructureError as exc:
                logger.debug("[DT-BUILD %s] no version of %s: %s", pipe.task_id, table, exc)
        return versions

    @staticmethod
    def data_definition_for(
        spec: Any, entity_mappings: Any, relationship_mappings: Any, mode: str
    ) -> dict:
        """The :meth:`MaterializedViewBuilder.data_mv_sql` keywords of ``…_data`` for *mode*.

        ``slots_fqn`` when a mapping is row-local, ``batch_branch_sql`` for the
        batch mappings (``None`` without), and ``refresh_clause``: a
        ``microbatch`` view refreshes between builds — its continuous pipeline
        keeps ``…_slots`` current, but only a refresh moves the view —
        ``TRIGGER ON UPDATE`` for at most ten sources, else every 15 minutes
        (:meth:`MaterializedViewBuilder.microbatch_refresh_clause`). Shared by
        the main build and the scheduled tick's staleness check.
        """
        from back.core.graphdb.delta.MaterializedViewBuilder import MaterializedViewBuilder

        clause = ""
        if mode == "microbatch":
            clause = MaterializedViewBuilder.microbatch_refresh_clause(spec.mv_source_count())
        return {
            "slots_fqn": spec.slots_fqn if spec.row_local_map_ids else None,
            "batch_branch_sql": TwinIncrementalBuild._batch_branch_sql(
                spec, entity_mappings, relationship_mappings
            ),
            "refresh_clause": clause,
        }

    @staticmethod
    def _batch_branch_sql(
        spec: Any, entity_mappings: Any, relationship_mappings: Any
    ) -> str | None:
        """Translator SQL for the batch mappings only; ``None`` when there are none.

        The translator is not called at all without batch mappings: a filter
        that rejects every mapping raises ``ValidationError``.
        """
        from back.core.w3c import sparql

        batch = spec.batch_mappings
        if not batch:
            return None
        result = sparql.translate_sparql_to_spark(
            _BATCH_SPARQL,
            entity_mappings,
            None,
            relationship_mappings,
            dialect="spark",
            incremental=True,
            mappings_filter=lambda m: any(m is b for b in batch),
        )
        sql = str((result or {}).get("sql") or "")
        if not sql.strip():
            raise ValidationError("The batch mappings translated to no SQL")
        return sql

    @staticmethod
    def _apply_views(pipe: Any, definition: dict) -> None:
        """Step d: the ``…_data`` materialized view, a synchronous refresh, its technique.

        *definition* holds the :meth:`MaterializedViewBuilder.data_mv_sql` keywords
        (``slots_fqn``, ``batch_branch_sql``, ``refresh_clause``).

        ``CREATE OR REPLACE`` would recompute the view from scratch, so it only
        runs when the stored definition fingerprint (``ontobricks.mv_sql_sha256``)
        differs from the current one; otherwise the existing view is refreshed
        in place. ``refresh_technique`` is read after the REFRESH, so it
        describes the refresh that actually ran.

        ``drop_existing_table`` is ``True`` whenever the view is created: no
        earlier incremental build is recorded yet (the build-run trace is
        in-memory until it is persisted), and the drop is best-effort — a
        no-op when ``…_data`` is absent, the needed drop when a full build left
        a TABLE, refused (harmlessly) when it already is a materialized view.
        """
        from back.core.graphdb.delta import _table_naming, materialize
        from back.core.graphdb.delta.MaterializedViewBuilder import MaterializedViewBuilder

        data = _table_naming.data_table_fqn(pipe.domain, pipe.settings)
        graph = _table_naming.graph_view_fqn(pipe.domain, pipe.settings)
        inferred = _table_naming.inferred_table_fqn(pipe.domain, pipe.settings)
        if not (data and graph and inferred):
            raise ValidationError("Could not resolve the incremental table names")
        client = pipe.source_client
        current = MaterializedViewBuilder.data_mv_sha(**definition)
        if MaterializedViewBuilder.existing_mv_sha(client, data) == current:
            logger.info(
                "[DT-BUILD %s] %s definition unchanged; refreshing it in place",
                pipe.task_id,
                data,
            )
        else:
            TwinIncrementalBuild._create_views(pipe, data, graph, inferred, definition)
        pipe.tm.update_progress(pipe.task_id, 32, f"Refreshing materialized view {data}...")
        TwinIncrementalBuild.warehouse_step(
            f"refresh {data}",
            lambda: materialize.run_sql(client, MaterializedViewBuilder.refresh_sql(data)),
        )
        pipe.refresh_technique = MaterializedViewBuilder.last_refresh_technique(client, data)
        pipe.data_table = data
        pipe.data_relation_mode = TwinBuildPostMaterialize.MATERIALIZED_VIEW

    @staticmethod
    def _create_views(pipe: Any, data: str, graph: str, inferred: str, definition: dict) -> None:
        from back.core.graphdb.delta.MaterializedViewBuilder import MaterializedViewBuilder

        client = pipe.source_client
        pipe.tm.update_progress(pipe.task_id, 30, f"Creating materialized view {data}...")
        TwinIncrementalBuild.warehouse_step(
            f"create the materialized view {data}",
            lambda: MaterializedViewBuilder.apply(
                client,
                data_fqn=data,
                graph_fqn=graph,
                inferred_fqn=inferred,
                drop_existing_table=True,
                with_graph_mv=False,
                **definition,
            ),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fall_back(pipe: Any, requested: str, exc: OntoBricksError) -> None:
        reason = f"incremental_failed:{type(exc).__name__}"
        logger.warning(
            "[DT-BUILD %s] %s build failed, running a full rebuild instead (%s): %s",
            pipe.task_id,
            requested,
            reason,
            exc,
        )
        pipe.mode_decision = ModeDecision(requested, "full", reason)
        # Message and driver detail, read by TwinMergeFallback.applies.
        pipe.mode_fallback_detail = f"{exc} {exc.detail or ''}".strip()
        pipe.slots_table = None
        pipe.refresh_technique = ""
        TwinIncrementalBuild._leave(pipe)
        pipe.tm.update_progress(
            pipe.task_id, 30, "Incremental build unavailable; running a full rebuild..."
        )

    @staticmethod
    def warehouse_step(what: str, statement: Callable[[], Any]) -> Any:
        """Run a warehouse step; driver errors become :class:`InfrastructureError`.

        The SQL connector raises its own exception types, which the fallback
        (``except OntoBricksError``) would otherwise miss and fail the build.
        """
        try:
            return statement()
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(f"Could not {what}: {exc}", detail=str(exc)) from exc
