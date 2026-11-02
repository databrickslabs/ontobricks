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
c. run an update and wait for it. A full refresh (new pipeline or changed
   SQL) always completes as a *triggered* update first, so ``…_data`` is never
   built over an empty ``…_slots``; a ``microbatch`` pipeline then switches to
   continuous and is waited for until ``RUNNING``;
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

import os
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

_TIMEOUT_ENV = "DTWIN_SLOT_PIPELINE_TIMEOUT_S"
_DEFAULT_TIMEOUT_S = 3600
_DEFAULT_CHANNEL = "PREVIEW"

#: Pipeline states in which an update may be running (it must be stopped first).
_ACTIVE_PIPELINE_STATES = frozenset(
    {"DEPLOYING", "RECOVERING", "RESETTING", "RUNNING", "STARTING", "STOPPING"}
)


class TwinIncrementalBuild:
    """Slot pipeline + materialized ``…_data`` for incremental / microbatch domains."""

    # ------------------------------------------------------------------
    # Settings
    # ------------------------------------------------------------------

    @staticmethod
    def slot_pipeline_channel(settings: Any) -> str:
        """Lakeflow channel of the slot pipeline (``PREVIEW`` unless configured)."""
        return str(getattr(settings, "slot_pipeline_channel", "") or _DEFAULT_CHANNEL)

    @staticmethod
    def slot_pipeline_timeout_s() -> int:
        """``DTWIN_SLOT_PIPELINE_TIMEOUT_S``; 3600 when unset, invalid or not positive."""
        try:
            value = int((os.environ.get(_TIMEOUT_ENV) or "").strip())
        except ValueError:
            return _DEFAULT_TIMEOUT_S
        return value if value > 0 else _DEFAULT_TIMEOUT_S

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
        try:
            spec = TwinIncrementalBuild.compile_for(
                pipe.domain,
                pipe.settings,
                pipe.entity_mappings,
                pipe.relationship_mappings,
                base_uri=getattr(pipe, "base_uri", ""),
            )
            pipe.mode_decision = TwinBuildPostMaterialize.decide_mode(
                pipe, source_tables=spec.source_tables
            )
            # The Lakebase sync schedule follows this decision; _fall_back keeps it.
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
        TwinIncrementalBuild._stop_slot_pipeline(pipe)

    @staticmethod
    def _stop_slot_pipeline(pipe: Any) -> None:
        """Best-effort stop of this version's slot pipeline when it is active.

        Failures are logged and swallowed: a pipeline left running costs
        compute, not correctness, and must not fail the full build.
        """
        # Follow-up (docs/incremental-materialization.md, Known limitations):
        # pipelines of earlier versions keep running after a version bump;
        # only the current version's pipeline is stopped here.
        from back.core.streaming import SlotPipelineRunner

        name = SlotPipelineRunner.pipeline_name(
            pipe.domain_name, TwinBuildRunHistory.build_run_key(pipe)[1]
        )
        try:
            runner = SlotPipelineRunner()
            pipeline_id = runner.find_pipeline_id(name)
            if pipeline_id and runner.get_state(pipeline_id) in _ACTIVE_PIPELINE_STATES:
                runner.stop(pipeline_id)
                logger.info("[DT-BUILD %s] stopped slot pipeline %s", pipe.task_id, name)
        except OperationCancelledError:
            raise
        except OntoBricksError as exc:
            logger.warning(
                "[DT-BUILD %s] could not stop slot pipeline %s (non-fatal): %s",
                pipe.task_id,
                name,
                exc,
            )

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
        domain: Any, settings: Any, entity_mappings: Any, relationship_mappings: Any, *, base_uri: str
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
        batch_sql = TwinIncrementalBuild._batch_branch_sql(pipe, spec)
        slots = spec.slots_fqn if spec.row_local_map_ids else None
        if slots:
            TwinIncrementalBuild._run_slot_pipeline(pipe, spec)
        TwinIncrementalBuild._apply_views(pipe, slots, batch_sql)
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
    def _batch_branch_sql(pipe: Any, spec: Any) -> str | None:
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
            pipe.entity_mappings,
            None,
            pipe.relationship_mappings,
            dialect="spark",
            incremental=True,
            mappings_filter=lambda m: any(m is b for b in batch),
        )
        sql = str((result or {}).get("sql") or "")
        if not sql.strip():
            raise ValidationError("The batch mappings translated to no SQL")
        return sql

    @staticmethod
    def _run_slot_pipeline(pipe: Any, spec: Any) -> None:
        """Steps b and c: ensure the pipeline, then update and wait.

        A full refresh runs when the pipeline is new or its stored SQL
        fingerprint differs (flows were added or removed, so stale slots must
        go). It always runs as a *triggered* update to ``COMPLETED`` — a
        continuous one would let ``…_data`` be built over a still-empty
        ``…_slots`` — and the pipeline keeps its previous fingerprint until
        then, so a failed refresh is retried by the next build. Afterwards the
        new fingerprint is stamped and a ``microbatch`` pipeline is switched
        to continuous (waited for until ``RUNNING``).

        Without a full refresh a ``microbatch`` pipeline already running the
        current SQL is left running; any other active pipeline is stopped
        before the update.
        """
        from back.core.streaming import SlotPipelineRunner

        runner = SlotPipelineRunner()
        name = SlotPipelineRunner.pipeline_name(
            pipe.domain_name, TwinBuildRunHistory.build_run_key(pipe)[1]
        )
        catalog, schema, _ = spec.slots_fqn.split(".", 2)
        definition = {
            "name": name,
            "catalog": catalog,
            "schema": schema,
            "sql_text": spec.to_sql(),
            "serverless": True,
            "channel": TwinIncrementalBuild.slot_pipeline_channel(pipe.settings),
        }
        current = SlotPipelineRunner.fingerprint(definition["sql_text"])
        continuous = pipe.mode_decision.effective == "microbatch"

        existing = runner.find_pipeline_id(name)
        stored = runner.stored_fingerprint(existing) if existing else ""
        full_refresh = stored != current
        if existing and continuous and not full_refresh and runner.get_state(existing) == "RUNNING":
            pipe.slot_pipeline_id = existing
            logger.info(
                "[DT-BUILD %s] slot pipeline %s already streams the current SQL",
                pipe.task_id,
                name,
            )
            return

        pipe.tm.update_progress(pipe.task_id, 27, f"Preparing slot pipeline {name}...")
        pipeline_id = runner.ensure(
            **definition,
            continuous=continuous and not full_refresh,
            fingerprint=stored if full_refresh else current,
        )
        pipe.slot_pipeline_id = pipeline_id
        if runner.get_state(pipeline_id) in _ACTIVE_PIPELINE_STATES:
            runner.stop(pipeline_id)
        TwinIncrementalBuild._update(
            pipe,
            runner,
            pipeline_id,
            name,
            full_refresh=full_refresh,
            until_running=continuous and not full_refresh,
        )
        if not full_refresh:
            return
        runner.ensure(**definition, continuous=continuous, fingerprint=current)
        if continuous:
            TwinIncrementalBuild._update(
                pipe, runner, pipeline_id, name, full_refresh=False, until_running=True
            )

    @staticmethod
    def _update(
        pipe: Any,
        runner: Any,
        pipeline_id: str,
        name: str,
        *,
        full_refresh: bool,
        until_running: bool,
    ) -> None:
        """:meth:`run_update` with the build's cancel check and progress reports."""
        TwinIncrementalBuild.run_update(
            runner,
            pipeline_id,
            full_refresh=full_refresh,
            until_running=until_running,
            cancel_check=pipe._is_cancelled,
            on_state=lambda state: pipe.tm.update_progress(
                pipe.task_id, 28, f"Slot pipeline {name}: {state.lower()}"
            ),
        )

    @staticmethod
    def run_update(
        runner: Any,
        pipeline_id: str,
        *,
        full_refresh: bool,
        until_running: bool,
        cancel_check: Callable[[], bool] | None = None,
        on_state: Callable[[str], None] | None = None,
    ) -> None:
        """Start one update of *pipeline_id* and wait for it (``COMPLETED``, or ``RUNNING``)."""
        update_id = runner.start_update(pipeline_id, full_refresh=full_refresh)
        runner.wait_for_update(
            pipeline_id,
            update_id,
            timeout_s=TwinIncrementalBuild.slot_pipeline_timeout_s(),
            cancel_check=cancel_check,
            on_state=on_state,
            until_running=until_running,
        )

    @staticmethod
    def _apply_views(pipe: Any, slots: str | None, batch_sql: str | None) -> None:
        """Step d: the ``…_data`` materialized view, a synchronous refresh, its technique.

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
        current = MaterializedViewBuilder.data_mv_sha(slots_fqn=slots, batch_branch_sql=batch_sql)
        if MaterializedViewBuilder.existing_mv_sha(client, data) == current:
            logger.info(
                "[DT-BUILD %s] %s definition unchanged; refreshing it in place",
                pipe.task_id,
                data,
            )
        else:
            TwinIncrementalBuild._create_views(pipe, data, graph, inferred, slots, batch_sql)
        pipe.tm.update_progress(pipe.task_id, 32, f"Refreshing materialized view {data}...")
        TwinIncrementalBuild.warehouse_step(
            f"refresh {data}",
            lambda: materialize.run_sql(client, MaterializedViewBuilder.refresh_sql(data)),
        )
        pipe.refresh_technique = MaterializedViewBuilder.last_refresh_technique(client, data)
        pipe.data_table = data
        pipe.data_relation_mode = TwinBuildPostMaterialize.MATERIALIZED_VIEW

    @staticmethod
    def _create_views(
        pipe: Any, data: str, graph: str, inferred: str, slots: str | None, batch_sql: str | None
    ) -> None:
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
                slots_fqn=slots,
                batch_branch_sql=batch_sql,
                drop_existing_table=True,
                with_graph_mv=False,
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
