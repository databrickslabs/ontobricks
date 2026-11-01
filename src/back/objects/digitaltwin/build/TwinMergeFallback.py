"""``…_data`` by MERGE for an incremental domain that cannot run serverless pipelines.

Extracted collaborator of
:class:`~back.objects.digitaltwin.build._build_pipeline._BuildPipeline`, in the
style of :class:`TwinIncrementalBuild`: static methods that take the running
pipeline (*pipe*); ``_materialize_data_table`` keeps a short hook in front of
its CTAS.

Without serverless pipelines (or when the slot pipeline failed for want of
them) an ``incremental`` domain cannot get its slot pipeline and materialized
view. Instead of replacing ``…_data`` by CTAS, the gateway VIEW is merged into
it (:class:`DeltaMergeMaterializer`): only changed rows are rewritten, and the
row-tracked table's change feed then serves a triggered Lakebase sync or the
net change apply. The build therefore still runs as ``incremental`` (strategy
``merge``, the fallback reason kept).

When every mapping is row-local, the fingerprint of the source table versions,
the mapping and the generated SQL tells an unchanged domain: if it equals the
base run's, ``…_data`` is still the table that run left and the store still
holds the graph, the build completes at once as skipped.
"""

from __future__ import annotations

from typing import Any

from back.core.errors import InfrastructureError, OntoBricksError, OperationCancelledError
from back.core.graphdb.MaterializationModeResolver import MaterializationModeResolver
from back.core.graphdb.ModeDecision import ModeDecision
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory
from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild

logger = get_logger(__name__)

#: Message (task and build run) of a build skipped for unchanged sources.
SKIPPED_MESSAGE = "no source changes (skipped)"

_INFRASTRUCTURE_FALLBACK = "incremental_failed:InfrastructureError"


class TwinMergeFallback:
    """MERGE-maintained ``…_data`` for incremental domains without serverless pipelines."""

    @staticmethod
    def applies(pipe: Any) -> bool:
        """True when an ``incremental`` request fell back to ``full`` for want of serverless.

        That is the resolver's ``serverless_unavailable``, or an infrastructure
        failure of the incremental build whose message or detail mentions
        serverless or pipelines. ``microbatch`` needs a continuous pipeline,
        which a MERGE cannot stand in for.
        """
        decision = getattr(pipe, "mode_decision", None)
        if not isinstance(decision, ModeDecision):
            return False
        if decision.requested != "incremental" or decision.effective != "full":
            return False
        if decision.reason == MaterializationModeResolver.REASON_SERVERLESS_UNAVAILABLE:
            return True
        detail = str(getattr(pipe, "mode_fallback_detail", "") or "").lower()
        return decision.reason.startswith(_INFRASTRUCTURE_FALLBACK) and (
            "serverless" in detail or "pipeline" in detail
        )

    @staticmethod
    def apply(pipe: Any) -> bool | None:
        """Merge ``…_data``: ``True`` merged; ``False`` build completed as skipped; ``None`` failed.

        ``None`` leaves the mode decision as the resolver's (``full``) so the
        caller builds ``…_data`` by CTAS. ``False`` means the task is complete
        and recorded: the caller stops the pipeline there.
        """
        from back.core.graphdb.delta import DeltaMergeMaterializer, _table_naming

        data = _table_naming.data_table_fqn(pipe.domain, pipe.settings)
        if not data:
            return None  # the CTAS path reports the unresolved name
        fingerprint = TwinMergeFallback._fingerprint(pipe)
        if fingerprint and TwinMergeFallback._unchanged(pipe, fingerprint, data):
            TwinMergeFallback._complete_skipped(pipe, data, fingerprint)
            return False
        pipe.tm.update_progress(pipe.task_id, 30, f"Merging mapped triples into {data}...")
        try:
            DeltaMergeMaterializer.apply(
                pipe.source_client,
                pipe.view_table,
                data,
                with_object_hash=pipe._wrap_view_sql_for_lakeflow(),
            )
        except OperationCancelledError:
            raise
        except OntoBricksError as exc:
            logger.warning(
                "[DT-BUILD %s] MERGE into %s failed, building it by CTAS instead: %s",
                pipe.task_id,
                data,
                exc,
            )
            return None
        TwinMergeFallback._record(pipe, data, fingerprint)
        logger.info("[DT-BUILD %s] merged %s into %s", pipe.task_id, pipe.view_table, data)
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fingerprint(pipe: Any) -> str:
        """Fingerprint of an all-row-local domain; ``""`` (never skip) otherwise.

        Batch mappings read SQL whose source tables are not known, so their
        changes would not move the fingerprint. A failed version read also
        gives ``""``.
        """
        from back.core.graphdb.delta import ChangeFeedReader, DeltaMergeMaterializer

        try:
            spec = TwinIncrementalBuild._compile(pipe)
            if spec.batch_mappings or not spec.source_tables:
                return ""
            versions = {
                table: ChangeFeedReader.latest_version(pipe.source_client, table)
                for table in spec.source_tables
            }
        except OperationCancelledError:
            raise
        except OntoBricksError as exc:
            logger.debug("[DT-BUILD %s] no MERGE fingerprint: %s", pipe.task_id, exc)
            return ""
        return DeltaMergeMaterializer.fingerprint(
            versions,
            getattr(pipe, "r2rml_content", "") or "",
            getattr(pipe, "base_uri", "") or "",
            view_sql=getattr(pipe, "spark_sql", "") or "",
        )

    @staticmethod
    def _unchanged(pipe: Any, fingerprint: str, data: str) -> bool:
        """The base run merged the same fingerprint, into the ``…_data`` still there.

        ``…_data`` must exist and, when the base recorded its version, still be
        at that version; the store must still hold the graph when it can say so.
        """
        from back.core.graphdb.delta import materialize

        base = TwinBuildRunHistory.previous_materialization_stats(pipe)
        if base.get("fingerprint") != fingerprint or base.get("strategy") != "merge":
            return False
        if not materialize.relation_exists(pipe.source_client, data):
            return False
        return TwinMergeFallback._data_unchanged_since(
            pipe, data, base.get("data_table_version")
        ) and TwinMergeFallback._store_holds_graph(pipe)

    @staticmethod
    def _data_unchanged_since(pipe: Any, data: str, version: Any) -> bool:
        """``…_data`` is still at the base's *version* (no recorded version: nothing to check)."""
        from back.core.graphdb.delta import ChangeFeedReader

        if isinstance(version, bool) or not isinstance(version, int):
            return True
        try:
            return ChangeFeedReader.latest_version(pipe.source_client, data) == version
        except InfrastructureError as exc:
            logger.debug("[DT-BUILD %s] no version of %s: %s", pipe.task_id, data, exc)
            return False

    @staticmethod
    def _store_holds_graph(pipe: Any) -> bool:
        """The store still has the graph (a store dropped outside a build never skips).

        Not asked on the Delta engine, whose graph is ``…_data``, nor of a store
        without the probe.
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        if GraphDBFactory._resolve_graph_backend(pipe.domain) == "databricks":
            return True
        if not hasattr(pipe.store, "table_exists"):
            return True
        return bool(pipe.store.table_exists(pipe.graph_name))

    @staticmethod
    def _record(pipe: Any, data: str, fingerprint: str) -> None:
        decision = pipe.mode_decision
        pipe.data_table, pipe.data_relation_mode = data, "table"
        pipe.mode_decision = ModeDecision(decision.requested, "incremental", decision.reason)
        pipe.strategy, pipe.fingerprint = "merge", fingerprint

    @staticmethod
    def _complete_skipped(pipe: Any, data: str, fingerprint: str) -> None:
        """Complete and record the build as a success through the pipeline's completion."""
        TwinMergeFallback._record(pipe, data, fingerprint)
        pipe.apply_mode = "none"
        pipe.triple_count = pipe._count_source_triples()
        logger.info(
            "[DT-BUILD %s] sources and mapping unchanged since %s was merged; build skipped",
            pipe.task_id,
            data,
        )
        if not pipe.is_api:
            pipe._populate_session_cache()
        pipe._complete_task(message=SKIPPED_MESSAGE, build_mode="incremental")
