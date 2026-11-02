"""Which relation Lakeflow syncs into Lakebase, on which key, and how often.

Extracted collaborator of
:class:`~back.objects.digitaltwin.build._build_pipeline._BuildPipeline`, in the
style of :class:`TwinBuildPostMaterialize`: :meth:`TwinLakebaseSync.sync_plan`
reads the running pipeline's facts; the scheduled-build mirror
(``registry/schedule/build.py``) calls the pure :meth:`TwinLakebaseSync.sync_plan_for`
with what it knows.

Source: ``…_data`` (table or materialized view — the complete graph, slots and
batch branch) keyed on ``LAKEFLOW_SYNC_PRIMARY_KEY``; the R2RML gateway VIEW
when ``…_data`` is not a copy (a pass-through VIEW, or nothing built yet); and
the ``…_slots`` streaming table, keyed on ``SLOT_SYNC_PRIMARY_KEY``, when the
schedule is triggered or continuous and the slots carry the whole graph (a slot
pipeline ran and the build counted no batch mapping). A streaming table has a
change feed every workspace can sync from; the ``…_data`` materialized view
only has one where the MV change-feed preview is enabled (measured live: the
sync pipeline fails with ``MATERIALIZED_VIEW_CDF_REQUIRES_EXTERNAL_METADATA``
without it). Asserted-only readers (entity-search rebuild,
``include_inferred=False`` reads, BFS expansion) query the Postgres asserted
view — distinct, non-blank — rather than the ``_sync`` table, so a slots source
reads like ``…_data`` to them. A build that fell back to the full path has no
live slots and syncs ``…_data`` again (a source switch recreates the
registration: delete, purge, full resync).

Schedule: the mode's schedule (``full`` → snapshot, ``incremental`` →
triggered, ``microbatch`` → continuous) capped by the configured
``graph_engine_config.lakebase.sync_table_mode`` over snapshot < triggered <
continuous. The default ``snapshot`` therefore keeps every domain on snapshots
until an admin raises it. A VIEW has no change feed and always syncs as a
snapshot.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from back.core.errors import InfrastructureError, OperationCancelledError
from back.core.logging import get_logger
from back.objects.digitaltwin.build.SyncPlan import SyncPlan
from back.objects.digitaltwin.build.TwinBuildPostMaterialize import (
    TwinBuildPostMaterialize,
)

logger = get_logger(__name__)

#: Lakeflow schedule each effective materialization mode asks for.
_MODE_SCHEDULE = {
    "full": "snapshot",
    "incremental": "triggered",
    "microbatch": "continuous",
    # Realtime serves its freshest rows from ``_live``; ``_data`` syncs like microbatch.
    "realtime": "continuous",
}
#: The mode whose schedule a registration is (the inverse of _MODE_SCHEDULE;
#: ``continuous`` reads back as ``microbatch``, the mode both share it with).
_SCHEDULE_MODE = {"snapshot": "full", "triggered": "incremental", "continuous": "microbatch"}

#: ``data_relation_mode`` values whose ``…_data`` holds a computed copy of the triples.
_COPIED_RELATIONS = ("table", TwinBuildPostMaterialize.MATERIALIZED_VIEW)

#: ``sync_fallback_reason`` of a triggered / continuous registration on the ``…_data``
#: materialized view re-registered as a snapshot because its sync pipeline failed:
#: a synced table reads its source's change feed, and an MV only has one where the
#: external-metadata preview is enabled (``MATERIALIZED_VIEW_CDF_REQUIRES_EXTERNAL_METADATA``).
MV_CHANGE_FEED_UNAVAILABLE = "mv_change_feed_unavailable"


class TwinLakebaseSync:
    """Lakeflow synced-table source, key and schedule for a build."""

    @staticmethod
    def sync_plan(pipe: Any) -> SyncPlan:
        """:meth:`sync_plan_for` from the running pipeline's relation and mode facts.

        The schedule follows ``sync_schedule_mode``, the resolver's decision
        before any runtime fallback (a MERGE promotion sets ``incremental``):
        a transient slot-pipeline or MV failure must not re-register a
        triggered/continuous table as a snapshot (delete, purge, full resync)
        and back on the next good build. The CTAS ``…_data`` such a fallback
        builds has row tracking and ``object_hash``, so the schedule still
        holds. Without it, the effective mode decides.
        """
        decision = getattr(pipe, "mode_decision", None)
        scheduled = getattr(pipe, "sync_schedule_mode", "")
        if not (isinstance(scheduled, str) and scheduled):
            scheduled = getattr(decision, "effective", "full")
        return TwinLakebaseSync.sync_plan_for(
            view_table=pipe.view_table,
            data_table=getattr(pipe, "data_table", "") or "",
            mode_effective=scheduled,
            configured_sync_table_mode=getattr(pipe.store, "sync_table_mode", "snapshot"),
            source_is_view=getattr(pipe, "data_relation_mode", None) not in _COPIED_RELATIONS,
            slots_table=getattr(pipe, "slots_table", "") or "",
            batch_mappings=getattr(pipe, "batch_mappings", None),
        )

    @staticmethod
    def previous_schedule_mode(pipe: Any) -> str:
        """The mode whose schedule the base run registered, ``""`` when there is none.

        Read from its ``stats.materialization.sync_mode_effective``
        (snapshot -> ``full``, triggered -> ``incremental``, continuous ->
        ``microbatch``) through :meth:`TwinBuildRunHistory.previous_materialization_stats`.
        """
        from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory

        facts = TwinBuildRunHistory.previous_materialization_stats(pipe)
        return _SCHEDULE_MODE.get(str(facts.get("sync_mode_effective") or ""), "")

    @staticmethod
    def sync_plan_for(
        *,
        view_table: str,
        data_table: str,
        mode_effective: str,
        configured_sync_table_mode: Any,
        source_is_view: bool = False,
        slots_table: str = "",
        batch_mappings: Any = None,
    ) -> SyncPlan:
        """Decide the source, primary key and schedule of the synced table.

        *source_is_view* means *data_table* is not a copy of the triples (a
        pass-through VIEW without ``object_hash``): Lakeflow then syncs the
        gateway VIEW, as a snapshot. A triggered or continuous schedule syncs
        *slots_table* instead of *data_table* when the slots carry the whole
        graph: *batch_mappings* is the build's count of join / aggregate /
        ``sql_query`` / relationship mappings and must be exactly ``0`` (a
        missing count — no slot pipeline, a fallback, an older trace — keeps
        ``…_data``).
        """
        from back.core.graphdb.lakebase._companion_ddl import (
            LAKEFLOW_SYNC_PRIMARY_KEY,
            SLOT_SYNC_PRIMARY_KEY,
            SOURCE_KIND_SLOTS,
        )

        if data_table and not source_is_view:
            schedule = TwinLakebaseSync._schedule(mode_effective, configured_sync_table_mode)
            whole_graph = (
                isinstance(batch_mappings, int)
                and not isinstance(batch_mappings, bool)
                and batch_mappings == 0
            )
            if schedule != "snapshot" and slots_table and whole_graph:
                return SyncPlan(slots_table, SLOT_SYNC_PRIMARY_KEY, schedule, SOURCE_KIND_SLOTS)
            return SyncPlan(data_table, LAKEFLOW_SYNC_PRIMARY_KEY, schedule)
        return SyncPlan(view_table, LAKEFLOW_SYNC_PRIMARY_KEY, "snapshot")

    @staticmethod
    def register(pipe: Any, mgr: Any, synced_uc: str, plan: SyncPlan) -> Any:
        """Register *synced_uc* as *plan* says; record the sync facts the trace reports.

        ``sync_source`` / ``sync_mode_effective`` are set on *pipe* only once
        ``ensure`` succeeded, so a failed registration traces no sync.
        """
        synced = mgr.ensure(
            synced_uc,
            before_delete=TwinLakebaseSync.union_view_dropper(pipe.store, pipe.graph_name),
            **plan.ensure_kwargs(),
        )
        pipe.sync_source, pipe.sync_mode_effective = plan.source_fqn, plan.sync_mode
        pipe.sync_source_kind = plan.source_kind
        return synced

    @staticmethod
    def sync_and_wait(
        pipe: Any, mgr: Any, synced_uc: str, plan: SyncPlan, *, on_state_change: Any = None
    ) -> str:
        """The build's sync wait (:meth:`trigger_and_wait`), recording a snapshot fallback on *pipe*."""
        state, synced = TwinLakebaseSync.trigger_and_wait(
            mgr,
            synced_uc,
            plan,
            timeout_s=pipe.store.sync_timeout_s,
            data_is_mv=getattr(pipe, "data_relation_mode", None)
            == TwinBuildPostMaterialize.MATERIALIZED_VIEW,
            cancel_check=pipe._is_cancelled,
            on_state_change=on_state_change,
            before_delete=TwinLakebaseSync.union_view_dropper(pipe.store, pipe.graph_name),
        )
        if synced is not plan:
            pipe.sync_mode_effective = synced.sync_mode
            pipe.sync_fallback_reason = MV_CHANGE_FEED_UNAVAILABLE
        return state

    @staticmethod
    def trigger_and_wait(
        mgr: Any,
        synced_uc: str,
        plan: SyncPlan,
        *,
        timeout_s: int,
        data_is_mv: bool,
        cancel_check: Any = None,
        on_state_change: Any = None,
        before_delete: Optional[Callable[[str], None]] = None,
    ) -> tuple[str, SyncPlan]:
        """``mgr.trigger_and_wait`` with the MV change-feed fallback; returns the state and the plan synced.

        A triggered or continuous registration on the ``…_data`` materialized
        view (*data_is_mv*) whose sync pipeline fails is re-registered as a
        snapshot of the same relation (``ensure`` recreates a registration whose
        policy changed) and synced again — measured live, the pipeline reads
        the MV's change feed and fails with
        ``MATERIALIZED_VIEW_CDF_REQUIRES_EXTERNAL_METADATA`` where that preview
        is missing, while the snapshot of the same view lands. Any other failure,
        a cancellation, and a failure of the snapshot itself propagate.
        """
        from back.core.graphdb.lakebase._companion_ddl import SOURCE_KIND_TRIPLES

        try:
            return (
                mgr.trigger_and_wait(
                    synced_uc,
                    timeout_s=timeout_s,
                    cancel_check=cancel_check,
                    on_state_change=on_state_change,
                ),
                plan,
            )
        except OperationCancelledError:
            raise
        except InfrastructureError as exc:
            mv_feed = (
                data_is_mv
                and plan.source_kind == SOURCE_KIND_TRIPLES
                and plan.sync_mode != "snapshot"
                and "PIPELINE_FAILED" in str(exc).upper()
            )
            if not mv_feed:
                raise
            logger.warning(
                "Synced table %s: the %s sync of the %s materialized view failed (%s); "
                "its change feed needs the external-metadata preview — re-registering "
                "the table as a snapshot of the same view",
                synced_uc,
                plan.sync_mode,
                plan.source_fqn,
                exc,
            )
        snapshot = SyncPlan(plan.source_fqn, plan.primary_key, "snapshot", plan.source_kind)
        mgr.ensure(synced_uc, before_delete=before_delete, **snapshot.ensure_kwargs())
        state = mgr.trigger_and_wait(
            synced_uc, timeout_s=timeout_s, cancel_check=cancel_check, on_state_change=on_state_change
        )
        return state, snapshot

    @staticmethod
    def union_view_dropper(store: Any, graph_name: str) -> Callable[[str], None]:
        """``before_delete`` hook: drop the Postgres union view of *graph_name*.

        ``SyncedTableManager.ensure`` calls it right before it deletes a drifted
        registration (whose purge drops the ``_sync`` table the view depends
        on), so a failed ``ensure`` never leaves readers without a view; the
        build's ``ensure_synced_union_view`` step re-creates it. Best-effort: a
        failure is logged and the delete goes ahead.
        """

        def _drop(synced_uc: str) -> None:
            try:
                store.drop_union_view(graph_name)
            except Exception as exc:  # noqa: BLE001 — best-effort; the delete still runs
                logger.warning(
                    "Could not drop the union view of %s before recreating %s: %s",
                    graph_name,
                    synced_uc,
                    exc,
                )
                return
            logger.info(
                "Dropped the union view of %s: synced table %s is being recreated",
                graph_name,
                synced_uc,
            )

        return _drop

    @staticmethod
    def _schedule(mode_effective: str, configured: Any) -> str:
        """The mode's schedule, capped by *configured* (unreadable → ``snapshot``)."""
        from back.core.graphdb.lakebase.LakebaseBase import SYNC_TABLE_MODES

        cap = configured.strip().lower() if isinstance(configured, str) else ""
        if cap not in SYNC_TABLE_MODES:
            cap = "snapshot"
        wanted = _MODE_SCHEDULE.get(mode_effective, "snapshot")
        return min(wanted, cap, key=SYNC_TABLE_MODES.index)
