"""Which relation Lakeflow syncs into Lakebase, on which key, and how often.

Extracted collaborator of
:class:`~back.objects.digitaltwin.build._build_pipeline._BuildPipeline`, in the
style of :class:`TwinBuildPostMaterialize`: :meth:`TwinLakebaseSync.sync_plan`
reads the running pipeline's facts; the scheduled-build mirror
(``registry/schedule/build.py``) calls the pure :meth:`TwinLakebaseSync.sync_plan_for`
with what it knows.

Source: ``…_data`` (table or materialized view — the complete graph, slots and
batch branch) keyed on ``LAKEFLOW_SYNC_PRIMARY_KEY``; the R2RML gateway VIEW
when ``…_data`` is not a copy (a pass-through VIEW, or nothing built yet).
``…_slots`` is never a source in this release, even when it holds every
triple (no batch mapping): asserted-only readers (entity-search rebuild,
``include_inferred=False`` reads, BFS expansion) query the ``_sync`` table
directly and would see one row per slot and the blank attribute objects
``…_data`` filters out.

Schedule: the mode's schedule (``full`` → snapshot, ``incremental`` →
triggered, ``microbatch`` → continuous) capped by the configured
``graph_engine_config.lakebase.sync_table_mode`` over snapshot < triggered <
continuous. The default ``snapshot`` therefore keeps every domain on snapshots
until an admin raises it. A VIEW has no change feed and always syncs as a
snapshot.
"""

from __future__ import annotations

from typing import Any, Callable

from back.core.logging import get_logger
from back.objects.digitaltwin.build.SyncPlan import SyncPlan
from back.objects.digitaltwin.build.TwinBuildPostMaterialize import (
    TwinBuildPostMaterialize,
)

logger = get_logger(__name__)

#: Lakeflow schedule each effective materialization mode asks for.
_MODE_SCHEDULE = {"full": "snapshot", "incremental": "triggered", "microbatch": "continuous"}

#: ``data_relation_mode`` values whose ``…_data`` holds a computed copy of the triples.
_COPIED_RELATIONS = ("table", TwinBuildPostMaterialize.MATERIALIZED_VIEW)


class TwinLakebaseSync:
    """Lakeflow synced-table source, key and schedule for a build."""

    @staticmethod
    def sync_plan(pipe: Any) -> SyncPlan:
        """:meth:`sync_plan_for` from the running pipeline's relation and mode facts."""
        decision = getattr(pipe, "mode_decision", None)
        return TwinLakebaseSync.sync_plan_for(
            view_table=pipe.view_table,
            data_table=getattr(pipe, "data_table", "") or "",
            mode_effective=getattr(decision, "effective", "full"),
            configured_sync_table_mode=getattr(pipe.store, "sync_table_mode", "snapshot"),
            source_is_view=getattr(pipe, "data_relation_mode", None) not in _COPIED_RELATIONS,
        )

    @staticmethod
    def sync_plan_for(
        *,
        view_table: str,
        data_table: str,
        mode_effective: str,
        configured_sync_table_mode: Any,
        source_is_view: bool = False,
    ) -> SyncPlan:
        """Decide the source, primary key and schedule of the synced table.

        *source_is_view* means *data_table* is not a copy of the triples (a
        pass-through VIEW without ``object_hash``): Lakeflow then syncs the
        gateway VIEW, as a snapshot.
        """
        from back.core.graphdb.lakebase._companion_ddl import LAKEFLOW_SYNC_PRIMARY_KEY

        if data_table and not source_is_view:
            schedule = TwinLakebaseSync._schedule(mode_effective, configured_sync_table_mode)
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
        return synced

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
