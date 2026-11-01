"""Which relation Lakeflow syncs into Lakebase, on which key, and how often.

Extracted collaborator of
:class:`~back.objects.digitaltwin.build._build_pipeline._BuildPipeline`, in the
style of :class:`TwinBuildPostMaterialize`: :meth:`TwinLakebaseSync.sync_plan`
reads the running pipeline's facts; the scheduled-build mirror
(``registry/schedule/build.py``) calls the pure :meth:`TwinLakebaseSync.sync_plan_for`
with what it knows.

Source:

* ``…_slots`` (on the slot key ``map_id, source_key, predicate``) for an
  ``incremental`` / ``microbatch`` build whose slots table holds every mapped
  triple — no mapping on the batch branch (``slots_complete``);
* otherwise ``…_data`` (table or materialized view: the complete graph), keyed
  on ``LAKEFLOW_SYNC_PRIMARY_KEY``;
* the R2RML gateway VIEW when ``…_data`` is not a copy (a pass-through VIEW or
  nothing built yet).

Schedule: the mode's schedule (``full`` → snapshot, ``incremental`` →
triggered, ``microbatch`` → continuous) capped by the configured
``graph_engine_config.lakebase.sync_table_mode`` over snapshot < triggered <
continuous. The default ``snapshot`` therefore keeps every domain on snapshots
until an admin raises it. A VIEW has no change feed and always syncs as a
snapshot.
"""

from __future__ import annotations

from typing import Any

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

    #: AUTO CDC ``KEYS`` of the ``…_slots`` streaming table (one row per slot).
    SLOTS_PRIMARY_KEY: tuple[str, ...] = ("map_id", "source_key", "predicate")

    @staticmethod
    def sync_plan(pipe: Any) -> SyncPlan:
        """:meth:`sync_plan_for` from the running pipeline's relation and mode facts."""
        decision = getattr(pipe, "mode_decision", None)
        relation_mode = getattr(pipe, "data_relation_mode", None)
        return TwinLakebaseSync.sync_plan_for(
            view_table=pipe.view_table,
            data_table=getattr(pipe, "data_table", "") or "",
            slots_table=getattr(pipe, "slots_table", None),
            slots_complete=bool(getattr(pipe, "slots_complete", False)),
            mode_effective=getattr(decision, "effective", "full"),
            configured_sync_table_mode=getattr(pipe.store, "sync_table_mode", "snapshot"),
            source_is_view=relation_mode not in _COPIED_RELATIONS,
        )

    @staticmethod
    def sync_plan_for(
        *,
        view_table: str,
        data_table: str,
        slots_table: str | None,
        slots_complete: bool,
        mode_effective: str,
        configured_sync_table_mode: Any,
        source_is_view: bool = False,
    ) -> SyncPlan:
        """Decide the source, primary key, schedule and source kind of the synced table.

        *source_is_view* means *data_table* is not a copy of the triples (a
        pass-through VIEW without ``object_hash``): Lakeflow then syncs the
        gateway VIEW, as a snapshot.
        """
        from back.core.graphdb.lakebase._companion_ddl import (
            LAKEFLOW_SYNC_PRIMARY_KEY,
            SOURCE_KIND_SLOTS,
            SOURCE_KIND_TRIPLES,
        )

        schedule = TwinLakebaseSync._schedule(mode_effective, configured_sync_table_mode)
        if mode_effective != "full" and slots_table and slots_complete:
            return SyncPlan(
                slots_table, TwinLakebaseSync.SLOTS_PRIMARY_KEY, schedule, SOURCE_KIND_SLOTS
            )
        if data_table and not source_is_view:
            return SyncPlan(data_table, LAKEFLOW_SYNC_PRIMARY_KEY, schedule, SOURCE_KIND_TRIPLES)
        return SyncPlan(view_table, LAKEFLOW_SYNC_PRIMARY_KEY, "snapshot", SOURCE_KIND_TRIPLES)

    @staticmethod
    def drop_union_view_if_drifted(
        store: Any, mgr: Any, synced_uc: str, graph_name: str, plan: SyncPlan
    ) -> bool:
        """Drop the Postgres union view when ``mgr.ensure`` is about to recreate *synced_uc*.

        Recreating a synced table purges its Postgres ``_sync`` table, which the
        union view depends on; the build's ``ensure_synced_union_view`` step
        re-creates the view afterwards. Best-effort: a failure is logged and
        ``ensure`` runs regardless. Returns ``True`` when the view was dropped.
        """
        from back.core.graphdb.lakebase import SyncedTableRegistration

        try:
            drift = SyncedTableRegistration.drift(
                mgr.get(synced_uc), plan.source_fqn, plan.sync_mode
            )
            if drift:
                store.drop_union_view(graph_name)
        except Exception as exc:  # noqa: BLE001 — best-effort; ensure() still runs
            logger.warning(
                "Could not drop the union view of %s before recreating %s: %s",
                graph_name,
                synced_uc,
                exc,
            )
            return False
        if drift:
            logger.info(
                "Dropped the union view of %s: synced table %s will be recreated (%s)",
                graph_name,
                synced_uc,
                drift,
            )
        return bool(drift)

    @staticmethod
    def _schedule(mode_effective: str, configured: Any) -> str:
        """The mode's schedule, capped by *configured* (unreadable → ``snapshot``)."""
        from back.core.graphdb.lakebase.LakebaseBase import SYNC_TABLE_MODES

        cap = configured.strip().lower() if isinstance(configured, str) else ""
        if cap not in SYNC_TABLE_MODES:
            cap = "snapshot"
        wanted = _MODE_SCHEDULE.get(mode_effective, "snapshot")
        return min(wanted, cap, key=SYNC_TABLE_MODES.index)
