"""Bring an incremental ``…_data`` materialized view up to date from a scheduled tick."""

from __future__ import annotations

from typing import Any

from back.core.errors import InfrastructureError, OntoBricksError
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory
from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild
from back.objects.digitaltwin.build.TwinSlotPipeline import TwinSlotPipeline
from back.objects.digitaltwin.build.TwinSlotRefreshPlan import (
    REASON_RECONCILE_DRIFT,
    TwinSlotRefreshPlan,
)

logger = get_logger(__name__)

#: Effective modes whose ``…_data`` is the slot pipeline's materialized view.
_VIEW_MODES = ("incremental", "microbatch")


class TwinScheduledRefresh:
    """Refresh, rather than replace, the ``…_data`` MV an incremental main build left.

    A scheduled tick (``registry/schedule/build.py``) does not recompile the
    slot pipeline or recreate the view. When ``…_data`` already is the main
    build's materialized view *and* still has the domain's current definition,
    it is brought up to date in place — ``incremental``: one triggered update
    of the slot pipeline, then ``REFRESH MATERIALIZED VIEW``; ``microbatch``:
    the refresh alone (the continuous pipeline keeps ``…_slots`` current) —
    instead of a CTAS, which would drop the view the next main build then has
    to recreate. A view built for other mappings (edited since the last main
    build) is not refreshed: the CTAS picks the change up.
    """

    @staticmethod
    def refresh(
        client: Any,
        domain: Any,
        settings: Any,
        data_fqn: str,
        mode: str,
        *,
        entity_mappings: Any = None,
        relationship_mappings: Any = None,
        spec: Any = None,
        facts: dict | None = None,
    ) -> bool:
        """True when ``…_data`` is kept (refreshed in place); ``False``: build it by CTAS.

        ``False`` for any other *mode* (``full``, a MERGE-promoted domain), when
        ``…_data`` is not a materialized view of ours, when the mappings are
        unknown or no longer give the view's definition (its stored fingerprint)
        or the slot pipeline's SQL, and when the slot pipeline is missing.
        *spec* is the tick's compiled slot spec of those mappings (``None``:
        they did not compile). A source history that cannot be read (often
        transient) keeps the view as it is for this tick — ``True``, nothing
        refreshed, nothing dropped, nothing recorded — rather than replace it
        by a CTAS every later tick would repeat.

        An ``incremental`` tick decides its slot update the way a build does
        (:class:`TwinSlotRefreshPlan`: a full refresh after a source rewrite or
        while one is owed; a drift also makes the view refresh ``FULL``) and
        writes the plan's facts into *facts* — the tick's
        ``stats.materialization`` — before the update and again once it and
        the refresh completed. Pipeline and warehouse errors raise
        :class:`InfrastructureError`; nothing has been dropped then.
        """
        from back.core.graphdb.delta import MaterializedViewBuilder, materialize

        if mode not in _VIEW_MODES:
            return False
        existing = MaterializedViewBuilder.existing_mv_sha(client, data_fqn)
        if not existing:
            return False
        if not TwinScheduledRefresh._definition_current(
            data_fqn, existing, mode, (entity_mappings, relationship_mappings, spec)
        ):
            return False
        facts = {} if facts is None else facts
        try:
            plan = TwinScheduledRefresh._plan(client, domain, settings, spec, mode)
        except InfrastructureError as exc:
            logger.warning(
                "Scheduled build: a source history is unreadable (%s); %s is left as it is "
                "this tick (nothing dropped)",
                exc,
                data_fqn,
            )
            return True
        facts.update(plan["before"])
        if spec.row_local_map_ids and not TwinScheduledRefresh._slots_current(
            domain, spec, update=mode == "incremental", full_refresh=bool(plan["reason"])
        ):
            return False
        full = plan["reason"] == REASON_RECONCILE_DRIFT
        TwinIncrementalBuild.warehouse_step(
            f"refresh {data_fqn}",
            lambda: materialize.run_sql(
                client, MaterializedViewBuilder.refresh_sql(data_fqn, full=full)
            ),
        )
        facts.update(plan["after"])
        logger.info("Scheduled build: refreshed the %s view %s in place", mode, data_fqn)
        return True

    @staticmethod
    def _plan(client: Any, domain: Any, settings: Any, spec: Any, mode: str) -> dict:
        """The slot refresh plan of an ``incremental`` tick; ``microbatch`` plans nothing.

        A source history that cannot be read raises :class:`InfrastructureError`.
        """
        if mode != "incremental":
            return {"reason": "", "before": {}, "after": {}}
        return TwinSlotRefreshPlan.plan(
            client, TwinBuildRunHistory.recent_runs_for(domain, settings), spec.source_tables
        )

    @staticmethod
    def _definition_current(data_fqn: str, existing: str, mode: str, mappings: tuple) -> bool:
        """The mappings and their *spec* still give the view's definition fingerprint.

        ``False`` (and an info log saying why) when they are unknown or did not
        compile, give no view definition, or another fingerprint than *existing*.
        """
        from back.core.graphdb.delta import MaterializedViewBuilder

        entity_mappings, relationship_mappings, spec = mappings
        if spec is None:
            logger.info(
                "Scheduled build: mappings unknown or not compiled; rebuilding %s by CTAS",
                data_fqn,
            )
            return False
        try:
            definition = TwinIncrementalBuild.data_definition_for(
                spec, entity_mappings, relationship_mappings, mode
            )
        except OntoBricksError as exc:
            logger.info(
                "Scheduled build: the mappings give no view definition (%s); "
                "rebuilding %s by CTAS",
                exc,
                data_fqn,
            )
            return False
        if MaterializedViewBuilder.data_mv_sha(**definition) != existing:
            logger.info(
                "Scheduled build: the view definition of %s no longer matches the mappings "
                "(edited since the last build); rebuilding it by CTAS, the next build "
                "recreates the view",
                data_fqn,
            )
            return False
        return True

    @staticmethod
    def _slots_current(
        domain: Any, spec: Any, *, update: bool, full_refresh: bool = False
    ) -> bool:
        """The slot pipeline runs the current SQL; with *update*, one triggered update.

        *full_refresh* makes that update a full refresh.

        ``False`` when the pipeline is missing or its stored SQL fingerprint is
        another mapping's.
        """
        from back.core.streaming import SlotPipelineRunner

        name = SlotPipelineRunner.pipeline_name(
            (getattr(domain, "info", None) or {}).get("name", ""),
            getattr(domain, "current_version", "") or "",
        )
        runner = SlotPipelineRunner()
        pipeline_id = runner.find_pipeline_id(name)
        if not pipeline_id:
            logger.warning(
                "Scheduled build: slot pipeline %s not found; rebuilding …_data by CTAS", name
            )
            return False
        if runner.stored_fingerprint(pipeline_id) != SlotPipelineRunner.fingerprint(spec.to_sql()):
            logger.info(
                "Scheduled build: slot pipeline %s runs SQL of other mappings; "
                "rebuilding …_data by CTAS",
                name,
            )
            return False
        if update:
            TwinSlotPipeline.run_update(
                runner, pipeline_id, full_refresh=full_refresh, until_running=False
            )
        return True
