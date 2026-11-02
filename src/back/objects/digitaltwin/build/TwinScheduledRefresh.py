"""Bring an incremental ``…_data`` materialized view up to date from a scheduled tick."""

from __future__ import annotations

from typing import Any

from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild

logger = get_logger(__name__)

#: Effective modes whose ``…_data`` is the slot pipeline's materialized view.
_VIEW_MODES = ("incremental", "microbatch")


class TwinScheduledRefresh:
    """Refresh, rather than replace, the ``…_data`` MV an incremental main build left.

    A scheduled tick (``registry/schedule/build.py``) does not recompile the
    slot pipeline or recreate the view. When ``…_data`` already is the main
    build's materialized view it is brought up to date in place —
    ``incremental``: one triggered update of the slot pipeline, then
    ``REFRESH MATERIALIZED VIEW``; ``microbatch``: the refresh alone (the
    continuous pipeline keeps ``…_slots`` current) — instead of a CTAS, which
    would drop the view the next main build then has to recreate.
    """

    @staticmethod
    def refresh(
        client: Any, domain: Any, data_fqn: str, mode: str, *, slots_expected: bool
    ) -> bool:
        """True when ``…_data`` was refreshed in place; ``False``: build it by CTAS.

        ``False`` for any other *mode* (``full``, a MERGE-promoted domain), when
        ``…_data`` is not a materialized view of ours (no definition
        fingerprint), or when the slot pipeline *slots_expected* by an
        ``incremental`` domain cannot be found. Pipeline and warehouse errors
        raise :class:`InfrastructureError`; nothing has been dropped then.
        """
        from back.core.graphdb.delta import MaterializedViewBuilder, materialize

        if mode not in _VIEW_MODES:
            return False
        if not MaterializedViewBuilder.existing_mv_sha(client, data_fqn):
            return False
        if mode == "incremental" and slots_expected:
            if not TwinScheduledRefresh._update_slots(domain):
                return False
        TwinIncrementalBuild.warehouse_step(
            f"refresh {data_fqn}",
            lambda: materialize.run_sql(client, MaterializedViewBuilder.refresh_sql(data_fqn)),
        )
        logger.info("Scheduled build: refreshed the %s view %s in place", mode, data_fqn)
        return True

    @staticmethod
    def _update_slots(domain: Any) -> bool:
        """One triggered update of the domain's slot pipeline; ``False`` when it is missing."""
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
        TwinIncrementalBuild.run_update(
            runner, pipeline_id, full_refresh=False, until_running=False
        )
        return True
