"""Bring an incremental ``…_data`` materialized view up to date from a scheduled tick."""

from __future__ import annotations

from typing import Any

from back.core.errors import OntoBricksError
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild

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
        base_uri: str = "",
    ) -> bool:
        """True when ``…_data`` was refreshed in place; ``False``: build it by CTAS.

        ``False`` for any other *mode* (``full``, a MERGE-promoted domain), when
        ``…_data`` is not a materialized view of ours, when the mappings are
        unknown or no longer give the view's definition (its stored fingerprint)
        or the slot pipeline's SQL, and when the slot pipeline is missing.
        Pipeline and warehouse errors raise :class:`InfrastructureError`;
        nothing has been dropped then.
        """
        from back.core.graphdb.delta import MaterializedViewBuilder, materialize

        if mode not in _VIEW_MODES:
            return False
        existing = MaterializedViewBuilder.existing_mv_sha(client, data_fqn)
        if not existing:
            return False
        spec = TwinScheduledRefresh._current_spec(
            domain, settings, data_fqn, existing, mode,
            (entity_mappings, relationship_mappings, base_uri),
        )
        if spec is None:
            return False
        if spec.row_local_map_ids:
            if not TwinScheduledRefresh._slots_current(domain, spec, update=mode == "incremental"):
                return False
        TwinIncrementalBuild.warehouse_step(
            f"refresh {data_fqn}",
            lambda: materialize.run_sql(client, MaterializedViewBuilder.refresh_sql(data_fqn)),
        )
        logger.info("Scheduled build: refreshed the %s view %s in place", mode, data_fqn)
        return True

    @staticmethod
    def _current_spec(
        domain: Any, settings: Any, data_fqn: str, existing: str, mode: str, mappings: tuple
    ) -> Any:
        """The slot spec of the current mappings when they still give the view's definition.

        ``None`` (and an info log saying why) when the mappings are unknown, do
        not compile, or give another definition fingerprint than *existing*.
        """
        from back.core.graphdb.delta import MaterializedViewBuilder

        entity_mappings, relationship_mappings, base_uri = mappings
        if entity_mappings is None and relationship_mappings is None:
            logger.info("Scheduled build: mappings unknown; rebuilding %s by CTAS", data_fqn)
            return None
        try:
            spec = TwinIncrementalBuild.compile_for(
                domain, settings, entity_mappings, relationship_mappings, base_uri=base_uri
            )
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
            return None
        if MaterializedViewBuilder.data_mv_sha(**definition) != existing:
            logger.info(
                "Scheduled build: the view definition of %s no longer matches the mappings "
                "(edited since the last build); rebuilding it by CTAS, the next build "
                "recreates the view",
                data_fqn,
            )
            return None
        return spec

    @staticmethod
    def _slots_current(domain: Any, spec: Any, *, update: bool) -> bool:
        """The slot pipeline runs the current SQL; with *update*, one triggered update.

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
            TwinIncrementalBuild.run_update(
                runner, pipeline_id, full_refresh=False, until_running=False
            )
        return True
