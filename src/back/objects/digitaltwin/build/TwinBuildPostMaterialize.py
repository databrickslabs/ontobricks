"""Build-pipeline steps that sit around the ``…_data`` relation.

Extracted from :class:`~back.objects.digitaltwin.build._build_pipeline._BuildPipeline`
so the pipeline class stays inside its size budget. Every method takes the
running pipeline (*pipe*) and reads its state by attribute; the pipeline keeps
one-line hooks that delegate here.

State is read with ``getattr`` defaults because the relation attributes
(``data_table``, ``data_relation_mode``, ``inferred_reset``, and the
incremental facts ``slots_table`` / ``slot_pipeline_id`` /
``refresh_technique``) only exist once the phase that sets them has run.
``mode_decision`` is set at the start of ``run()`` and refined by
:class:`TwinIncrementalBuild` once the mappings are compiled.
"""

from __future__ import annotations

from typing import Any

from back.core.graphdb.ModeDecision import ModeDecision
from back.core.logging import get_logger

logger = get_logger(__name__)

# What the trace reports when no decision was made (the pipeline never started).
_NO_DECISION = ModeDecision(requested="full", effective="full", reason="")


class TwinBuildPostMaterialize:
    """Source selection, inferred reset and tracing around the ``…_data`` relation."""

    #: ``data_relation_mode`` of an incremental build's ``…_data`` relation.
    MATERIALIZED_VIEW = "materialized_view"

    @staticmethod
    def triples_source_relation(pipe: Any) -> str:
        """Return the relation the count and the stream into the store should read.

        The ``…_data`` relation once it holds a computed copy of the mapped
        triples: a ``table`` (full build) or a ``materialized_view``
        (incremental build). A pass-through ``view`` ``…_data`` re-runs the
        R2RML SQL on every read exactly like the gateway VIEW does, so reading
        it buys nothing and the VIEW stays the source. (The Lakeflow sync
        source is chosen by :class:`TwinLakebaseSync`.)
        """
        data_table = getattr(pipe, "data_table", "") or ""
        mode = getattr(pipe, "data_relation_mode", None)
        if data_table and mode in ("table", TwinBuildPostMaterialize.MATERIALIZED_VIEW):
            return data_table
        return pipe.view_table

    @staticmethod
    def reset_inferred_companion(pipe: Any) -> bool:
        """Empty ``…_inferred`` and refresh the ``…_graph`` union view.

        Applies to the ``databricks`` backend only. Lakebase and Neo4j drop and
        recreate their graph table in the full rebuild, which discards
        inferences already. Here the companion is a separate Delta table that
        survives a rebuild, so reasoning and cohort triples written for the
        previous data would otherwise be served next to the new data.

        A failure fails the build: stale inferences are a correctness bug, not
        something to warn about. Returns ``False`` after calling
        ``tm.fail_task``.
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.graphdb.delta import _table_naming, materialize

        backend = GraphDBFactory._resolve_triple_store_backend(
            pipe.domain, pipe.settings
        )
        if backend != "databricks":
            logger.debug(
                "[DT-BUILD %s] backend %s rebuilds its graph table; no inferred "
                "companion to reset",
                pipe.task_id,
                backend,
            )
            return True

        inferred = _table_naming.inferred_table_fqn(pipe.domain, pipe.settings)
        graph = _table_naming.graph_view_fqn(pipe.domain, pipe.settings)
        data = getattr(pipe, "data_table", "")
        if not (inferred and graph and data):
            pipe.tm.fail_task(
                pipe.task_id, "Could not resolve the inferred-triples table name"
            )
            return False

        try:
            materialize.ensure_inferred_table(pipe.source_client, inferred)
            # Strict on purpose: ``materialize.truncate_table`` swallows errors.
            materialize.run_sql(
                pipe.source_client, materialize.build_truncate_sql(inferred)
            )
            materialize.ensure_graph_view(pipe.source_client, graph, data, inferred)
        except Exception as exc:  # noqa: BLE001
            msg = f"Could not reset the inferred triples table {inferred}: {exc}"
            logger.error("[DT-BUILD %s] %s", pipe.task_id, msg)
            pipe.tm.fail_task(pipe.task_id, msg)
            return False

        pipe.inferred_reset = True
        logger.info(
            "[DT-BUILD %s] reset %s and refreshed %s", pipe.task_id, inferred, graph
        )
        return True

    @staticmethod
    def serverless_available(settings: Any) -> bool:
        """``settings.serverless_pipelines_enabled``, ``True`` when absent (no probe yet)."""
        return bool(getattr(settings, "serverless_pipelines_enabled", True))

    @staticmethod
    def decide_mode(pipe: Any, *, source_tables: list | None = None) -> ModeDecision:
        """:meth:`decide_mode_for` the running pipeline's domain, settings and warehouse."""
        return TwinBuildPostMaterialize.decide_mode_for(
            pipe.domain,
            pipe.settings,
            pipe.source_client,
            source_tables=source_tables,
            task_id=pipe.task_id,
        )

    @staticmethod
    def decide_mode_for(
        domain: Any,
        settings: Any,
        client: Any,
        *,
        source_tables: list | None,
        task_id: str = "",
    ) -> ModeDecision:
        """Resolve the domain's requested materialization mode to the one that runs.

        Inputs: ``serverless_available`` from the settings flag, the domain's
        graph backend, and ``sources_row_tracked``, which needs the source
        tables of the compiled slot pipeline (*source_tables*):

        * ``None`` (the call at the start of ``run()``, before the mappings are
          parsed; a scheduled tick whose mappings did not compile) — the
          sources count as not tracked and nothing is probed;
        * a list — ``SHOW TBLPROPERTIES`` probes each table on *client*, but
          only when serverless and the backend would still allow a faster
          mode. No sources (every mapping is batch) counts as tracked.

        The main build and the scheduled mirror both call this, so a scheduled
        tick lands on the main build's mode. A downgrade is logged at info
        (``[DT-BUILD <task_id>]``) when *task_id* is given and the sources were
        known, else at debug. A failed probe raises :class:`InfrastructureError`
        (the incremental phase turns it into a fallback).
        """
        from back.core.streaming import SourceReadiness

        tracked = False
        if source_tables is not None:
            gate = TwinBuildPostMaterialize.gate_decision(domain, settings)
            if gate.effective != "full":
                readiness = SourceReadiness.check(client, source_tables)
                tracked = all(readiness.values())
        decision = TwinBuildPostMaterialize.resolve_for(
            domain, settings, sources_row_tracked=tracked
        )
        if decision.effective != decision.requested:
            TwinBuildPostMaterialize._log_downgrade(
                decision, task_id, informative=source_tables is not None
            )
        return decision

    @staticmethod
    def gate_decision(domain: Any, settings: Any) -> ModeDecision:
        """The mode the domain could at best run: its sources counted as row-tracked.

        Serverless and the backend still gate it. :meth:`decide_mode_for` probes
        only when this is not ``full``; the Lakebase sync schedule starts from it,
        so a probe that fails keeps the schedule the sources could support.
        """
        return TwinBuildPostMaterialize.resolve_for(domain, settings, sources_row_tracked=True)

    @staticmethod
    def resolve_for(domain: Any, settings: Any, *, sources_row_tracked: bool) -> ModeDecision:
        """The resolver's decision for the domain's requested mode and *sources_row_tracked*."""
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.graphdb.MaterializationModeResolver import (
            MaterializationModeResolver,
        )

        return MaterializationModeResolver.resolve(
            GraphDBFactory.resolve_materialization_mode(domain, settings),
            sources_row_tracked=sources_row_tracked,
            **TwinBuildPostMaterialize._resolver_inputs(domain, settings),
        )

    @staticmethod
    def _log_downgrade(decision: ModeDecision, task_id: str, *, informative: bool) -> None:
        """Log a mode downgrade: info for a build with known sources, debug otherwise."""
        reason = (decision.requested, decision.effective, decision.reason)
        if task_id and informative:
            logger.info("[DT-BUILD %s] materialization mode %s -> %s (%s)", task_id, *reason)
        elif task_id:
            logger.debug("[DT-BUILD %s] materialization mode %s -> %s (%s)", task_id, *reason)
        else:
            logger.debug("materialization mode %s -> %s (%s)", *reason)

    @staticmethod
    def _resolver_inputs(domain: Any, settings: Any) -> dict:
        """``MaterializationModeResolver.resolve`` inputs read from the domain and settings."""
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        return {
            "serverless_available": TwinBuildPostMaterialize.serverless_available(settings),
            "backend": GraphDBFactory._resolve_graph_backend(domain),
            "previews": {},
        }

    @staticmethod
    def trace_fields(pipe: Any) -> dict:
        """Keys the build-run trace records: relation read, reset, mode, incremental and sync facts.

        ``triples_source`` is the relation the count and the stream read;
        ``sync_source`` / ``sync_mode_effective`` are what the Lakebase synced
        table was registered on (``TwinLakebaseSync.register``). The incremental
        and sync facts are ``""`` when they do not apply (and for any value that
        is not a string, e.g. a test double's auto-attribute).
        """
        decision = getattr(pipe, "mode_decision", None)
        if not isinstance(decision, ModeDecision):
            decision = _NO_DECISION
        text = TwinBuildPostMaterialize.text_attr
        return {
            "triples_source": TwinBuildPostMaterialize.triples_source_relation(pipe),
            "inferred_reset": bool(getattr(pipe, "inferred_reset", False)),
            "materialization_mode_requested": decision.requested,
            "materialization_mode_effective": decision.effective,
            "mode_fallback_reason": decision.reason,
            "refresh_technique": text(pipe, "refresh_technique"),
            "slot_pipeline_id": text(pipe, "slot_pipeline_id"),
            "slots_table": text(pipe, "slots_table"),
            "sync_source": text(pipe, "sync_source"),
            "sync_mode_effective": text(pipe, "sync_mode_effective"),
        }

    @staticmethod
    def text_attr(pipe: Any, name: str) -> str:
        """``pipe.<name>`` when it is a string, else ``""`` (e.g. a test double's auto-attribute)."""
        value = getattr(pipe, name, "")
        return value if isinstance(value, str) else ""
