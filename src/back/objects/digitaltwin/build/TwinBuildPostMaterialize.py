"""Build-pipeline steps that sit around the ``…_data`` relation.

Extracted from :class:`~back.objects.digitaltwin.build._build_pipeline._BuildPipeline`
so the pipeline class stays inside its size budget. Every method takes the
running pipeline (*pipe*) and reads its state by attribute; the pipeline keeps
one-line hooks that delegate here.

State is read with ``getattr`` defaults because the relation attributes
(``data_table``, ``data_relation_mode``, ``inferred_reset``) only exist once
the phase that sets them has run. ``mode_decision`` is set at the start of
``run()``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from back.core.graphdb.ModeDecision import ModeDecision
from back.core.logging import get_logger

logger = get_logger(__name__)

# What the trace reports when no decision was made (the pipeline never started).
_NO_DECISION = ModeDecision(requested="full", effective="full", reason="")


class TwinBuildPostMaterialize:
    """Source selection, inferred reset and tracing around the ``…_data`` relation."""

    @staticmethod
    def triples_source_relation(pipe: Any) -> str:
        """Return the relation the count and the stream into the store should read.

        The ``…_data`` table once it has been materialized: it is the cheap,
        already-computed copy of the mapped triples. A pass-through ``view``
        ``…_data`` re-runs the R2RML SQL on every read exactly like the
        gateway VIEW does, so reading it buys nothing and the VIEW stays the
        source. (The Lakeflow sync source is chosen separately and is still the
        VIEW.)
        """
        data_table = getattr(pipe, "data_table", "") or ""
        if data_table and getattr(pipe, "data_relation_mode", None) == "table":
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
    def decide_mode(pipe: Any) -> ModeDecision:
        """Resolve the domain's requested materialization mode to the one that runs.

        There is no serverless or row-tracking probe yet, so both readiness
        inputs are ``False``: every non-``full`` request falls back to
        ``full`` and the trace records why. Later tasks replace the inputs.
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.graphdb.MaterializationModeResolver import (
            MaterializationModeResolver,
        )

        decision = MaterializationModeResolver.resolve(
            GraphDBFactory.resolve_materialization_mode(pipe.domain, pipe.settings),
            serverless_available=False,
            sources_row_tracked=False,
            backend=GraphDBFactory._resolve_graph_backend(pipe.domain),
            previews={},
        )
        if decision.effective != decision.requested:
            logger.info(
                "[DT-BUILD %s] materialization mode %s -> %s (%s)",
                pipe.task_id,
                decision.requested,
                decision.effective,
                decision.reason,
            )
        return decision

    @staticmethod
    def trace_fields(pipe: Any) -> dict:
        """Keys the build-run trace records: relation read, reset and mode decision."""
        decision = getattr(pipe, "mode_decision", None)
        if not isinstance(decision, ModeDecision):
            decision = _NO_DECISION
        return {
            "triples_source": TwinBuildPostMaterialize.triples_source_relation(pipe),
            "inferred_reset": bool(getattr(pipe, "inferred_reset", False)),
            "materialization_mode_requested": decision.requested,
            "materialization_mode_effective": decision.effective,
            "mode_fallback_reason": decision.reason,
        }

    @staticmethod
    def persist_last_build_to_registry(pipe: Any) -> None:
        """Stamp ``last_build`` on the registry row without rewriting JSONB.

        The session/UI build path stamps domain.last_build before the build
        thread starts; the API path does not. The timestamp must reach the
        ``domain_versions.last_build`` column so ReviewService.submit() can
        unblock the Submit-for-Review gate.

        A full ``write_version`` here is unsafe: the worker's live session
        can have empty ``ontology.classes`` (request-scoped DomainSession
        after Generate/complete, or a snapshot that only carried mappings).
        Rewriting the document would wipe a previously saved ontology while
        keeping mappings — SHACL suggest then returns nothing.

        Best-effort: a failure is logged but never propagates.
        """
        try:
            from back.objects.registry.RegistryService import RegistryService
            from back.objects.session import sanitize_domain_folder

            folder = getattr(pipe.domain, "uc_domain_folder", "") or (
                sanitize_domain_folder(pipe.domain_name)
            )
            version = (
                getattr(pipe.domain_snap, "current_version", None)
                or getattr(pipe.domain, "current_version", None)
                or ""
            )
            if not folder or not version:
                logger.warning(
                    "[DT-BUILD %s] _persist_last_build_to_registry: "
                    "cannot resolve folder=%r version=%r — skipping",
                    pipe.task_id,
                    folder,
                    version,
                )
                return

            # API build path never stamps last_build before starting; do it now.
            if not getattr(pipe.domain, "last_build", None):
                pipe.domain.last_build = datetime.now(timezone.utc).isoformat()

            svc = RegistryService.from_context(pipe.domain, pipe.settings)
            ok, msg = svc._store.stamp_last_build(
                folder, str(version), pipe.domain.last_build
            )
            if ok:
                logger.info(
                    "[DT-BUILD %s] stamped last_build=%s in registry "
                    "(folder=%s version=%s)",
                    pipe.task_id,
                    pipe.domain.last_build,
                    folder,
                    version,
                )
            else:
                logger.error(
                    "[DT-BUILD %s] stamp_last_build failed when persisting "
                    "last_build: %s",
                    pipe.task_id,
                    msg,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DT-BUILD %s] could not persist last_build to registry "
                "(non-fatal — Submit for Review gate may remain blocked): %s",
                pipe.task_id,
                exc,
            )
