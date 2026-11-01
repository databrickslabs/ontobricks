"""Apply the net changes of ``…_data`` to a Neo4j or app-managed Lakebase store.

Extracted collaborator of
:class:`~back.objects.digitaltwin.build._build_pipeline._BuildPipeline`, in the
style of :class:`TwinIncrementalBuild`: static methods that take the running
pipeline (*pipe*); the pipeline keeps a one-line hook in ``run()``.

An incremental build leaves ``…_data`` as a materialized view (or, without
serverless pipelines, a MERGE-maintained table) whose change feed holds exactly
what changed. Instead of dropping the store and streaming every triple, this
reads the feed since the version the store was last brought to (recorded by the
base build run, :meth:`TwinBuildRunHistory.previous_materialization_stats`)
and writes only the net deletes, then the net inserts:

* app-managed Lakebase: ``delete_from_sync_iter`` / ``bulk_load_into_sync`` on
  ``<graph>_sync``, the asserted table (never the ``__app`` companion);
* Neo4j: ``delete_triples`` / ``insert_triples``.

Afterwards the store's inferences are purged, as the full stream (which drops
the graph) would have discarded them. A store that cannot purge them alone,
no base run, an unreadable change feed or a changeset above 10 % of the graph
hands the build back to the full stream, with ``apply_fallback_reason`` set.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from itertools import groupby
from typing import Any

from back.core.errors import InfrastructureError
from back.core.graphdb.ModeDecision import ModeDecision
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory

logger = get_logger(__name__)

#: Above this share of the graph, rewriting the store is cheaper than the deltas.
MAX_CHANGESET_RATIO = 0.10

#: Backends whose store the app writes (the Delta engine's graph is ``…_data``).
_APP_WRITTEN_BACKENDS = ("lakebase", "neo4j")

_NEO4J_BATCH_SIZE = 500
_LAKEBASE_DELETE_BATCH_SIZE = 2000
_LAKEBASE_INSERT_BATCH_SIZE = 5000


class TwinNetChangeApply:
    """Net change-feed apply of ``…_data`` to a Neo4j / app-managed Lakebase store."""

    @staticmethod
    def applies(pipe: Any) -> bool:
        """True for an incremental build of ``…_data`` on a store the app writes.

        Not for a ``full`` build, before ``…_data`` exists, for the Delta engine
        (its graph is ``…_data``) or for managed-synced Lakebase (Lakeflow syncs it).
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        decision = getattr(pipe, "mode_decision", None)
        if not isinstance(decision, ModeDecision) or decision.effective == "full":
            return False
        if not getattr(pipe, "data_table", "") or pipe._uses_synced_pipeline():
            return False
        return GraphDBFactory._resolve_graph_backend(pipe.domain) in _APP_WRITTEN_BACKENDS

    @staticmethod
    def apply(pipe: Any) -> bool:
        """Apply the net changes; ``False`` (reason recorded) means "stream the whole graph".

        Store write errors propagate and fail the build; ``apply_mode`` is
        already ``net_changes`` then, so the next build streams in full.
        """
        from back.core.graphdb.delta import ChangeFeedReader

        t0 = time.time()
        client, data, store = pipe.source_client, pipe.data_table, pipe.store
        if getattr(store, "supports_materialized_inference_purge", False) is not True:
            return TwinNetChangeApply._fall_back(pipe, "inferred_not_separable")
        try:
            latest = ChangeFeedReader.latest_version(client, data)
        except InfrastructureError as exc:
            return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable", exc)
        pipe.data_table_version = latest
        since = TwinNetChangeApply._base_version(pipe)
        if since is None:
            return TwinNetChangeApply._fall_back(pipe, "no_previous_version")
        if since > latest:
            return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable")
        pipe.triple_count = pipe._count_source_triples()
        if since < latest:
            try:
                ratio = ChangeFeedReader.changeset_ratio(
                    client, data, since, latest, pipe.triple_count
                )
            except InfrastructureError as exc:
                return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable", exc)
            if ratio > MAX_CHANGESET_RATIO:
                return TwinNetChangeApply._fall_back(pipe, "changeset_too_large")
        pipe.apply_mode = "net_changes"
        if since < latest:
            pipe.tm.update_progress(
                pipe.task_id, 50, f"Applying the net changes of {data} since v{since}..."
            )
            changes = ChangeFeedReader.iter_net_changes(client, data, since, latest)
            TwinNetChangeApply._write(pipe, changes)
        TwinNetChangeApply._finish(pipe)
        pipe._log_phase("net_apply", t0)
        logger.info(
            "[DT-BUILD %s] applied the net changes of %s v%d..v%d to %s (%d triples)",
            pipe.task_id,
            data,
            since,
            latest,
            pipe.graph_name,
            pipe.triple_count,
        )
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _base_version(pipe: Any) -> int | None:
        """The ``…_data`` version the store was brought to, or ``None`` (no usable base).

        ``None`` too when the store reports its graph missing (dropped outside
        a build); a store without that probe is trusted.
        """
        facts = TwinBuildRunHistory.previous_materialization_stats(pipe)
        version = facts.get("data_table_version")
        if isinstance(version, bool) or not isinstance(version, int):
            return None
        if hasattr(pipe.store, "table_exists") and not pipe.store.table_exists(pipe.graph_name):
            return None
        return version

    @staticmethod
    def _write(pipe: Any, changes: Iterable[dict]) -> None:
        """Deletes, then inserts (the feed is ordered so), each as one store call."""
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        lakebase = GraphDBFactory._resolve_graph_backend(pipe.domain) == "lakebase"
        store, graph = pipe.store, pipe.graph_name
        for op, rows in groupby(changes, key=lambda row: row.get("op")):
            triples = TwinNetChangeApply._triples(rows)
            progress = TwinNetChangeApply._progress(pipe, op)
            deleting = op == "delete"
            if lakebase:
                write = store.delete_from_sync_iter if deleting else store.bulk_load_into_sync
                size = _LAKEBASE_DELETE_BATCH_SIZE if deleting else _LAKEBASE_INSERT_BATCH_SIZE
                written = write(graph, triples, batch_size=size, on_progress=progress)
            else:
                write = store.delete_triples if deleting else store.insert_triples
                written = write(
                    graph, list(triples), batch_size=_NEO4J_BATCH_SIZE, on_progress=progress
                )
            logger.info(
                "[DT-BUILD %s] net change apply: %s %d triple(s) in %s",
                pipe.task_id,
                op,
                written,
                graph,
            )

    @staticmethod
    def _finish(pipe: Any) -> None:
        """Purge the inferences, optimize, refresh the adjacency caches (as the full stream)."""
        pipe.store.purge_materialized_triples(pipe.graph_name)
        pipe.store.optimize_table(pipe.graph_name)
        pipe._rebuild_adjacency_if_supported()

    @staticmethod
    def _triples(rows: Iterable[dict]) -> Iterator[dict]:
        for row in rows:
            yield {
                "subject": row.get("subject"),
                "predicate": row.get("predicate"),
                "object": row.get("object"),
            }

    @staticmethod
    def _progress(pipe: Any, op: str) -> Callable[[int, int], None]:
        percent = 55 if op == "delete" else 70

        def _report(written: int, _total: int) -> None:
            pipe.tm.update_progress(
                pipe.task_id, percent, f"Net change apply: {written} triple {op}(s)..."
            )

        return _report

    @staticmethod
    def _fall_back(pipe: Any, reason: str, exc: Exception | None = None) -> bool:
        pipe.apply_mode, pipe.apply_fallback_reason = "full_stream", reason
        logger.info(
            "[DT-BUILD %s] net change apply not possible (%s), streaming the whole graph: %s",
            pipe.task_id,
            reason,
            exc if exc is not None else "-",
        )
        return False
