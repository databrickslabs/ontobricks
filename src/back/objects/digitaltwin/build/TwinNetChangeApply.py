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

The feed read is ``…_data``'s, except when the slot pipeline carries the whole
graph (every mapping row-local, ``batch_mappings == 0``) and ``…_data`` is a
materialized view: ``…_data`` is then the ``…_slots`` streaming table
deduplicated, and the table's automatic change feed is readable on every
workspace, where the view's needs the external-metadata preview. The relation
read (``feed_relation``: ``data`` / ``slots``) is recorded with the version it
was read at, and a base run that read the other one is no base.

The store's inferences are purged, as the full stream (which drops the graph)
would have discarded them: Lakebase afterwards, on its own
(``purge_materialized_triples``); Neo4j, which cannot mark a triple as
inferred, first, from the list its app writers keep in ``…_inferred``
(:class:`TwinListedInferencePurge`). A store with neither path, no base run on
the same relation (for a listing store: a base run that listed too), an
unreadable (or half-read) change feed or list, or a changeset above 10 % of the
graph hands the build back to the full stream, with ``apply_fallback_reason``
set.
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
from back.objects.digitaltwin.build.TwinListedInferencePurge import TwinListedInferencePurge

logger = get_logger(__name__)

#: Above this share of the graph, rewriting the store is cheaper than the deltas.
MAX_CHANGESET_RATIO = 0.10

#: Backends whose store the app writes (the Delta engine's graph is ``…_data``).
_APP_WRITTEN_BACKENDS = ("lakebase", "neo4j")

#: ``TwinBuildRunHistory.strategy`` values whose ``…_data`` has an incremental change feed.
_FEED_STRATEGIES = ("mv", "merge")

_NEO4J_BATCH_SIZE = 500
_LAKEBASE_DELETE_BATCH_SIZE = 2000
_LAKEBASE_INSERT_BATCH_SIZE = 5000


class _ChangeFeedReadError(InfrastructureError):
    """The change feed failed while its net changes were being written (not a store write)."""


class TwinNetChangeApply:
    """Net change-feed apply of ``…_data`` to a Neo4j / app-managed Lakebase store."""

    @staticmethod
    def applies(pipe: Any) -> bool:
        """True for an incremental build of ``…_data`` on a store the app writes.

        Not for a ``full`` build, for a ``…_data`` without an incremental change
        feed (none built, a CTAS table, a pass-through view), for the Delta
        engine (its graph is ``…_data``) or for managed-synced Lakebase
        (Lakeflow syncs it).
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        decision = getattr(pipe, "mode_decision", None)
        if not isinstance(decision, ModeDecision) or decision.effective == "full":
            return False
        if TwinBuildRunHistory.strategy(pipe) not in _FEED_STRATEGIES:
            return False
        if pipe._uses_synced_pipeline():
            return False
        return GraphDBFactory._resolve_graph_backend(pipe.domain) in _APP_WRITTEN_BACKENDS

    @staticmethod
    def feed(pipe: Any) -> tuple[str, str]:
        """``(relation, fqn)`` of the change feed to read: ``("slots", …_slots)`` or ``("data", …_data)``.

        The slots when the build's slot pipeline carried the whole graph
        (``batch_mappings == 0``) into a materialized ``…_data`` (strategy
        ``mv``); a MERGE table, or an ``…_data`` with batch mappings, is read
        itself.
        """
        slots, batch = getattr(pipe, "slots_table", None), getattr(pipe, "batch_mappings", None)
        whole_graph = isinstance(batch, int) and not isinstance(batch, bool) and batch == 0
        if slots and whole_graph and TwinBuildRunHistory.strategy(pipe) == "mv":
            return "slots", slots
        return "data", pipe.data_table

    @staticmethod
    def apply(pipe: Any) -> bool:
        """Apply the net changes; ``False`` (reason recorded) means "stream the whole graph".

        Store write errors propagate and fail the build; ``apply_mode`` is
        already ``net_changes`` then, so the next build streams in full.
        """
        from back.core.graphdb.delta import ChangeFeedReader

        t0 = time.time()
        client = pipe.source_client
        pipe.feed_relation, feed = TwinNetChangeApply.feed(pipe)
        ledger = TwinNetChangeApply._ledger(pipe.store)
        pipe.inferred_listed = ledger is not None  # recorded: a base run for a listing store
        purges_itself = getattr(pipe.store, "supports_materialized_inference_purge", False) is True
        if ledger is None and not purges_itself:
            return TwinNetChangeApply._fall_back(pipe, "inferred_not_separable")
        try:
            latest = ChangeFeedReader.latest_version(client, feed)
        except InfrastructureError as exc:
            return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable", exc)
        pipe.data_table_version = latest
        pipe.data_table_id = ChangeFeedReader.table_id(client, feed)
        since = TwinNetChangeApply._base_version(pipe)
        if since is None:
            return TwinNetChangeApply._fall_back(pipe, "no_previous_version")
        if since > latest:
            return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable")
        pipe.triple_count = pipe._count_source_triples()
        if since < latest and not TwinNetChangeApply._changeset_fits(pipe, since, latest):
            return False
        pipe.apply_mode = "net_changes"  # before the writes: a failed one leaves no base
        if ledger is not None and not TwinNetChangeApply._purge_listed(
            pipe, ledger.inferred_fqn, latest
        ):
            return False
        if since < latest and not TwinNetChangeApply._apply_window(pipe, since, latest):
            return False
        TwinNetChangeApply._finish(pipe, purge=ledger is None)
        pipe._log_phase("net_apply", t0)
        logger.info(
            "[DT-BUILD %s] applied the net changes of %s v%d..v%d to %s (%d triples)",
            pipe.task_id,
            feed,
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
        """The feed version the store was brought to, or ``None`` (no usable base).

        The base must come from the same relation: dropping an MV for a MERGE
        table (or the reverse) restarts the Delta history at version 0, so its
        strategy, its feed relation (``…_slots`` / ``…_data``; a run recorded
        before the relation was, ``…_data``) and its table id when both runs
        know one, must match. ``None`` too when the store reports its graph
        missing (dropped outside a build); a store without that probe is
        trusted. A store that lists its inferences (``inferred_listed``) needs
        a base that did too: inferences written before the list existed are
        not on it, and only the full stream such a build falls back to removes
        them.
        """
        facts = TwinBuildRunHistory.previous_materialization_stats(pipe)
        version = facts.get("data_table_version")
        if isinstance(version, bool) or not isinstance(version, int):
            return None
        if facts.get("strategy") != TwinBuildRunHistory.strategy(pipe):
            return None
        if (facts.get("feed_relation") or "data") != pipe.feed_relation:
            return None
        base_id, current_id = facts.get("data_table_id") or "", pipe.data_table_id or ""
        if base_id and current_id and base_id != current_id:
            return None
        if hasattr(pipe.store, "table_exists") and not pipe.store.table_exists(pipe.graph_name):
            return None
        if (
            getattr(pipe, "inferred_listed", False) is True
            and facts.get("inferred_listed") is not True
        ):
            return None  # the base predates the list: inferences written then are not listed
        return version

    @staticmethod
    def _ledger(store: Any) -> Any:
        """The store's :class:`InferredTripleLedger` (Neo4j), ``None`` for a store without one."""
        from back.core.graphdb.delta import InferredTripleLedger

        ledger = getattr(store, "inferred_ledger", None)
        return ledger if isinstance(ledger, InferredTripleLedger) else None

    @staticmethod
    def _changeset_fits(pipe: Any, since: int, latest: int) -> bool:
        """The changeset of ``(since, latest]`` is within :data:`MAX_CHANGESET_RATIO`; else a fallback."""
        from back.core.graphdb.delta import ChangeFeedReader

        relation, feed = TwinNetChangeApply.feed(pipe)
        try:
            ratio = ChangeFeedReader.changeset_ratio(
                pipe.source_client,
                feed,
                since,
                latest,
                pipe.triple_count,
                distinct_rows=relation == "data",
            )
        except InfrastructureError as exc:
            return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable", exc)
        if ratio > MAX_CHANGESET_RATIO:
            return TwinNetChangeApply._fall_back(pipe, "changeset_too_large")
        return True

    @staticmethod
    def _purge_listed(pipe: Any, inferred: str, latest: int) -> bool:
        """:meth:`TwinListedInferencePurge.purge`; ``False`` after a fallback (``inferred_unreadable``)."""
        try:
            TwinListedInferencePurge.purge(pipe, inferred, latest)
        except TwinListedInferencePurge.Unreadable as exc:
            return TwinNetChangeApply._fall_back(pipe, "inferred_unreadable", exc)
        return True

    @staticmethod
    def _apply_window(pipe: Any, since: int, latest: int) -> bool:
        """Write the net changes of ``(since, latest]``; ``False`` after a fallback.

        A change feed that fails half-way falls back: the full stream drops the
        graph, so the deletes already written do no harm.
        """
        from back.core.graphdb.delta import ChangeFeedReader

        relation, feed = TwinNetChangeApply.feed(pipe)
        pipe.tm.update_progress(
            pipe.task_id, 50, f"Applying the net changes of {feed} since v{since}..."
        )
        changes = ChangeFeedReader.iter_net_changes(
            pipe.source_client, feed, since, latest, distinct_rows=relation == "data"
        )
        try:
            TwinNetChangeApply._write(pipe, TwinNetChangeApply._read_errors_tagged(changes))
        except _ChangeFeedReadError as exc:
            return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable", exc)
        return True

    @staticmethod
    def _read_errors_tagged(changes: Iterable[dict]) -> Iterator[dict]:
        """*changes*, with the feed's own failures raised as :class:`_ChangeFeedReadError`.

        A store write error raised while a row is processed happens in the
        consumer's frame, not in here, so it keeps its type.
        """
        try:
            yield from changes
        except InfrastructureError as exc:
            raise _ChangeFeedReadError(exc.message, detail=exc.detail) from exc

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
    def _finish(pipe: Any, *, purge: bool) -> None:
        """Optimize; with *purge*, the store purges its inferences first (and rebuilds its graph cache)."""
        if purge:
            pipe.store.purge_materialized_triples(pipe.graph_name)
        pipe.store.optimize_table(pipe.graph_name)

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
