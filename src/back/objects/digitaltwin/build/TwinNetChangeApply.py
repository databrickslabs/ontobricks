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

The store's inferences are purged, as the full stream (which drops the graph)
would have discarded them: Lakebase afterwards, on its own
(``purge_materialized_triples``); Neo4j, which cannot mark a triple as
inferred, first, from the list its app writers keep in ``…_inferred``
(:class:`InferredTripleLedger`) — every listed triple that ``…_data`` does not
assert (``…_inferred EXCEPT …_data``) is deleted, the asserted triples those
deletes unset are written again, and the list is emptied. A store with neither
path, no base run on the same relation, an unreadable (or half-read) change
feed or list, or a changeset above 10 % of the graph hands the build back to
the full stream, with ``apply_fallback_reason`` set.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from itertools import groupby, islice
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

#: ``TwinBuildRunHistory.strategy`` values whose ``…_data`` has an incremental change feed.
_FEED_STRATEGIES = ("mv", "merge")

_NEO4J_BATCH_SIZE = 500
_LAKEBASE_DELETE_BATCH_SIZE = 2000
_LAKEBASE_INSERT_BATCH_SIZE = 5000


class _ChangeFeedReadError(InfrastructureError):
    """A warehouse read (the change feed, the listed inferences) failed while its rows were written.

    Not a store write: those keep their own type and fail the build.
    """


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
    def apply(pipe: Any) -> bool:
        """Apply the net changes; ``False`` (reason recorded) means "stream the whole graph".

        Store write errors propagate and fail the build; ``apply_mode`` is
        already ``net_changes`` then, so the next build streams in full.
        """
        from back.core.graphdb.delta import ChangeFeedReader

        t0 = time.time()
        client, data = pipe.source_client, pipe.data_table
        ledger = TwinNetChangeApply._ledger(pipe.store)
        purges_itself = getattr(pipe.store, "supports_materialized_inference_purge", False) is True
        if ledger is None and not purges_itself:
            return TwinNetChangeApply._fall_back(pipe, "inferred_not_separable")
        try:
            latest = ChangeFeedReader.latest_version(client, data)
        except InfrastructureError as exc:
            return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable", exc)
        pipe.data_table_version = latest
        pipe.data_table_id = ChangeFeedReader.table_id(client, data)
        since = TwinNetChangeApply._base_version(pipe)
        if since is None:
            return TwinNetChangeApply._fall_back(pipe, "no_previous_version")
        if since > latest:
            return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable")
        pipe.triple_count = pipe._count_source_triples()
        if since < latest and not TwinNetChangeApply._changeset_fits(pipe, since, latest):
            return False
        pipe.apply_mode = "net_changes"  # before the writes: a failed one leaves no base
        if ledger is not None and not TwinNetChangeApply._purge_listed(pipe, ledger.inferred_fqn):
            return False
        if since < latest and not TwinNetChangeApply._apply_window(pipe, since, latest):
            return False
        TwinNetChangeApply._finish(pipe, purge=ledger is None)
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

        The base must come from the same relation: dropping an MV for a MERGE
        table (or the reverse) restarts the Delta history at version 0, so its
        strategy, and its table id when both runs know one, must match. ``None``
        too when the store reports its graph missing (dropped outside a build);
        a store without that probe is trusted.
        """
        facts = TwinBuildRunHistory.previous_materialization_stats(pipe)
        version = facts.get("data_table_version")
        if isinstance(version, bool) or not isinstance(version, int):
            return None
        if facts.get("strategy") != TwinBuildRunHistory.strategy(pipe):
            return None
        base_id, current_id = facts.get("data_table_id") or "", pipe.data_table_id or ""
        if base_id and current_id and base_id != current_id:
            return None
        if hasattr(pipe.store, "table_exists") and not pipe.store.table_exists(pipe.graph_name):
            return None
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

        client, data = pipe.source_client, pipe.data_table
        try:
            ratio = ChangeFeedReader.changeset_ratio(client, data, since, latest, pipe.triple_count)
        except InfrastructureError as exc:
            return TwinNetChangeApply._fall_back(pipe, "cdf_unavailable", exc)
        if ratio > MAX_CHANGESET_RATIO:
            return TwinNetChangeApply._fall_back(pipe, "changeset_too_large")
        return True

    @staticmethod
    def _purge_listed(pipe: Any, inferred: str) -> bool:
        """Delete the listed inferences ``…_data`` does not assert; ``False`` after a fallback.

        ``…_inferred EXCEPT …_data`` is streamed into ``store.delete_triples``
        in batches. Neo4j keeps one value per node property, label set and
        name, so deleting a listed literal also unsets an asserted value of the
        same key: the ``…_data`` triples on the subject and predicate of a
        deleted one are written again. The list is then emptied. A list that
        cannot be read falls back to the full stream (``inferred_unreadable``),
        which drops the graph, so the deletes already written do no harm.
        """
        from back.core.graphdb.delta import materialize

        client, store = pipe.source_client, pipe.store
        listed = TwinNetChangeApply.listed_inferences_sql(inferred, pipe.data_table)
        deleted = restored = 0
        try:
            exists = TwinNetChangeApply._listed(client, inferred)
            if exists:
                deleted = TwinNetChangeApply._in_batches(
                    pipe, TwinNetChangeApply._rows(client, listed), store.delete_triples
                )
            if deleted:
                shadowed = TwinNetChangeApply.shadowed_triples_sql(listed, pipe.data_table)
                restored = TwinNetChangeApply._in_batches(
                    pipe, TwinNetChangeApply._rows(client, shadowed), store.insert_triples
                )
        except _ChangeFeedReadError as exc:
            return TwinNetChangeApply._fall_back(pipe, "inferred_unreadable", exc)
        if exists:
            materialize.truncate_table(client, inferred)
        logger.info(
            "[DT-BUILD %s] purged %d listed inference(s) from %s, %d asserted triple(s) rewritten",
            pipe.task_id,
            deleted,
            pipe.graph_name,
            restored,
        )
        return True

    @staticmethod
    def listed_inferences_sql(inferred: str, data: str) -> str:
        """The listed inferences ``…_data`` does not assert: ``<inferred> EXCEPT <data>``."""
        from back.core.helpers import validate_table_name

        validate_table_name(inferred)
        validate_table_name(data)
        return (
            f"SELECT subject, predicate, object FROM {inferred}\n"
            "EXCEPT\n"
            f"SELECT subject, predicate, object FROM {data}"
        )

    @staticmethod
    def shadowed_triples_sql(listed_sql: str, data: str) -> str:
        """The ``…_data`` triples on the subject and predicate of a listed inference (*listed_sql*)."""
        return (
            f"SELECT d.subject, d.predicate, d.object FROM {data} d\n"
            f"LEFT SEMI JOIN ({listed_sql}) x\n"
            "ON d.subject = x.subject AND d.predicate = x.predicate"
        )

    @staticmethod
    def _listed(client: Any, inferred: str) -> bool:
        """``…_inferred`` exists (a domain that never reasoned has none); a failed probe is a read error."""
        from back.core.graphdb.delta import materialize

        try:
            return materialize.relation_exists(client, inferred, strict=True)
        except InfrastructureError as exc:
            raise _ChangeFeedReadError(exc.message, detail=exc.detail) from exc

    @staticmethod
    def _rows(client: Any, sql: str) -> Iterator[dict]:
        """``client.iter_rows(sql)``, its failures raised as :class:`_ChangeFeedReadError`."""
        try:
            yield from client.iter_rows(sql, batch_size=_NEO4J_BATCH_SIZE)
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise _ChangeFeedReadError(f"Could not read {sql!r}: {exc}", detail=str(exc)) from exc

    @staticmethod
    def _in_batches(pipe: Any, rows: Iterable[dict], write: Callable[..., Any]) -> int:
        """``write(graph, batch, batch_size=…)`` per batch of *rows*; the number of rows written."""
        triples, total = TwinNetChangeApply._triples(rows), 0
        while batch := list(islice(triples, _NEO4J_BATCH_SIZE)):
            write(pipe.graph_name, batch, batch_size=_NEO4J_BATCH_SIZE)
            total += len(batch)
        return total

    @staticmethod
    def _apply_window(pipe: Any, since: int, latest: int) -> bool:
        """Write the net changes of ``(since, latest]``; ``False`` after a fallback.

        A change feed that fails half-way falls back: the full stream drops the
        graph, so the deletes already written do no harm.
        """
        from back.core.graphdb.delta import ChangeFeedReader

        client, data = pipe.source_client, pipe.data_table
        pipe.tm.update_progress(
            pipe.task_id, 50, f"Applying the net changes of {data} since v{since}..."
        )
        changes = ChangeFeedReader.iter_net_changes(client, data, since, latest)
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
