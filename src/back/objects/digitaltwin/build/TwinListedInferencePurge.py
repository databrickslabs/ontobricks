"""Purge from a Neo4j store the inferred triples its writers listed in ``…_inferred``."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from itertools import islice
from typing import Any

from back.core.errors import InfrastructureError
from back.core.logging import get_logger

logger = get_logger(__name__)

_BATCH_SIZE = 500


class TwinListedInferencePurge:
    """The net-change apply's purge for a store that cannot mark a triple as inferred (Neo4j).

    Its app writers list every inferred batch in the Delta ``…_inferred``
    table first (``InferredTripleLedger``). Before the net changes, every
    listed triple that ``…_data`` does not assert is deleted from the store,
    the asserted triples those deletes unset are written again, and the rows
    read are removed from the list. Both relations are read at pinned versions
    (``…_data`` at the version the net changes go up to), so the reads agree
    with each other and a reasoning run listing new triples meanwhile keeps
    its rows.
    """

    class Unreadable(InfrastructureError):
        """The list could not be read (the caller falls back to the full stream); not a store write."""

    @staticmethod
    def purge(pipe: Any, inferred: str, data_version: int) -> None:
        """Purge the listed inferences from ``pipe.store``; :class:`Unreadable` when the list is.

        ``<inferred> EXCEPT <data>`` is streamed into ``store.delete_triples``
        in batches of 500. Neo4j keeps one value per node property, label set
        and name, so deleting a listed literal also unsets an asserted value of
        the same key: the ``…_data`` triples on the subject and predicate of a
        deleted one are written again. Store write errors propagate (they fail
        the build); a list that cannot be read raises :class:`Unreadable`
        (deletes already written do no harm: the full stream drops the graph).
        """
        client, store, data = pipe.source_client, pipe.store, pipe.data_table
        version = TwinListedInferencePurge._list_version(client, inferred)
        if version is None:
            return
        listed = TwinListedInferencePurge.listed_sql(inferred, version, data, data_version)
        rows = TwinListedInferencePurge._rows(client, listed)
        deleted = TwinListedInferencePurge._in_batches(pipe, rows, store.delete_triples)
        restored = 0
        if deleted:
            shadowed = TwinListedInferencePurge.shadowed_sql(listed, data, data_version)
            rows = TwinListedInferencePurge._rows(client, shadowed)
            restored = TwinListedInferencePurge._in_batches(pipe, rows, store.insert_triples)
        TwinListedInferencePurge._forget(client, inferred, version)
        logger.info(
            "[DT-BUILD %s] purged %d listed inference(s) from %s, %d asserted triple(s) rewritten",
            pipe.task_id,
            deleted,
            pipe.graph_name,
            restored,
        )

    @staticmethod
    def listed_sql(inferred: str, version: int, data: str, data_version: int) -> str:
        """The listed inferences ``…_data`` does not assert, both at their pinned versions."""
        from back.core.helpers import validate_table_name

        validate_table_name(inferred)
        validate_table_name(data)
        return (
            f"SELECT subject, predicate, object FROM {inferred} VERSION AS OF {int(version)}\n"
            "EXCEPT\n"
            f"SELECT subject, predicate, object FROM {data} VERSION AS OF {int(data_version)}"
        )

    @staticmethod
    def shadowed_sql(listed_sql: str, data: str, data_version: int) -> str:
        """The ``…_data`` triples on the subject and predicate of a listed inference (*listed_sql*)."""
        return (
            f"SELECT d.subject, d.predicate, d.object FROM {data} VERSION AS OF {int(data_version)} d\n"
            f"LEFT SEMI JOIN ({listed_sql}) x\n"
            "ON d.subject = x.subject AND d.predicate = x.predicate"
        )

    @staticmethod
    def forget_sql(inferred: str, version: int) -> str:
        """Remove from the list the triples it held at *version* (the ones just purged)."""
        from back.core.helpers import validate_table_name

        validate_table_name(inferred)
        return (
            f"DELETE FROM {inferred} AS t WHERE EXISTS (\n"
            f"  SELECT 1 FROM {inferred} VERSION AS OF {int(version)} AS s\n"
            "  WHERE s.subject <=> t.subject AND s.predicate <=> t.predicate"
            " AND s.object <=> t.object)"
        )

    @staticmethod
    def _list_version(client: Any, inferred: str) -> int | None:
        """The list's current version; ``None`` when there is no list (the domain never reasoned)."""
        from back.core.graphdb.delta import ChangeFeedReader, materialize

        try:
            if not materialize.relation_exists(client, inferred, strict=True):
                return None
            return ChangeFeedReader.latest_version(client, inferred)
        except InfrastructureError as exc:
            raise TwinListedInferencePurge.Unreadable(exc.message, detail=exc.detail) from exc

    @staticmethod
    def _forget(client: Any, inferred: str, version: int) -> None:
        """Best-effort: a list not emptied only re-deletes absent triples next time."""
        from back.core.graphdb.delta import materialize

        try:
            materialize.run_sql(client, TwinListedInferencePurge.forget_sql(inferred, version))
        except Exception as exc:  # noqa: BLE001 — over-listing is harmless, see above
            logger.warning("Could not remove the purged inferences from %s: %s", inferred, exc)

    @staticmethod
    def _rows(client: Any, sql: str) -> Iterator[dict]:
        """``client.iter_rows(sql)``, its failures raised as :class:`Unreadable`."""
        try:
            yield from client.iter_rows(sql, batch_size=_BATCH_SIZE)
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise TwinListedInferencePurge.Unreadable(
                f"Could not read the listed inferences: {exc}", detail=str(exc)
            ) from exc

    @staticmethod
    def _in_batches(pipe: Any, rows: Iterable[dict], write: Callable[..., Any]) -> int:
        """``write(graph, batch, batch_size=…)`` per batch of *rows*; the number of rows written."""
        triples = (
            {"subject": r.get("subject"), "predicate": r.get("predicate"), "object": r.get("object")}
            for r in rows
        )
        total = 0
        while batch := list(islice(triples, _BATCH_SIZE)):
            write(pipe.graph_name, batch, batch_size=_BATCH_SIZE)
            total += len(batch)
        return total
