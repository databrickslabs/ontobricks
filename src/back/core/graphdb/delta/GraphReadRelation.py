"""Which relation Lakehouse graph reads take: the ``…_graph`` / ``…_data`` views, or the live slots."""

from __future__ import annotations

import threading
import time
from typing import Any

from back.core.graphdb.delta import _table_naming
from back.core.helpers import resolve_lakehouse_use_sea
from back.core.helpers.SQLHelpers import NON_BLANK_OBJECT_SQL, SAFE_SQL_IDENT
from back.core.logging import get_logger

logger = get_logger(__name__)


class GraphReadRelation:
    """The read relation of a Lakehouse domain: ``graph`` (the views) or ``slots_live``.

    ``slots_live`` is the Lakehouse//RT fast path of a ``microbatch`` domain:
    reads of ``…_graph`` / ``…_data`` go straight to ``…_slots`` (∪
    ``…_inferred``), which the continuous slot pipeline keeps current at its
    own cadence, instead of the materialized views that trail it by their
    chained ``TRIGGER ON UPDATE`` refreshes. It applies when the domain is on
    the Lakehouse engine with "Use Lakehouse//RT for queries" on and
    ``…_data``'s stored definition fingerprint is the one of a ``microbatch``
    view over the slots alone: one ``SHOW TBLPROPERTIES`` then says that
    ``…_data`` is a materialized view, that it reads exactly the non-blank
    slots (no batch branch) and that it is the self-refreshing variant.
    Anything else, or a probe that fails, is ``graph``. A decision is reused
    for :attr:`TTL_S` seconds per ``…_data`` table (one per domain version).
    """

    GRAPH = "graph"
    SLOTS_LIVE = "slots_live"

    #: Seconds a decision is reused before the fingerprint is read again.
    TTL_S = 30.0

    _cache: dict[str, tuple[float, str]] = {}
    _lock = threading.Lock()
    _clock = staticmethod(time.monotonic)

    @staticmethod
    def for_read(table_name: str, domain: Any, settings: Any, client: Any) -> str:
        """*table_name*, or the inline live relation when it is *domain*'s ``…_graph`` / ``…_data``.

        ``…_graph`` becomes the slots ∪ ``…_inferred``, ``…_data`` the slots
        alone (:meth:`relation_sql`), when :meth:`decide` says ``slots_live``
        on *client* (the reading store's own, so the probe runs as the reader).
        Every other name is returned unchanged.
        """
        suffixes = (_table_naming.graph_suffix(), _table_naming.data_suffix())
        if domain is None or not table_name.endswith(suffixes):
            return table_name
        if not GraphReadRelation._eligible(domain, settings):
            return table_name
        graph = _table_naming.graph_view_fqn(domain, settings)
        if table_name not in (graph, _table_naming.data_table_fqn(domain, settings)):
            return table_name
        if GraphReadRelation.decide(domain, settings, client) != GraphReadRelation.SLOTS_LIVE:
            return table_name
        slots = _table_naming.slots_table_fqn(domain, settings)
        inferred = _table_naming.inferred_table_fqn(domain, settings)
        if not all(SAFE_SQL_IDENT.fullmatch(name or "") for name in (slots, inferred)):
            return table_name
        if table_name == graph:
            return GraphReadRelation.relation_sql(slots, inferred)
        return GraphReadRelation.asserted_relation_sql(slots)

    @staticmethod
    def decide(domain: Any, settings: Any, client: Any) -> str:
        """``slots_live`` when *domain*'s reads take the live slots, else ``graph``.

        A domain that cannot run ``microbatch`` (another engine, a ``full`` or
        ``incremental`` request) is answered without any read; so is one
        without "Use Lakehouse//RT for queries". *client* reads the fingerprint;
        a failing read logs a warning (at most once per :attr:`TTL_S`) and is
        ``graph``.
        """
        if not GraphReadRelation._eligible(domain, settings):
            return GraphReadRelation.GRAPH
        data = ""
        try:
            data = _table_naming.data_table_fqn(domain, settings)
            cached = GraphReadRelation._cached(data)
            if cached is not None:
                return cached
            decision = GraphReadRelation._probe(domain, settings, data, client)
        except Exception as exc:  # noqa: BLE001 — an unknown answer keeps the views
            logger.warning(
                "Lakehouse//RT fast path of %s unavailable, reading the views: %s", data or "?", exc
            )
            decision = GraphReadRelation.GRAPH
        if data:
            with GraphReadRelation._lock:
                GraphReadRelation._cache[data] = (GraphReadRelation._clock(), decision)
        logger.debug("Lakehouse reads of %s use %s", data or "?", decision)
        return decision

    @staticmethod
    def relation_sql(slots_fqn: str, inferred_fqn: str) -> str:
        """``…_graph`` over the live slots: their distinct non-blank triples ``UNION ALL`` ``…_inferred``.

        The same multiset as the ``…_graph`` view (``…_data``'s ``GROUP BY``
        over the slots, ``UNION ALL`` the inferred). A parenthesised ``FROM``
        subquery without an alias: the read templates add theirs.
        """
        return (
            f"(SELECT DISTINCT subject, predicate, object FROM {slots_fqn} {NON_BLANK_OBJECT_SQL}"
            f" UNION ALL SELECT subject, predicate, object FROM {inferred_fqn})"
        )

    @staticmethod
    def asserted_relation_sql(slots_fqn: str) -> str:
        """``…_data`` over the live slots: their distinct non-blank triples."""
        return f"(SELECT DISTINCT subject, predicate, object FROM {slots_fqn} {NON_BLANK_OBJECT_SQL})"

    @staticmethod
    def clear_cache() -> None:
        """Forget every cached decision (in this process only)."""
        with GraphReadRelation._lock:
            GraphReadRelation._cache.clear()

    @staticmethod
    def _eligible(domain: Any, settings: Any) -> bool:
        """A Lakehouse domain that requested a continuous mode: no read needed to say no."""
        from back.core.graphdb.GraphDBFactory import (
            CONTINUOUS_MATERIALIZATION_MODES,
            GraphDBFactory,
            normalize_materialization_mode,
        )

        requested = normalize_materialization_mode(
            GraphDBFactory.resolve_materialization_mode(domain, settings)
        )
        return (
            requested in CONTINUOUS_MATERIALIZATION_MODES
            and GraphDBFactory._resolve_triple_store_backend(domain, settings) == "databricks"
        )

    @staticmethod
    def _cached(key: str) -> str | None:
        with GraphReadRelation._lock:
            entry = GraphReadRelation._cache.get(key)
        if entry is None or GraphReadRelation._clock() - entry[0] >= GraphReadRelation.TTL_S:
            return None
        return entry[1]

    @staticmethod
    def microbatch_data_sha(slots_fqn: str) -> str:
        """The fingerprint the fast path expects: the ``microbatch`` ``…_data`` view over *slots_fqn* alone.

        What ``TwinIncrementalBuild`` stores for a slots-only ``microbatch``
        definition (no batch branch; one source, so ``TRIGGER ON UPDATE``).
        """
        from back.core.graphdb.delta.MaterializedViewBuilder import MaterializedViewBuilder

        return MaterializedViewBuilder.data_mv_sha(
            slots_fqn=slots_fqn,
            batch_branch_sql=None,
            refresh_clause=MaterializedViewBuilder.microbatch_refresh_clause(1),
        )

    @staticmethod
    def _probe(domain: Any, settings: Any, data_fqn: str, client: Any) -> str:
        """The uncached decision: the RT toggle, then ``…_data``'s stored fingerprint."""
        from back.core.graphdb.delta.MaterializedViewBuilder import MaterializedViewBuilder

        if not resolve_lakehouse_use_sea(domain, settings):
            return GraphReadRelation.GRAPH
        expected = GraphReadRelation.microbatch_data_sha(_table_naming.slots_table_fqn(domain, settings))
        stored = MaterializedViewBuilder.existing_mv_sha(client, data_fqn, strict=True)
        return GraphReadRelation.SLOTS_LIVE if stored == expected else GraphReadRelation.GRAPH
