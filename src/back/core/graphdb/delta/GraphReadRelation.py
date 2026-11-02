"""Which relation Lakehouse graph reads take: the ``…_graph`` view, or the live slots."""

from __future__ import annotations

import threading
import time
from typing import Any

from back.core.graphdb.delta import _table_naming
from back.core.graphdb.delta.DeltaBase import create_databricks_client
from back.core.helpers import resolve_lakehouse_use_sea
from back.core.helpers.SQLHelpers import NON_BLANK_OBJECT_SQL, SAFE_SQL_IDENT
from back.core.logging import get_logger

logger = get_logger(__name__)


class GraphReadRelation:
    """The read relation of a Lakehouse domain: ``graph`` (``…_graph``) or ``slots_live``.

    ``slots_live`` is the Lakehouse//RT fast path of a ``microbatch`` domain:
    reads go straight to ``…_slots`` ∪ ``…_inferred``, which the continuous
    slot pipeline keeps current at its own cadence, instead of the ``…_graph``
    materialized view, which trails it by the chained ``TRIGGER ON UPDATE``
    refreshes of ``…_data`` and ``…_graph``. It applies only when all hold —
    the domain is on the Lakehouse engine, reads through Lakehouse//RT, its
    last successful build ran ``microbatch`` with a slots table and no batch
    mappings (so ``…_data`` is exactly the non-blank slots), and ``…_slots``
    exists while ``…_data`` still is that build's materialized view. Anything
    else, or any read that fails, is ``graph``. A decision is reused for
    :attr:`TTL_S` seconds per slots table (one per domain version), so a graph
    read costs no registry or catalog round-trip.
    """

    GRAPH = "graph"
    SLOTS_LIVE = "slots_live"

    #: Seconds a decision is reused before the registry and the catalog are read again.
    TTL_S = 30.0

    _cache: dict[str, tuple[float, str]] = {}
    _lock = threading.Lock()
    _clock = staticmethod(time.monotonic)

    @staticmethod
    def decide(domain: Any, settings: Any, *, client: Any = None) -> str:
        """``slots_live`` when *domain*'s reads take the live slots, else ``graph``.

        A domain that cannot run ``microbatch`` (another engine, a ``full`` or
        ``incremental`` request) is answered without any read. *client* runs
        the catalog lookups (default: a build-warehouse client).
        """
        from back.core.graphdb.GraphDBFactory import (
            CONTINUOUS_MATERIALIZATION_MODES,
            GraphDBFactory,
            normalize_materialization_mode,
        )

        requested = normalize_materialization_mode(
            GraphDBFactory.resolve_materialization_mode(domain, settings)
        )
        if requested not in CONTINUOUS_MATERIALIZATION_MODES:
            return GraphReadRelation.GRAPH
        if GraphDBFactory._resolve_triple_store_backend(domain, settings) != "databricks":
            return GraphReadRelation.GRAPH
        key = ""
        try:
            key = _table_naming.slots_table_fqn(domain, settings)
            cached = GraphReadRelation._cached(key)
            if cached is not None:
                return cached
            decision = GraphReadRelation._decide_now(domain, settings, requested, client)
        except Exception as exc:  # noqa: BLE001 — any unknown answer keeps the graph
            logger.debug("Read relation of %s unknown, reading the graph: %s", key or "?", exc)
            decision = GraphReadRelation.GRAPH
        if key:
            with GraphReadRelation._lock:
                GraphReadRelation._cache[key] = (GraphReadRelation._clock(), decision)
        logger.debug("Lakehouse reads of %s use %s", key or "?", decision)
        return decision

    @staticmethod
    def read_relation(
        domain: Any, settings: Any, *, include_inferred: bool = True
    ) -> str:
        """The inline relation *domain*'s reads take (:meth:`relation_sql`), ``""`` for ``…_graph``."""
        slots = _table_naming.slots_table_fqn(domain, settings)
        inferred = _table_naming.inferred_table_fqn(domain, settings)
        if not all(SAFE_SQL_IDENT.fullmatch(name or "") for name in (slots, inferred)):
            return ""
        if GraphReadRelation.decide(domain, settings) != GraphReadRelation.SLOTS_LIVE:
            return ""
        return GraphReadRelation.relation_sql(slots, inferred, include_inferred=include_inferred)

    @staticmethod
    def relation_sql(slots_fqn: str, inferred_fqn: str, *, include_inferred: bool = True) -> str:
        """The parenthesised ``FROM`` subquery of the live slots (no alias: callers add theirs).

        The slots branch is the ``…_data`` view's (non-blank objects); ``UNION``
        deduplicates the triples as the view's ``GROUP BY`` does. Without
        *include_inferred* it stands for ``…_data``: the distinct non-blank slots.
        """
        slots = f"SELECT subject, predicate, object FROM {slots_fqn} {NON_BLANK_OBJECT_SQL}"
        if not include_inferred:
            return f"({slots.replace('SELECT', 'SELECT DISTINCT', 1)})"
        return f"({slots} UNION SELECT subject, predicate, object FROM {inferred_fqn})"

    @staticmethod
    def is_inline(relation: Any) -> bool:
        """True when *relation* is an inline subquery rather than a table name."""
        return isinstance(relation, str) and relation.startswith("(")

    @staticmethod
    def includes_inferred(relation: str) -> bool:
        """True when an inline *relation* reads ``…_inferred`` too (it stands for ``…_graph``)."""
        return " UNION SELECT " in relation

    @staticmethod
    def physical(relation: str, domain: Any, settings: Any) -> str:
        """The UC relation *relation* stands for: ``…_graph`` / ``…_data`` for an inline one.

        Catalog probes, ``DESCRIBE``, cache rebuilds and display names need a
        table, not a subquery; a table name is returned unchanged.
        """
        if not GraphReadRelation.is_inline(relation):
            return relation
        if GraphReadRelation.includes_inferred(relation):
            return _table_naming.graph_view_fqn(domain, settings)
        return _table_naming.data_table_fqn(domain, settings)

    @staticmethod
    def clear_cache() -> None:
        """Forget every cached decision."""
        with GraphReadRelation._lock:
            GraphReadRelation._cache.clear()

    @staticmethod
    def _cached(key: str) -> str | None:
        with GraphReadRelation._lock:
            entry = GraphReadRelation._cache.get(key)
        if entry is None or GraphReadRelation._clock() - entry[0] >= GraphReadRelation.TTL_S:
            return None
        return entry[1]

    @staticmethod
    def _decide_now(domain: Any, settings: Any, requested: str, client: Any) -> str:
        """The uncached decision: RT toggle, last build's facts, then the catalog."""
        from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory

        if not resolve_lakehouse_use_sea(domain, settings):
            return GraphReadRelation.GRAPH
        facts = TwinBuildRunHistory.last_success_facts(domain, settings) or {}
        effective = facts.get("materialization_mode_effective") or requested
        batch = facts.get("batch_mappings")
        row_local_only = isinstance(batch, int) and not isinstance(batch, bool) and batch == 0
        if effective != "microbatch" or not facts.get("slots_table") or not row_local_only:
            return GraphReadRelation.GRAPH
        if not GraphReadRelation._relations_ready(domain, settings, client):
            return GraphReadRelation.GRAPH
        return GraphReadRelation.SLOTS_LIVE

    @staticmethod
    def _relations_ready(domain: Any, settings: Any, client: Any) -> bool:
        """``…_slots`` exists and ``…_data`` is still the materialized view over it.

        A scheduled tick replaces the view by a table when the mappings changed
        since the build: the slots then no longer are ``…_data``.
        """
        from back.core.graphdb.delta import materialize

        client = client or create_databricks_client(domain, settings, for_write=True)
        if client is None:
            return False
        slots = _table_naming.slots_table_fqn(domain, settings)
        data = _table_naming.data_table_fqn(domain, settings)
        return (
            materialize.relation_kind(client, slots) != "absent"
            and materialize.relation_kind(client, data) == "mv"
        )
