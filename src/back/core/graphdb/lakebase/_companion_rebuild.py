"""Rebuild the search-cache companions of one Lakebase graph version.

Two families are rebuilt together, in one transaction per family:

* certified (union view): ``_adj_out`` / ``_adj_in`` / ``_entity_search`` / ``_props``
* asserted (``…_asserted`` view, no derived rows): ``_entity_search_asserted``
  and ``_asserted_adj_out`` / ``_asserted_adj_in`` / ``_asserted_props``

The Original Graph reads only the asserted family, so it never walks
adjacency built from reasoning, cohort or community triples.
"""

from __future__ import annotations

from typing import Any

from back.core.graphdb.lakebase import _adjacency_ddl
from back.core.graphdb.props import forget_missing_props
from back.core.helpers import validate_table_name


def rebuild_graph_companions(store: Any, table_name: str) -> None:
    """Materialise every search-cache companion of *table_name* on *store*."""
    validate_table_name(table_name)
    union_view = store._sql_relation(table_name)
    adj_out, adj_in = store.adjacency_table_ids(table_name)
    search = store.entity_search_table_id(table_name)
    search_asserted = store.entity_search_asserted_table_id(table_name)
    asserted_spo = store.synced_table_name(table_name)
    props = store.props_table_id(table_name)
    # Asserted companions are named from the asserted relation, so these ids
    # belong to the ``…_asserted_*`` family even when *asserted_spo* is the
    # ``_sync`` fallback.
    asserted_adj_out, asserted_adj_in = store.adjacency_table_ids(asserted_spo)
    asserted_props = store.props_table_id(asserted_spo)

    with store._cursor() as cur:
        _adjacency_ddl.ensure_adjacency_tables(cur, adj_out, adj_in)
        _adjacency_ddl.ensure_adjacency_tables(cur, asserted_adj_out, asserted_adj_in)
        _adjacency_ddl.ensure_entity_search_table(cur, search)
        _adjacency_ddl.ensure_entity_search_table(cur, search_asserted)
        _adjacency_ddl.ensure_props_table(cur, props)
        _adjacency_ddl.ensure_props_table(cur, asserted_props)
    with store._txn_cursor() as (_, cur):
        _adjacency_ddl.rebuild_adjacency_data(cur, union_view, adj_out, adj_in)
        _adjacency_ddl.rebuild_adjacency_data(
            cur, asserted_spo, asserted_adj_out, asserted_adj_in
        )
        _adjacency_ddl.rebuild_entity_search_data(cur, union_view, search)
        _adjacency_ddl.rebuild_entity_search_data(cur, asserted_spo, search_asserted)
        _adjacency_ddl.rebuild_props_data(cur, union_view, props)
        _adjacency_ddl.rebuild_props_data(cur, asserted_spo, asserted_props)
    with store._cursor() as cur:
        _adjacency_ddl.analyze_adjacency_tables(cur, adj_out, adj_in)
        _adjacency_ddl.analyze_adjacency_tables(cur, asserted_adj_out, asserted_adj_in)
        _adjacency_ddl.analyze_entity_search_table(cur, search)
        _adjacency_ddl.analyze_entity_search_table(cur, search_asserted)
        _adjacency_ddl.analyze_props_table(cur, props)
        _adjacency_ddl.analyze_props_table(cur, asserted_props)
    forget_missing_props(props)
    forget_missing_props(asserted_props)
