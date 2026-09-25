"""SQL shape tests for shared adjacency helpers (no warehouse)."""

from __future__ import annotations

import pytest

from back.core.graphdb.adjacency import (
    expand_and_fetch_sql,
    expand_entity_neighbors_sql,
    seeded_bfs_sql,
    typed_in_select,
    typed_out_select,
)
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graphdb.props import props_page_sql


def test_typed_out_select_excludes_type_and_label():
    sql = typed_out_select("g._graph")
    assert "g._graph" in sql
    assert RDF_TYPE in sql
    assert RDFS_LABEL in sql
    assert "t.object LIKE 'http%'" in sql
    assert "typed.subject = t.object" in sql
    assert f"t.predicate != '{RDF_TYPE}'" in sql
    assert f"t.predicate != '{RDFS_LABEL}'" in sql
    assert "AS src" in sql
    assert "AS dst" in sql


def test_typed_in_select_reverses_endpoints():
    sql = typed_in_select("g._graph")
    assert "t.subject AS src" in sql or "AS src" in sql
    assert "typed.subject = t.subject" in sql
    assert f"t.predicate != '{RDF_TYPE}'" in sql
    assert f"t.predicate != '{RDFS_LABEL}'" in sql


def test_neighbors_sql_unions_out_and_in():
    sql = expand_entity_neighbors_sql(
        "t_adj_out", "t_adj_in", ["http://ex/a"], lambda s: s.replace("'", "''")
    )
    assert "FROM t_adj_out" in sql
    assert "FROM t_adj_in" in sql
    assert "http://ex/a" in sql
    assert "UNION ALL" in sql
    assert "SELECT DISTINCT entity FROM (" in sql
    assert " UNION " not in sql.replace("UNION ALL", "")


def test_neighbors_sql_rejects_empty_uris():
    with pytest.raises(ValueError, match="At least one URI is required"):
        expand_entity_neighbors_sql(
            "t_adj_out", "t_adj_in", [], lambda s: s.replace("'", "''")
        )


def test_spark_expansion_uses_left_anti_join():
    sql = expand_and_fetch_sql(
        flavor="spark",
        adj_out="o",
        adj_in="i",
        spo="g",
        selected_uris=["http://ex/a"],
        depth=2,
        max_entities=10,
        max_triples=100,
        escape=lambda s: s.replace("'", "''"),
    )
    assert "LEFT ANTI JOIN" in sql
    assert "FROM o " in sql or "FROM o\n" in sql or "FROM o t" in sql
    assert "FROM i " in sql or "FROM i t" in sql
    assert "FROM g " in sql or "FROM g triples" in sql
    assert "level_2" in sql
    assert "LIMIT 11" in sql
    assert "LIMIT 101" in sql


def test_postgres_expansion_uses_not_exists():
    sql = expand_and_fetch_sql(
        flavor="postgres",
        adj_out="o",
        adj_in="i",
        spo="g",
        selected_uris=["http://ex/O'Brien"],
        depth=1,
        max_entities=5,
        max_triples=20,
        escape=lambda s: s.replace("'", "''"),
    )
    assert "LEFT ANTI JOIN" not in sql
    assert "NOT EXISTS" in sql
    assert "O''Brien" in sql
    assert "VALUES (" in sql or "VALUES" in sql


def test_expansion_uses_props_only_for_final_payload_fetch():
    sql = expand_and_fetch_sql(
        flavor="spark",
        adj_out="o",
        adj_in="i",
        spo="g",
        props="g_props",
        selected_uris=["http://ex/a"],
        depth=1,
        max_entities=5,
        max_triples=20,
        escape=lambda s: s.replace("'", "''"),
    )

    assert "FROM o t" in sql
    assert "FROM i t" in sql
    assert "FROM g_props triples" in sql
    assert "FROM g triples" not in sql


def test_depth_zero_filters_payload_by_subject_in():
    sql = expand_and_fetch_sql(
        flavor="spark",
        adj_out="o",
        adj_in="i",
        spo="g",
        props="g_props",
        selected_uris=["http://ex/a", "http://ex/a"],
        depth=0,
        max_entities=10,
        max_triples=20,
        escape=lambda s: s.replace("'", "''"),
    )

    assert "level_1" not in sql
    assert "CROSS JOIN entity_stats" not in sql
    assert "WHERE subject IN" in sql
    assert "FROM g_props" in sql
    assert sql.count("http://ex/a") == 1
    assert "LIMIT 21" in sql
    assert "1 AS _ob_expanded_count" in sql


def test_spark_payload_join_broadcasts_entities():
    sql = expand_and_fetch_sql(
        flavor="spark",
        adj_out="o",
        adj_in="i",
        spo="g",
        selected_uris=["http://ex/a"],
        depth=1,
        max_entities=5,
        max_triples=20,
        escape=lambda s: s.replace("'", "''"),
    )

    assert "/*+ BROADCAST(entities) */" in sql


def test_postgres_payload_join_has_no_broadcast_hint():
    sql = expand_and_fetch_sql(
        flavor="postgres",
        adj_out="o",
        adj_in="i",
        spo="g",
        selected_uris=["http://ex/a"],
        depth=1,
        max_entities=5,
        max_triples=20,
        escape=lambda s: s.replace("'", "''"),
    )

    assert "BROADCAST" not in sql


def test_seeded_bfs_sql_depth_zero_is_seed_only() -> None:
    sql = seeded_bfs_sql(
        flavor="spark",
        adj_out="o",
        adj_in="i",
        seed_sql="SELECT uri FROM g_entity_search WHERE type_uri = 'X'",
        depth=0,
    )
    assert "level_1" not in sql
    assert "level_0(entity) AS (SELECT uri FROM g_entity_search WHERE type_uri = 'X')" in sql
    assert "SELECT entity, 0 AS lvl FROM level_0" in sql
    assert "GROUP BY entity" in sql


def test_seeded_bfs_sql_spark_uses_left_anti_join_per_level() -> None:
    sql = seeded_bfs_sql(
        flavor="spark",
        adj_out="o",
        adj_in="i",
        seed_sql="SELECT uri FROM g_entity_search",
        depth=2,
    )
    assert sql.count("LEFT ANTI JOIN") == 2
    assert "level_2" in sql
    assert "FROM o t" in sql
    assert "FROM i t" in sql
    assert "SELECT entity, MIN(lvl) AS min_lvl FROM (" in sql


def test_seeded_bfs_sql_postgres_uses_not_exists() -> None:
    sql = seeded_bfs_sql(
        flavor="postgres",
        adj_out="o",
        adj_in="i",
        seed_sql="SELECT uri FROM g_entity_search",
        depth=1,
    )
    assert "LEFT ANTI JOIN" not in sql
    assert "NOT EXISTS" in sql


def test_seeded_bfs_sql_unions_every_level_with_its_number() -> None:
    sql = seeded_bfs_sql(
        flavor="spark",
        adj_out="o",
        adj_in="i",
        seed_sql="SELECT uri FROM g_entity_search",
        depth=3,
    )
    for level in range(4):
        assert f"SELECT entity, {level} AS lvl FROM level_{level}" in sql


def test_props_page_sql_builds_ordered_page_with_total() -> None:
    sql = props_page_sql(
        payload_relation="g_props",
        uris=["http://ex/a", "http://ex/O'Brien"],
        limit=2,
        offset=3,
        escape=lambda value: value.replace("'", "''"),
    )

    assert "SELECT DISTINCT subject, predicate, object" in sql
    assert "COUNT(*) AS _ob_total" in sql
    assert "ORDER BY page.subject, page.predicate, page.object" in sql
    assert "LIMIT 2 OFFSET 3" in sql
    assert "O''Brien" in sql
