"""Lakebase adjacency DDL and expansion contracts."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("psycopg")

from back.core.graphdb.lakebase.LakebaseFlatStore import LakebaseFlatStore
from back.core.graphdb.lakebase import _adjacency_ddl
from back.core.graphdb.props import (
    known_missing_props,
    remember_missing_props,
    reset_missing_props_cache,
)


@pytest.fixture
def auth():
    a = MagicMock()
    a.database = "appdb"
    a.is_available = True
    return a


@pytest.fixture(autouse=True)
def _clear_missing_props_cache():
    reset_missing_props_cache()
    yield
    reset_missing_props_cache()


def _cursor_ctx(mock_cur):
    @contextmanager
    def cm():
        yield mock_cur

    return cm


def test_lakebase_adjacency_table_ids(auth):
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    assert store.adjacency_table_ids("G_V1") == ("g_v1_adj_out", "g_v1_adj_in")
    assert store.entity_search_table_id("G_V1") == "g_v1_entity_search"
    assert store.entity_search_asserted_table_id("G_V1") == (
        "g_v1_entity_search_asserted"
    )
    assert store.entity_search_asserted_table_id("g_v1_sync") == (
        "g_v1_entity_search_asserted"
    )
    assert store.props_table_id("G_V1") == "g_v1_props"


def test_lakebase_rebuild_adjacency_runs_ddl_and_rebuild_sql(auth):
    setup_cur = MagicMock()
    tx_cur = MagicMock()
    analyze_cur = MagicMock()
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    props = store.props_table_id("G_V1")
    remember_missing_props(props)

    @contextmanager
    def _tx_cm():
        yield MagicMock(), tx_cur

    with patch.object(
        store, "_cursor", side_effect=[_cursor_ctx(setup_cur)(), _cursor_ctx(analyze_cur)()]
    ), patch.object(store, "_txn_cursor", _tx_cm):
        store.rebuild_adjacency("G_V1")

    assert known_missing_props(props) is False
    executed = [str(c[0][0]) for c in setup_cur.execute.call_args_list]
    executed += [str(c[0][0]) for c in tx_cur.execute.call_args_list]
    executed += [str(c[0][0]) for c in analyze_cur.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS g_v1_adj_out" in s for s in executed)
    assert any("CREATE TABLE IF NOT EXISTS g_v1_adj_in" in s for s in executed)
    assert any("CREATE TABLE IF NOT EXISTS g_v1_entity_search" in s for s in executed)
    assert any("CREATE TABLE IF NOT EXISTS g_v1_entity_search_asserted" in s for s in executed)
    assert any("CREATE TABLE IF NOT EXISTS g_v1_props" in s for s in executed)
    assert any(
        "CREATE INDEX IF NOT EXISTS g_v1_adj_out_src_idx ON g_v1_adj_out (src, predicate)" in s
        for s in executed
    )
    assert any(
        "CREATE INDEX IF NOT EXISTS g_v1_adj_in_dst_idx ON g_v1_adj_in (dst, predicate)" in s
        for s in executed
    )
    assert any("TRUNCATE g_v1_adj_out" in s for s in executed)
    assert any("TRUNCATE g_v1_adj_in" in s for s in executed)
    assert any("TRUNCATE g_v1_entity_search" in s for s in executed)
    assert any("TRUNCATE g_v1_entity_search_asserted" in s for s in executed)
    assert any("TRUNCATE g_v1_props" in s for s in executed)
    assert any(
        "INSERT INTO g_v1_adj_out (src, predicate, dst) SELECT DISTINCT" in s
        for s in executed
    )
    assert any(
        "INSERT INTO g_v1_adj_in (dst, predicate, src) SELECT DISTINCT" in s
        for s in executed
    )
    assert any("ANALYZE g_v1_adj_out" in s for s in executed)
    assert any("ANALYZE g_v1_adj_in" in s for s in executed)
    assert any("ANALYZE g_v1_entity_search" in s for s in executed)
    assert any("ANALYZE g_v1_entity_search_asserted" in s for s in executed)
    assert any("ANALYZE g_v1_props" in s for s in executed)

    tx_sql = [str(c[0][0]) for c in tx_cur.execute.call_args_list]
    assert tx_sql[-2].startswith("TRUNCATE g_v1_props")
    assert tx_sql[-1].startswith(
        "INSERT INTO g_v1_props (subject, predicate, object)"
    )


def test_lakebase_rebuild_adjacency_is_atomic_on_insert_failure(auth):
    setup_cur = MagicMock()
    analyze_cur = MagicMock()
    tx_cur = MagicMock()
    tx_cur.execute.side_effect = [
        None,  # TRUNCATE out
        None,  # INSERT out
        None,  # TRUNCATE in
        RuntimeError("boom on second insert"),  # INSERT in
    ]
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")

    @contextmanager
    def _tx_cm():
        yield MagicMock(), tx_cur

    with patch.object(
        store, "_cursor", side_effect=[_cursor_ctx(setup_cur)(), _cursor_ctx(analyze_cur)()]
    ), patch.object(store, "_txn_cursor", _tx_cm):
        with pytest.raises(RuntimeError, match="boom on second insert"):
            store.rebuild_adjacency("G_V1")

    tx_sql = [str(c[0][0]) for c in tx_cur.execute.call_args_list]
    assert tx_sql[:3] == [
        "TRUNCATE g_v1_adj_out",
        "INSERT INTO g_v1_adj_out (src, predicate, dst) "
        "SELECT DISTINCT t.subject AS src, t.predicate, t.object AS dst "
        "FROM g_v1 t "
        "INNER JOIN g_v1 typed "
        "ON typed.subject = t.object AND typed.predicate = "
        "'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
        "WHERE t.object LIKE 'http%' "
        "AND t.predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
        "AND t.predicate != 'http://www.w3.org/2000/01/rdf-schema#label'",
        "TRUNCATE g_v1_adj_in",
    ]
    analyze_cur.execute.assert_not_called()


def test_expand_entity_neighbors_uses_adjacency_tables_when_ready(auth):
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "table_exists", return_value=True), patch.object(
        store, "execute_query", return_value=[]
    ) as mock_query:
        store.expand_entity_neighbors("G_V1", {"http://ex/seed"})

    sql = mock_query.call_args.args[0]
    assert "FROM g_v1_adj_out" in sql
    assert "FROM g_v1_adj_in" in sql
    assert "object LIKE 'http%'" not in sql


def test_expand_and_fetch_subgraph_uses_adjacency_tables_when_ready(auth):
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    rows = [
        {
            "subject": "s",
            "predicate": "p",
            "object": "o",
            "_ob_expanded_count": 2,
        }
    ]
    with patch.object(store, "table_exists", return_value=True), patch.object(
        store, "execute_query", return_value=rows
    ) as mock_query:
        payload = store.expand_and_fetch_subgraph("G_V1", ["http://ex/a"], 2, 100, 200)

    sql = mock_query.call_args.args[0]
    assert "FROM g_v1_adj_out t" in sql
    assert "FROM g_v1_adj_in t" in sql
    assert "FROM g_v1_props triples" in sql
    assert "FROM g_v1 triples" not in sql
    assert "NOT EXISTS" in sql
    assert payload["expanded_count"] == 2
    assert payload["count"] == 1
    assert payload["capped"] is False


def test_expand_and_fetch_subgraph_falls_back_to_spo_when_adj_missing(auth):
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "table_exists", return_value=False), patch.object(
        store, "execute_query", return_value=[]
    ) as mock_query:
        store.expand_and_fetch_subgraph("G_V1", ["http://ex/a"], 1, 50, 100)

    sql = mock_query.call_args.args[0]
    assert "WITH adj_out AS (" in sql
    assert "adj_in AS (" in sql
    assert "FROM g_v1 t" in sql
    assert "FROM adj_out t" in sql
    assert "FROM adj_in t" in sql


def test_expand_and_fetch_subgraph_retries_spo_when_props_missing(auth):
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "table_exists", return_value=True), patch.object(
        store,
        "execute_query",
        side_effect=[RuntimeError("relation g_v1_props does not exist"), []],
    ) as mock_query:
        store.expand_and_fetch_subgraph("G_V1", ["http://ex/a"], 1, 50, 100)

    assert mock_query.call_count == 2
    first_sql, fallback_sql = [call.args[0] for call in mock_query.call_args_list]
    assert "FROM g_v1_props triples" in first_sql
    assert "FROM g_v1 triples" in fallback_sql


def test_expand_and_fetch_subgraph_skips_props_when_known_missing(auth):
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "table_exists", return_value=True), patch.object(
        store,
        "execute_query",
        side_effect=[
            RuntimeError("relation g_v1_props does not exist"),
            [],
            [],
        ],
    ) as mock_query:
        for _ in range(2):
            store.expand_and_fetch_subgraph(
                "G_V1", ["http://ex/a"], 1, 50, 100
            )

    assert mock_query.call_count == 3
    second_expand_sql = mock_query.call_args_list[2].args[0]
    assert "FROM g_v1_props triples" not in second_expand_sql
    assert "FROM g_v1 triples" in second_expand_sql


def test_expand_and_fetch_subgraph_propagates_unrelated_props_failure(auth):
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "table_exists", return_value=True), patch.object(
        store, "execute_query", side_effect=RuntimeError("permission denied")
    ):
        with pytest.raises(RuntimeError, match="permission denied"):
            store.expand_and_fetch_subgraph("G_V1", ["http://ex/a"], 1, 50, 100)


def test_long_adjacency_names_fit_postgres_identifier_limit():
    graph = "Domain_" + ("A" * 120)
    out_name = _adjacency_ddl.adj_out_phy(graph)
    in_name = _adjacency_ddl.adj_in_phy(graph)
    assert len(out_name.encode("utf-8")) <= 63
    assert len(in_name.encode("utf-8")) <= 63
    assert out_name.endswith("_adj_out")
    assert in_name.endswith("_adj_in")


def test_long_entity_search_name_fits_postgres_identifier_limit():
    name = _adjacency_ddl.entity_search_phy("Domain_" + ("A" * 120))
    assert len(name.encode("utf-8")) <= 63
    assert name.endswith("_entity_search")


def test_long_asserted_entity_search_name_fits_postgres_identifier_limit():
    name = _adjacency_ddl.entity_search_asserted_phy("Domain_" + ("A" * 120))
    assert len(name.encode("utf-8")) <= 63
    assert name.endswith("_entity_search_asserted")


def test_entity_search_adds_gin_when_pg_trgm_present():
    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    _adjacency_ddl.ensure_entity_search_table(cur, "g_search")
    sql = [str(c[0][0]) for c in cur.execute.call_args_list]
    assert any("USING gin (label_lc gin_trgm_ops)" in s for s in sql)
    assert any("USING gin (uri_lc gin_trgm_ops)" in s for s in sql)


def test_entity_search_skips_gin_when_pg_trgm_missing():
    cur = MagicMock()
    cur.fetchone.return_value = None
    _adjacency_ddl.ensure_entity_search_table(cur, "g_search")
    sql = [str(c[0][0]) for c in cur.execute.call_args_list]
    assert not any("gin_trgm_ops" in s for s in sql)


def test_long_props_name_fits_postgres_identifier_limit():
    name = _adjacency_ddl.props_phy("Domain_" + ("A" * 120))
    assert len(name.encode("utf-8")) <= 63
    assert name.endswith("_props")


def test_long_adjacency_names_do_not_collide():
    graph_a = "Domain_" + ("A" * 100) + "X"
    graph_b = "Domain_" + ("A" * 100) + "Y"
    assert _adjacency_ddl.adj_out_phy(graph_a) != _adjacency_ddl.adj_out_phy(graph_b)
    assert _adjacency_ddl.adj_in_phy(graph_a) != _adjacency_ddl.adj_in_phy(graph_b)


def test_long_index_names_fit_postgres_identifier_limit():
    table = _adjacency_ddl.adj_out_phy("Domain_" + ("A" * 120))
    idx = _adjacency_ddl.adjacency_index_name(table, "src")
    assert len(idx.encode("utf-8")) <= 63
    assert idx.endswith("_src_idx")
