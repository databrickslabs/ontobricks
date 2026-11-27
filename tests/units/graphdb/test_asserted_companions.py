"""Asserted-graph companions: routing, rebuild and grouping (Delta side).

The Original Graph (asserted relation) must read adjacency, props and entity
search built from the asserted relation only. Before this contract, the
asserted view name fell through to the certified companions, so derived
triples (reasoning, cohorts, communities) could show up in the Original Graph.
"""

from unittest.mock import MagicMock

from back.core.graphdb.delta.MaterializedStore import MaterializedStore
from back.core.graphdb.delta.objects import object_base
from back.core.graphdb.entity_search import is_asserted_only_relation
from back.objects.digitaltwin.build.BuildObjects import BuildObjects, usage_of

GRAPH = "cat.sch.triplestore_mydomain_V1_graph"
ASSERTED = "cat.sch.triplestore_mydomain_V1_asserted"
DATA = "cat.sch.triplestore_mydomain_V1_data"


def _domain(name="MyDomain", version=1, catalog="cat", schema="sch"):
    d = MagicMock()
    d.info = {"name": name}
    d.current_version = version
    d.delta = {"catalog": catalog, "schema": schema}
    return d


def _executed_statements(client):
    statements = []
    for call in client.execute_statement.call_args_list:
        if call.args:
            statements.append(call.args[0])
        elif "sql" in call.kwargs:
            statements.append(call.kwargs["sql"])
    return statements


def test_asserted_view_and_fallbacks_are_asserted_relations():
    assert is_asserted_only_relation(ASSERTED) is True
    assert is_asserted_only_relation(DATA) is True
    assert is_asserted_only_relation(GRAPH) is False
    assert is_asserted_only_relation("MyDomain_V1") is False


def test_adjacency_and_props_route_by_relation_when_domain_bound():
    store = MaterializedStore(MagicMock(), domain=_domain())

    assert store.adjacency_table_ids(GRAPH) == (
        "cat.sch.triplestore_mydomain_V1_adj_out",
        "cat.sch.triplestore_mydomain_V1_adj_in",
    )
    assert store.props_table_id(GRAPH) == "cat.sch.triplestore_mydomain_V1_props"

    assert store.adjacency_table_ids(ASSERTED) == (
        "cat.sch.triplestore_mydomain_V1_asserted_adj_out",
        "cat.sch.triplestore_mydomain_V1_asserted_adj_in",
    )
    assert store.props_table_id(ASSERTED) == (
        "cat.sch.triplestore_mydomain_V1_asserted_props"
    )
    assert store.adjacency_table_ids(DATA) == store.adjacency_table_ids(ASSERTED)


def test_adjacency_and_props_route_by_relation_without_domain():
    store = MaterializedStore(MagicMock())

    assert store.adjacency_table_ids(GRAPH) == (
        "cat.sch.triplestore_mydomain_V1_adj_out",
        "cat.sch.triplestore_mydomain_V1_adj_in",
    )
    assert store.adjacency_table_ids(ASSERTED) == (
        "cat.sch.triplestore_mydomain_V1_asserted_adj_out",
        "cat.sch.triplestore_mydomain_V1_asserted_adj_in",
    )
    assert store.props_table_id(ASSERTED) == (
        "cat.sch.triplestore_mydomain_V1_asserted_props"
    )


def test_entity_search_for_asserted_view_uses_asserted_search_table():
    """The Original Graph seeds from the asserted search, never the union one."""
    store = MaterializedStore(MagicMock(), domain=_domain())
    probed = []
    store.table_exists = MagicMock(side_effect=lambda fqn: probed.append(fqn) or True)

    assert store.entity_search_ready(ASSERTED) is True
    assert probed == ["cat.sch.triplestore_mydomain_V1_entity_search_asserted"]


def test_rebuild_builds_asserted_companions_from_the_asserted_view():
    client = MagicMock()
    store = MaterializedStore(client, domain=_domain())
    store.table_exists = MagicMock(return_value=True)

    store.rebuild_adjacency("MyDomain_V1")

    statements = _executed_statements(client)
    asserted_adj = [
        sql
        for sql in statements
        if "triplestore_mydomain_V1_asserted_adj_" in sql and "USING DELTA" in sql
    ]
    assert len(asserted_adj) == 2
    for sql in asserted_adj:
        assert "FROM cat.sch.triplestore_mydomain_V1_asserted" in sql
        assert "triplestore_mydomain_V1_graph" not in sql

    asserted_props = [
        sql
        for sql in statements
        if "CREATE OR REPLACE TABLE cat.sch.triplestore_mydomain_V1_asserted_props"
        in sql
    ]
    assert len(asserted_props) == 1
    assert "FROM cat.sch.triplestore_mydomain_V1_asserted" in asserted_props[0]

    certified_props = [
        sql
        for sql in statements
        if "CREATE OR REPLACE TABLE cat.sch.triplestore_mydomain_V1_props" in sql
    ]
    assert len(certified_props) == 1
    assert "FROM cat.sch.triplestore_mydomain_V1_graph" in certified_props[0]


def test_asserted_companions_group_with_their_domain():
    assert object_base("triplestore_foo_V1_asserted_adj_out") == "triplestore_foo_V1"
    assert object_base("triplestore_foo_V1_asserted_adj_in") == "triplestore_foo_V1"
    assert object_base("triplestore_foo_V1_asserted_props") == "triplestore_foo_V1"


def test_asserted_companions_are_classified_as_search_cache():
    assert usage_of("triplestore_foo_V1_asserted_adj_out") == "cache"
    assert usage_of("triplestore_foo_V1_asserted_adj_in") == "cache"
    assert usage_of("triplestore_foo_V1_asserted_props") == "cache"


def test_postgres_asserted_companions_group_with_their_graph():
    assert BuildObjects.pg_base("g_v1_asserted_adj_out", "table") == "g_v1"
    assert BuildObjects.pg_base("g_v1_asserted_adj_in", "table") == "g_v1"
    assert BuildObjects.pg_base("g_v1_asserted_props", "table") == "g_v1"


def test_original_graph_expansion_walks_asserted_adjacency_only():
    """The expansion SQL for the asserted view joins the asserted companions."""
    store = MaterializedStore(MagicMock(), domain=_domain())
    store.execute_query = MagicMock(return_value=[])
    store.table_exists = MagicMock(return_value=True)

    store.expand_and_fetch_subgraph(ASSERTED, ["http://ex/a"], 2, 50, 100)

    sql = store.execute_query.call_args.args[0]
    assert "FROM cat.sch.triplestore_mydomain_V1_asserted_adj_out t" in sql
    assert "FROM cat.sch.triplestore_mydomain_V1_asserted_adj_in t" in sql
    assert "FROM cat.sch.triplestore_mydomain_V1_asserted_props triples" in sql
    assert "FROM cat.sch.triplestore_mydomain_V1_adj_out t" not in sql
    assert "FROM cat.sch.triplestore_mydomain_V1_props triples" not in sql
