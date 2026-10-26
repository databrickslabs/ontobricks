"""Knowledge Graph Overview must read managed companions, not the live VIEW."""

from unittest.mock import MagicMock

from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore


GRAPH = "cat.sch.triplestore_mydomain_V1_graph"
SEARCH = "cat.sch.triplestore_mydomain_V1_entity_search"
PROPS = "cat.sch.triplestore_mydomain_V1_props"
ADJ_OUT = "cat.sch.triplestore_mydomain_V1_adj_out"


def _domain():
    d = MagicMock()
    d.info = {"name": "MyDomain"}
    d.current_version = 1
    d.delta = {"catalog": "cat", "schema": "sch"}
    return d


def _store(client):
    client.sql = None
    store = DeltaFlatStore(client, domain=_domain())
    store.entity_search_ready = MagicMock(return_value=True)
    store.adjacency_ready = MagicMock(return_value=True)
    return store


def _sqls(client):
    return [call.args[0] for call in client.execute_query.call_args_list]


def _query(sql):
    if sql.startswith("DESCRIBE DETAIL"):
        raise AssertionError(f"must not use DESCRIBE DETAIL for overview: {sql}")
    if f"SELECT COUNT(*) AS cnt FROM {PROPS}" == sql:
        return [{"cnt": 1000}]
    if "AS subjects" in sql and SEARCH in sql:
        return [{"subjects": 80, "labels": 70}]
    if "COUNT(DISTINCT predicate)" in sql and ADJ_OUT in sql:
        return [{"cnt": 1}]
    if "AS type_uri" in sql and SEARCH in sql:
        return [{"type_uri": "http://ex/Person", "cnt": 80}]
    if "AS predicate" in sql and ADJ_OUT in sql:
        return [{"predicate": "http://ex/knows", "cnt": 12}]
    if GRAPH in sql:
        raise AssertionError(f"must not query the graph view: {sql}")
    raise AssertionError(f"unexpected SQL: {sql}")


class TestDeltaOverviewReadsCompanions:
    def test_aggregate_stats_count_companions_not_describe_metadata(self):
        client = MagicMock()
        client.execute_query.side_effect = _query
        stats = _store(client).get_aggregate_stats(GRAPH)
        assert stats["total"] == 1000
        assert stats["distinct_subjects"] == 80
        assert stats["type_assertion_count"] == 80
        assert stats["label_count"] == 70
        assert stats["distinct_predicates"] == 1
        assert not any(GRAPH in sql for sql in _sqls(client))
        assert not any("DESCRIBE DETAIL" in sql for sql in _sqls(client))

    def test_type_distribution_reads_entity_search(self):
        client = MagicMock()
        client.execute_query.side_effect = _query
        rows = _store(client).get_type_distribution(GRAPH)
        assert rows == [{"type_uri": "http://ex/Person", "cnt": 80}]
        assert not any(GRAPH in sql for sql in _sqls(client))

    def test_predicate_distribution_reads_adjacency(self):
        client = MagicMock()
        client.execute_query.side_effect = _query
        rows = _store(client).get_predicate_distribution(GRAPH)
        assert rows == [{"predicate": "http://ex/knows", "cnt": 12}]
        assert not any(GRAPH in sql for sql in _sqls(client))
