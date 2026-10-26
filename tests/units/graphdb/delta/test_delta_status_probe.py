"""Lakehouse status probes must not scan pass-through graph views."""

from unittest.mock import MagicMock

from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore

GRAPH = "cat.sch.triplestore_mydomain_V1_graph"
SEARCH = "cat.sch.triplestore_mydomain_V1_entity_search"
DATA = "cat.sch.triplestore_mydomain_V1_data"


def _domain():
    d = MagicMock()
    d.info = {"name": "MyDomain"}
    d.current_version = 1
    d.delta = {"catalog": "cat", "schema": "sch"}
    return d


def _sqls(client):
    return [call.args[0] for call in client.execute_query.call_args_list]


class TestDeltaTableExistsUsesCatalog:
    def test_graph_existence_describes_entity_search_not_the_view(self):
        client = MagicMock()
        client.execute_query.return_value = [{"format": "delta"}]
        store = DeltaFlatStore(client, domain=_domain())

        assert store.table_exists(GRAPH) is True
        sqls = _sqls(client)
        assert sqls == [f"DESCRIBE DETAIL {SEARCH}"]
        assert not any(GRAPH in sql for sql in sqls)
        assert not any("SELECT 1" in sql for sql in sqls)
        assert not any("COUNT(" in sql for sql in sqls)

    def test_missing_relation_is_false(self):
        client = MagicMock()
        client.execute_query.side_effect = RuntimeError(
            "TABLE_OR_VIEW_NOT_FOUND: graph"
        )
        store = DeltaFlatStore(client, domain=_domain())

        assert store.table_exists("cat.sch.missing_graph") is False


_VIEW_ERROR = (
    f"[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'DESCRIBE DETAIL' expects a table "
    f"but {DATA} is a view. SQLSTATE: 42809"
)


class TestZeroCopyDataView:
    """Zero-copy materialization makes ``…_data`` a VIEW, which DESCRIBE DETAIL rejects."""

    def test_data_view_exists(self):
        client = MagicMock()
        client.execute_query.side_effect = RuntimeError(_VIEW_ERROR)
        store = DeltaFlatStore(client, domain=_domain())

        assert store.table_exists(DATA) is True

    def test_data_view_exists_when_sea_hides_marker_in_detail(self):
        from back.core.errors import InfrastructureError
        from back.core.graphdb.delta.health import probe_table_status

        sea_view_error = InfrastructureError(
            "Databricks SQL statement failed",
            detail=(
                '{"error_code":"BAD_REQUEST","message":'
                f'"{_VIEW_ERROR}"}}'
            ),
        )

        def _query(sql):
            if sql == f"DESCRIBE DETAIL {DATA}":
                raise sea_view_error
            if sql == f"SELECT 1 FROM {DATA} LIMIT 1":
                return [{"1": 1}]
            raise AssertionError(f"unexpected SQL: {sql}")

        client = MagicMock()
        client.execute_query.side_effect = _query
        store = DeltaFlatStore(client, domain=_domain())

        assert store.table_exists(DATA) is True
        probed = probe_table_status(store, DATA)
        assert probed["exists"] is True
        assert probed["has_data"] is True
        assert probed["error"] is None

    def test_data_view_status_has_no_delta_detail_and_no_full_count(self):
        client = MagicMock()

        def _query(sql):
            if sql == f"DESCRIBE DETAIL {DATA}":
                raise RuntimeError(_VIEW_ERROR)
            if sql == f"SELECT 1 FROM {DATA} LIMIT 1":
                return [{"1": 1}]
            raise AssertionError(f"unexpected SQL: {sql}")

        client.execute_query.side_effect = _query
        store = DeltaFlatStore(client, domain=_domain())

        status = store.get_status(DATA)
        assert status["count"] == 1
        assert status.get("format") is None
        assert not any("COUNT(" in sql for sql in _sqls(client))


class TestDeltaGetStatusAvoidsFullCount:
    def test_data_table_status_does_not_count_star(self):
        client = MagicMock()

        def _query(sql):
            if sql == f"DESCRIBE DETAIL {DATA}":
                return [
                    {
                        "format": "delta",
                        "numRecords": 42,
                        "lastModified": "2026-10-15",
                        "path": "s3://x",
                    }
                ]
            if sql == f"SELECT 1 FROM {DATA} LIMIT 1":
                return [{"1": 1}]
            raise AssertionError(f"unexpected SQL: {sql}")

        client.execute_query.side_effect = _query
        store = DeltaFlatStore(client, domain=_domain())

        status = store.get_status(DATA)
        assert status["count"] == 1
        assert not any("COUNT(" in sql for sql in _sqls(client))

    def test_graph_status_counts_props_and_never_touches_the_view(self):
        client = MagicMock()
        client.sql = None
        props = "cat.sch.triplestore_mydomain_V1_props"

        def _query(sql):
            if GRAPH in sql:
                raise RuntimeError("CLOUD_IO_INVALID_ARGUMENT: graph view")
            if sql == f"DESCRIBE DETAIL {SEARCH}":
                return [{"format": "delta", "lastModified": None}]
            if sql == f"SELECT COUNT(*) AS cnt FROM {props}":
                return [{"cnt": 1234}]
            raise AssertionError(f"unexpected SQL: {sql}")

        client.execute_query.side_effect = _query
        store = DeltaFlatStore(client, domain=_domain())

        status = store.get_status(GRAPH)
        assert status["count"] == 1234
        assert not any(GRAPH in sql for sql in _sqls(client))

    def test_graph_status_falls_back_to_limit_1_without_props(self):
        client = MagicMock()
        client.sql = None

        def _query(sql):
            if GRAPH in sql:
                raise RuntimeError("CLOUD_IO_INVALID_ARGUMENT: graph view")
            if "_props" in sql:
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND: props")
            if sql == f"DESCRIBE DETAIL {SEARCH}":
                return [{"format": "delta"}]
            if sql == f"SELECT 1 FROM {SEARCH} LIMIT 1":
                return [{"1": 1}]
            raise AssertionError(f"unexpected SQL: {sql}")

        client.execute_query.side_effect = _query
        store = DeltaFlatStore(client, domain=_domain())

        assert store.get_status(GRAPH)["count"] == 1
