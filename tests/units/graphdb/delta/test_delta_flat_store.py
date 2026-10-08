"""Tests for DeltaFlatStore inferred companion routing."""

import threading
from unittest.mock import MagicMock, patch

import pytest
from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore
from back.core.graphdb.props import (
    known_missing_props,
    remember_missing_props,
    reset_missing_props_cache,
)


@pytest.fixture(autouse=True)
def _clear_missing_props_cache():
    reset_missing_props_cache()
    yield
    reset_missing_props_cache()


def _domain(name="MyDomain", version=1, catalog="cat", schema="sch"):
    d = MagicMock()
    d.info = {"name": name}
    d.current_version = version
    d.delta = {"catalog": catalog, "schema": schema}
    return d


def _executed_statements(client):
    """Collect SQL strings passed to execute_statement (positional or keyword)."""
    statements = []
    for call in client.execute_statement.call_args_list:
        if call.args:
            statements.append(call.args[0])
        elif "query" in call.kwargs:
            statements.append(call.kwargs["query"])
    return statements


class TestDeltaFlatStoreInferredRouting:
    def test_insert_triples_targets_inferred_companion(self):
        client = MagicMock()
        domain = _domain()
        store = DeltaFlatStore(client, domain=domain)
        triples = [
            {
                "subject": "http://ex/s",
                "predicate": "http://ex/p",
                "object": "http://ex/o",
            }
        ]
        with patch.object(
            store, "_execute_insert_triples", return_value=1
        ) as mock_insert:
            with patch(
                "back.core.graphdb.delta.materialize.ensure_inferred_table"
            ) as mock_ensure_inf:
                with patch(
                    "back.core.graphdb.delta.materialize.ensure_graph_view"
                ) as mock_ensure_view:
                    count = store.insert_triples("MyDomain_V1", triples)
        assert count == 1
        mock_ensure_inf.assert_called_once_with(
            client, "cat.sch.triplestore_mydomain_V1_inferred"
        )
        mock_ensure_view.assert_called_once_with(
            client,
            "cat.sch.triplestore_mydomain_V1_graph",
            "cat.sch.triplestore_mydomain_V1_data",
            "cat.sch.triplestore_mydomain_V1_inferred",
        )
        mock_insert.assert_called_once_with(
            "cat.sch.triplestore_mydomain_V1_inferred", triples, 2000, None
        )

    def test_synced_table_name_strips_graph_suffix(self):
        store = DeltaFlatStore(MagicMock())
        fqn = "cat.sch.triplestore_mydomain_V1_graph"
        assert store.synced_table_name(fqn) == "cat.sch.triplestore_mydomain_V1_data"

    def test_sql_relation_resolves_logical_graph_name(self):
        client = MagicMock()
        domain = _domain()
        store = DeltaFlatStore(client, domain=domain)
        with patch.object(store, "table_exists", return_value=True):
            ref = store.sql_table_reference("MyDomain_V1")
        assert ref == "cat.sch.triplestore_mydomain_V1_graph"

    def test_sql_relation_falls_back_to_data_when_graph_view_missing(self):
        client = MagicMock()
        domain = _domain()
        store = DeltaFlatStore(client, domain=domain)
        with patch.object(store, "table_exists", return_value=False):
            ref = store.sql_table_reference("MyDomain_V1")
        assert ref == "cat.sch.triplestore_mydomain_V1_data"

    def test_sql_relation_passes_through_fqn(self):
        store = DeltaFlatStore(MagicMock(), domain=_domain())
        fqn = "cat.sch.triplestore_mydomain_V1_data"
        assert store.sql_table_reference(fqn) == fqn

    def test_optimize_inferred_companion_targets_inferred_fqn(self):
        client = MagicMock()
        domain = _domain()
        store = DeltaFlatStore(client, domain=domain)
        with patch("back.core.graphdb.delta.materialize.optimize_table") as mock_opt:
            store.optimize_inferred_companion("MyDomain_V1")
        mock_opt.assert_called_once_with(
            client, "cat.sch.triplestore_mydomain_V1_inferred"
        )

    def test_rebuild_adjacency_builds_all_graph_index_companions(self):
        client = MagicMock()
        domain = _domain()
        store = DeltaFlatStore(client, domain=domain)
        props = store.props_table_id("MyDomain_V1")
        remember_missing_props(props)

        store.rebuild_adjacency("MyDomain_V1")

        assert known_missing_props(props) is False
        statements = [call[0][0] for call in client.execute_statement.call_args_list]
        assert any(
            "CREATE OR REPLACE TABLE cat.sch.triplestore_mydomain_V1_adj_out"
            in sql
            for sql in statements
        )
        assert any(
            "CREATE OR REPLACE TABLE cat.sch.triplestore_mydomain_V1_adj_in"
            in sql
            for sql in statements
        )
        assert any(
            "CREATE OR REPLACE TABLE cat.sch.triplestore_mydomain_V1_entity_search"
            in sql
            for sql in statements
        )
        assert any(
            "CREATE OR REPLACE TABLE cat.sch.triplestore_mydomain_V1_props"
            in sql
            and "CLUSTER BY (subject)" in sql
            for sql in statements
        )
        assert "OPTIMIZE cat.sch.triplestore_mydomain_V1_adj_out" in statements
        assert "OPTIMIZE cat.sch.triplestore_mydomain_V1_adj_in" in statements
        assert "OPTIMIZE cat.sch.triplestore_mydomain_V1_entity_search" in statements
        assert any(
            "CREATE OR REPLACE TABLE cat.sch.triplestore_mydomain_V1_entity_search_asserted"
            in sql
            for sql in statements
        )
        assert any(
            "FROM cat.sch.triplestore_mydomain_V1_data" in sql
            and "_entity_search_asserted" in sql
            for sql in statements
        )
        assert any(
            "delta.bloomFilter.columns" in sql for sql in statements
        )
        assert "OPTIMIZE cat.sch.triplestore_mydomain_V1_entity_search_asserted" in statements
        assert "OPTIMIZE cat.sch.triplestore_mydomain_V1_props" in statements

    def test_rebuild_adjacency_keeps_going_when_optimize_fails(self):
        client = MagicMock()
        domain = _domain()
        store = DeltaFlatStore(client, domain=domain)

        with patch(
            "back.core.graphdb.delta.materialize.optimize_table",
            side_effect=RuntimeError("optimize failed"),
        ), patch(
            "back.core.graphdb.delta.DeltaFlatStore.logger.warning"
        ) as mock_warning:
            store.rebuild_adjacency("MyDomain_V1")

        statements = [call[0][0] for call in client.execute_statement.call_args_list]
        assert any("_adj_out USING DELTA" in sql for sql in statements)
        assert any("_adj_in USING DELTA" in sql for sql in statements)
        assert any("_entity_search USING DELTA" in sql for sql in statements)
        assert any("_props USING DELTA" in sql for sql in statements)
        assert mock_warning.call_count == 5

    def test_rebuild_adjacency_still_fails_when_ctas_fails(self):
        client = MagicMock()
        domain = _domain()
        store = DeltaFlatStore(client, domain=domain)
        client.execute_statement.side_effect = RuntimeError("ctas failed")

        with pytest.raises(RuntimeError, match="ctas failed"):
            store.rebuild_adjacency("MyDomain_V1")

    def test_rebuild_adjacency_table_runs_ctas_and_optimize_for_direction(self):
        client = MagicMock()
        store = DeltaFlatStore(client, domain=_domain())
        relation = "cat.sch.graph"
        adj_out = "cat.sch.triplestore_mydomain_V1_adj_out"

        with patch("back.core.graphdb.delta.materialize.drop_relation") as mock_drop:
            with patch(
                "back.core.graphdb.delta.materialize.optimize_table"
            ) as mock_optimize:
                store._rebuild_adjacency_table(relation, adj_out, "out")

        mock_drop.assert_called_once_with(client, adj_out, kind="view")
        ctas_sql, = _executed_statements(client)
        assert adj_out in ctas_sql
        assert "_adj_out USING DELTA" in ctas_sql
        assert "CLUSTER BY (src, predicate)" in ctas_sql
        mock_optimize.assert_called_once_with(client, adj_out)

        client.reset_mock()
        adj_in = "cat.sch.triplestore_mydomain_V1_adj_in"
        with patch("back.core.graphdb.delta.materialize.drop_relation"):
            with patch("back.core.graphdb.delta.materialize.optimize_table") as mock_optimize:
                store._rebuild_adjacency_table(relation, adj_in, "in")

        ctas_sql, = _executed_statements(client)
        assert adj_in in ctas_sql
        assert "_adj_in USING DELTA" in ctas_sql
        assert "CLUSTER BY (dst, predicate)" in ctas_sql
        mock_optimize.assert_called_once_with(client, adj_in)

    def test_rebuild_props_forgets_missing_cache_only_after_success(self):
        client = MagicMock()
        store = DeltaFlatStore(client, domain=_domain())
        props = store.props_table_id("MyDomain_V1")
        remember_missing_props(props)

        store._rebuild_props_table("cat.sch.graph", props)

        assert known_missing_props(props) is False
        assert any("_props USING DELTA" in sql for sql in _executed_statements(client))

    def test_rebuild_props_forgets_missing_cache_when_optimize_fails(self):
        client = MagicMock()
        store = DeltaFlatStore(client, domain=_domain())
        props = store.props_table_id("MyDomain_V1")
        remember_missing_props(props)

        with patch(
            "back.core.graphdb.delta.materialize.optimize_table",
            side_effect=RuntimeError("optimize failed"),
        ):
            store._rebuild_props_table("cat.sch.graph", props)

        assert known_missing_props(props) is False
        assert any("_props USING DELTA" in sql for sql in _executed_statements(client))

    def test_rebuild_props_keeps_missing_cache_when_ctas_fails(self):
        client = MagicMock()
        client.execute_statement.side_effect = RuntimeError("ctas failed")
        store = DeltaFlatStore(client, domain=_domain())
        props = store.props_table_id("MyDomain_V1")
        remember_missing_props(props)

        with pytest.raises(RuntimeError, match="ctas failed"):
            store._rebuild_props_table("cat.sch.graph", props)

        assert known_missing_props(props) is True

    def test_rebuild_adjacency_runs_all_companion_jobs_concurrently(self):
        store = DeltaFlatStore(MagicMock(), domain=_domain())
        barrier = threading.Barrier(5, timeout=2)
        calls = []

        def wait_for_peers(*args):
            calls.append(args)
            barrier.wait()

        with patch.object(store, "_rebuild_adjacency_table", side_effect=wait_for_peers), \
             patch.object(store, "_rebuild_entity_search_table", side_effect=wait_for_peers), \
             patch.object(store, "_rebuild_props_table", side_effect=wait_for_peers):
            store.rebuild_adjacency("MyDomain_V1")

        assert len(calls) == 5

    def test_parallel_rebuild_propagates_companion_failure(self):
        store = DeltaFlatStore(MagicMock(), domain=_domain())

        with patch.object(
            store, "_rebuild_props_table", side_effect=RuntimeError("props failed")
        ):
            with pytest.raises(RuntimeError, match="props failed"):
                store.rebuild_adjacency("MyDomain_V1")

    # ------------------------------------------------------------------
    # _try_optimize helper — kind label preserved, best-effort semantics
    # ------------------------------------------------------------------

    def test_try_optimize_calls_optimize_table(self):
        client = MagicMock()
        store = DeltaFlatStore(client, domain=_domain())
        fqn = "cat.sch.some_table"

        with patch(
            "back.core.graphdb.delta.materialize.optimize_table"
        ) as mock_opt:
            store._try_optimize(fqn, "adjacency table")

        mock_opt.assert_called_once_with(client, fqn)

    def test_try_optimize_logs_kind_on_failure_and_does_not_raise(self):
        store = DeltaFlatStore(MagicMock(), domain=_domain())

        with patch(
            "back.core.graphdb.delta.materialize.optimize_table",
            side_effect=RuntimeError("boom"),
        ), patch(
            "back.core.graphdb.delta.DeltaFlatStore.logger.warning"
        ) as mock_warn:
            store._try_optimize("cat.sch.tbl", "entity-search table")

        mock_warn.assert_called_once()
        # Reconstruct the formatted message from positional args passed to logger.warning
        call_args = mock_warn.call_args.args
        formatted = call_args[0] % call_args[1:]
        assert "entity-search table" in formatted
        assert "cat.sch.tbl" in formatted

    @pytest.mark.parametrize(
        "method,kind",
        [
            ("_rebuild_adjacency_table", "adjacency table"),
            ("_rebuild_props_table", "property table"),
            ("_rebuild_entity_search_table", "entity-search table"),
        ],
    )
    def test_each_rebuild_helper_passes_correct_kind_to_try_optimize(
        self, method, kind
    ):
        client = MagicMock()
        store = DeltaFlatStore(client, domain=_domain())
        recorded: list[str] = []

        def _capture(fqn, k):
            recorded.append(k)

        with patch.object(store, "_try_optimize", side_effect=_capture):
            if method == "_rebuild_adjacency_table":
                store._rebuild_adjacency_table(
                    "cat.sch.graph", "cat.sch.tbl_adj_out", "out"
                )
            elif method == "_rebuild_props_table":
                store._rebuild_props_table("cat.sch.graph", "cat.sch.tbl_props")
            else:
                store._rebuild_entity_search_table(
                    "cat.sch.graph", "cat.sch.tbl_entity_search"
                )

        assert recorded == [kind]


class TestDeltaSingleStatementExpansion:
    RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

    @staticmethod
    def _store(rows=None):
        store = DeltaFlatStore(MagicMock())
        store.execute_query = MagicMock(return_value=rows or [])
        store.table_exists = MagicMock(return_value=False)
        return store

    def test_depth_three_uses_one_statement_and_three_levels(self):
        store = self._store(
            [
                {
                    "subject": "s",
                    "predicate": self.RDF_TYPE,
                    "object": "T",
                    "_ob_expanded_count": 4,
                }
            ]
        )

        result = store.expand_and_fetch_subgraph(
            "cat.sch.graph", ["http://ex/a"], 3, 3000, 100000
        )

        store.execute_query.assert_called_once()
        sql = store.execute_query.call_args.args[0]
        assert all(f"level_{level}" in sql for level in range(4))
        assert "LEFT ANTI JOIN visited_3" in sql
        assert "LIMIT 3001" in sql
        assert "LIMIT 100001" in sql
        assert result["expanded_count"] == 4

    def test_depth_zero_has_no_neighbor_level(self):
        store = self._store()

        store.expand_and_fetch_subgraph(
            "cat.sch.graph", ["http://ex/a"], 0, 100, 200
        )

        sql = store.execute_query.call_args.args[0]
        assert "level_0" in sql
        assert "level_1_candidates" not in sql

    def test_query_traverses_both_directions_and_only_typed_neighbors(self):
        store = self._store()

        store.expand_and_fetch_subgraph(
            "cat.sch.graph", ["http://ex/a"], 1, 100, 200
        )

        sql = store.execute_query.call_args.args[0]
        assert "t.subject = frontier.entity" in sql
        assert "t.object = frontier.entity" in sql
        assert f"typed.predicate = '{self.RDF_TYPE}'" in sql
        assert self.RDFS_LABEL in sql

    def test_seed_uris_are_escaped(self):
        store = self._store()

        store.expand_and_fetch_subgraph(
            "cat.sch.graph", ["http://ex/O'Brien"], 1, 100, 200
        )

        assert "O''Brien" in store.execute_query.call_args.args[0]

    def test_extra_triple_row_marks_result_capped(self):
        rows = [
            {
                "subject": f"s{index}",
                "predicate": "p",
                "object": "o",
                "_ob_expanded_count": 3,
            }
            for index in range(3)
        ]
        store = self._store(rows)

        result = store.expand_and_fetch_subgraph("g", ["s0"], 0, 10, 2)

        assert len(result["results"]) == 2
        assert result["capped"] is True
        assert all(
            "_ob_expanded_count" not in row for row in result["results"]
        )

    def test_entity_probe_marks_result_capped(self):
        store = self._store(
            [
                {
                    "subject": "s",
                    "predicate": "p",
                    "object": "o",
                    "_ob_expanded_count": 11,
                }
            ]
        )

        result = store.expand_and_fetch_subgraph("g", ["s"], 1, 10, 20)

        assert result["expanded_count"] == 10
        assert result["capped"] is True

    def test_when_adj_ready_expansion_uses_adj_tables_for_hops(self):
        store = DeltaFlatStore(MagicMock(), domain=_domain())
        store.execute_query = MagicMock(return_value=[])
        store.table_exists = MagicMock(return_value=True)

        store.expand_and_fetch_subgraph(
            "cat.sch.triplestore_mydomain_V1_data", ["http://ex/a"], 2, 50, 100
        )

        sql = store.execute_query.call_args.args[0]
        assert "FROM cat.sch.triplestore_mydomain_V1_adj_out t" in sql
        assert "FROM cat.sch.triplestore_mydomain_V1_adj_in t" in sql
        assert "FROM cat.sch.triplestore_mydomain_V1_props triples" in sql
        assert "FROM cat.sch.triplestore_mydomain_V1_data triples" not in sql
        assert "t.subject = frontier.entity" not in sql
        assert "t.object = frontier.entity" not in sql

    def test_when_props_is_missing_expansion_retries_with_spo(self):
        store = DeltaFlatStore(MagicMock(), domain=_domain())
        store.table_exists = MagicMock(return_value=True)
        store.execute_query = MagicMock(
            side_effect=[
                RuntimeError(
                    "TABLE_OR_VIEW_NOT_FOUND: "
                    "cat.sch.triplestore_mydomain_V1_props"
                ),
                [],
            ]
        )

        store.expand_and_fetch_subgraph(
            "cat.sch.triplestore_mydomain_V1_data", ["http://ex/a"], 1, 50, 100
        )

        assert store.execute_query.call_count == 2
        first_sql, fallback_sql = [
            call.args[0] for call in store.execute_query.call_args_list
        ]
        assert "FROM cat.sch.triplestore_mydomain_V1_props triples" in first_sql
        assert "FROM cat.sch.triplestore_mydomain_V1_data triples" in fallback_sql

    def test_when_props_is_known_missing_expansion_skips_props_sql(self):
        store = DeltaFlatStore(MagicMock(), domain=_domain())
        store.table_exists = MagicMock(return_value=True)
        store.execute_query = MagicMock(
            side_effect=[
                RuntimeError(
                    "TABLE_OR_VIEW_NOT_FOUND: "
                    "cat.sch.triplestore_mydomain_V1_props"
                ),
                [],
                [],
            ]
        )

        for _ in range(2):
            store.expand_and_fetch_subgraph(
                "cat.sch.triplestore_mydomain_V1_data",
                ["http://ex/a"],
                1,
                50,
                100,
            )

        assert store.execute_query.call_count == 3
        second_expand_sql = store.execute_query.call_args_list[2].args[0]
        assert (
            "FROM cat.sch.triplestore_mydomain_V1_props triples"
            not in second_expand_sql
        )
        assert (
            "FROM cat.sch.triplestore_mydomain_V1_data triples"
            in second_expand_sql
        )

    def test_unrelated_props_query_failure_propagates(self):
        store = DeltaFlatStore(MagicMock(), domain=_domain())
        store.table_exists = MagicMock(return_value=True)
        store.execute_query = MagicMock(side_effect=RuntimeError("permission denied"))

        with pytest.raises(RuntimeError, match="permission denied"):
            store.expand_and_fetch_subgraph(
                "cat.sch.triplestore_mydomain_V1_data",
                ["http://ex/a"],
                1,
                50,
                100,
            )


class _InlineSql:
    uses_inline_statements = True

    def __init__(self):
        self.statements = []

    def execute_statement(self, sql):
        self.statements.append(sql)
        return True


class _AppsClient:
    def __init__(self):
        self.sql = _InlineSql()

    def execute_statement(self, sql):
        return self.sql.execute_statement(sql)

    def get_sql_connection_params(self):
        raise AssertionError("Apps inserts must not open a Thrift session")


class TestDeltaFlatStoreAppsInsert:
    def test_insert_uses_statement_execution_not_thrift(self):
        client = _AppsClient()
        store = DeltaFlatStore(client)
        triples = [
            {
                "subject": "http://ex/s",
                "predicate": "http://ex/p",
                "object": "http://ex/o",
            }
        ]
        count = store._execute_insert_triples("cat.sch.g_inferred", triples, 2000, None)
        assert count == 1
        assert client.sql.statements
        assert "INSERT INTO cat.sch.g_inferred" in client.sql.statements[0]
