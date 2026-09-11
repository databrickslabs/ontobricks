"""Tests for DeltaFlatStore inferred companion routing."""

from unittest.mock import MagicMock, patch

from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore


def _domain(name="MyDomain", version=1, catalog="cat", schema="sch"):
    d = MagicMock()
    d.info = {"name": name}
    d.current_version = version
    d.delta = {"catalog": catalog, "schema": schema}
    return d


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


class TestDeltaSingleStatementExpansion:
    RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

    @staticmethod
    def _store(rows=None):
        store = DeltaFlatStore(MagicMock())
        store.execute_query = MagicMock(return_value=rows or [])
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
