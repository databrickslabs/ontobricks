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
