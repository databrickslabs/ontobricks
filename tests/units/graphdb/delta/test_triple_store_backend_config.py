"""Tests for triple_store_backend global config."""

from unittest.mock import MagicMock, patch

from back.core.helpers import effective_graph_query_table
from back.objects.session.GlobalConfigService import GlobalConfigService

REGISTRY = {"catalog": "c", "schema": "s", "volume": "v"}


class TestTripleStoreBackendConfig:
    def test_allowed_backends(self):
        assert "databricks" in GlobalConfigService.ALLOWED_TRIPLE_STORE_BACKENDS
        assert "lakebase" in GlobalConfigService.ALLOWED_TRIPLE_STORE_BACKENDS

    def test_default_is_lakebase(self):
        gcs = GlobalConfigService()
        empty = gcs._empty()
        assert empty.get("triple_store_backend") == "lakebase"

    def test_set_invalid_rejected(self):
        gcs = GlobalConfigService()
        ok, msg = gcs.set_triple_store_backend("", "", REGISTRY, "neo4j")
        assert not ok
        assert "Unknown" in msg


class TestEffectiveGraphQueryTable:
    def test_databricks_include_inferred_returns_graph_view_fqn(self):
        domain = MagicMock()
        domain.info = {"name": "Cust360"}
        domain.current_version = 5
        domain.delta = {"catalog": "benoit_cayla", "schema": "ontobricks"}
        with patch(
            "back.core.triplestore.TripleStoreFactory.TripleStoreFactory._resolve_triple_store_backend",
            return_value="databricks",
        ):
            table = effective_graph_query_table(domain, include_inferred=True)
        assert table == "benoit_cayla.ontobricks.triplestore_cust360_V5_graph"

    def test_databricks_exclude_inferred_returns_data_table_fqn(self):
        domain = MagicMock()
        domain.info = {"name": "Cust360"}
        domain.current_version = 5
        domain.delta = {"catalog": "benoit_cayla", "schema": "ontobricks"}
        with patch(
            "back.core.triplestore.TripleStoreFactory.TripleStoreFactory._resolve_triple_store_backend",
            return_value="databricks",
        ):
            table = effective_graph_query_table(domain, include_inferred=False)
        assert table == "benoit_cayla.ontobricks.triplestore_cust360_V5_data"

    def test_lakebase_returns_logical_graph_name(self):
        domain = MagicMock()
        domain.info = {"name": "Cust360"}
        domain.current_version = 5
        domain.delta = {}
        with patch(
            "back.core.triplestore.TripleStoreFactory.TripleStoreFactory._resolve_triple_store_backend",
            return_value="lakebase",
        ):
            table = effective_graph_query_table(domain)
        assert table == "Cust360_V5"
