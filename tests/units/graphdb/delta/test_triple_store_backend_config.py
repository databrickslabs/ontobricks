"""Tests for the per-domain graph backend resolution."""

from unittest.mock import MagicMock, patch

from back.core.helpers import effective_graph_query_table
from back.core.graphdb.GraphDBFactory import (
    GRAPH_BACKENDS,
    LAKEHOUSE_MATERIALIZATIONS,
    GraphDBFactory,
    normalize_graph_backend,
    normalize_lakehouse_materialization,
)

REGISTRY = {"catalog": "c", "schema": "s", "volume": "v"}


class TestGraphBackendVocabulary:
    def test_allowed_backends(self):
        assert set(GRAPH_BACKENDS) == {"lakebase", "databricks", "neo4j"}

    def test_normalize_defaults_to_lakebase(self):
        assert normalize_graph_backend(None) == "lakebase"
        assert normalize_graph_backend("") == "lakebase"
        assert normalize_graph_backend("unknown") == "lakebase"

    def test_normalize_case_insensitive(self):
        assert normalize_graph_backend("  NEO4J ") == "neo4j"
        assert normalize_graph_backend("Databricks") == "databricks"


class TestPerDomainResolution:
    def _domain(self, backend):
        d = MagicMock()
        d.info = {"graph_backend": backend}
        return d

    def test_lakebase_maps_to_lakebase(self):
        d = self._domain("lakebase")
        assert GraphDBFactory._resolve_triple_store_backend(d) == "lakebase"
        assert GraphDBFactory._resolve_graph_engine(d) == "lakebase"

    def test_databricks_maps_to_delta_backend(self):
        d = self._domain("databricks")
        assert GraphDBFactory._resolve_triple_store_backend(d) == "databricks"

    def test_neo4j_maps_to_neo4j_engine(self):
        d = self._domain("neo4j")
        assert GraphDBFactory._resolve_triple_store_backend(d) == "lakebase"
        assert GraphDBFactory._resolve_graph_engine(d) == "neo4j"

    def test_missing_defaults_to_lakebase(self):
        d = MagicMock()
        d.info = {}
        assert GraphDBFactory._resolve_triple_store_backend(d) == "lakebase"
        assert GraphDBFactory._resolve_graph_engine(d) == "lakebase"


class TestLakehouseMaterialization:
    """``…_data`` is a copy unless a Lakehouse domain asked otherwise."""

    def test_allowed_modes(self):
        assert set(LAKEHOUSE_MATERIALIZATIONS) == {"table", "view"}

    def test_normalize_defaults_to_table(self):
        assert normalize_lakehouse_materialization(None) == "table"
        assert normalize_lakehouse_materialization("") == "table"
        assert normalize_lakehouse_materialization("materialized") == "table"

    def test_normalize_case_insensitive(self):
        assert normalize_lakehouse_materialization(" VIEW ") == "view"

    def _domain(self, backend, materialization=None):
        d = MagicMock()
        d.info = {"graph_backend": backend}
        if materialization is not None:
            d.info["lakehouse_materialization"] = materialization
        return d

    def test_lakehouse_honours_view_only(self):
        d = self._domain("databricks", "view")
        assert GraphDBFactory.resolve_lakehouse_materialization(d) == "view"

    def test_lakehouse_defaults_to_table(self):
        d = self._domain("databricks")
        assert GraphDBFactory.resolve_lakehouse_materialization(d) == "table"

    def test_other_backends_ignore_the_setting(self):
        """Their graph is not what analytics reads, so the snapshot is mandatory."""
        for backend in ("lakebase", "neo4j"):
            d = self._domain(backend, "view")
            assert GraphDBFactory.resolve_lakehouse_materialization(d) == "table"


class TestEffectiveGraphQueryTable:
    def test_databricks_include_inferred_returns_graph_view_fqn(self):
        domain = MagicMock()
        domain.info = {"name": "Cust360"}
        domain.current_version = 5
        domain.delta = {"catalog": "benoit_cayla", "schema": "ontobricks"}
        with patch(
            "back.core.graphdb.GraphDBFactory.GraphDBFactory._resolve_triple_store_backend",
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
            "back.core.graphdb.GraphDBFactory.GraphDBFactory._resolve_triple_store_backend",
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
            "back.core.graphdb.GraphDBFactory.GraphDBFactory._resolve_triple_store_backend",
            return_value="lakebase",
        ):
            table = effective_graph_query_table(domain)
        assert table == "Cust360_V5"
