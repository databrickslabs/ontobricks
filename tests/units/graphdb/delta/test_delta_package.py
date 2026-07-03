"""Tests for graphdb/delta table naming and materialize SQL."""

from unittest.mock import MagicMock

import pytest

from back.core.graphdb.delta import _table_naming, materialize


def _domain(name="MyDomain", version=1, catalog="cat", schema="sch"):
    d = MagicMock()
    d.info = {"name": name}
    d.current_version = version
    d.delta = {"catalog": catalog, "schema": schema}
    return d


class TestTableNaming:
    def test_data_table_suffix(self):
        domain = _domain()
        view = _table_naming.view_fqn(domain)
        data = _table_naming.data_table_fqn(domain)
        assert view.endswith("_V1")
        assert data == view + "_data"

    def test_inferred_table_suffix(self):
        domain = _domain()
        view = _table_naming.view_fqn(domain)
        inferred = _table_naming.inferred_table_fqn(domain)
        assert inferred == view + "_inferred"

    def test_graph_view_suffix(self):
        domain = _domain()
        view = _table_naming.view_fqn(domain)
        graph = _table_naming.graph_view_fqn(domain)
        assert graph == view + "_graph"


class TestMaterializeSql:
    def test_ctas_includes_cluster_by(self):
        sql = materialize.build_ctas_sql("cat.sch.view1", "cat.sch.view1_data")
        assert "CREATE OR REPLACE TABLE cat.sch.view1_data" in sql
        assert "CLUSTER BY (predicate, subject)" in sql
        assert "FROM cat.sch.view1" in sql
        assert "(subject STRING" not in sql

    def test_materialize_from_view_executes(self):
        client = MagicMock()
        materialize.materialize_from_view(client, "c.s.v", "c.s.v_data")
        client.execute_statement.assert_called_once()
        assert "CREATE OR REPLACE TABLE" in client.execute_statement.call_args[0][0]

    def test_ensure_inferred_table_executes(self):
        client = MagicMock()
        materialize.ensure_inferred_table(client, "c.s.v_inferred")
        client.execute_statement.assert_called_once()
        sql = client.execute_statement.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS c.s.v_inferred" in sql

    def test_ensure_graph_view_unions_data_and_inferred(self):
        client = MagicMock()
        materialize.ensure_graph_view(
            client, "c.s.v_graph", "c.s.v_data", "c.s.v_inferred"
        )
        client.execute_statement.assert_called_once()
        sql = client.execute_statement.call_args[0][0]
        assert "CREATE OR REPLACE VIEW c.s.v_graph" in sql
        assert "FROM c.s.v_data" in sql
        assert "FROM c.s.v_inferred" in sql


class TestSettingsHealthSummary:
    def test_includes_registry_location_without_domain_tables(self):
        from back.core.graphdb.delta.health import settings_health_summary

        domain = MagicMock()
        domain.info = {}
        domain.delta = {}
        domain.current_version = 1
        summary = settings_health_summary(
            domain,
            registry_cfg={"catalog": "reg_cat", "schema": "reg_sch"},
        )
        assert summary["registry_catalog"] == "reg_cat"
        assert summary["registry_schema"] == "reg_sch"
        assert summary["storage_location"] == "reg_cat.reg_sch"
        assert summary["registry_configured"] is True
