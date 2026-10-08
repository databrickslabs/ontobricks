"""Tests for graphdb/delta table naming and materialize SQL."""

from unittest.mock import MagicMock

import pytest

from back.core.graphdb.adjacency import typed_in_select, typed_out_select
from back.core.graphdb.entity_search import entity_search_select
from back.core.graphdb.props import props_select
from back.core.graphdb.delta import _table_naming, materialize
from back.core.graphdb.delta import health


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

    def test_adj_fqns(self):
        domain = _domain()
        assert _table_naming.adj_out_fqn(domain) == (
            "cat.sch.triplestore_mydomain_V1_adj_out"
        )
        assert _table_naming.adj_in_fqn(domain) == (
            "cat.sch.triplestore_mydomain_V1_adj_in"
        )

    def test_entity_search_fqn(self):
        domain = _domain()
        assert _table_naming.entity_search_fqn(domain) == (
            "cat.sch.triplestore_mydomain_V1_entity_search"
        )

    def test_entity_search_asserted_fqn(self):
        domain = _domain()
        assert _table_naming.entity_search_asserted_fqn(domain) == (
            "cat.sch.triplestore_mydomain_V1_entity_search_asserted"
        )

    def test_props_fqn(self):
        domain = _domain()
        assert _table_naming.props_fqn(domain) == (
            "cat.sch.triplestore_mydomain_V1_props"
        )

    def test_analytics_snapshot_suffix(self):
        domain = _domain()
        view = _table_naming.view_fqn(domain)
        snapshot = _table_naming.analytics_snapshot_fqn(domain)
        assert snapshot == view + "_analytics"

    def test_analytics_snapshot_needs_a_qualified_view(self):
        """An unqualified name would make the job's CTAS land somewhere random."""
        domain = _domain(catalog="", schema="")
        assert _table_naming.analytics_snapshot_fqn(domain) == ""

    def test_the_snapshot_is_named_off_the_gateway_not_the_data_relation(self):
        """It must not end up as ``…_data_analytics`` — that breaks grouping."""
        domain = _domain()
        snapshot = _table_naming.analytics_snapshot_fqn(domain)
        assert "_data" not in snapshot


class TestMaterializeSql:
    def test_ctas_includes_cluster_by(self):
        sql = materialize.build_ctas_sql("cat.sch.view1", "cat.sch.view1_data")
        assert "CREATE OR REPLACE TABLE cat.sch.view1_data" in sql
        assert "CLUSTER BY (predicate, subject)" in sql
        assert "FROM cat.sch.view1" in sql
        assert "(subject STRING" not in sql

    def test_adj_ctas_clusters_src_for_out(self):
        sql = materialize.build_adj_ctas_sql(
            "cat.sch.g_graph", "cat.sch.g_adj_out", "out"
        )
        assert "CREATE OR REPLACE TABLE cat.sch.g_adj_out USING DELTA" in sql
        assert "CLUSTER BY (src, predicate)" in sql
        assert typed_out_select("cat.sch.g_graph") in sql
        assert "typed.predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'" in sql
        assert "t.predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'" in sql
        assert "t.predicate != 'http://www.w3.org/2000/01/rdf-schema#label'" in sql
        assert "t.object LIKE 'http%'" in sql

    def test_adj_ctas_clusters_dst_for_in(self):
        sql = materialize.build_adj_ctas_sql(
            "cat.sch.g_graph", "cat.sch.g_adj_in", "in"
        )
        assert "CREATE OR REPLACE TABLE cat.sch.g_adj_in USING DELTA" in sql
        assert "CLUSTER BY (dst, predicate)" in sql
        assert typed_in_select("cat.sch.g_graph") in sql
        assert "typed.predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'" in sql
        assert "t.predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'" in sql
        assert "t.predicate != 'http://www.w3.org/2000/01/rdf-schema#label'" in sql
        assert "t.object LIKE 'http%'" in sql

    def test_adj_ctas_out_does_not_append_duplicate_from_suffix(self):
        sql = materialize.build_adj_ctas_sql(
            "cat.sch.g_graph", "cat.sch.g_adj_out", "out"
        )
        expected = (
            "CREATE OR REPLACE TABLE cat.sch.g_adj_out USING DELTA "
            "CLUSTER BY (src, predicate) "
            f"AS {typed_out_select('cat.sch.g_graph')}"
        )
        assert sql == expected

    def test_adj_ctas_in_does_not_append_duplicate_from_suffix(self):
        sql = materialize.build_adj_ctas_sql(
            "cat.sch.g_graph", "cat.sch.g_adj_in", "in"
        )
        expected = (
            "CREATE OR REPLACE TABLE cat.sch.g_adj_in USING DELTA "
            "CLUSTER BY (dst, predicate) "
            f"AS {typed_in_select('cat.sch.g_graph')}"
        )
        assert sql == expected

    def test_entity_search_ctas_clusters_type_without_duplicate_from(self):
        select_sql = entity_search_select("cat.sch.g_graph")
        sql = materialize.build_entity_search_ctas_sql(
            "cat.sch.g_graph", "cat.sch.g_entity_search"
        )
        assert sql == (
            "CREATE OR REPLACE TABLE cat.sch.g_entity_search USING DELTA "
            "CLUSTER BY (type_uri, label_lc) "
            f"AS {select_sql}"
        )

    def test_set_bloom_filter_columns_emits_tblproperties(self):
        client = MagicMock()
        materialize.set_bloom_filter_columns(
            client, "cat.sch.g_entity_search", "label_lc,uri_lc"
        )
        sql = client.execute_statement.call_args[0][0]
        assert "ALTER TABLE cat.sch.g_entity_search SET TBLPROPERTIES" in sql
        assert "'delta.bloomFilter.columns' = 'label_lc,uri_lc'" in sql

    def test_set_bloom_filter_columns_swallows_alter_errors(self):
        client = MagicMock()
        client.execute_statement.side_effect = RuntimeError("no bloom")
        materialize.set_bloom_filter_columns(
            client, "cat.sch.g_entity_search", "label_lc,uri_lc"
        )

    def test_props_ctas_clusters_subject_without_duplicate_from(self):
        select_sql = props_select("cat.sch.g_graph")
        sql = materialize.build_props_ctas_sql(
            "cat.sch.g_graph", "cat.sch.g_props"
        )
        assert sql == (
            "CREATE OR REPLACE TABLE cat.sch.g_props USING DELTA "
            "CLUSTER BY (subject) "
            f"AS {select_sql}"
        )

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

    def test_run_sql_falls_back_to_execute_query(self):
        client = MagicMock(spec=["execute_query"])
        materialize.ensure_inferred_table(client, "c.s.v_inferred")
        client.execute_query.assert_called_once()
        assert "CREATE TABLE IF NOT EXISTS c.s.v_inferred" in (
            client.execute_query.call_args[0][0]
        )

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

    def test_data_view_sql_copies_nothing(self):
        sql = materialize.build_data_view_sql("cat.sch.view1", "cat.sch.view1_data")
        assert "CREATE OR REPLACE VIEW cat.sch.view1_data" in sql
        assert "FROM cat.sch.view1" in sql
        # A view has no storage, so neither clustering nor a column schema
        # applies — emitting either would fail on the warehouse.
        assert "CLUSTER BY" not in sql
        assert "USING DELTA" not in sql


class TestApplyDataRelation:
    """Both modes must clear the relation of the *other* kind first.

    Databricks refuses to replace a TABLE with a VIEW and vice versa, so
    without the cross-drop a domain could be built once and then never switch
    materialization without a manual DROP in the workspace.
    """

    @staticmethod
    def _statements(client):
        return [call[0][0] for call in client.execute_statement.call_args_list]

    def test_view_mode_drops_the_stale_table_then_creates_the_view(self):
        client = MagicMock()
        materialize.apply_data_relation(
            client, "c.s.v", "c.s.v_data", mode="view"
        )
        assert self._statements(client) == [
            "DROP TABLE IF EXISTS c.s.v_data",
            "CREATE OR REPLACE VIEW c.s.v_data AS "
            "SELECT subject, predicate, object FROM c.s.v",
        ]

    def test_table_mode_drops_the_stale_view_then_materializes(self):
        client = MagicMock()
        materialize.apply_data_relation(
            client, "c.s.v", "c.s.v_data", mode="table"
        )
        statements = self._statements(client)
        assert statements[0] == "DROP VIEW IF EXISTS c.s.v_data"
        assert "CREATE OR REPLACE TABLE c.s.v_data" in statements[1]
        assert len(statements) == 2

    def test_an_unknown_mode_materializes(self):
        """Anything but ``view`` is the safe default: a real table."""
        client = MagicMock()
        materialize.apply_data_relation(
            client, "c.s.v", "c.s.v_data", mode="nonsense"
        )
        assert "CREATE OR REPLACE TABLE" in self._statements(client)[1]

    def test_a_failed_cross_drop_does_not_abort_the_build(self):
        """The dropped relation usually does not exist at all."""
        client = MagicMock()
        client.execute_statement.side_effect = [
            RuntimeError("no such view"),
            None,
        ]
        materialize.apply_data_relation(
            client, "c.s.v", "c.s.v_data", mode="table"
        )
        assert "CREATE OR REPLACE TABLE" in self._statements(client)[1]

    def test_a_failed_create_is_raised(self):
        """A build that could not produce ..._data must not report success."""
        client = MagicMock()
        client.execute_statement.side_effect = [None, RuntimeError("no permission")]
        with pytest.raises(RuntimeError, match="no permission"):
            materialize.apply_data_relation(
                client, "c.s.v", "c.s.v_data", mode="view"
            )


class TestSchemaPermissionSummary:
    def test_schema_permission_normalizes_and_preserves_inherited_source(self):
        summary = health.schema_permission_summary(
            "main",
            "graph",
            "app-client-id",
            [
                {"privilege": "USE_CATALOG", "inherited_from": "main"},
                {"privilege": "USE_SCHEMA", "inherited_from": ""},
            ],
        )

        assert summary["permissions"][0] == {
            "name": "USE CATALOG",
            "granted": True,
            "inherited_from": "main",
        }
        assert summary["permissions"][1] == {
            "name": "USE SCHEMA",
            "granted": True,
            "inherited_from": "",
        }
        assert summary["operational"] is False
        assert summary["registry_catalog"] == "main"
        assert summary["registry_schema"] == "graph"
        assert summary["storage_location"] == "main.graph"
        assert summary["principal"] == "app-client-id"

    def test_schema_permission_all_privileges_satisfies_required_set(self):
        summary = health.schema_permission_summary(
            "main",
            "graph",
            "app-client-id",
            [{"privilege": "ALL_PRIVILEGES", "inherited_from": "metastore"}],
        )

        assert summary["operational"] is True
        assert [p["name"] for p in summary["permissions"]] == [
            "USE CATALOG",
            "USE SCHEMA",
            "CREATE TABLE",
            "CREATE VIEW",
            "SELECT",
            "MODIFY",
        ]
        assert all(p["granted"] for p in summary["permissions"])
        assert all(p["inherited_from"] == "metastore" for p in summary["permissions"])

    def test_schema_permission_required_order_and_name_normalization(self):
        summary = health.schema_permission_summary(
            "main",
            "graph",
            "app-client-id",
            [
                {"privilege": "CREATE_VIEW", "inherited_from": ""},
                {"privilege": "CREATE TABLE", "inherited_from": ""},
                {"privilege": "USE_SCHEMA", "inherited_from": ""},
                {"privilege": "MODIFY", "inherited_from": ""},
                {"privilege": "SELECT", "inherited_from": ""},
                {"privilege": "USE CATALOG", "inherited_from": ""},
            ],
        )

        assert [p["name"] for p in summary["permissions"]] == [
            "USE CATALOG",
            "USE SCHEMA",
            "CREATE TABLE",
            "CREATE VIEW",
            "SELECT",
            "MODIFY",
        ]
        assert summary["operational"] is True

    def test_schema_permission_empty_registry_location_when_incomplete(self):
        summary = health.schema_permission_summary(
            "main",
            "",
            "app-client-id",
            [{"privilege": "USE_SCHEMA", "inherited_from": ""}],
        )
        assert summary["storage_location"] == ""
