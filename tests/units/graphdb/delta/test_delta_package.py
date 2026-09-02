"""Tests for graphdb/delta table naming and materialize SQL."""

from unittest.mock import MagicMock, patch

import pytest

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


class TestSettingsHealthSummary:
    @staticmethod
    def _summary(info=None):
        from back.core.graphdb.delta.health import settings_health_summary

        domain = MagicMock()
        domain.info = info if info is not None else {}
        domain.delta = {}
        domain.current_version = 1
        # No warehouse: the card's registry and materialization fields are
        # resolved without one, and a MagicMock domain would otherwise have
        # the SQL connector dial out with mock credentials.
        with patch(
            "back.core.graphdb.delta.DeltaBase.create_databricks_client",
            return_value=None,
        ):
            return settings_health_summary(
                domain,
                registry_cfg={"catalog": "reg_cat", "schema": "reg_sch"},
            )

    def test_includes_registry_location_without_domain_tables(self):
        summary = self._summary()
        assert summary["registry_catalog"] == "reg_cat"
        assert summary["registry_schema"] == "reg_sch"
        assert summary["storage_location"] == "reg_cat.reg_sch"
        assert summary["registry_configured"] is True

    def test_reports_the_materialization_so_the_card_can_label_data(self):
        """The card says "Data TABLE"; on a view-only domain that is wrong."""
        assert (
            self._summary(
                {
                    "name": "Dom",
                    "graph_backend": "databricks",
                    "lakehouse_materialization": "view",
                }
            )["materialization"]
            == "view"
        )

    def test_defaults_the_materialization_to_table(self):
        assert self._summary()["materialization"] == "table"


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
