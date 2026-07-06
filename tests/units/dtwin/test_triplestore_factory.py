"""Tests for the merged graph DB factory (auto-resolve + view paths)."""

from unittest.mock import patch, MagicMock

from back.core.graphdb import GraphDBFactory, get_graphdb


def _mock_domain(host="https://h", token="tok", warehouse_id="wh"):
    domain = MagicMock()
    domain.databricks = {"host": host, "token": token, "warehouse_id": warehouse_id}
    domain.info = {"name": "TestDomain"}
    return domain


class TestGetGraphdb:
    def test_unknown_engine_returns_none(self):
        domain = _mock_domain()
        result = get_graphdb(domain, engine="unknown")
        assert result is None

    def test_auto_resolves_to_lakebase(self):
        """When engine is None, auto-resolve dispatches to the lakebase engine."""
        domain = _mock_domain()
        with (
            patch.object(
                GraphDBFactory, "_resolve_triple_store_backend", return_value="lakebase"
            ),
            patch.object(
                GraphDBFactory, "_resolve_graph_engine", return_value="lakebase"
            ),
            patch.object(
                GraphDBFactory, "_resolve_graph_engine_config", return_value={}
            ),
            patch.object(
                GraphDBFactory, "_create_lakebase", return_value=MagicMock()
            ) as mock_lb,
        ):
            result = get_graphdb(domain)
            assert result is not None
            mock_lb.assert_called_once()
            _, kwargs = mock_lb.call_args
            assert kwargs["engine_config"] == {}

    def test_auto_databricks_backend_uses_delta(self):
        domain = _mock_domain()
        with (
            patch.object(
                GraphDBFactory,
                "_resolve_triple_store_backend",
                return_value="databricks",
            ),
            patch.object(
                GraphDBFactory, "_create_delta", return_value=MagicMock()
            ) as mock_delta,
        ):
            result = get_graphdb(domain)
            assert result is not None
            mock_delta.assert_called_once()

    def test_auto_passes_resolved_engine_config(self):
        domain = _mock_domain()
        cfg = {"database": "db1", "schema": "g"}
        with (
            patch.object(
                GraphDBFactory, "_resolve_triple_store_backend", return_value="lakebase"
            ),
            patch.object(
                GraphDBFactory, "_resolve_graph_engine", return_value="lakebase"
            ),
            patch.object(
                GraphDBFactory, "_resolve_graph_engine_config", return_value=cfg
            ),
            patch.object(
                GraphDBFactory, "_create_lakebase", return_value=MagicMock()
            ) as mock_lb,
        ):
            result = get_graphdb(domain)
            assert result is not None
            _, kwargs = mock_lb.call_args
            assert kwargs["engine_config"] == cfg

    def test_view_missing_host_returns_none(self):
        domain = _mock_domain(host="", token="")
        with (
            patch(
                "back.core.helpers.get_databricks_host_and_token",
                return_value=("", ""),
            ),
            patch("back.core.helpers.resolve_warehouse_id", return_value="wh"),
            patch("back.core.databricks.is_databricks_app", return_value=False),
        ):
            result = get_graphdb(
                domain,
                settings=MagicMock(databricks_sql_warehouse_id="wh"),
                engine="view",
            )
        assert result is None

    def test_view_success(self):
        domain = _mock_domain()
        settings = MagicMock()
        settings.databricks_sql_warehouse_id = "wh"
        with (
            patch(
                "back.core.helpers.get_databricks_host_and_token",
                return_value=("https://h", "tok"),
            ),
            patch("back.core.helpers.resolve_warehouse_id", return_value="wh"),
            patch("back.core.databricks.DatabricksClient") as mock_client_cls,
            patch(
                "back.core.graphdb.delta.DeltaFlatStore.DeltaFlatStore"
            ) as mock_delta_cls,
        ):
            mock_client_cls.return_value = MagicMock()
            mock_delta_cls.return_value = MagicMock()
            result = get_graphdb(domain, settings=settings, engine="view")
            assert result is not None
