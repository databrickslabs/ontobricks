"""Tests for the Delta graph-engine Databricks client wiring."""

from unittest.mock import MagicMock, patch

import back.core.graphdb.delta.DeltaBase as delta_base


class TestCreateDatabricksClientUseSea:
    def test_passes_use_sea_from_lakehouse_config(self):
        captured = {}

        class FakeClient:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        with patch.object(
            delta_base, "get_databricks_host_and_token", return_value=("https://h", "tok")
        ), patch.object(
            delta_base, "resolve_delta_warehouse_id", return_value="wh-rt"
        ), patch.object(
            delta_base, "resolve_lakehouse_use_sea", return_value=True
        ), patch.object(
            delta_base, "resolve_use_cloud_fetch", return_value=False
        ), patch(
            "back.core.databricks.DatabricksClient", FakeClient
        ):
            client = delta_base.create_databricks_client(MagicMock(), settings=MagicMock())

        assert client is not None
        assert captured.get("use_sea") is True
        assert captured.get("warehouse_id") == "wh-rt"
        assert captured.get("use_cloud_fetch") is False

    def test_defaults_use_sea_false(self):
        captured = {}

        class FakeClient:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        with patch.object(
            delta_base, "get_databricks_host_and_token", return_value=("https://h", "tok")
        ), patch.object(
            delta_base, "resolve_delta_warehouse_id", return_value="wh-1"
        ), patch.object(
            delta_base, "resolve_lakehouse_use_sea", return_value=False
        ), patch.object(
            delta_base, "resolve_use_cloud_fetch", return_value=True
        ), patch(
            "back.core.databricks.DatabricksClient", FakeClient
        ):
            delta_base.create_databricks_client(MagicMock(), settings=MagicMock())

        assert captured.get("use_sea") is False

    def test_write_client_uses_build_warehouse_and_transport(self):
        captured = {}

        class FakeClient:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        with patch.object(
            delta_base,
            "get_build_sql_credentials",
            return_value=("https://h", "tok", "wh-build"),
        ), patch.object(
            delta_base, "resolve_build_use_sea", return_value=False
        ), patch.object(
            delta_base,
            "resolve_delta_warehouse_id",
            side_effect=AssertionError("query warehouse must not be resolved"),
        ), patch(
            "back.core.databricks.DatabricksClient", FakeClient
        ):
            client = delta_base.create_databricks_client(
                MagicMock(), settings=MagicMock(), for_write=True
            )

        assert client is not None
        assert captured["warehouse_id"] == "wh-build"
        assert captured["use_sea"] is False
