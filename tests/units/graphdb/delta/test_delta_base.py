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


class TestCreateDatabricksClientWarehouseOverride:
    """``warehouse_id`` swaps only the warehouse on the read branch."""

    @staticmethod
    def _create(*, for_write=False, warehouse_id="wh-build", app_mode=False):
        from types import SimpleNamespace

        from back.core.databricks.request_identity import (
            RequestIdentity,
            reset_request_identity,
            set_request_identity,
        )

        captured = {}
        resolve_query_wh = MagicMock(return_value="wh-rt")

        class FakeClient:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        tok = set_request_identity(
            RequestIdentity(email="u@x", user_token="USER_TOKEN", domain_role="viewer")
        )
        try:
            with patch.object(
                delta_base, "is_databricks_app", return_value=app_mode
            ), patch.object(
                delta_base,
                "get_databricks_host_and_token",
                return_value=("https://h", "SP_TOKEN"),
            ), patch.object(
                delta_base,
                "get_build_sql_credentials",
                return_value=("https://h", "SP_TOKEN", "wh-from-credentials"),
            ), patch.object(
                delta_base, "resolve_delta_warehouse_id", resolve_query_wh
            ), patch.object(
                delta_base, "resolve_lakehouse_use_sea", return_value=True
            ), patch.object(
                delta_base, "resolve_build_use_sea", return_value=False
            ), patch.object(
                delta_base, "resolve_use_cloud_fetch", return_value=False
            ), patch(
                "back.core.databricks.DatabricksClient", FakeClient
            ):
                delta_base.create_databricks_client(
                    SimpleNamespace(),
                    SimpleNamespace(),
                    for_write=for_write,
                    warehouse_id=warehouse_id,
                )
        finally:
            reset_request_identity(tok)
        return captured, resolve_query_wh

    def test_override_replaces_the_query_warehouse(self):
        captured, resolve_query_wh = self._create()

        assert captured["warehouse_id"] == "wh-build"
        resolve_query_wh.assert_not_called()

    def test_override_forces_classic_transport(self):
        # The lakehouse config says use_sea=True; the build warehouse is Thrift.
        captured, _ = self._create()

        assert captured["use_sea"] is False

    def test_override_keeps_the_callers_obo_token(self):
        captured, _ = self._create(app_mode=True)

        assert captured["token"] == "USER_TOKEN"

    def test_override_keeps_cloud_fetch_handling(self):
        captured, _ = self._create()

        assert captured["use_cloud_fetch"] is False

    def test_no_override_still_resolves_the_query_warehouse(self):
        captured, resolve_query_wh = self._create(warehouse_id=None)

        assert captured["warehouse_id"] == "wh-rt"
        assert captured["use_sea"] is True
        resolve_query_wh.assert_called_once()

    def test_override_is_ignored_on_the_write_branch(self):
        captured, _ = self._create(for_write=True)

        assert captured["warehouse_id"] == "wh-from-credentials"
