"""Tests for UnityCatalog (Databricks Unity Catalog metadata)."""
import importlib
import pytest
from unittest.mock import MagicMock, Mock, patch

_unity_catalog_mod = importlib.import_module("back.core.databricks.UnityCatalog")

from back.core.databricks.DatabricksAuth import DatabricksAuth
from back.core.databricks.UnityCatalog import UnityCatalog
from back.core.errors import ValidationError


def _make_sql_mocks(mock_connect, *, fetchall=None, fetchone=None):
    """Wire sql.connect context manager and cursor; set fetchall/fetchone on cursor."""
    mock_cursor = MagicMock()
    if fetchall is not None:
        mock_cursor.fetchall.return_value = fetchall
    if fetchone is not None:
        mock_cursor.fetchone.return_value = fetchone
    mock_conn = MagicMock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    return mock_cursor


@pytest.fixture
def clean_databricks_env(monkeypatch):
    """Avoid env-driven host/token/oauth when constructing DatabricksAuth in tests."""
    for key in (
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_APP_PORT",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "DATABRICKS_SQL_WAREHOUSE_ID",
        "DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT",
    ):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def auth_with_warehouse(clean_databricks_env):
    return DatabricksAuth(
        host="https://test.cloud.databricks.com",
        token="test-pat",
        warehouse_id="warehouse-123",
    )


@pytest.fixture
def auth_no_warehouse(clean_databricks_env):
    return DatabricksAuth(
        host="https://test.cloud.databricks.com",
        token="test-pat",
        warehouse_id="",
    )


class TestGetCatalogs:
    def test_raises_value_error_without_warehouse_id(self, auth_no_warehouse):
        uc = UnityCatalog(auth_no_warehouse)
        with pytest.raises(ValidationError, match="SQL Warehouse ID is required"):
            uc.get_catalogs()

    @patch("databricks.sql.connect")
    def test_returns_catalog_names_on_success(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(
            mock_connect, fetchall=[["main"], ["samples"], ["hive_metastore"]]
        )
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.get_catalogs()
        assert out == ["main", "samples", "hive_metastore"]
        mock_cursor.execute.assert_called_once_with("SHOW CATALOGS")


class TestGetSchemas:
    def test_raises_value_error_without_warehouse_id(self, auth_no_warehouse):
        uc = UnityCatalog(auth_no_warehouse)
        with pytest.raises(ValidationError, match="SQL Warehouse ID is required"):
            uc.get_schemas("main")

    @patch("databricks.sql.connect")
    def test_returns_schema_names(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(mock_connect, fetchall=[["default"], ["information_schema"]])
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.get_schemas("main")
        assert out == ["default", "information_schema"]
        mock_cursor.execute.assert_called_once_with("SHOW SCHEMAS IN main")


class TestGetTables:
    @patch("databricks.sql.connect")
    def test_returns_table_names_on_success(self, mock_connect, auth_with_warehouse):
        # SHOW TABLES rows: database, tableName, isTemporary
        mock_cursor = _make_sql_mocks(
            mock_connect,
            fetchall=[["sch", "t1", "false"], ["sch", "t2", "false"]],
        )
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.get_tables("cat", "sch")
        assert out == ["t1", "t2"]
        mock_cursor.execute.assert_called_once_with("SHOW TABLES IN cat.sch")

    @patch(
        "databricks.sql.connect",
        side_effect=RuntimeError("warehouse down"),
    )
    def test_returns_empty_list_on_error(self, _mock_connect, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_tables("cat", "sch") == []


class TestGetTableColumns:
    @patch("databricks.sql.connect")
    def test_returns_list_of_dicts(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(
            mock_connect,
            fetchall=[
                ["id", "bigint", "pk"],
                ["name", "string", None],
                ["x", "int", ""],
            ],
        )
        uc = UnityCatalog(auth_with_warehouse)
        cols = uc.get_table_columns("cat", "sch", "tbl")
        assert cols == [
            {"name": "id", "type": "bigint", "comment": "pk"},
            {"name": "name", "type": "string", "comment": ""},
            {"name": "x", "type": "int", "comment": ""},
        ]
        mock_cursor.execute.assert_called_once_with("DESCRIBE cat.sch.tbl")

    @patch(
        "databricks.sql.connect",
        side_effect=Exception("no table"),
    )
    def test_returns_empty_list_on_error(self, _mock_connect, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_table_columns("c", "s", "t") == []


class TestGetTableComment:
    @patch("databricks.sql.connect")
    def test_returns_comment_string(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(
            mock_connect,
            fetchone=["my table comment"],
        )
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_table_comment("cat", "sch", "tbl") == "my table comment"
        mock_cursor.execute.assert_called_once()
        call_sql = mock_cursor.execute.call_args[0][0]
        assert "information_schema.tables" in call_sql
        assert "cat" in call_sql and "sch" in call_sql and "tbl" in call_sql

    @patch("databricks.sql.connect")
    def test_returns_empty_when_no_row(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(mock_connect)
        mock_cursor.fetchone.return_value = None
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_table_comment("cat", "sch", "tbl") == ""

    @patch(
        "databricks.sql.connect",
        side_effect=Exception("timeout"),
    )
    def test_returns_empty_string_on_error(self, _mock_connect, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_table_comment("c", "s", "t") == ""


class TestGetVolumesSql:
    def test_raises_value_error_without_warehouse_id(self, auth_no_warehouse):
        uc = UnityCatalog(auth_no_warehouse)
        with pytest.raises(ValidationError, match="SQL Warehouse ID is required"):
            uc.get_volumes("main", "default")

    @patch("databricks.sql.connect")
    def test_returns_volume_names(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(
            mock_connect,
            fetchall=[["sch", "vol_a"], ["sch", "vol_b"]],
        )
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.get_volumes("main", "default")
        assert out == ["vol_a", "vol_b"]
        mock_cursor.execute.assert_called_once_with("SHOW VOLUMES IN main.default")


class TestListVolumesRest:
    @patch.object(_unity_catalog_mod.requests, "get")
    def test_returns_names_from_rest_api(self, mock_get, auth_with_warehouse):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {
            "volumes": [
                {"name": "v1", "full_name": "main.default.v1"},
                {"name": "v2"},
            ]
        }
        mock_get.return_value = mock_resp

        uc = UnityCatalog(auth_with_warehouse)
        out = uc.list_volumes("main", "default")
        assert out == ["v1", "v2"]
        mock_get.assert_called_once()
        call_kw = mock_get.call_args
        assert "unity-catalog/volumes" in call_kw[0][0]
        assert call_kw[1]["params"] == {
            "catalog_name": "main",
            "schema_name": "default",
        }

    def test_returns_empty_when_no_auth(self, clean_databricks_env):
        auth = DatabricksAuth(
            host="https://test.cloud.databricks.com",
            token="",
            warehouse_id="wh",
        )
        uc = UnityCatalog(auth)
        assert uc.list_volumes("main", "default") == []

    def test_returns_empty_when_no_host(self, clean_databricks_env):
        auth = DatabricksAuth(
            host="https://test.cloud.databricks.com",
            token="pat",
            warehouse_id="wh",
        )
        auth.host = ""
        uc = UnityCatalog(auth)
        assert uc.list_volumes("main", "default") == []

    @patch.object(_unity_catalog_mod.requests, "get")
    def test_returns_empty_on_http_error(self, mock_get, auth_with_warehouse):
        mock_get.side_effect = Exception("401 Unauthorized")
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.list_volumes("main", "default") == []


class TestCreateVolumeRest:
    @patch.object(_unity_catalog_mod.requests, "post")
    def test_returns_true_on_success(self, mock_post, auth_with_warehouse):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = Mock()
        mock_post.return_value = mock_resp

        uc = UnityCatalog(auth_with_warehouse)
        assert uc.create_volume("main", "default", "my_vol") is True
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert "unity-catalog/volumes" in args[0]
        assert kwargs["json"]["name"] == "my_vol"
        assert kwargs["json"]["volume_type"] == "MANAGED"

    @patch.object(_unity_catalog_mod.requests, "post")
    def test_returns_false_on_error(self, mock_post, auth_with_warehouse):
        mock_post.side_effect = Exception("conflict")
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.create_volume("main", "default", "x") is False

    def test_returns_false_when_no_auth(self, clean_databricks_env):
        auth = DatabricksAuth(
            host="https://test.cloud.databricks.com",
            token="",
            warehouse_id="wh",
        )
        uc = UnityCatalog(auth)
        assert uc.create_volume("main", "default", "v") is False
