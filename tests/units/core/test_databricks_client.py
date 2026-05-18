"""Tests for Databricks client."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from back.core.databricks import (
    DatabricksClient,
    get_workspace_host,
    is_databricks_app,
    normalize_host,
)
from back.core.errors import ValidationError


class TestHelperFunctions:
    def test_is_databricks_app_false(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        assert is_databricks_app() is False

    def test_is_databricks_app_true(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        assert is_databricks_app() is True

    def test_normalize_host_empty(self):
        assert normalize_host("") == ""

    def test_normalize_host_adds_https(self):
        assert normalize_host("test.databricks.com") == "https://test.databricks.com"

    def test_normalize_host_strips_trailing_slash(self):
        assert (
            normalize_host("https://test.databricks.com/")
            == "https://test.databricks.com"
        )

    def test_normalize_host_keeps_https(self):
        assert normalize_host("https://already.good.com") == "https://already.good.com"

    def test_get_workspace_host_from_env(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "https://env.databricks.com")
        assert get_workspace_host() == "https://env.databricks.com"


class TestDatabricksClientInit:
    def test_init_explicit(self):
        client = DatabricksClient(
            host="https://example.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse",
        )
        assert client.host == "https://example.databricks.com"
        assert client.token == "test-token"
        assert client.warehouse_id == "test-warehouse"

    def test_init_from_env(self, monkeypatch):
        """Host and token resolve from env via DatabricksAuth; warehouse_id must be passed explicitly."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://env.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
        client = DatabricksClient(warehouse_id="env-warehouse")
        assert client.host == "https://env.databricks.com"
        assert client.token == "env-token"
        assert client.warehouse_id == "env-warehouse"

    def test_has_valid_auth_with_token(self):
        client = DatabricksClient(host="https://h", token="tok", warehouse_id="wh")
        assert client.has_valid_auth() is True

    def test_has_valid_auth_no_token(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        client = DatabricksClient(host="https://h", token="", warehouse_id="wh")
        assert client.has_valid_auth() is False


class TestTestConnection:
    def test_missing_warehouse(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
        client = DatabricksClient(host="https://h", token="tok", warehouse_id="")
        success, msg = client.test_connection()
        assert success is False
        assert "Warehouse" in msg

    def test_missing_auth(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        client = DatabricksClient(host="https://h", token="", warehouse_id="wh")
        success, msg = client.test_connection()
        assert success is False

    @patch("databricks.sql.connect")
    def test_success(self, mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn = MagicMock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        client = DatabricksClient(host="https://h", token="tok", warehouse_id="wh")
        success, msg = client.test_connection()
        assert success is True
        assert "successful" in msg

    @patch("databricks.sql.connect", side_effect=Exception("fail"))
    def test_failure(self, mock_connect):
        client = DatabricksClient(host="https://h", token="tok", warehouse_id="wh")
        success, msg = client.test_connection()
        assert success is False
        assert "fail" in msg


class TestGetCatalogs:
    def test_missing_warehouse_raises(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
        client = DatabricksClient(host="https://h", token="tok", warehouse_id="")
        with pytest.raises(ValidationError):
            client.get_catalogs()

    @patch("databricks.sql.connect")
    def test_returns_catalogs(self, mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [["cat1"], ["cat2"]]
        mock_conn = MagicMock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        client = DatabricksClient(host="https://h", token="tok", warehouse_id="wh")
        catalogs = client.get_catalogs()
        assert catalogs == ["cat1", "cat2"]
        mock_cursor.execute.assert_called_once_with("SHOW CATALOGS")


class TestGetSchemas:
    @patch("databricks.sql.connect")
    def test_returns_schemas(self, mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [["s1"], ["s2"]]
        mock_conn = MagicMock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        client = DatabricksClient(host="https://h", token="tok", warehouse_id="wh")
        schemas = client.get_schemas("my_catalog")
        assert schemas == ["s1", "s2"]


class TestGetTables:
    @patch("databricks.sql.connect")
    def test_returns_tables(self, mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [["db", "tbl1", "x"], ["db", "tbl2", "y"]]
        mock_conn = MagicMock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        client = DatabricksClient(host="https://h", token="tok", warehouse_id="wh")
        tables = client.get_tables("cat", "sch")
        assert tables == ["tbl1", "tbl2"]


class TestGetTableColumns:
    @patch("databricks.sql.connect")
    def test_returns_columns(self, mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ["id", "int", "pk"],
            ["name", "string", ""],
        ]
        mock_conn = MagicMock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        client = DatabricksClient(host="https://h", token="tok", warehouse_id="wh")
        cols = client.get_table_columns("cat", "sch", "tbl")
        assert len(cols) == 2
        assert cols[0] == {"name": "id", "type": "int", "comment": "pk"}
        assert cols[1] == {"name": "name", "type": "string", "comment": ""}
