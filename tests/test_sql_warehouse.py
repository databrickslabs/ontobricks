"""Tests for SQLWarehouse (Databricks SQL Warehouse helper)."""

import importlib
import pytest
from unittest.mock import Mock, MagicMock, patch

_databricks_auth_mod = importlib.import_module("back.core.databricks.DatabricksAuth")

from back.core.databricks.DatabricksAuth import DatabricksAuth
from back.core.databricks.SQLWarehouse import SQLWarehouse
from back.core.errors import ValidationError


def _make_connect_mock(
    *,
    fetchone_value=None,
    description=None,
    fetchall_rows=None,
):
    """Build a mock chain for ``databricks.sql.connect`` context manager."""
    mock_cursor = MagicMock()
    if fetchone_value is not None:
        mock_cursor.fetchone.return_value = fetchone_value
    if description is not None:
        mock_cursor.description = description
    if fetchall_rows is not None:
        mock_cursor.fetchall.return_value = fetchall_rows

    mock_conn = MagicMock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    return mock_conn, mock_cursor


class TestTestConnection:
    def test_missing_warehouse_returns_false(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.test_connection()
        assert ok is False
        assert "Missing SQL Warehouse ID" in msg

    def test_missing_auth_non_app_returns_false(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.test_connection()
        assert ok is False
        assert "DATABRICKS_HOST" in msg or "DATABRICKS_TOKEN" in msg

    def test_missing_auth_app_mode_returns_false(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.test_connection()
        assert ok is False
        assert "OAuth" in msg or "CLIENT_ID" in msg

    @patch("databricks.sql.connect")
    def test_success_mocks_sql_connect(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_conn, _ = _make_connect_mock(fetchone_value=[1])
        mock_connect.return_value = mock_conn

        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="pat-token",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.test_connection()
        assert ok is True
        assert "successful" in msg.lower()
        mock_connect.assert_called_once()

    @patch(
        "databricks.sql.connect",
        side_effect=RuntimeError("connection refused"),
    )
    def test_failure_when_sql_connect_raises(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="pat-token",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.test_connection()
        assert ok is False
        assert "Connection failed" in msg
        assert "connection refused" in msg


class TestExecuteQuery:
    def test_raises_value_error_if_no_warehouse_id(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="",
        )
        sw = SQLWarehouse(auth)
        with pytest.raises(ValidationError, match="SQL Warehouse ID"):
            sw.execute_query("SELECT 1")

    @patch("databricks.sql.connect")
    def test_returns_list_of_dicts_on_success(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_conn, _ = _make_connect_mock(
            description=[("id",), ("name",)],
            fetchall_rows=[(1, "a"), (2, "b")],
        )
        mock_connect.return_value = mock_conn

        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        rows = sw.execute_query("SELECT id, name FROM t")
        assert rows == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
        ]

    @patch("databricks.sql.connect", side_effect=Exception("syntax error"))
    def test_raises_on_sql_error(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        with pytest.raises(Exception, match="syntax error"):
            sw.execute_query("BAD SQL")


class TestExecuteStatement:
    def test_raises_value_error_if_no_warehouse_id(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="",
        )
        sw = SQLWarehouse(auth)
        with pytest.raises(ValidationError, match="SQL Warehouse ID"):
            sw.execute_statement("DROP TABLE x")

    @patch("databricks.sql.connect")
    def test_returns_true_on_success(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_conn, mock_cursor = _make_connect_mock()
        mock_connect.return_value = mock_conn

        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        assert sw.execute_statement("CREATE TABLE x (i INT)") is True
        mock_cursor.execute.assert_called_once()

    @patch("databricks.sql.connect", side_effect=OSError("network down"))
    def test_raises_on_error(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        with pytest.raises(OSError, match="network down"):
            sw.execute_statement("SELECT 1")


class TestCreateOrReplaceView:
    @patch("databricks.sql.connect")
    def test_returns_true_and_message_on_success(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_conn, _ = _make_connect_mock()
        mock_connect.return_value = mock_conn

        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.create_or_replace_view("c", "s", "v", "SELECT 1")
        assert ok is True
        assert "created successfully" in msg
        assert "`c`.`s`.`v`" in msg

    @patch("databricks.sql.connect", side_effect=Exception("ddl failed"))
    def test_returns_false_and_message_on_failure(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.create_or_replace_view("c", "s", "v", "SELECT 1")
        assert ok is False
        assert "Failed to create view" in msg
        assert "ddl failed" in msg


class TestCreateOrReplaceTableFromQuery:
    @patch("databricks.sql.connect")
    def test_returns_true_and_message_on_success(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_conn, _ = _make_connect_mock()
        mock_connect.return_value = mock_conn

        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.create_or_replace_table_from_query("c", "s", "t", "SELECT 1")
        assert ok is True
        assert "created successfully" in msg
        assert "`c`.`s`.`t`" in msg

    @patch("databricks.sql.connect", side_effect=Exception("ctas failed"))
    def test_returns_false_and_message_on_failure(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.create_or_replace_table_from_query("c", "s", "t", "SELECT 1")
        assert ok is False
        assert "Failed to create table" in msg
        assert "ctas failed" in msg


class TestGetWarehouses:
    @patch("requests.get")
    def test_returns_list_via_rest_when_not_app_mode(self, mock_get, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {
            "warehouses": [
                {"id": "id1", "name": "Warehouse A", "state": "RUNNING"},
                {"id": "id2", "name": "Warehouse B", "state": "STOPPED"},
            ],
        }
        mock_get.return_value = mock_resp

        auth = DatabricksAuth(
            host="https://dbc.example.cloud.databricks.com",
            token="pat",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        result = sw.get_warehouses()

        assert len(result) == 2
        assert result[0] == {"id": "id1", "name": "Warehouse A", "state": "RUNNING"}
        assert result[1] == {"id": "id2", "name": "Warehouse B", "state": "STOPPED"}
        mock_get.assert_called_once()
        assert "sql/warehouses" in mock_get.call_args[0][0]

    def test_returns_empty_list_with_no_host(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        with patch.object(
            _databricks_auth_mod.DatabricksAuth,
            "get_workspace_host",
            return_value="",
        ):
            auth = DatabricksAuth(warehouse_id="wh-1")
            sw = SQLWarehouse(auth)
            assert sw.get_warehouses() == []

    def test_returns_empty_list_with_no_auth(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        assert sw.get_warehouses() == []
