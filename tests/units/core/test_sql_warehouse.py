"""Tests for SQLWarehouse (Databricks SQL Warehouse helper)."""

import importlib
import pytest
from unittest.mock import Mock, MagicMock, patch

_databricks_auth_mod = importlib.import_module("back.core.databricks.DatabricksAuth")

from databricks.sql.exc import ServerOperationError

from back.core.databricks.DatabricksAuth import DatabricksAuth
from back.core.databricks.SQLWarehouse import (
    SQLWarehouse,
    _SQL_MAX_ATTEMPTS,
    _is_retryable_transport_error,
)
from back.core.errors import ValidationError

_SEA_500 = "Error during request to server. SEA HTTP request failed with status 500"


def _make_connect_mock(
    *,
    fetchone_value=None,
    description=None,
    fetchall_rows=None,
    fetchmany_batches=None,
):
    """Build a mock chain for ``databricks.sql.connect`` context manager."""
    mock_cursor = MagicMock()
    if fetchone_value is not None:
        mock_cursor.fetchone.return_value = fetchone_value
    if description is not None:
        mock_cursor.description = description
    if fetchall_rows is not None:
        mock_cursor.fetchall.return_value = fetchall_rows
    if fetchmany_batches is not None:
        # Yield each batch, then an empty list to terminate the loop.
        mock_cursor.fetchmany.side_effect = list(fetchmany_batches) + [[]]

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


class TestRetryClassifier:
    """`_is_retryable_transport_error` must retry only transient transport
    failures and never real SQL / config errors."""

    def test_sea_5xx_is_retryable(self):
        assert _is_retryable_transport_error(Exception(_SEA_500)) is True

    def test_service_unavailable_is_retryable(self):
        assert _is_retryable_transport_error(Exception("503 Service Unavailable")) is True

    def test_connection_reset_is_retryable(self):
        assert _is_retryable_transport_error(ConnectionResetError("connection reset by peer")) is True

    def test_server_operation_error_not_retryable(self):
        exc = ServerOperationError("BAD_REQUEST: [TABLE_OR_VIEW_NOT_FOUND] missing")
        assert _is_retryable_transport_error(exc) is False

    def test_thrift_not_supported_not_retryable(self):
        # Config error on RT warehouse — retrying can never succeed.
        exc = Exception("BAD_REQUEST: Lakehouse/RT is not supported for Thrift protocol")
        assert _is_retryable_transport_error(exc) is False

    def test_plain_sql_error_not_retryable(self):
        assert _is_retryable_transport_error(Exception("syntax error near FROM")) is False


class TestExecuteQueryRetry:
    @patch("back.core.databricks.SQLWarehouse.time.sleep", return_value=None)
    @patch("databricks.sql.connect")
    def test_retries_transient_then_succeeds(self, mock_connect, _sleep, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        good_conn, _ = _make_connect_mock(
            description=[("x",)], fetchall_rows=[(1,)]
        )
        # First borrow raises a transient SEA 500 (warehouse cold-start),
        # second borrow reconnects and succeeds.
        mock_connect.side_effect = [Exception(_SEA_500), good_conn]

        auth = DatabricksAuth(
            host="https://h.databricks.com", token="tok", warehouse_id="wh-rt"
        )
        sw = SQLWarehouse(auth)
        rows = sw.execute_query("SELECT x FROM t")
        assert rows == [{"x": 1}]
        assert mock_connect.call_count == 2
        _sleep.assert_called_once()

    @patch("back.core.databricks.SQLWarehouse.time.sleep", return_value=None)
    @patch("databricks.sql.connect")
    def test_does_not_retry_real_sql_error(self, mock_connect, _sleep, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_connect.side_effect = ServerOperationError("BAD_REQUEST: table not found")

        auth = DatabricksAuth(
            host="https://h.databricks.com", token="tok", warehouse_id="wh-1"
        )
        sw = SQLWarehouse(auth)
        with pytest.raises(ServerOperationError):
            sw.execute_query("SELECT * FROM missing")
        assert mock_connect.call_count == 1
        _sleep.assert_not_called()

    @patch("back.core.databricks.SQLWarehouse.time.sleep", return_value=None)
    @patch("databricks.sql.connect")
    def test_gives_up_after_max_attempts(self, mock_connect, _sleep, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_connect.side_effect = Exception(_SEA_500)

        auth = DatabricksAuth(
            host="https://h.databricks.com", token="tok", warehouse_id="wh-rt"
        )
        sw = SQLWarehouse(auth)
        with pytest.raises(Exception, match="status 500"):
            sw.execute_query("SELECT 1")
        assert mock_connect.call_count == _SQL_MAX_ATTEMPTS


class TestExecuteStatementRetry:
    @patch("back.core.databricks.SQLWarehouse.time.sleep", return_value=None)
    @patch("databricks.sql.connect")
    def test_retries_transient_then_succeeds(self, mock_connect, _sleep, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        good_conn, _ = _make_connect_mock()
        mock_connect.side_effect = [Exception(_SEA_500), good_conn]

        auth = DatabricksAuth(
            host="https://h.databricks.com", token="tok", warehouse_id="wh-rt"
        )
        sw = SQLWarehouse(auth)
        assert sw.execute_statement("CREATE OR REPLACE VIEW v AS SELECT 1") is True
        assert mock_connect.call_count == 2


class TestSeaStatementTimeout:
    """On the SEA backend (Lakehouse/RT warehouses) a ``SET STATEMENT_TIMEOUT``
    statement returns HTTP 500 when cloud-fetch is off, so it must be skipped;
    classic Thrift warehouses keep the server-side bound."""

    @patch("databricks.sql.connect")
    def test_skips_set_timeout_on_sea(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_conn, mock_cursor = _make_connect_mock(
            description=[("x",)], fetchall_rows=[(1,)]
        )
        mock_connect.return_value = mock_conn

        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-rt",
            use_sea=True,
        )
        sw = SQLWarehouse(auth)
        rows = sw.execute_query("SELECT x FROM t", statement_timeout_s=30)
        assert rows == [{"x": 1}]
        executed = [c.args[0] for c in mock_cursor.execute.call_args_list]
        assert executed == ["SELECT x FROM t"]
        assert not any("STATEMENT_TIMEOUT" in q for q in executed)

    @patch("databricks.sql.connect")
    def test_applies_set_timeout_on_thrift(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_conn, mock_cursor = _make_connect_mock(
            description=[("x",)], fetchall_rows=[(1,)]
        )
        mock_connect.return_value = mock_conn

        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
            use_sea=False,
        )
        sw = SQLWarehouse(auth)
        sw.execute_query("SELECT x FROM t", statement_timeout_s=30)
        executed = [c.args[0] for c in mock_cursor.execute.call_args_list]
        assert any("SET STATEMENT_TIMEOUT = 30" in q for q in executed)
        assert any("SET STATEMENT_TIMEOUT = 0" in q for q in executed)


class TestIterRows:
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
            list(sw.iter_rows("SELECT 1"))

    def test_rejects_non_positive_batch_size(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        with pytest.raises(ValidationError, match="batch_size"):
            list(sw.iter_rows("SELECT 1", batch_size=0))

    @patch("databricks.sql.connect")
    def test_streams_rows_in_batches(self, mock_connect, monkeypatch):
        """``iter_rows`` must use ``fetchmany`` and never call ``fetchall``."""
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        mock_conn, mock_cursor = _make_connect_mock(
            description=[("subject",), ("predicate",), ("object",)],
            fetchmany_batches=[
                [("s1", "p", "o1"), ("s2", "p", "o2")],
                [("s3", "p", "o3")],
            ],
        )
        mock_connect.return_value = mock_conn

        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-1",
        )
        sw = SQLWarehouse(auth)
        rows = list(sw.iter_rows("SELECT subject, predicate, object FROM v", batch_size=2))
        assert rows == [
            {"subject": "s1", "predicate": "p", "object": "o1"},
            {"subject": "s2", "predicate": "p", "object": "o2"},
            {"subject": "s3", "predicate": "p", "object": "o3"},
        ]
        mock_cursor.fetchall.assert_not_called()
        # 2 fetchmany calls returning data + 1 returning [] to terminate.
        assert mock_cursor.fetchmany.call_count == 3


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
        mock_conn.commit.assert_called_once()

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

    @patch(
        "databricks.sql.connect",
        side_effect=Exception(
            "BAD_REQUEST: Lakehouse/RT is not supported for Thrift protocol"
        ),
    )
    def test_maps_rt_thrift_error_to_actionable_hint(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        auth = DatabricksAuth(
            host="https://h.databricks.com",
            token="tok",
            warehouse_id="wh-rt",
        )
        sw = SQLWarehouse(auth)
        ok, msg = sw.create_or_replace_view("c", "s", "v", "SELECT 1")
        assert ok is False
        assert "Lakehouse RT" in msg
        assert "Thrift" not in msg or "Lakehouse RT" in msg


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
