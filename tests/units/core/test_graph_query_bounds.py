"""Regression tests for the Graph Chat event-loop-hang fix.

Covers the two guards that stop a broad Graph Chat query from freezing the app:

* the SQL warehouse graph-read path issues (and resets) a session
  ``STATEMENT_TIMEOUT`` when bounded, and never touches it otherwise;
* the blocking thread pool auto-sizes from the instance's vCPU count and reports
  saturation for the resource-pressure advisory.
"""

import importlib
from unittest.mock import MagicMock, Mock, patch

import pytest

from back.core.databricks.DatabricksAuth import DatabricksAuth
from back.core.databricks.SQLWarehouse import SQLWarehouse

# Import the *module* explicitly: the ``back.core.helpers`` package re-exports a
# ``DatabricksHelpers`` class under the same name, which otherwise shadows the
# submodule when accessed as an attribute.
dh = importlib.import_module("back.core.helpers.DatabricksHelpers")


def _make_conn(description, rows):
    cur = MagicMock()
    cur.description = description
    cur.fetchall.return_value = rows
    conn = MagicMock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    conn.cursor.return_value.__enter__ = Mock(return_value=cur)
    conn.cursor.return_value.__exit__ = Mock(return_value=False)
    return conn, cur


def _executed(cur):
    return [str(c.args[0]) for c in cur.execute.call_args_list]


class TestWarehouseStatementTimeout:
    @patch("databricks.sql.connect")
    def test_bounded_sets_and_resets_timeout(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        conn, cur = _make_conn([("id",)], [(1,)])
        mock_connect.return_value = conn
        sw = SQLWarehouse(
            DatabricksAuth(host="https://h.databricks.com", token="t", warehouse_id="wh-1")
        )

        rows = sw.execute_query("SELECT id FROM t", statement_timeout_s=45)

        assert rows == [{"id": 1}]
        stmts = _executed(cur)
        assert any("STATEMENT_TIMEOUT = 45" in s for s in stmts)
        # Reset to 0 so the pooled connection doesn't leak the bound.
        assert any("STATEMENT_TIMEOUT = 0" in s for s in stmts)

    @patch("databricks.sql.connect")
    def test_unbounded_leaves_timeout_untouched(self, mock_connect, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        conn, cur = _make_conn([("id",)], [(1,)])
        mock_connect.return_value = conn
        sw = SQLWarehouse(
            DatabricksAuth(host="https://h.databricks.com", token="t", warehouse_id="wh-1")
        )

        sw.execute_query("SELECT id FROM t")

        assert all("STATEMENT_TIMEOUT" not in s for s in _executed(cur))


class TestDeltaStoreBoundsReads:
    def test_execute_query_passes_timeout_to_warehouse(self, monkeypatch):
        from back.core.triplestore.delta.DeltaTripleStore import DeltaTripleStore

        monkeypatch.setattr(
            "back.core.query_limits.get_graph_query_timeout_s", lambda: 42
        )
        sql_service = MagicMock()
        sql_service.execute_query.return_value = [{"n": 1}]
        client = MagicMock()
        client.sql = sql_service

        store = DeltaTripleStore(client)
        out = store.execute_query("SELECT 1")

        assert out == [{"n": 1}]
        sql_service.execute_query.assert_called_once_with(
            "SELECT 1", statement_timeout_s=42
        )


class TestLakebaseStatementTimeoutReset:
    """The pool hands out ``autocommit=True`` connections, so a read's
    ``statement_timeout`` must be reset or it leaks to the next borrower
    (a bulk write/DDL) and cancels it mid-flight."""

    def _store(self, cur):
        from back.core.graphdb.lakebase.LakebaseFlatStore import LakebaseFlatStore

        store = object.__new__(LakebaseFlatStore)
        store._schema = "s"
        conn = MagicMock()
        conn.__enter__ = Mock(return_value=conn)
        conn.__exit__ = Mock(return_value=False)
        conn.cursor.return_value.__enter__ = Mock(return_value=cur)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        pool = MagicMock()
        pool.connection.return_value = conn
        store._pool = lambda: pool
        store._require_pg = lambda: (None, dict)
        return store

    def test_resets_after_success(self, monkeypatch):
        monkeypatch.setattr(
            "back.core.query_limits.get_graph_query_timeout_s", lambda: 30
        )
        cur = MagicMock()
        cur.description = [("s",)]
        cur.fetchall.return_value = [{"s": "x"}]

        out = self._store(cur).execute_query("SELECT 1")

        assert out == [{"s": "x"}]
        stmts = _executed(cur)
        assert any("SET statement_timeout = 30000" in s for s in stmts)
        assert stmts[-1] == "RESET statement_timeout"

    def test_resets_even_when_query_raises(self, monkeypatch):
        monkeypatch.setattr(
            "back.core.query_limits.get_graph_query_timeout_s", lambda: 30
        )
        cur = MagicMock()
        cur.description = None

        def _exec(sql, *a, **k):
            if sql == "BOOM":
                raise RuntimeError("cancelled by statement_timeout")

        cur.execute.side_effect = _exec

        with pytest.raises(RuntimeError):
            self._store(cur).execute_query("BOOM")

        assert "RESET statement_timeout" in _executed(cur)


class TestBlockingPoolAutoTune:
    def test_explicit_env_size_wins(self, monkeypatch):
        monkeypatch.setenv("ONTOBRICKS_THREAD_POOL_SIZE", "7")
        assert dh._resolve_blocking_pool_size() == 7

    def test_derives_from_cpu_count(self, monkeypatch):
        monkeypatch.delenv("ONTOBRICKS_THREAD_POOL_SIZE", raising=False)
        monkeypatch.setattr(dh.os, "cpu_count", lambda: 16)
        assert dh._resolve_blocking_pool_size() == 64

    def test_floored_at_minimum(self, monkeypatch):
        monkeypatch.delenv("ONTOBRICKS_THREAD_POOL_SIZE", raising=False)
        monkeypatch.setattr(dh.os, "cpu_count", lambda: 1)
        assert dh._resolve_blocking_pool_size() == dh._BLOCKING_POOL_MIN

    def test_invalid_env_falls_back_to_cpu(self, monkeypatch):
        monkeypatch.setenv("ONTOBRICKS_THREAD_POOL_SIZE", "abc")
        monkeypatch.setattr(dh.os, "cpu_count", lambda: 8)
        assert dh._resolve_blocking_pool_size() == 32


class TestBlockingPoolStats:
    def test_stats_shape_and_idle(self, monkeypatch):
        monkeypatch.setattr(dh, "_inflight_blocking", 0)
        stats = dh.get_blocking_pool_stats()
        assert set(stats) >= {"max_workers", "active", "peak", "saturated"}
        assert stats["active"] == 0
        assert stats["saturated"] is False

    def test_saturation_flag(self, monkeypatch):
        monkeypatch.setattr(dh, "_inflight_blocking", dh._BLOCKING_POOL_SIZE)
        assert dh.get_blocking_pool_stats()["saturated"] is True
