"""Contracts for ``POST /dtwin/adjacency/refresh``."""

from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.core.errors import ValidationError

pytestmark = pytest.mark.unit


class _Request:
    async def json(self):
        return {}


class _Domain:
    def __init__(self, backend: str):
        self.info = {"graph_backend": backend}
        self.triplestore = {}
        self.last_build = "2026-01-01T00:00:00Z"
        self.last_update = "2026-01-01T00:00:00Z"

    def save(self):
        return None


class _TaskManager:
    def __init__(self):
        self.created = []
        self.completed = []
        self.failed = []
        self.started = []
        self.progress_messages: list[tuple[str, int, str]] = []

    def create_task(self, **kwargs):
        self.created.append(kwargs)
        return SimpleNamespace(id="task-1")

    def start_task(self, task_id, message=""):
        self.started.append((task_id, message))

    def update_progress(self, task_id, pct, message=""):
        self.progress_messages.append((task_id, pct, message))
        return None

    def complete_task(self, task_id, result=None, message=""):
        self.completed.append((task_id, result, message))

    def fail_task(self, task_id, error):
        self.failed.append((task_id, error))


class _ImmediateThread:
    def __init__(self, target=None, **_kwargs):
        self._target = target

    def start(self):
        self._target()


def _patch_common(monkeypatch, backend: str):
    from api.routers.internal import dtwin
    import back.core.task_manager as task_manager
    import back.core.helpers as helpers_pkg

    tm = _TaskManager()
    domain = _Domain(backend)

    monkeypatch.setattr(dtwin, "get_domain", lambda _mgr: domain)
    monkeypatch.setattr(dtwin, "DomainSnapshot", lambda d: d)
    monkeypatch.setattr(task_manager, "get_task_manager", lambda: tm)
    monkeypatch.setattr(threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        helpers_pkg, "effective_graph_name", lambda _d: "catalog.schema.graph"
    )
    return dtwin, tm, domain


class TestAdjacencyRefreshAllowedBackends:
    @pytest.mark.parametrize("backend", ["lakebase", "databricks"])
    async def test_allowed_backend_starts_background_task_and_completes(
        self, monkeypatch, backend
    ):
        dtwin, tm, _domain = _patch_common(monkeypatch, backend)
        import back.core.graphdb as graphdb_pkg

        store = MagicMock()
        store.supports_adjacency = True
        store.rebuild_adjacency = MagicMock()
        store.apply_data_relation = MagicMock()
        store.materialize_data_relation = MagicMock()
        store.purge_materialized_triples = MagicMock()
        store_calls = []

        def _get_graphdb(snap, settings, *, for_write=False):
            store_calls.append((snap, settings, for_write))
            return store

        monkeypatch.setattr(graphdb_pkg, "get_graphdb", _get_graphdb)

        resp = await dtwin.refresh_adjacency_only(
            request=_Request(), session_mgr=object(), settings=object()
        )

        assert resp["success"] is True
        assert resp["task_id"] == "task-1"
        assert tm.created[0]["task_type"] == "adjacency_refresh"
        assert store_calls[0][2] is (backend == "databricks")
        store.rebuild_adjacency.assert_called_once_with("catalog.schema.graph")
        store.apply_data_relation.assert_not_called()
        store.materialize_data_relation.assert_not_called()
        store.purge_materialized_triples.assert_not_called()
        assert tm.failed == []
        assert tm.completed[0][1] == {"mode": "adjacency_only", "backend": backend}

    async def test_empty_post_does_not_require_json_body(self, monkeypatch):
        dtwin, tm, _domain = _patch_common(monkeypatch, "lakebase")
        import back.core.graphdb as graphdb_pkg

        class _ExplodingRequest:
            async def json(self):
                raise AssertionError("route must not parse request body")

        store = MagicMock()
        store.supports_adjacency = True
        store.rebuild_adjacency = MagicMock()
        monkeypatch.setattr(
            graphdb_pkg, "get_graphdb", lambda snap, settings, **_kwargs: store
        )

        resp = await dtwin.refresh_adjacency_only(
            request=_ExplodingRequest(), session_mgr=object(), settings=object()
        )

        assert resp["success"] is True
        assert tm.failed == []


class TestAdjacencyRefreshRejectedBackends:
    @pytest.mark.parametrize("backend", ["neo4j", "none"])
    async def test_rejects_unsupported_graph_backend(self, monkeypatch, backend):
        dtwin, _tm, _domain = _patch_common(monkeypatch, backend)
        import back.core.graphdb as graphdb_pkg

        monkeypatch.setattr(
            graphdb_pkg,
            "get_graphdb",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("get_graphdb must not be called")
            ),
        )

        with pytest.raises(ValidationError, match="Adjacency refresh is only available"):
            await dtwin.refresh_adjacency_only(
                request=_Request(), session_mgr=object(), settings=object()
            )


class TestAdjacencyRefreshFailures:
    async def test_marks_task_failed_when_backend_does_not_support_adjacency(
        self, monkeypatch
    ):
        dtwin, tm, _domain = _patch_common(monkeypatch, "lakebase")
        import back.core.graphdb as graphdb_pkg

        store = MagicMock()
        store.supports_adjacency = False
        monkeypatch.setattr(
            graphdb_pkg, "get_graphdb", lambda snap, settings, **_kwargs: store
        )

        resp = await dtwin.refresh_adjacency_only(
            request=_Request(), session_mgr=object(), settings=object()
        )

        assert resp["success"] is True
        assert tm.completed == []
        assert tm.failed
        assert "does not support adjacency rebuild" in tm.failed[0][1]

    async def test_marks_task_failed_when_rebuild_raises(self, monkeypatch):
        dtwin, tm, _domain = _patch_common(monkeypatch, "databricks")
        import back.core.graphdb as graphdb_pkg

        store = MagicMock()
        store.supports_adjacency = True
        store.rebuild_adjacency = MagicMock(side_effect=RuntimeError("warehouse timeout"))
        monkeypatch.setattr(
            graphdb_pkg, "get_graphdb", lambda snap, settings, **_kwargs: store
        )

        resp = await dtwin.refresh_adjacency_only(
            request=_Request(), session_mgr=object(), settings=object()
        )

        assert resp["success"] is True
        assert tm.completed == []
        assert tm.failed
        assert "Adjacency refresh failed" in tm.failed[0][1]
        assert "warehouse timeout" in tm.failed[0][1]


class TestAdjacencyRefreshProgressWording:
    """Progress message must be backend-specific: parallel for Databricks, sequential for Lakebase."""

    async def test_databricks_progress_message_says_parallel(self, monkeypatch):
        dtwin, tm, _domain = _patch_common(monkeypatch, "databricks")
        import back.core.graphdb as graphdb_pkg

        store = MagicMock()
        store.supports_adjacency = True
        store.rebuild_adjacency = MagicMock()
        monkeypatch.setattr(
            graphdb_pkg, "get_graphdb", lambda snap, settings, **_kwargs: store
        )

        await dtwin.refresh_adjacency_only(
            request=_Request(), session_mgr=object(), settings=object()
        )

        rebuild_msgs = [msg for _, pct, msg in tm.progress_messages if pct == 70]
        assert rebuild_msgs, "No progress message at pct=70 found"
        assert any("in parallel" in msg for msg in rebuild_msgs), (
            f"Expected 'in parallel' in Databricks progress message, got: {rebuild_msgs}"
        )
        assert not any("sequentially" in msg for msg in rebuild_msgs)

    async def test_lakebase_progress_message_says_sequentially(self, monkeypatch):
        dtwin, tm, _domain = _patch_common(monkeypatch, "lakebase")
        import back.core.graphdb as graphdb_pkg

        store = MagicMock()
        store.supports_adjacency = True
        store.rebuild_adjacency = MagicMock()
        monkeypatch.setattr(
            graphdb_pkg, "get_graphdb", lambda snap, settings, **_kwargs: store
        )

        await dtwin.refresh_adjacency_only(
            request=_Request(), session_mgr=object(), settings=object()
        )

        rebuild_msgs = [msg for _, pct, msg in tm.progress_messages if pct == 70]
        assert rebuild_msgs, "No progress message at pct=70 found"
        assert any("sequentially" in msg for msg in rebuild_msgs), (
            f"Expected 'sequentially' in Lakebase progress message, got: {rebuild_msgs}"
        )
        assert not any("in parallel" in msg for msg in rebuild_msgs)
