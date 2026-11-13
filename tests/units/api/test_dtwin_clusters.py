"""Clusters routes: detection and materialization are background tasks, reads
are bounded, and the preflight reason reaches the client."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from api.routers.internal import dtwin
from back.core.errors import ValidationError

pytestmark = pytest.mark.unit


class _Request:
    def __init__(self, body=None):
        self._body = body

    async def json(self):
        if self._body is None:
            raise ValueError("no body")
        return self._body


@pytest.fixture
def wired(monkeypatch):
    dt = MagicMock()
    tm = MagicMock()
    tm.create_task.return_value = SimpleNamespace(id="t-1")
    started = []

    class _Thread:
        def __init__(self, target, daemon):
            self.target = target

        def start(self):
            started.append(self.target)

    monkeypatch.setattr(dtwin, "assert_domain_graph_read", lambda r: None)
    monkeypatch.setattr(dtwin, "get_domain", lambda sm: "domain")
    monkeypatch.setattr(dtwin, "_cluster_graph", lambda d, s: ("store", "g"))
    monkeypatch.setattr(dtwin, "community_job_status", lambda d, s: (True, ""))
    monkeypatch.setattr(dtwin, "TwinAnalytics", MagicMock(return_value=dt))
    tasks = MagicMock()
    monkeypatch.setattr(dtwin, "TwinBackgroundTasks", tasks)
    monkeypatch.setattr("back.core.task_manager.get_task_manager", lambda: tm)
    monkeypatch.setattr("threading.Thread", _Thread)

    async def _inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(dtwin, "run_blocking", _inline)
    return SimpleNamespace(dt=dt, tm=tm, started=started, tasks=tasks)


async def test_detect_starts_a_community_task(wired):
    out = await dtwin.detect_clusters(_Request({"class_filter": ["C"]}), "sm", "settings")
    assert out["task_id"] == "t-1"
    assert wired.tm.create_task.call_args.kwargs["task_type"] == "community_detection"
    wired.started[0]()
    args, kwargs = wired.tasks.run_community_task.call_args
    assert args[-1] == "g"
    assert kwargs == {"class_filter": ["C"], "predicate_filter": None}


async def test_detect_surfaces_the_preflight_reason(wired, monkeypatch):
    monkeypatch.setattr(dtwin, "community_job_status", lambda d, s: (False, "Build first"))
    with pytest.raises(ValidationError, match="Build first"):
        await dtwin.detect_clusters(_Request(), "sm", "settings")
    assert not wired.started


async def test_detect_rejects_a_malformed_filter(wired):
    with pytest.raises(ValidationError, match="class_filter"):
        await dtwin.detect_clusters(_Request({"class_filter": "C"}), "sm", "settings")


async def test_latest_reports_preflight_with_the_run(wired, monkeypatch):
    monkeypatch.setattr(dtwin, "community_job_status", lambda d, s: (False, "toggle off"))
    wired.dt.latest_clusters.return_value = {"clusters": [], "stats": None, "graph": None}
    out = await dtwin.latest_clusters(_Request(), 20, "sm", "settings")
    wired.dt.latest_clusters.assert_called_once_with("g", "settings", 20)
    assert out["job_available"] is False
    assert out["blocked_reason"] == "toggle off"


async def test_members_reads_the_requested_source(wired):
    wired.dt.cluster_members.return_value = {"members": []}
    wired.dt.materialized_members.return_value = {"members": ["m"]}
    await dtwin.cluster_members(_Request(), 3, 100, 50, "latest", "sm", "settings")
    wired.dt.cluster_members.assert_called_once_with("g", 3, 100, 50, "settings")
    out = await dtwin.cluster_members(_Request(), 3, 0, 50, "materialized", "sm", "settings")
    assert out["members"] == ["m"]
    wired.dt.materialized_members.assert_called_once_with("store", "g", 3, 0, 50)


async def test_assignments_for_drawn_uris(wired):
    wired.dt.cluster_assignments.return_value = {"u": 1}
    out = await dtwin.cluster_assignments(_Request({"uris": ["u", "v"]}), "sm", "settings")
    assert out["assignments"] == {"u": 1}
    wired.dt.cluster_assignments.assert_called_once_with("store", "g", ["u", "v"])


async def test_materialize_needs_a_run(wired):
    wired.dt.detection_summary.return_value = None
    with pytest.raises(ValidationError, match="Run Detect first"):
        await dtwin.materialize_clusters(_Request(), "sm", "settings")
    assert not wired.started


async def test_materialize_starts_a_task(wired):
    wired.dt.detection_summary.return_value = {"cluster_count": 2}
    out = await dtwin.materialize_clusters(_Request(), "sm", "settings")
    assert out["task_id"] == "t-1"
    assert wired.tm.create_task.call_args.kwargs["task_type"] == "community_materialize"
    wired.started[0]()
    assert wired.tasks.run_community_materialize_task.call_args.args[-2:] == ("store", "g")
