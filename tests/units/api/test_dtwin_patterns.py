"""Pattern routes: detection/materialize are background tasks; reads are bounded."""

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
    monkeypatch.setattr(dtwin, "pattern_job_status", lambda d, s: (True, ""))
    monkeypatch.setattr(dtwin, "TwinAnalytics", MagicMock(return_value=dt))
    tasks = MagicMock()
    monkeypatch.setattr(dtwin, "TwinBackgroundTasks", tasks)
    monkeypatch.setattr("back.core.task_manager.get_task_manager", lambda: tm)
    monkeypatch.setattr("threading.Thread", _Thread)

    async def _inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(dtwin, "run_blocking", _inline)
    return SimpleNamespace(dt=dt, tm=tm, started=started, tasks=tasks)


async def test_detect_starts_a_pattern_task(wired):
    out = await dtwin.detect_patterns(
        _Request({"class_filter": "http://ex/Customer", "hops": 2}), "sm", "settings"
    )
    assert out["task_id"] == "t-1"
    assert wired.tm.create_task.call_args.kwargs["task_type"] == "pattern_detection"
    wired.started[0]()
    kwargs = wired.tasks.run_pattern_task.call_args.kwargs
    assert kwargs["class_filter"] == "http://ex/Customer"
    assert kwargs["hops"] == 2


async def test_detect_requires_class_filter(wired):
    with pytest.raises(ValidationError, match="class_filter"):
        await dtwin.detect_patterns(_Request({}), "sm", "settings")
    assert not wired.started


async def test_detect_surfaces_the_preflight_reason(wired, monkeypatch):
    monkeypatch.setattr(dtwin, "pattern_job_status", lambda d, s: (False, "Build first"))
    with pytest.raises(ValidationError, match="Build first"):
        await dtwin.detect_patterns(
            _Request({"class_filter": "http://ex/C"}), "sm", "settings"
        )
    assert not wired.started


async def test_latest_reports_preflight(wired, monkeypatch):
    monkeypatch.setattr(dtwin, "pattern_job_status", lambda d, s: (False, "toggle off"))
    wired.dt.latest_patterns.return_value = {
        "patterns": [],
        "stats": None,
        "graph": {"nodes": [], "edges": []},
    }
    out = await dtwin.latest_patterns(_Request(), 20, "sm", "settings")
    wired.dt.latest_patterns.assert_called_once_with("g", "settings", 20)
    assert out["job_available"] is False
    assert out["blocked_reason"] == "toggle off"


async def test_save_needs_a_run(wired):
    wired.dt.pattern_detection_summary.return_value = None
    with pytest.raises(ValidationError, match="Run Detect first"):
        await dtwin.save_pattern(
            _Request({"pattern_id": "abc", "label": "X"}), "sm", "settings"
        )
    assert not wired.started


async def test_save_starts_a_task(wired):
    wired.dt.pattern_detection_summary.return_value = {"pattern_count": 2}
    out = await dtwin.save_pattern(
        _Request({"pattern_id": "abc", "label": "FR buyers"}), "sm", "settings"
    )
    assert out["task_id"] == "t-1"
    assert wired.tm.create_task.call_args.kwargs["task_type"] == "pattern_save"
    wired.started[0]()
    kwargs = wired.tasks.run_pattern_save_task.call_args.kwargs
    assert kwargs["pattern_id"] == "abc"
    assert kwargs["label"] == "FR buyers"


async def test_members_limit_is_capped_by_query(wired):
    wired.dt.pattern_members.return_value = {"members": []}
    await dtwin.pattern_members(_Request(), "abc", 0, 100, "sm", "settings")
    wired.dt.pattern_members.assert_called_once_with(
        "g", "abc", 0, 100, "settings"
    )
