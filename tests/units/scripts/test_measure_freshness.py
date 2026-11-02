"""``scripts/measure_freshness.py``: timing, percentiles and SLO verdicts, with fakes."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[3] / "scripts/measure_freshness.py"


@pytest.fixture(scope="module")
def script():
    spec = importlib.util.spec_from_file_location("measure_freshness", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Clock:
    """A fake monotonic clock that only moves when the probe sleeps."""

    def __init__(self):
        self.now = 100.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _shows_after(clock: _Clock, delays: list[float | None]):
    """A reader that shows the n-th marker once *delays[n]* seconds passed since its write."""
    state = {"written": None, "index": -1}

    def write(marker: str) -> None:
        state["written"], state["index"] = clock.now, state["index"] + 1

    def visible(marker: str) -> bool:
        delay = delays[state["index"]]
        return delay is not None and clock.now - state["written"] >= delay

    return write, visible


class TestMeasure:
    def test_each_change_is_timed_until_the_path_shows_it(self, script):
        clock = _Clock()
        write, visible = _shows_after(clock, [1.0, 2.5, 0.0])
        probe = script.FreshnessProbe(
            write, {"graph": visible}, poll_s=0.5, timeout_s=60, clock=clock, sleep=clock.sleep
        )

        latencies = probe.measure(changes=3, spacing_s=10.0)

        assert latencies == {"graph": [1.0, 2.5, 0.0]}

    def test_the_changes_are_spaced(self, script):
        clock = _Clock()
        write, visible = _shows_after(clock, [0.0, 0.0])
        probe = script.FreshnessProbe(write, {"data": visible}, clock=clock, sleep=clock.sleep)

        probe.measure(changes=2, spacing_s=7.0)

        assert clock.sleeps == [7.0]

    def test_a_change_never_seen_is_none_after_the_timeout(self, script):
        clock = _Clock()
        write, visible = _shows_after(clock, [None])
        probe = script.FreshnessProbe(
            write, {"live": visible}, poll_s=1.0, timeout_s=5.0, clock=clock, sleep=clock.sleep
        )

        assert probe.measure(changes=1) == {"live": [None]}
        assert sum(clock.sleeps) == 5.0

    def test_each_path_is_timed_on_its_own(self, script):
        clock = _Clock()
        written: dict = {}
        readers = {
            "fast": lambda marker: clock.now - written["at"] >= 0.5,
            "slow": lambda marker: clock.now - written["at"] >= 3.0,
        }
        probe = script.FreshnessProbe(
            lambda marker: written.update(at=clock.now),
            readers,
            poll_s=0.5,
            clock=clock,
            sleep=clock.sleep,
        )

        assert probe.measure(changes=1) == {"fast": [0.5], "slow": [3.0]}

    def test_every_change_writes_a_new_marker(self, script):
        markers: list[str] = []
        probe = script.FreshnessProbe(markers.append, {"data": lambda m: True}, sleep=lambda s: None)

        probe.measure(changes=3, spacing_s=0)

        assert len(set(markers)) == 3 and all(m.startswith("ob-freshness-") for m in markers)


class TestPercentiles:
    @pytest.mark.parametrize(("q", "expected"), [(50, 3.0), (95, 5.0), (99, 5.0)])
    def test_nearest_rank(self, script, q, expected):
        assert script.FreshnessProbe.percentile([5.0, 1.0, 3.0, 2.0, 4.0], q) == expected

    def test_a_change_never_seen_ranks_last(self, script):
        values = [1.0, None, 2.0, 3.0, 4.0]

        assert script.FreshnessProbe.percentile(values, 50) == 3.0
        assert script.FreshnessProbe.percentile(values, 95) is None

    def test_no_change_gives_none(self, script):
        assert script.FreshnessProbe.percentile([], 50) is None

    def test_the_summary_counts_what_was_seen(self, script):
        summary = script.FreshnessProbe.summarize({"graph": [1.0, None, 2.0]})

        assert summary == {"graph": {"changes": 3, "seen": 2, "p50": 2.0, "p95": None, "p99": None}}


class TestVerdict:
    @staticmethod
    def _summary(script, **paths):
        return script.FreshnessProbe.summarize({path: list(v) for path, v in paths.items()})

    def test_microbatch_holds_the_graph_to_30s_p95(self, script):
        ok = self._summary(script, graph=[10.0] * 19 + [29.0])
        slow = self._summary(script, graph=[10.0] * 18 + [31.0, 31.0])

        assert script.FreshnessProbe.violations(ok, "microbatch", {}) == []
        assert script.FreshnessProbe.violations(slow, "microbatch", {}) == ["graph p95 31.000s > 30s"]

    def test_realtime_holds_live_to_1s_p99(self, script):
        summary = self._summary(script, live=[0.2, 0.3, None])

        assert script.FreshnessProbe.violations(summary, "realtime", {}) == ["live p99 not seen > 1s"]

    def test_a_path_not_measured_is_not_judged(self, script):
        summary = self._summary(script, data=[500.0])

        assert script.FreshnessProbe.violations(summary, "microbatch", {}) == []

    def test_drift_fails_but_slots_data_does_not(self, script):
        pairs = {"slots_data": True, "view_data": True, "sync_data": False}

        assert script.FreshnessProbe.violations({}, "incremental", pairs) == ["drift: view_data"]

    def test_the_markdown_report(self, script):
        summary = self._summary(script, graph=[1.0, 2.0])

        text = script.FreshnessProbe.markdown("microbatch", summary, {"view_data": False}, [])

        assert "| graph | 2/2 | 1.000 | 2.000 | 2.000 |" in text
        assert "view_data=ok" in text and text.endswith("SLOs met")


class TestWarehouseSQL:
    def test_the_update_escapes_its_values(self, script):
        sql = script._Warehouse.update_sql("main.s.customers", "id", "4'2", "name", "ob-x")

        assert sql == "UPDATE main.s.customers SET name = 'ob-x' WHERE id = '4''2'"

    def test_a_null_original_value_is_restored_as_null(self, script):
        sql = script._Warehouse.update_sql("main.s.customers", "id", "42", "name", None)

        assert "SET name = NULL" in sql

    def test_a_bad_column_is_refused(self, script):
        with pytest.raises(ValueError, match="Not a column name"):
            script._Warehouse.update_sql("main.s.customers", "id; DROP", "42", "name", "x")

    def test_visibility_looks_for_the_marker_object(self, script):
        assert script._Warehouse.visible_sql("main.kg.t_V1_graph", "ob-1") == (
            "SELECT 1 AS hit FROM main.kg.t_V1_graph WHERE object = 'ob-1' LIMIT 1"
        )

    def test_the_reader_runs_on_the_read_warehouse(self, script):
        build, read = SimpleNamespace(), SimpleNamespace(execute_query=lambda sql: [{"hit": 1}])

        assert script._Warehouse(build, read).reader("main.kg.t_V1_graph")("ob-1") is True


class TestCli:
    def test_options_read_their_environment(self, script, monkeypatch):
        monkeypatch.setenv("ONTOBRICKS_FRESHNESS_STEM", "main.kg.triplestore_d_V1")
        monkeypatch.setenv("ONTOBRICKS_FRESHNESS_CHANGES", "3")

        args = script.parse_args([])

        assert (args.stem, args.changes, args.mode) == ("main.kg.triplestore_d_V1", 3, "microbatch")

    def test_a_missing_target_is_a_usage_error(self, script, monkeypatch):
        for name in ("STEM", "SOURCE_TABLE", "KEY_COLUMN", "VALUE_COLUMN"):
            monkeypatch.delenv(f"ONTOBRICKS_FRESHNESS_{name}", raising=False)

        assert script.main([]) == 2
