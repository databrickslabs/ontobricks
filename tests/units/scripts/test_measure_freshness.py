"""``scripts/measure_freshness.py``: timing, percentiles and SLO verdicts, with fakes."""

from __future__ import annotations

import importlib.util
import re
import time
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

    def test_each_path_is_polled_on_its_own_thread(self, script):
        """A slow path never delays the stamp of a fast one (realtime p99 < 1 s needs it)."""
        written: dict = {}

        def slow(shown: str) -> bool:
            time.sleep(0.2)
            return time.monotonic() - written["at"] >= 0.4

        probe = script.FreshnessProbe(
            lambda marker: written.update(at=time.monotonic()),
            {"live": lambda shown: True, "graph": slow},
            poll_s=0.01,
        )

        latencies = probe.measure(changes=1)

        assert latencies["live"][0] < 0.1 <= 0.4 <= latencies["graph"][0]

    def test_the_paths_look_for_the_object_the_write_shows(self, script):
        asked: list = []
        probe = script.FreshnessProbe(
            lambda marker: "1234.00", {"data": lambda shown: asked.append(shown) or True}
        )

        probe.measure(changes=1)

        assert asked == ["1234.00"]

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
    def test_the_update_casts_and_escapes_its_values(self, script):
        sql = script._Warehouse.update_sql("main.s.customers", "id", "4'2", "name", "ob-x", "string")

        assert sql == "UPDATE main.s.customers SET name = CAST('ob-x' AS string) WHERE id = '4''2'"

    def test_a_null_original_value_is_restored_as_null(self, script):
        sql = script._Warehouse.update_sql("main.s.customers", "id", "42", "age", None, "int")

        assert "SET age = NULL" in sql

    @pytest.mark.parametrize(
        ("column", "sql_type"), [("id; DROP", "string"), ("name", "string); DROP TABLE x --")]
    )
    def test_a_bad_column_or_type_is_refused(self, script, column, sql_type):
        with pytest.raises(ValueError, match="Not a"):
            script._Warehouse.update_sql("main.s.customers", "id", "42", column, "x", sql_type)

    def test_visibility_looks_for_the_marker_object(self, script):
        assert script._Warehouse.visible_sql("main.kg.t_V1_graph", "ob-1") == (
            "SELECT 1 AS hit FROM main.kg.t_V1_graph WHERE object = 'ob-1' LIMIT 1"
        )

    def test_visibility_can_be_held_to_the_row_s_triple(self, script):
        sql = script._Warehouse.visible_sql(
            "main.kg.t_V1_graph", "-2000000003", subject="http://ex/C/42", predicate="http://ex/age"
        )

        assert sql == (
            "SELECT 1 AS hit FROM main.kg.t_V1_graph WHERE object = '-2000000003' "
            "AND subject = 'http://ex/C/42' AND predicate = 'http://ex/age' LIMIT 1"
        )

    def test_the_reader_runs_on_the_read_warehouse(self, script):
        build, read = SimpleNamespace(), SimpleNamespace(execute_query=lambda sql: [{"hit": 1}])

        assert script._Warehouse(build, read).reader("main.kg.t_V1_graph")("ob-1") is True


class TestMarkerValue:
    @pytest.mark.parametrize("sql_type", ["string", "varchar(40)", "text", "character varying"])
    def test_a_text_column_takes_the_marker(self, script, sql_type):
        assert script.FreshnessProbe.marker_value("ob-freshness-ab12-3", sql_type) == (
            "ob-freshness-ab12-3",
            "ob-freshness-ab12-3",
        )

    @pytest.mark.parametrize(
        ("sql_type", "low", "high", "shown"),
        [
            ("int", -2**31, -2_000_000_000, r"-\d{10}"),
            ("bigint", -2**31, -2_000_000_000, r"-\d{10}"),
            ("smallint", -2**15, -32_000, r"-\d{5}"),
            ("double", -10**7 + 1, -9_900_000, r"-\d{7}\.0"),
            ("decimal(10,2)", -(10**8) + 1, -(10**8) + 1000, r"-\d{8}\.00"),
            ("numeric(12,0)", -(10**12) + 1, -(10**12) + 1000, r"-\d{12}"),
        ],
    )
    def test_a_numeric_column_takes_a_negative_number_at_the_bottom_of_its_range(
        self, script, sql_type, low, high, shown
    ):
        """No real value sits there, so the marker cannot match an existing literal."""
        value, text = script.FreshnessProbe.marker_value("ob-freshness-ab12-3", sql_type)

        assert low <= int(value) <= high
        assert re.fullmatch(shown, text) and text.startswith(value)

    def test_a_decimal_with_too_few_integer_digits_is_a_usage_error(self, script):
        with pytest.raises(script._UsageError, match="too few integer digits"):
            script.FreshnessProbe.marker_value("ob-freshness-ab12-0", "decimal(6,2)")

    @pytest.mark.parametrize("sql_type", ["int", "smallint", "double", "decimal(10,2)"])
    def test_each_change_of_a_run_gets_its_own_number(self, script, sql_type):
        marker = script.FreshnessProbe.marker_value
        values = {marker(f"ob-freshness-ab12-{i}", sql_type)[0] for i in range(5)}

        assert len(values) == 5

    def test_another_type_is_a_usage_error(self, script):
        with pytest.raises(script._UsageError, match="Unsupported value column type"):
            script.FreshnessProbe.marker_value("ob-freshness-ab12-0", "date")


class TestPathsAndGuard:
    @staticmethod
    def _args(script, *argv):
        return script.parse_args(["--source-table", "main.ob_it.ob_it_customers", *argv])

    def test_the_default_paths_follow_the_mode(self, script):
        assert script.paths_for(self._args(script)) == ["data", "graph"]
        realtime = self._args(script, "--mode", "realtime", "--pg-dsn", "x", "--live-table", "s.l")
        assert script.paths_for(realtime) == ["data", "graph", "live"]
        synced = self._args(script, "--pg-dsn", "x", "--sync-table", "s.t")
        assert script.paths_for(synced) == ["data", "graph", "sync"]

    def test_a_realtime_run_without_live_fails_fast(self, script):
        with pytest.raises(script._UsageError, match="live path needs"):
            script.paths_for(self._args(script, "--mode", "realtime"))

    def test_an_unknown_path_fails_fast(self, script):
        with pytest.raises(script._UsageError, match="Unknown path"):
            script.paths_for(self._args(script, "--paths", "data,slots"))

    def test_only_a_fixture_table_is_updated(self, script):
        script.guard_fixture(self._args(script))
        with pytest.raises(script._UsageError, match="not a fixture table"):
            script.guard_fixture(script.parse_args(["--source-table", "main.sales.customers"]))


class _OneRowTable:
    """A warehouse holding one source row (``id = 42``) whose value every path shows at once."""

    def __init__(self, value: str = "Ada"):
        self.value = value

    def execute_query(self, sql: str):
        if sql.startswith("DESCRIBE TABLE"):
            return [{"col_name": "id", "data_type": "int"}, {"col_name": "name", "data_type": "string"}]
        if sql.startswith("SELECT CAST(name AS STRING)"):
            return [{"v": self.value}]
        if sql.startswith("SELECT 1 AS hit"):
            return [{"hit": 1}] if f"'{self.value}'" in sql else []
        raise AssertionError(sql)

    def execute_statement(self, sql: str):
        match = re.search(r"SET name = (NULL|CAST\('([^']*)' AS string\))", sql)
        self.value = None if match.group(1) == "NULL" else match.group(2)
        return True


class TestRun:
    def test_it_reconciles_while_the_last_change_is_in_and_then_restores_the_row(self, script, monkeypatch):
        table = _OneRowTable()
        seen_at_reconcile: list = []
        monkeypatch.setattr(script, "_warehouse_clients", lambda read_id: (table, table))
        monkeypatch.setattr(
            script._Warehouse,
            "reconcile",
            lambda self, stem, **kw: seen_at_reconcile.append(table.value) or {"pairs": {"view_data": False}},
        )
        args = script.parse_args(
            [
                "--stem", "main.kg.triplestore_d_V1", "--source-table", "main.ob_it.ob_it_customers",
                "--key-column", "id", "--key-value", "42", "--value-column", "name",
                "--changes", "2", "--spacing", "0", "--poll", "0.01",
            ]
        )

        report = script.run(args)

        assert seen_at_reconcile[0].startswith("ob-freshness-") and seen_at_reconcile[0].endswith("-1")
        assert table.value == "Ada"
        assert report["violations"] == [] and report["summary"]["graph"]["seen"] == 2

    def test_an_unsupported_value_column_writes_nothing(self, script, monkeypatch):
        table = _OneRowTable()
        table.execute_query = lambda sql: (
            [{"col_name": "name", "data_type": "date"}]
            if sql.startswith("DESCRIBE TABLE")
            else _OneRowTable.execute_query(table, sql)
        )
        writes: list = []
        table.execute_statement = writes.append
        monkeypatch.setattr(script, "_warehouse_clients", lambda read_id: (table, table))
        args = script.parse_args(
            [
                "--stem", "main.kg.triplestore_d_V1", "--source-table", "main.ob_it.ob_it_customers",
                "--key-column", "id", "--key-value", "42", "--value-column", "name",
            ]
        )

        with pytest.raises(script._UsageError, match="Unsupported value column type"):
            script.run(args)
        assert writes == []

    def test_a_run_without_a_data_or_graph_path_warns(self, script, monkeypatch):
        table = _OneRowTable()
        monkeypatch.setattr(script, "_warehouse_clients", lambda read_id: (table, table))
        monkeypatch.setattr(script, "_Postgres", lambda dsn: SimpleNamespace(
            reader=lambda name, **kw: (lambda shown: True), count=lambda name, **kw: 0
        ))
        monkeypatch.setattr(script._Warehouse, "reconcile", lambda self, stem, **kw: {"pairs": {}})
        args = script.parse_args(
            [
                "--stem", "main.kg.triplestore_d_V1", "--source-table", "main.ob_it.ob_it_customers",
                "--key-column", "id", "--key-value", "42", "--value-column", "name",
                "--paths", "sync", "--pg-dsn", "x", "--sync-table", "s.t", "--changes", "1",
            ]
        )

        report = script.run(args)

        assert report["warnings"] and "reconciliation may run before" in report["warnings"][0]
        assert "Warning: no data / graph path" in report["markdown"]

    def test_a_missing_key_writes_nothing(self, script, monkeypatch):
        table = _OneRowTable()
        table.execute_query = lambda sql: [] if sql.startswith("SELECT CAST") else _OneRowTable.execute_query(table, sql)
        writes: list = []
        table.execute_statement = writes.append
        monkeypatch.setattr(script, "_warehouse_clients", lambda read_id: (table, table))
        args = script.parse_args(
            [
                "--stem", "main.kg.triplestore_d_V1", "--source-table", "main.ob_it.ob_it_customers",
                "--key-column", "id", "--key-value", "404", "--value-column", "name",
            ]
        )

        with pytest.raises(script._UsageError, match="no row with id = '404'"):
            script.run(args)
        assert writes == []


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


def test_an_empty_variable_reads_as_unset(script, monkeypatch):
    """CI sets an undefined repository variable to ''."""
    monkeypatch.setenv("ONTOBRICKS_FRESHNESS_ALLOW_PATTERN", "")
    monkeypatch.setenv("ONTOBRICKS_FRESHNESS_CHANGES", "")

    args = script.parse_args([])

    assert (args.allow_pattern, args.changes) == ("*ob_it_*,*fixture*", 5)
