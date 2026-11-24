"""A data quality run only covers the dimensions the user selected.

SHACL shapes have a checkbox each, but IF-THEN (Simple) rules do not: they are
reported under a fixed dimension (``structural``). The run used to take them
in full whatever was selected, so ticking Conformance alone still executed
every IF-THEN rule and listed the results under Structural.
"""

from __future__ import annotations

import threading

import pytest

from back.core.errors import ValidationError

pytestmark = pytest.mark.unit

TABLE = "cat.sch.triples"


class _Request:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


class _Domain:
    """A domain carrying one shape per dimension plus the IF-THEN rule family."""

    def __init__(self):
        self.shacl_shapes = [
            {"id": f"shape_{cat}", "category": cat, "enabled": True}
            for cat in ("conformance", "structural", "completeness")
        ]
        self.if_then_rules = [{"name": "if_then_1"}, {"name": "if_then_2"}]
        self.ontology = {}


class _Task:
    id = "task_1"


class _TaskManager:
    def create_task(self, **_kw):
        return _Task()


@pytest.fixture
def run(monkeypatch):
    """Start a run and return the keyword arguments the runner received."""
    from api.routers.internal import dtwin
    import back.core.task_manager as task_manager

    captured = {}

    class _FakeDigitalTwin:
        @staticmethod
        def run_data_quality_task(
            tm, task_id, settings, snap, shapes, table, total, **kwargs
        ):
            captured["shapes"] = shapes
            captured["table"] = table
            captured.update(kwargs)

    class _FakeThread:
        def __init__(self, target=None, **_kw):
            self._target = target

        def start(self):
            self._target()

    monkeypatch.setattr(dtwin, "get_domain", lambda _mgr: _Domain())
    monkeypatch.setattr(dtwin, "DomainSnapshot", lambda _d: object())
    monkeypatch.setattr(dtwin, "DigitalTwin", _FakeDigitalTwin)
    monkeypatch.setattr(dtwin, "effective_view_table", lambda _d, _s=None: TABLE)
    monkeypatch.setattr(task_manager, "get_task_manager", lambda: _TaskManager())
    monkeypatch.setattr(threading, "Thread", _FakeThread)

    async def _run(**payload):
        captured.clear()
        response = await dtwin.start_dataquality_checks(
            request=_Request(payload), session_mgr=object(), settings=object()
        )
        assert response["success"] is True
        return captured

    return _run


class TestDimensionSelection:
    async def test_conformance_only_leaves_out_the_if_then_rules(self, run):
        """The reported bug: IF-THEN results are filed under Structural."""
        selected = await run(dimensions=["conformance"])
        assert selected["if_then_rules"] == []

    async def test_structural_only_keeps_the_if_then_rules(self, run):
        selected = await run(dimensions=["structural"])
        assert len(selected["if_then_rules"]) == 2

    async def test_a_dimension_owning_no_rule_family_runs_shapes_only(self, run):
        selected = await run(dimensions=["completeness"])
        assert selected["if_then_rules"] == []
        assert [s["id"] for s in selected["shapes"]] == ["shape_completeness"]

    async def test_shapes_follow_the_selected_dimensions(self, run):
        selected = await run(dimensions=["conformance"])
        assert [s["id"] for s in selected["shapes"]] == ["shape_conformance"]

    async def test_no_selection_runs_everything(self, run):
        """An API client that names no dimension gets the whole suite."""
        selected = await run()
        assert len(selected["if_then_rules"]) == 2
        assert len(selected["shapes"]) == 3


class TestRuleSelection:
    """Picking individual rules runs those rules and nothing else.

    IF-THEN rules are addressed by the same check id the run reports them under,
    so a selection covers them as it covers shapes. Before that they had no id
    to be picked by and rode on the dimension, so choosing a couple of rules
    still ran every one of them.
    """

    async def test_naming_a_shape_leaves_the_other_families_out(self, run):
        selected = await run(
            shape_ids=["shape_conformance"], dimensions=["conformance"]
        )
        assert [s["id"] for s in selected["shapes"]] == ["shape_conformance"]
        assert selected["if_then_rules"] == []

    async def test_a_ticked_dimension_no_longer_drags_its_rules_in(self, run):
        """The reported bug: Structural stayed ticked, so every IF-THEN rule ran."""
        selected = await run(
            shape_ids=["shape_conformance"], dimensions=["conformance", "structural"]
        )
        assert selected["if_then_rules"] == []

    async def test_a_if_then_rule_can_be_picked_on_its_own(self, run):
        selected = await run(shape_ids=["if_then:if_then_2"], dimensions=["structural"])
        assert [r["name"] for r in selected["if_then_rules"]] == ["if_then_2"]
        assert selected["shapes"] == []

    async def test_unpicking_one_rule_of_a_family_keeps_the_others(self, run):
        selected = await run(shape_ids=["if_then:if_then_2"])
        assert [r["name"] for r in selected["if_then_rules"]] == ["if_then_2"]

    async def test_the_picked_id_travels_with_the_rule(self, run):
        """Filtering renumbers the family, so the id cannot be re-derived."""
        selected = await run(shape_ids=["if_then:if_then_2"])
        assert selected["if_then_rules"][0]["check_id"] == "if_then:if_then_2"

    async def test_a_dimension_may_run_with_no_shape_of_its_own(self, run):
        """Structural can hold IF-THEN rules and no shape at all."""
        selected = await run(dimensions=["structural"], shape_ids=[])
        assert len(selected["if_then_rules"]) == 2


class TestEmptySelection:
    async def test_a_selection_matching_nothing_is_rejected(self, run):
        with pytest.raises(ValidationError, match="selected dimensions"):
            await run(dimensions=["uniqueness"])


class TestExecutionTarget:
    """Checks run against the triple-store VIEW, whatever graph engine is set.

    The VIEW is created by every build, so it is the one target every domain
    has. Resolving it server-side also stops a stale client from naming a
    table the domain no longer uses.
    """

    async def test_the_view_is_the_target(self, run):
        selected = await run(dimensions=["conformance"])
        assert selected["table"] == TABLE

    async def test_a_client_supplied_table_is_ignored(self, run):
        selected = await run(
            dimensions=["conformance"], triplestore_table="cat.sch.somewhere_else"
        )
        assert selected["table"] == TABLE

    async def test_a_client_supplied_backend_is_ignored(self, run):
        selected = await run(dimensions=["conformance"], backend="graph")
        assert selected["table"] == TABLE

    async def test_an_unbuilt_domain_is_rejected(self, run, monkeypatch):
        from api.routers.internal import dtwin

        monkeypatch.setattr(dtwin, "effective_view_table", lambda _d, _s=None: "")
        with pytest.raises(ValidationError, match="Build the Knowledge Graph first"):
            await run(dimensions=["conformance"])
