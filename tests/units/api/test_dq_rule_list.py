"""Every rule a run executes is offered for selection, under its check id.

SWRL rules, decision tables and aggregate rules are not SHACL shapes, so they
have no shape id. A run addresses them by a synthetic one. The list endpoint,
the run selection and the reported result each derive that id, and if any of
them derives it differently the rule silently cannot be picked.
"""

from __future__ import annotations

import pytest

from back.core.w3c.shacl.constants import rule_check_id
from back.objects.digitaltwin import DigitalTwin

pytestmark = pytest.mark.unit


class _Domain:
    shacl_shapes = [{"id": "shape_1", "category": "conformance", "enabled": True}]
    swrl_rules = [{"name": "no_orphans"}, {"name": "disabled_one", "enabled": False}]
    ontology = {
        "decision_tables": [{"name": "pricing", "enabled": True}],
        "aggregate_rules": [{"name": "totals", "enabled": True}],
    }

    def deduplicate_shacl_shapes(self):
        return None


@pytest.fixture
def listed(monkeypatch):
    from api.routers.internal import ontology

    monkeypatch.setattr(ontology, "get_domain", lambda _mgr: _Domain())

    async def _list(**kwargs):
        return await ontology.list_shapes(session_mgr=object(), **kwargs)

    return _list


class TestRuleFamilies:
    async def test_the_families_are_offered_alongside_the_shapes(self, listed):
        response = await listed()
        assert [r["id"] for r in response["rules"]] == [
            "swrl:no_orphans",
            "dt:pricing",
            "agg:totals",
        ]

    async def test_the_shapes_stay_free_of_them(self, listed):
        """The ontology editor reads the same endpoint and manages shapes only."""
        response = await listed()
        assert [s["id"] for s in response["shapes"]] == ["shape_1"]

    async def test_a_disabled_rule_is_not_offered(self, listed):
        response = await listed()
        assert all("disabled_one" not in r["id"] for r in response["rules"])

    async def test_each_rule_carries_the_dimension_it_reports_under(self, listed):
        response = await listed()
        by_id = {r["id"]: r["category"] for r in response["rules"]}
        assert by_id["swrl:no_orphans"] == "structural"
        assert by_id["dt:pricing"] == "conformance"
        assert by_id["agg:totals"] == "conformance"

    async def test_a_category_filter_narrows_both_lists(self, listed):
        response = await listed(category="structural")
        assert [r["id"] for r in response["rules"]] == ["swrl:no_orphans"]
        assert response["shapes"] == []

    async def test_a_rule_with_no_name_still_gets_an_id(self, listed):
        assert rule_check_id("swrl", {}, 3) == "swrl:3"


class TestIdAgreement:
    """The runner must report the id the rule was picked by."""

    def test_the_runner_prefers_the_id_the_selector_stamped(self):
        rule = {"name": "second", "check_id": "swrl:second"}
        assert DigitalTwin._rule_check_id("swrl", rule, 0) == "swrl:second"

    def test_an_unstamped_rule_falls_back_to_the_shared_derivation(self):
        rule = {"name": "second"}
        assert DigitalTwin._rule_check_id("swrl", rule, 0) == rule_check_id(
            "swrl", rule, 0
        )
