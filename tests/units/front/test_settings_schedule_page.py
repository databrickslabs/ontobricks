"""The Scheduler tab has to offer all four task types, not just two.

The modal grew from two job kinds to four, and ``schedule.js`` went from
branching on ``kind`` in six places to a per-type descriptor. These are
wiring assertions: the fields each type needs exist in the template, the
descriptor covers every backend type, and the JS talks to the generic
endpoint rather than the two that no longer exist.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = ROOT / "src/front/templates/partials/settings/_settings_schedule.html"
SCRIPT = ROOT / "src/front/static/config/js/schedule.js"


@pytest.fixture(scope="module")
def template() -> str:
    return TEMPLATE.read_text()


@pytest.fixture(scope="module")
def script() -> str:
    return SCRIPT.read_text()


class TestTheTypeSelector:
    @pytest.mark.parametrize(
        "task_type", ["build", "cohort", "analytics", "reasoning"]
    )
    def test_every_backend_type_has_a_radio(self, template, task_type):
        assert f'name="scheduleType" id="scheduleType' in template
        assert f'value="{task_type}"' in template

    def test_the_radios_match_the_backend_registry(self, template):
        from back.objects.registry.scheduler_tasks import TASK_TYPES

        for key in TASK_TYPES:
            assert f'value="{key}"' in template, f"no radio for '{key}'"


class TestPerTypeFieldGroups:
    def test_cohort_keeps_its_rule_picker_and_outputs(self, template):
        assert 'class="mb-3 schedule-type-cohort"' in template
        assert 'id="scheduleCohortRule"' in template
        assert 'id="scheduleOutputGraph"' in template
        assert 'id="scheduleOutputUc"' in template

    def test_reasoning_offers_all_six_phases(self, template):
        from back.objects.registry.scheduler_tasks.reasoning import PHASES

        for phase in PHASES:
            assert f'data-phase="{phase}"' in template, f"no toggle for '{phase}'"

    def test_reasoning_offers_both_materialise_targets(self, template):
        assert 'schedule-type-reasoning' in template
        assert 'id="scheduleMaterializeGraph"' in template
        assert 'id="scheduleMaterializeDelta"' in template
        assert 'id="scheduleMaterializeTable"' in template

    def test_analytics_warns_about_the_run_length(self, template):
        """A run submits a Databricks job and blocks its worker, so the
        interval floor of 2 minutes is not a sane choice here."""
        assert 'schedule-type-analytics' in template
        analytics_block = template.split('schedule-type-analytics')[1][:600]
        assert "interval" in analytics_block.lower()


class TestTheScriptIsTypeDriven:
    def test_the_descriptor_covers_every_backend_type(self, script):
        from back.objects.registry.scheduler_tasks import TASK_TYPES

        descriptors = script.split("const TYPES = {")[1].split("\n    };")[0]
        for key in TASK_TYPES:
            assert f"{key}: {{" in descriptors, f"no descriptor for '{key}'"

    def test_it_reads_one_endpoint(self, script):
        assert "const API = '/settings/schedules'" in script
        assert "cohort-schedules" not in script, (
            "the cohort-only endpoints were removed from the router"
        )

    def test_the_target_travels_as_a_query_parameter(self, script):
        assert "'?target=' + encodeURIComponent(targetKey)" in script

    def test_per_type_options_travel_in_config(self, script):
        body = script.split("body: JSON.stringify({")[1].split("}),")[0]
        assert "task_type: taskType" in body
        assert "target_key: targetKey" in body
        assert "config: config" in body

    def test_the_config_blob_never_round_trips_through_a_dom_attribute(self, script):
        """``escapeHtml`` does not escape quotes, so serialising arbitrary
        JSON into an attribute would break the row (or worse)."""
        assert "data-config" not in script
        assert "schedulesByRow" in script

    def test_the_history_columns_come_from_the_descriptor(self, script):
        assert "extraColumns.map" in script
        assert "historyColumns" in script
