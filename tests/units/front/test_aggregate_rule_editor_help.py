"""Aggregate rule editor — inline explanations and live plain-English preview."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
JS = REPO_ROOT / "src/front/static/ontology/js/ontology-business-rules.js"
HTML = REPO_ROOT / "src/front/templates/partials/ontology/_ontology_business_rules.html"

needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node required")

_HARNESS = """
const fs = require('fs');
global.window = {};
global.document = { addEventListener: () => {}, getElementById: () => null };
eval(fs.readFileSync(__SOURCE__, 'utf8'));
const BR = window.BusinessRulesModule;
process.stdout.write(JSON.stringify(BR.aggDescribeRule(__RULE__)));
"""


def _describe(**rule):
    script = _HARNESS.replace("__SOURCE__", json.dumps(str(JS))).replace(
        "__RULE__", json.dumps(rule)
    )
    done = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def _modal_html():
    html = HTML.read_text(encoding="utf-8")
    start = html.index('id="aggEditorModal"')
    return html[start : html.index("</div>\n</div>", start)]


def test_modal_explains_the_rule_and_each_field():
    modal = _modal_html()
    assert "aggregate rule checks every instance" in modal
    for help_id in (
        "aggTargetClassHelp",
        "aggGroupByHelp",
        "aggAggPropHelp",
        "aggFunctionHelp",
        "aggThresholdHelp",
        "aggResultClassHelp",
    ):
        assert f'id="{help_id}"' in modal
        assert f'aria-describedby="{help_id}"' in modal


def test_modal_has_live_preview_region():
    modal = _modal_html()
    assert 'id="aggPreview"' in modal
    assert 'aria-live="polite"' in modal
    assert 'id="aggPreviewText"' in modal
    assert 'id="aggPreviewWarning"' in modal
    js = JS.read_text(encoding="utf-8")
    assert "_aggRenderPreview()" in js
    assert "el.closest('#aggEditorModal')" in js


@needs_node
def test_describe_requires_target():
    out = _describe()
    assert "Pick a target entity" in out["text"]
    assert out["warning"] == ""


@needs_node
def test_describe_relationship_and_attribute():
    out = _describe(
        target_class="Customer",
        group_by_property="hasOrder",
        aggregate_property="amount",
        aggregate_function="sum",
        operator="gt",
        threshold=1000,
        result_class="VipCustomer",
    )
    assert 'follow "hasOrder"' in out["text"]
    assert 'SUM of their "amount" values' in out["text"]
    assert "greater than 1000" in out["text"]
    assert "VipCustomer" in out["text"]
    assert out["warning"] == ""


@needs_node
def test_describe_count_of_links():
    out = _describe(
        target_class="Customer", group_by_property="hasOrder",
        aggregate_function="count", operator="lte", threshold=0,
    )
    assert 'count its "hasOrder" links' in out["text"]
    assert "less than or equal to 0" in out["text"]
    assert "violations" in out["text"]


@needs_node
def test_describe_own_attribute():
    out = _describe(
        target_class="Sensor", aggregate_property="reading",
        aggregate_function="avg", operator="neq", threshold=3.5,
    )
    assert 'AVG of its own "reading" values' in out["text"]
    assert "different from 3.5" in out["text"]


@needs_node
def test_describe_whole_class_count_and_warnings():
    out = _describe(target_class="Customer", aggregate_function="count", operator="lt", threshold=10)
    assert "Count all Customer instances" in out["text"]
    assert out["warning"] == ""

    bad_func = _describe(target_class="Customer", aggregate_function="sum", threshold=10)
    assert "Only COUNT" in bad_func["warning"]

    with_result = _describe(target_class="Customer", aggregate_function="count", result_class="Big")
    assert "Result entity" in with_result["warning"]
