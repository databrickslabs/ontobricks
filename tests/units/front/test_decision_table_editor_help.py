"""Decision table editor — inline explanations and live plain-English preview."""

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
process.stdout.write(JSON.stringify(BR.dtDescribeTable(__DT__)));
"""


def _describe(**dt):
    script = _HARNESS.replace("__SOURCE__", json.dumps(str(JS))).replace("__DT__", json.dumps(dt))
    done = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def _modal_html():
    html = HTML.read_text(encoding="utf-8")
    start = html.index('id="dtEditorModal"')
    return html[start : html.index("<!-- ═══ SPARQL Rule Editor Modal", start)]


def _cond(op, value=""):
    return {"op": op, "value": value}


RISK = dict(
    target_class="Customer",
    input_columns=[{"property": "income"}, {"property": "country"}],
    rows=[
        {"conditions": [_cond("gt", "100000"), _cond("eq", "FR")]},
        {"conditions": [_cond("any"), _cond("contains", "land")]},
    ],
    row_logic="or",
    hit_policy="first",
    output_column={"property": "riskTier", "action": "set_value", "value": "high"},
)


def test_modal_explains_the_table_and_each_field():
    modal = _modal_html()
    assert "decision table checks every instance" in modal
    for help_id in (
        "dtTargetClassHelp",
        "dtHitPolicyHelp",
        "dtGridHelp",
        "dtOutputHelp",
    ):
        assert f'id="{help_id}"' in modal
    for described in ("dtTargetClassHelp", "dtHitPolicyHelp", "dtOutputHelp"):
        assert f'aria-describedby="{described}"' in modal
    assert "case-insensitive" in modal
    assert "still match" in modal
    assert "conflict" in modal
    assert 'id="dtOutputClass"' in modal


def test_modal_has_live_preview_region():
    modal = _modal_html()
    assert 'id="dtPreview"' in modal
    assert 'aria-live="polite"' in modal
    assert 'id="dtPreviewText"' in modal
    assert 'id="dtPreviewWarning"' in modal
    js = JS.read_text(encoding="utf-8")
    assert "el.closest('#dtEditorModal')" in js
    assert "this._dtRenderPreview();" in js
    assert "An instance matches only if it satisfies <strong>every</strong> row" in js


@needs_node
def test_describe_requires_target():
    out = _describe()
    assert "Pick a target entity" in out["text"]
    assert out["warnings"] == []


@needs_node
def test_describe_or_rows_with_output_and_first_policy():
    out = _describe(**RISK)
    text = out["text"]
    assert text.startswith("For each Customer: it matches when ")
    assert '(income > 100000 AND country = "FR")' in text
    assert ' OR (country contains "land")' in text
    assert 'riskTier = "high"' in text
    assert "only the first matching row counts" in text
    assert out["warnings"] == []


@needs_node
def test_describe_and_logic_has_no_hit_policy_sentence():
    out = _describe(**{**RISK, "row_logic": "and", "hit_policy": "all"})
    assert ") AND (" in out["text"]
    assert "first matching row" not in out["text"]
    assert "every matching row" not in out["text"]


@needs_node
def test_describe_unique_policy():
    out = _describe(**{**RISK, "hit_policy": "unique"})
    assert "reported as a conflict" in out["text"]


@needs_node
def test_describe_assign_entity():
    out = _describe(**{**RISK, "output_column": {"property": "", "action": "assign_class", "value": "HighRisk"}})
    assert "typed as HighRisk" in out["text"]
    assert out["warnings"] == []


@needs_node
def test_describe_reports_only_without_output():
    out = _describe(**{**RISK, "output_column": {"property": "", "action": "set_value", "value": ""}})
    assert "reported as matches" in out["text"]


@needs_node
def test_describe_warnings():
    missing_col = _describe(**{**RISK, "input_columns": [{"property": "income"}, {"property": ""}]})
    assert any("Column 2" in w for w in missing_col["warnings"])

    empty = _describe(
        target_class="Customer",
        input_columns=[{"property": "income"}],
        rows=[{"conditions": [_cond("eq", "")]}],
    )
    assert any("matches nothing" in w for w in empty["warnings"])

    assign = _describe(**{**RISK, "output_column": {"property": "", "action": "assign_class", "value": ""}})
    assert any("Pick the entity to assign" in w for w in assign["warnings"])

    no_prop = _describe(**{**RISK, "output_column": {"property": "", "action": "set_value", "value": "x"}})
    assert any("output property" in w for w in no_prop["warnings"])
