"""Contracts for entity inference rules (IF-THEN) in the Studio Behaviour tab + Graph Explorer."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
STATIC = REPO_ROOT / "src/front/static"
PANELS_JS = STATIC / "ontology/js/ontology-shared-panels.js"
IF_THEN_JS = STATIC / "ontology/js/ontology-if-then.js"
LOADERS_JS = STATIC / "query/js/query-loaders.js"
SIGMA_JS = STATIC / "query/js/query-sigmagraph.js"
DETAILS_JS = STATIC / "query/js/query-entity-details.js"
BR_JS = STATIC / "query/js/query-business-rules.js"
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"
INFO_JS = STATIC / "ontology/js/ontology-information.js"


def test_behaviour_tab_has_inference_rules_box():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert 'id="sharedEntityBusinessRulesContent"' in js
    assert "openBusinessRuleSelectorModal()" in js
    assert "renderSharedEntityBusinessRules(viewOnly)" in js
    start = js.index('data-form-tab-content="behaviour"')
    end = js.index('data-form-tab-content="links"')
    pane = js[start:end]
    assert 'id="sharedEntityBusinessRules"' in pane
    assert 'id="sharedEntityActions"' in pane


def test_picker_lists_rules_for_class_and_parents_and_saves_refs():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "/ontology/if-then/list" in js
    assert "rulesUsingClass(" in js
    assert "ancestorClassNames(classes, parentName)" in js
    assert "business_rules: sharedPanelBusinessRules.length > 0" in js


def test_class_mappers_keep_business_rules():
    assert "business_rules: sharedPanelBusinessRules.length > 0" in PANELS_JS.read_text(encoding="utf-8")
    assert INFO_JS.read_text(encoding="utf-8").count("business_rules: cls.business_rules || []") == 2


def test_if_then_editor_cascades_rename_and_delete():
    js = IF_THEN_JS.read_text(encoding="utf-8")
    assert "_renameBusinessRuleRefs(previousName, rule.name)" in js
    assert "_dropBusinessRuleRefs(removedName)" in js


def test_explorer_wires_business_rules():
    assert "query/js/query-business-rules.js" in DTWIN_HTML.read_text(encoding="utf-8")
    assert "businessRules:" in LOADERS_JS.read_text(encoding="utf-8")
    sigma = SIGMA_JS.read_text(encoding="utf-8")
    assert "renderBusinessRuleSection(entity.id, nodeRules)" in sigma
    assert 'data-sg-node-action="business-rule"' in sigma
    assert "openEntityBusinessRuleModal(brUri, brName)" in sigma
    assert "renderBusinessRuleSection(entity.id, businessRules)" in DETAILS_JS.read_text(encoding="utf-8")


def test_trigger_uses_confirm_token_builder_gate_and_refresh():
    js = BR_JS.read_text(encoding="utf-8")
    assert "/dtwin/nodes/business-rule/request" in js
    assert "/dtwin/nodes/business-rule/confirm" in js
    assert "/dtwin/nodes/business-rule/cancel" in js
    assert "hasDomainRole('builder')" in js
    assert "SigmaGraph.refreshCurrentExpansion()" in js
    assert "_brRequestBody(" in js
    assert "simulation_id" in js
    assert "simulation sandbox" in js
    assert "KgGraphSource.current()" in js
    assert "live:inferred" in js


def test_explorer_modal_shows_if_then_instead_of_if_then_text():
    js = BR_JS.read_text(encoding="utf-8")
    assert "_brIfThenHtml(pending.antecedent, pending.consequent, esc)" in js
    assert "pending.antecedent} &rarr;" not in js


@pytest.mark.skipif(shutil.which("node") is None, reason="node is required to run the frontend module")
def test_if_then_view_hides_if_then_syntax():
    script = f"""
const fs = require('fs');
global.window = {{}};
eval(fs.readFileSync({json.dumps(str(BR_JS))}, 'utf8'));
const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const html = _brIfThenHtml(
    'Customer(?x) ^ Claim(?y) ^ Contract(?z) ^ Meter(?w) ^ hasclaim(?x, ?y) ^ relatedtocontract(?y, ?z) ^ hasmeter(?z, ?w) ^ status(?z, ?status) ^ builtin:contains(?status, "active")',
    'owns(?x, ?w)',
    esc,
);
const attr = _brIfThenHtml('Customer(?x) ^ builtin:greaterThanOrEqual(?n, 1000)', 'priority(?x, "high")', esc);
process.stdout.write(JSON.stringify({{ html, attr }}));
"""
    completed = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, timeout=30
    )
    assert completed.returncode == 0, completed.stderr
    out = json.loads(completed.stdout)
    html = out["html"]
    for leaked in ("?", "^", "builtin:", "&rarr;", "→"):
        assert leaked not in html, leaked
    assert "<li>x is a Customer</li>" in html
    assert "<li>x hasclaim y</li>" in html
    assert '<li>status contains "active"</li>' in html
    assert "<li>x owns w</li>" in html
    assert '<li>x priority = "high"</li>' in out["attr"]
    assert "<li>n ≥ 1000</li>" in out["attr"]
