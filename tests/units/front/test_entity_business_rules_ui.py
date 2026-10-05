"""Contracts for the entity Business rules UI (Studio References + Graph Explorer)."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
STATIC = REPO_ROOT / "src/front/static"
PANELS_JS = STATIC / "ontology/js/ontology-shared-panels.js"
SWRL_JS = STATIC / "ontology/js/ontology-swrl.js"
LOADERS_JS = STATIC / "query/js/query-loaders.js"
SIGMA_JS = STATIC / "query/js/query-sigmagraph.js"
DETAILS_JS = STATIC / "query/js/query-entity-details.js"
BR_JS = STATIC / "query/js/query-business-rules.js"
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"
DESIGN_JS = STATIC / "global/js/ontology-design.js"
INFO_JS = STATIC / "ontology/js/ontology-information.js"


def test_references_tab_has_business_rules_box():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert 'id="sharedEntityBusinessRulesContent"' in js
    assert "openBusinessRuleSelectorModal()" in js
    assert "renderSharedEntityBusinessRules(viewOnly)" in js


def test_picker_lists_rules_for_class_and_parents_and_saves_refs():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "/ontology/swrl/list" in js
    assert "rulesUsingClass(" in js
    assert "ancestorClassNames(classes, parentName)" in js
    assert "business_rules: sharedPanelBusinessRules.length > 0" in js


def test_class_mappers_keep_business_rules():
    assert "business_rules: existing.business_rules || []" in DESIGN_JS.read_text(encoding="utf-8")
    assert INFO_JS.read_text(encoding="utf-8").count("business_rules: cls.business_rules || []") == 2


def test_swrl_editor_cascades_rename_and_delete():
    js = SWRL_JS.read_text(encoding="utf-8")
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
