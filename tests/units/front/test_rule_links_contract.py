"""Contract for rule links in the inference phase tree (rule-links spec)."""

import re
from pathlib import Path

FRONT = Path(__file__).resolve().parents[3] / "src" / "front"


def _read(rel: str) -> str:
    return (FRONT / rel).read_text(encoding="utf-8")


def _ids(text: str) -> set:
    return set(re.findall(r'\bid="([^"]+)"', text))


def test_phase_tree_renders_rule_labels_as_links_when_flagged():
    js = _read("static/global/js/phase-tree.js")
    assert "data-rule-link" in js
    assert "item.link" in js


def test_phase_tree_emits_rule_open_event_and_skips_checkbox_toggle():
    js = _read("static/global/js/phase-tree.js")
    assert "ob:rule-open" in js
    assert "data-rule-link" in js
    assert "preventDefault()" in js


def test_named_rules_are_linkable_but_dq_items_are_not_changed():
    js = _read("static/global/js/phase-tree.js")
    assert "link: true" in js


def test_if_then_module_exposes_standalone_init_and_idempotent_binding():
    js = _read("static/ontology/js/ontology-if-then.js")
    assert "bindEditorDom()" in js
    assert "IfThenModule.initStandalone = function" in js
    assert "bindEditorDom = function" in js
    assert "_editorDomBound" in js


def test_business_rules_module_has_standalone_init_and_read_only_editor():
    js = _read("static/ontology/js/ontology-business-rules.js")
    assert "initStandalone()" in js
    assert "readOnly = false" in js
    assert "_setSparqlReadOnly(" in js


def test_both_save_paths_announce_rules_saved():
    if_then = _read("static/ontology/js/ontology-if-then.js")
    biz = _read("static/ontology/js/ontology-business-rules.js")
    assert "ob:rules-saved" in if_then
    assert "ob:rules-saved" in biz


def test_loader_loads_only_the_editor_assets_and_not_ontology_core():
    js = _read("static/global/js/rule-editor-loader.js")
    for asset in ("ontology/css/ontology-if-then.css",
                  "ontology/css/ontology-business-rules.css",
                  "ontology/js/ontology-if-then.js",
                  "ontology/js/ontology-business-rules.js"):
        assert asset in js
    assert "ontology-core.js" not in js
    assert "canEditOntology" in js
    assert "attach(" in js and "open(" in js


def test_both_pages_include_editor_markup_in_hidden_wrapper_and_load_loader():
    for page, module in (("templates/dtwin.html", "query/js/query-reasoning.js"),
                         ("templates/simulation.html", "simulation/js/simulation-rules.js")):
        src = _read(page)
        assert 'id="if-then-section"' in src
        assert "partials/ontology/_ontology_business_rules.html" in src
        assert "global/js/rule-editor-loader.js" in src
        assert src.index("global/js/phase-tree.js") < src.index("global/js/rule-editor-loader.js")
        assert src.index("global/js/rule-editor-loader.js") < src.index(module)
        assert 'data-asset-version="{{ asset_version }}"' in src
        assert "RuleEditorAssetVersion" not in src


def test_editor_partial_ids_do_not_collide_with_inference_or_simulation_pages():
    partial = _ids(_read("templates/partials/ontology/_ontology_business_rules.html"))
    assert partial, "partial should define ids"
    for page in ("templates/dtwin.html", "templates/simulation.html"):
        page_ids = _ids(_read(page))
        for partial_id in partial:
            if partial_id == "if-then-section":
                continue
            assert partial_id not in page_ids, f"{partial_id} duplicated in {page}"


def test_editor_partial_ids_do_not_collide_with_included_partials():
    partial = _ids(_read("templates/partials/ontology/_ontology_business_rules.html"))
    included = ""
    for rel in ("templates/partials/dtwin/_query_reasoning.html",
                "templates/partials/simulation/_sim_workbench.html",
                "templates/partials/layout/_inference_phase_tree.html"):
        included += _read(rel)
    collisions = partial & _ids(included)
    assert not collisions, f"id collisions: {sorted(collisions)}"


def test_both_inference_modules_attach_the_rule_editor_loader():
    graph = _read("static/query/js/query-reasoning.js")
    sim = _read("static/simulation/js/simulation-rules.js")
    assert "RuleEditorLoader.attach(" in graph
    assert "RuleEditorLoader.attach(" in sim


def test_rule_editor_host_keeps_modals_in_layout():
    # The host must not be display:none, or Bootstrap renders the editor modals 0x0.
    css = _read("static/query/css/query-reasoning.css")
    assert "#if-then-section.rule-editor-host > .content-section" in css
    for rel in ("templates/dtwin.html", "templates/simulation.html"):
        src = _read(rel)
        assert 'id="if-then-section" class="rule-editor-host"' in src
        assert 'id="if-then-section" class="d-none"' not in src


def test_phase_tree_title_can_shrink_inside_label():
    # Long rule names must ellipsize instead of overflowing the row on narrow screens.
    css = _read("static/query/css/query-reasoning.css")
    block = css[css.index(".ob-tree-title {") :]
    block = block[: block.index("}")]
    assert "min-width: 0" in block
    assert "text-overflow: ellipsis" in block


def test_loader_opens_simple_and_complex_editors_by_key():
    js = _read("static/global/js/rule-editor-loader.js")
    assert "key === 'if_then'" in js
    assert "IfThenModule.editRule" in js
    assert "IfThenModule.viewRule" in js
    assert "key === 'sparql_rules'" in js
    assert "sparqlOpenEditor(index, { readOnly: !canEdit })" in js


def test_loader_refreshes_the_matching_list_after_save():
    js = _read("static/global/js/rule-editor-loader.js")
    assert "/ontology/if-then/list" in js
    assert "/ontology/rules/sparql_rules/list" in js
    assert "tree.selectedRules()" in js
    assert "tree.applyRules(selection)" in js
    assert "key !== 'if_then' && key !== 'sparql_rules'" in js


def test_complex_read_only_mode_disables_fields_and_hides_save():
    js = _read("static/ontology/js/ontology-business-rules.js")
    for field_id in ("sparqlName", "sparqlDescription", "sparqlQuery", "craChatInput"):
        assert field_id in js
    assert "sparqlSaveBtn" in js
    assert "You can view this rule. Editing requires the Editor role." in js
