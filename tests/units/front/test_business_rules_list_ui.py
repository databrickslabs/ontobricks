"""Business Rules list UI: Simple-tab toolbar + shared Complex card style."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HTML = (
    ROOT / "src/front/templates/partials/ontology/_ontology_business_rules.html"
).read_text(encoding="utf-8")
IF_THEN_JS = (
    ROOT / "src/front/static/ontology/js/ontology-if-then.js"
).read_text(encoding="utf-8")
BR_JS = (
    ROOT / "src/front/static/ontology/js/ontology-business-rules.js"
).read_text(encoding="utf-8")
MAIN_CSS = (
    ROOT / "src/front/static/global/css/main.css"
).read_text(encoding="utf-8")
ONTOLOGY_CSS = (
    ROOT / "src/front/static/global/css/ontology.css"
).read_text(encoding="utf-8")
DQ_JS = (
    ROOT / "src/front/static/ontology/js/ontology-dataquality.js"
).read_text(encoding="utf-8")
ONTOLOGY_HTML = (
    ROOT / "src/front/templates/ontology.html"
).read_text(encoding="utf-8")


def _simple_pane() -> str:
    start = HTML.index('id="br-pane-if-then"')
    end = HTML.index('id="br-pane-sparql"')
    return HTML[start:end]


def _section_header() -> str:
    start = HTML.index('class="section-header')
    end = HTML.index('id="brTabs"')
    return HTML[start:end]


def test_auto_generate_and_shacl_live_in_simple_tab():
    pane = _simple_pane()
    header = _section_header()
    assert 'id="brAutoGenBtn"' in pane
    assert 'data-br-action="auto-generate"' in pane
    assert "openIfThenModal()" in pane
    assert "Auto-generate" in pane
    assert "SHACL" in pane
    assert 'id="brAutoGenBtn"' not in header
    assert "openIfThenModal()" not in header


def test_section_header_keeps_add_rule():
    header = _section_header()
    assert 'id="brAddBtn"' in header
    assert 'data-br-action="add-rule"' in header


def test_simple_reuses_complex_rule_card_renderer():
    """Simple rows go through BusinessRulesModule._renderRuleCard (Complex)."""
    assert "BusinessRulesModule._renderRuleCard" in IF_THEN_JS
    assert "br-rule-card" in IF_THEN_JS
    assert "if-then-rule-card" not in IF_THEN_JS
    assert "_renderRuleCard(container, { name, meta, enabled, badge, onEdit, onDelete, onToggle })" in BR_JS
    assert "ob-rule-card br-rule-card" in BR_JS
    assert ".ob-rule-card" in MAIN_CSS
    assert "ob-rule-card dq-shape-card" in DQ_JS


def test_simple_no_longer_has_purple_card_overrides():
    assert ".if-then-rule-card" not in ONTOLOGY_CSS
    assert ".if-then-rule-code" not in ONTOLOGY_CSS
    assert "#6f42c1" not in ONTOLOGY_CSS


def test_business_rules_module_loads_after_if_then():
    """renderRulesList needs BusinessRulesModule._renderRuleCard at runtime."""
    assert ONTOLOGY_HTML.index("ontology/js/ontology-if-then.js") < ONTOLOGY_HTML.index(
        "ontology/js/ontology-business-rules.js"
    )


def _complex_modal() -> str:
    start = HTML.index('id="sparqlEditorModal"')
    return HTML[start:start + 5200]


def test_complex_editor_is_two_column_assistant_modal():
    """The Complex editor is a fullscreen two-pane (chat + code) window,
    matching the Simple rule editor's window style."""
    modal = _complex_modal()
    assert "modal-fullscreen" in modal
    # Left column — assistant chat
    assert 'id="craChatLog"' in modal
    assert 'id="craChatInput"' in modal
    assert 'data-br-action="cra-send"' in modal
    # Right column — code fields kept by id so Validate/Save still work
    assert 'id="sparqlName"' in modal
    assert 'id="sparqlQuery"' in modal
    assert 'data-br-action="sparql-validate"' in modal
    assert 'data-br-action="sparql-save"' in modal


def test_business_rules_js_wires_complex_assistant():
    assert "cra-send" in BR_JS
    assert "craSend" in BR_JS
    assert "/ontology/business-rules/complex-assistant/chat" in BR_JS
    # The editor boots the assistant when opened.
    assert "_craKickoff" in BR_JS


def test_complex_assistant_reuses_graph_chat_styling():
    """Chat column shares the .assistant-* family with the Graph chat."""
    modal = _complex_modal()
    assert "assistant-messages" in modal
    assert "assistant-input-wrapper" in modal
    # JS renders the same bubble / thinking markup as query-chat.js.
    assert "assistant-msg" in BR_JS
    assert "assistant-msg-avatar" in BR_JS
    assert "assistant-thinking-dots" in BR_JS


def test_complex_save_is_two_step_validate_then_confirm():
    """Save validates first, then becomes a Confirm & Save on success."""
    assert "_craValidate" in BR_JS
    assert "_craDoSave" in BR_JS
    assert "_craSaveConfirmed" in BR_JS
    assert "_craSetSaveConfirmUI" in BR_JS
    assert "_craResetSaveConfirm" in BR_JS
    assert "Confirm & Save" in BR_JS
    # sparqlSave validates before delegating to the real save
    save_fn = BR_JS[BR_JS.index("async sparqlSave("):BR_JS.index("_craDoSave(")]
    assert "_craValidate" in save_fn
    # the validate endpoint is still used
    assert "/ontology/rules/sparql_rules/validate" in BR_JS


def test_complex_save_confirm_resets_on_change():
    """Confirm state resets on open and when the agent rewrites the draft."""
    assert "_craResetSaveConfirm" in BR_JS[BR_JS.index("sparqlOpenEditor("):BR_JS.index("_craKickoff(")] \
        or "_craResetSaveConfirm" in BR_JS[BR_JS.index("_craKickoff("):BR_JS.index("craSend(")]
    apply_fn = BR_JS[BR_JS.index("_craApplyResponse("):BR_JS.index("_OB_ICON_SVG")]
    assert "_craResetSaveConfirm" in apply_fn
