"""Structural contracts for the global fail-closed No-LLM UI gate."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_pages_start_with_fail_closed_llm_state():
    assert "llm-unconfigured" in _read("src/front/templates/base.html")


def test_global_gate_uses_navbar_state_and_attribute_markers():
    permissions = _read("src/front/static/global/js/permissions.js")
    navbar = _read("src/front/static/global/js/navbar.js")
    css = _read("src/front/static/global/css/permissions.css")

    assert "updateLlmAvailability" in permissions
    assert "data-requires-llm" in permissions
    assert "event.stopImmediatePropagation()" in permissions
    assert "info.llm_endpoint" in navbar
    assert "llm-unconfigured" in css
    assert "[data-requires-llm]" in css


def test_gate_preserves_owned_aria_state_without_changing_native_disabled():
    permissions = _read("src/front/static/global/js/permissions.js")

    assert "const llmAriaState = new WeakMap();" in permissions
    assert "control.getAttribute('aria-disabled')" in permissions
    assert "control.removeAttribute('aria-disabled')" in permissions
    assert "llmAriaState.delete(control)" in permissions
    assert "control.disabled" not in permissions
    assert "setAttribute('disabled'" not in permissions
    assert "removeAttribute('disabled'" not in permissions


def test_gate_blocks_pointer_and_activation_keys_in_capture_phase():
    permissions = _read("src/front/static/global/js/permissions.js")

    assert "document.addEventListener('click', blockUnavailableLlmControl, true)" in permissions
    assert "document.addEventListener('keydown', blockUnavailableLlmControl, true)" in permissions
    assert "event.key !== 'Enter' && event.key !== ' '" in permissions
    assert "event.preventDefault()" in permissions
    assert (
        "'No LLM selected. Select one in Domain → General → AI.'" in permissions
    )


def test_dynamic_controls_are_synchronized_by_one_observer():
    permissions = _read("src/front/static/global/js/permissions.js")

    assert permissions.count("new MutationObserver(") == 1
    assert "node.matches('[data-requires-llm]')" in permissions
    assert "node.querySelectorAll('[data-requires-llm]')" in permissions
    assert "childList: true, subtree: true" in permissions


def test_navbar_updates_gate_and_fails_closed():
    navbar = _read("src/front/static/global/js/navbar.js")

    assert (
        "window.OB?.updateLlmAvailability("
        "Boolean(data.info?.llm_endpoint?.trim())"
    ) in navbar
    assert "window.OB?.updateLlmAvailability(false)" in navbar


def test_llm_availability_style_follows_competing_permission_rules():
    css = _read("src/front/static/global/css/permissions.css")
    authorized_editor_rule = css.index(
        'body[data-domain-role="editor"] [data-requires="editor"]'
    )
    llm_gate = css.index("body.llm-unconfigured [data-requires-llm]")

    assert llm_gate > authorized_editor_rule


def test_every_domain_llm_trigger_has_declarative_marker():
    files = {
        "src/front/templates/partials/ontology/_ontology_wizard.html": [
            "wizardTopGenerateBtn",
            "wizardReviewContinueBtn",
            "wizardCompleteRetryBtn",
        ],
        "src/front/templates/partials/ontology/_ontology_map.html": [
            "mapAutoAssignIcons",
            "mapToggleAssistant",
            "assistantSendBtn",
            "assistant-suggestion",
        ],
        "src/front/templates/partials/ontology/_ontology_business_rules.html": [
            'data-br-action="auto-generate"',
        ],
        "src/front/templates/partials/mapping/_mapping_autoassign.html": [
            "startAutoAssignBtn",
            "reassignAttrsBtn",
        ],
        "src/front/templates/partials/mapping/_mapping_design.html": [
            "autoMapPanelBtn",
        ],
        "src/front/templates/partials/mapping/_mapping_manual.html": [
            "manualAutoMapBtn",
        ],
        "src/front/templates/partials/dtwin/_query_chat.html": [
            "chatSendBtn",
            "assistant-suggestion",
        ],
        "src/front/templates/partials/dtwin/_query_analytics.html": [
            "analyticsInterpretBtn",
        ],
    }

    for path, controls in files.items():
        tags = re.findall(r"<(?:button|a)\b[^>]*>", _read(path), flags=re.DOTALL)
        for control in controls:
            matching_tags = [tag for tag in tags if control in tag]
            assert matching_tags, f"{path}: missing control {control}"
            assert all("data-requires-llm" in tag for tag in matching_tags), (
                f"{path}: unmarked control {control}"
            )


def test_mapping_sql_wizard_marks_configured_generate_buttons():
    js = _read("src/front/static/mapping/js/mapping-shared.js")

    assert "generateButton.setAttribute('data-requires-llm', '')" in js


def test_mapping_context_auto_map_item_has_declarative_marker():
    js = _read("src/front/static/mapping/js/mapping-design.js")
    context_items = re.findall(
        r"<div\b[^>]*data-action=\"auto-assign\"[^>]*>", js, flags=re.DOTALL
    )

    assert context_items
    assert all("data-requires-llm" in item for item in context_items)


def test_chat_textareas_use_submit_only_llm_markers():
    files = {
        "src/front/templates/partials/ontology/_ontology_map.html": "assistantInput",
        "src/front/templates/partials/dtwin/_query_chat.html": "chatInput",
    }

    for path, control in files.items():
        textareas = re.findall(r"<textarea\b[^>]*>", _read(path), flags=re.DOTALL)
        matching = [tag for tag in textareas if control in tag]
        assert matching, f"{path}: missing textarea {control}"
        assert all("data-requires-llm-submit" in tag for tag in matching)
        assert all(re.search(r"\bdata-requires-llm(?:\s|=|>)", tag) is None for tag in matching)


def test_gate_blocks_only_unmodified_enter_on_llm_submit_markers():
    permissions = _read("src/front/static/global/js/permissions.js")

    assert "target.closest('[data-requires-llm-submit]')" in permissions
    assert "event.key === 'Enter' && !event.shiftKey" in permissions
    assert "blockUnavailableLlmInteraction(event)" in permissions
    assert (
        permissions.index("event.key === 'Enter' && !event.shiftKey")
        < permissions.index("event.key !== 'Enter' && event.key !== ' '")
    )


def test_llm_configuration_controls_remain_available_without_an_llm():
    domain_html = _read(
        "src/front/templates/partials/domain/_domain_information.html"
    )
    picker_html = _read(
        "src/front/templates/partials/layout/llm_endpoint_picker_modal.html"
    )
    domain_tags = re.findall(
        r"<(?:button|a)\b[^>]*>", domain_html, flags=re.DOTALL
    )
    picker_tags = re.findall(
        r"<(?:button|a)\b[^>]*>", picker_html, flags=re.DOTALL
    )

    for control in ("tab-llm", "domainLlmBrowse"):
        matching_tags = [tag for tag in domain_tags if control in tag]
        assert matching_tags, f"missing Domain → General control {control}"
        assert all("data-requires-llm" not in tag for tag in matching_tags)
    assert picker_tags
    assert all("data-requires-llm" not in tag for tag in picker_tags)
