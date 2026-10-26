"""Structural contracts for the shared LLM endpoint picker."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def _read(relative: str) -> str:
    return (REPO_ROOT / relative).read_text(encoding="utf-8")


def test_shared_picker_is_loaded_from_base():
    base = _read("src/front/templates/base.html")
    assert "partials/layout/llm_endpoint_picker_modal.html" in base
    assert "global/js/llm-endpoint-picker.js" in base
    assert "global/css/llm-endpoint-picker.css" in base


def test_domain_llm_uses_hidden_name_and_kind():
    html = _read("src/front/templates/partials/domain/_domain_information.html")
    assert 'id="domainLlmEndpoint"' in html
    assert 'id="domainLlmEndpointKind"' in html
    assert 'id="domainLlmBrowse"' in html
    assert 'id="domainLlmEndpointDisplay"' in html


def test_picker_gateway_group_precedes_serving_group():
    html = _read(
        "src/front/templates/partials/layout/llm_endpoint_picker_modal.html"
    )
    assert html.index("llmGatewayList") < html.index("llmServingList")
    assert 'id="llmEndpointSearch"' in html
    assert 'id="llmEndpointRefresh"' in html


def test_picker_exposes_shared_api_and_renders_api_values_safely():
    js = _read("src/front/static/global/js/llm-endpoint-picker.js")
    assert "window.openLlmEndpointPicker" in js
    assert "textContent" in js
    assert "/mapping/wizard/llm-endpoints" in js
    assert "ai_gateway" in js
    assert "serving" in js


def test_domain_and_new_domain_persist_endpoint_kind():
    navbar = _read("src/front/static/global/js/navbar.js")
    domain = _read("src/front/static/domain/js/domain.js")
    utils = _read("src/front/static/global/js/utils.js")
    assert "llm_endpoint_kind" in navbar
    assert "llm_endpoint_kind" in domain
    assert "openLlmEndpointPicker" in utils
    assert "llm_endpoint_kind" in utils


def test_late_domain_info_fetch_cannot_overwrite_a_user_selection():
    js = _read("src/front/static/domain/js/domain-information.js")
    assert "nameInput.dataset.userEdited = '1'" in js
    assert "!llmNameInput.dataset.userEdited" in js


def test_picker_layers_above_parent_modal_and_owns_escape():
    modal = _read("src/front/templates/partials/layout/llm_endpoint_picker_modal.html")
    css = _read("src/front/static/global/css/llm-endpoint-picker.css")
    js = _read("src/front/static/global/js/llm-endpoint-picker.js")
    assert "llm-endpoint-picker-modal" in modal
    assert ".llm-endpoint-picker-modal" in css
    assert ".llm-endpoint-picker-backdrop" in css
    assert "event.stopImmediatePropagation()" in js
    assert "document.addEventListener('keydown', onPickerKeydown, true)" in js


def test_picker_keeps_last_row_clear_of_footer():
    modal = _read("src/front/templates/partials/layout/llm_endpoint_picker_modal.html")
    assert 'class="modal-body pb-4"' in modal


def test_picker_toolbar_stacks_on_mobile():
    modal = _read("src/front/templates/partials/layout/llm_endpoint_picker_modal.html")
    css = _read("src/front/static/global/css/llm-endpoint-picker.css")
    assert "llm-picker-toolbar" in modal
    assert 'placeholder="Search models or endpoints"' in modal
    assert "@media (max-width: 575.98px)" in css
    assert "flex-direction: column" in css
