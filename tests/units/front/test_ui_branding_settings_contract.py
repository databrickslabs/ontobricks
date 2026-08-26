"""Contracts for Settings → Configuration → UI branding section."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MENU_CONFIG = REPO_ROOT / "src/front/config/menu_config.json"
SETTINGS_TEMPLATE = REPO_ROOT / "src/front/templates/settings.html"
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"
SIDEBAR_NAV_JS = REPO_ROOT / "src/front/static/global/js/sidebar-nav.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _settings_menu_items() -> list[dict]:
    data = json.loads(_read(MENU_CONFIG))
    settings_menu = next(m for m in data["menus"] if m["id"] == "settings")
    config_group = next(g for g in settings_menu["groups"] if g["id"] == "settings-config")
    return config_group["items"]


def _extract_section(html: str, section_id: str) -> str:
    match = re.search(
        rf'<div id="{re.escape(section_id)}-section" class="sidebar-section".*?</div>\s*</div>\s*(?=<!--\s*=+|\{{%)',
        html,
        flags=re.DOTALL,
    )
    assert match, f"Section {section_id} not found"
    return match.group(0)


def test_configuration_menu_contains_ui_item_after_global():
    items = _settings_menu_items()
    ids = [item["id"] for item in items]

    assert "global" in ids
    assert "ui" in ids
    assert ids.index("ui") > ids.index("global")

    ui_item = next(item for item in items if item["id"] == "ui")
    assert ui_item["label"] == "UI"
    assert ui_item["icon"] == "bi-palette"


def test_template_contains_ui_section_with_branding_and_theme_cards():
    html = _read(SETTINGS_TEMPLATE)

    assert 'id="ui-section"' in html
    assert "Branding" in html
    assert "Theme" in html
    assert 'id="uiBrandingTitle"' in html
    assert 'id="uiBrandingPrimaryColor"' in html
    assert 'id="uiBrandingPrimaryHex"' in html
    assert 'id="uiBrandingLogoFile"' in html
    assert 'id="uiBrandingResetIconBtn"' in html
    assert 'id="uiBrandingSaveBtn"' in html
    assert 'id="uiBrandingDiscardBtn"' in html
    assert 'id="uiBrandingResetDefaultsBtn"' in html
    assert 'id="uiBrandingStatus"' in html


def test_logo_file_input_has_accessible_label_and_help_association():
    html = _read(SETTINGS_TEMPLATE)
    assert 'for="uiBrandingLogoFile"' in html
    assert 'id="uiBrandingLogoHelp"' in html
    assert 'aria-describedby="uiBrandingLogoHelp"' in html or 'aria-label="Application logo file"' in html


def test_template_exposes_validation_error_associations_for_branding_inputs():
    html = _read(SETTINGS_TEMPLATE)
    assert 'id="uiBrandingTitleError"' in html
    assert 'id="uiBrandingColorError"' in html
    assert 'aria-describedby="uiBrandingTitleHelp uiBrandingTitleError"' in html
    assert 'aria-describedby="uiBrandingColorHelp uiBrandingColorError"' in html


def test_visual_controls_are_moved_out_of_global_section():
    html = _read(SETTINGS_TEMPLATE)
    global_section = _extract_section(html, "global")
    ui_section = _extract_section(html, "ui")

    assert "Default Entity Icon" not in global_section
    assert "Application Logo" not in global_section

    assert "Default Entity Icon" in ui_section
    assert "Application Logo" in ui_section


def test_settings_template_has_no_duplicate_dom_ids():
    html = _read(SETTINGS_TEMPLATE)
    ids = re.findall(r'\bid="([^"]+)"', html)
    duplicates = sorted({item for item in ids if ids.count(item) > 1})
    assert not duplicates, "Duplicate ids found: " + ", ".join(duplicates)


def test_settings_js_defines_ui_branding_workflow_functions():
    js = _read(SETTINGS_JS)
    for fn_name in (
        "loadUIBranding",
        "previewUIBranding",
        "saveUIBranding",
        "discardUIBrandingChanges",
        "resetUIBrandingDefaults",
    ):
        assert f"function {fn_name}" in js


def test_settings_js_uses_atomic_ui_branding_api_and_formdata():
    js = _read(SETTINGS_JS)

    assert "/settings/ui-branding" in js
    assert "new FormData()" in js
    assert "formData.append('app_title'" in js
    assert "formData.append('primary_color'" in js
    assert "formData.append('reset_logo'" in js
    assert "formData.append('logo_file'" in js


def test_settings_js_previews_title_css_vars_and_icons_accessibly():
    js = _read(SETTINGS_JS)

    assert "brandTitleText" in js
    assert ".textContent =" in js
    assert ".innerHTML =" not in js[js.find("function previewUIBranding") : js.find("function saveUIBranding")]
    assert "document.documentElement.style.setProperty('--db-primary'" in js
    assert "document.documentElement.style.setProperty('--db-primary-rgb'" in js
    assert "document.documentElement.style.setProperty('--db-on-primary'" in js
    assert "querySelectorAll('[data-brand-icon]')" in js
    assert "querySelectorAll('link[rel" in js
    assert "setAttribute('href'" in js


def test_settings_js_tracks_baseline_draft_and_discard_reset_behaviors():
    js = _read(SETTINGS_JS)

    assert "let savedUIBranding" in js
    assert "let draftUIBranding" in js
    assert "let uiBrandingDirty" in js
    assert "let uiBrandingValid" in js
    assert "discardUIBrandingChanges" in js
    assert "resetUIBrandingIconDraft" in js
    assert "resetUIBrandingDefaults" in js
    assert "URL.revokeObjectURL" in js or ".abort()" in js
    assert "SidebarNav.init({" in js
    assert "onBeforeSectionChange:" in js
    assert "await handleUIBrandingBeforeSectionChange" in js
    assert "document.addEventListener('click', async (event)" not in js


def test_settings_js_supports_preview_only_logo_reset_action():
    js = _read(SETTINGS_JS)
    assert "function resetUIBrandingIconDraft()" in js
    assert "uiBrandingResetIconBtn" in js
    assert "uiBrandingResetLogo = isCustomLogoBranding(savedUIBranding);" in js
    assert "uiBrandingPendingLogoFile = null;" in js
    assert "clearUIBrandingPreviewObjectUrl();" in js
    assert "draftUIBranding.preview_logo_url = '';" in js
    assert "previewUIBranding(draftUIBranding);" in js


def test_settings_js_toggles_aria_invalid_and_error_associations():
    js = _read(SETTINGS_JS)
    assert "setAttribute('aria-invalid', 'true')" in js
    assert "removeAttribute('aria-invalid')" in js
    assert "uiBrandingTitleError" in js
    assert "uiBrandingColorError" in js


def test_swatches_are_semantically_exposed_and_updated_with_values():
    html = _read(SETTINGS_TEMPLATE)
    js = _read(SETTINGS_JS)
    for swatch_id in (
        "uiSwatchPrimary",
        "uiSwatchHover",
        "uiSwatchLight",
        "uiSwatchFocus",
        "uiSwatchOnPrimary",
    ):
        assert f'id="{swatch_id}"' in html
    assert 'role="img"' in html
    assert "setAttribute('aria-label'" in js
    assert "setAttribute('title'" in js
    assert "Focus Ring" in js


def test_settings_uses_sidebar_nav_before_change_guard_contract():
    js = _read(SETTINGS_JS)
    assert "window.SIDEBAR_NAV_MANUAL_INIT = true;" in js
    dom_ready_idx = js.find("document.addEventListener('DOMContentLoaded'")
    manual_idx = js.find("window.SIDEBAR_NAV_MANUAL_INIT = true;")
    assert manual_idx != -1 and dom_ready_idx != -1 and manual_idx < dom_ready_idx
    init_call = re.search(r"SidebarNav\.init\(\{(.*?)\}\);", js, flags=re.DOTALL)
    assert init_call, "Settings must initialize SidebarNav with explicit options"
    body = init_call.group(1)
    assert "onBeforeSectionChange" in body
    assert "onSectionChange" in body
    assert "loadUIBranding()" in body


def test_sidebar_guard_cancel_keeps_ui_and_confirm_discards():
    js = _read(SETTINGS_JS)
    assert "async function handleUIBrandingBeforeSectionChange(targetSection)" in js
    assert "if (activeSection !== 'ui'" in js
    assert "if (!uiBrandingDirty) return true;" in js
    assert "if (!confirmed) return false;" in js
    assert "discardUIBrandingChanges();" in js
    assert "return true;" in js


def test_client_title_validation_counts_unicode_code_points():
    js = _read(SETTINGS_JS)
    assert "Array.from(normalizeTitle(value)).length" in js
    assert "title.length <= 60" not in js


def test_choose_on_primary_matches_backend_fallback_to_black():
    js = _read(SETTINGS_JS)
    fn = re.search(r"function chooseOnPrimary\(primaryRgb\) \{(.*?)\n\s*\}", js, flags=re.DOTALL)
    assert fn, "chooseOnPrimary function not found"
    body = fn.group(1)
    assert "bestRatio < 4.5" in body
    assert "blackRatio" in body
    assert "'#000000'" in body


def test_title_normalization_is_trimmed_consistently():
    js = _read(SETTINGS_JS)
    assert ".trimStart()" not in js
    assert "normalizeTitle(" in js
    assert "normalizeTitle(titleInput.value)" in js


def test_reset_defaults_is_noop_when_saved_state_is_already_default():
    js = _read(SETTINGS_JS)
    reset_fn = re.search(
        r"function resetUIBrandingDefaults\(\) \{(.*?)\n\s*\}",
        js,
        flags=re.DOTALL,
    )
    assert reset_fn, "resetUIBrandingDefaults function not found"
    body = reset_fn.group(1)
    assert "isSavedUIBrandingDefaultState()" in body
    assert "uiBrandingResetLogo = false;" in body


def test_sidebar_nav_init_is_idempotent_to_avoid_duplicate_listeners():
    js = _read(SIDEBAR_NAV_JS)
    assert "_initialized: false" in js
    assert "if (SidebarNav._initialized) {" in js
    assert "SidebarNav._initialized = true;" in js


def test_sidebar_nav_keeps_callbacks_on_later_no_options_init():
    js = _read(SIDEBAR_NAV_JS)
    assert "Object.prototype.hasOwnProperty.call(options, 'onSectionChange')" in js
    assert "Object.prototype.hasOwnProperty.call(options, 'onBeforeSectionChange')" in js
    assert "SidebarNav._onSectionChange = options.onSectionChange || null;" not in js
    assert "SidebarNav._onBeforeSectionChange = options.onBeforeSectionChange || null;" not in js
