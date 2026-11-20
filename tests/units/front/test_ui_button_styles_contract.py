"""Contracts for the Create / Share button styles in Settings → UI → Theme."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
SETTINGS_TEMPLATE = REPO_ROOT / "src/front/templates/settings.html"
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"
MAIN_CSS = REPO_ROOT / "src/front/static/global/css/main.css"
COMPONENTS_CSS = REPO_ROOT / "src/front/static/global/css/components.css"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("kind", ["ButtonPrimary", "ButtonSecondary"])
def test_theme_card_exposes_color_and_hex_pickers(kind):
    html = _read(SETTINGS_TEMPLATE)
    for suffix in ("Color", "Hex", "Help", "Error"):
        element_id = f"uiBranding{kind}{suffix}"
        assert f'id="{element_id}"' in html, f"missing {element_id}"


def test_theme_preview_shows_create_and_share_buttons():
    html = _read(SETTINGS_TEMPLATE)
    assert "btn-primary ui-branding-preview-btn" in html
    assert "btn-secondary ui-branding-preview-btn" in html


def test_settings_js_posts_both_button_colors():
    js = _read(SETTINGS_JS)
    assert "formData.append('button_primary_color'" in js
    assert "formData.append('button_secondary_color'" in js


def test_settings_js_mirrors_databricks_defaults_and_derivation():
    js = _read(SETTINGS_JS)
    assert "button_primary_color: '#2272B4'" in js
    assert "button_secondary_color: '#11171C'" in js
    assert "function deriveButtonPalette(" in js
    assert "BUTTON_COLOR_FIELDS" in js


def test_preview_publishes_create_and_share_variables():
    js = _read(SETTINGS_JS)
    for var in (
        "--db-btn-primary",
        "--db-btn-primary-hover",
        "--db-btn-primary-on",
        "--db-btn-secondary",
        "--db-btn-secondary-border",
        "--db-btn-secondary-hover",
    ):
        assert f"setProperty('{var}'" in js, f"preview does not set {var}"


def test_main_css_defines_button_tokens_with_databricks_defaults():
    css = _read(MAIN_CSS)
    assert "--db-btn-primary: #2272B4;" in css
    assert "--db-btn-primary-hover: #1D619C;" in css
    assert "--db-btn-secondary: #11171C;" in css
    assert "--db-btn-secondary-border: #C6C9D1;" in css
    assert "--db-btn-secondary-hover: #F0F1F3;" in css


def test_components_css_uses_tokens_not_gradients_for_create():
    css = _read(COMPONENTS_CSS)
    assert "var(--db-btn-primary)" in css
    assert "var(--db-btn-secondary-border)" in css
