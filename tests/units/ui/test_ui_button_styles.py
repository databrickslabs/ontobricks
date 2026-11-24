"""Create (filled) and Share (outlined) button styles: palette + CSS variables."""

from __future__ import annotations

import pytest

from back.core.helpers.UIBranding import (
    DEFAULT_BUTTON_PRIMARY_COLOR,
    DEFAULT_BUTTON_SECONDARY_COLOR,
    derive_button_palette,
    normalize_ui_branding,
)
from shared.fastapi.ui_branding import render_ui_branding_css_vars

pytestmark = pytest.mark.unit


def test_defaults_match_databricks_button_styles_exactly():
    assert DEFAULT_BUTTON_PRIMARY_COLOR == "#2272B4"
    assert DEFAULT_BUTTON_SECONDARY_COLOR == "#11171C"
    palette = derive_button_palette()
    assert palette == {
        "button_primary": "#2272B4",
        "button_primary_hover": "#1D619C",
        "button_on_primary": "#FFFFFF",
        "button_secondary": "#11171C",
        "button_secondary_border": "#C6C9D1",
        "button_secondary_hover": "#F0F1F3",
    }


def test_custom_colors_derive_companion_tints():
    palette = derive_button_palette("#FF0000", "#00AA00")
    assert palette["button_primary"] == "#FF0000"
    assert palette["button_primary_hover"] == "#DB0000"
    assert palette["button_on_primary"] == "#000000"
    assert palette["button_secondary"] == "#00AA00"
    assert palette["button_secondary_border"] == "#C2EBC2"
    assert palette["button_secondary_hover"] == "#F0FAF0"


def test_normalize_keeps_valid_and_replaces_invalid_stored_colors():
    branding = normalize_ui_branding(
        {"button_primary_color": "#ff0000", "button_secondary_color": "garbage"}
    )
    assert branding.button_primary_color == "#FF0000"
    assert branding.button_secondary_color == DEFAULT_BUTTON_SECONDARY_COLOR
    assert branding.palette.button_primary == "#FF0000"


def test_css_variables_publish_create_and_share_tokens():
    branding = normalize_ui_branding({"button_primary_color": "#FF0000"}).to_dict()
    css = render_ui_branding_css_vars(branding)
    assert "--db-btn-primary: #FF0000;" in css
    assert "--db-btn-primary-hover:" in css
    # Red is dark enough that the label flips to black for contrast.
    assert "--db-btn-primary-on: #000000;" in css
    assert "--db-btn-secondary: #11171C;" in css
    assert "--db-btn-secondary-border: #C6C9D1;" in css
    assert "--db-btn-secondary-hover: #F0F1F3;" in css
