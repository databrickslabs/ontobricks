"""Unit tests for normalized UI branding and derived palette."""

from __future__ import annotations

import pytest

from back.core.helpers.UIBranding import (
    DEFAULT_APP_TITLE,
    DEFAULT_LOGO_PATH,
    DEFAULT_PRIMARY_COLOR,
    derive_brand_palette,
    normalize_ui_branding,
)


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    color = color.strip().upper()
    return (int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16))


def _srgb_to_linear(channel: int) -> float:
    v = channel / 255.0
    if v <= 0.03928:
        return v / 12.92
    return ((v + 0.055) / 1.055) ** 2.4


def contrast_ratio(left: str, right: str) -> float:
    lr, lg, lb = _hex_to_rgb(left)
    rr, rg, rb = _hex_to_rgb(right)
    l1 = (
        0.2126 * _srgb_to_linear(lr)
        + 0.7152 * _srgb_to_linear(lg)
        + 0.0722 * _srgb_to_linear(lb)
    )
    l2 = (
        0.2126 * _srgb_to_linear(rr)
        + 0.7152 * _srgb_to_linear(rg)
        + 0.0722 * _srgb_to_linear(rb)
    )
    lighter, darker = (l1, l2) if l1 >= l2 else (l2, l1)
    return (lighter + 0.05) / (darker + 0.05)


def test_default_branding_is_ontobricks_indigo():
    branding = normalize_ui_branding({})
    assert branding.app_title == "OntoBricks"
    assert branding.primary_color == "#4F46E5"
    assert branding.logo_url == "/static/global/img/favicon.svg"
    assert branding.palette.primary_rgb == "79, 70, 229"


def test_defaults_constants_are_exposed():
    assert DEFAULT_APP_TITLE == "OntoBricks"
    assert DEFAULT_PRIMARY_COLOR == "#4F46E5"
    assert DEFAULT_LOGO_PATH == "/static/global/img/favicon.svg"


def test_title_is_trimmed_and_color_is_uppercase_hex():
    branding = normalize_ui_branding(
        {"app_title": "  Acme Graph  ", "primary_color": "#4f46e5"}
    )
    assert branding.app_title == "Acme Graph"
    assert branding.primary_color == "#4F46E5"


@pytest.mark.parametrize("title", ["", " ", "x" * 61])
def test_invalid_title_is_rejected(title: str):
    with pytest.raises(ValueError, match="title"):
        normalize_ui_branding({"app_title": title})


@pytest.mark.parametrize("color", ["red", "#fff", "#GG46E5", "", "#4f46e511"])
def test_invalid_primary_color_is_rejected(color: str):
    with pytest.raises(ValueError, match="primary color"):
        normalize_ui_branding({"primary_color": color})


def test_palette_derivation_is_deterministic():
    first = derive_brand_palette("#123456")
    second = derive_brand_palette("#123456")
    assert first == second
    assert first.primary_rgb == "18, 52, 86"
    assert first.primary_dark == "#0F2C49"
    assert first.primary_darker == "#0D243C"
    assert first.primary_light == "rgba(18, 52, 86, 0.10)"
    assert first.hover == "rgba(18, 52, 86, 0.06)"
    assert first.focus == "rgba(18, 52, 86, 0.18)"


def test_on_primary_always_meets_wcag_contrast():
    for color in ("#111111", "#777777", "#F5E642", "#4F46E5"):
        palette = derive_brand_palette(color)
        assert contrast_ratio(color, palette.on_primary) >= 4.5


def test_default_logo_path_is_used_when_custom_logo_is_empty():
    branding = normalize_ui_branding({"logo_data_url": ""})
    assert branding.logo_data_url == ""
    assert branding.logo_url == DEFAULT_LOGO_PATH
    assert branding.is_custom_logo is False


def test_custom_data_logo_is_preserved_and_exposed():
    data_url = "data:image/png;base64,abc123"
    branding = normalize_ui_branding({"logo_data_url": data_url})
    assert branding.logo_data_url == data_url
    assert branding.logo_url == data_url
    assert branding.is_custom_logo is True


def test_serialization_is_immutable_and_complete():
    branding = normalize_ui_branding({"app_title": "Acme", "primary_color": "#123456"})
    payload = branding.to_dict()
    assert payload["version"] == 1
    assert payload["app_title"] == "Acme"
    assert payload["primary_color"] == "#123456"
    assert payload["palette"]["primary_rgb"] == "18, 52, 86"
