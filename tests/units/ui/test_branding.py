"""Unit tests for normalized UI branding and derived palette."""

from __future__ import annotations

import pytest

from back.core.helpers.UIBranding import (
    DEFAULT_APP_TITLE,
    DEFAULT_AURORA_COLOR,
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


def test_aurora_is_derived_from_primary_when_omitted():
    branding = normalize_ui_branding({"primary_color": "#4F46E5"})
    assert branding.aurora_color != ""
    assert branding.palette.aurora == branding.aurora_color
    # Acceptance from the design spec: for the factory primary, the derived
    # hue must land close to the factory Aurora constant's hue (~192 deg).
    factory_hue, _, _ = _rgb_to_hsl_for_test(DEFAULT_AURORA_COLOR)
    derived_hue, _, _ = _rgb_to_hsl_for_test(branding.aurora_color)
    diff = abs(factory_hue - derived_hue)
    assert min(diff, 360 - diff) <= 15


def _rgb_to_hsl_for_test(hex_color: str) -> tuple[float, float, float]:
    from back.core.helpers.UIBranding import _hex_to_rgb, _rgb_to_hsl

    return _rgb_to_hsl(_hex_to_rgb(hex_color))


def test_explicit_aurora_is_normalized_and_preserved():
    branding = normalize_ui_branding(
        {"primary_color": "#4F46E5", "aurora_color": "#22a7c8"}
    )
    assert branding.aurora_color == "#22A7C8"
    assert branding.palette.aurora == "#22A7C8"


def test_invalid_stored_aurora_is_treated_as_missing_not_fatal():
    branding = normalize_ui_branding(
        {"primary_color": "#4F46E5", "aurora_color": "not-a-color"}
    )
    # Falls back to derivation instead of raising.
    assert branding.aurora_color != ""


def test_derive_brand_palette_rejects_malformed_explicit_aurora():
    with pytest.raises(ValueError, match="aurora"):
        derive_brand_palette("#4F46E5", "not-a-color")


def test_gradient_end_meets_contrast_against_on_primary():
    for primary in ("#4F46E5", "#111111", "#F5E642", "#0E9F6E"):
        palette = derive_brand_palette(primary)
        assert contrast_ratio(palette.on_primary, palette.gradient_end) >= 4.5


def test_canvas_tint_is_a_pale_composite_of_primary_on_white():
    palette = derive_brand_palette("#4F46E5")
    r, g, b = _hex_to_rgb(palette.canvas_tint)
    # 5% composite of a saturated primary on white must stay near-white.
    assert r > 235 and g > 235 and b > 235


def test_validate_optional_hex_color_accepts_blank_and_rejects_garbage():
    from back.core.helpers.UIBranding import validate_optional_hex_color

    assert validate_optional_hex_color("", "aurora color") == ""
    assert validate_optional_hex_color("  ", "aurora color") == ""
    assert validate_optional_hex_color("#22A7C8", "aurora color") == "#22A7C8"
    with pytest.raises(ValueError, match="Invalid aurora color"):
        validate_optional_hex_color("nope", "aurora color")
