"""Normalization and deterministic palette derivation for UI branding."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_HALF_UP
import re
from typing import Any, Mapping

DEFAULT_APP_TITLE = "OntoBricks"
DEFAULT_PRIMARY_COLOR = "#4F46E5"
DEFAULT_AURORA_COLOR = "#22A7C8"
DEFAULT_LOGO_PATH = "/static/global/img/favicon.svg"

_DARK_TEXT = "#111827"
_WHITE = "#FFFFFF"
_BLACK = "#000000"
_HEX_COLOR_RE = re.compile(r"^#[0-9A-F]{6}$")
_WARM_SURFACE_RGB = (255, 248, 239)


def _round_half_up(value: float) -> int:
    return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _normalize_hex_color(value: str) -> str:
    candidate = (value or "").strip().upper()
    if not _HEX_COLOR_RE.match(candidate):
        raise ValueError("Invalid primary color: expected #RRGGBB")
    return candidate


def _hex_to_rgb(value: str) -> tuple[int, int, int]:
    return (int(value[1:3], 16), int(value[3:5], 16), int(value[5:7], 16))


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    r, g, b = rgb
    return f"#{r:02X}{g:02X}{b:02X}"


def _mix_colors(
    rgb_a: tuple[int, int, int], rgb_b: tuple[int, int, int], ratio: float
) -> tuple[int, int, int]:
    """Linearly interpolate from *rgb_a* (ratio=0) to *rgb_b* (ratio=1)."""
    return tuple(
        max(0, min(255, _round_half_up(a * (1.0 - ratio) + b * ratio)))
        for a, b in zip(rgb_a, rgb_b)
    )


def _mix_with_black(rgb: tuple[int, int, int], ratio: float) -> tuple[int, int, int]:
    return _mix_colors(rgb, (0, 0, 0), ratio)


def _rgb_to_hsl(rgb: tuple[int, int, int]) -> tuple[float, float, float]:
    """Return (hue in [0, 360), saturation in [0, 1], lightness in [0, 1])."""
    r, g, b = (channel / 255.0 for channel in rgb)
    mx, mn = max(r, g, b), min(r, g, b)
    lightness = (mx + mn) / 2
    if mx == mn:
        return 0.0, 0.0, lightness
    delta = mx - mn
    saturation = delta / (2 - mx - mn) if lightness > 0.5 else delta / (mx + mn)
    if mx == r:
        hue = (g - b) / delta + (6 if g < b else 0)
    elif mx == g:
        hue = (b - r) / delta + 2
    else:
        hue = (r - g) / delta + 4
    return hue * 60, saturation, lightness


def _hue_to_channel(p: float, q: float, t: float) -> float:
    if t < 0:
        t += 1
    if t > 1:
        t -= 1
    if t < 1 / 6:
        return p + (q - p) * 6 * t
    if t < 1 / 2:
        return q
    if t < 2 / 3:
        return p + (q - p) * (2 / 3 - t) * 6
    return p


def _hsl_to_rgb(hue: float, saturation: float, lightness: float) -> tuple[int, int, int]:
    if saturation == 0:
        channel = _round_half_up(lightness * 255)
        return (channel, channel, channel)
    q = (
        lightness * (1 + saturation)
        if lightness < 0.5
        else lightness + saturation - lightness * saturation
    )
    p = 2 * lightness - q
    h = hue / 360
    r = _hue_to_channel(p, q, h + 1 / 3)
    g = _hue_to_channel(p, q, h)
    b = _hue_to_channel(p, q, h - 1 / 3)
    return tuple(max(0, min(255, _round_half_up(channel * 255))) for channel in (r, g, b))


_AURORA_HUE_SHIFT_DEG = -51.0
_AURORA_SATURATION_RANGE = (0.45, 0.75)
_AURORA_LIGHTNESS_RANGE = (0.40, 0.50)
_GRADIENT_MIX_RATIO = 0.45
_CANVAS_TINT_ALPHA = 0.05
_WHITE_RGB = (255, 255, 255)
_BLACK_RGB = (0, 0, 0)


def _derive_aurora_hex(primary_rgb: tuple[int, int, int]) -> str:
    """Rotate Primary's hue toward the cyan/teal family for the Aurora accent.

    The -51 degree shift and saturation/lightness clamps are tuned so the
    factory Primary (#4F46E5) lands within a few degrees of the factory
    Aurora constant (#22A7C8) — see
    ``test_aurora_is_derived_from_primary_when_omitted``.
    """
    hue, saturation, lightness = _rgb_to_hsl(primary_rgb)
    hue = (hue + _AURORA_HUE_SHIFT_DEG) % 360
    saturation = min(
        _AURORA_SATURATION_RANGE[1], max(_AURORA_SATURATION_RANGE[0], saturation)
    )
    lightness = min(
        _AURORA_LIGHTNESS_RANGE[1], max(_AURORA_LIGHTNESS_RANGE[0], lightness)
    )
    return _rgb_to_hex(_hsl_to_rgb(hue, saturation, lightness))


def _derive_gradient_end(
    primary_rgb: tuple[int, int, int],
    aurora_rgb: tuple[int, int, int],
    on_primary: str,
) -> str:
    """Mix Primary toward Aurora for the CTA gradient's second stop.

    Falls back toward black/white (whichever the gradient is trending away
    from) in 5% steps until the mixed stop keeps 4.5:1 contrast against
    *on_primary*; if even a full mix still fails, the gradient collapses to
    solid Primary so text is never illegible.
    """
    mixed = _mix_colors(primary_rgb, aurora_rgb, _GRADIENT_MIX_RATIO)
    on_primary_rgb = _hex_to_rgb(on_primary)
    if _contrast_ratio(mixed, on_primary_rgb) >= 4.5:
        return _rgb_to_hex(mixed)
    target = _BLACK_RGB if on_primary.upper() == "#FFFFFF" else _WHITE_RGB
    for step in range(1, 21):
        candidate = _mix_colors(mixed, target, min(1.0, step * 0.05))
        if _contrast_ratio(candidate, on_primary_rgb) >= 4.5:
            return _rgb_to_hex(candidate)
    return _rgb_to_hex(primary_rgb)


def _derive_canvas_tint(primary_rgb: tuple[int, int, int]) -> str:
    """Composite Primary at 5% alpha onto white for the page canvas tint."""
    return _rgb_to_hex(
        _composite_on_surface(primary_rgb, _CANVAS_TINT_ALPHA, _WHITE_RGB)
    )


def validate_optional_hex_color(value: str, field_label: str) -> str:
    """Validate an optional #RRGGBB API field.

    Blank input means "not provided" — callers derive a default. Non-blank,
    malformed input raises ``ValueError`` naming *field_label*, so callers
    can surface it as a 400 field error instead of a 500.
    """
    candidate = (value or "").strip()
    if not candidate:
        return ""
    try:
        return _normalize_hex_color(candidate)
    except ValueError as exc:
        raise ValueError(f"Invalid {field_label}: expected #RRGGBB") from exc


def _srgb_to_linear(channel: int) -> float:
    value = channel / 255.0
    if value <= 0.03928:
        return value / 12.92
    return ((value + 0.055) / 1.055) ** 2.4


def _relative_luminance(rgb: tuple[int, int, int]) -> float:
    r, g, b = rgb
    return (
        0.2126 * _srgb_to_linear(r)
        + 0.7152 * _srgb_to_linear(g)
        + 0.0722 * _srgb_to_linear(b)
    )


def _contrast_ratio(left: tuple[int, int, int], right: tuple[int, int, int]) -> float:
    l1 = _relative_luminance(left)
    l2 = _relative_luminance(right)
    lighter, darker = (l1, l2) if l1 >= l2 else (l2, l1)
    return (lighter + 0.05) / (darker + 0.05)


def _composite_on_surface(
    fg_rgb: tuple[int, int, int], alpha: float, bg_rgb: tuple[int, int, int]
) -> tuple[int, int, int]:
    return tuple(
        max(
            0,
            min(
                255,
                _round_half_up((fg_rgb[idx] * alpha) + (bg_rgb[idx] * (1.0 - alpha))),
            ),
        )
        for idx in range(3)
    )


def _choose_on_primary(primary_rgb: tuple[int, int, int]) -> str:
    dark_ratio = _contrast_ratio(primary_rgb, _hex_to_rgb(_DARK_TEXT))
    white_ratio = _contrast_ratio(primary_rgb, _hex_to_rgb(_WHITE))
    best = _DARK_TEXT if dark_ratio >= white_ratio else _WHITE
    best_ratio = dark_ratio if best == _DARK_TEXT else white_ratio
    if best_ratio < 4.5:
        # Defensive fallback for edge colors where #111827 and white are both
        # below 4.5:1 (e.g. mid greys).
        black_ratio = _contrast_ratio(primary_rgb, _hex_to_rgb(_BLACK))
        if black_ratio > best_ratio:
            return _BLACK
    return best


def _derive_selected_text(primary_rgb: tuple[int, int, int]) -> str:
    selected_bg = _composite_on_surface(primary_rgb, 0.10, _WARM_SURFACE_RGB)
    if _contrast_ratio(primary_rgb, selected_bg) >= 4.5:
        return _rgb_to_hex(primary_rgb)

    for step in range(1, 21):
        ratio = min(1.0, step * 0.05)
        candidate = _mix_with_black(primary_rgb, ratio)
        if _contrast_ratio(candidate, selected_bg) >= 4.5:
            return _rgb_to_hex(candidate)

    return _DARK_TEXT


@dataclass(frozen=True)
class BrandPalette:
    primary_rgb: str
    primary_dark: str
    primary_darker: str
    primary_light: str
    hover: str
    focus: str
    on_primary: str
    selected_text: str
    aurora: str
    aurora_rgb: str
    aurora_dark: str
    gradient_end: str
    canvas_tint: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UIBranding:
    version: int
    app_title: str
    primary_color: str
    aurora_color: str
    logo_data_url: str
    logo_url: str
    is_custom_logo: bool
    palette: BrandPalette

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def derive_brand_palette(
    primary_color: str, aurora_color: str | None = None
) -> BrandPalette:
    """Derive the full Aurora Pro palette from Primary (and optional Aurora).

    ``aurora_color``, when given, must already be a valid ``#RRGGBB`` string
    — this function raises on a malformed explicit value (mirrors
    ``primary_color``'s contract). Pass ``None`` to derive Aurora from Primary.
    """
    normalized = _normalize_hex_color(primary_color)
    rgb = _hex_to_rgb(normalized)
    primary_dark = _rgb_to_hex(_mix_with_black(rgb, 0.15))
    primary_darker = _rgb_to_hex(_mix_with_black(rgb, 0.30))
    rgb_csv = f"{rgb[0]}, {rgb[1]}, {rgb[2]}"
    on_primary = _choose_on_primary(rgb)

    if aurora_color:
        try:
            aurora_hex = _normalize_hex_color(aurora_color)
        except ValueError as exc:
            raise ValueError(f"Invalid aurora color: {exc}") from exc
    else:
        aurora_hex = _derive_aurora_hex(rgb)
    aurora_rgb = _hex_to_rgb(aurora_hex)
    aurora_rgb_csv = f"{aurora_rgb[0]}, {aurora_rgb[1]}, {aurora_rgb[2]}"

    return BrandPalette(
        primary_rgb=rgb_csv,
        primary_dark=primary_dark,
        primary_darker=primary_darker,
        primary_light=f"rgba({rgb_csv}, 0.10)",
        hover=f"rgba({rgb_csv}, 0.06)",
        focus=f"rgba({rgb_csv}, 0.18)",
        on_primary=on_primary,
        selected_text=_derive_selected_text(rgb),
        aurora=aurora_hex,
        aurora_rgb=aurora_rgb_csv,
        aurora_dark=_rgb_to_hex(_mix_with_black(aurora_rgb, 0.15)),
        gradient_end=_derive_gradient_end(rgb, aurora_rgb, on_primary),
        canvas_tint=_derive_canvas_tint(rgb),
    )


def normalize_ui_branding(raw: Mapping[str, Any]) -> UIBranding:
    data = dict(raw or {})

    title = str(data.get("app_title", DEFAULT_APP_TITLE)).strip()
    if not title:
        raise ValueError("Invalid title: value is required")
    if len(title) > 60:
        raise ValueError("Invalid title: maximum length is 60 characters")

    primary_color = _normalize_hex_color(
        str(data.get("primary_color", DEFAULT_PRIMARY_COLOR))
    )

    # Aurora is new and additive: unlike Primary, an invalid or missing
    # stored value must never fail the load — treat it as absent and
    # re-derive from Primary instead (see spec: "Invalid stored Aurora:
    # treat as missing and derive; do not blank the UI").
    aurora_raw = str(data.get("aurora_color", "") or "").strip()
    aurora_color: str | None = None
    if aurora_raw:
        try:
            aurora_color = _normalize_hex_color(aurora_raw)
        except ValueError:
            aurora_color = None

    palette = derive_brand_palette(primary_color, aurora_color)

    logo_data_url = str(data.get("logo_data_url", "") or "").strip()
    logo_url = logo_data_url if logo_data_url else DEFAULT_LOGO_PATH

    return UIBranding(
        version=int(data.get("version", 1) or 1),
        app_title=title,
        primary_color=primary_color,
        aurora_color=palette.aurora,
        logo_data_url=logo_data_url,
        logo_url=logo_url,
        is_custom_logo=bool(logo_data_url),
        palette=palette,
    )
