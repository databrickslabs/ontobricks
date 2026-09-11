"""Normalization and deterministic palette derivation for UI branding."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_HALF_UP
import re
from typing import Any, Mapping

DEFAULT_APP_TITLE = "OntoBricks"
DEFAULT_PRIMARY_COLOR = "#4F46E5"
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


def _mix_with_black(rgb: tuple[int, int, int], ratio: float) -> tuple[int, int, int]:
    scale = 1.0 - ratio
    return tuple(max(0, min(255, _round_half_up(channel * scale))) for channel in rgb)


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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UIBranding:
    version: int
    app_title: str
    primary_color: str
    logo_data_url: str
    logo_url: str
    is_custom_logo: bool
    palette: BrandPalette

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def derive_brand_palette(primary_color: str) -> BrandPalette:
    normalized = _normalize_hex_color(primary_color)
    rgb = _hex_to_rgb(normalized)
    primary_dark = _rgb_to_hex(_mix_with_black(rgb, 0.15))
    primary_darker = _rgb_to_hex(_mix_with_black(rgb, 0.30))
    rgb_csv = f"{rgb[0]}, {rgb[1]}, {rgb[2]}"
    return BrandPalette(
        primary_rgb=rgb_csv,
        primary_dark=primary_dark,
        primary_darker=primary_darker,
        primary_light=f"rgba({rgb_csv}, 0.10)",
        hover=f"rgba({rgb_csv}, 0.06)",
        focus=f"rgba({rgb_csv}, 0.18)",
        on_primary=_choose_on_primary(rgb),
        selected_text=_derive_selected_text(rgb),
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

    logo_data_url = str(data.get("logo_data_url", "") or "").strip()
    logo_url = logo_data_url if logo_data_url else DEFAULT_LOGO_PATH

    return UIBranding(
        version=int(data.get("version", 1) or 1),
        app_title=title,
        primary_color=primary_color,
        logo_data_url=logo_data_url,
        logo_url=logo_url,
        is_custom_logo=bool(logo_data_url),
        palette=derive_brand_palette(primary_color),
    )
