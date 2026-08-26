"""Request-scoped UI branding resolution and rendering helpers."""

from __future__ import annotations

from typing import Any, Mapping

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from back.core.helpers import resolve_app_registry_context
from back.core.helpers.UIBranding import (
    DEFAULT_APP_TITLE,
    DEFAULT_LOGO_PATH,
    DEFAULT_PRIMARY_COLOR,
    normalize_ui_branding,
)
from back.core.logging import get_logger
from back.objects.session import global_config_service
from shared.config.settings import Settings, get_settings

logger = get_logger(__name__)


def _default_branding() -> dict[str, Any]:
    return normalize_ui_branding(
        {
            "version": 1,
            "app_title": DEFAULT_APP_TITLE,
            "primary_color": DEFAULT_PRIMARY_COLOR,
            "logo_data_url": "",
        }
    ).to_dict()


def _normalize_branding(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    try:
        return normalize_ui_branding(raw or {}).to_dict()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Invalid UI branding payload, falling back to defaults: %s", exc)
        return _default_branding()


def resolve_request_ui_branding(
    request: Request,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Resolve normalized branding for the current request."""
    try:
        settings = settings or get_settings()
        host, token, registry_cfg = resolve_app_registry_context(settings)
        raw = global_config_service.get_ui_branding(host, token, registry_cfg)
        return _normalize_branding(raw)
    except Exception as exc:  # noqa: BLE001
        # Branding must never break page rendering.
        logger.debug("Could not resolve request UI branding: %s", exc)
        return _default_branding()


def get_request_ui_branding(request: Request | None) -> dict[str, Any]:
    """Read request branding once and memoize normalized value on state."""
    if request is None:
        return _default_branding()

    memoized = getattr(request.state, "ui_branding_normalized", None)
    if isinstance(memoized, dict):
        return memoized

    branding = getattr(request.state, "ui_branding", None)
    if isinstance(branding, Mapping):
        normalized = _normalize_branding(branding)
        request.state.ui_branding_normalized = normalized
        return normalized

    default = _default_branding()
    request.state.ui_branding_normalized = default
    return default


def brand_page_title(branding: Mapping[str, Any], page_label: str = "") -> str:
    app_title = str(branding.get("app_title") or DEFAULT_APP_TITLE).strip() or DEFAULT_APP_TITLE
    page = str(page_label or "").strip()
    return f"{page} - {app_title}" if page else app_title


def render_ui_branding_css_vars(branding: Mapping[str, Any]) -> str:
    palette = branding.get("palette", {}) if isinstance(branding, Mapping) else {}
    primary = str(branding.get("primary_color") or DEFAULT_PRIMARY_COLOR)
    primary_rgb = str(palette.get("primary_rgb") or "79, 70, 229")
    primary_dark = str(palette.get("primary_dark") or "#4338CA")
    primary_darker = str(palette.get("primary_darker") or "#3730A3")
    primary_light = str(palette.get("primary_light") or "rgba(79, 70, 229, 0.10)")
    hover = str(palette.get("hover") or "rgba(79, 70, 229, 0.06)")
    focus = str(palette.get("focus") or "rgba(79, 70, 229, 0.18)")
    on_primary = str(palette.get("on_primary") or "#FFFFFF")
    selected_text = str(palette.get("selected_text") or "#3730A3")

    return (
        ":root {\n"
        f"  --db-primary: {primary};\n"
        f"  --db-primary-rgb: {primary_rgb};\n"
        f"  --db-primary-dark: {primary_dark};\n"
        f"  --db-primary-darker: {primary_darker};\n"
        f"  --db-primary-light: {primary_light};\n"
        f"  --db-hover: {hover};\n"
        f"  --db-focus: {focus};\n"
        f"  --db-hover-indigo: {hover};\n"
        f"  --db-on-primary: {on_primary};\n"
        f"  --db-primary-selected-text: {selected_text};\n"
        f"  --db-focus-ring: 0 0 0 0.2rem {focus};\n"
        f"  --db-shadow-primary: 0 0 0 3px {focus};\n"
        "}"
    )


def _is_html_navigation_request(request: Request) -> bool:
    if request.method not in {"GET", "HEAD"}:
        return False
    path = request.scope.get("path", "")
    if path.startswith("/static/"):
        return False
    accept = (request.headers.get("accept") or "").lower()
    return "text/html" in accept


class UIBrandingMiddleware(BaseHTTPMiddleware):
    """Attach request-scoped UI branding for server-rendered pages."""

    def __init__(self, app, settings: Settings | None = None):
        super().__init__(app)
        self._settings = settings or get_settings()

    async def dispatch(self, request: Request, call_next) -> Response:
        if _is_html_navigation_request(request):
            resolved = resolve_request_ui_branding(request, self._settings)
            request.state.ui_branding = resolved
            request.state.ui_branding_normalized = resolved
        return await call_next(request)

