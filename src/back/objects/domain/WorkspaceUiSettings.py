"""Workspace UI branding and instance preference Settings.

Extracted from :class:`SettingsService` (Fowler Extract Class).
``SettingsService`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, Optional

from back.core.errors import InfrastructureError, ValidationError
from shared.config.settings import Settings
from back.core.logging import get_logger
from back.objects.session import SessionManager
from back.objects.domain.SettingsService import SettingsService

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule. Tests patch that module's globals (``global_config_service``,
# ``resolve_app_registry_context``).
_ss = importlib.import_module("back.objects.domain.SettingsService")

logger = get_logger(__name__)


class WorkspaceUiSettings:
    """Navbar branding, logos, cache TTL, graph limits, analytics and CloudFetch."""

    # Recommended upload size & format for the top-bar logo.
    # The navbar renders the image at 24×24 CSS pixels; keeping the source
    # at 64×64 (≈2.7×) gives crisp rendering on retina displays without
    # bloating the global config blob.
    NAVBAR_LOGO_RECOMMENDED_SIZE = "64×64 px"
    _NAVBAR_LOGO_ALLOWED_MIME = {
        "image/svg+xml",
        "image/png",
        "image/jpeg",
        "image/webp",
        "image/gif",
    }
    _NAVBAR_LOGO_MAX_BYTES = 1024 * 1024  # 1 MB — way more than a 64×64 icon needs

    @staticmethod
    def set_default_emoji_result(
        emoji: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = _ss.global_config_service.set_default_emoji(
            host, token, registry_cfg, emoji
        )
        if not ok:
            raise InfrastructureError("Failed to save default emoji", detail=msg)
        return {"success": True, "emoji": emoji}

    @staticmethod
    def save_base_uri_result(
        base_uri: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = _ss.global_config_service.set_default_base_uri(
            host, token, registry_cfg, base_uri
        )
        if not ok:
            raise InfrastructureError("Failed to save default base URI", detail=msg)
        return {"success": True, "base_uri": base_uri}

    @staticmethod
    def _validate_and_encode_logo(content: bytes, content_type: str) -> tuple[str, str]:
        """Validate logo payload and return ``(mime, data_url)``."""
        if not content:
            raise ValidationError("Empty file — pick an image to upload")
        if len(content) > WorkspaceUiSettings._NAVBAR_LOGO_MAX_BYTES:
            raise ValidationError(
                f"Logo too large ({len(content)} bytes); "
                f"max {WorkspaceUiSettings._NAVBAR_LOGO_MAX_BYTES} bytes"
            )

        mime = (content_type or "").split(";", 1)[0].strip().lower()
        if mime not in WorkspaceUiSettings._NAVBAR_LOGO_ALLOWED_MIME:
            raise ValidationError(
                f"Unsupported image type '{mime}'. "
                f"Allowed: {', '.join(sorted(WorkspaceUiSettings._NAVBAR_LOGO_ALLOWED_MIME))}"
            )

        import base64

        b64 = base64.b64encode(content).decode("ascii")
        return mime, f"data:{mime};base64,{b64}"

    @staticmethod
    def get_ui_branding_result(
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return normalized UI branding payload for Settings."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)
        host, token, registry_cfg = _ss.resolve_app_registry_context(settings)
        return {
            "success": True,
            "branding": _ss.global_config_service.get_ui_branding(host, token, registry_cfg),
        }

    @staticmethod
    def save_ui_branding_result(
        app_title: str,
        primary_color: str,
        aurora_color: str,
        logo_content: Optional[bytes],
        logo_mime: Optional[str],
        reset_logo: bool,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Validate and persist title/color/Aurora/logo atomically (admin only)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        if logo_content is not None and reset_logo:
            raise ValidationError("reset_logo cannot be true when logo_file is provided")

        try:
            validated_aurora = _ss.validate_optional_hex_color(aurora_color, "aurora color")
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

        host, token, registry_cfg = _ss.resolve_app_registry_context(settings)
        current = _ss.global_config_service.get_ui_branding(host, token, registry_cfg)

        logo_data_url = str(current.get("logo_data_url", "") or "")
        if reset_logo:
            logo_data_url = ""
        elif logo_content is not None:
            _, logo_data_url = SettingsService._validate_and_encode_logo(
                logo_content, logo_mime or ""
            )

        try:
            normalized = _ss.normalize_ui_branding(
                {
                    "version": current.get("version", 1),
                    "app_title": app_title,
                    "primary_color": primary_color,
                    "aurora_color": validated_aurora,
                    "logo_data_url": logo_data_url,
                }
            )
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

        ok, msg = _ss.global_config_service.set_ui_branding(
            host,
            token,
            registry_cfg,
            {
                "version": normalized.version,
                "app_title": normalized.app_title,
                "primary_color": normalized.primary_color,
                "aurora_color": normalized.aurora_color,
                "logo_data_url": normalized.logo_data_url,
            },
        )
        if not ok:
            raise InfrastructureError("Failed to save UI branding", detail=msg)

        return {"success": True, "branding": normalized.to_dict()}

    @staticmethod
    def get_navbar_logo_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the configured navbar logo (data URL) or the bundled default."""
        host, token, registry_cfg = _ss.resolve_app_registry_context(settings)
        branding = _ss.global_config_service.get_ui_branding(host, token, registry_cfg)
        custom = str(branding.get("logo_data_url", "") or "")
        return {
            "success": True,
            "logo_url": custom or _ss.DEFAULT_LOGO_PATH,
            "is_custom": bool(custom),
            "default_url": _ss.DEFAULT_LOGO_PATH,
            "recommended_size": WorkspaceUiSettings.NAVBAR_LOGO_RECOMMENDED_SIZE,
            "max_bytes": WorkspaceUiSettings._NAVBAR_LOGO_MAX_BYTES,
            "allowed_mime": sorted(WorkspaceUiSettings._NAVBAR_LOGO_ALLOWED_MIME),
        }

    @staticmethod
    def upload_navbar_logo_result(
        content: bytes,
        content_type: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Validate and persist an uploaded navbar logo (admin only, stored globally)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)
        mime, data_url = SettingsService._validate_and_encode_logo(content, content_type)

        host, token, registry_cfg = _ss.resolve_app_registry_context(settings)
        ok, msg = _ss.global_config_service.set_navbar_logo(
            host, token, registry_cfg, data_url
        )
        if not ok:
            raise InfrastructureError("Failed to save navbar logo", detail=msg)
        return {
            "success": True,
            "logo_url": data_url,
            "is_custom": True,
            "size_bytes": len(content),
            "mime": mime,
        }

    @staticmethod
    def reset_navbar_logo_result(
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Clear the custom navbar logo so the bundled default is used again."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        host, token, registry_cfg = _ss.resolve_app_registry_context(settings)
        ok, msg = _ss.global_config_service.set_navbar_logo(
            host, token, registry_cfg, ""
        )
        if not ok:
            raise InfrastructureError("Failed to reset navbar logo", detail=msg)
        return {
            "success": True,
            "logo_url": _ss.DEFAULT_LOGO_PATH,
            "is_custom": False,
        }

    @staticmethod
    def get_registry_cache_ttl_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ttl = _ss.global_config_service.get_registry_cache_ttl(host, token, registry_cfg)
        return {"success": True, "registry_cache_ttl": ttl}

    @staticmethod
    def save_registry_cache_ttl_result(
        ttl: int,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = _ss.global_config_service.set_registry_cache_ttl(
            host, token, registry_cfg, ttl
        )
        if not ok:
            raise InfrastructureError("Failed to save registry cache TTL", detail=msg)
        return {"success": True, "registry_cache_ttl": max(10, int(ttl))}

    @staticmethod
    def get_graph_limits_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the effective graph-read bounds for the Settings UI.

        ``graph_query_timeout_s`` bounds a single graph read (Lakebase /
        warehouse ``statement_timeout``); ``graph_chat_result_cap`` bounds the
        triples returned to the Graph Chat agent. Both resolve admin override →
        env var → built-in default.
        """
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        return {
            "success": True,
            "graph_query_timeout_s": _ss.global_config_service.get_graph_query_timeout_s(
                host, token, registry_cfg
            ),
            "graph_chat_result_cap": _ss.global_config_service.get_graph_chat_result_cap(
                host, token, registry_cfg
            ),
        }

    @staticmethod
    def save_graph_limits_result(
        graph_query_timeout_s: Optional[int],
        graph_chat_result_cap: Optional[int],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist admin-set graph-read bounds (``0``/``None`` = unset)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if graph_query_timeout_s is not None:
            ok, msg = _ss.global_config_service.set_graph_query_timeout_s(
                host, token, registry_cfg, int(graph_query_timeout_s)
            )
            if not ok:
                raise InfrastructureError(
                    "Failed to save graph query timeout", detail=msg
                )
        if graph_chat_result_cap is not None:
            ok, msg = _ss.global_config_service.set_graph_chat_result_cap(
                host, token, registry_cfg, int(graph_chat_result_cap)
            )
            if not ok:
                raise InfrastructureError(
                    "Failed to save graph chat result cap", detail=msg
                )
        return SettingsService.get_graph_limits_result(session_mgr, settings)

    @staticmethod
    def get_data_assets_import_limit_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the Settings → Global data-assets import cap (default 40)."""
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        limit = _ss.global_config_service.get_data_assets_import_limit(
            host, token, registry_cfg
        )
        return {"success": True, "data_assets_import_limit": limit}

    @staticmethod
    def save_data_assets_import_limit_result(
        limit: int,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist the data-assets import cap (admin only)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = _ss.global_config_service.set_data_assets_import_limit(
            host, token, registry_cfg, int(limit)
        )
        if not ok:
            raise InfrastructureError(
                "Failed to save data assets import limit", detail=msg
            )
        return WorkspaceUiSettings.get_data_assets_import_limit_result(
            session_mgr, settings
        )

    @staticmethod
    def get_edit_lock_ttl_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the effective DRAFT edit-lock lease TTL (seconds).

        Mirrors :meth:`EditLockService._ttl_seconds` resolution (global config
        → ``ONTOBRICKS_EDIT_LOCK_TTL_S`` → built-in default) so the Settings UI
        shows the value actually in force. ``0`` means the lease is disabled.
        """
        from back.objects.registry.lockmgt import EditLockService

        ttl_s = EditLockService._ttl_seconds(session_mgr, settings)
        return {"success": True, "edit_lock_ttl_s": ttl_s}

    @staticmethod
    def save_edit_lock_ttl_result(
        ttl_s: int,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist the DRAFT edit-lock lease TTL globally (admin only, seconds)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ttl_s = max(0, int(ttl_s))
        ok, msg = _ss.global_config_service.set_edit_lock_ttl_s(
            host, token, registry_cfg, ttl_s
        )
        if not ok:
            raise InfrastructureError(
                "Failed to save edit-lock lease TTL", detail=msg
            )
        return {"success": True, "edit_lock_ttl_s": ttl_s}

    @staticmethod
    def get_analytics_job_enabled_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the effective graph-analytics job toggle plus its provenance.

        ``source`` lets the Settings UI say whether the value in force came from
        an admin or from the deployment default, which matters because an
        unconfigured toggle silently tracks the env var — showing a bare
        checkbox would imply someone had chosen it.
        """
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        configured = None
        try:
            configured = _ss.global_config_service.get_analytics_job_enabled(
                host, token, registry_cfg
            )
        except Exception as exc:  # noqa: BLE001 - fall back to the env default
            logger.debug("Analytics-job toggle lookup skipped: %s", exc)

        env_default = bool(getattr(settings, "analytics_job_enabled", False))
        return {
            "success": True,
            "analytics_job_enabled": (
                env_default if configured is None else bool(configured)
            ),
            "source": "default" if configured is None else "admin",
            "env_default": env_default,
        }

    @staticmethod
    def save_analytics_job_enabled_result(
        enabled: bool,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist the graph-analytics job toggle globally (admin only)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        domain, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        enabled = bool(enabled)
        ok, msg = _ss.global_config_service.set_analytics_job_enabled(
            host, token, registry_cfg, enabled
        )
        if not ok:
            raise InfrastructureError(
                "Failed to save the graph-analytics job setting", detail=msg
            )

        # The Analytics banner reads job availability from the cached
        # ``/dtwin/sync/stats`` payload, which the page fetches without
        # ``refresh`` because the counts behind it are expensive. Left in place,
        # it would keep telling an admin to enable what they just enabled.
        try:
            from back.objects.digitaltwin.DigitalTwin import DigitalTwin

            DigitalTwin(domain).clear_ts_cache("stats")
        except Exception as exc:  # noqa: BLE001 - the value is already stored
            logger.debug("Could not drop the cached stats payload: %s", exc)

        return {"success": True, "analytics_job_enabled": enabled, "source": "admin"}

    @staticmethod
    def save_use_cloud_fetch_result(
        enabled: bool,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist the global CloudFetch toggle (admin only)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)
        _domain, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        enabled = bool(enabled)
        ok, msg = _ss.global_config_service.set_use_cloud_fetch(
            host, token, registry_cfg, enabled
        )
        if not ok:
            raise InfrastructureError(
                "Failed to save the CloudFetch setting", detail=msg
            )
        return {"success": True, "use_cloud_fetch": enabled}
