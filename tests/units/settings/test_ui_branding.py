"""Unified UI branding persistence and service orchestration tests."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.helpers.UIBranding import normalize_ui_branding
from back.objects.domain.SettingsService import SettingsService
from back.objects.session.GlobalConfigService import GlobalConfigService
from shared.fastapi.ui_branding import resolve_request_ui_branding

_svc_module = importlib.import_module("back.objects.domain.SettingsService")

REGISTRY_CFG = {"catalog": "cat", "schema": "sch", "volume": "vol"}


def _mock_context():
    return MagicMock(), MagicMock()


class _FakeGlobalConfigStore:
    backend = "lakebase"

    def __init__(self, initial: dict):
        self._data = dict(initial)

    def load_global_config(self) -> dict:
        return dict(self._data)

    def save_global_config(self, updates: dict):
        data = self.load_global_config()
        data.update(updates or {})
        self._data = data
        return True, "ok"


class TestGlobalConfigUiBranding:
    def test_empty_defaults_include_ui_branding(self):
        empty = GlobalConfigService._empty()
        assert "ui_branding" in empty
        assert empty["ui_branding"]["version"] == 1
        assert empty["ui_branding"]["app_title"] == "OntoBricks"
        assert empty["ui_branding"]["primary_color"] == "#4F46E5"
        assert empty["ui_branding"]["logo_data_url"] == ""

    def test_legacy_navbar_logo_is_resolved_into_unified_branding(self):
        svc = GlobalConfigService()
        data = GlobalConfigService._empty()
        data["navbar_logo"] = "data:image/png;base64,abc"
        with patch.object(svc, "load", return_value=data):
            branding = svc.get_ui_branding("h", "t", REGISTRY_CFG)
        assert branding["logo_data_url"] == "data:image/png;base64,abc"
        assert branding["is_custom_logo"] is True

    def test_ui_branding_prefers_unified_logo_over_legacy(self):
        svc = GlobalConfigService()
        data = GlobalConfigService._empty()
        data["navbar_logo"] = "data:image/png;base64,legacy"
        data["ui_branding"]["logo_data_url"] = "data:image/png;base64,new"
        with patch.object(svc, "load", return_value=data):
            branding = svc.get_ui_branding("h", "t", REGISTRY_CFG)
        assert branding["logo_data_url"] == "data:image/png;base64,new"

    def test_set_ui_branding_persists_single_merged_update(self):
        svc = GlobalConfigService()
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, _ = svc.set_ui_branding(
                "h",
                "t",
                REGISTRY_CFG,
                {"app_title": "Acme", "primary_color": "#123456", "logo_data_url": ""},
            )
        assert ok
        mock_save.assert_called_once()
        updates = mock_save.call_args[0][3]
        assert "ui_branding" in updates
        assert updates["ui_branding"]["app_title"] == "Acme"
        assert updates["navbar_logo"] == ""

    def test_failed_save_does_not_mutate_cache(self):
        svc = GlobalConfigService()
        initial = GlobalConfigService._empty()
        svc._cache = dict(initial)
        svc._cache_ts = 123.0
        with patch.object(svc, "_save", return_value=(False, "disk full")):
            ok, _ = svc.set_ui_branding(
                "h", "t", REGISTRY_CFG, {"app_title": "Acme", "primary_color": "#123456"}
            )
        assert ok is False
        assert svc._cache == initial
        assert svc._cache_ts == 123.0

    def test_successful_save_updates_cache_via_real_store_path(self):
        svc = GlobalConfigService()
        initial = GlobalConfigService._empty()
        store = _FakeGlobalConfigStore(initial)
        with patch.object(svc, "_store_for", return_value=store):
            ok, _ = svc.set_ui_branding(
                "h",
                "t",
                REGISTRY_CFG,
                {"app_title": "Acme Graph", "primary_color": "#123456", "logo_data_url": ""},
            )
        assert ok is True
        assert svc._cache["ui_branding"]["app_title"] == "Acme Graph"
        assert svc._cache["ui_branding"]["primary_color"] == "#123456"
        assert svc._cache["navbar_logo"] == ""

    def test_legacy_set_navbar_logo_updates_unified_branding(self):
        svc = GlobalConfigService()
        with patch.object(svc, "set_ui_branding", return_value=(True, "ok")) as setter:
            ok, _ = svc.set_navbar_logo("h", "t", REGISTRY_CFG, "data:image/png;base64,abc")
        assert ok is True
        setter.assert_called_once()
        assert setter.call_args[0][3]["logo_data_url"] == "data:image/png;base64,abc"

    def test_unified_reset_clears_legacy_navbar_logo(self):
        svc = GlobalConfigService()
        legacy_then_unified = {
            "version": 1,
            "navbar_logo": "data:image/png;base64,legacy",
            "ui_branding": {
                "version": 1,
                "app_title": "Acme Graph",
                "primary_color": "#123456",
                "logo_data_url": "data:image/png;base64,legacy",
            },
        }
        svc._cache = dict(legacy_then_unified)
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, _ = svc.set_ui_branding(
                "h",
                "t",
                REGISTRY_CFG,
                {"logo_data_url": ""},
            )
        assert ok is True
        updates = mock_save.call_args[0][3]
        assert updates["ui_branding"]["logo_data_url"] == ""
        assert updates["navbar_logo"] == ""

    def test_global_save_force_refreshes_before_merge(self):
        svc = GlobalConfigService()
        with patch.object(svc, "load", return_value=GlobalConfigService._empty()) as loader, patch.object(
            svc, "_store_for"
        ) as store_for:
            store = MagicMock()
            store.backend = "lakebase"
            store.save_global_config.return_value = (True, "ok")
            store_for.return_value = store
            ok, _ = svc._save("h", "t", REGISTRY_CFG, {"default_emoji": "🚀"})
        assert ok is True
        assert loader.call_args.kwargs["force"] is True

    def test_set_ui_branding_does_not_overwrite_force_refreshed_cache_union(self):
        svc = GlobalConfigService()
        stale_cache = GlobalConfigService._empty()
        stale_cache.pop("default_emoji", None)
        svc._cache = dict(stale_cache)

        fresh_store_state = GlobalConfigService._empty()
        fresh_store_state["default_emoji"] = "🧭"
        store = _FakeGlobalConfigStore(fresh_store_state)
        with patch.object(svc, "_store_for", return_value=store):
            ok, _ = svc.set_ui_branding(
                "h",
                "t",
                REGISTRY_CFG,
                {"app_title": "Acme Graph", "primary_color": "#123456"},
            )
        assert ok is True
        # The force-refreshed union (including fresh unrelated keys) must survive.
        assert svc._cache.get("default_emoji") == "🧭"


class TestSettingsServiceUiBranding:
    def test_get_ui_branding_result_returns_normalized_payload(self):
        session_mgr, settings = _mock_context()
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            _svc_module,
            "resolve_app_registry_context",
            return_value=("app-host", "app-token", {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"}),
        ), patch.object(_svc_module, "global_config_service") as gcs:
            gcs.get_ui_branding.return_value = {
                "app_title": "Acme",
                "primary_color": "#123456",
                "logo_url": "/static/global/img/favicon.svg",
                "is_custom_logo": False,
                "palette": {"primary_rgb": "18, 52, 86"},
            }
            payload = SettingsService.get_ui_branding_result(
                "u@x", "tok", session_mgr, settings
            )
        assert payload["success"] is True
        assert payload["branding"]["app_title"] == "Acme"
        gcs.get_ui_branding.assert_called_once_with(
            "app-host",
            "app-token",
            {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"},
        )

    def test_save_ui_branding_rejects_reset_and_upload_together(self):
        with patch.object(SettingsService, "require_admin_error"):
            with pytest.raises(ValidationError, match="reset_logo"):
                SettingsService.save_ui_branding_result(
                    app_title="Acme",
                    primary_color="#123456",
                    logo_content=b"abc",
                    logo_mime="image/png",
                    reset_logo=True,
                    email="u@x",
                    user_token="tok",
                    session_mgr=MagicMock(),
                    settings=MagicMock(),
                )

    def test_save_ui_branding_validates_title_and_color_before_write(self):
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            _svc_module, "global_config_service"
        ) as gcs:
            with pytest.raises(ValidationError, match="title"):
                SettingsService.save_ui_branding_result(
                    app_title="",
                    primary_color="#123456",
                    logo_content=None,
                    logo_mime=None,
                    reset_logo=False,
                    email="u@x",
                    user_token="tok",
                    session_mgr=MagicMock(),
                    settings=MagicMock(),
                )
            gcs.set_ui_branding.assert_not_called()

    def test_save_ui_branding_is_atomic(self):
        session_mgr, settings = _mock_context()
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            _svc_module,
            "resolve_app_registry_context",
            return_value=("app-host", "app-token", {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"}),
        ), patch.object(_svc_module, "global_config_service") as gcs:
            gcs.get_ui_branding.return_value = GlobalConfigService._empty()["ui_branding"]
            gcs.set_ui_branding.return_value = (True, "ok")
            result = SettingsService.save_ui_branding_result(
                app_title="Acme Graph",
                primary_color="#123456",
                logo_content=None,
                logo_mime=None,
                reset_logo=False,
                email="u@x",
                user_token="tok",
                session_mgr=session_mgr,
                settings=settings,
            )
        assert result["success"] is True
        gcs.set_ui_branding.assert_called_once()
        call = gcs.set_ui_branding.call_args[0]
        assert call[0:3] == (
            "app-host",
            "app-token",
            {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"},
        )

    def test_save_ui_branding_store_failure_raises_infra_error(self):
        session_mgr, settings = _mock_context()
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            _svc_module,
            "resolve_app_registry_context",
            return_value=("app-host", "app-token", {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"}),
        ), patch.object(_svc_module, "global_config_service") as gcs:
            gcs.get_ui_branding.return_value = GlobalConfigService._empty()["ui_branding"]
            gcs.set_ui_branding.return_value = (False, "registry down")
            with pytest.raises(InfrastructureError):
                SettingsService.save_ui_branding_result(
                    app_title="Acme Graph",
                    primary_color="#123456",
                    logo_content=None,
                    logo_mime=None,
                    reset_logo=False,
                    email="u@x",
                    user_token="tok",
                    session_mgr=session_mgr,
                    settings=settings,
                )

    def test_branding_context_is_app_level_even_with_active_domain(self):
        session_mgr, settings = _mock_context()
        fake_domain = MagicMock()
        fake_domain.info = {"name": "DomainScoped"}
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(fake_domain, "domain-host", "domain-token", REGISTRY_CFG),
        ) as domain_ctx, patch.object(
            _svc_module,
            "resolve_app_registry_context",
            return_value=("app-host", "app-token", {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"}),
        ) as app_ctx, patch.object(_svc_module, "global_config_service") as gcs:
            gcs.get_ui_branding.return_value = GlobalConfigService._empty()["ui_branding"]
            gcs.set_ui_branding.return_value = (True, "ok")
            SettingsService.save_ui_branding_result(
                app_title="Acme Graph",
                primary_color="#123456",
                logo_content=None,
                logo_mime=None,
                reset_logo=False,
                email="u@x",
                user_token="tok",
                session_mgr=session_mgr,
                settings=settings,
            )
            SettingsService.get_ui_branding_result("u@x", "tok", session_mgr, settings)

        assert app_ctx.call_count == 2
        first_read = gcs.get_ui_branding.call_args_list[0][0]
        second_read = gcs.get_ui_branding.call_args_list[1][0]
        assert first_read == second_read == (
            "app-host",
            "app-token",
            {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"},
        )
        domain_ctx.assert_not_called()

    def test_legacy_navbar_logo_endpoints_use_app_context(self):
        session_mgr, settings = _mock_context()
        app_ctx = ("app-host", "app-token", {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"})
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            _svc_module, "resolve_app_registry_context", return_value=app_ctx
        ), patch.object(_svc_module, "global_config_service") as gcs:
            gcs.get_ui_branding.return_value = {"logo_data_url": "data:image/png;base64,AAAA"}
            gcs.set_navbar_logo.return_value = (True, "ok")

            get_res = SettingsService.get_navbar_logo_result(session_mgr, settings)
            up_res = SettingsService.upload_navbar_logo_result(
                content=b"abcd",
                content_type="image/png",
                email="u@x",
                user_token="tok",
                session_mgr=session_mgr,
                settings=settings,
            )
            reset_res = SettingsService.reset_navbar_logo_result(
                email="u@x",
                user_token="tok",
                session_mgr=session_mgr,
                settings=settings,
            )

        assert get_res["success"] is True
        assert up_res["success"] is True
        assert reset_res["success"] is True
        gcs.get_ui_branding.assert_called_once_with(*app_ctx)
        assert gcs.set_navbar_logo.call_count == 2
        assert gcs.set_navbar_logo.call_args_list[0][0][0:3] == app_ctx
        assert gcs.set_navbar_logo.call_args_list[1][0][0:3] == app_ctx

    def test_save_with_active_domain_and_first_paint_read_share_app_context(self):
        session_mgr, settings = _mock_context()
        app_ctx = ("app-host", "app-token", {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"})
        stored: dict[tuple[str, str, str], dict] = {}

        def _fake_set(host, token, registry_cfg, payload):
            key = (host, token, f"{registry_cfg.get('catalog')}/{registry_cfg.get('schema')}/{registry_cfg.get('volume')}")
            stored[key] = dict(payload)
            return True, "ok"

        def _fake_get(host, token, registry_cfg):
            key = (host, token, f"{registry_cfg.get('catalog')}/{registry_cfg.get('schema')}/{registry_cfg.get('volume')}")
            payload = stored.get(
                key,
                {"version": 1, "app_title": "OntoBricks", "primary_color": "#4F46E5", "logo_data_url": ""},
            )
            return normalize_ui_branding(payload).to_dict()

        with patch.object(SettingsService, "require_admin_error"), patch.object(
            _svc_module, "resolve_app_registry_context", return_value=app_ctx
        ), patch(
            "shared.fastapi.ui_branding.resolve_app_registry_context", return_value=app_ctx
        ), patch(
            "back.objects.session.GlobalConfigService.global_config_service.set_ui_branding",
            side_effect=_fake_set,
        ), patch(
            "back.objects.session.GlobalConfigService.global_config_service.get_ui_branding",
            side_effect=_fake_get,
        ):
            SettingsService.save_ui_branding_result(
                app_title="Acme Graph",
                primary_color="#123456",
                logo_content=None,
                logo_mime=None,
                reset_logo=False,
                email="u@x",
                user_token="tok",
                session_mgr=session_mgr,
                settings=settings,
            )
            branding = resolve_request_ui_branding(MagicMock(), settings)

        assert branding["app_title"] == "Acme Graph"
        assert branding["primary_color"] == "#123456"
