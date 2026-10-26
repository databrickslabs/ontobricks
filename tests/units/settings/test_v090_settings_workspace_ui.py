"""WorkspaceUiSettings (0.9.0): branding (title / primary / Aurora accent /
logo), navbar logo, registry cache TTL, graph limits, data-assets import
limit, edit-lock TTL, analytics job toggle and CloudFetch toggle."""

from __future__ import annotations

import base64
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.core.errors import AuthorizationError, InfrastructureError, ValidationError
from back.objects.domain.settings.SettingsService import SettingsService
from back.objects.domain.settings.WorkspaceUiSettings import WorkspaceUiSettings

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

CFG = {"catalog": "cat", "schema": "sch"}
PNG = b"\x89PNG\r\n\x1a\nfake"


@pytest.fixture
def gcs(monkeypatch):
    """Global config service; every setter succeeds by default."""
    svc = MagicMock()
    for name in (
        "set_default_emoji",
        "set_default_base_uri",
        "set_ui_branding",
        "set_navbar_logo",
        "set_registry_cache_ttl",
        "set_graph_query_timeout_s",
        "set_graph_chat_result_cap",
        "set_data_assets_import_limit",
        "set_edit_lock_ttl_s",
        "set_analytics_job_enabled",
        "set_use_cloud_fetch",
    ):
        getattr(svc, name).return_value = (True, "ok")
    svc.get_ui_branding.return_value = {"version": 1, "logo_data_url": "data:image/png;base64,OLD"}
    monkeypatch.setattr(_ss, "global_config_service", svc)
    monkeypatch.setattr(_ss, "resolve_app_registry_context", lambda s: ("h", "t", CFG))
    monkeypatch.setattr(
        SettingsService,
        "_resolve_context",
        staticmethod(lambda sm, st: (MagicMock(), "h", "t", CFG)),
    )
    return svc


@pytest.fixture
def local_mode(monkeypatch):
    monkeypatch.setattr(_ss, "is_databricks_app", lambda: False)


@pytest.fixture
def non_admin(monkeypatch):
    monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
    perm = MagicMock()
    perm.is_admin.return_value = False
    monkeypatch.setattr(_ss, "permission_service", perm)
    return perm


# ---------------------------------------------------------------------------
# Admin gate — every write endpoint
# ---------------------------------------------------------------------------

_ADMIN_WRITES = [
    ("set_default_emoji_result", ("🧠",), "set_default_emoji"),
    ("save_base_uri_result", ("https://ex.org/",), "set_default_base_uri"),
    ("get_ui_branding_result", (), "get_ui_branding"),
    ("save_ui_branding_result", ("Acme", "#112233", "", None, None, False), "set_ui_branding"),
    ("upload_navbar_logo_result", (PNG, "image/png"), "set_navbar_logo"),
    ("reset_navbar_logo_result", (), "set_navbar_logo"),
    ("save_registry_cache_ttl_result", (60,), "set_registry_cache_ttl"),
    ("save_graph_limits_result", (30, 500), "set_graph_query_timeout_s"),
    ("save_data_assets_import_limit_result", (40,), "set_data_assets_import_limit"),
    ("save_edit_lock_ttl_result", (900,), "set_edit_lock_ttl_s"),
    ("save_analytics_job_enabled_result", (True,), "set_analytics_job_enabled"),
    ("save_use_cloud_fetch_result", (True,), "set_use_cloud_fetch"),
]


@pytest.mark.parametrize("method,args,store_call", _ADMIN_WRITES, ids=[m[0] for m in _ADMIN_WRITES])
def test_non_admin_is_rejected_before_any_persistence(gcs, non_admin, method, args, store_call):
    with pytest.raises(AuthorizationError):
        getattr(WorkspaceUiSettings, method)(*args, "viewer@x", "utok", MagicMock(), MagicMock())
    getattr(gcs, store_call).assert_not_called()


@pytest.mark.parametrize("method,args,store_call", _ADMIN_WRITES, ids=[m[0] for m in _ADMIN_WRITES])
def test_admin_passes_the_gate(gcs, monkeypatch, method, args, store_call):
    monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
    perm = MagicMock()
    perm.is_admin.return_value = True
    monkeypatch.setattr(_ss, "permission_service", perm)
    import back.objects.registry.collab as collab

    monkeypatch.setattr(collab.EditLockService, "_ttl_seconds", staticmethod(lambda sm, st: 0))

    result = getattr(WorkspaceUiSettings, method)(*args, "admin@x", "utok", MagicMock(), MagicMock())

    assert result["success"] is True
    getattr(gcs, store_call).assert_called()


# ---------------------------------------------------------------------------
# Defaults: emoji / base URI
# ---------------------------------------------------------------------------


class TestDefaults:
    def test_emoji_persisted_and_echoed(self, gcs, local_mode):
        assert WorkspaceUiSettings.set_default_emoji_result("🧬", "", "", MagicMock(), MagicMock()) == {
            "success": True,
            "emoji": "🧬",
        }
        gcs.set_default_emoji.assert_called_once_with("h", "t", CFG, "🧬")

    def test_emoji_store_failure(self, gcs, local_mode):
        gcs.set_default_emoji.return_value = (False, "readonly")
        with pytest.raises(InfrastructureError, match="default emoji"):
            WorkspaceUiSettings.set_default_emoji_result("x", "", "", MagicMock(), MagicMock())

    def test_base_uri_persisted_and_echoed(self, gcs, local_mode):
        result = WorkspaceUiSettings.save_base_uri_result("https://ex.org/", "", "", MagicMock(), MagicMock())
        assert result == {"success": True, "base_uri": "https://ex.org/"}
        gcs.set_default_base_uri.assert_called_once_with("h", "t", CFG, "https://ex.org/")

    def test_base_uri_store_failure(self, gcs, local_mode):
        gcs.set_default_base_uri.return_value = (False, "x")
        with pytest.raises(InfrastructureError, match="default base URI"):
            WorkspaceUiSettings.save_base_uri_result("u", "", "", MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# Logo validation
# ---------------------------------------------------------------------------


class TestLogoValidation:
    def test_empty_file_rejected(self):
        with pytest.raises(ValidationError, match="Empty file"):
            WorkspaceUiSettings._validate_and_encode_logo(b"", "image/png")

    def test_oversized_file_rejected(self):
        big = b"x" * (WorkspaceUiSettings._NAVBAR_LOGO_MAX_BYTES + 1)
        with pytest.raises(ValidationError, match="Logo too large"):
            WorkspaceUiSettings._validate_and_encode_logo(big, "image/png")

    def test_max_size_accepted(self):
        exact = b"x" * WorkspaceUiSettings._NAVBAR_LOGO_MAX_BYTES
        mime, _ = WorkspaceUiSettings._validate_and_encode_logo(exact, "image/png")
        assert mime == "image/png"

    @pytest.mark.parametrize("ctype", ["application/pdf", "", "image/bmp", "text/html"])
    def test_unsupported_type_rejected(self, ctype):
        with pytest.raises(ValidationError, match="Unsupported image type"):
            WorkspaceUiSettings._validate_and_encode_logo(PNG, ctype)

    def test_mime_parameters_and_case_are_normalized(self):
        mime, data_url = WorkspaceUiSettings._validate_and_encode_logo(
            b"<svg/>", "Image/SVG+XML; charset=utf-8"
        )
        assert mime == "image/svg+xml"
        assert data_url == "data:image/svg+xml;base64," + base64.b64encode(b"<svg/>").decode()


# ---------------------------------------------------------------------------
# Branding (title / primary / Aurora / logo)
# ---------------------------------------------------------------------------


class TestUiBranding:
    def _save(self, *, title="Acme", primary="#112233", aurora="", logo=None, mime=None, reset=False):
        return WorkspaceUiSettings.save_ui_branding_result(
            title, primary, aurora, logo, mime, reset, "", "", MagicMock(), MagicMock()
        )

    def test_get_returns_store_branding(self, gcs, local_mode):
        assert WorkspaceUiSettings.get_ui_branding_result("", "", MagicMock(), MagicMock()) == {
            "success": True,
            "branding": gcs.get_ui_branding.return_value,
        }

    def test_keeps_current_logo_and_persists_explicit_aurora(self, gcs, local_mode):
        result = self._save(aurora=" #aabbcc ")

        saved = gcs.set_ui_branding.call_args.args[3]
        assert saved == {
            "version": 1,
            "app_title": "Acme",
            "primary_color": "#112233",
            "aurora_color": "#AABBCC",
            "logo_data_url": "data:image/png;base64,OLD",
        }
        assert result["branding"]["palette"]["aurora"] == "#AABBCC"
        assert result["branding"]["is_custom_logo"] is True

    def test_blank_aurora_is_derived_from_primary(self, gcs, local_mode):
        result = self._save(aurora="  ")
        derived = result["branding"]["aurora_color"]
        assert derived.startswith("#") and len(derived) == 7

    def test_reset_logo_clears_custom_logo(self, gcs, local_mode):
        result = self._save(reset=True)
        assert gcs.set_ui_branding.call_args.args[3]["logo_data_url"] == ""
        assert result["branding"]["is_custom_logo"] is False

    def test_uploaded_logo_replaces_current(self, gcs, local_mode):
        self._save(logo=PNG, mime="image/png")
        stored = gcs.set_ui_branding.call_args.args[3]["logo_data_url"]
        assert stored == "data:image/png;base64," + base64.b64encode(PNG).decode()

    def test_uploaded_logo_is_validated(self, gcs, local_mode):
        with pytest.raises(ValidationError, match="Unsupported image type"):
            self._save(logo=PNG, mime=None)
        gcs.set_ui_branding.assert_not_called()

    def test_logo_and_reset_are_mutually_exclusive(self, gcs, local_mode):
        with pytest.raises(ValidationError, match="reset_logo cannot be true"):
            self._save(logo=PNG, mime="image/png", reset=True)

    def test_invalid_aurora_is_a_field_error(self, gcs, local_mode):
        with pytest.raises(ValidationError, match="aurora color"):
            self._save(aurora="#12")
        gcs.set_ui_branding.assert_not_called()

    @pytest.mark.parametrize("title,primary", [("", "#112233"), ("x" * 61, "#112233"), ("Acme", "blue")])
    def test_invalid_title_or_primary_rejected(self, gcs, local_mode, title, primary):
        with pytest.raises(ValidationError):
            self._save(title=title, primary=primary)
        gcs.set_ui_branding.assert_not_called()

    def test_store_failure(self, gcs, local_mode):
        gcs.set_ui_branding.return_value = (False, "locked")
        with pytest.raises(InfrastructureError, match="Failed to save UI branding"):
            self._save()


# ---------------------------------------------------------------------------
# Navbar logo
# ---------------------------------------------------------------------------


class TestNavbarLogo:
    def test_get_custom_logo(self, gcs):
        result = WorkspaceUiSettings.get_navbar_logo_result(MagicMock(), MagicMock())
        assert result["logo_url"] == "data:image/png;base64,OLD"
        assert result["is_custom"] is True
        assert result["default_url"] == _ss.DEFAULT_LOGO_PATH
        assert result["recommended_size"] == "64×64 px"
        assert result["max_bytes"] == 1024 * 1024
        assert "image/svg+xml" in result["allowed_mime"]

    def test_get_falls_back_to_default(self, gcs):
        gcs.get_ui_branding.return_value = {"logo_data_url": None}
        result = WorkspaceUiSettings.get_navbar_logo_result(MagicMock(), MagicMock())
        assert result["logo_url"] == _ss.DEFAULT_LOGO_PATH
        assert result["is_custom"] is False

    def test_upload_persists_data_url(self, gcs, local_mode):
        result = WorkspaceUiSettings.upload_navbar_logo_result(PNG, "image/png", "", "", MagicMock(), MagicMock())
        data_url = "data:image/png;base64," + base64.b64encode(PNG).decode()
        assert result == {
            "success": True,
            "logo_url": data_url,
            "is_custom": True,
            "size_bytes": len(PNG),
            "mime": "image/png",
        }
        gcs.set_navbar_logo.assert_called_once_with("h", "t", CFG, data_url)

    def test_upload_invalid_type_not_persisted(self, gcs, local_mode):
        with pytest.raises(ValidationError):
            WorkspaceUiSettings.upload_navbar_logo_result(PNG, "image/tiff", "", "", MagicMock(), MagicMock())
        gcs.set_navbar_logo.assert_not_called()

    def test_upload_store_failure(self, gcs, local_mode):
        gcs.set_navbar_logo.return_value = (False, "x")
        with pytest.raises(InfrastructureError, match="Failed to save navbar logo"):
            WorkspaceUiSettings.upload_navbar_logo_result(PNG, "image/png", "", "", MagicMock(), MagicMock())

    def test_reset_clears_logo(self, gcs, local_mode):
        assert WorkspaceUiSettings.reset_navbar_logo_result("", "", MagicMock(), MagicMock()) == {
            "success": True,
            "logo_url": _ss.DEFAULT_LOGO_PATH,
            "is_custom": False,
        }
        gcs.set_navbar_logo.assert_called_once_with("h", "t", CFG, "")

    def test_reset_store_failure(self, gcs, local_mode):
        gcs.set_navbar_logo.return_value = (False, "x")
        with pytest.raises(InfrastructureError, match="Failed to reset navbar logo"):
            WorkspaceUiSettings.reset_navbar_logo_result("", "", MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# Registry cache TTL
# ---------------------------------------------------------------------------


class TestRegistryCacheTtl:
    def test_get(self, gcs):
        gcs.get_registry_cache_ttl.return_value = 120
        assert WorkspaceUiSettings.get_registry_cache_ttl_result(MagicMock(), MagicMock()) == {
            "success": True,
            "registry_cache_ttl": 120,
        }

    @pytest.mark.parametrize("ttl,reported", [(300, 300), (10, 10), (3, 10), (0, 10)])
    def test_save_reports_floor_of_ten_seconds(self, gcs, local_mode, ttl, reported):
        result = WorkspaceUiSettings.save_registry_cache_ttl_result(ttl, "", "", MagicMock(), MagicMock())
        assert result == {"success": True, "registry_cache_ttl": reported}
        gcs.set_registry_cache_ttl.assert_called_once_with("h", "t", CFG, ttl)

    def test_save_store_failure(self, gcs, local_mode):
        gcs.set_registry_cache_ttl.return_value = (False, "x")
        with pytest.raises(InfrastructureError, match="registry cache TTL"):
            WorkspaceUiSettings.save_registry_cache_ttl_result(60, "", "", MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# Graph limits
# ---------------------------------------------------------------------------


class TestGraphLimits:
    def test_get_reports_both_bounds(self, gcs):
        gcs.get_graph_query_timeout_s.return_value = 45
        gcs.get_graph_chat_result_cap.return_value = 2000
        assert WorkspaceUiSettings.get_graph_limits_result(MagicMock(), MagicMock()) == {
            "success": True,
            "graph_query_timeout_s": 45,
            "graph_chat_result_cap": 2000,
        }

    def test_save_both_coerces_to_int_and_returns_effective(self, gcs, local_mode):
        gcs.get_graph_query_timeout_s.return_value = 30
        gcs.get_graph_chat_result_cap.return_value = 500

        result = WorkspaceUiSettings.save_graph_limits_result("30", 500.0, "", "", MagicMock(), MagicMock())

        gcs.set_graph_query_timeout_s.assert_called_once_with("h", "t", CFG, 30)
        gcs.set_graph_chat_result_cap.assert_called_once_with("h", "t", CFG, 500)
        assert result["graph_query_timeout_s"] == 30
        assert result["graph_chat_result_cap"] == 500

    def test_none_leaves_bound_untouched(self, gcs, local_mode):
        WorkspaceUiSettings.save_graph_limits_result(None, 0, "", "", MagicMock(), MagicMock())
        gcs.set_graph_query_timeout_s.assert_not_called()
        gcs.set_graph_chat_result_cap.assert_called_once_with("h", "t", CFG, 0)

    def test_timeout_store_failure(self, gcs, local_mode):
        gcs.set_graph_query_timeout_s.return_value = (False, "out of range")
        with pytest.raises(InfrastructureError, match="graph query timeout"):
            WorkspaceUiSettings.save_graph_limits_result(5, None, "", "", MagicMock(), MagicMock())

    def test_cap_store_failure(self, gcs, local_mode):
        gcs.set_graph_chat_result_cap.return_value = (False, "out of range")
        with pytest.raises(InfrastructureError, match="graph chat result cap"):
            WorkspaceUiSettings.save_graph_limits_result(None, 5, "", "", MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# Data-assets import limit (default 40)
# ---------------------------------------------------------------------------


class TestDataAssetsImportLimit:
    def test_get(self, gcs):
        gcs.get_data_assets_import_limit.return_value = 40
        assert WorkspaceUiSettings.get_data_assets_import_limit_result(MagicMock(), MagicMock()) == {
            "success": True,
            "data_assets_import_limit": 40,
        }

    def test_save_coerces_and_returns_effective_value(self, gcs, local_mode):
        gcs.get_data_assets_import_limit.return_value = 75
        result = WorkspaceUiSettings.save_data_assets_import_limit_result("75", "", "", MagicMock(), MagicMock())
        gcs.set_data_assets_import_limit.assert_called_once_with("h", "t", CFG, 75)
        assert result == {"success": True, "data_assets_import_limit": 75}

    def test_save_store_failure(self, gcs, local_mode):
        gcs.set_data_assets_import_limit.return_value = (False, "x")
        with pytest.raises(InfrastructureError, match="data assets import limit"):
            WorkspaceUiSettings.save_data_assets_import_limit_result(10, "", "", MagicMock(), MagicMock())

    def test_global_config_default_is_40(self):
        from back.objects.session.GlobalConfigService import GlobalConfigService

        svc = GlobalConfigService()
        svc.load = MagicMock(return_value=GlobalConfigService._empty())
        assert svc.get_data_assets_import_limit("h", "t", CFG) == 40


# ---------------------------------------------------------------------------
# Edit-lock TTL
# ---------------------------------------------------------------------------


class TestEditLockTtl:
    def test_get_mirrors_edit_lock_service(self, monkeypatch):
        import back.objects.registry.collab as collab

        monkeypatch.setattr(collab.EditLockService, "_ttl_seconds", staticmethod(lambda sm, st: 600))
        assert WorkspaceUiSettings.get_edit_lock_ttl_result(MagicMock(), MagicMock()) == {
            "success": True,
            "edit_lock_ttl_s": 600,
        }

    @pytest.mark.parametrize("ttl,stored", [(900, 900), ("120", 120), (0, 0), (-5, 0)])
    def test_save_clamps_negative_to_disabled(self, gcs, local_mode, ttl, stored):
        result = WorkspaceUiSettings.save_edit_lock_ttl_result(ttl, "", "", MagicMock(), MagicMock())
        gcs.set_edit_lock_ttl_s.assert_called_once_with("h", "t", CFG, stored)
        assert result == {"success": True, "edit_lock_ttl_s": stored}

    def test_save_store_failure(self, gcs, local_mode):
        gcs.set_edit_lock_ttl_s.return_value = (False, "x")
        with pytest.raises(InfrastructureError, match="edit-lock lease TTL"):
            WorkspaceUiSettings.save_edit_lock_ttl_result(60, "", "", MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# Analytics job toggle / CloudFetch toggle
# ---------------------------------------------------------------------------


class TestAnalyticsJobToggle:
    @pytest.mark.parametrize(
        "configured,env_default,expected,source",
        [
            (None, True, True, "default"),
            (None, False, False, "default"),
            (False, True, False, "admin"),
            (True, False, True, "admin"),
        ],
    )
    def test_get_reports_value_and_provenance(self, gcs, configured, env_default, expected, source):
        gcs.get_analytics_job_enabled.return_value = configured
        result = WorkspaceUiSettings.get_analytics_job_enabled_result(
            MagicMock(), SimpleNamespace(analytics_job_enabled=env_default)
        )
        assert result == {
            "success": True,
            "analytics_job_enabled": expected,
            "source": source,
            "env_default": env_default,
        }

    def test_get_lookup_failure_falls_back_to_env_default(self, gcs):
        gcs.get_analytics_job_enabled.side_effect = RuntimeError("down")
        result = WorkspaceUiSettings.get_analytics_job_enabled_result(MagicMock(), SimpleNamespace())
        assert result["analytics_job_enabled"] is False
        assert result["source"] == "default"

    def test_save_persists_and_drops_cached_stats(self, gcs, local_mode, monkeypatch):
        dt_mod = importlib.import_module("back.objects.digitaltwin.DigitalTwin")

        dt_cls = MagicMock()
        monkeypatch.setattr(dt_mod, "DigitalTwin", dt_cls)

        result = WorkspaceUiSettings.save_analytics_job_enabled_result(1, "", "", MagicMock(), MagicMock())

        assert result == {"success": True, "analytics_job_enabled": True, "source": "admin"}
        gcs.set_analytics_job_enabled.assert_called_once_with("h", "t", CFG, True)
        dt_cls.return_value.clear_ts_cache.assert_called_once_with("stats")

    def test_save_survives_cache_drop_failure(self, gcs, local_mode, monkeypatch):
        dt_mod = importlib.import_module("back.objects.digitaltwin.DigitalTwin")

        monkeypatch.setattr(dt_mod, "DigitalTwin", MagicMock(side_effect=RuntimeError("x")))
        result = WorkspaceUiSettings.save_analytics_job_enabled_result(False, "", "", MagicMock(), MagicMock())
        assert result["analytics_job_enabled"] is False

    def test_save_store_failure(self, gcs, local_mode):
        gcs.set_analytics_job_enabled.return_value = (False, "x")
        with pytest.raises(InfrastructureError, match="graph-analytics job"):
            WorkspaceUiSettings.save_analytics_job_enabled_result(True, "", "", MagicMock(), MagicMock())


class TestCloudFetchToggle:
    def test_save_coerces_to_bool(self, gcs, local_mode):
        assert WorkspaceUiSettings.save_use_cloud_fetch_result("", "", "", MagicMock(), MagicMock()) == {
            "success": True,
            "use_cloud_fetch": False,
        }
        gcs.set_use_cloud_fetch.assert_called_once_with("h", "t", CFG, False)

    def test_save_store_failure(self, gcs, local_mode):
        gcs.set_use_cloud_fetch.return_value = (False, "x")
        with pytest.raises(InfrastructureError, match="CloudFetch"):
            WorkspaceUiSettings.save_use_cloud_fetch_result(True, "", "", MagicMock(), MagicMock())
