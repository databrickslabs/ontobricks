"""Settings → Databricks global CloudFetch toggle."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import AuthorizationError, InfrastructureError
from back.objects.domain.SettingsService import SettingsService

REPO_ROOT = Path(__file__).resolve().parents[3]
SETTINGS_HTML = REPO_ROOT / "src/front/templates/settings.html"
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"
RCFG = {"catalog": "main", "schema": "ob"}


class TestCurrentConfigIncludesCloudFetch:
    def test_payload_includes_use_cloud_fetch(self):
        domain = MagicMock()
        domain.databricks = {"host": "https://h", "token": "tok"}
        settings = MagicMock()
        settings.databricks_host = "https://h"
        settings.databricks_token = "tok"

        with patch(
            "back.objects.domain.SettingsService.get_domain", return_value=domain
        ), patch(
            "back.objects.domain.SettingsService.resolve_warehouse_id",
            return_value="wh-build",
        ), patch(
            "back.objects.domain.SettingsService.resolve_build_use_sea",
            return_value=False,
        ), patch(
            "back.objects.domain.SettingsService.resolve_use_cloud_fetch",
            return_value=False,
        ), patch.object(SettingsService, "is_warehouse_locked", return_value=False):
            payload = SettingsService.build_current_config(MagicMock(), settings)

        assert payload["use_cloud_fetch"] is False


class TestServiceWrite:
    def test_requires_admin_before_writing(self):
        with patch.object(
            SettingsService,
            "require_admin_error",
            side_effect=AuthorizationError("nope"),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service"
            ".set_use_cloud_fetch"
        ) as setter:
            with pytest.raises(AuthorizationError):
                SettingsService.save_use_cloud_fetch_result(
                    False, "u@x", "tok", MagicMock(), MagicMock()
                )
        setter.assert_not_called()

    def test_persists_an_explicit_off(self):
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "https://h", "tok", RCFG),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service"
            ".set_use_cloud_fetch",
            return_value=(True, "saved"),
        ) as setter:
            result = SettingsService.save_use_cloud_fetch_result(
                False, "u@x", "tok", MagicMock(), MagicMock()
            )
        assert result == {"success": True, "use_cloud_fetch": False}
        setter.assert_called_once_with("https://h", "tok", RCFG, False)

    def test_store_failure_raises(self):
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "https://h", "tok", RCFG),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service"
            ".set_use_cloud_fetch",
            return_value=(False, "disk full"),
        ):
            with pytest.raises(InfrastructureError):
                SettingsService.save_use_cloud_fetch_result(
                    False, "u@x", "tok", MagicMock(), MagicMock()
                )


class TestUiWiring:
    def test_checkbox_lives_in_databricks_section(self):
        html = SETTINGS_HTML.read_text(encoding="utf-8")
        assert 'id="useCloudFetch"' in html
        block = html.split('id="useCloudFetch"')[1][:900]
        assert "Use CloudFetch" in block
        assert "settings-badge-admin-note" in block
        before = html.split('id="useCloudFetch"')[0][-800:]
        assert 'id="buildUseSea"' in before

    def test_help_text_explains_apps_egress(self):
        html = SETTINGS_HTML.read_text(encoding="utf-8")
        assert "external result download" in html.lower() or "result files" in html.lower()

    def test_hydrated_from_current_config(self):
        js = SETTINGS_JS.read_text(encoding="utf-8")
        assert "useCloudFetch" in js
        assert "data.use_cloud_fetch" in js
        assert "cloudFetchHydrated" in js

    def test_saved_by_shared_handler_including_unchecked(self):
        js = SETTINGS_JS.read_text(encoding="utf-8")
        assert "'/settings/save-cloud-fetch'" in js
        assert "use_cloud_fetch: cloudFetchInput.checked" in js
        assert "if (cloudFetchInput.checked)" not in js

    def test_databricks_save_does_not_validate_unloaded_query_warehouse(self):
        js = SETTINGS_JS.read_text(encoding="utf-8")
        assert "let deltaWarehouseHydrated = false;" in js
        assert "deltaWarehouseHydrated = true;" in js
        assert (
            "if (deltaWarehouseHydrated) {\n"
            "                await saveDeltaWarehouseSelection(errors);\n"
            "            }"
        ) in js
