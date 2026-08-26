"""API tests for Settings → UI Branding endpoints."""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest

from back.core.errors import AuthorizationError, InfrastructureError, ValidationError


class TestUiBrandingSettingsApi:
    def test_get_ui_branding_returns_payload(self, client):
        payload = {
            "success": True,
            "branding": {
                "app_title": "Acme Graph",
                "primary_color": "#123456",
                "logo_url": "/static/global/img/favicon.svg",
                "is_custom_logo": False,
                "palette": {"primary_rgb": "18, 52, 86"},
            },
        }
        with patch(
            "api.routers.internal.settings.config_service.get_ui_branding_result",
            return_value=payload,
        ) as getter:
            response = client.get("/settings/ui-branding")
        assert response.status_code == 200
        assert response.json()["branding"]["app_title"] == "Acme Graph"
        getter.assert_called_once()

    def test_save_ui_branding_accepts_atomic_multipart_without_logo(self, client):
        payload = {
            "success": True,
            "branding": {
                "app_title": "Acme Graph",
                "primary_color": "#123456",
                "logo_url": "/static/global/img/favicon.svg",
                "is_custom_logo": False,
                "palette": {"primary_rgb": "18, 52, 86"},
            },
        }
        with patch(
            "api.routers.internal.settings.config_service.save_ui_branding_result",
            return_value=payload,
        ) as saver:
            response = client.post(
                "/settings/ui-branding",
                data={
                    "app_title": "Acme Graph",
                    "primary_color": "#123456",
                    "reset_logo": "false",
                },
            )
        assert response.status_code == 200
        assert response.json()["branding"]["primary_color"] == "#123456"
        saver.assert_called_once()
        assert saver.call_args.kwargs["logo_content"] is None
        assert saver.call_args.kwargs["reset_logo"] is False

    def test_save_ui_branding_accepts_logo_upload(self, client):
        payload = {
            "success": True,
            "branding": {
                "app_title": "Acme Graph",
                "primary_color": "#123456",
                "logo_url": "data:image/png;base64,abc",
                "is_custom_logo": True,
                "palette": {"primary_rgb": "18, 52, 86"},
            },
        }
        with patch(
            "api.routers.internal.settings.config_service.save_ui_branding_result",
            return_value=payload,
        ) as saver:
            response = client.post(
                "/settings/ui-branding",
                data={
                    "app_title": "Acme Graph",
                    "primary_color": "#123456",
                    "reset_logo": "false",
                },
                files={"logo_file": ("logo.png", b"png-bytes", "image/png")},
            )
        assert response.status_code == 200
        assert response.json()["branding"]["is_custom_logo"] is True
        saver.assert_called_once()
        assert saver.call_args.kwargs["logo_content"] == b"png-bytes"
        assert saver.call_args.kwargs["logo_mime"] == "image/png"

    def test_save_ui_branding_reset_flag_is_forwarded(self, client):
        payload = {"success": True, "branding": {"app_title": "OntoBricks"}}
        with patch(
            "api.routers.internal.settings.config_service.save_ui_branding_result",
            return_value=payload,
        ) as saver:
            response = client.post(
                "/settings/ui-branding",
                data={
                    "app_title": "OntoBricks",
                    "primary_color": "#4F46E5",
                    "reset_logo": "true",
                },
            )
        assert response.status_code == 200
        assert saver.call_args.kwargs["reset_logo"] is True

    def test_save_ui_branding_ignores_filename_less_empty_upload(self, client):
        payload = {"success": True, "branding": {"app_title": "Acme Graph"}}
        with patch(
            "api.routers.internal.settings.config_service.save_ui_branding_result",
            return_value=payload,
        ) as saver:
            response = client.post(
                "/settings/ui-branding",
                data={
                    "app_title": "Acme Graph",
                    "primary_color": "#123456",
                    "reset_logo": "false",
                },
                files={"logo_file": ("", b"", "application/octet-stream")},
            )
        assert response.status_code == 200
        assert saver.call_args.kwargs["logo_content"] is None
        assert saver.call_args.kwargs["logo_mime"] is None

    def test_save_ui_branding_ignores_empty_file_upload(self, client):
        payload = {"success": True, "branding": {"app_title": "Acme Graph"}}
        with patch(
            "api.routers.internal.settings.config_service.save_ui_branding_result",
            return_value=payload,
        ) as saver:
            response = client.post(
                "/settings/ui-branding",
                data={
                    "app_title": "Acme Graph",
                    "primary_color": "#123456",
                    "reset_logo": "false",
                },
                files={"logo_file": ("logo.png", b"", "image/png")},
            )
        assert response.status_code == 200
        assert saver.call_args.kwargs["logo_content"] is None
        assert saver.call_args.kwargs["logo_mime"] is None

    def test_save_ui_branding_maps_validation_error_to_400(self, client):
        with patch(
            "api.routers.internal.settings.config_service.save_ui_branding_result",
            side_effect=ValidationError("Invalid primary color"),
        ):
            response = client.post(
                "/settings/ui-branding",
                data={
                    "app_title": "Acme Graph",
                    "primary_color": "red",
                    "reset_logo": "false",
                },
            )
        assert response.status_code == 400
        body = response.json()
        assert body["error"] == "validation"
        assert "primary color" in body["message"].lower()

    def test_save_ui_branding_maps_permission_error_to_403(self, client):
        with patch(
            "api.routers.internal.settings.config_service.save_ui_branding_result",
            side_effect=AuthorizationError("Access denied"),
        ):
            response = client.post(
                "/settings/ui-branding",
                data={
                    "app_title": "Acme Graph",
                    "primary_color": "#123456",
                    "reset_logo": "false",
                },
            )
        assert response.status_code == 403

    def test_save_ui_branding_maps_persistence_error_to_502(self, client):
        with patch(
            "api.routers.internal.settings.config_service.save_ui_branding_result",
            side_effect=InfrastructureError("Failed to save UI branding", detail="down"),
        ):
            response = client.post(
                "/settings/ui-branding",
                data={
                    "app_title": "Acme Graph",
                    "primary_color": "#123456",
                    "reset_logo": "false",
                },
            )
        assert response.status_code == 502

    def test_get_ui_branding_non_admin_is_forbidden(self, client):
        middleware_mod = importlib.import_module("shared.fastapi.main")
        with (
            patch("back.core.databricks.is_databricks_app", return_value=True),
            patch.object(
                middleware_mod.PermissionMiddleware,
                "_resolve_roles",
                return_value=("app_user", "viewer"),
            ),
            patch(
                "api.routers.internal.settings.config_service.get_ui_branding_result"
            ) as getter,
        ):
            response = client.get(
                "/settings/ui-branding",
                headers={"accept": "application/json"},
                follow_redirects=False,
            )
        assert response.status_code == 403
        getter.assert_not_called()

    @pytest.mark.parametrize(
        "title,color",
        [
            ("", "#123456"),
            ("Acme Graph", "#GG3456"),
        ],
    )
    def test_save_ui_branding_rejects_invalid_payload(self, client, title, color):
        with patch(
            "api.routers.internal.settings.config_service.save_ui_branding_result",
            side_effect=ValidationError("validation failed"),
        ):
            response = client.post(
                "/settings/ui-branding",
                data={"app_title": title, "primary_color": color, "reset_logo": "false"},
            )
        assert response.status_code == 400
