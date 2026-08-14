"""MCP companion app name must follow deploy.config.sh (issue #137).

``scripts/deploy.config.sh`` derives ``MCP_APP_NAME=mcp-${APP_NAME}``
(e.g. ``ontobricks-07x`` → ``mcp-ontobricks-07x``). In-app registry /
graph grants historically defaulted to the bare ``mcp-ontobricks`` string,
so Initialize skipped MCP grants on every instance-suffixed deploy.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from back.core.databricks.lakebase.grants import resolve_mcp_app_name
from back.objects.domain.SettingsService import SettingsService


class TestResolveMcpAppName:
    def test_explicit_override_wins(self):
        assert (
            resolve_mcp_app_name(
                app_name="ontobricks-07x", explicit="mcp-custom"
            )
            == "mcp-custom"
        )

    def test_derives_mcp_prefix_from_main_app(self):
        assert resolve_mcp_app_name(app_name="ontobricks-07x") == "mcp-ontobricks-07x"

    def test_env_override_when_no_explicit(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("MCP_APP_NAME", "mcp-from-env")
        assert resolve_mcp_app_name(app_name="ontobricks-07x") == "mcp-from-env"

    def test_fallback_when_no_app_name(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("MCP_APP_NAME", raising=False)
        assert resolve_mcp_app_name(app_name="") == "mcp-ontobricks"

    def test_does_not_double_prefix_mcp_apps(self):
        assert resolve_mcp_app_name(app_name="mcp-ontobricks-07x") == "mcp-ontobricks-07x"


class TestRegistryGrantAppNames:
    def test_uses_suffixed_mcp_companion(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("MCP_APP_NAME", raising=False)
        settings = SimpleNamespace(ontobricks_app_name="ontobricks-07x")
        assert SettingsService._registry_grant_app_names(settings) == [
            "ontobricks-07x",
            "mcp-ontobricks-07x",
        ]

    def test_env_still_overrides_derived_name(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("MCP_APP_NAME", "mcp-override")
        settings = SimpleNamespace(ontobricks_app_name="ontobricks-07x")
        assert SettingsService._registry_grant_app_names(settings) == [
            "ontobricks-07x",
            "mcp-override",
        ]
