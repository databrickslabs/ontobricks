"""Tests for Graph DB Engine configuration.

Covers: GlobalConfigService get/set graph_engine, SettingsService orchestration,
and TripleStoreFactory engine resolution.
"""
import pytest
from unittest.mock import patch, MagicMock

from back.core.errors import ValidationError
from back.objects.session.GlobalConfigService import GlobalConfigService
from back.objects.domain.settings_service import SettingsService


REGISTRY_CFG = {"catalog": "cat", "schema": "sch", "volume": "vol"}


def _mock_context():
    return MagicMock(), MagicMock()


# ---------------------------------------------------------------
#  GlobalConfigService – graph_engine
# ---------------------------------------------------------------

class TestGlobalConfigGraphEngine:

    def test_empty_defaults_contain_graph_engine(self):
        empty = GlobalConfigService._empty()
        assert "graph_engine" in empty
        assert empty["graph_engine"] == "ladybug"

    def test_get_graph_engine_default(self):
        svc = GlobalConfigService()
        with patch.object(svc, "load", return_value=GlobalConfigService._empty()):
            engine = svc.get_graph_engine("h", "t", REGISTRY_CFG)
        assert engine == "ladybug"

    def test_get_graph_engine_falls_back_on_unknown(self):
        svc = GlobalConfigService()
        data = GlobalConfigService._empty()
        data["graph_engine"] = "unknown_engine"
        with patch.object(svc, "load", return_value=data):
            engine = svc.get_graph_engine("h", "t", REGISTRY_CFG)
        assert engine == "ladybug"

    def test_set_graph_engine_valid(self):
        svc = GlobalConfigService()
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, msg = svc.set_graph_engine("h", "t", REGISTRY_CFG, "ladybug")
        assert ok
        mock_save.assert_called_once_with("h", "t", REGISTRY_CFG, {"graph_engine": "ladybug"})

    def test_set_graph_engine_invalid_rejected(self):
        svc = GlobalConfigService()
        ok, msg = svc.set_graph_engine("h", "t", REGISTRY_CFG, "neo4j")
        assert not ok
        assert "Unknown graph engine" in msg

    def test_set_graph_engine_empty_rejected(self):
        svc = GlobalConfigService()
        ok, msg = svc.set_graph_engine("h", "t", REGISTRY_CFG, "")
        assert not ok

    def test_set_graph_engine_normalises_case(self):
        svc = GlobalConfigService()
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, _ = svc.set_graph_engine("h", "t", REGISTRY_CFG, "  LADYBUG  ")
        assert ok
        mock_save.assert_called_once_with("h", "t", REGISTRY_CFG, {"graph_engine": "ladybug"})


# ---------------------------------------------------------------
#  SettingsService – graph engine orchestration
# ---------------------------------------------------------------

class TestSettingsServiceGraphEngine:

    def test_get_graph_engine_result(self):
        session_mgr, settings = _mock_context()

        with patch("back.objects.domain.settings_service.SettingsService._resolve_context",
                    return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch("back.objects.domain.settings_service.global_config_service") as gcs:
            gcs.get_graph_engine.return_value = "ladybug"
            gcs.ALLOWED_GRAPH_ENGINES = ("ladybug",)
            result = SettingsService.get_graph_engine_result(session_mgr, settings)

        assert result["success"]
        assert result["graph_engine"] == "ladybug"
        assert "ladybug" in result["allowed_engines"]

    def test_set_graph_engine_result_success(self):
        session_mgr, settings = _mock_context()
        request = MagicMock()

        with patch("back.objects.domain.settings_service.SettingsService._resolve_context",
                    return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch("back.objects.domain.settings_service.SettingsService.require_admin_error"), \
             patch("back.objects.domain.settings_service.global_config_service") as gcs:
            gcs.set_graph_engine.return_value = (True, "ok")
            result = SettingsService.set_graph_engine_result("ladybug", request, session_mgr, settings)

        assert result["success"]
        assert result["graph_engine"] == "ladybug"

    def test_set_graph_engine_result_validation_error(self):
        session_mgr, settings = _mock_context()
        request = MagicMock()

        with patch("back.objects.domain.settings_service.SettingsService._resolve_context",
                    return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch("back.objects.domain.settings_service.SettingsService.require_admin_error"), \
             patch("back.objects.domain.settings_service.global_config_service") as gcs:
            gcs.set_graph_engine.return_value = (False, "Unknown graph engine 'neo4j'")
            with pytest.raises(ValidationError, match="Unknown graph engine"):
                SettingsService.set_graph_engine_result("neo4j", request, session_mgr, settings)
