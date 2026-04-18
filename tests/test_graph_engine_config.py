"""Tests for Graph DB Engine configuration.

Covers: GlobalConfigService get/set graph_engine + graph_engine_config,
SettingsService orchestration, and TripleStoreFactory engine resolution.
"""
import importlib

import pytest
from unittest.mock import patch, MagicMock

from back.core.errors import ValidationError
from back.objects.session.GlobalConfigService import GlobalConfigService
from back.objects.domain.SettingsService import SettingsService

_svc_module = importlib.import_module("back.objects.domain.SettingsService")


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
#  GlobalConfigService – graph_engine_config
# ---------------------------------------------------------------

class TestGlobalConfigGraphEngineConfig:

    def test_empty_defaults_contain_graph_engine_config(self):
        empty = GlobalConfigService._empty()
        assert "graph_engine_config" in empty
        assert empty["graph_engine_config"] == {}

    def test_get_graph_engine_config_returns_dict(self):
        svc = GlobalConfigService()
        data = GlobalConfigService._empty()
        data["graph_engine_config"] = {"host": "neo4j.local", "port": 7687}
        with patch.object(svc, "load", return_value=data):
            cfg = svc.get_graph_engine_config("h", "t", REGISTRY_CFG)
        assert cfg == {"host": "neo4j.local", "port": 7687}

    def test_get_graph_engine_config_returns_empty_when_missing(self):
        svc = GlobalConfigService()
        data = GlobalConfigService._empty()
        del data["graph_engine_config"]
        with patch.object(svc, "load", return_value=data):
            cfg = svc.get_graph_engine_config("h", "t", REGISTRY_CFG)
        assert cfg == {}

    def test_get_graph_engine_config_returns_empty_when_not_a_dict(self):
        svc = GlobalConfigService()
        data = GlobalConfigService._empty()
        data["graph_engine_config"] = "not-a-dict"
        with patch.object(svc, "load", return_value=data):
            cfg = svc.get_graph_engine_config("h", "t", REGISTRY_CFG)
        assert cfg == {}

    def test_set_graph_engine_config_valid(self):
        svc = GlobalConfigService()
        config = {"host": "localhost", "port": 7687}
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, msg = svc.set_graph_engine_config("h", "t", REGISTRY_CFG, config)
        assert ok
        mock_save.assert_called_once_with("h", "t", REGISTRY_CFG, {"graph_engine_config": config})

    def test_set_graph_engine_config_empty_dict_valid(self):
        svc = GlobalConfigService()
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, msg = svc.set_graph_engine_config("h", "t", REGISTRY_CFG, {})
        assert ok
        mock_save.assert_called_once_with("h", "t", REGISTRY_CFG, {"graph_engine_config": {}})

    def test_set_graph_engine_config_rejects_non_dict(self):
        svc = GlobalConfigService()
        ok, msg = svc.set_graph_engine_config("h", "t", REGISTRY_CFG, "bad")
        assert not ok
        assert "JSON object" in msg

    def test_set_graph_engine_config_rejects_list(self):
        svc = GlobalConfigService()
        ok, msg = svc.set_graph_engine_config("h", "t", REGISTRY_CFG, [1, 2])
        assert not ok
        assert "JSON object" in msg


# ---------------------------------------------------------------
#  SettingsService – graph engine orchestration
# ---------------------------------------------------------------

class TestSettingsServiceGraphEngine:

    def test_get_graph_engine_result(self):
        session_mgr, settings = _mock_context()

        with patch.object(SettingsService, "_resolve_context",
                          return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch.object(_svc_module, "global_config_service") as gcs:
            gcs.get_graph_engine.return_value = "ladybug"
            gcs.ALLOWED_GRAPH_ENGINES = ("ladybug",)
            result = SettingsService.get_graph_engine_result(session_mgr, settings)

        assert result["success"]
        assert result["graph_engine"] == "ladybug"
        assert "ladybug" in result["allowed_engines"]

    def test_set_graph_engine_result_success(self):
        session_mgr, settings = _mock_context()

        with patch.object(SettingsService, "_resolve_context",
                          return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch.object(SettingsService, "require_admin_error"), \
             patch.object(_svc_module, "global_config_service") as gcs:
            gcs.set_graph_engine.return_value = (True, "ok")
            result = SettingsService.set_graph_engine_result("ladybug", "", "", session_mgr, settings)

        assert result["success"]
        assert result["graph_engine"] == "ladybug"

    def test_set_graph_engine_result_validation_error(self):
        session_mgr, settings = _mock_context()

        with patch.object(SettingsService, "_resolve_context",
                          return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch.object(SettingsService, "require_admin_error"), \
             patch.object(_svc_module, "global_config_service") as gcs:
            gcs.set_graph_engine.return_value = (False, "Unknown graph engine 'neo4j'")
            with pytest.raises(ValidationError, match="Unknown graph engine"):
                SettingsService.set_graph_engine_result("neo4j", "", "", session_mgr, settings)


# ---------------------------------------------------------------
#  SettingsService – graph engine config orchestration
# ---------------------------------------------------------------

class TestSettingsServiceGraphEngineConfig:

    def test_get_graph_engine_config_result(self):
        session_mgr, settings = _mock_context()
        expected_cfg = {"host": "remote.db", "port": 7687}

        with patch.object(SettingsService, "_resolve_context",
                          return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch.object(_svc_module, "global_config_service") as gcs:
            gcs.get_graph_engine_config.return_value = expected_cfg
            result = SettingsService.get_graph_engine_config_result(session_mgr, settings)

        assert result["success"]
        assert result["graph_engine_config"] == expected_cfg

    def test_get_graph_engine_config_result_empty(self):
        session_mgr, settings = _mock_context()

        with patch.object(SettingsService, "_resolve_context",
                          return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch.object(_svc_module, "global_config_service") as gcs:
            gcs.get_graph_engine_config.return_value = {}
            result = SettingsService.get_graph_engine_config_result(session_mgr, settings)

        assert result["success"]
        assert result["graph_engine_config"] == {}

    def test_set_graph_engine_config_result_success(self):
        session_mgr, settings = _mock_context()
        cfg = {"host": "localhost", "port": 7687}

        with patch.object(SettingsService, "_resolve_context",
                          return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch.object(SettingsService, "require_admin_error"), \
             patch.object(_svc_module, "global_config_service") as gcs:
            gcs.set_graph_engine_config.return_value = (True, "ok")
            result = SettingsService.set_graph_engine_config_result(cfg, "", "", session_mgr, settings)

        assert result["success"]
        assert result["graph_engine_config"] == cfg

    def test_set_graph_engine_config_result_validation_error(self):
        session_mgr, settings = _mock_context()

        with patch.object(SettingsService, "_resolve_context",
                          return_value=(MagicMock(), "h", "t", REGISTRY_CFG)), \
             patch.object(SettingsService, "require_admin_error"), \
             patch.object(_svc_module, "global_config_service") as gcs:
            gcs.set_graph_engine_config.return_value = (False, "graph_engine_config must be a JSON object")
            with pytest.raises(ValidationError, match="JSON object"):
                SettingsService.set_graph_engine_config_result("not-a-dict", "", "", session_mgr, settings)
