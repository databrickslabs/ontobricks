"""Tests for shared.config — Settings, menu config, logging config, global config."""

import os
import logging
import pytest
from unittest.mock import patch


class TestGlobalConfig:
    def test_constants_exist(self):
        from shared.config.constants import (
            APP_NAME,
            APP_VERSION,
            APP_LOGGER_NAME,
            DEFAULT_LOG_LEVEL,
            DEFAULT_LOG_FILE,
            LOG_MAX_BYTES,
            LOG_BACKUP_COUNT,
            ONTOBRICKS_NS,
            DEFAULT_BASE_URI,
            SESSION_COOKIE_NAME,
            WIZARD_TEMPLATES,
        )

        assert APP_NAME
        assert APP_LOGGER_NAME
        assert isinstance(LOG_MAX_BYTES, int)
        assert isinstance(LOG_BACKUP_COUNT, int)
        assert isinstance(WIZARD_TEMPLATES, dict)


class TestLoggingConfig:
    def test_setup_logging(self, tmp_path):
        from back.core.logging import setup_logging, get_logger

        setup_logging(level="DEBUG", log_dir=str(tmp_path))
        logger = get_logger("back.core.test")
        assert logger is not None
        assert logger.name.startswith("ontobricks")

    def test_get_logger_none(self):
        from back.core.logging import get_logger
        from shared.config.constants import APP_LOGGER_NAME

        logger = get_logger(None)
        assert logger.name == APP_LOGGER_NAME

    def test_get_logger_app_prefix(self):
        from back.core.logging import get_logger
        from shared.config.constants import APP_LOGGER_NAME

        logger = get_logger("back.core.owl")
        assert logger.name == f"{APP_LOGGER_NAME}.core.owl"

    def test_get_logger_custom_name(self):
        from back.core.logging import get_logger

        logger = get_logger("custom_lib")
        assert logger.name == "custom_lib"


class TestMenuConfig:
    def test_get_menu_config(self):
        from front.config import get_menu_config

        config = get_menu_config()
        assert "brand" in config
        assert "menus" in config
        assert isinstance(config["menus"], list)
        assert config["brand"]["label"] == "OntoBricks"

    def test_get_menu_by_id_found(self):
        from front.config import get_menu_config, get_menu_by_id

        config = get_menu_config()
        if config["menus"]:
            first_id = config["menus"][0]["id"]
            result = get_menu_by_id(first_id)
            assert result is not None
            assert result["id"] == first_id

    def test_get_menu_by_id_not_found(self):
        from front.config import get_menu_by_id

        result = get_menu_by_id("nonexistent_menu_xyz")
        assert result is None


class TestSettings:
    def test_defaults(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("SECRET_KEY", raising=False)
        monkeypatch.delenv("REGISTRY_VOLUME", raising=False)
        monkeypatch.delenv("REGISTRY_CATALOG", raising=False)
        monkeypatch.delenv("REGISTRY_SCHEMA", raising=False)
        from shared.config.settings import Settings

        s = Settings(_env_file=None)
        assert s.secret_key == "dev-secret-key-change-in-prod"
        assert s.databricks_catalog == "main"
        assert s.registry_volume == "OntoBricksRegistry"
        assert s.session_max_age == 86400

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("SECRET_KEY", "my-secret")
        monkeypatch.setenv("DATABRICKS_HOST", "https://custom.databricks.com")
        from shared.config.settings import Settings

        s = Settings()
        assert s.secret_key == "my-secret"
        assert s.databricks_host == "https://custom.databricks.com"

    def test_get_settings_returns_settings(self):
        from shared.config.settings import get_settings

        s = get_settings()
        assert hasattr(s, "databricks_host")
        assert hasattr(s, "registry_catalog")
