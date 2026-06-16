"""Tests for Graph DB Engine configuration.

Covers: GlobalConfigService get/set graph_engine + graph_engine_config,
SettingsService orchestration, and TripleStoreFactory engine resolution.
"""

import importlib

import pytest
from unittest.mock import patch, MagicMock

from back.core.errors import ValidationError
from back.objects.session.GlobalConfigService import GlobalConfigService
from back.objects.registry import RegistryCfg, RegistryService
from back.objects.domain.SettingsService import SettingsService

_svc_module = importlib.import_module("back.objects.domain.SettingsService")
_db_auth_mod = importlib.import_module("back.core.databricks.DatabricksAuth")
_uc_mod = importlib.import_module("back.core.databricks.UnityCatalog")


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
        assert empty["graph_engine"] == "lakebase"

    def test_get_graph_engine_default(self):
        svc = GlobalConfigService()
        with patch.object(svc, "load", return_value=GlobalConfigService._empty()):
            engine = svc.get_graph_engine("h", "t", REGISTRY_CFG)
        assert engine == "lakebase"

    def test_get_graph_engine_falls_back_on_unknown(self):
        svc = GlobalConfigService()
        data = GlobalConfigService._empty()
        data["graph_engine"] = "unknown_engine"
        with patch.object(svc, "load", return_value=data):
            engine = svc.get_graph_engine("h", "t", REGISTRY_CFG)
        assert engine == "lakebase"

    def test_set_graph_engine_lakebase_valid(self):
        svc = GlobalConfigService()
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, msg = svc.set_graph_engine("h", "t", REGISTRY_CFG, "lakebase")
        assert ok
        mock_save.assert_called_once_with(
            "h", "t", REGISTRY_CFG, {"graph_engine": "lakebase"}
        )

    def test_set_graph_engine_config_rejects_bad_schema(self):
        svc = GlobalConfigService()
        ok, msg = svc.set_graph_engine_config(
            "h", "t", REGISTRY_CFG, {"schema": "bad-schema!", "database": ""}
        )
        assert not ok
        assert "schema" in msg.lower() or "invalid" in msg.lower()

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
            ok, _ = svc.set_graph_engine("h", "t", REGISTRY_CFG, "  LAKEBASE  ")
        assert ok
        mock_save.assert_called_once_with(
            "h", "t", REGISTRY_CFG, {"graph_engine": "lakebase"}
        )


# ---------------------------------------------------------------
#  GlobalConfigService – stale-while-revalidate (regression)
#
#  Regression for the 2026-05-04 cohort-preview timeout: when the
#  registry backend (Lakebase) momentarily fails on a cache-miss
#  read, the service used to fall back to ``_empty()`` and overwrite
#  the previously-cached config. Every downstream caller then re-hit
#  the slow backend, compounding the outage. The service now keeps
#  the last-good cache for ``_STALE_CACHE_TTL`` and serves it on
#  failure.
# ---------------------------------------------------------------


class TestGlobalConfigStaleWhileRevalidate:
    """Backend failures must not blow away the previously-cached config."""

    def _good_cfg(self) -> dict:
        return {
            "warehouse_id": "wh-prod",
            "graph_engine": "lakebase",
            "default_base_uri": "https://example.com",
        }

    def test_serves_stale_cache_on_backend_failure(self):
        svc = GlobalConfigService()
        good = self._good_cfg()

        store_ok = MagicMock()
        store_ok.load_global_config.return_value = good
        store_ok.backend = "lakebase"
        store_fail = MagicMock()
        store_fail.load_global_config.side_effect = RuntimeError("SSL timeout")
        store_fail.backend = "lakebase"

        with patch.object(svc, "_store_for", return_value=store_ok):
            first = svc.load("h", "t", REGISTRY_CFG, force=True)
        assert first == good

        with patch.object(svc, "_store_for", return_value=store_fail):
            stale = svc.load("h", "t", REGISTRY_CFG, force=True)
        assert stale == good
        assert stale is svc._cache

    def test_falls_back_to_empty_when_no_prior_cache(self):
        svc = GlobalConfigService()
        store_fail = MagicMock()
        store_fail.load_global_config.side_effect = RuntimeError("SSL timeout")
        store_fail.backend = "lakebase"

        with patch.object(svc, "_store_for", return_value=store_fail):
            data = svc.load("h", "t", REGISTRY_CFG, force=True)

        assert data == GlobalConfigService._empty()


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
        mock_save.assert_called_once_with(
            "h", "t", REGISTRY_CFG, {"graph_engine_config": config}
        )

    def test_set_graph_engine_config_empty_dict_valid(self):
        svc = GlobalConfigService()
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, msg = svc.set_graph_engine_config("h", "t", REGISTRY_CFG, {})
        assert ok
        mock_save.assert_called_once_with(
            "h", "t", REGISTRY_CFG, {"graph_engine_config": {}}
        )

    def test_set_graph_engine_config_lakebase_database_and_schema(self):
        svc = GlobalConfigService()
        cfg = {"database": "analytics", "schema": "ontobricks_graph"}
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, msg = svc.set_graph_engine_config("h", "t", REGISTRY_CFG, cfg)
        assert ok
        mock_save.assert_called_once_with(
            "h", "t", REGISTRY_CFG, {"graph_engine_config": cfg}
        )

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

    def test_set_graph_engine_config_accepts_managed_synced_keys(self):
        svc = GlobalConfigService()
        cfg = {
            "schema": "ontobricks_graph",
            "sync_mode": "managed_synced",
            "sync_table_mode": "snapshot",
            "sync_timeout_s": 900,
            "sync_uc_catalog": "main",
        }
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, _ = svc.set_graph_engine_config("h", "t", REGISTRY_CFG, cfg)
        assert ok
        mock_save.assert_called_once_with(
            "h", "t", REGISTRY_CFG, {"graph_engine_config": cfg}
        )

    def test_set_graph_engine_config_rejects_bad_sync_mode(self):
        svc = GlobalConfigService()
        ok, msg = svc.set_graph_engine_config(
            "h", "t", REGISTRY_CFG, {"sync_mode": "weird"}
        )
        assert not ok
        assert "sync_mode" in msg

    def test_set_graph_engine_config_rejects_bad_sync_table_mode(self):
        svc = GlobalConfigService()
        ok, msg = svc.set_graph_engine_config(
            "h", "t", REGISTRY_CFG, {"sync_table_mode": "yearly"}
        )
        assert not ok
        assert "sync_table_mode" in msg

    def test_set_graph_engine_config_rejects_negative_timeout(self):
        svc = GlobalConfigService()
        ok, msg = svc.set_graph_engine_config(
            "h", "t", REGISTRY_CFG, {"sync_timeout_s": -10}
        )
        assert not ok
        assert "sync_timeout_s" in msg


# ---------------------------------------------------------------
#  SettingsService – graph engine orchestration
# ---------------------------------------------------------------


class TestSettingsServiceGraphEngine:

    def test_get_graph_engine_result(self):
        session_mgr, settings = _mock_context()

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.get_graph_engine.return_value = "lakebase"
            gcs.ALLOWED_GRAPH_ENGINES = ("lakebase",)
            result = SettingsService.get_graph_engine_result(session_mgr, settings)

        assert result["success"]
        assert result["graph_engine"] == "lakebase"
        assert "lakebase" in result["allowed_engines"]

    def test_set_graph_engine_result_success(self):
        session_mgr, settings = _mock_context()

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(SettingsService, "require_admin_error"),
            patch.object(SettingsService, "_mirror_graph_engine_to_domain_registry"),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.set_graph_engine.return_value = (True, "ok")
            gcs.get_graph_engine.return_value = "lakebase"
            result = SettingsService.set_graph_engine_result(
                "lakebase", "", "", session_mgr, settings
            )

        assert result["success"]
        assert result["graph_engine"] == "lakebase"

    def test_set_graph_engine_result_validation_error(self):
        session_mgr, settings = _mock_context()

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(SettingsService, "require_admin_error"),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.set_graph_engine.return_value = (False, "Unknown graph engine 'neo4j'")
            with pytest.raises(ValidationError, match="Unknown graph engine"):
                SettingsService.set_graph_engine_result(
                    "neo4j", "", "", session_mgr, settings
                )


# ---------------------------------------------------------------
#  SettingsService – graph engine config orchestration
# ---------------------------------------------------------------


class TestSettingsServiceGraphEngineConfig:

    def test_get_graph_engine_config_result(self):
        session_mgr, settings = _mock_context()
        expected_cfg = {"host": "remote.db", "port": 7687}

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.get_graph_engine_config.return_value = expected_cfg
            result = SettingsService.get_graph_engine_config_result(
                session_mgr, settings
            )

        assert result["success"]
        assert result["graph_engine_config"] == expected_cfg

    def test_get_graph_engine_config_result_empty(self):
        session_mgr, settings = _mock_context()

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.get_graph_engine_config.return_value = {}
            result = SettingsService.get_graph_engine_config_result(
                session_mgr, settings
            )

        assert result["success"]
        assert result["graph_engine_config"] == {}

    def test_set_graph_engine_config_result_success(self):
        session_mgr, settings = _mock_context()
        cfg = {"host": "localhost", "port": 7687}

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(SettingsService, "require_admin_error"),
            patch.object(SettingsService, "_mirror_graph_engine_to_domain_registry"),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.set_graph_engine_config.return_value = (True, "ok")
            gcs.get_graph_engine_config.return_value = cfg
            result = SettingsService.set_graph_engine_config_result(
                cfg, "", "", session_mgr, settings
            )

        assert result["success"]
        assert result["graph_engine_config"] == cfg

    def test_set_graph_engine_config_result_validation_error(self):
        session_mgr, settings = _mock_context()

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(SettingsService, "require_admin_error"),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.set_graph_engine_config.return_value = (
                False,
                "graph_engine_config must be a JSON object",
            )
            with pytest.raises(ValidationError, match="JSON object"):
                SettingsService.set_graph_engine_config_result(
                    "not-a-dict", "", "", session_mgr, settings
                )


class TestSettingsServiceRegistryPayloadGraphEngine:

    def test_build_registry_get_payload_includes_graph_engine(self):
        session_mgr, settings = _mock_context()
        rcfg = MagicMock()
        rcfg.is_configured = True
        rcfg.as_dict.return_value = {
            "catalog": "c",
            "schema": "s",
            "volume": "v",
            "backend": "volume",
            "lakebase_schema": "ontobricks_registry",
            "lakebase_database": "",
        }

        rs = MagicMock()
        rs.is_initialized.return_value = True

        with (
            patch.object(RegistryCfg, "from_session", return_value=rcfg),
            patch.object(RegistryService, "from_context", return_value=rs),
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(SettingsService, "is_registry_locked", return_value=False),
            patch.object(SettingsService, "_lakebase_runtime_info", return_value={}),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.get_graph_engine.return_value = "lakebase"
            gcs.get_graph_engine_config.return_value = {"schema": "ontobricks_graph"}
            payload = SettingsService.build_registry_get_payload(session_mgr, settings)

        assert payload["success"]
        assert payload["graph_engine"] == "lakebase"
        assert payload["graph_engine_config"] == {"schema": "ontobricks_graph"}

    def test_build_registry_get_payload_defaults_graph_when_not_configured(self):
        session_mgr, settings = _mock_context()
        rcfg = MagicMock()
        rcfg.is_configured = False
        rcfg.as_dict.return_value = {
            "catalog": "",
            "schema": "",
            "volume": "",
            "lakebase_schema": "ontobricks_registry",
            "lakebase_database": "",
        }

        with (
            patch.object(RegistryCfg, "from_session", return_value=rcfg),
            patch.object(SettingsService, "is_registry_locked", return_value=False),
            patch.object(SettingsService, "_lakebase_runtime_info", return_value={}),
        ):
            payload = SettingsService.build_registry_get_payload(session_mgr, settings)

        assert payload["graph_engine"] == "lakebase"
        assert payload["graph_engine_config"] == {}


class TestGraphEngineLakebaseHealth:
    def test_no_binding(self):
        session_mgr, settings = _mock_context()
        auth = MagicMock()
        auth.is_available = False
        with patch("back.core.databricks.get_lakebase_auth", return_value=auth):
            with pytest.raises(ValidationError, match="Lakebase not available"):
                SettingsService.graph_engine_lakebase_health_result(session_mgr, settings)

    def test_probe_success_schema_exists(self):
        session_mgr, settings = _mock_context()
        auth = MagicMock()
        auth.is_available = True
        auth.database = "bounddb"
        auth.kwargs.return_value = {
            "host": "h",
            "port": 5432,
            "dbname": "bounddb",
            "user": "u",
            "password": "tok",
            "sslmode": "require",
            "connect_timeout": 10,
            "application_name": "x",
            "keepalives": 1,
            "keepalives_idle": 10,
            "keepalives_interval": 5,
            "keepalives_count": 3,
        }

        mock_cur = MagicMock()
        mock_cur.fetchone.side_effect = [(True,), (5,)]
        cur_cm = MagicMock()
        cur_cm.__enter__.return_value = mock_cur
        cur_cm.__exit__.return_value = False

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cur_cm

        conn_cm = MagicMock()
        conn_cm.__enter__.return_value = mock_conn
        conn_cm.__exit__.return_value = False

        psycopg_mod = MagicMock()
        psycopg_mod.connect = MagicMock(return_value=conn_cm)

        with (
            patch("back.core.databricks.get_lakebase_auth", return_value=auth),
            patch.dict("os.environ", {"PGHOST": "lh", "PGUSER": "u", "PGPORT": "5432"}, clear=False),
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "global_config_service") as gcs,
            patch(
                "back.core.graphdb.lakebase.pool._require_psycopg",
                return_value=(psycopg_mod, MagicMock()),
            ),
        ):
            gcs.get_graph_engine_config.return_value = {
                "database": "",
                "schema": "ontobricks_graph",
            }
            out = SettingsService.graph_engine_lakebase_health_result(session_mgr, settings)

        assert out["success"] is True
        assert out["schema_exists"] is True
        assert out["tables_in_schema"] == 5
        assert out["graph_schema"] == "ontobricks_graph"
        psycopg_mod.connect.assert_called_once()
        call_kw = psycopg_mod.connect.call_args[1]
        assert call_kw["dbname"] == "bounddb"

    def test_probe_failure_returns_graceful_result(self):
        # A failed connection / missing database must NOT raise (502); it
        # returns success=False with a message so the UI shows a warning.
        session_mgr, settings = _mock_context()
        auth = MagicMock()
        auth.is_available = True
        auth.database = "bounddb"
        auth.kwargs.return_value = {"host": "h", "port": 5432, "dbname": "bounddb"}

        psycopg_mod = MagicMock()
        psycopg_mod.connect = MagicMock(
            side_effect=Exception('database "graph4" does not exist')
        )

        with (
            patch("back.core.databricks.get_lakebase_auth", return_value=auth),
            patch.dict("os.environ", {"PGHOST": "lh", "PGUSER": "u", "PGPORT": "5432"}, clear=False),
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "global_config_service") as gcs,
            patch(
                "back.core.graphdb.lakebase.pool._require_psycopg",
                return_value=(psycopg_mod, MagicMock()),
            ),
        ):
            gcs.get_graph_engine_config.return_value = {
                "database": "graph4",
                "schema": "ontobricks_graph",
            }
            out = SettingsService.graph_engine_lakebase_health_result(session_mgr, settings)

        assert out["success"] is False
        assert out["reason"] == "probe_failed"
        assert "graph4" in out["message"]
        assert out["schema_exists"] is False

    def test_bad_schema_config(self):
        session_mgr, settings = _mock_context()
        auth = MagicMock()
        auth.is_available = True
        with (
            patch("back.core.databricks.get_lakebase_auth", return_value=auth),
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.get_graph_engine_config.return_value = {"schema": "99bad"}
            with pytest.raises(ValidationError):
                SettingsService.graph_engine_lakebase_health_result(session_mgr, settings)


class TestGraphEngineUcCatalogs:
    def test_missing_warehouse_message(self):
        session_mgr, settings = _mock_context()
        # Pin all warehouse-id sources to empty so the "configure warehouse" message fires.
        settings.sql_warehouse_id = None
        domain_mock = MagicMock()
        domain_mock.databricks = None  # disables the session fallback
        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(domain_mock, "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "global_config_service") as gcs,
        ):
            gcs.load = MagicMock()
            gcs.get_warehouse_id.return_value = ""
            with pytest.raises(ValidationError, match="warehouse"):
                SettingsService.graph_engine_uc_catalogs_result(session_mgr, settings)

    def test_returns_sorted_catalogs(self):
        session_mgr, settings = _mock_context()
        mock_uc = MagicMock()
        mock_uc.get_catalogs.return_value = ["zeta", "main", "alpha"]
        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "global_config_service") as gcs,
            patch.object(_db_auth_mod, "DatabricksAuth", MagicMock()),
            patch.object(_uc_mod, "UnityCatalog", return_value=mock_uc),
        ):
            gcs.load = MagicMock()
            gcs.get_warehouse_id.return_value = "wh-1"
            out = SettingsService.graph_engine_uc_catalogs_result(session_mgr, settings)
        assert out["success"] is True
        assert out["catalogs"] == ["alpha", "main", "zeta"]
        mock_uc.get_catalogs.assert_called_once()
