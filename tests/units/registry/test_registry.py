"""Tests for back.objects.registry.RegistryService — RegistryCfg and RegistryService.

Lakebase is the only registry backend since v0.4.0 — the tests cover
config resolution, path builders (UC Volume side, for binary artefacts
only), domain/version CRUD wiring, and the scheduler's bootstrap-time
``RegistryCfg`` serialisation.
"""

import json
import pytest
from unittest.mock import MagicMock, patch, call

from back.objects.registry.RegistryService import (
    RegistryCfg,
    RegistryService,
    _DEFAULT_VOLUME,
    _DOMAINS_FOLDER,
    _LEGACY_DOMAINS_FOLDER,
)


# ------------------------------------------------------------------ helpers


def _make_domain(registry=None, host="", token=""):
    domain = MagicMock()
    domain.settings = {"registry": registry or {}}
    domain.databricks = {"host": host, "token": token}
    return domain


def _make_settings(**overrides):
    defaults = {
        "registry_catalog": "env_cat",
        "registry_schema": "env_sch",
        "registry_volume": "",
        "registry_volume_path": "",
        "lakebase_schema": "ontobricks_registry",
        "lakebase_database": "",
        "databricks_host": "https://host.databricks.com",
        "databricks_token": "tok-123",
    }
    defaults.update(overrides)
    s = MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


def _make_uc():
    return MagicMock()


def _make_svc(cfg=None, uc=None, store=None):
    """Build a RegistryService with a mocked store + domains folder pre-resolved.

    Avoids touching Lakebase from RegistryService construction.
    """
    svc = RegistryService(cfg or CFG, uc or _make_uc(), store=store or MagicMock())
    svc._resolved_domains_folder = _DOMAINS_FOLDER
    return svc


CFG = RegistryCfg(catalog="cat", schema="sch", volume="vol")


# ==================================================================
# RegistryCfg
# ==================================================================


class TestRegistryCfgConstruction:
    def test_direct(self):
        c = RegistryCfg(catalog="a", schema="b", volume="c")
        assert c.catalog == "a"
        assert c.schema == "b"
        assert c.volume == "c"

    def test_is_frozen(self):
        c = RegistryCfg(catalog="a", schema="b", volume="c")
        with pytest.raises(AttributeError):
            c.catalog = "x"

    def test_from_dict_full(self):
        c = RegistryCfg.from_dict({"catalog": "x", "schema": "y", "volume": "z"})
        assert c == RegistryCfg("x", "y", "z")

    def test_from_dict_defaults_volume(self):
        c = RegistryCfg.from_dict({"catalog": "x", "schema": "y"})
        assert c.volume == _DEFAULT_VOLUME

    def test_from_dict_empty_volume_gets_default(self):
        c = RegistryCfg.from_dict({"catalog": "x", "schema": "y", "volume": ""})
        assert c.volume == _DEFAULT_VOLUME

    def test_from_dict_empty(self):
        c = RegistryCfg.from_dict({})
        assert c.catalog == ""
        assert c.schema == ""
        assert c.volume == _DEFAULT_VOLUME


class TestRegistryCfgFromDomain:
    """Resolution order for ``RegistryCfg.from_domain``.

    Each test patches ``fetch_lakebase_registry_triplet`` so the unit
    suite stays hermetic — the live Lakebase round-trip is covered by
    the integration suite.
    """

    @staticmethod
    def _patch_no_lakebase_row(monkeypatch):
        from back.objects.registry.store.lakebase import store as _lb_store

        monkeypatch.setattr(
            _lb_store,
            "fetch_lakebase_registry_triplet",
            lambda schema, database="": None,
        )

    def test_uses_session_registry(self, monkeypatch):
        self._patch_no_lakebase_row(monkeypatch)
        domain = _make_domain(
            registry={"catalog": "s_cat", "schema": "s_sch", "volume": "s_vol"}
        )
        settings = _make_settings()
        c = RegistryCfg.from_domain(domain, settings)
        assert c.catalog == "s_cat"
        assert c.schema == "s_sch"
        assert c.volume == "s_vol"

    def test_falls_back_to_settings(self, monkeypatch):
        self._patch_no_lakebase_row(monkeypatch)
        domain = _make_domain(registry={})
        settings = _make_settings(
            registry_catalog="env_c", registry_schema="env_s", registry_volume="env_v"
        )
        c = RegistryCfg.from_domain(domain, settings)
        assert c.catalog == "env_c"
        assert c.schema == "env_s"
        assert c.volume == "env_v"

    def test_empty_volume_defaults(self, monkeypatch):
        self._patch_no_lakebase_row(monkeypatch)
        domain = _make_domain(registry={})
        settings = _make_settings(registry_volume="")
        c = RegistryCfg.from_domain(domain, settings)
        assert c.volume == _DEFAULT_VOLUME

    def test_registry_volume_path_overrides_session(self, monkeypatch):
        self._patch_no_lakebase_row(monkeypatch)
        domain = _make_domain(
            registry={"catalog": "wrong", "schema": "wrong", "volume": "wrong_vol"},
        )
        settings = _make_settings(
            registry_volume_path="/Volumes/benoit_cayla/ontobricks_deployed/registry",
        )
        c = RegistryCfg.from_domain(domain, settings)
        assert c.catalog == "benoit_cayla"
        assert c.schema == "ontobricks_deployed"
        assert c.volume == "registry"

    def test_lakebase_overrides_survive_volume_binding(self, monkeypatch):
        """Lakebase schema/database saved in the Settings UI must
        survive on a Databricks Apps deployment with a bound Volume."""
        self._patch_no_lakebase_row(monkeypatch)
        domain = _make_domain(
            registry={
                "lakebase_schema": "custom_schema",
                "lakebase_database": "custom_db",
            },
        )
        settings = _make_settings(
            registry_volume_path="/Volumes/benoit_cayla/ontobricks_deployed/registry",
            lakebase_schema="env_schema",
            lakebase_database="env_db",
        )
        c = RegistryCfg.from_domain(domain, settings)
        assert c.lakebase_schema == "custom_schema"
        assert c.lakebase_database == "custom_db"


class TestRegistryCfgFromDomainLakebaseRow:
    """``RegistryCfg.from_domain`` reads the catalog/schema/volume
    triplet from the Lakebase ``registries`` row when no Apps Volume
    path is injected; when both are present, the Volume binding wins.
    """

    def test_volume_binding_overrides_lakebase_row_triplet(self, monkeypatch):
        from back.objects.registry.store.lakebase import store as _lb_store

        monkeypatch.setattr(
            _lb_store,
            "fetch_lakebase_registry_triplet",
            lambda schema, database="": ("benoit_cayla", "ontobricks", "OntoBricksRegistry"),
        )
        domain = _make_domain(registry={})
        settings = _make_settings(
            registry_volume_path="/Volumes/benoit_cayla/ontobricks_deployed/registry",
        )
        c = RegistryCfg.from_domain(domain, settings)
        assert c.catalog == "benoit_cayla"
        assert c.schema == "ontobricks_deployed"
        assert c.volume == "registry"

    def test_lakebase_unreachable_falls_back_to_volume_binding(self, monkeypatch):
        from back.objects.registry.store.lakebase import store as _lb_store

        monkeypatch.setattr(
            _lb_store,
            "fetch_lakebase_registry_triplet",
            lambda schema, database="": None,
        )
        domain = _make_domain(registry={})
        settings = _make_settings(
            registry_volume_path="/Volumes/benoit_cayla/ontobricks_deployed/registry",
        )
        c = RegistryCfg.from_domain(domain, settings)
        assert c.catalog == "benoit_cayla"
        assert c.schema == "ontobricks_deployed"
        assert c.volume == "registry"

    def test_prefer_volume_binding_skips_lakebase_row(self, monkeypatch):
        """Initialize path: ``prefer_volume_binding=True`` must bypass
        the cached ``registries`` row so a re-bind + re-init cycle
        propagates the new triplet into Lakebase.
        """
        from back.objects.registry.store.lakebase import store as _lb_store

        called = {"yes": False}

        def _spy(schema, database=""):
            called["yes"] = True
            return ("benoit_cayla", "ontobricks", "OntoBricksRegistry")

        monkeypatch.setattr(_lb_store, "fetch_lakebase_registry_triplet", _spy)
        domain = _make_domain(registry={})
        settings = _make_settings(
            registry_volume_path=(
                "/Volumes/benoit_cayla/ontobricks_deployed_test/registry_test"
            ),
        )
        c = RegistryCfg.from_domain(
            domain, settings, prefer_volume_binding=True
        )
        assert called["yes"] is False
        assert c.catalog == "benoit_cayla"
        assert c.schema == "ontobricks_deployed_test"
        assert c.volume == "registry_test"

    def test_lakebase_database_override_passed_to_triplet_probe(self, monkeypatch):
        from back.objects.registry.store.lakebase import store as _lb_store

        captured = {}

        def _spy(schema, database=""):
            captured["schema"] = schema
            captured["database"] = database
            return ("c1", "s1", "v1")

        monkeypatch.setattr(_lb_store, "fetch_lakebase_registry_triplet", _spy)
        domain = _make_domain(
            registry={
                "lakebase_schema": "custom_schema",
                "lakebase_database": "custom_db",
            }
        )
        settings = _make_settings(
            registry_volume_path="/Volumes/benoit_cayla/ontobricks_deployed/registry",
        )
        RegistryCfg.from_domain(domain, settings)
        assert captured == {"schema": "custom_schema", "database": "custom_db"}


class TestRegistryCfgHelpers:
    def test_is_configured_true(self):
        assert RegistryCfg("a", "b", "c").is_configured is True

    def test_is_configured_missing_catalog(self):
        assert RegistryCfg("", "b", "c").is_configured is False

    def test_is_configured_missing_schema(self):
        assert RegistryCfg("a", "", "c").is_configured is False

    def test_is_configured_missing_volume(self):
        assert RegistryCfg("a", "b", "").is_configured is False

    def test_as_dict(self):
        c = RegistryCfg("x", "y", "z")
        assert c.as_dict() == {
            "catalog": "x",
            "schema": "y",
            "volume": "z",
            "lakebase_schema": "ontobricks_registry",
            "lakebase_database": "",
        }

    def test_as_dict_roundtrip(self):
        c = RegistryCfg("a", "b", "c")
        assert RegistryCfg.from_dict(c.as_dict()) == c


# ==================================================================
# RegistryService — path builders (UC Volume side, for binary artefacts)
# ==================================================================


class TestPathBuilders:
    def _svc(self, cfg=CFG):
        return _make_svc(cfg)

    def test_volume_root(self):
        assert self._svc().volume_root() == "/Volumes/cat/sch/vol"

    def test_domains_path(self):
        assert self._svc().domains_path() == "/Volumes/cat/sch/vol/domains"

    def test_domain_path(self):
        assert (
            self._svc().domain_path("my_proj") == "/Volumes/cat/sch/vol/domains/my_proj"
        )

    def test_version_path(self):
        assert self._svc().version_path("p", "3") == "/Volumes/cat/sch/vol/domains/p/V3"

    def test_version_file_path(self):
        assert (
            self._svc().version_file_path("p", "3")
            == "/Volumes/cat/sch/vol/domains/p/V3/V3.json"
        )

    def test_marker_path(self):
        assert self._svc().marker_path() == "/Volumes/cat/sch/vol/.registry"

    def test_config_file_path(self):
        assert (
            self._svc().config_file_path() == "/Volumes/cat/sch/vol/.global_config.json"
        )

    def test_history_file_path(self):
        assert (
            self._svc().history_file_path("p")
            == "/Volumes/cat/sch/vol/domains/p/.schedule_history.json"
        )


class TestResolveDomainsFolderFallback:
    """Backward-compatible folder resolution (domains/ vs legacy projects/)
    for the UC Volume side that still holds binary artefacts.
    """

    def test_prefers_domains_folder(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [], "")
        svc = RegistryService(CFG, uc, store=MagicMock())
        assert svc.domains_path().endswith("/domains")

    def test_falls_back_to_projects_folder(self):
        uc = _make_uc()
        uc.list_directory.side_effect = [
            (False, [], "not found"),
            (True, [], ""),
        ]
        svc = RegistryService(CFG, uc, store=MagicMock())
        assert svc.domains_path().endswith("/projects")

    def test_defaults_to_domains_when_neither_exists(self):
        uc = _make_uc()
        uc.list_directory.side_effect = [
            (False, [], "not found"),
            (False, [], "not found"),
        ]
        svc = RegistryService(CFG, uc, store=MagicMock())
        assert svc.domains_path().endswith("/domains")

    def test_resolution_is_cached(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [], "")
        svc = RegistryService(CFG, uc, store=MagicMock())
        svc.domains_path()
        svc.domains_path()
        assert uc.list_directory.call_count == 1


# ==================================================================
# RegistryService — lifecycle (initialize / is_initialized)
# ==================================================================


class TestIsInitialized:
    def test_delegates_to_store(self):
        store = MagicMock()
        store.is_initialized.return_value = True
        svc = _make_svc(store=store)
        assert svc.is_initialized() is True
        store.is_initialized.assert_called_once()


class TestInitialize:
    """``initialize`` ensures the binary UC Volume exists *and*
    forwards to the Lakebase store's ``initialize``.
    """

    @staticmethod
    def _cfg():
        return RegistryCfg(
            catalog="benoit_cayla",
            schema="ontobricks_deployed_test",
            volume="registry_test",
            lakebase_schema="ontobricks_registry",
        )

    def test_creates_volume_when_missing_and_initialises_store(self):
        store = MagicMock()
        store.initialize.return_value = (
            True,
            "Lakebase registry initialized at host/db (schema=ontobricks_registry)",
        )
        client = MagicMock()
        client.list_volumes.return_value = []
        client.create_volume.return_value = True

        svc = RegistryService(self._cfg(), _make_uc(), store=store)
        ok, msg = svc.initialize(client)

        assert ok is True
        client.list_volumes.assert_called_once_with(
            "benoit_cayla", "ontobricks_deployed_test"
        )
        client.create_volume.assert_called_once_with(
            "benoit_cayla", "ontobricks_deployed_test", "registry_test"
        )
        store.initialize.assert_called_once_with()
        assert "Lakebase registry initialized" in msg
        assert (
            "Created binary volume "
            "benoit_cayla.ontobricks_deployed_test.registry_test"
        ) in msg

    def test_skips_creation_when_volume_already_exists(self):
        store = MagicMock()
        store.initialize.return_value = (True, "Lakebase registry initialized")
        client = MagicMock()
        client.list_volumes.return_value = ["registry_test"]

        svc = RegistryService(self._cfg(), _make_uc(), store=store)
        ok, msg = svc.initialize(client)

        assert ok is True
        client.create_volume.assert_not_called()
        assert "already exists" in msg

    def test_volume_creation_failure_warns_but_still_initialises_schema(self):
        store = MagicMock()
        store.initialize.return_value = (True, "Lakebase registry initialized")
        client = MagicMock()
        client.list_volumes.return_value = []
        client.create_volume.return_value = False

        svc = RegistryService(self._cfg(), _make_uc(), store=store)
        ok, msg = svc.initialize(client)

        assert ok is True
        store.initialize.assert_called_once_with()
        assert "WARNING" in msg
        assert "CREATE VOLUME" in msg
        assert "registry_test" in msg

    def test_volume_probe_exception_is_reported_not_swallowed(self):
        store = MagicMock()
        store.initialize.return_value = (True, "Lakebase registry initialized")
        client = MagicMock()
        client.list_volumes.side_effect = RuntimeError("uc 503")

        svc = RegistryService(self._cfg(), _make_uc(), store=store)
        ok, msg = svc.initialize(client)

        assert ok is True
        store.initialize.assert_called_once_with()
        assert "WARNING" in msg
        assert "uc 503" in msg


# ==================================================================
# RegistryService — domain CRUD (delegates to store)
# ==================================================================


class TestListDomains:
    def test_delegates_to_store(self):
        store = MagicMock()
        store.list_domain_folders.return_value = (True, ["a", "b"], "")
        svc = _make_svc(store=store)

        ok, names, _ = svc.list_domains()
        assert ok is True
        assert names == ["a", "b"]
        store.list_domain_folders.assert_called_once()


class TestListAllBridges:
    """``list_all_bridges`` enumerates domains via the store, not
    by listing the UC Volume's ``domains/`` folder.
    """

    def test_uses_store_list_domain_folders(self):
        uc = _make_uc()
        uc.list_directory.side_effect = AssertionError(
            "list_all_bridges must not enumerate domains via _uc"
        )
        store = MagicMock()
        store.list_domain_folders.return_value = (True, ["proj_a", "proj_b"], "")
        svc = _make_svc(uc=uc, store=store)

        svc.load_latest_domain_data = lambda name: (
            True,
            {
                "info": {"description": ""},
                "versions": {
                    "1": {
                        "ontology": {
                            "base_uri": f"http://x/{name}",
                            "classes": [
                                {
                                    "name": "C",
                                    "uri": f"http://x/{name}#C",
                                    "emoji": "📦",
                                    "bridges": [
                                        {
                                            "target_domain": "other",
                                            "target_class_name": "T",
                                            "target_class_uri": "http://x/other#T",
                                            "label": "rel",
                                        }
                                    ],
                                }
                            ],
                        }
                    }
                },
            },
            "1",
            "",
        )

        ok, result, _ = svc.list_all_bridges()
        assert ok is True
        assert [d["name"] for d in result] == ["proj_a", "proj_b"]
        assert all(d["bridges"] for d in result)

    def test_skips_hidden_folder_names(self):
        store = MagicMock()
        store.list_domain_folders.return_value = (True, [".system", "real"], "")
        svc = _make_svc(store=store)
        svc.load_latest_domain_data = lambda name: (
            True,
            {"versions": {"1": {"ontology": {"base_uri": "u", "classes": []}}}},
            "1",
            "",
        )
        ok, result, _ = svc.list_all_bridges()
        assert ok is True
        assert [d["name"] for d in result] == ["real"]


class TestDeleteDomain:
    def test_delegates_to_store_and_volume(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [], "")
        uc.delete_directory.return_value = (True, "ok")
        store = MagicMock()
        store.delete_domain.return_value = []
        svc = _make_svc(uc=uc, store=store)

        errors = svc.delete_domain("my_proj")

        assert errors == []
        store.delete_domain.assert_called_once_with("my_proj")
        # Binary side (documents/ uploads) wiped via recursive_delete
        # against the UC Volume.
        assert uc.list_directory.call_args_list == [
            call("/Volumes/cat/sch/vol/domains/my_proj")
        ]


# ==================================================================
# RegistryService — version management (delegates to store)
# ==================================================================


class TestVersionDelegation:
    def test_list_versions_delegates(self):
        store = MagicMock()
        store.list_versions.return_value = (True, ["2", "1"], "")
        svc = _make_svc(store=store)
        ok, versions, _ = svc.list_versions("proj")
        assert ok and versions == ["2", "1"]

    def test_read_version_delegates(self):
        store = MagicMock()
        data = {"info": {"name": "test"}}
        store.read_version.return_value = (True, data, "")
        svc = _make_svc(store=store)
        ok, result, _ = svc.read_version("proj", "2")
        assert ok is True
        assert result == data
        store.read_version.assert_called_once_with("proj", "2")

    def test_write_version_parses_json_string(self):
        store = MagicMock()
        store.write_version.return_value = (True, "ok")
        svc = _make_svc(store=store)
        ok, _ = svc.write_version("proj", "5", '{"data": true}')
        assert ok is True
        store.write_version.assert_called_once()
        folder, version, payload = store.write_version.call_args.args
        assert folder == "proj"
        assert version == "5"
        assert payload == {"data": True}

    def test_write_version_rejects_invalid_json(self):
        store = MagicMock()
        svc = _make_svc(store=store)
        ok, msg = svc.write_version("proj", "5", "not-json{")
        assert ok is False
        assert "Invalid JSON" in msg
        store.write_version.assert_not_called()

    def test_delete_version_delegates(self):
        store = MagicMock()
        store.delete_version.return_value = (True, "ok")
        svc = _make_svc(store=store)
        ok, _ = svc.delete_version("proj", "3")
        assert ok is True
        store.delete_version.assert_called_once_with("proj", "3")


# ==================================================================
# RegistryService — recursive_delete (UC Volume binary side)
# ==================================================================


class TestRecursiveDelete:
    def test_deletes_files_then_directory(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [
                {
                    "name": "V1.json",
                    "path": "/Volumes/cat/sch/vol/domains/p/V1/V1.json",
                    "is_directory": False,
                },
            ],
            "",
        )
        uc.delete_file.return_value = (True, "ok")
        uc.delete_directory.return_value = (True, "ok")

        svc = _make_svc(uc=uc)
        errors = svc.recursive_delete("/Volumes/cat/sch/vol/domains/p")

        assert errors == []
        uc.delete_file.assert_called_once()
        uc.delete_directory.assert_called_once()

    def test_reports_errors(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [{"name": "f.json", "path": "/p/f.json", "is_directory": False}],
            "",
        )
        uc.delete_file.return_value = (False, "Permission denied")
        uc.delete_directory.return_value = (True, "ok")

        svc = _make_svc(uc=uc)
        errors = svc.recursive_delete("/p")

        assert len(errors) == 1
        assert "Permission denied" in errors[0]

    def test_listing_failure(self):
        uc = _make_uc()
        uc.list_directory.return_value = (False, [], "Cannot list")

        svc = _make_svc(uc=uc)
        errors = svc.recursive_delete("/gone")

        assert len(errors) == 1
        assert "Cannot list" in errors[0]


# ==================================================================
# RegistryService.from_context
# ==================================================================


class TestFromContext:
    @patch("back.core.helpers.get_databricks_host_and_token")
    def test_factory(self, mock_creds, monkeypatch):
        from back.objects.registry.store.lakebase import store as _lb_store

        monkeypatch.setattr(
            _lb_store,
            "fetch_lakebase_registry_triplet",
            lambda schema, database="": None,
        )
        mock_creds.return_value = ("https://host", "tok")
        domain = _make_domain(registry={"catalog": "c", "schema": "s", "volume": "v"})
        settings = _make_settings()

        with patch.object(
            RegistryService, "_build_store", return_value=MagicMock()
        ):
            svc = RegistryService.from_context(domain, settings)

        assert svc.cfg == RegistryCfg("c", "s", "v")
        assert svc.uc is not None


# ==================================================================
# BuildScheduler._resolve_creds — Lakebase fields plumbed at startup
# ==================================================================


class TestSchedulerResolveCredsLakebase:
    """At startup the scheduler restores jobs *before* the global
    config has been read, so the ``RegistryCfg`` it builds from
    *Settings* must already carry ``lakebase_schema`` /
    ``lakebase_database``.
    """

    def test_defaults_from_settings(self):
        from back.objects.registry.scheduler import BuildScheduler

        settings = _make_settings()
        host, token, cfg = BuildScheduler._resolve_creds(settings)
        assert host == "https://host.databricks.com"
        assert token == "tok-123"
        assert cfg["lakebase_schema"] == "ontobricks_registry"
        assert cfg["lakebase_database"] == ""

    def test_lakebase_with_database_override(self):
        from back.objects.registry.scheduler import BuildScheduler

        settings = _make_settings(
            lakebase_schema="ontobricks_registry",
            lakebase_database="ontobricks_other",
        )
        _h, _t, cfg = BuildScheduler._resolve_creds(settings)
        assert cfg["lakebase_schema"] == "ontobricks_registry"
        assert cfg["lakebase_database"] == "ontobricks_other"

    def test_registry_volume_path_overrides_static_env_triplet(self):
        """Scheduler boot must not use ``REGISTRY_VOLUME`` alone when the
        Apps runtime injects ``REGISTRY_VOLUME_PATH``."""
        from back.objects.registry.scheduler import BuildScheduler

        settings = _make_settings(
            registry_catalog="env_c",
            registry_schema="env_s",
            registry_volume="OntoBricksRegistry",
            registry_volume_path="/Volumes/acme/prod/custom_registry_vol",
        )
        _h, _t, cfg = BuildScheduler._resolve_creds(settings)
        assert cfg["catalog"] == "acme"
        assert cfg["schema"] == "prod"
        assert cfg["volume"] == "custom_registry_vol"


