"""Tests for back.objects.registry.RegistryService — RegistryCfg and RegistryService."""

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
        "registry_backend": "volume",
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


def _make_svc(cfg=None, uc=None):
    """Build a RegistryService with the domains folder pre-resolved (skips UC probe).

    The store now owns its own domain-folder probe, so we pre-resolve
    both the service- and the store-level cache.
    """
    svc = RegistryService(cfg or CFG, uc or _make_uc())
    svc._resolved_domains_folder = _DOMAINS_FOLDER
    if hasattr(svc._store, "_resolved_domains_folder"):
        svc._store._resolved_domains_folder = _DOMAINS_FOLDER
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

    def test_from_domain_uses_session_registry(self):
        domain = _make_domain(
            registry={"catalog": "s_cat", "schema": "s_sch", "volume": "s_vol"}
        )
        settings = _make_settings()
        c = RegistryCfg.from_domain(domain, settings)
        assert c.catalog == "s_cat"
        assert c.schema == "s_sch"
        assert c.volume == "s_vol"

    def test_from_domain_falls_back_to_settings(self):
        domain = _make_domain(registry={})
        settings = _make_settings(
            registry_catalog="env_c", registry_schema="env_s", registry_volume="env_v"
        )
        c = RegistryCfg.from_domain(domain, settings)
        assert c.catalog == "env_c"
        assert c.schema == "env_s"
        assert c.volume == "env_v"

    def test_from_domain_partial_fallback(self):
        domain = _make_domain(registry={"catalog": "session_cat"})
        settings = _make_settings(registry_catalog="env_cat", registry_schema="env_sch")
        c = RegistryCfg.from_domain(domain, settings)
        assert c.catalog == "session_cat"
        assert c.schema == "env_sch"

    def test_from_domain_empty_volume_defaults(self):
        domain = _make_domain(registry={})
        settings = _make_settings(registry_volume="")
        c = RegistryCfg.from_domain(domain, settings)
        assert c.volume == _DEFAULT_VOLUME

    def test_from_domain_registry_volume_path_overrides_session(self):
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

    def test_from_domain_volume_path_keeps_session_backend(self):
        """The Volume-resource path must NOT clobber the admin-saved
        backend choice. Otherwise flipping the radio in Settings →
        Registry Location is silently discarded on every request."""
        domain = _make_domain(
            registry={"backend": "lakebase"},
        )
        settings = _make_settings(
            registry_volume_path="/Volumes/benoit_cayla/ontobricks_deployed/registry",
            registry_backend="volume",
        )
        c = RegistryCfg.from_domain(domain, settings)
        assert c.backend == "lakebase"
        assert c.catalog == "benoit_cayla"
        assert c.schema == "ontobricks_deployed"
        assert c.volume == "registry"

    def test_from_domain_volume_path_keeps_session_lakebase_overrides(self):
        """Lakebase schema/database overrides saved in the Settings UI
        must survive on a Databricks Apps deployment."""
        domain = _make_domain(
            registry={
                "backend": "lakebase",
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
        assert c.backend == "lakebase"
        assert c.lakebase_schema == "custom_schema"
        assert c.lakebase_database == "custom_db"

    def test_from_domain_no_volume_path_session_backend_wins(self):
        """Same precedence on the non-bound (local-dev) path."""
        domain = _make_domain(
            registry={
                "catalog": "c",
                "schema": "s",
                "volume": "v",
                "backend": "lakebase",
            },
        )
        settings = _make_settings(registry_backend="volume")
        c = RegistryCfg.from_domain(domain, settings)
        assert c.backend == "lakebase"


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
        # ``backend`` and ``lakebase_*`` were added when Lakebase
        # support landed — Volume-only callers see the defaults.
        assert c.as_dict() == {
            "catalog": "x",
            "schema": "y",
            "volume": "z",
            "backend": "volume",
            "lakebase_schema": "ontobricks_registry",
            "lakebase_database": "",
        }

    def test_as_dict_roundtrip(self):
        c = RegistryCfg("a", "b", "c")
        assert RegistryCfg.from_dict(c.as_dict()) == c


# ==================================================================
# RegistryService — path builders
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

    def test_paths_with_different_cfg(self):
        c = RegistryCfg("a", "b", "c")
        svc = _make_svc(c)
        assert svc.volume_root() == "/Volumes/a/b/c"


class TestResolveDomainsFolderFallback:
    """Test backward-compatible folder resolution (domains/ vs legacy projects/)."""

    def test_prefers_domains_folder(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [], "")
        svc = RegistryService(CFG, uc)
        assert svc.domains_path().endswith("/domains")

    def test_falls_back_to_projects_folder(self):
        uc = _make_uc()
        uc.list_directory.side_effect = [
            (False, [], "not found"),
            (True, [], ""),
        ]
        svc = RegistryService(CFG, uc)
        assert svc.domains_path().endswith("/projects")

    def test_defaults_to_domains_when_neither_exists(self):
        uc = _make_uc()
        uc.list_directory.side_effect = [
            (False, [], "not found"),
            (False, [], "not found"),
        ]
        svc = RegistryService(CFG, uc)
        assert svc.domains_path().endswith("/domains")

    def test_resolution_is_cached(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [], "")
        svc = RegistryService(CFG, uc)
        svc.domains_path()
        svc.domains_path()
        assert uc.list_directory.call_count == 1


# ==================================================================
# RegistryService — lifecycle
# ==================================================================


class TestIsInitialized:
    def test_true_when_marker_exists(self):
        uc = _make_uc()
        uc.read_file.return_value = (True, "content", "")
        svc = RegistryService(CFG, uc)
        assert svc.is_initialized() is True
        uc.read_file.assert_called_once_with("/Volumes/cat/sch/vol/.registry")

    def test_false_when_marker_missing(self):
        uc = _make_uc()
        uc.read_file.return_value = (False, "", "Not found")
        svc = RegistryService(CFG, uc)
        assert svc.is_initialized() is False


class TestInitialize:
    def test_creates_volume_and_marker(self):
        uc = _make_uc()
        # ``initialize`` now goes through the store, which writes the
        # ``.registry`` marker via ``uc.write_file`` and expects a
        # ``(ok, msg)`` tuple back.
        uc.write_file.return_value = (True, "ok")
        client = MagicMock()
        client.list_volumes.return_value = []
        client.create_volume.return_value = True

        svc = RegistryService(CFG, uc)
        ok, msg = svc.initialize(client)

        assert ok is True
        assert "initialized" in msg.lower()
        client.create_volume.assert_called_once_with("cat", "sch", "vol")
        uc.write_file.assert_called_once()

    def test_skips_volume_creation_if_exists(self):
        uc = _make_uc()
        uc.write_file.return_value = (True, "ok")
        client = MagicMock()
        client.list_volumes.return_value = ["vol"]

        svc = RegistryService(CFG, uc)
        ok, _ = svc.initialize(client)

        assert ok is True
        client.create_volume.assert_not_called()

    def test_returns_false_if_volume_creation_fails(self):
        uc = _make_uc()
        client = MagicMock()
        client.list_volumes.return_value = []
        client.create_volume.return_value = False

        svc = RegistryService(CFG, uc)
        ok, msg = svc.initialize(client)

        assert ok is False
        assert "failed" in msg.lower()


# ==================================================================
# RegistryService — domain CRUD
# ==================================================================


class TestListDomains:
    def test_success(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [
                {"name": "proj_b"},
                {"name": ".hidden"},
                {"name": "proj_a"},
            ],
            "",
        )
        svc = _make_svc(uc=uc)

        ok, names, msg = svc.list_domains()
        assert ok is True
        assert names == ["proj_a", "proj_b"]

    def test_excludes_hidden_dirs(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [
                {"name": ".registry"},
                {"name": ".hidden"},
                {"name": "visible"},
            ],
            "",
        )
        svc = _make_svc(uc=uc)

        ok, names, _ = svc.list_domains()
        assert names == ["visible"]

    def test_failure(self):
        uc = _make_uc()
        uc.list_directory.return_value = (False, [], "Not found")
        svc = _make_svc(uc=uc)

        ok, names, msg = svc.list_domains()
        assert ok is False
        assert names == []
        assert msg == "Not found"


class TestListDomainDetails:
    def test_returns_names_descriptions_versions(self):
        uc = _make_uc()
        uc.list_directory.side_effect = [
            # First call: list domain folders under domains/
            (True, [{"name": "proj_a"}, {"name": ".hidden"}], ""),
            # Second call: list_versions -> list V{N}/ dirs inside proj_a
            (True, [{"name": "V2"}, {"name": "V1"}], ""),
        ]
        uc.read_file.side_effect = [
            (
                True,
                json.dumps(
                    {
                        "info": {
                            "description": "My desc",
                            "mcp_enabled": True,
                            "last_update": "2025-06-01T10:00:00",
                            "last_build": "2025-06-01T11:00:00",
                        }
                    }
                ),
                "",
            ),
            (True, json.dumps({"info": {"description": "Old desc"}}), ""),
        ]
        svc = _make_svc(uc=uc)

        ok, result, _ = svc.list_domain_details()
        assert ok is True
        assert len(result) == 1
        assert result[0]["name"] == "proj_a"
        assert result[0]["description"] == "My desc"
        assert result[0]["versions"] == [
            {
                "version": "2",
                "active": True,
                "last_update": "2025-06-01T10:00:00",
                "last_build": "2025-06-01T11:00:00",
            },
            {"version": "1", "active": False, "last_update": "", "last_build": ""},
        ]


class TestListAllBridges:
    """``list_all_bridges`` must enumerate domains via the active
    backend's :class:`RegistryStore` (so Lakebase registries work)
    and only fall back to the Volume for the ontology read path
    (which itself goes through ``read_version`` on the store too).
    """

    def test_uses_store_list_domain_folders_not_volume(self):
        """Regression: previously enumerated via
        ``_uc.list_directory(self.domains_path(), dirs_only=True)``
        which returns nothing on a Lakebase-only registry.
        """
        uc = _make_uc()
        # Make sure we'd FAIL noisily if the implementation regressed
        # back to listing through ``_uc`` against the domains folder.
        uc.list_directory.side_effect = AssertionError(
            "list_all_bridges must not enumerate domains via _uc"
        )
        svc = _make_svc(uc=uc)

        # Force the store to return two domains; load_latest_domain_data
        # is patched to short-circuit (no I/O).
        svc._store.list_domain_folders = lambda: (
            True,
            ["proj_a", "proj_b"],
            "",
        )
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
        """``.hidden`` entries from the store must be filtered out."""
        uc = _make_uc()
        svc = _make_svc(uc=uc)
        svc._store.list_domain_folders = lambda: (
            True,
            [".system", "real"],
            "",
        )
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
    def test_delegates_to_recursive_delete(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [], "")
        uc.delete_directory.return_value = (True, "ok")
        svc = _make_svc(uc=uc)

        errors = svc.delete_domain("my_proj")

        assert errors == []
        # Two passes: once via ``store.delete_domain`` (JSON side) and
        # once via ``recursive_delete`` (binary side — documents/ +
        # *.lbug.tar.gz live on the Volume regardless of backend).
        assert all(
            c == call("/Volumes/cat/sch/vol/domains/my_proj")
            for c in uc.list_directory.call_args_list
        )
        assert uc.list_directory.call_count == 2


# ==================================================================
# RegistryService — version management
# ==================================================================


class TestListVersions:
    def test_extracts_version_strings(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [
                {"name": "V1"},
                {"name": "V2"},
                {"name": "V10"},
                {"name": ".schedule_history.json"},
            ],
            "",
        )
        svc = _make_svc(uc=uc)

        ok, versions, _ = svc.list_versions("proj")
        assert ok is True
        assert set(versions) == {"1", "2", "10"}

    def test_failure(self):
        uc = _make_uc()
        uc.list_directory.return_value = (False, [], "err")
        svc = _make_svc(uc=uc)

        ok, versions, msg = svc.list_versions("proj")
        assert ok is False
        assert versions == []


class TestListVersionsSorted:
    def test_sorted_descending(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [
                {"name": "V1"},
                {"name": "V10"},
                {"name": "V2"},
            ],
            "",
        )
        svc = _make_svc(uc=uc)

        vs = svc.list_versions_sorted("proj")
        assert vs == ["10", "2", "1"]

    def test_sorted_ascending(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [
                {"name": "V3"},
                {"name": "V1"},
            ],
            "",
        )
        svc = _make_svc(uc=uc)

        vs = svc.list_versions_sorted("proj", reverse=False)
        assert vs == ["1", "3"]

    def test_empty_on_failure(self):
        uc = _make_uc()
        uc.list_directory.return_value = (False, [], "err")
        svc = _make_svc(uc=uc)

        assert svc.list_versions_sorted("proj") == []


class TestGetLatestVersion:
    def test_returns_highest(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [
                {"name": "V1"},
                {"name": "V3"},
                {"name": "V2"},
            ],
            "",
        )
        svc = _make_svc(uc=uc)

        assert svc.get_latest_version("proj") == "3"

    def test_returns_none_when_empty(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [], "")
        svc = _make_svc(uc=uc)

        assert svc.get_latest_version("proj") is None


class TestReadVersion:
    def test_success(self):
        uc = _make_uc()
        data = {"info": {"name": "test"}}
        uc.read_file.return_value = (True, json.dumps(data), "")
        svc = _make_svc(uc=uc)

        ok, result, msg = svc.read_version("proj", "2")
        assert ok is True
        assert result == data
        uc.read_file.assert_called_once_with(
            "/Volumes/cat/sch/vol/domains/proj/V2/V2.json"
        )

    def test_file_not_found(self):
        uc = _make_uc()
        uc.read_file.return_value = (False, "", "Not found")
        svc = _make_svc(uc=uc)

        ok, result, msg = svc.read_version("proj", "99")
        assert ok is False
        assert result == {}

    def test_invalid_json(self):
        uc = _make_uc()
        uc.read_file.return_value = (True, "not-json{", "")
        svc = _make_svc(uc=uc)

        ok, result, msg = svc.read_version("proj", "1")
        assert ok is False
        assert "Invalid JSON" in msg


class TestWriteVersion:
    def test_delegates_to_uc(self):
        uc = _make_uc()
        uc.write_file.return_value = (True, "ok")
        svc = _make_svc(uc=uc)

        ok, msg = svc.write_version("proj", "5", '{"data": true}')
        assert ok is True
        # The store re-serialises the parsed dict with ``indent=2`` and
        # always overwrites — we check the path + dict equivalence
        # rather than the exact string spelling.
        uc.write_file.assert_called_once()
        path, payload = uc.write_file.call_args.args
        assert path == "/Volumes/cat/sch/vol/domains/proj/V5/V5.json"
        assert json.loads(payload) == {"data": True}
        assert uc.write_file.call_args.kwargs == {"overwrite": True}


class TestDeleteVersion:
    def test_delegates_to_recursive_delete(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [
                {
                    "name": "V3.json",
                    "path": "/Volumes/cat/sch/vol/domains/proj/V3/V3.json",
                    "is_directory": False,
                },
            ],
            "",
        )
        uc.delete_file.return_value = (True, "ok")
        uc.delete_directory.return_value = (True, "ok")
        svc = _make_svc(uc=uc)

        ok, msg = svc.delete_version("proj", "3")
        assert ok is True
        # As with delete_domain, the JSON side (store) and the binary
        # side (service.recursive_delete) each list the version dir.
        for c in uc.list_directory.call_args_list:
            assert c == call("/Volumes/cat/sch/vol/domains/proj/V3")
        assert uc.list_directory.call_count >= 1


# ==================================================================
# RegistryService — load_latest_domain_data
# ==================================================================


class TestLoadLatestDomainData:
    def test_success(self):
        uc = _make_uc()
        uc.list_directory.return_value = (
            True,
            [
                {"name": "V1"},
                {"name": "V3"},
                {"name": "V2"},
            ],
            "",
        )
        data = {"info": {"name": "test"}}
        uc.read_file.return_value = (True, json.dumps(data), "")

        svc = _make_svc(uc=uc)
        ok, result, version, err = svc.load_latest_domain_data("proj")

        assert ok is True
        assert result == data
        assert version == "3"
        assert err == ""
        uc.read_file.assert_called_once_with(
            "/Volumes/cat/sch/vol/domains/proj/V3/V3.json"
        )

    def test_no_versions(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [], "")

        svc = _make_svc(uc=uc)
        ok, result, version, err = svc.load_latest_domain_data("empty_proj")

        assert ok is False
        assert "no versions" in err.lower()

    def test_read_failure(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [{"name": "V1"}], "")
        uc.read_file.return_value = (False, "", "Read error")

        svc = _make_svc(uc=uc)
        ok, result, version, err = svc.load_latest_domain_data("proj")

        assert ok is False
        assert version == "1"
        assert err == "Read error"


# ==================================================================
# RegistryService — recursive_delete
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
            [
                {"name": "f.json", "path": "/p/f.json", "is_directory": False},
            ],
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
    def test_factory(self, mock_creds):
        mock_creds.return_value = ("https://host", "tok")
        domain = _make_domain(registry={"catalog": "c", "schema": "s", "volume": "v"})
        settings = _make_settings()

        svc = RegistryService.from_context(domain, settings)

        assert svc.cfg == RegistryCfg("c", "s", "v")
        assert svc.uc is not None


# ==================================================================
# BuildScheduler._resolve_creds — backend fields plumbed at startup
# ==================================================================


class TestSchedulerResolveCredsBackend:
    """At startup the scheduler restores jobs *before* the global
    config has been read, so the ``RegistryCfg`` it builds from
    *Settings* must already carry ``backend`` / ``lakebase_schema``
    / ``lakebase_database``. Otherwise schedule reads/writes would
    silently default to Volume even on a Lakebase deployment.
    """

    def test_volume_defaults(self):
        from back.objects.registry.scheduler import BuildScheduler

        settings = _make_settings()
        host, token, cfg = BuildScheduler._resolve_creds(settings)
        assert host == "https://host.databricks.com"
        assert token == "tok-123"
        assert cfg["backend"] == "volume"
        assert cfg["lakebase_schema"] == "ontobricks_registry"
        assert cfg["lakebase_database"] == ""

    def test_lakebase_with_database_override(self):
        from back.objects.registry.scheduler import BuildScheduler

        settings = _make_settings(
            registry_backend="lakebase",
            lakebase_schema="ontobricks_registry",
            lakebase_database="ontobricks_other",
        )
        _h, _t, cfg = BuildScheduler._resolve_creds(settings)
        assert cfg["backend"] == "lakebase"
        assert cfg["lakebase_schema"] == "ontobricks_registry"
        assert cfg["lakebase_database"] == "ontobricks_other"


# ==================================================================
# POST /settings/registry — locked-resource semantics
# ==================================================================


class TestSaveRegistryRouteLockedSemantics:
    """The registry is "locked" when the Volume is supplied by a
    Databricks App resource binding (``settings.registry_volume_path``
    is set inside ``is_databricks_app()``). Locking must protect the
    bound triplet ``catalog/schema/volume`` only — the **backend
    chooser** (Volume ↔ Lakebase) and the Lakebase-side knobs
    (``lakebase_schema`` / ``lakebase_database``) must remain editable
    so admins can flip backends from the UI without redeploying.
    """

    # NOTE: these tests run the ``save_registry`` coroutine via a fresh
    # private event loop instead of relying on pytest-asyncio. With
    # ``asyncio_mode = auto`` and the ``anyio`` plugin both loaded, the
    # session-wide pytest-asyncio runner can leak state across tests
    # (``Runner.run() cannot be called from a running event loop``).
    # Using ``asyncio.new_event_loop().run_until_complete`` keeps these
    # cases hermetic.

    def test_locked_blocks_catalog_schema_volume_changes(self):
        from api.routers.internal import settings as settings_router

        req = MagicMock()
        req.json = MagicMock(return_value=_AwaitableValue({"catalog": "evil"}))
        sm = MagicMock()
        s = _make_settings()

        with patch.object(
            settings_router.config_service, "is_registry_locked", return_value=True
        ), patch.object(
            settings_router.config_service, "apply_registry_save"
        ) as apply_mock:
            with pytest.raises(Exception) as excinfo:
                _run(settings_router.save_registry(req, sm, s))
            assert "cannot be changed here" in str(excinfo.value)
            apply_mock.assert_not_called()

    def test_locked_allows_backend_switch(self):
        """Locked registry must still accept ``backend`` updates."""
        from api.routers.internal import settings as settings_router

        req = MagicMock()
        req.json = MagicMock(
            return_value=_AwaitableValue({"backend": "lakebase"})
        )
        sm = MagicMock()
        s = _make_settings()

        with patch.object(
            settings_router.config_service, "is_registry_locked", return_value=True
        ), patch.object(
            settings_router.config_service,
            "apply_registry_save",
            return_value={"success": True, "message": "ok"},
        ) as apply_mock:
            result = _run(settings_router.save_registry(req, sm, s))

        assert result["success"] is True
        apply_mock.assert_called_once()
        forwarded = apply_mock.call_args[0][0]
        assert forwarded == {"backend": "lakebase"}

    def test_locked_allows_lakebase_database_override(self):
        from api.routers.internal import settings as settings_router

        req = MagicMock()
        req.json = MagicMock(
            return_value=_AwaitableValue(
                {"lakebase_database": "ontobricks_other", "lakebase_schema": "reg"}
            )
        )
        sm = MagicMock()
        s = _make_settings()

        with patch.object(
            settings_router.config_service, "is_registry_locked", return_value=True
        ), patch.object(
            settings_router.config_service,
            "apply_registry_save",
            return_value={"success": True, "message": "ok"},
        ) as apply_mock:
            result = _run(settings_router.save_registry(req, sm, s))

        assert result["success"] is True
        forwarded = apply_mock.call_args[0][0]
        assert forwarded["lakebase_database"] == "ontobricks_other"
        assert forwarded["lakebase_schema"] == "reg"

    def test_locked_strips_locked_keys_silently_when_empty(self):
        """Empty locked-keys (e.g. ``catalog: ""``) are stripped, not rejected.

        Some clients echo the full registry config back; an empty / falsy
        value is a no-op and should not raise — only *non-empty* writes
        to the bound triplet are blocked.
        """
        from api.routers.internal import settings as settings_router

        req = MagicMock()
        req.json = MagicMock(
            return_value=_AwaitableValue(
                {"catalog": "", "schema": "", "backend": "lakebase"}
            )
        )
        sm = MagicMock()
        s = _make_settings()

        with patch.object(
            settings_router.config_service, "is_registry_locked", return_value=True
        ), patch.object(
            settings_router.config_service,
            "apply_registry_save",
            return_value={"success": True, "message": "ok"},
        ) as apply_mock:
            _run(settings_router.save_registry(req, sm, s))

        forwarded = apply_mock.call_args[0][0]
        assert "catalog" not in forwarded
        assert "schema" not in forwarded
        assert forwarded["backend"] == "lakebase"

    def test_unlocked_passes_payload_through(self):
        from api.routers.internal import settings as settings_router

        req = MagicMock()
        payload = {"catalog": "c", "schema": "s", "volume": "v", "backend": "volume"}
        req.json = MagicMock(return_value=_AwaitableValue(payload))
        sm = MagicMock()
        s = _make_settings()

        with patch.object(
            settings_router.config_service, "is_registry_locked", return_value=False
        ), patch.object(
            settings_router.config_service,
            "apply_registry_save",
            return_value={"success": True, "message": "ok"},
        ) as apply_mock:
            _run(settings_router.save_registry(req, sm, s))

        apply_mock.assert_called_once()
        forwarded = apply_mock.call_args[0][0]
        assert forwarded == payload


class _AwaitableValue:
    """Minimal awaitable wrapper so ``await req.json()`` works in tests."""

    def __init__(self, value):
        self._value = value

    def __await__(self):
        async def _coro():
            return self._value

        return _coro().__await__()


def _run(coro):
    """Run *coro* on a fresh event loop in a worker thread and return its result.

    Some earlier tests in the suite leave an event loop running on
    the main thread (a known pytest-asyncio + anyio interaction).
    Spawning a thread with its own loop sidesteps that pollution and
    keeps each ``save_registry`` invocation hermetic.
    """
    import asyncio
    import threading

    result = [None]
    error: list = []

    def _runner():
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            result[0] = loop.run_until_complete(coro)
        except BaseException as exc:  # noqa: BLE001
            error.append(exc)
        finally:
            loop.close()

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join()
    if error:
        raise error[0]
    return result[0]
