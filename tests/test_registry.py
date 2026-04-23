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
    """Build a RegistryService with the domains folder pre-resolved (skips UC probe)."""
    svc = RegistryService(cfg or CFG, uc or _make_uc())
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
        assert c.as_dict() == {"catalog": "x", "schema": "y", "volume": "z"}

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


class TestDeleteDomain:
    def test_delegates_to_recursive_delete(self):
        uc = _make_uc()
        uc.list_directory.return_value = (True, [], "")
        uc.delete_directory.return_value = (True, "ok")
        svc = _make_svc(uc=uc)

        errors = svc.delete_domain("my_proj")

        assert errors == []
        uc.list_directory.assert_called_once_with(
            "/Volumes/cat/sch/vol/domains/my_proj"
        )


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
        uc.write_file.assert_called_once_with(
            "/Volumes/cat/sch/vol/domains/proj/V5/V5.json",
            '{"data": true}',
        )


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
        uc.list_directory.assert_called_once_with(
            "/Volumes/cat/sch/vol/domains/proj/V3"
        )


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
