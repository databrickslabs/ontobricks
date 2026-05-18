"""Tests for the OBX (export/preview/import) service methods on SettingsService.

Covers:
- Export: resolving versions per mode (all/active/latest/selected) and
  building a valid envelope from registry contents.
- Preview: per-domain conflict flags and the rename suggestion.
- Import: skip / overwrite / rename actions, decision defaulting to skip,
  rename-conflict guard.
"""

from __future__ import annotations

import importlib
import json

import pytest
from unittest.mock import MagicMock, patch

from back.core.errors import ValidationError
from back.objects.domain.SettingsService import SettingsService
from back.objects.registry import obx_format

_svc_module = importlib.import_module("back.objects.domain.SettingsService")


REGISTRY_CFG_OK = MagicMock(is_configured=True)


def _mock_session_settings():
    return MagicMock(), MagicMock()


def _fake_doc(name: str, version: str, mcp: bool = False):
    return {
        "info": {
            "name": name,
            "description": f"{name} v{version}",
            "mcp_enabled": mcp,
        },
        "versions": {version: {"ontology": {"classes": []}}},
    }


def _make_registry_svc(*, configured: bool = True, listing=None, docs=None,
                       mcp_version=None, exists=None):
    svc = MagicMock()
    svc.cfg.is_configured = configured
    listing = listing or {}
    docs = docs or {}
    exists = exists or {}

    svc.list_versions_sorted.side_effect = lambda folder, **_kw: list(listing.get(folder, []))
    svc.find_mcp_version.side_effect = lambda folder: (
        (mcp_version.get(folder), docs.get((folder, mcp_version.get(folder)), {}))
        if mcp_version and mcp_version.get(folder) else (None, {})
    )
    svc.read_version.side_effect = lambda folder, ver: (
        (True, docs[(folder, ver)], "") if (folder, ver) in docs else (False, {}, "missing")
    )
    svc.domain_exists.side_effect = lambda folder: exists.get(folder, False)
    svc.write_version.return_value = (True, "ok")
    return svc


# ===========================================
# Export
# ===========================================


class TestExportRegistryObx:
    def test_export_latest_only(self):
        session_mgr, settings = _mock_session_settings()
        doc1 = _fake_doc("claims", "1")
        doc2 = _fake_doc("claims", "2", mcp=True)
        registry_svc = _make_registry_svc(
            listing={"claims": ["2", "1"]},
            docs={("claims", "1"): doc1, ("claims", "2"): doc2},
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.export_registry_obx_result(
                {"domains": [{"name": "claims", "mode": "latest"}]},
                session_mgr,
                settings,
                exported_by="me@example.com",
            )

        env = result["envelope"]
        assert env["format_version"] == obx_format.CURRENT_OBX_FORMAT_VERSION
        assert env["exported_by"] == "me@example.com"
        assert len(env["domains"]) == 1
        out = env["domains"][0]
        assert out["name"] == "claims"
        assert list(out["versions"].keys()) == ["2"]
        assert result["domain_count"] == 1
        assert result["version_count"] == 1

    def test_export_all_versions(self):
        session_mgr, settings = _mock_session_settings()
        doc1 = _fake_doc("claims", "1")
        doc2 = _fake_doc("claims", "2")
        registry_svc = _make_registry_svc(
            listing={"claims": ["2", "1"]},
            docs={("claims", "1"): doc1, ("claims", "2"): doc2},
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.export_registry_obx_result(
                {"domains": [{"name": "claims", "mode": "all"}]},
                session_mgr,
                settings,
            )

        out = result["envelope"]["domains"][0]
        assert sorted(out["versions"].keys()) == ["1", "2"]

    def test_export_active_falls_back_to_latest(self):
        session_mgr, settings = _mock_session_settings()
        doc1 = _fake_doc("claims", "1")
        registry_svc = _make_registry_svc(
            listing={"claims": ["1"]},
            docs={("claims", "1"): doc1},
            mcp_version={"claims": None},
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.export_registry_obx_result(
                {"domains": [{"name": "claims", "mode": "active"}]},
                session_mgr,
                settings,
            )

        out = result["envelope"]["domains"][0]
        assert list(out["versions"].keys()) == ["1"]

    def test_export_selected_versions_filters_missing(self):
        session_mgr, settings = _mock_session_settings()
        doc1 = _fake_doc("claims", "1")
        registry_svc = _make_registry_svc(
            listing={"claims": ["1"]},
            docs={("claims", "1"): doc1},
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.export_registry_obx_result(
                {"domains": [{"name": "claims", "mode": "selected", "versions": ["1", "99"]}]},
                session_mgr,
                settings,
            )

        out = result["envelope"]["domains"][0]
        assert list(out["versions"].keys()) == ["1"]

    def test_export_rejects_when_registry_not_configured(self):
        session_mgr, settings = _mock_session_settings()
        registry_svc = _make_registry_svc(configured=False)
        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            with pytest.raises(ValidationError, match="Registry not configured"):
                SettingsService.export_registry_obx_result(
                    {"domains": [{"name": "claims", "mode": "latest"}]},
                    session_mgr,
                    settings,
                )

    def test_export_rejects_when_no_domains(self):
        session_mgr, settings = _mock_session_settings()
        registry_svc = _make_registry_svc()
        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            with pytest.raises(ValidationError, match="No domains selected"):
                SettingsService.export_registry_obx_result(
                    {"domains": []}, session_mgr, settings
                )


# ===========================================
# Preview
# ===========================================


class TestPreviewObxImport:
    def _build_obx_bytes(self, *, domains):
        env = obx_format.build_envelope(domains, exported_by="anon")
        return json.dumps(env).encode("utf-8")

    def test_preview_marks_existing_conflicts(self):
        session_mgr, settings = _mock_session_settings()
        registry_svc = _make_registry_svc(
            listing={"claims": ["1"], "claims_imported": []},
            exists={"claims": True},
        )

        file_bytes = self._build_obx_bytes(
            domains=[
                {
                    "name": "claims",
                    "info": {"description": "x"},
                    "versions": {"1": _fake_doc("claims", "1"), "2": _fake_doc("claims", "2")},
                }
            ]
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.preview_obx_import_result(
                file_bytes, session_mgr, settings
            )

        assert result["success"]
        assert result["format_version"] == obx_format.CURRENT_OBX_FORMAT_VERSION
        d = result["domains"][0]
        assert d["name"] == "claims"
        assert d["exists"] is True
        assert d["conflicting_versions"] == ["1"]
        assert d["incoming_versions"] == ["2", "1"]
        assert d["suggested_new_name"].startswith("claims_imported")

    def test_preview_new_domain_has_no_conflicts(self):
        session_mgr, settings = _mock_session_settings()
        registry_svc = _make_registry_svc(exists={})

        file_bytes = self._build_obx_bytes(
            domains=[{"name": "new_one", "info": {}, "versions": {"1": _fake_doc("new_one", "1")}}]
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.preview_obx_import_result(
                file_bytes, session_mgr, settings
            )

        d = result["domains"][0]
        assert d["exists"] is False
        assert d["conflicting_versions"] == []
        assert d["suggested_new_name"] == "new_one"

    def test_preview_rejects_malformed_json(self):
        session_mgr, settings = _mock_session_settings()
        registry_svc = _make_registry_svc()
        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            with pytest.raises(Exception):
                SettingsService.preview_obx_import_result(
                    b"not json at all", session_mgr, settings
                )


# ===========================================
# Import
# ===========================================


class TestImportRegistryObx:
    def _build_obx_bytes(self, *, domains):
        env = obx_format.build_envelope(domains, exported_by="anon")
        return json.dumps(env).encode("utf-8")

    def test_skip_default_when_no_decision(self):
        session_mgr, settings = _mock_session_settings()
        registry_svc = _make_registry_svc(exists={"claims": True})
        file_bytes = self._build_obx_bytes(
            domains=[
                {"name": "claims", "info": {}, "versions": {"1": _fake_doc("claims", "1")}}
            ]
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.import_registry_obx_result(
                file_bytes, [], session_mgr, settings
            )

        assert result["skipped_domains"] == 1
        assert result["imported_versions"] == 0
        registry_svc.write_version.assert_not_called()

    def test_overwrite_writes_each_version(self):
        session_mgr, settings = _mock_session_settings()
        registry_svc = _make_registry_svc(
            exists={"claims": True}, listing={"claims": ["1"]}
        )
        doc1 = _fake_doc("claims", "1")
        doc2 = _fake_doc("claims", "2")
        file_bytes = self._build_obx_bytes(
            domains=[{"name": "claims", "info": {}, "versions": {"1": doc1, "2": doc2}}]
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.import_registry_obx_result(
                file_bytes,
                [{"name": "claims", "action": "overwrite"}],
                session_mgr,
                settings,
            )

        assert result["imported_versions"] == 2
        assert result["overwritten_versions"] == 1  # version "1" already existed
        assert registry_svc.write_version.call_count == 2

    def test_rename_writes_to_new_folder(self):
        session_mgr, settings = _mock_session_settings()
        registry_svc = _make_registry_svc(
            exists={"claims": True},   # original conflict
            listing={"claims": ["1"]},
        )
        doc1 = _fake_doc("claims", "1")
        file_bytes = self._build_obx_bytes(
            domains=[{"name": "claims", "info": {}, "versions": {"1": doc1}}]
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.import_registry_obx_result(
                file_bytes,
                [{"name": "claims", "action": "rename", "new_name": "claims_copy"}],
                session_mgr,
                settings,
            )

        assert result["renamed_domains"] == 1
        assert result["imported_versions"] == 1
        registry_svc.write_version.assert_called_once()
        args, _ = registry_svc.write_version.call_args
        assert args[0] == "claims_copy"

    def test_rename_conflict_falls_back_to_skip(self):
        session_mgr, settings = _mock_session_settings()
        registry_svc = _make_registry_svc(
            exists={"claims": True, "claims_copy": True},
            listing={"claims": ["1"]},
        )
        doc1 = _fake_doc("claims", "1")
        file_bytes = self._build_obx_bytes(
            domains=[{"name": "claims", "info": {}, "versions": {"1": doc1}}]
        )

        with patch.object(_svc_module, "RegistryService") as rs_cls:
            rs_cls.from_context.return_value = registry_svc
            result = SettingsService.import_registry_obx_result(
                file_bytes,
                [{"name": "claims", "action": "rename", "new_name": "claims_copy"}],
                session_mgr,
                settings,
            )

        assert result["renamed_domains"] == 0
        assert result["skipped_domains"] == 1
        registry_svc.write_version.assert_not_called()
