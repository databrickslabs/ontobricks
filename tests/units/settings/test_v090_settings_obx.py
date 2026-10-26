"""ObxSettings (0.9.0): OBX registry export / preview / import edge cases —
export mode resolution, payload validation, CamelCase import naming, rename
suggestion and the rename rewrite of identity / graph runtime."""

from __future__ import annotations

import importlib
import json
from unittest.mock import MagicMock

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.objects.domain.settings.ObxSettings import ObxSettings

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")


@pytest.fixture
def svc(monkeypatch):
    registry = MagicMock()
    registry.cfg.is_configured = True
    registry.domain_exists.return_value = False
    registry.list_versions_sorted.return_value = []
    registry.write_version.return_value = (True, "ok")
    rs_cls = MagicMock()
    rs_cls.from_context.return_value = registry
    monkeypatch.setattr(_ss, "RegistryService", rs_cls)
    monkeypatch.setattr(_ss, "get_domain", lambda sm: MagicMock())
    monkeypatch.setattr(_ss, "invalidate_registry_cache", MagicMock())
    monkeypatch.setattr(_ss, "resolve_default_base_uri", lambda d, s: "https://ex.org/")
    return registry


@pytest.fixture
def raw_envelope(monkeypatch):
    """Bypass ``obx_format.load`` so tests can feed arbitrary envelopes."""
    monkeypatch.setattr(_ss.obx_format, "load", lambda env: env)

    def _encode(domains, **extra):
        return json.dumps({"format_version": 1, "domains": domains, **extra}).encode()

    return _encode


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


class TestResolveVersionsForExport:
    def test_no_versions_returns_empty(self, svc):
        svc.list_versions_sorted.return_value = []
        assert ObxSettings._resolve_versions_for_export(svc, "d", "all", None) == []

    def test_active_falls_back_to_latest_without_mcp_version(self, svc):
        svc.list_versions_sorted.return_value = ["3", "2", "1"]
        svc.find_mcp_version.return_value = (None, {})
        assert ObxSettings._resolve_versions_for_export(svc, "d", "active", None) == ["3"]

    def test_selected_keeps_registry_order_and_drops_missing(self, svc):
        svc.list_versions_sorted.return_value = ["3", "2", "1"]
        assert ObxSettings._resolve_versions_for_export(svc, "d", "selected", [1, "3", "9"]) == ["3", "1"]

    def test_unknown_mode_rejected(self, svc):
        svc.list_versions_sorted.return_value = ["1"]
        with pytest.raises(ValidationError, match="Unknown export mode 'newest'"):
            ObxSettings._resolve_versions_for_export(svc, "d", "newest", None)


class TestExportRegistryObx:
    def _export(self, spec):
        return ObxSettings.export_registry_obx_result(spec, MagicMock(), MagicMock(), "me@x")

    def test_unconfigured_registry_rejected(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError, match="Registry not configured"):
            self._export({"domains": [{"name": "d"}]})

    def test_empty_selection_rejected(self, svc):
        with pytest.raises(ValidationError, match="No domains selected"):
            self._export({})

    def test_partial_export_collects_warnings(self, svc):
        listing = {"good": ["2", "1"], "empty": [], "broken": ["1"]}
        svc.list_versions_sorted.side_effect = lambda f: listing.get(f, [])

        def read(folder, ver):
            if folder == "broken" or ver == "1":
                return False, {}, "unreadable"
            return True, {"info": {"name": "Good"}}, ""

        svc.read_version.side_effect = read

        result = self._export(
            {
                "domains": [
                    {"name": "  "},
                    {"name": "empty"},
                    {"name": "broken", "mode": "all"},
                    {"name": "good", "mode": "all"},
                ]
            }
        )

        assert result["domain_count"] == 1
        assert result["version_count"] == 1
        assert result["filename"].startswith("ontobricks-") and result["filename"].endswith(".obx")
        assert result["warnings"] == [
            "Domain entry without a name was skipped",
            'No versions to export for domain "empty"',
            "broken v1: unreadable",
            "good v1: unreadable",
        ]
        exported = result["envelope"]["domains"][0]
        assert exported["name"] == "good"
        assert exported["info"] == {"name": "Good"}
        assert result["envelope"]["exported_by"] == "me@x"

    def test_nothing_readable_rejected(self, svc):
        svc.list_versions_sorted.return_value = ["1"]
        svc.read_version.return_value = (False, {}, "gone")
        with pytest.raises(ValidationError, match="Nothing to export"):
            self._export({"domains": [{"name": "d"}]})

    def test_unexpected_error_is_wrapped(self, svc):
        svc.list_versions_sorted.side_effect = RuntimeError("lakebase down")
        with pytest.raises(InfrastructureError, match="OBX export failed"):
            self._export({"domains": [{"name": "d"}]})


# ---------------------------------------------------------------------------
# Payload decoding
# ---------------------------------------------------------------------------


class TestDecodeObxPayload:
    def test_empty_rejected(self):
        with pytest.raises(ValidationError, match="Empty .obx file"):
            ObxSettings._decode_obx_payload(b"")

    def test_oversized_rejected(self, monkeypatch):
        monkeypatch.setattr(ObxSettings, "OBX_MAX_BYTES", 10)
        with pytest.raises(ValidationError, match="too large"):
            ObxSettings._decode_obx_payload(b"x" * 11)

    def test_non_utf8_rejected(self):
        with pytest.raises(ValidationError, match="not valid JSON"):
            ObxSettings._decode_obx_payload(b"\xff\xfe\x00")

    def test_facade_alias_matches_cap(self):
        from back.objects.domain.settings.SettingsService import SettingsService

        assert SettingsService.OBX_MAX_BYTES == ObxSettings.OBX_MAX_BYTES == 50 * 1024 * 1024


# ---------------------------------------------------------------------------
# Import naming
# ---------------------------------------------------------------------------


class TestImportNaming:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("customer claims-v2", "CustomerClaimsV2"),
            ("already CamelCase", "AlreadyCamelCase"),
            ("", "ImportedDomain"),
            ("2024 sales", "ImportedDomain"),
            ("---", "ImportedDomain"),
        ],
    )
    def test_camelcase_import_name(self, raw, expected):
        assert ObxSettings._camelcase_import_name(raw) == expected

    def test_camelcase_import_name_truncates_to_64(self):
        assert len(ObxSettings._camelcase_import_name("a" * 100)) == 64

    def test_suggest_increments_until_free(self, svc):
        taken = {"claimsimported", "claimsimported2"}
        svc.domain_exists.side_effect = lambda folder: folder in taken
        assert ObxSettings._suggest_import_name(svc, "claims") == "ClaimsImported3"

    def test_suggest_keeps_64_char_limit_with_suffix(self, svc):
        svc.domain_exists.side_effect = [True, False]
        suggestion = ObxSettings._suggest_import_name(svc, "A" * 70)
        assert len(suggestion) == 64
        assert suggestion.endswith("2")


class TestPrepareRenamedVersionDoc:
    def test_rewrites_root_and_nested_version_without_mutating_input(self):
        doc = {
            "info": {"name": "Old", "last_build": "2026-01-01"},
            "last_build": "2026-01-01",
            "ontology": {"base_uri": "https://old/#"},
            "triplestore": {"stats": {"triples": 9}},
            "versions": {
                "2": {
                    "info": {"name": "Old"},
                    "ontology": {"base_uri": "https://old/#", "base_uri_auto": False},
                    "triplestore": {"stats": {"triples": 9}},
                    "last_build": "x",
                }
            },
        }

        out = ObxSettings._prepare_renamed_version_doc(doc, "2", "NewName", "https://ex.org/NewName#")

        assert out["info"] == {"name": "NewName", "last_build": ""}
        assert out["last_build"] == ""
        assert out["ontology"] == {"base_uri": "https://ex.org/NewName#", "base_uri_auto": True}
        assert out["triplestore"]["stats"] == {}
        nested = out["versions"]["2"]
        assert nested["info"] == {"name": "NewName", "last_build": ""}
        assert nested["ontology"]["base_uri_auto"] is True
        assert nested["last_build"] == ""
        assert nested["triplestore"]["stats"] == {}
        assert doc["info"]["name"] == "Old"
        assert doc["versions"]["2"]["triplestore"]["stats"] == {"triples": 9}

    def test_root_info_is_created_when_missing(self):
        out = ObxSettings._prepare_renamed_version_doc({}, "1", "Fresh", "u")
        assert out["info"] == {"name": "Fresh", "last_build": ""}


# ---------------------------------------------------------------------------
# Preview / import
# ---------------------------------------------------------------------------


class TestPreviewObxImport:
    def test_unconfigured_registry_rejected(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            ObxSettings.preview_obx_import_result(b"{}", MagicMock(), MagicMock())

    def test_skips_nameless_entries_and_echoes_metadata(self, svc, raw_envelope):
        payload = raw_envelope(
            [{"name": " "}, {"name": "My Claims", "versions": {"1.10": {}, "1.9": {}, "draft": {}}}],
            ontobricks_version="0.9.0",
            exported_by="a@x",
        )

        result = ObxSettings.preview_obx_import_result(payload, MagicMock(), MagicMock())

        assert result["ontobricks_version"] == "0.9.0"
        assert result["exported_by"] == "a@x"
        assert len(result["domains"]) == 1
        preview = result["domains"][0]
        assert preview["name"] == "my_claims"
        assert preview["original_name"] == "My Claims"
        assert preview["display_name"] == "MyClaims"
        assert preview["incoming_versions"] == ["1.10", "1.9", "draft"]

    def test_unexpected_error_is_wrapped(self, svc, raw_envelope):
        svc.domain_exists.side_effect = RuntimeError("boom")
        with pytest.raises(InfrastructureError, match="Failed to read .obx file"):
            ObxSettings.preview_obx_import_result(
                raw_envelope([{"name": "d", "versions": {}}]), MagicMock(), MagicMock()
            )


class TestImportRegistryObx:
    def _import(self, payload, decisions):
        return ObxSettings.import_registry_obx_result(payload, decisions, MagicMock(), MagicMock())

    def test_unconfigured_registry_rejected(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            self._import(b"{}", [])

    def test_nameless_entry_reported(self, svc, raw_envelope):
        result = self._import(raw_envelope([{"name": ""}]), [])
        assert result["errors"] == ["Domain entry without a name was skipped"]
        assert result["message"] == "Imported 0 version(s) across 0 domain(s)"
        _ss.invalidate_registry_cache.assert_called_once()

    @pytest.mark.parametrize("new_name", ["", "lowercase", "Has Space", "A" * 65, "Bad-Dash"])
    def test_rename_requires_camelcase_alphanumeric(self, svc, raw_envelope, new_name):
        with pytest.raises(ValidationError, match="CamelCase alphanumeric"):
            self._import(
                raw_envelope([{"name": "claims", "versions": {"1": {}}}]),
                [{"name": "claims", "action": "rename", "new_name": new_name}],
            )
        svc.write_version.assert_not_called()

    def test_unknown_action_rejected(self, svc, raw_envelope):
        with pytest.raises(ValidationError, match="Unknown import action 'merge'"):
            self._import(
                raw_envelope([{"name": "claims", "versions": {"1": {}}}]),
                [{"name": "claims", "action": "MERGE"}],
            )

    def test_decision_matched_by_raw_name(self, svc, raw_envelope):
        result = self._import(
            raw_envelope([{"name": "My Claims", "versions": {"1": {"info": {}}}}]),
            [{"name": "My Claims", "action": "overwrite"}],
        )
        assert result["imported_versions"] == 1
        assert svc.write_version.call_args.args[0] == "my_claims"

    def test_bad_payloads_and_write_failures_are_reported(self, svc, raw_envelope):
        svc.domain_exists.return_value = True
        svc.list_versions_sorted.return_value = ["1"]
        svc.write_version.side_effect = lambda folder, ver, body: (
            (False, "disk full") if ver == "2" else (True, "ok")
        )

        result = self._import(
            raw_envelope([{"name": "claims", "versions": {"1": {}, "2": {}, "3": "oops"}}]),
            [{"name": "claims", "action": "overwrite"}],
        )

        assert result["imported_versions"] == 1
        assert result["overwritten_versions"] == 1
        assert result["errors"] == [
            "claims v2: disk full",
            "claims v3: payload is not an object, skipped",
        ]
        assert result["domains"] == [
            {
                "name": "claims",
                "original_name": "claims",
                "action": "overwrite",
                "versions_written": 1,
                "versions_overwritten": 1,
            }
        ]

    def test_unexpected_error_is_wrapped(self, svc, raw_envelope):
        svc.list_versions_sorted.side_effect = RuntimeError("x")
        svc.domain_exists.return_value = True
        with pytest.raises(InfrastructureError, match="OBX import failed"):
            self._import(
                raw_envelope([{"name": "claims", "versions": {"1": {}}}]),
                [{"name": "claims", "action": "overwrite"}],
            )
