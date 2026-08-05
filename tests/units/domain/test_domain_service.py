"""Tests for back.objects.domain.Domain."""

import pytest
from unittest.mock import MagicMock
from back.objects.domain import Domain
from back.core.errors import ConflictError, ValidationError


def _mock_domain(
    name="Test",
    classes=None,
    properties=None,
    entity_mappings=None,
    relationship_mappings=None,
):
    domain = MagicMock()
    domain.info = {
        "name": name,
        "description": "Desc",
        "author": "Author",
        "llm_endpoint": "",
    }
    domain.triplestore = {"stats": {}}
    domain.current_version = "1"
    domain.ontology = {"base_uri": "http://test.org#", "name": "Test"}
    domain.uc_location = {"catalog": "", "schema": "", "volume": ""}
    domain.registry = {
        "catalog": "cat",
        "schema": "sch",
        "volume": "OntoBricksRegistry",
    }
    domain.domain_folder = "test_domain"
    safe = name.lower().replace(" ", "_")
    domain.delta = {
        "catalog": "cat",
        "schema": "sch",
        "table_name": f"triplestore_{safe}_V1",
    }
    domain.snapshot_table = f"cat.sch._ob_snapshot_{safe}_v1"
    domain.design_layout = {"views": {}, "map": {}}
    domain.save = MagicMock()
    domain.get_classes.return_value = classes or []
    domain.get_properties.return_value = properties or []
    domain.get_entity_mappings.return_value = entity_mappings or []
    domain.get_relationship_mappings.return_value = relationship_mappings or []
    domain._data = {
        "domain": {"info": domain.info, "triplestore": domain.triplestore},
        "databricks": {"host": "h", "token": "secret"},
        "generated": {"owl": "x" * 600, "sql": ""},
        "assignment": {"r2rml_output": ""},
    }
    return domain


class TestGetDomainInfo:
    def test_basic(self):
        domain = _mock_domain()
        result = Domain(domain).get_domain_info()
        assert result["success"] is True
        assert result["info"]["name"] == "Test"
        assert "stats" in result

    def test_view_table(self):
        domain = _mock_domain()
        result = Domain(domain).get_domain_info()
        assert result["info"]["view_table"] == "cat.sch.triplestore_test_V1"

    def test_graph_name(self):
        domain = _mock_domain()
        result = Domain(domain).get_domain_info()
        assert result["info"]["graph_name"] == "Test_V1"


class TestGetDomainStats:
    def test_stats(self):
        domain = _mock_domain(
            classes=[{"name": "A"}],
            entity_mappings=[{}],
        )
        stats = Domain(domain).get_domain_stats()
        assert stats["entities"] == 1


class TestSaveDomainInfo:
    def test_save_name(self):
        domain = _mock_domain()
        result = Domain(domain).save_domain_info({"name": "New Name"})
        assert result["name"] == "New Name"
        domain.save.assert_called_once()

    def test_save_base_uri(self):
        domain = _mock_domain()
        Domain(domain).save_domain_info({"base_uri": "http://new.org#"})
        assert domain.ontology["base_uri"] == "http://new.org#"

    def test_auto_base_uri_generated_from_name(self):
        """Auto mode (no custom URI) generates {default}/{Name}# — never empty."""
        domain = _mock_domain()
        result = Domain(domain).save_domain_info({"name": "AcmeSales"})
        assert result["base_uri"].endswith("/AcmeSales#")
        assert result["base_uri"] != "Unknown"
        assert result["base_uri_auto"] is True

    def test_custom_base_uri_preserved_over_auto(self):
        """A custom URI with base_uri_auto=False is kept verbatim."""
        domain = _mock_domain()
        Domain(domain).save_domain_info(
            {"name": "AcmeSales", "base_uri": "http://acme.example/#", "base_uri_auto": False}
        )
        assert domain.ontology["base_uri"] == "http://acme.example/#"
        assert domain.ontology["base_uri_auto"] is False

    def test_empty_custom_base_uri_falls_back_to_generated(self):
        """A blank custom URI is never stored — it regenerates from the name."""
        domain = _mock_domain()
        Domain(domain).save_domain_info(
            {"name": "AcmeSales", "base_uri": "", "base_uri_auto": False}
        )
        assert domain.ontology["base_uri"].endswith("/AcmeSales#")

    def test_save_review_quorum(self):
        domain = _mock_domain()
        result = Domain(domain).save_domain_info({"review_quorum": 3})
        assert domain.info["review_quorum"] == 3
        assert result["review_quorum"] == 3

    def test_review_quorum_clamped_to_minimum_one(self):
        domain = _mock_domain()
        Domain(domain).save_domain_info({"review_quorum": 0})
        assert domain.info["review_quorum"] == 1

    def test_review_quorum_invalid_falls_back_to_one(self):
        domain = _mock_domain()
        Domain(domain).save_domain_info({"review_quorum": "abc"})
        assert domain.info["review_quorum"] == 1

    def test_review_quorum_defaults_when_absent(self):
        domain = _mock_domain()
        result = Domain(domain).get_domain_info()
        assert result["info"]["review_quorum"] == 1


class TestGetDomainTemplateData:
    def test_returns_fields(self):
        domain = _mock_domain(classes=[{"name": "A"}])
        data = Domain(domain).get_domain_template_data()
        assert data["name"] == "Test"
        assert data["has_ontology"] is True
        assert data["has_mapping"] is False


class TestAuditTrail:
    def _svc(self, events, runs, configured=True, versions=("2", "1")):
        svc = MagicMock()
        svc.cfg.is_configured = configured
        svc.list_review_events.return_value = events
        svc.load_build_runs.return_value = runs
        svc.list_versions_sorted.return_value = list(versions)
        return svc

    def test_merges_review_events_and_build_runs(self):
        domain = _mock_domain()
        domain.uc_domain_folder = "test_domain"
        events = [{"action": "submitted", "comment": "go", "created_at": "t1"}]
        runs = [{"id": 1, "status": "success", "started_at": "t2"}]
        result = Domain(domain).audit_trail_result(
            self._svc(events, runs), limit=10
        )
        assert result["success"] is True
        assert result["domain_folder"] == "test_domain"
        assert result["events"] == events
        assert result["runs"] == runs

    def test_returns_versions_for_dropdown(self):
        domain = _mock_domain()
        domain.uc_domain_folder = "test_domain"
        domain.current_version = "2"
        result = Domain(domain).audit_trail_result(
            self._svc([], [], versions=("2", "1"))
        )
        assert result["versions"] == ["2", "1"]
        assert result["current_version"] == "2"

    def test_passes_folder_and_limit_to_store(self):
        domain = _mock_domain()
        domain.uc_domain_folder = "test_domain"
        svc = self._svc([], [])
        Domain(domain).audit_trail_result(svc, limit=42)
        svc.list_review_events.assert_called_once_with("test_domain")
        svc.load_build_runs.assert_called_once_with("test_domain", limit=42)

    def test_requires_configured_registry(self):
        domain = _mock_domain()
        domain.uc_domain_folder = "test_domain"
        with pytest.raises(ValidationError):
            Domain(domain).audit_trail_result(self._svc([], [], configured=False))

    def test_requires_saved_domain(self):
        domain = _mock_domain()
        domain.uc_domain_folder = ""
        with pytest.raises(ValidationError):
            Domain(domain).audit_trail_result(self._svc([], []))


class TestGetVersionStatusAuthoritative:
    """The lifecycle status is sourced from the registry, not the stale
    in-session snapshot, so an out-of-band publish/reopen is reflected
    without requiring a reload."""

    def _svc(self, live_status, versions=("1",)):
        svc = MagicMock()
        svc.cfg.is_configured = True
        svc.list_versions_sorted.return_value = list(versions)
        svc.find_mcp_version.return_value = (None, {})
        svc.get_version_status.return_value = live_status
        return svc

    def test_self_heals_session_from_registry(self):
        domain = _mock_domain()
        domain.uc_domain_folder = "test_domain"
        domain.info["status"] = "DRAFT"  # stale snapshot
        d = Domain(domain)
        d.build_registry_service = MagicMock(
            return_value=self._svc("PUBLISHED")
        )
        result = d.get_version_status(refresh=True)
        assert result["status"] == "PUBLISHED"
        assert domain.info["status"] == "PUBLISHED"
        domain.save.assert_called()

    def test_keeps_session_status_when_registry_silent(self):
        domain = _mock_domain()
        domain.uc_domain_folder = "test_domain"
        domain.info["status"] = "IN-REVIEW"
        d = Domain(domain)
        d.build_registry_service = MagicMock(return_value=self._svc(None))
        result = d.get_version_status(refresh=True)
        assert result["status"] == "IN-REVIEW"


class TestSaveDomainToUcLifecycleLock:
    """``save_domain_to_uc`` must refuse to overwrite a version that is no
    longer a DRAFT — the server-side mirror of the UI read-only gate."""

    def _svc(self, live_status):
        svc = MagicMock()
        svc.cfg.is_configured = True
        svc.get_version_status.return_value = live_status
        svc.write_version.return_value = (True, "")
        return svc

    def test_rejects_overwrite_of_published(self):
        domain = _mock_domain()
        domain.export_for_save = MagicMock(return_value={"info": {}})
        with pytest.raises(ConflictError):
            Domain(domain).save_domain_to_uc(self._svc("PUBLISHED"))

    def test_rejects_overwrite_of_in_review(self):
        domain = _mock_domain()
        domain.export_for_save = MagicMock(return_value={"info": {}})
        with pytest.raises(ConflictError):
            Domain(domain).save_domain_to_uc(self._svc("IN-REVIEW"))

    def test_allows_overwrite_of_draft(self):
        domain = _mock_domain()
        domain.export_for_save = MagicMock(return_value={"info": {}})
        domain.clear_change_flags = MagicMock()
        domain.settings = {
            "registry": {"catalog": "c", "schema": "s", "volume": "v"}
        }
        svc = self._svc("DRAFT")
        result = Domain(domain).save_domain_to_uc(svc)
        assert result["success"] is True
        svc.write_version.assert_called_once()
