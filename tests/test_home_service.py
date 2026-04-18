"""Tests for home service module."""

import asyncio
import concurrent.futures
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from back.objects.domain.HomeService import HomeService
from back.core.helpers import DatabricksHelpers
from back.objects.digitaltwin import DigitalTwin
from back.objects.domain import Domain as DomainOps


def _run_async(coro):
    """Run a coroutine in a fresh thread to avoid event-loop pollution from other plugins."""

    def _target():
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_target).result()


def _make_domain(
    classes=None,
    properties=None,
    entity_mappings=None,
    relationship_mappings=None,
    r2rml="",
    design_views=None,
    name="Test",
    assignment=None,
):
    domain = MagicMock()
    domain.get_classes.return_value = classes or []
    domain.get_properties.return_value = properties or []
    domain.get_entity_mappings.return_value = entity_mappings or []
    domain.get_relationship_mappings.return_value = relationship_mappings or []
    domain.get_r2rml.return_value = r2rml
    domain.design_layout = {"views": design_views or {}}
    domain.info = {"name": name}
    domain.ontology = {"name": "TestOntology"}
    domain.assignment = assignment or {
        "entities": entity_mappings or [],
        "relationships": relationship_mappings or [],
    }
    domain.triplestore = {"stats": {}}
    domain.last_build = None
    domain.ontology_changed = False
    domain.assignment_changed = False
    domain._data = {"domain": {"metadata": {}}}
    domain.last_update = None

    from back.objects.session.DomainSession import DomainSession

    domain.get_session_status = lambda: DomainSession.get_session_status(domain)
    return domain


class TestGetSessionStatus:
    def test_empty_domain(self):
        domain = _make_domain()
        status = HomeService.get_session_status(domain)
        assert status["success"] is True
        assert status["class_count"] == 0
        assert status["domain_name"] == "Test"

    def test_with_data(self):
        domain = _make_domain(
            classes=[{"uri": "u1", "name": "A"}],
            properties=[{"uri": "p1"}],
            entity_mappings=[{}],
            r2rml="some content",
        )
        status = HomeService.get_session_status(domain)
        assert status["class_count"] == 1
        assert status["property_count"] == 1
        assert status["has_r2rml"] is True


class TestValidateOntology:
    def test_no_classes(self):
        domain = _make_domain()
        result = HomeService.validate_ontology(domain)
        assert result["valid"] is False
        assert "No classes defined" in result["errors"]

    def test_valid(self):
        domain = _make_domain(classes=[{"uri": "http://test/A", "name": "A"}])
        result = HomeService.validate_ontology(domain)
        assert result["valid"] is True

    def test_class_missing_uri(self):
        domain = _make_domain(classes=[{"label": "NoUri"}])
        result = HomeService.validate_ontology(domain)
        assert result["valid"] is False


class TestValidateStatus:
    def test_empty_domain(self):
        domain = _make_domain()
        result = HomeService.validate_status(domain)
        assert result["ontology_valid"] is False

    def test_valid_domain(self):
        classes = [{"uri": "http://test/A", "name": "A"}]
        mappings = [{"ontology_class": "http://test/A", "attribute_mappings": {}}]
        domain = _make_domain(
            classes=classes,
            entity_mappings=mappings,
            assignment={"entities": mappings, "relationships": []},
        )
        result = HomeService.validate_status(domain)
        assert result["ontology_valid"] is True


class TestGetDetailedValidation:
    def test_returns_all_sections(self):
        domain = _make_domain(classes=[{"uri": "http://test/A", "name": "A"}])
        settings = MagicMock()
        ts = {
            "success": True,
            "has_data": False,
            "count": 0,
            "view_table": "c.s.t",
            "graph_name": "g",
        }
        dt = {
            "view_exists": None,
            "local_lbug_exists": False,
            "registry_lbug_exists": None,
            "view_table": "c.s.t",
            "graph_name": "g",
            "local_lbug_path": "",
            "registry_lbug_path": "",
            "last_update": None,
            "last_built": None,
            "snapshot_table": "",
            "snapshot_exists": None,
        }

        async def _passthrough(func, *a, **kw):
            return func(*a, **kw)

        with (
            patch.object(
                DatabricksHelpers,
                "run_blocking",
                side_effect=_passthrough,
            ),
            patch.object(
                DigitalTwin,
                "sync_last_build_from_schedule",
                MagicMock(),
            ),
            patch.object(
                DigitalTwin,
                "get_or_fetch_graph_status",
                AsyncMock(return_value=ts),
            ),
            patch.object(
                DigitalTwin,
                "get_or_fetch_dt_existence",
                AsyncMock(return_value=dt),
            ),
            patch.object(
                DomainOps,
                "count_documents_in_volume",
                MagicMock(return_value=0),
            ),
        ):
            result = _run_async(HomeService.get_detailed_validation(domain, settings))
        assert "ontology_valid" in result
        assert "mapping_valid" in result
        assert "ontology" in result
        assert "mapping" in result
        assert "design" in result
