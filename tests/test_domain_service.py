"""Tests for back.objects.domain.Domain."""

import pytest
from unittest.mock import MagicMock
from back.objects.domain import Domain


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
    domain.delta = {"catalog": "cat", "schema": "sch", "table_name": "triples"}
    domain.design_layout = {"views": {}, "map": {}}
    domain.ladybug = {}
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


class TestGetDomainTemplateData:
    def test_returns_fields(self):
        domain = _mock_domain(classes=[{"name": "A"}])
        data = Domain(domain).get_domain_template_data()
        assert data["name"] == "Test"
        assert data["has_ontology"] is True
        assert data["has_mapping"] is False
