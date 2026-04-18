"""Tests for back.objects.mapping.Mapping domain class."""

import pytest
from unittest.mock import MagicMock
from back.objects.mapping import Mapping


def _mock_domain(entities=None, relationships=None):
    domain = MagicMock()
    domain.assignment = {
        "entities": list(entities or []),
        "relationships": list(relationships or []),
    }
    domain.get_entity_mappings.side_effect = lambda: domain.assignment["entities"]
    domain.get_relationship_mappings.side_effect = lambda: domain.assignment[
        "relationships"
    ]
    return domain


class TestBuildEntityMapping:
    def test_basic(self):
        data = {
            "ontology_class": "http://t/A",
            "ontology_class_label": "A",
            "sql_query": "SELECT *",
            "id_column": "id",
        }
        m = Mapping.build_entity_mapping(data)
        assert m["ontology_class"] == "http://t/A"
        assert m["id_column"] == "id"
        assert "excluded" not in m

    def test_excluded_flag(self):
        data = {"ontology_class": "A", "excluded": True}
        m = Mapping.build_entity_mapping(data)
        assert m["excluded"] is True


class TestBuildRelationshipMapping:
    def test_basic(self):
        data = {
            "property": "http://t/p",
            "source_class": "A",
            "target_class": "B",
        }
        m = Mapping.build_relationship_mapping(data)
        assert m["property"] == "http://t/p"
        assert m["direction"] == "forward"


class TestAddOrUpdateEntity:
    def test_add_new(self):
        domain = _mock_domain()
        was_update, mapping = Mapping(domain).add_or_update_entity_mapping(
            {
                "ontology_class": "http://t/A",
                "id_column": "id",
            }
        )
        assert was_update is False
        assert len(domain.assignment["entities"]) == 1

    def test_update_existing(self):
        existing = [{"ontology_class": "http://t/A", "id_column": "old"}]
        domain = _mock_domain(entities=existing)
        was_update, mapping = Mapping(domain).add_or_update_entity_mapping(
            {
                "ontology_class": "http://t/A",
                "id_column": "new",
            }
        )
        assert was_update is True
        assert domain.assignment["entities"][0]["id_column"] == "new"


class TestDeleteEntity:
    def test_delete_existing(self):
        existing = [{"ontology_class": "http://t/A"}]
        domain = _mock_domain(entities=existing)
        deleted = Mapping(domain).delete_entity_mapping("http://t/A")
        assert deleted is True

    def test_delete_nonexistent(self):
        domain = _mock_domain()
        deleted = Mapping(domain).delete_entity_mapping("http://t/Nope")
        assert deleted is False


class TestAddOrUpdateRelationship:
    def test_add_new(self):
        domain = _mock_domain()
        was_update, mapping = Mapping(domain).add_or_update_relationship_mapping(
            {
                "property": "http://t/p",
            }
        )
        assert was_update is False
        assert len(domain.assignment["relationships"]) == 1

    def test_update_existing(self):
        existing = [{"property": "http://t/p", "sql_query": "old"}]
        domain = _mock_domain(relationships=existing)
        was_update, mapping = Mapping(domain).add_or_update_relationship_mapping(
            {
                "property": "http://t/p",
                "sql_query": "new",
            }
        )
        assert was_update is True


class TestDeleteRelationship:
    def test_delete_existing(self):
        existing = [{"property": "http://t/p"}]
        domain = _mock_domain(relationships=existing)
        deleted = Mapping(domain).delete_relationship_mapping("http://t/p")
        assert deleted is True

    def test_delete_nonexistent(self):
        domain = _mock_domain()
        deleted = Mapping(domain).delete_relationship_mapping("nope")
        assert deleted is False


class TestGetMappingStats:
    def test_stats(self):
        domain = _mock_domain(
            entities=[{}, {}],
            relationships=[{}],
        )
        stats = Mapping(domain).get_mapping_stats()
        assert stats["entities"] == 2
        assert stats["relationships"] == 1


class TestSaveMappingConfig:
    def test_save(self):
        domain = _mock_domain()
        config = {
            "entities": [{"ontology_class": "A"}],
            "relationships": [{"property": "p"}],
        }
        stats = Mapping(domain).save_mapping_config(config)
        assert stats["entities"] == 1

    def test_reset(self):
        domain = _mock_domain(entities=[{}])
        Mapping(domain).reset_mapping()
        assert domain.assignment["entities"] == []
