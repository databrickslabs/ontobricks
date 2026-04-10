"""Tests for back.objects.mapping.Mapping domain class."""
import pytest
from unittest.mock import MagicMock
from back.objects.mapping import Mapping


def _mock_project(entities=None, relationships=None):
    project = MagicMock()
    project.assignment = {
        'entities': list(entities or []),
        'relationships': list(relationships or []),
    }
    project.get_entity_mappings.side_effect = lambda: project.assignment['entities']
    project.get_relationship_mappings.side_effect = lambda: project.assignment['relationships']
    return project


class TestBuildEntityMapping:
    def test_basic(self):
        data = {
            'ontology_class': 'http://t/A',
            'ontology_class_label': 'A',
            'sql_query': 'SELECT *',
            'id_column': 'id',
        }
        m = Mapping.build_entity_mapping(data)
        assert m['ontology_class'] == 'http://t/A'
        assert m['id_column'] == 'id'
        assert 'excluded' not in m

    def test_excluded_flag(self):
        data = {'ontology_class': 'A', 'excluded': True}
        m = Mapping.build_entity_mapping(data)
        assert m['excluded'] is True


class TestBuildRelationshipMapping:
    def test_basic(self):
        data = {
            'property': 'http://t/p',
            'source_class': 'A',
            'target_class': 'B',
        }
        m = Mapping.build_relationship_mapping(data)
        assert m['property'] == 'http://t/p'
        assert m['direction'] == 'forward'


class TestAddOrUpdateEntity:
    def test_add_new(self):
        project = _mock_project()
        was_update, mapping = Mapping(project).add_or_update_entity_mapping({
            'ontology_class': 'http://t/A',
            'id_column': 'id',
        })
        assert was_update is False
        assert len(project.assignment['entities']) == 1

    def test_update_existing(self):
        existing = [{'ontology_class': 'http://t/A', 'id_column': 'old'}]
        project = _mock_project(entities=existing)
        was_update, mapping = Mapping(project).add_or_update_entity_mapping({
            'ontology_class': 'http://t/A',
            'id_column': 'new',
        })
        assert was_update is True
        assert project.assignment['entities'][0]['id_column'] == 'new'


class TestDeleteEntity:
    def test_delete_existing(self):
        existing = [{'ontology_class': 'http://t/A'}]
        project = _mock_project(entities=existing)
        deleted = Mapping(project).delete_entity_mapping('http://t/A')
        assert deleted is True

    def test_delete_nonexistent(self):
        project = _mock_project()
        deleted = Mapping(project).delete_entity_mapping('http://t/Nope')
        assert deleted is False


class TestAddOrUpdateRelationship:
    def test_add_new(self):
        project = _mock_project()
        was_update, mapping = Mapping(project).add_or_update_relationship_mapping({
            'property': 'http://t/p',
        })
        assert was_update is False
        assert len(project.assignment['relationships']) == 1

    def test_update_existing(self):
        existing = [{'property': 'http://t/p', 'sql_query': 'old'}]
        project = _mock_project(relationships=existing)
        was_update, mapping = Mapping(project).add_or_update_relationship_mapping({
            'property': 'http://t/p',
            'sql_query': 'new',
        })
        assert was_update is True


class TestDeleteRelationship:
    def test_delete_existing(self):
        existing = [{'property': 'http://t/p'}]
        project = _mock_project(relationships=existing)
        deleted = Mapping(project).delete_relationship_mapping('http://t/p')
        assert deleted is True

    def test_delete_nonexistent(self):
        project = _mock_project()
        deleted = Mapping(project).delete_relationship_mapping('nope')
        assert deleted is False


class TestGetMappingStats:
    def test_stats(self):
        project = _mock_project(
            entities=[{}, {}],
            relationships=[{}],
        )
        stats = Mapping(project).get_mapping_stats()
        assert stats['entities'] == 2
        assert stats['relationships'] == 1


class TestSaveMappingConfig:
    def test_save(self):
        project = _mock_project()
        config = {
            'entities': [{'ontology_class': 'A'}],
            'relationships': [{'property': 'p'}],
        }
        stats = Mapping(project).save_mapping_config(config)
        assert stats['entities'] == 1

    def test_reset(self):
        project = _mock_project(entities=[{}])
        Mapping(project).reset_mapping()
        assert project.assignment['entities'] == []
