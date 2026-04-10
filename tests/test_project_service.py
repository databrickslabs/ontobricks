"""Tests for back.objects.project.Project domain class."""
import pytest
from unittest.mock import MagicMock
from back.objects.project import Project


def _mock_project(name='Test', classes=None, properties=None,
                  entity_mappings=None, relationship_mappings=None):
    project = MagicMock()
    project.info = {
        'name': name, 'description': 'Desc', 'author': 'Author',
        'llm_endpoint': '',
    }
    project.triplestore = {'stats': {}}
    project.current_version = '1'
    project.ontology = {'base_uri': 'http://test.org#', 'name': 'Test'}
    project.uc_location = {'catalog': '', 'schema': '', 'volume': ''}
    project.registry = {'catalog': 'cat', 'schema': 'sch', 'volume': 'OntoBricksRegistry'}
    project.project_folder = 'test_project'
    project.delta = {'catalog': 'cat', 'schema': 'sch', 'table_name': 'triples'}
    project.design_layout = {'views': {}, 'map': {}}
    project.get_classes.return_value = classes or []
    project.get_properties.return_value = properties or []
    project.get_entity_mappings.return_value = entity_mappings or []
    project.get_relationship_mappings.return_value = relationship_mappings or []
    project._data = {
        'project': {'info': project.info, 'triplestore': project.triplestore},
        'databricks': {'host': 'h', 'token': 'secret'},
        'generated': {'owl': 'x' * 600, 'sql': ''},
        'assignment': {'r2rml_output': ''},
    }
    return project


class TestGetProjectInfo:
    def test_basic(self):
        project = _mock_project()
        result = Project(project).get_project_info()
        assert result['success'] is True
        assert result['info']['name'] == 'Test'
        assert 'stats' in result

    def test_view_table(self):
        project = _mock_project()
        result = Project(project).get_project_info()
        assert result['info']['view_table'] == 'cat.sch.triplestore_test_V1'

    def test_graph_name(self):
        project = _mock_project()
        result = Project(project).get_project_info()
        assert result['info']['graph_name'] == 'Test_V1'


class TestGetProjectStats:
    def test_stats(self):
        project = _mock_project(
            classes=[{'name': 'A'}],
            entity_mappings=[{}],
        )
        stats = Project(project).get_project_stats()
        assert stats['entities'] == 1


class TestSaveProjectInfo:
    def test_save_name(self):
        project = _mock_project()
        result = Project(project).save_project_info({'name': 'New Name'})
        assert result['name'] == 'New Name'
        project.save.assert_called_once()

    def test_save_base_uri(self):
        project = _mock_project()
        Project(project).save_project_info({'base_uri': 'http://new.org#'})
        assert project.ontology['base_uri'] == 'http://new.org#'


class TestGetProjectTemplateData:
    def test_returns_fields(self):
        project = _mock_project(classes=[{'name': 'A'}])
        data = Project(project).get_project_template_data()
        assert data['name'] == 'Test'
        assert data['has_ontology'] is True
        assert data['has_mapping'] is False
