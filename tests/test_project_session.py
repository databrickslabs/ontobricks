"""Tests for ProjectSession."""
import pytest
from unittest.mock import MagicMock, patch

from back.objects.session.project_session import ProjectSession, get_empty_project


class TestGetEmptyProject:
    def test_has_required_keys(self):
        data = get_empty_project()
        assert 'project' in data
        assert 'ontology' in data
        assert 'assignment' in data
        assert 'design_layout' in data
        assert 'settings' in data
        assert 'databricks' in data['settings']
        assert 'registry' in data['settings']

    def test_default_name(self):
        data = get_empty_project()
        assert data['project']['info']['name'] == 'NewProject'

    def test_default_registry(self):
        data = get_empty_project()
        reg = data['settings']['registry']
        assert reg['catalog'] == ''
        assert reg['volume'] == 'OntoBricksRegistry'

    def test_no_persisted_reasoning_metadata_under_project(self):
        data = get_empty_project()
        assert 'reasoning' not in data
        assert 'reasoning' not in data['ontology']
        assert data['project']['metadata'] == {}

    def test_migrates_root_metadata_and_reasoning(self, mock_session_mgr):
        mock_session_mgr.set('project_data', {
            'project': get_empty_project()['project'],
            'ontology': {**get_empty_project()['ontology'], 'classes': []},
            'assignment': {'entities': [], 'relationships': []},
            'design_layout': get_empty_project()['design_layout'],
            'settings': get_empty_project()['settings'],
            'metadata': {'catalog': 'c', 'schema': 's', 'tables': [{'name': 't'}]},
            'reasoning': {'last_run': 'x', 'inferred_count': 3, 'violations_count': 0,
                          'inferred_triples': [], 'violations': []},
        })
        ps = ProjectSession(mock_session_mgr)
        assert ps.catalog_metadata.get('catalog') == 'c'
        assert 'reasoning' not in ps._data
        assert 'reasoning' not in ps.ontology
        assert 'metadata' not in ps._data


class TestUcProjectPath:
    def test_uc_project_path_prefers_registry_volume_path(self, mock_session_mgr):
        data = get_empty_project()
        data['settings']['registry'] = {
            "catalog": "stale_cat",
            "schema": "stale_sch",
            "volume": "stale_vol",
        }
        data['project']['project_folder'] = 'export_me'
        mock_session_mgr.set('project_data', data)
        ps = ProjectSession(mock_session_mgr)

        fake_settings = MagicMock()
        fake_settings.registry_volume_path = "/Volumes/benoit_cayla/ontobricks_deployed/registry"
        fake_settings.registry_catalog = ""
        fake_settings.registry_schema = ""
        fake_settings.registry_volume = ""

        with patch("shared.config.settings.get_settings", return_value=fake_settings):
            assert (
                ps.uc_project_path
                == "/Volumes/benoit_cayla/ontobricks_deployed/registry/projects/export_me"
            )


class TestProjectSession:
    def test_init_empty(self, mock_session_mgr):
        ps = ProjectSession(mock_session_mgr)
        assert ps.info['name'] == 'NewProject'

    def test_info_property(self, project_session):
        assert 'name' in project_session.info

    def test_set_and_get_info(self, project_session):
        project_session.info['name'] = 'Test'
        assert project_session.info['name'] == 'Test'

    def test_current_version(self, project_session):
        assert project_session.current_version == '1'
        project_session.current_version = '2'
        assert project_session.current_version == '2'

    def test_ontology(self, project_session):
        assert isinstance(project_session.ontology, dict)
        assert 'classes' in project_session.ontology

    def test_get_classes_empty(self, project_session):
        assert project_session.get_classes() == []

    def test_get_properties_empty(self, project_session):
        assert project_session.get_properties() == []

    def test_add_class(self, project_session):
        project_session.ontology['classes'].append({'name': 'Foo', 'uri': 'http://t/Foo'})
        assert len(project_session.get_classes()) == 1

    def test_assignment(self, project_session):
        assert isinstance(project_session.assignment, dict)
        assert project_session.get_entity_mappings() == []

    def test_generated_lazy_init(self, project_session):
        gen = project_session.generated
        assert gen == {'owl': '', 'sql': '', 'r2rml': ''}

    def test_get_set_r2rml(self, project_session):
        assert project_session.get_r2rml() == ''
        project_session.set_r2rml('some r2rml')
        assert project_session.get_r2rml() == 'some r2rml'

    def test_clear_generated_content(self, project_session):
        project_session.set_r2rml('content')
        project_session.generated['owl'] = 'owl content'
        project_session.clear_generated_content()
        assert project_session.get_r2rml() == ''
        assert project_session.generated['owl'] == ''


class TestSaveAndReset:
    def test_save(self, mock_session_mgr, project_session):
        project_session.info['name'] = 'Saved'
        project_session.save()
        data = mock_session_mgr.get('project_data')
        assert data is not None
        assert data['project']['info']['name'] == 'Saved'

    def test_save_excludes_generated(self, mock_session_mgr, project_session):
        project_session.generated['owl'] = 'test owl'
        project_session.save()
        data = mock_session_mgr.get('project_data')
        assert 'generated' not in data

    def test_reset(self, mock_session_mgr, project_session):
        project_session.info['name'] = 'Before Reset'
        project_session.save()
        project_session.reset()
        assert project_session.info['name'] == 'NewProject'


class TestExportImport:
    def test_export_for_save(self, project_session):
        project_session.info['name'] = 'Export Test'
        project_session.ontology['base_uri'] = 'http://test.org#'
        project_session.ontology['classes'] = [{'name': 'A'}]
        export = project_session.export_for_save()
        assert export['info']['name'] == 'Export Test'
        assert 'versions' in export

    def test_import_from_file(self, project_session):
        project_data = {
            'info': {'name': 'Imported', 'description': 'Test import'},
            'versions': {
                '1': {
                    'ontology': {
                        'name': 'ImpOntology',
                        'base_uri': 'http://imp.org#',
                        'classes': [{'name': 'Imp'}],
                        'properties': [],
                        'constraints': [],
                        'swrl_rules': [],
                        'axioms': [],
                        'expressions': [],
                    },
                    'assignment': {'entities': [], 'relationships': []},
                    'design_layout': {'views': {}, 'map': {}},
                }
            }
        }
        project_session.import_from_file(project_data)
        assert project_session.info['name'] == 'Imported'
        assert len(project_session.get_classes()) == 1


class TestLegacyMigration:
    def test_migrates_flat_constraints(self, mock_session_mgr):
        mock_session_mgr.set('project_data', {
            'ontology': {
                'classes': [{'name': 'A'}],
                'properties': [],
            },
            'constraints': [{'type': 'functional', 'property': 'p'}],
            'swrl_rules': [],
            'axioms': [],
        })
        ps = ProjectSession(mock_session_mgr)
        assert len(ps.constraints) == 1
        assert ps.constraints[0]['type'] == 'functional'

    def test_migrates_mapping_key(self, mock_session_mgr):
        mock_session_mgr.set('project_data', {
            'ontology': {'classes': [], 'properties': []},
            'mapping': {
                'data_source_mappings': [{'ontology_class': 'A'}],
                'relationship_mappings': [],
            },
        })
        ps = ProjectSession(mock_session_mgr)
        assert len(ps.get_entity_mappings()) == 1
