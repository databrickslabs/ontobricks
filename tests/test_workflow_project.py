"""Workflow tests: create project -> set ontology -> set mapping -> export -> import -> verify."""
import json
import pytest
from back.objects.session.project_session import ProjectSession


class TestProjectWorkflow:
    def test_full_roundtrip(self, mock_session_mgr):
        ps = ProjectSession(mock_session_mgr)
        ps.info['name'] = 'Roundtrip Test'
        ps.info['description'] = 'Testing export/import'

        ps.ontology['base_uri'] = 'http://test.org/ontology#'
        ps.ontology['name'] = 'TestOntology'
        ps.ontology['classes'] = [
            {'uri': 'http://test.org/ontology#Customer', 'name': 'Customer', 'label': 'Customer'},
            {'uri': 'http://test.org/ontology#Order', 'name': 'Order', 'label': 'Order'},
        ]
        ps.ontology['properties'] = [
            {'uri': 'http://test.org/ontology#hasOrder', 'name': 'hasOrder', 'label': 'has Order',
             'type': 'ObjectProperty', 'domain': 'Customer', 'range': 'Order'},
        ]
        ps.ontology['constraints'] = [{'type': 'functional', 'property': 'hasOrder'}]

        ps.assignment['entities'] = [{
            'ontology_class': 'http://test.org/ontology#Customer',
            'ontology_class_label': 'Customer',
            'sql_query': 'SELECT * FROM customers',
            'id_column': 'customer_id',
        }]

        ps.save()

        export_data = ps.export_for_save()
        assert export_data['info']['name'] == 'Roundtrip Test'
        assert 'versions' in export_data
        version_key = list(export_data['versions'].keys())[0]
        version_data = export_data['versions'][version_key]
        assert len(version_data['ontology']['classes']) == 2
        assert len(version_data['assignment']['entities']) == 1

        export_json = json.dumps(export_data)

        ps2 = ProjectSession(mock_session_mgr)
        ps2.reset()

        imported = json.loads(export_json)
        ps2.import_from_file(imported)

        assert ps2.info['name'] == 'Roundtrip Test'
        assert len(ps2.get_classes()) == 2
        assert len(ps2.get_properties()) == 1
        assert len(ps2.get_entity_mappings()) == 1
        assert ps2.constraints[0]['type'] == 'functional'

    def test_export_excludes_secrets(self, mock_session_mgr):
        ps = ProjectSession(mock_session_mgr)
        ps._data['settings']['databricks']['token'] = 'super-secret'
        export = ps.export_for_save()
        exported_json = json.dumps(export)
        assert 'super-secret' not in exported_json

    def test_version_management(self, mock_session_mgr):
        ps = ProjectSession(mock_session_mgr)
        assert ps.current_version == '1'
        ps.current_version = '2'
        ps.save()
        ps2 = ProjectSession(mock_session_mgr)
        assert ps2.current_version == '2'
