"""Workflow tests: create domain -> set ontology -> set mapping -> export -> import -> verify."""
import json
import pytest
from back.objects.session.DomainSession import DomainSession


class TestDomainWorkflow:
    def test_full_roundtrip(self, mock_session_mgr):
        ds = DomainSession(mock_session_mgr)
        ds.info['name'] = 'Roundtrip Test'
        ds.info['description'] = 'Testing export/import'

        ds.ontology['base_uri'] = 'http://test.org/ontology#'
        ds.ontology['name'] = 'TestOntology'
        ds.ontology['classes'] = [
            {'uri': 'http://test.org/ontology#Customer', 'name': 'Customer', 'label': 'Customer'},
            {'uri': 'http://test.org/ontology#Order', 'name': 'Order', 'label': 'Order'},
        ]
        ds.ontology['properties'] = [
            {'uri': 'http://test.org/ontology#hasOrder', 'name': 'hasOrder', 'label': 'has Order',
             'type': 'ObjectProperty', 'domain': 'Customer', 'range': 'Order'},
        ]
        ds.ontology['constraints'] = [{'type': 'functional', 'property': 'hasOrder'}]

        ds.assignment['entities'] = [{
            'ontology_class': 'http://test.org/ontology#Customer',
            'ontology_class_label': 'Customer',
            'sql_query': 'SELECT * FROM customers',
            'id_column': 'customer_id',
        }]

        ds.save()

        export_data = ds.export_for_save()
        assert export_data['info']['name'] == 'Roundtrip Test'
        assert 'versions' in export_data
        version_key = list(export_data['versions'].keys())[0]
        version_data = export_data['versions'][version_key]
        assert len(version_data['ontology']['classes']) == 2
        assert len(version_data['assignment']['entities']) == 1

        export_json = json.dumps(export_data)

        ds2 = DomainSession(mock_session_mgr)
        ds2.reset()

        imported = json.loads(export_json)
        ds2.import_from_file(imported)

        assert ds2.info['name'] == 'Roundtrip Test'
        assert len(ds2.get_classes()) == 2
        assert len(ds2.get_properties()) == 1
        assert len(ds2.get_entity_mappings()) == 1
        assert ds2.constraints[0]['type'] == 'functional'

    def test_export_excludes_secrets(self, mock_session_mgr):
        ds = DomainSession(mock_session_mgr)
        ds._data['settings']['databricks']['token'] = 'super-secret'
        export = ds.export_for_save()
        exported_json = json.dumps(export)
        assert 'super-secret' not in exported_json

    def test_version_management(self, mock_session_mgr):
        ds = DomainSession(mock_session_mgr)
        assert ds.current_version == '1'
        ds.current_version = '2'
        ds.save()
        ds2 = DomainSession(mock_session_mgr)
        assert ds2.current_version == '2'
