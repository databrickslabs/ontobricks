"""Tests for :mod:`back.objects.digitaltwin` domain module."""
import pytest
from back.objects.digitaltwin import augment_mappings_from_config, augment_relationships_from_config
from back.objects.digitaltwin import DigitalTwin


class TestNormalizeBaseUri:
    def test_trailing_slash(self):
        assert DigitalTwin._normalize_base_uri('http://test.org/ontology/') == 'http://test.org/ontology/'

    def test_trailing_hash(self):
        assert DigitalTwin._normalize_base_uri('http://test.org/ontology#') == 'http://test.org/ontology/'

    def test_no_trailing(self):
        assert DigitalTwin._normalize_base_uri('http://test.org/ontology') == 'http://test.org/ontology/'


class TestSafeClassLabel:
    def test_normal_label(self):
        assert DigitalTwin._safe_class_label('Customer', '') == 'Customer'

    def test_empty_label_falls_back_to_uri(self):
        assert DigitalTwin._safe_class_label('', 'http://test.org/ontology#MyClass') == 'MyClass'

    def test_empty_both(self):
        assert DigitalTwin._safe_class_label('', '') == 'Entity'

    def test_spaces_replaced(self):
        assert DigitalTwin._safe_class_label('My Class', '') == 'My_Class'


class TestAugmentMappingsFromConfig:
    def test_adds_entity_mapping(self):
        entity_mappings = {}
        mapping_config = {
            'entities': [{
                'ontology_class': 'http://test.org/ontology#Customer',
                'ontology_class_label': 'Customer',
                'sql_query': 'SELECT * FROM customers',
                'id_column': 'customer_id',
                'label_column': 'name',
                'attribute_mappings': {'firstName': 'first_name'},
            }],
        }
        result = augment_mappings_from_config(entity_mappings, mapping_config, 'http://test.org/ontology#')
        assert 'http://test.org/ontology#Customer' in result
        mapping = result['http://test.org/ontology#Customer']
        assert mapping['id_column'] == 'customer_id'
        assert mapping['sql_query'] == 'SELECT * FROM customers'

    def test_skips_excluded_entities(self):
        entity_mappings = {}
        mapping_config = {
            'entities': [{
                'ontology_class': 'http://test.org/ontology#Customer',
                'ontology_class_label': 'Customer',
                'sql_query': 'SELECT * FROM customers',
                'id_column': 'customer_id',
                'excluded': True,
                'attribute_mappings': {},
            }],
        }
        result = augment_mappings_from_config(entity_mappings, mapping_config, 'http://test.org/ontology#')
        assert 'http://test.org/ontology#Customer' not in result

    def test_empty_config(self):
        entity_mappings = {'existing': {'data': True}}
        result = augment_mappings_from_config(entity_mappings, None, 'http://test.org/')
        assert result == {'existing': {'data': True}}

    def test_attribute_mappings_added_as_predicates(self):
        entity_mappings = {}
        mapping_config = {
            'entities': [{
                'ontology_class': 'http://test.org/ontology#Customer',
                'ontology_class_label': 'Customer',
                'sql_query': 'SELECT * FROM customers',
                'id_column': 'cid',
                'label_column': '',
                'attribute_mappings': {'age': 'age_col'},
            }],
        }
        result = augment_mappings_from_config(entity_mappings, mapping_config, 'http://test.org/ontology/')
        mapping = result['http://test.org/ontology#Customer']
        pred_keys = list(mapping['predicates'].keys())
        assert any('age' in k for k in pred_keys)


class TestAugmentRelationshipsFromConfig:
    def test_adds_relationship(self):
        rel_mappings = []
        mapping_config = {
            'entities': [{
                'ontology_class': 'http://test.org/ontology#Customer',
                'ontology_class_label': 'Customer',
                'id_column': 'cid',
                'sql_query': 'SELECT * FROM cust',
                'attribute_mappings': {},
            }],
            'relationships': [{
                'property': 'http://test.org/ontology/hasOrder',
                'property_label': 'hasOrder',
                'sql_query': 'SELECT cid, oid FROM orders',
                'source_class': 'http://test.org/ontology#Customer',
                'source_class_label': 'Customer',
                'target_class': 'http://test.org/ontology#Order',
                'target_class_label': 'Order',
                'source_id_column': 'cid',
                'target_id_column': 'oid',
            }],
        }
        result = augment_relationships_from_config(rel_mappings, mapping_config, 'http://test.org/ontology/')
        assert len(result) == 1
        assert result[0]['predicate'] == 'http://test.org/ontology/hasOrder'

    def test_empty_config(self):
        result = augment_relationships_from_config([], None, 'http://test.org/')
        assert result == []
