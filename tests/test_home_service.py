"""Tests for home service module."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from back.services.home import (
    get_session_status, validate_ontology,
    get_detailed_validation, validate_status,
)
from back.objects.digitaltwin import DigitalTwin
from back.objects.domain import Domain as DomainOps


def _make_domain(classes=None, properties=None, entity_mappings=None,
                 relationship_mappings=None, r2rml='', design_views=None,
                 name='Test', assignment=None):
    domain = MagicMock()
    domain.get_classes.return_value = classes or []
    domain.get_properties.return_value = properties or []
    domain.get_entity_mappings.return_value = entity_mappings or []
    domain.get_relationship_mappings.return_value = relationship_mappings or []
    domain.get_r2rml.return_value = r2rml
    domain.design_layout = {'views': design_views or {}}
    domain.info = {'name': name}
    domain.ontology = {'name': 'TestOntology'}
    domain.assignment = assignment or {'entities': entity_mappings or [], 'relationships': relationship_mappings or []}
    domain.triplestore = {'stats': {}}
    domain.last_build = None
    domain.ontology_changed = False
    domain.assignment_changed = False
    domain._data = {'domain': {'metadata': {}}}
    domain.last_update = None

    from back.objects.session.domain_session import DomainSession
    domain.get_session_status = lambda: DomainSession.get_session_status(domain)
    return domain


class TestGetSessionStatus:
    def test_empty_domain(self):
        domain = _make_domain()
        status = get_session_status(domain)
        assert status['success'] is True
        assert status['class_count'] == 0
        assert status['domain_name'] == 'Test'

    def test_with_data(self):
        domain = _make_domain(
            classes=[{'uri': 'u1', 'name': 'A'}],
            properties=[{'uri': 'p1'}],
            entity_mappings=[{}],
            r2rml='some content',
        )
        status = get_session_status(domain)
        assert status['class_count'] == 1
        assert status['property_count'] == 1
        assert status['has_r2rml'] is True


class TestValidateOntology:
    def test_no_classes(self):
        domain = _make_domain()
        result = validate_ontology(domain)
        assert result['valid'] is False
        assert 'No classes defined' in result['errors']

    def test_valid(self):
        domain = _make_domain(classes=[{'uri': 'http://test/A', 'name': 'A'}])
        result = validate_ontology(domain)
        assert result['valid'] is True

    def test_class_missing_uri(self):
        domain = _make_domain(classes=[{'label': 'NoUri'}])
        result = validate_ontology(domain)
        assert result['valid'] is False


class TestValidateStatus:
    def test_empty_domain(self):
        domain = _make_domain()
        result = validate_status(domain)
        assert result['ontology_valid'] is False

    def test_valid_domain(self):
        classes = [{'uri': 'http://test/A', 'name': 'A'}]
        mappings = [{'ontology_class': 'http://test/A', 'attribute_mappings': {}}]
        domain = _make_domain(
            classes=classes,
            entity_mappings=mappings,
            assignment={'entities': mappings, 'relationships': []}
        )
        result = validate_status(domain)
        assert result['ontology_valid'] is True


class TestGetDetailedValidation:
    @pytest.mark.asyncio
    async def test_returns_all_sections(self):
        domain = _make_domain(classes=[{'uri': 'http://test/A', 'name': 'A'}])
        settings = MagicMock()
        ts = {
            'success': True,
            'has_data': False,
            'count': 0,
            'view_table': 'c.s.t',
            'graph_name': 'g',
        }
        dt = {
            'view_exists': None,
            'local_lbug_exists': False,
            'registry_lbug_exists': None,
            'view_table': 'c.s.t',
            'graph_name': 'g',
            'local_lbug_path': '',
            'registry_lbug_path': '',
            'last_update': None,
            'last_built': None,
            'snapshot_table': '',
            'snapshot_exists': None,
        }
        with (
            patch.object(
                DigitalTwin,
                'sync_last_build_from_schedule',
                MagicMock(),
            ),
            patch.object(
                DigitalTwin,
                'fetch_graph_triplestore_status',
                AsyncMock(return_value=ts),
            ),
            patch.object(
                DigitalTwin,
                'fetch_digital_twin_existence',
                AsyncMock(return_value=dt),
            ),
            patch.object(
                DomainOps,
                'count_documents_in_volume',
                MagicMock(return_value=0),
            ),
        ):
            result = await get_detailed_validation(domain, settings)
        assert 'ontology_valid' in result
        assert 'mapping_valid' in result
        assert 'ontology' in result
        assert 'mapping' in result
        assert 'design' in result
