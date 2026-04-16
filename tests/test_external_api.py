"""Tests for external API endpoints (/api/v1/*).

Covers the 18 endpoints that previously had no route-level HTTP tests,
plus a few enriched happy-path tests for endpoints that only had negative-case coverage.
"""
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi.testclient import TestClient

from shared.fastapi.main import app


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


# =========================================================================
# v1.py — POST /api/v1/query
# =========================================================================


class TestAPIv1Query:
    """Tests for POST /api/v1/query."""

    def test_query_missing_fields(self, client):
        """Omitting required fields returns 422 (Pydantic validation)."""
        response = client.post('/api/v1/query', json={})
        assert response.status_code == 422

    def test_query_invalid_sparql(self, client):
        """An invalid SPARQL query returns 400 ValidationError."""
        response = client.post('/api/v1/query', json={
            'domain_path': '/Volumes/main/test/domain.json',
            'query': 'THIS IS NOT SPARQL',
        })
        assert response.status_code == 400
        body = response.json()
        assert 'invalid' in body.get('message', '').lower() or 'error' in body

    def test_query_no_credentials(self, client):
        """Valid SPARQL but no credentials returns a credential-related error."""
        response = client.post('/api/v1/query', json={
            'domain_path': '/Volumes/main/test/domain.json',
            'query': 'SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10',
        })
        assert response.status_code in (400, 502)

    @patch('api.service.load_domain_from_uc')
    @patch('api.service.validate_sparql_query', return_value=(True, None))
    @patch('api.service.execute_sparql_query')
    def test_query_success(self, mock_exec, mock_validate, mock_load, client):
        """Happy path: valid SPARQL with mocked domain returns results."""
        mock_load.return_value = {'versions': {'1': {
            'ontology': {'name': 'T', 'base_uri': 'http://t#', 'classes': [], 'properties': []},
            'assignment': {'entities': [], 'relationships': [], 'r2rml_output': ''},
        }}}
        mock_exec.return_value = {
            'results': [{'s': 'http://ex/1', 'p': 'http://ex/p', 'o': 'val'}],
            'columns': ['s', 'p', 'o'],
            'count': 1,
            'engine': 'local',
        }
        response = client.post('/api/v1/query', json={
            'domain_path': '/Volumes/main/test/domain.json',
            'query': 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }',
            'databricks_host': 'https://test.databricks.com',
            'databricks_token': 'tok',
        })
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['data']['count'] == 1
        assert len(data['data']['results']) == 1

    @patch('api.service.load_domain_from_uc')
    @patch('api.service.validate_sparql_query', return_value=(True, None))
    @patch('api.service.execute_sparql_query')
    def test_query_with_engine_spark(self, mock_exec, mock_validate, mock_load, client):
        """Engine parameter is forwarded to the execution layer."""
        mock_load.return_value = {'versions': {'1': {
            'ontology': {'name': 'T', 'base_uri': 'http://t#', 'classes': [], 'properties': []},
            'assignment': {'entities': [], 'relationships': [], 'r2rml_output': ''},
        }}}
        mock_exec.return_value = {
            'results': [], 'columns': [], 'count': 0, 'engine': 'spark',
        }
        response = client.post('/api/v1/query', json={
            'domain_path': '/Volumes/main/test/domain.json',
            'query': 'SELECT ?s WHERE { ?s ?p ?o }',
            'engine': 'spark',
            'databricks_host': 'https://test.databricks.com',
            'databricks_token': 'tok',
        })
        assert response.status_code == 200
        assert response.json()['data']['engine'] == 'spark'


# =========================================================================
# v1.py — POST /api/v1/query/samples
# =========================================================================


class TestAPIv1QuerySamples:
    """Tests for POST /api/v1/query/samples."""

    def test_samples_missing_path(self, client):
        response = client.post('/api/v1/query/samples', json={})
        assert response.status_code == 422

    def test_samples_no_credentials(self, client):
        response = client.post('/api/v1/query/samples', json={
            'domain_path': '/Volumes/main/test/domain.json',
        })
        assert response.status_code in (400, 502)

    @patch('api.service.load_domain_from_uc')
    @patch('api.service.generate_sample_queries')
    def test_samples_success(self, mock_gen, mock_load, client):
        mock_load.return_value = {'versions': {'1': {
            'ontology': {'name': 'T', 'base_uri': 'http://t#',
                         'classes': [{'name': 'A', 'label': 'A'}], 'properties': []},
            'assignment': {'entities': [], 'relationships': []},
        }}}
        mock_gen.return_value = [
            {'name': 'All triples', 'query': 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'},
        ]
        response = client.post('/api/v1/query/samples', json={
            'domain_path': '/Volumes/main/test/domain.json',
            'databricks_host': 'https://test.databricks.com',
            'databricks_token': 'tok',
        })
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['data']['count'] == 1
        assert len(data['data']['queries']) == 1


# =========================================================================
# domains.py — GET /api/v1/domain/versions
# =========================================================================


class TestDomainVersions:
    """Tests for GET /api/v1/domain/versions."""

    def test_versions_missing_domain_name(self, client):
        """domain_name is required."""
        response = client.get('/api/v1/domain/versions')
        assert response.status_code == 422

    @patch('api.routers.domains.DigitalTwin.resolve_registry')
    def test_versions_registry_not_configured(self, mock_resolve, client):
        mock_resolve.return_value = {'catalog': '', 'schema': '', 'volume': ''}
        response = client.get('/api/v1/domain/versions?domain_name=test')
        assert response.status_code == 400

    @patch('api.routers.domains.RegistryService')
    @patch('api.routers.domains.DigitalTwin')
    def test_versions_not_found(self, mock_dt, mock_svc_cls, client):
        mock_dt.resolve_registry.return_value = {'catalog': 'c', 'schema': 's', 'volume': 'v'}
        mock_dt.uc_from_domain.return_value = MagicMock()
        svc = MagicMock()
        svc.list_versions_sorted.return_value = []
        mock_svc_cls.return_value = svc
        response = client.get('/api/v1/domain/versions?domain_name=no_such')
        assert response.status_code == 404

    @patch('api.routers.domains.RegistryService')
    @patch('api.routers.domains.DigitalTwin')
    def test_versions_success(self, mock_dt, mock_svc_cls, client):
        mock_dt.resolve_registry.return_value = {'catalog': 'c', 'schema': 's', 'volume': 'v'}
        mock_dt.uc_from_domain.return_value = MagicMock()
        svc = MagicMock()
        svc.list_versions_sorted.return_value = ['3', '2', '1']
        mock_svc_cls.return_value = svc
        response = client.get('/api/v1/domain/versions?domain_name=mydom')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['latest_version'] == '3'
        assert len(data['versions']) == 3
        assert data['versions'][0]['is_latest'] is True


# =========================================================================
# domains.py — GET /api/v1/domain/design-status
# =========================================================================


class TestDomainDesignStatus:
    """Tests for GET /api/v1/domain/design-status."""

    @patch('api.routers.domains.DigitalTwin')
    def test_design_status_empty_domain(self, mock_dt, client):
        """Session domain with no ontology returns a valid but 'not_started' status."""
        domain = MagicMock()
        domain.domain_folder = ''
        domain.info = {'name': 'empty'}
        domain.get_classes.return_value = []
        domain.ontology = {'properties': [], 'base_uri': ''}
        domain.constraints = []
        domain.is_ontology_valid.return_value = False
        domain.ensure_generated_content.return_value = None
        domain.generated = {}
        domain._data = {}
        domain.get_entity_mappings.return_value = []
        domain.get_relationship_mappings.return_value = []
        domain.shacl_shapes = []
        domain.swrl_rules = []
        domain.get_r2rml.return_value = ''
        mock_dt.resolve_domain.return_value = domain
        mock_dt.is_datatype_range = DigitalTwin_is_datatype_range

        response = client.get('/api/v1/domain/design-status')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['build_ready'] is False
        assert data['assignment']['status'] == 'not_started'

    @patch('api.routers.domains.DigitalTwin')
    def test_design_status_with_classes(self, mock_dt, client):
        """Domain with classes but no mappings returns in_progress or not_started."""
        domain = MagicMock()
        domain.domain_folder = 'test_domain'
        domain.info = {'name': 'test_domain'}
        domain.get_classes.return_value = [
            {'uri': 'http://ex/A', 'name': 'A', 'label': 'A', 'dataProperties': []},
        ]
        domain.ontology = {
            'properties': [{'uri': 'http://ex/p', 'name': 'p', 'type': 'ObjectProperty',
                            'domain': 'A', 'range': 'B'}],
            'base_uri': 'http://ex/',
        }
        domain.constraints = []
        domain.is_ontology_valid.return_value = True
        domain.ensure_generated_content.return_value = None
        domain.generated = {'owl': '<owl/>'}
        domain._data = {}
        domain.get_entity_mappings.return_value = []
        domain.get_relationship_mappings.return_value = []
        domain.shacl_shapes = []
        domain.swrl_rules = []
        domain.get_r2rml.return_value = ''
        mock_dt.resolve_domain.return_value = domain
        mock_dt.is_datatype_range = DigitalTwin_is_datatype_range

        response = client.get('/api/v1/domain/design-status?domain_name=test_domain')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['ontology']['ready'] is True
        assert data['ontology']['class_count'] == 1


def DigitalTwin_is_datatype_range(range_val):
    """Stub for DigitalTwin.is_datatype_range used in design-status tests."""
    return range_val.startswith('xsd:') or range_val.startswith('http://www.w3.org/2001/XMLSchema#')


# =========================================================================
# domains.py — GET /api/v1/domain/ontology (GET variant)
# =========================================================================


class TestDomainOntologyGET:
    """Tests for GET /api/v1/domain/ontology."""

    @patch('api.routers.domains.DigitalTwin')
    def test_ontology_no_classes(self, mock_dt, client):
        """Domain with empty ontology returns 400."""
        domain = MagicMock()
        domain.get_classes.return_value = []
        domain.ontology = {'properties': [], 'base_uri': ''}
        mock_dt.resolve_domain.return_value = domain

        response = client.get('/api/v1/domain/ontology')
        assert response.status_code == 400

    @patch('api.routers.domains.DigitalTwin')
    def test_ontology_success(self, mock_dt, client):
        """Domain with classes and generated OWL returns the Turtle content."""
        domain = MagicMock()
        domain.domain_folder = 'test'
        domain.get_classes.return_value = [{'name': 'A', 'label': 'A'}]
        domain.ontology = {'properties': [], 'base_uri': 'http://ex/'}
        domain.ensure_generated_content.return_value = None
        domain.generated = {'owl': '@prefix owl: ...'}
        mock_dt.resolve_domain.return_value = domain

        response = client.get('/api/v1/domain/ontology')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['format'] == 'turtle'
        assert data['content'] == '@prefix owl: ...'
        assert data['class_count'] == 1


# =========================================================================
# domains.py — GET /api/v1/domain/r2rml (GET variant)
# =========================================================================


class TestDomainR2RMLGET:
    """Tests for GET /api/v1/domain/r2rml."""

    @patch('api.routers.domains.DigitalTwin')
    def test_r2rml_no_entities(self, mock_dt, client):
        domain = MagicMock()
        domain.get_entity_mappings.return_value = []
        domain.get_relationship_mappings.return_value = []
        mock_dt.resolve_domain.return_value = domain

        response = client.get('/api/v1/domain/r2rml')
        assert response.status_code == 400

    @patch('api.routers.domains.DigitalTwin')
    def test_r2rml_success(self, mock_dt, client):
        domain = MagicMock()
        domain.domain_folder = 'test'
        domain.get_entity_mappings.return_value = [{'ontology_class': 'http://ex/A'}]
        domain.get_relationship_mappings.return_value = []
        domain.ensure_generated_content.return_value = None
        domain.get_r2rml.return_value = '@prefix rr: <http://www.w3.org/ns/r2rml#> .'
        domain.ontology = {'base_uri': 'http://ex/'}
        domain.assignment = {'entities': [], 'relationships': []}
        mock_dt.resolve_domain.return_value = domain

        response = client.get('/api/v1/domain/r2rml')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['format'] == 'turtle'
        assert 'r2rml' in data['content'].lower()


# =========================================================================
# domains.py — GET /api/v1/domain/sparksql
# =========================================================================


class TestDomainSparkSQL:
    """Tests for GET /api/v1/domain/sparksql."""

    @patch('api.routers.domains.DigitalTwin')
    def test_sparksql_no_r2rml(self, mock_dt, client):
        domain = MagicMock()
        domain.ensure_generated_content.return_value = None
        domain.get_r2rml.return_value = ''
        domain.ontology = {'base_uri': 'http://ex/'}
        mock_dt.resolve_domain.return_value = domain

        response = client.get('/api/v1/domain/sparksql')
        assert response.status_code == 400

    @patch('back.core.w3c.sparql.translate_sparql_to_spark')
    @patch('back.core.w3c.sparql.extract_r2rml_mappings')
    @patch('api.routers.domains.DigitalTwin')
    def test_sparksql_success(self, mock_dt, mock_extract, mock_translate, client):
        domain = MagicMock()
        domain.domain_folder = 'test'
        domain.ensure_generated_content.return_value = None
        domain.get_r2rml.return_value = '@prefix rr: ...'
        domain.ontology = {'base_uri': 'http://ex/'}
        domain.assignment = {'entities': [], 'relationships': []}
        mock_dt.resolve_domain.return_value = domain
        mock_dt.augment_mappings_from_config.return_value = [{'table': 't'}]
        mock_dt.augment_relationships_from_config.return_value = []
        mock_extract.return_value = ([{'table': 't'}], [])
        mock_translate.return_value = {
            'success': True,
            'sql': 'SELECT subject, predicate, object FROM ...',
        }

        response = client.get('/api/v1/domain/sparksql')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert 'SELECT' in data['sql']

    @patch('back.core.w3c.sparql.translate_sparql_to_spark')
    @patch('back.core.w3c.sparql.extract_r2rml_mappings')
    @patch('api.routers.domains.DigitalTwin')
    def test_sparksql_translation_failure(self, mock_dt, mock_extract, mock_translate, client):
        domain = MagicMock()
        domain.ensure_generated_content.return_value = None
        domain.get_r2rml.return_value = '@prefix rr: ...'
        domain.ontology = {'base_uri': 'http://ex/'}
        domain.assignment = {'entities': [], 'relationships': []}
        mock_dt.resolve_domain.return_value = domain
        mock_dt.augment_mappings_from_config.return_value = [{'table': 't'}]
        mock_dt.augment_relationships_from_config.return_value = []
        mock_extract.return_value = ([{'table': 't'}], [])
        mock_translate.return_value = {
            'success': False,
            'message': 'Translation error',
        }

        response = client.get('/api/v1/domain/sparksql')
        assert response.status_code == 502


# =========================================================================
# digitaltwin.py — POST /api/v1/digitaltwin/dataquality/start
# =========================================================================


class TestDigitalTwinDataQuality:
    """Tests for POST /api/v1/digitaltwin/dataquality/start and GET progress."""

    @patch('api.routers.digitaltwin.DigitalTwin')
    @patch('api.routers.digitaltwin.effective_view_table', return_value='cat.sch.view')
    @patch('api.routers.digitaltwin.effective_graph_name', return_value='cat.sch.graph')
    def test_dataquality_no_shapes(self, mock_gn, mock_vt, mock_dt, client):
        """When no shapes or rules are enabled, returns success=False."""
        domain = MagicMock()
        domain.shacl_shapes = []
        domain.swrl_rules = []
        domain.ontology = {}
        domain._data = {}
        mock_dt.resolve_domain.return_value = domain

        response = client.post('/api/v1/digitaltwin/dataquality/start', json={})
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is False
        assert 'no enabled' in data['message'].lower()

    @patch('api.routers.digitaltwin.DigitalTwin')
    @patch('api.routers.digitaltwin.effective_view_table', return_value='cat.sch.view')
    @patch('api.routers.digitaltwin.effective_graph_name', return_value='cat.sch.graph')
    def test_dataquality_start_success(self, mock_gn, mock_vt, mock_dt, client):
        """With enabled shapes, returns a task_id."""
        domain = MagicMock()
        domain.shacl_shapes = [
            {'name': 'shape1', 'enabled': True, 'category': 'cardinality'},
        ]
        domain.swrl_rules = []
        domain.ontology = {'classes': [], 'properties': []}
        domain._data = {'ontology': {}}
        mock_dt.resolve_domain.return_value = domain
        mock_dt.make_snapshot.return_value = MagicMock()

        response = client.post('/api/v1/digitaltwin/dataquality/start', json={})
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['task_id'] is not None
        assert data['shape_count'] == 1

    @patch('api.routers.digitaltwin.DigitalTwin')
    @patch('api.routers.digitaltwin.effective_view_table', return_value='cat.sch.view')
    @patch('api.routers.digitaltwin.effective_graph_name', return_value='cat.sch.graph')
    def test_dataquality_category_filter(self, mock_gn, mock_vt, mock_dt, client):
        """Category filter selects only matching shapes."""
        domain = MagicMock()
        domain.shacl_shapes = [
            {'name': 'shape1', 'enabled': True, 'category': 'cardinality'},
            {'name': 'shape2', 'enabled': True, 'category': 'value'},
        ]
        domain.swrl_rules = []
        domain.ontology = {}
        domain._data = {'ontology': {}}
        mock_dt.resolve_domain.return_value = domain
        mock_dt.make_snapshot.return_value = MagicMock()

        response = client.post('/api/v1/digitaltwin/dataquality/start',
                               json={'category': 'cardinality'})
        assert response.status_code == 200
        data = response.json()
        assert data['shape_count'] == 1


class TestDigitalTwinDataQualityProgress:
    """Tests for GET /api/v1/digitaltwin/dataquality/{task_id}."""

    def test_dataquality_progress_not_found(self, client):
        response = client.get('/api/v1/digitaltwin/dataquality/nonexistent-id')
        assert response.status_code == 404

    @patch('api.routers.digitaltwin.DigitalTwin')
    @patch('api.routers.digitaltwin.effective_view_table', return_value='cat.sch.view')
    @patch('api.routers.digitaltwin.effective_graph_name', return_value='cat.sch.graph')
    def test_dataquality_progress_after_start(self, mock_gn, mock_vt, mock_dt, client):
        """Start a task and immediately poll for it — should find the task."""
        domain = MagicMock()
        domain.shacl_shapes = [{'name': 's', 'enabled': True}]
        domain.swrl_rules = []
        domain.ontology = {}
        domain._data = {'ontology': {}}
        mock_dt.resolve_domain.return_value = domain
        mock_dt.make_snapshot.return_value = MagicMock()

        start = client.post('/api/v1/digitaltwin/dataquality/start', json={})
        task_id = start.json()['task_id']

        response = client.get(f'/api/v1/digitaltwin/dataquality/{task_id}')
        assert response.status_code == 200
        data = response.json()
        assert data['task_id'] == task_id
        assert data['status'] in ('pending', 'running', 'completed', 'failed')


# =========================================================================
# digitaltwin.py — POST /api/v1/digitaltwin/inference/start
# =========================================================================


class TestDigitalTwinInference:
    """Tests for POST /api/v1/digitaltwin/inference/start."""

    @patch('api.routers.digitaltwin.DigitalTwin')
    def test_inference_start_success(self, mock_dt, client):
        domain = MagicMock()
        domain.ensure_generated_content.return_value = None
        mock_dt.resolve_domain.return_value = domain
        mock_dt.make_snapshot.return_value = MagicMock()

        response = client.post('/api/v1/digitaltwin/inference/start', json={})
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['task_id'] is not None
        assert 'tbox' in data['message'].lower()

    @patch('api.routers.digitaltwin.DigitalTwin')
    def test_inference_start_custom_phases(self, mock_dt, client):
        domain = MagicMock()
        domain.ensure_generated_content.return_value = None
        mock_dt.resolve_domain.return_value = domain
        mock_dt.make_snapshot.return_value = MagicMock()

        response = client.post('/api/v1/digitaltwin/inference/start', json={
            'tbox': False,
            'swrl': True,
            'graph': False,
            'constraints': False,
        })
        assert response.status_code == 200
        data = response.json()
        assert 'swrl' in data['message'].lower()
        assert 'tbox' not in data['message'].lower()

    @patch('api.routers.digitaltwin.DigitalTwin')
    def test_inference_start_with_materialize(self, mock_dt, client):
        domain = MagicMock()
        domain.ensure_generated_content.return_value = None
        mock_dt.resolve_domain.return_value = domain
        mock_dt.make_snapshot.return_value = MagicMock()

        response = client.post('/api/v1/digitaltwin/inference/start', json={
            'materialize': True,
            'materialize_table': 'cat.sch.inferred',
        })
        assert response.status_code == 200
        assert response.json()['success'] is True


# =========================================================================
# digitaltwin.py — GET /api/v1/digitaltwin/inference/results
# =========================================================================


class TestDigitalTwinInferenceResults:
    """Tests for GET /api/v1/digitaltwin/inference/results (stub endpoint)."""

    def test_inference_results_stub(self, client):
        """The stub returns a message telling the user to poll the task."""
        response = client.get('/api/v1/digitaltwin/inference/results')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert 'not stored' in data['message'].lower() or 'task_id' in data['message'].lower()
        assert data['inferred_count'] == 0


# =========================================================================
# digitaltwin.py — GET /api/v1/digitaltwin/inference/{task_id}
# =========================================================================


class TestDigitalTwinInferenceProgress:
    """Tests for GET /api/v1/digitaltwin/inference/{task_id}."""

    def test_inference_progress_not_found(self, client):
        response = client.get('/api/v1/digitaltwin/inference/nonexistent-id')
        assert response.status_code == 404

    @patch('api.routers.digitaltwin.DigitalTwin')
    def test_inference_progress_after_start(self, mock_dt, client):
        """Start inference and immediately poll — task should be found."""
        domain = MagicMock()
        domain.ensure_generated_content.return_value = None
        mock_dt.resolve_domain.return_value = domain
        mock_dt.make_snapshot.return_value = MagicMock()

        start = client.post('/api/v1/digitaltwin/inference/start', json={})
        task_id = start.json()['task_id']

        response = client.get(f'/api/v1/digitaltwin/inference/{task_id}')
        assert response.status_code == 200
        data = response.json()
        assert data['task_id'] == task_id
        assert data['status'] in ('pending', 'running', 'completed', 'failed')


# =========================================================================
# graphql_routes.py — GET /api/v1/graphql
# =========================================================================


class TestGraphQLListDomains:
    """Tests for GET /api/v1/graphql."""

    @patch('back.fastapi.graphql_routes.RegistryService')
    def test_graphql_list_unconfigured(self, mock_svc_cls, client):
        svc = MagicMock()
        svc.cfg.is_configured = False
        mock_svc_cls.from_context.return_value = svc

        response = client.get('/api/v1/graphql')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is False
        assert 'not configured' in data['message'].lower()

    @patch('back.fastapi.graphql_routes.RegistryService')
    def test_graphql_list_success(self, mock_svc_cls, client):
        svc = MagicMock()
        svc.cfg.is_configured = True
        svc.list_mcp_domains.return_value = (
            True,
            [{'name': 'dom1', 'description': 'D1'}, {'name': 'dom2', 'description': 'D2'}],
            '',
        )
        mock_svc_cls.from_context.return_value = svc

        response = client.get('/api/v1/graphql')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert len(data['domains']) == 2
        assert data['domains'][0]['name'] == 'dom1'

    @patch('back.fastapi.graphql_routes.RegistryService')
    def test_graphql_list_failure(self, mock_svc_cls, client):
        svc = MagicMock()
        svc.cfg.is_configured = True
        svc.list_mcp_domains.return_value = (False, [], 'Registry read failed')
        mock_svc_cls.from_context.return_value = svc

        response = client.get('/api/v1/graphql')
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is False


# =========================================================================
# graphql_routes.py — GET /api/v1/graphql/settings/depth
# =========================================================================


class TestGraphQLDepthSettings:
    """Tests for GET /api/v1/graphql/settings/depth."""

    def test_depth_settings(self, client):
        response = client.get('/api/v1/graphql/settings/depth')
        assert response.status_code == 200
        data = response.json()
        assert 'default' in data
        assert 'max' in data
        assert isinstance(data['default'], int)
        assert isinstance(data['max'], int)
        assert data['max'] >= data['default']


# =========================================================================
# graphql_routes.py — GET /api/v1/graphql/{domain_name} (playground)
# =========================================================================


class TestGraphQLPlayground:
    """Tests for GET /api/v1/graphql/{domain_name}."""

    @patch('back.fastapi.graphql_routes._get_schema_and_context')
    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_playground_returns_html(self, mock_load, mock_schema, client):
        domain = MagicMock()
        domain.info = {'name': 'TestDomain'}
        mock_load.return_value = domain
        mock_schema.return_value = (MagicMock(), {})

        response = client.get('/api/v1/graphql/testdom')
        assert response.status_code == 200
        assert 'text/html' in response.headers['content-type']
        assert 'GraphiQL' in response.text or 'graphiql' in response.text

    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_playground_domain_not_found(self, mock_load, client):
        from back.core.errors import NotFoundError
        mock_load.side_effect = NotFoundError("Domain 'unknown' not found")

        response = client.get('/api/v1/graphql/unknown')
        assert response.status_code == 404


# =========================================================================
# graphql_routes.py — POST /api/v1/graphql/{domain_name}
# =========================================================================


class TestGraphQLExecute:
    """Tests for POST /api/v1/graphql/{domain_name}."""

    @patch('back.fastapi.graphql_routes._get_schema_and_context')
    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_execute_success(self, mock_load, mock_schema, client):
        domain = MagicMock()
        mock_load.return_value = domain

        schema = MagicMock()
        result = MagicMock()
        result.data = {'allCustomers': [{'name': 'Alice'}]}
        result.errors = None
        schema.execute_sync.return_value = result

        mock_schema.return_value = (schema, {'triplestore': MagicMock(), 'table_name': 't'})

        response = client.post('/api/v1/graphql/testdom', json={
            'query': '{ allCustomers { name } }',
        })
        assert response.status_code == 200
        data = response.json()
        assert 'data' in data
        assert data['data']['allCustomers'][0]['name'] == 'Alice'

    @patch('back.fastapi.graphql_routes._get_schema_and_context')
    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_execute_with_errors(self, mock_load, mock_schema, client):
        domain = MagicMock()
        mock_load.return_value = domain

        schema = MagicMock()
        error = MagicMock()
        error.__str__ = lambda self: 'Field not found'
        error.path = ['allCustomers']
        result = MagicMock()
        result.data = None
        result.errors = [error]
        schema.execute_sync.return_value = result

        mock_schema.return_value = (schema, {'triplestore': MagicMock(), 'table_name': 't'})

        response = client.post('/api/v1/graphql/testdom', json={
            'query': '{ nonExistentField }',
        })
        assert response.status_code == 400
        data = response.json()
        assert 'errors' in data

    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_execute_domain_not_found(self, mock_load, client):
        from back.core.errors import NotFoundError
        mock_load.side_effect = NotFoundError("Domain not found")

        response = client.post('/api/v1/graphql/unknown', json={
            'query': '{ allCustomers { name } }',
        })
        assert response.status_code == 404


# =========================================================================
# graphql_routes.py — GET /api/v1/graphql/{domain_name}/schema
# =========================================================================


class TestGraphQLSchema:
    """Tests for GET /api/v1/graphql/{domain_name}/schema."""

    @patch('back.fastapi.graphql_routes.print_schema', create=True)
    @patch('back.fastapi.graphql_routes._get_schema_and_context')
    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_schema_success(self, mock_load, mock_schema, mock_print, client):
        domain = MagicMock()
        mock_load.return_value = domain
        schema = MagicMock()
        mock_schema.return_value = (schema, {})

        with patch('strawberry.printer.print_schema', return_value='type Query { hello: String }'):
            response = client.get('/api/v1/graphql/testdom/schema')

        assert response.status_code == 200
        data = response.json()
        assert 'sdl' in data

    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_schema_domain_not_found(self, mock_load, client):
        from back.core.errors import NotFoundError
        mock_load.side_effect = NotFoundError("Domain not found")

        response = client.get('/api/v1/graphql/unknown/schema')
        assert response.status_code == 404


# =========================================================================
# graphql_routes.py — GET /api/v1/graphql/{domain_name}/debug
# =========================================================================


class TestGraphQLDebug:
    """Tests for GET /api/v1/graphql/{domain_name}/debug."""

    @patch('back.core.graphql.build_schema_for_domain')
    @patch('back.fastapi.graphql_routes._get_schema_and_context')
    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_debug_success(self, mock_load, mock_schema_ctx, mock_build, client):
        domain = MagicMock()
        domain.ontology = {'classes': [{'name': 'A'}], 'properties': [], 'base_uri': 'http://ex/'}
        domain.info = {'name': 'test'}
        mock_load.return_value = domain

        store = MagicMock()
        store.get_predicates_for_type.return_value = ['http://ex/p1']
        store.find_subjects_by_type.return_value = ['http://ex/s1', 'http://ex/s2']
        mock_schema_ctx.return_value = (MagicMock(), {
            'triplestore': store, 'table_name': 'graph_tbl', 'base_uri': 'http://ex/',
        })

        type_info = MagicMock()
        type_info.cls_uri = 'http://ex/A'
        type_info.predicate_to_field = {'http://ex/p1': 'p1'}
        metadata = MagicMock()
        metadata.types = {'A': type_info}
        mock_build.return_value = (MagicMock(), metadata)

        response = client.get('/api/v1/graphql/testdom/debug')
        assert response.status_code == 200
        data = response.json()
        assert '_backend' in data
        assert '_table' in data

    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_debug_domain_not_found(self, mock_load, client):
        from back.core.errors import NotFoundError
        mock_load.side_effect = NotFoundError("Domain not found")

        response = client.get('/api/v1/graphql/unknown/debug')
        assert response.status_code == 404

    @patch('back.core.graphql.build_schema_for_domain')
    @patch('back.fastapi.graphql_routes._get_schema_and_context')
    @patch('back.fastapi.graphql_routes._load_domain_from_registry')
    def test_debug_with_type_filter(self, mock_load, mock_schema_ctx, mock_build, client):
        domain = MagicMock()
        domain.ontology = {'classes': [{'name': 'A'}], 'properties': [], 'base_uri': 'http://ex/'}
        domain.info = {'name': 'test'}
        mock_load.return_value = domain

        store = MagicMock()
        store.get_predicates_for_type.return_value = []
        store.find_subjects_by_type.return_value = []
        mock_schema_ctx.return_value = (MagicMock(), {
            'triplestore': store, 'table_name': 'graph_tbl', 'base_uri': 'http://ex/',
        })

        type_info = MagicMock()
        type_info.cls_uri = 'http://ex/A'
        type_info.predicate_to_field = {}
        metadata = MagicMock()
        metadata.types = {'A': type_info, 'B': MagicMock()}
        mock_build.return_value = (MagicMock(), metadata)

        response = client.get('/api/v1/graphql/testdom/debug?type_name=A')
        assert response.status_code == 200
        data = response.json()
        assert 'A' in data
        assert 'B' not in data
