"""Pytest configuration and shared fixtures for OntoBricks."""

import importlib.util
import os
import warnings

import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient


def pytest_configure(config):
    """Runs before collection; filters noisy third-party warnings."""
    try:
        from urllib3.exceptions import NotOpenSSLWarning

        warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
    except ImportError:
        pass
    warnings.filterwarnings("ignore", category=FutureWarning, module=r"google\..*")


@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch):
    """Set up test environment variables."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")
    monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "test-warehouse")
    monkeypatch.setenv("SECRET_KEY", "test-secret-key")
    monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)


@pytest.fixture
def client():
    """Create FastAPI test client."""
    from shared.fastapi.main import app

    return TestClient(app)


@pytest.fixture
def mock_session_mgr():
    """Dict-backed session manager for DomainSession tests."""
    store = {}

    class _Mgr:
        def get(self, key, default=None):
            return store.get(key, default)

        def set(self, key, value):
            store[key] = value

        def delete(self, key):
            store.pop(key, None)

    return _Mgr()


@pytest.fixture
def domain_session(mock_session_mgr):
    """Create a DomainSession with a mock session manager."""
    from back.objects.session.DomainSession import DomainSession

    return DomainSession(mock_session_mgr)


@pytest.fixture
def sample_ontology_config():
    """Sample ontology configuration for testing."""
    return {
        "name": "TestOntology",
        "base_uri": "http://test.org/ontology#",
        "description": "Test ontology",
        "classes": [
            {
                "uri": "http://test.org/ontology#Customer",
                "name": "Customer",
                "label": "Customer",
                "comment": "A customer entity",
                "emoji": "👤",
                "parent": "",
                "dataProperties": [
                    {
                        "name": "firstName",
                        "localName": "firstName",
                        "label": "First Name",
                    },
                    {"name": "lastName", "localName": "lastName", "label": "Last Name"},
                ],
            },
            {
                "uri": "http://test.org/ontology#Order",
                "name": "Order",
                "label": "Order",
                "comment": "A sales order",
                "emoji": "📦",
                "parent": "",
                "dataProperties": [
                    {
                        "name": "orderDate",
                        "localName": "orderDate",
                        "label": "Order Date",
                    },
                ],
            },
        ],
        "properties": [
            {
                "uri": "http://test.org/ontology#hasOrder",
                "name": "hasOrder",
                "label": "has Order",
                "comment": "Links customer to order",
                "type": "ObjectProperty",
                "domain": "Customer",
                "range": "Order",
            },
        ],
        "constraints": [],
        "swrl_rules": [],
        "axioms": [],
        "expressions": [],
    }


@pytest.fixture
def sample_mapping_config():
    """Sample mapping configuration for testing."""
    return {
        "entities": [
            {
                "ontology_class": "http://test.org/ontology#Customer",
                "ontology_class_label": "Customer",
                "sql_query": "SELECT * FROM catalog.schema.customers",
                "id_column": "customer_id",
                "label_column": "name",
                "catalog": "catalog",
                "schema": "schema",
                "table": "customers",
                "attribute_mappings": {
                    "firstName": "first_name",
                    "lastName": "last_name",
                },
            },
            {
                "ontology_class": "http://test.org/ontology#Order",
                "ontology_class_label": "Order",
                "sql_query": "SELECT * FROM catalog.schema.orders",
                "id_column": "order_id",
                "label_column": "order_name",
                "catalog": "catalog",
                "schema": "schema",
                "table": "orders",
                "attribute_mappings": {"orderDate": "order_date"},
            },
        ],
        "relationships": [
            {
                "property": "http://test.org/ontology#hasOrder",
                "property_label": "hasOrder",
                "sql_query": "SELECT c.customer_id, o.order_id FROM customers c JOIN orders o ON c.id = o.customer_id",
                "source_class": "http://test.org/ontology#Customer",
                "source_class_label": "Customer",
                "target_class": "http://test.org/ontology#Order",
                "target_class_label": "Order",
                "source_id_column": "customer_id",
                "target_id_column": "order_id",
                "direction": "forward",
            },
        ],
    }


@pytest.fixture
def sample_owl_content():
    """Minimal valid OWL Turtle content for testing."""
    return """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://test.org/ontology#> .

<http://test.org/ontology> a owl:Ontology ;
    rdfs:label "TestOntology" .

:Customer a owl:Class ;
    rdfs:label "Customer" ;
    rdfs:comment "A customer entity" .

:Order a owl:Class ;
    rdfs:label "Order" ;
    rdfs:comment "A sales order" .

:Product a owl:Class ;
    rdfs:label "Product" ;
    rdfs:subClassOf :Order .

:hasOrder a owl:ObjectProperty ;
    rdfs:label "has Order" ;
    rdfs:domain :Customer ;
    rdfs:range :Order .

:firstName a owl:DatatypeProperty ;
    rdfs:label "firstName" ;
    rdfs:domain :Customer ;
    rdfs:range xsd:string .

:lastName a owl:DatatypeProperty ;
    rdfs:label "lastName" ;
    rdfs:domain :Customer ;
    rdfs:range xsd:string .

:orderDate a owl:DatatypeProperty ;
    rdfs:label "orderDate" ;
    rdfs:domain :Order ;
    rdfs:range xsd:string .
"""


@pytest.fixture
def mock_databricks_client():
    """Create a mock DatabricksClient."""
    client = MagicMock()
    client.host = "https://test.databricks.com"
    client.token = "test-token"
    client.warehouse_id = "test-warehouse"
    client.is_app_mode = False
    client.has_valid_auth.return_value = True
    client.test_connection.return_value = (
        True,
        "Connection successful (Personal Access Token)",
    )
    client.get_catalogs.return_value = ["catalog1", "catalog2"]
    client.get_schemas.return_value = ["schema1", "schema2"]
    client.get_tables.return_value = ["table1", "table2"]
    client.get_table_columns.return_value = [
        {"name": "id", "type": "int", "comment": "Primary key"},
        {"name": "name", "type": "string", "comment": "Name field"},
    ]
    return client


def pytest_collection_modifyitems(config, items):
    """Skip Playwright e2e tests when the optional dev dependency is not installed."""
    if importlib.util.find_spec("playwright") is not None:
        return
    skip_e2e = pytest.mark.skip(
        reason="playwright not installed; for e2e: pip install playwright && playwright install chromium",
    )
    for item in items:
        if "tests/e2e" in str(item.path).replace("\\", "/"):
            item.add_marker(skip_e2e)
