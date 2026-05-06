"""Tests for FastAPI routes."""

import json
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from shared.fastapi.main import app


@pytest.fixture
def client():
    """Create test client with cookies."""
    return TestClient(app)


class TestHealthRoutes:
    def test_health_check(self, client):
        # ``/health`` is now a comprehensive readiness probe.  Individual
        # checks may fail (no warehouse / no Lakebase in the test env)
        # but the route always returns 200 and a stable shape so external
        # probes can keep parsing the top-level ``status`` and
        # ``summary.errors`` fields.
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ("ok", "warning", "error")
        assert data["framework"] == "FastAPI"
        assert data["service"] == "OntoBricks"
        assert "summary" in data
        assert "checks" in data
        assert isinstance(data["checks"], list) and data["checks"]
        for c in data["checks"]:
            assert {"name", "label", "status", "detail", "duration_ms"} <= c.keys()
            assert c["status"] in ("ok", "warning", "error")

    def test_detailed_health_removed(self, client):
        # ``/health/detailed`` was retired — its information now lives in
        # ``/health``.  FastAPI returns 404 for the removed route.
        response = client.get("/health/detailed")
        assert response.status_code == 404


class TestMainRoutes:
    def test_index(self, client):
        response = client.get("/")
        assert response.status_code == 200

    def test_about(self, client):
        response = client.get("/about")
        assert response.status_code == 200

    def test_session_status(self, client):
        response = client.get("/session-status")
        assert response.status_code == 200

    def test_validate_ontology(self, client):
        response = client.get("/validate/ontology")
        assert response.status_code == 200

    def test_validate_detailed(self, client):
        response = client.get("/validate/detailed")
        assert response.status_code == 200


class TestSettingsRoutes:
    def test_settings_page(self, client):
        response = client.get("/settings")
        assert response.status_code == 200

    def test_settings_page_includes_health_tab(self, client):
        # In tests the request is treated as ``user_role == 'admin'`` (the
        # PermissionMiddleware short-circuit for non-Apps mode), so the
        # admin-only Health tab must be rendered.  Asserting on the nav-
        # link and the include's KPI container catches both the tab
        # registration and the partial wiring.
        response = client.get("/settings")
        assert response.status_code == 200
        body = response.text
        assert 'id="tab-health"' in body
        assert 'id="pane-health"' in body
        assert 'id="healthKpiTiles"' in body
        assert "settings-health.js" in body

    def test_settings_current(self, client):
        response = client.get("/settings/current")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    def test_settings_save(self, client):
        response = client.post(
            "/settings/save",
            json={
                "host": "https://test.databricks.com",
                "warehouse_id": "wh-123",
            },
        )
        assert response.status_code == 200

    def test_get_default_emoji(self, client):
        response = client.get("/settings/get-default-emoji")
        assert response.status_code == 200

    def test_get_base_uri(self, client):
        response = client.get("/settings/get-base-uri")
        assert response.status_code == 200


class TestOntologyRoutes:
    def test_ontology_page(self, client):
        response = client.get("/ontology/")
        assert response.status_code == 200

    def test_load_ontology(self, client):
        response = client.get("/ontology/load")
        assert response.status_code == 200
        data = response.json()
        assert "success" in data

    def test_save_ontology(self, client):
        config = {
            "name": "TestOntology",
            "base_uri": "http://test.org/ontology#",
            "classes": [{"name": "Foo", "label": "Foo"}],
            "properties": [],
        }
        response = client.post("/ontology/save", json=config)
        assert response.status_code == 200

    def test_add_class(self, client):
        data = {"name": "NewClass", "label": "New Class"}
        response = client.post("/ontology/class/add", json=data)
        assert response.status_code == 200

    def test_update_class(self, client):
        uri = "http://test.org/ontology#Updatable"
        client.post(
            "/ontology/class/add",
            json={"name": "Updatable", "label": "Updatable", "uri": uri},
        )
        data = {"uri": uri, "name": "Updatable", "label": "Updated Label"}
        response = client.post("/ontology/class/update", json=data)
        assert response.status_code == 200

    def test_delete_class(self, client):
        uri = "http://test.org/ontology#ToDelete"
        client.post(
            "/ontology/class/add",
            json={"name": "ToDelete", "label": "ToDelete", "uri": uri},
        )
        response = client.post("/ontology/class/delete", json={"uri": uri})
        assert response.status_code == 200

    def test_add_property(self, client):
        data = {"name": "hasProp", "domain": "A", "range": "B"}
        response = client.post("/ontology/property/add", json=data)
        assert response.status_code == 200

    def test_generate_owl(self, client):
        payload = {
            "name": "Test",
            "base_uri": "http://test.org#",
            "classes": [{"name": "X", "label": "X"}],
            "properties": [],
        }
        client.post("/ontology/save", json=payload)
        response = client.post("/ontology/generate-owl", json=payload)
        assert response.status_code == 200

    def test_import_owl(self, client):
        owl_content = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <http://test.org/ontology#> .

<http://test.org/ontology> a owl:Ontology ; rdfs:label "Imported" .
:ImportedClass a owl:Class ; rdfs:label "ImportedClass" .
"""
        response = client.post("/ontology/import-owl", json={"content": owl_content})
        assert response.status_code == 200

    def test_constraints_list(self, client):
        response = client.get("/ontology/constraints/list")
        assert response.status_code == 200

    def test_constraints_save_removed(self, client):
        """Legacy /constraints/save endpoint no longer exists (migrated to SHACL shapes)."""
        data = {"type": "functional", "property": "hasProp"}
        response = client.post("/ontology/constraints/save", json=data)
        assert response.status_code == 404

    def test_swrl_list(self, client):
        response = client.get("/ontology/swrl/list")
        assert response.status_code == 200

    def test_swrl_save(self, client):
        data = {
            "rule": {
                "name": "TestRule",
                "antecedent": "A(?x)",
                "consequent": "B(?x)",
                "description": "Test",
            },
            "index": -1,
        }
        response = client.post("/ontology/swrl/save", json=data)
        assert response.status_code == 200

    def test_axioms_list(self, client):
        response = client.get("/ontology/axioms/list")
        assert response.status_code == 200


class TestAutoAssignIconsAsync:
    """The icon-assignment endpoint must be non-blocking and task-tracked.

    Regression suite for the async refactor: the endpoint returns a
    ``task_id`` immediately and the agent work runs on a background
    thread, so it shows up under ``/tasks`` like the wizard generator.
    """

    @staticmethod
    def _wait_for_task(client, task_id, *, timeout=3.0, interval=0.05):
        import time

        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = client.get(f"/tasks/{task_id}")
            if resp.status_code == 200:
                data = resp.json()
                task = data.get("task") or {}
                if task.get("status") in ("completed", "failed", "cancelled"):
                    return task
            time.sleep(interval)
        raise AssertionError(f"Task {task_id} did not terminate within {timeout}s")

    def test_empty_entity_names_rejected(self, client):
        response = client.post("/ontology/auto-assign-icons", json={"entity_names": []})
        assert response.status_code == 400

    def test_returns_task_id_and_completes_on_success(self, client):
        from types import SimpleNamespace

        fake_result = SimpleNamespace(
            success=True,
            icons={"Customer": "🧑", "Order": "📋"},
            steps=[],
            iterations=1,
            usage={"prompt_tokens": 10, "completion_tokens": 5},
            error="",
        )

        with patch(
            "api.routers.internal.ontology.require_serving_llm",
            return_value=("https://h", "t", "ep"),
        ), patch.object(
            __import__(
                "api.routers.internal.ontology", fromlist=["Ontology"]
            ).Ontology,
            "assign_icons_with_agent",
            return_value=fake_result,
        ):
            response = client.post(
                "/ontology/auto-assign-icons",
                json={"entity_names": ["Customer", "Order"]},
            )

            assert response.status_code == 200
            body = response.json()
            assert body["success"] is True
            assert isinstance(body.get("task_id"), str) and body["task_id"]
            assert "icons" not in body, (
                "Icons must be returned via /tasks/{id}, not inline"
            )

            task = self._wait_for_task(client, body["task_id"])
            assert task["status"] == "completed"
            result = task.get("result") or {}
            assert result.get("icons") == {"Customer": "🧑", "Order": "📋"}
            assert result.get("missing") == []
            assert result.get("agent_iterations") == 1

    def test_agent_failure_marks_task_failed(self, client):
        from types import SimpleNamespace

        fake_result = SimpleNamespace(
            success=False,
            icons={},
            steps=[],
            iterations=0,
            usage={},
            error="LLM endpoint unreachable",
        )

        with patch(
            "api.routers.internal.ontology.require_serving_llm",
            return_value=("https://h", "t", "ep"),
        ), patch.object(
            __import__(
                "api.routers.internal.ontology", fromlist=["Ontology"]
            ).Ontology,
            "assign_icons_with_agent",
            return_value=fake_result,
        ):
            response = client.post(
                "/ontology/auto-assign-icons",
                json={"entity_names": ["Customer"]},
            )
            assert response.status_code == 200
            task_id = response.json()["task_id"]

            task = self._wait_for_task(client, task_id)
            assert task["status"] == "failed"


class TestMappingRoutes:
    def test_mapping_page(self, client):
        response = client.get("/mapping/")
        assert response.status_code == 200

    def test_load_mapping(self, client):
        response = client.get("/mapping/load")
        assert response.status_code == 200
        data = response.json()
        assert "success" in data

    def test_save_mapping(self, client):
        config = {
            "entities": [
                {"ontology_class": "A", "id_column": "id", "sql_query": "SELECT *"}
            ],
            "relationships": [],
        }
        response = client.post("/mapping/save", json=config)
        assert response.status_code == 200

    def test_add_entity_mapping(self, client):
        data = {
            "ontology_class": "http://test/A",
            "ontology_class_label": "A",
            "sql_query": "SELECT * FROM t",
            "id_column": "id",
        }
        response = client.post("/mapping/entity/add", json=data)
        assert response.status_code == 200

    def test_delete_entity_mapping(self, client):
        client.post(
            "/mapping/entity/add",
            json={
                "ontology_class": "http://test/Del",
                "ontology_class_label": "Del",
                "sql_query": "SELECT *",
                "id_column": "id",
            },
        )
        response = client.post(
            "/mapping/entity/delete", json={"ontology_class": "http://test/Del"}
        )
        assert response.status_code == 200

    def test_add_relationship_mapping(self, client):
        data = {
            "property": "http://test/p",
            "property_label": "p",
            "sql_query": "SELECT a, b FROM t",
            "source_id_column": "a",
            "target_id_column": "b",
        }
        response = client.post("/mapping/relationship/add", json=data)
        assert response.status_code == 200

    def test_generate_r2rml(self, client):
        client.post(
            "/ontology/save",
            json={
                "name": "T",
                "base_uri": "http://test.org#",
                "classes": [{"name": "A", "label": "A"}],
                "properties": [],
            },
        )
        client.post(
            "/mapping/entity/add",
            json={
                "ontology_class": "http://test.org#A",
                "ontology_class_label": "A",
                "sql_query": "SELECT * FROM t",
                "id_column": "id",
            },
        )
        response = client.post("/mapping/generate")
        assert response.status_code == 200


class TestDomainRoutes:
    def test_domain_page(self, client):
        response = client.get("/domain/")
        assert response.status_code == 200

    def test_get_domain_info(self, client):
        response = client.get("/domain/info")
        assert response.status_code == 200
        data = response.json()
        assert "success" in data

    def test_save_domain_info(self, client):
        data = {"name": "My Domain", "description": "A test domain"}
        response = client.post("/domain/save", json=data)
        assert response.status_code == 200

    def test_export_domain(self, client):
        response = client.get("/domain/export")
        assert response.status_code == 200

    def test_import_domain(self, client):
        domain_data = {
            "info": {"name": "Imported"},
            "versions": {
                "1": {
                    "ontology": {
                        "name": "O",
                        "base_uri": "http://t#",
                        "classes": [],
                        "properties": [],
                        "constraints": [],
                        "swrl_rules": [],
                        "axioms": [],
                        "expressions": [],
                    },
                    "assignment": {"entities": [], "relationships": []},
                    "design_layout": {"views": {}, "map": {}},
                }
            },
        }
        response = client.post("/domain/import", json=domain_data)
        assert response.status_code == 200

    def test_get_config(self, client):
        response = client.get("/domain/config")
        assert response.status_code == 200

    def test_session_debug(self, client, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        response = client.get("/domain/session-debug")
        assert response.status_code == 200
        assert response.json()["success"] is True

    def test_session_debug_blocked_when_not_debug(self, client, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "INFO")
        response = client.get("/domain/session-debug")
        assert response.status_code == 400
        body = response.json()
        assert body.get("error") == "validation"
        assert "session-debug" in body.get("message", "")

    def test_map_layout_get(self, client):
        response = client.get("/domain/map-layout")
        assert response.status_code == 200

    def test_design_views_get(self, client):
        response = client.get("/domain/design-views")
        assert response.status_code == 200


class TestQueryRoutes:
    def test_query_page(self, client):
        response = client.get("/dtwin/")
        assert response.status_code == 200


class TestTasksRoutes:
    def test_tasks_list(self, client):
        response = client.get("/tasks/")
        assert response.status_code == 200


class TestAPIv1Routes:
    def test_api_health(self, client):
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "OntoBricks API"

    def test_validate_query(self, client):
        response = client.post(
            "/api/v1/query/validate",
            json={"query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    def test_validate_invalid_query(self, client):
        response = client.post(
            "/api/v1/query/validate", json={"query": "INVALID QUERY"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["data"]["valid"] is False

    def test_domain_info_missing_path(self, client):
        response = client.post("/api/v1/domain/info", json={})
        assert response.status_code == 422

    def test_domain_info_no_credentials(self, client):
        response = client.post(
            "/api/v1/domain/info",
            json={"domain_path": "/Volumes/main/test/domain.json"},
        )
        assert response.status_code == 400
        body = response.json()
        assert (
            "credentials" in body.get("message", "").lower()
            or "credentials" in body.get("detail", "").lower()
        )

    def test_domain_ontology_no_credentials(self, client):
        response = client.post(
            "/api/v1/domain/ontology",
            json={"domain_path": "/Volumes/main/test/domain.json"},
        )
        assert response.status_code == 400

    def test_domain_ontology_classes_no_credentials(self, client):
        response = client.post(
            "/api/v1/domain/ontology/classes",
            json={"domain_path": "/Volumes/main/test/domain.json"},
        )
        assert response.status_code == 400

    def test_domain_ontology_properties_no_credentials(self, client):
        response = client.post(
            "/api/v1/domain/ontology/properties",
            json={"domain_path": "/Volumes/main/test/domain.json"},
        )
        assert response.status_code == 400

    def test_domain_mappings_no_credentials(self, client):
        response = client.post(
            "/api/v1/domain/mappings",
            json={"domain_path": "/Volumes/main/test/domain.json"},
        )
        assert response.status_code == 400

    def test_domain_r2rml_no_credentials(self, client):
        response = client.post(
            "/api/v1/domain/r2rml",
            json={"domain_path": "/Volumes/main/test/domain.json"},
        )
        assert response.status_code == 400


class TestDigitalTwinAPIRoutes:
    """Tests for /api/v1/digitaltwin/* endpoints."""

    def test_registry_endpoint_returns_200(self, client):
        response = client.get("/api/v1/digitaltwin/registry")
        assert response.status_code == 200
        data = response.json()
        assert "catalog" in data
        assert "schema" in data
        assert "volume" in data
        assert "configured" in data

    def test_registry_configured_flag(self, client):
        response = client.get("/api/v1/digitaltwin/registry")
        data = response.json()
        assert isinstance(data["configured"], bool)

    def test_domains_returns_error_when_unconfigured(self, client):
        response = client.get("/api/v1/domains")
        assert response.status_code in (200, 400, 502)

    def test_status_without_domain(self, client):
        response = client.get("/api/v1/digitaltwin/status")
        assert response.status_code == 200
        data = response.json()
        assert "success" in data

    def test_stats_without_domain(self, client):
        response = client.get("/api/v1/digitaltwin/stats")
        assert response.status_code in (200, 400, 502)

    def test_triples_find_requires_params(self, client):
        response = client.get("/api/v1/digitaltwin/triples/find")
        assert response.status_code == 400

    def test_triples_find_with_search(self, client):
        response = client.get("/api/v1/digitaltwin/triples/find?search=test")
        assert response.status_code in (200, 400, 502)

    def test_triples_without_domain(self, client):
        response = client.get("/api/v1/digitaltwin/triples")
        assert response.status_code in (200, 400, 404, 502)

    def test_build_progress_not_found(self, client):
        response = client.get("/api/v1/digitaltwin/build/nonexistent-task-id")
        assert response.status_code == 404

    def test_build_post(self, client):
        response = client.post("/api/v1/digitaltwin/build", json={})
        assert response.status_code in (200, 400)

    def test_registry_with_override_params(self, client):
        response = client.get(
            "/api/v1/domains"
            "?registry_catalog=cat&registry_schema=sch&registry_volume=vol"
        )
        assert response.status_code in (200, 400, 502)

    def test_openapi_includes_digitaltwin_paths(self, client):
        ext = client.get("/api/openapi.json")
        assert ext.status_code == 200
        paths = ext.json()["paths"]
        assert "/api/v1/digitaltwin/registry" in paths
        assert "/api/v1/domains" in paths
        assert "/api/v1/domain/versions" in paths
        assert "/api/v1/domain/design-status" in paths
        assert "/api/v1/domain/ontology" in paths
        assert "/api/v1/domain/r2rml" in paths
        assert "/api/v1/domain/sparksql" in paths
        assert "/api/v1/digitaltwin/status" in paths
        assert "/api/v1/digitaltwin/stats" in paths
        assert "/api/v1/digitaltwin/triples" in paths
        assert "/api/v1/digitaltwin/triples/find" in paths
        assert "/api/v1/digitaltwin/build" in paths
        assert "/api/v1/digitaltwin/build/{task_id}" in paths
        assert "/api/v1/graphql" in paths
        assert any(
            p.startswith("/api/v1/graphql/{") for p in paths
        ), "expected /api/v1/graphql/{domain_name} routes in external OpenAPI"

        internal = client.get("/openapi.json")
        assert internal.status_code == 200
        internal_paths = internal.json()["paths"]
        assert "/api/v1/digitaltwin/registry" not in internal_paths
        assert "/api/v1/graphql" not in internal_paths
        assert "/graphql" in internal_paths or any(
            p.startswith("/graphql/") for p in internal_paths
        )
