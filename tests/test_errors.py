"""Tests for the centralized error handling module (back.core.errors)."""
import pytest
from unittest.mock import patch, MagicMock

from back.core.errors import (
    OntoBricksError,
    NotFoundError,
    ValidationError,
    AuthorizationError,
    InfrastructureError,
    ConflictError,
    ErrorResponse,
    _error_code_from_class,
    register_exception_handlers,
)


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

class TestExceptionHierarchy:
    """All custom exceptions inherit from OntoBricksError."""

    def test_base_defaults(self):
        exc = OntoBricksError()
        assert exc.status_code == 500
        assert exc.message == "An unexpected error occurred"
        assert exc.detail is None
        assert str(exc) == "An unexpected error occurred"

    def test_base_with_detail(self):
        exc = OntoBricksError("boom", status_code=503, detail="extra")
        assert exc.status_code == 503
        assert exc.message == "boom"
        assert exc.detail == "extra"

    def test_not_found(self):
        exc = NotFoundError("Project missing")
        assert isinstance(exc, OntoBricksError)
        assert exc.status_code == 404
        assert exc.message == "Project missing"

    def test_validation(self):
        exc = ValidationError()
        assert exc.status_code == 400
        assert exc.message == "Validation failed"

    def test_authorization(self):
        exc = AuthorizationError()
        assert exc.status_code == 403

    def test_infrastructure(self):
        exc = InfrastructureError("DB down", detail="conn refused")
        assert exc.status_code == 502
        assert exc.detail == "conn refused"

    def test_conflict(self):
        exc = ConflictError()
        assert exc.status_code == 409

    def test_all_are_catchable_as_base(self):
        for cls in (NotFoundError, ValidationError, AuthorizationError,
                    InfrastructureError, ConflictError):
            with pytest.raises(OntoBricksError):
                raise cls()


# ---------------------------------------------------------------------------
# Error code derivation
# ---------------------------------------------------------------------------

class TestErrorCodeFromClass:
    def test_not_found(self):
        assert _error_code_from_class(NotFoundError) == "not_found"

    def test_validation(self):
        assert _error_code_from_class(ValidationError) == "validation"

    def test_authorization(self):
        assert _error_code_from_class(AuthorizationError) == "authorization"

    def test_infrastructure(self):
        assert _error_code_from_class(InfrastructureError) == "infrastructure"

    def test_conflict(self):
        assert _error_code_from_class(ConflictError) == "conflict"

    def test_base(self):
        assert _error_code_from_class(OntoBricksError) == "onto_bricks"


# ---------------------------------------------------------------------------
# ErrorResponse model
# ---------------------------------------------------------------------------

class TestErrorResponseModel:
    def test_full(self):
        r = ErrorResponse(
            error="not_found",
            message="Project missing",
            detail="path=/foo",
            request_id="abc-123",
        )
        d = r.model_dump()
        assert d["error"] == "not_found"
        assert d["message"] == "Project missing"
        assert d["detail"] == "path=/foo"
        assert d["request_id"] == "abc-123"

    def test_minimal(self):
        r = ErrorResponse(error="internal_error", message="boom")
        d = r.model_dump(exclude_none=True)
        assert "detail" not in d
        assert "request_id" not in d


# ---------------------------------------------------------------------------
# Global handler integration (via TestClient)
# ---------------------------------------------------------------------------

class TestGlobalExceptionHandler:
    """Test that the global handler converts exceptions to ErrorResponse JSON."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from shared.fastapi.main import app
        return TestClient(app, raise_server_exceptions=False)

    def test_ontobricks_error_returns_json(self, client):
        """POST to /api/v1/projects/list without credentials triggers ValidationError."""
        resp = client.post("/api/v1/projects/list", json={
            "catalog": "c", "schema": "s", "volume": "v",
        })
        assert resp.status_code == 400
        body = resp.json()
        assert "error" in body
        assert "message" in body
        assert body["error"] == "validation"

    def test_not_found_error_returns_404(self, client):
        """GET build progress for a non-existent task returns 404."""
        resp = client.get("/api/v1/digitaltwin/build/nonexistent-task-id")
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "not_found"

    def test_health_still_works(self, client):
        """Health check is unaffected by error handlers."""
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"


# ---------------------------------------------------------------------------
# Migrated API service tests
# ---------------------------------------------------------------------------

class TestApiServiceMigration:
    """Verify the service layer now raises instead of returning tuples."""

    def test_list_projects_no_creds_raises(self):
        from api.service import list_projects_from_uc
        with pytest.raises(ValidationError, match="credentials"):
            list_projects_from_uc("cat", "sch", "vol", None, None)

    def test_load_project_no_creds_raises(self):
        from api.service import load_project_from_uc
        with pytest.raises(ValidationError, match="credentials"):
            load_project_from_uc("/some/path", None, None)

    def test_execute_sparql_no_r2rml_raises(self):
        from api.service import execute_sparql_query
        with pytest.raises(ValidationError, match="R2RML"):
            execute_sparql_query({"ontology": {}}, "SELECT ?s WHERE {?s ?p ?o}")

    def test_execute_sparql_spark_engine_raises(self):
        from api.service import execute_sparql_query
        project_data = {"assignment": {"r2rml_output": "some content"}}
        with pytest.raises(ValidationError, match="Spark engine"):
            execute_sparql_query(project_data, "SELECT ?s WHERE {?s ?p ?o}", engine="spark")
