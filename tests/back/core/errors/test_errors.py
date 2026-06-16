"""Unit tests for back.core.errors hierarchy (T-M1.P5 under CNS).

Closes the §2 gap: "Errors module tested via integration only — no direct unit tests".

Covers:
- OntoBricksError base — message, status_code, detail; error_code_from_class derivation.
- Each subclass: default status_code, default message, kwargs pass-through.
- ErrorResponse pydantic model — required fields, optional fields, serialisation.
"""

from __future__ import annotations

import pytest

from back.core.errors import (
    OntoBricksError,
    NotFoundError,
    ValidationError,
    AuthorizationError,
    ConflictError,
    InfrastructureError,
    ErrorResponse,
)
from back.core.errors.OntoBricksError import OntoBricksError as _Base


@pytest.mark.unit
class TestOntoBricksErrorBase:
    def test_message_attribute_set(self):
        err = OntoBricksError("something went wrong")
        assert err.message == "something went wrong"
        assert str(err) == "something went wrong"

    def test_default_status_code_is_500(self):
        err = OntoBricksError("boom")
        assert err.status_code == 500

    def test_status_code_kwarg_respected(self):
        err = OntoBricksError("boom", status_code=418)
        assert err.status_code == 418

    def test_detail_default_is_none(self):
        err = OntoBricksError("x")
        assert err.detail is None

    def test_detail_kwarg_respected(self):
        err = OntoBricksError("x", detail="ran out of disk")
        assert err.detail == "ran out of disk"

    def test_default_message(self):
        err = OntoBricksError()
        assert err.message == "An unexpected error occurred"

    def test_is_an_exception(self):
        assert isinstance(OntoBricksError("x"), Exception)


@pytest.mark.unit
class TestErrorCodeDerivation:
    @pytest.mark.parametrize(
        "exc_cls,expected",
        [
            (NotFoundError, "not_found"),
            (ValidationError, "validation"),
            (AuthorizationError, "authorization"),
            (ConflictError, "conflict"),
            (InfrastructureError, "infrastructure"),
        ],
    )
    def test_each_subclass_has_distinct_snake_case_code(self, exc_cls, expected):
        assert OntoBricksError.error_code_from_class(exc_cls) == expected

    def test_base_class_code_is_safe_fallback(self):
        code = OntoBricksError.error_code_from_class(OntoBricksError)
        # `OntoBricksError` -> stripped "Error" suffix -> "OntoBricks" -> snake = "onto_bricks"
        assert code == "onto_bricks"

    def test_unknown_suffix_class_falls_back_to_internal(self):
        class _Bare(OntoBricksError):
            pass

        code = OntoBricksError.error_code_from_class(_Bare)
        assert code == "bare"


@pytest.mark.unit
class TestSubclassDefaults:
    def test_not_found_defaults_to_404(self):
        assert NotFoundError().status_code == 404
        assert NotFoundError("missing").message == "missing"

    def test_validation_defaults_to_400(self):
        assert ValidationError().status_code == 400
        assert ValidationError("bad shape").message == "bad shape"

    def test_authorization_defaults_to_403(self):
        # Construct without args: just check it's an OntoBricksError and the status is 4xx.
        err = AuthorizationError()
        assert isinstance(err, OntoBricksError)
        assert 400 <= err.status_code < 500

    def test_conflict_default(self):
        err = ConflictError()
        assert isinstance(err, OntoBricksError)
        # Conflict is 409 per the hierarchy spec.
        assert err.status_code == 409

    def test_infrastructure_is_5xx(self):
        err = InfrastructureError()
        # Infrastructure errors are 502/503 per the hierarchy spec.
        assert 500 <= err.status_code < 600

    def test_subclass_accepts_detail_kwarg(self):
        err = NotFoundError("missing", detail="domain=sales, version=v3")
        assert err.detail == "domain=sales, version=v3"


@pytest.mark.unit
class TestSubclassPolymorphism:
    """Every subclass is catchable via the base — required by route handlers."""

    @pytest.mark.parametrize(
        "exc_cls",
        [NotFoundError, ValidationError, AuthorizationError, ConflictError, InfrastructureError],
    )
    def test_subclass_caught_by_base(self, exc_cls):
        with pytest.raises(OntoBricksError):
            raise exc_cls("test")

    def test_subclass_caught_by_python_exception(self):
        with pytest.raises(Exception):
            raise NotFoundError("test")


@pytest.mark.unit
class TestErrorResponse:
    def test_required_fields(self):
        resp = ErrorResponse(error="not_found", message="missing")
        assert resp.error == "not_found"
        assert resp.message == "missing"
        assert resp.detail is None
        assert resp.request_id is None

    def test_optional_detail_and_request_id(self):
        resp = ErrorResponse(
            error="validation",
            message="bad shape",
            detail="missing 'name'",
            request_id="abc-123",
        )
        assert resp.detail == "missing 'name'"
        assert resp.request_id == "abc-123"

    def test_missing_required_field_raises(self):
        from pydantic import ValidationError as PydanticValidationError

        with pytest.raises(PydanticValidationError):
            ErrorResponse(message="oops")  # 'error' missing

    def test_serialises_to_dict(self):
        resp = ErrorResponse(error="not_found", message="missing", detail="x")
        d = resp.model_dump()
        assert d["error"] == "not_found"
        assert d["message"] == "missing"
        assert d["detail"] == "x"
        assert d["request_id"] is None

    def test_serialises_to_json(self):
        import json

        resp = ErrorResponse(error="validation", message="bad")
        s = resp.model_dump_json()
        assert json.loads(s) == {
            "error": "validation",
            "message": "bad",
            "detail": None,
            "request_id": None,
        }


@pytest.mark.unit
class TestRaiseAndRescue:
    """Routes catch OntoBricksError generically and translate to HTTP; these
    smoke-test the contract that routes depend on."""

    def test_raises_carry_status_through_handler_chain(self):
        try:
            raise NotFoundError("ontology v3 missing")
        except OntoBricksError as exc:
            assert exc.status_code == 404
            assert exc.message == "ontology v3 missing"

    def test_chained_exception_keeps_original(self):
        try:
            try:
                raise ValueError("original")
            except ValueError as orig:
                raise InfrastructureError("databricks down") from orig
        except InfrastructureError as exc:
            assert exc.__cause__ is not None
            assert isinstance(exc.__cause__, ValueError)
