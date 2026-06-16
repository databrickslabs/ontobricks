"""Unit tests for SHACLService (T-M1.P1 sample under CNS).

Covers the service-layer entry points used by the API + UI:
- create_shape: builds a well-formed dict with a stable id + defaults.
- update_shape: in-place merge respects unknown keys safely.
- delete_shape: removes by id; missing id is a no-op.
- import_shapes: round-trip from Turtle into dict list.
- generate_turtle: dict list back to Turtle (mirrors SHACLGenerator).
- validate_graph: uses pyshacl; reports conformance status.
"""

from __future__ import annotations

import pytest

from back.core.w3c.shacl.SHACLService import SHACLService
from tests.fixtures.factories import ShaclShapeFactory


@pytest.fixture
def service() -> SHACLService:
    return SHACLService(base_uri="http://test.org/ontology#")


@pytest.mark.unit
class TestCreateShape:
    def test_returns_dict_with_required_keys(self):
        shape = SHACLService.create_shape(
            category="cardinality",
            target_class="Customer",
            target_class_uri="http://test.org/ontology#Customer",
            property_path="firstName",
            property_uri="http://test.org/ontology#firstName",
            shacl_type="sh:minCount",
            parameters={"value": 1},
        )
        for key in (
            "id",
            "category",
            "target_class",
            "target_class_uri",
            "property_path",
            "property_uri",
            "shacl_type",
            "parameters",
            "severity",
            "enabled",
        ):
            assert key in shape

    def test_default_severity_is_violation(self):
        shape = SHACLService.create_shape(
            category="cardinality",
            target_class="Customer",
            target_class_uri="http://test.org/ontology#Customer",
        )
        assert shape["severity"] == "sh:Violation"

    def test_custom_shape_id_respected(self):
        shape = SHACLService.create_shape(
            category="cardinality",
            target_class="Customer",
            target_class_uri="http://test.org/ontology#Customer",
            shape_id="shape_custom_42",
        )
        assert shape["id"] == "shape_custom_42"


@pytest.mark.unit
class TestUpdateAndDeleteShape:
    def test_update_replaces_only_specified_keys(self):
        a = SHACLService.create_shape(
            category="cardinality",
            target_class="Customer",
            target_class_uri="http://test.org/ontology#Customer",
            shape_id="s1",
        )
        b = SHACLService.create_shape(
            category="cardinality",
            target_class="Order",
            target_class_uri="http://test.org/ontology#Order",
            shape_id="s2",
        )
        result = SHACLService.update_shape([a, b], "s2", {"severity": "sh:Warning"})
        # Only the matching shape changes.
        assert next(s for s in result if s["id"] == "s2")["severity"] == "sh:Warning"
        assert next(s for s in result if s["id"] == "s1")["severity"] == "sh:Violation"

    def test_update_missing_id_is_noop(self):
        a = SHACLService.create_shape(
            category="cardinality",
            target_class="Customer",
            target_class_uri="http://test.org/ontology#Customer",
            shape_id="s1",
        )
        result = SHACLService.update_shape([a], "does-not-exist", {"severity": "sh:Warning"})
        assert result == [a]

    def test_delete_removes_only_matching_id(self):
        a = SHACLService.create_shape(
            category="cardinality",
            target_class="Customer",
            target_class_uri="http://test.org/ontology#Customer",
            shape_id="s1",
        )
        b = SHACLService.create_shape(
            category="cardinality",
            target_class="Order",
            target_class_uri="http://test.org/ontology#Order",
            shape_id="s2",
        )
        result = SHACLService.delete_shape([a, b], "s1")
        assert [s["id"] for s in result] == ["s2"]

    def test_delete_missing_id_is_noop(self):
        a = SHACLService.create_shape(
            category="cardinality",
            target_class="Customer",
            target_class_uri="http://test.org/ontology#Customer",
            shape_id="s1",
        )
        result = SHACLService.delete_shape([a], "does-not-exist")
        assert result == [a]


@pytest.mark.unit
class TestRoundtrip:
    def test_import_then_generate_preserves_target_class(self, service):
        ttl = ShaclShapeFactory.build_turtle(
            target_class="http://test.org/ontology#Customer",
            path_property="http://test.org/ontology#firstName",
        )
        shapes = service.import_shapes(ttl)
        assert len(shapes) >= 1
        out = service.generate_turtle(shapes)
        assert "Customer" in out
        assert "sh:NodeShape" in out


@pytest.mark.unit
class TestValidateGraph:
    """pyshacl-backed validation. Only smoke-coverage: real semantic tests
    belong in integration."""

    def test_validate_returns_conforms_for_empty_shape_list(self, service):
        data = (
            "@prefix : <http://test.org/data/> .\n"
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
            ":alice rdf:type :Customer .\n"
        )
        result = service.validate_graph(data, shapes=[])
        # No shapes → conforming by definition.
        assert isinstance(result, dict)
        assert result.get("conforms") in {True, "True", None}
