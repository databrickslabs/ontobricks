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


@pytest.mark.unit
class TestSuggestFromOntology:
    """Gap closure for suggest_from_ontology — signals come from OWL structure.

    Completeness for object properties is gated on minCardinality / someValuesFrom,
    not invented for every declared property.
    """

    _BASE = "http://t.org/ontology#"

    def _obj_prop(self, name: str, domain: str, range_cls: str, ns: str = "") -> dict:
        prefix = ns or "http://t.org/ontology/"
        return {
            "name": name,
            "type": "ObjectProperty",
            "domain": domain,
            "range": range_cls,
            "uri": f"{prefix}{name}",
        }

    def _dat_prop(self, name: str, domain: str, range_xsd: str = "xsd:string") -> dict:
        return {
            "name": name,
            "type": "DatatypeProperty",
            "domain": domain,
            "range": range_xsd,
            "uri": f"http://t.org/ontology/{name}",
        }

    def _constraint(self, ctype: str, prop_name: str, **extra) -> dict:
        return {
            "type": ctype,
            "property": prop_name,
            "propertyUri": extra.pop("propertyUri", f"http://t.org/ontology/{prop_name}"),
            **extra,
        }

    def test_class_listed_data_property_still_suggests_completeness(self):
        classes = [
            {
                "name": "Trade",
                "uri": f"{self._BASE}Trade",
                "dataProperties": [
                    {"name": "status", "uri": "http://t.org/ontology/status"}
                ],
            }
        ]
        result = SHACLService.suggest_from_ontology(classes, [], self._BASE)
        comp = [
            s
            for s in result
            if s["shacl_type"] == "sh:minCount" and s["property_path"] == "status"
        ]
        assert len(comp) == 1
        assert comp[0]["category"] == "completeness"

    def test_no_completeness_for_bare_object_property(self):
        props = [self._obj_prop("bookedIn", "Trade", "Book")]
        result = SHACLService.suggest_from_ontology([], props, self._BASE)
        assert [s for s in result if s["shacl_type"] == "sh:minCount"] == []

    def test_completeness_from_min_cardinality(self):
        props = [self._obj_prop("bookedIn", "Trade", "Book")]
        constraints = [
            self._constraint(
                "minCardinality",
                "bookedIn",
                className="Trade",
                cardinalityValue=1,
            )
        ]
        result = SHACLService.suggest_from_ontology(
            [], props, self._BASE, constraints=constraints
        )
        comp = [
            s
            for s in result
            if s["shacl_type"] == "sh:minCount" and s["property_path"] == "bookedIn"
        ]
        assert len(comp) == 1
        assert comp[0]["category"] == "completeness"
        assert comp[0]["parameters"]["sh:minCount"] == 1

    def test_no_duplicate_when_class_list_and_min_cardinality(self):
        classes = [
            {
                "name": "Trade",
                "uri": f"{self._BASE}Trade",
                "dataProperties": [
                    {"name": "status", "uri": "http://t.org/ontology/status"}
                ],
            }
        ]
        constraints = [
            self._constraint(
                "minCardinality",
                "status",
                className="Trade",
                cardinalityValue=1,
            )
        ]
        result = SHACLService.suggest_from_ontology(
            classes, [self._dat_prop("status", "Trade")], self._BASE, constraints=constraints
        )
        comp = [
            s
            for s in result
            if s["property_path"] == "status" and s["shacl_type"] == "sh:minCount"
        ]
        assert len(comp) == 1

    def test_no_cardinality_without_functional_constraint(self):
        props = [self._obj_prop("bookedIn", "Trade", "Book")]
        result = SHACLService.suggest_from_ontology([], props, self._BASE)
        assert [s for s in result if s["shacl_type"] == "sh:maxCount"] == []

    def test_cardinality_with_functional_constraint_matches_uri(self):
        props = [self._obj_prop("bookedIn", "Trade", "Book")]
        constraints = [self._constraint("functional", "bookedIn")]
        result = SHACLService.suggest_from_ontology(
            [], props, self._BASE, constraints=constraints
        )
        card = [
            s
            for s in result
            if s["shacl_type"] == "sh:maxCount" and s["property_path"] == "bookedIn"
        ]
        assert len(card) == 1
        assert card[0]["category"] == "cardinality"
        assert card[0]["parameters"]["sh:maxCount"] == 1
        assert "at most" in card[0]["message"]

    def test_functional_does_not_match_same_local_name_other_namespace(self):
        props = [
            self._obj_prop("bookedIn", "Trade", "Book", ns="http://t.org/ontology/"),
            self._obj_prop("bookedIn", "Deal", "Book", ns="http://other.org/"),
        ]
        constraints = [
            {
                "type": "functional",
                "property": "bookedIn",
                "propertyUri": "http://t.org/ontology/bookedIn",
            }
        ]
        result = SHACLService.suggest_from_ontology(
            [], props, self._BASE, constraints=constraints
        )
        card = [s for s in result if s["shacl_type"] == "sh:maxCount"]
        assert {s["target_class"] for s in card} == {"Trade"}

    def test_no_uniqueness_without_inverse_functional_constraint(self):
        props = [self._dat_prop("record_id", "Entity")]
        result = SHACLService.suggest_from_ontology([], props, self._BASE)
        assert [s for s in result if s["category"] == "uniqueness"] == []

    def test_uniqueness_with_inverse_functional_on_data_and_object_props(self):
        props = [
            self._dat_prop("record_id", "Entity"),
            self._obj_prop("identifiedBy", "Entity", "Id"),
        ]
        constraints = [
            self._constraint("inverseFunctional", "record_id"),
            self._constraint("inverseFunctional", "identifiedBy"),
        ]
        result = SHACLService.suggest_from_ontology(
            [], props, self._BASE, constraints=constraints
        )
        uniq = [s for s in result if s["category"] == "uniqueness"]
        assert {s["property_path"] for s in uniq} == {"record_id", "identifiedBy"}
        assert all("FILTER($this != ?other)" in s["parameters"]["sh:select"] for s in uniq)

    def test_idempotent_ids(self):
        props = [self._obj_prop("bookedIn", "Trade", "Book")]
        constraints = [self._constraint("functional", "bookedIn")]
        r1 = SHACLService.suggest_from_ontology(
            [], props, self._BASE, constraints=constraints
        )
        r2 = SHACLService.suggest_from_ontology(
            [], props, self._BASE, constraints=constraints
        )
        assert {s["id"] for s in r1} == {s["id"] for s in r2}

    def test_empty_inputs_return_empty(self):
        assert SHACLService.suggest_from_ontology([], [], self._BASE) == []
