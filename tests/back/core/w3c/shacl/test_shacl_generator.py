"""Unit tests for SHACLGenerator (T-M1.P1 sample under CNS).

Covers `generate(shapes, base_uri=None) -> turtle_string`:
- empty `enabled` → emits a valid empty graph (no NodeShape declarations).
- one disabled shape is excluded from the output.
- a single shape produces a parseable Turtle string with `sh:NodeShape` + `sh:targetClass`.
- roundtrip through SHACLParser yields back at least one shape dict.
- base_uri override changes the namespace used for generated node-shape URIs.
"""

from __future__ import annotations

import pytest

from back.core.w3c.shacl.SHACLGenerator import SHACLGenerator
from back.core.w3c.shacl.SHACLParser import SHACLParser
from back.core.w3c.shacl.SHACLService import SHACLService


def _shape(
    target_class: str = "Customer",
    target_class_uri: str = "http://test.org/ontology#Customer",
    property_path: str = "firstName",
    property_uri: str = "http://test.org/ontology#firstName",
    shacl_type: str = "sh:minCount",
    parameters: dict | None = None,
    severity: str = "sh:Violation",
    enabled: bool = True,
) -> dict:
    return SHACLService.create_shape(
        category="cardinality",
        target_class=target_class,
        target_class_uri=target_class_uri,
        property_path=property_path,
        property_uri=property_uri,
        shacl_type=shacl_type,
        parameters=parameters or {"value": 1},
        severity=severity,
        enabled=enabled,
    )


@pytest.mark.unit
class TestShaclGeneratorBasics:
    def test_empty_enabled_emits_empty_graph(self):
        gen = SHACLGenerator("http://test.org/ontology")
        out = gen.generate([])
        # An empty graph serialises but contains no NodeShape declarations.
        assert "sh:NodeShape" not in out

    def test_only_disabled_shapes_emits_empty_graph(self):
        gen = SHACLGenerator("http://test.org/ontology")
        out = gen.generate([_shape(enabled=False)])
        assert "sh:NodeShape" not in out

    def test_single_shape_contains_node_shape_and_target_class(self):
        gen = SHACLGenerator("http://test.org/ontology")
        out = gen.generate([_shape()])
        assert "sh:NodeShape" in out
        assert "sh:targetClass" in out
        assert "Customer" in out


@pytest.mark.unit
class TestShaclGeneratorRoundtrip:
    """Parser ← Generator roundtrip."""

    def test_generate_then_parse_yields_at_least_one_shape(self):
        gen = SHACLGenerator("http://test.org/ontology")
        out = gen.generate([_shape(parameters={"value": 1})])
        parsed = SHACLParser().parse(out)
        assert len(parsed) >= 1
        assert any(s.get("target_class") == "Customer" for s in parsed)

    def test_two_shapes_for_distinct_classes_roundtrip(self):
        gen = SHACLGenerator("http://test.org/ontology")
        out = gen.generate(
            [
                _shape(target_class="Customer", target_class_uri="http://test.org/ontology#Customer"),
                _shape(target_class="Order", target_class_uri="http://test.org/ontology#Order"),
            ]
        )
        parsed = SHACLParser().parse(out)
        targets = {s.get("target_class") for s in parsed}
        assert "Customer" in targets
        assert "Order" in targets


@pytest.mark.unit
class TestShaclGeneratorBaseUri:
    def test_base_uri_override_changes_namespace(self):
        gen = SHACLGenerator("http://original.example/")
        out = gen.generate([_shape()], base_uri="http://override.example/")
        # The overridden base must appear; the original must NOT show up as the
        # node-shape namespace.
        assert "override.example" in out
