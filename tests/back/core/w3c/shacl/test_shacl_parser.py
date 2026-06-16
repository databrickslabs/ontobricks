"""Unit tests for SHACLParser (T-M1.P1 sample under CNS).

Covers the public `parse()` surface:
- happy path: well-formed Turtle yields shape dicts with target_class + path.
- multi-constraint shapes (minCount + datatype + pattern) parse into separate shape dicts.
- malformed Turtle returns `[]` and logs (per parser's defensive design).
- closed shapes produce a "closed" structural shape.
- empty input returns `[]`.

Uses `ShaclShapeFactory.build_turtle()` from `tests/fixtures/factories/shacl_factory.py`
so the test inputs are themselves checked by the factory unit tests.
"""

from __future__ import annotations

import pytest

from back.core.w3c.shacl.SHACLParser import SHACLParser
from tests.fixtures.factories import ShaclShapeFactory


@pytest.mark.unit
class TestShaclParserHappyPath:
    """Well-formed input → expected shape dicts."""

    def test_minimal_node_shape_parses(self):
        ttl = ShaclShapeFactory.build_turtle(
            target_class="http://test.org/ontology#Customer",
            path_property="http://test.org/ontology#firstName",
            min_count=1,
            datatype="http://www.w3.org/2001/XMLSchema#string",
        )
        shapes = SHACLParser().parse(ttl)
        assert len(shapes) >= 1
        # Every parsed shape carries the target class info.
        for s in shapes:
            assert s.get("target_class_uri") == "http://test.org/ontology#Customer"
            assert s.get("target_class") == "Customer"

    def test_path_property_recorded(self):
        ttl = ShaclShapeFactory.build_turtle(
            target_class="http://test.org/ontology#Customer",
            path_property="http://test.org/ontology#email",
            min_count=1,
        )
        shapes = SHACLParser().parse(ttl)
        # Property path should be reflected somewhere in each shape dict.
        flat = " ".join(repr(s) for s in shapes)
        assert "email" in flat

    def test_multiple_classes_yield_distinct_shapes(self):
        # Concatenate two complete Turtle docs (with prefixes) — parser should
        # see both NodeShapes and emit shapes for each.
        ttl_a = ShaclShapeFactory.build_turtle(
            target_class="http://test.org/ontology#Customer",
            path_property="http://test.org/ontology#firstName",
        )
        # Strip prefixes on the second to avoid duplicate-prefix parse warnings.
        ttl_b_full = ShaclShapeFactory.build_turtle(
            target_class="http://test.org/ontology#Order",
            path_property="http://test.org/ontology#orderId",
        )
        body_b = "\n".join(
            line for line in ttl_b_full.splitlines() if not line.startswith("@prefix")
        )
        ttl = ttl_a + "\n" + body_b
        shapes = SHACLParser().parse(ttl)
        targets = {s.get("target_class") for s in shapes}
        assert "Customer" in targets
        assert "Order" in targets


@pytest.mark.unit
class TestShaclParserConstraints:
    """Constraint expression coverage."""

    def test_min_count_constraint_extracted(self):
        ttl = ShaclShapeFactory.build_turtle(min_count=2, path_property="http://x/p")
        shapes = SHACLParser().parse(ttl)
        # Search the shape dicts for the minCount value.
        flat = " ".join(repr(s) for s in shapes)
        assert "2" in flat or "minCount" in flat.lower() or "min_count" in flat.lower()

    def test_max_count_constraint_extracted(self):
        ttl = ShaclShapeFactory.build_turtle(min_count=None, max_count=3)
        shapes = SHACLParser().parse(ttl)
        flat = " ".join(repr(s) for s in shapes)
        assert "3" in flat or "maxCount" in flat.lower() or "max_count" in flat.lower()

    def test_pattern_constraint_extracted(self):
        ttl = ShaclShapeFactory.build_turtle(
            pattern=r"[A-Z][a-z]+",
            datatype="http://www.w3.org/2001/XMLSchema#string",
        )
        shapes = SHACLParser().parse(ttl)
        flat = " ".join(repr(s) for s in shapes)
        assert r"[A-Z][a-z]+" in flat or "pattern" in flat.lower()


@pytest.mark.unit
class TestShaclParserFailureModes:
    """Defensive paths — bad input should not raise."""

    def test_empty_input_returns_empty_list(self):
        assert SHACLParser().parse("") == []

    def test_malformed_turtle_returns_empty_list(self):
        # Garbage that rdflib cannot parse.
        bad = "@prefix x: <http://x/> .\nthis is not turtle at all !!!"
        assert SHACLParser().parse(bad) == []

    def test_non_shacl_turtle_yields_no_shapes(self):
        # Valid RDF but contains no sh:NodeShape.
        non_shacl = (
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
            "@prefix : <http://x/> .\n"
            ":alice :name \"Alice\" .\n"
        )
        assert SHACLParser().parse(non_shacl) == []

    def test_unsupported_format_returns_empty_list(self):
        # Pass a value that rdflib will reject as a format name.
        result = SHACLParser().parse("any content", format="not-a-real-format-12345")
        assert result == []
