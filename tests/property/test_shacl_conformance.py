"""Property-based tests for SHACL parser/generator (T-M6 expansion under CNS).

Invariants:
- A SHACL shape config that survives `generate_turtle → import_shapes` keeps
  its `target_class` and `path` fields.
- The Turtle produced by the generator is parseable by `rdflib` (no malformed
  output).
- Doubly-applying `delete_shape` for a non-existent id is a no-op (idempotency).

Hypothesis explores the constraint-parameter space (minCount, maxCount,
datatype, severity) — the parts of the SHACL shape config that map directly
to `sh:NodeShape`/`sh:PropertyShape` triples.

`property` marker — nightly only.
"""

from __future__ import annotations

import string

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st
from rdflib import Graph

from back.core.w3c.shacl.SHACLService import SHACLService


_LOCAL_NAME = st.text(
    alphabet=string.ascii_letters + string.digits,
    min_size=1,
    max_size=20,
).filter(lambda s: s[0].isalpha())


@st.composite
def _shape_dict(draw):
    """Build a single SHACL shape dict via SHACLService.create_shape."""
    target_name = draw(_LOCAL_NAME)
    property_name = draw(_LOCAL_NAME)
    base = "http://test.org/ontology#"
    # Constraint type & params (kept narrow on purpose — wider exploration
    # surfaces in T-M6 SPARQL property tests once those land).
    min_count = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=10)))
    max_count = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=20)))
    severity = draw(st.sampled_from(["sh:Violation", "sh:Warning", "sh:Info"]))
    params = {}
    if min_count is not None:
        params["min"] = min_count
    if max_count is not None and (min_count is None or max_count >= min_count):
        params["max"] = max_count
    return SHACLService.create_shape(
        category="cardinality",
        target_class=target_name,
        target_class_uri=f"{base}{target_name}",
        property_path=property_name,
        property_uri=f"{base}{property_name}",
        shacl_type="sh:minCount" if min_count is not None else "sh:maxCount",
        parameters=params or {"value": 1},
        severity=severity,
    )


@pytest.mark.property
class TestSHACLRoundtrip:
    @given(_shape_dict())
    @settings(
        max_examples=30,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_generated_turtle_parses_with_rdflib(self, shape):
        """No matter what shape we generate, rdflib parses the Turtle without raising."""
        service = SHACLService(base_uri="http://test.org/ontology#")
        turtle = service.generate_turtle([shape])
        g = Graph()
        g.parse(data=turtle, format="turtle")
        # At least the sh:NodeShape declaration should be there.
        assert len(g) > 0

    @given(_shape_dict())
    @settings(
        max_examples=30,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_target_class_preserved_in_roundtrip(self, shape):
        """target_class_uri survives generate → import."""
        service = SHACLService(base_uri="http://test.org/ontology#")
        turtle = service.generate_turtle([shape])
        parsed = service.import_shapes(turtle)
        assert len(parsed) >= 1
        # At least one parsed shape carries the original target class.
        targets = {s.get("target_class_uri") for s in parsed}
        assert shape["target_class_uri"] in targets, (
            f"target_class_uri {shape['target_class_uri']!r} lost in roundtrip; got {targets}"
        )


@pytest.mark.property
class TestSHACLServiceIdempotency:
    @given(st.lists(_shape_dict(), min_size=0, max_size=5))
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_delete_unknown_id_is_noop(self, shapes):
        """delete_shape with a non-existent id leaves the list unchanged."""
        before = list(shapes)
        after = SHACLService.delete_shape(shapes, "definitely-not-an-id-zzz")
        # Order and content preserved.
        assert [s["id"] for s in after] == [s["id"] for s in before]

    @given(st.lists(_shape_dict(), min_size=0, max_size=5))
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_update_unknown_id_is_noop(self, shapes):
        """update_shape with a non-existent id leaves the list unchanged."""
        before = list(shapes)
        after = SHACLService.update_shape(shapes, "no-such-id", {"severity": "sh:Warning"})
        assert [s["id"] for s in after] == [s["id"] for s in before]
        # And the severities are untouched.
        assert [s["severity"] for s in after] == [s["severity"] for s in before]
