"""MCP formatting contracts for entity SHACL facets (behavioral)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_SRC = REPO_ROOT / "src" / "mcp-server"

if str(MCP_SRC) not in sys.path:
    sys.path.insert(0, str(MCP_SRC))

ENTITY = "http://example.org/Customer/CUST1"


def _facet(shape_id, status, **kw):
    facet = {
        "shape_id": shape_id,
        "label": shape_id,
        "category": "completeness",
        "severity": "sh:Violation",
        "target_class": "Customer",
        "inherited_from": None,
        "property_path": "",
        "constraint": "",
        "status": status,
        "values": [],
        "values_truncated": False,
        "details": [],
        "message": "",
    }
    facet.update(kw)
    return facet


def _payload(facets, **summary):
    counts = {"pass": 0, "fail": 0, "not_applicable": 0, "not_evaluated": 0, "error": 0}
    counts.update(summary)
    return {
        "success": True,
        "entity_uri": ENTITY,
        "entity_local_id": "CUST1",
        "entity_label": "ACME Corp",
        "classes": [{"name": "Customer", "uri": "http://example.org/Customer"}],
        "lineage": ["Customer", "Party"],
        "summary": {"total": len(facets), **counts,
                    "fail_by_severity": {"sh:Violation": 0, "sh:Warning": 0, "sh:Info": 0}},
        "facets": facets,
    }


@pytest.fixture(scope="module")
def fmt():
    try:
        from server.app import (  # type: ignore[import-not-found]
            _format_class_context_block,
            _format_node_context_response,
            _format_shacl_facets_response,
        )
    except ImportError as exc:
        pytest.fail(f"SHACL facet formatting is not implemented yet: {exc}")
    return _format_shacl_facets_response, _format_node_context_response, _format_class_context_block


class TestFacetsResponse:
    def test_header_names_the_entity_and_class(self, fmt):
        format_facets, _, _ = fmt
        text = format_facets(_payload([]))

        assert "SHACL Facets — CUST1  (Customer)" in text

    def test_failure_states_constraint_severity_and_observed_values(self, fmt):
        format_facets, _, _ = fmt
        text = format_facets(_payload(
            [_facet("Customer.email must not be empty", "fail",
                    property_path="email", constraint="sh:minCount 1")],
            fail=1,
        ))

        assert "FAIL  [Violation]  Customer.email must not be empty" in text
        assert "sh:minCount 1" in text
        assert "found: (none)" in text

    def test_observed_values_are_listed_and_truncation_is_stated(self, fmt):
        format_facets, _, _ = fmt
        text = format_facets(_payload(
            [_facet("Phone must start with +", "fail", severity="sh:Warning",
                    property_path="phone", constraint="sh:pattern ^\\+",
                    values=["06 12 34 56 78"], values_truncated=True)],
            fail=1,
        ))

        assert "FAIL  [Warning]" in text
        assert 'found: "06 12 34 56 78"' in text
        assert "(more values not shown)" in text

    def test_inherited_shape_names_its_ancestor(self, fmt):
        format_facets, _, _ = fmt
        text = format_facets(_payload(
            [_facet("Party.name must not be empty", "pass", inherited_from="Party")],
            **{"pass": 1},
        ))

        assert "inherited from Party" in text

    def test_unchecked_shapes_are_never_reported_as_passing(self, fmt):
        """A model reading 'PASS' for a shape that never ran would vouch for
        data nobody checked."""
        format_facets, _, _ = fmt
        text = format_facets(_payload(
            [_facet("Closed shape", "not_evaluated"),
             _facet("Broken regex", "error", message="[INVALID_PARAMETER_VALUE] bad regex")],
            not_evaluated=1, error=1,
        ))

        assert "NOT EVALUATED  Closed shape" in text
        assert "ERROR  Broken regex" in text
        assert "bad regex" in text
        assert "PASS" not in text

    def test_summary_line_counts_each_status(self, fmt):
        format_facets, _, _ = fmt
        text = format_facets(_payload(
            [_facet("a", "fail"), _facet("b", "pass"), _facet("c", "not_applicable")],
            fail=1, **{"pass": 1}, not_applicable=1,
        ))

        assert "1 failed, 1 passed, 1 not applicable, 0 not evaluated, 0 errors" in text

    def test_no_applicable_shape_is_stated(self, fmt):
        format_facets, _, _ = fmt
        text = format_facets(_payload([]))

        assert "No SHACL shape applies to Customer" in text

    def test_backend_failure_returns_its_message(self, fmt):
        format_facets, _, _ = fmt

        assert format_facets({"success": False, "message": "Graph store unavailable"}) == (
            "Graph store unavailable"
        )


class TestContextHint:
    def test_node_context_names_the_count_and_the_tool(self, fmt):
        _, format_node, _ = fmt
        text = format_node({
            "success": True, "entity_uri": ENTITY, "entity_local_id": "CUST1",
            "class_name": "Customer", "shacl_shape_count": 3,
        })

        assert "SHACL shapes (3)" in text
        assert "check_entity_shacl_facets(entity_uri)" in text

    def test_node_context_without_shapes_has_no_hint(self, fmt):
        _, format_node, _ = fmt
        text = format_node({
            "success": True, "entity_uri": ENTITY, "entity_local_id": "CUST1",
            "class_name": "Customer", "shacl_shape_count": 0,
        })

        assert "SHACL" not in text

    def test_class_with_only_shapes_still_renders_a_context_block(self, fmt):
        """``describe_entity`` reads the class listing; a class whose only
        attachment is its shapes must not fall through the early return."""
        _, _, format_class = fmt
        text = format_class(
            "CUST1",
            {"name": "Customer", "dataset": None, "bridges": [], "actions": [],
             "shacl_shape_count": 2},
        )

        assert "SHACL shapes (2)" in text
        assert "check_entity_shacl_facets(entity_uri)" in text
