"""Tests for the Planner tool surface (Sprint 2 of mapping-PGE).

The four planner tools wrap Databricks SQL queries behind a uniform
JSON-string return contract suitable for an LLM function-calling loop:

* ``sample_table``         — `SELECT * ORDER BY RAND() LIMIT n` with n capped at 100.
* ``column_value_overlap`` — one-sided distinct-value overlap between two columns.
* ``distinct_count``       — row / distinct / null counts for a candidate canonical id.
* ``submit_source_model``  — terminal tool: validates a ``SourceModel`` dict and stashes
  the dataclass instance on the ``ToolContext``.

Tests use a ``FakeClient`` whose ``execute_query(sql)`` is parameterised per-test;
handlers are exercised at the JSON-string boundary (parse the return, assert keys).
No real Databricks connection, no LLM.
"""

import json
from typing import Any, Callable, Dict, List, Optional

import pytest

from agents.agent_mapping_pge.contracts import SourceModel
from agents.tools.context import ToolContext
from agents.tools.planner import (
    PLANNER_TOOL_DEFINITIONS,
    PLANNER_TOOL_HANDLERS,
    tool_column_value_overlap,
    tool_distinct_count,
    tool_normalized_value_overlap,
    tool_sample_table,
    tool_submit_source_model,
)


# =====================================================
# Fakes
# =====================================================


class FakeClient:
    """Minimal stand-in for ``DatabricksClient`` — records the last SQL
    executed and dispatches to a per-test ``handler`` closure.
    """

    def __init__(self, handler: Callable[[str], Any]):
        self._handler = handler
        self.calls: List[str] = []

    def execute_query(self, sql: str):
        self.calls.append(sql)
        result = self._handler(sql)
        if isinstance(result, Exception):
            raise result
        return result


def _ctx(handler: Callable[[str], Any]) -> ToolContext:
    return ToolContext(host="x", token="y", client=FakeClient(handler))


# =====================================================
# sample_table
# =====================================================


class TestSampleTable:
    def test_returns_stringified_rows_and_row_count(self):
        def handler(sql: str):
            assert "ORDER BY RAND()" in sql
            assert "LIMIT 20" in sql
            assert "cat.sch.t" in sql
            return [
                {"id": 1, "name": "alice", "age": None},
                {"id": 2, "name": "bob", "age": 42},
            ]

        ctx = _ctx(handler)
        out = json.loads(tool_sample_table(ctx, full_name="cat.sch.t"))

        assert out["success"] is True
        assert out["columns"] == ["id", "name", "age"]
        assert out["row_count"] == 2
        # Values stringified, nulls preserved as None.
        assert out["rows"] == [
            ["1", "alice", None],
            ["2", "bob", "42"],
        ]

    def test_caps_n_at_100_when_500_requested(self):
        captured = {}

        def handler(sql: str):
            captured["sql"] = sql
            return []

        ctx = _ctx(handler)
        out = json.loads(tool_sample_table(ctx, full_name="cat.sch.t", n=500))

        assert out["success"] is True
        assert "LIMIT 100" in captured["sql"]
        # Make sure we didn't smuggle 500 anywhere in the SQL.
        assert "500" not in captured["sql"]

    def test_default_n_is_20(self):
        captured = {}

        def handler(sql: str):
            captured["sql"] = sql
            return []

        ctx = _ctx(handler)
        tool_sample_table(ctx, full_name="cat.sch.t")
        assert "LIMIT 20" in captured["sql"]

    def test_catches_exception_returns_success_false(self):
        def handler(sql: str):
            raise RuntimeError("table not found")

        ctx = _ctx(handler)
        out = json.loads(tool_sample_table(ctx, full_name="cat.sch.missing"))

        assert out["success"] is False
        assert "table not found" in out["error"]

    def test_sample_table_rejects_invalid_full_name(self):
        """Identifier validator must catch SQL-injection-shaped names
        *before* any SQL is composed or executed.
        """

        def handler(sql: str):  # pragma: no cover — must not be called
            raise AssertionError("execute_query should not have been called")

        ctx = _ctx(handler)
        out = json.loads(tool_sample_table(ctx, full_name="t; DROP TABLE x"))

        assert out["success"] is False
        assert "invalid full_name" in out["error"]
        # Confirm no SQL ever reached the client.
        assert ctx.client.calls == []

    def test_sample_table_returns_error_on_non_integer_n(self):
        """Strict ``n`` parsing — a non-coercible value is a tool-call error,
        not a silent fallback to the default.
        """

        def handler(sql: str):  # pragma: no cover — must not be called
            raise AssertionError("execute_query should not have been called")

        ctx = _ctx(handler)
        out = json.loads(tool_sample_table(ctx, full_name="cat.sch.t", n="abc"))

        assert out["success"] is False
        assert "invalid n" in out["error"]
        assert ctx.client.calls == []


# =====================================================
# column_value_overlap
# =====================================================


class TestColumnValueOverlap:
    def test_happy_path_percentage(self):
        # 100 distinct from-values, 80 distinct to-values, 60 in intersection.
        def handler(sql: str):
            return [
                {
                    "from_distinct_count": 100,
                    "to_distinct_count": 80,
                    "intersection_count": 60,
                }
            ]

        ctx = _ctx(handler)
        out = json.loads(
            tool_column_value_overlap(
                ctx,
                from_table="cat.sch.a",
                from_column="nhs",
                to_table="cat.sch.b",
                to_column="nhs_number",
            )
        )

        assert out["success"] is True
        assert out["from_distinct_count"] == 100
        assert out["to_distinct_count"] == 80
        assert out["intersection_count"] == 60
        assert out["overlap_pct"] == pytest.approx(0.6)
        # Symmetric shape with the zero-denom branch: ``note`` is always
        # present so downstream consumers can read it unconditionally.
        assert "note" in out and out["note"] == ""

    def test_zero_from_distinct_no_division_by_zero(self):
        def handler(sql: str):
            return [
                {
                    "from_distinct_count": 0,
                    "to_distinct_count": 50,
                    "intersection_count": 0,
                }
            ]

        ctx = _ctx(handler)
        out = json.loads(
            tool_column_value_overlap(
                ctx,
                from_table="cat.sch.empty",
                from_column="x",
                to_table="cat.sch.b",
                to_column="y",
            )
        )

        assert out["success"] is True
        assert out["overlap_pct"] == 0.0
        assert out["from_distinct_count"] == 0
        # Note the surface so the LLM knows why pct is 0.
        assert "note" in out

    def test_catches_exception(self):
        def handler(sql: str):
            raise RuntimeError("permission denied")

        ctx = _ctx(handler)
        out = json.loads(
            tool_column_value_overlap(
                ctx,
                from_table="cat.sch.a",
                from_column="x",
                to_table="cat.sch.b",
                to_column="y",
            )
        )

        assert out["success"] is False
        assert "permission denied" in out["error"]

    def test_column_value_overlap_rejects_invalid_from_column(self):
        """Injection-shaped identifier on any of the four args short-circuits
        before SQL is composed.
        """

        def handler(sql: str):  # pragma: no cover — must not be called
            raise AssertionError("execute_query should not have been called")

        ctx = _ctx(handler)
        out = json.loads(
            tool_column_value_overlap(
                ctx,
                from_table="cat.sch.a",
                from_column="nhs FROM secrets--",
                to_table="cat.sch.b",
                to_column="y",
            )
        )

        assert out["success"] is False
        assert "invalid from_column" in out["error"]
        assert ctx.client.calls == []


# =====================================================
# normalized_value_overlap
# =====================================================


class TestNormalizedValueOverlap:
    def test_happy_path_interpolates_expressions(self):
        captured = {}

        def handler(sql: str):
            captured["sql"] = sql
            return [
                {
                    "from_distinct_count": 100,
                    "to_distinct_count": 40,
                    "intersection_count": 35,
                }
            ]

        ctx = _ctx(handler)
        out = json.loads(
            tool_normalized_value_overlap(
                ctx,
                from_table="cat.trust_a.maternity_episode",
                from_expr="regexp_extract(EPISODE_ID, '([a-f0-9][a-f0-9-]+-preg-\\d+)', 1)",
                to_table="cat.trust_b.delivery",
                to_expr="regexp_extract(delivery_id, '([a-f0-9][a-f0-9-]+-preg-\\d+)', 1)",
            )
        )

        assert out["success"] is True
        assert out["overlap_pct"] == pytest.approx(0.35)
        # The expressions reach the SQL verbatim (not stripped to columns).
        assert "regexp_extract(EPISODE_ID" in captured["sql"]
        assert "regexp_extract(delivery_id" in captured["sql"]

    def test_zero_distinct_surfaces_revise_note(self):
        def handler(sql: str):
            return [
                {
                    "from_distinct_count": 0,
                    "to_distinct_count": 40,
                    "intersection_count": 0,
                }
            ]

        ctx = _ctx(handler)
        out = json.loads(
            tool_normalized_value_overlap(
                ctx,
                from_table="cat.trust_a.t",
                from_expr="regexp_extract(EPISODE_ID, 'nomatch', 1)",
                to_table="cat.trust_b.t",
                to_expr="delivery_id",
            )
        )

        assert out["success"] is True
        assert out["overlap_pct"] == 0.0
        assert "revise" in out["note"].lower()

    def test_rejects_injection_in_expression(self):
        def handler(sql: str):  # pragma: no cover — must not be called
            raise AssertionError("execute_query should not have been called")

        ctx = _ctx(handler)
        out = json.loads(
            tool_normalized_value_overlap(
                ctx,
                from_table="cat.sch.a",
                from_expr="x) AS v FROM cat.sch.a; DROP TABLE secrets--",
                to_table="cat.sch.b",
                to_expr="y",
            )
        )

        assert out["success"] is False
        assert "invalid from_expr" in out["error"]
        assert ctx.client.calls == []

    def test_rejects_subquery_keyword_in_expression(self):
        def handler(sql: str):  # pragma: no cover — must not be called
            raise AssertionError("execute_query should not have been called")

        ctx = _ctx(handler)
        out = json.loads(
            tool_normalized_value_overlap(
                ctx,
                from_table="cat.sch.a",
                from_expr="(SELECT max(id) FROM cat.sch.other)",
                to_table="cat.sch.b",
                to_expr="y",
            )
        )

        assert out["success"] is False
        assert "invalid from_expr" in out["error"]
        assert ctx.client.calls == []

    def test_requires_all_four_args(self):
        ctx = _ctx(lambda sql: [])
        out = json.loads(
            tool_normalized_value_overlap(
                ctx, from_table="cat.sch.a", from_expr="x", to_table="cat.sch.b"
            )
        )
        assert out["success"] is False
        assert "required" in out["error"]


# =====================================================
# distinct_count
# =====================================================


class TestDistinctCount:
    def test_unique_and_complete(self):
        def handler(sql: str):
            return [{"row_count": 100, "distinct_count": 100, "null_count": 0}]

        ctx = _ctx(handler)
        out = json.loads(
            tool_distinct_count(ctx, full_name="cat.sch.mothers", column="nhs_number")
        )

        assert out["success"] is True
        assert out["row_count"] == 100
        assert out["distinct_count"] == 100
        assert out["null_count"] == 0
        assert out["is_unique"] is True
        assert out["is_complete"] is True

    def test_with_nulls_is_not_complete(self):
        # 100 rows, 10 nulls, 90 distinct values in the non-null subset.
        def handler(sql: str):
            return [{"row_count": 100, "distinct_count": 90, "null_count": 10}]

        ctx = _ctx(handler)
        out = json.loads(
            tool_distinct_count(ctx, full_name="cat.sch.t", column="maybe_nullable")
        )

        assert out["success"] is True
        assert out["null_count"] == 10
        assert out["is_complete"] is False
        # 90 distinct out of (100 - 10) = 90 non-null rows -> unique.
        assert out["is_unique"] is True

    def test_with_duplicates_is_not_unique(self):
        # 100 rows, no nulls, only 70 distinct -> duplicates exist.
        def handler(sql: str):
            return [{"row_count": 100, "distinct_count": 70, "null_count": 0}]

        ctx = _ctx(handler)
        out = json.loads(
            tool_distinct_count(ctx, full_name="cat.sch.t", column="trust_local_id")
        )

        assert out["success"] is True
        assert out["is_unique"] is False
        assert out["is_complete"] is True

    def test_catches_exception(self):
        def handler(sql: str):
            raise RuntimeError("column does not exist")

        ctx = _ctx(handler)
        out = json.loads(
            tool_distinct_count(ctx, full_name="cat.sch.t", column="missing")
        )

        assert out["success"] is False
        assert "column does not exist" in out["error"]

    def test_distinct_count_rejects_invalid_column(self):
        """Injection-shaped column name short-circuits before SQL runs."""

        def handler(sql: str):  # pragma: no cover — must not be called
            raise AssertionError("execute_query should not have been called")

        ctx = _ctx(handler)
        out = json.loads(
            tool_distinct_count(
                ctx, full_name="cat.sch.t", column="nhs; DROP TABLE x"
            )
        )

        assert out["success"] is False
        assert "invalid column" in out["error"]
        assert ctx.client.calls == []


# =====================================================
# submit_source_model
# =====================================================


def _valid_source_model_dict() -> Dict[str, Any]:
    return {
        "table_roles": [
            {
                "table": "cat.sch.mothers",
                "ontology_class_candidates": [
                    {
                        "uri": "http://ex.org/maternity#Mother",
                        "confidence": 0.92,
                        "reason": "row per NHS number",
                    }
                ],
            },
            {
                "table": "cat.sch.babies",
                "ontology_class_candidates": [
                    {
                        "uri": "http://ex.org/maternity#Baby",
                        "confidence": 0.88,
                        "reason": "row per delivery",
                    }
                ],
            },
        ],
        "canonical_ids": [
            {
                "ontology_class": "http://ex.org/maternity#Mother",
                "canonical_column_per_table": {"cat.sch.mothers": "nhs_number"},
                "format_note": "NHS number, 10 digits",
            }
        ],
        "join_keys": [
            {
                "from_ref": "cat.sch.babies.mother_nhs",
                "to_ref": "cat.sch.mothers.nhs_number",
                "confidence": 0.9,
                "overlap_pct": 0.95,
                "kind": "same_trust_fk",
            }
        ],
        "mapping_plan": {
            "entity_order": [
                "http://ex.org/maternity#Mother",
                "http://ex.org/maternity#Baby",
            ],
            "relationship_order": ["http://ex.org/maternity#hasBaby"],
            "skip": [],
        },
    }


class TestSubmitSourceModel:
    def test_valid_model_stores_and_returns_summary(self):
        ctx = ToolContext(host="x", token="y")
        model = _valid_source_model_dict()

        out = json.loads(tool_submit_source_model(ctx, model=model))

        assert out["success"] is True
        assert isinstance(ctx.source_model, SourceModel)
        assert len(ctx.source_model.table_roles) == 2
        assert ctx.source_model.canonical_ids[0].ontology_class == (
            "http://ex.org/maternity#Mother"
        )
        # Summary mirrors the dataclass shape.
        summary = out["summary"]
        assert summary["table_roles"] == 2
        assert summary["canonical_ids"] == 1
        assert summary["join_keys"] == 1
        assert summary["entity_order_len"] == 2
        assert summary["relationship_order_len"] == 1

    def test_malformed_missing_required_field_returns_failure(self):
        ctx = ToolContext(host="x", token="y")
        # ``TableRole.from_dict`` requires the ``table`` key — drop it.
        bad = _valid_source_model_dict()
        del bad["table_roles"][0]["table"]

        out = json.loads(tool_submit_source_model(ctx, model=bad))

        assert out["success"] is False
        assert isinstance(out["error"], str) and out["error"]
        # ctx.source_model unchanged (still None).
        assert ctx.source_model is None

    def test_empty_table_roles_is_still_stored(self):
        """Structural validity only — semantic emptiness is the
        orchestrator's call, not the tool layer's.
        """
        ctx = ToolContext(host="x", token="y")
        model = _valid_source_model_dict()
        model["table_roles"] = []

        out = json.loads(tool_submit_source_model(ctx, model=model))

        assert out["success"] is True
        assert isinstance(ctx.source_model, SourceModel)
        assert ctx.source_model.table_roles == []
        assert out["summary"]["table_roles"] == 0


# =====================================================
# Aggregate exports
# =====================================================


class TestPlannerExports:
    _EXPECTED_TOOLS = {
        "sample_table",
        "column_value_overlap",
        "normalized_value_overlap",
        "distinct_count",
        "submit_source_model",
    }

    def test_definitions_cover_all_tools(self):
        names = {d["function"]["name"] for d in PLANNER_TOOL_DEFINITIONS}
        assert names == self._EXPECTED_TOOLS

    def test_handlers_match_definitions(self):
        assert set(PLANNER_TOOL_HANDLERS.keys()) == self._EXPECTED_TOOLS
        # All handlers must be callable.
        for fn in PLANNER_TOOL_HANDLERS.values():
            assert callable(fn)
