"""Tests for the deterministic (stage-1) evaluator of the mapping PGE pipeline.

The evaluator is a pure function: it takes a submitted mapping plus an
injectable ``execute_sql_fn`` and returns an ``EvalReport`` summarising
structural failures.  No LLM, no Databricks connection.

``execute_sql_fn`` contract (for the evaluator):
    def execute_sql_fn(sql: str) -> dict
returning::

    {"columns": [...], "rows": [{col: value, ...}, ...]}

This is the full result set — not the 3-row sample emitted by
``agents.tools.sql.tool_execute_sql``.  The PGE orchestrator (Sprint 7) is
responsible for wiring a runner that yields full rows.
"""

from typing import Dict, List

import pytest

from agents.agent_mapping_pge.contracts import EvalFailure, EvalReport
from agents.agent_mapping_pge.evaluator.deterministic import (
    evaluate_entity_mapping,
    evaluate_relationship_mapping,
)
from agents.agent_mapping_pge.evaluator.report import build_report


# =====================================================
# Fixtures
# =====================================================


MOTHER_CLASS = {
    "uri": "http://ex.org/maternity#Mother",
    "name": "Mother",
    "attributes": [
        {"name": "firstName"},
        {"name": "lastName"},
        {"name": "nhsNumber"},
    ],
}

BABY_CLASS = {
    "uri": "http://ex.org/maternity#Baby",
    "name": "Baby",
    "attributes": [
        {"name": "birthWeight"},
    ],
}


def _mother_mapping(*, attribute_mappings=None, unmapped_attributes=None):
    mapping = {
        "ontology_class": MOTHER_CLASS["uri"],
        "class_name": "Mother",
        "sql_query": "SELECT nhs_number AS ID, full_name AS Label, first_name, last_name, nhs_number FROM cat.sch.mothers",
        "id_column": "ID",
        "label_column": "Label",
        "attribute_mappings": attribute_mappings
        if attribute_mappings is not None
        else {
            "firstName": "first_name",
            "lastName": "last_name",
            "nhsNumber": "nhs_number",
        },
    }
    if unmapped_attributes is not None:
        mapping["unmapped_attributes"] = unmapped_attributes
    return mapping


def _baby_mapping():
    return {
        "ontology_class": BABY_CLASS["uri"],
        "class_name": "Baby",
        "sql_query": "SELECT baby_id AS ID, baby_id AS Label, birth_weight FROM cat.sch.babies",
        "id_column": "ID",
        "label_column": "Label",
        "attribute_mappings": {"birthWeight": "birth_weight"},
    }


def _mother_to_baby_relationship():
    return {
        "property": "http://ex.org/maternity#hasBaby",
        "property_name": "hasBaby",
        "sql_query": (
            "SELECT mother_nhs AS source_id, baby_id AS target_id "
            "FROM cat.sch.babies"
        ),
        "source_id_column": "source_id",
        "target_id_column": "target_id",
        "source_class": MOTHER_CLASS["uri"],
        "target_class": BABY_CLASS["uri"],
    }


def _make_sql_fn(table: dict):
    """Return an execute_sql_fn closure that routes by SQL substring.

    ``table`` maps a unique substring -> {"columns": [...], "rows": [...]}.
    """

    def fn(sql: str) -> dict:
        for needle, payload in table.items():
            if needle in sql:
                return payload
        raise AssertionError(f"unexpected SQL in test: {sql}")

    return fn


# =====================================================
# Entity evaluator
# =====================================================


class TestEvaluateEntityMapping:
    def test_pass_happy_path(self):
        mapping = _mother_mapping()
        sql_fn = _make_sql_fn(
            {
                "mothers": {
                    "columns": ["ID", "Label", "first_name", "last_name", "nhs_number"],
                    "rows": [
                        {
                            "ID": "NHS-001",
                            "Label": "Alice Smith",
                            "first_name": "Alice",
                            "last_name": "Smith",
                            "nhs_number": "NHS-001",
                        },
                        {
                            "ID": "NHS-002",
                            "Label": "Bob Jones",
                            "first_name": "Bob",
                            "last_name": "Jones",
                            "nhs_number": "NHS-002",
                        },
                    ],
                }
            }
        )

        report = evaluate_entity_mapping(
            mapping=mapping,
            ontology_class=MOTHER_CLASS,
            execute_sql_fn=sql_fn,
        )

        assert isinstance(report, EvalReport)
        assert report.status == "PASS"
        assert report.stage == "deterministic"
        assert report.failures == []
        assert report.bubble_to_planner is False
        assert report.metrics["row_count"] == 2
        assert report.metrics["distinct_id_count"] == 2
        assert report.metrics["null_id_count"] == 0
        assert report.metrics["unmapped_attribute_pct"] == 0.0

    def test_fail_row_count_zero_bubbles_to_planner(self):
        mapping = _mother_mapping()
        sql_fn = _make_sql_fn(
            {"mothers": {"columns": ["ID", "Label"], "rows": []}}
        )

        report = evaluate_entity_mapping(
            mapping=mapping,
            ontology_class=MOTHER_CLASS,
            execute_sql_fn=sql_fn,
        )

        assert report.status == "FAIL"
        assert report.bubble_to_planner is True
        check_names = [f.check for f in report.failures]
        assert "row_count" in check_names

    def test_fail_duplicate_ids(self):
        mapping = _mother_mapping()
        sql_fn = _make_sql_fn(
            {
                "mothers": {
                    "columns": ["ID", "Label", "first_name", "last_name", "nhs_number"],
                    "rows": [
                        {
                            "ID": "NHS-001",
                            "Label": "Alice",
                            "first_name": "Alice",
                            "last_name": "Smith",
                            "nhs_number": "NHS-001",
                        },
                        {
                            "ID": "NHS-001",
                            "Label": "Alice dup",
                            "first_name": "Alice",
                            "last_name": "Smith",
                            "nhs_number": "NHS-001",
                        },
                    ],
                }
            }
        )

        report = evaluate_entity_mapping(
            mapping=mapping,
            ontology_class=MOTHER_CLASS,
            execute_sql_fn=sql_fn,
        )

        assert report.status == "FAIL"
        assert report.bubble_to_planner is False
        check_names = [f.check for f in report.failures]
        assert "distinct_id_count" in check_names

    def test_fail_null_ids(self):
        mapping = _mother_mapping()
        sql_fn = _make_sql_fn(
            {
                "mothers": {
                    "columns": ["ID", "Label", "first_name", "last_name", "nhs_number"],
                    "rows": [
                        {
                            "ID": None,
                            "Label": "Alice",
                            "first_name": "Alice",
                            "last_name": "Smith",
                            "nhs_number": None,
                        },
                        {
                            "ID": "NHS-002",
                            "Label": "Bob",
                            "first_name": "Bob",
                            "last_name": "Jones",
                            "nhs_number": "NHS-002",
                        },
                    ],
                }
            }
        )

        report = evaluate_entity_mapping(
            mapping=mapping,
            ontology_class=MOTHER_CLASS,
            execute_sql_fn=sql_fn,
        )

        assert report.status == "FAIL"
        check_names = [f.check for f in report.failures]
        assert "null_id_count" in check_names

    def test_fail_unmapped_attribute(self):
        # Omit lastName from attribute_mappings, no unmapped_attributes list.
        mapping = _mother_mapping(
            attribute_mappings={
                "firstName": "first_name",
                "nhsNumber": "nhs_number",
            },
        )
        sql_fn = _make_sql_fn(
            {
                "mothers": {
                    "columns": ["ID", "Label", "first_name", "last_name", "nhs_number"],
                    "rows": [
                        {
                            "ID": "NHS-001",
                            "Label": "Alice",
                            "first_name": "Alice",
                            "last_name": "Smith",
                            "nhs_number": "NHS-001",
                        },
                    ],
                }
            }
        )

        report = evaluate_entity_mapping(
            mapping=mapping,
            ontology_class=MOTHER_CLASS,
            execute_sql_fn=sql_fn,
        )

        assert report.status == "FAIL"
        check_names = [f.check for f in report.failures]
        assert "unmapped_attribute_pct" in check_names
        # 1 of 3 attributes missing -> ~0.333
        assert report.metrics["unmapped_attribute_pct"] == pytest.approx(1 / 3)

    def test_pass_when_unmapped_attribute_is_declared(self):
        mapping = _mother_mapping(
            attribute_mappings={
                "firstName": "first_name",
                "nhsNumber": "nhs_number",
            },
            unmapped_attributes=["lastName"],
        )
        sql_fn = _make_sql_fn(
            {
                "mothers": {
                    "columns": ["ID", "Label", "first_name", "last_name", "nhs_number"],
                    "rows": [
                        {
                            "ID": "NHS-001",
                            "Label": "Alice",
                            "first_name": "Alice",
                            "last_name": "Smith",
                            "nhs_number": "NHS-001",
                        },
                    ],
                }
            }
        )

        report = evaluate_entity_mapping(
            mapping=mapping,
            ontology_class=MOTHER_CLASS,
            execute_sql_fn=sql_fn,
        )

        assert report.status == "PASS"
        assert report.metrics["unmapped_attribute_pct"] == 0.0

    def test_pass_when_unmapped_attribute_is_declared_as_dict(self):
        """The Generator may emit unmapped_attributes as [{name, reason}, ...].
        Hashing dicts would crash the evaluator — names must be extracted."""
        mapping = _mother_mapping(
            attribute_mappings={
                "firstName": "first_name",
                "nhsNumber": "nhs_number",
            },
            unmapped_attributes=[
                {"name": "lastName", "reason": "no source column"}
            ],
        )
        sql_fn = _make_sql_fn(
            {
                "mothers": {
                    "columns": ["ID", "Label", "first_name", "last_name", "nhs_number"],
                    "rows": [
                        {
                            "ID": "NHS-001",
                            "Label": "Alice",
                            "first_name": "Alice",
                            "last_name": "Smith",
                            "nhs_number": "NHS-001",
                        },
                    ],
                }
            }
        )

        report = evaluate_entity_mapping(
            mapping=mapping,
            ontology_class=MOTHER_CLASS,
            execute_sql_fn=sql_fn,
        )

        assert report.status == "PASS"
        assert report.metrics["unmapped_attribute_pct"] == 0.0

    def test_report_is_json_serialisable(self):
        mapping = _mother_mapping()
        sql_fn = _make_sql_fn(
            {
                "mothers": {
                    "columns": ["ID", "Label", "first_name", "last_name", "nhs_number"],
                    "rows": [
                        {
                            "ID": "NHS-001",
                            "Label": "Alice",
                            "first_name": "Alice",
                            "last_name": "Smith",
                            "nhs_number": "NHS-001",
                        },
                    ],
                }
            }
        )

        report = evaluate_entity_mapping(
            mapping=mapping,
            ontology_class=MOTHER_CLASS,
            execute_sql_fn=sql_fn,
        )
        d = report.to_dict()
        assert d["status"] == "PASS"
        assert d["stage"] == "deterministic"
        assert isinstance(d["metrics"], dict)
        assert isinstance(d["failures"], list)


# =====================================================
# Relationship evaluator
# =====================================================


def _entity_rows(ids):
    return {
        "columns": ["ID", "Label"],
        "rows": [{"ID": i, "Label": i} for i in ids],
    }


class TestEvaluateRelationshipMapping:
    def test_pass_happy_path(self):
        rel = _mother_to_baby_relationship()
        sql_fn = _make_sql_fn(
            {
                # Relationship edges
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": [
                        {"source_id": "NHS-001", "target_id": "B-1"},
                        {"source_id": "NHS-002", "target_id": "B-2"},
                    ],
                },
                # Source entity universe
                "mothers": _entity_rows(["NHS-001", "NHS-002", "NHS-003"]),
                # Target entity universe
                "babies": _entity_rows(["B-1", "B-2", "B-3"]),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
        )

        assert report.status == "PASS"
        assert report.bubble_to_planner is False
        assert report.metrics["total_edges"] == 2
        assert report.metrics["dangling_source_pct"] == 0.0
        assert report.metrics["dangling_target_pct"] == 0.0

    def test_fail_47_pct_dangling_source_bubbles(self):
        rel = _mother_to_baby_relationship()
        # 100 edges, 47 source_ids unknown to source universe.
        edge_rows = [
            {"source_id": f"NHS-{i:03d}", "target_id": f"B-{i}"}
            for i in range(1, 101)
        ]
        # Only NHS-001..NHS-053 exist as mothers.
        mother_ids = [f"NHS-{i:03d}" for i in range(1, 54)]
        baby_ids = [f"B-{i}" for i in range(1, 201)]

        sql_fn = _make_sql_fn(
            {
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": edge_rows,
                },
                "mothers": _entity_rows(mother_ids),
                "babies": _entity_rows(baby_ids),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
        )

        assert report.status == "FAIL"
        assert report.bubble_to_planner is False  # 0.47 < 0.5 threshold
        check_names = [f.check for f in report.failures]
        assert "dangling_source_pct" in check_names
        assert report.metrics["dangling_source_pct"] == pytest.approx(0.47)

    def test_fail_above_50_pct_dangling_source_bubbles_to_planner(self):
        rel = _mother_to_baby_relationship()
        edge_rows = [
            {"source_id": f"NHS-{i:03d}", "target_id": f"B-{i}"}
            for i in range(1, 101)
        ]
        # Only NHS-001..NHS-040 are known mothers -> 60% dangling
        mother_ids = [f"NHS-{i:03d}" for i in range(1, 41)]
        baby_ids = [f"B-{i}" for i in range(1, 201)]

        sql_fn = _make_sql_fn(
            {
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": edge_rows,
                },
                "mothers": _entity_rows(mother_ids),
                "babies": _entity_rows(baby_ids),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
        )

        assert report.status == "FAIL"
        assert report.bubble_to_planner is True

    def test_pass_3_pct_dangling_source_under_threshold(self):
        rel = _mother_to_baby_relationship()
        # 100 edges, only 3 source ids not in mother universe -> 3%.
        edge_rows = [
            {"source_id": f"NHS-{i:03d}", "target_id": f"B-{i}"}
            for i in range(1, 101)
        ]
        mother_ids = [f"NHS-{i:03d}" for i in range(1, 98)]  # 97 known, 3 dangling
        baby_ids = [f"B-{i}" for i in range(1, 201)]

        sql_fn = _make_sql_fn(
            {
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": edge_rows,
                },
                "mothers": _entity_rows(mother_ids),
                "babies": _entity_rows(baby_ids),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
        )

        assert report.status == "PASS"
        assert report.bubble_to_planner is False
        assert report.metrics["dangling_source_pct"] == pytest.approx(0.03)

    def test_fail_zero_edges_bubbles_to_planner(self):
        rel = _mother_to_baby_relationship()
        sql_fn = _make_sql_fn(
            {
                "source_id": {"columns": ["source_id", "target_id"], "rows": []},
                "mothers": _entity_rows(["NHS-001"]),
                "babies": _entity_rows(["B-1"]),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
        )

        assert report.status == "FAIL"
        assert report.bubble_to_planner is True
        check_names = [f.check for f in report.failures]
        assert "total_edges" in check_names

    def test_cross_source_band_fail_when_outside(self):
        rel = _mother_to_baby_relationship()
        # 100 edges, all source ids in mother universe.
        edge_rows = [
            {"source_id": f"NHS-{i:03d}", "target_id": f"B-{i}"}
            for i in range(1, 101)
        ]
        sql_fn = _make_sql_fn(
            {
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": edge_rows,
                },
                "mothers": _entity_rows([f"NHS-{i:03d}" for i in range(1, 101)]),
                "babies": _entity_rows([f"B-{i}" for i in range(1, 101)]),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
            expected_cross_source_overlap_band=(0.25, 0.4),
        )
        # overlap_pct = 1.0 (every source row matches a target id); outside band.
        assert report.status == "FAIL"
        check_names = [f.check for f in report.failures]
        assert "cross_source_overlap_pct" in check_names

    def test_cross_source_band_pass_when_inside(self):
        rel = _mother_to_baby_relationship()
        # Build edges where only ~30% of source ids match a target id (band 0.25..0.4 ).
        edge_rows = []
        for i in range(1, 101):
            edge_rows.append(
                {
                    "source_id": f"NHS-{i:03d}",
                    "target_id": f"B-{i}" if i <= 30 else f"X-{i}",
                }
            )
        sql_fn = _make_sql_fn(
            {
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": edge_rows,
                },
                "mothers": _entity_rows([f"NHS-{i:03d}" for i in range(1, 101)]),
                "babies": _entity_rows([f"B-{i}" for i in range(1, 101)]),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
            expected_cross_source_overlap_band=(0.25, 0.4),
        )
        # overlap = 30/100 = 0.3, inside band.
        assert report.status == "PASS"
        assert report.metrics["cross_source_overlap_pct"] == pytest.approx(0.3)

    def test_band_present_overlap_outside_band_with_catastrophic_dangling_bubbles(self):
        """Band FAILS (overlap 0.05 << lo=0.25) AND dangling > 0.5 → bubble.

        The realised overlap is materially worse than the Planner predicted,
        so the catastrophic-dangling structural failure fires alongside the
        band-check failure, and ``bubble_to_planner`` flips True.
        """
        rel = _mother_to_baby_relationship()
        # 100 edges, only the first 5 target_ids land in the babies universe
        # → overlap = 0.05, dangling_target = 0.95.
        edge_rows = []
        for i in range(1, 101):
            edge_rows.append(
                {
                    "source_id": f"NHS-{i:03d}",
                    "target_id": f"B-{i}" if i <= 5 else f"X-{i}",
                }
            )
        sql_fn = _make_sql_fn(
            {
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": edge_rows,
                },
                "mothers": _entity_rows([f"NHS-{i:03d}" for i in range(1, 101)]),
                "babies": _entity_rows([f"B-{i}" for i in range(1, 101)]),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
            expected_cross_source_overlap_band=(0.25, 0.4),
        )

        assert report.status == "FAIL"
        assert report.bubble_to_planner is True
        assert report.metrics["dangling_target_pct"] == pytest.approx(0.95)
        check_names = [f.check for f in report.failures]
        # Both the band failure AND the catastrophic-dangling row must surface.
        assert "cross_source_overlap_pct" in check_names
        assert "dangling_target_pct_catastrophic" in check_names
        # The strict 0.05 dangling_target_pct row is gated behind "band is None"
        # — it must NOT appear here.
        assert "dangling_target_pct" not in check_names

    def test_band_present_overlap_outside_band_with_mild_dangling_does_not_bubble(self):
        """Band FAILS but dangling is exactly at the bubble threshold (not > 0.5)
        → status FAIL on the band row but ``bubble_to_planner`` stays False.
        """
        rel = _mother_to_baby_relationship()
        # 100 edges, 50 land in target universe → overlap = 0.50, dangling = 0.50.
        # Band is (0.6, 0.8) so band check fails (0.50 < 0.6); dangling NOT > 0.5.
        edge_rows = []
        for i in range(1, 101):
            edge_rows.append(
                {
                    "source_id": f"NHS-{i:03d}",
                    "target_id": f"B-{i}" if i <= 50 else f"X-{i}",
                }
            )
        sql_fn = _make_sql_fn(
            {
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": edge_rows,
                },
                "mothers": _entity_rows([f"NHS-{i:03d}" for i in range(1, 101)]),
                "babies": _entity_rows([f"B-{i}" for i in range(1, 101)]),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
            expected_cross_source_overlap_band=(0.6, 0.8),
        )

        assert report.status == "FAIL"
        assert report.bubble_to_planner is False
        assert report.metrics["dangling_target_pct"] == pytest.approx(0.5)
        check_names = [f.check for f in report.failures]
        assert "cross_source_overlap_pct" in check_names
        # No catastrophic row because dangling is not strictly > 0.5.
        assert "dangling_target_pct_catastrophic" not in check_names

    def test_relationship_evaluator_uses_id_universe_cache(self):
        """Sharing a cache across calls avoids re-running the entity SQLs."""
        rel = _mother_to_baby_relationship()
        base_fn = _make_sql_fn(
            {
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": [
                        {"source_id": "NHS-001", "target_id": "B-1"},
                        {"source_id": "NHS-002", "target_id": "B-2"},
                    ],
                },
                "mothers": _entity_rows(["NHS-001", "NHS-002", "NHS-003"]),
                "babies": _entity_rows(["B-1", "B-2", "B-3"]),
            }
        )

        calls: List[str] = []

        def counting_fn(sql: str) -> dict:
            calls.append(sql)
            return base_fn(sql)

        cache: Dict[str, set] = {}

        # First call: source + target entity SQLs + relationship SQL = 3 calls.
        evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=counting_fn,
            id_universe_cache=cache,
        )
        first_call_count = len(calls)
        assert first_call_count == 3

        mother_sql = _mother_mapping()["sql_query"]
        baby_sql = _baby_mapping()["sql_query"]
        assert mother_sql in cache
        assert baby_sql in cache

        # Second call with same cache: only the relationship SQL should be
        # re-executed; both entity universes are served from cache.
        evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=counting_fn,
            id_universe_cache=cache,
        )

        delta = calls[first_call_count:]
        assert len(delta) == 1
        assert mother_sql not in delta
        assert baby_sql not in delta

    def test_band_absent_catastrophic_target_dangling_bubbles(self):
        """No band supplied + dangling_target > 0.5 → strict check fires and bubbles."""
        rel = _mother_to_baby_relationship()
        # 100 edges, only 20 target_ids land in babies universe → dangling = 0.80.
        edge_rows = []
        for i in range(1, 101):
            edge_rows.append(
                {
                    "source_id": f"NHS-{i:03d}",
                    "target_id": f"B-{i}" if i <= 20 else f"X-{i}",
                }
            )
        sql_fn = _make_sql_fn(
            {
                "source_id": {
                    "columns": ["source_id", "target_id"],
                    "rows": edge_rows,
                },
                "mothers": _entity_rows([f"NHS-{i:03d}" for i in range(1, 101)]),
                "babies": _entity_rows([f"B-{i}" for i in range(1, 101)]),
            }
        )

        report = evaluate_relationship_mapping(
            mapping=rel,
            source_entity_mapping=_mother_mapping(),
            target_entity_mapping=_baby_mapping(),
            execute_sql_fn=sql_fn,
        )

        assert report.status == "FAIL"
        assert report.bubble_to_planner is True
        assert report.metrics["dangling_target_pct"] == pytest.approx(0.8)
        check_names = [f.check for f in report.failures]
        assert "dangling_target_pct" in check_names


# =====================================================
# build_report — bubble demotion warning
# =====================================================


def test_build_report_warns_when_bubble_demoted(caplog):
    """``bubble_to_planner=True`` with no failures (status PASS) should
    emit a warning, AND silently-PASSing reports should not warn.
    """
    import logging

    # PASS + bubble_to_planner=True → warning expected, bubble demoted.
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        passing = build_report(
            stage="deterministic",
            metrics={"row_count": 1},
            failures=[],
            bubble_to_planner=True,
        )
    assert passing.status == "PASS"
    assert passing.bubble_to_planner is False
    assert any(
        "bubble_to_planner=True" in rec.message and rec.levelname == "WARNING"
        for rec in caplog.records
    )

    # PASS + bubble_to_planner=False → no warning.
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        build_report(
            stage="deterministic",
            metrics={"row_count": 1},
            failures=[],
            bubble_to_planner=False,
        )
    assert not any(
        "bubble_to_planner=True" in rec.message for rec in caplog.records
    )

    # FAIL + bubble_to_planner=True → no demotion, no warning.
    caplog.clear()
    failure = EvalFailure(
        kind="structural",
        check="row_count",
        expected="> 0",
        observed="0",
        hint="",
    )
    with caplog.at_level(logging.WARNING):
        failing = build_report(
            stage="deterministic",
            metrics={"row_count": 0},
            failures=[failure],
            bubble_to_planner=True,
        )
    assert failing.status == "FAIL"
    assert failing.bubble_to_planner is True
    assert not any(
        "bubble_to_planner=True" in rec.message for rec in caplog.records
    )
