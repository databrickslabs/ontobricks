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

import pytest

from agents.agent_mapping_pge.contracts import EvalReport
from agents.agent_mapping_pge.evaluator.deterministic import (
    evaluate_entity_mapping,
    evaluate_relationship_mapping,
)


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

    def test_cross_source_band_pass_inside(self):
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
