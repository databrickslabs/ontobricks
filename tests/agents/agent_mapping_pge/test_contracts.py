"""Smoke tests for the PGE contracts.

These are intentionally narrow — they only assert that every contract
dataclass round-trips cleanly through ``to_dict`` / ``from_dict`` so that
downstream sprints (Planner, Generator, orchestrator) can rely on
JSON-safe serialisation for MLflow artefacts and registry persistence.
"""

import json

from agents.agent_mapping_pge.contracts import (
    CanonicalId,
    EvalFailure,
    EvalReport,
    JoinKey,
    MappingPlan,
    RetryState,
    SkipItem,
    SourceModel,
    TableRole,
    TableRoleCandidate,
)


def _roundtrip(obj):
    """Serialise to dict -> JSON string -> dict -> reconstruct via from_dict."""
    cls = type(obj)
    d = obj.to_dict()
    encoded = json.dumps(d)
    back = cls.from_dict(json.loads(encoded))
    return back, d


def test_source_model_roundtrip():
    sm = SourceModel(
        table_roles=[
            TableRole(
                table="cat.sch.mothers",
                ontology_class_candidates=[
                    TableRoleCandidate(
                        uri="http://ex.org#Mother", confidence=0.92, reason="row match"
                    ),
                ],
            ),
        ],
        canonical_ids=[
            CanonicalId(
                ontology_class="http://ex.org#Mother",
                canonical_column_per_table={"cat.sch.mothers": "nhs_number"},
                format_note="NHS number, 10 digits, no separators",
            )
        ],
        join_keys=[
            JoinKey(
                from_ref="cat.sch.babies.mother_nhs",
                to_ref="cat.sch.mothers.nhs_number",
                confidence=0.88,
                overlap_pct=0.97,
                kind="same_trust_fk",
            )
        ],
        mapping_plan=MappingPlan(
            entity_order=["http://ex.org#Mother", "http://ex.org#Baby"],
            relationship_order=["http://ex.org#hasBaby"],
            skip=[SkipItem(item="http://ex.org#Ghost", reason="no source table")],
        ),
    )
    back, d = _roundtrip(sm)
    assert back.to_dict() == d
    assert back.table_roles[0].table == "cat.sch.mothers"
    assert back.canonical_ids[0].canonical_column_per_table["cat.sch.mothers"] == "nhs_number"
    assert back.join_keys[0].kind == "same_trust_fk"
    assert back.mapping_plan.skip[0].item == "http://ex.org#Ghost"


def test_eval_report_roundtrip():
    report = EvalReport(
        status="FAIL",
        stage="deterministic",
        metrics={"row_count": 0},
        failures=[
            EvalFailure(
                kind="structural",
                check="row_count",
                expected="> 0",
                observed="0",
                hint="fix the FROM clause",
            )
        ],
        bubble_to_planner=True,
    )
    back, d = _roundtrip(report)
    assert back.to_dict() == d
    assert back.status == "FAIL"
    assert back.failures[0].check == "row_count"


def test_retry_state_roundtrip_with_and_without_report():
    rs_empty = RetryState(item_uri="http://ex.org#Mother")
    back, d = _roundtrip(rs_empty)
    assert back.to_dict() == d
    assert back.last_eval_report is None

    rs = RetryState(
        item_uri="http://ex.org#Baby",
        generator_attempts=2,
        planner_reinvocations=1,
        last_eval_report=EvalReport(
            status="FAIL",
            stage="deterministic",
            failures=[
                EvalFailure(
                    kind="structural",
                    check="total_edges",
                    expected="> 0",
                    observed="0",
                    hint="fix join",
                )
            ],
            bubble_to_planner=True,
        ),
    )
    back, d = _roundtrip(rs)
    assert back.to_dict() == d
    assert back.last_eval_report is not None
    assert back.last_eval_report.failures[0].check == "total_edges"
