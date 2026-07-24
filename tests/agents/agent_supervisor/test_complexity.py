"""Unit tests for the deterministic complexity assessor + baseline routing."""

import json
import re
from pathlib import Path

import pytest

from agents.agent_supervisor.complexity import (
    COMPLEXITY_THRESHOLD,
    ComplexityAssessor,
    assess,
)

pytestmark = pytest.mark.unit

_BASELINE = (
    Path(__file__).resolve().parents[2]
    / "eval"
    / "datasets"
    / "agent_supervisor"
    / "baseline.jsonl"
)
_UC_SQL = (
    Path(__file__).resolve().parents[3]
    / "src"
    / "agents"
    / "agent_supervisor"
    / "uc_function.sql"
)


def test_single_table_small_ontology_is_simple():
    report = assess(
        {"tables": [{"name": "patients", "columns": ["id", "name", "dob"]}]},
        {"classes": [{"name": "Patient", "attributes": ["name", "dob"]}], "properties": []},
    )
    assert report.tier == "simple"
    assert report.recommended_engine == "simple"
    assert report.score < COMPLEXITY_THRESHOLD


def test_three_sources_sharing_key_is_complex():
    md = {
        "tables": [
            {"name": "trust_a", "columns": ["EPISODE_ID", "MOTHER_NHS_NO", "DELIVERY_DATE"]},
            {"name": "trust_b", "columns": ["pregnancy_id", "mother_nhs_no", "booking_date"]},
            {"name": "trust_c", "columns": ["event_id", "mother_nhs_number", "event_date"]},
        ]
    }
    onto = {
        "classes": [{"name": n} for n in ("Mother", "Baby", "Pregnancy", "Delivery", "Labour")],
        "properties": [{"name": "hasBaby", "domain": "Mother", "range": "Baby"}],
    }
    report = assess(md, onto)
    assert report.tier == "complex"
    assert report.recommended_engine == "pge"
    assert report.signals["cross_source"] > 0


def test_cross_source_zero_for_single_table():
    report = assess(
        {"tables": [{"name": "t", "columns": ["id", "x"]}]},
        {"classes": [{"name": "T"}], "properties": []},
    )
    assert report.signals["cross_source"] == 0.0


def test_assessment_is_deterministic():
    md = {"tables": [{"name": "a", "columns": ["id", "v"]}, {"name": "b", "columns": ["id", "w"]}]}
    onto = {"classes": [{"name": "A"}, {"name": "B"}], "properties": []}
    first = assess(md, onto).to_dict()
    for _ in range(5):
        assert assess(md, onto).to_dict() == first


def test_empty_inputs_do_not_crash():
    report = assess({}, {})
    assert report.tier == "simple"
    assert report.recommended_engine == "simple"


def test_report_is_json_serialisable():
    report = assess({"tables": [{"name": "t", "columns": ["id"]}]}, {"classes": [], "properties": []})
    json.dumps(report.to_dict())  # must not raise


def _load_baseline():
    rows = []
    with _BASELINE.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def test_baseline_dataset_has_min_examples():
    rows = _load_baseline()
    assert len(rows) >= 20, "eval-gate requires >= 20 examples for a new agent"


@pytest.mark.parametrize("row", _load_baseline(), ids=lambda r: r["id"])
def test_baseline_routing_accuracy(row):
    """Every baseline case must route to its expected engine (accuracy == 1.0)."""
    report = assess(row["input"]["metadata"], row["input"]["ontology"])
    assert report.recommended_engine == row["expected"]["recommended_engine"], (
        f"{row['id']}: got {report.recommended_engine} "
        f"(score={report.score:.3f}, signals={report.signals})"
    )


def test_uc_function_parity():
    """The UC function must embed the same weights/threshold as complexity.py."""
    from agents.agent_supervisor import complexity as c

    sql = _UC_SQL.read_text()
    # Constants line: W_TABLES, W_CLASSES, W_RELS, W_CROSS, W_HET = ...
    weights = re.search(
        r"W_TABLES,\s*W_CLASSES,\s*W_RELS,\s*W_CROSS,\s*W_HET\s*=\s*"
        r"([0-9.]+),\s*([0-9.]+),\s*([0-9.]+),\s*([0-9.]+),\s*([0-9.]+)",
        sql,
    )
    assert weights, "weights constant not found in uc_function.sql"
    got = [float(x) for x in weights.groups()]
    assert got == [
        c.WEIGHT_TABLES,
        c.WEIGHT_CLASSES,
        c.WEIGHT_RELATIONSHIPS,
        c.WEIGHT_CROSS_SOURCE,
        c.WEIGHT_HETEROGENEITY,
    ]
    threshold = re.search(r"THRESHOLD\s*=\s*([0-9.]+)", sql)
    assert threshold and float(threshold.group(1)) == c.COMPLEXITY_THRESHOLD
