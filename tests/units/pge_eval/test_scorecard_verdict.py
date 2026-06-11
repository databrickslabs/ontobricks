"""End-to-end scorecard verdict + exit-code tests (§3.6)."""

import pytest

from agents.pge_eval.scorecard import score_artifact

from tests.units.pge_eval import _fixtures as fx


def _score(artifact, **kw):
    kw.setdefault("no_judge", True)
    kw.setdefault("use_baseline", False)
    kw.setdefault("run_id", "t")
    kw.setdefault("timestamp", "2026-06-10T00:00:00Z")
    return score_artifact(artifact, **kw)


def test_clean_artifact_is_green_exit_zero():
    sc = _score(fx.clean_artifact())
    assert sc["verdict"] == "GREEN"
    assert sc["exit_code"] == 0
    # All Stage-1, Stage-2, pipeline metrics populated.
    assert set(sc["stages"]) == {"ontology", "mapping", "pipeline"}
    assert sc["stages"]["ontology"]["metrics"]["orphan_class_count"] == 0
    assert sc["stages"]["mapping"]["metrics"]["id_integrity"] == 1.0
    assert "coverage_loss" in sc["stages"]["pipeline"]


def test_dangling_fk_artifact_is_red_exit_nonzero():
    sc = _score(fx.artifact_with_dangling_fk())
    assert sc["verdict"] == "RED"
    assert sc["exit_code"] != 0
    assert any(
        f["metric"] == "mapping.dangling_target_pct_max"
        for f in sc["gates"]["tier1_absolute"]["failures"]
    )


def test_orphan_class_artifact_is_red():
    sc = _score(fx.artifact_with_orphan_class())
    assert sc["verdict"] == "RED"
    assert any(
        f["metric"] == "ontology.orphan_class_count"
        for f in sc["gates"]["tier1_absolute"]["failures"]
    )


def test_sql_failure_artifact_is_red():
    sc = _score(fx.artifact_with_sql_failure())
    assert sc["verdict"] == "RED"
    assert any(
        f["metric"] == "mapping.sql_exec_failures"
        for f in sc["gates"]["tier1_absolute"]["failures"]
    )


def test_schema_version_and_digests_present():
    sc = _score(fx.clean_artifact())
    assert sc["schema_version"] == "1.0"
    assert sc["inputs"]["source_metadata_digest"]
    assert sc["inputs"]["ontology_digest"]
    assert sc["inputs"]["endpoint"] is None  # no_judge


def test_no_judge_makes_no_network_call(monkeypatch):
    """--no-judge must perform ZERO calls to the serving endpoint."""
    import agents.engine_base as eb

    def _boom(*a, **k):
        raise AssertionError("network call made despite --no-judge")

    monkeypatch.setattr(eb, "call_serving_endpoint", _boom)
    sc = _score(fx.clean_artifact(), no_judge=True)
    assert sc["stages"]["ontology"]["judge"]["score"] is None
    assert sc["stages"]["mapping"]["judge"]["score"] is None
