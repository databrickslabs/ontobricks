"""Tier-3 self-baseline regression: store a GREEN run, then a worse run REDs."""

from agents.pge_eval.baseline import load_baseline, save_scorecard
from agents.pge_eval.scorecard import score_artifact

from tests.units.pge_eval import _fixtures as fx


def test_worse_run_regresses_against_stored_baseline(tmp_path):
    baseline_dir = str(tmp_path / "goals")

    # First run: clean -> GREEN, stored as the baseline.
    first = score_artifact(
        fx.clean_artifact(),
        no_judge=True,
        baseline_dir=baseline_dir,
        run_id="run-001",
        timestamp="2026-06-10T00:00:00Z",
    )
    assert first["verdict"] == "GREEN"
    save_scorecard(first, baseline_dir)

    # A GREEN baseline is now discoverable.
    base = load_baseline(baseline_dir, exclude_run_id="run-002")
    assert base is not None and base["run_id"] == "run-001"

    # Second run: a worse artifact (entity completeness drops) scored against
    # the stored baseline -> Tier-3 regression -> RED on the regressed metric.
    worse = fx.clean_artifact()
    for entry in worse["mapping_run_log"]:
        if entry["item"] in ("ex:Order", "ex:Product"):
            entry["final_status"] = "FAIL"
    second = score_artifact(
        worse,
        no_judge=True,
        baseline_dir=baseline_dir,
        run_id="run-002",
        timestamp="2026-06-10T01:00:00Z",
    )
    assert second["verdict"] == "RED"
    regressions = second["gates"]["tier3_regression"]["regressions"]
    assert second["gates"]["tier3_regression"]["baseline_run_id"] == "run-001"
    assert any(r["metric"] == "mapping.entity_completeness" for r in regressions)


def test_red_run_does_not_become_baseline(tmp_path):
    baseline_dir = str(tmp_path / "goals")
    red = score_artifact(
        fx.artifact_with_dangling_fk(),
        no_judge=True,
        baseline_dir=baseline_dir,
        run_id="red-001",
        timestamp="2026-06-10T00:00:00Z",
        use_baseline=False,
    )
    assert red["verdict"] == "RED"
    save_scorecard(red, baseline_dir)
    # RED runs are never selected as a baseline.
    assert load_baseline(baseline_dir) is None
