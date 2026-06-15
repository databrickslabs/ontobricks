"""Gate-tier tests (Tier-1 absolute, Tier-2 ratio, Tier-3 regression)."""

from agents.pge_eval import gates


def _stages(onto=None, mapping=None):
    base_onto = {
        "table_footprint_coverage": 1.0,
        "column_footprint_coverage": 1.0,
        "orphan_class_count": 0,
        "dangling_domain_range_count": 0,
        "naming_violation_count": 0,
        "duplicate_class_count": 0,
    }
    base_map = {
        "entity_completeness": 1.0,
        "relationship_completeness": 1.0,
        "attribute_coverage": 1.0,
        "dangling_target_pct_max": 0.0,
        "dangling_source_pct_max": 0.0,
        "id_integrity": 1.0,
        "sql_exec_failures": 0,
        "cross_source_band_compliance": 1.0,
    }
    base_onto.update(onto or {})
    base_map.update(mapping or {})
    return {
        "ontology": {"metrics": base_onto},
        "mapping": {"metrics": base_map},
    }


def test_tier1_passes_when_clean():
    res = gates.evaluate_tier1(_stages())
    assert res["passed"] is True
    assert res["failures"] == []


def test_tier1_fails_on_orphan():
    res = gates.evaluate_tier1(_stages(onto={"orphan_class_count": 2}))
    assert res["passed"] is False
    assert any(f["metric"] == "ontology.orphan_class_count" for f in res["failures"])


def test_tier1_fails_on_dangling_fk():
    res = gates.evaluate_tier1(_stages(mapping={"dangling_target_pct_max": 0.47}))
    assert res["passed"] is False


def test_tier1_fails_on_sql_exec():
    res = gates.evaluate_tier1(_stages(mapping={"sql_exec_failures": 1}))
    assert res["passed"] is False


def test_tier1_band_skipped_when_inactive():
    # band compliance < 1 but conditional inactive -> not a failure.
    res = gates.evaluate_tier1(
        _stages(mapping={"cross_source_band_compliance": 0.5}),
        active_conditionals={"band_active": False},
    )
    assert res["passed"] is True


def test_tier1_band_gated_when_active():
    res = gates.evaluate_tier1(
        _stages(mapping={"cross_source_band_compliance": 0.5}),
        active_conditionals={"band_active": True},
    )
    assert res["passed"] is False


def test_tier2_warns_but_does_not_gate_by_default():
    res = gates.evaluate_tier2(
        _stages(onto={"column_footprint_coverage": 0.5}), gate_ratios=False
    )
    assert res["passed"] is True  # warn only
    assert res["warnings"]


def test_tier2_gates_when_requested():
    res = gates.evaluate_tier2(
        _stages(onto={"column_footprint_coverage": 0.5}), gate_ratios=True
    )
    assert res["passed"] is False


def test_tier3_no_baseline_passes():
    res = gates.evaluate_tier3(_stages(), None)
    assert res["passed"] is True
    assert res["baseline_run_id"] is None


def test_tier3_detects_ratio_regression():
    baseline = {"run_id": "b1", "stages": _stages(mapping={"entity_completeness": 1.0})}
    current = _stages(mapping={"entity_completeness": 0.6})
    res = gates.evaluate_tier3(current, baseline)
    assert res["passed"] is False
    assert any(r["metric"] == "mapping.entity_completeness" for r in res["regressions"])


def test_tier3_detects_count_regression():
    baseline = {"run_id": "b1", "stages": _stages(onto={"orphan_class_count": 0})}
    current = _stages(onto={"orphan_class_count": 1})
    res = gates.evaluate_tier3(current, baseline)
    assert res["passed"] is False


def test_tier3_tolerance_absorbs_tiny_drop():
    baseline = {"run_id": "b1", "stages": _stages(mapping={"entity_completeness": 1.0})}
    current = _stages(mapping={"entity_completeness": 0.99})  # within 0.02 tol
    res = gates.evaluate_tier3(current, baseline)
    assert res["passed"] is True


def test_tier3_conditional_band_not_flagged_when_inactive():
    # Baseline had no band (inactive 1.0); current introduces a band < 1.0.
    # This is a first measurement, NOT a regression — must NOT flag.
    base = _stages(mapping={"cross_source_band_compliance": 1.0})
    base["mapping"]["band_active"] = False
    baseline = {"run_id": "b1", "stages": base}
    cur = _stages(mapping={"cross_source_band_compliance": 0.6})
    cur["mapping"]["band_active"] = True
    res = gates.evaluate_tier3(cur, baseline)
    assert res["passed"] is True
    assert not any(r["metric"].endswith("cross_source_band_compliance") for r in res["regressions"])


def test_tier3_conditional_band_flagged_when_active_in_both():
    base = _stages(mapping={"cross_source_band_compliance": 1.0})
    base["mapping"]["band_active"] = True
    baseline = {"run_id": "b1", "stages": base}
    cur = _stages(mapping={"cross_source_band_compliance": 0.6})
    cur["mapping"]["band_active"] = True
    res = gates.evaluate_tier3(cur, baseline)
    assert res["passed"] is False
    assert any(r["metric"].endswith("cross_source_band_compliance") for r in res["regressions"])
