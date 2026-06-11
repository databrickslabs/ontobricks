"""Stage-2 mapping metric tests (deterministic, no LLM)."""

from agents.pge_eval.mapping_metrics import evaluate_mapping

from tests.units.pge_eval import _fixtures as fx


def test_clean_mapping_metrics():
    metrics, extras = evaluate_mapping(fx.clean_artifact(), fx.clean_ontology())
    assert metrics["entity_completeness"] == 1.0
    assert metrics["relationship_completeness"] == 1.0
    assert metrics["attribute_coverage"] == 1.0
    assert metrics["dangling_target_pct_max"] == 0.0
    assert metrics["dangling_source_pct_max"] == 0.0
    assert metrics["id_integrity"] == 1.0
    assert metrics["sql_exec_failures"] == 0
    assert metrics["cross_source_band_compliance"] == 1.0
    assert extras["band_active"] is False


def test_dangling_target_max_picks_worst():
    metrics, _ = evaluate_mapping(fx.artifact_with_dangling_fk(), fx.clean_ontology())
    assert metrics["dangling_target_pct_max"] == 0.47


def test_sql_exec_failure_counted():
    metrics, _ = evaluate_mapping(fx.artifact_with_sql_failure(), fx.clean_ontology())
    assert metrics["sql_exec_failures"] == 1


def test_entity_completeness_drops_on_failure():
    metrics, _ = evaluate_mapping(fx.artifact_with_sql_failure(), fx.clean_ontology())
    # One of three entities failed -> 2/3.
    assert metrics["entity_completeness"] < 1.0


def test_id_integrity_detects_duplicates():
    art = fx.clean_artifact()
    art["mapping_evaluations"]["ex:Customer"]["metrics"]["distinct_id_count"] = 90
    metrics, _ = evaluate_mapping(art, fx.clean_ontology())
    assert metrics["id_integrity"] < 1.0


def test_attribute_coverage_partial():
    art = fx.clean_artifact()
    # Drop one attribute mapping from Customer (3 dp, now 2 mapped).
    art["entity_mappings"][0]["attribute_mappings"].pop("email")
    metrics, _ = evaluate_mapping(art, fx.clean_ontology())
    assert metrics["attribute_coverage"] < 1.0


def test_band_compliance_active_and_failing():
    art = fx.clean_artifact()
    art["mapping_evaluations"]["ex:placesOrder"]["metrics"].update(
        {"expected_cross_source_overlap_band": [0.2, 0.4], "cross_source_overlap_pct": 0.9}
    )
    metrics, extras = evaluate_mapping(art, fx.clean_ontology())
    assert extras["band_active"] is True
    assert metrics["cross_source_band_compliance"] < 1.0
