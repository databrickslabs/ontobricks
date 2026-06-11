"""Pipeline-level metric tests (coverage_loss + convergence)."""

from agents.pge_eval.mapping_metrics import evaluate_mapping
from agents.pge_eval.ontology_metrics import evaluate_ontology
from agents.pge_eval.pipeline_metrics import evaluate_pipeline

from tests.units.pge_eval import _fixtures as fx


def _footprint_and_mapped(artifact, ontology, metadata):
    _, _, footprint = evaluate_ontology(ontology, metadata)
    _, extras = evaluate_mapping(artifact, ontology)
    return footprint, extras["mapped_cols"]


def test_coverage_loss_zero_when_all_surfaced_cols_mapped():
    art = fx.clean_artifact()
    fp, mapped = _footprint_and_mapped(art, fx.clean_ontology(), fx.clean_metadata())
    pipeline = evaluate_pipeline(art, fp, mapped)
    assert pipeline["coverage_loss"] == 0


def test_coverage_loss_positive_when_mapping_drops_a_column():
    art = fx.clean_artifact()
    # Ontology surfaces email, but the mapping never binds it.
    art["entity_mappings"][0]["attribute_mappings"].pop("email")
    fp, mapped = _footprint_and_mapped(art, fx.clean_ontology(), fx.clean_metadata())
    pipeline = evaluate_pipeline(art, fp, mapped)
    assert pipeline["coverage_loss"] >= 1


def test_convergence_fields_present():
    art = fx.clean_artifact()
    fp, mapped = _footprint_and_mapped(art, fx.clean_ontology(), fx.clean_metadata())
    conv = evaluate_pipeline(art, fp, mapped)["convergence"]
    assert conv["mean_generator_attempts"] == 1.0
    assert conv["planner_reinvocations"] == 0
    assert conv["total_tokens"] == 1400
    assert conv["wall_clock_s"] == 42.5
