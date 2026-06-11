"""In-app scorecard hooks — run inside the app after generation/mapping."""

from agents.pge_eval import inapp

from tests.units.pge_eval import _fixtures as fx

_TTL = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://ex.org/o#> .
:Vehicle a owl:Class ; rdfs:label "Vehicle" .
:Depot a owl:Class ; rdfs:label "Depot" .
:stationedAt a owl:ObjectProperty ; rdfs:domain :Vehicle ; rdfs:range :Depot .
:plate a owl:DatatypeProperty ; rdfs:domain :Vehicle ; rdfs:range xsd:string .
:depotName a owl:DatatypeProperty ; rdfs:domain :Depot ; rdfs:range xsd:string .
"""

# Model sometimes prepends prose — the hook must still score (clean_owl_output).
_TTL_PROSE = "Here is the ontology you asked for.\n\n" + _TTL


def test_score_generated_ontology_clean_is_green():
    sc = inapp.score_generated_ontology(_TTL, {"tables": []})
    assert sc is not None
    assert sc["verdict"] == "GREEN"
    assert sc["mode"] == "live"
    assert sc["stages"]["ontology"]["metrics"]["orphan_class_count"] == 0


def test_score_generated_ontology_handles_prose_preamble():
    sc = inapp.score_generated_ontology(_TTL_PROSE, {"tables": []})
    assert sc is not None and sc["verdict"] == "GREEN"


def test_score_generated_ontology_never_raises_on_garbage():
    # Fails open -> None, never throws into the generation path.
    assert inapp.score_generated_ontology("not turtle {{{", {}) is None


def test_score_mapping_run_clean_is_green():
    art = fx.clean_artifact()
    sc = inapp.score_mapping_run(
        ontology=art["ontology"],
        metadata=art["metadata"],
        mapping_run_log=art["mapping_run_log"],
        mapping_evaluations=art["mapping_evaluations"],
        entity_mappings=art["entity_mappings"],
        relationship_mappings=art.get("relationship_mappings"),
        usage=art.get("usage"),
    )
    assert sc is not None
    assert sc["verdict"] == "GREEN"
    assert sc["stages"]["mapping"]["metrics"]["id_integrity"] == 1.0


def test_score_mapping_run_red_on_seeded_dangling_fk():
    art = fx.artifact_with_dangling_fk()
    sc = inapp.score_mapping_run(
        ontology=art["ontology"],
        metadata=art["metadata"],
        mapping_run_log=art["mapping_run_log"],
        mapping_evaluations=art["mapping_evaluations"],
        entity_mappings=art["entity_mappings"],
    )
    assert sc is not None and sc["verdict"] == "RED"
