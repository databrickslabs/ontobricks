"""Domain-agnostic live-run input loaders (used by `goals_eval.py run`)."""

import json

import pytest

from agents.pge_eval import loaders


def test_to_agent_shape_from_registry_shape():
    reg = {
        "classes": [
            {"uri": "ex:A", "name": "A", "label": "A", "dataProperties": [{"name": "x"}]},
        ],
        "properties": [
            {"uri": "ex:rel", "name": "rel", "type": "ObjectProperty", "domain": "A", "range": "A"},
            {"uri": "ex:x", "name": "x", "type": "DatatypeProperty", "domain": "A", "range": "string"},
        ],
    }
    out = loaders.to_agent_shape(reg)
    assert [e["name"] for e in out["entities"]] == ["A"]
    # Only the ObjectProperty becomes a relationship; domain/range resolve to URIs.
    assert len(out["relationships"]) == 1
    assert out["relationships"][0]["domain"] == "ex:A"


def test_to_agent_shape_passthrough_when_already_agent_shape():
    agent = {"entities": [{"uri": "ex:A", "name": "A"}], "relationships": []}
    out = loaders.to_agent_shape(agent)
    assert out["entities"][0]["name"] == "A"


def test_load_run_inputs_registry_json_single_version(tmp_path):
    dump = {"versions": {"7": {"ontology": {"classes": [{"uri": "ex:A", "name": "A"}], "properties": []},
                               "metadata": {"tables": [{"name": "t", "columns": []}]}}}}
    p = tmp_path / "dump.json"
    p.write_text(json.dumps(dump))
    ont, meta = loaders.load_run_inputs(registry_json=str(p))
    assert ont["entities"][0]["name"] == "A"
    assert meta["tables"][0]["name"] == "t"


def test_load_run_inputs_requires_version_when_ambiguous(tmp_path):
    dump = {"versions": {"1": {"ontology": {}, "metadata": {}}, "2": {"ontology": {}, "metadata": {}}}}
    p = tmp_path / "dump.json"
    p.write_text(json.dumps(dump))
    with pytest.raises(ValueError, match="pass --version"):
        loaders.load_run_inputs(registry_json=str(p))
    # explicit version resolves
    ont, meta = loaders.load_run_inputs(registry_json=str(p), version="2")
    assert "entities" in ont


def test_load_run_inputs_from_ontology_and_metadata_files(tmp_path):
    op = tmp_path / "o.json"; mp = tmp_path / "m.json"
    op.write_text(json.dumps({"entities": [{"uri": "ex:B", "name": "B"}], "relationships": []}))
    mp.write_text(json.dumps({"tables": [{"name": "tb", "columns": []}]}))
    ont, meta = loaders.load_run_inputs(ontology_path=str(op), metadata_path=str(mp))
    assert ont["entities"][0]["name"] == "B"
    assert meta["tables"][0]["name"] == "tb"


def test_load_run_inputs_no_source_raises():
    with pytest.raises(ValueError, match="needs an ontology source"):
        loaders.load_run_inputs()
