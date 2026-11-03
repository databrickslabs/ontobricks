"""``TwinIncrementalBuild.mappings_for``: the one mapping derivation builds, ticks and probes share."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.objects.digitaltwin.build import TwinIncrementalBuild

pytestmark = pytest.mark.unit


class _Domain(SimpleNamespace):
    def get_r2rml(self):
        return self.r2rml


@pytest.fixture
def derivation(monkeypatch):
    """R2RML extraction and augmentation record what they were given."""
    from back.core.w3c import sparql
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    seen: dict = {}
    monkeypatch.setattr(sparql, "extract_r2rml_mappings", lambda r2rml: ({"C": {}}, [{"r": 1}]))

    def augment(kind):
        def inner(mappings, config, base_uri, ontology):
            seen[kind] = (config, base_uri)
            return mappings

        return inner

    monkeypatch.setattr(DigitalTwin, "augment_mappings_from_config", augment("entities"))
    monkeypatch.setattr(DigitalTwin, "augment_relationships_from_config", augment("relationships"))
    return seen


def test_the_ontology_base_uri_and_the_mapping_config_are_used(derivation):
    domain = _Domain(r2rml="<r2rml/>", ontology={"base_uri": "http://ex.org/"}, assignment={"a": 1})

    ent, rels, base_uri = TwinIncrementalBuild.mappings_for(domain)

    assert (ent, rels, base_uri) == ({"C": {}}, [{"r": 1}], "http://ex.org/")
    assert derivation == {
        "entities": ({"a": 1}, "http://ex.org/"),
        "relationships": ({"a": 1}, "http://ex.org/"),
    }


def test_without_an_ontology_base_uri_the_build_default_is_used(derivation):
    from shared.config.constants import DEFAULT_BASE_URI

    domain = _Domain(r2rml="<r2rml/>", ontology={}, assignment=None)

    assert TwinIncrementalBuild.mappings_for(domain)[2] == DEFAULT_BASE_URI


def test_a_domain_without_r2rml_has_no_mappings():
    assert TwinIncrementalBuild.mappings_for(_Domain(r2rml="")) is None


class _RegistryDomain(_Domain):
    """A domain opened from the registry: mappings saved, session-only R2RML not yet regenerated."""

    def ensure_generated_content(self):
        self.r2rml = "<r2rml/>"
        self.ensured = True
        return {"owl": False, "r2rml": True}


def test_the_session_only_r2rml_is_regenerated_first_as_the_build_does(derivation):
    domain = _RegistryDomain(r2rml="", ontology={"base_uri": "http://ex.org/"}, assignment={"a": 1})

    ent, _rels, base_uri = TwinIncrementalBuild.mappings_for(domain)

    assert (domain.ensured, ent, base_uri) == (True, {"C": {}}, "http://ex.org/")


def test_the_scheduled_tick_derives_its_mappings_the_same_way(monkeypatch):
    from back.core.w3c import sparql
    from back.objects.registry.schedule import build as scheduled_build

    monkeypatch.setattr(
        TwinIncrementalBuild, "mappings_for", staticmethod(lambda d: ({"C": {}}, [], "urn:b/"))
    )
    monkeypatch.setattr(sparql, "translate_sparql_to_spark", lambda *a, **k: {"sql": "SELECT 1"})
    domain = SimpleNamespace(info={"name": "dom"}, current_version="3", delta={"catalog": "c", "schema": "s"})

    _sql, _view, _graph, base_uri, ent, _rels = scheduled_build._generate_sql_from_r2rml(domain, "dom")

    assert (base_uri, ent) == ("urn:b/", {"C": {}})
