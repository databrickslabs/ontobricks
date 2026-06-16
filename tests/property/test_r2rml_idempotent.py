"""Property-based tests for R2RML generator/parser (T-M6 expansion under CNS).

Invariants:
- Generating R2RML twice from the same mapping config produces the same Turtle.
- The generator output is valid Turtle (rdflib parses it).
- Number of entities/relationships round-trips through parse if the parser
  surfaces them (best-effort — R2RML parsing is lossier than OWL).

`property` marker — nightly only.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st
from rdflib import Graph

from back.core.w3c.r2rml.R2RMLGenerator import R2RMLGenerator
from tests.fixtures.factories import R2RMLMappingFactory


@st.composite
def _mapping_config(draw):
    """Build an R2RML mapping config via the factory."""
    entity_count = draw(st.integers(min_value=1, max_value=4))
    relationship_count = draw(st.integers(min_value=0, max_value=3))
    return R2RMLMappingFactory.build(
        entity_count=entity_count,
        relationship_count=relationship_count,
    )


@pytest.mark.property
class TestR2RMLGeneration:
    @given(_mapping_config())
    @settings(
        max_examples=30,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_generation_is_semantically_deterministic(self, config):
        """Generating R2RML twice from the same config produces the same RDF graph.

        String equality is too strict — the generator's column-iteration order
        is not stable (rr:objectMap predicate-object pairs may swap). What
        matters semantically is that the two runs produce the same set of
        RDF triples (modulo blank-node labels).
        """
        gen = R2RMLGenerator(base_uri="http://test.org/ontology#")
        ga = Graph()
        ga.parse(data=gen.generate_mapping(config), format="turtle")
        gb = Graph()
        gb.parse(data=gen.generate_mapping(config), format="turtle")
        # rdflib's isomorphic() respects blank-node renaming and compares the
        # actual graph shape.
        from rdflib.compare import isomorphic

        assert isomorphic(ga, gb), (
            "Two generate_mapping calls produced non-isomorphic graphs. "
            "If this trips intermittently, the generator has hidden non-determinism."
        )

    @given(_mapping_config())
    @settings(
        max_examples=30,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_generated_turtle_is_parseable(self, config):
        """Whatever the generator emits, rdflib should parse it without raising."""
        gen = R2RMLGenerator(base_uri="http://test.org/ontology#")
        turtle = gen.generate_mapping(config)
        g = Graph()
        g.parse(data=turtle, format="turtle")
        # The mapping graph must contain at least one triple (the ontology
        # declaration or a triplesmap header).
        assert len(g) > 0

    @given(_mapping_config())
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_class_uris_appear_in_output(self, config):
        """Every entity's ontology_class URI must appear somewhere in the R2RML Turtle."""
        gen = R2RMLGenerator(base_uri="http://test.org/ontology#")
        turtle = gen.generate_mapping(config)
        for entity in config["entities"]:
            cls_uri = entity["ontology_class"]
            # rr:class declarations carry the entity's ontology class URI.
            assert cls_uri in turtle, (
                f"class URI {cls_uri!r} not present in generated R2RML"
            )


@pytest.mark.property
class TestR2RMLFactoryShape:
    """Factory-shape invariants — ensures the test generator itself is sane."""

    @given(_mapping_config())
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_entity_count_matches_request(self, config):
        # By construction the factory honours `entity_count`.
        # We can't recover the original `entity_count` arg, but we can verify
        # the entities list is non-empty and has unique class URIs.
        entities = config["entities"]
        assert len(entities) >= 1
        class_uris = [e["ontology_class"] for e in entities]
        assert len(class_uris) == len(set(class_uris)), "duplicate entity class URIs"

    @given(_mapping_config())
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_relationships_reference_declared_entities(self, config):
        """Every relationship's source/target class must appear as an entity."""
        entity_uris = {e["ontology_class"] for e in config["entities"]}
        for rel in config["relationships"]:
            assert rel["source_class"] in entity_uris
            assert rel["target_class"] in entity_uris
