"""Property-based roundtrip tests for OWL parser ↔ generator (T-M6 under CNS).

The invariant: generating Turtle from an ontology config and parsing it back
preserves the class set and the object-property set. This catches translator
bugs that example-based tests miss — e.g., a class with a name that collides
with an RDFS keyword, a property with an unusual character, an empty
description, etc.

`property` marker — nightly only in CI (see pyproject.toml markers).

Hypothesis caveat: we constrain the generated configs to use ASCII-safe names
and the OntoBricks-canonical base URI shape. Wider exploration is valuable but
needs the parser to be more defensive first; revisit as T-M6 expands.
"""

from __future__ import annotations

import string

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from back.core.w3c.owl.OntologyGenerator import OntologyGenerator
from back.core.w3c.owl.OntologyParser import OntologyParser


# Strategies — kept narrow on purpose for the first property test.

_FIRST_CHAR = st.sampled_from(string.ascii_uppercase)
_REST = st.text(alphabet=string.ascii_letters + string.digits, min_size=0, max_size=15)


def _class_name() -> st.SearchStrategy[str]:
    return st.builds(lambda first, rest: first + rest, _FIRST_CHAR, _REST)


def _ontology_config_strategy():
    """Build an ontology config with N unique classes and M relationships.

    Hypothesis explores N in [1, 5] and M in [0, 4].
    """

    @st.composite
    def _build(draw):
        class_count = draw(st.integers(min_value=1, max_value=5))
        prop_count = draw(st.integers(min_value=0, max_value=4))
        names = draw(
            st.lists(
                _class_name(),
                min_size=class_count,
                max_size=class_count,
                unique=True,
            )
        )
        base = "http://test.org/ontology#"
        classes = [
            {
                "uri": f"{base}{name}",
                "name": name,
                "label": name,
                "comment": "",
                "emoji": "",
                "parent": "",
                "dataProperties": [],
            }
            for name in names
        ]
        # Build forward-only relationships in a deterministic round-robin.
        properties = []
        for i in range(prop_count):
            src = names[i % class_count]
            tgt = names[(i + 1) % class_count]
            properties.append(
                {
                    "uri": f"{base}has{tgt}{i}",
                    "name": f"has{tgt}{i}",
                    "label": f"has {tgt} {i}",
                    "comment": "",
                    "type": "ObjectProperty",
                    "domain": src,
                    "range": tgt,
                }
            )
        return {
            "name": "TestOntology",
            "base_uri": base,
            "classes": classes,
            "properties": properties,
        }

    return _build()


@pytest.mark.property
class TestOWLRoundtrip:
    """Generated config → Turtle → parsed config should preserve names."""

    @given(_ontology_config_strategy())
    @settings(
        max_examples=30,
        deadline=None,  # rdflib parse can be slow on first call
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_class_names_preserved(self, config):
        gen = OntologyGenerator(
            base_uri=config["base_uri"],
            ontology_name=config["name"],
            classes=config["classes"],
            properties=config["properties"],
        )
        turtle = gen.generate()
        parser = OntologyParser(turtle)
        parsed_classes = parser.get_classes()

        input_names = {c["name"] for c in config["classes"]}
        parsed_names = {c.get("name") for c in parsed_classes}

        # Every input class name must appear in the parsed output.
        # The parser may add inferred classes (rare); we don't require equality
        # both directions — only that the input set is a subset of the output.
        assert input_names.issubset(parsed_names), (
            f"missing classes after roundtrip: {input_names - parsed_names}"
        )

    @given(_ontology_config_strategy())
    @settings(
        max_examples=30,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_object_property_names_preserved(self, config):
        gen = OntologyGenerator(
            base_uri=config["base_uri"],
            ontology_name=config["name"],
            classes=config["classes"],
            properties=config["properties"],
        )
        turtle = gen.generate()
        parser = OntologyParser(turtle)
        parsed_props = parser.get_properties()

        input_obj_props = {p["name"] for p in config["properties"]}
        parsed_names = {p.get("name") for p in parsed_props}

        # Object properties must roundtrip. Data properties may not be in the input.
        assert input_obj_props.issubset(parsed_names), (
            f"missing object properties after roundtrip: {input_obj_props - parsed_names}"
        )

    @given(_ontology_config_strategy())
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_generated_turtle_is_parseable_by_rdflib(self, config):
        """Independent check: rdflib parses the output without raising."""
        from rdflib import Graph

        gen = OntologyGenerator(
            base_uri=config["base_uri"],
            ontology_name=config["name"],
            classes=config["classes"],
            properties=config["properties"],
        )
        turtle = gen.generate()
        g = Graph()
        g.parse(data=turtle, format="turtle")
        # At least the ontology declaration is there.
        assert len(g) > 0
