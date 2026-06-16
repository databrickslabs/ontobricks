"""Test-data factories for OntoBricks tests.

Dataclass-based builders that produce realistic-shape inputs for the domain
classes (Ontology, R2RML Mapping, Triple, Domain, Shacl Shape) and Databricks
surface mocks. Designed to replace the inline `sample_*` dicts that have
proliferated across the 75+ test files.

Why dataclasses instead of `factory_boy`: zero extra dependency, plays well
with mypy, and the builders are simple enough that the .build() pattern is
sufficient. If we ever need sequences, post-generation hooks, or sub-factories,
swap in `factory_boy` — the import surface here is intentionally narrow.

Example:

    from tests.fixtures.factories import OntologyFactory

    onto = OntologyFactory.build(classes=3, properties=2)
    assert len(onto["classes"]) == 3

All factories accept a `seed` kwarg for deterministic output in property tests.
"""

from tests.fixtures.factories.ontology_factory import OntologyFactory
from tests.fixtures.factories.mapping_factory import R2RMLMappingFactory
from tests.fixtures.factories.triple_factory import TripleFactory
from tests.fixtures.factories.domain_factory import DomainFactory
from tests.fixtures.factories.shacl_factory import ShaclShapeFactory

__all__ = [
    "OntologyFactory",
    "R2RMLMappingFactory",
    "TripleFactory",
    "DomainFactory",
    "ShaclShapeFactory",
]
