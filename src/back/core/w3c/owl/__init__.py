"""OWL utilities for ontology generation and parsing."""

from back.core.w3c.owl.OntologyGenerator import OntologyGenerator
from back.core.w3c.owl.OntologyParser import OntologyParser
from back.core.w3c.owl.OntologyRdfExtras import OntologyRdfExtras
from back.core.w3c.owl.ConflictReport import ConflictItem, ConflictReport
from back.core.w3c.owl.OntologyConflictDetector import OntologyConflictDetector

__all__ = [
    "OntologyGenerator",
    "OntologyParser",
    "OntologyRdfExtras",
    "OntologyConflictDetector",
    "ConflictReport",
    "ConflictItem",
]
