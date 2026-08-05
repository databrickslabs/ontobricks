"""OWL utilities for ontology generation and parsing."""

from back.core.w3c.owl.OntologyGenerator import OntologyGenerator
from back.core.w3c.owl.OntologyParser import OntologyParser
from back.core.w3c.owl.OntologyConflictDetector import (
    OntologyConflictDetector,
    ConflictReport,
    ConflictItem,
)

__all__ = [
    "OntologyGenerator",
    "OntologyParser",
    "OntologyConflictDetector",
    "ConflictReport",
    "ConflictItem",
]
