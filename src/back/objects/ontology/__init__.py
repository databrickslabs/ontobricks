"""Ontology domain: OWL/RDFS/SHACL and industry import."""

from back.objects.ontology.json_views import (
    get_ontology_classes,
    get_ontology_info,
    get_ontology_properties,
)
from back.objects.ontology.Ontology import IndustryKind, Ontology, QUALITY_CATEGORIES

__all__ = [
    "IndustryKind",
    "Ontology",
    "QUALITY_CATEGORIES",
    "get_ontology_classes",
    "get_ontology_info",
    "get_ontology_properties",
]
