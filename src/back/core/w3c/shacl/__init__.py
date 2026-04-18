"""SHACL — Shapes Constraint Language utilities for data quality."""

from back.core.w3c.shacl.constants import QUALITY_CATEGORIES
from back.core.w3c.shacl.SHACLGenerator import SHACLGenerator
from back.core.w3c.shacl.SHACLParser import SHACLParser
from back.core.w3c.shacl.SHACLService import SHACLService


def resolve_prop_uri(prop_uri: str, available_predicates: set) -> str:
    """Backward-compatible wrapper for :meth:`SHACLService.resolve_prop_uri`."""
    return SHACLService.resolve_prop_uri(prop_uri, available_predicates)


__all__ = [
    "SHACLService",
    "QUALITY_CATEGORIES",
    "SHACLGenerator",
    "SHACLParser",
    "resolve_prop_uri",
]
