"""Descriptor of one SWRL built-in predicate (arity, category, SQL/Cypher templates)."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SWRLBuiltin:
    """Descriptor for a single SWRL built-in predicate."""

    name: str
    arity: int
    category: str
    sql_template: str
    cypher_template: str
    cast_numeric: bool = False
