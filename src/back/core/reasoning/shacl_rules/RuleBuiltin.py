"""Descriptor of one rule built-in predicate (arity, category, SQL/Cypher templates)."""

from dataclasses import dataclass


@dataclass(frozen=True)
class RuleBuiltin:
    """Descriptor for a single rule built-in predicate (IF-THEN-style syntax)."""

    name: str
    arity: int
    category: str
    sql_template: str
    cypher_template: str
    cast_numeric: bool = False
