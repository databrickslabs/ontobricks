"""SWRL built-in registry — maps built-in names to SQL / Cypher expressions.

Each built-in has a ``name``, ``arity``, ``category``, and templates for
both SQL and Cypher translation.  Templates use positional placeholders
``{0}``, ``{1}``, ... that the translator fills with bound variable
references or literal values.
"""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class SWRLBuiltin:
    """Descriptor for a single SWRL built-in predicate."""

    name: str
    arity: int
    category: str
    sql_template: str
    cypher_template: str
    cast_numeric: bool = False


class SWRLBuiltinRegistry:
    """Class-based registry for SWRL built-in predicates.

    All methods are class-level or static; no instance is needed.
    The registry is populated once at class-definition time.
    """

    _BUILTINS: Dict[str, SWRLBuiltin] = {}

    # ------------------------------------------------------------------
    # Registry population
    # ------------------------------------------------------------------

    @classmethod
    def _register(cls, b: SWRLBuiltin) -> None:
        cls._BUILTINS[b.name.lower()] = b

    @classmethod
    def _init_builtins(cls) -> None:
        """Populate the built-in registry (called once at module load)."""
        r = cls._register

        # -- Comparison ------------------------------------------------
        r(
            SWRLBuiltin(
                "greaterThan",
                2,
                "comparison",
                "CAST({0} AS DOUBLE) > CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) > CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "lessThan",
                2,
                "comparison",
                "CAST({0} AS DOUBLE) < CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) < CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "greaterThanOrEqual",
                2,
                "comparison",
                "CAST({0} AS DOUBLE) >= CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) >= CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "lessThanOrEqual",
                2,
                "comparison",
                "CAST({0} AS DOUBLE) <= CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) <= CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(SWRLBuiltin("equal", 2, "comparison", "{0} = {1}", "{0} = {1}"))
        r(SWRLBuiltin("notEqual", 2, "comparison", "{0} <> {1}", "{0} <> {1}"))

        # -- Math ------------------------------------------------------
        r(
            SWRLBuiltin(
                "add",
                3,
                "math",
                "CAST({0} AS DOUBLE) + CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) + CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "subtract",
                3,
                "math",
                "CAST({0} AS DOUBLE) - CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) - CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "multiply",
                3,
                "math",
                "CAST({0} AS DOUBLE) * CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) * CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "divide",
                3,
                "math",
                "CAST({0} AS DOUBLE) / NULLIF(CAST({1} AS DOUBLE), 0)",
                "CASE WHEN CAST({1} AS DOUBLE) = 0 THEN null ELSE CAST({0} AS DOUBLE) / CAST({1} AS DOUBLE) END",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "mod",
                3,
                "math",
                "MOD(CAST({0} AS BIGINT), CAST({1} AS BIGINT))",
                "CAST({0} AS INT64) % CAST({1} AS INT64)",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "abs",
                2,
                "math",
                "ABS(CAST({0} AS DOUBLE))",
                "abs(CAST({0} AS DOUBLE))",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "round",
                2,
                "math",
                "ROUND(CAST({0} AS DOUBLE))",
                "round(CAST({0} AS DOUBLE), 0)",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "ceiling",
                2,
                "math",
                "CEIL(CAST({0} AS DOUBLE))",
                "ceil(CAST({0} AS DOUBLE))",
                cast_numeric=True,
            )
        )
        r(
            SWRLBuiltin(
                "floor",
                2,
                "math",
                "FLOOR(CAST({0} AS DOUBLE))",
                "floor(CAST({0} AS DOUBLE))",
                cast_numeric=True,
            )
        )

        # -- String ----------------------------------------------------
        r(
            SWRLBuiltin(
                "startsWith",
                2,
                "string",
                "{0} LIKE CONCAT({1}, '%%')",
                "starts with({0}, {1})",
            )
        )
        r(
            SWRLBuiltin(
                "endsWith",
                2,
                "string",
                "{0} LIKE CONCAT('%%', {1})",
                "ends with({0}, {1})",
            )
        )
        r(
            SWRLBuiltin(
                "contains",
                2,
                "string",
                "{0} LIKE CONCAT('%%', {1}, '%%')",
                "contains({0}, {1})",
            )
        )
        r(SWRLBuiltin("stringLength", 2, "string", "LENGTH({0})", "size({0})"))
        r(SWRLBuiltin("upperCase", 2, "string", "UPPER({0})", "toUpper({0})"))
        r(SWRLBuiltin("lowerCase", 2, "string", "LOWER({0})", "toLower({0})"))
        r(SWRLBuiltin("matches", 2, "string", "{0} RLIKE {1}", "{0} =~ {1}"))

        # -- Date/Time -------------------------------------------------
        r(
            SWRLBuiltin(
                "before",
                2,
                "date",
                "CAST({0} AS TIMESTAMP) < CAST({1} AS TIMESTAMP)",
                "datetime({0}) < datetime({1})",
            )
        )
        r(
            SWRLBuiltin(
                "after",
                2,
                "date",
                "CAST({0} AS TIMESTAMP) > CAST({1} AS TIMESTAMP)",
                "datetime({0}) > datetime({1})",
            )
        )
        r(
            SWRLBuiltin(
                "dateDiff",
                3,
                "date",
                "DATEDIFF(CAST({0} AS DATE), CAST({1} AS DATE))",
                "duration.between(datetime({1}), datetime({0})).days",
            )
        )
        r(SWRLBuiltin("now", 1, "date", "CURRENT_TIMESTAMP()", "datetime()"))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @classmethod
    def get(cls, name: str) -> Optional[SWRLBuiltin]:
        """Look up a built-in by name (case-insensitive)."""
        return cls._BUILTINS.get(name.lower())

    @classmethod
    def is_builtin(cls, name: str) -> bool:
        """Return True if *name* is a registered built-in."""
        return name.lower() in cls._BUILTINS

    @classmethod
    def all(cls) -> Dict[str, SWRLBuiltin]:
        """Return a copy of the full registry."""
        return dict(cls._BUILTINS)

    @classmethod
    def by_category(cls) -> Dict[str, list]:
        """Return built-ins grouped by category for UI consumption."""
        groups: Dict[str, list] = {}
        for b in cls._BUILTINS.values():
            groups.setdefault(b.category, []).append(
                {
                    "name": b.name,
                    "arity": b.arity,
                    "category": b.category,
                }
            )
        return groups

    # ------------------------------------------------------------------
    # Literal helpers (stateless utilities)
    # ------------------------------------------------------------------

    @staticmethod
    def is_literal(token: str) -> bool:
        """Return True if *token* is a literal value (not a variable).

        Variables start with ``?``.  Quoted strings and bare numbers are literals.
        """
        return not token.startswith("?")

    @staticmethod
    def literal_sql(token: str) -> str:
        """Convert a SWRL literal token to a SQL literal expression."""
        if token.startswith('"') and token.endswith('"'):
            return f"'{token[1:-1]}'"
        if token.startswith("'") and token.endswith("'"):
            return token
        try:
            float(token)
            return token
        except ValueError:
            return f"'{token}'"

    @staticmethod
    def literal_cypher(token: str) -> str:
        """Convert a SWRL literal token to a Cypher literal expression."""
        if token.startswith('"') and token.endswith('"'):
            return token
        if token.startswith("'") and token.endswith("'"):
            return f'"{token[1:-1]}"'
        try:
            float(token)
            return token
        except ValueError:
            return f'"{token}"'


# Populate the registry once at import time
SWRLBuiltinRegistry._init_builtins()
