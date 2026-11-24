"""Rule built-in registry — maps built-in names to SQL / Cypher expressions.

Built-ins use IF-THEN-style authoring syntax (``builtin:greaterThan`` ...), which is
the vocabulary surfaced in the UI; execution lives entirely in this SHACL rule
layer.

Each built-in has a ``name``, ``arity``, ``category``, and templates for
both SQL and Cypher translation.  Templates use positional placeholders
``{0}``, ``{1}``, ... that the translator fills with bound variable
references or literal values.
"""

import re
from typing import Dict, Optional

from back.core.helpers import SQL_DIALECT_POSTGRES
from back.core.reasoning.shacl_rules.RuleBuiltin import RuleBuiltin

# POSIX character class so the pattern is safe inside a Postgres string
# literal (no backslash escapes). Accepts the same shapes as Spark DOUBLE.
_PG_FLOAT_RE = r"^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$"
_PG_INT_RE = r"^[+-]?[0-9]+$"
_TRY_CAST_RE = re.compile(
    r"TRY_CAST\((\{[0-9]+\}|[^\)]+) AS (DOUBLE|BIGINT|TIMESTAMP|DATE)\)",
    re.IGNORECASE,
)


class RuleBuiltinRegistry:
    """Class-based registry for rule built-in predicates (IF-THEN-style syntax).

    All methods are class-level or static; no instance is needed.
    The registry is populated once at class-definition time.
    """

    _BUILTINS: Dict[str, RuleBuiltin] = {}

    # ------------------------------------------------------------------
    # Registry population
    # ------------------------------------------------------------------

    @classmethod
    def _register(cls, b: RuleBuiltin) -> None:
        cls._BUILTINS[b.name.lower()] = b

    @classmethod
    def _init_builtins(cls) -> None:
        """Populate the built-in registry (called once at module load)."""
        r = cls._register

        # -- Comparison ------------------------------------------------
        r(
            RuleBuiltin(
                "greaterThan",
                2,
                "comparison",
                "TRY_CAST({0} AS DOUBLE) > TRY_CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) > CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "lessThan",
                2,
                "comparison",
                "TRY_CAST({0} AS DOUBLE) < TRY_CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) < CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "greaterThanOrEqual",
                2,
                "comparison",
                "TRY_CAST({0} AS DOUBLE) >= TRY_CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) >= CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "lessThanOrEqual",
                2,
                "comparison",
                "TRY_CAST({0} AS DOUBLE) <= TRY_CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) <= CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(RuleBuiltin("equal", 2, "comparison", "{0} = {1}", "{0} = {1}"))
        r(RuleBuiltin("notEqual", 2, "comparison", "{0} <> {1}", "{0} <> {1}"))

        # -- Math ------------------------------------------------------
        r(
            RuleBuiltin(
                "add",
                3,
                "math",
                "TRY_CAST({0} AS DOUBLE) + TRY_CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) + CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "subtract",
                3,
                "math",
                "TRY_CAST({0} AS DOUBLE) - TRY_CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) - CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "multiply",
                3,
                "math",
                "TRY_CAST({0} AS DOUBLE) * TRY_CAST({1} AS DOUBLE)",
                "CAST({0} AS DOUBLE) * CAST({1} AS DOUBLE)",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "divide",
                3,
                "math",
                "TRY_CAST({0} AS DOUBLE) / NULLIF(TRY_CAST({1} AS DOUBLE), 0)",
                "CASE WHEN CAST({1} AS DOUBLE) = 0 THEN null ELSE CAST({0} AS DOUBLE) / CAST({1} AS DOUBLE) END",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "mod",
                3,
                "math",
                "MOD(TRY_CAST({0} AS BIGINT), TRY_CAST({1} AS BIGINT))",
                "CAST({0} AS INT64) % CAST({1} AS INT64)",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "abs",
                2,
                "math",
                "ABS(TRY_CAST({0} AS DOUBLE))",
                "abs(CAST({0} AS DOUBLE))",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "round",
                2,
                "math",
                "ROUND(TRY_CAST({0} AS DOUBLE))",
                "round(CAST({0} AS DOUBLE), 0)",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "ceiling",
                2,
                "math",
                "CEIL(TRY_CAST({0} AS DOUBLE))",
                "ceil(CAST({0} AS DOUBLE))",
                cast_numeric=True,
            )
        )
        r(
            RuleBuiltin(
                "floor",
                2,
                "math",
                "FLOOR(TRY_CAST({0} AS DOUBLE))",
                "floor(CAST({0} AS DOUBLE))",
                cast_numeric=True,
            )
        )

        # -- String ----------------------------------------------------
        r(
            RuleBuiltin(
                "startsWith",
                2,
                "string",
                "{0} LIKE CONCAT({1}, '%%')",
                "starts with({0}, {1})",
            )
        )
        r(
            RuleBuiltin(
                "endsWith",
                2,
                "string",
                "{0} LIKE CONCAT('%%', {1})",
                "ends with({0}, {1})",
            )
        )
        r(
            RuleBuiltin(
                "contains",
                2,
                "string",
                "{0} LIKE CONCAT('%%', {1}, '%%')",
                "contains({0}, {1})",
            )
        )
        r(RuleBuiltin("stringLength", 2, "string", "LENGTH({0})", "size({0})"))
        r(RuleBuiltin("upperCase", 2, "string", "UPPER({0})", "toUpper({0})"))
        r(RuleBuiltin("lowerCase", 2, "string", "LOWER({0})", "toLower({0})"))
        r(RuleBuiltin("matches", 2, "string", "{0} RLIKE {1}", "{0} =~ {1}"))

        # -- Date/Time -------------------------------------------------
        r(
            RuleBuiltin(
                "before",
                2,
                "date",
                "TRY_CAST({0} AS TIMESTAMP) < TRY_CAST({1} AS TIMESTAMP)",
                "datetime({0}) < datetime({1})",
            )
        )
        r(
            RuleBuiltin(
                "after",
                2,
                "date",
                "TRY_CAST({0} AS TIMESTAMP) > TRY_CAST({1} AS TIMESTAMP)",
                "datetime({0}) > datetime({1})",
            )
        )
        r(
            RuleBuiltin(
                "dateDiff",
                3,
                "date",
                "DATEDIFF(TRY_CAST({0} AS DATE), TRY_CAST({1} AS DATE))",
                "duration.between(datetime({1}), datetime({0})).days",
            )
        )
        r(RuleBuiltin("now", 1, "date", "CURRENT_TIMESTAMP()", "datetime()"))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @classmethod
    def sql_template_for(cls, builtin: RuleBuiltin, dialect: Optional[str] = None) -> str:
        """Return the SQL template for *dialect* (Spark by default).

        Lakebase is Postgres: Spark ``TRY_CAST`` / ``RLIKE`` / ``CURRENT_TIMESTAMP()``
        abort the whole rule there, which the engine used to swallow as
        ``errors += 1`` with zero inferences.
        """
        template = builtin.sql_template
        if (dialect or "").lower() != SQL_DIALECT_POSTGRES:
            return template
        return cls._spark_sql_to_postgres(template)

    @classmethod
    def numeric_sql(cls, expr: str, dialect: Optional[str] = None) -> str:
        """Cast *expr* to a number for *dialect*, NULL when it is not one."""
        if (dialect or "").lower() == SQL_DIALECT_POSTGRES:
            return cls._pg_try_cast(expr, "DOUBLE")
        return f"TRY_CAST({expr} AS DOUBLE)"

    @staticmethod
    def _pg_try_cast(inner: str, pg_type: str) -> str:
        if pg_type == "DOUBLE":
            return (
                f"(CASE WHEN ({inner}) ~ '{_PG_FLOAT_RE}' "
                f"THEN ({inner})::float8 END)"
            )
        if pg_type == "BIGINT":
            return (
                f"(CASE WHEN ({inner}) ~ '{_PG_INT_RE}' "
                f"THEN ({inner})::bigint END)"
            )
        mapped = "timestamptz" if pg_type == "TIMESTAMP" else "date"
        return f"(NULLIF(BTRIM(({inner})::text), '')::{mapped})"

    @classmethod
    def _spark_sql_to_postgres(cls, template: str) -> str:
        out = template.replace(
            "DATEDIFF(TRY_CAST({0} AS DATE), TRY_CAST({1} AS DATE))",
            f"({cls._pg_try_cast('{0}', 'DATE')} - {cls._pg_try_cast('{1}', 'DATE')})",
        )
        out = _TRY_CAST_RE.sub(
            lambda m: cls._pg_try_cast(m.group(1), m.group(2).upper()),
            out,
        )
        return (
            out.replace(" RLIKE ", " ~ ")
            .replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")
        )

    @staticmethod
    def _normalize(name: str) -> str:
        """Case-fold *name* and drop its namespace.

        Accepts the bare name, any ``prefix:`` (``builtin:`` is canonical; rules
        saved before the rename use ``swrlb:``) and a full IRI
        (``http://www.w3.org/2003/11/swrlb#greaterThanOrEqual``). The local name
        is the part after the last ``#`` or ``/``, then after the last ``:``.
        """
        local = re.split(r"[#/]", name)[-1].lower()
        return local.rsplit(":", 1)[-1]

    @classmethod
    def get(cls, name: str) -> Optional[RuleBuiltin]:
        """Look up a built-in by name (case-insensitive, prefix-tolerant)."""
        return cls._BUILTINS.get(cls._normalize(name))

    @classmethod
    def is_builtin(cls, name: str) -> bool:
        """Return True if *name* is a registered built-in."""
        return cls._normalize(name) in cls._BUILTINS

    @classmethod
    def all(cls) -> Dict[str, RuleBuiltin]:
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
        """Convert an IF-THEN literal token to a SQL literal expression."""
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
        """Convert an IF-THEN literal token to a Cypher literal expression."""
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
RuleBuiltinRegistry._init_builtins()
