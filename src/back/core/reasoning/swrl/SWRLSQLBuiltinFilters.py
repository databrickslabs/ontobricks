"""SWRL builtin atoms → SQL ``WHERE`` filters, per dialect.

Extracted from :class:`~back.core.reasoning.swrl.SWRLSQLTranslator.SWRLSQLTranslator`
(Extract Class: the translator assembles rule SQL; this class compiles the
``swrlb:`` atoms of a rule into filter expressions over the rule's variable
bindings). The templates come from ``SWRLBuiltinRegistry.sql_template_for``
and differ by dialect (Spark by default, Postgres for Lakebase-backed stores).
"""

from __future__ import annotations

from typing import Dict, List

from back.core.helpers import SQL_DIALECT_SPARK
from back.core.reasoning.swrl.SWRLBuiltinRegistry import SWRLBuiltinRegistry


class SWRLSQLBuiltinFilters:
    """Compile a rule's builtin atoms into SQL filter expressions."""

    @staticmethod
    def resolve_arg(token: str, var_bindings: Dict[str, tuple]) -> str:
        """Resolve a SWRL argument to a SQL expression.

        Variables (``?x``) are resolved via *var_bindings* to ``alias.column``;
        literals are converted to SQL literal syntax.
        """
        if not SWRLBuiltinRegistry.is_literal(token):
            stripped = token.lstrip("?")
            if stripped in var_bindings or token in var_bindings:
                key = token if token in var_bindings else stripped
                alias, column = var_bindings[key]
                return f"{alias}.{column}"
            return f"'{stripped}'"
        return SWRLBuiltinRegistry.literal_sql(token)

    @staticmethod
    def build(
        builtin_atoms: List[Dict],
        var_bindings: Dict[str, tuple],
        dialect: str = SQL_DIALECT_SPARK,
    ) -> List[str]:
        """One SQL filter per known builtin atom, in the atoms' order.

        A comparison / string / date builtin of arity two is its template over
        the two resolved arguments; a math builtin of arity three equates the
        third argument to the template over the first two; any other builtin
        gets the template over its first *arity* arguments. Unknown builtins
        are skipped.
        """
        filters: List[str] = []
        for atom in builtin_atoms:
            builtin = SWRLBuiltinRegistry.get(atom["name"])
            if builtin is None:
                continue
            resolved = [
                SWRLSQLBuiltinFilters.resolve_arg(arg, var_bindings) for arg in atom["args"]
            ]
            template = SWRLBuiltinRegistry.sql_template_for(builtin, dialect)
            if builtin.category == "math" and builtin.arity == 3:
                result_ref = SWRLSQLBuiltinFilters.resolve_arg(atom["args"][2], var_bindings)
                filters.append(f"{result_ref} = ({template.format(*resolved[:2])})")
            elif builtin.category in ("comparison", "string", "date") and builtin.arity == 2:
                filters.append(template.format(*resolved[:2]))
            elif builtin.arity <= len(resolved):
                filters.append(template.format(*resolved[: builtin.arity]))
        return filters
