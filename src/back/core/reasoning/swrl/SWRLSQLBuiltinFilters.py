"""Compile SWRL built-in atoms into SQL WHERE predicates."""

from typing import Dict, List

from back.core.reasoning.swrl.SWRLBuiltinRegistry import SWRLBuiltinRegistry


class SWRLSQLBuiltinFilters:
    """Turn built-in atoms into dialect-aware SQL filter expressions."""

    def __init__(self, dialect: str = "databricks") -> None:
        self._dialect = dialect

    @staticmethod
    def resolve_arg(token: str, var_bindings: Dict[str, tuple]) -> str:
        """Resolve a SWRL argument to a SQL expression.

        Variables (``?x``) are resolved via *var_bindings*. Literals are
        converted to SQL literal syntax.
        """
        if not SWRLBuiltinRegistry.is_literal(token):
            stripped = token.lstrip("?")
            if stripped in var_bindings or token in var_bindings:
                key = token if token in var_bindings else stripped
                a, c = var_bindings[key]
                return f"{a}.{c}"
            return f"'{stripped}'"
        return SWRLBuiltinRegistry.literal_sql(token)

    def build(
        self,
        builtin_atoms: List[Dict],
        var_bindings: Dict[str, tuple],
    ) -> List[str]:
        filters: List[str] = []
        for atom in builtin_atoms:
            bi = SWRLBuiltinRegistry.get(atom["name"])
            if bi is None:
                continue
            resolved = [self.resolve_arg(a, var_bindings) for a in atom["args"]]
            if bi.category == "comparison" and bi.arity == 2:
                expr = bi.sql_template.format(*resolved[:2])
                filters.append(expr)
            elif bi.category == "string" and bi.arity == 2:
                expr = bi.sql_template.format(*resolved[:2])
                filters.append(expr)
            elif bi.category == "math" and bi.arity == 3:
                expr = bi.sql_template.format(*resolved[:2])
                result_ref = self.resolve_arg(atom["args"][2], var_bindings)
                filters.append(f"{result_ref} = ({expr})")
            elif bi.category == "date" and bi.arity == 2:
                expr = bi.sql_template.format(*resolved[:2])
                filters.append(expr)
            elif bi.arity <= len(resolved):
                expr = bi.sql_template.format(*resolved[: bi.arity])
                filters.append(expr)
        if self._dialect == "postgres":
            # Postgres has neither TRY_CAST nor a bare DOUBLE type; plain CAST
            # is the closest equivalent. Known gaps of this post-format rewrite:
            #  * semantics: TRY_CAST yields NULL for a non-numeric literal,
            #    CAST raises and fails the whole inference statement;
            #  * coverage: only the TRY_CAST/DOUBLE templates (comparison,
            #    math) are mapped. ``matches`` (RLIKE), ``dateDiff`` (DATEDIFF)
            #    and ``now`` (CURRENT_TIMESTAMP()) stay Databricks-shaped and
            #    still error on Lakebase.
            # Proper fix: per-dialect templates on ``SWRLBuiltin`` (follow-up).
            filters = [
                f.replace("TRY_CAST(", "CAST(").replace(
                    " AS DOUBLE)", " AS DOUBLE PRECISION)"
                )
                for f in filters
            ]
        return filters
