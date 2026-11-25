"""Compile a Complex rule (SPARQL ``CONSTRUCT``) to SQL on the flat triple table.

The query is parsed into rdflib's SPARQL algebra and only a fixed subset is
accepted. Anything outside it raises :class:`ValidationError` (fail-closed),
so a rule never runs with part of its pattern silently dropped.

Supported:

- a WHERE made of triple patterns that connect through shared variables;
- ``FILTER`` with ``= != < > <= >=``, ``&&``, ``||``, ``CONTAINS`` /
  ``STRSTARTS`` / ``STRENDS`` on a variable, and ``NOT EXISTS { triples }``;
- a CONSTRUCT head with any number of triples.

Each head triple becomes one ``SELECT s, p, o`` (``UNION ALL``) that skips
triples already in the store.
"""

from typing import Dict, List

from rdflib.term import Literal, Variable

from back.core.errors import ValidationError
from back.core.reasoning.rules.SparqlPatternCompiler import SparqlPatternCompiler


class SparqlConstructCompiler(SparqlPatternCompiler):
    """Compile one CONSTRUCT rule into a ``SELECT s, p, o`` over the triple table."""


    def compile(self, query: str, table: str) -> str:
        """Return the SQL for *query*, or raise :class:`ValidationError`."""
        self._alias_count = 0
        algebra = self._parse(query)
        if algebra.name != "ConstructQuery":
            raise ValidationError(
                "Query must be a SPARQL CONSTRUCT (CONSTRUCT { ... } WHERE { ... })"
            )
        if algebra.get("datasetClause"):
            self._unsupported("FROM / FROM NAMED")
        template = algebra.get("template") or []
        if not template:
            raise ValidationError("The CONSTRUCT head must contain at least one triple.")

        triples, filters = self._where_parts(algebra.p)
        bindings: Dict[Variable, str] = {}
        from_sql, where = self._pattern_sql(table, triples, bindings)
        where += [self._filter_sql(expr, table, bindings) for expr in filters]
        selects = [
            self._head_select(table, triple, bindings, from_sql, where)
            for triple in template
        ]
        return "\nUNION ALL\n".join(selects)

    # -- CONSTRUCT head ---------------------------------------------------

    def _head_select(
        self,
        table: str,
        triple: tuple,
        bindings: Dict[Variable, str],
        from_sql: str,
        where: List[str],
    ) -> str:
        s, p, o = triple
        self._check_triple(triple)
        if isinstance(s, Literal):
            raise ValidationError("A CONSTRUCT triple cannot have a literal subject.")
        s_sql, p_sql, o_sql = (self._head_term(t, bindings) for t in (s, p, o))
        alias = self._next_alias()
        already_known = (
            f"NOT EXISTS (SELECT 1 FROM {table} {alias} "
            f"WHERE {alias}.subject = {s_sql} AND {alias}.predicate = {p_sql} "
            f"AND {alias}.object = {o_sql})"
        )
        return (
            f"SELECT DISTINCT {s_sql} AS s, {p_sql} AS p, {o_sql} AS o\n"
            f"{from_sql}\n"
            f"WHERE {' AND '.join(where + [already_known])}"
        )
