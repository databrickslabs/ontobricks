"""Compile a removal rule (SPARQL 1.1 Update) to SQL on the flat triple table.

Accepted forms: ``DELETE WHERE``, ``DELETE DATA`` and ``DELETE {…} WHERE {…}``
with no ``INSERT`` part and no ``GRAPH`` clause. The output is one
``SELECT DISTINCT s, p, o`` per template triple; the reasoning pipeline hides
those triples through the store's ``delete_triples``.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from rdflib.term import Literal, URIRef, Variable

from back.core.errors import ValidationError
from back.core.reasoning.rules.SparqlPatternCompiler import (
    _FEATURE_NAMES,
    SparqlPatternCompiler,
)

_FORMS = ("DeleteWhere", "DeleteData", "Modify")


class SparqlUpdateCompiler(SparqlPatternCompiler):
    """Compile one removal rule into a ``SELECT s, p, o`` over the triple table."""

    def compile_removal(self, query: str, table: str) -> str:
        """Return the SQL selecting the triples to hide, or raise ValidationError."""
        self._alias_count = 0
        form, template, where = self._removal_parts(query)
        if form == "DeleteData":
            return self._data_sql(table, template)
        if where is None:  # DELETE WHERE: the template is also the pattern
            pattern, filters = template, []
        else:
            pattern, filters = self._where_parts(where)
        self._check_guard(pattern)
        bindings: Dict[Variable, str] = {}
        from_sql, conditions = self._pattern_sql(table, pattern, bindings)
        conditions += [self._filter_sql(expr, table, bindings) for expr in filters]
        selects = [
            self._removal_select(triple, bindings, from_sql, conditions)
            for triple in template
        ]
        return "\nUNION ALL\n".join(selects)

    def data_triples(self, query: str) -> List[Tuple[str, str, str]]:
        """Constant triples of a ``DELETE DATA``, resolved like the SQL is.

        Used to report the triples that were not found in the store.
        """
        form, template, _where = self._removal_parts(query)
        if form != "DeleteData":
            raise ValidationError("data_triples() only applies to DELETE DATA")
        return [
            (self._value(s), self._value(p), self._value(o)) for s, p, o in template
        ]

    # -- Parsing ----------------------------------------------------------

    def _removal_parts(self, query: str) -> Tuple[str, List[tuple], Optional[object]]:
        """Return ``(form, template triples, WHERE node or None)`` or raise."""
        operations = self._parse_update(query)
        if len(operations) != 1 or operations[0].name not in _FORMS:
            raise ValidationError(
                "Removal rules accept only DELETE WHERE, DELETE DATA or "
                "DELETE {…} WHERE {…}."
            )
        op = operations[0]
        if op.name == "Modify":
            if "insert" in op.keys():
                raise ValidationError("Removal rules cannot contain an INSERT part.")
            if "delete" not in op.keys():
                raise ValidationError("A removal must contain a DELETE part.")
            clause = op["delete"]
            template, quads, where = list(clause["triples"]), clause["quads"], op["where"]
        else:
            template, quads, where = list(op["triples"]), op["quads"], None
        if quads:
            self._unsupported("GRAPH")
        return op.name, template, where

    def _parse_update(self, query: str):
        from rdflib.plugins.sparql import prepareUpdate

        try:
            return prepareUpdate(self._with_prefixes(query)).algebra
        except Exception as exc:
            raise ValidationError(f"Invalid SPARQL update: {exc}") from exc

    def _where_parts(self, node) -> Tuple[List[tuple], List]:
        """Triples and FILTERs of a DELETE WHERE group.

        rdflib wraps the group of ``DELETE {…} WHERE {…}`` in a ``Join`` with an
        empty leading ``BGP``, so the sides of a ``Join`` are collected rather
        than rejected as in CONSTRUCT.
        """
        triples: List[tuple] = []
        filters: List = []
        self._collect_group(node, triples, filters)
        if not triples:
            raise ValidationError("The WHERE clause must contain at least one triple pattern.")
        return triples, filters

    def _collect_group(self, node, triples: List[tuple], filters: List) -> None:
        name = node.name
        if name == "BGP":
            triples.extend(node.triples or [])
        elif name == "Filter":
            filters.append(node.expr)
            self._collect_group(node.p, triples, filters)
        elif name == "Join":
            self._collect_group(node.p1, triples, filters)
            self._collect_group(node.p2, triples, filters)
        else:
            self._unsupported(_FEATURE_NAMES.get(name, name))

    # -- Guard and SQL ----------------------------------------------------

    @staticmethod
    def _check_guard(pattern: List[tuple]) -> None:
        """D4: at least one pattern triple must bind its subject or its object.

        The pattern is the WHERE group (or the DELETE WHERE template), so a
        scoped removal such as ``?m :status ?o`` under ``?m :meterOf :C42`` passes.
        """
        if not pattern:
            raise ValidationError("A removal must delete at least one triple.")
        if any(not isinstance(s, Variable) or not isinstance(o, Variable) for s, _p, o in pattern):
            return
        raise ValidationError(
            "A removal must bind a subject or an object; a whole-predicate delete "
            "is not allowed."
        )

    def _data_sql(self, table: str, data: List[tuple]) -> str:
        if not data:
            raise ValidationError("DELETE DATA must contain at least one triple.")
        selects = []
        for triple in data:
            self._check_triple(triple)
            s, p, o = (self._term_sql(term) for term in triple)
            alias = self._next_alias()
            selects.append(
                f"SELECT DISTINCT {alias}.subject AS s, {alias}.predicate AS p, "
                f"{alias}.object AS o FROM {table} {alias} "
                f"WHERE {alias}.subject = {s} AND {alias}.predicate = {p} "
                f"AND {alias}.object = {o}"
            )
        return "\nUNION ALL\n".join(selects)

    def _removal_select(
        self,
        triple: tuple,
        bindings: Dict[Variable, str],
        from_sql: str,
        conditions: List[str],
    ) -> str:
        self._check_triple(triple)
        s_sql, p_sql, o_sql = (self._head_term(term, bindings) for term in triple)
        return (
            f"SELECT DISTINCT {s_sql} AS s, {p_sql} AS p, {o_sql} AS o\n"
            f"{from_sql}\n"
            f"WHERE {' AND '.join(conditions)}"
        )

    def _value(self, term) -> str:
        """String value of a constant term, with the same resolution as the SQL."""
        if isinstance(term, URIRef):
            return self._resolve(str(term))
        if isinstance(term, Literal):
            return str(term)
        raise ValidationError("DELETE DATA accepts only IRIs and literals.")
