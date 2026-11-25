"""Shared SPARQL graph-pattern compilation to SQL on the flat triple table.

Used by the CONSTRUCT compiler (Complex rules) and the removal compiler
(SPARQL Update). Anything outside the supported subset raises
:class:`ValidationError` (fail-closed).
"""

import re
from typing import Dict, List, Optional, Set, Tuple

from rdflib.namespace import XSD
from rdflib.paths import Path
from rdflib.plugins.sparql import prepareQuery
from rdflib.term import Literal, URIRef, Variable

from back.core.errors import ValidationError
from back.core.helpers import (
    LIKE_ESCAPE,
    SQL_DIALECT_SPARK,
    is_sql_numeric,
    sql_like_escape,
    sql_literal,
)
from back.core.reasoning.constants import NS_PREFIX_MAP
from back.core.reasoning.shacl_rules.RuleBuiltinRegistry import RuleBuiltinRegistry

#: Namespace given to undeclared prefixes (``:Order``, ``ex:total``); their
#: local name is resolved against the ontology like the rest of the app.
_LOCAL_NS = "urn:ontobricks:local:"

_DECLARED_PREFIX_RE = re.compile(r"PREFIX\s+([A-Za-z][\w.-]*)?:", re.IGNORECASE)
_PREFIXED_NAME_RE = re.compile(r"(?<![\w?$:])([A-Za-z][\w-]*)?:")
_IRI_OR_STRING_RE = re.compile(r"<[^<>\s]*>|\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'")

_NUMERIC_TYPES = {XSD.integer, XSD.decimal, XSD.double, XSD.float}

_SQL_OPS = {"=": "=", "!=": "<>", "<": "<", ">": ">", "<=": "<=", ">=": ">="}
_FLIPPED_OPS = {"=": "=", "!=": "!=", "<": ">", ">": "<", "<=": ">=", ">=": "<="}
_LIKE_PATTERNS = {
    "Builtin_CONTAINS": "%{}%",
    "Builtin_STRSTARTS": "{}%",
    "Builtin_STRENDS": "%{}",
}

_FEATURE_NAMES = {
    "LeftJoin": "OPTIONAL",
    "Union": "UNION",
    "Extend": "BIND",
    "Slice": "LIMIT / OFFSET",
    "OrderBy": "ORDER BY",
    "Group": "GROUP BY",
    "AggregateJoin": "GROUP BY",
    "Minus": "MINUS",
    "Graph": "GRAPH",
    "ServiceGraphPattern": "SERVICE",
    "ToMultiSet": "VALUES or sub-queries",
    "Join": "several graph pattern groups",
    "Builtin_EXISTS": "EXISTS",
    "UnaryNot": "!",
}


class SparqlPatternCompiler:
    """Compile a SPARQL WHERE pattern to SQL conditions over the triple table."""


    def __init__(
        self,
        base_uri: str = "",
        uri_map: Optional[Dict[str, str]] = None,
        dialect: str = SQL_DIALECT_SPARK,
    ) -> None:
        self._base_uri = base_uri or ""
        self._uri_map = uri_map or {}
        self._dialect = dialect or SQL_DIALECT_SPARK
        self._alias_count = 0

    # -- Parsing ----------------------------------------------------------

    def _parse(self, query: str):
        try:
            return prepareQuery(self._with_prefixes(query)).algebra
        except Exception as exc:
            raise ValidationError(f"Invalid SPARQL query: {exc}") from exc

    @staticmethod
    def _with_prefixes(query: str) -> str:
        """Declare every prefix the query uses but does not declare."""
        declared = {p or "" for p in _DECLARED_PREFIX_RE.findall(query)}
        body = _IRI_OR_STRING_RE.sub(" ", query)
        used = {p or "" for p in _PREFIXED_NAME_RE.findall(body)}
        header = [
            f"PREFIX {p}: <{NS_PREFIX_MAP.get(p) or f'{_LOCAL_NS}{p}#'}>"
            for p in sorted(used - declared)
        ]
        return "\n".join(header + [query])

    def _where_parts(self, node) -> Tuple[List[tuple], List]:
        """Unwrap ``Project`` / ``Filter`` down to the BGP: (triples, filters)."""
        while node.name == "Project":
            node = node.p
        filters = []
        while node.name == "Filter":
            filters.append(node.expr)
            node = node.p
        if node.name != "BGP":
            self._unsupported(_FEATURE_NAMES.get(node.name, node.name))
        triples = list(node.triples or [])
        if not triples:
            raise ValidationError("The WHERE clause must contain at least one triple pattern.")
        return triples, filters

    # -- Graph patterns ---------------------------------------------------

    def _pattern_sql(
        self, table: str, triples: List[tuple], bindings: Dict[Variable, str]
    ) -> Tuple[str, List[str]]:
        """``FROM … JOIN …`` for *triples* and the WHERE conditions of the first one."""
        for triple in triples:
            self._check_triple(triple)
        joins: List[str] = []
        where: List[str] = []
        for i, triple in enumerate(self._connected_order(triples, set(bindings))):
            alias = self._next_alias()
            conditions = self._triple_conditions(alias, triple, bindings)
            if i == 0:
                joins.append(f"FROM {table} {alias}")
                where.extend(conditions)
            else:
                joins.append(f"JOIN {table} {alias} ON {' AND '.join(conditions)}")
        return "\n".join(joins), where

    def _check_triple(self, triple: tuple) -> None:
        s, p, o = triple
        if isinstance(p, Path):
            self._unsupported("property paths")
        if isinstance(p, Variable):
            self._unsupported("variable predicates")
        for term in (s, p, o):
            if not isinstance(term, (Variable, URIRef, Literal)):
                self._unsupported("blank nodes")

    @staticmethod
    def _connected_order(triples: List[tuple], known: Set[Variable]) -> List[tuple]:
        """Order *triples* so each one shares a variable with those before it.

        A disconnected pattern would compile to a cross join over the whole
        triple table, so it is refused instead.
        """
        pending = list(triples)
        known = set(known)
        ordered = []
        while pending:
            idx = next(
                (
                    i
                    for i, t in enumerate(pending)
                    if not known or {x for x in t if isinstance(x, Variable)} & known
                ),
                None,
            )
            if idx is None:
                raise ValidationError(
                    "The graph pattern is not connected: every triple must share "
                    "a variable with the rest of the pattern."
                )
            triple = pending.pop(idx)
            ordered.append(triple)
            known |= {x for x in triple if isinstance(x, Variable)}
        return ordered

    def _triple_conditions(
        self, alias: str, triple: tuple, bindings: Dict[Variable, str]
    ) -> List[str]:
        s, p, o = triple
        conditions = [f"{alias}.predicate = {self._term_sql(p)}"]
        for term, column in ((s, "subject"), (o, "object")):
            ref = f"{alias}.{column}"
            if not isinstance(term, Variable):
                conditions.append(f"{ref} = {self._term_sql(term)}")
            elif term in bindings:
                conditions.append(f"{ref} = {bindings[term]}")
            else:
                bindings[term] = ref
        return conditions

    # -- FILTER -----------------------------------------------------------

    def _filter_sql(self, expr, table: str, bindings: Dict[Variable, str]) -> str:
        name = getattr(expr, "name", "")
        if name in ("ConditionalAndExpression", "ConditionalOrExpression"):
            joiner = " AND " if name == "ConditionalAndExpression" else " OR "
            parts = [expr.expr] + list(expr.other or [])
            return "(" + joiner.join(self._filter_sql(p, table, bindings) for p in parts) + ")"
        if name == "RelationalExpression":
            return self._comparison_sql(expr, bindings)
        if name in _LIKE_PATTERNS:
            return self._like_sql(expr, bindings)
        if name == "Builtin_NOTEXISTS":
            return self._not_exists_sql(expr.graph, table, bindings)
        self._unsupported(f"FILTER {_FEATURE_NAMES.get(name, name or 'expression')}")

    def _comparison_sql(self, expr, bindings: Dict[Variable, str]) -> str:
        op, left, right = expr.op, expr.expr, expr.other
        if op not in _SQL_OPS:
            self._unsupported(f"FILTER operator {op}")
        if not isinstance(left, Variable) and isinstance(right, Variable):
            left, right, op = right, left, _FLIPPED_OPS[op]
        if not isinstance(left, Variable):
            self._unsupported("FILTER comparisons without a variable")
        ref = self._bound(left, bindings)
        sql_op = _SQL_OPS[op]
        if isinstance(right, Variable):
            if op not in ("=", "!="):
                self._unsupported("ordering comparisons between two variables")
            return f"{ref} {sql_op} {self._bound(right, bindings)}"
        if isinstance(right, Literal) and right.datatype in _NUMERIC_TYPES:
            value = str(right)
            if is_sql_numeric(value):
                return f"{RuleBuiltinRegistry.numeric_sql(ref, self._dialect)} {sql_op} {value}"
        if isinstance(right, (Literal, URIRef)):
            return f"{ref} {sql_op} {self._term_sql(right)}"
        self._unsupported("computed FILTER expressions")

    def _like_sql(self, expr, bindings: Dict[Variable, str]) -> str:
        arg, value = expr.arg1, expr.arg2
        if not isinstance(arg, Variable) or not isinstance(value, Literal):
            self._unsupported("string functions other than on a variable and a string")
        pattern = _LIKE_PATTERNS[expr.name].format(sql_like_escape(str(value)))
        return (
            f"{self._bound(arg, bindings)} LIKE "
            f"{sql_literal(pattern, self._dialect)} {LIKE_ESCAPE}"
        )

    def _not_exists_sql(self, group, table: str, bindings: Dict[Variable, str]) -> str:
        triples = self._group_triples(group)
        if not triples:
            raise ValidationError("NOT EXISTS must contain at least one triple pattern.")
        from_sql, where = self._pattern_sql(table, triples, dict(bindings))
        return f"NOT EXISTS (SELECT 1 {from_sql} WHERE {' AND '.join(where)})"

    def _group_triples(self, node) -> List[tuple]:
        """Triples of a NOT EXISTS group.

        rdflib hands the group over either raw (``GroupGraphPatternSub`` of
        ``TriplesBlock``) or already translated (``Join`` of ``BGP``).
        """
        name = getattr(node, "name", "")
        if name == "GroupGraphPatternSub":
            return [t for part in node.part or [] for t in self._group_triples(part)]
        if name in ("TriplesBlock", "BGP"):
            return [tuple(t) for t in node.triples or []]
        if name == "Join":
            return self._group_triples(node.p1) + self._group_triples(node.p2)
        self._unsupported("NOT EXISTS with anything other than triple patterns")

    def _head_term(self, term, bindings: Dict[Variable, str]) -> str:
        if isinstance(term, Variable):
            if term not in bindings:
                raise ValidationError(
                    f"The CONSTRUCT head uses ?{term}, which the WHERE clause does not bind."
                )
            return bindings[term]
        return self._term_sql(term)

    # -- Terms ------------------------------------------------------------

    def _bound(self, var: Variable, bindings: Dict[Variable, str]) -> str:
        if var not in bindings:
            raise ValidationError(
                f"FILTER uses ?{var}, which the WHERE clause does not bind."
            )
        return bindings[var]

    def _term_sql(self, term) -> str:
        if isinstance(term, URIRef):
            return sql_literal(self._resolve(str(term)), self._dialect)
        return sql_literal(str(term), self._dialect)

    def _resolve(self, uri: str) -> str:
        """Map an undeclared-prefix IRI to the ontology URI of its local name."""
        if not uri.startswith(_LOCAL_NS):
            return uri
        local = uri.split("#", 1)[1]
        resolved = self._uri_map.get(local.lower())
        if resolved:
            return resolved
        if not self._base_uri:
            return local
        sep = "" if self._base_uri.endswith(("#", "/")) else "#"
        return self._base_uri + sep + local

    def _next_alias(self) -> str:
        self._alias_count += 1
        return f"t{self._alias_count}"

    @staticmethod
    def _unsupported(feature: str) -> None:
        raise ValidationError(f"Complex rules do not support {feature}.")
