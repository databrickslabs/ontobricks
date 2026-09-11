"""Validate that a SPARQL query stays inside Spark-supported capabilities."""

from __future__ import annotations

from collections.abc import Iterable

from rdflib.paths import Path
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.term import Literal, URIRef, Variable

from back.core.errors import ValidationError


class SparqlCapabilityValidator:
    """Fail closed when unsupported SPARQL algebra is detected."""

    _FEATURE_BY_NAME = {
        "Group": "GROUP BY",
        "AggregateJoin": "GROUP BY",
        "Aggregate_Count": "GROUP BY",
        "Aggregate_Sample": "GROUP BY",
        "OrderBy": "ORDER BY",
        "OrderCondition": "ORDER BY",
        "Minus": "MINUS",
        "Graph": "GRAPH",
        "ServiceGraphPattern": "SERVICE",
        "values": "VALUES",
    }

    @classmethod
    def validate(cls, query: str) -> None:
        try:
            algebra = prepareQuery(query).algebra
        except Exception as exc:
            raise ValidationError("Invalid SPARQL query.", detail=str(exc)) from exc

        cls._validate_node(algebra)

    @classmethod
    def _validate_node(
        cls, node, projected_vars: set[str] | None = None
    ) -> None:
        if isinstance(node, CompValue):
            cls._validate_comp_value(node, projected_vars)
            return
        if isinstance(node, (list, tuple, set)):
            for child in node:
                cls._validate_node(child, projected_vars)

    @classmethod
    def _validate_comp_value(
        cls, node: CompValue, projected_vars: set[str] | None = None
    ) -> None:
        name = node.name
        if name == "SelectQuery":
            next_projected = cls._projected_vars_from_node(node.get("PV")) or projected_vars
            cls._validate_node(node.get("p"), next_projected)
            return
        if name == "Project":
            next_projected = cls._projected_vars_from_node(node.get("PV")) or projected_vars
            cls._validate_node(node.get("p"), next_projected)
            return
        if name == "Distinct":
            cls._validate_node(node.get("p"), projected_vars)
            return
        if name == "Slice":
            start = node.get("start")
            if start not in (None, 0):
                cls._unsupported("OFFSET")
            cls._validate_node(node.get("p"), projected_vars)
            return
        if name == "BGP":
            cls._validate_bgp(node)
            return
        if name == "LeftJoin":
            expr = node.get("expr")
            if not (
                expr == "TrueFilter"
                or (isinstance(expr, CompValue) and expr.name == "TrueFilter")
            ):
                cls._unsupported("unsupported SPARQL construct")
            cls._validate_node(node.get("p1"), projected_vars)
            cls._validate_node(node.get("p2"), projected_vars)
            return
        if name == "Filter":
            if not cls._is_allowed_filter_expr(node.get("expr")):
                cls._unsupported(cls._unsupported_filter_feature(node.get("expr")))
            cls._validate_node(node.get("p"), projected_vars)
            return
        if name == "Join":
            cls._unsupported(cls._join_feature(node))
            return
        if name == "Extend":
            expr = node.get("expr")
            parent = node.get("p")
            if isinstance(expr, CompValue) and expr.name.startswith("Aggregate_"):
                cls._unsupported("GROUP BY")
            if cls._contains_comp_value_name(parent, {"AggregateJoin", "Group"}):
                cls._unsupported("GROUP BY")
            if not isinstance(expr, Literal):
                cls._unsupported("non-literal BIND")
            cls._validate_node(parent, projected_vars)
            return
        if name == "Union":
            cls._validate_union(node, projected_vars)
            return
        if name == "ToMultiSet":
            inner = node.get("p")
            # RDFLib uses a lowercase algebra node name "values" here.
            if isinstance(inner, CompValue) and inner.name == "values":
                cls._unsupported("VALUES")
            cls._unsupported("subquery")
            return
        if name in cls._FEATURE_BY_NAME:
            cls._unsupported(cls._FEATURE_BY_NAME[name])
            return
        cls._unsupported("unsupported SPARQL construct")

    @classmethod
    def _validate_bgp(cls, node: CompValue) -> None:
        triples = node.get("triples") or []
        for triple in triples:
            if not isinstance(triple, tuple) or len(triple) != 3:
                cls._unsupported("unsupported SPARQL construct")
            subject, predicate, obj = triple
            if isinstance(predicate, Path):
                cls._unsupported("property paths")
            if not cls._is_allowed_bgp_term(subject):
                cls._unsupported("unsupported SPARQL construct")
            if not cls._is_allowed_bgp_term(predicate):
                cls._unsupported("unsupported SPARQL construct")
            if not cls._is_allowed_bgp_term(obj):
                cls._unsupported("unsupported SPARQL construct")

    @staticmethod
    def _is_allowed_bgp_term(value) -> bool:
        return isinstance(value, (Variable, URIRef, Literal))

    @classmethod
    def _is_allowed_filter_expr(cls, expr) -> bool:
        if not isinstance(expr, CompValue):
            return False

        if expr.name in ("Builtin_CONTAINS", "Builtin_STRSTARTS", "Builtin_STRENDS"):
            return cls._is_lcase_str_var(expr.get("arg1")) and isinstance(
                expr.get("arg2"), Literal
            )

        if expr.name != "RelationalExpression":
            return False

        op = (expr.get("op") or "").upper()
        left = expr.get("expr")
        right = expr.get("other")

        if op == "=":
            return cls._is_str_var(left) and isinstance(right, Literal)

        if op == "IN":
            return (
                isinstance(left, Variable)
                and str(left) in {"predicate", "p", "pred"}
                and cls._all_uris(right)
            )

        return False

    @classmethod
    def _is_str_var(cls, node) -> bool:
        return (
            isinstance(node, CompValue)
            and node.name == "Builtin_STR"
            and isinstance(node.get("arg"), Variable)
        )

    @classmethod
    def _is_lcase_str_var(cls, node) -> bool:
        return (
            isinstance(node, CompValue)
            and node.name == "Builtin_LCASE"
            and cls._is_str_var(node.get("arg"))
        )

    @staticmethod
    def _all_uris(values: Iterable) -> bool:
        if not isinstance(values, list) or not values:
            return False
        return all(isinstance(value, URIRef) for value in values)

    @classmethod
    def _unsupported_filter_feature(cls, expr) -> str:
        if isinstance(expr, CompValue) and expr.name == "RelationalExpression":
            op = (expr.get("op") or "").upper()
            if op in {">", "<", ">=", "<="}:
                return "numeric FILTER"
        return "complex FILTER"

    @classmethod
    def _validate_union(
        cls, node: CompValue, projected_vars: set[str] | None
    ) -> None:
        branches = cls._collect_union_branches(node)
        if not branches:
            cls._unsupported("UNION")
        if projected_vars != {"subject", "predicate", "object"}:
            cls._unsupported("UNION")
        for branch in branches:
            if not cls._is_allowed_relationship_union_branch(branch):
                cls._unsupported("UNION")

    @staticmethod
    def _projected_vars_from_node(projected_vars_node) -> set[str] | None:
        if not isinstance(projected_vars_node, list):
            return None
        names = set()
        for var in projected_vars_node:
            if not isinstance(var, Variable):
                return None
            names.add(str(var))
        return names

    @classmethod
    def _join_feature(cls, node: CompValue) -> str:
        for key in ("p1", "p2"):
            child = node.get(key)
            if isinstance(child, CompValue) and child.name == "ToMultiSet":
                inner = child.get("p")
                if isinstance(inner, CompValue) and inner.name == "values":
                    return "VALUES"
                return "subquery"
        return "subquery"

    @classmethod
    def _collect_union_branches(cls, node: CompValue) -> list[CompValue]:
        branches: list[CompValue] = []
        pending = [node]
        while pending:
            current = pending.pop()
            if not isinstance(current, CompValue):
                cls._unsupported("UNION")
            if current.name == "Union":
                pending.append(current.get("p1"))
                pending.append(current.get("p2"))
                continue
            branches.append(current)
        return branches

    @classmethod
    def _is_allowed_relationship_union_branch(cls, branch: CompValue) -> bool:
        if not isinstance(branch, CompValue) or branch.name != "Extend":
            return False

        expr = branch.get("expr")
        var = branch.get("var")
        bgp = branch.get("p")
        if not isinstance(expr, URIRef) or not isinstance(var, Variable) or str(var) != "predicate":
            return False
        if not isinstance(bgp, CompValue) or bgp.name != "BGP":
            return False

        triples = bgp.get("triples") or []
        if len(triples) != 1:
            return False
        triple = triples[0]
        if not isinstance(triple, tuple) or len(triple) != 3:
            return False
        subject, predicate, obj = triple
        if isinstance(predicate, Path):
            return False
        if not isinstance(subject, Variable) or not isinstance(obj, Variable):
            return False
        if str(subject) != "subject" or str(obj) != "object":
            return False
        if not isinstance(predicate, URIRef):
            return False
        return predicate == expr

    @classmethod
    def _contains_comp_value_name(cls, node, names: set[str]) -> bool:
        if isinstance(node, CompValue):
            if node.name in names:
                return True
            for value in node.values():
                if cls._contains_comp_value_name(value, names):
                    return True
            return False
        if isinstance(node, (list, tuple, set)):
            return any(cls._contains_comp_value_name(value, names) for value in node)
        return False

    @staticmethod
    def _unsupported(feature: str) -> None:
        raise ValidationError(f"Spark SPARQL does not support {feature}.")
