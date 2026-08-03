"""Conditional guards for SHACL data-quality shapes.

A conformance or consistency shape may carry an optional list of conditions,
read as *IF status = active AND amount > 1000 THEN <constraint> must hold*.
The conditions only narrow the focus nodes the constraint applies to — the
constraint itself is untouched, which is why every translator here produces a
**subject filter** rather than a new kind of check.

A single condition is ``{property, property_uri, op, value}``.  Unlike a
decision-table cell it carries its own property, so rows are independent and
there is no rectangular invariant to maintain.

Operators are the decision-table vocabulary (``DT_OP_SQL``) plus ``exists`` /
``notExists`` for relationships, so the two rule builders stay in sync.
"""

from typing import Callable, Dict, List, Optional, Sequence, Set

from back.core.graphdb.constants import RDF_TYPE
from back.core.reasoning.constants import (
    DT_NUMERIC_OPS,
    DT_OP_SQL,
    DT_STRING_OPS,
    NS_PREFIX_MAP,
)

#: Only these quality dimensions accept conditions.
CONDITION_CATEGORIES = ("conformance", "consistency")

#: Relationship operators — they ignore ``value``.
EXISTENCE_OPS = frozenset({"exists", "notExists"})

#: Every operator a condition may use (``any`` is a decision-table wildcard
#: with no meaning for a guard, so it is excluded).
CONDITION_OPS = frozenset(set(DT_OP_SQL) - {"any"} | EXISTENCE_OPS)

CONDITION_LOGICS = ("and", "or")

_XSD_DOUBLE = f"<{NS_PREFIX_MAP['xsd']}double>"


def _is_numeric(value: str) -> bool:
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def get_conditions(shape: Dict) -> List[Dict]:
    """Return the usable conditions of *shape*, or an empty list.

    Conditions with no property URI or an unknown operator are dropped so a
    partially filled row can never widen or narrow a rule by accident.
    """
    raw = shape.get("conditions")
    if not isinstance(raw, list):
        return []
    return [
        c
        for c in raw
        if isinstance(c, dict)
        and c.get("property_uri")
        and c.get("op") in CONDITION_OPS
    ]


def get_logic(shape: Dict) -> str:
    return "or" if shape.get("condition_logic") == "or" else "and"


def validate(
    conditions,
    logic: str,
    category: str,
    target_class_uri: str,
) -> Optional[str]:
    """Return an error message for an invalid condition list, else ``None``."""
    if conditions in (None, [], {}):
        return None
    if not isinstance(conditions, list):
        return "Conditions must be a list"
    if category not in CONDITION_CATEGORIES:
        return (
            "Conditions are only supported on "
            f"{' and '.join(CONDITION_CATEGORIES)} rules"
        )
    if not target_class_uri:
        return "A rule with conditions must target an entity"
    if logic and logic not in CONDITION_LOGICS:
        return "Condition logic must be 'and' or 'or'"
    for cond in conditions:
        if not isinstance(cond, dict):
            return "Each condition must be an object"
        if not cond.get("property"):
            return "Each condition needs a property"
        op = cond.get("op", "")
        if op not in CONDITION_OPS:
            return f"Unknown condition operator: {op or '(empty)'}"
        if op not in EXISTENCE_OPS and str(cond.get("value", "")).strip() == "":
            return f"Condition on '{cond['property']}' needs a value"
    return None


# ---------------------------------------------------------------------------
# SQL — subject filter over a (subject, predicate, object) triple table
# ---------------------------------------------------------------------------


def subject_sql(
    conditions: Sequence[Dict],
    logic: str,
    cls_uri: str,
    table: str,
    esc: Callable[[str], str],
    normalize_uri: Callable[[str], str] = lambda u: u,
) -> Optional[str]:
    """Build a query selecting the subjects that satisfy *conditions*.

    Attribute conditions use a ``LEFT JOIN``: under ``or`` an inner join on a
    missing property would drop subjects the other branch should match, and
    under ``and`` a NULL object fails its comparison anyway, so one join form
    is correct for both.
    """
    if not conditions or not cls_uri:
        return None

    joins: List[str] = []
    predicates: List[str] = []

    for i, cond in enumerate(conditions):
        op = cond.get("op", "")
        prop_uri = normalize_uri(cond.get("property_uri", ""))
        if not prop_uri or op not in CONDITION_OPS:
            continue

        if op in EXISTENCE_OPS:
            keyword = "EXISTS" if op == "exists" else "NOT EXISTS"
            predicates.append(
                f"{keyword} (SELECT 1 FROM {table} x{i} "
                f"WHERE x{i}.subject = t0.subject "
                f"AND x{i}.predicate = '{esc(prop_uri)}')"
            )
            continue

        template = DT_OP_SQL.get(op)
        if not template:
            continue

        alias = f"c{i}"
        joins.append(
            f"LEFT JOIN {table} {alias} ON {alias}.subject = t0.subject "
            f"AND {alias}.predicate = '{esc(prop_uri)}'"
        )
        value = str(cond.get("value", ""))
        if _is_numeric(value):
            left = (
                f"CAST({alias}.object AS DOUBLE)"
                if op in DT_NUMERIC_OPS
                else f"{alias}.object"
            )
            right = value
        else:
            left = (
                f"LOWER({alias}.object)"
                if op in DT_STRING_OPS
                else f"{alias}.object"
            )
            right = f"'{esc(value.lower())}'"
        predicates.append(f"{left} {template.format(v=right)}")

    if not predicates:
        return None

    joiner = " OR " if logic == "or" else " AND "
    join_block = ("\n".join(joins) + "\n") if joins else ""
    return (
        f"SELECT DISTINCT t0.subject AS s\n"
        f"FROM {table} t0\n"
        f"{join_block}"
        f"WHERE t0.predicate = '{RDF_TYPE}' AND t0.object = '{esc(cls_uri)}'\n"
        f"  AND ({joiner.join(predicates)})"
    )


def wrap_sql(base_sql: str, condition_sql: str) -> str:
    """Restrict an existing violation query to condition-matching subjects."""
    return (
        f"SELECT v.* FROM (\n{base_sql}\n) v\n"
        f"WHERE v.s IN (\n{condition_sql}\n)"
    )


# ---------------------------------------------------------------------------
# In-memory — subject filter over an indexed triple list
# ---------------------------------------------------------------------------


def _matches(op: str, values: List[str], value: str) -> bool:
    """Existential match, mirroring the LEFT JOIN semantics of the SQL path."""
    if op == "exists":
        return bool(values)
    if op == "notExists":
        return not values

    numeric = _is_numeric(value)
    for obj in values:
        if numeric:
            try:
                left, right = float(obj), float(value)
            except (ValueError, TypeError):
                if op in DT_NUMERIC_OPS:
                    continue
                left, right = obj, value
        elif op in DT_STRING_OPS:
            left, right = obj.lower(), value.lower()
        else:
            left, right = obj, value

        if op == "eq" and left == right:
            return True
        if op == "neq" and left != right:
            return True
        if op == "gt" and left > right:
            return True
        if op == "gte" and left >= right:
            return True
        if op == "lt" and left < right:
            return True
        if op == "lte" and left <= right:
            return True
        if op == "startsWith" and str(left).startswith(str(right)):
            return True
        if op == "endsWith" and str(left).endswith(str(right)):
            return True
        if op == "contains" and str(right) in str(left):
            return True
    return False


def matching_subjects(
    conditions: Sequence[Dict],
    logic: str,
    instances: Set[str],
    subj_by_pred: Dict[str, Dict[str, List[str]]],
    resolve_uri: Callable[[str], str] = lambda u: u,
) -> Set[str]:
    """Return the subset of *instances* satisfying *conditions*."""
    if not conditions:
        return set(instances)

    resolved = []
    for cond in conditions:
        prop_uri = resolve_uri(cond.get("property_uri", ""))
        if not prop_uri or cond.get("op") not in CONDITION_OPS:
            continue
        resolved.append((cond["op"], prop_uri, str(cond.get("value", ""))))

    if not resolved:
        return set(instances)

    use_or = logic == "or"
    matched = set()
    for subject in instances:
        results = (
            _matches(op, subj_by_pred.get(uri, {}).get(subject, []), val)
            for op, uri, val in resolved
        )
        if any(results) if use_or else all(results):
            matched.add(subject)
    return matched


# ---------------------------------------------------------------------------
# SPARQL — the sh:SPARQLTarget body of a conditional shape
# ---------------------------------------------------------------------------


def _sparql_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _sparql_expression(op: str, var: str, value: str) -> Optional[str]:
    if _is_numeric(value):
        left, right = f"{_XSD_DOUBLE}({var})", value
    elif op in DT_STRING_OPS:
        left, right = f"LCASE(STR({var}))", _sparql_literal(value.lower())
    else:
        left, right = f"STR({var})", _sparql_literal(value)

    if op == "eq":
        return f"{left} = {right}"
    if op == "neq":
        return f"{left} != {right}"
    if op in ("gt", "gte", "lt", "lte"):
        symbol = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
        return f"{left} {symbol} {right}"
    if op == "startsWith":
        return f"STRSTARTS({left}, {right})"
    if op == "endsWith":
        return f"STRENDS({left}, {right})"
    if op == "contains":
        return f"CONTAINS({left}, {right})"
    return None


def sparql_target(
    conditions: Sequence[Dict],
    logic: str,
    cls_uri: str,
) -> Optional[str]:
    """Build the ``sh:select`` query selecting the guarded focus nodes.

    Full IRIs are used throughout: prefixes inside a ``sh:select`` string
    resolve from ``sh:prefixes``, not from the enclosing document, and no
    ``sh:prefixes`` declaration is emitted.
    """
    if not conditions or not cls_uri:
        return None

    use_or = logic == "or"
    patterns: List[str] = [f"$this a <{cls_uri}> ."]
    filters: List[str] = []
    expressions: List[str] = []

    for i, cond in enumerate(conditions):
        op = cond.get("op", "")
        prop_uri = cond.get("property_uri", "")
        if not prop_uri or op not in CONDITION_OPS:
            continue

        if op in EXISTENCE_OPS:
            block = f"{{ $this <{prop_uri}> ?x{i} }}"
            if use_or:
                patterns.append(f"BIND(EXISTS {block} AS ?e{i})")
                expressions.append(f"?e{i}" if op == "exists" else f"!?e{i}")
            else:
                keyword = "EXISTS" if op == "exists" else "NOT EXISTS"
                filters.append(f"FILTER {keyword} {block}")
            continue

        expression = _sparql_expression(op, f"?c{i}", str(cond.get("value", "")))
        if not expression:
            continue
        triple = f"$this <{prop_uri}> ?c{i} ."
        if use_or:
            patterns.append(f"OPTIONAL {{ {triple} }}")
            expressions.append(f"({expression})")
        else:
            patterns.append(triple)
            expressions.append(f"({expression})")

    if not expressions and not filters:
        return None

    if expressions:
        joiner = " || " if use_or else " && "
        filters.insert(0, f"FILTER({joiner.join(expressions)})")

    body = "\n    ".join(patterns + filters)
    return f"SELECT $this WHERE {{\n    {body}\n}}"
