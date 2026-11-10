"""Export and import of Simple (IF-THEN) and Complex (SPARQL) rules as one
versioned JSON document. Pure functions: no session, no I/O.

The route layer reads the domain's rule lists, calls these functions, and
saves the result.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from back.core.errors import ValidationError
from back.objects.ontology.ImportResult import ImportResult
from back.objects.ontology.Rules import Rules

FORMAT = "ontobricks-rules"
VERSION = 1

_IF_THEN = "if_then_rules"
_SPARQL = "sparql_rules"


def build_package(
    domain_name: str,
    if_then_rules: List[Dict[str, Any]],
    sparql_rules: List[Dict[str, Any]],
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Return the export document for the given rule lists."""
    when = now or datetime.now(timezone.utc)
    return {
        "format": FORMAT,
        "version": VERSION,
        "exported_at": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "domain": domain_name,
        _IF_THEN: [dict(r) for r in if_then_rules or []],
        _SPARQL: [dict(r) for r in sparql_rules or []],
    }


def _check_header(data: Any) -> None:
    if not isinstance(data, dict) or data.get("format") != FORMAT:
        raise ValidationError("Not an OntoBricks rules file")
    version = data.get("version")
    if not isinstance(version, int) or version > VERSION:
        raise ValidationError(f"Unsupported rules file version: {version}")


def _check_if_then(rule: Dict[str, Any]) -> List[str]:
    return Rules.validate_if_then_rule(rule)


def _check_sparql(rule: Dict[str, Any]) -> List[str]:
    if not rule.get("name"):
        return ["Rule name is required"]
    if rule.get("enabled", True):
        # Lazy import: the reasoning package imports the ontology package.
        from back.core.reasoning import SPARQLRuleEngine

        return SPARQLRuleEngine.validate_rule(rule)
    return []


def _apply(
    kind: str,
    existing: List[Dict[str, Any]],
    incoming: Any,
    check,
    result: ImportResult,
) -> List[Dict[str, Any]]:
    if not isinstance(incoming, list):
        raise ValidationError(f"'{kind}' must be a list")
    rules = list(existing)
    for raw in incoming:
        rule = dict(raw) if isinstance(raw, dict) else {}
        name = str(rule.get("name", "")).strip()
        errors = check(rule)
        if errors:
            result.rejected.append({"type": kind, "name": name, "reason": "; ".join(errors)})
            continue
        index = next((i for i, r in enumerate(rules) if r.get("name") == name), -1)
        if index >= 0:
            rules[index] = rule
            result.replaced.append(name)
            result.changes.append((kind, "replaced", name))
        else:
            rules.append(rule)
            result.added.append(name)
            result.changes.append((kind, "added", name))
    return rules


def apply_package(
    data: Any,
    existing_if_then: List[Dict[str, Any]],
    existing_sparql: List[Dict[str, Any]],
) -> ImportResult:
    """Upsert the rules of *data* into the existing lists, reporting rejects."""
    _check_header(data)
    result = ImportResult(if_then_rules=list(existing_if_then), sparql_rules=list(existing_sparql))
    result.if_then_rules = _apply(
        _IF_THEN, existing_if_then, data.get(_IF_THEN) or [], _check_if_then, result
    )
    result.sparql_rules = _apply(
        _SPARQL, existing_sparql, data.get(_SPARQL) or [], _check_sparql, result
    )
    return result
