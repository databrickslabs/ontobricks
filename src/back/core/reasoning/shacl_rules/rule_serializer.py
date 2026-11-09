"""Serialise IF/THEN rules to their generated SHACL-AF form (read-only).

The UI authors rules as IF/THEN (``antecedent`` / ``consequent``); these
helpers transpile them to canonical SHACL-AF via
:class:`~back.core.reasoning.shacl_rules.IfThenToShaclTranspiler.IfThenToShaclTranspiler`
and render them for display / export. Transpilation is one-directional, so the
output is read-only — there is no SHACL-AF → rule parser.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.reasoning.shacl_rules.IfThenToShaclTranspiler import IfThenToShaclTranspiler

#: The shared prefix header for a combined SHACL-AF document.
_PREFIX_HEADER = (
    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
    "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
)


def _strip_prefixes(turtle: str) -> str:
    """Drop ``@prefix`` lines and surrounding blank lines from a shape block.

    Per-rule Turtle emitted by the transpiler repeats a prefix header; in a
    combined document the header is written once (see
    :func:`serialize_rules_to_turtle`).
    """
    lines = turtle.splitlines()
    body = [ln for ln in lines if not ln.lstrip().startswith("@prefix")]
    return "\n".join(body).strip("\n")


def serialize_rules_to_turtle(
    rules: Optional[List[Dict[str, Any]]],
    ontology: Optional[Dict[str, Any]] = None,
) -> str:
    """Render *rules* as one combined SHACL-AF Turtle document.

    A single shared ``@prefix`` header is followed by one ``sh:NodeShape``
    block per rule. The ``ex:`` prefix is bound to the ontology base URI.
    """
    rules = rules or []
    transpiler = IfThenToShaclTranspiler(ontology or {})
    base_uri = (ontology or {}).get("base_uri", "") or "http://example.org/ontology#"

    header = _PREFIX_HEADER + f"@prefix ex: <{base_uri}> .\n"
    if not rules:
        return header + "\n# No rules defined.\n"

    blocks = [_strip_prefixes(transpiler.transpile(r).turtle) for r in rules]
    return header + "\n" + "\n\n".join(b for b in blocks if b) + "\n"


def preview_rule_shacl(
    name: str,
    antecedent: str,
    consequent: str,
    ontology: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Transpile a single IF/THEN rule and return its SHACL-AF views."""
    transpiler = IfThenToShaclTranspiler(ontology or {})
    rule = transpiler.transpile(
        {
            "name": name or "rule",
            "antecedent": antecedent or "",
            "consequent": consequent or "",
        }
    )
    return {
        "turtle": rule.turtle,
        "construct_sparql": rule.construct_sparql,
        "rule_type": rule.rule_type,
    }
