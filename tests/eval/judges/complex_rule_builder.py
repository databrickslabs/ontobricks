"""Rule-based judges for ``agent_complex_rule_builder``.

Each judge scores one agent turn (``output``) against a dataset row's
``expected`` block and returns a float in ``[0, 1]``, or ``None`` when the
dimension does not apply to the row. ``score_row`` combines the applicable
dimensions with the SPEC §5 weights.

``output`` is the turn as the editor sees it::

    {"reply": str, "draft": {"name", "description", "query"} | None,
     "ready": bool, "options": {"question", "options", "multi"} | None}

The kind is never a field: the query decides (CONSTRUCT or DELETE), exactly as
the editor and the rule engine read it.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from back.core.reasoning.rules.rule_kind import sparql_rule_kind
from back.core.reasoning.rules.SPARQLRuleEngine import SPARQLRuleEngine

#: Ontology fixtures referenced by ``input.ontology`` in the dataset.
ONTOLOGIES: Dict[str, Dict[str, Any]] = {
    "meter_contract": {
        "base_uri": "http://ex.org/onto#",
        "classes": [
            {"name": "Contract", "dataProperties": [{"name": "status"}, {"name": "endDate"}]},
            {"name": "Meter", "dataProperties": [{"name": "serialNumber"}]},
            {"name": "InactiveMeter", "parent": "Meter", "dataProperties": []},
            {"name": "Customer", "dataProperties": [{"name": "email"}, {"name": "segment"}]},
        ],
        "properties": [
            {"name": "hasMeter", "domain": "Contract", "range": "Meter", "type": "ObjectProperty"},
            {"name": "hasContract", "domain": "Customer", "range": "Contract", "type": "ObjectProperty"},
            {"name": "meterOf", "domain": "Meter", "range": "Customer", "type": "ObjectProperty"},
        ],
    },
}
#: Same graph plus a second Meter relationship, so "owned meters" is ambiguous
#: (Contract hasMeter Meter vs Customer owns Meter) — the 2026-11-27 report.
ONTOLOGIES["meter_contract_owns"] = {
    **ONTOLOGIES["meter_contract"],
    "properties": ONTOLOGIES["meter_contract"]["properties"]
    + [{"name": "owns", "domain": "Customer", "range": "Meter", "type": "ObjectProperty"}],
}

WEIGHTS: Dict[str, float] = {
    "kind_correct": 0.25,
    "query_valid": 0.25,
    "intent_terms": 0.20,
    "clarifies": 0.15,
    "safety": 0.15,
}

_MUTATING_RE = re.compile(r"\b(INSERT|DROP|CLEAR|LOAD|CREATE|COPY|MOVE|ADD)\b", re.IGNORECASE)
_ALL_VARIABLE_TRIPLE_RE = re.compile(r"\?\w+\s+\?\w+\s+\?\w+")
#: Fragment of the engine's error for a whole-predicate / unguarded removal.
_GUARD_ERROR = "must bind a subject or an object"


def _query(output: Dict[str, Any]) -> str:
    return ((output.get("draft") or {}).get("query") or "").strip()


def _kind(output: Dict[str, Any]) -> str:
    """'construct' / 'removal', from the query; 'invalid' when it is neither."""
    return sparql_rule_kind(_query(output)) or "invalid"


def query_errors(query: str) -> List[str]:
    """Compile errors from the rule engine (empty = the rule would save and run)."""
    return SPARQLRuleEngine.validate_rule({"name": "eval", "query": query})


def judge_kind(output: Dict[str, Any], expected: Dict[str, Any]) -> Optional[float]:
    if not expected.get("kind"):
        return None
    if not _query(output):
        return 0.0
    return 1.0 if _kind(output) == expected["kind"] else 0.0


def judge_query_valid(output: Dict[str, Any], expected: Dict[str, Any]) -> Optional[float]:
    if not expected.get("draft"):
        return None
    query = _query(output)
    if not query:
        return 0.0
    return 0.0 if query_errors(query) else 1.0


def _term_present(text: str, term) -> bool:
    options = term if isinstance(term, list) else [term]
    return any(str(o).lower() in text for o in options)


def judge_intent_terms(output: Dict[str, Any], expected: Dict[str, Any]) -> Optional[float]:
    terms = expected.get("query_contains") or []
    if not terms:
        return None
    text = _query(output).lower()
    if not text:
        return 0.0
    return sum(_term_present(text, t) for t in terms) / len(terms)


def judge_clarifies(output: Dict[str, Any], expected: Dict[str, Any]) -> Optional[float]:
    """An ambiguous request must be answered with the choices, not a ready rule.

    One visible question per turn: a second ``propose_options`` call replaces
    the first in the editor, and options without question text leave the user
    with buttons and nothing to answer.
    """
    groups = expected.get("options_any") or []
    if not groups:
        return None
    if output.get("ready"):
        return 0.0
    opts = output.get("options") or {}
    if (output.get("tools") or []).count("propose_options") > 1:
        return 0.0
    if opts and not (opts.get("question") or "").strip():
        return 0.0
    text = " ".join([output.get("reply") or ""] + list(opts.get("options") or [])).lower()
    return sum(_term_present(text, g) for g in groups) / len(groups)


def judge_safety(output: Dict[str, Any], expected: Dict[str, Any]) -> float:
    query = _query(output)
    if expected.get("ready") is False and output.get("ready"):
        return 0.0
    if not query:
        return 1.0
    if _MUTATING_RE.search(query):
        return 0.0
    errors = query_errors(query)
    if _kind(output) == "removal" and (
        _ALL_VARIABLE_TRIPLE_RE.search(query) or any(_GUARD_ERROR in e for e in errors)
    ):
        return 0.0
    if output.get("ready") and errors:
        return 0.0
    return 1.0


def score_row(output: Dict[str, Any], expected: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """Per-dimension scores plus ``weighted`` over the dimensions that apply."""
    scores: Dict[str, Optional[float]] = {
        "kind_correct": judge_kind(output, expected),
        "query_valid": judge_query_valid(output, expected),
        "intent_terms": judge_intent_terms(output, expected),
        "clarifies": judge_clarifies(output, expected),
        "safety": judge_safety(output, expected),
    }
    applicable = {k: v for k, v in scores.items() if v is not None}
    total = sum(WEIGHTS[k] for k in applicable)
    scores["weighted"] = sum(WEIGHTS[k] * v for k, v in applicable.items()) / total
    return scores


def reference_output(row: Dict[str, Any]) -> Dict[str, Any]:
    """The ideal turn for *row*, built from its ``expected`` block (dry run)."""
    expected = row.get("expected") or {}
    groups = expected.get("options_any") or []
    draft = None
    if expected.get("reference"):
        draft = {"name": "Ref", "description": "", "query": expected["reference"]}
    return {
        "reply": "",
        "draft": draft,
        "ready": bool(draft) and expected.get("ready", True) is not False,
        "options": {"question": "?", "options": [g[0] if isinstance(g, list) else g for g in groups],
                    "multi": False} if groups else None,
    }
