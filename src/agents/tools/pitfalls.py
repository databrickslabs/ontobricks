"""
Pitfall-check utility for the OWL Generator Agent.

``tool_check_owl_pitfalls`` is called directly by the engine after each OWL
text output (external validation loop).  It is NOT offered to the LLM as a
tool — keeping the Turtle out of the LLM's context window.
"""

from __future__ import annotations

import json
import re
from typing import Callable, Dict, List, Optional

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)

# ── Pitfall patterns that don't require ML / SentenceTransformer ─────────────
NON_ML_PATTERNS: List[str] = [
    "P1.1", "P1.2", "P1.3",
    "P2.1", "P2.2", "P2.3", "P2.4", "P2.5", "P2.6",
    "P3.1", "P3.2", "P3.3",
    "P4.1",
]

# ── Generic rule reminder per pattern (shown above the per-item list) ─────────
_PATTERN_RULES: Dict[str, str] = {
    "P3.2": (
        "RULE: A property name must NOT end with its range class name "
        "(e.g. `containsEvent` where range is `Event` is wrong).\n"
        "FIX: Rename each property below — remove the trailing range word and keep only the verb."
    ),
    "P3.3": (
        "RULE: A property name must NOT start with its domain class name "
        "(e.g. `orderContains` where domain is `Order` is wrong).\n"
        "FIX: Rename each property below — remove the leading domain word and keep only the verb."
    ),
    "P2.6": (
        "RULE: When two properties look like one specialises the other, "
        "you MUST declare rdfs:subPropertyOf or merge them.\n"
        "FIX: For each pair below, add the subPropertyOf triple shown."
    ),
    "P2.5": (
        "RULE: A sub-property must declare a domain/range NARROWER than its parent.\n"
        "FIX: Widen the parent declaration or narrow the child's domain/range."
    ),
    "P1.1": (
        "RULE: A class cannot be disjoint from one of its own subclasses.\n"
        "FIX: Remove the owl:disjointWith axiom between the class and its subclass."
    ),
    "P1.2": (
        "RULE: Do not add a direct rdfs:subClassOf to a grandparent — transitivity implies it.\n"
        "FIX: Remove the redundant direct subClassOf triple."
    ),
    "P1.3": (
        "RULE: A class declared as subclass of two mutually disjoint classes is unsatisfiable.\n"
        "FIX: Remove one subClassOf link or remove the disjointness axiom."
    ),
}


def _item_label(item) -> str:
    """Fallback: return a compact readable label from an item dict."""
    if isinstance(item, dict):
        return (
            item.get("class_label")
            or item.get("property_label")
            or item.get("child_label")
            or item.get("p1_label")
            or item.get("class_1_label")
            or item.get("short_label")
            or str(item)
        )
    return str(item)


def _suggest_p3_2_rename(prop_label: str, range_label: str) -> str:
    """Strip the range class name from the end of the property name."""
    stripped = re.sub(rf"(?i){re.escape(range_label)}$", "", prop_label).rstrip()
    # Lowercase first character to keep lowerCamelCase
    if stripped and stripped != prop_label:
        return stripped[0].lower() + stripped[1:]
    return prop_label


def _suggest_p3_3_rename(prop_label: str, domain_label: str) -> str:
    """Strip the domain class name from the start of the property name."""
    stripped = re.sub(rf"(?i)^{re.escape(domain_label)}", "", prop_label, count=1).lstrip()
    # Lowercase first character
    if stripped and stripped != prop_label:
        return stripped[0].lower() + stripped[1:]
    return prop_label


def _format_items(pid: str, items: list) -> List[str]:
    """Produce concrete, actionable per-item fix lines for a given pitfall id."""
    lines: List[str] = []
    if pid == "P3.2":
        for it in items:
            prop = it.get("property_label", _item_label(it))
            rng = it.get("range_label", "")
            suggestion = _suggest_p3_2_rename(prop, rng) if rng else prop
            if suggestion and suggestion != prop:
                lines.append(f"  • rename `{prop}` → `{suggestion}` (removes range '{rng}' from end)")
            else:
                lines.append(f"  • `{prop}` — ends with range class '{rng}', choose a verb-only name")
    elif pid == "P3.3":
        for it in items:
            prop = it.get("property_label", _item_label(it))
            dom = it.get("domain_label", "")
            suggestion = _suggest_p3_3_rename(prop, dom) if dom else prop
            if suggestion and suggestion != prop:
                lines.append(f"  • rename `{prop}` → `{suggestion}` (removes domain '{dom}' from start)")
            else:
                lines.append(f"  • `{prop}` — starts with domain class '{dom}', choose a verb-only name")
    elif pid == "P2.6":
        for it in items:
            p1 = it.get("p1_label", _item_label(it))
            p2 = it.get("p2_label", "")
            if p2:
                lines.append(
                    f"  • `{p1}` and `{p2}` look related → "
                    f"add `:{ p2} rdfs:subPropertyOf :{p1} .`  "
                    f"(or merge into one property)"
                )
            else:
                lines.append(f"  • `{p1}`")
    elif pid in ("P1.1", "P1.2", "P1.3"):
        for it in items:
            c1 = it.get("class_label") or it.get("child_label") or it.get("class_1_label") or _item_label(it)
            c2 = it.get("parent_label") or it.get("class_2_label") or ""
            lines.append(f"  • `{c1}`" + (f" ↔ `{c2}`" if c2 else ""))
    else:
        for it in items:
            lines.append(f"  • {_item_label(it)}")
    return lines


# ── Tool implementation ───────────────────────────────────────────────────────

def tool_check_owl_pitfalls(
    ctx: ToolContext,
    *,
    turtle_text: str = "",
    **_kwargs,
) -> str:
    """Validate Turtle OWL text against the non-ML pitfall patterns.

    Returns a JSON object with:
      - ``score``          (int 0-100, 100 = fully clean)
      - ``is_clean``       (bool, True when score=100 and zero warnings)
      - ``total_warnings`` (int)
      - ``warnings``       (list of {id, title, count, items, hint})
      - ``fix_instruction``(str, human-readable prompt to fix all warnings — empty when clean)
    """
    if not turtle_text or not turtle_text.strip():
        return json.dumps({"error": "turtle_text is required and must not be empty."})

    try:
        from rdflib import Graph
        from rdflib.namespace import OWL, RDF
        from back.core.external.pitfalls import PitfallsService
        from back.core.external.pitfalls.PitfallsService import compute_precision_score

        graph = Graph()
        try:
            graph.parse(data=turtle_text, format="turtle")
        except Exception as parse_err:
            logger.warning("tool_check_owl_pitfalls: Turtle parse error: %s", parse_err)
            return json.dumps({
                "error": f"Invalid Turtle syntax: {parse_err}",
                "is_clean": False,
                "score": 0,
            })

        metadata = {
            "classes": len(list(graph.subjects(RDF.type, OWL.Class))),
            "object_properties": len(list(graph.subjects(RDF.type, OWL.ObjectProperty))),
            "datatype_properties": len(list(graph.subjects(RDF.type, OWL.DatatypeProperty))),
        }

        svc = PitfallsService()
        result = svc.run_analysis(graph, patterns=NON_ML_PATTERNS)

        issues = {
            pid: r
            for pid, r in result["results"].items()
            if isinstance(r.get("count"), int) and r["count"] > 0
        }

        score = compute_precision_score(result["results"], metadata)
        is_clean = len(issues) == 0

        if is_clean:
            logger.info(
                "tool_check_owl_pitfalls: CLEAN — score=%d, classes=%d, props=%d+%d",
                score,
                metadata["classes"],
                metadata["object_properties"],
                metadata["datatype_properties"],
            )
            return json.dumps({
                "score": score,
                "is_clean": True,
                "total_warnings": 0,
                "warnings": [],
                "fix_instruction": "",
            })

        # Build structured warnings list — no cap on items
        warnings = []
        for pid, r in issues.items():
            raw_items = r.get("items") or []
            item_labels = [_item_label(it) for it in raw_items]
            warnings.append({
                "id": pid,
                "title": r.get("title", pid),
                "count": r["count"],
                "items": item_labels,
                "raw_items": raw_items,   # kept for fix_instruction generation below
            })

        total = sum(w["count"] for w in warnings)
        logger.info(
            "tool_check_owl_pitfalls: %d warning(s), score=%d, patterns=%s",
            total,
            score,
            [w["id"] for w in warnings],
        )

        # Build a concrete, actionable fix instruction
        lines = [
            f"The ontology has {total} pitfall warning(s) — precision score: {score}/100.",
            "Fix EVERY issue listed below, then output the corrected Turtle.",
            "",
        ]
        for w in warnings:
            pid = w["id"]
            lines.append(f"━━━ [{pid}] {w['title']} ({w['count']} occurrence(s)) ━━━")
            rule = _PATTERN_RULES.get(pid, "")
            if rule:
                lines.append(rule)
            item_lines = _format_items(pid, w["raw_items"])
            lines.extend(item_lines)
            lines.append("")

        fix_instruction = "\n".join(lines).rstrip()

        # Strip raw_items from the returned warnings (keeps the JSON lean)
        clean_warnings = [
            {k: v for k, v in w.items() if k != "raw_items"}
            for w in warnings
        ]

        return json.dumps({
            "score": score,
            "is_clean": False,
            "total_warnings": total,
            "warnings": clean_warnings,
            "fix_instruction": fix_instruction,
        })

    except Exception as exc:
        logger.error("tool_check_owl_pitfalls: unexpected error: %s", exc)
        return json.dumps({"error": str(exc), "is_clean": False, "score": 0})


# ── OpenAI function-calling schema ────────────────────────────────────────────

PITFALL_TOOL_DEFINITIONS: List[dict] = [
    {
        "type": "function",
        "function": {
            "name": "check_owl_pitfalls",
            "description": (
                "Validate a Turtle OWL ontology against structural and semantic pitfall patterns. "
                "Returns a precision score (0-100), a list of warnings with fix hints, and an "
                "is_clean flag. ALWAYS call this after generating or modifying the ontology. "
                "If is_clean is false, fix all warnings and call this tool again."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "turtle_text": {
                        "type": "string",
                        "description": (
                            "The full Turtle ontology text to validate, starting with @prefix declarations."
                        ),
                    }
                },
                "required": ["turtle_text"],
            },
        },
    }
]

PITFALL_TOOL_HANDLERS: Dict[str, Callable] = {
    "check_owl_pitfalls": tool_check_owl_pitfalls,
}
