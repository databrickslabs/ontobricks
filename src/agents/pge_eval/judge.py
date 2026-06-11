"""Advisory LLM-judge (§3.2 / §3.3, D5).

The judge is **advisory only** — it never gates a run (Tier-exempt).  It
emits a 0–1 score per axis plus flagged issues that inform ``retry_hint``s.

This module is the **only** place the scorer touches the network.  The
deterministic metrics never import it, and the orchestrator only calls
:func:`run_judge` when ``--no-judge`` is NOT set — guaranteeing zero network
traffic in ``--no-judge`` mode.  ``requests``/serving imports are lazy so
merely importing this module makes no connection either.

The judge is usecase-agnostic: it asks generic coherence questions and is
handed the *actual* runtime ontology/mapping, never a reference answer.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)

_JUDGE_TIMEOUT = 60
_MAX_TOKENS = 1024


def _empty_axis(reason: str = "") -> Dict[str, Any]:
    flags = [reason] if reason else []
    return {"score": None, "flags": flags}


def _ontology_summary(ontology: dict) -> str:
    from agents.pge_eval.normalize import normalize_ontology

    norm = normalize_ontology(ontology)
    classes = [
        f"{c['name']}({len(c.get('data_properties', []))} dp)" for c in norm.classes
    ]
    rels = [
        f"{op['name']}: {op.get('domain', '?')}->{op.get('range', '?')}"
        for op in norm.object_properties
    ]
    return (
        f"Classes ({len(classes)}): {', '.join(classes[:60])}\n"
        f"ObjectProperties ({len(rels)}): {', '.join(rels[:60])}"
    )


def _mapping_summary(artifact: dict) -> str:
    log = artifact.get("mapping_run_log", []) or []
    lines = [
        f"{e.get('kind')}: {e.get('item')} -> {e.get('final_status')}"
        for e in log[:80]
    ]
    return "\n".join(lines)


def _parse_axis(text: str) -> Dict[str, Any]:
    """Pull a ``{"score": float, "flags": [str]}`` object out of LLM text."""
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        obj = json.loads(text[start:end])
        score = obj.get("score")
        score = float(score) if score is not None else None
        flags = [str(f) for f in (obj.get("flags") or [])]
        return {"score": score, "flags": flags}
    except (ValueError, TypeError, json.JSONDecodeError):
        return _empty_axis("judge response could not be parsed")


def _ask(host: str, token: str, endpoint_name: str, system: str, user: str) -> Dict[str, Any]:
    # Lazy import: no network dependency unless the judge actually runs.
    from agents.engine_base import call_serving_endpoint, extract_message_content

    try:
        resp = call_serving_endpoint(
            host,
            token,
            endpoint_name,
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            tools=None,
            max_tokens=_MAX_TOKENS,
            temperature=0.0,
            timeout=_JUDGE_TIMEOUT,
            trace_name="pge_eval:judge",
        )
        return _parse_axis(extract_message_content(resp))
    except Exception as exc:  # noqa: BLE001 — advisory, must never crash scoring
        logger.warning("pge_eval judge call failed (advisory, ignored): %s", exc)
        return _empty_axis(f"judge unavailable: {exc}")


_ONTOLOGY_SYSTEM = (
    "You are an ontology reviewer. Judge whether the classes and properties "
    "are coherent and non-redundant for the implied domain. Reply ONLY with a "
    'JSON object: {"score": <0..1 float>, "flags": ["short issue", ...]}. '
    "score=1 means fully coherent; flags list concrete redundancy/incoherence "
    "issues. Do not compare against any reference ontology."
)

_MAPPING_SYSTEM = (
    "You are a data-mapping reviewer. Given per-item mapping outcomes, judge "
    "holistically what the mapping likely missed or got wrong. Reply ONLY with "
    'a JSON object: {"score": <0..1 float>, "flags": ["short issue", ...]}. '
    "score=1 means the mapping looks complete and correct."
)


def run_judge(
    *,
    host: str,
    token: str,
    endpoint_name: str,
    ontology: dict,
    artifact: dict,
    stage1_issues: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Run both advisory axes. Returns ``{"ontology": {...}, "mapping": {...}}``.

    Never raises; any failure degrades to an empty axis with a flag.
    """
    if not endpoint_name:
        return {"ontology": _empty_axis("no endpoint"), "mapping": _empty_axis("no endpoint")}

    onto_user = _ontology_summary(ontology)
    if stage1_issues:
        onto_user += "\n\nDeterministic issues already found:\n" + "\n".join(
            f"- {i['check']}: {i['observed']}" for i in stage1_issues[:20]
        )
    ontology_axis = _ask(host, token, endpoint_name, _ONTOLOGY_SYSTEM, onto_user)
    mapping_axis = _ask(host, token, endpoint_name, _MAPPING_SYSTEM, _mapping_summary(artifact))
    return {"ontology": ontology_axis, "mapping": mapping_axis}
