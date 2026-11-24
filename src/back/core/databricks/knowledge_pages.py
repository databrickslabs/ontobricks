"""Normalize and validate UC Knowledge Page selection snapshots.

Persisted on ``domain.info.knowledge_pages`` for later Ontology Generate use.
Bodies are never stored here — preview fetches them on demand.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

_PAGE_NAME_RE = re.compile(r"^pages/[0-9a-fA-F-]{8,}$")
_DOMAIN_NAME_RE = re.compile(r"^domains/[0-9a-fA-F-]{8,}$")
_ALLOWED_STATES = frozenset({"DRAFT", "PUBLISHED"})
_MAX_SELECTION = 200
_MAX_STRING = 2000


def normalize_knowledge_pages(raw: Any) -> List[Dict[str, Any]]:
    """Return a cleaned list of page snapshots, or ``[]`` when missing/invalid."""
    if raw is None:
        return []
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        snap = _normalize_one(item)
        if snap is None:
            continue
        name = snap["name"]
        if name in seen:
            continue
        seen.add(name)
        out.append(snap)
        if len(out) >= _MAX_SELECTION:
            break
    return out


def _normalize_one(item: Any) -> Dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    name = str(item.get("name") or "").strip()
    if not _PAGE_NAME_RE.match(name):
        return None
    domain = str(item.get("domain") or "").strip()
    if domain and not _DOMAIN_NAME_RE.match(domain):
        domain = ""
    state = str(item.get("state") or "").strip().upper()
    if state not in _ALLOWED_STATES:
        state = "PUBLISHED"
    display_name = _clip(str(item.get("display_name") or "").strip(), 200) or name
    description = _clip(str(item.get("description") or "").strip(), _MAX_STRING)
    update_time = _clip(str(item.get("update_time") or "").strip(), 64)
    synonyms_raw = item.get("synonyms") or []
    synonyms: List[str] = []
    if isinstance(synonyms_raw, list):
        for s in synonyms_raw[:50]:
            text = _clip(str(s or "").strip(), 200)
            if text and text not in synonyms:
                synonyms.append(text)
    return {
        "name": name,
        "domain": domain,
        "display_name": display_name,
        "state": state,
        "description": description,
        "synonyms": synonyms,
        "update_time": update_time,
    }


def _clip(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit]
