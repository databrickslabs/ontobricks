"""Domain-agnostic input loaders for the live ``goals_eval.py run`` path.

The score-only path is already usecase-agnostic (it ingests a captured
artifact). Live ``run`` previously reused the smoke-test loader, which
hard-pinned a single demo domain (a fixed ``/tmp`` dump + a fixed version key).
These helpers replace that: they load the ontology + source metadata for ANY
domain, from either an exported registry version dump or plain ontology/metadata
JSON files — no domain, table, or version is baked in.

Pure functions (file IO + dict reshaping only) — no LLM, no DB, no domain
knowledge — so they are unit-testable offline.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple


def to_agent_shape(ontology: dict) -> dict:
    """Convert a registry-shape ontology (``{classes, properties}``) to the
    ``{entities, relationships}`` shape the mapping-PGE engine consumes.

    If the input is already in agent shape (has ``entities``) it is returned
    unchanged. Fully generic — only field copying + domain/range resolution.
    """
    ontology = ontology or {}
    if "entities" in ontology or "relationships" in ontology:
        return {
            "entities": list(ontology.get("entities", []) or []),
            "relationships": list(ontology.get("relationships", []) or []),
        }

    classes = ontology.get("classes", []) or []
    properties = ontology.get("properties", []) or []
    name_to_uri = {c["name"]: c["uri"] for c in classes if c.get("name") and c.get("uri")}

    def _resolve(ref: str) -> str:
        if not ref or str(ref).startswith("http"):
            return ref
        return name_to_uri.get(ref, ref)

    entities = [
        {
            "uri": c.get("uri", ""),
            "name": c.get("name", ""),
            "label": c.get("label", ""),
            "comment": c.get("comment", ""),
            "parent": c.get("parent", ""),
            "attributes": list(c.get("dataProperties", []) or []),
        }
        for c in classes
    ]
    relationships = [
        {
            "uri": p.get("uri", ""),
            "name": p.get("name", ""),
            "label": p.get("label", p.get("name", "")),
            "comment": p.get("comment", ""),
            "domain": _resolve(p.get("domain", "")),
            "range": _resolve(p.get("range", "")),
        }
        for p in properties
        if p.get("type", "ObjectProperty") == "ObjectProperty"
    ]
    return {"entities": entities, "relationships": relationships}


def _pick_version(versions: Dict[str, Any], version: Optional[str]) -> str:
    """Choose a version key from a registry dump's ``versions`` map.

    Explicit ``version`` wins; otherwise the single version if there's exactly
    one; otherwise raise asking the caller to disambiguate. Never guesses a
    domain-specific default (the old code hard-coded ``"1_1"``).
    """
    if version is not None:
        if version not in versions:
            raise ValueError(
                f"version {version!r} not in registry dump; available: "
                f"{sorted(versions)}"
            )
        return version
    keys = list(versions)
    if len(keys) == 1:
        return keys[0]
    raise ValueError(
        f"registry dump has {len(keys)} versions {sorted(keys)}; "
        "pass --version to choose one"
    )


def load_run_inputs(
    *,
    registry_json: Optional[str] = None,
    version: Optional[str] = None,
    ontology_path: Optional[str] = None,
    metadata_path: Optional[str] = None,
) -> Tuple[dict, dict]:
    """Resolve ``(ontology_agent_shape, metadata)`` for a live run, domain-agnostic.

    Exactly one source must be given:

    * ``registry_json`` (+ optional ``version``) — an exported registry version
      dump shaped ``{"versions": {<ver>: {"ontology": ..., "metadata": ...}}}``.
    * ``ontology_path`` (+ optional ``metadata_path``) — plain JSON files holding
      the ontology (registry or agent shape) and source metadata.
    """
    if registry_json:
        with open(registry_json) as f:
            doc = json.load(f)
        versions = doc.get("versions") or {}
        if not versions:
            raise ValueError(f"{registry_json} has no 'versions' map")
        ver = _pick_version(versions, version)
        v = versions[ver]
        return to_agent_shape(v.get("ontology", {})), (v.get("metadata", {}) or {})

    if ontology_path:
        with open(ontology_path) as f:
            ontology = json.load(f)
        metadata: dict = {}
        if metadata_path:
            with open(metadata_path) as f:
                metadata = json.load(f)
        return to_agent_shape(ontology), metadata

    raise ValueError(
        "live run needs an ontology source: pass --registry-json (+--version) "
        "or --ontology (+--metadata)"
    )
