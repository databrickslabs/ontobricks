"""Stable content hashes for version documents and candidates."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from back.objects.domain.payload import version_document_slice

_GENERATED_KEYS = ("r2rml_output",)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _design_content(
    ontology: Dict[str, Any], assignment: Dict[str, Any]
) -> Dict[str, Any]:
    assignment = {
        k: v for k, v in (assignment or {}).items() if k not in _GENERATED_KEYS
    }
    return {"ontology": ontology or {}, "assignment": assignment}


def design_hash(ontology: Dict[str, Any], assignment: Dict[str, Any]) -> str:
    """sha256 of the ontology and mapping design, ignoring generated R2RML."""
    payload = _canonical(_design_content(ontology, assignment))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def version_document_hash(document: Dict[str, Any], version: str) -> str:
    """sha256 of the design content of *version* inside a registry document."""
    versions = document.get("versions") or {}
    slice_ = version_document_slice(versions, version) if versions else document
    return design_hash(
        slice_.get("ontology") or {},
        slice_.get("assignment") or slice_.get("mapping") or {},
    )
