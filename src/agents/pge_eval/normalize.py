"""Shape normalisation + footprint helpers for the PGE intrinsic evaluator.

Everything in this module is pure Python — no LLM, no DB, no domain
knowledge.  It exists so the rest of the scorer can reason over one stable
in-memory shape regardless of whether the caller handed it the *agent*
ontology shape (``{entities, relationships}``), the *registry* ontology
shape (``{classes, properties}``), or raw source metadata.

Design constraints (see `.planning/agents/agent_owl_generator/SPEC.md` §6):

* **Usecase-agnostic.**  No table name, identifier, or count from any
  particular domain is encoded here.  The only constants are generic
  audit/surrogate column heuristics that hold for any relational source.
* **Deterministic.**  Pure functions of their inputs; no randomness, no
  wall-clock, no network.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set


# =====================================================
# Name normalisation
# =====================================================


def normalize_name(name: Optional[str]) -> str:
    """Collapse a column / property / class name to a comparison key.

    Lower-cases and strips every non-alphanumeric character so that
    ``first_name``, ``firstName`` and ``FirstName`` all collapse to
    ``firstname``.  This is the footprint-matching key used to decide
    whether a source column "became" a data property without consulting
    the mapping (Stage-1 is mapping-independent — see D2/D3).
    """
    if not name:
        return ""
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def local_name(uri_or_name: Optional[str]) -> str:
    """Return the local name of a URI/CURIE, or the value unchanged.

    ``http://x/Customer`` -> ``Customer``; ``ex:Customer`` -> ``Customer``;
    ``Customer`` -> ``Customer``.
    """
    if not uri_or_name:
        return ""
    s = str(uri_or_name)
    for sep in ("#", "/"):
        if sep in s:
            s = s.rsplit(sep, 1)[-1]
    if ":" in s and not s.startswith("http"):
        s = s.rsplit(":", 1)[-1]
    return s


# =====================================================
# Audit / surrogate column heuristics (generic, not domain-specific)
# =====================================================

# Audit tokens that mark a column as non-analytical bookkeeping.  These are
# generic ETL/CDC conventions, not tied to any domain.
_AUDIT_TOKENS = (
    "createdat",
    "updatedat",
    "createdon",
    "updatedon",
    "createdby",
    "updatedby",
    "modifiedat",
    "modifiedby",
    "deletedat",
    "ingestedat",
    "loadedat",
    "loadts",
    "etltimestamp",
    "dwcreated",
    "dwupdated",
)
_AUDIT_PREFIXES = ("etl", "ingest", "_ingest", "dw")
# Exact surrogate row-key names + suffixes for warehouse surrogate keys.
_SURROGATE_EXACT = ("id", "rowid", "rownum", "rownumber")
_SURROGATE_SUFFIXES = ("sk", "surrogatekey")


def is_surrogate_or_audit(column_name: str) -> bool:
    """Heuristic: True when *column_name* is a surrogate row key or audit
    column with no analytical value.

    The OWL generator is instructed to drop exactly these, so they are
    excluded from coverage denominators (D3).  Intentionally conservative:
    it does NOT drop every ``*_id`` column (foreign keys can be meaningful),
    only obvious surrogate keys and audit bookkeeping.
    """
    norm = normalize_name(column_name)
    if not norm:
        return True
    if norm in _SURROGATE_EXACT:
        return True
    if any(norm.endswith(sfx) for sfx in _SURROGATE_SUFFIXES):
        return True
    if any(tok in norm for tok in _AUDIT_TOKENS):
        return True
    raw = re.sub(r"[^a-z0-9_]", "", str(column_name).lower())
    if any(raw.startswith(p) for p in _AUDIT_PREFIXES):
        return True
    return False


# =====================================================
# Ontology normalisation
# =====================================================


def _attr_names(raw_attrs: Any) -> List[str]:
    """Normalise an attribute container to a flat list of name strings.

    Accepts the agent shape (list of str or ``{name|uri|label}`` dicts) and
    the registry shape (list of ``{name|localName}`` dicts).
    """
    out: List[str] = []
    for a in raw_attrs or []:
        if isinstance(a, str):
            out.append(a)
        elif isinstance(a, dict):
            name = a.get("name") or a.get("localName") or a.get("uri") or a.get("label")
            if name:
                out.append(local_name(name))
    return out


class NormalizedOntology:
    """A flat, shape-agnostic view of a generated ontology.

    Attributes:
        classes: list of ``{"name", "uri", "data_properties": [str]}``.
        object_properties: list of ``{"name", "uri", "domain", "range"}``
            where domain/range are the raw refs as authored (URI or local).
    """

    def __init__(self, classes: List[dict], object_properties: List[dict]):
        self.classes = classes
        self.object_properties = object_properties

    # --- derived sets, computed lazily but cheaply ------------------

    @property
    def class_resolution_set(self) -> Set[str]:
        """Every token a domain/range ref could legitimately resolve to."""
        out: Set[str] = set()
        for c in self.classes:
            if c.get("uri"):
                out.add(c["uri"])
                out.add(local_name(c["uri"]))
            if c.get("name"):
                out.add(c["name"])
                out.add(local_name(c["name"]))
        return out

    @property
    def all_data_property_keys(self) -> Set[str]:
        """Normalised keys of every data property across every class."""
        keys: Set[str] = set()
        for c in self.classes:
            for dp in c.get("data_properties", []):
                k = normalize_name(local_name(dp))
                if k:
                    keys.add(k)
        return keys

    @property
    def class_name_keys(self) -> Set[str]:
        keys: Set[str] = set()
        for c in self.classes:
            k = normalize_name(local_name(c.get("name") or c.get("uri")))
            if k:
                keys.add(k)
        return keys


def normalize_ontology(ontology: dict) -> NormalizedOntology:
    """Normalise either the agent shape or the registry shape.

    * Agent shape:    ``{"entities": [...], "relationships": [...]}``
    * Registry shape: ``{"classes": [...], "properties": [...]}``
    """
    ontology = ontology or {}
    classes: List[dict] = []
    object_props: List[dict] = []

    if "entities" in ontology or "relationships" in ontology:
        for e in ontology.get("entities", []) or []:
            classes.append(
                {
                    "name": e.get("name") or local_name(e.get("uri")),
                    "uri": e.get("uri", ""),
                    "data_properties": _attr_names(e.get("attributes")),
                }
            )
        for r in ontology.get("relationships", []) or []:
            object_props.append(
                {
                    "name": r.get("name") or local_name(r.get("uri")),
                    "uri": r.get("uri", ""),
                    "domain": r.get("domain", ""),
                    "range": r.get("range", ""),
                }
            )
    else:
        for c in ontology.get("classes", []) or []:
            classes.append(
                {
                    "name": c.get("name") or local_name(c.get("uri")),
                    "uri": c.get("uri", ""),
                    "data_properties": _attr_names(c.get("dataProperties")),
                }
            )
        for p in ontology.get("properties", []) or []:
            if p.get("type") and p.get("type") != "ObjectProperty":
                continue
            object_props.append(
                {
                    "name": p.get("name") or local_name(p.get("uri")),
                    "uri": p.get("uri", ""),
                    "domain": p.get("domain", ""),
                    "range": p.get("range", ""),
                }
            )

    return NormalizedOntology(classes=classes, object_properties=object_props)


# =====================================================
# Source-metadata normalisation
# =====================================================


def normalize_metadata(metadata: dict) -> List[dict]:
    """Return ``[{"name", "columns": [str]}]`` from domain metadata.

    Accepts the ``{"tables": [{"name"|"full_name", "columns": [...]}]}``
    shape produced by the metadata tools.  Column entries may be plain
    strings or ``{"name": ...}`` dicts.
    """
    out: List[dict] = []
    for t in (metadata or {}).get("tables", []) or []:
        cols: List[str] = []
        for c in t.get("columns", []) or []:
            if isinstance(c, str):
                cols.append(c)
            elif isinstance(c, dict) and c.get("name"):
                cols.append(c["name"])
        out.append(
            {
                "name": t.get("full_name") or t.get("name") or "",
                "columns": cols,
            }
        )
    return out
