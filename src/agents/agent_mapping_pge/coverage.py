"""Deterministic coverage enforcement + derived-mapping construction.

The Planner is good at the *how* (which column is the canonical id, which
tables join, in what order to attempt things) but it must NOT be trusted with
the *what*: the set of entities and relationships that need mapping is fixed —
it is exactly the input ontology.  Leaving coverage to the LLM produced
non-deterministic partial runs (some classes silently dropped into
``mapping_plan.skip``) and, because relationships were skipped whenever an
endpoint entity was absent, zero relationship coverage.

This module computes the FULL, dependency-ordered coverage set from the
ontology itself (using the Planner's order only as a hint), and builds the two
kinds of mapping the LLM Generators cannot/should not produce:

* **Abstract-superclass mappings** — a class with subclasses but no source
  table of its own (``Person``, ``Patient``, ``Clinicalencounter``,
  ``Clinicalfinding``).  Its instances are exactly the UNION of its concrete
  leaf subclasses, so we derive it mechanically from the already-validated
  subclass mappings.  Reusing the subclasses' verbatim ``sql_query`` keeps the
  abstract id-universe byte-identical to the union of its parts, which is what
  makes relationships pointing at the abstract (e.g. ``managedby`` with domain
  ``Clinicalencounter``) join with zero dangling.

* **Synthetic endpoint id-universe** — when a relationship endpoint entity has
  no full mapping yet, we synthesise a minimal ``{sql_query, id_column}`` from
  the Planner's ``canonical_ids`` so the relationship can still be attempted
  instead of silently skipped.

All functions here are pure data transforms — no LLM, no I/O — so they are
fast and unit-testable.
"""

from typing import Dict, List, Optional, Set, Tuple

from back.core.helpers import sql_cast
from back.core.logging import get_logger
from agents.agent_mapping_pge.contracts import SourceModel

logger = get_logger(__name__)


# =====================================================
# Ontology structure helpers
# =====================================================


def _entities(ontology: dict) -> List[dict]:
    return (ontology or {}).get("entities", []) or []


def _relationships(ontology: dict) -> List[dict]:
    return (ontology or {}).get("relationships", []) or []


def _uri(entity: dict) -> str:
    return entity.get("uri") or entity.get("name") or ""


def name_to_uri(ontology: dict) -> Dict[str, str]:
    """Map both short name and label to URI for parent/domain/range resolution."""
    out: Dict[str, str] = {}
    for e in _entities(ontology):
        uri = _uri(e)
        if not uri:
            continue
        out[uri] = uri
        if e.get("name"):
            out[e["name"]] = uri
        if e.get("label"):
            out[e["label"]] = uri
    return out


def parent_uri(entity: dict, n2u: Dict[str, str]) -> Optional[str]:
    """Resolve a class's parent (stored as a name/label/uri) to a URI."""
    p = (entity.get("parent") or "").strip()
    if not p:
        return None
    return n2u.get(p, p if p.startswith("http") else None)


def _tables_for_class(source_model: Optional[SourceModel]) -> Set[str]:
    """URIs that the Planner assigned at least one source table to."""
    if source_model is None:
        return set()
    out: Set[str] = set()
    for role in source_model.table_roles:
        for cand in role.ontology_class_candidates:
            out.add(cand.uri)
    return out


def classify(
    ontology: dict,
    source_model: Optional[SourceModel],
    *,
    synthesized_uris: Optional[Set[str]] = None,
) -> Tuple[Set[str], Set[str]]:
    """Partition classes into (concrete, abstract).

    A class is **abstract/derived** when it has subclasses but no source table
    of its own — its rows are the union of its concrete descendants.  Every
    other class is **concrete** (it has, or will have via synthesis, a source
    table and gets a normal Generator mapping).
    """
    n2u = name_to_uri(ontology)
    has_children: Set[str] = set()
    for e in _entities(ontology):
        p = parent_uri(e, n2u)
        if p:
            has_children.add(p)

    has_table = _tables_for_class(source_model) | (synthesized_uris or set())

    concrete: Set[str] = set()
    abstract: Set[str] = set()
    for e in _entities(ontology):
        uri = _uri(e)
        if not uri:
            continue
        if uri in has_children and uri not in has_table:
            abstract.add(uri)
        else:
            concrete.add(uri)
    return concrete, abstract


def concrete_leaf_descendants(
    abstract_uri: str, ontology: dict, concrete: Set[str]
) -> List[str]:
    """All concrete descendant class URIs beneath ``abstract_uri`` (transitive)."""
    n2u = name_to_uri(ontology)
    children_of: Dict[str, List[str]] = {}
    for e in _entities(ontology):
        p = parent_uri(e, n2u)
        if p:
            children_of.setdefault(p, []).append(_uri(e))

    out: List[str] = []
    stack = list(children_of.get(abstract_uri, []))
    seen: Set[str] = set()
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        if cur in concrete:
            out.append(cur)
        stack.extend(children_of.get(cur, []))
    return out


# =====================================================
# Coverage ordering (engine-enforced — the "what")
# =====================================================


def full_entity_order(
    ontology: dict,
    source_model: Optional[SourceModel],
    *,
    synthesized_uris: Optional[Set[str]] = None,
) -> List[str]:
    """Complete entity order: every class, concrete-first, abstracts after
    their descendants.

    Uses the Planner's ``entity_order`` only to order concrete classes (it
    knows base-vs-referencer dependencies); abstracts are appended in
    descendant-before-ancestor order so a derived union can read its parts.
    """
    concrete, abstract = classify(
        ontology, source_model, synthesized_uris=synthesized_uris
    )
    planned = (
        list(source_model.mapping_plan.entity_order) if source_model else []
    )

    ordered: List[str] = []
    seen: Set[str] = set()

    # 1. Concrete classes in the Planner's order first…
    for uri in planned:
        if uri in concrete and uri not in seen:
            ordered.append(uri)
            seen.add(uri)
    # 2. …then any concrete class the Planner omitted (coverage guarantee).
    for e in _entities(ontology):
        uri = _uri(e)
        if uri in concrete and uri not in seen:
            ordered.append(uri)
            seen.add(uri)

    # 3. Abstracts, ordered so each appears after all its descendants.
    remaining = [u for u in abstract if u not in seen]

    def _depth(uri: str) -> int:
        # number of concrete leaves — deeper subtrees (more leaves) last is
        # fine; what matters is a class never precedes its own descendant.
        return len(concrete_leaf_descendants(uri, ontology, concrete))

    # Topologically: a class with fewer abstract-ancestors first. Simple stable
    # approach: repeatedly emit abstracts whose abstract-children are all done.
    n2u = name_to_uri(ontology)
    abstract_children: Dict[str, List[str]] = {}
    for e in _entities(ontology):
        uri = _uri(e)
        if uri in abstract:
            p = parent_uri(e, n2u)
            if p in abstract:
                abstract_children.setdefault(p, []).append(uri)

    progress = True
    while remaining and progress:
        progress = False
        for uri in list(remaining):
            kids = abstract_children.get(uri, [])
            if all(k in seen for k in kids):
                ordered.append(uri)
                seen.add(uri)
                remaining.remove(uri)
                progress = True
    # Anything left (cycles — shouldn't happen) just append.
    for uri in remaining:
        ordered.append(uri)
        seen.add(uri)

    return ordered


def full_relationship_order(
    ontology: dict, entity_order: List[str], source_model: Optional[SourceModel]
) -> List[str]:
    """Every object property, ordered so both endpoints precede it where
    possible (falls back to Planner order / declaration order)."""
    rels = _relationships(ontology)
    rel_uris = [r.get("uri") or r.get("name") for r in rels]
    rel_uris = [u for u in rel_uris if u]

    planned = (
        list(source_model.mapping_plan.relationship_order) if source_model else []
    )
    ordered: List[str] = []
    seen: Set[str] = set()
    for uri in planned + rel_uris:
        if uri and uri not in seen:
            ordered.append(uri)
            seen.add(uri)
    return ordered


# =====================================================
# Derived mappings (the "how" the LLM cannot produce)
# =====================================================

_ID = "ID"


def _attr_names(entity: dict) -> List[str]:
    out: List[str] = []
    for a in entity.get("attributes", []) or []:
        if isinstance(a, dict):
            n = a.get("name")
            if n:
                out.append(str(n))
        elif a is not None:
            out.append(str(a))
    return out


def build_abstract_union_mapping(
    abstract_uri: str,
    abstract_entity: dict,
    subclass_mappings: List[dict],
) -> Optional[dict]:
    """Build a derived entity mapping for an abstract class as the UNION ALL of
    its concrete subclass mappings.

    Reuses each subclass's verbatim ``sql_query`` (wrapped in a subquery) so the
    abstract id-universe equals the union of the parts exactly.  Projects the
    abstract class's own attributes by re-aliasing each subclass's
    ``attribute_mappings`` value to the ontology attribute name; subclasses that
    do not carry an attribute contribute ``NULL`` for it.
    """
    subs = [m for m in subclass_mappings if m and m.get("sql_query")]
    if not subs:
        return None

    attrs = _attr_names(abstract_entity)
    selects: List[str] = []
    for m in subs:
        amap = m.get("attribute_mappings") or {}
        cols = [_ID]
        for attr in attrs:
            src_alias = amap.get(attr)
            if src_alias:
                cols.append(f"{src_alias} AS {attr}")
            else:
                cols.append(f"{sql_cast('NULL', 'STRING')} AS {attr}")
        selects.append(f"SELECT {', '.join(cols)} FROM ({m['sql_query']}) ")
    union = " UNION ALL ".join(selects)
    # DISTINCT on the whole projection collapses any incidental duplicates while
    # preserving distinct ids (subclass id spaces are disjoint by construction).
    sql = f"SELECT DISTINCT * FROM ({union}) _abstract WHERE {_ID} IS NOT NULL"

    return {
        "class_uri": abstract_uri,
        "ontology_class": abstract_uri,
        "class_name": abstract_entity.get("name", abstract_uri),
        "sql_query": sql,
        "id_column": _ID,
        "label_column": _ID,
        "attribute_mappings": {a: a for a in attrs},
        "unmapped_attributes": [],
        "derived": "abstract_union",
    }


def synthetic_endpoint_mapping(
    source_model: Optional[SourceModel], class_uri: str
) -> Optional[dict]:
    """Build a minimal id-universe-only entity mapping for a relationship
    endpoint from the Planner's ``canonical_ids``.

    Used as a fallback so a relationship is never skipped just because its
    endpoint entity lacks a full mapping.  Produces a UNION ALL of
    ``SELECT <canonical-col-or-expr> AS ID FROM <table>`` over every table the
    Planner recorded for the class.
    """
    if source_model is None:
        return None
    cid = next(
        (c for c in source_model.canonical_ids if c.ontology_class == class_uri),
        None,
    )
    if cid is None or not cid.canonical_column_per_table:
        return None
    selects = [
        f"SELECT {expr} AS {_ID} FROM {table}"
        for table, expr in cid.canonical_column_per_table.items()
        if expr and table
    ]
    if not selects:
        return None
    inner = " UNION ALL ".join(selects)
    sql = f"SELECT DISTINCT {_ID} FROM ({inner}) _u WHERE {_ID} IS NOT NULL"
    return {
        "class_uri": class_uri,
        "ontology_class": class_uri,
        "sql_query": sql,
        "id_column": _ID,
        "attribute_mappings": {},
        "unmapped_attributes": [],
        "derived": "synthetic_endpoint",
    }
