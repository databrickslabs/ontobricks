"""Lookups over one version's ontology and mappings, for coverage checks."""

from __future__ import annotations

import re
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_WORD = re.compile(r"[^a-z0-9 ]+")

Edge = Tuple[str, str, str]  # (property name, from class, to class) in walk direction


def normalise(text: str) -> str:
    """Lower-case words, camel-case split, punctuation dropped, trailing plural 's' dropped."""
    words = _NON_WORD.sub(" ", _CAMEL.sub(" ", str(text or "")).lower()).split()
    words = [
        w[:-1] if len(w) > 3 and w.endswith("s") and not w.endswith("ss") else w
        for w in words
    ]
    return " ".join(words)


class OntologyIndex:
    """Read-only index of classes, object properties and their mappings."""

    def __init__(self, ontology: Dict[str, Any], assignment: Dict[str, Any]):
        self.classes: Dict[str, Dict[str, Any]] = {
            c["name"]: c for c in ontology.get("classes") or [] if c.get("name")
        }
        self._class_by_uri = {c.get("uri"): name for name, c in self.classes.items()}
        self.relationships: Dict[str, Dict[str, Any]] = {
            p["name"]: p
            for p in ontology.get("properties") or []
            if p.get("name") and p.get("type") == "ObjectProperty"
        }
        self._entities = {
            self._class_by_uri.get(e.get("ontology_class")): e
            for e in assignment.get("entities") or []
            if not e.get("excluded")
        }
        self._rel_mappings = {
            r.get("property"): r
            for r in assignment.get("relationships") or []
            if not r.get("excluded")
        }
        self.base_uri = str(ontology.get("base_uri") or "")

    # -- classes --------------------------------------------------------

    def _forms(self, name: str) -> List[str]:
        cls = self.classes[name]
        raw = [name, cls.get("label") or ""] + list(cls.get("alternate_labels") or [])
        return [f for f in (normalise(r) for r in raw) if f]

    def resolve_mention(self, mention: str) -> List[str]:
        """Classes a phrase may name: exact form, or a form ending with the phrase."""
        m = normalise(mention)
        if not m:
            return []
        hits = []
        for name in self.classes:
            forms = self._forms(name)
            if any(f == m or f.endswith(" " + m) for f in forms):
                hits.append(name)
        return sorted(hits)

    def attribute(self, cls: str, attribute: str) -> Optional[Dict[str, Any]]:
        for attr in self.classes.get(cls, {}).get("dataProperties") or []:
            if attribute in (attr.get("name"), attr.get("localName")):
                return attr
        return None

    # -- mappings -------------------------------------------------------

    def entity(self, cls: str) -> Optional[Dict[str, str]]:
        """``{table, id_column, attributes}`` of the class mapping, or None if unmapped."""
        mapping = self._entities.get(cls)
        if not mapping:
            return None
        parts = [mapping.get("catalog"), mapping.get("schema"), mapping.get("table")]
        table = ".".join(parts) if all(parts) else ""
        return {
            "table": table,
            "id_column": str(mapping.get("id_column") or ""),
            "label_column": str(mapping.get("label_column") or ""),
            "attributes": dict(mapping.get("attribute_mappings") or {}),
        }

    def attribute_column(self, cls: str, attribute: str) -> Optional[str]:
        entity = self.entity(cls)
        if not entity:
            return None
        if attribute == "id":
            return entity["id_column"] or None
        return entity["attributes"].get(attribute)

    def relationship_mapping(self, name: str) -> Optional[Dict[str, Any]]:
        rel = self.relationships.get(name)
        return self._rel_mappings.get(rel.get("uri")) if rel else None

    # -- graph ----------------------------------------------------------

    def neighbours(self, cls: str) -> List[Edge]:
        out = []
        for name, rel in self.relationships.items():
            if rel.get("domain") == cls and rel.get("range") in self.classes:
                out.append((name, cls, rel["range"]))
            if rel.get("range") == cls and rel.get("domain") in self.classes:
                out.append((name, cls, rel["domain"]))
        return out

    def paths(
        self, start: str, goal: str, max_edges: int, limit: int = 5
    ) -> List[List[Edge]]:
        """Shortest-first simple paths from *start* to *goal* (either direction per edge)."""
        found: List[List[Edge]] = []
        queue = deque([(start, [], {start})])
        while queue and len(found) < limit:
            node, path, seen = queue.popleft()
            if node == goal and path:
                found.append(path)
                continue
            if len(path) >= max_edges:
                continue
            for edge in self.neighbours(node):
                nxt = edge[2]
                if nxt not in seen:
                    queue.append((nxt, path + [edge], seen | {nxt}))
        return found

    def component(self, start: str) -> List[str]:
        """Every class reachable from *start* through object properties."""
        seen, queue = {start}, deque([start])
        while queue:
            for _, _, nxt in self.neighbours(queue.popleft()):
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        return sorted(seen)
