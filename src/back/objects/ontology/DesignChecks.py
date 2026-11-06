"""Advisory ontology design checks (integrity, hygiene, naming, hierarchy).

Findings never block save, mapping, build, or publish. The report is a
stateless scan of the in-session class / property / group dicts.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

_CLASS_NAME_RE = re.compile(r"^[A-Z][A-Za-z0-9]*$")
_PROP_NAME_RE = re.compile(r"^[a-z][A-Za-z0-9]*$")
_STRIP_CHARS = str.maketrans("", "", "_- ")

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"

KIND_CLASS = "class"
KIND_PROPERTY = "property"
KIND_ATTRIBUTE = "attribute"
KIND_GROUP = "group"

MAX_HIERARCHY_DEPTH = 5
NEAR_DUP_MIN_LEN = 5

# User-facing nouns: OntoBricks says "entity" (not class) and "domain" means
# an OntoBricks domain, never the RDF rdfs:domain.
_KIND_NOUNS = {
    KIND_CLASS: "entity",
    KIND_ATTRIBUTE: "attribute",
    KIND_PROPERTY: "relationship",
    KIND_GROUP: "group",
}
_NOUN_PLURALS = {"entity": "entities"}

RULE_META: Dict[str, Dict[str, str]] = {
    "DC-E01": {"severity": SEVERITY_ERROR, "title": "Broken parent"},
    "DC-E02": {"severity": SEVERITY_ERROR, "title": "Broken entity link"},
    "DC-E03": {"severity": SEVERITY_ERROR, "title": "Broken range"},
    "DC-E04": {"severity": SEVERITY_ERROR, "title": "Broken group member"},
    "DC-E05": {"severity": SEVERITY_ERROR, "title": "Hierarchy cycle"},
    "DC-E06": {"severity": SEVERITY_ERROR, "title": "Duplicate URI"},
    "DC-W01": {"severity": SEVERITY_WARNING, "title": "Orphan entity"},
    "DC-W02": {"severity": SEVERITY_WARNING, "title": "Missing label"},
    "DC-W03": {"severity": SEVERITY_WARNING, "title": "Missing entity"},
    "DC-W04": {"severity": SEVERITY_WARNING, "title": "Missing range"},
    "DC-W05": {"severity": SEVERITY_WARNING, "title": "Duplicate name"},
    "DC-I01": {"severity": SEVERITY_INFO, "title": "Missing description"},
    "DC-I02": {"severity": SEVERITY_INFO, "title": "Entity naming"},
    "DC-I03": {"severity": SEVERITY_INFO, "title": "Attribute / relationship naming"},
    "DC-I04": {"severity": SEVERITY_INFO, "title": "Near-duplicate name"},
    "DC-I05": {"severity": SEVERITY_INFO, "title": "Deep hierarchy"},
}


class DesignChecks:
    """Run the ontology design-check catalog against session entities."""

    @staticmethod
    def run(
        classes: Optional[List[Dict[str, Any]]] = None,
        properties: Optional[List[Dict[str, Any]]] = None,
        groups: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Return ``{"findings": [...], "summary": {error, warning, info}}``."""
        classes = list(classes or [])
        properties = list(properties or [])
        groups = list(groups or [])
        findings: List[Dict[str, Any]] = []
        index = _ClassIndex(classes)

        DesignChecks._check_broken_parents(classes, index, findings)
        DesignChecks._check_property_refs(properties, index, findings)
        DesignChecks._check_group_members(groups, index, findings)
        DesignChecks._check_cycles(classes, index, findings)
        DesignChecks._check_duplicate_uris(classes, KIND_CLASS, findings)
        DesignChecks._check_duplicate_uris(properties, KIND_PROPERTY, findings)
        DesignChecks._check_orphans(classes, properties, groups, index, findings)
        DesignChecks._check_missing_labels(classes, KIND_CLASS, findings)
        DesignChecks._check_missing_labels(properties, KIND_PROPERTY, findings)
        DesignChecks._check_missing_descriptions(classes, KIND_CLASS, findings)
        DesignChecks._check_missing_descriptions(properties, KIND_PROPERTY, findings)
        DesignChecks._check_duplicate_names(classes, KIND_CLASS, findings)
        DesignChecks._check_duplicate_names(properties, KIND_PROPERTY, findings)
        DesignChecks._check_class_naming(classes, findings)
        DesignChecks._check_property_naming(properties, findings)
        DesignChecks._check_near_duplicates(classes, KIND_CLASS, findings)
        DesignChecks._check_near_duplicates(properties, KIND_PROPERTY, findings)
        DesignChecks._check_deep_hierarchy(classes, index, findings)

        summary = {"error": 0, "warning": 0, "info": 0}
        for item in findings:
            summary[item["severity"]] = summary.get(item["severity"], 0) + 1
        return {"findings": findings, "summary": summary}

    @staticmethod
    def _check_broken_parents(
        classes: List[Dict[str, Any]],
        index: "_ClassIndex",
        findings: List[Dict[str, Any]],
    ) -> None:
        for cls in classes:
            parent = _parent_ref(cls)
            if not parent:
                continue
            if index.resolve(parent) is None:
                findings.append(
                    _finding(
                        "DC-E01",
                        KIND_CLASS,
                        cls,
                        f"Parent '{parent}' is not an entity in this ontology.",
                    )
                )

    @staticmethod
    def _check_property_refs(
        properties: List[Dict[str, Any]],
        index: "_ClassIndex",
        findings: List[Dict[str, Any]],
    ) -> None:
        for prop in properties:
            is_attr = _is_datatype_property(prop)
            owner = _text(prop.get("domain"))
            if owner and index.resolve(owner) is None:
                findings.append(
                    _finding(
                        "DC-E02",
                        KIND_PROPERTY,
                        prop,
                        (
                            f"Owning entity '{owner}' does not exist in this ontology."
                            if is_attr
                            else f"Source entity '{owner}' does not exist in this ontology."
                        ),
                    )
                )
            elif not owner:
                findings.append(
                    _finding(
                        "DC-W03",
                        KIND_PROPERTY,
                        prop,
                        (
                            "Attribute is not attached to any entity."
                            if is_attr
                            else "Relationship has no source entity."
                        ),
                    )
                )

            range_ref = _text(prop.get("range") or prop.get("rangeLabel"))
            if not range_ref:
                findings.append(
                    _finding(
                        "DC-W04",
                        KIND_PROPERTY,
                        prop,
                        (
                            "Attribute has no range (datatype)."
                            if is_attr
                            else "Relationship has no range (target entity)."
                        ),
                    )
                )
            elif _is_object_property(prop) and index.resolve(range_ref) is None:
                findings.append(
                    _finding(
                        "DC-E03",
                        KIND_PROPERTY,
                        prop,
                        f"Range '{range_ref}' is not an entity in this ontology.",
                    )
                )

    @staticmethod
    def _check_group_members(
        groups: List[Dict[str, Any]],
        index: "_ClassIndex",
        findings: List[Dict[str, Any]],
    ) -> None:
        for group in groups:
            for member in group.get("members") or []:
                ref = _text(member)
                if not ref:
                    continue
                if index.resolve(ref) is None:
                    finding = _finding(
                        "DC-E04",
                        KIND_GROUP,
                        group,
                        f"Member '{ref}' is not an entity in this ontology.",
                    )
                    finding["member"] = ref
                    findings.append(finding)

    @staticmethod
    def _check_cycles(
        classes: List[Dict[str, Any]],
        index: "_ClassIndex",
        findings: List[Dict[str, Any]],
    ) -> None:
        parent_map = index.parent_keys()
        reported: set[frozenset[str]] = set()
        for start in parent_map:
            seen: Dict[str, int] = {}
            path: List[str] = []
            current: Optional[str] = start
            while current:
                if current in seen:
                    cycle = path[seen[current] :]
                    key = frozenset(cycle)
                    if cycle and key not in reported:
                        reported.add(key)
                        names = " → ".join(cycle + [cycle[0]])
                        first = index.by_key.get(cycle[0]) or {}
                        findings.append(
                            _finding(
                                "DC-E05",
                                KIND_CLASS,
                                first,
                                f"Parent cycle: {names}.",
                            )
                        )
                    break
                seen[current] = len(path)
                path.append(current)
                nxt = parent_map.get(current)
                if not nxt or nxt not in parent_map:
                    break
                current = nxt

    @staticmethod
    def _check_duplicate_uris(
        items: List[Dict[str, Any]],
        kind: str,
        findings: List[Dict[str, Any]],
    ) -> None:
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            uri = _text(item.get("uri"))
            if not uri:
                continue
            buckets.setdefault(uri, []).append(item)
        for uri, group in buckets.items():
            if len(group) < 2:
                continue
            names = ", ".join(_display_name(i) or "(unnamed)" for i in group)
            findings.append(
                _finding(
                    "DC-E06",
                    kind,
                    group[0],
                    f"URI '{uri}' is used by {len(group)} {_plural(kind, group)}: {names}.",
                )
            )

    @staticmethod
    def _check_orphans(
        classes: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        groups: List[Dict[str, Any]],
        index: "_ClassIndex",
        findings: List[Dict[str, Any]],
    ) -> None:
        used: set[str] = set()
        for cls in classes:
            parent = _parent_ref(cls)
            resolved = index.resolve(parent) if parent else None
            if resolved is not None:
                child_key = index.key_of(cls)
                parent_key = index.key_of(resolved)
                if child_key:
                    used.add(child_key)
                if parent_key:
                    used.add(parent_key)
        for prop in properties:
            for ref in (prop.get("domain"), prop.get("range"), prop.get("rangeLabel")):
                resolved = index.resolve(_text(ref))
                if resolved is not None:
                    key = index.key_of(resolved)
                    if key:
                        used.add(key)
        for group in groups:
            for member in group.get("members") or []:
                resolved = index.resolve(_text(member))
                if resolved is not None:
                    key = index.key_of(resolved)
                    if key:
                        used.add(key)
        for cls in classes:
            key = index.key_of(cls)
            if key and key not in used:
                findings.append(
                    _finding(
                        "DC-W01",
                        KIND_CLASS,
                        cls,
                        "Entity has no parent, children, group membership, attributes, or relationships.",
                    )
                )

    @staticmethod
    def _check_missing_labels(
        items: List[Dict[str, Any]],
        kind: str,
        findings: List[Dict[str, Any]],
    ) -> None:
        for item in items:
            if _text(item.get("label")):
                continue
            findings.append(
                _finding(
                    "DC-W02", kind, item, f"{_noun(kind, item).capitalize()} has no label."
                )
            )

    @staticmethod
    def _check_missing_descriptions(
        items: List[Dict[str, Any]],
        kind: str,
        findings: List[Dict[str, Any]],
    ) -> None:
        for item in items:
            if _text(item.get("description")) or _text(item.get("comment")):
                continue
            findings.append(
                _finding(
                    "DC-I01",
                    kind,
                    item,
                    f"{_noun(kind, item).capitalize()} has no description.",
                )
            )

    @staticmethod
    def _check_duplicate_names(
        items: List[Dict[str, Any]],
        kind: str,
        findings: List[Dict[str, Any]],
    ) -> None:
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            norm = _norm(_display_name(item))
            if not norm:
                continue
            buckets.setdefault(norm, []).append(item)
        for norm, group in buckets.items():
            uris = {_text(i.get("uri")) for i in group}
            uris.discard("")
            if len(group) < 2:
                continue
            if len(uris) <= 1 and len(group) == len(uris):
                continue
            distinct = len(uris) >= 2 or (not uris and len(group) >= 2)
            if not distinct:
                continue
            names = ", ".join(_display_name(i) or "(unnamed)" for i in group)
            findings.append(
                _finding(
                    "DC-W05",
                    kind,
                    group[0],
                    f"Normalized name '{norm}' is shared by {_plural(kind, group)}: {names}.",
                )
            )

    @staticmethod
    def _check_class_naming(
        classes: List[Dict[str, Any]],
        findings: List[Dict[str, Any]],
    ) -> None:
        for cls in classes:
            local = _local_name(cls)
            if not local:
                continue
            if not _CLASS_NAME_RE.match(local):
                findings.append(
                    _finding(
                        "DC-I02",
                        KIND_CLASS,
                        cls,
                        f"Entity local name '{local}' should be PascalCase.",
                    )
                )

    @staticmethod
    def _check_property_naming(
        properties: List[Dict[str, Any]],
        findings: List[Dict[str, Any]],
    ) -> None:
        for prop in properties:
            local = _local_name(prop)
            if not local:
                continue
            if not _PROP_NAME_RE.match(local):
                findings.append(
                    _finding(
                        "DC-I03",
                        KIND_PROPERTY,
                        prop,
                        f"{_noun(KIND_PROPERTY, prop).capitalize()} local name "
                        f"'{local}' should be camelCase.",
                    )
                )

    @staticmethod
    def _check_near_duplicates(
        items: List[Dict[str, Any]],
        kind: str,
        findings: List[Dict[str, Any]],
    ) -> None:
        entries: List[Tuple[str, Dict[str, Any]]] = []
        seen_exact: Dict[str, int] = {}
        for item in items:
            norm = _norm(_display_name(item))
            if not norm:
                continue
            seen_exact[norm] = seen_exact.get(norm, 0) + 1
            entries.append((norm, item))
        reported: set[frozenset[str]] = set()
        for i, (left, left_item) in enumerate(entries):
            if seen_exact.get(left, 0) > 1:
                continue
            if len(left) < NEAR_DUP_MIN_LEN:
                continue
            for right, right_item in entries[i + 1 :]:
                if seen_exact.get(right, 0) > 1:
                    continue
                if len(right) < NEAR_DUP_MIN_LEN:
                    continue
                if left == right:
                    continue
                if _levenshtein(left, right) != 1:
                    continue
                pair = frozenset((left, right))
                if pair in reported:
                    continue
                reported.add(pair)
                findings.append(
                    _finding(
                        "DC-I04",
                        kind,
                        left_item,
                        f"'{_display_name(left_item)}' is one character from "
                        f"'{_display_name(right_item)}'.",
                    )
                )

    @staticmethod
    def _check_deep_hierarchy(
        classes: List[Dict[str, Any]],
        index: "_ClassIndex",
        findings: List[Dict[str, Any]],
    ) -> None:
        parent_map = index.parent_keys()
        for cls in classes:
            key = index.key_of(cls)
            if not key:
                continue
            depth = 0
            seen: set[str] = set()
            current = parent_map.get(key)
            while current:
                if current in seen:
                    depth = 0
                    break
                seen.add(current)
                depth += 1
                current = parent_map.get(current)
            if depth > MAX_HIERARCHY_DEPTH:
                findings.append(
                    _finding(
                        "DC-I05",
                        KIND_CLASS,
                        cls,
                        f"Entity sits at depth {depth} (limit {MAX_HIERARCHY_DEPTH}).",
                    )
                )


class _ClassIndex:
    """Resolve class refs by URI, then by normalized name."""

    def __init__(self, classes: Iterable[Dict[str, Any]]) -> None:
        self.classes = list(classes)
        self.by_uri: Dict[str, Dict[str, Any]] = {}
        self.by_norm: Dict[str, Dict[str, Any]] = {}
        self.by_key: Dict[str, Dict[str, Any]] = {}
        for cls in self.classes:
            uri = _text(cls.get("uri"))
            if uri:
                self.by_uri[uri] = cls
            name = _display_name(cls)
            if name:
                self.by_norm[_norm(name)] = cls
            key = self.key_of(cls)
            if key:
                self.by_key[key] = cls

    def resolve(self, ref: str) -> Optional[Dict[str, Any]]:
        token = _text(ref)
        if not token:
            return None
        hit = self.by_uri.get(token)
        if hit is not None:
            return hit
        return self.by_norm.get(_norm(token))

    def key_of(self, cls: Dict[str, Any]) -> str:
        return _text(cls.get("uri")) or _norm(_display_name(cls))

    def parent_keys(self) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for cls in self.classes:
            key = self.key_of(cls)
            if not key:
                continue
            parent = self.resolve(_parent_ref(cls))
            if parent is None:
                mapping[key] = ""
            else:
                mapping[key] = self.key_of(parent)
        return mapping


def _finding(
    rule: str,
    kind: str,
    item: Dict[str, Any],
    message: str,
) -> Dict[str, Any]:
    meta = RULE_META[rule]
    # DatatypeProperties live under Entities as attributes. Tagging them as
    # ``property`` would send Sanity Checks clicks to Relationships, which
    # only lists ObjectProperties — the finding would look like a dead link.
    if kind == KIND_PROPERTY and _is_datatype_property(item):
        kind = KIND_ATTRIBUTE
    finding = {
        "rule": rule,
        "severity": meta["severity"],
        "kind": kind,
        "uri": _text(item.get("uri")),
        "name": _display_name(item),
        "message": message,
    }
    domain = _text(item.get("domain"))
    if domain:
        finding["domain"] = domain
    return finding


def _noun(kind: str, item: Optional[Dict[str, Any]] = None) -> str:
    if kind == KIND_PROPERTY and item is not None and _is_datatype_property(item):
        kind = KIND_ATTRIBUTE
    return _KIND_NOUNS.get(kind, kind)


def _plural(kind: str, items: List[Dict[str, Any]]) -> str:
    nouns = sorted({_noun(kind, i) for i in items})
    return "/".join(_NOUN_PLURALS.get(n, n + "s") for n in nouns)


def _parent_ref(cls: Dict[str, Any]) -> str:
    return _text(cls.get("parent_uri") or cls.get("parent"))


def _display_name(item: Dict[str, Any]) -> str:
    return _text(
        item.get("name") or item.get("localName") or item.get("label")
    )


def _local_name(item: Dict[str, Any]) -> str:
    local = _text(item.get("localName") or item.get("name"))
    if local:
        return local
    uri = _text(item.get("uri"))
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    if "/" in uri:
        return uri.rsplit("/", 1)[-1]
    return uri


def _text(value: Any) -> str:
    return str(value or "").strip()


def _norm(value: str) -> str:
    return _text(value).lower().translate(_STRIP_CHARS)


def _is_object_property(prop: Dict[str, Any]) -> bool:
    return _text(prop.get("type")).replace("owl:", "") == "ObjectProperty"


def _is_datatype_property(prop: Dict[str, Any]) -> bool:
    return _text(prop.get("type")).replace("owl:", "") == "DatatypeProperty"


def _levenshtein(left: str, right: str) -> int:
    if left == right:
        return 0
    if abs(len(left) - len(right)) > 1:
        return 2
    if not left:
        return len(right)
    if not right:
        return len(left)
    prev = list(range(len(right) + 1))
    for i, lch in enumerate(left, start=1):
        row = [i]
        for j, rch in enumerate(right, start=1):
            ins = row[j - 1] + 1
            delete = prev[j] + 1
            sub = prev[j - 1] + (lch != rch)
            row.append(min(ins, delete, sub))
        prev = row
    return prev[-1]
