"""Conflict detection for append-mode ontology imports.

Compares an incoming parsed OWL/RDFS result against an existing ontology
session and classifies every incoming entity as one of:

- ``new``           — URI and name both absent → safe to append.
- ``duplicate``     — same URI and equivalent definition → skip silently.
- ``uri_conflict``  — same URI but different definition → user must resolve.
- ``name_conflict`` — same name but different URI → user must resolve.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ConflictItem:
    """Represents a single entity comparison result."""

    entity_type: str                    # "class" | "property" | "constraint" | …
    uri: str                            # Incoming URI (may be empty for unnamed items)
    name: str                           # Incoming name / label
    conflict_type: str                  # "new" | "duplicate" | "uri_conflict" | "name_conflict"
    incoming: Dict[str, Any]
    existing: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "uri": self.uri,
            "name": self.name,
            "conflict_type": self.conflict_type,
            "incoming": self.incoming,
            "existing": self.existing,
        }


@dataclass
class ConflictReport:
    """Aggregated result of a conflict analysis."""

    new_items: List[ConflictItem] = field(default_factory=list)
    duplicates: List[ConflictItem] = field(default_factory=list)
    conflicts: List[ConflictItem] = field(default_factory=list)

    def has_conflicts(self) -> bool:
        return bool(self.conflicts)

    def total_incoming(self) -> int:
        return len(self.new_items) + len(self.duplicates) + len(self.conflicts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "new_items": [i.to_dict() for i in self.new_items],
            "duplicates": [i.to_dict() for i in self.duplicates],
            "conflicts": [i.to_dict() for i in self.conflicts],
            "has_conflicts": self.has_conflicts(),
            "summary": {
                "new": len(self.new_items),
                "duplicates": len(self.duplicates),
                "conflicts": len(self.conflicts),
                "total_incoming": self.total_incoming(),
            },
        }


# ---------------------------------------------------------------------------
# Equality helpers
# ---------------------------------------------------------------------------

def _norm(value: Any) -> str:
    """Normalise a value to a lowercase string for loose comparison."""
    return str(value or "").strip().lower()


def _classes_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """True when two class dicts represent the same definition."""
    # Consider equal if same name AND same parent (or both have no parent).
    same_name = _norm(a.get("name") or a.get("label")) == _norm(b.get("name") or b.get("label"))
    same_parent = _norm(a.get("parent_uri") or a.get("parent")) == _norm(
        b.get("parent_uri") or b.get("parent")
    )
    return same_name and same_parent


def _properties_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """True when two property dicts represent the same definition."""
    same_name = _norm(a.get("name")) == _norm(b.get("name"))
    same_type = _norm(a.get("type")) == _norm(b.get("type"))
    same_range = _norm(a.get("range") or a.get("rangeLabel")) == _norm(
        b.get("range") or b.get("rangeLabel")
    )
    return same_name and same_type and same_range


def _generic_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Loose equality for constraints, axioms, expressions: compare serialised bodies."""
    def _sig(d: Dict[str, Any]) -> str:
        return json.dumps({k: v for k, v in sorted(d.items()) if k not in ("id", "uri")},
                          sort_keys=True, default=str)
    return _sig(a) == _sig(b)


def _name_key(item: Dict[str, Any]) -> str:
    return _norm(item.get("name") or item.get("label") or "")


def _uri_key(item: Dict[str, Any]) -> str:
    return (item.get("uri") or "").strip()


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

class OntologyConflictDetector:
    """Compares incoming parsed entities against an existing ontology."""

    def analyze(
        self,
        existing: Dict[str, Any],
        incoming: Dict[str, Any],
    ) -> ConflictReport:
        """Return a :class:`ConflictReport` for the full incoming ontology.

        Parameters
        ----------
        existing:
            ``domain.ontology`` dict (session state).
        incoming:
            Dict with keys ``classes``, ``properties``, ``constraints``,
            ``swrl_rules``, ``axioms``, ``expressions``, ``groups`` — the
            structured output from the OWL/RDFS parser.
        """
        report = ConflictReport()

        self._analyze_list(
            entity_type="class",
            existing_list=existing.get("classes") or [],
            incoming_list=incoming.get("classes") or [],
            equality_fn=_classes_equal,
            report=report,
        )
        self._analyze_list(
            entity_type="property",
            existing_list=existing.get("properties") or [],
            incoming_list=incoming.get("properties") or [],
            equality_fn=_properties_equal,
            report=report,
        )
        self._analyze_named_list(
            entity_type="constraint",
            existing_list=existing.get("constraints") or [],
            incoming_list=incoming.get("constraints") or [],
            equality_fn=_generic_equal,
            report=report,
        )
        self._analyze_named_list(
            entity_type="swrl_rule",
            existing_list=existing.get("swrl_rules") or [],
            incoming_list=incoming.get("swrl_rules") or [],
            equality_fn=_generic_equal,
            report=report,
        )
        self._analyze_named_list(
            entity_type="group",
            existing_list=existing.get("groups") or [],
            incoming_list=incoming.get("groups") or [],
            equality_fn=_generic_equal,
            report=report,
        )
        # Axioms and expressions: compare by body (no stable URI/name)
        self._analyze_body_list(
            entity_type="axiom",
            existing_list=existing.get("axioms") or [],
            incoming_list=incoming.get("axioms") or [],
            report=report,
        )
        self._analyze_body_list(
            entity_type="expression",
            existing_list=existing.get("expressions") or [],
            incoming_list=incoming.get("expressions") or [],
            report=report,
        )
        return report

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _analyze_list(
        self,
        *,
        entity_type: str,
        existing_list: List[Dict],
        incoming_list: List[Dict],
        equality_fn,
        report: ConflictReport,
    ) -> None:
        """Classify items that have both a URI and a name."""
        existing_by_uri: Dict[str, Dict] = {
            _uri_key(e): e for e in existing_list if _uri_key(e)
        }
        existing_by_name: Dict[str, Dict] = {
            _name_key(e): e for e in existing_list if _name_key(e)
        }

        for inc in incoming_list:
            inc_uri = _uri_key(inc)
            inc_name = _name_key(inc)

            if inc_uri and inc_uri in existing_by_uri:
                ex = existing_by_uri[inc_uri]
                if equality_fn(inc, ex):
                    report.duplicates.append(
                        ConflictItem(
                            entity_type=entity_type,
                            uri=inc_uri,
                            name=inc_name,
                            conflict_type="duplicate",
                            incoming=inc,
                            existing=ex,
                        )
                    )
                else:
                    report.conflicts.append(
                        ConflictItem(
                            entity_type=entity_type,
                            uri=inc_uri,
                            name=inc_name,
                            conflict_type="uri_conflict",
                            incoming=inc,
                            existing=ex,
                        )
                    )
            elif inc_name and inc_name in existing_by_name and inc_uri != _uri_key(
                existing_by_name[inc_name]
            ):
                ex = existing_by_name[inc_name]
                report.conflicts.append(
                    ConflictItem(
                        entity_type=entity_type,
                        uri=inc_uri,
                        name=inc_name,
                        conflict_type="name_conflict",
                        incoming=inc,
                        existing=ex,
                    )
                )
            else:
                report.new_items.append(
                    ConflictItem(
                        entity_type=entity_type,
                        uri=inc_uri,
                        name=inc_name,
                        conflict_type="new",
                        incoming=inc,
                        existing=None,
                    )
                )

    def _analyze_named_list(
        self,
        *,
        entity_type: str,
        existing_list: List[Dict],
        incoming_list: List[Dict],
        equality_fn,
        report: ConflictReport,
    ) -> None:
        """Classify items identified only by name (constraints, SWRL rules, groups)."""
        existing_by_name: Dict[str, Dict] = {
            _name_key(e): e for e in existing_list if _name_key(e)
        }

        for inc in incoming_list:
            inc_name = _name_key(inc)
            inc_uri = _uri_key(inc)

            if inc_name and inc_name in existing_by_name:
                ex = existing_by_name[inc_name]
                if equality_fn(inc, ex):
                    report.duplicates.append(
                        ConflictItem(
                            entity_type=entity_type,
                            uri=inc_uri,
                            name=inc_name,
                            conflict_type="duplicate",
                            incoming=inc,
                            existing=ex,
                        )
                    )
                else:
                    report.conflicts.append(
                        ConflictItem(
                            entity_type=entity_type,
                            uri=inc_uri,
                            name=inc_name,
                            conflict_type="uri_conflict",
                            incoming=inc,
                            existing=ex,
                        )
                    )
            else:
                report.new_items.append(
                    ConflictItem(
                        entity_type=entity_type,
                        uri=inc_uri,
                        name=inc_name,
                        conflict_type="new",
                        incoming=inc,
                        existing=None,
                    )
                )

    def _analyze_body_list(
        self,
        *,
        entity_type: str,
        existing_list: List[Dict],
        incoming_list: List[Dict],
        report: ConflictReport,
    ) -> None:
        """Classify items without stable identifiers (axioms, expressions) by body hash."""
        def _sig(d: Dict) -> str:
            return json.dumps(
                {k: v for k, v in sorted(d.items()) if k not in ("id",)},
                sort_keys=True,
                default=str,
            )

        existing_sigs = {_sig(e) for e in existing_list}

        for inc in incoming_list:
            sig = _sig(inc)
            if sig in existing_sigs:
                report.duplicates.append(
                    ConflictItem(
                        entity_type=entity_type,
                        uri=_uri_key(inc),
                        name=_name_key(inc),
                        conflict_type="duplicate",
                        incoming=inc,
                        existing=None,
                    )
                )
            else:
                report.new_items.append(
                    ConflictItem(
                        entity_type=entity_type,
                        uri=_uri_key(inc),
                        name=_name_key(inc),
                        conflict_type="new",
                        incoming=inc,
                        existing=None,
                    )
                )
