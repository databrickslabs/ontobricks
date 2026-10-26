"""Result of an OWL import conflict scan: the report and its conflict items."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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
