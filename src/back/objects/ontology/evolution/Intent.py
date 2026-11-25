"""Typed interpretation of a business question."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from back.core.errors import ValidationError
from back.objects.ontology.evolution.IntentFilter import IntentFilter

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class Intent:
    """What the question asks for, entered by the user or extracted by the agent.

    ``aggregate`` and ``select`` are kept so coverage can report them as
    unsupported instead of silently dropping them.
    """

    MAX_MENTIONS = 20
    MAX_FILTERS = 10
    MAX_EDGES_CEILING = 3

    output_class: Optional[str]
    mentions: List[str] = field(default_factory=list)
    path_hint: List[str] = field(default_factory=list)
    filters: List[IntentFilter] = field(default_factory=list)
    aggregate: Optional[Dict[str, Any]] = None
    select: List[str] = field(default_factory=list)
    max_edges: int = MAX_EDGES_CEILING
    schema_version: int = SCHEMA_VERSION

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "Intent":
        if not isinstance(data, dict):
            raise ValidationError("Intent must be an object")
        mentions = [
            str(m).strip() for m in (data.get("mentions") or []) if str(m).strip()
        ]
        filters = [IntentFilter.from_dict(f) for f in (data.get("filters") or [])]
        if len(mentions) > cls.MAX_MENTIONS or len(filters) > cls.MAX_FILTERS:
            raise ValidationError("Intent has too many mentions or filters")
        try:
            max_edges = int(data.get("max_edges") or cls.MAX_EDGES_CEILING)
        except (TypeError, ValueError):
            raise ValidationError("max_edges must be an integer")
        if not 1 <= max_edges <= cls.MAX_EDGES_CEILING:
            raise ValidationError(
                f"max_edges must be between 1 and {cls.MAX_EDGES_CEILING}"
            )
        aggregate = data.get("aggregate")
        if aggregate is not None and not isinstance(aggregate, dict):
            raise ValidationError("aggregate must be an object")
        output = data.get("output_class")
        return cls(
            output_class=str(output).strip() if output else None,
            mentions=mentions,
            path_hint=[
                str(p).strip() for p in (data.get("path_hint") or []) if str(p).strip()
            ],
            filters=filters,
            aggregate=aggregate,
            select=[
                str(s).strip() for s in (data.get("select") or []) if str(s).strip()
            ],
            max_edges=max_edges,
        )

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["filters"] = [f.to_dict() for f in self.filters]
        return out
