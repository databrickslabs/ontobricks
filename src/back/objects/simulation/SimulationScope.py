"""Which slice of the live graph a simulation copies into its sandbox."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from back.core.errors import ValidationError
from back.core.helpers import is_uri
from back.core.logging import get_logger
from back.objects.digitaltwin.constants import RDF_TYPE

logger = get_logger(__name__)


@dataclass
class SimulationScope:
    """Entity neighbourhood, cohort, or class population (capped).

    The slice is every live triple whose *subject* belongs to the resolved
    entity set, so outgoing links and literals of in-scope entities are kept.
    """

    TYPES = ("entity", "cohort", "class")
    MAX_HOPS = 4

    type: str = "entity"
    focus: str = ""
    hops: int = 2
    cap: int = 5000
    focus_label: str = ""

    @classmethod
    def from_dict(
        cls, data: Optional[Dict[str, Any]], *, default_hops: int, default_cap: int
    ) -> "SimulationScope":
        data = data or {}
        try:
            hops = int(data.get("hops", default_hops))
            cap = int(data.get("cap", default_cap))
        except (TypeError, ValueError):
            raise ValidationError("Scope hops and cap must be integers")
        scope = cls(
            type=str(data.get("type", "entity") or "entity").strip().lower(),
            focus=str(data.get("focus", "") or "").strip(),
            hops=hops,
            cap=min(max(1, cap), default_cap),
            focus_label=str(data.get("focus_label", "") or "").strip(),
        )
        scope.validate()
        return scope

    def validate(self) -> None:
        if self.type not in self.TYPES:
            raise ValidationError(
                f"Scope type must be one of {', '.join(self.TYPES)}", detail=self.type
            )
        if not self.focus:
            raise ValidationError(f"Scope '{self.type}' requires a focus value")
        if self.type in ("entity", "class") and not is_uri(self.focus):
            raise ValidationError("Scope focus must be a URI", detail=self.focus)
        if not 0 <= self.hops <= self.MAX_HOPS:
            raise ValidationError(f"Scope hops must be between 0 and {self.MAX_HOPS}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "focus": self.focus,
            "hops": self.hops,
            "cap": self.cap,
            "focus_label": self.focus_label,
        }

    # -- Entity-set resolution against the live store ----------------------

    def resolve_entities(
        self,
        store: Any,
        graph_name: str,
        *,
        cohort_service: Any = None,
        cohort_rule: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """Return the in-scope entity URIs (read-only on the live graph)."""
        if self.type == "entity":
            entities = self._entity_neighbourhood(store, graph_name)
        elif self.type == "cohort":
            entities = self._cohort_members(store, graph_name, cohort_service, cohort_rule)
        else:
            entities = self._class_population(store, graph_name)
        if not entities:
            raise ValidationError("The simulation scope matched no entities")
        return entities

    def _entity_neighbourhood(self, store: Any, graph_name: str) -> List[str]:
        sub = store.expand_and_fetch_subgraph(
            graph_name,
            [self.focus],
            self.hops,
            self.cap,
            self.cap * 50,
        )
        seen: Dict[str, None] = {self.focus: None}
        for t in sub.get("results") or []:
            seen.setdefault(t.get("subject", ""), None)
            obj = t.get("object", "")
            if t.get("predicate") != RDF_TYPE and is_uri(obj):
                seen.setdefault(obj, None)
        seen.pop("", None)
        return list(seen)[: self.cap]

    def _cohort_members(
        self,
        store: Any,
        graph_name: str,
        cohort_service: Any,
        cohort_rule: Optional[Dict[str, Any]],
    ) -> List[str]:
        if cohort_service is None or not cohort_rule:
            raise ValidationError("Cohort rule not found", detail=self.focus)
        payload = cohort_service.dry_run(cohort_rule, store, graph_name)
        members: Dict[str, None] = {}
        for cohort in payload.get("cohorts") or []:
            for m in cohort.get("members") or []:
                uri = m.get("uri") if isinstance(m, dict) else str(m)
                if uri:
                    members.setdefault(uri, None)
        if len(members) > self.cap:
            raise ValidationError(
                f"Cohort has {len(members)} members, above the {self.cap} simulation cap"
            )
        return list(members)

    def _class_population(self, store: Any, graph_name: str) -> List[str]:
        subjects = store.find_subjects_by_type(
            graph_name, self.focus, limit=int(self.cap) + 1
        ) or []
        if len(subjects) > self.cap:
            raise ValidationError(
                f"Class population exceeds the {self.cap} entity simulation cap — "
                "narrow the scope with a cohort or an entity neighbourhood"
            )
        return [s for s in subjects if s]
