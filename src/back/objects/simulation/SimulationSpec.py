"""Definition of a what-if simulation: scope, patches, reasoning phases."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from back.core.errors import ValidationError
from back.objects.simulation.SimulationPatch import SimulationPatch
from back.objects.simulation.SimulationScope import SimulationScope


@dataclass
class SimulationSpec:
    """User-editable part of a simulation (persisted as ``spec`` jsonb)."""

    DEFAULT_PHASES = {
        "tbox": False,
        "swrl": True,
        "graph": True,
        "decision_tables": False,
        "sparql_rules": False,
        "aggregate_rules": False,
    }
    RULE_NAME_PHASES = (
        "swrl",
        "decision_tables",
        "sparql_rules",
        "aggregate_rules",
    )
    _RULE_OPTION_KEYS = {
        "swrl": "swrl_rule_names",
        "decision_tables": "decision_table_names",
        "sparql_rules": "sparql_rule_names",
        "aggregate_rules": "aggregate_rule_names",
    }
    MAX_PATCHES = 500
    MAX_LABEL_LEN = 200
    MAX_RULE_NAMES = 500

    id: str
    label: str
    scope: SimulationScope
    patches: List[SimulationPatch] = field(default_factory=list)
    phases: Dict[str, bool] = field(default_factory=dict)
    rule_names: Dict[str, List[str]] = field(default_factory=dict)
    description: str = ""

    @staticmethod
    def new_id() -> str:
        """Short id safe to embed in a Postgres identifier (letter-first)."""
        return "s" + uuid.uuid4().hex[:10]

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        *,
        default_hops: int,
        default_cap: int,
        sim_id: Optional[str] = None,
    ) -> "SimulationSpec":
        if not isinstance(data, dict):
            raise ValidationError("Simulation body must be an object")
        label = str(data.get("label", "") or "").strip()
        if not label:
            raise ValidationError("Simulation label is required")
        if len(label) > cls.MAX_LABEL_LEN:
            raise ValidationError(
                f"Simulation label must be at most {cls.MAX_LABEL_LEN} characters"
            )
        raw_patches = data.get("patches") or []
        if not isinstance(raw_patches, list):
            raise ValidationError("Patches must be a list")
        if len(raw_patches) > cls.MAX_PATCHES:
            raise ValidationError(f"At most {cls.MAX_PATCHES} patches per simulation")
        raw_phases = data.get("phases") or {}
        if not isinstance(raw_phases, dict):
            raise ValidationError("Phases must be an object")
        phases = {
            key: bool(raw_phases.get(key, default))
            for key, default in cls.DEFAULT_PHASES.items()
        }
        return cls(
            id=sim_id or cls.new_id(),
            label=label,
            description=str(data.get("description", "") or "").strip(),
            scope=SimulationScope.from_dict(
                data.get("scope"), default_hops=default_hops, default_cap=default_cap
            ),
            patches=[SimulationPatch.from_dict(p) for p in raw_patches],
            phases=phases,
            rule_names=cls._parse_rule_names(data.get("rule_names")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "description": self.description,
            "scope": self.scope.to_dict(),
            "patches": [p.to_dict() for p in self.patches],
            "phases": dict(self.phases),
            "rule_names": {key: list(names) for key, names in self.rule_names.items()},
        }

    def reasoning_options(self) -> Dict[str, Any]:
        """Options for :meth:`ReasoningService.run_full_reasoning` (never materialise)."""
        opts: Dict[str, Any] = dict(self.phases)
        opts["materialize"] = False
        for phase, option_key in self._RULE_OPTION_KEYS.items():
            names = self.rule_names.get(phase) or []
            if names and opts.get(phase):
                opts[option_key] = set(names)
        return opts

    @classmethod
    def _parse_rule_names(cls, raw: Any) -> Dict[str, List[str]]:
        if raw is None or raw == {}:
            return {}
        if not isinstance(raw, dict):
            raise ValidationError("Rule names must be an object")
        out: Dict[str, List[str]] = {}
        for phase in cls.RULE_NAME_PHASES:
            names = raw.get(phase)
            if names is None:
                continue
            if not isinstance(names, list):
                raise ValidationError(f"Rule names for '{phase}' must be a list")
            cleaned: List[str] = []
            seen = set()
            for name in names:
                text = str(name or "").strip()
                if not text or text in seen:
                    continue
                seen.add(text)
                cleaned.append(text)
            if len(cleaned) > cls.MAX_RULE_NAMES:
                raise ValidationError(
                    f"At most {cls.MAX_RULE_NAMES} selected rules per phase"
                )
            if cleaned:
                out[phase] = cleaned
        return out
