"""Data models for graph analysis: community detection and cohort discovery."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Community detection (existing)
# ---------------------------------------------------------------------------


@dataclass
class ClusterRequest:
    """Parameters for a community detection request."""

    algorithm: str = "louvain"
    resolution: float = 1.0
    predicate_filter: Optional[List[str]] = None
    class_filter: Optional[List[str]] = None
    max_triples: int = 500_000


@dataclass
class ClusterResult:
    """A single detected community cluster."""

    id: int
    members: List[str] = field(default_factory=list)
    size: int = 0


@dataclass
class DetectionStats:
    """Aggregate statistics from a community detection run."""

    node_count: int = 0
    edge_count: int = 0
    cluster_count: int = 0
    modularity: float = 0.0
    algorithm: str = "louvain"
    elapsed_ms: int = 0


@dataclass
class DetectionResult:
    """Full result of a community detection run."""

    clusters: List[ClusterResult] = field(default_factory=list)
    stats: DetectionStats = field(default_factory=DetectionStats)


# ---------------------------------------------------------------------------
# Cohort discovery
# ---------------------------------------------------------------------------


# Compatibility primitive types.
_COMPAT_SAME_VALUE = "same_value"
_COMPAT_VALUE_EQUALS = "value_equals"
_COMPAT_VALUE_IN = "value_in"
_COMPAT_VALUE_RANGE = "value_range"
COHORT_COMPAT_TYPES = frozenset(
    {
        _COMPAT_SAME_VALUE,
        _COMPAT_VALUE_EQUALS,
        _COMPAT_VALUE_IN,
        _COMPAT_VALUE_RANGE,
    }
)
COHORT_GROUP_TYPES = frozenset({"connected", "strict"})
COHORT_LINKS_COMBINE = frozenset({"any", "all"})


@dataclass
class CohortHop:
    """One hop along a multi-hop linkage path.

    *via* — predicate URI traversed in this hop (e.g. ``:assignedTo``).
    *target_class* — class URI of the hop's target node (e.g. ``:Project``).
    *where* — optional per-hop attribute filters applied to the hop's
    target node before it's added to the next frontier. Reuses the
    :class:`CohortCompat` primitives (``value_equals`` / ``value_in`` /
    ``value_range``); ``same_value`` is not meaningful at hop scope and
    is ignored. Lets users say *"Person → Project → ComplianceType
    where complianceTypeId = 'Individual'"* without misusing the
    rule-level compatibility (which filters the source class only).

    The hop's *source class* is implicit: hop 0 starts at the rule's
    ``class_uri``; hop *i* starts at hop *i-1*'s ``target_class``.
    """

    via: str
    target_class: str
    where: List["CohortCompat"] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "via": self.via,
            "target_class": self.target_class,
        }
        if self.where:
            out["where"] = [w.to_dict() for w in self.where]
        return out

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CohortHop":
        where_data = data.get("where") or []
        where = [CohortCompat.from_dict(w) for w in where_data]
        return CohortHop(
            via=str(data.get("via", "")).strip(),
            target_class=str(data.get("target_class", "")).strip(),
            where=where,
        )


@dataclass
class CohortLink:
    """Together-relation: two members are linked when they share an entity
    reachable by an ordered path.

    The canonical representation is *path* — a list of :class:`CohortHop`
    starting from the rule's ``class_uri`` and ending at the "shared
    entity" the two members must agree on.

    *shared_class* and *via* are the legacy 1-hop fields, kept for
    backwards compatibility. When *path* is empty but those fields are
    set, ``hops()`` synthesises a single-hop path on the fly so the
    engine has a uniform representation.
    """

    shared_class: str = ""
    via: str = ""
    path: List["CohortHop"] = field(default_factory=list)

    def hops(self) -> List["CohortHop"]:
        """Return the canonical hop list (legacy 1-hop links auto-promoted)."""
        if self.path:
            return list(self.path)
        if self.shared_class or self.via:
            return [CohortHop(via=self.via, target_class=self.shared_class)]
        return []

    @property
    def terminal_class(self) -> str:
        """Class URI of the shared entity at the end of the path."""
        hops = self.hops()
        return hops[-1].target_class if hops else ""

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.shared_class:
            out["shared_class"] = self.shared_class
        if self.via:
            out["via"] = self.via
        if self.path:
            out["path"] = [h.to_dict() for h in self.path]
        return out

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CohortLink":
        path_data = data.get("path") or []
        path = [CohortHop.from_dict(h) for h in path_data]
        return CohortLink(
            shared_class=str(data.get("shared_class", "")).strip(),
            via=str(data.get("via", "")).strip(),
            path=path,
        )


@dataclass
class CohortCompat:
    """One compatibility constraint applied to candidate cohort members."""

    type: str
    property: str
    value: Optional[Any] = None
    values: Optional[List[Any]] = None
    min: Optional[float] = None
    max: Optional[float] = None
    allow_missing: bool = False

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": self.type, "property": self.property}
        if self.value is not None:
            out["value"] = self.value
        if self.values is not None:
            out["values"] = list(self.values)
        if self.min is not None:
            out["min"] = self.min
        if self.max is not None:
            out["max"] = self.max
        if self.allow_missing:
            out["allow_missing"] = True
        return out

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CohortCompat":
        return CohortCompat(
            type=str(data.get("type", "")).strip(),
            property=str(data.get("property", "")).strip(),
            value=data.get("value"),
            values=list(data["values"]) if data.get("values") is not None else None,
            min=float(data["min"]) if data.get("min") is not None else None,
            max=float(data["max"]) if data.get("max") is not None else None,
            allow_missing=bool(data.get("allow_missing", False)),
        )


@dataclass
class CohortUCTarget:
    """Unity Catalog Delta table target for cohort materialisation."""

    catalog: str
    schema: str
    table_name: str

    def fq_name(self) -> str:
        """Return the fully-qualified ``catalog.schema.table_name`` identifier."""
        return f"`{self.catalog}`.`{self.schema}`.`{self.table_name}`"

    def to_dict(self) -> Dict[str, str]:
        return {
            "catalog": self.catalog,
            "schema": self.schema,
            "table_name": self.table_name,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CohortUCTarget":
        return CohortUCTarget(
            catalog=str(data.get("catalog", "")).strip(),
            schema=str(data.get("schema", "")).strip(),
            table_name=str(data.get("table_name", "")).strip(),
        )


@dataclass
class CohortOutput:
    """Where cohort results should be written."""

    graph: bool = True
    uc_table: Optional[CohortUCTarget] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"graph": bool(self.graph)}
        if self.uc_table is not None:
            out["uc_table"] = self.uc_table.to_dict()
        return out

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CohortOutput":
        uc = data.get("uc_table")
        target: Optional[CohortUCTarget] = None
        if isinstance(uc, dict) and uc.get("table_name"):
            target = CohortUCTarget.from_dict(uc)
        return CohortOutput(graph=bool(data.get("graph", True)), uc_table=target)


@dataclass
class CohortRule:
    """A user-authored cohort discovery rule."""

    id: str
    label: str
    class_uri: str
    links: List[CohortLink] = field(default_factory=list)
    links_combine: str = "any"
    compatibility: List[CohortCompat] = field(default_factory=list)
    group_type: str = "connected"
    min_size: int = 2
    max_triples: int = 500_000
    output: CohortOutput = field(default_factory=CohortOutput)
    enabled: bool = True
    description: str = ""

    def validate(self) -> List[str]:
        """Return a list of human-readable validation errors (empty when OK)."""
        errors: List[str] = []
        if not self.id or not self.id.strip():
            errors.append("Rule id is required.")
        if not self.label or not self.label.strip():
            errors.append("Rule label is required.")
        if not self.class_uri or not self.class_uri.strip():
            errors.append("Target class URI is required.")
        if self.links_combine not in COHORT_LINKS_COMBINE:
            errors.append(
                f"links_combine must be one of {sorted(COHORT_LINKS_COMBINE)}."
            )
        if self.group_type not in COHORT_GROUP_TYPES:
            errors.append(
                f"group_type must be one of {sorted(COHORT_GROUP_TYPES)}."
            )
        if self.min_size < 2:
            errors.append("min_size must be at least 2.")
        if self.max_triples < 1:
            errors.append("max_triples must be a positive integer.")
        for idx, lk in enumerate(self.links, start=1):
            hops = lk.hops()
            if not hops:
                errors.append(
                    f"Link #{idx}: path is required (at least one hop)."
                )
                continue
            for h_idx, h in enumerate(hops, start=1):
                if not h.via:
                    errors.append(
                        f"Link #{idx} hop {h_idx}: via predicate is required."
                    )
                if not h.target_class:
                    errors.append(
                        f"Link #{idx} hop {h_idx}: target_class is required."
                    )
                for w_idx, w in enumerate(h.where, start=1):
                    if w.type == _COMPAT_SAME_VALUE:
                        errors.append(
                            f"Link #{idx} hop {h_idx} where #{w_idx}: "
                            f"'same_value' is not meaningful at hop scope."
                        )
                        continue
                    if w.type not in COHORT_COMPAT_TYPES:
                        errors.append(
                            f"Link #{idx} hop {h_idx} where #{w_idx}: "
                            f"unknown type '{w.type}'."
                        )
                    if not w.property:
                        errors.append(
                            f"Link #{idx} hop {h_idx} where #{w_idx}: "
                            f"property is required."
                        )
                    if w.type == _COMPAT_VALUE_EQUALS and w.value is None:
                        errors.append(
                            f"Link #{idx} hop {h_idx} where #{w_idx} "
                            f"(value_equals): value is required."
                        )
                    if w.type == _COMPAT_VALUE_IN and not w.values:
                        errors.append(
                            f"Link #{idx} hop {h_idx} where #{w_idx} "
                            f"(value_in): values list is required."
                        )
                    if w.type == _COMPAT_VALUE_RANGE and (
                        w.min is None and w.max is None
                    ):
                        errors.append(
                            f"Link #{idx} hop {h_idx} where #{w_idx} "
                            f"(value_range): min or max is required."
                        )
        for idx, cc in enumerate(self.compatibility, start=1):
            if cc.type not in COHORT_COMPAT_TYPES:
                errors.append(
                    f"Compatibility #{idx}: unknown type '{cc.type}'."
                )
            if not cc.property:
                errors.append(f"Compatibility #{idx}: property is required.")
            if cc.type == _COMPAT_VALUE_EQUALS and cc.value is None:
                errors.append(
                    f"Compatibility #{idx} (value_equals): value is required."
                )
            if cc.type == _COMPAT_VALUE_IN and not cc.values:
                errors.append(
                    f"Compatibility #{idx} (value_in): values list is required."
                )
            if cc.type == _COMPAT_VALUE_RANGE and (
                cc.min is None and cc.max is None
            ):
                errors.append(
                    f"Compatibility #{idx} (value_range): min or max is required."
                )
        return errors

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "class_uri": self.class_uri,
            "links": [lk.to_dict() for lk in self.links],
            "links_combine": self.links_combine,
            "compatibility": [cc.to_dict() for cc in self.compatibility],
            "group_type": self.group_type,
            "min_size": int(self.min_size),
            "max_triples": int(self.max_triples),
            "output": self.output.to_dict(),
            "enabled": bool(self.enabled),
            "description": self.description,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CohortRule":
        links = [CohortLink.from_dict(lk) for lk in data.get("links", []) or []]
        compats = [
            CohortCompat.from_dict(cc) for cc in data.get("compatibility", []) or []
        ]
        output = CohortOutput.from_dict(data.get("output", {}) or {})
        return CohortRule(
            id=str(data.get("id", "")).strip(),
            label=str(data.get("label", "")).strip(),
            class_uri=str(data.get("class_uri", "")).strip(),
            links=links,
            links_combine=str(data.get("links_combine", "any")).strip() or "any",
            compatibility=compats,
            group_type=str(data.get("group_type", "connected")).strip()
            or "connected",
            min_size=int(data.get("min_size", 2) or 2),
            max_triples=int(data.get("max_triples", 500_000) or 500_000),
            output=output,
            enabled=bool(data.get("enabled", True)),
            description=str(data.get("description", "") or ""),
        )


@dataclass
class CohortGroup:
    """One detected cohort: a content-hash URI and the sorted member list."""

    id: str
    idx: int
    size: int
    members: List[str] = field(default_factory=list)


@dataclass
class CohortStats:
    """Aggregate statistics from a cohort discovery run."""

    rule_id: str
    class_member_count: int = 0
    survivor_count: int = 0
    edge_count: int = 0
    cohort_count: int = 0
    grouped_member_count: int = 0
    elapsed_ms: int = 0


@dataclass
class CohortResult:
    """Full result of a cohort discovery run."""

    rule_id: str
    cohorts: List[CohortGroup] = field(default_factory=list)
    stats: CohortStats = field(
        default_factory=lambda: CohortStats(rule_id="")
    )
