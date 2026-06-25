"""Typed contracts for the mapping PGE pipeline.

These dataclasses are the load-bearing interface between Planner, Generator,
Evaluator, and the orchestrator (added in later sprints).  All shapes here
are JSON round-trippable via ``to_dict()`` / ``from_dict()`` so they can be
persisted as artefacts, attached to MLflow traces, or shipped over the wire
to the UI.

No LLM code lives here; this is a pure-data module.
"""

from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, Dict, List, Optional


# =====================================================
# SourceModel — Planner output
# =====================================================


@dataclass
class TableRoleCandidate:
    """A candidate ontology class for a given source table."""

    uri: str
    confidence: float  # 0.0 .. 1.0
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {"uri": self.uri, "confidence": self.confidence, "reason": self.reason}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableRoleCandidate":
        return cls(
            uri=data["uri"],
            confidence=float(data["confidence"]),
            reason=data.get("reason", ""),
        )


@dataclass
class TableRole:
    """A source table together with its ranked ontology-class candidates."""

    table: str  # full name catalog.schema.table
    ontology_class_candidates: List[TableRoleCandidate] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "ontology_class_candidates": [
                c.to_dict() for c in self.ontology_class_candidates
            ],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableRole":
        return cls(
            table=data["table"],
            ontology_class_candidates=[
                TableRoleCandidate.from_dict(c)
                for c in data.get("ontology_class_candidates", [])
            ],
        )


@dataclass
class CanonicalId:
    """Identifier conventions for an ontology class across its source tables.

    ``canonical_column_per_table`` maps a full table name -> the column to
    use as the canonical identifier in that table (e.g. NHS number rather
    than the trust-local patient id).
    """

    ontology_class: str  # class URI
    canonical_column_per_table: Dict[str, str] = field(default_factory=dict)
    format_note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ontology_class": self.ontology_class,
            "canonical_column_per_table": dict(self.canonical_column_per_table),
            "format_note": self.format_note,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CanonicalId":
        return cls(
            ontology_class=data["ontology_class"],
            canonical_column_per_table=dict(
                data.get("canonical_column_per_table", {})
            ),
            format_note=data.get("format_note", ""),
        )


@dataclass
class JoinKey:
    """A proposed join between two table.column references.

    ``kind`` distinguishes within-trust foreign keys from value-matched
    cross-source joins (e.g. NHS-number-to-NHS-number across trusts).
    """

    from_ref: str  # "table.col"
    to_ref: str  # "table.col"
    confidence: float  # 0..1
    overlap_pct: float  # 0..1
    kind: str  # "same_trust_fk" | "cross_source_value_match"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from_ref": self.from_ref,
            "to_ref": self.to_ref,
            "confidence": self.confidence,
            "overlap_pct": self.overlap_pct,
            "kind": self.kind,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JoinKey":
        return cls(
            from_ref=data["from_ref"],
            to_ref=data["to_ref"],
            confidence=float(data["confidence"]),
            overlap_pct=float(data["overlap_pct"]),
            kind=data["kind"],
        )


@dataclass
class SkipItem:
    """An ontology entity/relationship the planner has decided to skip."""

    item: str  # uri
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {"item": self.item, "reason": self.reason}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SkipItem":
        return cls(item=data["item"], reason=data.get("reason", ""))


@dataclass
class MappingPlan:
    """The order in which the Generator should attempt entity/relationship
    mappings, plus any items the planner chose to drop."""

    entity_order: List[str] = field(default_factory=list)
    relationship_order: List[str] = field(default_factory=list)
    skip: List[SkipItem] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_order": list(self.entity_order),
            "relationship_order": list(self.relationship_order),
            "skip": [s.to_dict() for s in self.skip],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MappingPlan":
        return cls(
            entity_order=list(data.get("entity_order", [])),
            relationship_order=list(data.get("relationship_order", [])),
            skip=[SkipItem.from_dict(s) for s in data.get("skip", [])],
        )


@dataclass
class SourceModel:
    """Output of the Planner stage; input to the Generator.

    Contains the planner's understanding of the source schema (table roles,
    canonical ids, join keys) and the ordered plan of work for the
    Generator.
    """

    table_roles: List[TableRole] = field(default_factory=list)
    canonical_ids: List[CanonicalId] = field(default_factory=list)
    join_keys: List[JoinKey] = field(default_factory=list)
    mapping_plan: MappingPlan = field(default_factory=MappingPlan)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_roles": [t.to_dict() for t in self.table_roles],
            "canonical_ids": [c.to_dict() for c in self.canonical_ids],
            "join_keys": [j.to_dict() for j in self.join_keys],
            "mapping_plan": self.mapping_plan.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceModel":
        return cls(
            table_roles=[
                TableRole.from_dict(t) for t in data.get("table_roles", [])
            ],
            canonical_ids=[
                CanonicalId.from_dict(c) for c in data.get("canonical_ids", [])
            ],
            join_keys=[JoinKey.from_dict(j) for j in data.get("join_keys", [])],
            mapping_plan=MappingPlan.from_dict(data.get("mapping_plan", {})),
        )


# =====================================================
# EvalReport — Evaluator output
# =====================================================


@dataclass
class EvalFailure:
    """A single failed check inside an :class:`EvalReport`.

    ``hint`` is the actionable correction text fed back to the Generator on
    retry; it should be concrete and template-y, not a free-form essay.
    """

    kind: str  # "structural" | "semantic"
    check: str  # e.g. "dangling_source_pct"
    expected: str  # e.g. "< 0.05"
    observed: str  # e.g. "0.47"
    hint: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "check": self.check,
            "expected": self.expected,
            "observed": self.observed,
            "hint": self.hint,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EvalFailure":
        return cls(
            kind=data["kind"],
            check=data["check"],
            expected=data["expected"],
            observed=data["observed"],
            hint=data.get("hint", ""),
        )


@dataclass
class EvalReport:
    """Outcome of evaluating a single submitted mapping.

    ``bubble_to_planner`` signals that the failure cannot reasonably be
    fixed by the Generator alone and warrants re-planning (e.g. wrong
    canonical id column, table assigned to wrong ontology class).
    """

    status: str  # "PASS" | "FAIL"
    stage: str  # "deterministic" | "semantic"
    metrics: Dict[str, Any] = field(default_factory=dict)
    failures: List[EvalFailure] = field(default_factory=list)
    bubble_to_planner: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "stage": self.stage,
            "metrics": dict(self.metrics),
            "failures": [f.to_dict() for f in self.failures],
            "bubble_to_planner": self.bubble_to_planner,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EvalReport":
        return cls(
            status=data["status"],
            stage=data["stage"],
            metrics=dict(data.get("metrics", {})),
            failures=[EvalFailure.from_dict(f) for f in data.get("failures", [])],
            bubble_to_planner=bool(data.get("bubble_to_planner", False)),
        )


# =====================================================
# RetryState — orchestrator bookkeeping (used in Sprint 7)
# =====================================================


@dataclass
class RetryState:
    """Per-item retry budget tracked by the orchestrator.

    The orchestrator caps the Generator at 3 attempts per item before
    giving up, and bumps the Planner at most twice per item if the
    evaluator keeps bubbling failures upstream.
    """

    item_uri: str
    generator_attempts: int = 0
    planner_reinvocations: int = 0
    last_eval_report: Optional[EvalReport] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_uri": self.item_uri,
            "generator_attempts": self.generator_attempts,
            "planner_reinvocations": self.planner_reinvocations,
            "last_eval_report": (
                self.last_eval_report.to_dict()
                if self.last_eval_report is not None
                else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetryState":
        last = data.get("last_eval_report")
        return cls(
            item_uri=data["item_uri"],
            generator_attempts=int(data.get("generator_attempts", 0)),
            planner_reinvocations=int(data.get("planner_reinvocations", 0)),
            last_eval_report=EvalReport.from_dict(last) if last is not None else None,
        )


# =====================================================
# Sanity check — keep dataclass discovery introspectable
# =====================================================

_ALL_CONTRACTS = (
    TableRoleCandidate,
    TableRole,
    CanonicalId,
    JoinKey,
    SkipItem,
    MappingPlan,
    SourceModel,
    EvalFailure,
    EvalReport,
    RetryState,
)
for _cls in _ALL_CONTRACTS:
    assert is_dataclass(_cls), f"{_cls.__name__} must be a dataclass"
    # touch ``fields`` to ensure all defaults are well-formed at import time.
    fields(_cls)
del _cls
