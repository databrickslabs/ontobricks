"""Data models for the reasoning engine."""
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class InferredTriple:
    """A triple produced by a reasoning phase."""

    subject: str
    predicate: str
    object: str
    provenance: str  # e.g. "owlrl", "swrl:RuleName", "graph:transitive"
    rule_name: str = ""


@dataclass
class RuleViolation:
    """A constraint or rule violation found during reasoning."""

    rule_name: str
    subject: str
    message: str
    check_type: str  # "swrl", "cardinality", "value", etc.
    rule_type: str = ""  # "swrl", "decision_table", "sparql", "aggregate"


@dataclass
class ReasoningResult:
    """Aggregated output of one or more reasoning phases."""

    inferred_triples: List[InferredTriple] = field(default_factory=list)
    violations: List[RuleViolation] = field(default_factory=list)
    stats: Dict = field(default_factory=dict)

    def merge(self, other: "ReasoningResult") -> None:
        """Merge another result into this one."""
        self.inferred_triples.extend(other.inferred_triples)
        self.violations.extend(other.violations)
        for key, val in other.stats.items():
            if isinstance(val, (int, float)) and isinstance(self.stats.get(key), (int, float)):
                self.stats[key] = self.stats[key] + val
            else:
                self.stats[key] = val

    def deduplicate(self) -> int:
        """Remove duplicate inferred triples (same subject, predicate, object).

        When the same triple is inferred by multiple phases, keeps the first
        occurrence (preserving provenance of the earliest phase).

        Returns the number of duplicates removed.
        """
        seen: set = set()
        unique: List[InferredTriple] = []
        for t in self.inferred_triples:
            key = (t.subject, t.predicate, t.object)
            if key not in seen:
                seen.add(key)
                unique.append(t)
        removed = len(self.inferred_triples) - len(unique)
        self.inferred_triples = unique
        return removed

    def to_dict(self) -> Dict:
        """Serialise for JSON transport / session storage."""
        return {
            "inferred_triples": [
                {
                    "subject": t.subject,
                    "predicate": t.predicate,
                    "object": t.object,
                    "provenance": t.provenance,
                    "rule_name": t.rule_name,
                }
                for t in self.inferred_triples
            ],
            "violations": [
                {
                    "rule_name": v.rule_name,
                    "subject": v.subject,
                    "message": v.message,
                    "check_type": v.check_type,
                    "rule_type": v.rule_type,
                }
                for v in self.violations
            ],
            "stats": dict(self.stats),
        }


@dataclass
class SWRLAtomPartition:
    """Partitioned antecedent/consequent atoms for SWRL rule translation."""

    class_atoms: List[Dict[str, Any]]
    prop_atoms: List[Dict[str, Any]]
    builtin_atoms: List[Dict[str, Any]]
    negated_atoms: List[Dict[str, Any]]
    consequent_atoms: List[Dict[str, Any]]
