"""Aggregated output of a reasoning phase."""

from dataclasses import dataclass, field
from typing import Dict, List

from back.core.reasoning.results.InferredTriple import InferredTriple
from back.core.reasoning.results.RuleViolation import RuleViolation


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
            if isinstance(val, (int, float)) and isinstance(
                self.stats.get(key), (int, float)
            ):
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
