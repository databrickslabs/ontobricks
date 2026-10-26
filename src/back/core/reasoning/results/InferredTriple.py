"""A triple produced by a reasoning phase."""

from dataclasses import dataclass


@dataclass
class InferredTriple:
    """A triple produced by a reasoning phase."""

    subject: str
    predicate: str
    object: str
    provenance: str  # e.g. "owlrl", "swrl:RuleName", "graph:transitive"
    rule_name: str = ""
