"""A constraint or rule violation found during reasoning."""

from dataclasses import dataclass


@dataclass
class RuleViolation:
    """A constraint or rule violation found during reasoning."""

    rule_name: str
    subject: str
    message: str
    check_type: str  # "if_then", "cardinality", "value", etc.
    rule_type: str = ""  # "if_then", "sparql"
