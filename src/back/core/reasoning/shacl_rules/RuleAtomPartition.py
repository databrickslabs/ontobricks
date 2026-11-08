"""Antecedent/consequent atom partition used by SHACL-AF rule translation."""

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class RuleAtomPartition:
    """Partitioned antecedent/consequent atoms for SHACL-AF rule translation."""

    class_atoms: List[Dict[str, Any]]
    prop_atoms: List[Dict[str, Any]]
    builtin_atoms: List[Dict[str, Any]]
    negated_atoms: List[Dict[str, Any]]
    consequent_atoms: List[Dict[str, Any]]
