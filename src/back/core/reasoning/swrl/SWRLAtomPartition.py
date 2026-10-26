"""Antecedent/consequent atom partition used by SWRL rule translation."""

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class SWRLAtomPartition:
    """Partitioned antecedent/consequent atoms for SWRL rule translation."""

    class_atoms: List[Dict[str, Any]]
    prop_atoms: List[Dict[str, Any]]
    builtin_atoms: List[Dict[str, Any]]
    negated_atoms: List[Dict[str, Any]]
    consequent_atoms: List[Dict[str, Any]]
