"""Outcome of applying a rules package (see RulesPackage.apply_package)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


@dataclass
class ImportResult:
    """Merged rule lists plus what was added, replaced and rejected."""

    if_then_rules: List[Dict[str, Any]]
    sparql_rules: List[Dict[str, Any]]
    added: List[str] = field(default_factory=list)
    replaced: List[str] = field(default_factory=list)
    rejected: List[Dict[str, str]] = field(default_factory=list)
    # (kind, action, name): kind is "if_then_rules" or "sparql_rules";
    # action is "added" or "replaced".
    changes: List[Tuple[str, str, str]] = field(default_factory=list)
