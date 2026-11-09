"""Select SHACL / IF-THEN checks for a data-quality run.

Extracted from ``api.routers.internal.dtwin.start_dataquality_checks``
(Fowler Extract Class). The route still owns TaskManager + the worker thread.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from back.core.errors import ValidationError
from back.core.w3c.shacl.constants import (
    RULE_FAMILY_CATEGORIES,
    IF_THEN_ID_PREFIX,
    rule_check_id,
)


@dataclass
class TwinDataQualityRun:
    """The checks a user selected for one async data-quality task."""

    shapes: list
    if_then_rules: list
    ontology_dict: dict
    violation_limit: Optional[int]

    @property
    def total(self) -> int:
        return len(self.shapes) + len(self.if_then_rules)

    @staticmethod
    def read_ontology(domain) -> dict:
        ontology_dict = getattr(domain, "ontology", None)
        if isinstance(ontology_dict, dict):
            return ontology_dict
        if hasattr(domain, "_data"):
            return domain._data.get("ontology", {}) or {}
        return {}

    @staticmethod
    def _selected_rules(
        prefix: str,
        family: list,
        *,
        selected_ids: set,
        dimensions: list,
    ) -> list:
        if (
            not selected_ids
            and dimensions
            and RULE_FAMILY_CATEGORIES[prefix] not in dimensions
        ):
            return []
        selected = []
        for index, rule in enumerate(family or []):
            if not rule.get("enabled", True):
                continue
            check_id = rule_check_id(prefix, rule, index)
            if selected_ids and check_id not in selected_ids:
                continue
            selected.append({**rule, "check_id": check_id})
        return selected

    @classmethod
    def from_request(cls, domain, data: dict) -> TwinDataQualityRun:
        """Filter shapes and rule families from the POST /dataquality/start body."""
        dimensions = data.get("dimensions") or []
        shape_ids = data.get("shape_ids") or []
        violation_limit: Any = int(data.get("violation_limit", 10))
        if violation_limit <= 0:
            violation_limit = None

        shapes = domain.shacl_shapes
        if shape_ids:
            shape_ids_set = set(shape_ids)
            shapes = [s for s in shapes if s.get("id") in shape_ids_set]
        elif dimensions:
            shapes = [s for s in shapes if s.get("category") in dimensions]
        shapes = [s for s in shapes if s.get("enabled", True)]

        ontology_dict = cls.read_ontology(domain)
        selected_ids = set(shape_ids)
        if_then_rules = cls._selected_rules(
            IF_THEN_ID_PREFIX,
            domain.if_then_rules,
            selected_ids=selected_ids,
            dimensions=dimensions,
        )
        if not shapes and not if_then_rules:
            raise ValidationError(
                "Nothing to check in the selected dimensions."
                if dimensions or shape_ids
                else "No enabled shapes or IF-THEN rules to check."
            )

        return cls(
            shapes=shapes,
            if_then_rules=if_then_rules,
            ontology_dict=ontology_dict,
            violation_limit=violation_limit,
        )
