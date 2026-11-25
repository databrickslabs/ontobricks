"""Build a candidate design by applying Evolve ops to a fresh copy of the parent."""

from __future__ import annotations

import copy
from typing import Any, Dict, List

from back.core.errors import ValidationError
from back.objects.ontology.autofix_apply import apply_operations
from back.objects.ontology.autofix_ops import EVOLVE_OPS
from back.objects.ontology.evolution.content_hash import design_hash
from back.objects.registry.schedule.TaskContext import _FakeSessionMgr


class CandidateFactory:
    """Apply ops to an isolated, session-free copy of the parent version.

    The parent document is deep-copied and loaded into its own
    ``DomainSession`` backed by an in-memory session manager, so nothing
    the user has open — and nothing in the registry — is touched.
    """

    @staticmethod
    def session_for(document: Dict[str, Any], version: str):
        from back.objects.session.DomainSession import DomainSession

        domain = DomainSession(_FakeSessionMgr())
        domain.import_from_file(copy.deepcopy(document), version=str(version))
        return domain

    @classmethod
    def build(
        cls,
        parent_document: Dict[str, Any],
        parent_version: str,
        operations: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        domain = cls.session_for(parent_document, parent_version)
        result = apply_operations(domain, copy.deepcopy(operations), allowed=EVOLVE_OPS)
        if not result.get("success"):
            raise ValidationError(
                "The proposed change cannot be applied", detail=result.get("error")
            )
        domain.clear_generated_content()
        ontology = copy.deepcopy(domain._data.get("ontology") or {})
        assignment = copy.deepcopy(domain._data.get("assignment") or {})
        assignment.pop("r2rml_output", None)
        return {
            "ontology": ontology,
            "assignment": assignment,
            "design_hash": design_hash(ontology, assignment),
            "operations": copy.deepcopy(operations),
        }
