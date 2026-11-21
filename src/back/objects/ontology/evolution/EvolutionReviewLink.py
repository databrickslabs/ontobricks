"""Tie a version created by Ontology Evolve to the evaluation behind it."""

from __future__ import annotations

from typing import Any, Dict

from back.core.errors import ValidationError
from back.objects.ontology.evolution.content_hash import version_document_hash


class EvolutionReviewLink:
    """Review metadata for an Evolve version, and the stale-evidence gate.

    A version accepted from an Evolve run carries ``info.evolution``. When
    it is submitted for review the evaluation summary is attached to the
    review event; if its ontology or mapping changed after the evaluation
    the submission is refused, because the evidence no longer describes it.
    """

    @staticmethod
    def review_meta(document: Dict[str, Any], version: str) -> Dict[str, Any]:
        """``{}`` for ordinary versions, else the evaluation summary.

        Raises :class:`ValidationError` when the version changed since its
        evaluation.
        """
        link = (document.get("info") or {}).get("evolution")
        if not isinstance(link, dict) or not link.get("design_hash"):
            return {}
        if version_document_hash(document, str(version)) != link["design_hash"]:
            raise ValidationError(
                "This version changed after its Ontology Evolve evaluation. "
                "Evaluate the change again before submitting it for review.",
                detail=str(link.get("run_id") or ""),
            )
        return {
            "evolution": {
                "run_id": link.get("run_id", ""),
                "parent_version": link.get("parent_version", ""),
                "decision": link.get("decision", ""),
                "evaluation_hash": link.get("evaluation_hash", ""),
            }
        }
