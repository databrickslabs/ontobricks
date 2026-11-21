"""Accept an evaluated Evolve candidate as the next DRAFT registry version."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any, Dict, List

from back.core.errors import ConflictError, ValidationError
from back.core.logging import get_logger
from back.objects.ontology.evolution.Decision import Decision
from back.objects.ontology.evolution.EvaluationService import source_tables
from back.objects.ontology.evolution.EvolutionPipeline import parent_slice
from back.objects.ontology.evolution.EvolutionService import EvolutionService
from back.objects.ontology.evolution.SourceProbe import SourceProbe
from back.objects.ontology.evolution.content_hash import design_hash

logger = get_logger(__name__)

# Session/build state that must not travel from the parent to a new version.
_DROP_INFO = ("status", "last_build", "evolution")


class VersionAllocator:
    """Turns a PASS run into one new DRAFT version, idempotently.

    Preconditions, checked here and again under the store's row locks:
    the decision is PASS, the stored candidate still hashes to what was
    evaluated, the parent is unchanged and the caller can still read
    every source. The version is a normal DRAFT afterwards; its ``info``
    carries ``evolution`` so review can show (and guard) the evidence.
    """

    def __init__(self, service: EvolutionService, registry: Any, probe: SourceProbe):
        self._svc = service
        self._registry = registry
        self._probe = probe

    def accept(self, run_id: str, revision: int) -> Dict[str, Any]:
        run = self._svc.get_run(run_id)
        if run.get("accepted_version"):
            return {"version": run["accepted_version"], "created": False, "run": run}
        evaluation = run.get("evaluation") or {}
        if evaluation.get("decision") != Decision.PASS.value:
            raise ValidationError(
                "Only a run whose evaluation passed can be accepted",
                detail=str(evaluation.get("decision") or "not evaluated"),
            )
        candidate = run.get("candidate") or {}
        current = design_hash(
            candidate.get("ontology") or {}, candidate.get("assignment") or {}
        )
        if not (
            current
            == candidate.get("design_hash")
            == evaluation.get("candidate_design_hash")
        ):
            raise ConflictError(
                "The candidate changed after it was evaluated — evaluate again"
            )
        self._svc.ensure_parent_unchanged(run)
        for table in source_tables(candidate["ontology"], candidate["assignment"]):
            self._probe.columns(table)

        document = self._svc.read_parent(run["parent_version"])
        row = self.version_row(document, run)
        result = self._svc.accept_version(run_id, revision, row)
        result["documents"] = (
            self._copy_documents(run, result) if result["created"] else []
        )
        return result

    @staticmethod
    def version_row(document: Dict[str, Any], run: Dict[str, Any]) -> Dict[str, Any]:
        sl = parent_slice(document, run["parent_version"])
        info = {
            k: copy.deepcopy(v)
            for k, v in (document.get("info") or {}).items()
            if k not in _DROP_INFO
        }
        evaluation = run["evaluation"]
        info["last_update"] = datetime.now(timezone.utc).isoformat()
        info["evolution"] = {
            "run_id": run["id"],
            "parent_version": run["parent_version"],
            "decision": evaluation["decision"],
            "evaluation_hash": evaluation["evaluation_hash"],
            "design_hash": run["candidate"]["design_hash"],
        }
        return {
            "info": info,
            "ontology": copy.deepcopy(run["candidate"]["ontology"]),
            "assignment": copy.deepcopy(run["candidate"]["assignment"]),
            "design_layout": copy.deepcopy(sl.get("design_layout") or {}),
            "metadata": copy.deepcopy(sl.get("metadata") or {}),
        }

    def _copy_documents(self, run: Dict[str, Any], result: Dict[str, Any]) -> List[str]:
        """Carry the Knowledge Store corpus forward; failures are reported, not raised."""
        try:
            _count, errors = self._registry.copy_version_documents(
                self._svc.folder, run["parent_version"], result["version"]
            )
            return errors
        except Exception as exc:  # noqa: BLE001 — the version already exists
            logger.warning("Evolve accept could not copy documents: %s", exc)
            return [str(exc)]
