"""Coverage and proposal stages of one Evolve run."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.errors import ConflictError, ValidationError
from back.objects.domain.payload import version_document_slice
from back.objects.ontology.evolution.CandidateFactory import CandidateFactory
from back.objects.ontology.evolution.CoverageService import CoverageService
from back.objects.ontology.evolution.EvolutionService import EvolutionService
from back.objects.ontology.evolution.Intent import Intent
from back.objects.ontology.evolution.OntologyIndex import OntologyIndex
from back.objects.ontology.evolution.ProposalBuilder import ProposalBuilder
from back.objects.ontology.evolution.ProposalValidator import ProposalValidator
from back.objects.ontology.evolution.RunStatus import RunStatus
from back.objects.ontology.evolution.SourceProbe import SourceProbe


def parent_slice(document: Dict[str, Any], version: str) -> Dict[str, Any]:
    versions = document.get("versions") or {}
    return version_document_slice(versions, str(version)) if versions else document


class EvolutionPipeline:
    """Runs coverage, then a proposal with its candidate and validation.

    Each stage re-reads the parent, refuses to continue when the parent
    changed since the run started, and writes its result with the run's
    revision so concurrent edits surface as conflicts.
    """

    def __init__(self, service: EvolutionService, probe: SourceProbe):
        self._svc = service
        self._probe = probe

    def _parent(self, run: Dict[str, Any]):
        self._svc.ensure_parent_unchanged(run)
        document = self._svc.read_parent(run["parent_version"])
        sl = parent_slice(document, run["parent_version"])
        return document, OntologyIndex(
            sl.get("ontology") or {}, sl.get("assignment") or {}
        )

    def assess(self, run_id: str, revision: int) -> Dict[str, Any]:
        run = self._svc.get_run(run_id)
        if run["cancel_requested"]:
            raise ConflictError("The run was cancelled")
        _doc, index = self._parent(run)
        coverage = CoverageService(index, self._probe).assess(
            Intent.from_dict(run["intent"])
        )
        coverage["choice_needed"] = ProposalBuilder.needs_choice(coverage)
        return self._svc.update(
            run_id,
            revision,
            {
                "coverage": coverage,
                "proposal": {},
                "candidate": {},
                "evaluation": {},
                "status": RunStatus.ASSESSED.value,
                "stage": "coverage",
            },
        )

    def propose(
        self,
        run_id: str,
        revision: int,
        operations: Optional[List[Dict[str, Any]]] = None,
        suggestion: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build the proposal and validate it.

        Rule-based by default; with *operations*, a builder's edit; with
        *suggestion*, the agent's alternatives mapped onto coverage's join
        candidates (see :meth:`ProposalBuilder.from_suggestion`).
        """
        run = self._svc.get_run(run_id)
        coverage = run.get("coverage") or {}
        if not coverage:
            raise ValidationError("Run the coverage check first")
        document, index = self._parent(run)
        if operations is not None:
            proposal = {
                "schema_version": ProposalBuilder.SCHEMA_VERSION,
                "source": "manual",
                "selected": 0,
                "alternatives": [
                    {
                        "operations": operations,
                        "rationale": "Edited by a builder.",
                        "evidence_ids": [],
                    }
                ],
            }
        elif suggestion is not None:
            proposal = ProposalBuilder(index).from_suggestion(coverage, suggestion)
            if proposal is None:
                raise ValidationError(
                    "Nothing to propose for this question",
                    detail=str(coverage.get("decision_hint")),
                )
            if not proposal["alternatives"]:
                raise ValidationError(
                    "None of the agent's alternatives could be used",
                    detail=", ".join(r["reason"] for r in proposal["rejected"])
                    or "no alternatives",
                )
            proposal["agent"] = dict(suggestion.get("meta") or {})
        else:
            proposal = ProposalBuilder(index).build(coverage)
            if proposal is None:
                raise ValidationError(
                    "Nothing to propose for this question",
                    detail=str(coverage.get("decision_hint")),
                )
        return self._validate_and_store(run, revision, document, coverage, proposal)

    def select(self, run_id: str, revision: int, selected: int) -> Dict[str, Any]:
        """Switch to another alternative of the current proposal and re-validate it."""
        run = self._svc.get_run(run_id)
        proposal = dict(run.get("proposal") or {})
        alternatives = proposal.get("alternatives") or []
        if not 0 <= int(selected) < len(alternatives):
            raise ValidationError("No such alternative")
        document, _index = self._parent(run)
        proposal["selected"] = int(selected)
        # Picking a join is the builder's decision the evaluation waits for.
        proposal["choice_confirmed"] = bool(proposal.get("needs_choice"))
        return self._validate_and_store(
            run, revision, document, run.get("coverage") or {}, proposal
        )

    def _join_for(self, coverage, proposal) -> Optional[Dict[str, Any]]:
        join = coverage.get("join") or {}
        if proposal["source"] == "rule":
            return join.get("chosen")
        if proposal["source"] == "agent":
            alt = proposal["alternatives"][proposal["selected"]]
            return (join.get("candidates") or [])[alt["candidate"]]
        return None

    def _validate_and_store(self, run, revision, document, coverage, proposal):
        ops = proposal["alternatives"][proposal["selected"]]["operations"]
        candidate, apply_error = None, ""
        try:
            candidate = CandidateFactory.build(document, run["parent_version"], ops)
        except ValidationError as exc:
            apply_error = str(exc.detail or exc.message)
        proposal["validation"] = ProposalValidator(self._probe).validate(
            ops, candidate, apply_error, self._join_for(coverage, proposal)
        )
        return self._svc.update(
            run["id"],
            revision,
            {
                "proposal": proposal,
                "candidate": candidate or {},
                "evaluation": {},
                "status": RunStatus.PROPOSED.value,
                "stage": "proposal",
            },
        )
