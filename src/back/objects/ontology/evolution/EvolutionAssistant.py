"""LLM help for an Evolve run: suggest the intent, suggest repair alternatives."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from back.core.errors import ValidationError
from back.objects.ontology.evolution.ContextPages import ContextPages
from back.objects.ontology.evolution.EvolutionPipeline import (
    EvolutionPipeline,
    parent_slice,
)
from back.objects.ontology.evolution.EvolutionService import EvolutionService
from back.objects.ontology.evolution.OntologyIndex import OntologyIndex


class EvolutionAssistant:
    """Runs the agent with the parent version and its curated Genie Pages.

    *llm* is ``(host, token, endpoint, endpoint_kind)``; *fetch_page* reads
    one Knowledge Page as the caller. The agent's output is a suggestion:
    the intent goes back to the user to edit before coverage, and proposal
    alternatives go through :meth:`EvolutionPipeline.propose` like any other.
    """

    def __init__(
        self,
        service: EvolutionService,
        llm: tuple,
        fetch_page: Callable[[str], Dict[str, Any]],
        call_llm: Optional[Callable[..., dict]] = None,
    ):
        self._svc = service
        self._llm = llm
        self._fetch_page = fetch_page
        self._call_llm = call_llm

    def _llm_kwargs(self) -> Dict[str, Any]:
        host, token, endpoint, kind = self._llm
        return {
            "host": host,
            "token": token,
            "endpoint": endpoint,
            "endpoint_kind": kind,
            "call_llm": self._call_llm,
        }

    def _context(self, parent_version: str):
        document = self._svc.read_parent(parent_version)
        sl = parent_slice(document, parent_version)
        index = OntologyIndex(sl.get("ontology") or {}, sl.get("assignment") or {})
        return index, ContextPages.load(document, self._fetch_page)

    def _meta(self, result, pages: ContextPages) -> Dict[str, Any]:
        return {
            "model": self._llm[2],
            "usage": dict(result.usage),
            "context_pages": pages.manifest(),
            "dropped_citations": result.output.get("dropped_citations", 0),
        }

    def suggest_intent(self, parent_version: str, question: str) -> Dict[str, Any]:
        from agents.agent_ontology_evolution import suggest_intent

        if not str(question or "").strip():
            raise ValidationError("Type the question first")
        index, pages = self._context(parent_version)
        result = suggest_intent(question, index, pages, **self._llm_kwargs())
        if not result.success:
            raise ValidationError(
                "The assistant could not suggest an intent", detail=result.error
            )
        out = result.output
        return {
            "intent": out["intent"],
            "ambiguities": out["ambiguities"],
            "citations": out["citations"],
            "notes": out["notes"],
            "agent": self._meta(result, pages),
        }

    def propose(
        self, pipeline: EvolutionPipeline, run_id: str, revision: int
    ) -> Dict[str, Any]:
        from agents.agent_ontology_evolution import suggest_proposal

        run = self._svc.get_run(run_id)
        coverage = run.get("coverage") or {}
        if coverage.get("decision_hint") != "PROPOSE":
            raise ValidationError(
                "Nothing to propose for this question",
                detail=str(
                    coverage.get("decision_hint") or "run the coverage check first"
                ),
            )
        self._svc.ensure_parent_unchanged(run)
        index, pages = self._context(run["parent_version"])
        result = suggest_proposal(
            (run.get("manifest") or {}).get("question") or "",
            run.get("intent") or {},
            coverage,
            index,
            pages,
            **self._llm_kwargs(),
        )
        if not result.success:
            raise ValidationError(
                "The assistant could not suggest a repair", detail=result.error
            )
        meta = {
            **self._meta(result, pages),
            "summary": result.output.get("summary", ""),
        }
        return pipeline.propose(
            run_id,
            revision,
            suggestion={"alternatives": result.output["alternatives"], "meta": meta},
        )
