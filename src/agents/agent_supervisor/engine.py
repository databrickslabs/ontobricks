"""Supervisor engine - assess complexity, then dispatch to the right engine.

This is the in-process brain the Agent Bricks Supervisor delegates to. Given a
task ("mapping" or "ontology") plus the domain's metadata and ontology, it runs
the deterministic ``ComplexityAssessor``, picks the engine, and invokes it via
``AgentClient``.

Engine selection (deterministic):

* "mapping" - the genuine two-engine choice. PGE: ``agent_mapping_pge``;
  simple: ``agent_auto_assignment`` (the original single-agent engine from
  ``master``). This is what the complexity score routes between.
* "ontology" - a single engine, ``agent_owl_generator`` (its PGE Evaluator stage
  is bounded internally; there is no separate "simple ontology engine"). The
  complexity report is still produced for observability, but dispatch is
  unconditional.

The mapping selection can be forced via ``engine_override`` for callers that
already know which engine they want (e.g. the supervisor acting on its own
routing decision).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Tuple

from agents.agent_supervisor.complexity import ComplexityReport, assess
from agents.tracing import trace_agent
from back.core.agents.AgentClient import get_agent_client
from back.core.logging import get_logger

logger = get_logger(__name__)

_VALID_TASKS = ("mapping", "ontology")
_VALID_ENGINES = ("pge", "simple")


@dataclass
class SupervisorResult:
    """Outcome of a supervised run: the routing decision + the engine result."""

    task: str
    engine_used: str
    complexity: ComplexityReport
    result: Any = None
    success: bool = False
    error: str = ""
    extras: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "task": self.task,
            "engine_used": self.engine_used,
            "success": self.success,
            "error": self.error,
            "complexity": self.complexity.to_dict() if self.complexity else None,
            "extras": self.extras,
        }


class SupervisorEngine:
    """Routes a domain task to the PGE or simple engine by complexity."""

    @staticmethod
    def decide_engine(
        metadata: dict,
        ontology: dict,
        engine_override: Optional[str] = None,
    ) -> Tuple[str, ComplexityReport]:
        """Return ``(engine, report)``.

        ``engine_override`` short-circuits the recommendation but the report is
        still computed for observability.
        """
        report = assess(metadata, ontology)
        if engine_override in _VALID_ENGINES:
            return engine_override, report
        return report.recommended_engine, report

    @staticmethod
    @trace_agent("supervisor:run")
    def run(
        *,
        task: str,
        host: str,
        token: str,
        endpoint_name: str,
        metadata: dict,
        ontology: dict,
        engine_override: Optional[str] = None,
        client: Any = None,
        entity_mappings: Any = None,
        relationship_mappings: Any = None,
        base_uri: str = "",
        selected_tables: Optional[list] = None,
        documents: Any = None,
        on_step: Optional[Callable] = None,
    ) -> SupervisorResult:
        """Assess complexity, choose an engine, and run it.

        ``task`` is ``"mapping"`` or ``"ontology"``. Engine-specific arguments are
        forwarded to the chosen engine via :class:`AgentClient`.
        """
        if task not in _VALID_TASKS:
            raise ValueError(f"task must be one of {_VALID_TASKS}, got {task!r}")

        engine, report = SupervisorEngine.decide_engine(
            metadata, ontology, engine_override
        )
        agent = get_agent_client()

        if task == "mapping":
            engine_used = engine
            logger.info("Supervisor routing - task=mapping engine=%s", engine)
        else:
            # Ontology generation has a single engine; report.recommended_engine
            # is advisory only.
            engine_used = "owl_generator"
            logger.info(
                "Supervisor routing - task=ontology engine=owl_generator "
                "(complexity tier=%s, advisory)",
                report.tier,
            )

        try:
            if task == "mapping":
                run_engine = (
                    agent.run_mapping_pge
                    if engine == "pge"
                    else agent.run_auto_assignment
                )
                result = run_engine(
                    host=host,
                    token=token,
                    endpoint_name=endpoint_name,
                    client=client,
                    metadata=metadata,
                    ontology=ontology,
                    entity_mappings=entity_mappings or [],
                    relationship_mappings=relationship_mappings or [],
                    documents=documents,
                    on_step=on_step,
                )
            else:
                result = agent.run_owl_generator(
                    host=host,
                    token=token,
                    endpoint_name=endpoint_name,
                    base_uri=base_uri,
                    selected_tables=selected_tables or [],
                    metadata=metadata,
                    ontology=ontology,
                    on_step=on_step,
                )
        except Exception as exc:  # surfaced to the caller; never swallowed silently
            logger.error(
                "Supervisor run failed (task=%s engine=%s): %s", task, engine_used, exc
            )
            return SupervisorResult(
                task=task,
                engine_used=engine_used,
                complexity=report,
                success=False,
                error=str(exc),
            )

        return SupervisorResult(
            task=task,
            engine_used=engine_used,
            complexity=report,
            result=result,
            success=bool(getattr(result, "success", True)),
        )
