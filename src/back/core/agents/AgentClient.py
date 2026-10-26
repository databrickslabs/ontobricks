"""Abstraction over agent invocation.

Currently runs agents in-process via the ``agents`` package.
When agents become a separate service, route calls through an HTTP
client without changing callers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from agents.agent_auto_assignment.engine import AgentResult as AutoAssignAgentResult
    from agents.agent_mapping_pge.engine import AgentResult as MappingPGEAgentResult

from back.core.logging import get_logger

logger = get_logger(__name__)


class AgentClient:
    """Unified gateway to all LLM-agent capabilities.

    Usage::

        client = AgentClient()
        result = client.run_mapping_pge(host=..., token=..., ...)

    Note: the one-shot ``run_owl_generator`` bridge to
    ``agents.agent_owl_generator.engine.run_agent`` (the deprecated,
    pitfall-rewrite-loop OWL generator) has been removed — see the
    three-stage Generate design
    (``docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md``).
    Ontology generation now goes through the staged workflow
    (``back.objects.ontology.generate.GenerateWorkflow`` +
    ``agents.agent_owl_generator.staged``) instead of this gateway.
    """

    def run_auto_assignment(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        client: Any,
        metadata: Any,
        ontology: Any,
        entity_mappings: Any,
        relationship_mappings: Any,
        documents: Any = None,
        on_step: Optional[Callable] = None,
        max_iterations: int = 10,
    ) -> "AutoAssignAgentResult":
        """Propose entity and relationship SQL mappings using the auto-assignment agent.

        Args:
            host: Databricks workspace host (with or without ``https://``).
            token: Bearer token for the workspace APIs.
            endpoint_name: Model serving endpoint name for the agent.
            client: SQL client (typically :class:`~back.core.databricks.DatabricksClient`)
                used to validate or sample queries against the configured warehouse.
            metadata: Schema context (for example UC table metadata) for the agent.
            ontology: Ontology dict describing classes and properties to map.
            entity_mappings: Existing or partial entity mapping list for the agent
                to refine or extend.
            relationship_mappings: Existing or partial relationship mapping list.
            documents: Optional list of document dicts (``name``, ``content``) for
                grounding.
            on_step: Optional progress callback invoked by the agent loop.
            max_iterations: Upper bound on agent refinement iterations.

        Returns:
            Structured result from ``agents.agent_auto_assignment`` describing
            proposed mappings and per-item status.

        Raises:
            Exception: Propagates any failure raised by ``run_agent``.
        """
        from agents.agent_auto_assignment import run_agent

        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            client=client,
            metadata=metadata,
            ontology=ontology,
            entity_mappings=entity_mappings,
            relationship_mappings=relationship_mappings,
            documents=documents,
            on_step=on_step,
            max_iterations=max_iterations,
        )

    def run_mapping_pge(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        client: Any,
        metadata: Any,
        ontology: Any,
        entity_mappings: Any,
        relationship_mappings: Any,
        documents: Any = None,
        on_step: Optional[Callable] = None,
        max_iterations: int = 10,
    ) -> "MappingPGEAgentResult":
        """Propose entity and relationship SQL mappings using the mapping PGE agent.

        This is the Planner–Generator–Evaluator (PGE) mapping engine
        (``agents.agent_mapping_pge``). It shares the same call signature as
        :meth:`run_auto_assignment` but drives a deterministic, coverage-checked
        loop with a semantic critic and a structured run log.

        Args:
            host: Databricks workspace host (with or without ``https://``).
            token: Bearer token for the workspace APIs.
            endpoint_name: Model serving endpoint name for the agent.
            client: SQL client (typically :class:`~back.core.databricks.DatabricksClient`)
                used to validate or sample queries against the configured warehouse.
            metadata: Schema context (for example UC table metadata) for the agent.
            ontology: Ontology dict describing classes and properties to map.
            entity_mappings: Existing or partial entity mapping list for the agent
                to refine or extend.
            relationship_mappings: Existing or partial relationship mapping list.
            documents: Optional list of document dicts (``name``, ``content``) for
                grounding.
            on_step: Optional progress callback invoked by the agent loop.
            max_iterations: Upper bound on agent refinement iterations.

        Returns:
            Structured result from ``agents.agent_mapping_pge`` describing
            proposed mappings, per-item status, and PGE diagnostics.

        Raises:
            Exception: Propagates any failure raised by ``run_agent``.
        """
        from agents.agent_mapping_pge import run_agent

        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            client=client,
            metadata=metadata,
            ontology=ontology,
            entity_mappings=entity_mappings,
            relationship_mappings=relationship_mappings,
            documents=documents,
            on_step=on_step,
            max_iterations=max_iterations,
        )



_default_client: Optional[AgentClient] = None


def get_agent_client() -> AgentClient:
    """Return the singleton AgentClient instance."""
    global _default_client
    if _default_client is None:
        _default_client = AgentClient()
    return _default_client
