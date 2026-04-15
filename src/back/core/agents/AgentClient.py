"""Abstraction over agent invocation.

Currently runs agents in-process via the ``agents`` package.
When agents become a separate service, route calls through an HTTP
client without changing callers.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

if TYPE_CHECKING:
    from agents.agent_owl_generator.engine import AgentResult
    from agents.agent_auto_assignment.engine import AgentResult as AutoAssignAgentResult
    from agents.agent_auto_icon_assign.engine import AgentResult as IconAssignAgentResult


class AgentClient:
    """Unified gateway to all LLM-agent capabilities.

    Usage::

        client = AgentClient()
        result = client.run_owl_generator(host=..., token=..., ...)
    """

    def run_owl_generator(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        base_uri: str,
        selected_tables: List[str],
        metadata: Optional[Dict] = None,
        ontology: Optional[Dict] = None,
        on_step: Optional[Callable] = None,
    ) -> "AgentResult":
        from agents.agent_owl_generator import run_agent
        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            base_uri=base_uri,
            selected_tables=selected_tables,
            metadata=metadata,
            ontology=ontology,
            on_step=on_step,
        )

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

    def run_icon_assign(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        entity_names: List[str],
        metadata: Optional[Dict] = None,
        ontology: Optional[Dict] = None,
        on_step: Optional[Callable] = None,
    ) -> "IconAssignAgentResult":
        from agents.agent_auto_icon_assign import run_agent
        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            entity_names=entity_names,
            metadata=metadata,
            ontology=ontology,
            on_step=on_step,
        )

    def run_ontology_assistant(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        messages: List[Dict[str, str]],
        ontology_context: Dict[str, Any],
        on_step: Optional[Callable] = None,
    ) -> Any:
        from agents.agent_ontology_assistant import run_agent
        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            messages=messages,
            ontology_context=ontology_context,
            on_step=on_step,
        )


_default_client: Optional[AgentClient] = None


def get_agent_client() -> AgentClient:
    """Return the singleton AgentClient instance."""
    global _default_client
    if _default_client is None:
        _default_client = AgentClient()
    return _default_client
