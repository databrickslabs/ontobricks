"""
Graph Chat Agent -- conversational agent for querying a Digital Twin
knowledge graph via natural language.

Exports:
    run_agent / AgentResult -- entry point used by the HTTP route
        ``POST /dtwin/assistant/chat``.

The agent calls the same external REST surface as the MCP server
(``/api/v1/digitaltwin/...``, ``/graphql/{domain}``) plus the internal
``/dtwin/execute`` SPARQL endpoint -- all over loopback -- so the user
can ask questions like "how many orders per customer?" in the UI.
"""

from agents.agent_dtwin_chat.engine import run_agent, AgentResult  # noqa: F401

__all__ = ["run_agent", "AgentResult"]
