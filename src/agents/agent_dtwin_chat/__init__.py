"""
Graph Chat Agent -- conversational agent for querying a Knowledge Graph
knowledge graph via natural language.

Exports:
    run_agent / AgentResult -- entry point used by the HTTP route
        ``POST /dtwin/assistant/chat``.

The agent calls the external REST and GraphQL surfaces plus internal
``/dtwin/...`` endpoints over loopback, so the user can ask questions
like "how many orders per customer?" in the UI.
"""

from agents.agent_dtwin_chat.engine import run_agent, AgentResult  # noqa: F401

__all__ = ["run_agent", "AgentResult"]
