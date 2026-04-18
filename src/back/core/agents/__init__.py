"""Agent abstraction layer.

Provides :class:`AgentClient` as a seam between domain logic and
LLM agent engines, enabling a future switch to HTTP-based invocation.
"""

from back.core.agents.AgentClient import AgentClient, get_agent_client

__all__ = ["AgentClient", "get_agent_client"]
