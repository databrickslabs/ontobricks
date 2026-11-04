"""
Simulation facts agent — natural language to hypothetical S/P/O patches.

``POST /simulation/facts/agent`` runs :func:`run_agent` and returns the
proposed patches plus a complete scenario description. The agent never
persists the scenario.
"""

from agents.agent_simulation_facts.AgentResult import AgentResult
from agents.agent_simulation_facts.engine import run_agent

__all__ = ["run_agent", "AgentResult"]
