"""Business insights agent for the Simulation Compare → Evaluate tab.

Compares the Live and Sandbox answers for one prompt and explains the
business difference, anchored on the scenario's fact changes.

Exports:
    run_agent / AgentResult -- entry point used by the streaming route
        ``POST /dtwin/assistant/insights/stream``.
"""

from agents.agent_simulation_insights.AgentResult import AgentResult
from agents.agent_simulation_insights.engine import run_agent

__all__ = ["run_agent", "AgentResult"]
