"""
Business Rules Generator Agent — proposes SWRL, Decision Table, SPARQL, and
Aggregate rules from the domain's uploaded documents and the live ontology
design, for the user to review and accept in the Business Rules page.

Exports:
    run_agent / AgentResult — entry point used by
        ``POST /ontology/business-rules/generate-async`` to propose rule
        candidates the user can review and save.
"""

from agents.agent_business_rules_generator.engine import (  # noqa: F401
    run_agent,
    AgentResult,
)

__all__ = ["run_agent", "AgentResult"]
