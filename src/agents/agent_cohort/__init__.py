"""
Cohort Discovery Agent — translates a natural-language prompt into a
validated :class:`CohortRule` JSON via tool-calling against the active
session's ontology + graph.

Exports:
    run_agent / AgentResult — entry point used by
        ``POST /dtwin/cohorts/agent`` to propose a rule the user can
        review and save in the cohort form.

Stage 2 of the Cohort Discovery feature (see
``releasereq/cohort_design.md`` §12 and ``docs/cohort_discovery.md``).
"""

from agents.agent_cohort.engine import run_agent, AgentResult  # noqa: F401

__all__ = ["run_agent", "AgentResult"]
