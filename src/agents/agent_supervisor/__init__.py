"""Agent Bricks Supervisor for OntoBricks mapping-engine selection.

Deterministically assesses a domain's complexity and routes the mapping task to
either the PGE engine (``agent_mapping_pge``) or the original simple engine
(``agent_auto_assignment``). Exposed to a Databricks Agent Bricks Multi-Agent
Supervisor via a complexity UC function (``uc_function.sql``) + per-engine Model
Serving endpoints (``responses_agent.py``), wired by ``mas.py``.
"""

from agents.agent_supervisor.complexity import (
    ComplexityAssessor,
    ComplexityReport,
    assess,
)
from agents.agent_supervisor.engine import SupervisorEngine, SupervisorResult
from agents.agent_supervisor.mas import SupervisorProvisioner

__all__ = [
    "ComplexityAssessor",
    "ComplexityReport",
    "assess",
    "SupervisorEngine",
    "SupervisorResult",
    "SupervisorProvisioner",
]
