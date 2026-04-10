"""
Ontology Assistant Agent – conversational agent for modifying ontologies via natural language.

Exports:
    run_agent / AgentResult — original engine (used by existing routes)
    OntologyAssistantResponsesAgent — MLflow ResponsesAgent wrapper (requires mlflow)
"""

from agents.agent_ontology_assistant.engine import run_agent, AgentResult  # noqa: F401

try:
    from agents.agent_ontology_assistant.responses_agent import (  # noqa: F401
        OntologyAssistantResponsesAgent,
    )
except ImportError:
    OntologyAssistantResponsesAgent = None  # mlflow not available (local dev)
