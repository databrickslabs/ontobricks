"""Evaluator stage of the mapping PGE pipeline.

Stage 1 (this module) is the *deterministic* evaluator — pure-Python checks
backed by SQL counts.  Stage 2 (added in a later sprint) is the semantic
evaluator that uses an LLM to judge naming/semantic fidelity.

The deterministic checks live in :mod:`agents.agent_mapping_pge.evaluator.deterministic`.
"""

from agents.agent_mapping_pge.evaluator.deterministic import (
    evaluate_entity_mapping,
    evaluate_relationship_mapping,
)

__all__ = [
    "evaluate_entity_mapping",
    "evaluate_relationship_mapping",
]
