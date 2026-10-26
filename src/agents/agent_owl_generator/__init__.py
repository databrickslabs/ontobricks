"""OWL Generator Agent — staged, human-in-the-loop ontology Generate.

Public path (SPEC §2, staged contract): the four explicit staged entry points
:func:`detect_entities`, :func:`infer_relations`, :func:`infer_attributes`,
and :func:`infer_axioms` from :mod:`agents.agent_owl_generator.staged`. The
staged flow is detect → human review → checkpointed relations/attributes/
axioms completion, appending to (never replacing) the existing ontology.

The one-shot ``run_agent`` bridge in :mod:`agents.agent_owl_generator.engine`
is **deprecated**: it contains the post-generation pitfall-rewrite loop the
three-stage design replaces, and every production caller
(``Ontology.generate_with_agent``, ``AgentClient.run_owl_generator``, the
supervisor's ``"ontology"`` task, and the ``/ontology/wizard/generate-async``
route) has been removed as of Task 4 of ``staged-ontology-generate``. The
module still exists only for direct, explicit, non-production use (e.g. ad
hoc scripts); it is intentionally NOT re-exported as part of the default
public surface, and the staged module never imports or falls back to it.
"""

from agents.agent_owl_generator.staged import (  # noqa: F401
    CompletionResult,
    DetectionResult,
    detect_entities,
    infer_attributes,
    infer_axioms,
    infer_relations,
)

__all__ = [
    "detect_entities",
    "infer_relations",
    "infer_attributes",
    "infer_axioms",
    "DetectionResult",
    "CompletionResult",
]
