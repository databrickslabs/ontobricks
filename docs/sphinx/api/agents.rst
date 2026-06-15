``agents`` -- LLM Agents & Tools
=================================

.. automodule:: agents
   :members:
   :undoc-members:
   :show-inheritance:

LLM Utilities
-------------

.. automodule:: agents.llm_utils
   :members:
   :undoc-members:
   :show-inheritance:

Tracing
-------

.. automodule:: agents.tracing
   :members:
   :undoc-members:
   :show-inheritance:

Mapping PGE Pipeline
--------------------

The Mapping PGE pipeline replaces the legacy single-loop auto-assignment agent
with a Planner → Generator → Evaluator decomposition: a global planner emits a
typed ``SourceModel``, narrow per-item generators produce SQL against that
plan, and a two-stage evaluator (deterministic checks + semantic critic) gates
every mapping.

.. automodule:: agents.agent_mapping_pge
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_mapping_pge.engine
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_mapping_pge.planner
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_mapping_pge.contracts
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_mapping_pge.generators.entity
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_mapping_pge.generators.relationship
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_mapping_pge.evaluator.deterministic
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_mapping_pge.evaluator.critic
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_mapping_pge.evaluator.report
   :members:
   :undoc-members:
   :show-inheritance:

Auto Icon Assignment Agent
--------------------------

.. automodule:: agents.agent_auto_icon_assign
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_auto_icon_assign.engine
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_auto_icon_assign.tools
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: ToolContext

Ontology Assistant Agent
------------------------

.. automodule:: agents.agent_ontology_assistant
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_ontology_assistant.engine
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_ontology_assistant.tools
   :members:
   :undoc-members:
   :show-inheritance:

OWL Generator Agent
-------------------

.. automodule:: agents.agent_owl_generator
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_owl_generator.engine
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_owl_generator.tools
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: ToolContext

Cohort Discovery Agent
----------------------

.. automodule:: agents.agent_cohort
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_cohort.engine
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_cohort.tools
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: ToolContext

Graph Chat Agent
----------------

.. automodule:: agents.agent_dtwin_chat
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_dtwin_chat.engine
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.agent_dtwin_chat.tools
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: ToolContext

Shared Tools
------------

.. automodule:: agents.tools.context
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.tools.documents
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.tools.icons
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.tools.mapping
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.tools.metadata
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.tools.ontology
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.tools.planner
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.tools.evaluation
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.tools.sql
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.tools.loopback_http
   :members:
   :undoc-members:
   :show-inheritance:

PGE Intrinsic Evaluation
------------------------

A usecase-agnostic, gold-free scorecard for the PGE pipeline (ontology +
mapping generation). Intrinsic structural/self-consistency metrics plus an
advisory LLM-judge — no stored reference answer. The deterministic core
(``score_artifact``) ingests a captured ``AgentResult`` artifact and emits the
scorecard JSON with zero LLM calls; the in-app hooks run it live after
generation/mapping; the CLI lives in ``scripts/goals_eval.py``.

.. automodule:: agents.pge_eval
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.scorecard
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.normalize
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.ontology_metrics
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.mapping_metrics
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.pipeline_metrics
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.gates
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.baseline
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.judge
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.inapp
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: agents.pge_eval.loaders
   :members:
   :undoc-members:
   :show-inheritance:
