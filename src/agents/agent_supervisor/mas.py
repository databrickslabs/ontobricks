"""Agent Bricks Supervisor (MAS) configuration + provisioning.

Builds the Multi-Agent Supervisor that orchestrates OntoBricks mapping. The
supervisor wires three agents:

1. ``complexity_assessor`` - the deterministic UC function (``uc_function.sql``)
   that scores a domain and recommends an engine. The supervisor calls this
   FIRST.
2. ``pge_mapping`` - the Model Serving endpoint wrapping ``agent_mapping_pge``.
3. ``simple_mapping`` - the Model Serving endpoint wrapping
   ``agent_auto_assignment`` (the original single-agent engine).

Routing is the requested hybrid: a deterministic UC function provides the hard
recommendation, and natural-language instructions tell the supervisor to act on
it. The supervisor reads ``recommended_engine`` from the assessor and routes to
the matching mapping endpoint.

``build_config`` is pure (no I/O) so it can be unit-tested; ``provision`` applies
it via the Agent Bricks ``manage_mas`` MCP tool / SDK at deploy time.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from back.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_SUPERVISOR_NAME = "OntoBricks Mapping Supervisor"

ROUTING_INSTRUCTIONS = """\
You orchestrate entity/relationship mapping for an OntoBricks domain. You have
three tools:

1. complexity_assessor - a deterministic function that scores a domain's mapping
   complexity from its source metadata + ontology and returns JSON with a
   `recommended_engine` field ("pge" or "simple").
2. pge_mapping - the heavyweight Planner-Generator-Evaluator mapping engine. Use
   for COMPLEX domains: many source tables, cross-source reconciliation (the same
   entity keyed across several feeds), large ontologies, or heterogeneous column
   naming. It plans a source model and gates every mapping with a deterministic
   evaluator plus a semantic critic.
3. simple_mapping - the lightweight single-agent engine. Use for SIMPLE domains:
   a single source, few tables, a small ontology, and uniform schema.

ALWAYS follow this procedure:
- Step 1: call complexity_assessor with the domain's metadata and ontology.
- Step 2: read `recommended_engine` from its JSON response.
- Step 3: if it is "pge", route the mapping task to pge_mapping; if "simple",
  route to simple_mapping. Pass the domain context through unchanged.
- Never skip the assessor. Never pick an engine on your own judgement when the
  assessor has given a recommendation; the assessor's verdict is authoritative.
"""


@dataclass
class SupervisorAgentRef:
    """One agent entry in the MAS config."""

    name: str
    description: str
    uc_function_name: str = ""
    endpoint_name: str = ""

    def to_dict(self) -> dict:
        d = {"name": self.name, "description": self.description}
        if self.uc_function_name:
            d["uc_function_name"] = self.uc_function_name
        if self.endpoint_name:
            d["endpoint_name"] = self.endpoint_name
        return d


class SupervisorProvisioner:
    """Build and provision the OntoBricks mapping Supervisor Agent."""

    @staticmethod
    def build_config(
        *,
        catalog: str,
        schema: str,
        pge_endpoint: str,
        simple_endpoint: str,
        name: str = DEFAULT_SUPERVISOR_NAME,
    ) -> dict:
        """Return the ``manage_mas`` create-or-update payload (pure, no I/O)."""
        assessor_fn = f"{catalog}.{schema}.assess_domain_complexity"
        agents: List[SupervisorAgentRef] = [
            SupervisorAgentRef(
                name="complexity_assessor",
                uc_function_name=assessor_fn,
                description=(
                    "Deterministically scores a domain's mapping complexity from its "
                    "source metadata and ontology. Returns JSON including "
                    "recommended_engine ('pge' or 'simple'). CALL THIS FIRST to decide "
                    "which mapping engine to use."
                ),
            ),
            SupervisorAgentRef(
                name="pge_mapping",
                endpoint_name=pge_endpoint,
                description=(
                    "Heavyweight Planner-Generator-Evaluator mapping engine for COMPLEX "
                    "domains: many tables, cross-source reconciliation, large or "
                    "heterogeneous schemas. Plans a source model and gates each mapping "
                    "with a deterministic evaluator + semantic critic."
                ),
            ),
            SupervisorAgentRef(
                name="simple_mapping",
                endpoint_name=simple_endpoint,
                description=(
                    "Lightweight single-agent mapping engine for SIMPLE domains: a single "
                    "source, few tables, a small ontology, uniform schema. Fast; no "
                    "planning or independent evaluation."
                ),
            ),
        ]
        return {
            "name": name,
            "description": (
                "Routes OntoBricks entity/relationship mapping to the PGE or the simple "
                "engine based on a deterministic complexity assessment."
            ),
            "instructions": ROUTING_INSTRUCTIONS,
            "agents": [a.to_dict() for a in agents],
            "examples": SupervisorProvisioner._examples(),
        }

    @staticmethod
    def _examples() -> List[Dict[str, str]]:
        return [
            {
                "question": (
                    "Map this domain: 3 trust feeds (trust_a, trust_b, trust_c) sharing "
                    "MOTHER_NHS_NO, ~17 ontology classes."
                ),
                "guideline": (
                    "Call complexity_assessor; it returns recommended_engine='pge' "
                    "(cross-source + large ontology). Route to pge_mapping."
                ),
            },
            {
                "question": (
                    "Map this domain: one table 'patients' with 6 columns, 2 ontology "
                    "classes."
                ),
                "guideline": (
                    "Call complexity_assessor; it returns recommended_engine='simple'. "
                    "Route to simple_mapping."
                ),
            },
        ]

    @staticmethod
    def provision(config: dict) -> str:
        """Create/update the Supervisor Agent from *config*.

        Uses the Agent Bricks ``manage_mas`` capability. Kept import-local and
        best-effort so the module imports cleanly in environments without the
        Agent Bricks SDK (e.g. unit tests); raises if provisioning is attempted
        without it.
        """
        try:
            from databricks.agents import mas  # type: ignore
        except Exception as exc:  # pragma: no cover - deploy-time path
            raise RuntimeError(
                "Agent Bricks SDK not available; provision via the manage_mas MCP "
                "tool or run inside a Databricks environment."
            ) from exc
        logger.info("Provisioning Supervisor Agent %r", config.get("name"))
        return mas.create_or_update(**config)  # pragma: no cover
