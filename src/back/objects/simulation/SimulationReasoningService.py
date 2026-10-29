"""Reasoning service retargeted to a simulation sandbox table."""

from __future__ import annotations

from typing import Any

from back.core.reasoning.ReasoningService import ReasoningService


class SimulationReasoningService(ReasoningService):
    """Run every reasoning phase against ``sandbox_table`` instead of the live graph.

    All phases resolve their table through :meth:`_get_graph_name`, so
    overriding it (together with a sandbox-pinned store) is enough to make
    T-Box / SWRL / graph / decision-table / SPARQL / aggregate phases read
    and materialise inside the sandbox only.
    """

    def __init__(self, domain_session: Any, sandbox_store: Any, sandbox_table: str) -> None:
        super().__init__(domain_session, sandbox_store)
        self._sandbox_table = sandbox_table

    def _get_graph_name(self) -> str:
        return self._sandbox_table
