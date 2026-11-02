"""Capability interface: sandbox / simulation support for a graph backend.

Factory-vended via :meth:`GraphDBFactory.get_simulation` so the ``simulation/**``
package never imports a backend package directly. It groups the backend-specific
touch-points the sandbox lifecycle needs: the write store that isolates sandbox
mutations, the physical names of the ``__sim_`` table / ``__smu_`` union view, and
the Explorer union-view lifecycle.

Backends diverge sharply (Lakebase overlays through Postgres union views; Delta
builds a UC view in the sandbox class itself; Neo4j isolates by marker label), so
the base supplies safe defaults and each impl overrides only what it supports.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class GraphSimulation(ABC):
    """Sandbox / simulation operations for one graph backend."""

    @abstractmethod
    def simulation_store(self, live_store: Any) -> Any:
        """Return the write store that sandbox mutations target.

        The live store is read-only during a simulation; this returns a store
        whose writes land only in the sandbox relation (never the live domain).
        """

    def is_native_store(self, live_store: Any) -> bool:
        """True when *live_store* is this backend's store type."""
        return False

    def sandbox_table_name(self, graph_name: str, sim_id: str) -> str:
        """Physical name of the sandbox (``__sim_``) relation."""
        raise NotImplementedError

    def sim_union_view_name(self, graph_name: str, sim_id: str) -> str:
        """Physical name of the Explorer union (``__smu_``) view."""
        raise NotImplementedError

    def sim_table_name(self, graph_name: str, sim_id: str) -> str:
        """Physical name of the sandbox table (artifacts listing)."""
        raise NotImplementedError

    def drop_union(self, live_store: Any, graph_name: str, sim_id: str) -> None:
        """Drop the Explorer union view(s) for a sandbox (no-op if unsupported)."""
        return None

    def ensure_union(
        self, live_store: Any, view: str, live_union: str, sandbox_table: str
    ) -> None:
        """Materialise the Explorer union view (no-op if unsupported)."""
        return None
