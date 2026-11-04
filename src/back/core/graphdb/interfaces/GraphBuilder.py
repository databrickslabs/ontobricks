"""Capability interface: build / materialize a domain's graph.

Factory-vended via :meth:`GraphDBFactory.get_builder` so no module outside
``back/core/graphdb/`` imports a backend package directly. Every engine that owns
a materialized copy (Delta ``table``, Lakebase, Neo4j) provides an
implementation; a no-copy/virtual (Delta ``view``) or graphless domain has no
builder and the factory returns ``None``.

This is a **capability marker**: the concrete method surface is engine-specific
(Delta materializes Unity-Catalog relations and drives the triple-store build
pipeline; Lakebase drives the Lakeflow synced-table pipeline), so the methods
live on the concrete implementations (:class:`Builder`,
:class:`Builder`). Callers obtain the right builder from the factory
and use the operations that backend exposes.
"""

from __future__ import annotations

from abc import ABC


class GraphBuilder(ABC):
    """Build / materialize operations for a domain's graph backend."""
