"""Capability interface: health / quality / object-listing probes.

Factory-vended via :meth:`GraphDBFactory.get_health_probe` so health, quality
and object-listing logic is reachable without importing a backend package
directly. A graphless domain has none and the factory returns ``None``.

This is a **capability marker**: the concrete probe surface is engine-specific
(Delta probes Unity-Catalog tables / permissions on a SQL warehouse; Lakebase
probes a Postgres schema over psycopg), so the methods live on the concrete
implementations (:class:`DeltaHealthProbe`, :class:`LakebaseHealthProbe`).
"""

from __future__ import annotations

from abc import ABC


class GraphHealthProbe(ABC):
    """Health, quality and object-listing probes for a graph backend."""
