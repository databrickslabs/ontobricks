"""Capability interface: provision schema / instance / synced tables.

Factory-vended via :meth:`GraphDBFactory.get_provisioner`. Only backends that
own provisionable infrastructure implement it — today that is Lakebase (schema
bootstrap, Postgres instance, Lakeflow synced tables). Delta and Neo4j domains,
and graphless domains, have no provisioner and the factory returns ``None``.

The concrete method surface is populated in the Lakebase migration step; this
module defines the shared marker contract so callers can depend on the interface
rather than ``back.core.graphdb.lakebase.*``.
"""

from __future__ import annotations

from abc import ABC


class GraphProvisioner(ABC):
    """Schema / instance / synced-table provisioning for a graph backend."""
