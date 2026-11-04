"""Lakebase implementation of the :class:`GraphHealthProbe` capability.

Lakebase health lives in Postgres, reachable only over a psycopg connection
built from caller-supplied auth. This facade vends the psycopg driver handle so
health / settings probes keep their connection orchestration without importing
``lakebase.pool`` directly — the only ``lakebase.*`` symbol they needed.
"""

from __future__ import annotations

from typing import Any

from back.core.graphdb.interfaces.GraphHealthProbe import GraphHealthProbe


class HealthProbe(GraphHealthProbe):
    """Postgres-backed health probes for a Lakebase domain."""

    def psycopg(self) -> Any:
        """Return the imported ``psycopg`` driver module (raises if missing)."""
        from back.core.graphdb.lakebase.pool import _require_psycopg

        psycopg, _ = _require_psycopg()
        return psycopg
