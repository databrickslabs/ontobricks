"""Neo4j implementation of the :class:`GraphHealthProbe` capability.

Neo4j health / object-listing lives behind a Bolt driver reachable only from a
:class:`Connection`. This facade builds that connection from an engine
config and vends the read / write op wrappers, so Settings callers keep their
connection orchestration (``verify_connectivity`` / ``run`` / ``close``) without
importing ``neo4j.*`` directly.
"""

from __future__ import annotations

from typing import Any, Dict

from back.core.graphdb.interfaces.GraphHealthProbe import GraphHealthProbe


class HealthProbe(GraphHealthProbe):
    """Bolt connection + read/write op factory for a Neo4j domain."""

    def connection(self, engine_config: Dict[str, Any]) -> Any:
        """Build a :class:`Connection` from a Neo4j engine config section."""
        from back.core.graphdb.neo4j.Connection import (
            Connection,
            resolve_neo4j_database,
        )

        return Connection(
            uri=str(engine_config.get("uri") or "").strip(),
            database=resolve_neo4j_database(engine_config),
            auth_method=str(
                engine_config.get("auth_method") or "databricks_secret"
            ).strip()
            or "databricks_secret",
            engine_config=engine_config,
            encrypted=bool(engine_config.get("encrypted", True)),
        )

    def read_ops(self, connection: Any) -> Any:
        """Read-side Cypher ops (``list_databases`` / ``list_labels`` …)."""
        from back.core.graphdb.neo4j.ReadOps import ReadOps

        return ReadOps(connection)

    def write_ops(self, connection: Any) -> Any:
        """Write-side Cypher ops (``drop_table`` …)."""
        from back.core.graphdb.neo4j.WriteOps import WriteOps

        return WriteOps(connection)
