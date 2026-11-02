"""Neo4j implementation of the :class:`GraphHealthProbe` capability.

Neo4j health / object-listing lives behind a Bolt driver reachable only from a
:class:`Neo4jConnection`. This facade builds that connection from an engine
config and vends the read / write op wrappers, so Settings callers keep their
connection orchestration (``verify_connectivity`` / ``run`` / ``close``) without
importing ``neo4j.*`` directly.
"""

from __future__ import annotations

from typing import Any, Dict

from back.core.graphdb.GraphHealthProbe import GraphHealthProbe


class Neo4jHealthProbe(GraphHealthProbe):
    """Bolt connection + read/write op factory for a Neo4j domain."""

    def connection(self, engine_config: Dict[str, Any]) -> Any:
        """Build a :class:`Neo4jConnection` from a Neo4j engine config section."""
        from back.core.graphdb.neo4j.Neo4jConnection import (
            Neo4jConnection,
            resolve_neo4j_database,
        )

        return Neo4jConnection(
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
        from back.core.graphdb.neo4j.Neo4jReadOps import Neo4jReadOps

        return Neo4jReadOps(connection)

    def write_ops(self, connection: Any) -> Any:
        """Write-side Cypher ops (``drop_table`` …)."""
        from back.core.graphdb.neo4j.Neo4jWriteOps import Neo4jWriteOps

        return Neo4jWriteOps(connection)
