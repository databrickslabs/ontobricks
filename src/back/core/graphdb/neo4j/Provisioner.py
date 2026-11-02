"""Neo4j implementation of the :class:`GraphProvisioner` capability.

Thin facade over the Neo4j credential-source helpers so Settings callers can ask
"where does the Neo4j password come from?" without importing ``neo4j.*``
directly.
"""

from __future__ import annotations

from back.core.graphdb.interfaces.GraphProvisioner import GraphProvisioner


class Provisioner(GraphProvisioner):
    """Credential / config helpers for a Neo4j domain."""

    def password_env(self) -> str:
        """Name of the env var holding the Neo4j password (Databricks secret)."""
        from back.core.graphdb.neo4j.Connection import NEO4J_PASSWORD_ENV

        return NEO4J_PASSWORD_ENV

    def is_password_from_secret(self) -> bool:
        """True when the Neo4j password is present in the environment."""
        from back.core.graphdb.neo4j.Connection import (
            is_neo4j_password_from_secret,
        )

        return is_neo4j_password_from_secret()
