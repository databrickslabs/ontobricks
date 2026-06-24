"""Neo4j graph database backend.

Cypher-based, remote-only (Neo4j Aura / self-hosted Neo4j). Bolt protocol
via the official ``neo4j`` Python driver.

Public API (split during the PR #47 review — Benoit 2026-06-18):

- :class:`Neo4jStore` — ``GraphDBBackend`` implementation. Thin façade
  composing the three services below.
- :class:`Neo4jConnection` — driver lifecycle, auth resolution, single
  Cypher execution entry point (with INFO-level logging).
- :class:`Neo4jWriteOps` — schema + bulk write paths.
- :class:`Neo4jReadOps` — statistics, entity lookup, KG-filter primitives,
  reasoning helpers.
- :func:`is_neo4j_password_from_secret` — module helper telling the
  Settings layer whether the runtime sources the Bolt password from the
  ``NEO4J_PASSWORD`` env var (Databricks Apps secret resource) or from
  the persisted ``engine_config`` (local-dev fallback).
"""

try:
    import neo4j as _neo4j  # noqa: F401
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False

if NEO4J_AVAILABLE:
    from back.core.graphdb.neo4j.Neo4jConnection import (  # noqa: F401
        Neo4jConnection,
        is_neo4j_password_from_secret,
    )
    from back.core.graphdb.neo4j.Neo4jReadOps import Neo4jReadOps  # noqa: F401
    from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore  # noqa: F401
    from back.core.graphdb.neo4j.Neo4jWriteOps import Neo4jWriteOps  # noqa: F401

    __all__ = [
        "NEO4J_AVAILABLE",
        "Neo4jConnection",
        "Neo4jReadOps",
        "Neo4jStore",
        "Neo4jWriteOps",
        "is_neo4j_password_from_secret",
    ]
else:
    __all__ = ["NEO4J_AVAILABLE"]
