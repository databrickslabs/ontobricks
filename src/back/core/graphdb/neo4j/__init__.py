"""Neo4j graph database backend.

Cypher-based, remote-only (Neo4j Aura / self-hosted Neo4j). Bolt protocol
via the official ``neo4j`` Python driver. See ``Neo4jStore`` for the
implementation contract.

PR 1 ships the skeleton + flat-triple CRUD. Named-query Cypher overrides
(transitive closure, BFS, type distribution, …) land in PR 2 alongside
``SWRLFlatCypherTranslator`` for reasoning.
"""

try:
    import neo4j as _neo4j  # noqa: F401
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False

if NEO4J_AVAILABLE:
    from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore  # noqa: F401

    __all__ = ["Neo4jStore", "NEO4J_AVAILABLE"]
else:
    __all__ = ["NEO4J_AVAILABLE"]
