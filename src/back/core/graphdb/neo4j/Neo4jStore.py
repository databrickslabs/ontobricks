"""Neo4j graph database backend — thin façade over three composed services.

Bolt-based (Cypher) flat-triple store. Triples are persisted as
``(:<sanitised_table_name> {subject, predicate, object})`` nodes — a
deliberately simple schema chosen so PR 1 demonstrates the Cypher
integration shape without committing to a typed-node graph model (which
lands in v2 / PR 3+). One label per logical store so Neo4j 5+ ``CREATE
CONSTRAINT`` (which only accepts single-label patterns) works.

Implementation is split across three services (extracted during the PR
#47 review — Benoit 2026-06-18 "la classe est trop grosse"):

- :class:`Neo4jConnection` — driver lifecycle, auth resolution
  (NEO4J_PASSWORD env var first, engine_config fallback in local dev,
  hard refusal in the deployed app without a secret resource), and the
  single :meth:`Neo4jConnection.run` execution path that emits one INFO
  log line per Cypher statement.
- :class:`Neo4jWriteOps` — schema (constraint create/drop), bulk writes
  (``UNWIND`` + ``MERGE`` / ``DETACH DELETE``), cohort wipes.
- :class:`Neo4jReadOps` — the 16+ named-query methods of the
  ``TripleStoreBackend`` contract: statistics, entity lookup, pagination,
  KG-filter primitives (``find_seed_subjects`` / ``bfs_traversal`` /
  ``expand_entity_neighbors``), and reasoning helpers (``transitive_closure``,
  ``symmetric_expand``, ``shortest_path``).

``execute_query`` deliberately raises ``NotImplementedError`` — no raw
Cypher entry point. All writes go through ``insert_triples`` after
ontology validation in the build pipeline (Benoit's C2 safeguard:
"l'entrée se fait par l'ontologie").

Reasoning translation (``get_query_translator``) returns a
:class:`SWRLFlatCypherTranslator` that is currently scaffolded only —
full SWRL → Cypher translation lands in a follow-up PR.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.graphdb.neo4j.Neo4jConnection import (
    DEFAULT_AUTH_METHOD,
    DEFAULT_DATABASE,
    NEO4J_PASSWORD_ENV,
    Neo4jConnection,
    SUPPORTED_AUTH_METHODS,
    _normalise_cypher_for_log,  # noqa: F401  — re-exported for legacy callers/tests
    is_neo4j_password_from_secret,
    resolve_neo4j_database,
)
from back.core.graphdb.neo4j.Neo4jReadOps import Neo4jReadOps
from back.core.graphdb.neo4j.Neo4jWriteOps import Neo4jWriteOps, sanitise_label
from back.core.logging import get_logger
from shared.config.constants import DEFAULT_GRAPH_NAME

logger = get_logger(__name__)

# Re-exported so callers that did ``from Neo4jStore import is_neo4j_password_from_secret``
# (SettingsService, home.py) keep working without churn.
__all__ = [
    "DEFAULT_AUTH_METHOD",
    "DEFAULT_DATABASE",
    "NEO4J_PASSWORD_ENV",
    "Neo4jStore",
    "SUPPORTED_AUTH_METHODS",
    "is_neo4j_password_from_secret",
]


class Neo4jStore(GraphDBBackend):
    """Neo4j (Bolt / Cypher) graph database backend — flat triple model.

    Public façade composing :class:`Neo4jConnection`, :class:`Neo4jWriteOps`,
    and :class:`Neo4jReadOps`. Implements both the ``TripleStoreBackend``
    and ``GraphDBBackend`` contracts.

    Parameters
    ----------
    db_name:
        Logical name for the triple set, used as the ``table`` label in the
        Cypher schema (every triple node carries the single label
        ``:<sanitised_db_name>``).
    engine_config:
        JSON dict from Settings > Graph DB > Engine Configuration. Keys:

        ``uri`` (required)
            Bolt URI, e.g. ``neo4j+s://b4810af7.databases.neo4j.io``.
        ``database`` (default ``"neo4j"``)
            Logical Neo4j database name on the target instance.
        ``auth_method`` (default ``"basic"``)
            ``"basic"`` → username + password. ``"databricks_secret"`` →
            credentials resolved from a Databricks secret scope (PR 3,
            deferred).
        ``username``
            Required when ``auth_method == "basic"``.
        ``password``
            Local-dev fallback when ``auth_method == "basic"``. **In the
            deployed app** (when ``DATABRICKS_APP_PORT`` is set) the password
            MUST come from the ``NEO4J_PASSWORD`` env var, populated via a
            Databricks Apps secret resource bound in ``app.yaml``. The
            persisted JSON ``password`` is ignored in prod and stripped at
            save-time so no clear-text credential ever lands in
            ``global_config``. See
            ``docs/pr47-neo4j-demo/secret-configuration.md``.
        ``secret_scope``, ``secret_key``
            Required when ``auth_method == "databricks_secret"``.
        ``encrypted`` (default ``True``)
            Bolt-level encryption flag (ignored when URI is ``neo4j+s://``).
    """

    def __init__(
        self,
        db_name: str = DEFAULT_GRAPH_NAME,
        engine_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.db_name = db_name
        self.engine_config: Dict[str, Any] = engine_config or {}
        cfg = self.engine_config

        uri = str(cfg.get("uri") or "").strip()
        if not uri:
            raise ValueError(
                "Neo4jStore: engine_config['uri'] is required "
                "(e.g. 'neo4j+s://<aura-id>.databases.neo4j.io')"
            )
        database = resolve_neo4j_database(cfg)
        auth_method = str(cfg.get("auth_method") or DEFAULT_AUTH_METHOD).strip()
        if auth_method not in SUPPORTED_AUTH_METHODS:
            raise ValueError(
                "Neo4jStore: unsupported auth_method %r (allowed: %s)"
                % (auth_method, ", ".join(SUPPORTED_AUTH_METHODS))
            )
        encrypted = bool(cfg.get("encrypted", True))

        # Cache constructor-derived fields so the existing test suite (which
        # reads them directly) keeps passing.
        self._uri = uri
        self._database = database
        self._auth_method = auth_method
        self._encrypted = encrypted

        self._connection = Neo4jConnection(
            uri=uri,
            database=database,
            auth_method=auth_method,
            engine_config=self.engine_config,
            encrypted=encrypted,
        )
        self._writes = Neo4jWriteOps(self._connection)
        self._reads = Neo4jReadOps(self._connection)

    # ======================================================================
    #  GraphDBBackend — capability flags
    # ======================================================================

    @property
    def supports_cypher(self) -> bool:
        return True

    @property
    def supports_graph_model(self) -> bool:
        # PR 1: flat-triple model (single :Triple node label).
        # Typed-node graph model is a future PR.
        return False

    @property
    def query_dialect(self) -> str:
        return "cypher"

    # ======================================================================
    #  Connection — delegates to Neo4jConnection
    # ======================================================================

    def get_connection(self) -> Any:
        return self._connection.get_driver()

    def close(self) -> None:
        self._connection.close()

    @property
    def _driver(self) -> Any:
        # Back-compat: some tests patch `store._driver` directly. Surface
        # the connection's driver attribute through this property so the
        # old patching pattern keeps working.
        return self._connection._driver

    @_driver.setter
    def _driver(self, value: Any) -> None:
        self._connection._driver = value

    def _run(self, cypher: str, **params: Any) -> List[Dict[str, Any]]:
        """Back-compat passthrough — tests mock ``store._run``.

        The split moved actual execution to :meth:`Neo4jConnection.run`.
        New code in this module calls ``self._connection.run`` directly
        (via the WriteOps / ReadOps helpers) so mocking happens at the
        connection layer. This passthrough preserves the public API
        surface for the few external callers that still drive Cypher
        through the store.
        """
        return self._connection.run(cypher, **params)

    @staticmethod
    def _is_deployed_app() -> bool:
        """Back-compat delegate to :meth:`Neo4jConnection._is_deployed_app`."""
        return Neo4jConnection._is_deployed_app()

    def _resolve_auth(self) -> Tuple[str, str]:
        """Back-compat delegate to :meth:`Neo4jConnection._resolve_auth`."""
        return self._connection._resolve_auth()

    # ======================================================================
    #  GraphDBBackend — schema helpers
    # ======================================================================

    def get_node_table(self, table_name: str) -> str:
        return sanitise_label(table_name)

    def get_graph_schema(self) -> Optional[Any]:
        # Flat model — no schema object.
        return None

    # ======================================================================
    #  GraphDBBackend — sync to/from UC Volume (Aura is remote; no-ops)
    # ======================================================================

    def sync_to_remote(self, uc_path: str, volume_service: Any) -> Tuple[bool, str]:
        return False, "Neo4j is remote-only; no UC Volume sync"

    def sync_from_remote(self, uc_path: str, volume_service: Any) -> Tuple[bool, str]:
        return False, "Neo4j is remote-only; no UC Volume sync"

    def local_path(self) -> Optional[str]:
        return None

    def remote_archive_path(self, uc_domain_path: str) -> Optional[str]:
        return None

    # ======================================================================
    #  GraphDBBackend — reasoning support
    # ======================================================================

    def get_query_translator(self, table_name: str = "") -> Any:
        """Return the SWRL/rule translator for this engine.

        Returns a :class:`SWRLFlatCypherTranslator` — currently scaffolded
        (every translation returns ``None``), so reasoning on Neo4j
        reports zero violations / zero inferences instead of crashing.
        Full SWRL → Cypher translation is a follow-up PR.
        """
        from back.core.reasoning.SWRLFlatCypherTranslator import (
            SWRLFlatCypherTranslator,
        )

        return SWRLFlatCypherTranslator(node_label=self.get_node_table(table_name))

    # ======================================================================
    #  TripleStoreBackend — write delegators (→ Neo4jWriteOps)
    # ======================================================================

    def create_table(self, table_name: str) -> None:
        return self._writes.create_table(table_name)

    def drop_table(self, table_name: str) -> None:
        return self._writes.drop_table(table_name)

    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        return self._writes.insert_triples(table_name, triples, batch_size, on_progress)

    def delete_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        return self._writes.delete_triples(table_name, triples, batch_size, on_progress)

    def optimize_table(self, table_name: str) -> None:
        return self._writes.optimize_table(table_name)

    def delete_cohort_triples(
        self,
        table_name: str,
        cohort_uri_prefix: str,
        in_cohort_predicate: str,
    ) -> int:
        return self._writes.delete_cohort_triples(
            table_name, cohort_uri_prefix, in_cohort_predicate
        )

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        # Deliberate: no raw Cypher entry point. All writes go through
        # the build pipeline after ontology validation (C2 safeguard).
        raise NotImplementedError(
            "Neo4jStore does not expose raw Cypher execution. "
            "Use the named query methods on TripleStoreBackend instead."
        )

    # ======================================================================
    #  TripleStoreBackend — read delegators (→ Neo4jReadOps)
    # ======================================================================

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        return self._reads.query_triples(table_name)

    def count_triples(self, table_name: str) -> int:
        return self._reads.count_triples(table_name)

    def table_exists(self, table_name: str) -> bool:
        return self._reads.table_exists(table_name)

    def get_status(self, table_name: str) -> Dict[str, Any]:
        return self._reads.get_status(table_name)

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        return self._reads.get_aggregate_stats(table_name)

    def get_type_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        return self._reads.get_type_distribution(table_name)

    def get_predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        return self._reads.get_predicate_distribution(table_name)

    def find_subjects_by_type(
        self,
        table_name: str,
        type_uri: str,
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[str]:
        return self._reads.find_subjects_by_type(
            table_name, type_uri, limit=limit, offset=offset, search=search
        )

    def resolve_subject_by_id(
        self, table_name: str, type_uri: str, id_fragment: str
    ) -> Optional[str]:
        return self._reads.resolve_subject_by_id(table_name, type_uri, id_fragment)

    def get_entity_metadata(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        return self._reads.get_entity_metadata(table_name, subjects)

    def get_triples_for_subjects(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        return self._reads.get_triples_for_subjects(table_name, subjects)

    def get_predicates_for_type(self, table_name: str, type_uri: str) -> List[str]:
        return self._reads.get_predicates_for_type(table_name, type_uri)

    def paginated_triples(
        self,
        table_name: str,
        conditions: List[str],
        limit: int,
        offset: int,
    ) -> List[Dict[str, str]]:
        return self._reads.paginated_triples(table_name, conditions, limit, offset)

    def paginated_count(self, table_name: str, conditions: List[str]) -> int:
        return self._reads.paginated_count(table_name, conditions)

    def bfs_traversal(
        self,
        table_name: str,
        seed_where: str,
        depth: int,
        search: str = "",
        entity_type: str = "",
    ) -> List[Dict[str, Any]]:
        return self._reads.bfs_traversal(
            table_name, seed_where, depth, search=search, entity_type=entity_type
        )

    def find_seed_subjects(
        self,
        table_name: str,
        entity_type: str = "",
        field: str = "any",
        match_type: str = "contains",
        value: str = "",
        limit: int = 0,
    ) -> Set[str]:
        return self._reads.find_seed_subjects(
            table_name,
            entity_type=entity_type,
            field=field,
            match_type=match_type,
            value=value,
            limit=limit,
        )

    def find_subjects_by_patterns(
        self, table_name: str, like_patterns: List[str]
    ) -> Set[str]:
        return self._reads.find_subjects_by_patterns(table_name, like_patterns)

    def expand_entity_neighbors(
        self, table_name: str, entity_uris: Set[str]
    ) -> Set[str]:
        return self._reads.expand_entity_neighbors(table_name, entity_uris)

    def transitive_closure(
        self,
        table_name: str,
        predicate_uri: str,
        start_uri: Optional[str] = None,
        max_depth: int = 20,
    ) -> List[Dict[str, Any]]:
        return self._reads.transitive_closure(
            table_name, predicate_uri, start_uri=start_uri, max_depth=max_depth
        )

    def symmetric_expand(
        self, table_name: str, predicate_uri: str
    ) -> List[Dict[str, Any]]:
        return self._reads.symmetric_expand(table_name, predicate_uri)

    def shortest_path(
        self,
        table_name: str,
        source_uri: str,
        target_uri: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        return self._reads.shortest_path(
            table_name, source_uri, target_uri, max_depth=max_depth
        )
