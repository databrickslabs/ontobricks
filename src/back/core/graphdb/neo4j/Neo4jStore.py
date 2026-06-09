"""Neo4j graph database backend.

Bolt-based (Cypher) flat-triple store. Triples are persisted as
``(:Triple {subject, predicate, object})`` nodes — a deliberately simple
schema chosen so PR 1 demonstrates the Cypher integration shape without
committing to a typed-node graph model (which lands in v2 / PR 3+).

PR 1 (this file) ships the connection management + flat-triple CRUD.
Named-query Cypher overrides (transitive closure, BFS, type distribution,
predicate distribution, etc.) are deliberately stubbed with safe
defaults — see ``# TODO(PR2)`` markers throughout. The app keeps working
on Neo4j; advanced features degrade gracefully until PR 2 lands.

``execute_query`` deliberately raises ``NotImplementedError`` — no raw
Cypher entry point. All writes go through ``insert_triples`` after
ontology validation in the build pipeline (Benoit's C2 safeguard:
"l'entrée se fait par l'ontologie").
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.logging import get_logger
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL
from shared.config.constants import DEFAULT_GRAPH_NAME

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
#  Guarded import — neo4j driver is an optional dependency.
# ---------------------------------------------------------------------------
try:
    import neo4j as _neo4j
except ImportError:
    _neo4j = None  # type: ignore[assignment]


DEFAULT_DATABASE = "neo4j"
DEFAULT_AUTH_METHOD = "basic"
SUPPORTED_AUTH_METHODS = ("basic", "databricks_secret")


class Neo4jStore(GraphDBBackend):
    """Neo4j (Bolt / Cypher) graph database backend — flat triple model.

    Parameters
    ----------
    db_name:
        Logical name for the triple set, used as the ``table`` label in the
        Cypher schema (every triple node carries ``:Triple:<db_name>``).
    engine_config:
        JSON dict from Settings > Graph DB > Engine Configuration. Keys:

        ``uri`` (required)
            Bolt URI, e.g. ``neo4j+s://b4810af7.databases.neo4j.io``.
        ``database`` (default ``"neo4j"``)
            Logical Neo4j database name on the target instance.
        ``auth_method`` (default ``"basic"``)
            ``"basic"`` → username + password. ``"databricks_secret"`` →
            credentials resolved from a Databricks secret scope (PR 2).
        ``username``, ``password``
            Required when ``auth_method == "basic"``.
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
        if _neo4j is None:
            raise ImportError(
                "neo4j is required for the Neo4j backend. "
                "Install it with: pip install 'neo4j>=5.0'"
            )
        self.db_name = db_name
        self.engine_config: Dict[str, Any] = engine_config or {}
        self._driver: Optional[Any] = None

        cfg = self.engine_config
        self._uri = str(cfg.get("uri") or "").strip()
        if not self._uri:
            raise ValueError(
                "Neo4jStore: engine_config['uri'] is required "
                "(e.g. 'neo4j+s://<aura-id>.databases.neo4j.io')"
            )
        self._database = str(cfg.get("database") or DEFAULT_DATABASE).strip() or DEFAULT_DATABASE
        self._auth_method = str(cfg.get("auth_method") or DEFAULT_AUTH_METHOD).strip()
        if self._auth_method not in SUPPORTED_AUTH_METHODS:
            raise ValueError(
                "Neo4jStore: unsupported auth_method %r (allowed: %s)"
                % (self._auth_method, ", ".join(SUPPORTED_AUTH_METHODS))
            )
        self._encrypted = bool(cfg.get("encrypted", True))

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
    #  GraphDBBackend — connection management
    # ======================================================================

    def get_connection(self) -> Any:
        """Return (lazily create) the Neo4j driver.

        Neo4j's Python driver itself is a thread-safe connection pool.
        Sessions are short-lived and created per-query in ``_run``.
        """
        if self._driver is not None:
            return self._driver
        auth = self._resolve_auth()
        kwargs: Dict[str, Any] = {"auth": auth}
        # neo4j+s:// embeds TLS — passing encrypted=True is rejected.
        if not self._uri.startswith(("neo4j+s://", "neo4j+ssc://", "bolt+s://", "bolt+ssc://")):
            kwargs["encrypted"] = self._encrypted
        self._driver = _neo4j.GraphDatabase.driver(self._uri, **kwargs)
        logger.info("Neo4j driver opened for %s (database=%s)", self._uri, self._database)
        return self._driver

    def close(self) -> None:
        if self._driver is not None:
            try:
                self._driver.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Neo4j driver close failed: %s", exc)
        self._driver = None
        logger.debug("Neo4j driver closed")

    def _resolve_auth(self) -> Tuple[str, str]:
        cfg = self.engine_config
        if self._auth_method == "basic":
            user = str(cfg.get("username") or "").strip()
            pwd = str(cfg.get("password") or "")
            if not user or not pwd:
                raise ValueError(
                    "Neo4jStore: auth_method=basic requires "
                    "engine_config['username'] and ['password']"
                )
            return (user, pwd)
        if self._auth_method == "databricks_secret":
            scope = str(cfg.get("secret_scope") or "").strip()
            key = str(cfg.get("secret_key") or "").strip()
            if not scope or not key:
                raise ValueError(
                    "Neo4jStore: auth_method=databricks_secret requires "
                    "engine_config['secret_scope'] and ['secret_key']"
                )
            # TODO(PR2): resolve via Databricks secrets API.
            # For PR 1 the secret_scope/key are validated but resolution
            # is deferred — basic auth is the only path tested live.
            raise NotImplementedError(
                "auth_method=databricks_secret is reserved for PR 2"
            )
        raise ValueError("Unsupported auth_method: %s" % self._auth_method)

    def _run(self, cypher: str, **params: Any) -> List[Dict[str, Any]]:
        """Execute a Cypher statement against the configured database.

        Returns rows as dicts. Wraps the session in a single transaction.
        """
        driver = self.get_connection()
        with driver.session(database=self._database) as session:
            result = session.run(cypher, **params)
            return [dict(record) for record in result]

    # ======================================================================
    #  GraphDBBackend — schema helpers
    # ======================================================================

    def get_node_table(self, table_name: str) -> str:
        # Neo4j labels are case-sensitive identifiers; we sanitise to a
        # safe Cypher identifier by replacing non-alphanumerics with '_'.
        return "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)

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
        # TODO(PR2): return SWRLFlatCypherTranslator(node_label=self.get_node_table(table_name))
        # PR 1 falls back to the SQL default — reasoning will not work
        # on Neo4j until PR 2 ships the Cypher translator. Documented.
        return super().get_query_translator(table_name)

    # ======================================================================
    #  TripleStoreBackend — core CRUD
    # ======================================================================

    def create_table(self, table_name: str) -> None:
        label = self.get_node_table(table_name)
        cypher = (
            f"CREATE CONSTRAINT triple_{label}_spo IF NOT EXISTS "
            f"FOR (t:Triple:{label}) "
            f"REQUIRE (t.subject, t.predicate, t.object) IS UNIQUE"
        )
        self._run(cypher)
        logger.info("Created Neo4j triple label: %s", label)

    def drop_table(self, table_name: str) -> None:
        label = self.get_node_table(table_name)
        self._run(f"DROP CONSTRAINT triple_{label}_spo IF EXISTS")
        self._run(f"MATCH (t:Triple:{label}) DETACH DELETE t")
        logger.info("Dropped Neo4j triple label: %s", label)

    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        if not triples:
            return 0
        label = self.get_node_table(table_name)
        total = 0
        cypher = (
            f"UNWIND $rows AS r "
            f"MERGE (t:Triple:{label} {{subject: r.subject, predicate: r.predicate, object: r.object}})"
        )
        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            rows = [
                {
                    "subject": t.get("subject", ""),
                    "predicate": t.get("predicate", ""),
                    "object": t.get("object", ""),
                }
                for t in batch
            ]
            self._run(cypher, rows=rows)
            total += len(batch)
            if on_progress:
                on_progress(total, len(triples))
        logger.info("Inserted %d triples into Neo4j label %s", total, label)
        return total

    def delete_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        if not triples:
            return 0
        label = self.get_node_table(table_name)
        deleted = 0
        cypher = (
            f"UNWIND $rows AS r "
            f"MATCH (t:Triple:{label} {{subject: r.subject, predicate: r.predicate, object: r.object}}) "
            f"DETACH DELETE t"
        )
        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            rows = [
                {
                    "subject": t.get("subject", ""),
                    "predicate": t.get("predicate", ""),
                    "object": t.get("object", ""),
                }
                for t in batch
            ]
            self._run(cypher, rows=rows)
            deleted += len(batch)
            if on_progress:
                on_progress(deleted, len(triples))
        logger.info("Deleted %d triples from Neo4j label %s", deleted, label)
        return deleted

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        label = self.get_node_table(table_name)
        cypher = (
            f"MATCH (t:Triple:{label}) "
            f"RETURN t.subject AS subject, t.predicate AS predicate, t.object AS object"
        )
        rows = self._run(cypher)
        return [
            {"subject": r["subject"], "predicate": r["predicate"], "object": r["object"]}
            for r in rows
        ]

    def count_triples(self, table_name: str) -> int:
        label = self.get_node_table(table_name)
        rows = self._run(f"MATCH (t:Triple:{label}) RETURN count(t) AS cnt")
        return int(rows[0]["cnt"]) if rows else 0

    def table_exists(self, table_name: str) -> bool:
        label = self.get_node_table(table_name)
        rows = self._run(
            "SHOW CONSTRAINTS YIELD name WHERE name = $cname RETURN name",
            cname=f"triple_{label}_spo",
        )
        return bool(rows)

    def get_status(self, table_name: str) -> Dict[str, Any]:
        return {
            "count": self.count_triples(table_name),
            "last_modified": None,
            "path": None,
            "format": "neo4j",
        }

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        # Deliberate: no raw Cypher entry point. All writes go through
        # the build pipeline after ontology validation (C2 safeguard).
        raise NotImplementedError(
            "Neo4jStore does not expose raw Cypher execution. "
            "Use the named query methods on TripleStoreBackend instead."
        )

    def optimize_table(self, table_name: str) -> None:
        # Neo4j has no manual VACUUM/OPTIMIZE; the indexer runs online.
        return None

    # ======================================================================
    #  Named query overrides — STUBBED in PR 1, Cypher impls in PR 2.
    #
    #  The inherited TripleStoreBackend defaults are SQL. They will raise
    #  at runtime against Neo4j (no execute_query). We override with safe
    #  empty-result returns so the app degrades gracefully on Neo4j until
    #  PR 2 lands. Every stub carries a `# TODO(PR2)` marker.
    # ======================================================================

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        # TODO(PR2): Cypher equivalent of TripleStoreBackend.get_aggregate_stats.
        return {
            "total": self.count_triples(table_name),
            "distinct_subjects": 0,
            "distinct_predicates": 0,
            "type_assertion_count": 0,
            "label_count": 0,
        }

    def get_type_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        # TODO(PR2): MATCH (t:Triple:<label>) WHERE t.predicate = $rdf_type
        # RETURN t.object AS type_uri, count(*) AS cnt ORDER BY cnt DESC.
        return []

    def get_predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        # TODO(PR2): MATCH (t:Triple:<label>) RETURN t.predicate, count(*) ORDER BY ...
        return []

    def find_subjects_by_type(
        self,
        table_name: str,
        type_uri: str,
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[str]:
        # TODO(PR2): MATCH (t:Triple:<label> {predicate: $rdf_type, object: $type_uri}) ...
        return []

    def resolve_subject_by_id(
        self, table_name: str, type_uri: str, id_fragment: str
    ) -> Optional[str]:
        # TODO(PR2)
        return None

    def get_entity_metadata(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        # TODO(PR2)
        return []

    def get_triples_for_subjects(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        # TODO(PR2)
        return []

    def get_predicates_for_type(self, table_name: str, type_uri: str) -> List[str]:
        # TODO(PR2)
        return []

    def paginated_triples(
        self,
        table_name: str,
        conditions: List[str],
        limit: int,
        offset: int,
    ) -> List[Dict[str, str]]:
        # TODO(PR2): conditions are SQL fragments from the caller — needs
        # a structured condition format for Cypher translation.
        return []

    def paginated_count(self, table_name: str, conditions: List[str]) -> int:
        # TODO(PR2)
        return 0

    def bfs_traversal(
        self,
        table_name: str,
        seed_where: str,
        depth: int,
        search: str = "",
        entity_type: str = "",
    ) -> List[Dict[str, Any]]:
        # TODO(PR2): Cypher pattern with variable-length path on Triple nodes.
        return []

    def find_seed_subjects(
        self,
        table_name: str,
        entity_type: str = "",
        field: str = "any",
        match_type: str = "contains",
        value: str = "",
        limit: int = 0,
    ) -> Set[str]:
        # TODO(PR2)
        return set()

    def find_subjects_by_patterns(
        self, table_name: str, like_patterns: List[str]
    ) -> Set[str]:
        # TODO(PR2)
        return set()

    def transitive_closure(
        self,
        table_name: str,
        predicate_uri: str,
        start_uri: Optional[str] = None,
        max_depth: int = 20,
    ) -> List[Dict[str, Any]]:
        # TODO(PR2): MATCH path = (a:Triple:<label>)-[*..N]-(b:Triple:<label>) ...
        return []

    def symmetric_expand(
        self, table_name: str, predicate_uri: str
    ) -> List[Dict[str, Any]]:
        # TODO(PR2)
        return []

    def shortest_path(
        self,
        table_name: str,
        source_uri: str,
        target_uri: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        # TODO(PR2): MATCH path = shortestPath(...). Native Cypher win.
        return []

    def expand_entity_neighbors(
        self, table_name: str, entity_uris: Set[str]
    ) -> Set[str]:
        # TODO(PR2)
        return set()

    def delete_cohort_triples(
        self,
        table_name: str,
        cohort_uri_prefix: str,
        in_cohort_predicate: str,
    ) -> int:
        # TODO(PR2)
        return 0
