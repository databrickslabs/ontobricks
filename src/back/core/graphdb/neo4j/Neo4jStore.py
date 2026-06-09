"""Neo4j graph database backend.

Bolt-based (Cypher) flat-triple store. Triples are persisted as
``(:<sanitised_table_name> {subject, predicate, object})`` nodes — a
deliberately simple schema chosen so PR 1 demonstrates the Cypher
integration shape without committing to a typed-node graph model (which
lands in v2 / PR 3+). One label per logical store so Neo4j 5+ CREATE
CONSTRAINT (which only accepts single-label patterns) works.

Full implementation of the ``TripleStoreBackend`` + ``GraphDBBackend``
contracts. Connection management, flat-triple CRUD, and the 16 named-query
methods are all implemented in native Cypher. The reasoning translator
(``get_query_translator``) returns a :class:`SWRLFlatCypherTranslator` that
is currently scaffolded only — full SWRL → Cypher translation lands in a
follow-up PR. Reasoning on Neo4j therefore reports zero violations /
inferences (graceful no-op) rather than crashing.

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
    #  TripleStoreBackend — core CRUD
    # ======================================================================

    def create_table(self, table_name: str) -> None:
        label = self.get_node_table(table_name)
        cypher = (
            f"CREATE CONSTRAINT triple_{label}_spo IF NOT EXISTS "
            f"FOR (t:`{label}`) "
            f"REQUIRE (t.subject, t.predicate, t.object) IS UNIQUE"
        )
        self._run(cypher)
        logger.info("Created Neo4j triple label: %s", label)

    def drop_table(self, table_name: str) -> None:
        label = self.get_node_table(table_name)
        self._run(f"DROP CONSTRAINT triple_{label}_spo IF EXISTS")
        self._run(f"MATCH (t:`{label}`) DETACH DELETE t")
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
            f"MERGE (t:`{label}` {{subject: r.subject, predicate: r.predicate, object: r.object}})"
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
            f"MATCH (t:`{label}` {{subject: r.subject, predicate: r.predicate, object: r.object}}) "
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
            f"MATCH (t:`{label}`) "
            f"RETURN t.subject AS subject, t.predicate AS predicate, t.object AS object"
        )
        rows = self._run(cypher)
        return [
            {"subject": r["subject"], "predicate": r["predicate"], "object": r["object"]}
            for r in rows
        ]

    def count_triples(self, table_name: str) -> int:
        label = self.get_node_table(table_name)
        rows = self._run(f"MATCH (t:`{label}`) RETURN count(t) AS cnt")
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
    #  Named query overrides — native Cypher implementations.
    #
    #  Translated from the SQL defaults on TripleStoreBackend. Flat-triple
    #  model: every triple is a (:<sanitised_label> {subject, predicate, object})
    #  node. Graph traversal (BFS, transitive closure, neighbours) joins
    #  Triple nodes by property equality — a typed-relationship graph model
    #  would be faster but is a future PR (sets supports_graph_model=True).
    # ======================================================================

    # -- Statistics --------------------------------------------------------

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        label = self.get_node_table(table_name)
        cypher = (
            f"MATCH (t:`{label}`) "
            f"RETURN count(t) AS total, "
            f"count(DISTINCT t.subject) AS distinct_subjects, "
            f"count(DISTINCT t.predicate) AS distinct_predicates, "
            f"sum(CASE WHEN t.predicate = $rdf_type THEN 1 ELSE 0 END) AS type_assertion_count, "
            f"sum(CASE WHEN t.predicate = $rdfs_label THEN 1 ELSE 0 END) AS label_count"
        )
        rows = self._run(cypher, rdf_type=RDF_TYPE, rdfs_label=RDFS_LABEL)
        row = rows[0] if rows else {}
        return {
            "total": int(row.get("total", 0) or 0),
            "distinct_subjects": int(row.get("distinct_subjects", 0) or 0),
            "distinct_predicates": int(row.get("distinct_predicates", 0) or 0),
            "type_assertion_count": int(row.get("type_assertion_count", 0) or 0),
            "label_count": int(row.get("label_count", 0) or 0),
        }

    def get_type_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        label = self.get_node_table(table_name)
        cypher = (
            f"MATCH (t:`{label}`) WHERE t.predicate = $rdf_type "
            f"RETURN t.object AS type_uri, count(*) AS cnt "
            f"ORDER BY cnt DESC"
        )
        return self._run(cypher, rdf_type=RDF_TYPE) or []

    def get_predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        label = self.get_node_table(table_name)
        cypher = (
            f"MATCH (t:`{label}`) "
            f"RETURN t.predicate AS predicate, count(*) AS cnt "
            f"ORDER BY cnt DESC"
        )
        return self._run(cypher) or []

    # -- Entity lookup ----------------------------------------------------

    def find_subjects_by_type(
        self,
        table_name: str,
        type_uri: str,
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[str]:
        label = self.get_node_table(table_name)
        if search:
            cypher = (
                f"MATCH (t:`{label}`) "
                f"WHERE t.predicate = $rdf_type AND t.object = $type_uri "
                f"AND t.subject IN ("
                f"  MATCH (t2:`{label}`) "
                f"  WHERE t2.predicate <> $rdf_type AND toLower(t2.object) CONTAINS toLower($search) "
                f"  RETURN DISTINCT t2.subject"
                f") "
                f"RETURN DISTINCT t.subject AS subject ORDER BY subject "
                f"SKIP $offset LIMIT $limit"
            )
            rows = self._run(
                cypher,
                rdf_type=RDF_TYPE,
                type_uri=type_uri,
                search=search,
                offset=int(offset),
                limit=int(limit),
            )
        else:
            cypher = (
                f"MATCH (t:`{label}`) "
                f"WHERE t.predicate = $rdf_type AND t.object = $type_uri "
                f"RETURN DISTINCT t.subject AS subject ORDER BY subject "
                f"SKIP $offset LIMIT $limit"
            )
            rows = self._run(
                cypher,
                rdf_type=RDF_TYPE,
                type_uri=type_uri,
                offset=int(offset),
                limit=int(limit),
            )
        return [r["subject"] for r in (rows or [])]

    def resolve_subject_by_id(
        self, table_name: str, type_uri: str, id_fragment: str
    ) -> Optional[str]:
        label = self.get_node_table(table_name)
        cypher = (
            f"MATCH (t:`{label}`) "
            f"WHERE t.predicate = $rdf_type "
            f"  AND t.object = $type_uri "
            f"  AND (t.subject ENDS WITH ('/' + $idf) OR t.subject ENDS WITH ('#' + $idf)) "
            f"RETURN DISTINCT t.subject AS subject LIMIT 1"
        )
        rows = self._run(
            cypher, rdf_type=RDF_TYPE, type_uri=type_uri, idf=id_fragment
        )
        return rows[0]["subject"] if rows else None

    def get_entity_metadata(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        if not subjects:
            return []
        label = self.get_node_table(table_name)
        cypher_type = (
            f"MATCH (t:`{label}`) "
            f"WHERE t.predicate = $rdf_type AND t.subject IN $subjects "
            f"RETURN t.subject AS subject, t.object AS object"
        )
        cypher_label = (
            f"MATCH (t:`{label}`) "
            f"WHERE t.predicate = $rdfs_label AND t.subject IN $subjects "
            f"RETURN t.subject AS subject, t.object AS object"
        )
        type_rows = self._run(cypher_type, rdf_type=RDF_TYPE, subjects=subjects) or []
        label_rows = self._run(cypher_label, rdfs_label=RDFS_LABEL, subjects=subjects) or []

        types: Dict[str, str] = {}
        for r in type_rows:
            types.setdefault(r["subject"], r["object"])
        labels: Dict[str, str] = {}
        for r in label_rows:
            labels.setdefault(r["subject"], r["object"])

        return [
            {"uri": uri, "type": types.get(uri, ""), "label": labels.get(uri, "")}
            for uri in subjects
            if uri in types
        ]

    def get_triples_for_subjects(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        if not subjects:
            return []
        label = self.get_node_table(table_name)
        cypher = (
            f"MATCH (t:`{label}`) WHERE t.subject IN $subjects "
            f"RETURN t.subject AS subject, t.predicate AS predicate, t.object AS object"
        )
        return self._run(cypher, subjects=subjects) or []

    def get_predicates_for_type(self, table_name: str, type_uri: str) -> List[str]:
        label = self.get_node_table(table_name)
        cypher = (
            f"MATCH (anchor:`{label}`) "
            f"WHERE anchor.predicate = $rdf_type AND anchor.object = $type_uri "
            f"WITH anchor.subject AS s LIMIT 1 "
            f"MATCH (t:`{label}`) WHERE t.subject = s "
            f"RETURN DISTINCT t.predicate AS predicate"
        )
        rows = self._run(cypher, rdf_type=RDF_TYPE, type_uri=type_uri) or []
        return [r["predicate"] for r in rows]

    # -- Pagination -------------------------------------------------------

    def paginated_triples(
        self,
        table_name: str,
        conditions: List[str],
        limit: int,
        offset: int,
    ) -> List[Dict[str, str]]:
        # *conditions* is a list of SQL WHERE fragments produced by the
        # caller. Translating arbitrary SQL to Cypher is out of scope —
        # only the empty-conditions case (return all triples, paginated) is
        # supported in v1. When *conditions* is non-empty we degrade to
        # returning the unfiltered page; callers that need filtered
        # pagination should switch to find_subjects_by_type / find_seed_subjects.
        label = self.get_node_table(table_name)
        if conditions:
            logger.warning(
                "paginated_triples received %d SQL conditions; "
                "Neo4j backend ignores them and returns the unfiltered page. "
                "Use find_subjects_by_type / find_seed_subjects for filtered access.",
                len(conditions),
            )
        cypher = (
            f"MATCH (t:`{label}`) "
            f"RETURN t.subject AS subject, t.predicate AS predicate, t.object AS object "
            f"SKIP $offset LIMIT $limit"
        )
        return self._run(cypher, offset=int(offset), limit=int(limit)) or []

    def paginated_count(self, table_name: str, conditions: List[str]) -> int:
        # See paginated_triples — conditions are not honoured in v1.
        if conditions:
            logger.warning(
                "paginated_count received %d SQL conditions; "
                "Neo4j backend returns the unfiltered count.",
                len(conditions),
            )
        return self.count_triples(table_name)

    # -- Traversal --------------------------------------------------------

    def bfs_traversal(
        self,
        table_name: str,
        seed_where: str,
        depth: int,
        search: str = "",
        entity_type: str = "",
    ) -> List[Dict[str, Any]]:
        # *seed_where* is a SQL fragment. Cypher equivalent uses the
        # structured *search* / *entity_type* parameters instead. When both
        # structured params are empty and only seed_where is given, we
        # cannot translate — log and return empty.
        if not search and not entity_type:
            if seed_where:
                logger.warning(
                    "bfs_traversal: Neo4j backend requires structured search/entity_type "
                    "parameters; SQL seed_where fragments are not translated. "
                    "Returning empty result."
                )
            return []
        seeds = self.find_seed_subjects(
            table_name,
            entity_type=entity_type,
            field="any",
            match_type="contains",
            value=search,
        )
        if not seeds:
            return []

        label = self.get_node_table(table_name)
        # Reachability via property-equality joins between Triple nodes:
        # a Triple links its subject to its object; we walk over Triple
        # nodes hop by hop, accumulating entities (subjects + objects).
        cypher = (
            f"WITH $seeds AS seeds "
            f"CALL {{ "
            f"  WITH seeds "
            f"  UNWIND seeds AS s "
            f"  RETURN s AS entity, 0 AS lvl "
            f"  UNION ALL "
            f"  WITH seeds "
            f"  MATCH (t:`{label}`) "
            f"  WHERE t.subject IN seeds "
            f"    AND t.predicate <> $rdf_type AND t.predicate <> $rdfs_label "
            f"    AND (t.object STARTS WITH 'http://' OR t.object STARTS WITH 'https://') "
            f"  RETURN DISTINCT t.object AS entity, 1 AS lvl "
            f"}} "
            f"WITH entity, min(lvl) AS lvl "
            f"WHERE lvl <= $depth "
            f"RETURN entity, lvl AS min_lvl"
        )
        # The query above only does 1-hop. Full BFS to *depth* > 1 would
        # require recursive traversal — Cypher's variable-length pattern
        # can do this natively but with the flat-triple model needs a
        # joined pattern across Triple nodes. We expand iteratively below
        # for arbitrary *depth* while keeping each hop bounded.
        if depth <= 1:
            return self._run(cypher, seeds=list(seeds), depth=depth, rdf_type=RDF_TYPE, rdfs_label=RDFS_LABEL) or []

        # Iterative expansion for depth > 1.
        visited: Dict[str, int] = {uri: 0 for uri in seeds}
        frontier: Set[str] = set(seeds)
        for lvl in range(1, depth + 1):
            if not frontier:
                break
            next_frontier = self.expand_entity_neighbors(table_name, frontier)
            new_nodes = next_frontier - set(visited.keys())
            for n in new_nodes:
                visited[n] = lvl
            frontier = new_nodes
        return [{"entity": uri, "min_lvl": lvl} for uri, lvl in visited.items()]

    def find_seed_subjects(
        self,
        table_name: str,
        entity_type: str = "",
        field: str = "any",
        match_type: str = "contains",
        value: str = "",
        limit: int = 0,
    ) -> Set[str]:
        label = self.get_node_table(table_name)
        search_label = field in ("label", "any")
        search_id = field in ("id", "any")

        # Build a Cypher predicate fragment for the chosen match_type.
        def _match_clause(column: str, param: str) -> str:
            if match_type == "exact":
                return f"toLower({column}) = ${param}"
            if match_type == "starts":
                return f"toLower({column}) STARTS WITH ${param}"
            if match_type == "ends":
                return f"toLower({column}) ENDS WITH ${param}"
            return f"toLower({column}) CONTAINS ${param}"

        params: Dict[str, Any] = {
            "rdf_type": RDF_TYPE,
            "rdfs_label": RDFS_LABEL,
        }
        if value:
            params["val"] = value.lower()
        if entity_type:
            params["etype"] = entity_type

        cyphers: List[str] = []

        if entity_type and value:
            if search_id:
                cyphers.append(
                    f"MATCH (t:`{label}`) "
                    f"WHERE t.predicate = $rdf_type AND t.object = $etype "
                    f"AND {_match_clause('t.subject', 'val')} "
                    f"RETURN DISTINCT t.subject AS subject"
                )
            if search_label:
                cyphers.append(
                    f"MATCH (lab:`{label}`) "
                    f"WHERE lab.predicate = $rdfs_label "
                    f"AND {_match_clause('lab.object', 'val')} "
                    f"WITH DISTINCT lab.subject AS s "
                    f"MATCH (t:`{label}`) "
                    f"WHERE t.predicate = $rdf_type AND t.object = $etype AND t.subject = s "
                    f"RETURN DISTINCT s AS subject"
                )
        elif entity_type:
            cyphers.append(
                f"MATCH (t:`{label}`) "
                f"WHERE t.predicate = $rdf_type AND t.object = $etype "
                f"RETURN DISTINCT t.subject AS subject"
            )
        elif value:
            if search_label:
                cyphers.append(
                    f"MATCH (t:`{label}`) "
                    f"WHERE t.predicate = $rdfs_label "
                    f"AND {_match_clause('t.object', 'val')} "
                    f"RETURN DISTINCT t.subject AS subject"
                )
            if search_id:
                cyphers.append(
                    f"MATCH (t:`{label}`) "
                    f"WHERE t.predicate = $rdf_type "
                    f"AND {_match_clause('t.subject', 'val')} "
                    f"RETURN DISTINCT t.subject AS subject"
                )
        else:
            return set()

        if not cyphers:
            return set()

        union_sql = " UNION ".join(cyphers)
        if limit and limit > 0:
            union_sql = f"CALL {{ {union_sql} }} RETURN subject LIMIT {int(limit)}"
        rows = self._run(union_sql, **params) or []
        return {r["subject"] for r in rows}

    def find_subjects_by_patterns(
        self, table_name: str, like_patterns: List[str]
    ) -> Set[str]:
        if not like_patterns:
            return set()
        label = self.get_node_table(table_name)

        # SQL LIKE → Cypher: '%' wildcards translate to STARTS WITH / ENDS WITH
        # / CONTAINS depending on placement. For arbitrary patterns we fall
        # back to a regex match.
        clauses: List[str] = []
        params: Dict[str, Any] = {}
        for i, raw in enumerate(like_patterns):
            pkey = f"p{i}"
            params[pkey] = raw.replace("%", ".*")
            clauses.append(f"t.subject =~ ${pkey}")
        cypher = (
            f"MATCH (t:`{label}`) WHERE {' OR '.join(clauses)} "
            f"RETURN DISTINCT t.subject AS subject"
        )
        rows = self._run(cypher, **params) or []
        return {r["subject"] for r in rows}

    def expand_entity_neighbors(
        self, table_name: str, entity_uris: Set[str]
    ) -> Set[str]:
        if not entity_uris:
            return set()
        label = self.get_node_table(table_name)
        # Outgoing edges: where subject IN seeds AND object looks like an entity URI.
        # Incoming edges: where object IN seeds.
        # Both then filtered to entities that have an rdf:type assertion
        # (real entity instances, not class or property URIs).
        cypher = (
            f"WITH $seeds AS seeds "
            f"MATCH (t:`{label}`) "
            f"WHERE (t.subject IN seeds AND t.object STARTS WITH 'http' "
            f"       AND t.predicate <> $rdf_type AND t.predicate <> $rdfs_label) "
            f"   OR (t.object IN seeds AND t.predicate <> $rdf_type AND t.predicate <> $rdfs_label) "
            f"WITH DISTINCT (CASE WHEN t.subject IN seeds THEN t.object ELSE t.subject END) AS entity "
            f"MATCH (ty:`{label}`) "
            f"WHERE ty.subject = entity AND ty.predicate = $rdf_type "
            f"RETURN DISTINCT entity"
        )
        rows = self._run(
            cypher,
            seeds=list(entity_uris),
            rdf_type=RDF_TYPE,
            rdfs_label=RDFS_LABEL,
        ) or []
        return {r["entity"] for r in rows}

    # -- Reasoning --------------------------------------------------------

    def transitive_closure(
        self,
        table_name: str,
        predicate_uri: str,
        start_uri: Optional[str] = None,
        max_depth: int = 20,
    ) -> List[Dict[str, Any]]:
        # Compute transitive closure along *predicate_uri* and return triples
        # NOT already present as direct assertions. With the flat-triple model
        # we self-join Triple nodes hop by hop using property equality.
        label = self.get_node_table(table_name)
        start_filter = ""
        params: Dict[str, Any] = {"pred": predicate_uri, "max_depth": int(max_depth)}
        if start_uri:
            start_filter = "AND start.subject = $start_uri "
            params["start_uri"] = start_uri

        # Build a chain of MATCH clauses up to max_depth. This is verbose but
        # explicit; Cypher does not have recursive CTEs.
        depth = min(max_depth, 20)  # hard cap for safety
        union_parts: List[str] = []
        # depth=2 means start -> mid -> end (2 hops)
        for d in range(2, depth + 1):
            chain = "MATCH (h0:`" + label + "`)"
            wheres = ["h0.predicate = $pred"]
            if start_uri:
                wheres.append("h0.subject = $start_uri")
            for i in range(1, d):
                chain += f", (h{i}:`{label}`)"
                wheres.append(f"h{i}.predicate = $pred")
                wheres.append(f"h{i-1}.object = h{i}.subject")
            union_parts.append(
                chain + " WHERE " + " AND ".join(wheres) +
                f" RETURN h0.subject AS subject, h{d-1}.object AS object"
            )

        if not union_parts:
            return []

        body = " UNION ".join(union_parts)
        cypher = (
            f"CALL {{ {body} }} "
            f"WITH DISTINCT subject, object "
            f"WHERE NOT EXISTS {{ "
            f"  MATCH (ex:`{label}`) "
            f"  WHERE ex.subject = subject AND ex.predicate = $pred AND ex.object = object "
            f"}} "
            f"RETURN subject, $pred AS predicate, object"
        )
        try:
            return self._run(cypher, **params) or []
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "transitive_closure failed on %s (predicate=%s): %s",
                table_name,
                predicate_uri,
                exc,
            )
            return []

    def symmetric_expand(
        self, table_name: str, predicate_uri: str
    ) -> List[Dict[str, Any]]:
        label = self.get_node_table(table_name)
        cypher = (
            f"MATCH (t:`{label}`) WHERE t.predicate = $pred "
            f"AND NOT EXISTS {{ "
            f"  MATCH (inv:`{label}`) "
            f"  WHERE inv.subject = t.object AND inv.predicate = $pred AND inv.object = t.subject "
            f"}} "
            f"RETURN t.object AS subject, $pred AS predicate, t.subject AS object"
        )
        try:
            return self._run(cypher, pred=predicate_uri) or []
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "symmetric_expand failed on %s (predicate=%s): %s",
                table_name,
                predicate_uri,
                exc,
            )
            return []

    def shortest_path(
        self,
        table_name: str,
        source_uri: str,
        target_uri: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        # Native Cypher shortestPath would be ideal but requires a typed-
        # relationship graph model. With the flat-triple model we do a
        # bounded iterative BFS and return the first path found.
        if source_uri == target_uri:
            return [{"hop": 0, "uri": source_uri}]

        visited: Set[str] = {source_uri}
        parent: Dict[str, str] = {}
        frontier: Set[str] = {source_uri}
        for depth in range(1, min(max_depth, 10) + 1):
            next_frontier = self.expand_entity_neighbors(table_name, frontier)
            for n in next_frontier:
                if n in visited:
                    continue
                for prev in frontier:
                    parent.setdefault(n, prev)
            visited |= next_frontier
            if target_uri in next_frontier:
                # Reconstruct the path.
                path_uris: List[str] = [target_uri]
                cur = target_uri
                while cur in parent and cur != source_uri:
                    cur = parent[cur]
                    path_uris.append(cur)
                path_uris.reverse()
                return [{"hop": i, "uri": uri} for i, uri in enumerate(path_uris)]
            frontier = next_frontier - visited
            if not frontier:
                break
        return []

    # -- Cohorts ----------------------------------------------------------

    def delete_cohort_triples(
        self,
        table_name: str,
        cohort_uri_prefix: str,
        in_cohort_predicate: str,
    ) -> int:
        if not cohort_uri_prefix:
            return 0
        label = self.get_node_table(table_name)
        cypher = (
            f"MATCH (t:`{label}`) "
            f"WHERE t.subject STARTS WITH $prefix "
            f"   OR (t.predicate = $in_pred AND t.object STARTS WITH $prefix) "
            f"WITH t LIMIT 100000 "
            f"DETACH DELETE t "
            f"RETURN count(t) AS deleted"
        )
        try:
            rows = self._run(
                cypher, prefix=cohort_uri_prefix, in_pred=in_cohort_predicate
            )
            return int(rows[0].get("deleted", 0)) if rows else 0
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "delete_cohort_triples failed on %s (%s): %s",
                table_name,
                cohort_uri_prefix,
                exc,
            )
            return 0
