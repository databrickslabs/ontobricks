"""Digital twin domain: SPARQL/R2RML pipeline, triple-store helpers, registry resolution."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL
from back.objects.digitaltwin.models import DomainSnapshot
from back.objects.digitaltwin.cache.TwinStoreCache import (
    _TS_STATS_CACHE_TTL_SECONDS as _TS_STATS_CACHE_TTL_SECONDS,
)
from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks
from back.objects.digitaltwin.quality.QualitySqlBuilder import QualitySqlBuilder
from back.objects.digitaltwin.TwinBackgroundTasks import TwinBackgroundTasks
from back.objects.digitaltwin.query.TwinMapping import TwinMapping
from back.objects.digitaltwin.TwinAnalytics import TwinAnalytics
from back.objects.digitaltwin.TwinResolve import TwinResolve
from back.objects.session import get_domain  # tests patch DigitalTwin.get_domain


class DigitalTwin:
    """Centralizes digital-twin query pipeline, data quality, and API resolution helpers.

    Constructed with a domain session (``DomainSession`` or snapshot) for instance
    methods that need domain state.  Pure transforms and background-thread
    runners are exposed as ``@staticmethod``.
    """

    RDF_TYPE = RDF_TYPE
    RDFS_LABEL = RDFS_LABEL
    DomainSnapshot = DomainSnapshot
    _SQL_ERROR_MAX_CHARS = SqlQualityChecks._SQL_ERROR_MAX_CHARS

    def __init__(self, domain) -> None:
        self._domain = domain

    # ------------------------------------------------------------------
    # Private helpers (static)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_base_uri(uri: str) -> str:
        return TwinMapping._normalize_base_uri(uri)

    @staticmethod
    def _safe_class_label(class_label: str, class_uri: str) -> str:
        return TwinMapping._safe_class_label(class_label, class_uri)

    _SELECT_CLAUSE_RE = TwinMapping._SELECT_CLAUSE_RE
    _ALIAS_RE = TwinMapping._ALIAS_RE

    @staticmethod
    def _extract_select_columns(sql_query: str) -> Set[str] | None:
        return TwinMapping._extract_select_columns(sql_query)

    # ------------------------------------------------------------------
    # VIEW error diagnostics
    # ------------------------------------------------------------------

    @staticmethod
    def diagnose_view_error(
        error_msg: str,
        entity_mappings: Dict[str, Any],
        relationship_mappings: list | None = None,
    ) -> str:
        return TwinMapping.diagnose_view_error(
            error_msg, entity_mappings, relationship_mappings
        )

    # ------------------------------------------------------------------
    # R2RML mapping augmentation (static -- pure transforms)
    # ------------------------------------------------------------------

    @staticmethod
    def augment_mappings_from_config(
        entity_mappings, mapping_config, base_uri, ontology_config=None
    ):
        return TwinMapping.augment_mappings_from_config(
            entity_mappings, mapping_config, base_uri, ontology_config
        )

    @staticmethod
    def augment_relationships_from_config(
        relationship_mappings, mapping_config, base_uri, ontology_config=None
    ):
        return TwinMapping.augment_relationships_from_config(
            relationship_mappings, mapping_config, base_uri, ontology_config
        )

    # ------------------------------------------------------------------
    # Triplestore cache (instance methods -- use self._domain)
    # ------------------------------------------------------------------

    def get_ts_cache(self, section: str) -> Optional[dict]:
        from back.objects.digitaltwin.cache.TwinStoreCache import TwinStoreCache

        return TwinStoreCache(self._domain).get_ts_cache(section)


    def set_ts_cache(self, section: str, data: dict):
        from back.objects.digitaltwin.cache.TwinStoreCache import TwinStoreCache

        return TwinStoreCache(self._domain).set_ts_cache(section, data)


    def clear_ts_cache(self, section: str) -> None:
        from back.objects.digitaltwin.cache.TwinStoreCache import TwinStoreCache

        return TwinStoreCache(self._domain).clear_ts_cache(section)


    async def get_or_fetch_graph_status(
        self, settings, force_refresh: bool = False
    ) -> Dict[str, Any]:
        from back.objects.digitaltwin.cache.TwinStoreCache import TwinStoreCache

        return await TwinStoreCache(self._domain).get_or_fetch_graph_status(settings, force_refresh)


    async def get_or_fetch_dt_existence(
        self, settings, force_refresh: bool = False
    ) -> Dict[str, Any]:
        from back.objects.digitaltwin.cache.TwinStoreCache import TwinStoreCache

        return await TwinStoreCache(self._domain).get_or_fetch_dt_existence(settings, force_refresh)


    def pending_dt_existence(self, settings) -> Dict[str, Any]:
        from back.objects.digitaltwin.cache.TwinStoreCache import TwinStoreCache

        return TwinStoreCache(self._domain).pending_dt_existence(settings)


    # ------------------------------------------------------------------
    # Schedule sync (instance method)
    # ------------------------------------------------------------------

    def sync_last_build_from_schedule(self, settings) -> None:
        from back.objects.digitaltwin.cache.TwinStoreCache import TwinStoreCache

        return TwinStoreCache(self._domain).sync_last_build_from_schedule(settings)


    # ------------------------------------------------------------------
    # Live Knowledge Graph status (instance methods)
    # ------------------------------------------------------------------

    async def fetch_graph_triplestore_status(self, settings) -> Dict[str, Any]:
        from back.objects.digitaltwin.cache.TwinStoreCache import TwinStoreCache

        return await TwinStoreCache(self._domain).fetch_graph_triplestore_status(settings)


    @staticmethod
    def resolve_graph_engine(domain: Any, settings: Any) -> str:
        """Return the graph DB engine resolved from the per-domain backend choice.

        The selection lives in ``DomainSession.info['graph_backend']`` (Domain
        → General → Knowledge Graph tab) and maps to a concrete engine
        (``lakebase`` or ``neo4j``) via
        :class:`back.core.graphdb.GraphDBFactory`.
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        return GraphDBFactory._resolve_graph_engine(domain, settings) or "lakebase"

    # ------------------------------------------------------------------
    # SPARQL execution pipeline (instance method)
    # ------------------------------------------------------------------

    async def execute_spark_query(
        self,
        sparql_query: str,
        r2rml_content: str,
        limit: int,
        settings,
    ) -> Dict[str, Any]:
        return await TwinMapping(self._domain).execute_spark_query(
            sparql_query, r2rml_content, limit, settings
        )

    # ------------------------------------------------------------------
    # Triplestore stats (instance method)
    # ------------------------------------------------------------------

    def classify_predicates(self, top_predicates: list) -> list:
        from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks

        return SqlQualityChecks(self._domain).classify_predicates(top_predicates)


    # ------------------------------------------------------------------
    # Backend label (instance method)
    # ------------------------------------------------------------------

    def effective_backend_label(self) -> str:
        return TwinResolve(self._domain).effective_backend_label()

    # ------------------------------------------------------------------
    # Data quality: private helpers (static)
    # ------------------------------------------------------------------

    @staticmethod
    def _count_class_population_sql(
        store, table: str, class_uri: str, cache: dict = None
    ) -> Optional[int]:
        from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks

        return SqlQualityChecks._count_class_population_sql(store, table, class_uri, cache)


    @staticmethod
    def _enrich_with_population(result: dict, total_population: Optional[int]) -> dict:
        from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks

        return SqlQualityChecks._enrich_with_population(result, total_population)


    @staticmethod
    def _sql_error_detail(exc: Exception) -> str:
        from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks

        return SqlQualityChecks._sql_error_detail(exc)


    @staticmethod
    def _failed_check_result(
        name: str, category: str, check_id, sql: str, exc: Exception
    ) -> dict:
        from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks

        return SqlQualityChecks._failed_check_result(name, category, check_id, sql, exc)


    @staticmethod
    def _rule_check_id(prefix: str, rule: dict, index: int) -> str:
        from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks

        return SqlQualityChecks._rule_check_id(prefix, rule, index)


    # ------------------------------------------------------------------
    # Data quality: SQL checks (static -- runs in background thread)
    # ------------------------------------------------------------------

    @staticmethod
    def run_sql_checks(
        tm,
        task,
        shapes,
        triplestore_table,
        store,
        t0,
        total,
        swrl_rules=None,
        ontology=None,
        decision_tables=None,
        aggregate_rules=None,
        violation_limit=None,
        parallel=False,
    ):
        from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks

        return SqlQualityChecks.run_sql_checks(
            tm,
            task,
            shapes,
            triplestore_table,
            store,
            t0,
            total,
            swrl_rules,
            ontology,
            decision_tables,
            aggregate_rules,
            violation_limit,
            parallel=parallel,
        )


    # ------------------------------------------------------------------
    # Data quality: task completion (static)
    # ------------------------------------------------------------------

    @staticmethod
    def complete_dq_task(tm, task, results, duration):
        return TwinBackgroundTasks.complete_dq_task(tm, task, results, duration)

    # ------------------------------------------------------------------
    # Background task orchestration (routers stay thin)
    # ------------------------------------------------------------------

    @staticmethod
    def run_build_task(
        tm,
        task_id: str,
        domain,
        settings,
        domain_snap: DomainSnapshot,
        host: str,
        token: str,
        warehouse_id: str,
        view_table: str,
        graph_name: str,
        r2rml_content: str,
        base_uri: str,
        mapping_config,
        ontology_config,
        delta_cfg: dict,
        *,
        build_kind: str = "session",
    ) -> None:
        return TwinBackgroundTasks.run_build_task(
            tm,
            task_id,
            domain,
            settings,
            domain_snap,
            host,
            token,
            warehouse_id,
            view_table,
            graph_name,
            r2rml_content,
            base_uri,
            mapping_config,
            ontology_config,
            delta_cfg,
            build_kind=build_kind,
        )

    @staticmethod
    def run_adjacency_refresh_task(
        tm,
        task_id: str,
        settings,
        domain_snap: DomainSnapshot,
        *,
        backend: str,
    ) -> None:
        return TwinBackgroundTasks.run_adjacency_refresh_task(
            tm, task_id, settings, domain_snap, backend=backend
        )

    @staticmethod
    def run_data_quality_task(
        tm,
        task_id: str,
        settings,
        domain_snap: DomainSnapshot,
        shapes: list,
        triplestore_table: str,
        total: int,
        *,
        swrl_rules=None,
        ontology_dict=None,
        decision_tables=None,
        aggregate_rules=None,
        violation_limit=None,
        failure_message: str = "Data quality checks failed",
        use_exception_message_on_failure: bool = False,
    ) -> None:
        return TwinBackgroundTasks.run_data_quality_task(
            tm,
            task_id,
            settings,
            domain_snap,
            shapes,
            triplestore_table,
            total,
            swrl_rules=swrl_rules,
            ontology_dict=ontology_dict,
            decision_tables=decision_tables,
            aggregate_rules=aggregate_rules,
            violation_limit=violation_limit,
            failure_message=failure_message,
            use_exception_message_on_failure=use_exception_message_on_failure,
        )

    @staticmethod
    def run_node_shacl_task(
        tm,
        task_id: str,
        settings,
        domain_snap: DomainSnapshot,
        entity_uri: str,
        triplestore_table: str,
    ) -> None:
        return TwinBackgroundTasks.run_node_shacl_task(
            tm, task_id, settings, domain_snap, entity_uri, triplestore_table
        )

    @staticmethod
    def run_metrics_task(
        tm,
        task_id: str,
        domain,
        settings,
        graph_name: str,
        *,
        predicate_filter: Optional[List[str]] = None,
        class_filter: Optional[List[str]] = None,
        top_n: int = 100,
    ) -> None:
        return TwinBackgroundTasks.run_metrics_task(
            tm,
            task_id,
            domain,
            settings,
            graph_name,
            predicate_filter=predicate_filter,
            class_filter=class_filter,
            top_n=top_n,
        )

    @staticmethod
    def run_inference_task(
        tm,
        task_id: str,
        settings,
        domain_snap: DomainSnapshot,
        options: Dict[str, Any],
        *,
        build_kind: str = "session",
    ) -> None:
        return TwinBackgroundTasks.run_inference_task(
            tm, task_id, settings, domain_snap, options, build_kind=build_kind
        )

    # ------------------------------------------------------------------
    # Legacy quality SQL builders (static)
    # ------------------------------------------------------------------

    @staticmethod
    def build_quality_sql(check_type: str, table: str, params: dict) -> Optional[str]:
        return QualitySqlBuilder.build_quality_sql(check_type, table, params)

    # ------------------------------------------------------------------
    # Registry / domain resolution (static -- API helpers)
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_registry(
        session_mgr,
        settings,
        registry_catalog=None,
        registry_schema=None,
        registry_volume=None,
    ):
        return TwinResolve.resolve_registry(
            session_mgr,
            settings,
            registry_catalog,
            registry_schema,
            registry_volume,
        )

    @staticmethod
    def resolve_domain(
        domain_name,
        session_mgr,
        settings,
        registry_catalog=None,
        registry_schema=None,
        registry_volume=None,
        domain_version=None,
        *,
        read_only=False,
    ):
        return TwinResolve.resolve_domain(
            domain_name,
            session_mgr,
            settings,
            registry_catalog,
            registry_schema,
            registry_volume,
            domain_version,
            read_only=read_only,
        )

    @staticmethod
    def uc_from_domain(domain, settings):
        return TwinResolve.uc_from_domain(domain, settings)

    # ------------------------------------------------------------------
    # Misc utilities (static)
    # ------------------------------------------------------------------

    @staticmethod
    def is_datatype_range(range_val: str) -> bool:
        return TwinResolve.is_datatype_range(range_val)

    @staticmethod
    def make_snapshot(domain):
        return TwinResolve.make_snapshot(domain)

    @staticmethod
    def extract_local_id(uri: str) -> str:
        return TwinResolve.extract_local_id(uri)

    @staticmethod
    def expand_uri_aliases(store, table_name: str, uris: Set[str]) -> Set[str]:
        """Find alternate URI forms for a set of entity URIs."""
        from back.objects.digitaltwin.explorer.GraphFind import GraphFind

        return GraphFind.expand_uri_aliases(store, table_name, uris)

    @staticmethod
    def find_triples_bfs(
        store: Any,
        table: str,
        *,
        entity_type: Optional[str] = None,
        search: Optional[str] = None,
        depth: int = 1,
        limit: int = 1000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Seed + BFS neighbourhood walk used by both find endpoints.

        Returns a normalized payload dict (``seed_count``, ``triples``,
        pagination fields). An empty match yields ``message`` and empty lists.
        """
        from back.objects.digitaltwin.explorer.GraphFind import GraphFind

        return GraphFind.find_triples_bfs(
            store,
            table,
            entity_type=entity_type,
            search=search,
            depth=depth,
            limit=limit,
            offset=offset,
        )

    @staticmethod
    def is_owlrl_available() -> bool:
        return TwinResolve.is_owlrl_available()

    # ------------------------------------------------------------------
    # Community detection
    # ------------------------------------------------------------------

    def detect_clusters(
        self,
        store: Any,
        graph_name: str,
        algorithm: str = "louvain",
        resolution: float = 1.0,
        predicate_filter: Optional[List[str]] = None,
        class_filter: Optional[List[str]] = None,
        max_triples: int = 500_000,
    ) -> Dict[str, Any]:
        return TwinAnalytics(self._domain).detect_clusters(
            store,
            graph_name,
            algorithm=algorithm,
            resolution=resolution,
            predicate_filter=predicate_filter,
            class_filter=class_filter,
            max_triples=max_triples,
        )

    def compute_graph_metrics(
        self,
        graph_name: str,
        predicate_filter: Optional[List[str]] = None,
        class_filter: Optional[List[str]] = None,
        top_n: int = 100,
        settings: Any = None,
        on_progress: Optional[Any] = None,
    ) -> Dict[str, Any]:
        return TwinAnalytics(self._domain).compute_graph_metrics(
            graph_name,
            predicate_filter=predicate_filter,
            class_filter=class_filter,
            top_n=top_n,
            settings=settings,
            on_progress=on_progress,
        )

    @staticmethod
    def build_job_metrics(
        domain: Any,
        settings: Any,
        *,
        source_table: str,
        graph_name: str,
        top_n: int = 100,
    ) -> Any:
        return TwinAnalytics.build_job_metrics(
            domain,
            settings,
            source_table=source_table,
            graph_name=graph_name,
            top_n=top_n,
        )

    def load_graph_metric_series(
        self,
        graph_name: str,
        metric: str,
        settings: Any = None,
    ) -> Dict[str, Any]:
        return TwinAnalytics(self._domain).load_graph_metric_series(
            graph_name, metric, settings=settings
        )

    @staticmethod
    def analytics_output_table(output_schema: str, domain: Any, graph_name: str) -> str:
        return TwinAnalytics.analytics_output_table(output_schema, domain, graph_name)

    def interpret_graph_metrics(
        self,
        payload: Dict[str, Any],
        host: str,
        token: str,
        endpoint_name: str,
        base_url: str = "",
        session_cookies: Optional[Dict[str, str]] = None,
        session_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        return TwinAnalytics(self._domain).interpret_graph_metrics(
            payload,
            host,
            token,
            endpoint_name,
            base_url=base_url,
            session_cookies=session_cookies,
            session_headers=session_headers,
        )

    @staticmethod
    def compute_dtwin_indicator(
        domain: Any,
        ts_status: Dict[str, Any],
        dt_exist: Dict[str, Any],
    ) -> Dict[str, Any]:
        return TwinAnalytics.compute_dtwin_indicator(domain, ts_status, dt_exist)

    # ------------------------------------------------------------------
    # Cohort discovery -- thin delegations to CohortService
    # ------------------------------------------------------------------
    #
    # The actual logic lives in
    # :class:`back.objects.digitaltwin.cohort.CohortService.CohortService`
    # (Extract Class refactor).  These wrappers preserve the public
    # surface that routes and tests have been calling so far.

    def _cohort_service(self) -> Any:
        from back.objects.digitaltwin.cohort.CohortService import CohortService

        return CohortService(self._domain)

    def list_cohort_rules(self) -> List[Dict[str, Any]]:
        """Return all saved cohort rules for the active domain."""
        return self._cohort_service().list_rules()

    def save_cohort_rule(self, rule_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and upsert *rule_dict* into ``domain.cohort_rules``."""
        return self._cohort_service().save_rule(rule_dict)

    def delete_cohort_rule(self, rule_id: str) -> bool:
        """Remove a cohort rule by id; returns ``True`` when something was deleted."""
        return self._cohort_service().delete_rule(rule_id)

    def dry_run_cohort(
        self,
        rule_dict: Dict[str, Any],
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        """Run the cohort engine on *rule_dict* without writing anything."""
        return self._cohort_service().dry_run(rule_dict, store, graph_name)

    def materialize_cohort(
        self,
        rule_id: str,
        store: Any,
        graph_name: str,
        client: Any = None,
        domain_version: str = "",
        member_label_resolver: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Re-run the engine for a saved rule and write outputs as configured."""
        return self._cohort_service().materialize(
            rule_id,
            store,
            graph_name,
            client=client,
            domain_version=domain_version,
            member_label_resolver=member_label_resolver,
        )

    def cohort_class_stats(
        self,
        class_uri: str,
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        """Return ``{instance_count}`` for *class_uri* in the live graph."""
        return self._cohort_service().class_stats(class_uri, store, graph_name)

    def cohort_edge_count(
        self,
        rule_dict: Dict[str, Any],
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        return self._cohort_service().edge_count(rule_dict, store, graph_name)

    def cohort_node_count(
        self,
        rule_dict: Dict[str, Any],
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        return self._cohort_service().node_count(rule_dict, store, graph_name)

    def cohort_path_trace(
        self,
        rule_dict: Dict[str, Any],
        store: Any,
        graph_name: str,
    ) -> Dict[str, Any]:
        """Per-hop frontier diagnostic for the rule's ``links``."""
        return self._cohort_service().path_trace(rule_dict, store, graph_name)

    def cohort_sample_values(
        self,
        class_uri: str,
        property_uri: str,
        store: Any,
        graph_name: str,
        limit: int = 20,
    ) -> Dict[str, Any]:
        return self._cohort_service().sample_values(
            class_uri, property_uri, store, graph_name, limit=limit
        )

    def cohort_suggest_uc_target(
        self, settings: Any = None, rule_name: str = ""
    ) -> Dict[str, Any]:
        """Return a suggested UC Delta target for the active domain.

        When *rule_name* is provided, the suggested ``table_name`` is
        ``cohorts_<snake_rule_name>`` so the UC table reads naturally
        and stays scoped to the rule the user is editing.
        """
        return self._cohort_service().suggest_uc_target(settings, rule_name)

    @staticmethod
    def cohort_probe_uc_write(
        target_dict: Dict[str, Any], client: Any
    ) -> Dict[str, Any]:
        """Run a 3-step read-only permission probe for a UC Delta target."""
        from back.objects.digitaltwin.cohort.CohortService import CohortService

        return CohortService.probe_uc_write(target_dict, client)

    # ------------------------------------------------------------------
    # Filter / sync helpers (used by /sync/filter)
    # ------------------------------------------------------------------

    @staticmethod
    def filter_preview(
        store: Any,
        graph_name: str,
        entity_type: str,
        field: str,
        match_type: str,
        value: str,
        max_preview: int = 500,
    ) -> Dict[str, Any]:
        """Seed-search phase: return a flat entity list for the filter modal."""
        from back.objects.digitaltwin.explorer.GraphFilter import GraphFilter

        return GraphFilter.preview(
            store,
            graph_name,
            entity_type,
            field,
            match_type,
            value,
            max_preview=max_preview,
        )

    @staticmethod
    def filter_expand(
        store: Any,
        graph_name: str,
        selected_uris: List[str],
        include_rels: bool = True,
        depth: int = 3,
        max_entities: int = 5000,
        batch_size: int = 1000,
        max_triples: int = 100_000,
        max_fetch_seconds: float = 120.0,
    ) -> Dict[str, Any]:
        """BFS-expand selected URIs and return the induced subgraph triples."""
        from back.objects.digitaltwin.explorer.GraphFilter import GraphFilter

        return GraphFilter.expand(
            store,
            graph_name,
            selected_uris,
            include_rels=include_rels,
            depth=depth,
            max_entities=max_entities,
            batch_size=batch_size,
            max_triples=max_triples,
            max_fetch_seconds=max_fetch_seconds,
        )
