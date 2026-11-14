"""Graph analytics, clusters, metrics job wiring, and KG indicator.

Extracted from :class:`DigitalTwin` (Fowler Extract Class).
``DigitalTwin`` keeps one-line delegators for callers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError


def _dt():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    return DigitalTwin


class TwinAnalytics:
    """Community detection, Databricks graph metrics, and navbar KG indicator."""

    def __init__(self, domain) -> None:
        self._domain = domain

    # ------------------------------------------------------------------
    # Community detection
    # ------------------------------------------------------------------

    @staticmethod
    def analytics_output_schema(domain: Any, settings: Any) -> str:
        """``catalog.schema`` holding the analytics jobs' output tables."""
        from back.objects.registry import RegistryCfg

        schema = (getattr(settings, "analytics_job_output_schema", "") or "").strip()
        if schema:
            return schema
        rcfg = RegistryCfg.from_domain(domain, settings)
        return f"{rcfg.catalog}.{rcfg.schema}"

    @staticmethod
    def build_job_communities(
        domain: Any, settings: Any, graph_name: str, source_table: str = ""
    ) -> Any:
        """Wire a :class:`JobCommunities` on the Build warehouse.

        *source_table* is only needed to start a run; reading a previous run's
        output does not touch the source.
        """
        from back.core.graph_analysis import (
            JobCommunities,
            LakeflowRunner,
            community_output_table,
        )
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.helpers import resolve_community_job_name

        builder = GraphDBFactory.get_builder(domain, settings, engine="delta")
        if not builder.has_client():
            raise InfrastructureError(
                "The community detection output cannot be read",
                detail="No Build SQL Warehouse client could be created",
            )
        runner = LakeflowRunner(
            resolve_community_job_name(settings),
            timeout_s=int(getattr(settings, "analytics_job_timeout_s", 3600) or 3600),
            running_label="Detecting communities on Databricks",
        )
        return JobCommunities(
            source_table,
            runner=runner,
            query=builder.execute_query,
            output_table=community_output_table(
                TwinAnalytics.analytics_output_schema(domain, settings),
                domain,
                graph_name,
            ),
        )

    def detect_clusters(
        self,
        graph_name: str,
        *,
        class_filter: Optional[List[str]] = None,
        predicate_filter: Optional[List[str]] = None,
        settings: Any = None,
        on_progress: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Run the community job on the mapped snapshot; blocks until done.

        Called from a background task. Returns the run summary; the partition
        itself stays in the job's output tables (see :meth:`latest_clusters`).
        """
        from back.core.graph_analysis import analytics_snapshot, resolve_analytics_source

        source_table, reason = resolve_analytics_source(self._domain, settings)
        if not source_table:
            raise InfrastructureError(
                "The community detection job cannot read this domain", detail=reason
            )
        with analytics_snapshot(self._domain, settings, source_table) as scan_table:
            communities = self.build_job_communities(
                self._domain, settings, graph_name, scan_table
            )
            return communities.run(
                class_filter=class_filter,
                exclude_predicates=predicate_filter,
                on_progress=on_progress,
            )

    def latest_clusters(
        self, graph_name: str, settings: Any = None, top_n: int = 50
    ) -> Dict[str, Any]:
        """Summary, top-N clusters and preview of the last detection run."""
        return self.build_job_communities(self._domain, settings, graph_name).latest(top_n)

    def detection_summary(self, graph_name: str, settings: Any = None) -> Optional[Dict[str, Any]]:
        """Stats of the last detection run, or ``None`` when there is none."""
        return self.build_job_communities(self._domain, settings, graph_name).summary()

    def cluster_members(
        self,
        graph_name: str,
        cluster_id: int,
        offset: int = 0,
        limit: int = 100,
        settings: Any = None,
    ) -> Dict[str, Any]:
        """One page of a cluster from the last detection run."""
        return self.build_job_communities(self._domain, settings, graph_name).members(
            cluster_id, offset, limit
        )

    def _base_uri(self) -> str:
        from shared.config.constants import DEFAULT_BASE_URI

        ontology = getattr(self._domain, "ontology", None) or {}
        if isinstance(ontology, dict):
            return (ontology.get("base_uri") or DEFAULT_BASE_URI).strip()
        return DEFAULT_BASE_URI

    def _materializer(self, store: Any, graph_name: str):
        from back.core.graph_analysis import CommunityMaterializer

        return CommunityMaterializer(store, graph_name, self._base_uri())

    def materialize_clusters(
        self,
        store: Any,
        graph_name: str,
        settings: Any = None,
        on_progress: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Replace the graph partition with the last detection run."""
        from back.core.errors import ValidationError

        communities = self.build_job_communities(self._domain, settings, graph_name)
        stats = communities.summary()
        if not stats:
            raise ValidationError("No community detection run to materialize. Run Detect first.")
        inserted = self._materializer(store, graph_name).materialize(
            communities, stats, on_progress=on_progress
        )
        return {"inserted": inserted, "cluster_count": stats["cluster_count"]}

    def load_current_clusters(
        self, store: Any, graph_name: str, limit: int = 50
    ) -> Dict[str, Any]:
        """Top-N materialized clusters and totals, or empty clusters."""
        return self._materializer(store, graph_name).load_current(limit)

    def materialized_members(
        self, store: Any, graph_name: str, cluster_id: int, offset: int = 0, limit: int = 100
    ) -> Dict[str, Any]:
        """One page of a materialized cluster's members."""
        return self._materializer(store, graph_name).members(cluster_id, offset, limit)

    def cluster_assignments(
        self, store: Any, graph_name: str, uris: List[str]
    ) -> Dict[str, int]:
        """Materialized cluster id for each of *uris* that has one."""
        return self._materializer(store, graph_name).assignments(uris)

    def clear_clusters(self, store: Any, graph_name: str) -> Dict[str, Any]:
        """Drop the materialized partition."""
        deleted = self._materializer(store, graph_name).clear()
        return {"deleted": deleted}

    # ------------------------------------------------------------------
    # Pattern detection
    # ------------------------------------------------------------------

    @staticmethod
    def build_job_patterns(
        domain: Any, settings: Any, graph_name: str, source_table: str = ""
    ) -> Any:
        """Wire a :class:`JobPatterns` on the Build warehouse."""
        from back.core.graph_analysis import (
            JobPatterns,
            LakeflowRunner,
            pattern_output_table,
        )
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.helpers import resolve_pattern_job_name

        builder = GraphDBFactory.get_builder(domain, settings, engine="delta")
        if not builder.has_client():
            raise InfrastructureError(
                "The pattern detection output cannot be read",
                detail="No Build SQL Warehouse client could be created",
            )
        runner = LakeflowRunner(
            resolve_pattern_job_name(settings),
            timeout_s=int(getattr(settings, "analytics_job_timeout_s", 3600) or 3600),
            running_label="Detecting patterns on Databricks",
        )
        return JobPatterns(
            source_table,
            runner=runner,
            query=builder.execute_query,
            output_table=pattern_output_table(
                TwinAnalytics.analytics_output_schema(domain, settings),
                domain,
                graph_name,
            ),
        )

    def detect_patterns(
        self,
        graph_name: str,
        *,
        class_filter: str,
        hops: int = 3,
        bins: int = 8,
        min_support: int = 3,
        refine_top: int = 0,
        settings: Any = None,
        on_progress: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Run the pattern job on the mapped snapshot; blocks until done."""
        from back.core.errors import ValidationError
        from back.core.graph_analysis import analytics_snapshot, resolve_analytics_source

        if not (class_filter or "").strip():
            raise ValidationError("class_filter is required")
        source_table, reason = resolve_analytics_source(self._domain, settings)
        if not source_table:
            raise InfrastructureError(
                "The pattern detection job cannot read this domain", detail=reason
            )
        with analytics_snapshot(self._domain, settings, source_table) as scan_table:
            patterns = self.build_job_patterns(
                self._domain, settings, graph_name, scan_table
            )
            return patterns.run(
                class_filter=class_filter.strip(),
                hops=hops,
                bins=bins,
                min_support=min_support,
                refine_top=refine_top,
                on_progress=on_progress,
            )

    def latest_patterns(
        self, graph_name: str, settings: Any = None, top_n: int = 50
    ) -> Dict[str, Any]:
        return self.build_job_patterns(self._domain, settings, graph_name).latest(top_n)

    def pattern_detection_summary(
        self, graph_name: str, settings: Any = None
    ) -> Optional[Dict[str, Any]]:
        return self.build_job_patterns(self._domain, settings, graph_name).summary()

    def pattern_members(
        self,
        graph_name: str,
        pattern_id: str,
        offset: int = 0,
        limit: int = 100,
        settings: Any = None,
    ) -> Dict[str, Any]:
        return self.build_job_patterns(self._domain, settings, graph_name).members(
            pattern_id, offset, limit
        )

    def pattern_shape(
        self, graph_name: str, pattern_id: str, settings: Any = None
    ) -> Optional[Dict[str, Any]]:
        return self.build_job_patterns(self._domain, settings, graph_name).shape(
            pattern_id, with_diagram=True
        )

    def _pattern_materializer(self, store: Any, graph_name: str):
        from back.core.graph_analysis import PatternMaterializer

        return PatternMaterializer(store, graph_name, self._base_uri())

    def materialize_pattern(
        self,
        store: Any,
        graph_name: str,
        pattern_id: str,
        label: str,
        settings: Any = None,
        on_progress: Optional[Any] = None,
    ) -> Dict[str, Any]:
        from back.core.errors import ValidationError

        patterns = self.build_job_patterns(self._domain, settings, graph_name)
        if not patterns.summary():
            raise ValidationError(
                "No pattern detection run to materialize. Run Detect first."
            )
        return self._pattern_materializer(store, graph_name).materialize(
            patterns, pattern_id, label, on_progress=on_progress
        )

    def list_saved_patterns(self, store: Any, graph_name: str) -> Dict[str, Any]:
        items = self._pattern_materializer(store, graph_name).list_saved()
        return {"patterns": items, "count": len(items)}

    def saved_pattern_members(
        self,
        store: Any,
        graph_name: str,
        saved_id: str,
        offset: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        return self._pattern_materializer(store, graph_name).members(
            saved_id, offset, limit
        )

    def delete_saved_pattern(
        self, store: Any, graph_name: str, saved_id: str
    ) -> Dict[str, Any]:
        deleted = self._pattern_materializer(store, graph_name).delete(saved_id)
        return {"deleted": deleted, "saved_id": saved_id}

    def compute_graph_metrics(
        self,
        graph_name: str,
        predicate_filter: Optional[List[str]] = None,
        class_filter: Optional[List[str]] = None,
        top_n: int = 100,
        settings: Any = None,
        on_progress: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Compute centrality and structural metrics on the mapped graph.

        There is one compute path: the Databricks analytics job, reading the
        ``…_data`` snapshot the Build materialises from the R2RML VIEW. That is
        deliberate — the same domain must produce the same KPIs whatever engine
        holds its graph, and at any size.

        A view-only Lakehouse domain has no such snapshot standing, so
        :func:`analytics_snapshot` materialises a disposable one around the run
        and drops it afterwards. The job therefore always scans a Delta table,
        whatever the domain's materialization.

        Raises rather than degrading when the job cannot run: a run that
        silently returns fewer metrics is exactly what this path replaced.

        *graph_name* names the output table only; the data comes from the
        resolved source. *on_progress* receives ``(percent, message)``.

        Returns a JSON-serializable dict matching the API contract.
        """
        from back.core.graph_analysis import (
            MetricsRequest,
            analytics_snapshot,
            resolve_analytics_source,
        )

        source_table, reason = resolve_analytics_source(self._domain, settings)
        if not source_table:
            raise InfrastructureError(
                "The graph analytics job cannot read this domain", detail=reason
            )

        request = MetricsRequest(
            predicate_filter=predicate_filter, class_filter=class_filter
        )
        with analytics_snapshot(self._domain, settings, source_table) as scan_table:
            job_metrics = _dt().build_job_metrics(
                self._domain,
                settings,
                source_table=scan_table,
                graph_name=graph_name,
                top_n=top_n,
            )
            return job_metrics.compute(request, on_progress=on_progress).to_dict()

    @staticmethod
    def build_job_metrics(
        domain: Any,
        settings: Any,
        *,
        source_table: str,
        graph_name: str,
        top_n: int = 100,
    ) -> Any:
        """Wire a :class:`JobMetrics` from domain credentials and settings.

        *graph_name* is used only to name the output table, not to read data:
        the job reads *source_table*, which is always the mapped snapshot.
        """
        from back.core.graph_analysis import JobMetrics, LakeflowRunner
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.helpers import resolve_analytics_job_name

        builder = GraphDBFactory.get_builder(domain, settings, engine="delta")
        if not builder.has_client():
            raise InfrastructureError(
                "The graph analytics job output cannot be read",
                detail="No Build SQL Warehouse client could be created",
            )

        job_name = resolve_analytics_job_name(settings)

        runner = LakeflowRunner(
            job_name,
            timeout_s=int(getattr(settings, "analytics_job_timeout_s", 3600) or 3600),
        )

        output_schema = TwinAnalytics.analytics_output_schema(domain, settings)

        return JobMetrics(
            source_table,
            runner=runner,
            query=builder.execute_query,
            output_table=_dt().analytics_output_table(
                output_schema, domain, graph_name
            ),
            top_n=top_n,
            pagerank_iterations=int(
                getattr(settings, "analytics_job_pagerank_iterations", 20) or 20
            ),
            pivots=int(getattr(settings, "analytics_job_pivots", 64) or 0),
            max_depth=int(getattr(settings, "analytics_job_max_depth", 32) or 32),
        )

    def load_graph_metric_series(
        self,
        graph_name: str,
        metric: str,
        settings: Any = None,
    ) -> Dict[str, Any]:
        """Return the ranked node-series, rank-bucketed on the warehouse."""
        from back.core.graph_analysis import (
            metric_series_query,
            validate_metric_series_column,
        )
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        metric_name = validate_metric_series_column(metric)

        builder = GraphDBFactory.get_builder(self._domain, settings, engine="delta")
        if not builder.has_client():
            raise InfrastructureError(
                "The graph analytics metric series cannot be read",
                detail="No Build SQL Warehouse client could be created",
            )

        output_schema = self.analytics_output_schema(self._domain, settings)

        output_table = _dt().analytics_output_table(
            output_schema, self._domain, graph_name
        )
        sql = metric_series_query(output_table, metric_name)
        rows = builder.execute_query(sql) or []
        total = int(rows[0].get("total_count") or len(rows)) if rows else 0
        return {
            "total": total,
            "sampled": len(rows) < total,
            "ranks": [int(row.get("node_rank") or i + 1) for i, row in enumerate(rows)],
            "uris": [str(row.get("node_uri") or "") for row in rows],
            "labels": [str(row.get("label") or "") for row in rows],
            "scores": [float(row.get("score", 0.0) or 0.0) for row in rows],
        }

    @staticmethod
    def analytics_output_table(output_schema: str, domain: Any, graph_name: str) -> str:
        """Build the per-version output table name for the analytics job.

        Only ``[A-Za-z0-9_]`` survives, because the name is interpolated into
        generated SQL unquoted on both the job and the read-back side.
        """
        import re

        folder = str(getattr(domain, "uc_domain_folder", "") or "") or graph_name
        version = str(getattr(domain, "current_version", "") or "")
        slug = re.sub(r"[^A-Za-z0-9_]+", "_", f"{folder}_{version}").strip("_").lower()
        return f"{output_schema}.graph_metrics_{slug or 'default'}"

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
        """Delegate graph-metrics interpretation to ``agent_graph_interpreter``.

        ``payload`` is the JSON dict returned by ``compute_graph_metrics`` plus an
        optional ``class_filter`` list added by the API layer.
        Returns ``{ success, sections: [{ title, body | items }] }``.
        """
        from agents.agent_graph_interpreter import run_agent

        # DomainSession stores the name under domain.info["name"], not .name
        domain_name = ""
        if self._domain is not None:
            _info = getattr(self._domain, "info", None) or {}
            domain_name = (_info.get("name") or "").strip() if isinstance(_info, dict) else ""

        result = run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            metrics_payload=payload,
            base_url=base_url,
            domain_name=domain_name,
            session_cookies=session_cookies or {},
            session_headers=session_headers,
        )

        if not result.success:
            raise InfrastructureError(
                result.error or "Graph metrics interpretation failed"
            )

        return {"success": True, "sections": result.sections}

    @staticmethod
    def compute_dtwin_indicator(
        domain: Any,
        ts_status: Dict[str, Any],
        dt_exist: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Derive a three-state Knowledge Graph indicator from live graph and artefact checks.

        Returns a dict with:
            indicator: ``'green'`` | ``'orange'`` | ``'red'``
            title:     tooltip text for the navbar
            count:     triple count (0 when unknown)
            pending:   ``True`` when no status has been fetched yet
        """
        if not ts_status and not dt_exist:
            if not domain.last_build:
                return {
                    "indicator": "red",
                    "title": "Knowledge Graph never built",
                    "count": 0,
                    "pending": False,
                }
            return {
                "indicator": "orange",
                "title": "Knowledge Graph status not yet checked",
                "count": 0,
                "pending": True,
            }

        graph_loaded = bool(
            ts_status and ts_status.get("has_data") and ts_status.get("count", 0) > 0
        )
        count = (ts_status or {}).get("count", 0)

        view_exists = (dt_exist or {}).get("view_exists")

        if graph_loaded and view_exists is not False:
            return {
                "indicator": "green",
                "title": f"Knowledge Graph active — {count:,} triples",
                "count": count,
                "pending": False,
            }

        if (
            not domain.last_build
            and not graph_loaded
            and not view_exists
        ):
            return {
                "indicator": "red",
                "title": "Knowledge Graph never built",
                "count": 0,
                "pending": False,
            }

        parts = []
        if view_exists is False:
            parts.append("view missing")
        if not graph_loaded:
            parts.append("graph not loaded")
        title = (
            "Knowledge Graph incomplete — " + ", ".join(parts)
            if parts
            else "Knowledge Graph partially available"
        )
        return {"indicator": "orange", "title": title, "count": count, "pending": False}

