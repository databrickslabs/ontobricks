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
        """Run community detection on the full knowledge graph.

        Delegates to :class:`CommunityDetector` from ``back.core.graph_analysis``.
        Returns a JSON-serializable dict matching the API contract.
        """
        from back.core.graph_analysis import CommunityDetector, ClusterRequest

        request = ClusterRequest(
            algorithm=algorithm,
            resolution=resolution,
            predicate_filter=predicate_filter,
            class_filter=class_filter,
            max_triples=max_triples,
        )
        detector = CommunityDetector(store, graph_name)
        result = detector.detect(request)

        return {
            "clusters": [
                {"id": c.id, "members": c.members, "size": c.size}
                for c in result.clusters
            ],
            "stats": {
                "node_count": result.stats.node_count,
                "edge_count": result.stats.edge_count,
                "cluster_count": result.stats.cluster_count,
                "modularity": result.stats.modularity,
                "algorithm": result.stats.algorithm,
                "elapsed_ms": result.stats.elapsed_ms,
            },
        }

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
        from back.core.graphdb.delta.DeltaBase import create_databricks_client
        from back.core.helpers import resolve_analytics_job_name
        from back.objects.registry import RegistryCfg

        client = create_databricks_client(domain, settings, for_write=True)
        if client is None:
            raise InfrastructureError(
                "The graph analytics job output cannot be read",
                detail="No Build SQL Warehouse client could be created",
            )

        job_name = resolve_analytics_job_name(settings)

        runner = LakeflowRunner(
            job_name,
            timeout_s=int(getattr(settings, "analytics_job_timeout_s", 3600) or 3600),
        )

        output_schema = (
            getattr(settings, "analytics_job_output_schema", "") or ""
        ).strip()
        if not output_schema:
            rcfg = RegistryCfg.from_domain(domain, settings)
            output_schema = f"{rcfg.catalog}.{rcfg.schema}"

        return JobMetrics(
            source_table,
            runner=runner,
            query=client.execute_query,
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
        """Return one exhaustive node-series, sampled server-side if needed."""
        from back.core.graph_analysis import (
            metric_series_query,
            sample_metric_series,
            validate_metric_series_column,
        )
        from back.core.graphdb.delta.DeltaBase import create_databricks_client
        from back.objects.registry import RegistryCfg

        metric_name = validate_metric_series_column(metric)

        client = create_databricks_client(self._domain, settings, for_write=True)
        if client is None:
            raise InfrastructureError(
                "The graph analytics metric series cannot be read",
                detail="No Build SQL Warehouse client could be created",
            )

        output_schema = (
            getattr(settings, "analytics_job_output_schema", "") or ""
        ).strip()
        if not output_schema:
            rcfg = RegistryCfg.from_domain(self._domain, settings)
            output_schema = f"{rcfg.catalog}.{rcfg.schema}"

        output_table = _dt().analytics_output_table(
            output_schema, self._domain, graph_name
        )
        sql = metric_series_query(output_table, metric_name)
        rows = client.execute_query(sql) or []
        sampled_rows, ranks, sampled = sample_metric_series(rows)
        total = len(rows)
        return {
            "total": total,
            "sampled": sampled,
            "ranks": ranks,
            "uris": [str(row.get("node_uri") or "") for row in sampled_rows],
            "labels": [str(row.get("label") or "") for row in sampled_rows],
            "scores": [float(row.get("score", 0.0) or 0.0) for row in sampled_rows],
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

