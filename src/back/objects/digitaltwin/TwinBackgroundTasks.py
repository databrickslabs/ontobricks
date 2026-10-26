"""Background TaskManager runners for graph build, DQ, metrics, and inference.

Extracted from :class:`DigitalTwin` (Fowler Extract Class).
``DigitalTwin`` keeps one-line delegators for callers.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.objects.digitaltwin.models import DomainSnapshot

logger = get_logger(__name__)


def _dt():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    return DigitalTwin


class TwinBackgroundTasks:
    """Run Knowledge Graph jobs inside worker threads (TaskManager progress)."""

    @staticmethod
    def complete_dq_task(tm, task, results, duration):
        """Finalize a data quality task with summary counts."""
        passed = sum(1 for r in results if r["status"] == "success")
        failed = sum(1 for r in results if r["status"] == "error")
        warnings = sum(1 for r in results if r["status"] in ("warning", "info"))
        tm.complete_task(
            task.id,
            result={
                "results": results,
                "summary": {
                    "total": len(results),
                    "passed": passed,
                    "failed": failed,
                    "warnings": warnings,
                },
                "duration_seconds": round(duration, 1),
            },
            message=(
                f"Data quality checks complete: {passed} passed, "
                f"{failed} failed, {warnings} warnings"
            ),
        )

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
        """Execute Knowledge Graph build/sync in a worker thread.

        ``build_kind``:
          * ``"session"`` — UI/internal build (diagnostics, progress callbacks,
            session cache, volume archive, phase timings).
          * ``"api"`` — external REST build (matches legacy ``digitaltwin.dt_build``).

        Implementation lives in :class:`_BuildPipeline`.
        """
        from back.objects.digitaltwin.build._build_pipeline import _BuildPipeline

        _BuildPipeline(
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
        ).run()

    @staticmethod
    def run_adjacency_refresh_task(
        tm,
        task_id: str,
        settings,
        domain_snap: DomainSnapshot,
        *,
        backend: str,
    ) -> None:
        """Rebuild only adjacency companions for the current graph relation."""
        from back.core.graphdb import get_graphdb
        from back.core.helpers import effective_graph_name

        try:
            tm.start_task(task_id, "Starting adjacency refresh...")
            tm.update_progress(task_id, 20, "Opening graph backend")

            store = get_graphdb(
                domain_snap,
                settings,
                for_write=backend == "databricks",
            )
            if not store:
                tm.fail_task(
                    task_id,
                    "Adjacency refresh failed: graph backend is not configured.",
                )
                return

            if not getattr(store, "supports_adjacency", False):
                tm.fail_task(
                    task_id,
                    (
                        "Adjacency refresh failed: "
                        f"{backend} backend does not support adjacency rebuild."
                    ),
                )
                return

            from back.core.graphdb.search_cache import (
                CACHE_DISABLED_REFRESH_MESSAGE,
                graph_cache_rebuild_allowed,
                rebuild_graph_cache_if_enabled,
            )

            if not graph_cache_rebuild_allowed(domain_snap):
                tm.fail_task(task_id, CACHE_DISABLED_REFRESH_MESSAGE)
                return

            graph_name = effective_graph_name(domain_snap).strip()
            if not graph_name:
                tm.fail_task(
                    task_id,
                    "Adjacency refresh failed: graph name is not configured.",
                )
                return

            if backend == "databricks":
                _rebuild_msg = (
                    f"Rebuilding graph indexes in parallel for {graph_name}"
                )
            else:
                _rebuild_msg = (
                    f"Rebuilding graph indexes sequentially for {graph_name}"
                )
            tm.update_progress(task_id, 70, _rebuild_msg)
            rebuild_graph_cache_if_enabled(store, graph_name, domain_snap)

            tm.complete_task(
                task_id,
                result={"mode": "adjacency_only", "backend": backend},
                message="Adjacency refresh completed",
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Adjacency refresh task failed: %s", exc)
            tm.fail_task(task_id, f"Adjacency refresh failed: {exc}")

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
        """Run SHACL / SWRL / DT / aggregate checks inside a worker thread.

        Every check is compiled to SQL and executed against the triple-store
        VIEW on the SQL warehouse, or against a run-scoped snapshot of it.
        """
        import time

        from back.core.graphdb import get_graphdb as _get_graphdb
        from back.objects.digitaltwin.quality.DataQualitySnapshot import (
            DataQualitySnapshot,
        )

        task_ref = SimpleNamespace(id=task_id)
        t0 = time.time()
        try:
            tm.start_task(task_id, f"Running {total} data quality checks...")

            store = _get_graphdb(domain_snap, settings, engine="view")
            if not store:
                tm.fail_task(task_id, "Could not reach the SQL warehouse")
                return

            snapshot_enabled = total >= 2
            if snapshot_enabled:
                tm.update_progress(task_id, 0, "Materializing DQ snapshot...")
            with DataQualitySnapshot(
                domain_snap,
                settings,
                triplestore_table,
                task_id,
                enabled=snapshot_enabled,
            ) as snap:
                _dt().run_sql_checks(
                    tm,
                    task_ref,
                    shapes,
                    snap.table,
                    store,
                    t0,
                    total,
                    swrl_rules=swrl_rules,
                    ontology=ontology_dict,
                    decision_tables=decision_tables,
                    aggregate_rules=aggregate_rules,
                    violation_limit=violation_limit,
                    parallel=snap.parallel,
                )

        except Exception as exc:
            logger.exception("Data quality checks failed: %s", exc)
            if use_exception_message_on_failure:
                tm.fail_task(task_id, str(exc))
            else:
                tm.fail_task(task_id, failure_message)

    @staticmethod
    def run_node_shacl_task(
        tm,
        task_id: str,
        settings,
        domain_snap: DomainSnapshot,
        entity_uri: str,
        triplestore_table: str,
    ) -> None:
        """Evaluate entity SHACL facets inside a worker thread."""
        import asyncio

        from back.core.errors import InfrastructureError
        from back.objects.digitaltwin.node.NodeShaclService import NodeShaclService

        tm.start_task(task_id, "Running data quality checks...")
        try:
            payload = asyncio.run(
                NodeShaclService.evaluate(
                    domain_snap,
                    settings,
                    entity_uri=entity_uri,
                    table=triplestore_table,
                    progress=lambda pct, msg: tm.update_progress(task_id, pct, msg),
                    snapshot_task_id=task_id,
                )
            )
            summary = payload.get("summary") or {}
            fail = int(summary.get("fail") or 0)
            total = int(summary.get("total") or 0)
            tm.complete_task(
                task_id,
                result=payload,
                message=f"Checked {total} shapes ({fail} failed)",
            )
        except InfrastructureError as exc:
            tm.fail_task(task_id, str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Entity SHACL facet check failed: %s", exc)
            tm.fail_task(task_id, "Could not run the data quality checks")

    @staticmethod
    def _analytics_run_entry(
        *,
        status: str,
        class_filter: List[str],
        task_id: str,
        duration_ms: int,
        computed_at: str,
        stats: Optional[Dict[str, Any]] = None,
        error: str = "",
    ) -> Dict[str, Any]:
        """Build one ``graph_analytics_runs`` history row (success or failure)."""
        stats = stats or {}
        return {
            "status": status,
            "class_filter": class_filter,
            "node_count": int(stats.get("node_count", 0) or 0),
            "edge_count": int(stats.get("edge_count", 0) or 0),
            "connected_components": int(stats.get("connected_components", 0) or 0),
            "avg_degree": float(stats.get("avg_degree", 0) or 0),
            "density": float(stats.get("density", 0) or 0),
            "duration_ms": duration_ms,
            "task_id": task_id,
            "error": error,
            "computed_at": computed_at,
        }

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
        """Compute graph metrics in a worker thread and persist the LAST result."""
        import time as _time
        from datetime import datetime, timezone

        from back.objects.registry.RegistryService import RegistryService

        folder = getattr(domain, "uc_domain_folder", "") or ""
        version = str(getattr(domain, "current_version", "") or "")
        class_filter_list = list(class_filter or [])
        t0 = _time.time()
        try:
            tm.start_task(task_id, "Computing knowledge graph metrics...")
            tm.update_progress(
                task_id, 20, "Starting the Databricks graph analytics job"
            )

            dt = _dt()(domain)
            result = dt.compute_graph_metrics(
                graph_name,
                predicate_filter=predicate_filter,
                class_filter=class_filter,
                top_n=top_n,
                settings=settings,
                on_progress=lambda pct, msg: tm.update_progress(task_id, pct, msg),
            )

            tm.update_progress(task_id, 85, "Storing analytics result")

            duration_ms = int((_time.time() - t0) * 1000)
            now_iso = datetime.now(timezone.utc).isoformat()
            stats = result.get("stats", {}) or {}
            entry = {
                "status": "completed",
                "graph_name": graph_name,
                "class_filter": class_filter_list,
                "stats": stats,
                "top_pagerank": result.get("top_pagerank", []),
                "result": result,
                "error": "",
                "task_id": task_id,
                "duration_ms": duration_ms,
                "computed_at": now_iso,
            }
            if folder and version:
                svc = RegistryService.from_context(domain, settings)
                svc.save_graph_analytics(folder, version, entry)
                svc.record_graph_analytics_run(
                    folder,
                    version,
                    TwinBackgroundTasks._analytics_run_entry(
                        status="completed",
                        class_filter=class_filter_list,
                        task_id=task_id,
                        duration_ms=duration_ms,
                        computed_at=now_iso,
                        stats=stats,
                    ),
                )
            else:
                logger.warning(
                    "run_metrics_task %s: missing folder/version (%r/%r) — "
                    "result not persisted",
                    task_id,
                    folder,
                    version,
                )

            node_count = stats.get("node_count", 0)
            tm.complete_task(
                task_id,
                result={
                    "node_count": node_count,
                    "duration_ms": duration_ms,
                    "mode": "job",
                },
                message=(
                    f"Analysis done: {node_count:,} nodes in {duration_ms} ms"
                    " (computed on Databricks)"
                ),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Graph metrics task failed: %s", exc)
            if folder and version:
                try:
                    RegistryService.from_context(
                        domain, settings
                    ).record_graph_analytics_run(
                        folder,
                        version,
                        TwinBackgroundTasks._analytics_run_entry(
                            status="failed",
                            class_filter=class_filter_list,
                            task_id=task_id,
                            duration_ms=int((_time.time() - t0) * 1000),
                            computed_at=datetime.now(timezone.utc).isoformat(),
                            error=str(exc),
                        ),
                    )
                except Exception:  # noqa: BLE001
                    pass
            tm.fail_task(task_id, str(exc))

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
        """Run ReasoningService phases; ``options`` drive append/materialize."""
        import datetime as _dt_mod

        from back.core.helpers import get_databricks_client, is_uri
        from back.core.reasoning import ReasoningService
        from back.core.reasoning.results.ReasoningResult import ReasoningResult as _RR
        from back.core.graphdb import get_graphdb

        is_api = build_kind == "api"
        label = {
            "api": "API inference",
            "scheduled": "Scheduled inference",
        }.get(build_kind, "Reasoning")
        try:
            logger.info("%s task %s: starting", label, task_id)
            tm.start_task(task_id)
            tm.update_progress(task_id, 10, "Initialising triple store")

            store = get_graphdb(domain_snap, settings)
            if store is None:
                logger.info(
                    "%s task %s: graph store unavailable, falling back to view",
                    label,
                    task_id,
                )
                store = get_graphdb(domain_snap, settings, engine="view")
            logger.info(
                "%s task %s: store=%s",
                label,
                task_id,
                type(store).__name__ if store else "None",
            )

            svc = ReasoningService(domain_snap, store)
            tm.update_progress(task_id, 30, "Running inference phases")

            logger.info(
                "%s task %s: running phases (tbox=%s, swrl=%s, graph=%s, "
                "constraints=%s, decision_tables=%s, sparql_rules=%s, "
                "aggregate_rules=%s)",
                label,
                task_id,
                options.get("tbox"),
                options.get("swrl"),
                options.get("graph"),
                options.get("constraints"),
                options.get("decision_tables"),
                options.get("sparql_rules"),
                options.get("aggregate_rules"),
            )

            def _swrl_progress(idx: int, total: int, rule_name: str) -> None:
                pct = 30 + int((idx / max(total, 1)) * 50)
                tm.update_progress(
                    task_id, pct, f"SWRL {idx + 1}/{total}: {rule_name}"
                )

            result = svc.run_full_reasoning(options, progress_callback=_swrl_progress)
            logger.info(
                "%s task %s: phases done — %d inferred, %d violations",
                label,
                task_id,
                len(result.inferred_triples),
                len(result.violations),
            )

            tm.update_progress(task_id, 90, "Finalising")

            result_dict = result.to_dict()
            if not is_api:
                result_dict.pop("violations", None)
            result_dict["last_run"] = _dt_mod.datetime.utcnow().isoformat()
            result_dict["inferred_count"] = len(result.inferred_triples)
            if is_api:
                result_dict["violations_count"] = len(result.violations)

            if options.get("append_graph") and result.inferred_triples:
                tm.update_progress(
                    task_id, 92, "Appending inferred triples to graph..."
                )
                try:
                    graph_store = get_graphdb(domain_snap, settings, for_write=True)
                    if graph_store is None:
                        logger.warning(
                            "%s %s: cannot append to graph — store unavailable",
                            label,
                            task_id,
                        )
                        result_dict["append_graph_error"] = "Graph store not available"
                    else:
                        append_count = ReasoningService(
                            domain_snap, graph_store
                        ).materialize_inferred(
                            _RR(inferred_triples=result.inferred_triples)
                        )
                        result_dict["append_graph_count"] = append_count
                        logger.info(
                            "%s %s: appended %d triples to graph",
                            label,
                            task_id,
                            append_count,
                        )
                except Exception as ag_err:
                    logger.exception(
                        "%s %s: append to graph failed: %s", label, task_id, ag_err
                    )
                    result_dict["append_graph_error"] = str(ag_err)

            mat_table = (options.get("materialize_table") or "").strip()
            if (
                options.get("materialize")
                and mat_table
                and len(mat_table.split(".")) == 3
            ):
                tm.update_progress(task_id, 95, f"Materialising to {mat_table}...")

                triples = [
                    {"subject": t.subject, "predicate": t.predicate, "object": t.object}
                    for t in result.inferred_triples
                    if is_uri(t.subject) and is_uri(t.predicate) and is_uri(t.object)
                ]
                if triples:
                    try:
                        client = get_databricks_client(domain_snap, settings)
                        if client is None:
                            logger.warning(
                                "%s %s: cannot materialise — no credentials",
                                label,
                                task_id,
                            )
                        else:
                            count = ReasoningService.materialize_to_delta(
                                client, mat_table, triples
                            )
                            result_dict["materialize_count"] = count
                            result_dict["materialize_table"] = mat_table
                            logger.info(
                                "%s %s: materialised %d triples to %s",
                                label,
                                task_id,
                                count,
                                mat_table,
                            )
                    except Exception as mat_err:
                        logger.exception(
                            "%s %s: materialisation failed: %s",
                            label,
                            task_id,
                            mat_err,
                        )
                        result_dict["materialize_error"] = str(mat_err)

            msg = f"Inference complete: {len(result.inferred_triples)} inferred"
            if is_api:
                msg += f", {len(result.violations)} violations"
            for extra_key, extra_label in (
                ("append_graph_count", "appended to graph"),
                ("materialize_count", "written to Delta"),
            ):
                if extra_key in result_dict:
                    msg += f", {result_dict[extra_key]} {extra_label}"

            tm.complete_task(task_id, result=result_dict, message=msg)
            logger.info("%s task %s: completed", label, task_id)
        except Exception as e:
            logger.exception("%s task %s failed: %s", label, task_id, e)
            tm.fail_task(
                task_id, "Inference failed" if build_kind == "session" else str(e)
            )
