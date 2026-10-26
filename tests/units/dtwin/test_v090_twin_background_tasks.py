"""TwinBackgroundTasks: TaskManager progress / completion / failure contracts
for build, adjacency refresh, data quality, graph metrics and inference."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb.SearchCachePolicy import CACHE_DISABLED_REFRESH_MESSAGE
from back.objects.digitaltwin.DigitalTwin import DigitalTwin
from back.objects.digitaltwin.TwinBackgroundTasks import TwinBackgroundTasks

pytestmark = pytest.mark.unit


def _progress_messages(tm):
    return [c.args[2] for c in tm.update_progress.call_args_list]


# ---------------------------------------------------------------------------
# complete_dq_task / run_build_task
# ---------------------------------------------------------------------------


class TestCompleteDqTask:
    def test_summary_counts_by_status(self):
        tm = MagicMock()
        results = [
            {"status": "success"},
            {"status": "success"},
            {"status": "error"},
            {"status": "warning"},
            {"status": "info"},
        ]
        TwinBackgroundTasks.complete_dq_task(tm, SimpleNamespace(id="t1"), results, 2.345)

        args, kwargs = tm.complete_task.call_args
        assert args == ("t1",)
        assert kwargs["result"]["summary"] == {
            "total": 5,
            "passed": 2,
            "failed": 1,
            "warnings": 2,
        }
        assert kwargs["result"]["duration_seconds"] == 2.3
        assert kwargs["message"] == (
            "Data quality checks complete: 2 passed, 1 failed, 2 warnings"
        )


class TestRunBuildTask:
    def test_delegates_every_input_to_the_build_pipeline(self):
        with patch(
            "back.objects.digitaltwin.build._build_pipeline._BuildPipeline"
        ) as pipeline:
            TwinBackgroundTasks.run_build_task(
                "tm", "t1", "dom", "settings", "snap", "h", "tok", "wh",
                "c.s.v", "g", "r2rml", "http://b/", {"m": 1}, {"o": 1}, {"d": 1},
                build_kind="api",
            )
        pipeline.assert_called_once_with(
            "tm", "t1", "dom", "settings", "snap", "h", "tok", "wh",
            "c.s.v", "g", "r2rml", "http://b/", {"m": 1}, {"o": 1}, {"d": 1},
            build_kind="api",
        )
        pipeline.return_value.run.assert_called_once_with()


# ---------------------------------------------------------------------------
# run_adjacency_refresh_task
# ---------------------------------------------------------------------------


@contextmanager
def _adjacency(store, *, allowed=True, graph_name="sales_graph", rebuild_exc=None):
    rebuild = MagicMock(side_effect=rebuild_exc)
    get_graphdb = MagicMock(return_value=store)
    with patch("back.core.graphdb.get_graphdb", get_graphdb), patch(
        "back.core.helpers.effective_graph_name", return_value=graph_name
    ), patch(
        "back.core.graphdb.search_cache.graph_cache_rebuild_allowed",
        return_value=allowed,
    ), patch(
        "back.core.graphdb.search_cache.rebuild_graph_cache_if_enabled", rebuild
    ):
        yield get_graphdb, rebuild


def _adj_store(supports=True):
    return SimpleNamespace(supports_adjacency=supports)


class TestAdjacencyRefresh:
    def test_databricks_rebuild_opens_a_write_store_and_runs_in_parallel(self):
        tm = MagicMock()
        store = _adj_store()
        snap = SimpleNamespace()
        with _adjacency(store) as (get_graphdb, rebuild):
            TwinBackgroundTasks.run_adjacency_refresh_task(
                tm, "t1", "settings", snap, backend="databricks"
            )

        get_graphdb.assert_called_once_with(snap, "settings", for_write=True)
        rebuild.assert_called_once_with(store, "sales_graph", snap)
        assert "Rebuilding graph indexes in parallel for sales_graph" in _progress_messages(tm)
        tm.complete_task.assert_called_once_with(
            "t1",
            result={"mode": "adjacency_only", "backend": "databricks"},
            message="Adjacency refresh completed",
        )
        tm.fail_task.assert_not_called()

    def test_other_backends_rebuild_sequentially_with_a_read_store(self):
        tm = MagicMock()
        with _adjacency(_adj_store()) as (get_graphdb, _rebuild):
            TwinBackgroundTasks.run_adjacency_refresh_task(
                tm, "t1", "settings", SimpleNamespace(), backend="lakebase"
            )
        assert get_graphdb.call_args.kwargs["for_write"] is False
        assert "Rebuilding graph indexes sequentially for sales_graph" in _progress_messages(tm)
        tm.complete_task.assert_called_once()

    def test_missing_backend_fails_without_rebuilding(self):
        tm = MagicMock()
        with _adjacency(None) as (_g, rebuild):
            TwinBackgroundTasks.run_adjacency_refresh_task(
                tm, "t1", None, SimpleNamespace(), backend="databricks"
            )
        tm.fail_task.assert_called_once_with(
            "t1", "Adjacency refresh failed: graph backend is not configured."
        )
        rebuild.assert_not_called()
        tm.complete_task.assert_not_called()

    def test_backend_without_adjacency_support_fails(self):
        tm = MagicMock()
        with _adjacency(_adj_store(supports=False)) as (_g, rebuild):
            TwinBackgroundTasks.run_adjacency_refresh_task(
                tm, "t1", None, SimpleNamespace(), backend="neo4j"
            )
        msg = tm.fail_task.call_args.args[1]
        assert "neo4j backend does not support adjacency rebuild" in msg
        rebuild.assert_not_called()

    def test_disabled_search_cache_fails_with_the_policy_message(self):
        tm = MagicMock()
        with _adjacency(_adj_store(), allowed=False) as (_g, rebuild):
            TwinBackgroundTasks.run_adjacency_refresh_task(
                tm, "t1", None, SimpleNamespace(), backend="databricks"
            )
        tm.fail_task.assert_called_once_with("t1", CACHE_DISABLED_REFRESH_MESSAGE)
        rebuild.assert_not_called()

    def test_blank_graph_name_fails(self):
        tm = MagicMock()
        with _adjacency(_adj_store(), graph_name="   ") as (_g, rebuild):
            TwinBackgroundTasks.run_adjacency_refresh_task(
                tm, "t1", None, SimpleNamespace(), backend="databricks"
            )
        tm.fail_task.assert_called_once_with(
            "t1", "Adjacency refresh failed: graph name is not configured."
        )
        rebuild.assert_not_called()

    def test_rebuild_exception_fails_the_task_with_reason(self):
        tm = MagicMock()
        with _adjacency(_adj_store(), rebuild_exc=RuntimeError("CTAS failed")):
            TwinBackgroundTasks.run_adjacency_refresh_task(
                tm, "t1", None, SimpleNamespace(), backend="databricks"
            )
        tm.fail_task.assert_called_once_with(
            "t1", "Adjacency refresh failed: CTAS failed"
        )
        tm.complete_task.assert_not_called()


# ---------------------------------------------------------------------------
# run_data_quality_task
# ---------------------------------------------------------------------------


class TestDataQualityTask:
    def _domain(self):
        return SimpleNamespace(
            info={"name": "Dom"},
            current_version=1,
            delta={"catalog": "cat", "schema": "sch"},
        )

    def _run(
        self,
        store,
        *,
        checks_exc=None,
        total=4,
        table="c.s.v",
        snapshot_ok=True,
        snapshot_exc=None,
        domain=None,
        **kw,
    ):
        tm = MagicMock()
        get_graphdb = MagicMock(return_value=store)
        statements = []
        domain = domain if domain is not None else self._domain()

        def _client(dom, settings=None, *, for_write=False):
            if not snapshot_ok:
                return None
            return SimpleNamespace(execute_statement=statements.append)

        def _ctas(client, view_fqn, table_fqn):
            if snapshot_exc:
                raise snapshot_exc
            client.execute_statement(f"CTAS {table_fqn} FROM {view_fqn}")

        with patch("back.core.graphdb.get_graphdb", get_graphdb), patch.object(
            DigitalTwin, "run_sql_checks", side_effect=checks_exc
        ) as checks, patch(
            "back.core.graphdb.delta.DeltaBase.create_databricks_client",
            _client,
        ), patch(
            "back.core.graphdb.delta.materialize.materialize_from_view",
            _ctas,
        ), patch(
            "back.core.graphdb.delta.materialize.drop_relation",
            lambda client, fqn, *, kind: statements.append(f"DROP {fqn}"),
        ):
            TwinBackgroundTasks.run_data_quality_task(
                tm, "t1", "settings", domain, ["shape"], table, total,
                swrl_rules=["r"], ontology_dict={"o": 1}, decision_tables=["d"],
                aggregate_rules=["a"], violation_limit=50, **kw,
            )
        return tm, get_graphdb, checks, statements

    def test_checks_run_on_a_task_scoped_snapshot_when_there_are_several(self):
        store = MagicMock()
        tm, get_graphdb, checks, statements = self._run(store)

        tm.start_task.assert_called_once_with("t1", "Running 4 data quality checks...")
        tm.update_progress.assert_any_call("t1", 0, "Materializing DQ snapshot...")
        get_graphdb.assert_called_once()
        assert get_graphdb.call_args.args[1] == "settings"
        assert get_graphdb.call_args.kwargs == {"engine": "view"}
        args, kwargs = checks.call_args
        assert args[1].id == "t1"
        assert args[2] == ["shape"]
        assert args[3] == "cat.sch.triplestore_dom_V1_dq_t1"
        assert args[4] is store
        assert args[6] == 4
        assert kwargs["parallel"] is True
        assert kwargs["violation_limit"] == 50
        assert any(s.startswith("CTAS ") for s in statements)
        assert any(s.startswith("DROP ") for s in statements)
        tm.fail_task.assert_not_called()

    def test_a_single_check_stays_on_the_live_view(self):
        store = MagicMock()
        tm, _g, checks, statements = self._run(store, total=1)
        args, kwargs = checks.call_args
        assert args[3] == "c.s.v"
        assert kwargs["parallel"] is False
        assert statements == []
        tm.update_progress.assert_not_called()

    def test_ctas_failure_falls_back_to_the_live_view_sequentially(self):
        store = MagicMock()
        tm, _g, checks, _s = self._run(
            store, snapshot_exc=RuntimeError("no CREATE TABLE")
        )
        args, kwargs = checks.call_args
        assert args[3] == "c.s.v"
        assert kwargs["parallel"] is False
        tm.fail_task.assert_not_called()

    def test_unreachable_warehouse_fails(self):
        tm, _g, checks, _s = self._run(None)
        tm.fail_task.assert_called_once_with("t1", "Could not reach the SQL warehouse")
        checks.assert_not_called()

    def test_exception_uses_the_generic_failure_message_by_default(self):
        tm, *_ = self._run(MagicMock(), checks_exc=RuntimeError("SQL broke"),
                           failure_message="DQ failed")
        tm.fail_task.assert_called_once_with("t1", "DQ failed")

    def test_exception_message_can_be_surfaced(self):
        tm, *_ = self._run(MagicMock(), checks_exc=RuntimeError("SQL broke"),
                           use_exception_message_on_failure=True)
        tm.fail_task.assert_called_once_with("t1", "SQL broke")


# ---------------------------------------------------------------------------
# run_metrics_task
# ---------------------------------------------------------------------------


class TestAnalyticsRunEntry:
    def test_missing_stats_default_to_zero(self):
        entry = TwinBackgroundTasks._analytics_run_entry(
            status="failed", class_filter=["A"], task_id="t", duration_ms=5,
            computed_at="now", error="boom",
        )
        assert entry["node_count"] == 0
        assert entry["density"] == 0.0
        assert entry["error"] == "boom"
        assert entry["status"] == "failed"

    def test_stats_are_coerced(self):
        entry = TwinBackgroundTasks._analytics_run_entry(
            status="completed", class_filter=[], task_id="t", duration_ms=5,
            computed_at="now",
            stats={"node_count": "3", "edge_count": 4, "connected_components": None,
                   "avg_degree": "1.5", "density": 0.25},
        )
        assert (entry["node_count"], entry["edge_count"], entry["connected_components"]) == (3, 4, 0)
        assert entry["avg_degree"] == 1.5
        assert entry["density"] == 0.25


def _metrics_domain(folder="sales", version="2"):
    return SimpleNamespace(uc_domain_folder=folder, current_version=version)


class TestMetricsTask:
    def _run(self, domain, *, compute=None, compute_exc=None, registry=None,
             from_context_exc=None):
        tm = MagicMock()
        registry = registry or MagicMock()

        def _compute(self, graph_name, **kw):
            if compute_exc:
                raise compute_exc
            kw["on_progress"](50, "job running")
            return compute or {"stats": {"node_count": 1234}, "top_pagerank": [{"u": 1}]}

        from_context = MagicMock(return_value=registry, side_effect=from_context_exc)
        with patch.object(DigitalTwin, "compute_graph_metrics", _compute), patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            from_context,
        ):
            TwinBackgroundTasks.run_metrics_task(
                tm, "t1", domain, "settings", "g",
                predicate_filter=["p"], class_filter=("A",), top_n=10,
            )
        return tm, registry, from_context

    def test_success_persists_latest_result_and_history(self):
        tm, registry, _fc = self._run(_metrics_domain())

        assert (50, "job running") in [c.args[1:] for c in tm.update_progress.call_args_list]
        folder, version, entry = registry.save_graph_analytics.call_args.args
        assert (folder, version) == ("sales", "2")
        assert entry["status"] == "completed"
        assert entry["class_filter"] == ["A"]
        assert entry["top_pagerank"] == [{"u": 1}]
        assert entry["task_id"] == "t1"
        hist = registry.record_graph_analytics_run.call_args.args[2]
        assert hist["status"] == "completed"
        assert hist["node_count"] == 1234
        kwargs = tm.complete_task.call_args.kwargs
        assert kwargs["result"]["node_count"] == 1234
        assert kwargs["result"]["mode"] == "job"
        assert "1,234 nodes" in kwargs["message"]
        tm.fail_task.assert_not_called()

    def test_missing_folder_or_version_skips_persistence_but_completes(self):
        tm, registry, from_context = self._run(_metrics_domain(version=""))
        from_context.assert_not_called()
        registry.save_graph_analytics.assert_not_called()
        tm.complete_task.assert_called_once()

    def test_failure_records_a_failed_run_and_fails_the_task(self):
        tm, registry, _fc = self._run(
            _metrics_domain(), compute_exc=RuntimeError("job timed out")
        )
        hist = registry.record_graph_analytics_run.call_args.args[2]
        assert hist["status"] == "failed"
        assert hist["error"] == "job timed out"
        registry.save_graph_analytics.assert_not_called()
        tm.fail_task.assert_called_once_with("t1", "job timed out")
        tm.complete_task.assert_not_called()

    def test_history_write_failure_does_not_mask_the_original_error(self):
        tm, _r, _fc = self._run(
            _metrics_domain(),
            compute_exc=RuntimeError("job timed out"),
            from_context_exc=RuntimeError("registry down"),
        )
        tm.fail_task.assert_called_once_with("t1", "job timed out")

    def test_failure_without_folder_skips_history(self):
        tm, _r, from_context = self._run(
            _metrics_domain(folder=""), compute_exc=RuntimeError("x")
        )
        from_context.assert_not_called()
        tm.fail_task.assert_called_once_with("t1", "x")


# ---------------------------------------------------------------------------
# run_inference_task — branches not covered by test_run_inference_task.py
# ---------------------------------------------------------------------------


class _T:
    def __init__(self, s, p, o):
        self.subject, self.predicate, self.object = s, p, o


class _Res:
    def __init__(self, triples, violations=()):
        self.inferred_triples = list(triples)
        self.violations = list(violations)

    def to_dict(self):
        return {"violations": ["v"] * len(self.violations), "inferred": len(self.inferred_triples)}


_URI_T = _T("http://ex.org/s", "http://ex.org/p", "http://ex.org/o")
_LIT_T = _T("http://ex.org/s", "http://ex.org/p", "literal")


def _infer(options, *, build_kind="scheduled", graphdb_returns=(), client="default",
           triples=(_URI_T,), violations=(), delta_exc=None, swrl_calls=()):
    tm = MagicMock()
    svc = MagicMock()

    def _full(opts, progress_callback=None):
        for call in swrl_calls:
            progress_callback(*call)
        return _Res(triples, violations)

    svc.run_full_reasoning.side_effect = _full
    svc.materialize_inferred.return_value = len(triples)
    get_graphdb = MagicMock(side_effect=list(graphdb_returns) or None,
                            return_value=MagicMock())
    client_obj = MagicMock() if client == "default" else client
    with patch("back.core.reasoning.ReasoningService", return_value=svc) as cls, patch(
        "back.core.graphdb.get_graphdb", get_graphdb
    ), patch("back.core.helpers.get_databricks_client", return_value=client_obj):
        cls.materialize_to_delta = MagicMock(return_value=1, side_effect=delta_exc)
        TwinBackgroundTasks.run_inference_task(
            tm, "t1", "settings", "snap", options, build_kind=build_kind
        )
    return tm, cls, svc, get_graphdb


class TestInferenceTaskBranches:
    def test_missing_graph_store_falls_back_to_the_view(self):
        view_store = MagicMock(name="view")
        tm, cls, _svc, get_graphdb = _infer(
            {"swrl": True}, graphdb_returns=[None, view_store]
        )
        assert get_graphdb.call_args_list[1].kwargs == {"engine": "view"}
        assert cls.call_args.args == ("snap", view_store)
        tm.complete_task.assert_called_once()

    def test_swrl_progress_is_mapped_into_the_30_to_80_band(self):
        tm, *_ = _infer({"swrl": True}, swrl_calls=[(0, 4, "r1"), (3, 4, "r4")])
        progress = [c.args[1:] for c in tm.update_progress.call_args_list]
        assert (30, "SWRL 1/4: r1") in progress
        assert (67, "SWRL 4/4: r4") in progress

    def test_append_without_graph_store_reports_error(self):
        tm, _cls, svc, _g = _infer(
            {"swrl": True, "append_graph": True},
            graphdb_returns=[MagicMock(), None],
        )
        result = tm.complete_task.call_args.kwargs["result"]
        assert result["append_graph_error"] == "Graph store not available"
        svc.materialize_inferred.assert_not_called()

    def test_materialize_without_credentials_writes_nothing(self):
        tm, cls, *_ = _infer(
            {"materialize": True, "materialize_table": "c.s.t"}, client=None
        )
        cls.materialize_to_delta.assert_not_called()
        result = tm.complete_task.call_args.kwargs["result"]
        assert "materialize_count" not in result
        assert "materialize_error" not in result

    def test_materialize_failure_is_reported_not_raised(self):
        tm, *_ = _infer(
            {"materialize": True, "materialize_table": "c.s.t"},
            delta_exc=RuntimeError("UC denied"),
        )
        tm.fail_task.assert_not_called()
        assert tm.complete_task.call_args.kwargs["result"]["materialize_error"] == "UC denied"

    def test_materialize_skips_literal_only_results(self):
        tm, cls, *_ = _infer(
            {"materialize": True, "materialize_table": "c.s.t"}, triples=(_LIT_T,)
        )
        cls.materialize_to_delta.assert_not_called()

    def test_api_run_keeps_violations_and_counts_them_in_the_message(self):
        tm, *_ = _infer({"swrl": True}, build_kind="api", violations=("v1", "v2"))
        kwargs = tm.complete_task.call_args.kwargs
        assert kwargs["result"]["violations_count"] == 2
        assert kwargs["result"]["violations"] == ["v", "v"]
        assert kwargs["message"] == "Inference complete: 1 inferred, 2 violations"

    def test_session_run_strips_violations(self):
        tm, *_ = _infer({"swrl": True}, build_kind="session", violations=("v1",))
        result = tm.complete_task.call_args.kwargs["result"]
        assert "violations" not in result
        assert "violations_count" not in result
        assert result["inferred_count"] == 1


# ---------------------------------------------------------------------------
# run_node_shacl_task
# ---------------------------------------------------------------------------


class TestNodeShaclTask:
    def test_completes_with_the_facet_payload(self):
        tm = MagicMock()
        payload = {
            "success": True,
            "summary": {"total": 3, "fail": 1},
            "facets": [{"shape_id": "s_email"}],
        }

        async def _evaluate(*_a, **kwargs):
            kwargs["progress"](20, "Checking s_email (1/3)...")
            return payload

        with patch(
            "back.objects.digitaltwin.node.NodeShaclService.NodeShaclService.evaluate",
            _evaluate,
        ):
            TwinBackgroundTasks.run_node_shacl_task(
                tm, "t1", "settings", SimpleNamespace(), "http://e/1", "c.s.v"
            )

        tm.start_task.assert_called_once_with("t1", "Running data quality checks...")
        tm.update_progress.assert_called()
        tm.complete_task.assert_called_once_with(
            "t1",
            result=payload,
            message="Checked 3 shapes (1 failed)",
        )

    def test_infrastructure_error_fails_the_task(self):
        from back.core.errors import InfrastructureError

        tm = MagicMock()

        async def _evaluate(*_a, **_kw):
            raise InfrastructureError("Could not reach the SQL warehouse")

        with patch(
            "back.objects.digitaltwin.node.NodeShaclService.NodeShaclService.evaluate",
            _evaluate,
        ):
            TwinBackgroundTasks.run_node_shacl_task(
                tm, "t1", "settings", SimpleNamespace(), "http://e/1", "c.s.v"
            )

        tm.fail_task.assert_called_once_with("t1", "Could not reach the SQL warehouse")
        tm.complete_task.assert_not_called()
