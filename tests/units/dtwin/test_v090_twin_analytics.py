"""TwinAnalytics: community detection payload, job-metrics wiring through the
Build warehouse, metric series read-back, interpretation and KG indicator."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError
from back.objects.digitaltwin.DigitalTwin import DigitalTwin
from back.objects.digitaltwin.TwinAnalytics import TwinAnalytics

pytestmark = pytest.mark.unit


def _domain(**over):
    base = dict(uc_domain_folder="Sales Domain", current_version="4", info={"name": " Sales "},
                last_build="")
    base.update(over)
    return SimpleNamespace(**base)


def _settings(**over):
    base = dict(
        analytics_job_timeout_s=0,
        analytics_job_output_schema="",
        analytics_job_pagerank_iterations=None,
        analytics_job_pivots=8,
        analytics_job_max_depth=None,
    )
    base.update(over)
    return SimpleNamespace(**base)


class TestDetectClusters:
    def test_detector_result_is_serialised_to_the_api_contract(self):
        cluster = SimpleNamespace(id=0, members=["a", "b"], size=2)
        stats = SimpleNamespace(node_count=2, edge_count=1, cluster_count=1,
                                modularity=0.4, algorithm="louvain", elapsed_ms=3)
        detector = MagicMock()
        detector.detect.return_value = SimpleNamespace(clusters=[cluster], stats=stats)
        with patch(
            "back.core.graph_analysis.CommunityDetector", return_value=detector
        ) as cls:
            out = TwinAnalytics(_domain()).detect_clusters(
                "store", "g", algorithm="label_propagation", resolution=0.5,
                predicate_filter=["p"], class_filter=["C"], max_triples=10,
            )

        cls.assert_called_once_with("store", "g")
        req = detector.detect.call_args.args[0]
        assert (req.algorithm, req.resolution, req.max_triples) == ("label_propagation", 0.5, 10)
        assert out == {
            "clusters": [{"id": 0, "members": ["a", "b"], "size": 2}],
            "stats": {"node_count": 2, "edge_count": 1, "cluster_count": 1,
                      "modularity": 0.4, "algorithm": "louvain", "elapsed_ms": 3},
        }


class TestComputeGraphMetrics:
    def test_unreadable_domain_raises_with_reason(self):
        with patch(
            "back.core.graph_analysis.resolve_analytics_source",
            return_value=("", "no snapshot"),
        ):
            with pytest.raises(InfrastructureError) as exc:
                TwinAnalytics(_domain()).compute_graph_metrics("g", settings=None)
        assert exc.value.detail == "no snapshot"

    def test_job_scans_the_snapshot_table(self):
        @contextmanager
        def _snapshot(domain, settings, source):
            assert source == "c.s.v_data"
            yield "c.s.tmp_snapshot"

        job = MagicMock()
        job.compute.return_value.to_dict.return_value = {"stats": {}}
        progress = MagicMock()
        with patch(
            "back.core.graph_analysis.resolve_analytics_source",
            return_value=("c.s.v_data", ""),
        ), patch("back.core.graph_analysis.analytics_snapshot", _snapshot), patch.object(
            DigitalTwin, "build_job_metrics", return_value=job
        ) as build:
            out = TwinAnalytics(_domain()).compute_graph_metrics(
                "g", class_filter=["C"], top_n=7, settings="s", on_progress=progress
            )

        assert out == {"stats": {}}
        assert build.call_args.kwargs == {
            "source_table": "c.s.tmp_snapshot", "graph_name": "g", "top_n": 7
        }
        assert job.compute.call_args.args[0].class_filter == ["C"]
        assert job.compute.call_args.kwargs["on_progress"] is progress


@contextmanager
def _job_wiring(client):
    create = MagicMock(return_value=client)
    rcfg = SimpleNamespace(catalog="reg_cat", schema="reg_sch")
    with patch(
        "back.core.graphdb.delta.DeltaBase.create_databricks_client", create
    ), patch("back.core.graph_analysis.LakeflowRunner") as runner, patch(
        "back.core.graph_analysis.JobMetrics"
    ) as job_metrics, patch(
        "back.core.helpers.resolve_analytics_job_name", return_value="ob-analytics"
    ), patch("back.objects.registry.RegistryCfg.RegistryCfg.from_domain", return_value=rcfg):
        yield create, runner, job_metrics


class TestBuildJobMetrics:
    def test_job_output_is_read_through_the_build_warehouse(self):
        client = MagicMock()
        dom = _domain()
        with _job_wiring(client) as (create, runner, job_metrics):
            TwinAnalytics.build_job_metrics(
                dom, _settings(), source_table="c.s.v_data", graph_name="g", top_n=5
            )

        create.assert_called_once()
        assert create.call_args.kwargs == {"for_write": True}
        runner.assert_called_once_with("ob-analytics", timeout_s=3600)
        args, kwargs = job_metrics.call_args
        assert args == ("c.s.v_data",)
        assert kwargs["query"] == client.execute_query
        assert kwargs["output_table"] == "reg_cat.reg_sch.graph_metrics_sales_domain_4"
        assert kwargs["top_n"] == 5
        assert kwargs["pagerank_iterations"] == 20
        assert kwargs["pivots"] == 8
        assert kwargs["max_depth"] == 32

    def test_explicit_output_schema_wins_over_registry(self):
        with _job_wiring(MagicMock()) as (_c, _r, job_metrics):
            TwinAnalytics.build_job_metrics(
                _domain(), _settings(analytics_job_output_schema=" out.sch "),
                source_table="t", graph_name="g",
            )
        assert job_metrics.call_args.kwargs["output_table"].startswith("out.sch.graph_metrics_")

    def test_no_build_client_raises(self):
        with _job_wiring(None):
            with pytest.raises(InfrastructureError, match="output cannot be read"):
                TwinAnalytics.build_job_metrics(
                    _domain(), _settings(), source_table="t", graph_name="g"
                )


class TestMetricSeries:
    def test_series_is_read_through_the_build_warehouse(self):
        client = MagicMock()
        client.execute_query.return_value = [
            {"node_uri": "u1", "label": "A", "score": 0.9},
            {"node_uri": None, "label": None, "score": None},
        ]
        with _job_wiring(client) as (create, _r, _j):
            out = TwinAnalytics(_domain()).load_graph_metric_series(
                "g", "pagerank", settings=_settings()
            )

        assert create.call_args.kwargs == {"for_write": True}
        sql = client.execute_query.call_args.args[0]
        assert "FROM reg_cat.reg_sch.graph_metrics_sales_domain_4" in sql
        assert "pagerank AS score" in sql
        assert out == {
            "total": 2,
            "sampled": False,
            "ranks": [1, 2],
            "uris": ["u1", ""],
            "labels": ["A", ""],
            "scores": [0.9, 0.0],
        }

    def test_explicit_output_schema_is_used(self):
        client = MagicMock()
        client.execute_query.return_value = None
        with _job_wiring(client):
            out = TwinAnalytics(_domain()).load_graph_metric_series(
                "g", "degree", settings=_settings(analytics_job_output_schema="o.s")
            )
        assert "FROM o.s.graph_metrics_" in client.execute_query.call_args.args[0]
        assert out["total"] == 0

    def test_no_build_client_raises(self):
        with _job_wiring(None):
            with pytest.raises(InfrastructureError, match="metric series"):
                TwinAnalytics(_domain()).load_graph_metric_series(
                    "g", "pagerank", settings=_settings()
                )


class TestOutputTableName:
    def test_folder_and_version_are_slugged(self):
        dom = _domain(uc_domain_folder="Ventes-2026 (FR", current_version="1.2")
        out = TwinAnalytics.analytics_output_table("c.s", dom, "g")
        assert out == "c.s.graph_metrics_ventes_2026_fr_1_2"
        assert out.count(".") == 2

    def test_graph_name_is_used_without_folder(self):
        dom = SimpleNamespace()
        assert TwinAnalytics.analytics_output_table("c.s", dom, "My Graph") == (
            "c.s.graph_metrics_my_graph"
        )

    def test_empty_slug_falls_back_to_default(self):
        dom = SimpleNamespace(uc_domain_folder="", current_version="")
        assert TwinAnalytics.analytics_output_table("c.s", dom, "---") == (
            "c.s.graph_metrics_default"
        )


class TestInterpretGraphMetrics:
    def test_success_returns_sections_and_passes_domain_name(self):
        result = SimpleNamespace(success=True, sections=[{"title": "T"}], error="")
        with patch("agents.agent_graph_interpreter.run_agent", return_value=result) as run:
            out = TwinAnalytics(_domain()).interpret_graph_metrics(
                {"stats": {}}, "h", "tok", "ep"
            )
        assert out == {"success": True, "sections": [{"title": "T"}]}
        kwargs = run.call_args.kwargs
        assert kwargs["domain_name"] == "Sales"
        assert kwargs["session_cookies"] == {}

    def test_non_dict_info_yields_empty_domain_name(self):
        result = SimpleNamespace(success=True, sections=[], error="")
        with patch("agents.agent_graph_interpreter.run_agent", return_value=result) as run:
            TwinAnalytics(_domain(info="bad")).interpret_graph_metrics({}, "h", "t", "e")
        assert run.call_args.kwargs["domain_name"] == ""

    def test_failure_raises_with_agent_error(self):
        result = SimpleNamespace(success=False, sections=[], error="LLM quota")
        with patch("agents.agent_graph_interpreter.run_agent", return_value=result):
            with pytest.raises(InfrastructureError, match="LLM quota"):
                TwinAnalytics(None).interpret_graph_metrics({}, "h", "t", "e")


class TestDtwinIndicator:
    def _ind(self, ts, ex, last_build=""):
        return TwinAnalytics.compute_dtwin_indicator(
            SimpleNamespace(last_build=last_build), ts, ex
        )

    def test_never_built_without_status_is_red(self):
        out = self._ind({}, {})
        assert (out["indicator"], out["pending"]) == ("red", False)

    def test_built_but_unchecked_is_pending_orange(self):
        out = self._ind({}, {}, last_build="2026")
        assert (out["indicator"], out["pending"]) == ("orange", True)

    def test_loaded_graph_with_view_is_green_with_count(self):
        out = self._ind({"has_data": True, "count": 1234}, {"view_exists": True})
        assert out["indicator"] == "green"
        assert out["title"] == "Knowledge Graph active — 1,234 triples"

    def test_loaded_graph_with_unknown_view_is_still_green(self):
        out = self._ind({"has_data": True, "count": 5}, {})
        assert out["indicator"] == "green"

    def test_nothing_and_never_built_is_red(self):
        out = self._ind({"has_data": False}, {"view_exists": False})
        assert out["indicator"] == "red"

    def test_missing_view_on_loaded_graph_is_orange(self):
        out = self._ind({"has_data": True, "count": 5}, {"view_exists": False}, "2026")
        assert out["indicator"] == "orange"
        assert out["title"] == "Knowledge Graph incomplete — view missing"

    def test_view_without_graph_is_orange(self):
        out = self._ind({"has_data": False, "count": 0}, {"view_exists": True})
        assert out["title"] == "Knowledge Graph incomplete — graph not loaded"

    def test_built_with_both_missing_lists_both(self):
        out = self._ind({"has_data": False}, {"view_exists": False}, "2026")
        assert out["title"] == "Knowledge Graph incomplete — view missing, graph not loaded"
