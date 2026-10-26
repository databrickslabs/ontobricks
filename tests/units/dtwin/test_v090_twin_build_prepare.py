"""TwinGraphBuild / TwinLakehouseBuild session preparation, inferred-triple
materialisation (Delta + graph) and Knowledge Graph stats assembly."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.digitaltwin.DigitalTwin import DigitalTwin
from back.objects.digitaltwin.build.TwinGraphBuild import TwinGraphBuild
from back.objects.digitaltwin.TwinGraphStats import TwinGraphStats
from back.objects.digitaltwin.build.TwinInferredMaterialize import TwinInferredMaterialize
from back.objects.digitaltwin.build.TwinLakehouseBuild import TwinLakehouseBuild

pytestmark = pytest.mark.unit

_MAT_MODULE = importlib.import_module("back.objects.digitaltwin.build.TwinInferredMaterialize")


def _domain(**over) -> SimpleNamespace:
    base = dict(
        info={"name": "Sales"},
        databricks={"host": "https://h", "token": "secret"},
        delta={"catalog": "c"},
        triplestore={"stats": {"x": 1}, "_ts_cache_timestamp": 1.0},
        settings={},
        current_version="3",
        ontology={"base_uri": "http://ex.org/onto#"},
        generated={},
        assignment={"entities": []},
        last_update="2026-10-01T10:00:00",
        last_build="",
        save=MagicMock(),
        ensure_generated_content=MagicMock(),
        get_r2rml=MagicMock(return_value="@prefix rr: <http://www.w3.org/ns/r2rml#> ."),
    )
    base.update(over)
    return SimpleNamespace(**base)


# ---------------------------------------------------------------------------
# TwinGraphBuild
# ---------------------------------------------------------------------------


class TestGraphPipelineSteps:
    def _steps(self, engine, cfg=None, exc=None):
        kw = {"side_effect": exc} if exc else {"return_value": engine}
        with patch.object(GraphDBFactory, "_resolve_graph_engine", **kw), patch.object(
            GraphDBFactory, "_resolve_graph_engine_config", return_value=cfg or {}
        ):
            return TwinGraphBuild.pipeline_steps(_domain(), MagicMock())

    def test_managed_synced_lakebase_exposes_the_sync_steps(self):
        steps = self._steps("lakebase", {"lakebase": {"sync_mode": "managed_synced"}})
        assert [s["name"] for s in steps] == [
            "uc_schema",
            "sync_register",
            "sync_companion",
            "sync_data",
            "union_view",
            "finalize",
        ]

    def test_app_managed_lakebase_has_a_single_graph_step(self):
        steps = self._steps("lakebase", {"lakebase": {"sync_mode": "app_managed"}})
        assert [s["name"] for s in steps] == ["graph"]

    def test_other_engines_have_a_single_graph_step(self):
        assert [s["name"] for s in self._steps("neo4j")] == ["graph"]

    def test_engine_resolution_failure_defaults_to_single_step(self):
        steps = self._steps(None, exc=RuntimeError("boom"))
        assert [s["name"] for s in steps] == ["graph"]


def _prepare_graph(domain, *, view="c.s.v", creds=("https://h", "tok", "wh"), app=False):
    with patch.object(
        TwinGraphBuild, "pipeline_steps", return_value=[{"name": "graph"}]
    ):
        return TwinGraphBuild.prepare_session_sync(
            domain,
            MagicMock(),
            view_table_fn=lambda d: view,
            graph_name_fn=lambda d: "sales_graph",
            get_credentials=lambda d, s: creds,
            is_databricks_app_fn=lambda: app,
        )


class TestPrepareSessionSync:
    def test_valid_domain_returns_plan_and_stamps_last_build(self):
        dom = _domain()
        plan = _prepare_graph(dom)

        assert plan.view_table == "c.s.v"
        assert plan.graph_name == "sales_graph"
        assert plan.warehouse_id == "wh"
        assert plan.base_uri == "http://ex.org/onto#"
        assert plan.graph_steps == [{"name": "graph"}]
        assert plan.delta_cfg == {"catalog": "c"}
        assert dom.last_build
        assert dom.triplestore == {"build_last_update": "2026-10-01T10:00:00"}
        dom.save.assert_called_once()
        dom.ensure_generated_content.assert_called_once()
        assert "token" not in plan.domain_snap.databricks

    def test_unqualified_view_is_rejected_before_any_mutation(self):
        dom = _domain()
        with pytest.raises(ValidationError, match="fully qualified"):
            _prepare_graph(dom, view="schema.view")
        dom.save.assert_not_called()

    def test_missing_r2rml_is_rejected(self):
        with pytest.raises(ValidationError, match="R2RML"):
            _prepare_graph(_domain(get_r2rml=MagicMock(return_value="")))

    @pytest.mark.parametrize("creds", [("", "tok", "wh"), ("https://h", "", "wh")])
    def test_missing_host_or_token_outside_apps_is_rejected(self, creds):
        with pytest.raises(ValidationError, match="Databricks not configured"):
            _prepare_graph(_domain(), creds=creds)

    def test_apps_mode_tolerates_missing_host_and_token(self):
        plan = _prepare_graph(_domain(), creds=("", "", "wh"), app=True)
        assert plan.warehouse_id == "wh"

    def test_missing_warehouse_is_rejected(self):
        dom = _domain()
        with pytest.raises(ValidationError, match="warehouse"):
            _prepare_graph(dom, creds=("https://h", "tok", ""), app=True)
        dom.save.assert_not_called()

    def test_without_last_update_no_build_stamp_is_written(self):
        dom = _domain(last_update="")
        _prepare_graph(dom)
        assert "build_last_update" not in dom.triplestore


# ---------------------------------------------------------------------------
# TwinLakehouseBuild
# ---------------------------------------------------------------------------


def _prepare_lakehouse(domain, *, backend="databricks", view="c.s.v",
                       data="c.s.v_data", creds=("https://h", "tok", "wh"), app=False):
    with patch.object(
        GraphDBFactory, "_resolve_triple_store_backend", return_value=backend
    ), patch.object(
        GraphDBFactory, "resolve_lakehouse_materialization", return_value="table"
    ):
        return TwinLakehouseBuild.prepare_session(
            domain,
            MagicMock(),
            view_table_fn=lambda d: view,
            data_table_fn=lambda d, s: data,
            get_credentials=lambda d, s: creds,
            is_databricks_app_fn=lambda: app,
        )


class TestLakehousePrepareSession:
    def test_valid_domain_returns_inputs_and_stamps_last_build(self):
        dom = _domain()
        plan = _prepare_lakehouse(dom)

        assert plan.view_table == "c.s.v"
        assert plan.data_table == "c.s.v_data"
        assert plan.materialization == "table"
        assert plan.warehouse_id == "wh"
        assert dom.last_build
        assert dom.triplestore == {"build_last_update": "2026-10-01T10:00:00"}
        dom.save.assert_called_once()

    def test_non_databricks_backend_is_rejected(self):
        with pytest.raises(ValidationError, match="triple_store_backend"):
            _prepare_lakehouse(_domain(), backend="lakebase")

    def test_unqualified_view_is_rejected(self):
        with pytest.raises(ValidationError, match="View location"):
            _prepare_lakehouse(_domain(), view="v")

    def test_unresolved_data_table_is_rejected(self):
        with pytest.raises(ValidationError, match="Delta data table"):
            _prepare_lakehouse(_domain(), data="v_data")

    def test_missing_r2rml_is_rejected(self):
        with pytest.raises(ValidationError, match="R2RML"):
            _prepare_lakehouse(_domain(get_r2rml=MagicMock(return_value=None)))

    @pytest.mark.parametrize("creds", [("", "tok", "wh"), ("https://h", None, "wh")])
    def test_missing_host_or_token_outside_apps_is_rejected(self, creds):
        with pytest.raises(ValidationError, match="Databricks not configured"):
            _prepare_lakehouse(_domain(), creds=creds)

    def test_missing_warehouse_is_rejected_even_in_apps(self):
        dom = _domain()
        with pytest.raises(ValidationError, match="warehouse"):
            _prepare_lakehouse(dom, creds=("", "", ""), app=True)
        dom.save.assert_not_called()


# ---------------------------------------------------------------------------
# TwinInferredMaterialize
# ---------------------------------------------------------------------------

_GOOD = {
    "subject": "http://ex.org/s1",
    "predicate": "http://ex.org/p",
    "object": "http://ex.org/o1",
    "provenance": "swrl:rule1",
}
_LITERAL = {"subject": "http://ex.org/s2", "predicate": "http://ex.org/p", "object": "42"}


def _task(triples):
    return SimpleNamespace(result={"inferred_triples": triples})


def _materialize(*, do_delta=True, do_graph=True, mat_table="c.s.inferred",
                 triples=(_GOOD, _LITERAL), store="default", client="default",
                 svc=None, delta_count=1):
    reasoning_cls = MagicMock()
    svc = svc or MagicMock()
    svc.materialize_inferred.return_value = 1
    reasoning_cls.return_value = svc
    reasoning_cls.materialize_to_delta.return_value = delta_count
    store_obj = MagicMock() if store == "default" else store
    client_obj = MagicMock() if client == "default" else client
    get_store = MagicMock(return_value=store_obj)
    get_client = MagicMock(return_value=client_obj)
    with patch.object(_MAT_MODULE, "ReasoningService", reasoning_cls):
        result = TwinInferredMaterialize.run(
            _domain(),
            MagicMock(),
            task_id="t1",
            do_delta=do_delta,
            do_graph=do_graph,
            mat_table=mat_table,
            get_task=lambda tid: _task(list(triples)),
            get_store=get_store,
            get_client=get_client,
        )
    return result, reasoning_cls, svc, get_store, get_client, store_obj, client_obj


class TestInferredMaterialize:
    def test_uri_triples_drops_literal_objects(self):
        assert TwinInferredMaterialize.uri_triples([_GOOD, _LITERAL, {}]) == [_GOOD]

    def test_missing_task_id_is_rejected(self):
        with pytest.raises(ValidationError):
            TwinInferredMaterialize.run(
                _domain(), None, task_id="", do_delta=True, do_graph=False,
                mat_table="", get_task=MagicMock(), get_store=MagicMock(),
                get_client=MagicMock(),
            )

    def test_no_target_is_rejected(self):
        with pytest.raises(ValidationError, match="at least one"):
            _materialize(do_delta=False, do_graph=False)

    def test_unknown_task_is_not_found(self):
        with pytest.raises(NotFoundError):
            TwinInferredMaterialize.run(
                _domain(), None, task_id="t", do_delta=True, do_graph=True,
                mat_table="", get_task=lambda tid: None, get_store=MagicMock(),
                get_client=MagicMock(),
            )

    def test_task_without_triples_is_rejected(self):
        with pytest.raises(ValidationError, match="no inferred triples"):
            _materialize(triples=())

    def test_graph_write_receives_only_uri_triples_with_provenance(self):
        result, cls, svc, get_store, _gc, store, _c = _materialize(do_delta=False)

        assert result == {"success": True, "materialize_graph_count": 1}
        assert cls.call_args.args[1] is store
        rr = svc.materialize_inferred.call_args.args[0]
        assert [(t.subject, t.predicate, t.object, t.provenance) for t in rr.inferred_triples] == [
            ("http://ex.org/s1", "http://ex.org/p", "http://ex.org/o1", "swrl:rule1")
        ]

    def test_graph_write_opens_the_store_on_the_build_warehouse(self):
        _r, _cls, _svc, get_store, *_ = _materialize(do_delta=False)
        assert get_store.call_args.kwargs == {"for_write": True}

    def test_delta_write_goes_to_the_named_table_with_uri_triples(self):
        result, cls, _svc, _gs, _gc, _s, client = _materialize(do_graph=False, delta_count=1)

        cls.materialize_to_delta.assert_called_once_with(client, "c.s.inferred", [_GOOD])
        assert result["materialize_count"] == 1
        assert result["materialize_table"] == "c.s.inferred"
        assert "materialize_graph_count" not in result

    def test_both_targets_are_written(self):
        result, *_ = _materialize()
        assert result["materialize_count"] == 1
        assert result["materialize_graph_count"] == 1

    def test_non_three_part_table_skips_delta(self):
        result, cls, *_ = _materialize(do_graph=False, mat_table="s.inferred")
        cls.materialize_to_delta.assert_not_called()
        assert result == {"success": True}

    def test_missing_client_reports_credentials_error(self):
        result, *_ = _materialize(do_graph=False, client=None)
        assert result["materialize_error"] == "Databricks credentials not configured"

    def test_delta_failure_is_reported_not_raised(self):
        reasoning_cls = MagicMock()
        reasoning_cls.materialize_to_delta.side_effect = RuntimeError("UC denied")
        with patch.object(_MAT_MODULE, "ReasoningService", reasoning_cls):
            result = TwinInferredMaterialize.run(
                _domain(), None, task_id="t", do_delta=True, do_graph=False,
                mat_table="c.s.t", get_task=lambda tid: _task([_GOOD]),
                get_store=MagicMock(), get_client=MagicMock(),
            )
        assert result["materialize_error"] == "Materialise to Delta failed"
        assert result["materialize_table"] == "c.s.t"
        assert result["success"] is True

    def test_missing_graph_store_is_reported(self):
        result, *_ = _materialize(do_delta=False, store=None)
        assert result["materialize_graph_error"] == "Graph store not available"

    def test_graph_failure_is_reported_not_raised(self):
        svc = MagicMock()
        svc.materialize_inferred.side_effect = AttributeError("execute_statement")
        reasoning_cls = MagicMock(return_value=svc)
        with patch.object(_MAT_MODULE, "ReasoningService", reasoning_cls):
            result = TwinInferredMaterialize.run(
                _domain(), None, task_id="t", do_delta=False, do_graph=True,
                mat_table="", get_task=lambda tid: _task([_GOOD]),
                get_store=MagicMock(), get_client=MagicMock(),
            )
        assert result["materialize_graph_error"] == "Materialise to graph failed"

    def test_only_literal_triples_writes_nothing(self):
        result, cls, svc, get_store, get_client, *_ = _materialize(triples=(_LITERAL,))
        get_store.assert_not_called()
        get_client.assert_not_called()
        assert result == {"success": True}


# ---------------------------------------------------------------------------
# TwinGraphStats
# ---------------------------------------------------------------------------


class TestGraphStats:
    def test_empty_cache_is_stale(self):
        assert TwinGraphStats.cache_is_fresh(None) is False
        assert TwinGraphStats.cache_is_fresh({}) is False

    def test_cache_without_predicate_kind_is_stale(self):
        cached = {
            "top_predicates": [{"uri": "p"}],
            "analytics_job_blocked_reason": "",
        }
        assert TwinGraphStats.cache_is_fresh(cached) is False

    def test_cache_without_job_reason_is_stale(self):
        cached = {"top_predicates": [{"uri": "p", "kind": "attribute"}]}
        assert TwinGraphStats.cache_is_fresh(cached) is False

    def test_cache_with_kind_and_job_reason_is_fresh(self):
        cached = {
            "top_predicates": [{"uri": "p", "kind": "attribute"}],
            "analytics_job_blocked_reason": "",
        }
        assert TwinGraphStats.cache_is_fresh(cached) is True

    def test_cache_with_types_but_zero_triples_is_stale(self):
        cached = {
            "top_predicates": [{"uri": "p", "kind": "attribute"}],
            "analytics_job_blocked_reason": "",
            "entity_types": [{"uri": "t", "count": 5}],
            "total_triples": 0,
        }
        assert TwinGraphStats.cache_is_fresh(cached) is False

    def test_assemble_derives_relationship_count_and_classifies(self):
        classified = [{"uri": "p", "kind": "relationship"}]
        with patch.object(
            DigitalTwin, "classify_predicates", return_value=classified
        ) as cls:
            payload = TwinGraphStats.assemble(
                _domain(),
                MagicMock(),
                agg={
                    "total": 100,
                    "label_count": 10,
                    "distinct_subjects": 20,
                    "distinct_predicates": 5,
                },
                entity_types=[
                    {"type_uri": "http://ex.org/A", "cnt": "15"},
                    {"type_uri": "http://ex.org/B", "cnt": 5},
                ],
                top_predicates=[{"uri": "p"}],
                inferred_count=3,
                job_available=False,
                job_blocked_reason="not configured",
            )

        cls.assert_called_once_with([{"uri": "p"}])
        assert payload["type_assertion_count"] == 20
        assert payload["relationship_count"] == 70
        assert payload["entity_types"] == [
            {"uri": "http://ex.org/A", "count": 15},
            {"uri": "http://ex.org/B", "count": 5},
        ]
        assert payload["top_predicates"] == classified
        assert payload["analytics_job_available"] is False
        assert payload["analytics_job_blocked_reason"] == "not configured"
        assert payload["inferred_triples"] == 3

    def test_relationship_count_never_goes_negative(self):
        with patch.object(DigitalTwin, "classify_predicates", return_value=[]):
            payload = TwinGraphStats.assemble(
                _domain(),
                None,
                agg={"total": 5, "label_count": 4, "distinct_subjects": 1,
                     "distinct_predicates": 1},
                entity_types=[{"type_uri": "A", "cnt": 3}],
                top_predicates=[],
                inferred_count=None,
                job_available=True,
                job_blocked_reason="",
            )
        assert payload["relationship_count"] == 0
