"""Simulation facade, background runner and internal API contracts."""

from __future__ import annotations

import sys
import threading
from types import SimpleNamespace

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.core.reasoning.results.InferredTriple import InferredTriple
from back.core.reasoning.results.ReasoningResult import ReasoningResult
from back.objects.simulation import (
    Simulation,
    SimulationDiff,
    SimulationRunner,
    SimulationScope,
    SimulationSpec,
)

runner_mod = sys.modules["back.objects.simulation.SimulationRunner"]
sim_mod = sys.modules["back.objects.simulation.Simulation"]

pytestmark = pytest.mark.unit

EX = "http://ex.org/"


class _Registry:
    def __init__(self):
        self.rows = {}
        self.runs = []

    def list_simulations(self, folder, version):
        return [dict(r) for r in self.rows.values()]

    def load_simulation(self, folder, version, sim_id):
        row = self.rows.get(sim_id)
        return dict(row) if row else None

    def save_simulation(self, folder, version, entry):
        merged = dict(self.rows.get(entry["id"], {}))
        merged.update(entry)
        self.rows[entry["id"]] = merged

    def delete_simulation(self, folder, version, sim_id):
        return self.rows.pop(sim_id, None) is not None

    def record_simulation_run(self, folder, version, entry):
        self.runs.append(entry)

    def load_simulation_runs(self, folder, version, *, sim_id=None, limit=50):
        return [r for r in self.runs if not sim_id or r["simulation_id"] == sim_id][:limit]


class _Domain:
    def __init__(self, backend="lakebase"):
        self.info = {"name": "Dom", "graph_backend": backend}
        self.current_version = "1"
        self.uc_domain_folder = "dom"
        self.cohort_rules = []

    def ensure_generated_content(self):
        return None


class _TM:
    def __init__(self):
        self.completed, self.failed, self.progress, self.results = [], [], [], []

    def create_task(self, **kwargs):
        return SimpleNamespace(id="task-1")

    def merge_result(self, task_id, patch):
        self.results.append((task_id, patch))
        return True

    def start_task(self, task_id, message=""):
        return True

    def update_progress(self, task_id, pct, message=""):
        self.progress.append((pct, message))

    def complete_task(self, task_id, result=None, message=""):
        self.completed.append((result, message))

    def fail_task(self, task_id, error):
        self.failed.append(error)


def _body(**over):
    body = {
        "label": "Score drop",
        "scope": {"type": "entity", "focus": EX + "acme", "hops": 1},
        "patches": [{"op": "set", "subject": EX + "acme", "predicate": EX + "score", "object": "300"}],
    }
    body.update(over)
    return body


# -- facade -----------------------------------------------------------------


class TestSimulationFacade:
    def test_backend_support(self):
        assert Simulation.is_backend_supported(_Domain("lakebase"))
        assert Simulation.is_backend_supported(_Domain("databricks"))
        assert Simulation.is_backend_supported(_Domain("neo4j"))
        assert not Simulation.is_backend_supported(_Domain("none"))

    def test_create_requires_supported_backend(self):
        sim = Simulation(_Domain("none"), None, registry=_Registry())
        with pytest.raises(ValidationError):
            sim.create(_body())

    def test_requires_saved_domain(self):
        domain = _Domain()
        domain.uc_domain_folder = ""
        with pytest.raises(ValidationError):
            Simulation(domain, None, registry=_Registry()).list_all()

    def test_crud(self, monkeypatch):
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        monkeypatch.setattr(sim, "_drop_sandbox", lambda sim_id: None)
        row = sim.create(_body(), owner="a@b.c")
        assert row["status"] == "draft" and row["owner"] == "a@b.c"
        catalog = row["artifacts"]
        assert catalog["status"] == "draft"
        assert catalog["objects"]
        assert all(o.get("name") for o in catalog["objects"])
        assert catalog["counts"] == {}
        assert [r["id"] for r in sim.list_all()] == [row["id"]]
        updated = sim.update(row["id"], _body(label="Renamed"))
        assert updated["label"] == "Renamed"
        assert sim.delete(row["id"]) is True
        with pytest.raises(NotFoundError):
            sim.get(row["id"])

    def test_update_keeps_ready_results_when_scope_unchanged(self, monkeypatch):
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        monkeypatch.setattr(sim, "_drop_sandbox", lambda sim_id: None)
        row = sim.create(_body())
        sim_id = row["id"]
        reg.rows[sim_id]["status"] = "ready"
        reg.rows[sim_id]["diff"] = {"added": [{"subject": "s"}]}
        reg.rows[sim_id]["diff_summary"] = {"added_count": 1}
        updated = sim.update(sim_id, _body(label="Jacob renamed"))
        stored = reg.rows[sim_id]
        assert updated["label"] == "Jacob renamed"
        assert stored["status"] == "ready"
        assert stored["diff"]["added"]
        assert stored["diff_summary"]["added_count"] == 1

    def test_update_keeps_diff_when_spec_changes(self, monkeypatch):
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        monkeypatch.setattr(sim, "_drop_sandbox", lambda sim_id: None)
        row = sim.create(_body())
        sim_id = row["id"]
        reg.rows[sim_id]["status"] = "ready"
        reg.rows[sim_id]["diff"] = {"added": [{"subject": "s"}]}
        changed = _body(
            patches=[
                {
                    "op": "set",
                    "subject": EX + "acme",
                    "predicate": EX + "score",
                    "object": "10",
                }
            ]
        )
        sim.update(sim_id, changed)
        stored = reg.rows[sim_id]
        assert stored["status"] == "draft"
        assert stored["diff"]["added"]

    def test_save_simulation_sql_keeps_diff_unless_provided(self):
        import inspect

        from back.objects.registry.store.lakebase.Simulations import Simulations

        src = inspect.getsource(Simulations.save_simulation)
        assert "replace_diff" in src
        assert "CASE WHEN %s THEN EXCLUDED.diff" in src
        assert "ELSE {sch}.simulations.diff END" in src

    def test_update_refused_while_running(self, monkeypatch):
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        row = sim.create(_body())
        reg.rows[row["id"]]["status"] = "running"
        reg.rows[row["id"]]["task_id"] = "task-live"
        monkeypatch.setattr(Simulation, "_run_task_is_active", staticmethod(lambda tid: True))
        with pytest.raises(ValidationError):
            sim.update(row["id"], _body())

    def test_update_clears_stale_running_lock(self):
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        row = sim.create(_body())
        reg.rows[row["id"]]["status"] = "running"
        updated = sim.update(row["id"], _body(label="After stale run"))
        assert updated["label"] == "After stale run"
        assert updated["status"] == "draft"

    def test_abort_unlocks_running_scenario(self, monkeypatch):
        cancelled = []
        monkeypatch.setattr(
            "back.core.task_manager.get_task_manager",
            lambda: SimpleNamespace(cancel_task=lambda tid: cancelled.append(tid)),
        )
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        row = sim.create(_body())
        reg.rows[row["id"]]["status"] = "running"
        reg.rows[row["id"]]["task_id"] = "task-stuck"
        monkeypatch.setattr(Simulation, "_run_task_is_active", staticmethod(lambda tid: True))
        out = sim.abort(row["id"])
        assert out["status"] == "failed"
        assert out["task_id"] == ""
        assert cancelled == ["task-stuck"]

    def test_start_run_cohort_rule_must_exist(self, monkeypatch):
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        row = sim.create(_body(scope={"type": "cohort", "focus": "missing"}))
        with pytest.raises(NotFoundError):
            sim.start_run(row["id"])

    def test_start_run_launches_runner_thread(self, monkeypatch):
        import back.core.task_manager as task_manager
        import back.objects.digitaltwin as dt_pkg

        tm = _TM()
        ran = []
        monkeypatch.setattr(task_manager, "get_task_manager", lambda: tm)
        monkeypatch.setattr(dt_pkg, "DomainSnapshot", lambda d: d)
        monkeypatch.setattr(sim_mod.SimulationRunner, "run", lambda self: ran.append(self._spec.id))
        started = []

        class _Thread:
            def __init__(self, target=None, daemon=None):
                self._target = target
                started.append(daemon)

            def start(self):
                self._target()

        monkeypatch.setattr(threading, "Thread", _Thread)
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        row = sim.create(_body())
        assert sim.start_run(row["id"]) == "task-1"
        assert ran == [row["id"]] and started == [True]
        assert tm.results == [("task-1", {"simulation_id": row["id"]})]

    def test_sandbox_relation_requires_ready(self, monkeypatch):
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        row = sim.create(_body())
        with pytest.raises(ValidationError, match="exploring"):
            sim.sandbox_relation(row["id"])
        reg.rows[row["id"]]["status"] = "ready"
        monkeypatch.setattr(sim_mod, "effective_graph_name", lambda d: "G_HR_v1")
        from back.core.graphdb.lakebase.LakebaseSimulationStore import (
            LakebaseSimulationStore,
        )

        name = sim.sandbox_relation(row["id"])
        assert name == LakebaseSimulationStore.sandbox_table_name("G_HR_v1", row["id"])

    def test_explorer_relation_unions_live_and_sandbox(self, monkeypatch):
        from back.core.graphdb.lakebase import _companion_ddl
        from back.core.graphdb.lakebase.LakebaseSimulationStore import (
            LakebaseSimulationStore,
        )

        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        row = sim.create(_body())
        reg.rows[row["id"]]["status"] = "ready"
        monkeypatch.setattr(sim_mod, "effective_graph_name", lambda d: "G_HR_v1")
        monkeypatch.setattr(
            sim_mod, "effective_graph_query_table",
            lambda d, s, include_inferred=True, store=None: "g_hr_v1" if include_inferred else "g_hr_v1_sync",
        )
        from contextlib import contextmanager

        seen = []

        class Store:
            @contextmanager
            def _cursor(self):
                class Cur:
                    def execute(self, sql, params=None):
                        seen.append(sql)
                yield Cur()

        store = Store()
        view = sim.explorer_relation(row["id"], include_inferred=True, store=store)
        expect = _companion_ddl.sim_union_phy("G_HR_v1", row["id"], include_inferred=True)
        sandbox = LakebaseSimulationStore.sandbox_table_name("G_HR_v1", row["id"])
        assert view == expect
        assert seen and "UNION ALL" in seen[0]
        assert "FROM g_hr_v1 l" in seen[0]
        assert f"FROM {sandbox} s" in seen[0]

    def test_diff_page(self):
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        row = sim.create(_body())
        reg.rows[row["id"]].update(
            diff={"added": [{"subject": "s", "predicate": "p", "object": "o", "origin": "inferred"}]},
            diff_summary={"added_count": 1},
        )
        page = sim.diff_page(row["id"], "added", 1, 50)
        assert page["total"] == 1 and page["summary"] == {"added_count": 1}


# -- runner -------------------------------------------------------------------


class _FakeSandbox:
    instances = []

    def __init__(self, live, graph_name, sim_id):
        self.table = f"{graph_name.lower()}__sim_{sim_id}"
        self._graph_name = graph_name
        self._sim_id = sim_id
        self.store = object()
        self.rows = {}
        self.seeded = None
        _FakeSandbox.instances.append(self)

    def reset(self):
        self.rows = {}

    def seed(self, entities):
        self.seeded = list(entities)
        self.seed_calls = getattr(self, "seed_calls", [])
        self.seed_calls.append(list(entities))
        for t in ({"subject": EX + "acme", "predicate": EX + "score", "object": "700"},):
            self.rows[SimulationDiff.key(t)] = t

    def seed_neighbourhood(self, uris, hops=1):
        self.neighbourhood_calls = getattr(self, "neighbourhood_calls", [])
        self.neighbourhood_calls.append((list(uris), hops))
        self.neighbourhood = list(uris)
        self.neighbourhood_hops = hops
        return self.seed(uris)

    def triples(self):
        return list(self.rows.values())

    def apply(self, patches):
        out = set()
        for p in patches:
            self.rows = {k: v for k, v in self.rows.items() if (k[0], k[1]) != (p.subject, p.predicate)}
            self.rows[SimulationDiff.key(p.triple())] = p.triple()
            out.add(SimulationDiff.key(p.triple()))
        return out


class _FakeReasoning:
    def __init__(self, domain, store, table):
        self.table = table
        self.sandbox = _FakeSandbox.instances[-1]

    def run_full_reasoning(self, options, progress_callback=None):
        assert options["materialize"] is False
        return ReasoningResult(
            inferred_triples=[InferredTriple(EX + "acme", EX + "risk", "high", "swrl:Risk")],
            stats={"graph_error": "boom"},
        )

    def materialize_inferred(self, result):
        for t in result.inferred_triples:
            row = {"subject": t.subject, "predicate": t.predicate, "object": t.object}
            self.sandbox.rows[SimulationDiff.key(row)] = row
        return len(result.inferred_triples)


def _runner(reg, tm, live_store=object()):
    spec = SimulationSpec.from_dict(_body(), default_hops=2, default_cap=100, sim_id="s1")
    return SimulationRunner(
        tm=tm,
        task_id="task-1",
        settings=None,
        domain_snap=_Domain(),
        registry=reg,
        folder="dom",
        version="1",
        spec=spec,
        owner="a@b.c",
        get_store=lambda d, s: live_store,
    )


class TestSimulationRunner:
    def test_success_persists_diff_and_run(self, monkeypatch):
        _FakeSandbox.instances = []
        monkeypatch.setattr(runner_mod, "SimulationSandbox", _FakeSandbox)
        monkeypatch.setattr(runner_mod, "SimulationReasoningService", _FakeReasoning)
        monkeypatch.setattr(
            SimulationScope, "resolve_entities", lambda self, store, g, **kw: [EX + "acme"]
        )
        reg, tm = _Registry(), _TM()
        _runner(reg, tm).run()

        row = reg.rows["s1"]
        assert row["status"] == "ready"
        s = row["diff_summary"]
        assert s["added_patch_count"] == 1 and s["added_inferred_count"] == 1
        assert s["removed_count"] == 1
        assert s["phase_errors"] == {"graph": "boom"}
        assert s["sandbox_table"] == "dom_v1__sim_s1"
        names = [o["name"] for o in s["artifacts"]]
        assert "dom_v1__sim_s1" in names
        assert any(o["kind"] == "view" for o in s["artifacts"])
        assert reg.runs[-1]["status"] == "success"
        assert tm.completed and not tm.failed

    def test_seeds_added_contract_neighbourhood(self, monkeypatch):
        _FakeSandbox.instances = []
        monkeypatch.setattr(runner_mod, "SimulationSandbox", _FakeSandbox)
        monkeypatch.setattr(runner_mod, "SimulationReasoningService", _FakeReasoning)
        monkeypatch.setattr(
            SimulationScope,
            "resolve_entities",
            lambda self, store, g, **kw: [EX + "acme"],
        )
        contract = EX + "contract2"
        spec = SimulationSpec.from_dict(
            _body(
                patches=[
                    {
                        "op": "add",
                        "subject": EX + "acme",
                        "predicate": EX + "hasContract",
                        "object": contract,
                    }
                ]
            ),
            default_hops=2,
            default_cap=100,
            sim_id="s1",
        )
        reg, tm = _Registry(), _TM()
        SimulationRunner(
            tm=tm,
            task_id="task-1",
            settings=None,
            domain_snap=_Domain(),
            registry=reg,
            folder="dom",
            version="1",
            spec=spec,
            owner="a@b.c",
            get_store=lambda d, s: object(),
        ).run()
        sandbox = _FakeSandbox.instances[0]
        assert any(
            contract in uris and hops == 1
            for uris, hops in sandbox.neighbourhood_calls
        )
        assert all(EX + "acme" not in uris for uris, _hops in sandbox.neighbourhood_calls)

    def test_normalizes_ontology_patch_predicate_for_sandbox(self, monkeypatch):
        _FakeSandbox.instances = []
        monkeypatch.setattr(runner_mod, "SimulationSandbox", _FakeSandbox)
        monkeypatch.setattr(runner_mod, "SimulationReasoningService", _FakeReasoning)
        monkeypatch.setattr(
            SimulationScope, "resolve_entities", lambda self, store, g, **kw: [EX + "acme"]
        )
        spec = SimulationSpec.from_dict(
            _body(
                patches=[
                    {
                        "op": "add",
                        "subject": EX + "acme",
                        "predicate": "http://ex.org#hascontract",
                        "object": EX + "contract2",
                    }
                ]
            ),
            default_hops=2,
            default_cap=100,
            sim_id="s1",
        )
        domain = _Domain()
        domain.ontology = {
            "base_uri": "http://ex.org#",
            "properties": [
                {"uri": "http://ex.org#hascontract", "name": "hascontract"}
            ],
        }
        reg, tm = _Registry(), _TM()
        SimulationRunner(
            tm=tm,
            task_id="task-1",
            settings=None,
            domain_snap=domain,
            registry=reg,
            folder="dom",
            version="1",
            spec=spec,
            owner="a@b.c",
            get_store=lambda d, s: object(),
        ).run()
        rows = _FakeSandbox.instances[0].rows
        assert (EX + "acme", EX + "hascontract", EX + "contract2") in rows

    def test_failure_marks_failed_without_leaking_internals(self, monkeypatch):
        reg, tm = _Registry(), _TM()

        def _boom(self, *a, **kw):
            raise RuntimeError("psycopg secret dsn")

        monkeypatch.setattr(SimulationScope, "resolve_entities", _boom)
        _runner(reg, tm).run()
        assert reg.rows["s1"]["status"] == "failed"
        assert "secret" not in reg.rows["s1"]["error"]
        assert reg.runs[-1]["status"] == "error"
        assert tm.failed and "secret" not in tm.failed[0]

    def test_swrl_skip_fails_the_run(self, monkeypatch):
        class _SkipSwrl(_FakeReasoning):
            def run_full_reasoning(self, options, progress_callback=None):
                return ReasoningResult(
                    stats={
                        "swrl_skipped": True,
                        "swrl_reason": "No SWRL rules defined in ontology",
                    }
                )

        _FakeSandbox.instances = []
        monkeypatch.setattr(runner_mod, "SimulationSandbox", _FakeSandbox)
        monkeypatch.setattr(runner_mod, "SimulationReasoningService", _SkipSwrl)
        monkeypatch.setattr(
            SimulationScope, "resolve_entities", lambda self, store, g, **kw: [EX + "acme"]
        )
        reg, tm = _Registry(), _TM()
        _runner(reg, tm).run()
        assert tm.failed == ["SWRL did not run: No SWRL rules defined in ontology"]
        assert reg.rows["s1"]["status"] == "failed"

    def test_swrl_sql_errors_fail_the_run(self, monkeypatch):
        class _BrokenSwrl(_FakeReasoning):
            def run_full_reasoning(self, options, progress_callback=None):
                return ReasoningResult(
                    stats={
                        "swrl_errors": 1,
                        "swrl_error_details": "attach: function try_cast does not exist",
                    }
                )

        _FakeSandbox.instances = []
        monkeypatch.setattr(runner_mod, "SimulationSandbox", _FakeSandbox)
        monkeypatch.setattr(runner_mod, "SimulationReasoningService", _BrokenSwrl)
        monkeypatch.setattr(
            SimulationScope, "resolve_entities", lambda self, store, g, **kw: [EX + "acme"]
        )
        reg, tm = _Registry(), _TM()
        _runner(reg, tm).run()
        assert tm.failed == [
            "SWRL failed on 1 rule(s): attach: function try_cast does not exist"
        ]

    def test_validation_error_message_is_surfaced(self, monkeypatch):
        reg, tm = _Registry(), _TM()

        def _cap(self, *a, **kw):
            raise ValidationError("Class population exceeds the cap")

        monkeypatch.setattr(SimulationScope, "resolve_entities", _cap)
        _runner(reg, tm).run()
        assert tm.failed == ["Class population exceeds the cap"]


# -- internal API -------------------------------------------------------------


class _Req:
    def __init__(self, body=None):
        self._body = body
        self.state = SimpleNamespace(user_email="a@b.c")
        self.headers = {}

    async def json(self):
        return self._body


class TestSimulationApi:
    @pytest.fixture(autouse=True)
    def _no_graph_gate(self, monkeypatch):
        from api.routers.internal import simulation as api

        monkeypatch.setattr(api, "assert_domain_graph_read", lambda request: None)

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        monkeypatch.setattr(api, "run_blocking", _inline)
        self.api = api

    def _sim(self, monkeypatch):
        sim = Simulation(_Domain(), None, registry=_Registry())
        monkeypatch.setattr(sim, "_drop_sandbox", lambda sim_id: None)
        return sim

    async def test_create_list_get_delete(self, monkeypatch):
        sim = self._sim(monkeypatch)
        created = await self.api.create_simulation(_Req(), body=_body(), sim=sim)
        sim_id = created["simulation"]["id"]
        listed = await self.api.list_simulations(_Req(), sim=sim)
        assert listed["count"] == 1 and listed["supported"] is True
        got = await self.api.get_simulation(sim_id, _Req(), sim=sim)
        assert "diff" not in got["simulation"]
        deleted = await self.api.delete_simulation(sim_id, sim=sim)
        assert deleted == {"success": True, "deleted": True}

    async def test_purge_keeps_spec_and_resets_draft(self, monkeypatch):
        sim = self._sim(monkeypatch)
        created = await self.api.create_simulation(_Req(), body=_body(), sim=sim)
        sim_id = created["simulation"]["id"]
        sim.registry.rows[sim_id]["status"] = "ready"
        sim.registry.rows[sim_id]["diff"] = {"added": [{"s": 1}]}
        sim.registry.rows[sim_id]["diff_summary"] = {"added_count": 1}
        dropped = []
        monkeypatch.setattr(sim, "_drop_sandbox", lambda sid: dropped.append(sid))
        out = await self.api.purge_simulation(sim_id, sim=sim)
        assert dropped == [sim_id]
        row = out["simulation"]
        assert row["status"] == "draft"
        assert "diff" not in row
        stored = sim.registry.rows[sim_id]
        assert stored["status"] == "draft"
        assert stored["diff"] == {}
        assert stored["spec"]

    async def test_purge_refuses_running(self, monkeypatch):
        sim = self._sim(monkeypatch)
        created = await self.api.create_simulation(_Req(), body=_body(), sim=sim)
        sim_id = created["simulation"]["id"]
        sim.registry.rows[sim_id]["status"] = "running"
        sim.registry.rows[sim_id]["task_id"] = "task-live"
        monkeypatch.setattr(Simulation, "_run_task_is_active", staticmethod(lambda tid: True))
        with pytest.raises(ValidationError, match="running"):
            await self.api.purge_simulation(sim_id, sim=sim)

    async def test_abort_route(self, monkeypatch):
        sim = self._sim(monkeypatch)
        monkeypatch.setattr(sim, "abort", lambda sim_id: {"id": sim_id, "status": "failed", "diff": {}})
        out = await self.api.abort_simulation("s1", sim=sim)
        assert out["success"] is True
        assert out["simulation"]["status"] == "failed"
        assert "diff" not in out["simulation"]

    async def test_json_body_must_be_object(self):
        with pytest.raises(ValidationError):
            await self.api._json_object(_Req(body=[1, 2]))

    async def test_run_returns_task_id(self, monkeypatch):
        sim = self._sim(monkeypatch)
        monkeypatch.setattr(sim, "start_run", lambda sim_id, owner="": "task-9")
        out = await self.api.run_simulation("s1", _Req(), sim=sim)
        assert out == {"success": True, "task_id": "task-9"}

    def test_write_routes_are_builder_guarded(self):
        guarded = {
            (route.path, tuple(sorted(route.methods)))
            for route in self.api.router.routes
            if route.dependencies
        }
        assert ("/simulation/create", ("POST",)) in guarded
        assert ("/simulation/{sim_id}", ("PUT",)) in guarded
        assert ("/simulation/{sim_id}", ("DELETE",)) in guarded
        assert ("/simulation/{sim_id}/run", ("POST",)) in guarded
        assert ("/simulation/{sim_id}/purge", ("POST",)) in guarded
        assert ("/simulation/{sim_id}/abort", ("POST",)) in guarded
        readonly = {r.path for r in self.api.router.routes if not r.dependencies}
        assert {"/simulation/list", "/simulation/runs", "/simulation/{sim_id}/diff"} <= readonly

    def test_router_is_registered(self):
        from api.routers.internal import all_internal_routers
        from front.routes import all_frontend_routers

        assert self.api.router in all_internal_routers
        assert any(r.prefix == "/simulation" for r in all_frontend_routers)
