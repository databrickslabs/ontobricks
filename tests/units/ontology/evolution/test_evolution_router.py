"""Evolve router guards: feature flag, explicit folder, middleware classification."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from api.routers.internal import evolution as router_module
from back.core.errors import ConflictError, ValidationError

pytestmark = pytest.mark.unit


def _settings(enabled=True):
    return SimpleNamespace(evolution_enabled=enabled)


@pytest.fixture
def loaded(monkeypatch):
    domain = SimpleNamespace(
        domain_folder="EvoSupplyChain", info={"graph_backend": "databricks"}
    )
    monkeypatch.setattr(router_module, "get_domain", lambda _mgr: domain)
    monkeypatch.setattr(
        router_module.RegistryService,
        "from_context",
        staticmethod(lambda d, s: SimpleNamespace(store=object())),
    )
    return domain


def test_disabled_feature_is_refused(loaded):
    with pytest.raises(ValidationError):
        router_module._service(object(), _settings(False), "EvoSupplyChain")


def test_folder_must_match_loaded_context(loaded):
    with pytest.raises(ConflictError):
        router_module._service(object(), _settings(), "OtherDomain")


def test_folder_is_required(loaded):
    with pytest.raises(ValidationError):
        router_module._service(object(), _settings(), "")


def test_matching_folder_builds_a_service(loaded):
    svc = router_module._service(object(), _settings(), "EvoSupplyChain")
    assert svc.folder == "EvoSupplyChain"


async def test_capabilities_report_backend_support(loaded):
    out = await router_module.capabilities(session_mgr=object(), settings=_settings())
    assert out["execution_supported"] is True
    assert out["operations"] == ["add_relationship", "set_relationship_mapping"]
    loaded.info["graph_backend"] = "neo4j"
    out = await router_module.capabilities(session_mgr=object(), settings=_settings())
    assert out["execution_supported"] is False


class TestMiddlewareClassification:
    def test_evolution_is_domain_scoped(self):
        from shared.fastapi.main import _DOMAIN_SCOPED_PREFIXES

        assert "/evolution/" in _DOMAIN_SCOPED_PREFIXES

    @pytest.mark.parametrize("method", ["POST", "PATCH", "DELETE"])
    def test_evolution_writes_are_not_status_gated(self, method):
        # Evolve runs against PUBLISHED parents and never edits them.
        from shared.fastapi.main import _is_status_gated_edit

        assert not _is_status_gated_edit("/evolution/runs", method)

    def test_mutating_routes_require_builder(self):
        guarded = {
            (route.path, tuple(sorted(route.methods)))
            for route in router_module.router.routes
            if route.dependencies
        }
        assert ("/evolution/runs", ("POST",)) in guarded
        assert ("/evolution/runs/{run_id}/intent", ("PATCH",)) in guarded
        assert ("/evolution/runs/{run_id}/cancel", ("POST",)) in guarded
        assert ("/evolution/runs/{run_id}", ("DELETE",)) in guarded
        assert ("/evolution/runs/{run_id}/assess", ("POST",)) in guarded
        assert ("/evolution/runs/{run_id}/proposal", ("POST",)) in guarded
        assert ("/evolution/runs/{run_id}/evaluate", ("POST",)) in guarded
        assert ("/evolution/runs/{run_id}/accept", ("POST",)) in guarded
        assert ("/evolution/runs/{run_id}/cleanup", ("POST",)) in guarded


class _Request:
    def __init__(self, body):
        self._body = body
        self.state = SimpleNamespace(user_email="a@x")
        self.headers = {}

    async def json(self):
        return self._body


@pytest.fixture
def wired(monkeypatch):
    """Router handlers on the in-memory world; workers collected, not started."""
    from tests.fixtures.evolution.memory_graph import memory_query_factory
    from tests.fixtures.evolution.memory_probe import MemorySourceProbe
    from tests.units.ontology.evolution.test_evaluation import (
        ACME,
        FakeGraphBuilder,
        _proposed,
        _world,
    )

    svc, _evaluator, probe, registry, log = _world()
    session_domain = SimpleNamespace(domain_folder="EvoSupplyChain", settings={})
    spawned = []
    monkeypatch.setattr(router_module, "_service", lambda m, s, f: svc)
    monkeypatch.setattr(router_module, "_probe", lambda m, s: MemorySourceProbe())
    monkeypatch.setattr(router_module, "get_domain", lambda m: session_domain)
    monkeypatch.setattr(
        router_module,
        "_candidate_builder",
        lambda m, s: router_module.CandidateBuilder(
            None, {}, builder_factory=lambda d: FakeGraphBuilder(d, log)
        ),
    )
    real = router_module.EvaluationService
    monkeypatch.setattr(
        router_module,
        "EvaluationService",
        lambda *a, **k: real(*a, query_factory=memory_query_factory()),
    )
    monkeypatch.setattr(router_module, "_spawn", spawned.append)
    run = _proposed(svc, probe, ACME)
    return SimpleNamespace(
        svc=svc, run=run, spawned=spawned, registry=registry, log=log
    )


@pytest.fixture(autouse=True)
def _credentials(monkeypatch):
    from back.core.helpers.DatabricksHelpers import DatabricksHelpers

    monkeypatch.setattr(
        DatabricksHelpers,
        "get_triplestore_sql_credentials",
        staticmethod(lambda d, s: ("https://h", "", "wh")),
    )


async def test_evaluate_accept_results_and_delete(wired):
    body = {
        "domain": "EvoSupplyChain",
        "revision": wired.run["revision"],
        "expected_ids": ["O1", "O3", "O5"],
    }
    out = await router_module.evaluate(
        wired.run["id"], _Request(body), session_mgr=object(), settings=_settings()
    )
    assert out["run"]["status"] == "RUNNING"
    (work,) = wired.spawned
    work()
    assert len(wired.spawned) == 2  # the heartbeat ticker was started
    run = wired.svc.get_run(wired.run["id"])
    assert run["evaluation"]["decision"] == "PASS"

    res = await router_module.results(
        run["id"], domain="EvoSupplyChain", session_mgr=object(), settings=_settings()
    )
    assert len(res["results"]) == 2

    accepted = await router_module.accept(
        run["id"],
        _Request({"domain": "EvoSupplyChain", "revision": run["revision"]}),
        session_mgr=object(),
        settings=_settings(),
    )
    assert accepted["version"] == "2" and accepted["created"]

    cleaned = await router_module.cleanup(
        run["id"],
        _Request({"domain": "EvoSupplyChain"}),
        session_mgr=object(),
        settings=_settings(),
    )
    assert len(cleaned["dropped"]) == 8
    assert wired.svc.get_run(run["id"])["status"] == "ACCEPTED"

    other = wired.svc.create_run(
        "1", run["intent"], question="q", owner="o", idempotency_key="other"
    )
    other = wired.svc.update(
        other["id"], other["revision"], {"manifest": run["manifest"]}
    )
    gone = await router_module.delete_run(
        other["id"], domain="EvoSupplyChain", session_mgr=object(), settings=_settings()
    )
    assert gone["deleted"] is True and len(gone["dropped"]) == 8


def test_worker_heartbeat_ticks_until_the_work_ends(monkeypatch):
    beats = []
    svc = SimpleNamespace(heartbeat=lambda run_id: beats.append(run_id))
    evaluator = SimpleNamespace(execute=lambda run_id: None)
    started = []
    monkeypatch.setattr(router_module, "_spawn", started.append)
    monkeypatch.setattr(router_module, "HEARTBEAT_EVERY_S", 0)
    work = router_module._evaluation_worker(evaluator, svc, "r1")
    work()
    (beat,) = started
    beat()  # the stop event is already set once the work finished
    assert beats == []


def test_probe_runs_as_the_caller(monkeypatch):
    """The probe client comes from the on-behalf-of helper, never the app client."""
    from back.core.helpers.DatabricksHelpers import DatabricksHelpers

    seen = {}

    class Client:
        class sql:
            @staticmethod
            def execute_query(sql, statement_timeout_s=None):
                return [{"column_name": "a", "data_type": "string"}]

    def data_plane(domain, settings):
        seen["called"] = True
        return Client()

    monkeypatch.setattr(
        DatabricksHelpers, "get_data_plane_client", staticmethod(data_plane)
    )
    monkeypatch.setattr(router_module, "get_domain", lambda m: object())
    probe = router_module._probe(object(), _settings())
    assert seen["called"] is True
    assert probe.columns("c.s.t") == {"a": "string"}


class TestAssistantRoutes:
    def test_suggest_route_requires_builder(self):
        guarded = {
            (route.path, tuple(sorted(route.methods)))
            for route in router_module.router.routes
            if route.dependencies
        }
        assert ("/evolution/intent/suggest", ("POST",)) in guarded

    def test_assistant_reads_pages_as_the_caller(self, monkeypatch, loaded):
        from back.core.helpers.DatabricksHelpers import DatabricksHelpers

        pages = SimpleNamespace(get_page=lambda name: {"body": ""})
        seen = []
        monkeypatch.setattr(
            router_module, "require_domain_llm", lambda d, s: ("h", "t", "e", "")
        )
        monkeypatch.setattr(
            DatabricksHelpers,
            "get_data_plane_client",
            staticmethod(
                lambda d, s: seen.append(d) or SimpleNamespace(knowledge_pages=pages)
            ),
        )
        assistant = router_module._assistant(object(), _settings(), object())
        assert seen == [loaded] and assistant._fetch_page == pages.get_page
        monkeypatch.setattr(
            DatabricksHelpers, "get_data_plane_client", staticmethod(lambda d, s: None)
        )
        with pytest.raises(ValidationError):
            router_module._assistant(object(), _settings(), object())

    async def test_suggest_and_agent_proposal_dispatch(self, wired, monkeypatch):
        calls = []
        fake = SimpleNamespace(
            suggest_intent=lambda v, q: calls.append(("intent", v, q))
            or {"intent": {}},
            propose=lambda pipe, rid, rev: calls.append(("propose", rid, rev))
            or {"id": rid},
        )
        monkeypatch.setattr(router_module, "_assistant", lambda m, s, svc: fake)
        out = await router_module.suggest_intent(
            _Request(
                {"domain": "EvoSupplyChain", "parent_version": "1", "question": "q"}
            ),
            session_mgr=object(),
            settings=_settings(),
        )
        assert out == {"suggestion": {"intent": {}}}
        with pytest.raises(ValidationError):
            await router_module.suggest_intent(
                _Request({"domain": "EvoSupplyChain", "question": "q"}),
                session_mgr=object(),
                settings=_settings(),
            )
        out = await router_module.propose(
            wired.run["id"],
            _Request({"domain": "EvoSupplyChain", "revision": 3, "source": "agent"}),
            session_mgr=object(),
            settings=_settings(),
        )
        assert calls[-1] == ("propose", wired.run["id"], 3)

    async def test_select_switches_alternative(self, wired):
        run = wired.svc.get_run(wired.run["id"])
        out = await router_module.propose(
            run["id"],
            _Request(
                {"domain": "EvoSupplyChain", "revision": run["revision"], "selected": 0}
            ),
            session_mgr=object(),
            settings=_settings(),
        )
        assert out["run"]["proposal"]["selected"] == 0

    async def test_create_run_keeps_the_suggestion_source(self, wired):
        run = wired.svc.get_run(wired.run["id"])
        out = await router_module.create_run(
            _Request(
                {
                    "domain": "EvoSupplyChain",
                    "parent_version": "1",
                    "intent": run["intent"],
                    "idempotency_key": "with-suggestion",
                    "suggestion": {"intent": run["intent"]},
                }
            ),
            session_mgr=object(),
            settings=_settings(),
        )
        assert out["run"]["manifest"]["intent_source"]["kind"] == "agent"

    async def test_capabilities_report_the_assistant(self, loaded):
        out = await router_module.capabilities(
            session_mgr=object(), settings=_settings()
        )
        assert out["assistant"] is False
        loaded.info["llm_endpoint"] = "databricks-claude-sonnet-5-5"
        out = await router_module.capabilities(
            session_mgr=object(), settings=_settings()
        )
        assert out["assistant"] is True
