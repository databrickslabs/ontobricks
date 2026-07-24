"""Unit tests for the supervisor engine selection + dispatch."""

import pytest

from agents.agent_supervisor import mas as mas_mod
from agents.agent_supervisor.engine import SupervisorEngine

pytestmark = pytest.mark.unit


class _FakeResult:
    def __init__(self):
        self.success = True


class _FakeAgentClient:
    def __init__(self):
        self.calls = []

    def run_mapping_pge(self, **kw):
        self.calls.append(("pge", kw))
        return _FakeResult()

    def run_auto_assignment(self, **kw):
        self.calls.append(("simple", kw))
        return _FakeResult()

    def run_owl_generator(self, **kw):
        self.calls.append(("owl", kw))
        return _FakeResult()


_SIMPLE_MD = {"tables": [{"name": "t", "columns": ["id", "x"]}]}
_SIMPLE_ONTO = {"classes": [{"name": "T"}], "properties": []}
_COMPLEX_MD = {
    "tables": [
        {"name": "a", "columns": ["entity_id", "p"]},
        {"name": "b", "columns": ["entity_id", "q"]},
        {"name": "c", "columns": ["ENTITY_ID", "r"]},
    ]
}
_COMPLEX_ONTO = {
    "classes": [{"name": n} for n in ("A", "B", "C", "D", "E", "F", "G", "H")],
    "properties": [{"name": "rel", "domain": "A", "range": "B"}],
}


@pytest.fixture
def fake_client(monkeypatch):
    client = _FakeAgentClient()
    monkeypatch.setattr(
        "agents.agent_supervisor.engine.get_agent_client", lambda: client
    )
    return client


def test_invalid_task_raises(fake_client):
    with pytest.raises(ValueError):
        SupervisorEngine.run(
            task="nonsense",
            host="h",
            token="t",
            endpoint_name="e",
            metadata=_SIMPLE_MD,
            ontology=_SIMPLE_ONTO,
        )


def test_simple_domain_routes_to_simple_engine(fake_client):
    res = SupervisorEngine.run(
        task="mapping",
        host="h",
        token="t",
        endpoint_name="e",
        metadata=_SIMPLE_MD,
        ontology=_SIMPLE_ONTO,
        client=object(),
    )
    assert res.engine_used == "simple"
    assert fake_client.calls[0][0] == "simple"
    assert res.success


def test_complex_domain_routes_to_pge_engine(fake_client):
    res = SupervisorEngine.run(
        task="mapping",
        host="h",
        token="t",
        endpoint_name="e",
        metadata=_COMPLEX_MD,
        ontology=_COMPLEX_ONTO,
        client=object(),
    )
    assert res.engine_used == "pge"
    assert fake_client.calls[0][0] == "pge"


def test_engine_override_forces_engine(fake_client):
    res = SupervisorEngine.run(
        task="mapping",
        host="h",
        token="t",
        endpoint_name="e",
        metadata=_COMPLEX_MD,
        ontology=_COMPLEX_ONTO,
        engine_override="simple",
        client=object(),
    )
    assert res.engine_used == "simple"
    assert fake_client.calls[0][0] == "simple"
    # complexity report is still computed for observability
    assert res.complexity.tier == "complex"


def test_ontology_task_uses_single_engine(fake_client):
    res = SupervisorEngine.run(
        task="ontology",
        host="h",
        token="t",
        endpoint_name="e",
        metadata=_COMPLEX_MD,
        ontology=_COMPLEX_ONTO,
        base_uri="http://x#",
        selected_tables=["a", "b"],
    )
    assert res.engine_used == "owl_generator"
    assert fake_client.calls[0][0] == "owl"


def test_engine_failure_is_surfaced(monkeypatch):
    class _Boom(_FakeAgentClient):
        def run_auto_assignment(self, **kw):
            raise RuntimeError("engine exploded")

    monkeypatch.setattr(
        "agents.agent_supervisor.engine.get_agent_client", lambda: _Boom()
    )
    res = SupervisorEngine.run(
        task="mapping",
        host="h",
        token="t",
        endpoint_name="e",
        metadata=_SIMPLE_MD,
        ontology=_SIMPLE_ONTO,
        client=object(),
    )
    assert res.success is False
    assert "engine exploded" in res.error


def test_mas_config_shape():
    cfg = mas_mod.SupervisorProvisioner.build_config(
        catalog="cat",
        schema="sch",
        pge_endpoint="ob-mapping-pge",
        simple_endpoint="ob-mapping-simple",
    )
    names = [a["name"] for a in cfg["agents"]]
    assert names == ["complexity_assessor", "pge_mapping", "simple_mapping"]
    assessor = cfg["agents"][0]
    assert assessor["uc_function_name"] == "cat.sch.assess_domain_complexity"
    assert cfg["agents"][1]["endpoint_name"] == "ob-mapping-pge"
    assert cfg["agents"][2]["endpoint_name"] == "ob-mapping-simple"
    assert "complexity_assessor" in cfg["instructions"]
    assert len(cfg["examples"]) >= 2
