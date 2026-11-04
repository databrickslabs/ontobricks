"""SPARQL route safety is enforced before session or warehouse access."""

import pytest

from api.routers.internal import dtwin
from back.core.errors import ValidationError


class JsonRequest:
    def __init__(self, payload):
        self.payload = payload

    async def json(self):
        return self.payload


@pytest.mark.asyncio
@pytest.mark.parametrize("route", [dtwin.execute_sparql, dtwin.translate_sparql])
@pytest.mark.parametrize(
    ("query", "message"),
    [
        (
            "WITH <https://example.com/g> "
            "DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }",
            "read-only",
        ),
        ("SELECT WHERE {", "Invalid SPARQL query"),
    ],
)
async def test_invalid_sparql_is_rejected_before_domain_lookup(route, query, message):
    request = JsonRequest({"query": query})

    with pytest.raises(ValidationError, match=message):
        if route is dtwin.execute_sparql:
            await route(request, session_mgr=None, settings=None)
        else:
            await route(request, session_mgr=None)


@pytest.mark.asyncio
async def test_execute_sparql_routes_sandbox_queries(monkeypatch):
    from unittest.mock import MagicMock

    called = {}

    class _Engine:
        def __init__(self, domain=None, settings=None):
            pass

        def execute(self, query, limit, simulation_id):
            called["sid"] = simulation_id
            called["query"] = query
            return {"success": True, "engine": "sandbox", "results": []}

    async def _inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    import importlib

    sandbox_mod = importlib.import_module(
        "back.objects.digitaltwin.query.TwinSandboxSparql"
    )
    monkeypatch.setattr(dtwin, "get_domain", lambda mgr: MagicMock())
    monkeypatch.setattr(dtwin, "run_blocking", _inline)
    monkeypatch.setattr(sandbox_mod, "TwinSandboxSparql", _Engine)
    out = await dtwin.execute_sparql(
        JsonRequest(
            {
                "query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
                "simulation_id": "s1abc",
            }
        ),
        session_mgr=MagicMock(),
        settings=MagicMock(),
    )
    assert out["engine"] == "sandbox"
    assert called["sid"] == "s1abc"
