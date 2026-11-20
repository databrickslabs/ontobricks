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
