"""OBO identity contract across every graph backend.

The design (`docs/superpowers/specs/2026-09-26-domain-obo-graph-access-design.md`)
splits data-plane identity by backend:

* **Delta / Lakehouse (Unity Catalog)** — a user-driven read runs on-behalf-of
  the caller (their forwarded token), so UC governs the data. Build / health /
  background reads carry no request identity and keep the service principal.
* **Lakebase Postgres** and **Neo4j** — connect as the app service principal /
  stored Bolt profile. They *never* consult the request identity; access is
  gated at the endpoint by Team membership (`assert_domain_graph_read`).

These tests pin that split at the `GraphDBFactory` / `DeltaBase` construction
layer so a future refactor can't silently start (or stop) forwarding the user
token on the wrong backend.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import back.core.databricks.request_identity as ri
from back.core.databricks.request_identity import (
    RequestIdentity,
    reset_request_identity,
    set_request_identity,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def obo_spy(monkeypatch):
    """Bind a user identity and count every ``get_request_identity`` call."""
    calls: list[int] = []
    orig = ri.get_request_identity

    def _spy():
        calls.append(1)
        return orig()

    monkeypatch.setattr(ri, "get_request_identity", _spy)
    tok = set_request_identity(
        RequestIdentity(email="u@x", user_token="USER_TOKEN", domain_role="viewer")
    )
    try:
        yield calls
    finally:
        reset_request_identity(tok)


# ── Delta / Lakehouse: OBO IS applied ─────────────────────────────────────


def _patch_delta(monkeypatch, *, app_mode: bool):
    """Wire ``create_databricks_client`` deps and capture the built client."""
    import back.core.graphdb.delta.DeltaBase as dbase

    captured: dict = {}

    class _Client:
        def __init__(
            self, host="", token="", warehouse_id="", use_sea=False, use_cloud_fetch=False
        ):
            captured["token"] = token

    monkeypatch.setattr(dbase, "is_databricks_app", lambda: app_mode)
    monkeypatch.setattr(
        dbase, "get_databricks_host_and_token", lambda d, s: ("https://x", "SP_TOKEN")
    )
    monkeypatch.setattr(dbase, "resolve_delta_warehouse_id", lambda d, s: "wh")
    monkeypatch.setattr(dbase, "resolve_lakehouse_use_sea", lambda d, s: False)
    monkeypatch.setattr(dbase, "resolve_use_cloud_fetch", lambda d, s: False)
    monkeypatch.setattr("back.core.databricks.DatabricksClient", _Client)
    return captured


def test_delta_backend_read_applies_obo(monkeypatch, obo_spy):
    """A user-driven Delta read binds the caller's forwarded token (OBO)."""
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    import importlib

    captured = _patch_delta(monkeypatch, app_mode=True)
    dfs_mod = importlib.import_module("back.core.graphdb.delta.DeltaFlatStore")
    monkeypatch.setattr(
        dfs_mod, "DeltaFlatStore", lambda client, **kw: SimpleNamespace(client=client)
    )

    store = GraphDBFactory().create(
        SimpleNamespace(info={"name": "Dom"}),
        SimpleNamespace(),
        engine="delta",
    )

    assert store is not None
    assert captured["token"] == "USER_TOKEN"  # OBO, not SP_TOKEN
    assert sum(obo_spy) >= 1  # the Delta path consulted the request identity


def test_delta_backend_local_mode_skips_obo(monkeypatch):
    """Outside App mode the Delta read keeps the resolved SP token (no OBO)."""
    import back.core.graphdb.delta.DeltaBase as dbase

    captured = _patch_delta(monkeypatch, app_mode=False)

    # A user identity is bound, but local mode must ignore it.
    tok = set_request_identity(
        RequestIdentity(email="u@x", user_token="USER_TOKEN", domain_role="viewer")
    )
    try:
        dbase.create_databricks_client(
            SimpleNamespace(), SimpleNamespace(), for_write=False
        )
    finally:
        reset_request_identity(tok)

    assert captured["token"] == "SP_TOKEN"


def test_delta_build_keeps_service_principal(monkeypatch, obo_spy):
    """A build (for_write) read keeps the SP credentials even with an identity bound."""
    import back.core.graphdb.delta.DeltaBase as dbase

    captured: dict = {}

    class _Client:
        def __init__(
            self, host="", token="", warehouse_id="", use_sea=False, use_cloud_fetch=False
        ):
            captured["token"] = token

    monkeypatch.setattr(dbase, "is_databricks_app", lambda: True)
    monkeypatch.setattr(
        dbase, "get_build_sql_credentials", lambda d, s: ("https://x", "SP_TOKEN", "wh")
    )
    monkeypatch.setattr(dbase, "resolve_build_use_sea", lambda d, s: False)
    monkeypatch.setattr(dbase, "resolve_use_cloud_fetch", lambda d, s: False)
    monkeypatch.setattr("back.core.databricks.DatabricksClient", _Client)

    dbase.create_databricks_client(SimpleNamespace(), SimpleNamespace(), for_write=True)

    assert captured["token"] == "SP_TOKEN"
    assert sum(obo_spy) == 0  # build never consults the request identity


# ── Lakebase Postgres: OBO is NOT applied ─────────────────────────────────


def test_lakebase_backend_never_consults_obo(monkeypatch, obo_spy):
    """Lakebase connects as the SP auth and never reads the request identity."""
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    factory = GraphDBFactory()
    domain = SimpleNamespace(settings={"registry": {}}, info={"name": "Dom"})
    settings = SimpleNamespace(
        registry_catalog="",
        registry_schema="",
        registry_volume="",
        lakebase_schema="ontobricks_registry",
        lakebase_database="",
        registry_volume_path="",
    )
    sp_auth = MagicMock(is_available=True, instance_name="inst", database="ldb")

    with (
        patch("back.core.graphdb.lakebase.LAKEBASE_AVAILABLE", True),
        patch("back.core.databricks.get_lakebase_auth", return_value=sp_auth),
        patch(
            "back.objects.registry.RegistryCfg.from_domain",
            return_value=MagicMock(catalog="c", schema="s", volume="v"),
        ),
        patch(
            "back.core.graphdb.lakebase.LakebaseFlatStore.LakebaseFlatStore",
        ) as mock_lb,
    ):
        mock_lb.return_value = MagicMock()
        factory.create(
            domain, settings, engine="lakebase", engine_config={"schema": "g"}
        )

    # The store is built from the service-principal auth, not any user token…
    assert mock_lb.call_args.args[0] is sp_auth
    # …and the Lakebase path never looked at the request identity.
    assert sum(obo_spy) == 0


# ── Neo4j: OBO is NOT applied ─────────────────────────────────────────────


def _connections_config(name: str = "Aura Prod", **profile_overrides):
    profile = {
        "uri": "neo4j+s://b4810af7.databases.neo4j.io",
        "database": "neo4j",
        "auth_method": "basic",
        "username": "neo4j",
        "password": "test-password-123",
        "name": name,
    }
    profile.update(profile_overrides)
    return {"connections": [profile]}


def test_neo4j_backend_never_consults_obo(monkeypatch, obo_spy):
    """Neo4j connects with its stored Bolt profile, never the request identity."""
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    factory = GraphDBFactory()
    domain = MagicMock()
    domain.info = {"name": "dom", "neo4j_connection": "Lab"}
    domain.current_version = "1"

    store = factory.create(
        domain,
        settings=None,
        engine="neo4j",
        engine_config=_connections_config(name="Lab", database="insurbricks"),
    )

    assert store is not None
    assert store.__class__.__name__ == "Neo4jStore"
    # Database comes from the named profile, not from any OBO identity…
    assert store._database == "insurbricks"
    # …and the Neo4j path never looked at the request identity.
    assert sum(obo_spy) == 0
