"""Search-cache policy for Lakehouse / Lakebase companions."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from back.core.errors import ValidationError
from back.core.graphdb.search_cache import (
    CACHE_BACKEND_MESSAGE,
    CACHE_MISSING_MESSAGE,
    GRAPH_CACHE_STATE_DISABLED,
    GRAPH_CACHE_STATE_PENDING,
    GRAPH_CACHE_STATE_READY,
    apply_search_cache_to_store,
    graph_cache_rebuild_allowed,
    normalize_graph_cache_enabled,
    normalize_graph_cache_state,
    parse_cache_param,
    resolve_search_cache_mode,
    search_cache_usage,
)
from back.core.graphdb.GraphDBBackend import GraphDBBackend

pytestmark = pytest.mark.unit


class _FakeStore(GraphDBBackend):
    supports_adjacency = True
    supports_entity_search = True
    supports_props = True

    def __init__(self, *, adj=True, search=True, props=True):
        self._adj = adj
        self._search = search
        self._props = props

    def create_table(self, table_name: str) -> None:
        return None

    def drop_table(self, table_name: str) -> None:
        return None

    def insert_triples(self, table_name, triples, batch_size=500, on_progress=None):
        return 0

    def query_triples(self, table_name: str):
        return []

    def count_triples(self, table_name: str) -> int:
        return 0

    def table_exists(self, table_name: str) -> bool:
        if "adj" in table_name:
            return self._adj
        if "search" in table_name:
            return self._search
        if "props" in table_name:
            return self._props
        return False

    def get_status(self, table_name: str):
        return {}

    def execute_query(self, query: str):
        return []

    def get_connection(self):
        return None

    def close(self) -> None:
        return None

    def adjacency_table_ids(self, table_name: str):
        return ("g_adj_out", "g_adj_in")

    def entity_search_table_id(self, table_name: str) -> str:
        return "g_entity_search"

    def props_table_id(self, table_name: str) -> str:
        return "g_props"


def _domain(**info):
    return SimpleNamespace(info=dict(info), graph_backend=info.get("graph_backend", "lakebase"))


class TestNormalize:
    def test_enabled_defaults_true(self):
        assert normalize_graph_cache_enabled(None) is True

    def test_legacy_state_missing_is_ready(self):
        assert (
            normalize_graph_cache_state(
                None, enabled=True, default_when_missing=GRAPH_CACHE_STATE_READY
            )
            == GRAPH_CACHE_STATE_READY
        )

    def test_unknown_state_follows_enabled(self):
        assert (
            normalize_graph_cache_state(
                "nope", enabled=True, default_when_missing=GRAPH_CACHE_STATE_READY
            )
            == GRAPH_CACHE_STATE_PENDING
        )
        assert (
            normalize_graph_cache_state(
                "nope", enabled=False, default_when_missing=GRAPH_CACHE_STATE_READY
            )
            == GRAPH_CACHE_STATE_DISABLED
        )


class TestParseCacheParam:
    def test_omitted(self):
        assert parse_cache_param(None) is None
        assert parse_cache_param("") is None

    @pytest.mark.parametrize("raw", ["true", "1", "yes", True])
    def test_true(self, raw):
        assert parse_cache_param(raw) is True

    @pytest.mark.parametrize("raw", ["false", "0", "no", False])
    def test_false(self, raw):
        assert parse_cache_param(raw) is False

    def test_invalid(self):
        with pytest.raises(ValidationError):
            parse_cache_param("maybe")

    def test_query_object_is_omitted(self):
        assert parse_cache_param(object()) is None


class TestResolveMode:
    def test_request_true_wins(self):
        domain = _domain(
            graph_backend="lakebase",
            graph_cache_enabled=False,
            graph_cache_state=GRAPH_CACHE_STATE_DISABLED,
        )
        assert resolve_search_cache_mode(domain, True) == "force_on"

    def test_request_false_wins(self):
        domain = _domain(
            graph_backend="lakebase",
            graph_cache_enabled=True,
            graph_cache_state=GRAPH_CACHE_STATE_READY,
        )
        assert resolve_search_cache_mode(domain, False) == "force_off"

    def test_omitted_ready_is_auto(self):
        domain = _domain(
            graph_backend="databricks",
            graph_cache_enabled=True,
            graph_cache_state=GRAPH_CACHE_STATE_READY,
        )
        assert resolve_search_cache_mode(domain, None) == "auto"

    def test_omitted_pending_is_force_off(self):
        domain = _domain(
            graph_backend="lakebase",
            graph_cache_enabled=True,
            graph_cache_state=GRAPH_CACHE_STATE_PENDING,
        )
        assert resolve_search_cache_mode(domain, None) == "force_off"

    def test_neo4j_omitted_is_force_off(self):
        domain = _domain(graph_backend="neo4j")
        assert resolve_search_cache_mode(domain, None) == "force_off"


class TestApplyToStore:
    def test_force_on_neo4j_store_raises(self):
        store = _FakeStore()
        store.supports_adjacency = False
        domain = _domain(graph_backend="neo4j")
        with pytest.raises(ValidationError, match="Lakehouse"):
            apply_search_cache_to_store(store, domain, True)
        assert CACHE_BACKEND_MESSAGE

    def test_sets_mode_on_store(self):
        store = _FakeStore()
        domain = _domain(
            graph_backend="lakebase",
            graph_cache_enabled=True,
            graph_cache_state=GRAPH_CACHE_STATE_READY,
        )
        apply_search_cache_to_store(store, domain, False)
        assert store.search_cache_mode() == "force_off"


class TestBackendGates:
    def test_force_off_skips_existence_probe(self):
        store = _FakeStore()
        store.set_search_cache_mode("force_off")
        probed = []

        def _exists(name):
            probed.append(name)
            return True

        store.table_exists = _exists
        assert store.adjacency_ready("g") is False
        assert store.entity_search_ready("g") is False
        assert probed == []

    def test_force_on_missing_raises(self):
        store = _FakeStore(adj=False, search=False)
        store.set_search_cache_mode("force_on")
        with pytest.raises(ValidationError, match="rebuild"):
            store.adjacency_ready("g")
        assert CACHE_MISSING_MESSAGE

    def test_auto_uses_tables_when_present(self):
        store = _FakeStore()
        store.set_search_cache_mode("auto")
        assert store.adjacency_ready("g") is True
        assert store.entity_search_ready("g") is True

    def test_usage_force_off(self):
        store = _FakeStore()
        store.set_search_cache_mode("force_off")
        assert search_cache_usage(store, "g") == ("force_off", False)


class TestRebuildAllowed:
    def test_disabled_domain(self):
        domain = _domain(graph_cache_enabled=False)
        assert graph_cache_rebuild_allowed(domain) is False

    def test_enabled_default(self):
        domain = _domain()
        assert graph_cache_rebuild_allowed(domain) is True


class TestDomainSaveTransitions:
    def test_disable_then_reenable_goes_pending(self):
        from back.objects.domain.Domain import Domain

        domain = object.__new__(Domain)
        domain._s = SimpleNamespace(
            info={
                "graph_cache_enabled": True,
                "graph_cache_state": GRAPH_CACHE_STATE_READY,
            }
        )
        Domain._apply_graph_cache_save(domain, {"graph_cache_enabled": False})
        assert domain._s.info["graph_cache_enabled"] is False
        assert domain._s.info["graph_cache_state"] == GRAPH_CACHE_STATE_DISABLED
        Domain._apply_graph_cache_save(domain, {"graph_cache_enabled": True})
        assert domain._s.info["graph_cache_enabled"] is True
        assert domain._s.info["graph_cache_state"] == GRAPH_CACHE_STATE_PENDING


class TestApiContracts:
    def test_find_response_exposes_cache_fields(self):
        from api.routers.digitaltwin import FindResponse

        body = FindResponse(success=True)
        assert body.cache_used is False
        assert body.cache_mode == "auto"

    def test_public_find_declares_cache_query(self):
        src = (
            Path(__file__).resolve().parents[3]
            / "src/api/routers/digitaltwin.py"
        ).read_text(encoding="utf-8")
        assert "cache: Optional[str] = Query(" in src
        assert "cache_used" in src

    def test_graphql_declares_cache_query(self):
        src = (
            Path(__file__).resolve().parents[3]
            / "src/back/fastapi/graphql_routes.py"
        ).read_text(encoding="utf-8")
        assert "parse_cache_param(cache)" in src

    def test_mcp_describe_entity_forwards_cache(self):
        src = (
            Path(__file__).resolve().parents[3]
            / "src/mcp-server/server/tools.py"
        ).read_text(encoding="utf-8")
        assert "cache: Optional[bool] = None" in src
        assert 'params["cache"]' in src
