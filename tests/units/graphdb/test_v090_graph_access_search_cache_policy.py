"""0.9.0 SearchCachePolicy: domain switch, request override, state persistence."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.errors import ValidationError
from back.core.graphdb.SearchCachePolicy import (
    CACHE_BACKEND_MESSAGE,
    GRAPH_CACHE_STATE_DISABLED,
    GRAPH_CACHE_STATE_PENDING,
    GRAPH_CACHE_STATE_READY,
    SearchCachePolicy as P,
)

pytestmark = pytest.mark.unit


def _domain(backend="lakebase", save=None, **info):
    info = {"graph_backend": backend, **info}
    ns = SimpleNamespace(info=info)
    if save is not None:
        ns.save = save
    return ns


class TestNormalizeEnabled:
    @pytest.mark.parametrize("raw", [True, 1, 2.5, "1", "TRUE", " yes ", "On"])
    def test_truthy_values(self, raw):
        assert P.normalize_graph_cache_enabled(raw, default=False) is True

    @pytest.mark.parametrize("raw", [False, 0, 0.0, "0", "false", "No", " off "])
    def test_falsy_values(self, raw):
        assert P.normalize_graph_cache_enabled(raw, default=True) is False

    @pytest.mark.parametrize("raw", ["maybe", "", [1], {"a": 1}])
    def test_unrecognised_values_use_default(self, raw):
        assert P.normalize_graph_cache_enabled(raw, default=True) is True
        assert P.normalize_graph_cache_enabled(raw, default=False) is False


class TestNormalizeState:
    def test_blank_state_uses_default_when_missing(self):
        assert (
            P.normalize_graph_cache_state("  ", enabled=True, default_when_missing="x")
            == "x"
        )

    def test_disabled_switch_forces_disabled_state(self):
        assert (
            P.normalize_graph_cache_state(
                "READY", enabled=False, default_when_missing=GRAPH_CACHE_STATE_READY
            )
            == GRAPH_CACHE_STATE_DISABLED
        )

    def test_enabled_switch_with_stale_disabled_state_becomes_pending(self):
        assert (
            P.normalize_graph_cache_state(
                "disabled", enabled=True, default_when_missing=GRAPH_CACHE_STATE_READY
            )
            == GRAPH_CACHE_STATE_PENDING
        )

    def test_valid_state_is_kept(self):
        assert (
            P.normalize_graph_cache_state(
                "pending_refresh", enabled=True, default_when_missing="ready"
            )
            == GRAPH_CACHE_STATE_PENDING
        )


class TestParseCacheParam:
    @pytest.mark.parametrize("raw,expected", [(1, True), (0, False), (1.0, True)])
    def test_numeric_zero_one(self, raw, expected):
        assert P.parse_cache_param(raw) is expected

    def test_other_numbers_are_treated_as_omitted(self):
        assert P.parse_cache_param(2) is None

    @pytest.mark.parametrize("raw", ["maybe", "2", "truee"])
    def test_garbage_strings_raise(self, raw):
        with pytest.raises(ValidationError, match="cache must be true or false"):
            P.parse_cache_param(raw)

    def test_non_scalar_query_object_means_omitted(self):
        assert P.parse_cache_param(object()) is None


class TestResolveMode:
    def test_request_true_wins_over_disabled_domain(self):
        d = _domain(graph_cache_enabled=False)
        assert P.resolve_search_cache_mode(d, True) == "force_on"

    def test_request_false_wins_over_ready_domain(self):
        d = _domain(graph_cache_enabled=True, graph_cache_state="ready")
        assert P.resolve_search_cache_mode(d, False) == "force_off"

    @pytest.mark.parametrize("backend", ["neo4j", "none"])
    def test_non_sql_backend_is_always_force_off_without_override(self, backend):
        d = _domain(backend=backend, graph_cache_enabled=True)
        assert P.resolve_search_cache_mode(d, None) == "force_off"

    def test_unknown_backend_defaults_to_lakebase_policy(self):
        d = _domain(backend="ladybug", graph_cache_enabled=True)
        assert P.resolve_search_cache_mode(d, None) == "auto"

    @pytest.mark.parametrize("backend", ["lakebase", "databricks"])
    def test_sql_backend_ready_domain_is_auto(self, backend):
        d = _domain(backend=backend, graph_cache_enabled=True, graph_cache_state="ready")
        assert P.resolve_search_cache_mode(d, None) == "auto"

    def test_legacy_domain_without_flags_is_auto(self):
        assert P.resolve_search_cache_mode(_domain(), None) == "auto"

    def test_pending_refresh_domain_is_force_off(self):
        d = _domain(graph_cache_enabled=True, graph_cache_state="pending_refresh")
        assert P.resolve_search_cache_mode(d, None) == "force_off"

    def test_disabled_domain_is_force_off(self):
        d = _domain(graph_cache_enabled="off", graph_cache_state="ready")
        assert P.resolve_search_cache_mode(d, None) == "force_off"

    def test_domain_without_info_dict_is_treated_as_legacy(self):
        d = SimpleNamespace(info=None)
        assert P._domain_info(d) == {}


class _Store:
    def __init__(self, *, mode="auto", adjacency=True, adj=True, search=True):
        self._mode = mode
        self.supports_adjacency = adjacency
        self._adj = adj
        self._search = search
        self.set_calls = []
        self.rebuilt = []

    def search_cache_mode(self):
        return self._mode

    def set_search_cache_mode(self, mode):
        self.set_calls.append(mode)

    def adjacency_ready(self, t):
        return self._adj

    def entity_search_ready(self, t):
        return self._search

    def rebuild_adjacency(self, t):
        self.rebuilt.append(t)


class TestSearchCacheUsage:
    def test_force_on_reports_used(self):
        assert P.search_cache_usage(_Store(mode="force_on"), "g") == ("force_on", True)

    def test_force_off_reports_unused(self):
        assert P.search_cache_usage(_Store(mode="force_off"), "g") == ("force_off", False)

    def test_unknown_mode_is_treated_as_auto(self):
        assert P.search_cache_usage(_Store(mode="weird"), "g") == ("auto", True)

    def test_auto_requires_both_companions(self):
        assert P.search_cache_usage(_Store(adj=True, search=False), "g") == ("auto", False)

    def test_store_without_getters_reports_auto_unused(self):
        assert P.search_cache_usage(object(), "g") == ("auto", False)


class TestApplyToStore:
    def test_force_on_on_non_adjacency_store_raises(self):
        with pytest.raises(ValidationError) as exc:
            P.apply_search_cache_to_store(_Store(adjacency=False), _domain(), True)
        assert exc.value.message == CACHE_BACKEND_MESSAGE

    def test_mode_is_pushed_to_store(self):
        store = _Store()
        assert P.apply_search_cache_to_store(store, _domain(), None) == "auto"
        assert store.set_calls == ["auto"]

    def test_store_without_setter_is_tolerated(self):
        store = SimpleNamespace(supports_adjacency=True)
        assert P.apply_search_cache_to_store(store, _domain(), False) == "force_off"


class TestMarkStates:
    def test_mark_ready_persists_when_enabled(self):
        saved = []
        d = _domain(save=lambda: saved.append(1), graph_cache_state="pending_refresh")
        P.mark_graph_cache_ready(d)
        assert d.info["graph_cache_state"] == GRAPH_CACHE_STATE_READY
        assert saved == [1]

    def test_mark_ready_swallows_save_failure(self):
        def boom():
            raise RuntimeError("disk full")

        d = _domain(save=boom)
        P.mark_graph_cache_ready(d)
        assert d.info["graph_cache_state"] == GRAPH_CACHE_STATE_READY

    def test_mark_ready_noop_when_disabled(self):
        d = _domain(graph_cache_enabled=False, graph_cache_state="disabled")
        P.mark_graph_cache_ready(d)
        assert d.info["graph_cache_state"] == "disabled"

    def test_mark_ready_noop_without_info(self):
        P.mark_graph_cache_ready(SimpleNamespace(info={}))

    def test_mark_pending_when_enabled(self):
        d = _domain()
        P.mark_graph_cache_pending(d)
        assert d.info["graph_cache_state"] == GRAPH_CACHE_STATE_PENDING

    def test_mark_pending_noop_when_disabled_or_no_info(self):
        d = _domain(graph_cache_enabled=False)
        P.mark_graph_cache_pending(d)
        assert "graph_cache_state" not in d.info
        P.mark_graph_cache_pending(SimpleNamespace(info=None))


class TestRebuildIfEnabled:
    def test_none_store_skips(self):
        assert P.rebuild_graph_cache_if_enabled(None, "g", _domain()) is False

    def test_non_adjacency_store_skips(self):
        store = _Store(adjacency=False)
        assert P.rebuild_graph_cache_if_enabled(store, "g", _domain()) is False
        assert store.rebuilt == []

    def test_disabled_domain_skips(self):
        store = _Store()
        d = _domain(graph_cache_enabled=False)
        assert P.rebuild_graph_cache_if_enabled(store, "g", d) is False
        assert store.rebuilt == []

    def test_successful_rebuild_marks_ready(self):
        store = _Store()
        d = _domain(graph_cache_state="pending_refresh")
        assert P.rebuild_graph_cache_if_enabled(store, "g", d) is True
        assert store.rebuilt == ["g"]
        assert d.info["graph_cache_state"] == GRAPH_CACHE_STATE_READY

    def test_failed_rebuild_marks_pending_and_reraises(self):
        store = _Store()

        def fail(t):
            raise RuntimeError("warehouse down")

        store.rebuild_adjacency = fail
        d = _domain(graph_cache_state="ready")
        with pytest.raises(RuntimeError, match="warehouse down"):
            P.rebuild_graph_cache_if_enabled(store, "g", d)
        assert d.info["graph_cache_state"] == GRAPH_CACHE_STATE_PENDING


class TestStateAfterToggle:
    def test_disabling_sets_disabled(self):
        assert (
            P.state_after_enabled_change(previous_enabled=True, enabled=False)
            == GRAPH_CACHE_STATE_DISABLED
        )

    @pytest.mark.parametrize("previous", [True, False])
    def test_enabling_requires_refresh(self, previous):
        assert (
            P.state_after_enabled_change(previous_enabled=previous, enabled=True)
            == GRAPH_CACHE_STATE_PENDING
        )
