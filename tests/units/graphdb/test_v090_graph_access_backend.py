"""0.9.0 GraphDBBackend companions: cache modes, live-graph fallback, paging."""

from __future__ import annotations

import uuid
from typing import Callable, List, Optional

import pytest

from back.core.errors import ValidationError
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.graphdb.search_cache import CACHE_BACKEND_MESSAGE, CACHE_MISSING_MESSAGE

pytestmark = pytest.mark.unit


class _Store(GraphDBBackend):
    """Minimal SQL backend that records queries and answers via a responder."""

    supports_adjacency = True
    supports_entity_search = True
    supports_props = True

    def __init__(
        self,
        *,
        responder: Optional[Callable[[str], list]] = None,
        existing: Optional[set] = None,
        probe_error: Optional[Exception] = None,
        props_id: Optional[str] = None,
    ):
        self.queries: List[str] = []
        self._responder = responder or (lambda sql: [])
        self._existing = existing if existing is not None else set()
        self._probe_error = probe_error
        self._props_id = props_id if props_id is not None else f"props_{uuid.uuid4().hex}"

    def create_table(self, table_name):
        return None

    def drop_table(self, table_name):
        return None

    def insert_triples(self, table_name, triples, batch_size=500, on_progress=None):
        return 0

    def query_triples(self, table_name):
        return []

    def count_triples(self, table_name):
        return 0

    def table_exists(self, table_name):
        if self._probe_error is not None:
            raise self._probe_error
        return table_name in self._existing

    def get_status(self, table_name):
        return {}

    def execute_query(self, query):
        self.queries.append(query)
        return self._responder(query)

    def get_connection(self):
        return None

    def close(self):
        return None

    def adjacency_table_ids(self, table_name):
        return (f"{table_name}_adj_out", f"{table_name}_adj_in")

    def entity_search_table_id(self, table_name):
        return f"{table_name}_entity_search"

    def entity_search_asserted_table_id(self, table_name):
        return f"{table_name}_entity_search_asserted"

    def props_table_id(self, table_name):
        return self._props_id


class _BareStore(_Store):
    """Backend keeping the base-class companion id defaults."""

    def entity_search_asserted_table_id(self, table_name):
        return GraphDBBackend.entity_search_asserted_table_id(self, table_name)

    def props_table_id(self, table_name):
        return GraphDBBackend.props_table_id(self, table_name)


def _all_companions(table: str, props: str = "") -> set:
    names = {
        f"{table}_adj_out",
        f"{table}_adj_in",
        f"{table}_entity_search",
        f"{table}_entity_search_asserted",
    }
    if props:
        names.add(props)
    return names


# ---------------------------------------------------------------------------
# Search cache mode setter / companion probes
# ---------------------------------------------------------------------------


class TestSearchCacheModeSetter:
    @pytest.mark.parametrize("mode", ["auto", "force_on", "force_off"])
    def test_valid_modes_are_pinned_on_the_instance(self, mode):
        store = _Store()
        store.set_search_cache_mode(mode)
        assert store.search_cache_mode() == mode

    def test_invalid_mode_is_rejected(self):
        store = _Store()
        with pytest.raises(ValidationError, match="Invalid search cache mode"):
            store.set_search_cache_mode("always")
        assert store.search_cache_mode() == "auto"

    def test_mode_is_per_instance_not_class_wide(self):
        a, b = _Store(), _Store()
        a.set_search_cache_mode("force_off")
        assert b.search_cache_mode() == "auto"


class TestCompanionProbeFallback:
    def test_auto_mode_treats_probe_failure_as_not_ready(self):
        store = _Store(probe_error=RuntimeError("warehouse unreachable"))
        assert store.adjacency_ready("g") is False
        assert store.entity_search_ready("g") is False

    def test_force_on_surfaces_probe_failure(self):
        store = _Store(probe_error=RuntimeError("warehouse unreachable"))
        store.set_search_cache_mode("force_on")
        with pytest.raises(RuntimeError, match="warehouse unreachable"):
            store.adjacency_ready("g")

    def test_force_on_with_missing_companions_raises_cache_missing(self):
        store = _Store(existing=set())
        store.set_search_cache_mode("force_on")
        with pytest.raises(ValidationError) as exc:
            store.entity_search_ready("g")
        assert exc.value.message == CACHE_MISSING_MESSAGE

    def test_force_off_never_probes(self):
        store = _Store(probe_error=AssertionError("must not probe"))
        store.set_search_cache_mode("force_off")
        assert store.adjacency_ready("g") is False
        assert store.entity_search_ready("g") is False

    def test_asserted_only_relation_uses_asserted_search_table(self):
        store = _Store(existing={"g_data_entity_search_asserted"})
        assert store.entity_search_ready("cat.sch.g_data") is False
        store = _Store(existing={"cat.sch.g_data_entity_search_asserted"})
        assert store.entity_search_ready("cat.sch.g_data") is True

    def test_base_asserted_search_id_is_empty_so_not_ready(self):
        store = _BareStore(existing={"g_data_entity_search"})
        assert store.entity_search_asserted_table_id("g_data") == ""
        assert store.entity_search_ready("g_data") is False


# ---------------------------------------------------------------------------
# Property companion selection
# ---------------------------------------------------------------------------


class TestPropsTableForRead:
    def test_force_on_without_props_support_raises_backend_message(self):
        store = _Store()
        store.supports_props = False
        store.set_search_cache_mode("force_on")
        with pytest.raises(ValidationError) as exc:
            store._props_table_for_read("g")
        assert exc.value.message == CACHE_BACKEND_MESSAGE

    def test_auto_without_props_support_uses_live_graph(self):
        store = _Store()
        store.supports_props = False
        assert store._props_table_for_read("g") == ""

    def test_force_off_uses_live_graph(self):
        store = _Store()
        store.set_search_cache_mode("force_off")
        assert store._props_table_for_read("g") == ""

    def test_empty_props_id_falls_back_in_auto(self):
        store = _BareStore()
        assert store._props_table_for_read("g") == ""

    def test_empty_props_id_raises_in_force_on(self):
        store = _BareStore()
        store.set_search_cache_mode("force_on")
        with pytest.raises(ValidationError) as exc:
            store._props_table_for_read("g")
        assert exc.value.message == CACHE_MISSING_MESSAGE

    def test_force_on_with_missing_props_table_raises(self):
        store = _Store(existing=set())
        store.set_search_cache_mode("force_on")
        with pytest.raises(ValidationError) as exc:
            store._props_table_for_read("g")
        assert exc.value.message == CACHE_MISSING_MESSAGE

    def test_force_on_with_existing_props_table_returns_it(self):
        props = f"props_{uuid.uuid4().hex}"
        store = _Store(existing={props}, props_id=props)
        store.set_search_cache_mode("force_on")
        assert store._props_table_for_read("g") == props

    def test_auto_returns_props_id_without_probing(self):
        store = _Store(probe_error=AssertionError("must not probe"))
        assert store._props_table_for_read("g") == store.props_table_id("g")


# ---------------------------------------------------------------------------
# Paged triple fetch (BFS payload paging)
# ---------------------------------------------------------------------------


def _page_rows(triples, total):
    if not triples:
        return [{"subject": None, "predicate": None, "object": None, "_ob_total": total}]
    return [
        {"subject": s, "predicate": p, "object": o, "_ob_total": total}
        for s, p, o in triples
    ]


class TestTriplesPageForSubjects:
    def test_empty_subject_list_short_circuits(self):
        store = _Store()
        assert store.get_triples_page_for_subjects("g", [], limit=10) == {
            "rows": [],
            "total": 0,
        }
        assert store.queries == []

    def test_live_graph_page_when_props_unavailable(self):
        store = _Store(
            responder=lambda sql: _page_rows([("s1", "p", "o")], total=7)
        )
        store.set_search_cache_mode("force_off")
        out = store.get_triples_page_for_subjects("g", ["s1"], limit=1, offset=3)
        assert out == {"rows": [{"subject": "s1", "predicate": "p", "object": "o"}], "total": 7}
        sql = store.queries[0]
        assert "FROM g " in sql
        assert "LIMIT 1 OFFSET 3" in sql
        assert "COUNT(*) AS _ob_total" in sql

    def test_offset_past_end_keeps_total_but_drops_null_padding_row(self):
        store = _Store(responder=lambda sql: _page_rows([], total=4))
        store.set_search_cache_mode("force_off")
        out = store.get_triples_page_for_subjects("g", ["s1"], limit=10, offset=10)
        assert out == {"rows": [], "total": 4}

    def test_no_rows_at_all_yields_zero_total(self):
        store = _Store(responder=lambda sql: [])
        store.set_search_cache_mode("force_off")
        assert store.get_triples_page_for_subjects("g", ["s1"], limit=5) == {
            "rows": [],
            "total": 0,
        }

    def test_props_companion_page_used_in_auto(self):
        store = _Store(responder=lambda sql: _page_rows([("s", "p", "o")], 1))
        store.get_triples_page_for_subjects("g", ["s"], limit=5)
        assert f"FROM {store.props_table_id('g')} " in store.queries[0]

    def test_props_missing_error_falls_back_to_live_graph(self):
        props = f"props_{uuid.uuid4().hex}"

        def responder(sql):
            if props in sql:
                raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] {props} does not exist")
            return _page_rows([("s", "p", "o")], 1)

        store = _Store(responder=responder, props_id=props)
        out = store.get_triples_page_for_subjects("g", ["s"], limit=5)
        assert out["total"] == 1
        assert "FROM g " in store.queries[-1]

    def test_subject_quotes_are_escaped(self):
        store = _Store(responder=lambda sql: [])
        store.set_search_cache_mode("force_off")
        store.get_triples_page_for_subjects("g", ["http://x/O'Brien"], limit=5)
        assert "'http://x/O''Brien'" in store.queries[0]


class TestGetTriplesForSubjects:
    def test_force_off_reads_live_graph(self):
        store = _Store(responder=lambda sql: [{"subject": "s"}])
        store.set_search_cache_mode("force_off")
        assert store.get_triples_for_subjects("g", ["s"]) == [{"subject": "s"}]
        assert "FROM g " in store.queries[0]

    def test_auto_reads_props_companion(self):
        store = _Store(responder=lambda sql: [])
        store.get_triples_for_subjects("g", ["s"])
        assert store.props_table_id("g") in store.queries[0]


# ---------------------------------------------------------------------------
# Typed lookup / BFS / preview fallbacks
# ---------------------------------------------------------------------------


class TestFindSubjectsByType:
    def test_uses_asserted_search_table_for_asserted_relation(self):
        table = "cat.sch.g_data"
        store = _Store(
            existing=_all_companions(table),
            responder=lambda sql: [{"uri": "u1"}, {"uri": "u2"}],
        )
        out = store.find_subjects_by_type(table, "http://x#T", limit=2, offset=4)
        assert out == ["u1", "u2"]
        sql = store.queries[0]
        assert f"FROM {table}_entity_search_asserted" in sql
        assert "LIMIT 2 OFFSET 4" in sql

    def test_union_relation_uses_union_search_table(self):
        store = _Store(existing=_all_companions("g"), responder=lambda sql: [])
        store.find_subjects_by_type("g", "http://x#T")
        assert "FROM g_entity_search " in store.queries[0]

    def test_non_missing_error_is_not_swallowed(self):
        def responder(sql):
            raise RuntimeError("permission denied")

        store = _Store(existing=_all_companions("g"), responder=responder)
        with pytest.raises(RuntimeError, match="permission denied"):
            store.find_subjects_by_type("g", "http://x#T")
        assert len(store.queries) == 1

    def test_missing_search_table_falls_back_to_spo(self):
        def responder(sql):
            if "_entity_search" in sql:
                raise RuntimeError("relation g_entity_search does not exist")
            return [{"subject": "s9"}]

        store = _Store(existing=_all_companions("g"), responder=responder)
        assert store.find_subjects_by_type("g", "http://x#T", search="Ab") == ["s9"]
        spo = store.queries[-1]
        assert "LOWER(object) LIKE '%ab%'" in spo
        assert f"predicate = '{RDF_TYPE}'" in spo

    def test_spo_search_value_quotes_are_escaped(self):
        store = _Store(responder=lambda sql: [])
        store.set_search_cache_mode("force_off")
        store.find_subjects_by_type("g", "http://x#T", search="x' OR '1'='1")
        sql = store.queries[0]
        assert "x'' or ''1''=''1" in sql
        assert "x' or '1'='1" not in sql.replace("''", "")


class TestBfsTraversal:
    def test_companion_bfs_uses_asserted_search_table(self):
        table = "g_sync"
        store = _Store(existing=_all_companions(table), responder=lambda sql: [{"entity": "e", "min_lvl": 0}])
        rows = store.bfs_traversal(table, " WHERE 1=1", 1, search="a", entity_type="T")
        assert rows == [{"entity": "e", "min_lvl": 0}]
        assert f"{table}_entity_search_asserted" in store.queries[0]

    def test_companion_bfs_non_missing_error_propagates(self):
        def responder(sql):
            raise RuntimeError("syntax error")

        store = _Store(existing=_all_companions("g"), responder=responder)
        with pytest.raises(RuntimeError, match="syntax error"):
            store.bfs_traversal("g", " WHERE 1=1", 1)

    def test_companion_bfs_missing_table_falls_back_to_spo(self):
        def responder(sql):
            if "_adj_out" in sql:
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND g_adj_out")
            return [{"entity": "e2", "min_lvl": 1}]

        store = _Store(existing=_all_companions("g"), responder=responder)
        assert store.bfs_traversal("g", " WHERE 1=1", 1) == [{"entity": "e2", "min_lvl": 1}]
        assert len(store.queries) == 2


class TestFindPreviewSeeds:
    def test_missing_search_table_falls_back_to_spo_metadata(self):
        def responder(sql):
            if "_entity_search" in sql:
                raise RuntimeError("undefined table g_entity_search")
            if "IN ('" in sql and RDFS_LABEL in sql:
                return [
                    {"subject": "http://x/b", "predicate": RDF_TYPE, "object": "http://x#T"},
                    {"subject": "http://x/b", "predicate": RDFS_LABEL, "object": "Bee"},
                ]
            return [{"subject": "http://x/b"}]

        store = _Store(existing=_all_companions("g"), responder=responder)
        rows = store.find_preview_seeds("g", value="be", limit=5)
        assert rows == [{"uri": "http://x/b", "type": "http://x#T", "label": "Bee"}]

    def test_non_missing_error_on_search_table_propagates(self):
        def responder(sql):
            raise RuntimeError("quota exceeded")

        store = _Store(existing=_all_companions("g"), responder=responder)
        with pytest.raises(RuntimeError, match="quota exceeded"):
            store.find_preview_seeds("g", value="be", limit=5)


class TestEntityMetadata:
    def test_empty_subjects_returns_empty_without_query(self):
        store = _Store()
        assert store.get_entity_metadata("g", []) == []
        assert store.queries == []

    def test_single_round_trip_splits_type_and_label_and_keeps_first(self):
        rows = [
            {"subject": "a", "predicate": RDF_TYPE, "object": "T1"},
            {"subject": "a", "predicate": RDF_TYPE, "object": "T2"},
            {"subject": "a", "predicate": RDFS_LABEL, "object": "Alpha"},
            {"subject": "a", "predicate": RDFS_LABEL, "object": "Other"},
            {"subject": "b", "predicate": RDFS_LABEL, "object": "Untyped"},
            {"subject": "c", "predicate": RDF_TYPE, "object": "T3"},
        ]
        store = _Store(responder=lambda sql: rows)
        out = store.get_entity_metadata("g", ["a", "b", "c"])
        assert out == [
            {"uri": "a", "type": "T1", "label": "Alpha"},
            {"uri": "c", "type": "T3", "label": ""},
        ]
        assert len(store.queries) == 1
        assert f"predicate IN ('{RDF_TYPE}', '{RDFS_LABEL}')" in store.queries[0]

    def test_none_result_is_tolerated(self):
        store = _Store(responder=lambda sql: None)
        assert store.get_entity_metadata("g", ["a"]) == []


class TestAggregateStats:
    def test_default_distinct_count_is_exact(self):
        store = _Store()
        assert store._distinct_count_expr("subject") == "COUNT(DISTINCT subject)"
