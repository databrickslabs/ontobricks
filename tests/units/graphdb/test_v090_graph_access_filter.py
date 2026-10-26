"""0.9.0 Explorer filter (GraphFilter), find paging (GraphFind), and seed SQL escaping."""

from __future__ import annotations

import sys

import pytest

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.graphdb.entity_search import (
    entity_search_seed_sql,
    preview_select_sql,
)
from back.core.helpers import sql_escape
from back.objects.digitaltwin.explorer.GraphFilter import (
    EXPLORER_MAX_ENTITIES_DEFAULT,
    EXPLORER_MAX_ENTITIES_MIN,
    EXPLORER_MAX_ENTITIES_UI_MAX,
    GraphFilter,
)
from back.objects.digitaltwin.explorer.GraphFind import GraphFind

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------


class _PreviewStore:
    def __init__(self, rows=None, exc=None):
        self.rows = rows or []
        self.exc = exc
        self.calls = []

    def find_preview_seeds(self, graph_name, **kw):
        self.calls.append((graph_name, kw))
        if self.exc is not None:
            raise self.exc
        return self.rows


class TestClampMaxEntities:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            (None, EXPLORER_MAX_ENTITIES_DEFAULT),
            ("abc", EXPLORER_MAX_ENTITIES_DEFAULT),
            (1, EXPLORER_MAX_ENTITIES_MIN),
            ("750", 750),
            (10**9, EXPLORER_MAX_ENTITIES_UI_MAX),
        ],
    )
    def test_clamps_to_ui_range(self, raw, expected):
        assert GraphFilter.clamp_max_entities(raw) == expected


class TestPreview:
    def test_probes_one_more_than_max_preview(self):
        store = _PreviewStore()
        GraphFilter.preview(store, "g", "T", "label", "contains", "x", max_preview=3)
        assert store.calls[0][1]["limit"] == 4

    def test_empty_result_message(self):
        out = GraphFilter.preview(_PreviewStore(), "g", "", "any", "contains", "zz")
        assert out["seeds"] == [] and out["total"] == 0 and out["capped"] is False
        assert "No entities found" in out["message"]

    def test_capped_when_probe_returns_more_than_max(self):
        rows = [
            {"uri": f"http://x/T/{i}", "type": "http://x#T", "label": f"L{i}"}
            for i in range(4)
        ]
        out = GraphFilter.preview(
            _PreviewStore(rows), "g", "", "any", "contains", "", max_preview=3
        )
        assert out["capped"] is True
        assert out["total"] == 4
        assert len(out["seeds"]) == 3

    def test_not_capped_at_exact_boundary(self):
        rows = [{"uri": "http://x/a", "type": "", "label": ""}]
        out = GraphFilter.preview(
            _PreviewStore(rows), "g", "", "any", "contains", "", max_preview=1
        )
        assert out["capped"] is False
        seed = out["seeds"][0]
        assert seed["type"] == "Unknown"
        assert seed["label"] == "a"

    def test_seeds_sorted_by_type_then_label(self):
        rows = [
            {"uri": "u1", "type": "http://x#B", "label": "a"},
            {"uri": "u2", "type": "http://x#A", "label": "z"},
            {"uri": "u3", "type": "http://x#A", "label": "b"},
        ]
        out = GraphFilter.preview(_PreviewStore(rows), "g", "", "any", "contains", "")
        assert [s["uri"] for s in out["seeds"]] == ["u3", "u2", "u1"]

    @pytest.mark.parametrize(
        "exc", [ValidationError("bad"), InfrastructureError("down"), NotFoundError("nf")]
    )
    def test_domain_errors_propagate_unchanged(self, exc):
        with pytest.raises(type(exc)) as raised:
            GraphFilter.preview(_PreviewStore(exc=exc), "g", "", "any", "contains", "")
        assert raised.value is exc

    def test_missing_graph_maps_to_not_found(self):
        store = _PreviewStore(exc=RuntimeError("Table g does not exist"))
        with pytest.raises(NotFoundError, match="Run Build first"):
            GraphFilter.preview(store, "g", "", "any", "contains", "")

    def test_other_errors_map_to_infrastructure(self):
        store = _PreviewStore(exc=RuntimeError("timeout"))
        with pytest.raises(InfrastructureError, match="Error querying graph"):
            GraphFilter.preview(store, "g", "", "any", "contains", "")


# ---------------------------------------------------------------------------
# Expand (BFS fallback path without single-statement expansion)
# ---------------------------------------------------------------------------


class _ExpandStore:
    def __init__(self, graph=None, triples_per_subject=1, neighbor_exc=None, fetch_exc=None):
        self.graph = graph or {}
        self.triples_per_subject = triples_per_subject
        self.neighbor_exc = neighbor_exc
        self.fetch_exc = fetch_exc
        self.neighbor_calls = 0
        self.fetch_batches = []

    def expand_entity_neighbors(self, graph_name, level):
        self.neighbor_calls += 1
        if self.neighbor_exc is not None:
            raise self.neighbor_exc
        out = set(level)
        for uri in level:
            out |= set(self.graph.get(uri, ()))
        return out

    def get_triples_for_subjects(self, graph_name, subjects):
        self.fetch_batches.append(list(subjects))
        if self.fetch_exc is not None:
            raise self.fetch_exc
        return [
            {"subject": s, "predicate": f"p{i}", "object": "o"}
            for s in subjects
            for i in range(self.triples_per_subject)
        ]


class TestExpand:
    def test_single_statement_expand_is_preferred(self):
        class S:
            def expand_and_fetch_subgraph(self, g, uris, depth, max_e, max_t):
                self.args = (g, sorted(uris), depth, max_e, max_t)
                return {"results": [], "count": 0, "capped": False}

        s = S()
        out = GraphFilter.expand(s, "g", ["a", "a", "b"], include_rels=False, depth=3)
        assert s.args == ("g", ["a", "b"], 0, 5000, 100_000)
        assert out["phase"] == "expand"
        assert out["initial_count"] == 2
        assert out["columns"] == ["subject", "predicate", "object"]

    def test_bfs_stops_when_no_new_neighbors(self):
        store = _ExpandStore(graph={"a": ["b"], "b": ["a"]})
        out = GraphFilter.expand(store, "g", ["a"], depth=5)
        assert out["expanded_count"] == 2
        assert out["capped"] is False
        assert store.neighbor_calls == 2

    def test_bfs_caps_at_max_entities(self):
        store = _ExpandStore(graph={"a": ["b", "c", "d", "e"]})
        out = GraphFilter.expand(store, "g", ["a"], depth=3, max_entities=3)
        assert out["expanded_count"] == 3
        assert out["capped"] is True
        assert store.neighbor_calls == 1

    def test_seeds_already_at_max_skip_expansion(self):
        store = _ExpandStore(graph={"a": ["z"]})
        out = GraphFilter.expand(store, "g", ["a", "b"], depth=3, max_entities=2)
        assert store.neighbor_calls == 0
        assert out["expanded_count"] == 2
        assert out["capped"] is False

    def test_neighbor_failure_keeps_seed_subgraph(self):
        store = _ExpandStore(neighbor_exc=RuntimeError("boom"))
        out = GraphFilter.expand(store, "g", ["a"], depth=2)
        assert out["expanded_count"] == 1
        assert out["count"] == 1

    def test_fetch_is_batched(self):
        store = _ExpandStore()
        GraphFilter.expand(
            store, "g", ["a", "b", "c"], include_rels=False, batch_size=2
        )
        assert sorted(len(b) for b in store.fetch_batches) == [1, 2]

    def test_triple_budget_truncates_and_caps(self):
        store = _ExpandStore(triples_per_subject=3)
        out = GraphFilter.expand(
            store, "g", ["a", "b"], include_rels=False, batch_size=1, max_triples=4
        )
        assert out["count"] == 4
        assert out["capped"] is True
        assert len(store.fetch_batches) == 2

    def test_time_budget_caps(self, monkeypatch):
        calls = {"n": 0}

        def fake_monotonic():
            calls["n"] += 1
            return 0.0 if calls["n"] <= 2 else 100.0

        module = sys.modules[GraphFilter.__module__]
        monkeypatch.setattr(module, "time", type("T", (), {"monotonic": staticmethod(fake_monotonic)}))
        store = _ExpandStore()
        out = GraphFilter.expand(
            store, "g", ["a", "b"], include_rels=False, batch_size=1, max_fetch_seconds=1
        )
        assert out["timeout_capped"] is True
        assert out["capped"] is True
        assert len(store.fetch_batches) == 1

    def test_domain_errors_from_fetch_propagate(self):
        err = NotFoundError("gone")
        with pytest.raises(NotFoundError):
            GraphFilter.expand(_ExpandStore(fetch_exc=err), "g", ["a"], include_rels=False)

    def test_unexpected_fetch_error_maps_to_infrastructure(self):
        with pytest.raises(InfrastructureError, match="Error fetching triples"):
            GraphFilter.expand(
                _ExpandStore(fetch_exc=RuntimeError("x")), "g", ["a"], include_rels=False
            )


# ---------------------------------------------------------------------------
# Find paging (has_more)
# ---------------------------------------------------------------------------


class _FindStore:
    def __init__(self, bfs_rows, total, page_size):
        self.bfs_rows = bfs_rows
        self.total = total
        self.page_size = page_size
        self.page_args = None

    def bfs_traversal(self, table, seed_where, depth, search="", entity_type=""):
        self.seed_where = seed_where
        return self.bfs_rows

    def find_subjects_by_patterns(self, table, patterns):
        return set()

    def get_triples_page_for_subjects(self, table, subjects, *, limit, offset):
        self.page_args = (limit, offset)
        n = max(0, min(self.page_size, self.total - offset))
        return {"rows": [{"subject": "s"}] * n, "total": self.total}


class TestFindPaging:
    _rows = [{"entity": "http://x/T/1", "min_lvl": 0}, {"entity": "http://x/T/2", "min_lvl": 1}]

    def test_no_seed_match_has_no_more(self):
        out = GraphFind.find_triples_bfs(_FindStore([], 0, 0), "g", search="x", limit=5)
        assert out["has_more"] is False and out["total"] == 0

    @pytest.mark.parametrize(
        "total,limit,offset,has_more",
        [(10, 5, 0, True), (10, 5, 5, False), (11, 5, 5, True), (5, 5, 0, False), (3, 5, 10, False)],
    )
    def test_has_more_boundaries(self, total, limit, offset, has_more):
        store = _FindStore(self._rows, total, limit)
        out = GraphFind.find_triples_bfs(store, "g", search="x", limit=limit, offset=offset)
        assert out["has_more"] is has_more
        assert out["total"] == total
        assert store.page_args == (limit, offset)
        assert out["seed_count"] == 1

    def test_seed_where_escapes_quotes(self):
        where = GraphFind.build_find_seed_where("g", entity_type="O'T", search="it's")
        assert "o''t" in where
        assert "it''s" in where


# ---------------------------------------------------------------------------
# Search value escaping in entity-search SQL
# ---------------------------------------------------------------------------


class TestSearchValueEscaping:
    def test_quote_is_doubled_in_preview_sql(self):
        sql = preview_select_sql(
            search_table="t", entity_type="http://x#T'", field="any",
            match_type="contains", value="a' OR 1=1 --", limit=5, escape=sql_escape,
        )
        assert "type_uri = 'http://x#T'''" in sql
        assert "LIKE '%a'' or 1=1 --%'" in sql

    def test_backslash_is_escaped(self):
        sql = preview_select_sql(
            search_table="t", entity_type="", field="label", match_type="exact",
            value="a\\", limit=1, escape=sql_escape,
        )
        assert "label_lc = 'a\\\\'" in sql

    @pytest.mark.parametrize(
        "match_type,expected",
        [("exact", "= 'ab'"), ("starts", "LIKE 'ab%'"), ("ends", "LIKE '%ab'"), ("contains", "LIKE '%ab%'")],
    )
    def test_match_types(self, match_type, expected):
        sql = preview_select_sql(
            search_table="t", entity_type="", field="id", match_type=match_type,
            value="AB", limit=1, escape=sql_escape,
        )
        assert f"uri_lc {expected}" in sql
        assert "label_lc" not in sql

    def test_unknown_field_yields_no_text_clause(self):
        sql = preview_select_sql(
            search_table="t", entity_type="", field="nope", match_type="exact",
            value="AB", limit=1, escape=sql_escape,
        )
        assert "WHERE" not in sql

    def test_non_positive_limit_is_rejected(self):
        with pytest.raises(ValueError):
            preview_select_sql(
                search_table="t", entity_type="", field="any", match_type="contains",
                value="", limit=0, escape=sql_escape,
            )

    def test_seed_sql_entity_type_is_local_name_suffix(self):
        sql = entity_search_seed_sql(search_table="t", entity_type="Cust'", escape=sql_escape)
        assert "LIKE '%#cust'''" in sql and "LIKE '%/cust'''" in sql

    def test_like_wildcards_in_search_value_are_escaped(self):
        sql = preview_select_sql(
            search_table="t", entity_type="", field="label", match_type="contains",
            value="a_b%", limit=1, escape=sql_escape,
        )
        assert "label_lc LIKE '%a!_b!%%' ESCAPE '!'" in sql

    def test_like_escape_character_in_search_value_is_doubled(self):
        sql = preview_select_sql(
            search_table="t", entity_type="", field="label", match_type="starts",
            value="hi!", limit=1, escape=sql_escape,
        )
        assert "label_lc LIKE 'hi!!%' ESCAPE '!'" in sql

    def test_exact_match_keeps_wildcards_verbatim(self):
        sql = preview_select_sql(
            search_table="t", entity_type="", field="label", match_type="exact",
            value="a_b", limit=1, escape=sql_escape,
        )
        assert "label_lc = 'a_b'" in sql
