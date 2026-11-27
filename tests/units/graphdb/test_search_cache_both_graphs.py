"""The search cache for both graphs, end to end on Delta and Lakebase.

Runs the stores' *real* rebuild and read SQL against the in-memory SQLite
doubles (``tests/fixtures/sqlite_graph_backends.py``) and checks, for the
**Certified Graph** (effective graph: mapped + user edits + derived triples) and
the **Original Graph** (asserted graph: mapped + user edits, no derived triples):

* what each cache (adjacency, props, entity search) contains after a rebuild;
* that reads served from the cache return the right graph;
* that a cached read answers exactly like the uncached (SPO) read;
* that the cache is a snapshot, refreshed by a rebuild, and that a graph without
  a cache still reads correctly.

The scenario (``S1..S4`` are entities, ``C`` / ``D`` types)::

    mapped    S1,S2,S3 : C    S1 label "Alpha"    S1 knows S2    S1 old S2
    reasoning S1 derivedLink S3                    S4 : D          (derived only)
    user      add S2 userLink S3                   delete S1 old S2

so ``knows`` and ``userLink`` are in both graphs, ``derivedLink`` and ``S4`` only
in the Certified Graph, and ``old`` in neither.
"""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg")

from tests.units.graphdb.test_overrides_behaviour import (  # noqa: E402
    DeltaHarness,
    LakebaseHarness,
    T,
)

pytestmark = pytest.mark.unit

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
S1, S2, S3, S4 = (f"http://ex/s{i}" for i in range(1, 5))
C, D = "http://ex/C", "http://ex/D"
KNOWS, OLD, DERIVED, USER, LATER = (
    f"http://ex/{name}" for name in ("knows", "old", "derivedLink", "userLink", "later")
)
EDGE_PREDICATES = {KNOWS, OLD, DERIVED, USER, LATER}

MAPPED = [
    T(S1, RDF_TYPE, C),
    T(S2, RDF_TYPE, C),
    T(S3, RDF_TYPE, C),
    T(S1, RDFS_LABEL, "Alpha"),
    T(S1, KNOWS, S2),
    T(S1, OLD, S2),
]
DERIVED_TRIPLES = [T(S1, DERIVED, S3), T(S4, RDF_TYPE, D)]

ORIGINAL_EDGES = {(S1, KNOWS, S2), (S2, USER, S3)}
CERTIFIED_EDGES = ORIGINAL_EDGES | {(S1, DERIVED, S3)}

_HARNESSES = {"delta": DeltaHarness, "lakebase": LakebaseHarness}


@pytest.fixture(autouse=True)
def _no_policy_rebuild(monkeypatch):
    """Edits must not rebuild the cache behind the test's back."""
    monkeypatch.setattr(
        "back.core.graphdb.search_cache.rebuild_graph_cache_if_enabled",
        lambda *args, **kwargs: None,
    )


def _scenario(h):
    h.seed_mapped(MAPPED)
    h.seed_derived("reasoning", DERIVED_TRIPLES)
    h.store.add_triples(h.table, [T(S2, USER, S3)])
    h.store.delete_triples(h.table, [T(S1, OLD, S2)])
    return h


@pytest.fixture(params=sorted(_HARNESSES))
def h(request):
    """The scenario, cache not built yet."""
    return _scenario(_HARNESSES[request.param]())


@pytest.fixture
def cached(h):
    """The scenario with both caches built."""
    h.store.rebuild_adjacency(h.table)
    return h


def certified(h) -> str:
    return h.store.physical_table_id(h.table) if h.name == "delta" else h.table


def original(h) -> str:
    return h.store.synced_table_name(h.table)


GRAPHS = {"certified": certified, "original": original}


@pytest.fixture(params=sorted(GRAPHS))
def graph(request, h):
    """``(label, relation name)`` of one graph; the cache is not built."""
    return request.param, GRAPHS[request.param](h)


def _query(h, sql):
    return h.client.execute_query(sql) if h.name == "delta" else h.pg.query(sql)


def _triples(rows) -> set[tuple[str, str, str]]:
    return {(r["subject"], r["predicate"], r["object"]) for r in rows}


def _edges(triples) -> set[tuple[str, str, str]]:
    return {t for t in triples if t[1] in EDGE_PREDICATES}


def _adjacency(h, name) -> set[tuple[str, str, str]]:
    adj_out, adj_in = h.store.adjacency_table_ids(name)
    out = {(r["src"], r["predicate"], r["dst"]) for r in _query(h, f"SELECT src, predicate, dst FROM {adj_out}")}
    back = {(r["src"], r["predicate"], r["dst"]) for r in _query(h, f"SELECT src, predicate, dst FROM {adj_in}")}
    assert out == back, "adj_in must mirror adj_out"
    return out


def _props(h, name) -> set[tuple[str, str, str]]:
    props = h.store.props_table_id(name)
    return _triples(_query(h, f"SELECT subject, predicate, object FROM {props}"))


def _search_uris(h, name) -> set[str]:
    store = h.store
    table = (
        store.entity_search_asserted_table_id(name)
        if name == original(h)
        else store.entity_search_table_id(name)
    )
    return {r["uri"] for r in _query(h, f"SELECT uri FROM {table}")}


def _expand(h, name):
    result = h.store.expand_and_fetch_subgraph(name, [S1], 2, 50, 100)
    return _triples(result["results"])


# ---------------------------------------------------------------------------
# What each cache contains
# ---------------------------------------------------------------------------


class TestCompanionContents:
    def test_certified_adjacency_holds_derived_and_user_edges(self, cached):
        assert _adjacency(cached, certified(cached)) == CERTIFIED_EDGES

    def test_original_adjacency_holds_no_derived_edge(self, cached):
        assert _adjacency(cached, original(cached)) == ORIGINAL_EDGES

    def test_a_user_deleted_edge_is_in_neither_adjacency(self, cached):
        for name in (certified(cached), original(cached)):
            assert (S1, OLD, S2) not in _adjacency(cached, name)

    def test_certified_props_hold_derived_triples(self, cached):
        props = _props(cached, certified(cached))
        assert (S1, DERIVED, S3) in props
        assert (S4, RDF_TYPE, D) in props
        assert (S2, USER, S3) in props
        assert (S1, OLD, S2) not in props

    def test_original_props_hold_no_derived_triple(self, cached):
        props = _props(cached, original(cached))
        assert (S1, DERIVED, S3) not in props
        assert (S4, RDF_TYPE, D) not in props
        assert (S2, USER, S3) in props
        assert (S1, KNOWS, S2) in props
        assert (S1, OLD, S2) not in props

    def test_certified_entity_search_includes_the_derived_entity(self, cached):
        assert _search_uris(cached, certified(cached)) == {S1, S2, S3, S4}

    def test_original_entity_search_excludes_the_derived_entity(self, cached):
        assert _search_uris(cached, original(cached)) == {S1, S2, S3}

    def test_the_two_caches_live_in_different_tables(self, cached):
        c_out, c_in = cached.store.adjacency_table_ids(certified(cached))
        o_out, o_in = cached.store.adjacency_table_ids(original(cached))
        assert len({c_out, c_in, o_out, o_in}) == 4
        assert cached.store.props_table_id(certified(cached)) != cached.store.props_table_id(
            original(cached)
        )


# ---------------------------------------------------------------------------
# Reads served from the cache
# ---------------------------------------------------------------------------


class TestReadsServedFromTheCache:
    def test_both_graphs_report_a_ready_cache(self, cached):
        for name in (certified(cached), original(cached)):
            assert cached.store.adjacency_ready(name) is True
            assert cached.store.entity_search_ready(name) is True

    def test_certified_expansion_follows_the_derived_edge(self, cached):
        triples = _expand(cached, certified(cached))
        assert _edges(triples) == CERTIFIED_EDGES

    def test_original_expansion_never_follows_the_derived_edge(self, cached):
        triples = _expand(cached, original(cached))
        assert _edges(triples) == ORIGINAL_EDGES

    def test_neighbours(self, cached):
        certified_n = cached.store.expand_entity_neighbors(certified(cached), [S1])
        original_n = cached.store.expand_entity_neighbors(original(cached), [S1])
        assert set(certified_n) == {S2, S3}
        assert set(original_n) == {S2}

    def test_entities_of_a_derived_only_type(self, cached):
        assert cached.store.find_subjects_by_type(certified(cached), D) == [S4]
        assert cached.store.find_subjects_by_type(original(cached), D) == []

    def test_entities_of_a_mapped_type_are_the_same_in_both(self, cached):
        for name in (certified(cached), original(cached)):
            assert set(cached.store.find_subjects_by_type(name, C)) == {S1, S2, S3}

    def test_preview_seeds(self, cached):
        def uris(name):
            rows = cached.store.find_preview_seeds(name, entity_type=D, limit=10)
            return {r["uri"] for r in rows}

        assert uris(certified(cached)) == {S4}
        assert uris(original(cached)) == set()

    def test_property_payload_of_a_subject(self, cached):
        certified_rows = _triples(cached.store.get_triples_for_subjects(certified(cached), [S1, S4]))
        original_rows = _triples(cached.store.get_triples_for_subjects(original(cached), [S1, S4]))
        assert (S1, DERIVED, S3) in certified_rows
        assert (S4, RDF_TYPE, D) in certified_rows
        assert (S1, DERIVED, S3) not in original_rows
        assert not any(t[0] == S4 for t in original_rows)


# ---------------------------------------------------------------------------
# A cached read answers like the uncached read
# ---------------------------------------------------------------------------


def _read_all(h, name) -> dict[str, object]:
    """Every cache-aware read, as comparable values."""
    store = h.store
    return {
        "expand": _expand(h, name),
        "neighbours": frozenset(store.expand_entity_neighbors(name, [S1, S2])),
        "type_c": frozenset(store.find_subjects_by_type(name, C)),
        "type_d": frozenset(store.find_subjects_by_type(name, D)),
        "seeds": frozenset(
            r["uri"] for r in store.find_preview_seeds(name, entity_type=C, limit=10)
        ),
        "payload": frozenset(_triples(store.get_triples_for_subjects(name, [S1, S2, S3, S4]))),
    }


class TestCachedReadsMatchUncachedReads:
    @pytest.mark.parametrize("which", sorted(GRAPHS))
    def test_every_read_matches_the_spo_answer(self, cached, which):
        name = GRAPHS[which](cached)

        cached.store.set_search_cache_mode("force_off")
        uncached = _read_all(cached, name)
        cached.store.set_search_cache_mode("auto")
        from_cache = _read_all(cached, name)

        assert from_cache == uncached

    @pytest.mark.parametrize("which", sorted(GRAPHS))
    def test_the_comparison_really_uses_the_cache(self, cached, which):
        """Guard: the two runs above differ in the path they take, not just the mode."""
        name = GRAPHS[which](cached)
        cached.store.set_search_cache_mode("force_off")
        assert cached.store.adjacency_ready(name) is False
        cached.store.set_search_cache_mode("auto")
        assert cached.store.adjacency_ready(name) is True


# ---------------------------------------------------------------------------
# No cache, snapshot, refresh
# ---------------------------------------------------------------------------


class TestWithoutACache:
    def test_neither_graph_reports_a_cache_before_the_first_build(self, h):
        for name in (certified(h), original(h)):
            assert h.store.adjacency_ready(name) is False
            assert h.store.entity_search_ready(name) is False

    def test_reads_still_return_the_right_graph(self, h):
        assert _edges(_expand(h, certified(h))) == CERTIFIED_EDGES
        assert _edges(_expand(h, original(h))) == ORIGINAL_EDGES
        assert h.store.find_subjects_by_type(certified(h), D) == [S4]
        assert h.store.find_subjects_by_type(original(h), D) == []

    def test_a_graph_cached_before_the_asserted_companions_falls_back_to_spo(self, cached):
        """A graph built before this layer has only the certified family."""
        name = original(cached)
        adj_out, adj_in = cached.store.adjacency_table_ids(name)
        props = cached.store.props_table_id(name)
        for table in (adj_out, adj_in, props):
            _query(cached, f"DROP TABLE {table}")

        assert cached.store.adjacency_ready(name) is False
        assert _edges(_expand(cached, name)) == ORIGINAL_EDGES
        assert cached.store.adjacency_ready(certified(cached)) is True


class TestSnapshotAndRefresh:
    def test_an_edit_reaches_the_cache_only_at_the_next_refresh(self, cached):
        cached.store.add_triples(cached.table, [T(S3, LATER, S1)])
        later = (S3, LATER, S1)

        # Uncached reads see the edit at once ...
        cached.store.set_search_cache_mode("force_off")
        assert later in _edges(_expand_from(cached, certified(cached), S3))
        assert later in _edges(_expand_from(cached, original(cached), S3))
        # ... the cached snapshot does not, until it is rebuilt.
        cached.store.set_search_cache_mode("auto")
        assert later not in _edges(_expand_from(cached, certified(cached), S3))
        assert later not in _edges(_expand_from(cached, original(cached), S3))

        cached.store.rebuild_adjacency(cached.table)

        assert later in _edges(_expand_from(cached, certified(cached), S3))
        assert later in _edges(_expand_from(cached, original(cached), S3))

    def test_refresh_never_moves_a_derived_triple_into_the_original_graph(self, cached):
        cached.store.rebuild_adjacency(cached.table)
        assert _adjacency(cached, original(cached)) == ORIGINAL_EDGES
        assert _adjacency(cached, certified(cached)) == CERTIFIED_EDGES

    def test_a_full_rebuild_drops_derived_rows_from_the_certified_cache_only(self, cached):
        cached.rebuild(MAPPED)  # what a Build does: derived rows go, user rows stay
        cached.store.rebuild_adjacency(cached.table)

        assert _adjacency(cached, certified(cached)) == ORIGINAL_EDGES
        assert _adjacency(cached, original(cached)) == ORIGINAL_EDGES
        assert _search_uris(cached, certified(cached)) == {S1, S2, S3}

    def test_a_user_deletion_leaves_both_caches_at_the_next_refresh(self, cached):
        cached.store.delete_triples(cached.table, [T(S2, USER, S3)])
        cached.store.rebuild_adjacency(cached.table)

        assert (S2, USER, S3) not in _adjacency(cached, certified(cached))
        assert (S2, USER, S3) not in _adjacency(cached, original(cached))
        assert (S1, DERIVED, S3) in _adjacency(cached, certified(cached))
        assert (S1, DERIVED, S3) not in _adjacency(cached, original(cached))


def _expand_from(h, name, seed):
    result = h.store.expand_and_fetch_subgraph(name, [seed], 1, 50, 100)
    return _triples(result["results"])
