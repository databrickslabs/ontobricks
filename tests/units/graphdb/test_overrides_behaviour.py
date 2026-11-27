"""Overrides layer, end to end: what an edit writes and what the graphs return.

Unlike the statement-shape tests next to it, these run the stores' *real* SQL
(add / delete / modify, the ``_overrides`` table, the ``_graph`` and
``_asserted`` views) against in-memory SQLite doubles (see
``tests/fixtures/sqlite_graph_backends.py``). Every scenario runs on both the
Delta and the Lakebase store and must give the same answer, because the
overrides contract is the same on every backend.

Two questions are asked each time:

* **Table**: which rows did the edit leave in ``_overrides``?
* **Graphs**: what do the effective graph (mapped + adds - dels, derived rows
  included) and the asserted graph (derived rows left out) return afterwards?
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock

import pytest

pytest.importorskip("psycopg")

from back.core.errors import ValidationError
from back.core.graphdb.delta import materialize
from back.core.graphdb.delta.MaterializedStore import MaterializedStore
from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase.FlatStore import FlatStore
from tests.fixtures.sqlite_graph_backends import SqliteDeltaWarehouse, SqlitePostgres

pytestmark = pytest.mark.unit

BASE = "http://ex/"
S1, S2 = "http://ex/s1", "http://ex/s2"
P, Q = "http://ex/p", "http://ex/q"

DELTA_DATA = "cat.sch.triplestore_dom_V1_data"
DELTA_OVR = "cat.sch.triplestore_dom_V1_overrides"
DELTA_GRAPH = "cat.sch.triplestore_dom_V1_graph"
DELTA_ASSERTED = "cat.sch.triplestore_dom_V1_asserted"
DELTA_LEGACY = "cat.sch.triplestore_dom_V1_inferred"

PG_TABLE = "G_V1"
PG_SYNC = _companion_ddl.synced_phy(PG_TABLE)
PG_OVR = _companion_ddl.companion_phy(PG_TABLE)
PG_VIEW = _companion_ddl.view_phy(PG_TABLE)
PG_ASSERTED = _companion_ddl.asserted_phy(PG_TABLE)

_OVERRIDE_COLUMNS = "subject, predicate, object, op, origin, change_id, author"


def T(subject: str, predicate: str, obj: str) -> dict[str, str]:
    return {"subject": subject, "predicate": predicate, "object": obj}


def K(subject: str, predicate: str, obj: str) -> tuple[str, str, str]:
    return (subject, predicate, obj)


def _keys(rows) -> set[tuple[str, str, str]]:
    return {K(r["subject"], r["predicate"], r["object"]) for r in rows}


def _domain():
    domain = MagicMock()
    domain.info = {"name": "dom"}
    domain.current_version = 1
    domain.delta = {"catalog": "cat", "schema": "sch"}
    domain.ontology = {"base_uri": BASE}
    return domain


# ---------------------------------------------------------------------------
# Backend harnesses: the same small surface over both stores
# ---------------------------------------------------------------------------


class DeltaHarness:
    name = "delta"
    table = "dom_V1"

    def __init__(self, *, built: bool = True) -> None:
        self.client = SqliteDeltaWarehouse()
        self.store = MaterializedStore(self.client, domain=_domain())
        self.store._mark_cache_stale = MagicMock()
        self._raw = MaterializedStore(self.client)
        self._raw.create_table(DELTA_DATA)
        if built:
            self.store._ensure_companion_objects(self.table)

    def seed_mapped(self, triples) -> None:
        self._raw.insert_triples(DELTA_DATA, triples)

    def seed_derived(self, origin: str, triples) -> None:
        self.store.insert_triples(self.table, triples, origin=origin)

    def rebuild(self, mapped) -> None:
        """What a full Build does: drop derived rows, reload the mapped data."""
        materialize.clear_derived(self.client, DELTA_OVR)
        materialize.truncate_table(self.client, DELTA_DATA)
        self.seed_mapped(mapped)

    def mapped(self):
        return _keys(self.client.rows(DELTA_DATA))

    def graph(self):
        return _keys(self.store.query_triples(self.store.physical_table_id(self.table)))

    def asserted(self):
        return _keys(self.store.query_triples(self.store.synced_table_name(self.table)))

    def count(self) -> int:
        return self.store.count_triples(self.store.physical_table_id(self.table))

    def rows(self):
        return self.client.rows(DELTA_OVR, _OVERRIDE_COLUMNS)


class LakebaseHarness:
    name = "lakebase"
    table = PG_TABLE

    def __init__(self, *, built: bool = True) -> None:
        self.pg = SqlitePostgres()
        self.pg.run(
            f"CREATE TABLE {PG_SYNC} (subject TEXT NOT NULL, predicate TEXT NOT NULL, "
            "object TEXT NOT NULL, PRIMARY KEY (subject, predicate, object))"
        )
        auth = MagicMock()
        auth.database = "appdb"
        self.store = FlatStore(auth, schema="ontobricks_graph")
        self.store._cursor = self.pg.cursor
        self.store._txn_cursor = self.pg.transaction
        self.store.execute_query = self.pg.query
        self.store._mark_cache_stale = MagicMock()
        if built:
            self._create_overrides_layer()
            self.store._override_ready.add(self.table)

    def _create_overrides_layer(self) -> None:
        """The production DDL, minus what SQLite lacks (``digest()`` generated column)."""
        columns = _companion_ddl._OVERRIDE_TABLE_COLUMNS
        columns = re.sub(
            r"object_hash BYTEA GENERATED ALWAYS AS \(.*?\) STORED,", "", columns, flags=re.S
        )
        columns = columns.replace("object_hash", "object").replace(
            "TIMESTAMPTZ NOT NULL DEFAULT now()", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
        )
        self.pg.run(f"CREATE TABLE {PG_OVR} ({columns})")
        self.pg.run(_companion_ddl.union_view_sql(PG_VIEW, PG_SYNC, PG_OVR, asserted=False))
        self.pg.run(_companion_ddl.union_view_sql(PG_ASSERTED, PG_SYNC, PG_OVR, asserted=True))

    def seed_mapped(self, triples) -> None:
        for t in triples:
            self.pg.run(
                f"INSERT INTO {PG_SYNC} (subject, predicate, object) VALUES (%s, %s, %s)",
                (t["subject"], t["predicate"], t["object"]),
            )

    def seed_derived(self, origin: str, triples) -> None:
        for t in triples:
            self.pg.run(
                f"INSERT INTO {PG_OVR} (subject, predicate, object, op, origin) "
                "VALUES (%s, %s, %s, 'add', %s)",
                (t["subject"], t["predicate"], t["object"], origin),
            )

    def rebuild(self, mapped) -> None:
        """What ``reset_for_rebuild`` + a reload do: derived rows go, user rows stay."""
        with self.pg.cursor() as cur:
            _companion_ddl.clear_derived(cur, PG_OVR)
        self.pg.run(f"DELETE FROM {PG_SYNC}")
        self.seed_mapped(mapped)

    def mapped(self):
        return _keys(self.pg.query(f"SELECT subject, predicate, object FROM {PG_SYNC}"))

    def graph(self):
        return _keys(self.store.query_triples(self.table))

    def asserted(self):
        return _keys(self.store.query_triples(self.store.synced_table_name(self.table)))

    def count(self) -> int:
        return self.store.count_triples(self.table)

    def rows(self):
        return self.pg.query(f"SELECT {_OVERRIDE_COLUMNS} FROM {PG_OVR}")


_HARNESSES = {"delta": DeltaHarness, "lakebase": LakebaseHarness}


@pytest.fixture(autouse=True)
def _no_search_cache(monkeypatch):
    monkeypatch.setattr(
        "back.core.graphdb.search_cache.rebuild_graph_cache_if_enabled",
        lambda *args, **kwargs: None,
    )


@pytest.fixture(params=sorted(_HARNESSES))
def h(request):
    """A built graph (overrides table and views exist) on each backend."""
    return _HARNESSES[request.param]()


@pytest.fixture(params=sorted(_HARNESSES))
def unbuilt(request):
    """A graph whose overrides layer was never created, on each backend."""
    return _HARNESSES[request.param](built=False)


def override_rows(h) -> list[tuple[str, str, str, str, str]]:
    """``(op, origin, subject, predicate, object)`` of every ``_overrides`` row."""
    return sorted(
        (r["op"], r["origin"], r["subject"], r["predicate"], r["object"]) for r in h.rows()
    )


def breakdown(h) -> dict[str, tuple[int, int]]:
    """``{category: (triples, entities)}`` of the Overrides tab."""
    return {
        c["category"]: (c["triples"], c["entities"])
        for c in h.store.get_inferred_breakdown(h.table, BASE)
    }


# ---------------------------------------------------------------------------
# Add
# ---------------------------------------------------------------------------


class TestAdd:
    def test_a_new_triple_lands_in_the_table_and_in_both_graphs(self, h):
        h.seed_mapped([T(S1, P, "1")])

        handled = h.store.add_triples(h.table, [T(S1, P, "2")], author="ann", change_id="c1")

        assert handled == 1
        assert override_rows(h) == [("add", "user", S1, P, "2")]
        row = h.rows()[0]
        assert (row["change_id"], row["author"]) == ("c1", "ann")
        assert h.graph() == {K(S1, P, "1"), K(S1, P, "2")}
        assert h.asserted() == h.graph()

    def test_the_mapped_data_is_never_written(self, h):
        h.seed_mapped([T(S1, P, "1")])

        h.store.add_triples(h.table, [T(S1, P, "2")])

        assert h.mapped() == {K(S1, P, "1")}

    def test_a_triple_already_in_the_graph_writes_nothing(self, h):
        h.seed_mapped([T(S1, P, "1")])

        h.store.add_triples(h.table, [T(S1, P, "1")])

        assert override_rows(h) == []
        assert h.graph() == {K(S1, P, "1")}
        assert h.count() == 1

    def test_adding_twice_is_idempotent(self, h):
        h.store.add_triples(h.table, [T(S1, P, "1")])
        h.store.add_triples(h.table, [T(S1, P, "1")])

        assert override_rows(h) == [("add", "user", S1, P, "1")]
        assert h.count() == 1

    def test_duplicates_in_one_batch_write_one_row(self, h):
        handled = h.store.add_triples(h.table, [T(S1, P, "1"), T(S1, P, "1"), T(S2, P, "1")])

        assert handled == 2
        assert len(h.rows()) == 2

    def test_adding_a_visible_derived_triple_does_not_duplicate_it(self, h):
        h.seed_derived("reasoning", [T(S1, Q, "x")])

        h.store.add_triples(h.table, [T(S1, Q, "x")])

        assert override_rows(h) == [("add", "reasoning", S1, Q, "x")]
        assert h.count() == 1

    def test_adding_a_hidden_triple_restores_it_without_a_new_row(self, h):
        h.seed_mapped([T(S1, P, "1")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])
        assert h.graph() == set()

        h.store.add_triples(h.table, [T(S1, P, "1")])

        assert override_rows(h) == []
        assert h.graph() == {K(S1, P, "1")}
        assert h.asserted() == {K(S1, P, "1")}

    def test_quotes_and_backslashes_round_trip(self, h):
        value = "O'Brien \\ \"x\""

        h.store.add_triples(h.table, [T(S1, P, value)])

        assert h.graph() == {K(S1, P, value)}


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


class TestDelete:
    def test_a_mapped_triple_is_hidden_from_both_graphs(self, h):
        h.seed_mapped([T(S1, P, "1"), T(S1, P, "2")])

        h.store.delete_triples(h.table, [T(S1, P, "1")], author="ann", change_id="c9")

        assert override_rows(h) == [("del", "user", S1, P, "1")]
        assert h.graph() == {K(S1, P, "2")}
        assert h.asserted() == {K(S1, P, "2")}
        assert h.mapped() == {K(S1, P, "1"), K(S1, P, "2")}  # source untouched
        row = h.rows()[0]
        assert (row["change_id"], row["author"]) == ("c9", "ann")

    def test_a_user_added_triple_is_dropped_without_a_del_row(self, h):
        h.store.add_triples(h.table, [T(S1, P, "1")])

        h.store.delete_triples(h.table, [T(S1, P, "1")])

        assert override_rows(h) == []
        assert h.graph() == set()

    def test_a_derived_triple_is_hidden_from_the_graph_only(self, h):
        h.seed_mapped([T(S1, P, "1")])
        h.seed_derived("reasoning", [T(S1, Q, "x")])
        assert h.graph() == {K(S1, P, "1"), K(S1, Q, "x")}
        assert h.asserted() == {K(S1, P, "1")}

        h.store.delete_triples(h.table, [T(S1, Q, "x")])

        assert h.graph() == {K(S1, P, "1")}
        assert h.asserted() == {K(S1, P, "1")}
        assert override_rows(h) == [
            ("add", "reasoning", S1, Q, "x"),
            ("del", "user", S1, Q, "x"),
        ]

    def test_an_unknown_triple_writes_nothing(self, h):
        h.seed_mapped([T(S1, P, "1")])

        h.store.delete_triples(h.table, [T(S2, P, "nope")])

        assert override_rows(h) == []
        assert h.graph() == {K(S1, P, "1")}

    def test_deleting_twice_writes_one_del_row(self, h):
        h.seed_mapped([T(S1, P, "1")])

        h.store.delete_triples(h.table, [T(S1, P, "1")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])

        assert override_rows(h) == [("del", "user", S1, P, "1")]

    def test_a_mixed_batch_handles_each_triple_by_its_origin(self, h):
        h.seed_mapped([T(S1, P, "mapped")])
        h.store.add_triples(h.table, [T(S1, P, "mine")])

        h.store.delete_triples(
            h.table, [T(S1, P, "mapped"), T(S1, P, "mine"), T(S1, P, "ghost")]
        )

        assert override_rows(h) == [("del", "user", S1, P, "mapped")]
        assert h.graph() == set()

    def test_delete_then_add_round_trips(self, h):
        h.seed_mapped([T(S1, P, "1")])

        h.store.delete_triples(h.table, [T(S1, P, "1")])
        h.store.add_triples(h.table, [T(S1, P, "1")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])

        assert override_rows(h) == [("del", "user", S1, P, "1")]
        assert h.graph() == set()


# ---------------------------------------------------------------------------
# Modify
# ---------------------------------------------------------------------------


class TestModify:
    def test_a_mapped_value_is_replaced_by_one_change(self, h):
        h.seed_mapped([T(S1, P, "old")])

        change_id = h.store.modify_triple(h.table, T(S1, P, "old"), T(S1, P, "new"), author="ann")

        assert override_rows(h) == [
            ("add", "user", S1, P, "new"),
            ("del", "user", S1, P, "old"),
        ]
        assert {r["change_id"] for r in h.rows()} == {change_id}
        assert change_id
        assert {r["author"] for r in h.rows()} == {"ann"}
        assert h.graph() == {K(S1, P, "new")}
        assert h.asserted() == {K(S1, P, "new")}
        assert h.mapped() == {K(S1, P, "old")}

    def test_a_user_added_value_is_replaced_without_a_del_row(self, h):
        h.store.add_triples(h.table, [T(S1, P, "old")])

        h.store.modify_triple(h.table, T(S1, P, "old"), T(S1, P, "new"))

        assert override_rows(h) == [("add", "user", S1, P, "new")]
        assert h.graph() == {K(S1, P, "new")}

    def test_subject_and_predicate_can_change_too(self, h):
        h.seed_mapped([T(S1, P, "1")])

        h.store.modify_triple(h.table, T(S1, P, "1"), T(S2, Q, "1"))

        assert h.graph() == {K(S2, Q, "1")}

    def test_modifying_onto_an_existing_triple_merges_the_two(self, h):
        h.seed_mapped([T(S1, P, "a"), T(S1, P, "b")])

        h.store.modify_triple(h.table, T(S1, P, "a"), T(S1, P, "b"))

        assert override_rows(h) == [("del", "user", S1, P, "a")]
        assert h.graph() == {K(S1, P, "b")}

    def test_a_chain_of_edits_keeps_only_the_last_value(self, h):
        h.seed_mapped([T(S1, P, "a")])

        h.store.modify_triple(h.table, T(S1, P, "a"), T(S1, P, "b"))
        h.store.modify_triple(h.table, T(S1, P, "b"), T(S1, P, "c"))

        assert override_rows(h) == [
            ("add", "user", S1, P, "c"),
            ("del", "user", S1, P, "a"),
        ]
        assert h.graph() == {K(S1, P, "c")}

    def test_an_identical_triple_is_rejected_and_writes_nothing(self, h):
        h.seed_mapped([T(S1, P, "a")])

        with pytest.raises(ValidationError):
            h.store.modify_triple(h.table, T(S1, P, "a"), T(S1, P, "a"))

        assert override_rows(h) == []


# ---------------------------------------------------------------------------
# What the graphs return
# ---------------------------------------------------------------------------


class TestGraphs:
    def test_graph_and_asserted_differ_only_by_derived_triples(self, h):
        h.seed_mapped([T(S1, P, "kept"), T(S1, P, "hidden")])
        h.seed_derived("reasoning", [T(S1, Q, "inferred")])
        h.store.add_triples(h.table, [T(S2, P, "mine")])
        h.store.delete_triples(h.table, [T(S1, P, "hidden")])

        assert h.graph() == {K(S1, P, "kept"), K(S1, Q, "inferred"), K(S2, P, "mine")}
        assert h.asserted() == {K(S1, P, "kept"), K(S2, P, "mine")}
        assert h.count() == 3

    def test_every_derived_origin_is_in_the_graph_and_out_of_the_asserted_graph(self, h):
        h.seed_mapped([T(S1, P, "1")])
        h.seed_derived("reasoning", [T(S1, Q, "r")])
        h.seed_derived("cohort", [T(S1, Q, "c")])
        h.seed_derived("community", [T(S1, Q, "m")])

        assert len(h.graph()) == 4
        assert h.asserted() == {K(S1, P, "1")}

    def test_an_edit_shows_up_in_the_next_read(self, h):
        h.seed_mapped([T(S1, P, "1")])
        before = h.graph()

        h.store.add_triples(h.table, [T(S2, P, "2")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])

        assert before == {K(S1, P, "1")}
        assert h.graph() == {K(S2, P, "2")}

    def test_edits_mark_the_search_cache_stale(self, h):
        h.store.add_triples(h.table, [T(S1, P, "1")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])

        assert h.store._mark_cache_stale.call_count == 2

    def test_empty_edits_leave_the_cache_alone(self, h):
        h.store.add_triples(h.table, [])
        h.store.delete_triples(h.table, [])

        h.store._mark_cache_stale.assert_not_called()


# ---------------------------------------------------------------------------
# Overrides tab, counters and Build
# ---------------------------------------------------------------------------


class TestBreakdownAndStats:
    def test_the_breakdown_counts_derived_rows_by_origin_and_ignores_user_rows(self, h):
        h.seed_derived("reasoning", [T(S1, Q, "a"), T(S2, Q, "a")])
        h.seed_derived("cohort", [T(S1, f"{BASE}inCohort", f"{BASE}cohort/x")])
        h.seed_derived("community", [T(S2, f"{BASE}inCluster", f"{BASE}cluster/y")])
        h.store.add_triples(h.table, [T(S1, P, "mine")])
        h.seed_mapped([T(S1, P, "m")])
        h.store.delete_triples(h.table, [T(S1, P, "m")])

        assert breakdown(h) == {
            "reasoning": (2, 2),
            "cohorts": (1, 1),
            "communities": (1, 1),
        }
        assert h.store.get_inferred_triple_count(h.table) == 4

    def test_override_stats_count_user_rows_only(self, h):
        h.seed_mapped([T(S1, P, "1")])
        h.seed_derived("reasoning", [T(S1, Q, "x")])
        h.store.add_triples(h.table, [T(S2, P, "2")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])

        assert h.store.get_override_stats(h.table) == {
            "user_adds": 1,
            "user_dels": 1,
            "dormant_dels": 0,
        }

    def test_a_del_whose_triple_left_the_source_is_dormant(self, h):
        h.seed_mapped([T(S1, P, "1"), T(S1, P, "2")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])

        h.rebuild([T(S1, P, "2")])  # the source no longer maps (S1, P, 1)

        assert h.store.get_override_stats(h.table)["dormant_dels"] == 1
        assert h.graph() == {K(S1, P, "2")}


class TestBuildAndPurge:
    def test_a_rebuild_keeps_user_edits_and_drops_derived_rows(self, h):
        h.seed_mapped([T(S1, P, "1"), T(S1, P, "2")])
        h.seed_derived("reasoning", [T(S1, Q, "x")])
        h.store.add_triples(h.table, [T(S2, P, "mine")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])

        h.rebuild([T(S1, P, "1"), T(S1, P, "2"), T(S1, P, "3")])

        assert override_rows(h) == [
            ("add", "user", S2, P, "mine"),
            ("del", "user", S1, P, "1"),
        ]
        assert h.graph() == {K(S1, P, "2"), K(S1, P, "3"), K(S2, P, "mine")}

    def test_a_triple_hidden_by_the_user_stays_hidden_when_the_source_brings_it_back(self, h):
        h.seed_mapped([T(S1, P, "1")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])

        h.rebuild([T(S1, P, "1")])

        assert h.graph() == set()

    def test_purge_removes_derived_rows_and_spares_user_edits(self, h):
        h.seed_mapped([T(S1, P, "1"), T(S1, P, "2")])
        h.seed_derived("reasoning", [T(S1, Q, "a")])
        h.seed_derived("cohort", [T(S1, Q, "b")])
        h.store.add_triples(h.table, [T(S2, P, "mine")])
        h.store.delete_triples(h.table, [T(S1, P, "1")])

        purged = h.store.purge_materialized_triples(h.table)

        assert purged == 2
        assert override_rows(h) == [
            ("add", "user", S2, P, "mine"),
            ("del", "user", S1, P, "1"),
        ]
        assert h.graph() == {K(S1, P, "2"), K(S2, P, "mine")}

    def test_a_partition_delete_only_touches_its_own_origin(self, h):
        prefix = f"{BASE}cohort/"
        member = f"{BASE}inCohort"
        h.seed_derived(
            "cohort", [T(f"{prefix}x", P, "1"), T(S1, member, f"{prefix}x")]
        )
        h.seed_derived("reasoning", [T(f"{prefix}y", P, "kept")])
        h.store.add_triples(h.table, [T(f"{prefix}z", P, "mine")])

        h.store.delete_cohort_triples(h.table, prefix, member)

        assert override_rows(h) == [
            ("add", "reasoning", f"{prefix}y", P, "kept"),
            ("add", "user", f"{prefix}z", P, "mine"),
        ]


# ---------------------------------------------------------------------------
# Before the overrides layer exists
# ---------------------------------------------------------------------------


class TestMissingOverridesTable:
    def test_the_breakdown_reports_zero_rows_instead_of_failing(self, unbuilt):
        assert breakdown(unbuilt) == {
            "reasoning": (0, 0),
            "cohorts": (0, 0),
            "communities": (0, 0),
        }

    def test_the_derived_count_is_zero(self, unbuilt):
        assert unbuilt.store.get_inferred_triple_count(unbuilt.table) == 0


class TestDeltaLazySetup:
    """Delta creates its tables on the first edit, and reads follow the views."""

    def test_reads_use_the_data_table_until_the_first_edit(self):
        h = DeltaHarness(built=False)
        h.seed_mapped([T(S1, P, "1")])
        assert h.store.physical_table_id(h.table) == DELTA_DATA
        assert h.store.synced_table_name(h.table) == DELTA_DATA

        h.store.add_triples(h.table, [T(S2, P, "2")])

        assert h.store.physical_table_id(h.table) == DELTA_GRAPH
        assert h.store.synced_table_name(h.table) == DELTA_ASSERTED
        assert h.graph() == {K(S1, P, "1"), K(S2, P, "2")}

    def test_the_first_edit_creates_the_table_and_both_views(self):
        h = DeltaHarness(built=False)
        h.seed_mapped([T(S1, P, "1")])

        h.store.delete_triples(h.table, [T(S1, P, "1")])

        assert materialize.relation_exists(h.client, DELTA_OVR)
        assert materialize.relation_exists(h.client, DELTA_GRAPH)
        assert materialize.relation_exists(h.client, DELTA_ASSERTED)
        assert h.graph() == set()

    def test_legacy_inferred_rows_are_migrated_with_their_origin(self):
        h = DeltaHarness(built=False)
        h.seed_mapped([T(S1, P, "1")])
        h.client.execute_statement(
            f"CREATE TABLE IF NOT EXISTS {DELTA_LEGACY} "
            "(subject STRING, predicate STRING, object STRING) USING DELTA"
        )
        h.client.execute_statement(
            f"INSERT INTO {DELTA_LEGACY} (subject, predicate, object) VALUES "
            f"('{S1}', '{Q}', 'reasoned'), "
            f"('{S2}', '{BASE}inCohort', '{BASE}cohort/x'), "
            f"('{S2}', '{BASE}inCluster', '{BASE}cluster/y')"
        )

        h.store.add_triples(h.table, [T(S2, P, "mine")])

        assert override_rows(h) == [
            ("add", "cohort", S2, f"{BASE}inCohort", f"{BASE}cohort/x"),
            ("add", "community", S2, f"{BASE}inCluster", f"{BASE}cluster/y"),
            ("add", "reasoning", S1, Q, "reasoned"),
            ("add", "user", S2, P, "mine"),
        ]
        assert len(h.graph()) == 5
        assert h.asserted() == {K(S1, P, "1"), K(S2, P, "mine")}
