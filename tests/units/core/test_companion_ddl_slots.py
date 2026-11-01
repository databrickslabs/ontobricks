"""Union view and indexes over a ``…_slots``-sourced Lakebase synced table.

A synced table registered on the ``…_slots`` streaming table carries
``map_id`` / ``source_key`` next to the triple, holds one row per slot (classes
that share a TriplesMap emit the same triple twice) and keeps the blank
attribute objects ``…_data`` filters out. It has no ``object_hash`` column.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase._companion_ddl import (
    SOURCE_KIND_SLOTS,
    SOURCE_KIND_TRIPLES,
    build_union_view_sql,
    build_union_view_sql_for_slots,
    ensure_graph_indexes,
    ensure_union_view,
)

pytestmark = pytest.mark.unit


def _statements(cur: MagicMock) -> list[str]:
    return [str(c.args[0]) for c in cur.execute.call_args_list]


class TestSlotsUnionViewSQL:
    def test_the_synced_side_projects_three_columns_and_deduplicates(self):
        sql = build_union_view_sql_for_slots("g_v1", "g_v1_sync", "g_v1__app")

        assert sql.startswith("CREATE OR REPLACE VIEW g_v1 AS ")
        assert (
            "SELECT DISTINCT subject, predicate, object, "
            "NULL::TEXT AS datatype, NULL::TEXT AS lang FROM g_v1_sync"
        ) in sql
        assert "map_id" not in sql and "source_key" not in sql and "*" not in sql

    def test_blank_attribute_objects_are_dropped_like_the_data_relation_does(self):
        sql = build_union_view_sql_for_slots("g_v1", "g_v1_sync", "g_v1__app")

        assert "WHERE object IS NOT NULL AND TRIM(object) <> ''" in sql

    def test_the_companion_keeps_its_five_columns(self):
        sql = build_union_view_sql_for_slots("g_v1", "g_v1_sync", "g_v1__app")

        assert sql.endswith(
            "UNION ALL SELECT subject, predicate, object, datatype, lang FROM g_v1__app"
        )

    def test_the_triples_view_is_unchanged(self):
        sql = build_union_view_sql("g_v1", "g_v1_sync", "g_v1__app")

        assert sql == (
            "CREATE OR REPLACE VIEW g_v1 AS "
            "SELECT subject, predicate, object, NULL::TEXT AS datatype, NULL::TEXT AS lang "
            "FROM g_v1_sync UNION ALL "
            "SELECT subject, predicate, object, datatype, lang FROM g_v1__app"
        )


class TestEnsureUnionView:
    def test_a_slots_source_creates_the_slots_view(self):
        cur = MagicMock()

        ensure_union_view(cur, "g_v1", "g_v1_sync", "g_v1__app", source_kind=SOURCE_KIND_SLOTS)

        assert _statements(cur)[-1] == build_union_view_sql_for_slots(
            "g_v1", "g_v1_sync", "g_v1__app"
        )

    def test_the_default_source_creates_the_triples_view(self):
        cur = MagicMock()

        ensure_union_view(cur, "g_v1", "g_v1_sync", "g_v1__app")

        assert _statements(cur)[-1] == build_union_view_sql("g_v1", "g_v1_sync", "g_v1__app")

    def test_a_stale_table_under_the_view_name_is_still_dropped_first(self):
        cur = MagicMock()

        ensure_union_view(cur, "g_v1", "g_v1_sync", "g_v1__app", source_kind=SOURCE_KIND_SLOTS)

        assert "DROP TABLE IF EXISTS g_v1 CASCADE" in _statements(cur)[0]


class TestGraphIndexes:
    def test_a_slots_sourced_table_gets_the_subject_index_only(self):
        cur = MagicMock()

        ensure_graph_indexes(cur, "g_v1_sync", source_kind=SOURCE_KIND_SLOTS)

        statements = _statements(cur)
        assert len(statements) == 1
        assert "ON g_v1_sync (subject, predicate)" in statements[0]
        assert not any("object_hash" in s for s in statements)

    def test_a_triples_table_keeps_all_three_indexes(self):
        cur = MagicMock()

        ensure_graph_indexes(cur, "g_v1_sync", source_kind=SOURCE_KIND_TRIPLES)

        assert len(_statements(cur)) == 3


class TestFlatStoreSlotsView:
    @pytest.fixture
    def store(self):
        pytest.importorskip("psycopg")
        from back.core.graphdb.lakebase.LakebaseFlatStore import LakebaseFlatStore

        auth = MagicMock()
        auth.database = "appdb"
        auth.is_available = True
        return LakebaseFlatStore(
            auth,
            schema="ontobricks_graph",
            sync_mode="managed_synced",
            sync_uc_catalog="catX",
            synced_manager=MagicMock(),
        )

    @staticmethod
    def _cursor(cur):
        @contextmanager
        def cm():
            yield cur

        return cm

    def test_ensure_synced_union_view_builds_the_slots_view_for_a_slots_source(self, store):
        cur = MagicMock()
        with patch.object(store, "_cursor", self._cursor(cur)):
            store.ensure_synced_union_view("G_V1", source_kind=SOURCE_KIND_SLOTS)

        executed = _statements(cur)
        view_ddl = next(s for s in executed if "CREATE OR REPLACE VIEW" in s)
        assert "SELECT DISTINCT subject, predicate, object" in view_ddl
        indexes = [s for s in executed if "CREATE INDEX IF NOT EXISTS" in s]
        assert len(indexes) == 1 and "object_hash" not in indexes[0]

    def test_drop_union_view_drops_only_the_view(self, store):
        cur = MagicMock()
        with patch.object(store, "_cursor", self._cursor(cur)):
            store.drop_union_view("G_V1")

        assert _statements(cur) == ["DROP VIEW IF EXISTS g_v1"]


def test_the_source_kinds_are_the_two_documented_values():
    assert (_companion_ddl.SOURCE_KIND_TRIPLES, _companion_ddl.SOURCE_KIND_SLOTS) == (
        "triples",
        "slots",
    )
