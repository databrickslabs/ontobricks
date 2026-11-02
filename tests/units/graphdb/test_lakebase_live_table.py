"""Lakebase ``_live``: the realtime serving table's DDL, purge, count and union view."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.core.errors import InfrastructureError
from back.core.graphdb.lakebase import LakebaseLiveTable, _companion_ddl

pytestmark = pytest.mark.unit


class TestLiveTableDDL:
    def test_the_table_name_follows_the_graph(self):
        assert _companion_ddl.live_phy("Sales_V3") == "sales_v3_live"

    def test_the_table_has_the_slot_key_and_the_sink_columns(self):
        create = _companion_ddl.live_table_ddl("g_v1_live")[0]

        assert create == (
            "CREATE TABLE IF NOT EXISTS g_v1_live (map_id TEXT NOT NULL, source_key TEXT NOT NULL, "
            "predicate TEXT NOT NULL, subject TEXT, object TEXT, object_hash TEXT, "
            "deleted BOOLEAN NOT NULL DEFAULT FALSE, seq BIGINT NOT NULL, "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
            "PRIMARY KEY (map_id, source_key, predicate))"
        )

    def test_the_indexes_are_partial_over_the_live_rows(self):
        statements = _companion_ddl.live_table_ddl("g_v1_live")[1:4]

        assert statements == [
            "CREATE INDEX IF NOT EXISTS g_g_v1_live_ls ON g_v1_live (subject) WHERE NOT deleted",
            "CREATE INDEX IF NOT EXISTS g_g_v1_live_lps ON g_v1_live (predicate, subject) "
            "WHERE NOT deleted",
            "CREATE INDEX IF NOT EXISTS g_g_v1_live_loh ON g_v1_live (object_hash) WHERE NOT deleted",
        ]

    def test_autovacuum_runs_early_on_the_upsert_table(self):
        assert _companion_ddl.live_table_ddl("g_v1_live")[-1] == (
            "ALTER TABLE g_v1_live SET (autovacuum_vacuum_scale_factor = 0.02, "
            "autovacuum_analyze_scale_factor = 0.01)"
        )

    def test_the_purge_deletes_old_tombstones_only(self):
        assert _companion_ddl.purge_live_tombstones_sql("g_v1_live", 48) == (
            "DELETE FROM g_v1_live WHERE deleted AND updated_at < now() - interval '48 hours'"
        )

    @pytest.mark.parametrize("hours", [-1, 1.5, "24", True])
    def test_the_purge_age_must_be_a_whole_number(self, hours):
        with pytest.raises(InfrastructureError):
            _companion_ddl.purge_live_tombstones_sql("g_v1_live", hours)


class TestUnionViewWithLive:
    def test_the_triples_view_unions_the_live_rows_distinctly(self):
        sql = _companion_ddl.build_union_view_sql_with_live("g_v1", "g_v1_sync", "g_v1__app", "g_v1_live")

        assert sql == (
            "CREATE OR REPLACE VIEW g_v1 AS "
            "SELECT subject, predicate, object, NULL::TEXT AS datatype, NULL::TEXT AS lang "
            "FROM g_v1_sync UNION "
            "SELECT subject, predicate, object, datatype, lang FROM g_v1__app UNION "
            "SELECT subject, predicate, object, NULL::TEXT AS datatype, NULL::TEXT AS lang "
            "FROM g_v1_live WHERE NOT deleted AND object IS NOT NULL AND TRIM(object) <> ''"
        )
        assert "UNION ALL" not in sql

    def test_a_slots_synced_table_keeps_its_projection(self):
        sql = _companion_ddl.build_union_view_sql_with_live(
            "g_v1", "g_v1_sync", "g_v1__app", "g_v1_live", source_kind=_companion_ddl.SOURCE_KIND_SLOTS
        )

        assert "SELECT DISTINCT subject, predicate, object" in sql and "FROM g_v1_live" in sql

    def _ensure(self, *, live_exists: bool, view="g_v1") -> list[str]:
        cur = MagicMock()
        cur.fetchone.return_value = (1,) if live_exists else None
        _companion_ddl.ensure_union_view(cur, view, "g_v1_sync", "g_v1__app")
        return [str(c.args[0]) for c in cur.execute.call_args_list]

    def test_an_existing_live_table_joins_the_view(self):
        statements = self._ensure(live_exists=True)

        assert statements[-1] == _companion_ddl.build_union_view_sql_with_live(
            "g_v1", "g_v1_sync", "g_v1__app", "g_v1_live"
        )

    def test_without_a_live_table_the_view_is_unchanged(self):
        statements = self._ensure(live_exists=False)

        assert statements[-1] == _companion_ddl.build_union_view_sql("g_v1", "g_v1_sync", "g_v1__app")

    def test_a_schema_qualified_view_looks_for_its_live_table_by_bare_name(self):
        cur = MagicMock()
        cur.fetchone.return_value = None

        _companion_ddl.ensure_union_view(cur, '"graphs".g_v1', "g_v1_sync", "g_v1__app")

        lookups = [c.args[1] for c in cur.execute.call_args_list if len(c.args) > 1]
        assert ("g_v1_live",) in lookups


class _Cursor:
    def __init__(self, *, rowcount=0, row=None):
        self.statements: list[str] = []
        self.rowcount = rowcount
        self._row = row

    def execute(self, sql, params=None):
        self.statements.append(sql)

    def fetchone(self):
        return self._row


def _live(cur) -> LakebaseLiveTable:
    @contextmanager
    def cursor():
        yield cur

    auth = SimpleNamespace(host="pg.lakebase.io", database="graphs")
    return LakebaseLiveTable(cursor, schema="ontobricks_graph", auth=auth)


class TestLakebaseLiveTable:
    def test_ensure_runs_the_ddl(self):
        cur = _Cursor()

        assert _live(cur).ensure("Sales_V3") == "sales_v3_live"
        assert cur.statements == _companion_ddl.live_table_ddl("sales_v3_live")

    def test_the_purge_reports_the_deleted_rows(self):
        cur = _Cursor(rowcount=7)

        assert _live(cur).purge_tombstones("Sales_V3", 12) == 7
        assert cur.statements == [_companion_ddl.purge_live_tombstones_sql("sales_v3_live", 12)]

    def test_the_count_reads_the_live_rows(self):
        cur = _Cursor(row={"cnt": 42})

        assert _live(cur).count("Sales_V3") == 42
        assert cur.statements == [
            "SELECT count(*) AS cnt FROM sales_v3_live "
            "WHERE NOT deleted AND object IS NOT NULL AND TRIM(object) <> ''"
        ]

    def test_the_connection_facts_carry_no_credential(self):
        assert _live(_Cursor()).connection() == {
            "host": "pg.lakebase.io",
            "database": "graphs",
            "schema": "ontobricks_graph",
        }

    def test_the_store_hands_out_its_live_table(self):
        from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase

        store = SimpleNamespace(
            _schema="ontobricks_graph",
            _auth=SimpleNamespace(host="h", database="d"),
            _cursor=lambda: None,
        )

        live = LakebaseBase.live_table(store)

        assert isinstance(live, LakebaseLiveTable)
        assert live.connection()["schema"] == "ontobricks_graph"
