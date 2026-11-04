"""Lakebase ``_live``: the realtime serving table's DDL, purge, count and union view."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.graphdb.lakebase import LakebaseLiveTable, _companion_ddl
from back.core.streaming.SlotChange import SlotChange

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
        with pytest.raises(ValidationError):
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

    def _ensure(self, *, comment, view="g_v1") -> list[str]:
        cur = MagicMock()
        cur.fetchone.return_value = None if comment is False else {"comment": comment}
        _companion_ddl.ensure_union_view(cur, view, "g_v1_sync", "g_v1__app")
        return [str(c.args[0]) for c in cur.execute.call_args_list]

    def test_a_served_live_table_joins_the_view(self):
        statements = self._ensure(comment=_companion_ddl.LIVE_SERVING_COMMENT)

        assert statements[-2] == _companion_ddl.build_union_view_sql_with_live(
            "g_v1", "g_v1_sync", "g_v1__app", "g_v1_live"
        )

    @pytest.mark.parametrize("comment", [False, None, "something else"], ids=["absent", "retired", "other"])
    def test_an_absent_or_retired_live_table_stays_out_of_the_view(self, comment):
        statements = self._ensure(comment=comment)

        assert statements[-2] == _companion_ddl.build_union_view_sql("g_v1", "g_v1_sync", "g_v1__app")

    def test_a_schema_qualified_view_looks_for_its_live_table_by_bare_name(self):
        cur = MagicMock()
        cur.fetchone.return_value = None

        _companion_ddl.ensure_union_view(cur, '"graphs".g_v1', "g_v1_sync", "g_v1__app")

        lookups = [c.args[1] for c in cur.execute.call_args_list if len(c.args) > 1]
        assert ("g_v1_live",) in lookups


class _Cursor:
    def __init__(self, *, rowcount=0, row=None):
        self.statements: list[str] = []
        self.batches: list[list] = []
        self.rowcount = rowcount
        self._row = row

    def execute(self, sql, params=None):
        self.statements.append(sql)

    def executemany(self, sql, rows):
        self.statements.append(sql)
        self.batches.append(list(rows))

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

    def test_ensure_grants_the_stream_role_what_the_sink_needs(self):
        cur = _Cursor()

        _live(cur).ensure("Sales_V3", writer_role="ontobricks_rtm")

        assert cur.statements[-2:] == [
            'GRANT USAGE ON SCHEMA "ontobricks_graph" TO "ontobricks_rtm"',
            'GRANT SELECT, INSERT, UPDATE ON sales_v3_live TO "ontobricks_rtm"',
        ]

    def test_an_unsafe_role_name_is_refused(self):
        with pytest.raises(ValidationError):
            _live(_Cursor()).ensure("Sales_V3", writer_role='x"; DROP TABLE y; --')

    def test_a_driver_error_is_an_infrastructure_error(self):
        """A writer role that does not exist fails the GRANT inside the driver (live: a raw
        ``psycopg.errors.UndefinedObject`` surfaced as a 500); it is the hierarchy's error."""

        class _Failing(_Cursor):
            def execute(self, statement, params=None):
                if statement.startswith("GRANT"):
                    raise RuntimeError('role "ontobricks_rtm_live" does not exist')
                super().execute(statement, params)

        with pytest.raises(InfrastructureError, match="live table .* for role 'ontobricks_rtm_live'") as exc_info:
            _live(_Failing()).ensure("Sales_V3", writer_role="ontobricks_rtm_live")
        assert "does not exist" in exc_info.value.detail

    def test_attach_empties_seeds_the_snapshot_and_serves(self):
        cur = _Cursor()
        snapshot = [
            {"map_id": "m", "source_key": "1", "predicate": "p", "subject": "s1", "object": "a"},
            {"map_id": "m", "source_key": "2", "predicate": "p", "subject": "s2", "object": "  "},
            {"map_id": "m", "source_key": "3", "predicate": "p", "subject": None, "object": "b"},
        ]

        assert _live(cur).attach("Sales_V3", snapshot) == 1

        assert cur.statements[0] == "TRUNCATE TABLE sales_v3_live"
        assert cur.statements[1] == _companion_ddl.seed_live_sql("sales_v3_live")
        [[row]] = cur.batches
        assert row == ("m", "1", "p", "s1", "a", SlotChange.hash_object("a"), -1)
        assert cur.statements[-1] == "COMMENT ON TABLE sales_v3_live IS 'ontobricks:live:serving'"

    def test_detach_empties_and_stops_serving(self):
        cur = _Cursor(row=(1,))

        _live(cur).detach("Sales_V3")

        assert cur.statements[-2:] == [
            "TRUNCATE TABLE sales_v3_live",
            "COMMENT ON TABLE sales_v3_live IS NULL",
        ]

    def test_detach_without_a_table_does_nothing(self):
        cur = _Cursor(row=None)

        _live(cur).detach("Sales_V3")

        assert not any(s.startswith(("TRUNCATE", "COMMENT")) for s in cur.statements)

    def test_serving_reads_the_table_comment(self):
        assert _live(_Cursor(row={"comment": "ontobricks:live:serving"})).serving("Sales_V3") is True
        assert _live(_Cursor(row={"comment": None})).serving("Sales_V3") is False

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
        from back.core.graphdb.lakebase.Base import Base as LakebaseBase

        store = SimpleNamespace(
            _schema="ontobricks_graph",
            _auth=SimpleNamespace(host="h", database="d"),
            _cursor=lambda: None,
        )

        live = LakebaseBase.live_table(store)

        assert isinstance(live, LakebaseLiveTable)
        assert live.connection()["schema"] == "ontobricks_graph"
