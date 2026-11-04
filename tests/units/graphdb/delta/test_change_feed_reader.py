"""Unit tests for ``ChangeFeedReader`` (net triple changes from a Delta change feed)."""

from __future__ import annotations

import sqlite3

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.graphdb.delta import ChangeFeedReader as ExportedReader
from back.core.graphdb.delta.ChangeFeedReader import ChangeFeedReader

pytestmark = pytest.mark.unit

DATA = "main.ob.triplestore_d_V1_data"

NET_SELECT = (
    "SELECT subject, predicate, object, "
    "CASE WHEN net_change > 0 THEN 'insert' ELSE 'delete' END AS op\n"
    "FROM (\n"
    "  SELECT subject, predicate, object,\n"
    "    SUM(CASE WHEN _change_type IN ('insert', 'update_postimage') THEN 1 ELSE -1 END)"
    " AS net_change\n"
    f"  FROM table_changes('{DATA}', 6, 9)\n"
    "  WHERE _change_type IN ('insert', 'update_postimage', 'delete', 'update_preimage')\n"
    "  GROUP BY subject, predicate, object\n"
    ") c\n"
    "WHERE net_change <> 0"
)

SLOTS = "main.ob.triplestore_d_V1_slots"

_ON = "ON {s}.subject = t.subject AND {s}.predicate = t.predicate AND {s}.object = t.object"

SLOT_NET_SELECT = (
    "WITH touched AS (\n"
    "  SELECT DISTINCT subject, predicate, object\n"
    f"  FROM table_changes('{SLOTS}', 6, 9)\n"
    "  WHERE _change_type IN ('insert', 'update_postimage', 'delete', 'update_preimage')\n"
    "), at_since AS (\n"
    f"  SELECT DISTINCT subject, predicate, object FROM {SLOTS} VERSION AS OF 5\n"
    "), at_until AS (\n"
    f"  SELECT DISTINCT subject, predicate, object FROM {SLOTS} VERSION AS OF 9\n"
    ")\n"
    "SELECT t.subject, t.predicate, t.object, "
    "CASE WHEN u.subject IS NULL THEN 'delete' ELSE 'insert' END AS op\n"
    "FROM touched t\n"
    f"LEFT JOIN at_since s {_ON.format(s='s')}\n"
    f"LEFT JOIN at_until u {_ON.format(s='u')}\n"
    "WHERE (s.subject IS NULL) <> (u.subject IS NULL)"
)


class _Client:
    """Warehouse double: records queries, serves canned rows or raises."""

    def __init__(self, rows=None, error=None, stream=None, stream_error=None):
        self.queries: list[str] = []
        self.streams: list[tuple[str, int]] = []
        self._rows = rows if rows is not None else []
        self._error = error
        self._stream = stream if stream is not None else []
        self._stream_error = stream_error

    def execute_query(self, sql):
        self.queries.append(sql)
        if self._error is not None:
            raise self._error
        return self._rows

    def iter_rows(self, sql, batch_size=5000):
        self.streams.append((sql, batch_size))
        yield from self._stream
        if self._stream_error is not None:
            raise self._stream_error


class TestPackageExport:
    def test_is_reexported_from_the_delta_package(self):
        assert ExportedReader is ChangeFeedReader


class TestNetChangesSql:
    def test_exact_statement_orders_the_deletes_first(self):
        assert ChangeFeedReader.net_changes_sql(DATA, 5, 9) == NET_SELECT + "\nORDER BY op"

    def test_reads_the_feed_after_the_last_applied_version_up_to_the_recorded_one(self):
        """The window is closed: commits after *until* are left for the next build."""
        assert f"table_changes('{DATA}', 1, 3)" in ChangeFeedReader.net_changes_sql(DATA, 0, 3)

    @pytest.mark.parametrize("fqn", ["", "main.ob.x'); DROP TABLE y; --", "main.ob.x y"])
    def test_an_unsafe_name_is_refused(self, fqn):
        with pytest.raises(ValidationError):
            ChangeFeedReader.net_changes_sql(fqn, 5, 9)

    @pytest.mark.parametrize("since", [-1, True, "5", 1.5, None])
    def test_the_version_must_be_a_non_negative_integer(self, since):
        with pytest.raises(ValidationError):
            ChangeFeedReader.net_changes_sql(DATA, since, 9)

    @pytest.mark.parametrize("until", [5, 4, True, "9", None])
    def test_the_window_must_hold_at_least_one_version(self, until):
        with pytest.raises(ValidationError):
            ChangeFeedReader.net_changes_sql(DATA, 5, until)


class TestNetReduction:
    """The reduction runs as-is on SQLite once ``table_changes(...)`` is a table."""

    @staticmethod
    def _run(changes):
        sql = ChangeFeedReader.net_changes_sql(DATA, 5, 9).replace(
            f"table_changes('{DATA}', 6, 9)", "changes"
        )
        con = sqlite3.connect(":memory:")
        con.execute(
            "CREATE TABLE changes (subject TEXT, predicate TEXT, object TEXT, _change_type TEXT)"
        )
        con.executemany("INSERT INTO changes VALUES (?, ?, ?, ?)", changes)
        return con.execute(sql).fetchall()

    def test_an_insert_is_an_insert(self):
        assert self._run([("s", "p", "o", "insert")]) == [("s", "p", "o", "insert")]

    def test_a_delete_is_a_delete(self):
        assert self._run([("s", "p", "o", "delete")]) == [("s", "p", "o", "delete")]

    def test_an_insert_then_a_delete_nets_to_nothing(self):
        assert self._run([("s", "p", "o", "insert"), ("s", "p", "o", "delete")]) == []

    def test_an_update_deletes_the_preimage_and_inserts_the_postimage(self):
        rows = self._run(
            [("s", "p", "old", "update_preimage"), ("s", "p", "new", "update_postimage")]
        )

        assert rows == [("s", "p", "old", "delete"), ("s", "p", "new", "insert")]

    def test_insert_delete_insert_is_one_insert(self):
        changes = [("s", "p", "o", t) for t in ("insert", "delete", "insert")]

        assert self._run(changes) == [("s", "p", "o", "insert")]

    def test_deletes_come_before_inserts(self):
        rows = self._run(
            [("a", "p", "1", "insert"), ("b", "p", "2", "delete"), ("c", "p", "3", "insert")]
        )

        assert [op for *_, op in rows] == ["delete", "insert", "insert"]


class TestSlotNetChangesSql:
    """``distinct_rows=False``: the relation's rows are slots, several per triple."""

    def test_exact_statement_diffs_the_deduplicated_snapshots_of_the_touched_triples(self):
        sql = ChangeFeedReader.net_changes_sql(SLOTS, 5, 9, distinct_rows=False)

        assert sql == SLOT_NET_SELECT + "\nORDER BY op"

    def test_the_ratio_counts_the_same_statement(self):
        client = _Client(rows=[{"cnt": 3}])

        ratio = ChangeFeedReader.changeset_ratio(client, SLOTS, 5, 9, 30, distinct_rows=False)

        assert ratio == 0.1
        assert SLOT_NET_SELECT in client.queries[0]

    def test_the_stream_reads_the_same_statement(self):
        client = _Client(stream=[{"op": "insert"}])

        list(ChangeFeedReader.iter_net_changes(client, SLOTS, 5, 9, distinct_rows=False))

        assert client.streams[0][0] == SLOT_NET_SELECT + "\nORDER BY op"


class TestSlotNetReduction:
    """The reduction runs as-is on SQLite once the feed and the two snapshots are tables."""

    @staticmethod
    def _run(touched, at_since, at_until):
        sql = (
            ChangeFeedReader.net_changes_sql(SLOTS, 5, 9, distinct_rows=False)
            .replace(f"table_changes('{SLOTS}', 6, 9)", "changes")
            .replace(f"{SLOTS} VERSION AS OF 5", "slots_v5")
            .replace(f"{SLOTS} VERSION AS OF 9", "slots_v9")
        )
        con = sqlite3.connect(":memory:")
        con.execute(
            "CREATE TABLE changes (subject TEXT, predicate TEXT, object TEXT, _change_type TEXT)"
        )
        for name, rows in (("slots_v5", at_since), ("slots_v9", at_until)):
            con.execute(
                f"CREATE TABLE {name} (map_id TEXT, subject TEXT, predicate TEXT, object TEXT)"
            )
            con.executemany(f"INSERT INTO {name} VALUES (?, ?, ?, ?)", rows)
        con.executemany("INSERT INTO changes VALUES (?, ?, ?, ?)", touched)
        return con.execute(sql).fetchall()

    def test_a_slot_deleted_while_another_carries_its_triple_is_no_change(self):
        both = [("m1", "s", "p", "o"), ("m2", "s", "p", "o")]

        assert self._run([("s", "p", "o", "delete")], both, both[1:]) == []

    def test_an_update_deletes_the_old_triple_and_inserts_the_new_one_in_that_order(self):
        touched = [("s", "p", "new", "update_postimage"), ("s", "p", "old", "update_preimage")]

        rows = self._run(touched, [("m1", "s", "p", "old")], [("m1", "s", "p", "new")])

        assert rows == [("s", "p", "old", "delete"), ("s", "p", "new", "insert")]

    def test_an_insert_then_a_delete_in_the_window_nets_to_nothing(self):
        touched = [("s", "p", "o", "insert"), ("s", "p", "o", "delete")]

        assert self._run(touched, [], []) == []

    def test_several_commits_of_one_slot_are_one_insert(self):
        touched = [
            ("s", "p", "a", "insert"),
            ("s", "p", "a", "update_preimage"),
            ("s", "p", "b", "update_postimage"),
        ]

        assert self._run(touched, [], [("m1", "s", "p", "b")]) == [("s", "p", "b", "insert")]

    def test_a_triple_already_present_is_not_inserted_again(self):
        """A second slot inserting a triple the store already holds is no change."""
        before = [("m1", "s", "p", "o")]

        assert (
            self._run([("s", "p", "o", "insert")], before, before + [("m2", "s", "p", "o")]) == []
        )


class TestLatestVersion:
    def test_reads_the_newest_history_row(self):
        client = _Client(rows=[{"version": 12, "operation": "MERGE"}])

        assert ChangeFeedReader.latest_version(client, DATA) == 12
        assert client.queries == [f"DESCRIBE HISTORY {DATA} LIMIT 1"]

    def test_no_history_is_an_infrastructure_error(self):
        with pytest.raises(InfrastructureError):
            ChangeFeedReader.latest_version(_Client(rows=[]), DATA)

    def test_a_driver_error_becomes_an_infrastructure_error(self):
        with pytest.raises(InfrastructureError):
            ChangeFeedReader.latest_version(_Client(error=RuntimeError("no such table")), DATA)

    def test_an_unsafe_name_is_refused_before_any_query(self):
        client = _Client(rows=[{"version": 1}])

        with pytest.raises(ValidationError):
            ChangeFeedReader.latest_version(client, "main.ob.x'")
        assert client.queries == []


class TestLastCommit:
    def test_version_and_timestamp_of_the_newest_commit(self):
        from datetime import datetime, timezone

        stamp = datetime(2026, 11, 2, 1, 2, 3, tzinfo=timezone.utc)
        client = _Client(rows=[{"version": 7, "timestamp": stamp}])

        assert ChangeFeedReader.last_commit(client, DATA) == {
            "version": 7,
            "committed_at": "2026-11-02T01:02:03+00:00",
        }
        assert client.queries == [f"DESCRIBE HISTORY {DATA} LIMIT 1"]

    def test_a_naive_timestamp_reads_as_utc(self):
        from datetime import datetime

        client = _Client(rows=[{"version": 1, "timestamp": datetime(2026, 11, 2, 1, 2, 3)}])

        assert ChangeFeedReader.last_commit(client, DATA)["committed_at"] == (
            "2026-11-02T01:02:03+00:00"
        )

    def test_a_text_timestamp_is_kept(self):
        client = _Client(rows=[{"version": 1, "timestamp": "2026-11-02 01:02:03.5"}])

        assert ChangeFeedReader.last_commit(client, DATA)["committed_at"] == "2026-11-02 01:02:03.5"

    def test_no_history_is_an_infrastructure_error(self):
        with pytest.raises(InfrastructureError):
            ChangeFeedReader.last_commit(_Client(rows=[]), DATA)


class TestIterNetChanges:
    def test_streams_the_net_changes_query(self):
        rows = [{"subject": "s", "predicate": "p", "object": "o", "op": "delete"}]
        client = _Client(stream=rows)

        assert list(ChangeFeedReader.iter_net_changes(client, DATA, 5, 9, batch_size=100)) == rows
        assert client.streams == [(NET_SELECT + "\nORDER BY op", 100)]

    def test_a_driver_error_while_streaming_becomes_an_infrastructure_error(self):
        client = _Client(stream=[{"op": "delete"}], stream_error=RuntimeError("feed gone"))
        changes = ChangeFeedReader.iter_net_changes(client, DATA, 5, 9)

        assert next(changes) == {"op": "delete"}
        with pytest.raises(InfrastructureError):
            next(changes)


class TestChangesetRatio:
    def test_counts_the_net_changes_against_the_total(self):
        client = _Client(rows=[{"cnt": 3}])

        assert ChangeFeedReader.changeset_ratio(client, DATA, 5, 9, 30) == pytest.approx(0.1)
        assert client.queries == [f"SELECT COUNT(*) AS cnt FROM (\n{NET_SELECT}\n) n"]

    def test_no_changes_is_zero_even_for_an_empty_total(self):
        assert ChangeFeedReader.changeset_ratio(_Client(rows=[{"cnt": 0}]), DATA, 5, 9, 0) == 0.0

    def test_changes_against_an_empty_total_are_infinitely_large(self):
        ratio = ChangeFeedReader.changeset_ratio(_Client(rows=[{"cnt": 2}]), DATA, 5, 9, 0)

        assert ratio == float("inf")

    def test_a_driver_error_becomes_an_infrastructure_error(self):
        with pytest.raises(InfrastructureError):
            ChangeFeedReader.changeset_ratio(
                _Client(error=RuntimeError("CDF is not enabled")), DATA, 5, 9, 30
            )


class TestTableId:
    """The table id tells a relation from a re-created one whose history restarted."""

    def test_reads_the_id_from_the_table_detail(self):
        client = _Client(rows=[{"id": "5f1c-…", "format": "delta"}])

        assert ChangeFeedReader.table_id(client, DATA) == "5f1c-…"
        assert client.queries == [f"DESCRIBE DETAIL {DATA}"]

    @pytest.mark.parametrize(
        "client",
        [_Client(rows=[]), _Client(rows=[{"format": "delta"}]), _Client(error=RuntimeError("x"))],
        ids=["no-row", "no-id", "error"],
    )
    def test_an_unknown_id_is_empty(self, client):
        assert ChangeFeedReader.table_id(client, DATA) == ""

    def test_an_unsafe_name_is_refused(self):
        with pytest.raises(ValidationError):
            ChangeFeedReader.table_id(_Client(), "main.ob.x'")
