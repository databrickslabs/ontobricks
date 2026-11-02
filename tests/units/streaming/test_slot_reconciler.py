"""SlotReconciler: counts and triple hashes of ``_slots`` versus ``_data``."""

from __future__ import annotations

from decimal import Decimal

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.streaming import SlotReconciler

pytestmark = pytest.mark.unit

_SLOTS = "main.kg.triplestore_sales_V1_slots"
_DATA = "main.kg.triplestore_sales_V1_data"
_FILTER = "WHERE subject IS NOT NULL AND object IS NOT NULL AND TRIM(object) != ''"
_HASH = "sum(CAST(xxhash64(subject, predicate, object) AS DECIMAL(38, 0))) AS h"


class _Client:
    """Answers the totals / per-map queries from canned rows; records the SQL."""

    def __init__(self, *, totals=None, per_map=None, fail: bool = False):
        self.calls: list[str] = []
        self._totals = totals or {}
        self._per_map = per_map or []
        self._fail = fail

    def execute_query(self, sql: str):
        self.calls.append(sql)
        if self._fail:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        if "GROUP BY map_id" in sql:
            return list(self._per_map)
        for fqn, rows in self._totals.items():
            if f"FROM {fqn} " in sql:
                return list(rows) if isinstance(rows, list) else [rows]
        return [{"n": 0, "h": None}]


class TestCountsSql:
    def test_the_totals_count_distinct_non_blank_triples(self):
        """The ``_data`` MV groups the slots and drops blank objects (unset
        attributes); counting the slots the same way is what makes the two
        sides comparable."""
        sql = SlotReconciler.counts_sql(_DATA)

        assert sql == (
            f"SELECT count(*) AS n, {_HASH} "
            f"FROM (SELECT DISTINCT subject, predicate, object FROM {_DATA} {_FILTER})"
        )

    def test_the_per_mapping_counts_group_by_map_id_with_the_same_filter(self):
        sql = SlotReconciler.counts_sql(_SLOTS, group_by_map_id=True)

        assert sql == (
            f"SELECT map_id, count(*) AS n, {_HASH} "
            f"FROM {_SLOTS} {_FILTER} GROUP BY map_id ORDER BY map_id"
        )

    @pytest.mark.parametrize("fqn", ["", "main.kg.t; DROP TABLE x", "main.kg.t'--"])
    def test_an_unsafe_relation_name_is_refused(self, fqn):
        with pytest.raises(ValidationError):
            SlotReconciler.counts_sql(fqn)


class TestCompare:
    def test_equal_counts_and_hashes_mean_no_drift(self):
        client = _Client(totals={_SLOTS: {"n": 3, "h": 42}, _DATA: {"n": 3, "h": 42}})

        result = SlotReconciler.compare(client, _SLOTS, _DATA)

        assert result["drift"] is False
        assert result["slots"] == {"rows": 3, "hash": 42}
        assert result["data"] == {"rows": 3, "hash": 42}

    def test_a_row_count_mismatch_is_drift(self):
        client = _Client(totals={_SLOTS: {"n": 3, "h": 42}, _DATA: {"n": 2, "h": 42}})

        assert SlotReconciler.compare(client, _SLOTS, _DATA)["drift"] is True

    def test_a_hash_only_mismatch_is_drift(self):
        client = _Client(totals={_SLOTS: {"n": 3, "h": 42}, _DATA: {"n": 3, "h": 43}})

        assert SlotReconciler.compare(client, _SLOTS, _DATA)["drift"] is True

    def test_an_empty_relation_hashes_to_zero(self):
        client = _Client(totals={_SLOTS: {"n": 0, "h": None}, _DATA: {"n": 0, "h": None}})

        result = SlotReconciler.compare(client, _SLOTS, _DATA)

        assert result["slots"] == {"rows": 0, "hash": 0}
        assert result["drift"] is False

    def test_decimal_sums_are_returned_as_integers(self):
        big = Decimal("184467440737095516150")
        client = _Client(totals={_SLOTS: {"n": 1, "h": big}, _DATA: {"n": 1, "h": str(big)}})

        result = SlotReconciler.compare(client, _SLOTS, _DATA)

        assert result["slots"]["hash"] == 184467440737095516150
        assert result["drift"] is False

    def test_the_slots_are_also_listed_per_mapping(self):
        client = _Client(
            totals={_SLOTS: {"n": 3, "h": 1}, _DATA: {"n": 3, "h": 1}},
            per_map=[{"map_id": "map_customer", "n": 2, "h": 5}, {"map_id": "map_order", "n": 1, "h": -4}],
        )

        result = SlotReconciler.compare(client, _SLOTS, _DATA)

        assert result["by_map_id"] == [
            {"map_id": "map_customer", "rows": 2, "hash": 5},
            {"map_id": "map_order", "rows": 1, "hash": -4},
        ]
        assert any("GROUP BY map_id" in sql and _SLOTS in sql for sql in client.calls)

    def test_a_warehouse_error_is_an_infrastructure_error(self):
        with pytest.raises(InfrastructureError, match="Could not count"):
            SlotReconciler.compare(_Client(fail=True), _SLOTS, _DATA)

    def test_an_unreadable_count_is_an_error_not_a_zero(self):
        """Two garbled answers must never compare as 'no drift'."""
        client = _Client(totals={_SLOTS: {"n": "many", "h": 1}, _DATA: {"n": 1, "h": 1}})

        with pytest.raises(InfrastructureError, match="Unreadable"):
            SlotReconciler.compare(client, _SLOTS, _DATA)

    def test_a_missing_row_is_an_error(self):
        client = _Client(totals={_SLOTS: [], _DATA: {"n": 1, "h": 1}})

        with pytest.raises(InfrastructureError, match="no row returned"):
            SlotReconciler.compare(client, _SLOTS, _DATA)


_VIEW = "main.kg.triplestore_sales_V1"


class TestComparePairs:
    """The batch recompute (gateway VIEW) and the Lakebase synced table against ``_data``."""

    def test_without_the_extra_sides_only_the_slots_pair_is_compared(self):
        client = _Client(totals={_SLOTS: {"n": 3, "h": 42}, _DATA: {"n": 2, "h": 42}})

        result = SlotReconciler.compare(client, _SLOTS, _DATA)

        assert result["pairs"] == {"slots_data": True}
        assert "view" not in result and "sync" not in result

    def test_the_gateway_view_is_counted_like_the_data_and_compared_with_it(self):
        client = _Client(
            totals={
                _SLOTS: {"n": 3, "h": 42},
                _DATA: {"n": 3, "h": 42},
                _VIEW: {"n": 3, "h": 41},
            }
        )

        result = SlotReconciler.compare(client, _SLOTS, _DATA, view_fqn=_VIEW)

        assert result["view"] == {"rows": 3, "hash": 41}
        assert result["pairs"] == {"slots_data": False, "view_data": True}
        assert SlotReconciler.counts_sql(_VIEW) in client.calls

    def test_the_synced_table_row_count_is_compared_with_the_data_rows(self):
        client = _Client(totals={_SLOTS: {"n": 3, "h": 42}, _DATA: {"n": 3, "h": 42}})

        result = SlotReconciler.compare(client, _SLOTS, _DATA, sync_rows=2)

        assert result["sync"] == {"rows": 2}
        assert result["pairs"] == {"slots_data": False, "sync_data": True}

    def test_matching_sides_are_no_drift_on_any_pair(self):
        totals = {"n": 3, "h": 42}
        client = _Client(totals={_SLOTS: totals, _DATA: totals, _VIEW: totals})

        result = SlotReconciler.compare(client, _SLOTS, _DATA, view_fqn=_VIEW, sync_rows=3)

        assert result["pairs"] == {"slots_data": False, "view_data": False, "sync_data": False}


class TestCompareWithoutSlots:
    """A batch-only domain has no ``_slots``: only the VIEW and the synced table are compared."""

    def test_the_view_and_the_sync_pairs_alone(self):
        client = _Client(totals={_DATA: {"n": 3, "h": 42}, _VIEW: {"n": 3, "h": 40}})

        result = SlotReconciler.compare(client, None, _DATA, view_fqn=_VIEW, sync_rows=3)

        assert result["pairs"] == {"view_data": True, "sync_data": False}
        assert result["slots"] is None and result["drift"] is False
        assert result["by_map_id"] == []
        assert not any("GROUP BY map_id" in sql for sql in client.calls)


class TestNullSubjects:
    """NULL-subject rows (from NULL source keys) are left out on every side."""

    def test_the_gateway_view_row_with_a_null_subject_is_no_drift(self):
        """The VIEW keeps the NULL-subject triple, the slot view drops its row: both agree."""
        import sqlite3

        db = sqlite3.connect(":memory:")
        db.create_function("xxhash64", 3, lambda s, p, o: hash((s, p, o)) & 0xFFFFFFFF)
        for table in ("slots", "data", "view"):
            db.execute(f"CREATE TABLE {table} (map_id TEXT, subject TEXT, predicate TEXT, object TEXT)")
        triple = ("map_c", "urn:c/1", "urn:p", "a")
        for table in ("slots", "data", "view"):
            db.execute(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", triple)
        db.execute("INSERT INTO view VALUES (NULL, NULL, 'urn:p', 'b')")

        class _SQLite:
            def execute_query(self, sql):
                sql = sql.replace("main.kg.", "").replace("DECIMAL(38, 0)", "INTEGER")
                cursor = db.execute(sql.replace("TRIM(object)", "trim(object)"))
                names = [d[0] for d in cursor.description]
                return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

        result = SlotReconciler.compare(
            _SQLite(), "main.kg.slots", "main.kg.data", view_fqn="main.kg.view"
        )

        assert result["pairs"] == {"slots_data": False, "view_data": False}
        assert result["view"]["rows"] == 1
