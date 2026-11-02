"""Unit tests for ``DeltaMergeMaterializer`` (``_data`` kept in step by MERGE)."""

from __future__ import annotations

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.graphdb.delta import DeltaMergeMaterializer as ExportedMaterializer
from back.core.graphdb.delta.DeltaMergeMaterializer import DeltaMergeMaterializer

pytestmark = pytest.mark.unit

VIEW = "main.ob.triplestore_d_V1"
DATA = "main.ob.triplestore_d_V1_data"

PROPERTIES = (
    "(delta.enableRowTracking = true, delta.enableDeletionVectors = true, "
    "delta.enableChangeDataFeed = true)"
)


class RecordingClient:
    """Warehouse double: records statements; fails those containing *fail_on*."""

    def __init__(self, fail_on: str | None = None):
        self.statements: list[str] = []
        self._fail_on = fail_on

    def execute_statement(self, sql):
        self.statements.append(sql)
        if self._fail_on and self._fail_on in sql:
            raise RuntimeError("warehouse refused")
        return True


class TestPackageExport:
    def test_is_reexported_from_the_delta_package(self):
        assert ExportedMaterializer is DeltaMergeMaterializer


class TestMergeSql:
    def test_exact_statement(self):
        assert DeltaMergeMaterializer.merge_sql(VIEW, DATA) == (
            f"MERGE INTO {DATA} t\n"
            f"USING (SELECT subject, predicate, object FROM {VIEW} "
            "GROUP BY subject, predicate, object) s\n"
            "  ON t.subject = s.subject AND t.predicate = s.predicate "
            "AND t.object <=> s.object\n"
            "WHEN NOT MATCHED THEN INSERT (subject, predicate, object) "
            "VALUES (s.subject, s.predicate, s.object)\n"
            "WHEN NOT MATCHED BY SOURCE THEN DELETE"
        )

    def test_a_lakeflow_source_hashes_new_rows_and_backfills_old_ones(self):
        assert DeltaMergeMaterializer.merge_sql(VIEW, DATA, with_object_hash=True) == (
            f"MERGE INTO {DATA} t\n"
            f"USING (SELECT subject, predicate, object FROM {VIEW} "
            "GROUP BY subject, predicate, object) s\n"
            "  ON t.subject = s.subject AND t.predicate = s.predicate "
            "AND t.object <=> s.object\n"
            "WHEN MATCHED AND t.object_hash IS NULL "
            "THEN UPDATE SET object_hash = sha2(s.object, 256)\n"
            "WHEN NOT MATCHED THEN INSERT (subject, predicate, object, object_hash) "
            "VALUES (s.subject, s.predicate, s.object, sha2(s.object, 256))\n"
            "WHEN NOT MATCHED BY SOURCE THEN DELETE"
        )

    def test_without_the_hash_matched_rows_are_left_alone(self):
        assert "WHEN MATCHED" not in DeltaMergeMaterializer.merge_sql(VIEW, DATA)

    @pytest.mark.parametrize(("view", "data"), [("", DATA), (VIEW, "")])
    def test_names_are_required(self, view, data):
        with pytest.raises(ValidationError):
            DeltaMergeMaterializer.merge_sql(view, data)


class TestEnsureTableSql:
    def test_exact_statement(self):
        assert DeltaMergeMaterializer.ensure_table_sql(DATA) == (
            f"CREATE TABLE IF NOT EXISTS {DATA} "
            "(subject STRING, predicate STRING, object STRING) USING DELTA "
            f"CLUSTER BY (predicate, subject) TBLPROPERTIES {PROPERTIES}"
        )

    def test_a_lakeflow_source_has_an_object_hash_column(self):
        sql = DeltaMergeMaterializer.ensure_table_sql(DATA, with_object_hash=True)

        assert "(subject STRING, predicate STRING, object STRING, object_hash STRING)" in sql


class TestFingerprint:
    ARGS = ({"main.s.a": 3, "main.s.b": 7}, "@prefix : <x> .", "http://ex.org/")

    def test_is_a_sha256_hex_digest(self):
        value = DeltaMergeMaterializer.fingerprint(*self.ARGS)

        assert len(value) == 64 and int(value, 16) >= 0

    def test_is_stable_whatever_the_order_of_the_sources(self):
        reordered = ({"main.s.b": 7, "main.s.a": 3}, *self.ARGS[1:])

        assert DeltaMergeMaterializer.fingerprint(*reordered) == DeltaMergeMaterializer.fingerprint(
            *self.ARGS
        )

    @pytest.mark.parametrize(
        "changed",
        [
            ({"main.s.a": 4, "main.s.b": 7}, "@prefix : <x> .", "http://ex.org/"),
            ({"main.s.a": 3}, "@prefix : <x> .", "http://ex.org/"),
            ({"main.s.a": 3, "main.s.b": 7}, "@prefix : <y> .", "http://ex.org/"),
            ({"main.s.a": 3, "main.s.b": 7}, "@prefix : <x> .", "http://ex.org/v2/"),
        ],
        ids=["version", "source set", "r2rml", "base uri"],
    )
    def test_changes_with_every_input(self, changed):
        assert DeltaMergeMaterializer.fingerprint(*changed) != DeltaMergeMaterializer.fingerprint(
            *self.ARGS
        )

    def test_changes_with_the_generated_view_sql(self):
        base = DeltaMergeMaterializer.fingerprint(*self.ARGS)

        assert DeltaMergeMaterializer.fingerprint(*self.ARGS, view_sql="SELECT 1") != base
        assert DeltaMergeMaterializer.fingerprint(*self.ARGS, view_sql="") == base


class TestApply:
    def test_clears_other_relation_kinds_then_ensures_and_merges(self):
        client = RecordingClient()

        DeltaMergeMaterializer.apply(client, VIEW, DATA)

        assert client.statements == [
            f"DROP MATERIALIZED VIEW IF EXISTS {DATA}",
            f"DROP VIEW IF EXISTS {DATA}",
            DeltaMergeMaterializer.ensure_table_sql(DATA),
            f"ALTER TABLE {DATA} SET TBLPROPERTIES {PROPERTIES}",
            DeltaMergeMaterializer.merge_sql(VIEW, DATA),
        ]

    def test_the_object_hash_flag_reaches_both_statements(self):
        client = RecordingClient()

        DeltaMergeMaterializer.apply(client, VIEW, DATA, with_object_hash=True)

        assert DeltaMergeMaterializer.ensure_table_sql(DATA, with_object_hash=True) in (
            client.statements
        )
        assert DeltaMergeMaterializer.merge_sql(VIEW, DATA, with_object_hash=True) in (
            client.statements
        )

    def test_a_refused_drop_is_harmless(self):
        client = RecordingClient(fail_on="DROP")

        DeltaMergeMaterializer.apply(client, VIEW, DATA)

        assert client.statements[-1].startswith(f"MERGE INTO {DATA}")

    def test_a_refused_merge_becomes_an_infrastructure_error(self):
        with pytest.raises(InfrastructureError):
            DeltaMergeMaterializer.apply(RecordingClient(fail_on="MERGE INTO"), VIEW, DATA)

    def test_an_invalid_name_runs_nothing(self):
        client = RecordingClient()

        with pytest.raises(ValidationError):
            DeltaMergeMaterializer.apply(client, "", DATA)
        assert client.statements == []


_ALL_PROPERTIES = {
    "delta.enableRowTracking": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableChangeDataFeed": "true",
}


class DescribingClient(RecordingClient):
    """Also answers ``SHOW TBLPROPERTIES`` and ``SHOW COLUMNS`` of the merge target."""

    def __init__(self, *, properties=None, columns=("subject", "predicate", "object")):
        super().__init__()
        self.queries: list[str] = []
        self._properties = dict(_ALL_PROPERTIES if properties is None else properties)
        self._columns = list(columns)

    def execute_query(self, sql):
        self.queries.append(sql)
        if sql.startswith("SHOW TBLPROPERTIES"):
            return [{"key": k, "value": v} for k, v in self._properties.items()]
        if sql.startswith("SHOW COLUMNS"):
            return [{"col_name": c} for c in self._columns]
        return []


def _alters(client) -> list:
    return [s for s in client.statements if s.startswith(f"ALTER TABLE {DATA}")]


class TestApplyTouchesOnlyWhatIsMissing:
    """No metadata commit per build: the table is altered only when it lacks something."""

    def test_a_table_with_every_property_is_not_altered(self):
        client = DescribingClient()

        DeltaMergeMaterializer.apply(client, VIEW, DATA)

        assert _alters(client) == []
        assert client.queries == [f"SHOW TBLPROPERTIES {DATA}"]
        assert client.statements[-1].startswith(f"MERGE INTO {DATA}")

    @pytest.mark.parametrize("missing", list(_ALL_PROPERTIES))
    def test_a_missing_property_is_set(self, missing):
        properties = {k: v for k, v in _ALL_PROPERTIES.items() if k != missing}
        client = DescribingClient(properties=properties)

        DeltaMergeMaterializer.apply(client, VIEW, DATA)

        assert _alters(client) == [f"ALTER TABLE {DATA} SET TBLPROPERTIES {PROPERTIES}"]

    def test_a_property_set_to_false_is_set(self):
        properties = {**_ALL_PROPERTIES, "delta.enableChangeDataFeed": "false"}
        client = DescribingClient(properties=properties)

        DeltaMergeMaterializer.apply(client, VIEW, DATA)

        assert len(_alters(client)) == 1

    def test_an_unreadable_table_is_altered_as_before(self):
        client = RecordingClient()  # no execute_query at all

        DeltaMergeMaterializer.apply(client, VIEW, DATA)

        assert _alters(client) == [f"ALTER TABLE {DATA} SET TBLPROPERTIES {PROPERTIES}"]

    def test_a_missing_object_hash_column_is_added_before_the_merge(self):
        client = DescribingClient()

        DeltaMergeMaterializer.apply(client, VIEW, DATA, with_object_hash=True)

        add = f"ALTER TABLE {DATA} ADD COLUMNS (object_hash STRING)"
        assert _alters(client) == [add]
        assert client.statements.index(add) < next(
            i for i, s in enumerate(client.statements) if s.startswith("MERGE INTO")
        )

    def test_the_rows_of_an_added_object_hash_column_are_backfilled_by_the_merge(self):
        client = DescribingClient()

        DeltaMergeMaterializer.apply(client, VIEW, DATA, with_object_hash=True)

        ensure = DeltaMergeMaterializer.ensure_table_sql(DATA, with_object_hash=True)
        add = f"ALTER TABLE {DATA} ADD COLUMNS (object_hash STRING)"
        merge = DeltaMergeMaterializer.merge_sql(VIEW, DATA, with_object_hash=True)
        assert [s for s in client.statements if not s.startswith("DROP")] == [ensure, add, merge]
        assert client.queries == [f"SHOW TBLPROPERTIES {DATA}", f"SHOW COLUMNS IN {DATA}"]
        assert "WHEN MATCHED AND t.object_hash IS NULL" in merge

    def test_an_existing_object_hash_column_is_kept(self):
        client = DescribingClient(columns=("subject", "predicate", "object", "object_hash"))

        DeltaMergeMaterializer.apply(client, VIEW, DATA, with_object_hash=True)

        assert _alters(client) == []

    def test_without_the_hash_the_columns_are_not_read(self):
        client = DescribingClient()

        DeltaMergeMaterializer.apply(client, VIEW, DATA)

        assert not any(q.startswith("SHOW COLUMNS") for q in client.queries)

