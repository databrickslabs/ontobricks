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

    def test_a_lakeflow_source_also_inserts_the_object_hash(self):
        sql = DeltaMergeMaterializer.merge_sql(VIEW, DATA, with_object_hash=True)

        assert (
            "WHEN NOT MATCHED THEN INSERT (subject, predicate, object, object_hash) "
            "VALUES (s.subject, s.predicate, s.object, sha2(s.object, 256))"
        ) in sql

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
