"""Unit tests for the object_hash primary-key fix (issue #108).

Postgres B-tree index tuples cannot exceed ~2704 bytes, so keying the triple
tables on the raw ``object`` column aborts the sync whenever a mapped literal is
larger than that.  These tests pin the DDL that keys the primary key on a
fixed-width ``object_hash`` digest and makes the object-bearing secondary
indexes size-guarded, plus the migration helper for pre-existing tables and the
Spark-side ``object_hash`` column used by ``managed_synced``.
"""

from unittest.mock import MagicMock

from back.core.graphdb.lakebase import _companion_ddl
from back.core.helpers import add_object_hash_column


def _executed(cur: MagicMock) -> str:
    """Concatenate every SQL string passed to ``cur.execute`` for substring checks."""
    return "\n".join(str(c[0][0]) for c in cur.execute.call_args_list)


class TestEnsureSyncedDDL:
    def test_synced_table_keys_pk_on_object_hash(self):
        cur = MagicMock()
        _companion_ddl.ensure_synced(cur, "ontobricks_graph", "g_v1_sync")
        sql = _executed(cur)
        assert "object_hash bytea GENERATED ALWAYS AS" in sql
        assert "sha256(convert_to(object, 'UTF8'))" in sql
        assert "PRIMARY KEY (subject, predicate, object_hash)" in sql
        # The raw object must NOT be part of the primary key any more.
        assert "PRIMARY KEY (subject, predicate, object)" not in sql

    def test_companion_table_keys_pk_on_object_hash(self):
        cur = MagicMock()
        _companion_ddl.ensure_companion(cur, "ontobricks_graph", "g_v1__app")
        sql = _executed(cur)
        assert "PRIMARY KEY (subject, predicate, object_hash)" in sql
        assert "PRIMARY KEY (subject, predicate, object)" not in sql

    def test_object_bearing_indexes_are_size_guarded(self):
        cur = MagicMock()
        _companion_ddl.ensure_synced(cur, "ontobricks_graph", "g_v1_sync")
        idx_stmts = [
            str(c[0][0])
            for c in cur.execute.call_args_list
            if "CREATE INDEX" in str(c[0][0])
        ]
        po = next(s for s in idx_stmts if "(predicate, object)" in s)
        ops = next(s for s in idx_stmts if "(object, predicate)" in s)
        assert "WHERE octet_length(object) <= 2000" in po
        assert "WHERE octet_length(object) <= 2000" in ops
        # The subject/predicate index carries no object column, so it stays full.
        sp = next(s for s in idx_stmts if "(subject, predicate)" in s)
        assert "octet_length" not in sp


class TestObjectHashMigration:
    def test_migration_adds_column_swaps_pk_and_drops_legacy_indexes(self):
        cur = MagicMock()
        _companion_ddl.ensure_object_hash_pk(cur, "g_v1_sync")
        sql = _executed(cur)
        assert "ADD COLUMN IF NOT EXISTS object_hash bytea" in sql
        # PK swap only fires for the legacy (subject,predicate,object) shape.
        assert "IF pk_cols = 'subject,predicate,object' THEN" in sql
        assert "ADD PRIMARY KEY (subject, predicate, object_hash)" in sql
        # Legacy full-object secondary indexes are dropped so the partial ones win.
        assert "DROP INDEX IF EXISTS g_g_v1_sync_po" in sql
        assert "DROP INDEX IF EXISTS g_g_v1_sync_ops" in sql

    def test_migration_is_best_effort_on_owner_error(self):
        cur = MagicMock()
        cur.execute.side_effect = RuntimeError("must be owner of table g_v1_sync")
        # Must not raise: a table owned by another role should never abort a build.
        _companion_ddl.ensure_object_hash_pk(cur, "g_v1_sync")


class TestAddObjectHashColumn:
    def test_wraps_select_with_sha2_object_hash(self):
        wrapped = add_object_hash_column(
            "SELECT subject, predicate, object FROM t"
        )
        assert "sha2(CAST(_obh.object AS STRING), 256) AS object_hash" in wrapped
        assert "SELECT subject, predicate, object FROM t" in wrapped

    def test_strips_trailing_semicolon_before_wrapping(self):
        wrapped = add_object_hash_column("SELECT 1 AS object;")
        assert wrapped.count(";") == 0
        assert "object_hash" in wrapped

    def test_empty_input_returns_input_unchanged(self):
        assert add_object_hash_column("") == ""
