"""Lakebase ``__overrides`` layer: DDL, views and the store's edit operations.

The SQL cannot run against Postgres here, so the DDL is checked for the properties
the effective graph depends on, and the store is checked for the statements it
issues and for the transaction they share.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("psycopg")

from back.core.errors import ValidationError
from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase.FlatStore import FlatStore
from back.core.graphdb.lakebase.SimulationStore import SimulationStore

pytestmark = pytest.mark.unit

T1 = {"subject": "http://ex/s", "predicate": "http://ex/p", "object": "http://ex/o"}
T2 = {"subject": "http://ex/s", "predicate": "http://ex/p", "object": "http://ex/o2"}


@pytest.fixture
def store():
    auth = MagicMock()
    auth.database = "appdb"
    s = FlatStore(auth, schema="ontobricks_graph")
    s._override_ready.add("G_V1")  # layout is covered separately
    s._mark_cache_stale = MagicMock()
    return s


def _cursor_cm(cur):
    @contextmanager
    def cm():
        yield cur

    return cm


def _txn_cm(cur):
    @contextmanager
    def cm():
        yield MagicMock(), cur

    return cm


def _staged_cursor():
    cur = MagicMock()
    cur.rowcount = 0
    copy_ctx = MagicMock()
    copy_ctx.__enter__.return_value = MagicMock()
    copy_ctx.__exit__.return_value = False
    cur.copy.return_value = copy_ctx
    return cur


def _sql(cur):
    return [str(c.args[0]) for c in cur.execute.call_args_list]


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


class TestDdl:
    def test_names(self):
        assert _companion_ddl.companion_phy("G_V1") == "g_v1__overrides"
        assert _companion_ddl.legacy_companion_phy("G_V1") == "g_v1__app"
        assert _companion_ddl.asserted_phy("G_V1") == "g_v1_asserted"

    def test_asserted_name_stays_within_the_postgres_limit(self):
        assert len(_companion_ddl.asserted_phy("g" * 100)) <= 63

    def test_union_view_is_synced_plus_adds_minus_dels(self):
        sql = _companion_ddl.union_view_sql(
            "g_v1", "g_v1_sync", "g_v1__overrides", asserted=False
        )
        assert "FROM g_v1_sync s" in sql
        assert "FROM g_v1__overrides a WHERE a.op = 'add'" in sql
        assert sql.count("NOT EXISTS") == 2
        assert "d.op = 'del'" in sql
        assert "origin = 'user'" not in sql

    def test_asserted_view_only_keeps_user_adds(self):
        sql = _companion_ddl.union_view_sql(
            "g_v1_asserted", "g_v1_sync", "g_v1__overrides", asserted=True
        )
        assert "a.op = 'add' AND a.origin = 'user'" in sql

    def test_ensure_union_view_creates_the_asserted_view_on_request(self):
        cur = MagicMock()
        _companion_ddl.ensure_union_view(
            cur, "g_v1", "g_v1_sync", "g_v1__overrides", "g_v1_asserted"
        )
        executed = _sql(cur)
        assert any("CREATE OR REPLACE VIEW g_v1 AS" in s for s in executed)
        assert any("CREATE OR REPLACE VIEW g_v1_asserted AS" in s for s in executed)

    def test_clear_derived_spares_user_rows(self):
        cur = MagicMock()
        cur.rowcount = 4
        assert _companion_ddl.clear_derived(cur, "g_v1__overrides") == 4
        assert _sql(cur) == ["DELETE FROM g_v1__overrides WHERE origin <> 'user'"]


class TestCompanionMigration:
    def _run(self, *, existed: bool, legacy_exists: bool):
        cur = MagicMock()
        cur.fetchone.side_effect = [object() if existed else None] + (
            [object() if legacy_exists else None] if not existed else []
        ) + [None] * 10
        with patch.object(_companion_ddl, "ensure_pgcrypto"), patch.object(
            _companion_ddl, "_create_triple_table"
        ):
            created = _companion_ddl.ensure_companion(
                cur,
                "sch",
                "g_v1__overrides",
                legacy="g_v1__app",
                origin_expr="'cohort'",
            )
        return created, _sql(cur)

    def test_a_new_table_is_seeded_from_the_legacy_one(self):
        created, executed = self._run(existed=False, legacy_exists=True)
        assert created is True
        seed = next(s for s in executed if s.startswith("INSERT INTO g_v1__overrides"))
        assert "FROM g_v1__app" in seed
        assert "'add', 'cohort'" in seed
        assert "ON CONFLICT DO NOTHING" in seed

    def test_nothing_to_seed_without_a_legacy_table(self):
        created, executed = self._run(existed=False, legacy_exists=False)
        assert created is True
        assert not any(s.startswith("INSERT INTO") for s in executed)

    def test_an_existing_table_is_never_reseeded(self):
        created, executed = self._run(existed=True, legacy_exists=True)
        assert created is False
        assert not any(s.startswith("INSERT INTO") for s in executed)


# ---------------------------------------------------------------------------
# Edits
# ---------------------------------------------------------------------------


class TestAddTriples:
    def test_restore_then_insert_in_one_transaction(self, store):
        cur = _staged_cursor()
        with patch.object(store, "_txn_cursor", _txn_cm(cur)):
            assert store.add_triples("G_V1", [T1, dict(T1)], author="bob") == 1
        statements = [s for s in _sql(cur) if "_ob_copy_stage t" in s]
        assert len(statements) == 2
        restore, insert = statements
        assert restore.startswith("DELETE FROM g_v1__overrides d USING")
        assert "d.op = 'del'" in restore
        assert insert.startswith("INSERT INTO g_v1__overrides")
        assert "'add', 'user'" in insert
        assert "FROM g_v1 g" in insert  # visibility is checked on the union view
        insert_call = [
            c for c in cur.execute.call_args_list if "'add', 'user'" in str(c.args[0])
        ][0]
        assert insert_call.args[1] == (None, "bob")
        store._mark_cache_stale.assert_called_once()

    def test_nothing_to_add(self, store):
        assert store.add_triples("G_V1", []) == 0
        store._mark_cache_stale.assert_not_called()

    def test_one_transaction_per_batch(self, store):
        cur = _staged_cursor()
        entered = []

        @contextmanager
        def counting_txn():
            entered.append(1)
            yield MagicMock(), cur

        triples = [{**T1, "object": f"http://ex/o{i}"} for i in range(5)]
        with patch.object(store, "_txn_cursor", counting_txn):
            assert store.add_triples("G_V1", triples, batch_size=2) == 5
        assert len(entered) == 3


class TestDeleteTriples:
    def test_hide_then_drop_user_adds_in_one_transaction(self, store):
        cur = _staged_cursor()
        with patch.object(store, "_txn_cursor", _txn_cm(cur)):
            assert store.delete_triples("G_V1", [T1], author="bob", change_id="c1") == 1
        hide = next(s for s in _sql(cur) if s.startswith("INSERT INTO g_v1__overrides"))
        drop = next(s for s in _sql(cur) if s.startswith("DELETE FROM g_v1__overrides a"))
        assert _sql(cur).index(hide) < _sql(cur).index(drop)
        assert "'del', 'user'" in hide
        assert "FROM g_v1_sync s" in hide  # mapped triples can be hidden
        assert "a.origin <> 'user'" in hide  # derived triples too
        assert "a.op = 'add' AND a.origin = 'user'" in drop
        hide_call = [
            c for c in cur.execute.call_args_list if "'del', 'user'" in str(c.args[0])
        ][0]
        assert hide_call.args[1] == ("c1", "bob")
        store._mark_cache_stale.assert_called_once()

    def test_a_sandbox_still_deletes_physically(self, store):
        store.supports_overrides = False
        with patch.object(store, "_delete_triples_physical", return_value=1) as phys:
            assert store.delete_triples("G_V1", [T1]) == 1
        phys.assert_called_once()

    def test_the_simulation_store_has_no_overrides_layer(self):
        assert SimulationStore.supports_overrides is False
        assert FlatStore.supports_overrides is True


class TestModify:
    def test_adds_the_new_value_before_deleting_the_old_one(self, store):
        order = []
        with patch.object(
            store, "add_triples", side_effect=lambda *a, **k: order.append(("add", k))
        ), patch.object(
            store,
            "delete_triples",
            side_effect=lambda *a, **k: order.append(("del", k)),
        ):
            change_id = store.modify_triple("G_V1", T1, T2, author="bob")
        assert [step for step, _ in order] == ["add", "del"]
        assert all(k["change_id"] == change_id for _, k in order)

    def test_rejects_a_no_op(self, store):
        with pytest.raises(ValidationError):
            store.modify_triple("G_V1", T1, dict(T1))


class TestDerivedWrites:
    def test_a_derived_insert_cannot_claim_the_user_origin(self, store):
        with pytest.raises(ValidationError):
            store.insert_triples("G_V1", [T1], origin="user")

    def test_executemany_tags_the_origin(self, store):
        cur = MagicMock()
        with patch.object(store, "_cursor", _cursor_cm(cur)):
            store.insert_triples("G_V1", [T1], origin="community")
        sql = cur.executemany.call_args.args[0]
        rows = cur.executemany.call_args.args[1]
        assert "op, origin" in sql
        assert rows[0][-1] == "community"

    def test_the_sandbox_insert_keeps_plain_columns(self, store):
        store.supports_overrides = False
        cur = MagicMock()
        with patch.object(store, "_cursor", _cursor_cm(cur)):
            store.insert_triples("G_V1", [T1])
        sql = cur.executemany.call_args.args[0]
        assert "origin" not in sql


class TestPartitionsAndStats:
    def test_cohort_delete_targets_only_cohort_rows(self, store):
        cur = MagicMock()
        cur.rowcount = 3
        with patch.object(store, "_cursor", _cursor_cm(cur)):
            assert store.delete_cohort_triples("G_V1", "http://ex/c/", "http://ex/in") == 3
        sql, params = cur.execute.call_args.args
        assert "FROM g_v1__overrides WHERE origin = %s" in sql
        assert params[0] == "cohort"

    def test_community_delete_targets_only_community_rows(self, store):
        cur = MagicMock()
        cur.rowcount = 1
        with patch.object(store, "_cursor", _cursor_cm(cur)):
            store.delete_community_triples("G_V1", "http://ex/k/", "http://ex/inK")
        assert cur.execute.call_args.args[1][0] == "community"

    def test_an_empty_prefix_deletes_nothing(self, store):
        assert store.delete_cohort_triples("G_V1", "", "http://ex/in") == 0

    def test_override_stats(self, store):
        store.execute_query = MagicMock(
            side_effect=[[{"user_adds": 2, "user_dels": 3}], [{"cnt": 1}]]
        )
        assert store.get_override_stats("G_V1") == {
            "user_adds": 2,
            "user_dels": 3,
            "dormant_dels": 1,
        }

    def test_the_inferred_count_is_the_derived_adds(self, store):
        cur = MagicMock()
        cur.fetchone.return_value = {"cnt": 7}
        with patch.object(store, "_cursor", _cursor_cm(cur)):
            assert store.get_inferred_triple_count("G_V1") == 7
        assert "op = 'add' AND origin <> 'user'" in cur.execute.call_args.args[0]


# ---------------------------------------------------------------------------
# Layout, rebuild and reads
# ---------------------------------------------------------------------------


class TestLayout:
    def test_the_first_write_repoints_the_views_at_the_overrides(self):
        auth = MagicMock()
        s = FlatStore(auth, schema="ontobricks_graph")
        cur = MagicMock()
        with patch.object(s, "_cursor", _cursor_cm(cur)), patch.object(
            _companion_ddl, "ensure_companion", return_value=False
        ) as ensure, patch.object(
            _companion_ddl, "relation_exists", return_value=True
        ), patch.object(_companion_ddl, "ensure_union_view") as view:
            s._ensure_override_layout("G_V1")
            s._ensure_override_layout("G_V1")  # cached
        ensure.assert_called_once()
        view.assert_called_once()
        assert view.call_args.args[1:] == (
            "g_v1",
            "g_v1_sync",
            "g_v1__overrides",
            "g_v1_asserted",
        )

    def test_reset_for_rebuild_keeps_the_overrides(self, store):
        calls = []
        cur = MagicMock()
        with patch.object(
            store, "drop_table", side_effect=lambda *a, **k: calls.append(("drop", k))
        ), patch.object(
            store, "create_table", side_effect=lambda *a, **k: calls.append(("create", k))
        ), patch.object(store, "_cursor", _cursor_cm(cur)), patch.object(
            _companion_ddl, "relation_exists", return_value=True
        ), patch.object(_companion_ddl, "clear_derived") as clear:
            store.reset_for_rebuild("G_V1")
        assert calls == [("drop", {"keep_overrides": True}), ("create", {})]
        clear.assert_called_once_with(cur, "g_v1__overrides")

    def test_asserted_reads_use_the_asserted_view_when_it_exists(self, store):
        cur = MagicMock()
        with patch.object(store, "_cursor", _cursor_cm(cur)), patch.object(
            _companion_ddl, "relation_exists", return_value=True
        ):
            assert store.synced_table_name("G_V1") == "g_v1_asserted"

    def test_asserted_reads_fall_back_to_the_sync_table(self, store):
        cur = MagicMock()
        with patch.object(store, "_cursor", _cursor_cm(cur)), patch.object(
            _companion_ddl, "relation_exists", return_value=False
        ):
            assert store.synced_table_name("G_V1") == "g_v1_sync"

    def test_a_probe_failure_falls_back_to_the_sync_table(self, store):
        with patch.object(store, "_cursor", side_effect=RuntimeError("down")):
            assert store.synced_table_name("G_V1") == "g_v1_sync"


class TestBuildPaths:
    def test_scheduled_builds_load_mapped_data_into_the_sync_table(self):
        from back.objects.registry.schedule.build import _stream_into_store

        fake_store = MagicMock()
        fake_store.bulk_load_into_sync.return_value = 5
        src = MagicMock()
        assert _stream_into_store(fake_store, src, "G_V1", "SELECT 1") == 5
        fake_store.bulk_insert_iter.assert_not_called()
