"""Delta ``…_overrides`` layer: contract, SQL builders and store behaviour.

The SQL cannot run against a warehouse here, so the builders are checked for the
properties the effective graph depends on, and the store is checked for the
*order* of the statements it issues (the Delta backend has no multi-statement
transaction; crash safety comes from that order).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ValidationError
from back.core.graphdb import overrides
from back.core.graphdb.delta import _table_naming, materialize
from back.core.graphdb.delta.MaterializedStore import MaterializedStore
from back.core.graphdb.inferred_breakdown import build_breakdown_sql

pytestmark = pytest.mark.unit

OVR = "cat.sch.triplestore_dom_V1_overrides"
DATA = "cat.sch.triplestore_dom_V1_data"
GRAPH = "cat.sch.triplestore_dom_V1_graph"
ASSERTED = "cat.sch.triplestore_dom_V1_asserted"

T1 = {"subject": "http://ex/s", "predicate": "http://ex/p", "object": "http://ex/o"}
T2 = {"subject": "http://ex/s", "predicate": "http://ex/p", "object": "http://ex/o2"}


def _domain():
    d = MagicMock()
    d.info = {"name": "dom"}
    d.current_version = 1
    d.delta = {"catalog": "cat", "schema": "sch"}
    d.ontology = {"base_uri": "http://ex/"}
    return d


def _store():
    client = MagicMock()
    store = MaterializedStore(client, domain=_domain())
    # Companion objects are covered separately; keep the statements under test.
    store._ensure_companion_objects = MagicMock(return_value=OVR)
    store._mark_cache_stale = MagicMock()
    return store, client


def _statements(client):
    return [c.args[0] for c in client.execute_statement.call_args_list]


# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------


class TestContract:
    def test_validate_op_rejects_unknown_values(self):
        assert overrides.validate_op("add") == "add"
        with pytest.raises(ValidationError):
            overrides.validate_op("update")

    def test_validate_origin_rejects_unknown_values(self):
        assert overrides.validate_origin("user") == "user"
        with pytest.raises(ValidationError):
            overrides.validate_origin("bot")

    def test_derived_writers_cannot_claim_the_user_origin(self):
        with pytest.raises(ValidationError):
            overrides.validate_derived_origin("user")
        for origin in overrides.DERIVED_ORIGINS:
            assert overrides.validate_derived_origin(origin) == origin

    def test_change_ids_are_unique(self):
        assert overrides.new_change_id() != overrides.new_change_id()

    def test_unique_triples_keeps_first_seen_order(self):
        assert overrides.unique_triples([T1, T2, dict(T1)]) == [T1, T2]


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------


class TestNaming:
    def test_overrides_and_asserted_fqns(self):
        domain = _domain()
        assert _table_naming.overrides_fqn(domain, None) == OVR
        assert _table_naming.asserted_view_fqn(domain, None) == ASSERTED

    def test_new_suffixes_are_known_graph_leaves(self):
        leaves = _table_naming.graph_leaf_suffixes()
        assert "_overrides" in leaves
        assert "_asserted" in leaves


# ---------------------------------------------------------------------------
# SQL builders
# ---------------------------------------------------------------------------


class TestSqlBuilders:
    def test_table_has_every_contract_column(self):
        sql = materialize.build_ensure_overrides_sql(OVR)
        for column in (
            "subject",
            "predicate",
            "object",
            "op",
            "origin",
            "change_id",
            "author",
            "created_at",
        ):
            assert column in sql
        assert "CLUSTER BY (predicate, subject)" in sql

    def test_graph_view_is_mapped_plus_adds_minus_dels(self):
        sql = materialize.build_union_view_sql(GRAPH, DATA, OVR)
        assert f"FROM {DATA}" in sql
        assert "op = 'add'" in sql
        assert "LEFT ANTI JOIN" in sql
        assert "op = 'del'" in sql

    def test_asserted_view_leaves_derived_rows_out(self):
        sql = materialize.build_asserted_view_sql(ASSERTED, DATA, OVR)
        assert "op = 'add' AND origin = 'user'" in sql
        assert "LEFT ANTI JOIN" in sql

    def test_clear_derived_never_touches_user_rows(self):
        sql = materialize.build_clear_derived_sql(OVR)
        assert sql == (
            f"DELETE FROM {OVR} WHERE COALESCE(origin, '') <> 'user'"
        )

    def test_clear_origin_is_scoped_to_one_origin_and_prefix(self):
        sql = materialize.build_clear_origin_sql(
            OVR, "cohort", "http://ex/cohort/c1", "http://ex/inCohort"
        )
        assert "origin = 'cohort'" in sql
        assert "subject LIKE 'http://ex/cohort/c1%'" in sql

    def test_add_user_skips_triples_already_in_the_graph(self):
        sql = materialize.build_add_user_sql(
            OVR, GRAPH, [T1], author="bob", change_id="c1"
        )
        assert "'add', 'user', 'c1', 'bob'" in sql
        assert f"NOT EXISTS (SELECT 1 FROM {GRAPH}" in sql

    def test_hide_only_targets_triples_that_are_visible(self):
        sql = materialize.build_hide_sql(OVR, DATA, [T1], change_id="c1")
        assert "'del', 'user', 'c1', NULL" in sql
        assert f"EXISTS (SELECT 1 FROM {DATA}" in sql
        assert "<> 'user'" in sql  # derived adds count as visible too
        assert f"NOT EXISTS (SELECT 1 FROM {OVR} x WHERE x.op = 'del'" in sql

    def test_literals_are_escaped(self):
        nasty = {"subject": "s", "predicate": "p", "object": "it's"}
        sql = materialize.build_restore_sql(OVR, [nasty])
        assert "it\\'s" in sql or "it''s" in sql
        assert "'it's'" not in sql

    def test_builders_reject_empty_table_names(self):
        with pytest.raises(ValidationError):
            materialize.build_ensure_overrides_sql("")


# ---------------------------------------------------------------------------
# Legacy migration
# ---------------------------------------------------------------------------


class TestMigration:
    def test_migrate_copies_inferred_rows_as_derived_adds(self):
        sql = materialize.build_migrate_inferred_sql(
            OVR, "cat.sch.triplestore_dom_V1_inferred", "'reasoning'"
        )
        assert "'add'" in sql
        assert "FROM cat.sch.triplestore_dom_V1_inferred" in sql

    def test_store_seeds_a_new_overrides_table_from_the_legacy_one(self):
        client = MagicMock()
        store = MaterializedStore(client, domain=_domain())
        with (
            patch.object(materialize, "ensure_overrides_table", return_value=True),
            patch.object(materialize, "ensure_graph_view"),
            patch.object(materialize, "ensure_asserted_view"),
            patch.object(materialize, "migrate_inferred", return_value=True) as mig,
        ):
            store._ensure_companion_objects("dom_V1")
        mig.assert_called_once()
        assert mig.call_args.args[1] == OVR
        assert mig.call_args.args[2] == "cat.sch.triplestore_dom_V1_inferred"

    def test_store_does_not_migrate_an_existing_overrides_table(self):
        client = MagicMock()
        store = MaterializedStore(client, domain=_domain())
        with (
            patch.object(materialize, "ensure_overrides_table", return_value=False),
            patch.object(materialize, "ensure_graph_view"),
            patch.object(materialize, "ensure_asserted_view"),
            patch.object(materialize, "migrate_inferred") as mig,
        ):
            store._ensure_companion_objects("dom_V1")
        mig.assert_not_called()

    def test_companions_are_ensured_once_per_store(self):
        client = MagicMock()
        store = MaterializedStore(client, domain=_domain())
        with (
            patch.object(
                materialize, "ensure_overrides_table", return_value=False
            ) as ensure,
            patch.object(materialize, "ensure_graph_view"),
            patch.object(materialize, "ensure_asserted_view"),
        ):
            store._ensure_companion_objects("dom_V1")
            store._ensure_companion_objects("dom_V1")
        ensure.assert_called_once()


# ---------------------------------------------------------------------------
# Store: user edits
# ---------------------------------------------------------------------------


class TestUserEdits:
    def test_add_restores_then_inserts(self):
        store, client = _store()
        assert store.add_triples("dom_V1", [T1, dict(T1)], author="bob") == 1
        restore, insert = _statements(client)
        assert restore.startswith(f"MERGE INTO {OVR}")
        assert "t.op = 'del'" in restore
        assert insert.startswith(f"INSERT INTO {OVR}")
        assert "'add', 'user'" in insert
        store._mark_cache_stale.assert_called_once()

    def test_delete_hides_before_dropping_user_adds(self):
        store, client = _store()
        assert store.delete_triples("dom_V1", [T1]) == 1
        hide, drop = _statements(client)
        assert hide.startswith(f"INSERT INTO {OVR}")
        assert "'del', 'user'" in hide
        assert drop.startswith(f"MERGE INTO {OVR}")
        assert "t.op = 'add'" in drop and "t.origin = 'user'" in drop
        store._mark_cache_stale.assert_called_once()

    def test_empty_batches_do_not_invalidate_the_cache(self):
        store, client = _store()
        assert store.add_triples("dom_V1", []) == 0
        assert store.delete_triples("dom_V1", []) == 0
        assert _statements(client) == []
        store._mark_cache_stale.assert_not_called()

    def test_batches_split_large_edits(self):
        store, client = _store()
        triples = [{**T1, "object": f"http://ex/o{i}"} for i in range(5)]
        assert store.add_triples("dom_V1", triples, batch_size=2) == 5
        assert len(_statements(client)) == 6  # 3 batches x (restore + insert)

    def test_modify_adds_the_new_value_before_deleting_the_old_one(self):
        store, client = _store()
        change_id = store.modify_triple("dom_V1", T1, T2, author="bob")
        statements = _statements(client)
        assert len(statements) == 4
        assert "'add', 'user'" in statements[1]
        assert "'del', 'user'" in statements[2]
        assert all(change_id in s for s in (statements[1], statements[2]))

    def test_modify_rejects_a_no_op(self):
        store, client = _store()
        with pytest.raises(ValidationError):
            store.modify_triple("dom_V1", T1, dict(T1))
        assert _statements(client) == []

    def test_edits_need_a_domain_bound_store(self):
        store = MaterializedStore(MagicMock())
        with pytest.raises(ValidationError):
            store.add_triples("t", [T1])
        with pytest.raises(ValidationError):
            store.modify_triple("t", T1, T2)


class TestRawModeDelete:
    def test_raw_store_deletes_physically(self):
        client = MagicMock()
        store = MaterializedStore(client)
        assert store.delete_triples("cat.sch.sandbox", [T1]) == 1
        (sql,) = _statements(client)
        assert sql.startswith("MERGE INTO cat.sch.sandbox")
        assert "WHEN MATCHED THEN DELETE" in sql


# ---------------------------------------------------------------------------
# Store: derived writers
# ---------------------------------------------------------------------------


class TestDerivedWriters:
    def test_insert_tags_rows_with_their_origin(self):
        sql = MaterializedStore._insert_sql(OVR, [T1], "cohort")
        assert "'add', 'cohort'" in sql
        assert MaterializedStore._insert_sql("t", [T1]).count("VALUES") == 1

    def test_raw_insert_keeps_three_columns(self):
        sql = MaterializedStore._insert_sql("cat.sch.sandbox", [T1])
        assert "(subject, predicate, object) VALUES" in sql

    def test_derived_insert_rejects_the_user_origin(self):
        store, _ = _store()
        with pytest.raises(ValidationError):
            store.insert_triples("dom_V1", [T1], origin="user")

    def test_insert_from_select_projects_override_columns(self):
        store, client = _store()
        store.insert_triples_from_select(
            "dom_V1", "SELECT s AS subject, p AS predicate, o AS object FROM x",
            origin="community",
        )
        (sql,) = _statements(client)
        assert "'add', 'community'" in sql
        assert "FROM (SELECT s AS subject" in sql

    def test_partition_delete_targets_only_the_given_origin(self):
        store, client = _store()
        store._writable_table_fqn = MagicMock(return_value=OVR)
        store.delete_cohort_triples("dom_V1", "http://ex/cohort/c1", "http://ex/in")
        store.delete_community_triples("dom_V1", "http://ex/cl/c1", "http://ex/inC")
        cohort_sql, community_sql = _statements(client)
        assert "origin = 'cohort'" in cohort_sql
        assert "origin = 'community'" in community_sql

    def test_overrides_stats_counts_each_kind(self):
        store, client = _store()
        store._writable_table_fqn = MagicMock(return_value=OVR)
        store.execute_query = MagicMock(
            side_effect=[
                [{"user_adds": 2, "user_dels": 3}],
                [{"cnt": 1}],
            ]
        )
        stats = store.get_override_stats("dom_V1")
        assert stats == {"user_adds": 2, "user_dels": 3, "dormant_dels": 1}


# ---------------------------------------------------------------------------
# Breakdown by origin
# ---------------------------------------------------------------------------


class TestBreakdown:
    def test_breakdown_reads_derived_adds_by_origin(self):
        sql = build_breakdown_sql(OVR, "http://ex/", lambda v: v, has_origin=True)
        assert "op = 'add'" in sql
        assert "<> 'user'" in sql
        assert "origin" in sql

    def test_legacy_breakdown_stays_vocabulary_based(self):
        sql = build_breakdown_sql(
            "cat.sch.triplestore_dom_V1_inferred",
            "http://ex/",
            lambda v: v,
            has_origin=False,
        )
        assert "op = 'add'" not in sql


# ---------------------------------------------------------------------------
# Read routing
# ---------------------------------------------------------------------------


class TestHasUserOverrides:
    def _builder(self, client):
        from back.core.graphdb.delta.Builder import Builder

        return Builder(_domain(), client=client)

    def test_true_when_a_user_row_exists(self):
        client = MagicMock()
        client.execute_query.return_value = [{"ok": 1}]
        assert self._builder(client).has_user_overrides() is True
        sql = client.execute_query.call_args.args[0]
        assert "origin = 'user'" in sql and "LIMIT 1" in sql

    def test_false_when_no_user_row(self):
        client = MagicMock()
        client.execute_query.return_value = []
        assert self._builder(client).has_user_overrides() is False

    def test_false_when_table_missing(self):
        client = MagicMock()
        client.execute_query.side_effect = RuntimeError(
            "TABLE_OR_VIEW_NOT_FOUND: cat.sch.x"
        )
        assert self._builder(client).has_user_overrides() is False

    def test_other_errors_propagate(self):
        client = MagicMock()
        client.execute_query.side_effect = RuntimeError("warehouse unreachable")
        with pytest.raises(RuntimeError):
            self._builder(client).has_user_overrides()


class TestSyncedTableName:
    def test_prefers_asserted_view_once_it_exists(self):
        store, _ = _store()
        store.table_exists = MagicMock(return_value=True)
        assert store.synced_table_name(GRAPH) == ASSERTED

    def test_falls_back_to_data_before_the_view_exists(self):
        store, _ = _store()
        store.table_exists = MagicMock(return_value=False)
        assert store.synced_table_name(GRAPH) == DATA

    def test_data_and_asserted_names_pass_through(self):
        store, _ = _store()
        assert store.synced_table_name(DATA) == DATA
        assert store.synced_table_name(ASSERTED) == ASSERTED


class TestGraphQueryTable:
    def test_asserted_scope_goes_through_the_store(self):
        from back.core.helpers.SQLHelpers import SQLHelpers

        store = MagicMock()
        store.synced_table_name.return_value = ASSERTED
        with patch(
            "back.core.graphdb.GraphDBFactory.GraphDBFactory._resolve_triple_store_backend",
            return_value="databricks",
        ), patch.object(
            SQLHelpers, "effective_databricks_table", return_value=DATA
        ):
            table = SQLHelpers.effective_graph_query_table(
                _domain(), None, include_inferred=False, store=store
            )
        assert table == ASSERTED
        store.synced_table_name.assert_called_once_with(DATA)

    def test_asserted_scope_without_store_keeps_data(self):
        from back.core.helpers.SQLHelpers import SQLHelpers

        with patch(
            "back.core.graphdb.GraphDBFactory.GraphDBFactory._resolve_triple_store_backend",
            return_value="databricks",
        ), patch.object(
            SQLHelpers, "effective_databricks_table", return_value=DATA
        ):
            assert (
                SQLHelpers.effective_graph_query_table(
                    _domain(), None, include_inferred=False
                )
                == DATA
            )
