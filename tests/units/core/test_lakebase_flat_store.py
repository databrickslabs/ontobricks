"""Unit tests for LakebaseFlatStore (mocked DB cursor)."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("psycopg")

from back.core.graphdb.lakebase.LakebaseFlatStore import LakebaseFlatStore


@pytest.fixture
def auth():
    a = MagicMock()
    a.database = "appdb"
    a.is_available = True
    return a


def _cursor_ctx(mock_cur):
    @contextmanager
    def cm():
        yield mock_cur

    return cm


def _txn_cursor_ctx(mock_cur, mock_conn=None):
    """Mimic ``LakebaseFlatStore._txn_cursor`` yielding ``(conn, cur)``."""
    conn = mock_conn or MagicMock()

    @contextmanager
    def cm():
        yield conn, mock_cur

    return cm


class _CopyRecorder:
    """Capture rows passed to ``cur.copy(...).write_row(...)``."""

    def __init__(self) -> None:
        self.statement = ""
        self.rows: list = []

    def __call__(self, statement: str):
        self.statement = statement
        recorder = self

        class _CopyCtx:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                return False

            def write_row(self_inner, row):
                recorder.rows.append(tuple(row))

        return _CopyCtx()


def test_create_table_issues_schema_ddl_and_indexes(auth):
    """app_managed create_table now produces the 3-object layout (*_sync + *__app + view)."""
    cur = MagicMock()
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "_cursor", _cursor_ctx(cur)):
        store.create_table("MyDomain_V1")
    executed = [str(c[0][0]) for c in cur.execute.call_args_list]
    assert any("CREATE SCHEMA" in s for s in executed)
    # Both *_sync and *__app tables must be created.
    create_tables = [s for s in executed if "CREATE TABLE" in s]
    assert len(create_tables) >= 2
    assert any("mydomain_v1_sync" in s for s in create_tables)
    assert any("mydomain_v1__app" in s for s in create_tables)
    # Union view must be created.
    assert any("CREATE OR REPLACE VIEW" in s and "mydomain_v1" in s for s in executed)
    assert any("CREATE INDEX" in s for s in executed)


def test_writable_table_id_is_companion_for_app_managed(auth):
    """app_managed _writable_table_id resolves to the *__app companion table."""
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    assert store._writable_table_id("G_V1") == "g_v1__app"


def test_drop_table_app_managed_drops_view_companion_and_sync(auth):
    """app_managed drop_table removes all 3 Postgres objects without Lakeflow API calls."""
    cur = MagicMock()
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "_cursor", _cursor_ctx(cur)):
        store.drop_table("G_V1")
    executed = [str(c[0][0]) for c in cur.execute.call_args_list]
    assert any("DROP VIEW IF EXISTS g_v1" in s for s in executed)
    assert any("DROP TABLE IF EXISTS g_v1__app" in s for s in executed)
    assert any("DROP TABLE IF EXISTS g_v1_sync" in s for s in executed)


def test_optimize_table_vacuums_sync_and_companion_for_app_managed(auth):
    """app_managed optimize_table vacuums both *_sync and *__app."""
    cur = MagicMock()
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "_cursor", _cursor_ctx(cur)):
        store.optimize_table("G_V1")
    executed = [str(c[0][0]) for c in cur.execute.call_args_list]
    assert any("VACUUM ANALYZE g_v1_sync" in s for s in executed)
    assert any("VACUUM ANALYZE g_v1__app" in s for s in executed)


def test_bulk_load_into_sync_writes_to_sync_table(auth):
    """bulk_load_into_sync targets *_sync (not the companion) via _copy_insert_batch_phy."""
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")

    written_to: list = []

    def _fake_copy_batch_phy(phy: str, batch: list) -> int:
        written_to.append(phy)
        return len(batch)

    triples = [
        {"subject": f"http://ex/{i}", "predicate": "http://ex/p", "object": "http://ex/o"}
        for i in range(25)
    ]
    with patch.object(store, "_copy_insert_batch_phy", side_effect=_fake_copy_batch_phy):
        n = store.bulk_load_into_sync("G_V1", iter(triples), batch_size=20)
    assert n == 25
    # Every batch must target the *_sync table, never the companion.
    assert all(phy == "g_v1_sync" for phy in written_to)
    assert written_to == ["g_v1_sync", "g_v1_sync"]  # 20 + 5


def test_insert_triples_executemany(auth):
    cur = MagicMock()
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "_cursor", _cursor_ctx(cur)):
        triples = [
            {"subject": "http://ex/a", "predicate": "http://ex/p", "object": "http://ex/o"},
        ]
        n = store.insert_triples("G_V1", triples)
    assert n == 1
    cur.executemany.assert_called_once()
    sql = cur.executemany.call_args[0][0]
    assert "INSERT INTO" in sql and "ON CONFLICT DO NOTHING" in sql
    assert "datatype" in sql and "lang" in sql


def test_query_triples_maps_rows(auth):
    cur = MagicMock()
    cur.fetchall.return_value = [
        {"subject": "s", "predicate": "p", "object": "o", "datatype": None, "lang": None},
    ]
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "_cursor", _cursor_ctx(cur)):
        rows = store.query_triples("G_V1")
    assert rows == [{"subject": "s", "predicate": "p", "object": "o"}]


def test_query_triples_includes_literal_meta_when_present(auth):
    cur = MagicMock()
    cur.fetchall.return_value = [
        {
            "subject": "s",
            "predicate": "p",
            "object": "lit",
            "datatype": "http://www.w3.org/2001/XMLSchema#string",
            "lang": "en",
        },
    ]
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "_cursor", _cursor_ctx(cur)):
        rows = store.query_triples("G_V1")
    assert rows == [
        {
            "subject": "s",
            "predicate": "p",
            "object": "lit",
            "datatype": "http://www.w3.org/2001/XMLSchema#string",
            "lang": "en",
        }
    ]


def test_count_triples(auth):
    cur = MagicMock()
    cur.fetchone.return_value = {"cnt": 42}
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "_cursor", _cursor_ctx(cur)):
        assert store.count_triples("G_V1") == 42


def test_bulk_insert_iter_batches_without_recursion(auth):
    """``bulk_insert_iter`` must batch the iterator and route each batch through COPY."""
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")

    calls: list = []

    def _fake_copy_batch(table_name: str, batch: list) -> int:
        calls.append(len(batch))
        return len(batch)

    gen = (
        {"subject": f"http://ex/{i}", "predicate": "http://ex/p", "object": "http://ex/o"}
        for i in range(55)
    )
    with patch.object(store, "_copy_insert_batch", side_effect=_fake_copy_batch):
        n = store.bulk_insert_iter("G_V1", gen, batch_size=20)
    assert n == 55
    # 20 + 20 + 15 — bounded memory: only one batch held at a time.
    assert calls == [20, 20, 15]


def test_copy_insert_batch_streams_via_temp_table(auth):
    """COPY → temp table → INSERT … ON CONFLICT DO NOTHING (no executemany on bulk path)."""
    cur = MagicMock()
    recorder = _CopyRecorder()
    cur.copy = recorder
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    batch = [
        {
            "subject": "http://ex/s",
            "predicate": "http://ex/p",
            "object": "lit",
            "datatype": "http://www.w3.org/2001/XMLSchema#string",
            "lang": "en",
        },
        {"subject": "http://ex/s2", "predicate": "http://ex/p", "object": "http://ex/o"},
    ]
    with patch.object(store, "_txn_cursor", _txn_cursor_ctx(cur)):
        n = store._copy_insert_batch("G_V1", batch)
    assert n == 2
    cur.executemany.assert_not_called()
    assert "COPY _ob_copy_stage" in recorder.statement
    assert recorder.rows[0][:3] == ("http://ex/s", "http://ex/p", "lit")
    assert recorder.rows[0][3] == "http://www.w3.org/2001/XMLSchema#string"
    assert recorder.rows[0][4] == "en"
    assert recorder.rows[1] == ("http://ex/s2", "http://ex/p", "http://ex/o", None, None)
    executed = [str(c[0][0]) for c in cur.execute.call_args_list]
    assert any("CREATE TEMP TABLE _ob_copy_stage" in s for s in executed)
    assert any(
        "INSERT INTO" in s and "_ob_copy_stage" in s and "ON CONFLICT DO NOTHING" in s
        for s in executed
    )


def test_bulk_delete_iter_uses_copy_temp_table_join(auth):
    """``bulk_delete_iter`` must group rows into a temp-table JOIN ``DELETE``."""
    cur = MagicMock()
    cur.rowcount = 3
    recorder = _CopyRecorder()
    cur.copy = recorder
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    triples = [
        {"subject": f"http://ex/{i}", "predicate": "http://ex/p", "object": "http://ex/o"}
        for i in range(3)
    ]
    with patch.object(store, "_txn_cursor", _txn_cursor_ctx(cur)):
        deleted = store.bulk_delete_iter("G_V1", iter(triples), batch_size=10)
    assert deleted == 3
    assert "COPY _ob_del_stage" in recorder.statement
    assert recorder.rows[0] == ("http://ex/0", "http://ex/p", "http://ex/o")
    executed = [str(c[0][0]) for c in cur.execute.call_args_list]
    assert any("CREATE TEMP TABLE _ob_del_stage" in s for s in executed)
    assert any(
        "DELETE FROM" in s and "USING _ob_del_stage d" in s for s in executed
    )


def test_delete_triples_routes_large_payload_to_bulk(auth):
    """``delete_triples`` must delegate to the bulk COPY path for large payloads."""
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    triples = [
        {"subject": f"http://ex/{i}", "predicate": "http://ex/p", "object": "http://ex/o"}
        for i in range(60)
    ]
    with patch.object(
        store, "bulk_delete_iter", return_value=60
    ) as mock_bulk:
        n = store.delete_triples("G_V1", triples, batch_size=2000)
    assert n == 60
    mock_bulk.assert_called_once()


def test_iter_triples_pages_through_limits(auth):
    row = {"subject": "s", "predicate": "p", "object": "o", "datatype": None, "lang": None}
    cur = MagicMock()
    cur.fetchall.side_effect = [[row] * 10, [row]]
    store = LakebaseFlatStore(auth, schema="ontobricks_graph")
    with patch.object(store, "_cursor", _cursor_ctx(cur)):
        out = list(store.iter_triples("G_V1", batch_size=10))
    assert len(out) == 11


def test_default_schema_constant():
    from back.core.graphdb.lakebase import default_schema

    assert default_schema() == "ontobricks_graph"


def test_find_subjects_by_type_delegates(auth):
    store = LakebaseFlatStore(auth, schema="g")
    with patch.object(
        store,
        "execute_query",
        return_value=[{"subject": "http://ex/1"}, {"subject": "http://ex/2"}],
    ):
        subs = store.find_subjects_by_type(
            "G_V1",
            "http://ex.org/Customer",
            limit=10,
            offset=0,
        )
    assert subs == ["http://ex/1", "http://ex/2"]


def test_bfs_traversal_sql_path(auth):
    store = LakebaseFlatStore(auth, schema="g")
    with patch.object(
        store,
        "execute_query",
        return_value=[{"entity": "http://seed", "min_lvl": 0}],
    ):
        rows = store.bfs_traversal("G_V1", " WHERE subject = 'http://seed'", depth=3)
    assert rows == [{"entity": "http://seed", "min_lvl": 0}]


# ---------------------------------------------------------------
# managed_synced mode -- writes route to companion, reads to view
# ---------------------------------------------------------------


@pytest.fixture
def synced_store(auth):
    """LakebaseFlatStore in managed_synced mode with a mocked SyncedTableManager."""
    mgr = MagicMock()
    return LakebaseFlatStore(
        auth,
        schema="ontobricks_graph",
        sync_mode="managed_synced",
        sync_uc_catalog="catX",
        synced_manager=mgr,
    )


class TestManagedSyncedRouting:
    """Direct writes target the companion; reads see the union view."""

    def test_writable_table_id_is_companion(self, synced_store):
        assert synced_store._writable_table_id("G_V1") == "g_v1__app"

    def test_readable_table_id_is_union_view(self, synced_store):
        # The union view name equals the legacy table name for backwards
        # compatibility with existing readers.
        assert synced_store._readable_table_id("G_V1") == "g_v1"

    def test_synced_uc_name_uses_engine_config_catalog(self, synced_store):
        # No sync_uc_schema set → falls back to pg schema
        assert (
            synced_store.synced_uc_name("G_V1")
            == "catX.ontobricks_graph.g_v1_sync"
        )

    def test_synced_uc_name_uses_sync_uc_schema_when_set(self, auth):
        store = LakebaseFlatStore(
            auth,
            schema="ontobricks_graph",
            sync_mode="managed_synced",
            sync_uc_catalog="catX",
            sync_uc_schema="ontobricks",
            synced_manager=MagicMock(),
        )
        assert store.synced_uc_name("G_V1") == "catX.ontobricks.g_v1_sync"

    def test_synced_uc_name_falls_back_to_caller_catalog(self, auth):
        store = LakebaseFlatStore(
            auth,
            schema="ontobricks_graph",
            sync_mode="managed_synced",
            sync_uc_catalog="",
            synced_manager=MagicMock(),
        )
        assert (
            store.synced_uc_name("G_V1", fallback_catalog="dlt_cat")
            == "dlt_cat.ontobricks_graph.g_v1_sync"
        )

    def test_create_table_is_noop(self, synced_store):
        cur = MagicMock()
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            synced_store.create_table("G_V1")
        cur.execute.assert_not_called()

    def test_insert_executemany_targets_companion(self, synced_store):
        cur = MagicMock()
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            synced_store.insert_triples(
                "G_V1",
                [
                    {
                        "subject": "http://ex/s",
                        "predicate": "http://ex/p",
                        "object": "http://ex/o",
                    }
                ],
            )
        cur.executemany.assert_called_once()
        sql = cur.executemany.call_args[0][0]
        assert "INSERT INTO g_v1__app" in sql

    def test_copy_insert_batch_targets_companion(self, synced_store):
        cur = MagicMock()
        recorder = _CopyRecorder()
        cur.copy = recorder
        batch = [
            {"subject": "http://ex/s", "predicate": "http://ex/p", "object": "o"}
        ]
        with patch.object(synced_store, "_txn_cursor", _txn_cursor_ctx(cur)):
            synced_store._copy_insert_batch("G_V1", batch)
        executed = [str(c[0][0]) for c in cur.execute.call_args_list]
        assert any(
            "INSERT INTO g_v1__app" in s and "ON CONFLICT DO NOTHING" in s
            for s in executed
        )

    def test_copy_delete_batch_targets_companion(self, synced_store):
        cur = MagicMock()
        cur.rowcount = 1
        cur.copy = _CopyRecorder()
        batch = [
            {"subject": "http://ex/s", "predicate": "http://ex/p", "object": "o"}
        ]
        with patch.object(synced_store, "_txn_cursor", _txn_cursor_ctx(cur)):
            synced_store._copy_delete_batch("G_V1", batch)
        executed = [str(c[0][0]) for c in cur.execute.call_args_list]
        assert any(
            "DELETE FROM g_v1__app" in s and "USING _ob_del_stage" in s
            for s in executed
        )

    def test_query_triples_targets_union_view(self, synced_store):
        cur = MagicMock()
        cur.fetchall.return_value = []
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            synced_store.query_triples("G_V1")
        sql = cur.execute.call_args_list[-1][0][0]
        # Reads target the union view, which keeps the legacy bare name.
        assert "FROM g_v1 " in sql

    def test_count_triples_targets_union_view(self, synced_store):
        cur = MagicMock()
        cur.fetchone.return_value = {"cnt": 7}
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            assert synced_store.count_triples("G_V1") == 7
        sql = cur.execute.call_args_list[-1][0][0]
        assert "FROM g_v1" in sql

    def test_optimize_table_targets_companion_only(self, synced_store):
        cur = MagicMock()
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            synced_store.optimize_table("G_V1")
        sql = cur.execute.call_args_list[-1][0][0]
        assert sql == "VACUUM ANALYZE g_v1__app"

    def test_drop_table_drops_view_companion_and_synced(self, synced_store):
        cur = MagicMock()
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            synced_store.drop_table("G_V1")
        executed = [str(c[0][0]) for c in cur.execute.call_args_list]
        assert any("DROP VIEW IF EXISTS g_v1" in s for s in executed)
        assert any("DROP TABLE IF EXISTS g_v1__app" in s for s in executed)
        # synced_store has no sync_uc_schema, falls back to pg schema
        synced_store._synced_manager.delete.assert_called_once_with(
            "catX.ontobricks_graph.g_v1_sync", purge_data=True
        )


    def test_ensure_synced_layout_creates_companion_and_view(self, synced_store):
        cur = MagicMock()
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            synced_store.ensure_synced_layout("G_V1")
        executed = [str(c[0][0]) for c in cur.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS g_v1__app" in s for s in executed)
        assert any(
            "CREATE OR REPLACE VIEW g_v1" in s
            and "FROM g_v1_sync" in s
            and "FROM g_v1__app" in s
            for s in executed
        )


    def test_ensure_synced_union_view_qualifies_sync_table_when_schemas_differ(self, auth):
        """When sync_uc_schema != pg schema, the _sync reference is schema-qualified."""
        store = LakebaseFlatStore(
            auth,
            schema="ontobricks_graph",
            sync_mode="managed_synced",
            sync_uc_catalog="catX",
            sync_uc_schema="ontobricks",
            synced_manager=MagicMock(),
        )
        cur = MagicMock()
        with patch.object(store, "_cursor", _cursor_ctx(cur)):
            store.ensure_synced_union_view("G_V1")
        executed = [str(c[0][0]) for c in cur.execute.call_args_list]
        view_ddl = next(s for s in executed if "CREATE OR REPLACE VIEW" in s)
        assert '"ontobricks".g_v1_sync' in view_ddl
        assert "FROM g_v1__app" in view_ddl

    def test_ensure_synced_union_view_unqualified_when_schemas_same(self, synced_store):
        """When sync_uc_schema matches pg schema, the _sync reference is unqualified."""
        cur = MagicMock()
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            synced_store.ensure_synced_union_view("G_V1")
        executed = [str(c[0][0]) for c in cur.execute.call_args_list]
        view_ddl = next(s for s in executed if "CREATE OR REPLACE VIEW" in s)
        assert "FROM g_v1_sync" in view_ddl
        assert '"ontobricks_graph"' not in view_ddl

    def test_ensure_synced_companion_skips_union_view(self, synced_store):
        cur = MagicMock()
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            synced_store.ensure_synced_companion("G_V1")
        executed = [str(c[0][0]) for c in cur.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS g_v1__app" in s for s in executed)
        assert not any("CREATE OR REPLACE VIEW" in s for s in executed)

    def test_truncate_companion_runs_truncate(self, synced_store):
        cur = MagicMock()
        with patch.object(synced_store, "_cursor", _cursor_ctx(cur)):
            synced_store.truncate_companion("G_V1")
        sql = cur.execute.call_args_list[-1][0][0]
        assert sql == "TRUNCATE TABLE g_v1__app"


class TestResolveLakebaseGraphSchema:
    def test_explicit_config_schema_wins_over_registry(self):
        """Explicit graph_engine_config.schema always overrides the registry volume schema."""
        from types import SimpleNamespace

        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_lakebase_graph_schema,
        )

        domain = SimpleNamespace(settings={"registry": {}})
        settings = SimpleNamespace()
        with patch(
            "back.objects.registry.RegistryCfg.from_domain",
            return_value=MagicMock(catalog="c", schema="volume_sch", volume="v"),
        ):
            out = resolve_lakebase_graph_schema(
                domain,
                settings,
                "ontobricks_graph",  # explicit
            )
        assert out == "ontobricks_graph"

    def test_falls_back_to_registry_when_config_schema_empty(self):
        """When config schema is empty, the registry volume schema is used."""
        from types import SimpleNamespace

        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_lakebase_graph_schema,
        )

        domain = SimpleNamespace(settings={"registry": {}})
        settings = SimpleNamespace()
        with patch(
            "back.objects.registry.RegistryCfg.from_domain",
            return_value=MagicMock(catalog="c", schema="volume_sch", volume="v"),
        ):
            out = resolve_lakebase_graph_schema(domain, settings, "")
        assert out == "volume_sch"

    def test_falls_back_when_registry_schema_empty(self):
        from types import SimpleNamespace

        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_lakebase_graph_schema,
        )

        domain = SimpleNamespace()
        settings = SimpleNamespace()
        with patch(
            "back.objects.registry.RegistryCfg.from_domain",
            return_value=MagicMock(catalog="c", schema="", volume="v"),
        ):
            out = resolve_lakebase_graph_schema(domain, settings, "custom_g")
        assert out == "custom_g"


class TestResolveSyncUcFallbackCatalog:
    def test_prefers_registry_catalog_over_delta(self):
        from types import SimpleNamespace

        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_sync_uc_fallback_catalog,
        )

        domain = SimpleNamespace(settings={"registry": {}})
        settings = SimpleNamespace()
        with patch(
            "back.objects.registry.RegistryCfg.from_domain",
            return_value=MagicMock(catalog="team_catalog", schema="s", volume="v"),
        ):
            out = resolve_sync_uc_fallback_catalog(
                domain,
                settings,
                {"catalog": "benoit_cayla", "schema": "x"},
            )
        assert out == "team_catalog"

    def test_falls_back_to_delta_when_no_registry_catalog(self):
        from types import SimpleNamespace

        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_sync_uc_fallback_catalog,
        )

        domain = SimpleNamespace()
        settings = SimpleNamespace()
        with patch(
            "back.objects.registry.RegistryCfg.from_domain",
            return_value=MagicMock(catalog="", schema="", volume=""),
        ):
            out = resolve_sync_uc_fallback_catalog(
                domain,
                settings,
                {"catalog": "delta_cat"},
            )
        assert out == "delta_cat"

    def test_env_ontobricks_sync_uc_catalog_pins_before_registry(self, monkeypatch):
        """Deployment-wide UC catalog when graph_engine_config.sync_uc_catalog is empty."""
        from types import SimpleNamespace

        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_sync_uc_fallback_catalog,
        )

        monkeypatch.setenv("ONTOBRICKS_SYNC_UC_CATALOG", "shared_team_catalog")

        domain = SimpleNamespace(settings={"registry": {}})
        settings = SimpleNamespace()
        with patch(
            "back.objects.registry.RegistryCfg.from_domain",
            return_value=MagicMock(catalog="team_catalog", schema="s", volume="v"),
        ):
            out = resolve_sync_uc_fallback_catalog(
                domain,
                settings,
                {"catalog": "benoit_cayla"},
            )
        assert out == "shared_team_catalog"

    def test_env_legacy_typoed_var_still_works_with_warning(self, monkeypatch):
        """Backwards-compat: the legacy ``ONTBRICKS_*`` spelling is still honoured."""
        from types import SimpleNamespace

        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_sync_uc_fallback_catalog,
        )

        monkeypatch.delenv("ONTOBRICKS_SYNC_UC_CATALOG", raising=False)
        monkeypatch.setenv("ONTBRICKS_SYNC_UC_CATALOG", "legacy_catalog")

        domain = SimpleNamespace(settings={"registry": {}})
        settings = SimpleNamespace()
        with patch(
            "back.objects.registry.RegistryCfg.from_domain",
            return_value=MagicMock(catalog="team_catalog", schema="s", volume="v"),
        ):
            out = resolve_sync_uc_fallback_catalog(
                domain,
                settings,
                {"catalog": "benoit_cayla"},
            )
        assert out == "legacy_catalog"

