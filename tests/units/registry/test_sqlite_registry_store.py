"""End-to-end tests: the Lakebase registry mixins running on the SQLite store.

Every test uses a real SQLite file (no fakes), so a statement the dialect
can't translate fails here exactly as it would in the app.
"""

from __future__ import annotations

import os
from datetime import datetime

import pytest

from back.objects.registry.RegistryCfg import RegistryCfg
from back.objects.registry.store.sqlite import (
    SQLiteDialect,
    SQLiteRegistryStore,
    SQLiteSnapshot,
)

pytestmark = pytest.mark.unit

CFG = RegistryCfg(catalog="cat", schema="sch", volume="vol")


def _doc(status="DRAFT", **info):
    return {
        "info": {"status": status, "description": "Water network", "last_update": "2026-11-14", **info},
        "ontology": {"base_uri": "http://example.org/water#", "classes": [{"name": "Pipe"}]},
        "assignment": {"entities": []},
        "design_layout": {"nodes": []},
        "metadata": {"tables": []},
    }


@pytest.fixture
def store(tmp_path):
    return SQLiteRegistryStore(registry_cfg=CFG, local_path=str(tmp_path / "reg.sqlite"))


@pytest.fixture
def domain(store):
    ok, msg = store.write_version("water", "1", _doc())
    assert ok, msg
    return store


class TestSchemaAndIdentity:
    def test_new_file_is_created_with_the_schema(self, store):
        assert os.path.exists(store.local_path)

    def test_backend_is_sqlite(self, store):
        assert store.backend == "sqlite"

    def test_initialize_seeds_the_registry_row(self, store):
        ok, msg = store.initialize()
        assert ok, msg

    def test_is_initialized_after_initialize(self, store):
        store.initialize()
        assert store.is_initialized() is True

    def test_table_row_counts_report_present_tables(self, domain):
        counts = domain.table_row_counts(("domains", "domain_versions", "nope"))
        assert counts == {"domains": 1, "domain_versions": 1, "nope": 0}


class TestCatalog:
    def test_written_version_reads_back(self, domain):
        ok, data, _ = domain.read_version("water", "1")
        assert ok and data["versions"]["1"]["ontology"]["classes"] == [{"name": "Pipe"}]

    def test_domain_is_listed(self, domain):
        ok, folders, _ = domain.list_domain_folders()
        assert ok and folders == ["water"]

    def test_listing_with_metadata(self, domain):
        ok, items, msg = domain.list_domains_with_metadata()
        assert ok, msg
        assert [i.get("name") or i.get("folder") for i in items] == ["water"]

    def test_versions_sort_numerically(self, domain):
        for v in ("2", "10"):
            domain.write_version("water", v, _doc())
        ok, versions, _ = domain.list_versions("water")
        assert ok and versions == ["1", "2", "10"]

    def test_status_update_rewrites_info_json(self, domain):
        ok, msg = domain.update_version_status("water", "1", "IN-REVIEW")
        assert ok, msg
        _, data, _ = domain.read_version("water", "1")
        assert data["info"]["status"] == "IN-REVIEW"

    def test_get_version_status(self, domain):
        assert domain.get_version_status("water", "1") == "DRAFT"

    def test_delete_version(self, domain):
        domain.write_version("water", "2", _doc())
        ok, msg = domain.delete_version("water", "2")
        assert ok, msg
        assert domain.list_versions("water")[1] == ["1"]

    def test_delete_domain_cascades(self, domain):
        domain.delete_domain("water")
        assert domain.domain_exists("water") is False

    def test_permissions_round_trip(self, domain):
        perms = {"permissions": [{"principal": "ana@x.com", "principal_type": "user", "role": "editor"}]}
        ok, msg = domain.save_domain_permissions("water", perms)
        assert ok, msg
        roles = [p["role"] for p in domain.load_domain_permissions("water")["permissions"]]
        assert roles == ["editor"]

    def test_quorum_defaults_to_one(self, domain):
        assert domain.get_domain_quorum("water") == 1


class TestGlobalConfigAndSchedules:
    def test_global_config_round_trip(self, store):
        ok, msg = store.save_global_config({"warehouse_id": "abc"})
        assert ok, msg
        assert store.load_global_config().get("warehouse_id") == "abc"

    def test_schedule_round_trip(self, domain):
        ok, msg = domain.save_schedules({"water": {"interval_minutes": 60, "enabled": True, "version": "latest"}})
        assert ok, msg
        assert domain.load_schedules()["build::water::"]["interval_minutes"] == 60

    def test_schedule_history_round_trip(self, domain):
        domain.save_schedules({"water": {"interval_minutes": 60}})
        domain.append_schedule_history("water", {"timestamp": "2026-11-14T10:00:00+00:00", "status": "success"})
        assert [h["status"] for h in domain.load_schedule_history("water")] == ["success"]


class TestAuditAndCollab:
    def test_review_events(self, domain):
        domain.record_review_event("water", "1", "ana@x.com", "submitted", from_status="DRAFT", to_status="IN-REVIEW")
        assert [e["action"] for e in domain.list_review_events("water", "1")] == ["submitted"]

    def test_change_events(self, domain):
        ok, msg = domain.record_change_events(
            "water", "1", "ana@x.com",
            [{"action": "class_added", "entity_type": "class", "entity_ref": "Pipe",
              "occurred_at": "2026-11-14T10:00:00Z"}],
        )
        assert ok, msg
        assert [e["action"] for e in domain.list_change_events("water", "1")] == ["class_added"]

    def test_comment_and_resolution(self, domain):
        c = domain.insert_comment("water", "1", author="ana@x.com", body="Check the valves")
        ok, msg = domain.resolve_comment("water", str(c["id"]))
        assert ok, msg
        assert domain.list_comments("water", "1")[0]["resolved"] is True

    def test_task_for_assignee(self, domain):
        domain.insert_task("water", "1", assignee="bob@x.com", created_by="ana@x.com", title="Map valves",
                           due_date="2026-12-01")
        assert [t["title"] for t in domain.list_tasks_for_assignee("bob@x.com")] == ["Map valves"]


class TestEditLocks:
    def test_second_user_is_refused(self, domain):
        domain.acquire_edit_lock("water", "1", holder_email="ana@x.com")
        res = domain.acquire_edit_lock("water", "1", holder_email="bob@x.com")
        assert (domain.get_edit_lock("water", "1") or {}).get("holder_email") == "ana@x.com", res

    def test_stale_lock_is_reclaimed(self, domain):
        domain.acquire_edit_lock("water", "1", holder_email="ana@x.com")
        with domain._connect() as conn, conn.cursor() as cur:
            cur.execute("UPDATE domain_edit_locks SET heartbeat_at = '2020-01-01T00:00:00.000+00:00'")
        domain.acquire_edit_lock("water", "1", holder_email="bob@x.com", ttl_seconds=60)
        assert domain.get_edit_lock("water", "1", ttl_seconds=60)["holder_email"] == "bob@x.com"

    def test_release(self, domain):
        domain.acquire_edit_lock("water", "1", holder_email="ana@x.com")
        assert domain.release_edit_lock("water", "1", holder_email="ana@x.com") is True


class TestBuildRunsAndAnalytics:
    @staticmethod
    def _two_runs(store):
        for status, ts, n in (("success", "2026-11-14T09:00:00+00:00", 5), ("error", "2026-11-14T10:00:00+00:00", 0)):
            store.record_build_run("water", {"version": "1", "status": status, "started_at": ts, "triple_count": n})

    def test_build_runs_are_recorded(self, domain):
        self._two_runs(domain)
        assert len(domain.load_build_runs("water", version="1")) == 2

    def test_rollup_reports_the_latest_run_status(self, domain):
        self._two_runs(domain)
        assert domain.build_analytics("water", version="1")["per_version"][0]["last_status"] == "error"

    def test_active_build_is_the_latest_success(self, domain):
        self._two_runs(domain)
        assert domain.build_analytics("water", version="1")["active_build"]["triple_count"] == 5

    def test_graph_analytics_round_trip(self, domain):
        domain.save_graph_analytics("water", "1", {"status": "completed", "stats": {"nodes": 3}})
        assert domain.load_graph_analytics("water", "1")["stats"] == {"nodes": 3}


class TestDocuments:
    def test_document_lifecycle(self, domain):
        ok, msg = domain.upsert_document("water", "1", filename="manual.pdf", source_hash="h1", parser="plaintext",
                                         status="pending", size_bytes=3)
        assert ok, msg
        domain.set_document_ready("water", "1", "manual.pdf", parsed_text="Valve procedure")
        assert domain.read_document_text("water", "1", "manual.pdf")[0] == "Valve procedure"

    def test_delete_documents_by_name(self, domain):
        domain.upsert_document("water", "1", filename="a.txt", source_hash="h", parser="plaintext",
                               status="ready", size_bytes=1)
        domain.delete_documents("water", "1", ["a.txt"])
        assert domain.count_documents("water", "1") == 0


class TestDialect:
    def test_placeholders_become_question_marks(self):
        assert SQLiteDialect.translate("SELECT %s, %s")[0] == "SELECT ?, ?"

    def test_casts_are_stripped(self):
        assert SQLiteDialect.translate("SELECT %s::jsonb, '{}'::jsonb")[0] == "SELECT ?, '{}'"

    def test_any_param_is_flagged(self):
        sql, lists, _ = SQLiteDialect.translate("WHERE a = %s AND b = ANY(%s)")
        assert lists == {1} and "json_each(?)" in sql

    def test_timestamp_param_is_flagged(self):
        assert SQLiteDialect.translate("VALUES (%s, %s::timestamptz)")[2] == {1}

    def test_delete_using_becomes_rowid_subquery(self):
        sql = SQLiteDialect.translate("DELETE FROM t l USING d x WHERE l.id = x.id")[0]
        assert sql == "DELETE FROM t WHERE rowid IN (SELECT l.rowid FROM t AS l, d AS x WHERE l.id = x.id)"


class TestSnapshot:
    class _FakeVolume:
        files: dict = {}

        def read_binary_file(self, path):
            data = self.files.get(path)
            return (data is not None, data or b"", "ok" if data else "missing")

        def write_binary_file(self, path, data, overwrite=True):
            self.files[path] = data
            return True, "saved"

    def test_registry_survives_a_lost_local_disk(self, tmp_path):
        vol = self._FakeVolume()
        vol.files = {}
        local = str(tmp_path / "a" / "reg.sqlite")
        first = SQLiteSnapshot(local, "/Volumes/c/s/v/_registry/r.sqlite", lambda: vol)
        st = SQLiteRegistryStore(registry_cfg=CFG, local_path=local, snapshot=first)
        st.write_version("water", "1", _doc())
        assert first.flush()
        os.remove(local)  # the App restarted: local disk is gone
        second = SQLiteSnapshot(local, "/Volumes/c/s/v/_registry/r.sqlite", lambda: vol)
        restored = SQLiteRegistryStore(registry_cfg=CFG, local_path=local, snapshot=second)
        assert restored.list_domain_folders()[1] == ["water"]

    def test_default_timestamps_read_back_as_aware_datetimes(self, domain):
        with domain._connect() as conn, conn.cursor(row_factory=object()) as cur:
            cur.execute("SELECT created_at FROM domains")
            created = cur.fetchone()["created_at"]
        assert isinstance(created, datetime) and created.tzinfo is not None


class TestNoInitializeStep:
    def test_new_store_is_initialized_without_initialize(self, store):
        assert store.init_status()["initialized"] is True


class TestHealthOnSQLite:
    @pytest.fixture(autouse=True)
    def _sqlite_backend(self, monkeypatch, tmp_path):
        from shared.config.settings import get_settings

        monkeypatch.setattr(get_settings(), "registry_backend", "sqlite")
        monkeypatch.setenv("REGISTRY_SQLITE_PATH", str(tmp_path / "health.sqlite"))
        monkeypatch.setenv("REGISTRY_SQLITE_SNAPSHOT_PATH", "")

    def test_lakebase_only_checks_report_not_used(self):
        from shared.fastapi import health

        assert health._check_lakebase_env_vars()[0] == health._OK

    def test_registry_check_reports_the_sqlite_registry(self, monkeypatch):
        from shared.config.settings import get_settings
        from shared.fastapi import health

        monkeypatch.setattr(health, "_resolve_registry_cfg", lambda s: RegistryCfg(catalog="", schema="", volume=""))
        status, detail = health._check_lakebase(get_settings())
        assert "SQLite registry ready" in detail
