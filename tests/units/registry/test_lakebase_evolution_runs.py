"""Real SQL of the Ontology Evolve run store (``EvolutionRuns`` mixin).

Driven through the scripted cursor from ``_v090_fake_pg``: idempotent
creation, compare-and-swap updates, loud failures, bind parameters, and
parity between the lazy DDL and ``schema.sql``.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from back.core.errors import ConflictError, InfrastructureError, NotFoundError
from back.objects.registry.store.lakebase.EvolutionRuns import EvolutionRuns

from ._v090_fake_pg import (
    QSCHEMA,
    REGISTRY_ID,
    FakeDB,
    fake_psycopg,  # noqa: F401
    make_store,
)

pytestmark = pytest.mark.unit

_T0 = datetime(2026, 11, 21, 9, 0, tzinfo=timezone.utc)
SCHEMA_SQL = (
    Path(__file__).resolve().parents[3]
    / "src/back/objects/registry/store/lakebase/schema.sql"
)


def _row(**over):
    row = {
        "id": "run-1",
        "parent_version": "3",
        "status": "CREATED",
        "stage": "",
        "revision": 1,
        "idempotency_key": "key-1",
        "owner": "alice@example.com",
        "task_id": "",
        "cancel_requested": False,
        "manifest": {"parent_content_hash": "abc"},
        "intent": {"output_class": "Order"},
        "coverage": {},
        "proposal": {},
        "candidate": {},
        "evaluation": {},
        "accepted_version": "",
        "error_code": "",
        "error": "",
        "heartbeat_at": None,
        "created_at": _T0,
        "updated_at": _T0,
    }
    row.update(over)
    return row


def _run(**over):
    run = {
        "id": "run-1",
        "parent_version": "3",
        "idempotency_key": "key-1",
        "owner": "alice@example.com",
        "manifest": {"parent_content_hash": "abc"},
        "intent": {"output_class": "Order"},
    }
    run.update(over)
    return run


class TestCreate:
    def test_insert_returns_created_row(self, fake_psycopg):
        db = FakeDB().on("INSERT INTO", rows=[_row()])
        store = make_store(db)

        out = store.create_evolution_run("supply", _run())

        assert out["created"] is True
        assert out["id"] == "run-1" and out["revision"] == 1
        assert out["manifest"] == {"parent_content_hash": "abc"}
        call = db.one("INSERT INTO")
        assert f"{QSCHEMA}.evolution_runs" in call.sql
        assert "ON CONFLICT (domain_id, idempotency_key) DO NOTHING" in call.sql
        assert call.params[-2:] == (REGISTRY_ID, "supply")

    def test_same_key_returns_existing_run_not_a_second_one(self, fake_psycopg):
        db = (
            FakeDB()
            .on("INSERT INTO", rows=[])
            .on("r.idempotency_key = %s", rows=[_row(id="run-0")])
        )
        store = make_store(db)

        out = store.create_evolution_run("supply", _run(id="run-new"))

        assert out["created"] is False
        assert out["id"] == "run-0"

    def test_unknown_domain_raises(self, fake_psycopg):
        store = make_store(FakeDB())
        with pytest.raises(NotFoundError):
            store.create_evolution_run("missing", _run())

    def test_values_are_bound_not_interpolated(self, fake_psycopg):
        db = FakeDB().on("INSERT INTO", rows=[_row()])
        store = make_store(db)
        store.create_evolution_run("supply", _run(owner="x'); DROP TABLE domains; --"))
        db.assert_not_interpolated("DROP TABLE domains", "alice@example.com", "key-1")

    def test_missing_tables_raise_instead_of_dropping_writes(self, fake_psycopg):
        db = FakeDB().on("information_schema.tables", raises=RuntimeError("no perms"))
        store = make_store(db, ready=False)
        with pytest.raises(InfrastructureError):
            store.create_evolution_run("supply", _run())


class TestUpdate:
    def test_cas_increments_revision_and_scopes_by_folder(self, fake_psycopg):
        db = FakeDB().on("UPDATE", rows=[_row(revision=2, status="RUNNING")])
        store = make_store(db)

        out = store.update_evolution_run(
            "supply",
            "run-1",
            1,
            {
                "status": "RUNNING",
                "coverage": {"ready": False},
                "touch_heartbeat": True,
            },
        )

        assert out["revision"] == 2 and out["status"] == "RUNNING"
        call = db.one("UPDATE")
        assert "r.revision = %s" in call.sql
        assert "revision = r.revision + 1" in call.sql
        assert "heartbeat_at = now()" in call.sql
        assert call.params[-4:] == (REGISTRY_ID, "supply", "run-1", 1)

    def test_stale_revision_raises_conflict(self, fake_psycopg):
        db = FakeDB().on("UPDATE", rows=[]).on("r.id = %s", rows=[_row(revision=5)])
        store = make_store(db)
        with pytest.raises(ConflictError) as err:
            store.update_evolution_run("supply", "run-1", 4, {"status": "RUNNING"})
        assert "current 5" in err.value.detail

    def test_missing_run_raises_not_found(self, fake_psycopg):
        store = make_store(FakeDB())
        with pytest.raises(NotFoundError):
            store.update_evolution_run("supply", "nope", 1, {"status": "RUNNING"})

    def test_unknown_field_is_rejected_before_sql(self, fake_psycopg):
        db = FakeDB()
        store = make_store(db)
        with pytest.raises(ValueError):
            store.update_evolution_run("supply", "run-1", 1, {"revision": 99})
        assert db.calls == []


class TestReadsAndResults:
    def test_list_filters_by_parent_version(self, fake_psycopg):
        db = FakeDB().on("ORDER BY r.created_at DESC", rows=[_row(), _row(id="run-2")])
        store = make_store(db)
        out = store.list_evolution_runs("supply", parent_version="3", limit=10)
        assert [r["id"] for r in out] == ["run-1", "run-2"]
        assert db.calls[-1].params == (REGISTRY_ID, "supply", "3", 10)

    def test_append_and_list_results(self, fake_psycopg):
        db = (
            FakeDB()
            .on("INSERT INTO", rowcount=1)
            .on(
                'FROM "ontobricks_registry".evolution_results x JOIN',
                rows=[
                    {
                        "question_id": "q1",
                        "arm": "candidate",
                        "status": "PASS",
                        "observed": {"ids": ["O1"]},
                        "created_at": _T0,
                    }
                ],
            )
        )
        store = make_store(db)
        store.append_evolution_result(
            "supply",
            "run-1",
            {
                "question_id": "q1",
                "arm": "candidate",
                "status": "PASS",
                "observed": {"ids": ["O1"]},
            },
        )
        out = store.list_evolution_results("supply", "run-1")
        assert out == [
            {
                "question_id": "q1",
                "arm": "candidate",
                "status": "PASS",
                "observed": {"ids": ["O1"]},
                "created_at": _T0.isoformat(),
            }
        ]

    def test_append_to_unknown_domain_raises(self, fake_psycopg):
        store = make_store(FakeDB())
        with pytest.raises(NotFoundError):
            store.append_evolution_result(
                "missing",
                "run-1",
                {"question_id": "q", "arm": "parent", "status": "FAIL"},
            )

    def test_mark_interrupted_targets_stale_running_runs(self, fake_psycopg):
        db = FakeDB().on("SET status = 'INTERRUPTED'", rowcount=2)
        store = make_store(db)
        assert store.mark_interrupted_evolution_runs(300) == 2
        call = db.one("SET status = 'INTERRUPTED'")
        assert "r.status = 'RUNNING'" in call.sql
        assert call.params == (REGISTRY_ID, 300)


def _columns(ddl: str, table: str) -> list[str]:
    body = re.search(
        rf"CREATE TABLE IF NOT EXISTS \S*{table} \((.*?)\n\s*\)", ddl, re.S
    ).group(1)
    cols = []
    for line in body.splitlines():
        token = line.strip().split(" ")[0]
        if (
            token
            and token.islower()
            and token not in {"primary", "unique", "references"}
        ):
            cols.append(token)
    return cols


@pytest.mark.parametrize("table", ["evolution_runs", "evolution_results"])
def test_lazy_ddl_matches_schema_sql(table):
    lazy = "\n".join(EvolutionRuns.evolution_ddl("s"))
    assert _columns(lazy, table) == _columns(SCHEMA_SQL.read_text(), table)


def test_lazy_creation_runs_ddl_once_when_tables_are_missing(fake_psycopg):
    db = (
        FakeDB()
        .on("information_schema.tables", rows=[(0,)])
        .on("INSERT INTO", rows=[_row()], once=False)
    )
    store = make_store(db, ready=False)
    store.create_evolution_run("supply", _run())
    assert len(db.sql_with("CREATE TABLE IF NOT EXISTS")) == 2
    store.create_evolution_run("supply", _run())
    assert len(db.sql_with("CREATE TABLE IF NOT EXISTS")) == 2


def test_delete_removes_results_then_the_run(fake_psycopg):
    db = FakeDB().on('DELETE FROM "ontobricks_registry".evolution_runs', rowcount=1)
    store = make_store(db)
    assert store.delete_evolution_run("supply", "run-1") is True
    deletes = [c.sql for c in db.calls if c.sql.startswith("DELETE")]
    assert "evolution_results" in deletes[0] and "evolution_runs" in deletes[1]


VERSION_ROW = {
    "info": {"name": "Evo", "mcp_enabled": True, "last_update": "2026-11-21"},
    "ontology": {"classes": []},
    "assignment": {"entities": []},
    "design_layout": {},
    "metadata": {},
}


class TestAccept:
    def _db(self, run, next_version=4, inserted=True):
        db = FakeDB()
        db.on("FROM " + QSCHEMA + ".domains WHERE", rows=[{"id": "dom-1"}])
        db.on("FROM " + QSCHEMA + ".evolution_runs WHERE", rows=[run])
        db.on("COALESCE(MAX(version::integer)", rows=[{"next": next_version}])
        db.on(
            "INSERT INTO " + QSCHEMA + ".domain_versions",
            rows=[{"version": str(next_version)}] if inserted else [],
        )
        db.on(
            "UPDATE " + QSCHEMA + ".evolution_runs",
            rows=[
                _row(status="ACCEPTED", accepted_version=str(next_version), revision=8)
            ],
        )
        return db

    def test_inserts_the_next_version_in_one_locked_transaction(self, fake_psycopg):
        db = self._db(_row(status="EVALUATED", revision=7))
        store = make_store(db)
        out = store.accept_evolution_run("Evo", "run-1", 7, VERSION_ROW)
        assert out["version"] == "4" and out["created"] is True
        assert out["run"]["status"] == "ACCEPTED"
        assert db.transactions == 1
        assert "FOR UPDATE" in db.one("FROM " + QSCHEMA + ".domains WHERE").sql
        assert "FOR UPDATE" in db.one("FROM " + QSCHEMA + ".evolution_runs WHERE").sql
        insert = db.one("INSERT INTO " + QSCHEMA + ".domain_versions")
        assert "ON CONFLICT (domain_id, version) DO NOTHING" in insert.sql
        assert "'DRAFT'" in insert.sql
        assert insert.params[1] == "4"
        # The domains row (review_quorum, mcp_policy) is never written.
        assert not db.sql_with("INSERT INTO " + QSCHEMA + ".domains")
        assert not db.sql_with("UPDATE " + QSCHEMA + ".domains")

    def test_repeat_accept_returns_the_existing_version(self, fake_psycopg):
        db = self._db(_row(status="ACCEPTED", accepted_version="4", revision=8))
        store = make_store(db)
        out = store.accept_evolution_run("Evo", "run-1", 2, VERSION_ROW)
        assert out == {**out, "version": "4", "created": False}
        assert not db.sql_with("INSERT INTO " + QSCHEMA + ".domain_versions")

    @pytest.mark.parametrize(
        "run,revision",
        [
            (_row(status="EVALUATED", revision=7), 6),
            (_row(status="PROPOSED", revision=7), 7),
        ],
    )
    def test_stale_or_unevaluated_run_conflicts(self, fake_psycopg, run, revision):
        db = self._db(run)
        store = make_store(db)
        with pytest.raises(ConflictError):
            store.accept_evolution_run("Evo", "run-1", revision, VERSION_ROW)
        assert not db.sql_with("INSERT INTO " + QSCHEMA + ".domain_versions")

    def test_lost_version_race_conflicts(self, fake_psycopg):
        db = self._db(_row(status="EVALUATED", revision=7), inserted=False)
        store = make_store(db)
        with pytest.raises(ConflictError):
            store.accept_evolution_run("Evo", "run-1", 7, VERSION_ROW)
        assert not db.sql_with("UPDATE " + QSCHEMA + ".evolution_runs")

    @pytest.mark.parametrize("missing", ["domain", "run"])
    def test_missing_domain_or_run(self, fake_psycopg, missing):
        db = FakeDB()
        if missing == "run":
            db.on("FROM " + QSCHEMA + ".domains WHERE", rows=[{"id": "dom-1"}])
        store = make_store(db)
        with pytest.raises(NotFoundError):
            store.accept_evolution_run("Evo", "run-1", 1, VERSION_ROW)


def test_heartbeat_does_not_bump_revision(fake_psycopg):
    db = FakeDB()
    store = make_store(db)
    store.touch_evolution_heartbeat("Evo", "run-1")
    call = db.one("SET heartbeat_at = now()")
    assert "revision" not in call.sql and "status = 'RUNNING'" in call.sql
    assert call.params == (REGISTRY_ID, "Evo", "run-1")
