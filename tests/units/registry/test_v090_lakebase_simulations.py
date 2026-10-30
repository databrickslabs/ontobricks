"""SQL contracts for the Lakebase simulations mixin."""

from __future__ import annotations

import json

import pytest

from back.core.errors import InfrastructureError

from ._v090_fake_pg import REGISTRY_ID, FakeDB, fake_psycopg, make_store  # noqa: F401

pytestmark = pytest.mark.unit


def test_copy_simulations_to_version_copies_draft_specs_and_runs():
    db = FakeDB()
    store = make_store(db)

    assert store.copy_simulations_to_version("acme", "5", "6") == (True, "")
    inserts = db.sql_with("INSERT INTO")
    assert len(inserts) == 2
    spec_sql, run_sql = inserts[0].sql, inserts[1].sql
    assert "'draft'" in spec_sql
    assert "NULL" in spec_sql and "last_run_at" in spec_sql
    assert "ON CONFLICT (domain_id, version, id) DO NOTHING" in spec_sql
    assert inserts[0].params == ("6", REGISTRY_ID, "acme", "5")
    assert "simulation_runs" in run_sql
    assert inserts[1].params == ("6", REGISTRY_ID, "acme", "5")
    db.assert_not_interpolated("acme", "5", "6")


def test_copy_simulations_to_version_reports_error():
    db = FakeDB().on("INSERT INTO", raises=RuntimeError("e"))
    assert make_store(db).copy_simulations_to_version("acme", "1", "2") == (False, "e")


def test_delete_simulations_for_version_drops_runs_then_specs():
    db = FakeDB()
    store = make_store(db)

    assert store.delete_simulations_for_version("acme", "5") == (True, "")
    deletes = db.sql_with("DELETE FROM")
    assert len(deletes) == 2
    assert "simulation_runs" in deletes[0].sql
    assert "simulations" in deletes[1].sql
    assert deletes[0].params == (REGISTRY_ID, "acme", "5")
    assert deletes[1].params == (REGISTRY_ID, "acme", "5")


def test_persist_version_simulations_skips_when_payload_has_no_keys():
    db = FakeDB()
    make_store(db).persist_version_simulations(
        "acme", "5", {"versions": {"5": {"ontology": {}}}}
    )
    assert db.calls == []


def test_persist_version_simulations_replaces_specs_from_blob(fake_psycopg):
    db = FakeDB().on("INSERT INTO", rowcount=1, once=False)
    store = make_store(db)
    store.persist_version_simulations(
        "acme",
        "5",
        {
            "versions": {
                "5": {
                    "simulations": [
                        {
                            "id": "se1",
                            "label": "Jacob",
                            "status": "draft",
                            "spec": {"patches": []},
                        }
                    ],
                    "simulation_runs": [
                        {"simulation_id": "se1", "status": "success", "stats": {"n": 1}}
                    ],
                }
            }
        },
    )
    assert any("DELETE FROM" in c.sql and "simulations" in c.sql for c in db.calls)
    inserts = [c for c in db.calls if "INSERT INTO" in c.sql]
    assert any("simulations" in c.sql for c in inserts)
    spec_insert = next(c for c in inserts if "simulations" in c.sql and "id, label" in c.sql)
    assert spec_insert.params[1] == "se1"
    assert json.loads(spec_insert.params[4]) == {"patches": []}
    assert any("simulation_runs" in c.sql for c in inserts)


def test_attach_simulations_to_doc_fills_blob(fake_psycopg):
    db = FakeDB()
    db.on(
        "FROM \"ontobricks_registry\".simulations s",
        rows=[
            {
                "id": "se1",
                "version": "5",
                "label": "Jacob",
                "status": "draft",
                "spec": {"label": "Jacob"},
                "diff_summary": {},
                "owner": "a@x",
                "task_id": "",
                "error": "",
                "created_at": None,
                "updated_at": None,
                "last_run_at": None,
            }
        ],
    )
    db.on(
        "FROM \"ontobricks_registry\".simulation_runs r",
        rows=[
            {
                "id": 9,
                "simulation_id": "se1",
                "status": "success",
                "message": "+1 / -0",
                "error": "",
                "task_id": "t",
                "started_at": None,
                "finished_at": None,
                "duration_s": 1.2,
                "stats": {"added_count": 1},
                "owner": "a@x",
            }
        ],
    )
    doc = {"versions": {"5": {"ontology": {}}}}
    make_store(db).attach_simulations_to_doc(doc, "acme", "5")
    blob = doc["versions"]["5"]
    assert blob["simulations"][0]["id"] == "se1"
    assert "diff" not in blob["simulations"][0]
    assert blob["simulation_runs"][0]["simulation_id"] == "se1"
    assert blob["simulation_runs"][0]["stats"] == {"added_count": 1}


def test_save_simulation_unknown_domain_raises():
    db = FakeDB().on("INSERT INTO", rowcount=0)
    store = make_store(db)
    with pytest.raises(InfrastructureError, match="not registered"):
        store.save_simulation("missing", "1", {"id": "se1", "label": "x"})


def test_base_copy_and_delete_are_noops():
    from back.objects.registry.store.base import RegistryStore

    assert RegistryStore.copy_simulations_to_version(None, "f", "1", "2") == (True, "")
    assert RegistryStore.delete_simulations_for_version(None, "f", "1") == (True, "")
