"""Real SQL of :class:`Catalog` against a scripted cursor.

Covers domain/version CRUD, the guarded (Draft-only) version deletion, the
lifecycle status update, permissions replacement, and the lazy column
self-heal migrations.
"""

from __future__ import annotations

import json

import pytest

from back.core.errors import ConflictError, InfrastructureError

from ._v090_fake_pg import (
    DICT_ROW,
    QSCHEMA,
    REGISTRY_ID,
    SCHEMA,
    FakeDB,
    fake_psycopg,  # noqa: F401
    make_store,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Listing / existence
# ---------------------------------------------------------------------------


def test_list_domain_folders_is_registry_scoped_and_ordered():
    db = FakeDB().on("SELECT folder FROM", rows=[("alpha",), ("beta",)])
    store = make_store(db)

    ok, names, err = store.list_domain_folders()

    assert (ok, names, err) == (True, ["alpha", "beta"], "")
    call = db.one("SELECT folder FROM")
    assert f"FROM {QSCHEMA}.domains" in call.sql
    assert "ORDER BY folder" in call.sql
    assert call.params == (REGISTRY_ID,)


def test_list_domain_folders_reports_failure_instead_of_raising():
    db = FakeDB().on("SELECT folder FROM", raises=RuntimeError("boom"))

    assert make_store(db).list_domain_folders() == (False, [], "boom")


def test_list_domains_with_metadata_merges_latest_version_info(fake_psycopg):
    db = FakeDB()
    db.on(
        "FROM \"ontobricks_registry\".domains d WHERE d.registry_id",
        rows=[
            {
                "id": "d1",
                "folder": "acme",
                "description": "",
                "base_uri": "",
                "review_quorum": 3,
                "mcp_policy": None,
            },
            {
                "id": "d2",
                "folder": "empty",
                "description": "Desc",
                "base_uri": "http://e/",
                "review_quorum": 0,
                "mcp_policy": None,
            },
        ],
    )
    db.on(
        "FROM \"ontobricks_registry\".domain_versions v",
        rows=[
            {
                "domain_id": "d1",
                "version": "2",
                "mcp_enabled": 1,
                "status": None,
                "last_update": None,
                "last_build": "2026-01-01",
                "info": {
                    "description": "from info",
                    "graph_backend": None,
                    "neo4j_connection": "  bolt://x  ",
                },
                "ontology": {"base_uri": "http://acme/", "classes": [{"n": 1}]},
            },
            {
                "domain_id": "d1",
                "version": "1",
                "mcp_enabled": 0,
                "status": "PUBLISHED",
                "last_update": "u",
                "last_build": None,
                "info": None,
                "ontology": None,
            },
        ],
    )
    store = make_store(db)

    ok, result, err = store.list_domains_with_metadata()

    assert ok and err == ""
    acme, empty = result
    assert acme["name"] == "acme"
    assert acme["description"] == "from info"
    assert acme["base_uri"] == "http://acme/"
    assert acme["neo4j_connection"] == "bolt://x"
    assert acme["review_quorum"] == 3
    assert [v["version"] for v in acme["versions"]] == ["2", "1"]
    assert acme["versions"][0] == {
        "version": "2",
        "active": True,
        "status": "DRAFT",
        "last_update": "",
        "last_build": "2026-01-01",
        "graph_backend": acme["versions"][0]["graph_backend"],
        "has_ontology": True,
    }
    assert acme["versions"][1]["status"] == "PUBLISHED"
    assert acme["versions"][1]["has_ontology"] is False
    assert empty["versions"] == []
    assert empty["description"] == "Desc"
    assert empty["review_quorum"] == 1, "quorum is clamped to >= 1"
    assert all(c.row_factory is DICT_ROW for c in db.calls)
    assert "string_to_array(v.version, '.')::int[] DESC" in db.calls[1].sql


def test_list_domains_with_metadata_reports_errors(fake_psycopg):
    db = FakeDB().on("FROM \"ontobricks_registry\".domains d", raises=RuntimeError("x"))

    assert make_store(db).list_domains_with_metadata() == (False, [], "x")


def test_domain_exists_true_false_and_error():
    db = FakeDB().on("SELECT 1 FROM", rows=[(1,)])
    store = make_store(db)
    assert store.domain_exists("acme") is True
    assert db.calls[-1].params == (REGISTRY_ID, "acme")
    assert store.domain_exists("acme") is False

    db.on("SELECT 1 FROM", raises=RuntimeError("down"))
    assert store.domain_exists("acme") is False


@pytest.mark.parametrize(
    "rows,expected", [([(4,)], 4), ([(0,)], 1), ([(None,)], 1), ([], 1)]
)
def test_get_domain_quorum_clamps_and_defaults(rows, expected):
    db = FakeDB().on("SELECT review_quorum", rows=rows)

    assert make_store(db).get_domain_quorum("acme") == expected


def test_get_domain_quorum_raises_on_read_error():
    db = FakeDB().on("SELECT review_quorum", raises=RuntimeError("down"))

    with pytest.raises(InfrastructureError):
        make_store(db).get_domain_quorum("acme")


def test_delete_domain_is_parameterised_and_reports_errors():
    db = FakeDB()
    store = make_store(db)

    assert store.delete_domain("acme'; DROP TABLE x;--") == []
    call = db.one("DELETE FROM")
    assert call.params == (REGISTRY_ID, "acme'; DROP TABLE x;--")
    db.assert_not_interpolated("DROP TABLE x")

    db.on("DELETE FROM", raises=RuntimeError("fk"))
    assert store.delete_domain("acme") == ["fk"]


def test_list_versions_orders_numerically():
    db = FakeDB().on("SELECT v.version", rows=[("1",), ("2",), ("10",)])

    ok, versions, _ = make_store(db).list_versions("acme")

    assert ok and versions == ["1", "2", "10"]
    assert "ORDER BY string_to_array(v.version, '.')::int[]" in db.calls[0].sql
    assert db.calls[0].params == (REGISTRY_ID, "acme")


def test_list_versions_error():
    db = FakeDB().on("SELECT v.version", raises=RuntimeError("e"))

    assert make_store(db).list_versions("acme") == (False, [], "e")


# ---------------------------------------------------------------------------
# read / write version
# ---------------------------------------------------------------------------


def _version_row(**over):
    row = {
        "info": {"name": "acme"},
        "ontology": {"classes": []},
        "assignment": None,
        "design_layout": None,
        "metadata": {"k": "v"},
        "version": "3",
        "mcp_enabled": True,
        "status": "",
        "last_update": "2026-02-02",
        "last_build": "",
        "review_quorum": 2,
        "mcp_policy": None,
        "domain_base_uri": "http://domain/",
    }
    row.update(over)
    return row


def test_read_version_assembles_document_and_falls_back_to_domain_base_uri(
    fake_psycopg,
):
    db = FakeDB().on("SELECT v.info, v.ontology", rows=[_version_row()])

    ok, doc, err = make_store(db).read_version("acme", "3")

    assert ok and err == ""
    info = doc["info"]
    assert info["mcp_enabled"] is True
    assert info["review_quorum"] == 2
    assert info["status"] == "DRAFT"
    assert info["last_update"] == "2026-02-02"
    assert "last_build" not in info
    blob = doc["versions"]["3"]
    assert blob["ontology"]["base_uri"] == "http://domain/"
    assert blob["assignment"] == {} and blob["design_layout"] == {}
    assert blob["metadata"] == {"k": "v"}
    call = db.one("SELECT v.info, v.ontology")
    assert call.params == (REGISTRY_ID, "acme", "3")
    assert call.row_factory is DICT_ROW


def test_read_version_keeps_ontology_base_uri_when_present(fake_psycopg):
    row = _version_row(
        ontology={"base_uri": "http://onto/"}, last_build="b", status="REVIEW"
    )
    db = FakeDB().on("SELECT v.info, v.ontology", rows=[row])

    _ok, doc, _ = make_store(db).read_version("acme", "3")

    assert doc["versions"]["3"]["ontology"]["base_uri"] == "http://onto/"
    assert doc["info"]["last_build"] == "b"
    assert doc["info"]["status"] == "REVIEW"


def test_read_version_missing_row(fake_psycopg):
    ok, doc, err = make_store(FakeDB()).read_version("acme", "9")

    assert not ok and doc == {}
    assert err == "Version 9 not found for domain acme"


def test_read_version_error(fake_psycopg):
    db = FakeDB().on("SELECT v.info", raises=RuntimeError("bad"))

    assert make_store(db).read_version("acme", "1") == (False, {}, "bad")


def test_write_version_upserts_domain_then_version_with_json_params():
    db = FakeDB().on("INSERT INTO \"ontobricks_registry\".domains", rows=[("dom-uuid",)])
    data = {
        "info": {
            "description": "D",
            "mcp_enabled": 1,
            "status": "REVIEW",
            "last_update": "lu",
            "review_quorum": 0,
        },
        "versions": {
            "4": {
                "ontology": {"base_uri": "http://x/", "classes": [1]},
                "assignment": {"a": 1},
            }
        },
        "design_layout": {"top": 1},
    }

    ok, err = make_store(db).write_version("acme", "4", data)

    assert (ok, err) == (True, "")
    dom = db.one("INSERT INTO \"ontobricks_registry\".domains")
    assert "ON CONFLICT (registry_id, folder)" in dom.sql
    assert "WHEN EXCLUDED.base_uri != ''" in dom.sql, "empty base_uri keeps stored value"
    assert dom.params[:5] == (REGISTRY_ID, "acme", "D", "http://x/", 1)
    assert json.loads(dom.params[5]) == {}

    ver = db.one("INSERT INTO \"ontobricks_registry\".domain_versions")
    assert "ON CONFLICT (domain_id, version)" in ver.sql
    p = ver.params
    assert p[0] == "dom-uuid" and p[1] == "4"
    assert json.loads(p[2])["status"] == "REVIEW"
    assert json.loads(p[3]) == {"base_uri": "http://x/", "classes": [1]}
    assert json.loads(p[4]) == {"a": 1}
    assert json.loads(p[5]) == {"top": 1}, "falls back to top-level design_layout"
    assert json.loads(p[6]) == {}
    assert p[7:] == (True, "REVIEW", "lu", "")


def test_write_version_reports_failure():
    db = FakeDB().on("INSERT INTO", raises=RuntimeError("constraint"))

    assert make_store(db).write_version("acme", "1", {}) == (False, "constraint")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_delete_version_refuses_non_draft_versions_with_conflict():
    db = FakeDB().on("DELETE FROM", rowcount=0)

    with pytest.raises(ConflictError):
        make_store(db).delete_version("acme", "1")
    assert db.one("DELETE FROM").params[-1] == "DRAFT"


def test_delete_version_of_draft_succeeds_with_scoped_delete():
    db = FakeDB().on("DELETE FROM", rowcount=1)

    assert make_store(db).delete_version("acme", "1") == (True, "")
    deletes = db.sql_with("DELETE FROM")
    call = next(c for c in deletes if "domain_versions" in c.sql)
    assert f"DELETE FROM {QSCHEMA}.domain_versions AS v USING {QSCHEMA}.domains AS d" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "1", "DRAFT")
    assert any("simulation_runs" in c.sql for c in deletes)
    assert any("simulations s" in c.sql for c in deletes)


def test_delete_version_non_conflict_errors_become_tuples():
    db = FakeDB().on("DELETE FROM", raises=RuntimeError("io"))

    assert make_store(db).delete_version("acme", "1") == (False, "io")


def test_update_version_status_mirrors_status_into_info():
    db = FakeDB().on("UPDATE", rowcount=1)

    assert make_store(db).update_version_status("acme", "2", "PUBLISHED") == (True, "")
    call = db.one("UPDATE")
    assert "jsonb_set(v.info, '{status}', to_jsonb(%s::text))" in call.sql
    assert call.params == ("PUBLISHED", "PUBLISHED", REGISTRY_ID, "acme", "2")


def test_update_version_status_missing_version_and_error():
    store = make_store(FakeDB().on("UPDATE", rowcount=0))
    assert store.update_version_status("acme", "2", "X") == (
        False,
        "Version 2 not found for domain acme",
    )
    store = make_store(FakeDB().on("UPDATE", raises=RuntimeError("e")))
    assert store.update_version_status("acme", "2", "X") == (False, "e")


def test_get_version_status_found_missing_and_error():
    db = FakeDB().on("SELECT v.status", rows=[("PUBLISHED",)])
    store = make_store(db)
    assert store.get_version_status("acme", "1") == "PUBLISHED"
    assert db.calls[0].params == (REGISTRY_ID, "acme", "1")
    assert store.get_version_status("acme", "1") is None
    db.on("SELECT v.status", raises=RuntimeError("e"))
    assert store.get_version_status("acme", "1") is None


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------


def test_load_domain_permissions_maps_rows(fake_psycopg):
    rows = [{"principal": "a@x", "principal_type": "user", "display_name": "A", "role": "editor"}]
    db = FakeDB().on("FROM \"ontobricks_registry\".domain_permissions p", rows=rows)

    out = make_store(db).load_domain_permissions("acme")

    assert out == {"version": 1, "permissions": rows}
    assert "ORDER BY lower(p.principal)" in db.calls[0].sql


def test_load_domain_permissions_missing_domain_returns_empty(fake_psycopg):
    assert make_store(FakeDB()).load_domain_permissions("acme") == {
        "version": 1,
        "permissions": [],
    }


def test_load_domain_permissions_read_error_raises(fake_psycopg):
    db = FakeDB().on("domain_permissions", raises=RuntimeError("e"))

    with pytest.raises(InfrastructureError):
        make_store(db).load_domain_permissions("acme")


def test_save_domain_permissions_replaces_entries_with_defaults():
    db = FakeDB().on("SELECT id FROM", rows=[("dom-1",)])
    data = {"permissions": [{"principal": "a@x", "role": "builder"}, {"principal": "g"}]}

    ok, msg = make_store(db).save_domain_permissions("acme", data)

    assert ok and msg == "Domain permissions saved"
    assert db.transactions == 1, "DELETE + INSERTs run in a single transaction"
    assert db.one("DELETE FROM").params == ("dom-1",)
    inserts = db.sql_with("INSERT INTO")
    assert [c.params for c in inserts] == [
        ("dom-1", "a@x", "user", "", "builder"),
        ("dom-1", "g", "user", "", "viewer"),
    ]


def test_save_domain_permissions_unknown_domain_and_error():
    db = FakeDB()
    assert make_store(db).save_domain_permissions("nope", {}) == (
        False,
        "Domain 'nope' not found",
    )
    assert not db.sql_with("DELETE FROM")
    db.on("SELECT id FROM", raises=RuntimeError("e"))
    assert make_store(db).save_domain_permissions("x", {}) == (False, "e")


# ---------------------------------------------------------------------------
# Lazy column self-heal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method,flag,column,ddl",
    [
        ("_ensure_domain_versions_status_column", "_status_column_ready", "status", "idx_domain_versions_status"),
        ("_ensure_domains_review_quorum_column", "_quorum_column_ready", "review_quorum", "ADD COLUMN IF NOT EXISTS review_quorum"),
        ("_ensure_domains_mcp_policy_column", "_mcp_policy_column_ready", "mcp_policy", "ADD COLUMN IF NOT EXISTS mcp_policy"),
    ],
)
def test_column_self_heal_skips_ddl_when_column_exists(method, flag, column, ddl):
    db = FakeDB().on("information_schema.columns", rows=[(1,)])
    store = make_store(db, ready=False)

    assert getattr(store, method)() is True
    assert getattr(store, flag) is True
    assert db.calls[0].params == (SCHEMA,)
    assert f"column_name = '{column}'" in db.calls[0].sql
    assert not db.sql_with("ALTER TABLE")
    # Cached: no second round-trip.
    assert getattr(store, method)() is True
    assert len(db.calls) == 1


@pytest.mark.parametrize(
    "method,flag,ddl",
    [
        ("_ensure_domain_versions_status_column", "_status_column_ready", "idx_domain_versions_status"),
        ("_ensure_domains_review_quorum_column", "_quorum_column_ready", "ADD COLUMN IF NOT EXISTS review_quorum"),
        ("_ensure_domains_mcp_policy_column", "_mcp_policy_column_ready", "ADD COLUMN IF NOT EXISTS mcp_policy"),
    ],
)
def test_column_self_heal_runs_ddl_when_missing_and_fails_soft(method, flag, ddl):
    db = FakeDB()
    store = make_store(db, ready=False)

    assert getattr(store, method)() is True
    assert db.sql_with(ddl)
    assert all(QSCHEMA in c.sql for c in db.sql_with("ALTER TABLE"))

    broken = make_store(
        FakeDB().on("ALTER TABLE", raises=RuntimeError("must be owner")), ready=False
    )
    assert getattr(broken, method)() is False
    assert getattr(broken, flag) is False
