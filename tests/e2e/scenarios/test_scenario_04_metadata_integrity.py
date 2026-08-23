"""
E2E (LIVE) — Pillar 1 (Data Source & Mapping Integrity), end to end.

Exercises, against a running app and a real SQL warehouse, the three
behaviours added for "Pillar 1 · Data Source & Mapping Integrity" as one
connected journey:

    1. schema drift        — a bound column renamed upstream is flagged by
                              GET /mapping/schema-drift
    2. metadata refresh diff — POST /domain/metadata/update-async reports the
                              rename as removed+added, and nothing is
                              persisted until /domain/metadata/save is called
                              (mirroring the browser's discard/apply gate)
    3. deletion guard        — POST /domain/metadata/removal-impact lists the
                              mappings a table removal would orphan, and the
                              removal itself still proceeds (guard informs,
                              doesn't block) and reports the same impact

Unlike scenarios 1-3, this journey does **not** reuse ``testscenario1`` — it
builds its own domain over two scratch Unity Catalog tables it creates and
drops itself, so it can freely rename/drop columns without touching shared
data. It is a sibling journey, not another chapter of scenario 1's story, so
it is intentionally left out of the campaign's dependency chain (registered
under its own ``chain_marker`` name with no ``depends``, so campaign runs
don't skip it, but nothing else is gated on it either).

The domain (``TestMetadataIntegrity``) is intentionally **not** deleted at
the end, same rationale as scenario 1: open the app afterwards and inspect
the (now partially broken, on purpose) mapping by hand.

Run (against the local dev server, started separately via ``scripts/start.sh``):

    ONTOBRICKS_SCENARIO_LIVE=1 \\
    uv run pytest tests/e2e/scenarios/test_scenario_04_metadata_integrity.py \\
        -m scenario -v -s --no-cov

This test process needs its own Databricks credentials to run the scratch-
table DDL directly (the same ones ``scripts/start.sh`` needs, so running in
the same shell as the dev server is enough): ``DATABRICKS_HOST``,
``DATABRICKS_TOKEN``, ``DATABRICKS_SQL_WAREHOUSE_ID``.

Override the target / inputs via env:
    ONTOBRICKS_LIVE_BASE          base URL (default http://localhost:8000)
    ONTOBRICKS_SCENARIO_CATALOG   scratch-table catalog (default benoit_cayla)
    ONTOBRICKS_SCENARIO_SCHEMA    scratch-table schema  (default customer)
    ONTOBRICKS_SCENARIO4_PREFIX   scratch-table name prefix (default pillar1_scratch)
"""

from __future__ import annotations

import json
import os

import pytest

from tests.e2e.scenarios._harness import (
    chain_marker,
    csrf_headers,
    json_body,
    make_step,
    poll_task,
)

# ── Gate: this is a live, mutating journey against a real warehouse ─────────
pytestmark = [
    pytest.mark.skipif(
        os.environ.get("ONTOBRICKS_SCENARIO_LIVE") != "1",
        reason="live scenario — set ONTOBRICKS_SCENARIO_LIVE=1 to run "
        "(needs a running app + warehouse; writes a durable domain + two "
        "scratch UC tables it drops on teardown)",
    ),
    *chain_marker("scenario_4"),
]

_DOMAIN_NAME = "TestMetadataIntegrity"
_DOMAIN_FOLDER = _DOMAIN_NAME.lower()
_BASE_URI = f"http://ontobricks.ai/{_DOMAIN_NAME}#"

_CATALOG = os.environ.get("ONTOBRICKS_SCENARIO_CATALOG", "benoit_cayla")
_SCHEMA = os.environ.get("ONTOBRICKS_SCENARIO_SCHEMA", "customer")
_PREFIX = os.environ.get("ONTOBRICKS_SCENARIO4_PREFIX", "pillar1_scratch")
_TBL_CUSTOMERS = f"{_PREFIX}_customers"
_TBL_ORDERS = f"{_PREFIX}_orders"
_FQ_CUSTOMERS = f"{_CATALOG}.{_SCHEMA}.{_TBL_CUSTOMERS}"
_FQ_ORDERS = f"{_CATALOG}.{_SCHEMA}.{_TBL_ORDERS}"

_UPDATE_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO4_UPDATE_TIMEOUT", "120"))

_OWL_CONTENT = f"""@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <{_BASE_URI}> .

<http://ontobricks.ai/{_DOMAIN_NAME}> a owl:Ontology ;
    rdfs:label "{_DOMAIN_NAME}" .

:Customer a owl:Class ;
    rdfs:label "Customer" .

:Order a owl:Class ;
    rdfs:label "Order" .

:places a owl:ObjectProperty ;
    rdfs:label "places" ;
    rdfs:domain :Customer ;
    rdfs:range :Order .

:email a owl:DatatypeProperty ;
    rdfs:label "email" ;
    rdfs:domain :Customer ;
    rdfs:range xsd:string .
"""

_MAPPING_CONFIG = {
    "entities": [
        {
            "ontology_class": f"{_BASE_URI}Customer",
            "ontology_class_label": "Customer",
            "catalog": _CATALOG,
            "schema": _SCHEMA,
            "table": _TBL_CUSTOMERS,
            "id_column": "customer_id",
            "label_column": "full_name",
            # Plain table binding (no sql_query) so schema-drift compares
            # against the live table schema, not a SQL projection.
            "attribute_mappings": {"email": "email"},
        },
        {
            "ontology_class": f"{_BASE_URI}Order",
            "ontology_class_label": "Order",
            "catalog": _CATALOG,
            "schema": _SCHEMA,
            "table": _TBL_ORDERS,
            "id_column": "order_id",
            "label_column": "order_id",
        },
    ],
    "relationships": [
        {
            "property": f"{_BASE_URI}places",
            "property_label": "places",
            "source_class": f"{_BASE_URI}Customer",
            "source_class_label": "Customer",
            "target_class": f"{_BASE_URI}Order",
            "target_class_label": "Order",
            "source_table": _FQ_CUSTOMERS,
            "target_table": _FQ_ORDERS,
            "source_id_column": "customer_id",
            "target_id_column": "customer_id",
            "direction": "forward",
        },
    ],
}

_csrf_headers = csrf_headers
_json = json_body
_step = make_step("scenario_4")


@pytest.fixture(scope="module", autouse=True)
def _scratch_tables():
    """Create the two scratch UC tables this journey mutates, drop them after.

    Runs in the pytest process itself (not through the app), so it needs its
    own Databricks credentials — skip cleanly rather than hang/fail obscurely
    if they're not resolvable here.
    """
    from back.core.databricks.DatabricksClient import DatabricksClient

    client = DatabricksClient()
    if not client.has_valid_auth() or not client.warehouse_id:
        pytest.skip(
            "No Databricks credentials/warehouse resolvable in this shell — "
            "this test process needs DATABRICKS_HOST/DATABRICKS_TOKEN/"
            "DATABRICKS_SQL_WAREHOUSE_ID too (same as scripts/start.sh)."
        )

    _step(f"creating scratch tables {_FQ_CUSTOMERS}, {_FQ_ORDERS}")
    client.execute_statement(f"DROP TABLE IF EXISTS {_FQ_CUSTOMERS}")
    client.execute_statement(f"DROP TABLE IF EXISTS {_FQ_ORDERS}")
    client.execute_statement(
        f"CREATE TABLE {_FQ_CUSTOMERS} "
        "(customer_id INT, full_name STRING, email STRING) "
        # RENAME COLUMN (step 4 below) needs Delta column mapping enabled.
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    )
    client.execute_statement(
        f"CREATE TABLE {_FQ_ORDERS} "
        "(order_id INT, customer_id INT, amount DECIMAL(10,2))"
    )
    client.execute_statement(
        f"INSERT INTO {_FQ_CUSTOMERS} VALUES (1, 'Ada Lovelace', 'ada@example.com')"
    )
    client.execute_statement(f"INSERT INTO {_FQ_ORDERS} VALUES (100, 1, 42.50)")

    yield client

    _step("dropping scratch tables")
    client.execute_statement(f"DROP TABLE IF EXISTS {_FQ_CUSTOMERS}")
    client.execute_statement(f"DROP TABLE IF EXISTS {_FQ_ORDERS}")


class TestScenario4MetadataIntegrity:
    """Import → map → drift → refresh-diff → deletion-guard, one journey."""

    def test_schema_drift_diff_preview_and_deletion_guard(
        self, scenario_page, scenario_base, _scratch_tables
    ):
        page = scenario_page
        base = scenario_base

        # ── 1. Prime the session, clean slate, create the domain ─────────────
        _step(f"priming session at {base}")
        page.goto(base)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)

        def _registry_names() -> set[str]:
            try:
                data = _json(page.request.get(f"{base}/domain/list-projects"))
            except Exception:  # noqa: BLE001
                return set()
            out = set()
            for d in data.get("domains", []) or []:
                name = d if isinstance(d, str) else (d.get("name") or d.get("folder") or "")
                if name:
                    out.add(name.lower())
            return out

        if _DOMAIN_FOLDER in _registry_names():
            _step(f"'{_DOMAIN_FOLDER}' exists — deleting it for a clean rebuild")
            resp = page.context.request.delete(
                f"{base}/settings/registry/domains/{_DOMAIN_FOLDER}",
                headers=headers,
                timeout=60_000,
            )
            assert resp.status in (200, 204), resp.text()

        resp = page.context.request.post(f"{base}/domain/reset", headers=headers)
        assert resp.status == 200, resp.text()

        resp = page.context.request.post(
            f"{base}/domain/info",
            headers=headers,
            data=json.dumps(
                {
                    "name": _DOMAIN_NAME,
                    "description": "Live scenario: schema drift, refresh diff, deletion guard",
                    "base_uri": _BASE_URI,
                }
            ),
        )
        assert resp.status == 200, resp.text()
        _step(f"domain '{_DOMAIN_NAME}' created")

        # ── 2. Import the two scratch tables as data sources ──────────────────
        _step(f"importing {_CATALOG}.{_SCHEMA} ({_TBL_CUSTOMERS}, {_TBL_ORDERS})")
        resp = page.context.request.post(
            f"{base}/domain/metadata/initialize",
            headers=headers,
            data=json.dumps(
                {
                    "catalog": _CATALOG,
                    "schema": _SCHEMA,
                    "selected_tables": [_TBL_CUSTOMERS, _TBL_ORDERS],
                }
            ),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        meta = _json(resp)
        assert meta.get("success") is True, meta
        tables = {t["name"]: t for t in meta.get("metadata", {}).get("tables", [])}
        assert {_TBL_CUSTOMERS, _TBL_ORDERS} <= set(tables), tables
        cust_cols = {c.get("name") or c.get("col_name") for c in tables[_TBL_CUSTOMERS]["columns"]}
        assert {"customer_id", "full_name", "email"} <= cust_cols, cust_cols
        _step("both scratch tables imported")

        # ── 3. Minimal ontology + plain-table mapping ─────────────────────────
        _step("parsing the minimal Customer/Order/places ontology")
        resp = page.context.request.post(
            f"{base}/ontology/parse-owl",
            headers=headers,
            data=json.dumps({"content": _OWL_CONTENT}),
        )
        assert resp.status == 200, resp.text()
        onto = _json(page.request.get(f"{base}/ontology/load"))["config"]
        class_names = {c.get("name", "") for c in onto["classes"]}
        assert {"Customer", "Order"} <= class_names, class_names

        _step("saving the entity + relationship mapping")
        resp = page.context.request.post(
            f"{base}/mapping/save",
            headers=headers,
            data=json.dumps({"config": _MAPPING_CONFIG}),
        )
        assert resp.status == 200, resp.text()
        mapping_cfg = _json(page.request.get(f"{base}/mapping/load"))["config"]
        assert len(mapping_cfg["entities"]) == 2
        assert len(mapping_cfg["relationships"]) == 1

        # ── 4. DRIFT: rename customers.email upstream, expect it flagged ─────
        _step("renaming customers.email -> email_address directly in UC")
        _scratch_tables.execute_statement(
            f"ALTER TABLE {_FQ_CUSTOMERS} RENAME COLUMN email TO email_address"
        )

        drift = _json(page.request.get(f"{base}/mapping/schema-drift"))
        assert drift.get("success") is True, drift
        cust_drift = drift.get("entities", {}).get(f"{_BASE_URI}Customer")
        assert cust_drift is not None, (
            f"Customer entity not flagged for drift: {drift}"
        )
        assert "email" in cust_drift["columns"], cust_drift
        assert f"{_BASE_URI}places" not in drift.get("relationships", {}), (
            "the 'places' relationship only binds customer_id, which is "
            f"untouched — it must not be flagged: {drift['relationships']}"
        )
        _step("drift correctly flagged on Customer, not on the relationship")

        # ── 5. REFRESH DIFF: discard leaves stored metadata untouched ────────
        _step("Update from UC (customers) — expecting the rename as removed+added")
        resp = page.context.request.post(
            f"{base}/domain/metadata/update-async",
            headers=headers,
            data=json.dumps({"table_names": [_TBL_CUSTOMERS]}),
        )
        assert resp.status == 200, resp.text()
        task_id = _json(resp)["task_id"]
        task = poll_task(page, base, task_id, _UPDATE_TIMEOUT_S, "metadata update", step=_step)
        assert task.get("status") == "completed", task
        diff = task.get("result", {}).get("diff", {})
        table_diff = diff.get(_TBL_CUSTOMERS, {})
        assert "email" in {c["name"] for c in table_diff.get("removed", [])}, diff
        assert "email_address" in {c["name"] for c in table_diff.get("added", [])}, diff
        _step("diff correctly reports email removed, email_address added")

        # Discard: the browser simply never calls /domain/metadata/save. The
        # stored metadata must still show the pre-refresh column.
        stored = _json(page.request.get(f"{base}/domain/metadata"))
        stored_cols = {
            c.get("name") or c.get("col_name")
            for t in stored["metadata"]["tables"]
            if t["name"] == _TBL_CUSTOMERS
            for c in t["columns"]
        }
        assert "email" in stored_cols, "discard must leave stored metadata untouched"
        _step("discard confirmed — stored metadata unchanged")

        # Apply: re-run, then actually persist via /domain/metadata/save.
        _step("Update from UC again, this time applying the change")
        resp = page.context.request.post(
            f"{base}/domain/metadata/update-async",
            headers=headers,
            data=json.dumps({"table_names": [_TBL_CUSTOMERS]}),
        )
        assert resp.status == 200, resp.text()
        task_id = _json(resp)["task_id"]
        task = poll_task(page, base, task_id, _UPDATE_TIMEOUT_S, "metadata update", step=_step)
        refreshed_tables = task["result"]["metadata"]["tables"]

        resp = page.context.request.post(
            f"{base}/domain/metadata/save",
            headers=headers,
            data=json.dumps({"tables": refreshed_tables}),
        )
        assert resp.status == 200, resp.text()

        stored = _json(page.request.get(f"{base}/domain/metadata"))
        stored_cols = {
            c.get("name") or c.get("col_name")
            for t in stored["metadata"]["tables"]
            if t["name"] == _TBL_CUSTOMERS
            for c in t["columns"]
        }
        assert "email_address" in stored_cols and "email" not in stored_cols, stored_cols
        _step("apply confirmed — stored metadata now has email_address")

        # ── 6. DELETION GUARD: removal-impact pre-flight, then the removal ───
        customers_table = next(
            t for t in stored["metadata"]["tables"] if t["name"] == _TBL_CUSTOMERS
        )
        orders_table = next(
            t for t in stored["metadata"]["tables"] if t["name"] == _TBL_ORDERS
        )
        customers_ident = customers_table.get("full_name") or customers_table["name"]

        _step(f"removal-impact pre-flight for {customers_ident}")
        resp = page.context.request.post(
            f"{base}/domain/metadata/removal-impact",
            headers=headers,
            data=json.dumps({"table_names": [customers_ident]}),
        )
        assert resp.status == 200, resp.text()
        impact_report = _json(resp)
        referrers = impact_report.get("impact", {}).get(customers_ident, [])
        assert any(r.startswith("Entity: Customer") for r in referrers), referrers
        assert any(r.startswith("Rel: places") for r in referrers), referrers
        assert impact_report["affected_table_count"] >= 1
        _step(f"guard reports referrers: {referrers}")

        _step("removing customers anyway (guard informs, doesn't block)")
        resp = page.context.request.post(
            f"{base}/domain/metadata/save",
            headers=headers,
            data=json.dumps({"tables": [orders_table]}),
        )
        assert resp.status == 200, resp.text()
        save_result = _json(resp)
        assert save_result.get("success") is True, save_result
        assert customers_ident in save_result.get("impact", {}), save_result

        stored = _json(page.request.get(f"{base}/domain/metadata"))
        remaining = {t["name"] for t in stored["metadata"]["tables"]}
        assert remaining == {_TBL_ORDERS}, remaining
        _step("customers removed from data sources, orders retained")

        # ── 7. Leave the domain for manual inspection ─────────────────────────
        _step("saving the domain to the registry for manual follow-up inspection")
        resp = page.context.request.post(
            f"{base}/domain/save-to-uc",
            headers=_csrf_headers(page.context),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        assert _json(resp).get("success") is True, resp.text()
        _step(
            f"DONE — open {base}, load '{_DOMAIN_NAME}' and inspect the "
            "Customer entity (now missing its data source) in Mapping Diagnostics."
        )
