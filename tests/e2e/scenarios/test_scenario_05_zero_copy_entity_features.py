"""
E2E (LIVE) — ``test_scenario_5``: Lakehouse zero-copy backend, virtual
attributes and entity actions (SWRL business rule + Unity Catalog function).

One connected journey against a running app and a real SQL warehouse:

    1. create a domain (default backend), then **switch its graph backend** to
       the Lakehouse in **zero-copy** mode (``graph_backend=databricks``,
       ``lakehouse_materialization=view``)
    2. build the knowledge graph through the Lakehouse build
       (``/dtwin/databricks-build/start``) — the ``…_data`` relation is a VIEW
    3. **prove zero-copy**: insert a customer directly in Unity Catalog and
       watch the ``…_data`` triple count grow with no rebuild
    4. declare, on the ``Customer`` class, a **virtual attribute** group (a
       ``RETURNS TABLE`` UC function), a **UC function action** (scalar) and a
       **SWRL business rule**; check the node context exposes all three
    5. **compute the virtual attributes** of an entity
    6. **invoke the UC function action** through the request → confirm flow,
       and check the one-time token cannot be replayed
    7. **trigger the SWRL business rule** on one entity (request → confirm):
       it infers for a customer that placed an order, and nothing for one that
       did not (the rule is scoped to the focus entity)

Like scenario 4 this journey is self-contained: it builds its own domain over
scratch Unity Catalog tables and functions it creates and drops itself, so it
is a sibling of the scenario 1-3 story (own ``chain_marker`` name, no
``depends``) and never touches ``zztestscenario1``.

The domain (``ZzTestZeroCopy``) is intentionally **not** deleted at the end, same
rationale as scenarios 1 and 4: open the app afterwards and inspect it. Its
graph is a view over scratch tables that teardown drops, so it reads empty
until the scenario runs again.

Run (against the local dev server, started separately via ``scripts/start.sh``):

    ONTOBRICKS_SCENARIO_LIVE=1 \\
    uv run --frozen pytest tests/e2e/scenarios/test_scenario_05_zero_copy_entity_features.py \\
        -m scenario -v -s --no-cov

This test process needs its own Databricks credentials to run the scratch DDL
directly. Export ``DATABRICKS_CONFIG_PROFILE`` (or ``DATABRICKS_HOST`` +
``DATABRICKS_TOKEN``) and ``DATABRICKS_SQL_WAREHOUSE_ID`` in the pytest shell.

Override the target / inputs via env:
    ONTOBRICKS_LIVE_BASE            base URL (default http://localhost:8000)
    ONTOBRICKS_SCENARIO_CATALOG     scratch catalog (default benoit_cayla)
    ONTOBRICKS_SCENARIO_SCHEMA      scratch schema  (default customer)
    ONTOBRICKS_SCENARIO5_PREFIX     scratch object name prefix (default zerocopy_scratch)
    ONTOBRICKS_SCENARIO5_BUILD_TIMEOUT  max seconds for the Lakehouse build (default 420)
"""

from __future__ import annotations

import json
import os
from urllib.parse import quote

import pytest

from tests.e2e.scenarios._harness import (
    ZERO_COPY_DOMAIN_NAME,
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
        "(needs a running app + warehouse; writes a durable domain + scratch "
        "UC tables and functions it drops on teardown)",
    ),
    *chain_marker("scenario_5", depends=("scenario_0",)),
]

_DOMAIN_NAME = ZERO_COPY_DOMAIN_NAME
_DOMAIN_FOLDER = _DOMAIN_NAME.lower()
_BASE_URI = f"http://ontobricks.ai/{_DOMAIN_NAME}#"

_CATALOG = os.environ.get("ONTOBRICKS_SCENARIO_CATALOG", "benoit_cayla")
_SCHEMA = os.environ.get("ONTOBRICKS_SCENARIO_SCHEMA", "customer")
_PREFIX = os.environ.get("ONTOBRICKS_SCENARIO5_PREFIX", "zerocopy_scratch")
_TBL_CUSTOMERS = f"{_PREFIX}_customers"
_TBL_ORDERS = f"{_PREFIX}_orders"
_FQ_CUSTOMERS = f"{_CATALOG}.{_SCHEMA}.{_TBL_CUSTOMERS}"
_FQ_ORDERS = f"{_CATALOG}.{_SCHEMA}.{_TBL_ORDERS}"
_FN_PROFILE = f"{_PREFIX}_profile"
_FN_FLAG = f"{_PREFIX}_flag"
_FQ_FN_PROFILE = f"{_CATALOG}.{_SCHEMA}.{_FN_PROFILE}"
_FQ_FN_FLAG = f"{_CATALOG}.{_SCHEMA}.{_FN_FLAG}"

_RULE_NAME = "ActiveCustomerRule"

_BUILD_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO5_BUILD_TIMEOUT", "420"))

_OWL_CONTENT = f"""@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <{_BASE_URI}> .

<http://ontobricks.ai/{_DOMAIN_NAME}> a owl:Ontology ;
    rdfs:label "{_DOMAIN_NAME}" .

:Customer a owl:Class ;
    rdfs:label "Customer" .

:ActiveCustomer a owl:Class ;
    rdfs:label "ActiveCustomer" ;
    rdfs:subClassOf :Customer .

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
            # A relationship triple map needs a query yielding both ends' IDs.
            "sql_query": f"SELECT customer_id, order_id FROM {_FQ_ORDERS}",
            "source_id_column": "customer_id",
            "target_id_column": "order_id",
            "direction": "forward",
        },
    ],
}

_RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

_csrf_headers = csrf_headers
_json = json_body
_step = make_step("scenario_5")


@pytest.fixture(scope="module", autouse=True)
def _scratch_objects():
    """Create the scratch UC tables + functions this journey uses, drop them after.

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

    def _drop_all() -> None:
        client.execute_statement(f"DROP FUNCTION IF EXISTS {_FQ_FN_PROFILE}")
        client.execute_statement(f"DROP FUNCTION IF EXISTS {_FQ_FN_FLAG}")
        client.execute_statement(f"DROP TABLE IF EXISTS {_FQ_CUSTOMERS}")
        client.execute_statement(f"DROP TABLE IF EXISTS {_FQ_ORDERS}")

    _step(f"creating scratch tables + functions under {_CATALOG}.{_SCHEMA} ({_PREFIX}_*)")
    _drop_all()
    client.execute_statement(
        f"CREATE TABLE {_FQ_CUSTOMERS} (customer_id INT, full_name STRING, email STRING)"
    )
    client.execute_statement(
        f"CREATE TABLE {_FQ_ORDERS} (order_id INT, customer_id INT, amount DECIMAL(10,2))"
    )
    client.execute_statement(
        f"INSERT INTO {_FQ_CUSTOMERS} VALUES "
        "(1, 'Ada Lovelace', 'ada@example.com'), (2, 'Alan Turing', 'alan@example.com')"
    )
    client.execute_statement(f"INSERT INTO {_FQ_ORDERS} VALUES (100, 1, 42.50)")
    # Virtual attribute source: one row per entity, one attribute per column.
    client.execute_statement(
        f"CREATE FUNCTION {_FQ_FN_PROFILE}(entity_id STRING) "
        "RETURNS TABLE (display_name STRING, name_length INT) "
        "COMMENT 'Scenario 5 virtual attributes: display name and its length' "
        "RETURN SELECT upper(c.full_name) AS display_name, "
        "length(c.full_name) AS name_length "
        f"FROM {_FQ_CUSTOMERS} c WHERE CAST(c.customer_id AS STRING) = entity_id"
    )
    # Entity action: a scalar function echoing the entity it ran on.
    client.execute_statement(
        f"CREATE FUNCTION {_FQ_FN_FLAG}(entity_id STRING) RETURNS STRING "
        "COMMENT 'Scenario 5 action: flag a customer' "
        "RETURN concat('flagged:', entity_id)"
    )

    yield client

    _step("dropping scratch tables + functions")
    _drop_all()


class TestScenario5ZeroCopyEntityFeatures:
    """Switch to Lakehouse zero-copy → build → virtual attributes → actions."""

    def test_zero_copy_backend_virtual_attributes_and_actions(
        self, scenario_page, scenario_base, _scratch_objects
    ):
        page = scenario_page
        base = scenario_base

        def headers() -> dict:
            return _csrf_headers(page.context)

        def post(path: str, body: dict | None = None, timeout: int = 60_000):
            return page.context.request.post(
                f"{base}{path}",
                headers=headers(),
                data=json.dumps(body if body is not None else {}),
                timeout=timeout,
            )

        def get(path: str, timeout: int = 60_000):
            return page.context.request.get(f"{base}{path}", timeout=timeout)

        def ok(resp) -> dict:
            assert resp.status == 200, resp.text()
            body = _json(resp)
            assert body.get("success") is True, body
            return body

        def save_to_registry() -> None:
            ok(post("/domain/save-to-uc", timeout=120_000))

        # ── 1. Prime the session, clean slate, create the domain ─────────────
        _step(f"priming session at {base}")
        page.goto(base)
        page.wait_for_load_state("domcontentloaded")

        listed = _json(get("/domain/list-projects")).get("domains", []) or []
        names = {
            (d if isinstance(d, str) else (d.get("name") or d.get("folder") or "")).lower()
            for d in listed
        }
        if _DOMAIN_FOLDER in names:
            _step(f"'{_DOMAIN_FOLDER}' exists — deleting it for a clean rebuild")
            resp = page.context.request.delete(
                f"{base}/settings/registry/domains/{_DOMAIN_FOLDER}",
                headers=headers(),
                timeout=60_000,
            )
            assert resp.status in (200, 204), resp.text()

        assert post("/domain/reset").status == 200
        info = ok(
            post(
                "/domain/info",
                {
                    "name": _DOMAIN_NAME,
                    "description": "Live scenario: Lakehouse zero-copy, virtual "
                    "attributes, SWRL + UC function actions",
                    "base_uri": _BASE_URI,
                },
            )
        )["info"]
        _step(
            f"domain '{_DOMAIN_NAME}' created on the default backend "
            f"({info.get('graph_backend')})"
        )

        # ── 2. Data sources, ontology, mapping ───────────────────────────────
        _step(f"importing {_TBL_CUSTOMERS}, {_TBL_ORDERS} as data sources")
        meta = ok(
            post(
                "/domain/metadata/initialize",
                {
                    "catalog": _CATALOG,
                    "schema": _SCHEMA,
                    "selected_tables": [_TBL_CUSTOMERS, _TBL_ORDERS],
                },
                timeout=120_000,
            )
        )
        imported = {t["name"] for t in meta.get("metadata", {}).get("tables", [])}
        assert {_TBL_CUSTOMERS, _TBL_ORDERS} <= imported, imported

        _step("parsing the Customer / ActiveCustomer / Order ontology")
        assert post("/ontology/parse-owl", {"content": _OWL_CONTENT}).status == 200
        onto = _json(get("/ontology/load"))["config"]
        classes = {c.get("name", ""): c for c in onto["classes"]}
        assert {"Customer", "ActiveCustomer", "Order"} <= set(classes), sorted(classes)

        _step("saving the entity + relationship mapping")
        assert post("/mapping/save", {"config": _MAPPING_CONFIG}).status == 200

        # ── 3. Switch the graph backend to the Lakehouse, zero-copy ─────────
        _step("switching the graph backend to Lakehouse (zero-copy: _data as a VIEW)")
        info = ok(
            post(
                "/domain/info",
                {"graph_backend": "databricks", "lakehouse_materialization": "view"},
            )
        )["info"]
        assert info.get("graph_backend") == "databricks", info
        assert info.get("lakehouse_materialization") == "view", info

        save_to_registry()

        build_info = ok(get("/dtwin/databricks-build/info"))
        assert build_info.get("triple_store_backend") == "databricks", build_info
        assert build_info.get("materialization") == "view", build_info
        _step(
            f"Lakehouse backend active — data relation {build_info.get('data_table')} "
            f"(materialization={build_info.get('materialization')})"
        )

        # ── 4. Build the knowledge graph through the Lakehouse build ────────
        _step("building the knowledge graph (POST /dtwin/databricks-build/start)")
        build_start = ok(post("/dtwin/databricks-build/start", timeout=120_000))
        build_task = poll_task(
            page, base, build_start["task_id"], _BUILD_TIMEOUT_S, "Lakehouse build", step=_step
        )
        assert build_task.get("status") == "completed", (
            f"Lakehouse build did not complete: status={build_task.get('status')}, "
            f"error={build_task.get('error')}"
        )

        # The probe must see the VIEW: DESCRIBE DETAIL rejects views, which once
        # made a zero-copy _data look missing right after a successful build.
        status = ok(get("/dtwin/databricks-build/info")).get("triplestore_status") or {}
        assert status.get("exists") and status.get("has_data"), status
        assert not status.get("error"), status
        data_table = build_info["data_table"]
        _step(f"build done — {data_table} reported present with data")

        # The app's status count is a LIMIT 1 probe on purpose (no scan of a
        # live mapping view), so read the view directly from this process.
        # Subjects follow the domain's minted base URI, so resolve them by label.
        def subjects_labelled(label: str) -> list[str]:
            rows = _scratch_objects.execute_query(
                f"SELECT subject FROM {data_table} "
                f"WHERE predicate = '{_RDFS_LABEL}' AND object = '{label}'"
            )
            return [r["subject"] for r in rows or []]

        ada_subjects = subjects_labelled("Ada Lovelace")
        alan_subjects = subjects_labelled("Alan Turing")
        assert len(ada_subjects) == 1 and len(alan_subjects) == 1, (ada_subjects, alan_subjects)
        ada, alan = ada_subjects[0], alan_subjects[0]
        assert ada.endswith("/Customer/1") and alan.endswith("/Customer/2"), (ada, alan)
        places = _scratch_objects.execute_query(
            f"SELECT object FROM {data_table} WHERE subject = '{ada}' "
            "AND (predicate LIKE '%#places' OR predicate LIKE '%/places')"
        )
        assert places, f"no places triple for {ada}: the relationship mapping produced nothing"
        _step(f"graph holds {ada} (with places → {places[0]['object']}) and {alan}")

        # ── 5. Zero-copy proof: new UC rows appear without a rebuild ────────
        assert not subjects_labelled("Grace Hopper"), "Grace Hopper must not exist yet"
        _step("inserting Customer/3 (Grace Hopper) directly in UC — no rebuild")
        _scratch_objects.execute_statement(
            f"INSERT INTO {_FQ_CUSTOMERS} VALUES (3, 'Grace Hopper', 'grace@example.com')"
        )
        grace = subjects_labelled("Grace Hopper")
        assert grace, (
            f"{data_table} did not follow the source table: a zero-copy VIEW "
            "replays the mapping against the live Delta data"
        )
        _step(f"zero-copy confirmed — {grace[0]} is in _data with no rebuild")

        # ── 6. UC functions are eligible in the class pickers ───────────────
        fns = ok(get(f"/settings/uc-functions?catalog={_CATALOG}&schema={_SCHEMA}"))
        by_name = {f.get("name"): f for f in fns.get("functions", []) or []}
        for fn in (_FN_PROFILE, _FN_FLAG):
            assert fn in by_name, f"{fn} not listed by /settings/uc-functions: {sorted(by_name)}"
            assert int(by_name[fn].get("param_count") or 0) == 1, by_name[fn]
        assert by_name[_FN_PROFILE].get("returns_table") is True, by_name[_FN_PROFILE]
        _step("both scratch functions listed with exactly one parameter (picker-eligible)")

        # ── 7. Declare the SWRL rule, then bind everything to Customer ──────
        _step(f"adding SWRL rule {_RULE_NAME}: Customer(?c) ^ places(?c, ?o) -> ActiveCustomer(?c)")
        ok(
            post(
                "/ontology/swrl/save",
                {
                    "rule": {
                        "name": _RULE_NAME,
                        "description": "A customer who placed an order is active",
                        "antecedent": "Customer(?c) ^ places(?c, ?o)",
                        "consequent": "ActiveCustomer(?c)",
                        "enabled": True,
                    },
                    "index": -1,
                },
            )
        )

        _step("declaring the virtual attributes, UC action and business rule on Customer")
        ok(
            post(
                "/ontology/class/update",
                {
                    "uri": classes["Customer"]["uri"],
                    "virtualAttributes": [
                        {
                            "catalog": _CATALOG,
                            "schema": _SCHEMA,
                            "function": _FN_PROFILE,
                            "fullName": _FQ_FN_PROFILE,
                            "description": "Display name and its length",
                            "returns_table": True,
                            "attributes": [
                                {
                                    "name": "display_name",
                                    "column": "display_name",
                                    "label": "Display name",
                                    "dataType": "string",
                                },
                                {
                                    "name": "name_length",
                                    "column": "name_length",
                                    "label": "Name length",
                                    "dataType": "int",
                                },
                            ],
                        }
                    ],
                    "actions": [
                        {
                            "catalog": _CATALOG,
                            "schema": _SCHEMA,
                            "function": _FN_FLAG,
                            "fullName": _FQ_FN_FLAG,
                            "description": "Flag this customer",
                            "returns_table": False,
                        }
                    ],
                    "business_rules": [{"name": _RULE_NAME}],
                },
            )
        )
        save_to_registry()

        ctx = ok(get(f"/dtwin/nodes/context?entity_uri={quote(ada, safe='')}"))
        assert ctx.get("class_name") == "Customer", ctx
        assert [a.get("fullName") for a in ctx.get("actions") or []] == [_FQ_FN_FLAG], ctx
        assert [r.get("name") for r in ctx.get("business_rules") or []] == [_RULE_NAME], ctx
        va_decl = ctx.get("virtual_attributes") or []
        assert [g.get("fullName") for g in va_decl] == [_FQ_FN_PROFILE], ctx
        _step("node context exposes the virtual attributes, the UC action and the business rule")

        # ── 8. Compute the virtual attributes ───────────────────────────────
        _step("computing the virtual attributes of Customer/1")
        va = ok(get(f"/dtwin/nodes/virtual-attributes?entity_uri={quote(ada, safe='')}"))
        groups = va.get("virtual_attributes") or []
        assert len(groups) == 1 and not groups[0].get("error"), groups
        values = groups[0].get("values") or {}
        assert values.get("display_name") == "ADA LOVELACE", values
        assert int(values.get("name_length")) == len("Ada Lovelace"), values
        _step(f"virtual attributes computed: {values}")

        # ── 9. Invoke the UC function action (request → confirm) ────────────
        _step(f"requesting the UC action {_FN_FLAG} on Customer/1")
        pending = ok(
            post("/dtwin/nodes/action/request", {"entity_uri": ada, "action_full_name": _FQ_FN_FLAG})
        )["pending_action"]
        token = pending["token"]
        result = ok(post("/dtwin/nodes/action/confirm", {"token": token}, timeout=120_000))
        rows = result.get("rows") or []
        assert rows and rows[0].get("result") == "flagged:1", result
        _step(f"UC action ran: {rows[0]}")

        replay = post("/dtwin/nodes/action/confirm", {"token": token})
        assert _json(replay).get("success") is not True, (
            f"a consumed action token must not run twice: {replay.text()}"
        )
        _step("replaying the consumed token is refused (one-time confirmation)")

        # ── 10. Trigger the SWRL business rule on one entity ────────────────
        def run_rule(entity_uri: str) -> dict:
            pending_rule = ok(
                post("/dtwin/nodes/business-rule/request", {"entity_uri": entity_uri, "rule": _RULE_NAME})
            )["pending_business_rule"]
            return ok(
                post(
                    "/dtwin/nodes/business-rule/confirm",
                    {"token": pending_rule["token"]},
                    timeout=180_000,
                )
            )

        _step(f"triggering {_RULE_NAME} on Customer/1 (has an order)")
        ada_run = run_rule(ada)
        assert ada_run.get("inferred_count", 0) >= 1, ada_run
        assert ada_run.get("materialized_count", 0) >= 1, ada_run
        inferred = ada_run.get("triples") or []
        assert any(
            t.get("subject") == ada and str(t.get("object", "")).endswith("ActiveCustomer")
            for t in inferred
        ), inferred
        _step(
            f"rule inferred {ada_run['inferred_count']} triple(s), wrote "
            f"{ada_run['materialized_count']} to the graph"
        )

        _step(f"triggering {_RULE_NAME} on Customer/2 (no order)")
        alan_run = run_rule(alan)
        assert alan_run.get("inferred_count", 0) == 0, alan_run
        _step("nothing inferred for Customer/2 — the rule is scoped to the focus entity")

        inferred_status = ok(get("/dtwin/reasoning/inferred"))
        if inferred_status.get("materialized_inference_count") is not None:
            assert inferred_status["materialized_inference_count"] >= 1, inferred_status
        _step(
            "graph holds the materialized inference "
            f"(count={inferred_status.get('materialized_inference_count')})"
        )

        # ── 11. Leave the domain for manual inspection ──────────────────────
        save_to_registry()
        _step(
            f"DONE — open {base}, load '{_DOMAIN_NAME}' and inspect Customer/1 in the "
            "Graph Explorer (virtual attributes, actions, business rule)."
        )
