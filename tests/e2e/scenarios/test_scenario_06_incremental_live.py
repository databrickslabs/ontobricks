"""
E2E (LIVE) — ``ZzIncrementalLive``: the incremental materialization journey on a
deployed app, deterministic (no LLM).

Creates a Lakehouse-engine domain in ``incremental`` mode over a row-tracked
source table, saves a one-class ontology and a row-local mapping, regenerates
the R2RML, then exercises what the design's readiness / status / observability
rows promise through the app itself:

    1. ``GET /dtwin/incremental/readiness`` — the requested mode, the decision,
       the row-tracking probe per source, the per-mapping class (``slot`` /
       ``batch``), the Lakehouse//RT facts and the read relation
    2. (optional) a second, untracked source: ``row_tracking: false`` in the
       report, then ``POST /dtwin/incremental/enable-row-tracking`` flips it
    3. the rendered Domain page with the readiness card (screenshot)
    4. ``POST /dtwin/sync/start`` — the incremental build (slot pipeline +
       ``_data`` view) — polled to completion
    5. ``GET /dtwin/incremental/status`` and ``GET /domain/build-runs`` — the
       pipeline state, the relations, the build-run trace facts
       (``materialization_mode_effective``, ``refresh_technique``, ``slots_changed``)
    6. ``GET /dtwin/incremental/overview`` (app admin) — domains per mode
    7. the rendered Query page with the status card (screenshot)

The domain is left in the registry for inspection. Gated behind
``ONTOBRICKS_SCENARIO_LIVE=1``; mutates the target app's registry and bills a
warehouse and a Lakeflow pipeline.

Run (against a deployed app):
    ONTOBRICKS_SCENARIO_LIVE=1 ONTOBRICKS_LIVE_BASE=https://<app-url> \\
    DATABRICKS_CONFIG_PROFILE=<profile> ONTOBRICKS_E2E_PROFILE=<profile> \\
    ONTOBRICKS_SCENARIO_CATALOG=<catalog> ONTOBRICKS_SCENARIO_SCHEMA=<schema> \\
    ONTOBRICKS_SCENARIO6_TABLE=customers ONTOBRICKS_SCENARIO6_UNTRACKED=untracked \\
    uv run --frozen pytest tests/e2e/scenarios/test_scenario_06_incremental_live.py -m scenario -v -s --no-cov

Inputs via env:
    ONTOBRICKS_SCENARIO_CATALOG / ONTOBRICKS_SCENARIO_SCHEMA   the source tables' schema
    ONTOBRICKS_SCENARIO6_TABLE      a row-tracked table with ``id`` / ``name`` columns (default ``customers``)
    ONTOBRICKS_SCENARIO6_UNTRACKED  a table without row tracking, same columns (default: skip step 2)
    ONTOBRICKS_SCENARIO6_BUILD_TIMEOUT  seconds to wait for the build (default 1200)
"""

from __future__ import annotations

import json
import os
import pathlib

import pytest

from tests.e2e.scenarios._harness import (
    chain_marker,
    csrf_headers,
    json_body,
    make_step,
    poll_task,
    registry_domain_keys,
)

pytestmark = [
    pytest.mark.scenario,
    pytest.mark.skipif(
        os.environ.get("ONTOBRICKS_SCENARIO_LIVE") != "1",
        reason="live scenario — set ONTOBRICKS_SCENARIO_LIVE=1 to run",
    ),
    *chain_marker("scenario_6", depends=("scenario_0",)),
]

_DOMAIN = "ZzIncrementalLive"
_CATALOG = os.environ.get("ONTOBRICKS_SCENARIO_CATALOG", "benoit_cayla")
_SCHEMA = os.environ.get("ONTOBRICKS_SCENARIO_SCHEMA", "customer")
_TABLE = os.environ.get("ONTOBRICKS_SCENARIO6_TABLE", "customers")
_UNTRACKED = os.environ.get("ONTOBRICKS_SCENARIO6_UNTRACKED", "")
_BUILD_TIMEOUT = int(os.environ.get("ONTOBRICKS_SCENARIO6_BUILD_TIMEOUT", "1200"))
_BASE_URI = f"http://ontobricks.ai/{_DOMAIN}#"
_ARTIFACTS = pathlib.Path("artifacts/scenarios")

_step = make_step("scenario_6")


def _entity(class_name: str, table: str) -> dict:
    """A row-local entity mapping: a table (no ``sql_query``), id + label columns."""
    return {
        "ontology_class": f"{_BASE_URI}{class_name}",
        "ontology_class_label": class_name,
        "catalog": _CATALOG,
        "schema": _SCHEMA,
        "table": table,
        "id_column": "id",
        "label_column": "name",
        "attribute_mappings": {},
    }


class TestScenario6IncrementalLive:
    def test_incremental_readiness_build_status_and_overview(self, scenario_page, scenario_base):
        page, base = scenario_page, scenario_base
        _ARTIFACTS.mkdir(parents=True, exist_ok=True)

        _step(f"priming session at {base}")
        page.goto(base)
        page.wait_for_load_state("domcontentloaded")
        headers = csrf_headers(page.context)

        _step(f"removing a leftover '{_DOMAIN}' and resetting the session")
        if _DOMAIN.lower() in registry_domain_keys(page, base):
            resp = page.context.request.delete(  # by the folder key the listing returns
                f"{base}/settings/registry/domains/{_DOMAIN.lower()}", headers=headers, timeout=60_000
            )
            assert resp.status in (200, 204, 404), resp.text()
            for _ in range(10):  # the registry row and the folder go separately
                if _DOMAIN.lower() not in registry_domain_keys(page, base):
                    break
                page.wait_for_timeout(2000)
            assert _DOMAIN.lower() not in registry_domain_keys(page, base), "leftover domain still listed"
        assert page.context.request.post(f"{base}/domain/reset", headers=headers).status == 200
        resp = page.context.request.post(
            f"{base}/domain/info",
            headers=headers,
            data=json.dumps(
                {
                    "name": _DOMAIN,
                    "description": "Live scenario: incremental materialization journey",
                    "base_uri": _BASE_URI,
                    "graph_backend": "databricks",
                    "materialization_mode": "incremental",
                }
            ),
        )
        assert resp.status == 200, resp.text()
        body = json_body(resp)
        assert "info" in body, resp.text()[:400]  # an HTML body here is an access-denied page
        info = body["info"]
        assert info["materialization_mode"] == "incremental", info
        _step(f"domain info saved (mode={info['materialization_mode']}, backend={info.get('graph_backend')})")

        _step("claiming the registry folder (initial save-to-uc)")
        resp = page.context.request.post(f"{base}/domain/save-to-uc", headers=headers, timeout=120_000)
        assert resp.status == 200 and json_body(resp).get("success") is True, resp.text()

        tables = [_TABLE] + ([_UNTRACKED] if _UNTRACKED else [])
        _step(f"importing {_CATALOG}.{_SCHEMA} {tables} (warehouse may cold-start)")
        resp = page.context.request.post(
            f"{base}/domain/metadata/initialize",
            headers=headers,
            data=json.dumps({"catalog": _CATALOG, "schema": _SCHEMA, "selected_tables": tables}),
            timeout=180_000,
        )
        assert resp.status == 200 and json_body(resp).get("success") is True, resp.text()

        _step("saving the ontology (Customer, Account) and the row-local mappings")
        classes = [{"name": "Customer", "label": "Customer"}]
        entities = [_entity("Customer", _TABLE)]
        if _UNTRACKED:
            classes.append({"name": "Account", "label": "Account"})
            entities.append(_entity("Account", _UNTRACKED))
        resp = page.context.request.post(
            f"{base}/ontology/save",
            headers=headers,
            data=json.dumps({"name": _DOMAIN, "base_uri": _BASE_URI, "classes": classes, "properties": []}),
        )
        assert resp.status == 200, resp.text()
        resp = page.context.request.post(
            f"{base}/mapping/save",
            headers=headers,
            data=json.dumps({"config": {"entities": entities, "relationships": []}}),
        )
        assert resp.status == 200 and json_body(resp).get("success") is True, resp.text()
        loaded = json_body(page.request.get(f"{base}/mapping/load"))
        saved_entities = (loaded.get("config") or loaded).get("entities") or []
        assert len(saved_entities) == len(entities), loaded
        resp = page.context.request.post(f"{base}/mapping/generate", headers=headers, timeout=120_000)
        assert resp.status == 200 and json_body(resp).get("success") is True, resp.text()
        r2rml = json_body(resp).get("r2rml") or ""
        tracked_fqn = f"{_CATALOG}.{_SCHEMA}.{_TABLE}".lower()
        assert "TriplesMap" in r2rml and tracked_fqn in r2rml.lower(), r2rml[:800]
        resp = page.context.request.post(f"{base}/domain/save-to-uc", headers=headers, timeout=120_000)
        assert resp.status == 200 and json_body(resp).get("success") is True, resp.text()

        _step("GET /dtwin/incremental/readiness")
        readiness = json_body(page.request.get(f"{base}/dtwin/incremental/readiness", timeout=180_000))
        print(json.dumps(readiness, indent=1, default=str)[:3000])
        assert readiness["mode_requested"] == "incremental"
        sources = {s["table"].lower(): s["row_tracking"] for s in readiness["sources"]}
        assert sources.get(tracked_fqn) is True, sources
        kinds = {m["class_uri"]: m["kind"] for m in readiness["mappings"]}
        assert kinds.get(f"{_BASE_URI}Customer") == "slot", kinds
        assert readiness["decision"]["effective"] in ("incremental", "full"), readiness["decision"]
        assert readiness["read_relation"] == "graph"
        assert set(readiness["lakehouse_rt"]) == {"query_warehouse_configured", "use_sea", "fast_path"}

        if _UNTRACKED:
            untracked_fqn = f"{_CATALOG}.{_SCHEMA}.{_UNTRACKED}".lower()
            assert sources.get(untracked_fqn) is False, sources
            assert readiness["decision"]["effective"] == "full", readiness["decision"]
            _step(f"POST /dtwin/incremental/enable-row-tracking on {untracked_fqn}")
            resp = page.context.request.post(
                f"{base}/dtwin/incremental/enable-row-tracking",
                headers=headers,
                data=json.dumps({"tables": [untracked_fqn]}),
                timeout=180_000,
            )
            assert resp.status == 200, resp.text()
            enabled = json_body(resp)
            print(json.dumps(enabled, indent=1, default=str)[:800])
            assert all(t.get("success") for t in enabled.get("tables", [])), enabled
            readiness = json_body(page.request.get(f"{base}/dtwin/incremental/readiness", timeout=180_000))
            sources = {s["table"].lower(): s["row_tracking"] for s in readiness["sources"]}
            assert sources.get(untracked_fqn) is True, sources
            assert readiness["decision"]["effective"] == "incremental", readiness["decision"]

        _step("rendered Domain page › Backend tab with the readiness card")
        page.goto(f"{base}/domain")
        page.wait_for_load_state("domcontentloaded")
        page.click("#tab-triplestore")  # the card lives on the Backend tab; the report loads on page init
        mappings_line = page.locator("#materializationReadinessMappings")
        mappings_line.wait_for(state="visible", timeout=180_000)
        card_text = (page.locator("#materializationReadiness").text_content() or "").strip()
        print("readiness card:", " ".join(card_text.split())[:600])
        assert "incremental runs as" in card_text, card_text
        assert "row-local" in (mappings_line.text_content() or ""), card_text
        card = page.locator("#materializationReadiness")
        card.scroll_into_view_if_needed()  # the content pane scrolls, not the document
        page.screenshot(path=str(_ARTIFACTS / "scenario06_domain_readiness.png"))
        card.screenshot(path=str(_ARTIFACTS / "scenario06_domain_readiness_card.png"))

        _step("POST /dtwin/sync/start — incremental build")
        resp = page.context.request.post(f"{base}/dtwin/sync/start", headers=headers, data=json.dumps({}))
        assert resp.status == 200, resp.text()
        started = json_body(resp)
        task_id = started.get("task_id") or (started.get("task") or {}).get("id")
        assert task_id, started
        task = poll_task(page, base, task_id, _BUILD_TIMEOUT, "build", step=_step)
        print("build task:", json.dumps({k: task.get(k) for k in ("status", "progress", "message", "error")}, default=str)[:600])
        assert task.get("status") == "completed", task

        _step("GET /dtwin/incremental/status + /domain/build-runs")
        status = json_body(page.request.get(f"{base}/dtwin/incremental/status", timeout=180_000))
        print(json.dumps(status, indent=1, default=str)[:3000])
        assert status["mode_requested"] == "incremental"
        assert status["data"]["kind"] in ("mv", "table"), status["data"]  # the MV, or the MERGE fallback table
        assert status["slots"]["kind"] == "streaming_table", status["slots"]
        assert (status.get("slot_pipeline") or {}).get("name"), status.get("slot_pipeline")
        runs = json_body(page.request.get(f"{base}/domain/build-runs"))
        facts = ((runs.get("runs") or runs.get("build_runs") or [{}])[0].get("stats") or {}).get("materialization") or {}
        print("last build facts:", json.dumps(facts, default=str)[:1200])
        assert facts.get("materialization_mode_requested") == "incremental", facts

        _step("GET /dtwin/incremental/overview")
        resp = page.request.get(f"{base}/dtwin/incremental/overview")
        print("overview ->", resp.status, resp.text()[:600])
        if resp.status == 200:
            overview = json_body(resp)
            mine = [d for d in overview.get("domains", []) if d["name"].lower() == _DOMAIN.lower()]
            assert mine and mine[0]["mode_requested"] == "incremental", overview  # listed by folder

        _step("rendered Query page › Build section with the status card")
        page.goto(f"{base}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        page.click('.sidebar-nav .nav-link[data-section="sync"]')  # the page lands on Explorer
        status_card = page.locator("#sync-databricks-panel [data-incremental-status]")
        status_card.wait_for(state="visible", timeout=180_000)
        status_text = " ".join((status_card.text_content() or "").split())
        print("status card:", status_text[:600])
        assert "Slot pipeline:" in status_text and "_data:" in status_text, status_text
        status_card.scroll_into_view_if_needed()
        page.screenshot(path=str(_ARTIFACTS / "scenario06_query_status.png"))
        status_card.screenshot(path=str(_ARTIFACTS / "scenario06_query_status_card.png"))
        resp = page.context.request.post(f"{base}/domain/save-to-uc", headers=headers, timeout=120_000)
        assert resp.status == 200, resp.text()
        _step("done — domain left in the registry for inspection")
