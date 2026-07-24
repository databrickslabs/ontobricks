"""
E2E (LIVE) — ``test_scenario_1``: import data sources → generate the OWL from
the **Generate page** (LLM wizard) → persist the domain to the registry.

Unlike :mod:`test_full_lifecycle` (which is deterministic, session-only and
CI-safe), this journey is deliberately *live*: it needs a real SQL warehouse
(to read Unity Catalog table metadata), a real LLM serving endpoint (the
``Generate`` wizard runs ``agent_owl_generator`` asynchronously), and it
**writes a durable domain** into the registry so it can be inspected by hand
afterwards.

It therefore runs against an **already-running OntoBricks instance** — by
default the local dev server on ``http://localhost:8000`` (``scripts/start.sh``)
so the persisted ``test_scenario_1`` domain lands in the *same* registry the
local app reads, and shows up immediately when you open the app and browse the
registry. Point it elsewhere with ``ONTOBRICKS_LIVE_BASE``.

The journey:

    1. prime the session (sets the CSRF cookie)
    2. reset to a clean slate, then create domain ``test_scenario_1`` with the
       Claude serving endpoint attached (so the wizard has an LLM)
    3. import the data sources (``benoit_cayla.customer`` tables) — *beforehand*,
       so the Generate page can see them
    4. open the real **Generate** page, confirm the imported tables render, and
       click the real Generate button → the wizard runs the async LLM task and
       auto-applies the resulting OWL to the session
    5. poll until the generated ontology lands in the session
    6. run **Auto-Map** from the real Mapping page (the AI mapping wizard that
       generates the SQL queries + column mappings for every entity/relationship)
    7. save the domain to the registry (clean create — must precede the build,
       which records itself against the registry domain)
    8. **Build the knowledge graph** (POST /dtwin/sync/start → CREATE VIEW +
       populate the graph store) and poll the task to completion
    9. re-save to capture the build metadata, then verify it is listed

The domain is intentionally **NOT** reset/deleted at the end — that is the
whole point: open the app afterwards and load ``test_scenario_1``.

Gated behind ``ONTOBRICKS_SCENARIO_LIVE=1`` so it never runs in the default
matrix (it costs warehouse + LLM time and mutates the registry).

Run (against the local dev server):

    ONTOBRICKS_SCENARIO_LIVE=1 \\
    uv run pytest tests/e2e/scenarios/test_scenario_01_generate_live.py \\
        -v -s --no-cov

Override the target / inputs via env:
    ONTOBRICKS_LIVE_BASE          base URL (default http://localhost:8000)
    ONTOBRICKS_SCENARIO_CATALOG   data-source catalog (default benoit_cayla)
    ONTOBRICKS_SCENARIO_SCHEMA    data-source schema  (default customer)
    ONTOBRICKS_SCENARIO_LLM       serving endpoint    (default databricks-claude-sonnet-4-5)
    ONTOBRICKS_SCENARIO_GEN_TIMEOUT      max seconds to wait for OWL generation (default 420)
    ONTOBRICKS_SCENARIO_AUTOMAP_TIMEOUT  max seconds to wait for Auto-Map (default 600)
    ONTOBRICKS_SCENARIO_BUILD_TIMEOUT    max seconds to wait for the KG build (default 420)
"""

from __future__ import annotations

import json
import os
import time

import pytest

from tests.e2e.scenarios._harness import (
    chain_marker,
    csrf_headers,
    json_body,
    make_step,
    poll_task,
)


# ── Gate: this is a live, mutating, billable journey ─────────────────────────
# skipif gates the whole suite off unless ONTOBRICKS_SCENARIO_LIVE=1; the
# chain_marker adds campaign ordering only when ONTOBRICKS_SCENARIO_CHAIN=1.
pytestmark = [
    pytest.mark.skipif(
        os.environ.get("ONTOBRICKS_SCENARIO_LIVE") != "1",
        reason="live scenario — set ONTOBRICKS_SCENARIO_LIVE=1 to run "
        "(needs a running app + warehouse + LLM; writes a durable domain)",
    ),
    *chain_marker("scenario_1"),
]


_DOMAIN_NAME = "test_scenario_1"
_CATALOG = os.environ.get("ONTOBRICKS_SCENARIO_CATALOG", "benoit_cayla")
_SCHEMA = os.environ.get("ONTOBRICKS_SCENARIO_SCHEMA", "customer")
_LLM_ENDPOINT = os.environ.get("ONTOBRICKS_SCENARIO_LLM", "databricks-claude-sonnet-4-5")
_GEN_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO_GEN_TIMEOUT", "420"))
_AUTOMAP_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO_AUTOMAP_TIMEOUT", "600"))
_BUILD_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO_BUILD_TIMEOUT", "420"))

# Base tables in benoit_cayla.customer (views excluded — they add noise and the
# wizard works best off concrete tables). Override the schema via env if your
# layout differs.
_SELECTED_TABLES = [
    "call",
    "claim",
    "contract",
    "customer",
    "interaction",
    "invoice",
    "meter",
    "meter_reading",
    "payment",
    "subscription",
]


# URL / CSRF / JSON / task-poll helpers and the ``scenario_base`` /
# ``scenario_page`` fixtures are shared — see ``_harness.py`` + ``conftest.py``.
# The short local names are kept so the journey below reads identically across
# every scenario suite.
_csrf_headers = csrf_headers
_json = json_body
_step = make_step("scenario_1")


def _poll_task(page, base: str, task_id: str, timeout_s: int, label: str) -> dict:
    return poll_task(page, base, task_id, timeout_s, label, step=_step)


class TestScenario1GenerateLive:
    """Import data sources → generate OWL → Auto-Map → build graph → persist."""

    def test_import_generate_automap_build_and_persist(self, scenario_page, scenario_base):
        page = scenario_page
        base = scenario_base

        # ── 1. Prime the session (GET sets the csrf cookie) ──────────────────
        _step(f"priming session at {base}")
        page.goto(base)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)

        # ── 2. Clean slate: delete any prior 'test_scenario_1', then create it ─
        # Deleting an existing copy makes every run re-exercise the full
        # generate → Auto-Map → build pipeline and keeps the final save a clean
        # create (no name conflict). The domain is recreated below and persists
        # after the run, so manual inspection still works.
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

        if _DOMAIN_NAME in _registry_names():
            _step(f"'{_DOMAIN_NAME}' exists — deleting it for a clean rebuild")
            resp = page.context.request.delete(
                f"{base}/settings/registry/domains/{_DOMAIN_NAME}",
                headers=headers,
                timeout=60_000,
            )
            assert resp.status in (200, 204), resp.text()

        _step("resetting session for a fresh 'test_scenario_1'")
        resp = page.context.request.post(f"{base}/domain/reset", headers=headers)
        assert resp.status == 200, resp.text()

        resp = page.context.request.post(
            f"{base}/domain/info",
            headers=headers,
            data=json.dumps(
                {
                    "name": _DOMAIN_NAME,
                    "description": "Live scenario: data-source import + wizard OWL generation",
                    "llm_endpoint": _LLM_ENDPOINT,
                    "base_uri": f"http://ontobricks.ai/{_DOMAIN_NAME}#",
                }
            ),
        )
        assert resp.status == 200, resp.text()
        info = _json(resp)["info"]
        assert info["name"] == _DOMAIN_NAME
        assert info["llm_endpoint"] == _LLM_ENDPOINT
        _step(f"domain info saved (llm_endpoint={info['llm_endpoint']})")

        # ── 2b. Claim the registry folder up-front (clean create) ───────────
        # The async Auto-Map and Build tasks persist their domain *snapshot* to
        # the registry on a background thread, which would otherwise create the
        # folder without the live session owning its ``domain_folder`` — making
        # the later save a name conflict. Saving now (registry is empty right
        # after the delete) claims the folder so every subsequent save (and the
        # task snapshots) overwrites it instead of conflicting.
        _step("claiming the registry folder (initial save-to-uc)")
        resp = page.context.request.post(
            f"{base}/domain/save-to-uc",
            headers=_csrf_headers(page.context),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        assert _json(resp).get("success") is True, resp.text()

        # ── 3. Import the data sources BEFOREHAND (UC metadata read) ─────────
        _step(
            f"importing data sources {_CATALOG}.{_SCHEMA} "
            f"({len(_SELECTED_TABLES)} tables) — warehouse may cold-start"
        )
        resp = page.context.request.post(
            f"{base}/domain/metadata/initialize",
            headers=headers,
            data=json.dumps(
                {
                    "catalog": _CATALOG,
                    "schema": _SCHEMA,
                    "selected_tables": _SELECTED_TABLES,
                }
            ),
            timeout=120_000,  # warehouse wake-up can take ~30s
        )
        assert resp.status == 200, resp.text()
        meta_result = _json(resp)
        assert meta_result.get("success") is True, meta_result
        imported = meta_result.get("metadata", {}).get("tables", [])
        assert len(imported) >= 1, meta_result
        _step(f"imported {len(imported)} tables into the session metadata")

        # Truth check via the same endpoint the wizard reads.
        md = _json(page.request.get(f"{base}/domain/metadata"))
        md_tables = {
            (t.get("full_name") or t.get("name")) for t in md["metadata"]["tables"]
        }
        assert any(t and t.endswith(".customer") or t == "customer" for t in md_tables), (
            md_tables
        )

        # ── 4. Open the real Generate page and confirm the tables render ─────
        _step("opening the Generate (wizard) page")
        page.goto(f"{base}/ontology?section=wizard")
        page.wait_for_load_state("domcontentloaded")
        # The section link auto-clicks ~200ms after load → initOntologyWizard()
        # → loadWizardMetadata() populates the table picker. Wait for a row.
        page.wait_for_selector(".wizard-table-checkbox", timeout=30_000)
        rows = page.locator(".wizard-table-checkbox").count()
        assert rows >= 1, "wizard did not render the imported data-source tables"
        _step(f"wizard shows {rows} selectable data-source tables")

        gen_btn = page.locator("#wizardTopGenerateBtn")
        page.wait_for_function(
            "() => { const b = document.getElementById('wizardTopGenerateBtn');"
            " return b && !b.disabled; }",
            timeout=30_000,
        )

        # ── 5. Click the real Generate button → async LLM wizard ─────────────
        _step("clicking Generate → running the LLM wizard (this can take minutes)")
        gen_btn.click()

        # The wizard polls /tasks/<id> in-page and, on success, auto-applies the
        # OWL to the session via /ontology/parse-owl. We poll the SESSION truth
        # (/ontology/load) so we're decoupled from front-end timing.
        deadline = time.monotonic() + _GEN_TIMEOUT_S
        classes: list = []
        last_log = 0.0
        while time.monotonic() < deadline:
            page.wait_for_timeout(3000)
            try:
                onto = _json(page.request.get(f"{base}/ontology/load")).get("config", {})
            except Exception:  # noqa: BLE001 — transient during generation
                onto = {}
            classes = onto.get("classes", []) or []
            if classes:
                break
            now = time.monotonic()
            if now - last_log > 20:
                last_log = now
                remaining = int(deadline - now)
                _step(f"  …still generating (no classes yet, {remaining}s left)")

        assert classes, (
            f"Wizard did not produce an ontology within {_GEN_TIMEOUT_S}s. "
            "Check the serving endpoint / warehouse, or raise "
            "ONTOBRICKS_SCENARIO_GEN_TIMEOUT."
        )
        class_names = sorted({c.get("name", "") for c in classes})
        _step(f"generated ontology with {len(classes)} classes: {class_names[:12]}")

        # ── 6. Auto-Map: generate SQL + column mappings from the real UI ─────
        _step("opening the Mapping page and waiting for the ontology to hydrate")
        page.goto(f"{base}/mapping?section=autoassign")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_function(
            "() => typeof MappingState !== 'undefined' && MappingState.initialized"
            " && MappingState.loadedOntology"
            " && (MappingState.loadedOntology.classes || []).length > 0"
            " && typeof AutoAssignModule !== 'undefined'",
            timeout=60_000,
        )
        # Let AutoAssignModule.init() (triggered by the section nav) hydrate its
        # mapping config so start() sees the unassigned entities.
        page.wait_for_timeout(1500)

        _step("starting Auto-Map → AI generates SQL + column mappings (can take minutes)")
        # Invoke the module directly rather than clicking the button: the button
        # re-renders its label/gauges on every status tick, so a DOM click races
        # with "element not stable". start() is the exact handler the button calls.
        page.evaluate("() => AutoAssignModule.start()")

        # Poll the session mapping config until entities carry generated SQL.
        deadline = time.monotonic() + _AUTOMAP_TIMEOUT_S
        mapped = 0
        last_log = 0.0
        while time.monotonic() < deadline:
            page.wait_for_timeout(3000)
            try:
                cfg = _json(page.request.get(f"{base}/mapping/load")).get("config", {})
            except Exception:  # noqa: BLE001
                cfg = {}
            ents = cfg.get("entities", []) or []
            mapped = sum(1 for m in ents if m.get("sql_query"))
            if mapped > 0:
                # Give the task a moment to flush the remaining items, then stop
                # once it's no longer running in the page.
                still_running = page.evaluate(
                    "() => typeof AutoAssignModule !== 'undefined' && AutoAssignModule.isRunning"
                )
                if not still_running:
                    break
            now = time.monotonic()
            if now - last_log > 20:
                last_log = now
                _step(f"  …auto-mapping ({mapped} entities mapped, {int(deadline - now)}s left)")

        assert mapped > 0, (
            f"Auto-Map produced no entity SQL within {_AUTOMAP_TIMEOUT_S}s. "
            "Check the serving endpoint / warehouse or raise "
            "ONTOBRICKS_SCENARIO_AUTOMAP_TIMEOUT."
        )
        mapping_cfg = _json(page.request.get(f"{base}/mapping/load")).get("config", {})
        n_ent = sum(1 for m in mapping_cfg.get("entities", []) if m.get("sql_query"))
        n_rel = sum(1 for m in mapping_cfg.get("relationships", []) if m.get("sql_query"))
        _step(f"Auto-Map done — {n_ent} entity + {n_rel} relationship mappings with SQL")

        # ── 7. Persist the generated ontology + mappings BEFORE building ─────
        # Overwrites the folder claimed in step 2b so the build (which records
        # itself against the registry domain) runs against an up-to-date copy.
        _step("saving the domain to the registry (ontology + mappings, pre-build)")
        resp = page.context.request.post(
            f"{base}/domain/save-to-uc",
            headers=_csrf_headers(page.context),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        assert _json(resp).get("success") is True, resp.text()

        # ── 8. Build the knowledge graph (CREATE VIEW + populate graph store) ─
        _step("starting the knowledge-graph build (POST /dtwin/sync/start)")
        resp = page.context.request.post(
            f"{base}/dtwin/sync/start",
            headers=_csrf_headers(page.context),
            data=json.dumps({}),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        build_start = _json(resp)
        assert build_start.get("success") is True, build_start
        build_task_id = build_start["task_id"]
        _step(f"build task started ({build_task_id}) — populating the graph")

        build_task = _poll_task(
            page, base, build_task_id, _BUILD_TIMEOUT_S, label="KG build"
        )
        assert build_task.get("status") == "completed", (
            f"KG build did not complete cleanly: status={build_task.get('status')}, "
            f"error={build_task.get('error')}"
        )
        _step(f"KG build completed: {build_task.get('message', 'done')}")

        # ── 9. Re-save to capture the build metadata, then verify listing ────
        # domain_folder is now owned by the session (set by the pre-build save),
        # so this overwrites V1 instead of conflicting.
        _step("re-saving the domain to capture build metadata (overwrite)")
        resp = page.context.request.post(
            f"{base}/domain/save-to-uc",
            headers=_csrf_headers(page.context),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        save_result = _json(resp)
        assert save_result.get("success") is True, save_result
        _step(f"save-to-uc result: {save_result.get('message', save_result)}")

        listed = _json(page.request.get(f"{base}/domain/list-projects"))

        def _entry_name(d) -> str:
            # Registry entries may be folder-name strings or {"name"/"folder": ...} dicts.
            if isinstance(d, str):
                return d
            return d.get("name") or d.get("folder") or ""

        names = {_entry_name(d) for d in listed.get("domains", [])}
        assert any((n or "").lower() == _DOMAIN_NAME for n in names), (
            f"{_DOMAIN_NAME!r} not found in registry listing: "
            f"{sorted(n for n in names if n)}"
        )
        _step(
            f"DONE — '{_DOMAIN_NAME}' generated, mapped, graph-built and persisted. "
            f"Open {base}, load it from the registry, and explore the knowledge graph."
        )
