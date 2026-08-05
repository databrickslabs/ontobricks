"""
E2E (LIVE) — ``test_scenario_3``: rules → quality → reasoning → analysis on the
domain produced by :mod:`test_scenario_01_generate_live` and versioned by
:mod:`test_scenario_02_collab_lifecycle`.

This journey **reuses the durable ``test_scenario_1`` domain** and operates on
the fresh DRAFT version scenario 2 branched off (V2). It exercises the
"governed enrichment" loop that sits on top of a built knowledge graph:

    1. load the highest DRAFT version from the registry (V2 after scenario 2;
       skips if no DRAFT version exists — scenario 2 is the prerequisite)
    2. ensure the knowledge graph for that version is built (a freshly branched
       version has no graph yet, so build + poll once; later re-runs reuse it)
    3. **generate data-quality rules** — auto-suggest SHACL shapes from the
       ontology and accept them into the domain
    4. **generate business rules** — run ``agent_business_rules_generator``
       (SWRL / decision tables / SPARQL / aggregate) and accept the proposals
    5. persist the enriched domain to the registry
    6. **run the data-quality rules on the knowledge graph**
       (``/dtwin/dataquality/start`` with ``backend=graph``) and poll
    7. **run the business rules** (``/dtwin/reasoning/start`` with every phase
       enabled), poll, then **materialise** the inferred triples back into the
       graph
    8. **run the Analysis** (``/dtwin/metrics/compute``) and **interpret** the
       result with the graph-interpreter agent (``/dtwin/metrics/interpret``)
    9. **add the interpreted result to the audit trail** — post a domain
       comment with the full write-up and create a task that anchors it, which
       appends a ``commented`` row to the review audit log; then verify it
       surfaces in ``/domain/audit-trail``
   10. re-save the domain so the rules + run metadata persist

Because comments/tasks are writable only while a version is DRAFT or IN-REVIEW
(and the audit-trail entry is written through that path), this scenario targets
the DRAFT V2 rather than the PUBLISHED V1 — and leaves the published V1
untouched.

Gated behind ``ONTOBRICKS_SCENARIO_LIVE=1`` (and the ``scenario`` marker) so it
never runs in the default matrix (it costs warehouse + LLM time and mutates the
registry).

Run (against the local dev server, after scenarios 1 and 2):

    ONTOBRICKS_SCENARIO_LIVE=1 \\
    uv run pytest tests/e2e/scenarios/test_scenario_03_rules_analysis.py \\
        -m scenario -v -s --no-cov

Override the target / timeouts via env:
    ONTOBRICKS_LIVE_BASE              base URL (default http://localhost:8000)
    ONTOBRICKS_SCENARIO_DOMAIN        reused domain folder (default test_scenario_1)
    ONTOBRICKS_SCENARIO_BUILD_TIMEOUT     max seconds for a KG build (default 420)
    ONTOBRICKS_SCENARIO_RULES_TIMEOUT     max seconds for business-rule generation (default 600)
    ONTOBRICKS_SCENARIO_DQ_TIMEOUT        max seconds for the data-quality run (default 300)
    ONTOBRICKS_SCENARIO_REASONING_TIMEOUT max seconds for the reasoning run (default 420)
    ONTOBRICKS_SCENARIO_ANALYSIS_TIMEOUT  max seconds for compute+interpret (default 300)
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


# ── Gate: live, mutating, billable journey against a real registry ───────────
pytestmark = [
    pytest.mark.skipif(
        os.environ.get("ONTOBRICKS_SCENARIO_LIVE") != "1",
        reason="live scenario — set ONTOBRICKS_SCENARIO_LIVE=1 to run "
        "(needs a running app + the test_scenario_1 domain from scenarios 1 & 2)",
    ),
    *chain_marker("scenario_3", depends=("scenario_2",)),
]


_DOMAIN_NAME = os.environ.get("ONTOBRICKS_SCENARIO_DOMAIN", "test_scenario_1")
_BUILD_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO_BUILD_TIMEOUT", "420"))
_RULES_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO_RULES_TIMEOUT", "600"))
_DQ_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO_DQ_TIMEOUT", "300"))
_REASONING_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO_REASONING_TIMEOUT", "420"))
_ANALYSIS_TIMEOUT_S = int(os.environ.get("ONTOBRICKS_SCENARIO_ANALYSIS_TIMEOUT", "300"))
_ANALYSIS_TIMEOUT_MS = _ANALYSIS_TIMEOUT_S * 1000


# URL / CSRF / JSON / task-poll helpers and the ``scenario_base`` /
# ``scenario_page`` fixtures are shared — see ``_harness.py`` + ``conftest.py``.
# The short local names are kept so the journey below reads identically across
# every scenario suite.
_csrf_headers = csrf_headers
_json = json_body
_step = make_step("scenario_3")


def _poll_task(page, base: str, task_id: str, timeout_s: int, label: str) -> dict:
    return poll_task(page, base, task_id, timeout_s, label, step=_step)


class TestScenario3RulesAnalysis:
    """Generate rules → run quality + reasoning → analyse → audit trail."""

    def test_rules_quality_reasoning_analysis_audit(self, scenario_page, scenario_base):
        page = scenario_page
        base = scenario_base

        def headers() -> dict:
            return _csrf_headers(page.context)

        # ── 1. Prime the session (GET sets the csrf cookie) ──────────────────
        _step(f"priming session at {base}")
        page.goto(base)
        page.wait_for_load_state("domcontentloaded")

        # ── 2. Prerequisite: the domain must exist in the registry ───────────
        try:
            registry = _json(page.request.get(f"{base}/domain/list-projects"))
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"Could not read the registry: {exc}")
        names = {
            (d if isinstance(d, str) else (d.get("name") or d.get("folder") or "")).lower()
            for d in registry.get("domains", []) or []
        }
        if _DOMAIN_NAME.lower() not in names:
            pytest.skip(
                f"'{_DOMAIN_NAME}' not in the registry — run scenario 1 (and 2) "
                "first to build, publish, and branch it."
            )

        # ── 3. Resolve a DRAFT version backed by a built knowledge graph ─────
        # The analysis write-up is appended to the audit trail through the
        # comment/task path, which is writable only on a DRAFT/IN-REVIEW
        # version — so the working version must be DRAFT. The data-quality,
        # reasoning, and analysis steps all run *on the knowledge graph*, so
        # that version must also have a built graph.
        #
        # A freshly branched version (scenario 2's create-version) has the
        # ontology + mappings but no graph data. This scenario always targets
        # that **DRAFT branch** (the highest DRAFT version, e.g. V2) and builds
        # its graph when missing — it never reopens or enriches the published
        # base version (V1). That keeps V1 exactly as scenario 2 published it
        # and guarantees the graph ends up built on the version *after* V1.
        # Building is expensive (managed-synced Lakebase triggers a Lakeflow
        # pipeline), so if the DRAFT branch already has a graph (a previous
        # scenario-3 run) we reuse it instead of rebuilding.
        #
        # ``/domain/versions-list`` reports versions for the *loaded* session
        # domain, so load the canonical base version (V1) first to enumerate.
        def _load_version(version: str) -> None:
            resp = page.context.request.post(
                f"{base}/domain/load-from-uc",
                headers=headers(),
                data=json.dumps({"domain": _DOMAIN_NAME, "version": version}),
                timeout=120_000,
            )
            assert resp.status == 200, resp.text()
            assert _json(resp).get("success") is True, resp.text()

        def _graph_built() -> bool:
            """True when the loaded version's graph table currently holds data."""
            status = _json(page.request.get(f"{base}/dtwin/sync/status"))
            return bool(status.get("has_data")) and int(status.get("count", 0) or 0) > 0

        def _version_status(version: str) -> str:
            detail = _json(page.request.get(f"{base}/review/{_DOMAIN_NAME}/{version}"))
            return str(detail.get("status", "")).upper()

        _step(f"loading {_DOMAIN_NAME} v1 from the registry (to enumerate versions)")
        _load_version("1")

        versions = _json(page.request.get(f"{base}/domain/versions-list")).get(
            "versions", []
        )
        version_ids = sorted(
            (int(str(v.get("version"))) for v in versions if str(v.get("version", "")).isdigit()),
            reverse=True,
        )
        statuses = {str(v.get("version")): str(v.get("status", "")).upper() for v in versions}
        if not version_ids:
            pytest.skip(f"'{_DOMAIN_NAME}' has no versions — run scenario 1 first.")
        _step(f"registry versions: {statuses}")

        drafts = [str(v) for v in version_ids if statuses.get(str(v)) == "DRAFT"]

        # Always work on the freshly branched DRAFT (the highest DRAFT version,
        # e.g. V2). scenario 2 leaves V1 PUBLISHED + V2 DRAFT, so the published
        # base version is never reopened or mutated here — the graph build and
        # all enrichment land on the version *after* V1.
        if not drafts:
            pytest.skip(
                f"'{_DOMAIN_NAME}' has no DRAFT version (versions={statuses}) — "
                "run scenarios 1 & 2 first (scenario 2 branches the DRAFT this "
                "scenario needs)."
            )
        work_version = drafts[0]
        need_reopen = False

        # Load the chosen DRAFT so /dtwin/sync/status reports its graph.
        if work_version != "1":
            _load_version(work_version)

        # Reuse the graph if this DRAFT was already built on a previous run;
        # otherwise build it (a freshly branched version has no graph yet).
        build_required = not _graph_built()
        if build_required:
            _step(f"selected DRAFT V{work_version}: no graph yet — will build it (slow)")
        else:
            _step(f"selected DRAFT V{work_version}: already built — reusing its graph")
        _step(f"working version: V{work_version}")

        # ── 4. Make sure the working version is a built DRAFT ───────────────
        if need_reopen or _version_status(work_version) != "DRAFT":
            _step(f"reopening V{work_version} to DRAFT (admin) so the audit write is allowed")
            resp = page.context.request.post(
                f"{base}/review/{_DOMAIN_NAME}/{work_version}/reopen",
                headers=headers(),
                data=json.dumps({"comment": "scenario_3: reopening to enrich + analyse"}),
            )
            assert resp.status == 200, resp.text()
            assert _version_status(work_version) == "DRAFT", _version_status(work_version)

        if build_required and not _graph_built():
            _step("building the knowledge graph (POST /dtwin/sync/start) — may take minutes")
            resp = page.context.request.post(
                f"{base}/dtwin/sync/start",
                headers=headers(),
                data=json.dumps({}),
                timeout=120_000,
            )
            assert resp.status == 200, resp.text()
            build_start = _json(resp)
            assert build_start.get("success") is True, build_start
            build_task = _poll_task(
                page, base, build_start["task_id"], _BUILD_TIMEOUT_S, label="KG build"
            )
            assert build_task.get("status") == "completed", (
                f"KG build did not complete cleanly: status={build_task.get('status')}, "
                f"error={build_task.get('error')}"
            )
            _step(f"KG build completed: {build_task.get('message', 'done')}")

        assert _graph_built(), (
            f"V{work_version} has no knowledge-graph data to analyse — build it first."
        )
        _step(f"knowledge graph ready for V{work_version}")

        # ── 5. Generate data-quality rules (SHACL) from the ontology ─────────
        _step("auto-suggesting SHACL data-quality shapes from the ontology")
        suggestions = _json(
            page.request.get(f"{base}/ontology/dataquality/suggest")
        ).get("suggestions", [])
        _step(f"suggested {len(suggestions)} new SHACL shape(s)")
        if suggestions:
            resp = page.context.request.post(
                f"{base}/ontology/dataquality/accept-suggestions",
                headers=headers(),
                data=json.dumps({"shapes": suggestions}),
            )
            assert resp.status == 200, resp.text()
            _step(f"accepted SHACL shapes — {_json(resp).get('message', '')}")

        shapes = _json(
            page.request.get(f"{base}/ontology/dataquality/list")
        ).get("shapes", [])
        assert len(shapes) >= 1, (
            "No SHACL data-quality shapes available after generation — the "
            "ontology may be empty (re-run scenario 1)."
        )
        _step(f"domain now carries {len(shapes)} SHACL data-quality shape(s)")

        # ── 6. Generate business rules with the AI agent ────────────────────
        _step("generating business rules (agent: SWRL / decision tables / SPARQL / aggregate)")
        resp = page.context.request.post(
            f"{base}/ontology/business-rules/generate-async",
            headers=headers(),
            data=json.dumps(
                {
                    "guidelines": (
                        "Propose pragmatic, high-value rules across all four "
                        "paradigms for this customer/utility domain: completeness "
                        "and consistency checks, simple classification (e.g. flag "
                        "high-value or at-risk customers), and useful aggregates "
                        "(e.g. totals per customer)."
                    ),
                    "options": {},
                    "documents": [],
                }
            ),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        gen_start = _json(resp)
        assert gen_start.get("success") is True, gen_start
        gen_task = _poll_task(
            page, base, gen_start["task_id"], _RULES_TIMEOUT_S, label="business-rule gen"
        )
        assert gen_task.get("status") == "completed", (
            f"Business-rule generation did not complete: status={gen_task.get('status')}, "
            f"error={gen_task.get('error')}"
        )
        proposed = gen_task.get("result") or {}
        rule_keys = ("swrl_rules", "decision_tables", "sparql_rules", "aggregate_rules")
        proposed_counts = {k: len(proposed.get(k, []) or []) for k in rule_keys}
        _step(f"agent proposed rules: {proposed_counts} — {gen_task.get('message', '')}")

        accepted_total = 0
        if any(proposed_counts.values()):
            resp = page.context.request.post(
                f"{base}/ontology/business-rules/accept-suggestions",
                headers=headers(),
                data=json.dumps({k: proposed.get(k, []) or [] for k in rule_keys}),
            )
            assert resp.status == 200, resp.text()
            accept = _json(resp)
            accepted_total = int(accept.get("added_total", 0) or 0)
            _step(
                f"accepted {accepted_total} business rule(s) "
                f"(rejected={len(accept.get('rejected', []))}, "
                f"duplicates={accept.get('duplicates_total', 0)})"
            )
        else:
            _step("agent proposed no business rules this run — continuing with SHACL + TBox/graph")

        # ── 7. Persist the enriched domain (rules) to the registry ──────────
        _step("saving the enriched domain to the registry (SHACL + business rules)")
        resp = page.context.request.post(
            f"{base}/domain/save-to-uc",
            headers=headers(),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        assert _json(resp).get("success") is True, resp.text()

        # ── 8. Run the data-quality rules ON the knowledge graph ────────────
        _step("running data-quality checks on the knowledge graph (backend=graph)")
        resp = page.context.request.post(
            f"{base}/dtwin/dataquality/start",
            headers=headers(),
            data=json.dumps({"backend": "graph", "violation_limit": 10}),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        dq_start = _json(resp)
        assert dq_start.get("success") is True, dq_start
        dq_task = _poll_task(
            page, base, dq_start["task_id"], _DQ_TIMEOUT_S, label="data-quality run"
        )
        assert dq_task.get("status") == "completed", (
            f"Data-quality run did not complete: status={dq_task.get('status')}, "
            f"error={dq_task.get('error')}"
        )
        dq_summary = (dq_task.get("result") or {}).get("summary", {}) or {}
        _step(
            f"data-quality run done — total={dq_summary.get('total', 0)}, "
            f"passed={dq_summary.get('passed', 0)}, failed={dq_summary.get('failed', 0)}, "
            f"warnings={dq_summary.get('warnings', 0)}"
        )

        # ── 9. Run the business rules (reasoning), then materialise ─────────
        _step("running the business rules (reasoning: all phases enabled)")
        resp = page.context.request.post(
            f"{base}/dtwin/reasoning/start",
            headers=headers(),
            data=json.dumps(
                {
                    "tbox": True,
                    "swrl": True,
                    "graph": True,
                    "decision_tables": True,
                    "sparql_rules": True,
                    "aggregate_rules": True,
                }
            ),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        rsn_start = _json(resp)
        assert rsn_start.get("success") is True, rsn_start
        rsn_task_id = rsn_start["task_id"]
        rsn_task = _poll_task(
            page, base, rsn_task_id, _REASONING_TIMEOUT_S, label="reasoning run"
        )
        assert rsn_task.get("status") == "completed", (
            f"Reasoning did not complete: status={rsn_task.get('status')}, "
            f"error={rsn_task.get('error')}"
        )
        inferred_count = int((rsn_task.get("result") or {}).get("inferred_count", 0) or 0)
        _step(f"reasoning done — {inferred_count} inferred triple(s)")

        _step("materialising the inferred triples back into the knowledge graph")
        resp = page.context.request.post(
            f"{base}/dtwin/reasoning/materialize",
            headers=headers(),
            data=json.dumps({"task_id": rsn_task_id, "materialize_graph": True}),
            timeout=120_000,
        )
        if inferred_count > 0:
            assert resp.status == 200, resp.text()
            mat = _json(resp)
            assert mat.get("success") is True, mat
            materialized = int(mat.get("materialize_graph_count", 0) or 0)
            _step(f"materialise done — {materialized} triple(s) written to the graph")
        else:
            # Nothing to materialise — the endpoint rejects an empty set; that's
            # an expected, benign outcome for a run that inferred nothing.
            _step(f"no inferred triples to materialise (status={resp.status})")

        # ── 10. Run the Analysis on the knowledge graph, then interpret it ──
        # The analysis runs asynchronously: POST returns a task id, we poll
        # /tasks/<id> to completion, then read the persisted result from
        # /dtwin/metrics/latest.
        _step("starting the graph analysis (POST /dtwin/metrics/compute)")
        resp = page.context.request.post(
            f"{base}/dtwin/metrics/compute",
            headers=headers(),
            data=json.dumps({}),
            timeout=_ANALYSIS_TIMEOUT_MS,
        )
        assert resp.status == 200, resp.text()
        analysis_start = _json(resp)
        assert analysis_start.get("success") is True, analysis_start
        analysis_task_id = analysis_start.get("task_id")
        assert analysis_task_id, "analysis task was not created"
        _poll_task(
            page,
            base,
            analysis_task_id,
            _ANALYSIS_TIMEOUT_S,
            label="graph analysis",
        )

        _step("loading the stored analysis result (GET /dtwin/metrics/latest)")
        resp = page.context.request.get(
            f"{base}/dtwin/metrics/latest",
            headers=headers(),
            timeout=_ANALYSIS_TIMEOUT_MS,
        )
        assert resp.status == 200, resp.text()
        metrics = _json(resp)
        assert metrics.get("success") is True, metrics
        assert metrics.get("has_result") is True, metrics
        m_stats = metrics.get("stats", {}) or {}
        top_pr = metrics.get("top_pagerank", []) or []
        _step(
            f"analysis computed — nodes={m_stats.get('node_count', '?')}, "
            f"edges={m_stats.get('edge_count', '?')}, top_pagerank={len(top_pr)}"
        )

        _step("interpreting the analysis with the graph-interpreter agent")
        resp = page.context.request.post(
            f"{base}/dtwin/metrics/interpret",
            headers=headers(),
            data=json.dumps(metrics),
            timeout=_ANALYSIS_TIMEOUT_MS,
        )
        assert resp.status == 200, resp.text()
        interp = _json(resp)
        assert interp.get("success") is True, interp
        sections = interp.get("sections", []) or []
        _step(f"interpretation produced {len(sections)} section(s)")

        # Render the interpretation to a compact, human-readable write-up.
        def _render_sections(secs: list) -> str:
            out: list[str] = []
            for s in secs:
                title = (s.get("title") or "").strip()
                if title:
                    out.append(f"### {title}")
                body = (s.get("body") or "").strip()
                if body:
                    out.append(body)
                for item in s.get("items", []) or []:
                    if isinstance(item, str):
                        out.append(f"- {item}")
                    elif isinstance(item, dict):
                        label = item.get("label") or item.get("title") or ""
                        val = item.get("value") or item.get("body") or ""
                        out.append(f"- {label}: {val}".strip(": "))
            return "\n".join(out).strip()

        interpretation = _render_sections(sections) or "(agent returned no narrative sections)"
        writeup = (
            f"Automated graph analysis (scenario 3) for {_DOMAIN_NAME} V{work_version}.\n\n"
            f"Data quality: {dq_summary.get('passed', 0)} passed / "
            f"{dq_summary.get('failed', 0)} failed / {dq_summary.get('warnings', 0)} warnings "
            f"across {dq_summary.get('total', 0)} checks.\n"
            f"Reasoning: {inferred_count} inferred triple(s) (materialised to the graph).\n"
            f"Graph: nodes={m_stats.get('node_count', '?')}, edges={m_stats.get('edge_count', '?')}.\n\n"
            f"Interpretation:\n{interpretation}"
        )

        # ── 11. Add the interpreted result to the audit trail ───────────────
        # A plain comment is *not* recorded in the review audit log, but a task
        # (which anchors the comment) appends a ``commented`` row — that is the
        # surface the unified audit trail reads. So: post the full write-up as a
        # comment, then create a task that references it.
        cm_base = f"{base}/comments/{_DOMAIN_NAME}/{work_version}"

        _step("posting the analysis write-up as a domain comment")
        resp = page.context.request.post(
            cm_base,
            headers=headers(),
            data=json.dumps({"body": writeup}),
            timeout=60_000,
        )
        assert resp.status == 200, resp.text()
        comment_id = (_json(resp).get("comment") or {}).get("id")
        assert comment_id, "comment was not created"

        # Resolve an assignee (the current user is always a safe target).
        cu = _json(page.request.get(f"{base}/domain/current-user"))
        assignee = (
            cu.get("email")
            or cu.get("username")
            or (cu.get("user") or {}).get("email")
            or "me@ontobricks.ai"
        )

        analysis_headline = (
            f"[Analysis] Graph analysis & QA for V{work_version}: "
            f"{dq_summary.get('failed', 0)} DQ failures, {inferred_count} inferred triples"
        )
        _step("creating a task that anchors the write-up → appends a 'commented' audit event")
        resp = page.context.request.post(
            f"{cm_base}/tasks",
            headers=headers(),
            data=json.dumps(
                {
                    "assignee": assignee,
                    "title": analysis_headline[:300],
                    "description": writeup[:7000],
                    "comment_id": comment_id,
                }
            ),
            timeout=60_000,
        )
        assert resp.status == 200, resp.text()
        audit_task_id = (_json(resp).get("task") or {}).get("id")
        assert audit_task_id, "audit task was not created"

        # ── 12. Verify the result surfaced in the unified audit trail ───────
        _step("verifying the analysis result is recorded in /domain/audit-trail")
        audit = _json(page.request.get(f"{base}/domain/audit-trail"))
        assert audit.get("success") is True, audit
        events = audit.get("events", []) or []
        matched = [
            e
            for e in events
            if e.get("action") == "commented"
            and (e.get("meta") or {}).get("task_id") == audit_task_id
        ]
        assert matched, (
            "the analysis result was not found in the audit trail — expected a "
            f"'commented' event for task {audit_task_id}. Events: "
            f"{[(e.get('action'), (e.get('meta') or {}).get('event')) for e in events][-10:]}"
        )
        _step(f"audit trail now records the analysis ({len(events)} total event(s))")

        # ── 13. Re-save so the rules + run metadata persist for inspection ──
        _step("re-saving the domain to capture the enriched, analysed state")
        resp = page.context.request.post(
            f"{base}/domain/save-to-uc",
            headers=headers(),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        assert _json(resp).get("success") is True, resp.text()

        _step(
            f"DONE — '{_DOMAIN_NAME}' V{work_version}: data-quality + business rules "
            f"generated and run, inferred triples materialised, graph analysed and "
            f"interpreted, and the result recorded in the audit trail. "
            f"Open {base}, load V{work_version}, and inspect the Validation timeline."
        )
