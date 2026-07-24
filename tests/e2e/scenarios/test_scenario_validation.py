"""
E2E (LIVE) — ``test_scenario_validation``: end-to-end **validation + report**
over the state left behind by the preceding scenarios (1 → 2 → 3 → …).

This is intended to run **last** in the campaign: add new ``test_scenario_N_*``
suites after scenario 3, and this validation walk stays the final gate (its
filename sorts after any digit-suffixed scenario, so default collection order
keeps it last). Extend the ``[Scenario N]`` groups below as new scenarios add
assets/actions worth asserting.

Unlike scenarios 1-3 (which *do* things), this journey is **read-only**: it
inspects the durable ``test_scenario_1`` domain and asserts that every asset,
comment, and action the prior scenarios were supposed to create is actually
present in the registry. At the end it prints a compact **validation report**
(PASS / FAIL / INFO per check) to stdout, then fails the test if any hard check
did not pass.

What it validates:

    [Scenario 1]  base version (V1)
        - the domain is in the registry
        - V1 exists and is PUBLISHED
        - the ontology has classes + properties
        - the mapping has entity/relationship SQL
        - the V1 knowledge graph is built (triples > 0)

    [Scenario 2]  collaboration + review lifecycle (V1)
        - comments were added (>= 3)
        - tasks were created (>= 2)
        - the review lifecycle ran (submitted → published in the audit log)
        - a new DRAFT branch was created (V2)

    [Scenario 3]  rules + quality + reasoning + analysis (the DRAFT branch, V2)
        - the branch's knowledge graph is built (triples > 0)
        - SHACL data-quality shapes exist (>= 1)
        - business rules were generated (INFO — the agent may propose none)
        - the analysis write-up was recorded in the audit trail

Because it only reads, it is safe to run repeatedly. It still needs a running
app + the durable domain, so it is gated the same way as the other scenarios.

Run (against the local dev server, after the other scenarios):

    ONTOBRICKS_SCENARIO_LIVE=1 \\
    uv run pytest tests/e2e/scenarios/test_scenario_validation.py \\
        -m scenario -v -s --no-cov

Override the target via env:
    ONTOBRICKS_LIVE_BASE       base URL (default http://localhost:8000)
    ONTOBRICKS_SCENARIO_DOMAIN reused domain folder (default test_scenario_1)
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
)


# ── Gate: live journey against a real registry (read-only) ───────────────────
pytestmark = [
    pytest.mark.skipif(
        os.environ.get("ONTOBRICKS_SCENARIO_LIVE") != "1",
        reason="live scenario — set ONTOBRICKS_SCENARIO_LIVE=1 to run "
        "(needs a running app + the test_scenario_1 domain from scenarios 1-3)",
    ),
    *chain_marker(
        "scenario_validation",
        depends=("scenario_1", "scenario_2", "scenario_3"),
    ),
]


_DOMAIN_NAME = os.environ.get("ONTOBRICKS_SCENARIO_DOMAIN", "test_scenario_1")
_BASE_VERSION = "1"

# Hard thresholds — the minimum each prior scenario is expected to have created.
_MIN_COMMENTS = 3   # scenario 2 adds 2 comments, then 1 more after reopen
_MIN_TASKS = 2      # scenario 2 turns both comments into tasks
_MIN_SHACL = 1      # scenario 3 accepts at least one SHACL shape


# URL / CSRF / JSON helpers and the ``scenario_base`` / ``scenario_page``
# fixtures are shared — see ``_harness.py`` + ``conftest.py``.
_csrf_headers = csrf_headers
_json = json_body
_step = make_step("scenario_validation")


def _first_list_len(data: dict) -> int:
    """Length of the first list value in a ``{key: [...]}`` response."""
    if not isinstance(data, dict):
        return 0
    for value in data.values():
        if isinstance(value, list):
            return len(value)
    return 0


class _Report:
    """Collects PASS / FAIL / INFO checks and renders a final report."""

    def __init__(self, domain: str) -> None:
        self.domain = domain
        self.rows: list[dict] = []

    def check(self, group: str, name: str, ok: bool, detail: str = "") -> bool:
        self.rows.append(
            {"group": group, "name": name, "ok": bool(ok), "detail": detail, "info": False}
        )
        tag = "PASS" if ok else "FAIL"
        _step(f"[{tag}] {group} :: {name}{(' — ' + detail) if detail else ''}")
        return bool(ok)

    def info(self, group: str, name: str, detail: str = "") -> None:
        self.rows.append(
            {"group": group, "name": name, "ok": True, "detail": detail, "info": True}
        )
        _step(f"[INFO] {group} :: {name}{(' — ' + detail) if detail else ''}")

    @property
    def failures(self) -> list[dict]:
        return [r for r in self.rows if not r["ok"]]

    def render(self) -> str:
        width = 68
        lines = ["", "=" * width, f" SCENARIO VALIDATION REPORT — {self.domain}", "=" * width]
        groups: list[str] = []
        for r in self.rows:
            if r["group"] not in groups:
                groups.append(r["group"])
        for group in groups:
            lines.append(f" {group}")
            for r in (x for x in self.rows if x["group"] == group):
                tag = "INFO" if r["info"] else ("PASS" if r["ok"] else "FAIL")
                detail = f" — {r['detail']}" if r["detail"] else ""
                lines.append(f"   {tag:<4}  {r['name']}{detail}")
        passed = sum(1 for r in self.rows if r["ok"] and not r["info"])
        failed = sum(1 for r in self.rows if not r["ok"])
        info = sum(1 for r in self.rows if r["info"])
        lines.append("-" * width)
        lines.append(f" RESULT: {passed} passed, {failed} failed, {info} info")
        lines.append("=" * width)
        return "\n".join(lines)

    def render_markdown(self) -> str:
        """Human-readable Markdown mirror of :meth:`render` for artifacts/."""
        passed = sum(1 for r in self.rows if r["ok"] and not r["info"])
        failed = sum(1 for r in self.rows if not r["ok"])
        info = sum(1 for r in self.rows if r["info"])
        verdict = "PASS" if failed == 0 else "FAIL"
        lines = [
            f"# Scenario validation report — `{self.domain}`",
            "",
            f"**Result: {verdict}** — {passed} passed, {failed} failed, {info} info",
            "",
        ]
        groups: list[str] = []
        for r in self.rows:
            if r["group"] not in groups:
                groups.append(r["group"])
        for group in groups:
            lines += [f"## {group}", "", "| Status | Check | Detail |", "| --- | --- | --- |"]
            for r in (x for x in self.rows if x["group"] == group):
                tag = "INFO" if r["info"] else ("PASS" if r["ok"] else "FAIL")
                detail = str(r["detail"]).replace("|", "\\|") if r["detail"] else ""
                lines.append(f"| {tag} | {r['name']} | {detail} |")
            lines.append("")
        return "\n".join(lines)


def _write_report(report: "_Report") -> str | None:
    """Write the Markdown report to ``ONTOBRICKS_SCENARIO_REPORT`` (best-effort).

    Defaults to ``artifacts/scenarios/campaign_report.md`` so ``make
    scenario-campaign`` collects it alongside the JUnit + HTML reports.
    """
    dest = os.environ.get(
        "ONTOBRICKS_SCENARIO_REPORT", "artifacts/scenarios/campaign_report.md"
    )
    try:
        path = pathlib.Path(dest)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report.render_markdown(), encoding="utf-8")
        return str(path)
    except OSError as exc:  # noqa: BLE001 — reporting must never fail the suite
        _step(f"could not write campaign report to {dest}: {exc}")
        return None


class TestScenarioValidation:
    """Read-only validation of the assets/comments/actions from all scenarios."""

    def test_validate_all_scenarios_and_report(self, scenario_page, scenario_base):
        page = scenario_page
        base = scenario_base
        report = _Report(_DOMAIN_NAME)

        def headers() -> dict:
            return _csrf_headers(page.context)

        def get(path: str) -> dict:
            return _json(page.request.get(f"{base}{path}"))

        def load_version(version: str) -> bool:
            resp = page.context.request.post(
                f"{base}/domain/load-from-uc",
                headers=headers(),
                data=json.dumps({"domain": _DOMAIN_NAME, "version": version}),
                timeout=120_000,
            )
            return resp.status == 200 and _json(resp).get("success") is True

        # ── Prime the session (GET sets the csrf cookie) ────────────────────
        _step(f"priming session at {base}")
        page.goto(base)
        page.wait_for_load_state("domcontentloaded")

        # ── Prerequisite: the domain must exist in the registry ─────────────
        registry = get("/domain/list-projects")
        names = {
            (d if isinstance(d, str) else (d.get("name") or d.get("folder") or "")).lower()
            for d in registry.get("domains", []) or []
        }
        if _DOMAIN_NAME.lower() not in names:
            pytest.skip(
                f"'{_DOMAIN_NAME}' not in the registry — run scenarios 1-3 first."
            )

        # Enumerate versions (load V1 so versions-list is scoped to this domain).
        if not load_version(_BASE_VERSION):
            pytest.skip(
                f"Could not load '{_DOMAIN_NAME}' V{_BASE_VERSION} — run scenario 1 first."
            )
        versions = get("/domain/versions-list").get("versions", []) or []
        version_ids = sorted(
            (int(str(v.get("version"))) for v in versions if str(v.get("version", "")).isdigit())
        )
        if len(version_ids) < 2:
            pytest.skip(
                f"'{_DOMAIN_NAME}' has versions {version_ids} — run scenarios 2 & 3 "
                "first (2 branches the DRAFT that 3 enriches)."
            )
        enriched_version = str(version_ids[-1])  # highest version = the branch
        _step(f"validating base V{_BASE_VERSION} and enriched V{enriched_version}")

        # =====================================================================
        # [Scenario 1] base version (V1): assets
        # =====================================================================
        g1 = "[Scenario 1] Generate + Auto-Map + Build (V1)"
        report.check(g1, "domain present in registry", True, _DOMAIN_NAME)

        review1 = get(f"/review/{_DOMAIN_NAME}/{_BASE_VERSION}")
        v1_status = str(review1.get("status", "")).upper()
        report.check(
            g1, "V1 exists in the registry", bool(review1.get("success")), v1_status or "?"
        )

        onto = get("/ontology/load").get("config", {}) or {}
        n_classes = len(onto.get("classes", []) or [])
        n_props = len(onto.get("properties", []) or [])
        report.check(
            g1, "ontology has classes", n_classes >= 1,
            f"{n_classes} classes / {n_props} properties",
        )

        mapping = get("/mapping/load").get("config", {}) or {}
        n_ent = sum(1 for m in mapping.get("entities", []) or [] if m.get("sql_query"))
        n_rel = sum(1 for m in mapping.get("relationships", []) or [] if m.get("sql_query"))
        report.check(
            g1, "mappings have SQL", n_ent >= 1,
            f"{n_ent} entity + {n_rel} relationship mappings with SQL",
        )

        sync1 = get("/dtwin/sync/status")
        v1_triples = int(sync1.get("count", 0) or 0)
        report.check(
            g1, "V1 knowledge graph built",
            bool(sync1.get("has_data")) and v1_triples > 0,
            f"{v1_triples} triples in {sync1.get('graph_name', '?')}",
        )

        # =====================================================================
        # [Scenario 2] collaboration + review lifecycle (V1): comments/actions
        # =====================================================================
        g2 = "[Scenario 2] Collaboration + Review lifecycle (V1)"
        comments = get(f"/comments/{_DOMAIN_NAME}/{_BASE_VERSION}").get("comments", []) or []
        report.check(
            g2, f"comments added (>= {_MIN_COMMENTS})", len(comments) >= _MIN_COMMENTS,
            f"{len(comments)} comment(s)",
        )

        tasks = get(f"/comments/{_DOMAIN_NAME}/{_BASE_VERSION}/tasks").get("tasks", []) or []
        report.check(
            g2, f"tasks created (>= {_MIN_TASKS})", len(tasks) >= _MIN_TASKS,
            f"{len(tasks)} task(s)",
        )

        actions = {str(e.get("action", "")).lower() for e in review1.get("events", []) or []}
        review_ran = {"submitted", "published"} <= actions
        report.check(
            g2, "review lifecycle ran (submitted → published)", review_ran,
            ", ".join(sorted(a for a in actions if a)) or "no events",
        )
        report.check(
            g2, "V1 is PUBLISHED", v1_status == "PUBLISHED", v1_status or "?"
        )

        review2 = get(f"/review/{_DOMAIN_NAME}/{enriched_version}")
        v2_status = str(review2.get("status", "")).upper()
        report.check(
            g2, f"new version branched (V{enriched_version})",
            bool(review2.get("success")), f"status={v2_status or '?'}",
        )

        # =====================================================================
        # [Scenario 3] rules + quality + reasoning + analysis (V2): enrichment
        # =====================================================================
        g3 = "[Scenario 3] Rules + Quality + Reasoning + Analysis " f"(V{enriched_version})"
        if not load_version(enriched_version):
            report.check(g3, f"load V{enriched_version}", False, "load-from-uc failed")
        else:
            sync2 = get("/dtwin/sync/status")
            v2_triples = int(sync2.get("count", 0) or 0)
            report.check(
                g3, f"V{enriched_version} knowledge graph built",
                bool(sync2.get("has_data")) and v2_triples > 0,
                f"{v2_triples} triples in {sync2.get('graph_name', '?')}",
            )

            shapes = get("/ontology/dataquality/list").get("shapes", []) or []
            report.check(
                g3, f"SHACL data-quality shapes (>= {_MIN_SHACL})", len(shapes) >= _MIN_SHACL,
                f"{len(shapes)} shape(s)",
            )

            rule_counts = {
                "swrl": _first_list_len(get("/ontology/swrl/list")),
                "decision_tables": _first_list_len(get("/ontology/rules/decision_tables/list")),
                "sparql": _first_list_len(get("/ontology/rules/sparql_rules/list")),
                "aggregate": _first_list_len(get("/ontology/rules/aggregate_rules/list")),
            }
            total_rules = sum(rule_counts.values())
            # Soft: the agent legitimately proposes zero rules on some runs.
            report.info(
                g3, "business rules present",
                f"total={total_rules} ({', '.join(f'{k}={v}' for k, v in rule_counts.items())})",
            )

            audit = get("/domain/audit-trail")
            events = audit.get("events", []) or []
            analysis_events = [
                e for e in events
                if str(e.get("action", "")).lower() == "commented"
                and "analysis" in str(e.get("comment", "")).lower()
            ]
            report.check(
                g3, "analysis recorded in audit trail", bool(analysis_events),
                f"{len(analysis_events)} analysis event(s) of {len(events)} total",
            )

            v2_comments = get(f"/comments/{_DOMAIN_NAME}/{enriched_version}").get(
                "comments", []
            ) or []
            has_writeup = any(
                "graph analysis" in str(c.get("body", "")).lower() for c in v2_comments
            )
            report.info(
                g3, "analysis write-up comment present",
                "yes" if has_writeup else f"not found ({len(v2_comments)} comment(s))",
            )

        # ── Final report ────────────────────────────────────────────────────
        print(report.render(), flush=True)
        report_path = _write_report(report)
        if report_path:
            _step(f"campaign report written → {report_path}")

        assert not report.failures, (
            f"{len(report.failures)} validation check(s) failed: "
            + "; ".join(f"{r['group']} :: {r['name']}" for r in report.failures)
        )
