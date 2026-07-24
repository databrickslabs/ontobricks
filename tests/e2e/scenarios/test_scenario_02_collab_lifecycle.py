"""
E2E (LIVE) — ``test_scenario_2``: collaboration + review lifecycle on the
domain produced by :mod:`test_scenario_01_generate_live`.

This journey **reuses the durable ``test_scenario_1`` domain** (built and
persisted by scenario 1) instead of regenerating one, then exercises the
human-in-the-loop workflow that sits on top of a built version:

    1. load ``test_scenario_1`` V1 from the registry (skips if scenario 1
       hasn't been run — it is the prerequisite)
    2. make sure V1 is a clean, built DRAFT (reopen it if a previous
       scenario-2 run left it elsewhere)
    3. add **comments** (one framed around the ontology, one around the
       mapping) and turn them into **tasks**, then advance a task's status
    4. **submit for review** (DRAFT → IN-REVIEW)
    5. **reopen** back to DRAFT (admin) and add a fresh comment
    6. **publish** the version (submit → sign-off → publish → PUBLISHED)
    7. **create a new version** (V2), leaving V1 PUBLISHED and V2 as a fresh
       DRAFT branched from it

Comments and tasks are persisted domain-wide per ``(folder, version)``; the
"ontology" / "mapping" framing lives in the comment/task text (the surface a
comment was opened from is a UI affordance, not stored on the record).

Because comments are writable only while a version is DRAFT or IN-REVIEW, and
``submit`` requires the version to have been **built** at least once, this
scenario depends on scenario 1 having produced a built ``test_scenario_1``.

Gated behind ``ONTOBRICKS_SCENARIO_LIVE=1`` (and the ``scenario`` marker) so
it never runs in the default matrix.

Run (against the local dev server, after scenario 1):

    ONTOBRICKS_SCENARIO_LIVE=1 \\
    uv run pytest tests/e2e/scenarios/test_scenario_02_collab_lifecycle.py \\
        -m scenario -v -s --no-cov

Override the target via env:
    ONTOBRICKS_LIVE_BASE       base URL (default http://localhost:8000)
    ONTOBRICKS_SCENARIO_DOMAIN reused domain folder (default test_scenario_1)
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
)


# ── Gate: live, mutating journey against a real registry ─────────────────────
pytestmark = [
    pytest.mark.skipif(
        os.environ.get("ONTOBRICKS_SCENARIO_LIVE") != "1",
        reason="live scenario — set ONTOBRICKS_SCENARIO_LIVE=1 to run "
        "(needs a running app + the test_scenario_1 domain from scenario 1)",
    ),
    *chain_marker("scenario_2", depends=("scenario_1",)),
]


_DOMAIN_NAME = os.environ.get("ONTOBRICKS_SCENARIO_DOMAIN", "test_scenario_1")
_BASE_VERSION = "1"


# URL / CSRF / JSON helpers and the ``scenario_base`` / ``scenario_page``
# fixtures are shared — see ``_harness.py`` + ``conftest.py``. The short local
# names are kept so the journey below reads identically across every scenario.
_csrf_headers = csrf_headers
_json = json_body
_step = make_step("scenario_2")


class TestScenario2CollabLifecycle:
    """Comments + tasks → review → reopen → publish → new version (V2)."""

    def test_comments_tasks_review_publish_and_version(
        self, scenario_page, scenario_base
    ):
        page = scenario_page
        base = scenario_base

        def headers() -> dict:
            return _csrf_headers(page.context)

        def review_detail(version: str) -> dict:
            return _json(page.request.get(f"{base}/review/{_DOMAIN_NAME}/{version}"))

        # ── 1. Prime the session (GET sets the csrf cookie) ──────────────────
        _step(f"priming session at {base}")
        page.goto(base)
        page.wait_for_load_state("domcontentloaded")

        # ── 2. Prerequisite: test_scenario_1 must exist in the registry ──────
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
                f"'{_DOMAIN_NAME}' not in the registry — run scenario 1 first "
                "(test_scenario_01_generate_live.py) to build and persist it."
            )

        # ── 3. Load V1 and require the canonical clean state (single version) ─
        _step(f"loading {_DOMAIN_NAME} v{_BASE_VERSION} from the registry")
        resp = page.context.request.post(
            f"{base}/domain/load-from-uc",
            headers=headers(),
            data=json.dumps({"domain": _DOMAIN_NAME, "version": _BASE_VERSION}),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        assert _json(resp).get("success") is True, resp.text()

        versions = _json(page.request.get(f"{base}/domain/versions-list")).get(
            "versions", []
        )
        version_ids = {str(v.get("version")) for v in versions}
        if version_ids - {_BASE_VERSION}:
            pytest.skip(
                f"'{_DOMAIN_NAME}' already has versions {sorted(version_ids)} — "
                "re-run scenario 1 for a clean single-version (V1) build before "
                "scenario 2."
            )

        # ── 4. Ensure V1 is a built DRAFT (reopen if a prior run left it on) ─
        detail = review_detail(_BASE_VERSION)
        if not detail.get("last_build"):
            pytest.skip(
                f"'{_DOMAIN_NAME}' v{_BASE_VERSION} has never been built — "
                "re-run scenario 1 (submit-for-review requires a KG build)."
            )
        if detail.get("status") != "DRAFT":
            _step(f"v{_BASE_VERSION} is {detail.get('status')} — reopening to DRAFT")
            resp = page.context.request.post(
                f"{base}/review/{_DOMAIN_NAME}/{_BASE_VERSION}/reopen",
                headers=headers(),
                data=json.dumps({"comment": "scenario_2 reset to DRAFT"}),
            )
            assert resp.status == 200, resp.text()
            assert review_detail(_BASE_VERSION).get("status") == "DRAFT"
        _step(f"v{_BASE_VERSION} ready: status=DRAFT, built={bool(detail.get('last_build'))}")

        # Resolve an assignee (the current user is always a safe target).
        cu = _json(page.request.get(f"{base}/domain/current-user"))
        assignee = (
            cu.get("email")
            or cu.get("username")
            or (cu.get("user") or {}).get("email")
            or "me@ontobricks.ai"
        )
        _step(f"assignee resolved: {assignee}")

        cm_base = f"{base}/comments/{_DOMAIN_NAME}/{_BASE_VERSION}"

        # ── 5. Add comments (ontology + mapping) and turn them into tasks ────
        _step("adding an ontology comment and a mapping comment")
        resp = page.context.request.post(
            cm_base,
            headers=headers(),
            data=json.dumps(
                {"body": "[Ontology] The Customer/Invoice class hierarchy looks correct."}
            ),
        )
        assert resp.status == 200, resp.text()
        onto_comment_id = (_json(resp).get("comment") or {}).get("id")

        resp = page.context.request.post(
            cm_base,
            headers=headers(),
            data=json.dumps(
                {"body": "[Mapping] Double-check the Invoice→Customer join in the auto-mapped SQL."}
            ),
        )
        assert resp.status == 200, resp.text()
        map_comment_id = (_json(resp).get("comment") or {}).get("id")

        _step("creating one task per comment (ontology + mapping)")
        resp = page.context.request.post(
            f"{cm_base}/tasks",
            headers=headers(),
            data=json.dumps(
                {
                    "assignee": assignee,
                    "title": "[Ontology] Review class labels & descriptions",
                    "description": "Confirm labels read well for business users.",
                    "comment_id": onto_comment_id,
                }
            ),
        )
        assert resp.status == 200, resp.text()
        onto_task_id = (_json(resp).get("task") or {}).get("id")

        resp = page.context.request.post(
            f"{cm_base}/tasks",
            headers=headers(),
            data=json.dumps(
                {
                    "assignee": assignee,
                    "title": "[Mapping] Validate auto-generated SQL against UC",
                    "description": "Spot-check row counts for two entities.",
                    "comment_id": map_comment_id,
                }
            ),
        )
        assert resp.status == 200, resp.text()

        _step("advancing the ontology task to in_progress")
        resp = page.context.request.post(
            f"{cm_base}/tasks/{onto_task_id}/status",
            headers=headers(),
            data=json.dumps({"status": "in_progress"}),
        )
        assert resp.status == 200, resp.text()

        comments = _json(page.request.get(cm_base)).get("comments", [])
        tasks = _json(page.request.get(f"{cm_base}/tasks")).get("tasks", [])
        assert len(comments) >= 2, comments
        assert len(tasks) >= 2, tasks
        _step(f"thread now has {len(comments)} comments and {len(tasks)} tasks")

        # ── 6. Submit for review (DRAFT → IN-REVIEW) ─────────────────────────
        _step("submitting v1 for review (DRAFT → IN-REVIEW)")
        resp = page.context.request.post(
            f"{base}/review/{_DOMAIN_NAME}/{_BASE_VERSION}/submit",
            headers=headers(),
            data=json.dumps({"comment": "Ready for review — comments addressed inline."}),
        )
        assert resp.status == 200, resp.text()
        assert review_detail(_BASE_VERSION).get("status") == "IN-REVIEW", review_detail(
            _BASE_VERSION
        )

        # ── 7. Reopen back to DRAFT (admin) and add a fresh comment ──────────
        _step("reopening v1 (IN-REVIEW → DRAFT)")
        resp = page.context.request.post(
            f"{base}/review/{_DOMAIN_NAME}/{_BASE_VERSION}/reopen",
            headers=headers(),
            data=json.dumps({"comment": "Reopening to capture one more change."}),
        )
        assert resp.status == 200, resp.text()
        assert review_detail(_BASE_VERSION).get("status") == "DRAFT"

        _step("adding a new comment now that it's back in DRAFT")
        resp = page.context.request.post(
            cm_base,
            headers=headers(),
            data=json.dumps({"body": "[Review] Feedback addressed — ready to publish."}),
        )
        assert resp.status == 200, resp.text()
        assert len(_json(page.request.get(cm_base)).get("comments", [])) >= 3

        # ── 8. Publish (submit → sign-off → publish → PUBLISHED) ─────────────
        _step("re-submitting for review ahead of publish")
        resp = page.context.request.post(
            f"{base}/review/{_DOMAIN_NAME}/{_BASE_VERSION}/submit",
            headers=headers(),
            data=json.dumps({"comment": "Re-submitting for publish."}),
        )
        assert resp.status == 200, resp.text()

        _step("signing off (approve) to meet the review quorum")
        resp = page.context.request.post(
            f"{base}/review/{_DOMAIN_NAME}/{_BASE_VERSION}/signoff",
            headers=headers(),
            data=json.dumps({"decision": "approve", "comment": "LGTM."}),
        )
        assert resp.status == 200, resp.text()

        _step("publishing v1 (IN-REVIEW → PUBLISHED)")
        resp = page.context.request.post(
            f"{base}/review/{_DOMAIN_NAME}/{_BASE_VERSION}/publish",
            headers=headers(),
            data=json.dumps({"comment": "Publishing v1."}),
        )
        assert resp.status == 200, resp.text()
        assert review_detail(_BASE_VERSION).get("status") == "PUBLISHED", review_detail(
            _BASE_VERSION
        )

        # ── 9. Create a new version (V2), branched from the published V1 ─────
        _step("creating a new version (V2) from the published V1")
        resp = page.context.request.post(
            f"{base}/domain/create-version",
            headers=headers(),
            timeout=120_000,
        )
        assert resp.status == 200, resp.text()
        created = _json(resp)
        assert created.get("success") is True, created
        new_version = str(created.get("new_version"))
        assert new_version == "2", created
        _step(f"new version created: V{new_version} ({created.get('message')})")

        # ── 10. Verify the final registry shape: V1 PUBLISHED + V2 DRAFT ─────
        versions = _json(page.request.get(f"{base}/domain/versions-list")).get(
            "versions", []
        )
        by_ver = {str(v.get("version")): v for v in versions}
        assert by_ver.get("1", {}).get("status") == "PUBLISHED", versions
        assert by_ver.get("2", {}).get("status") == "DRAFT", versions
        _step(
            f"DONE — '{_DOMAIN_NAME}': V1 PUBLISHED (with comments + tasks), "
            f"V2 DRAFT. Open {base} and inspect the Validation workspace + versions."
        )
