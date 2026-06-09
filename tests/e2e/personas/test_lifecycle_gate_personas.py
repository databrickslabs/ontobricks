"""UAT — version lifecycle edit-gate per persona.

Content is editable only while a version is DRAFT; once IN-REVIEW / PUBLISHED
the mutating endpoints are blocked for *all* roles (admins included) until the
version is set back to DRAFT.

* The DRAFT-allowed case is deterministic offline (a fresh session is DRAFT).
* The locked case requires the registry backend to move a version out of DRAFT,
  so it skips cleanly when no workspace/registry is available.
"""

from __future__ import annotations

import json

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import (
    body_json,
    csrf_headers,
    prime,
    seed_ontology,
)

pytestmark = [pytest.mark.e2e, pytest.mark.uat]


class TestDraftEditable:
    @pytest.mark.parametrize(
        "persona",
        [P.ONTOLOGY_ENGINEER, P.DATA_ENGINEER, P.DATA_STEWARD, P.ADMIN],
        ids=lambda p: p.key,
    )
    def test_draft_session_allows_edits(self, persona_page, live_server, persona):
        page = persona_page(persona)
        seed_ontology(page, live_server)  # fresh session → status DRAFT
        resp = page.context.request.post(
            f"{live_server}/ontology/class/add",
            headers=csrf_headers(page.context),
            data=json.dumps({"name": "DraftClass"}),
        )
        assert resp.status != 403, (
            f"{persona.key} should edit a DRAFT version, got "
            f"{resp.status}: {resp.text()[:200]}"
        )


class TestLockedVersionBlocksEdits:
    """Best-effort: needs the registry to lock a version out of DRAFT."""

    def test_locked_version_blocks_even_builder(self, persona_page, live_server):
        page = persona_page(P.ONTOLOGY_ENGINEER)
        prime(page, live_server)

        # Try to discover a registered domain + version to lock.
        listed = page.context.request.get(
            f"{live_server}/domain/list-projects",
            headers={"Accept": "application/json"},
        )
        if listed.status != 200:
            pytest.skip("registry unavailable (offline) — cannot exercise lock")

        projects = body_json(listed)
        # Shape is registry-dependent; bail out gracefully if we can't parse it.
        name = None
        if isinstance(projects, dict):
            items = projects.get("projects") or projects.get("domains") or []
            if items and isinstance(items[0], dict):
                name = items[0].get("name") or items[0].get("domain_name")
        if not name:
            pytest.skip("no registered domain available to lock")

        # Attempt to move it to IN-REVIEW (state machine enforced server-side).
        lock = page.context.request.post(
            f"{live_server}/domain/set-version-status",
            headers=csrf_headers(page.context),
            data=json.dumps(
                {"domain_name": name, "version": "v1", "status": "IN-REVIEW"}
            ),
        )
        if lock.status != 200:
            pytest.skip(
                "could not lock a version (state machine / permissions / "
                "registry) — edit-gate negative covered by unit tests"
            )

        # With a locked version loaded, a mutating edit must be refused.
        edit = page.context.request.post(
            f"{live_server}/ontology/class/add",
            headers=csrf_headers(page.context),
            data=json.dumps({"name": "ShouldBeBlocked"}),
        )
        assert edit.status == 403, (
            f"locked version must block edits, got {edit.status}: "
            f"{edit.text()[:200]}"
        )
