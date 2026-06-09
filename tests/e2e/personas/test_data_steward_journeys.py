"""UAT — Data Steward (Sam, editor) end-to-end journey.

Sam refines and reviews. An editor may edit ontology/mappings and run quality
checks, but the route guards stop them building or defining cohorts (builder+)
and the admin gates stop them touching settings. Review reads are reachable;
the registry-backed sign-off itself is time-boxed/skipped offline.
"""

from __future__ import annotations

import json

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import (
    body_role,
    csrf_headers,
    open_page,
    prime,
    seed_ontology,
)

pytestmark = [pytest.mark.e2e, pytest.mark.uat]

PERSONA = P.DATA_STEWARD


class TestDataStewardCanEdit:
    def test_lands_as_editor(self, persona_page, live_server):
        page = persona_page(PERSONA)
        open_page(page, live_server, "/ontology")
        app_role, domain_role = body_role(page)
        assert app_role == "app_user" and domain_role == "editor"

    def test_edit_class_allowed(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/class/add",
            headers=headers,
            data=json.dumps({"name": "StewardEdit"}),
        )
        assert resp.status != 403, resp.text()[:300]

    def test_edit_mapping_allowed(self, persona_page, live_server):
        page = persona_page(PERSONA)
        seed_ontology(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/mapping/save",
            headers=csrf_headers(page.context),
            data=json.dumps({"mappings": {}}),
        )
        assert resp.status != 403, resp.text()[:300]

    def test_data_quality_allowed(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/dataquality/save",
            headers=headers,
            data=json.dumps({"shapes": []}),
        )
        assert resp.status != 403, resp.text()[:300]


class TestDataStewardCannotBuildOrPublish:
    def test_build_blocked(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/dtwin/sync/start",
            headers=headers,
            data=json.dumps({}),
        )
        assert resp.status == 403, f"editor must not build: {resp.text()[:200]}"

    def test_cohort_define_blocked(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/dtwin/cohorts/rules",
            headers=headers,
            data=json.dumps({"id": "c1", "name": "C1", "definition": {}}),
        )
        assert (
            resp.status == 403
        ), f"cohort rules are builder-gated; editor blocked: {resp.text()[:200]}"

    def test_settings_blocked(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/settings/save-base-uri",
            headers=headers,
            data=json.dumps({"base_uri": "https://nope.test/"}),
        )
        assert resp.status == 403


class TestDataStewardReview:
    def test_review_queue_reachable(self, persona_page, live_server):
        page = persona_page(PERSONA)
        prime(page, live_server)
        resp = page.context.request.get(
            f"{live_server}/review/my-tasks", headers={"Accept": "application/json"}
        )
        # Editors may see their review queue; registry errors are tolerated.
        assert resp.status != 403, resp.text()[:200]
