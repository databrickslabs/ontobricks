"""UAT — permission boundary matrix (persona × protected endpoint).

The exhaustive negative coverage: for every persona, drive a representative
write/read from each feature area and assert the authorization outcome derived
from the role model (``ROLE_HIERARCHY`` + middleware admin-only / domain-scoped
/ viewer-write gates + the route-level ``require(ROLE_BUILDER)`` guards).

All requests are CSRF-correct, so any 403 is an *authorization* decision, not a
CSRF rejection. ``block`` cases short-circuit in the middleware/guard before any
Databricks call (fast, deterministic). ``allow`` cases on Databricks-backed
endpoints are time-boxed and skipped when no workspace creds are available — the
security boundary is still proven by the block cases.
"""

from __future__ import annotations

import json

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import csrf_headers, prime

pytestmark = [pytest.mark.e2e, pytest.mark.uat]


# (method, path, kind, local, payload) — kind drives the expected outcome.
ENDPOINTS = [
    # --- session-local domain writes (editor+ may write, viewer may not) ---
    {
        "method": "POST",
        "path": "/ontology/class/add",
        "kind": "domain_write",
        "local": True,
        "payload": {"name": "mx_probe"},
    },
    {
        "method": "POST",
        "path": "/ontology/property/add",
        "kind": "domain_write",
        "local": True,
        "payload": {"name": "mx_rel"},
    },
    {
        "method": "POST",
        "path": "/ontology/dataquality/save",
        "kind": "domain_write",
        "local": True,
        "payload": {},
    },
    {
        "method": "POST",
        "path": "/mapping/save",
        "kind": "domain_write",
        "local": True,
        "payload": {},
    },
    {
        "method": "POST",
        "path": "/mapping/entity/add",
        "kind": "domain_write",
        "local": True,
        "payload": {},
    },
    # --- domain reads (any team member may read) ---
    {"method": "GET", "path": "/ontology/load", "kind": "domain_read", "local": True},
    {"method": "GET", "path": "/mapping/load", "kind": "domain_read", "local": True},
    # --- builder-only (route require(ROLE_BUILDER); editor/viewer blocked) ---
    {
        "method": "POST",
        "path": "/dtwin/sync/start",
        "kind": "builder_only",
        "local": False,
        "payload": {},
    },
    {
        "method": "POST",
        "path": "/dtwin/cohorts/rules",
        "kind": "builder_only",
        "local": False,
        "payload": {"id": "mx", "name": "MX", "definition": {}},
    },
    {
        "method": "POST",
        "path": "/dtwin/cohorts/materialize",
        "kind": "builder_only",
        "local": False,
        "payload": {},
    },
    {
        "method": "DELETE",
        "path": "/dtwin/cohorts/rules/mx_probe",
        "kind": "builder_only",
        "local": False,
    },
    # --- admin-only (only the Platform Admin persona) ---
    {
        "method": "POST",
        "path": "/settings/save-base-uri",
        "kind": "admin_only",
        "local": False,
        "payload": {"base_uri": "https://uat.test/"},
    },
    {
        "method": "POST",
        "path": "/settings/teams",
        "kind": "admin_only",
        "local": False,
        "payload": {},
    },
    {
        "method": "POST",
        "path": "/settings/registry/initialize",
        "kind": "admin_only",
        "local": False,
        "payload": {},
    },
]


def _expected(persona: P.Persona, kind: str) -> str:
    lvl = persona.effective_domain_level
    if kind == "admin_only":
        return "allow" if persona.is_admin else "block"
    if kind == "domain_read":
        return "allow" if lvl >= P.ROLE_LEVEL[P.ROLE_VIEWER] else "block"
    if kind == "domain_write":
        return "allow" if lvl >= P.ROLE_LEVEL[P.ROLE_EDITOR] else "block"
    if kind == "builder_only":
        return "allow" if lvl >= P.ROLE_LEVEL[P.ROLE_BUILDER] else "block"
    raise ValueError(f"unknown endpoint kind: {kind}")


def _call(page, live_server, spec, timeout=None):
    url = f"{live_server}{spec['path']}"
    kw = {"timeout": timeout} if timeout else {}
    method = spec["method"]
    if method == "GET":
        return page.context.request.get(
            url, headers={"Accept": "application/json"}, **kw
        )
    if method == "DELETE":
        return page.context.request.delete(
            url, headers=csrf_headers(page.context), **kw
        )
    return page.context.request.post(
        url,
        headers=csrf_headers(page.context),
        data=json.dumps(spec.get("payload", {})),
        **kw,
    )


class TestPermissionBoundaryMatrix:
    @pytest.mark.parametrize("persona", P.ALL_PERSONAS, ids=P.ids)
    @pytest.mark.parametrize(
        "spec", ENDPOINTS, ids=lambda s: f"{s['method']}:{s['path']}"
    )
    def test_boundary(self, persona_page, live_server, persona, spec):
        page = persona_page(persona)
        prime(page, live_server)
        exp = _expected(persona, spec["kind"])

        if exp == "block":
            resp = _call(page, live_server, spec)
            assert resp.status == 403, (
                f"{persona.key} expected 403 on {spec['method']} {spec['path']}, "
                f"got {resp.status}: {resp.text()[:200]}"
            )
            return

        # exp == "allow"
        if spec["local"]:
            resp = _call(page, live_server, spec)
        else:
            try:
                resp = _call(page, live_server, spec, timeout=20000)
            except Exception as exc:  # noqa: BLE001 — Databricks unreachable offline
                pytest.skip(
                    f"{spec['method']} {spec['path']} needs Databricks creds "
                    f"(offline/fake-creds run): {exc}"
                )
        assert resp.status != 403, (
            f"{persona.key} unexpectedly blocked on {spec['method']} "
            f"{spec['path']}: {resp.text()[:200]}"
        )
