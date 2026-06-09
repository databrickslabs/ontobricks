"""Shared helpers for persona UAT flows.

Mirrors the conventions used across ``tests/e2e`` (CSRF priming via a GET,
``page.context.request.*`` for API calls, ``SidebarNav.switchTo`` for section
navigation) so persona tests read like the rest of the suite.
"""

from __future__ import annotations

import json

# A small, self-contained ontology (two classes + one object property) used to
# seed a session so read-only personas have content to browse. Classes carry
# explicit URIs so downstream update/delete/generate calls have stable targets.
SEED_BASE = "http://uat.ontobricks.test/"
SEED_OWL = """
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@base <http://uat.ontobricks.test/> .

<http://uat.ontobricks.test/>          a owl:Ontology .
<http://uat.ontobricks.test/Customer>  a owl:Class ; rdfs:label "Customer" .
<http://uat.ontobricks.test/Order>     a owl:Class ; rdfs:label "Order" .
<http://uat.ontobricks.test/placed>    a owl:ObjectProperty ; rdfs:label "placed" ;
    rdfs:domain <http://uat.ontobricks.test/Customer> ;
    rdfs:range  <http://uat.ontobricks.test/Order> .
"""
CUSTOMER_URI = "http://uat.ontobricks.test/Customer"
ORDER_URI = "http://uat.ontobricks.test/Order"


# --- HTTP / CSRF -----------------------------------------------------------


def csrf_headers(context) -> dict:
    """Write-ready JSON headers carrying the double-submit CSRF token.

    The persona role headers ride on the browser *context* (extra_http_headers)
    so they are merged into every request automatically; these per-call headers
    only add the content-type, an explicit JSON Accept (so authorization
    failures come back as 403 JSON rather than an HTML redirect), and the CSRF
    token lifted from the ``csrf_token`` cookie.
    """
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    token = cookies.get("csrf_token")
    if token:
        headers["X-CSRF-Token"] = token
    return headers


def admin_headers(context) -> dict:
    """CSRF headers that additionally override the seam role to admin.

    Used to seed session-level data inside a non-admin persona's context
    (same session cookie) so read-only personas can browse real content.
    Per-request headers override the context's persona role headers.
    """
    headers = csrf_headers(context)
    headers["x-ontobricks-test-role"] = "admin"
    headers["x-ontobricks-test-domain-role"] = "admin"
    return headers


def prime(page, live_server: str) -> dict:
    """Visit home (issues the CSRF cookie) and return write-ready headers."""
    page.goto(live_server)
    page.wait_for_load_state("domcontentloaded")
    return csrf_headers(page.context)


def post(page, live_server: str, path: str, payload=None, headers=None):
    headers = headers if headers is not None else csrf_headers(page.context)
    return page.context.request.post(
        f"{live_server}{path}",
        headers=headers,
        data=json.dumps(payload if payload is not None else {}),
    )


def delete(page, live_server: str, path: str, headers=None):
    headers = headers if headers is not None else csrf_headers(page.context)
    return page.context.request.delete(f"{live_server}{path}", headers=headers)


def get(page, live_server: str, path: str):
    return page.context.request.get(
        f"{live_server}{path}", headers={"Accept": "application/json"}
    )


def body_json(resp):
    try:
        return json.loads(resp.body())
    except Exception:  # noqa: BLE001 — non-JSON body
        return {}


def is_forbidden(resp) -> bool:
    """True when *resp* is an authorization 403 (not a CSRF rejection)."""
    if resp.status != 403:
        return False
    return body_json(resp).get("error") != "csrf"


# --- Data seeding ----------------------------------------------------------


def seed_ontology(page, live_server: str, owl: str = SEED_OWL):
    """Import a small ontology into the page's session, acting as admin.

    Works inside any persona context: the import POST carries admin override
    headers, but the session cookie is the context's own, so subsequent reads
    by the persona see the seeded ontology.
    """
    page.goto(live_server)
    page.wait_for_load_state("domcontentloaded")
    return page.context.request.post(
        f"{live_server}/ontology/import-owl",
        headers=admin_headers(page.context),
        data=json.dumps({"content": owl}),
    )


# --- UI navigation ---------------------------------------------------------


def open_page(page, live_server: str, route: str, settle_ms: int = 400):
    """Navigate to a top-level area page and let initial JS settle."""
    page.goto(f"{live_server}{route}")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(settle_ms)


def switch_section(page, section: str, settle_ms: int = 400):
    """Switch the sidebar to *section* (mirrors the existing e2e pattern)."""
    page.evaluate(f'SidebarNav.switchTo("{section}")')
    page.wait_for_timeout(settle_ms)


def body_role(page) -> tuple[str, str]:
    """Server-rendered (app_role, domain_role) from the <body> attributes."""
    el = page.locator("body")
    return (
        el.get_attribute("data-app-role") or "",
        el.get_attribute("data-domain-role") or "",
    )
