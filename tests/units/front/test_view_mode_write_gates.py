"""View-mode gates for Unmap all, Build, and Materialise.

On a non-editable domain (``body.read-only-version`` / ``.role-viewer`` /
``.read-only-locked``) these write actions must be CSS-neutralised and their
JS entry points must refuse to run. The Information "Unmap all" button used
to be *hidden* via ``.ontology-edit-btn``; the TODO asks for disabled, so it
must stay visible under the pointer-events gate instead.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PERMISSIONS_CSS = REPO_ROOT / "src/front/static/global/css/permissions.css"
INFO_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_information.html"
DESIGN_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_design.html"
INFO_JS = REPO_ROOT / "src/front/static/global/js/mapping-information.js"
SYNC_JS = REPO_ROOT / "src/front/static/query/js/query-sync.js"
COHORT_JS = REPO_ROOT / "src/front/static/query/js/query-cohorts.js"
REASONING_JS = REPO_ROOT / "src/front/static/query/js/query-reasoning.js"

pytestmark = pytest.mark.unit

READ_ONLY = "body:is(.read-only-version, .role-viewer, .read-only-locked)"


def _css() -> str:
    return PERMISSIONS_CSS.read_text(encoding="utf-8")


def _gated(css: str, selector: str) -> bool:
    """True when *selector* sits in a read-only disable rule (not the hide rule)."""
    for block in css.split("{"):
        if selector not in block:
            continue
        # The selector list ends just before the opening brace we split on;
        # only count blocks that also carry the read-only body gate.
        if READ_ONLY in block or READ_ONLY.replace(" ", "") in block.replace(" ", ""):
            return True
        # Multi-line selector lists put the body gate on an earlier line of
        # the same rule; walk a short window of preceding text.
    # Simpler: the disable rule is the one that sets pointer-events: none.
    # Find each occurrence of the selector and check the following property
    # block mentions pointer-events (hide rules use display: none).
    idx = 0
    while True:
        pos = css.find(selector, idx)
        if pos < 0:
            return False
        # Look back for the nearest body:is(.read-only…) opener of this rule.
        rule_start = css.rfind("body:is(.read-only-version", 0, pos)
        brace = css.find("{", pos)
        props = css[brace : css.find("}", brace) + 1] if brace >= 0 else ""
        if rule_start >= 0 and "pointer-events: none" in props:
            return True
        idx = pos + len(selector)


class TestCssGates:
    def test_unmap_all_information_is_disabled_not_hidden(self):
        css = _css()
        assert _gated(css, "#resetMappingsBtn")
        # Must not be under the display:none ontology-edit-btn hide rule only.
        html = INFO_HTML.read_text(encoding="utf-8")
        assert 'id="resetMappingsBtn"' in html
        assert "ontology-edit-btn" not in html.split('id="resetMappingsBtn"')[0][-80:]

    def test_unmap_all_designer_is_disabled(self):
        assert _gated(_css(), "#resetMappingsDesignBtn")
        assert 'id="resetMappingsDesignBtn"' in DESIGN_HTML.read_text(encoding="utf-8")

    def test_build_is_disabled(self):
        assert _gated(_css(), "#syncStartBtn")

    def test_cohort_materialise_is_disabled(self):
        assert _gated(_css(), "#cohortMaterializeBtn")

    def test_reasoning_materialise_is_disabled(self):
        assert _gated(_css(), "#runMaterializeBtn")


class TestJsGuards:
    def test_unmap_all_refuses_when_cannot_edit(self):
        js = INFO_JS.read_text(encoding="utf-8")
        assert "canEditOntology" in js.split("async function confirmResetMappings")[1][
            :500
        ]

    def test_build_refuses_when_cannot_edit(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        start = js.split("async function startTripleStoreSync")[1][:600]
        assert "canEditOntology" in start
        assert "Build is unavailable" in start

    def test_build_readiness_respects_read_only(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        assert "canBuild" in js
        assert "canEditOntology" in js

    def test_cohort_materialise_refuses_when_cannot_edit(self):
        js = COHORT_JS.read_text(encoding="utf-8")
        assert "canEditOntology" in js.split("openMaterializeModal()")[1][:400]
        assert "canEditOntology" in js.split("async materialize()")[1][:400]

    def test_reasoning_materialise_refuses_when_cannot_edit(self):
        js = REASONING_JS.read_text(encoding="utf-8")
        assert "canEditOntology" in js.split("async runMaterialize()")[1][:400]
