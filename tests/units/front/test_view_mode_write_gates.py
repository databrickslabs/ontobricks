"""View-mode gates for design writes and graph refresh operations.

On a non-editable domain (``body.read-only-version`` / ``.role-viewer`` /
``.read-only-locked``), design writes must be neutralised. Build and reasoning
materialisation are different: builders may refresh graph data on a published
version, while viewers and users blocked by another editor's lock remain unable
to run them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PERMISSIONS_CSS = REPO_ROOT / "src/front/static/global/css/permissions.css"
PERMISSIONS_JS = REPO_ROOT / "src/front/static/global/js/permissions.js"
INFO_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_information.html"
DESIGN_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_design.html"
INFO_JS = REPO_ROOT / "src/front/static/global/js/mapping-information.js"
SYNC_JS = REPO_ROOT / "src/front/static/query/js/query-sync.js"
COHORT_JS = REPO_ROOT / "src/front/static/query/js/query-cohorts.js"
REASONING_JS = REPO_ROOT / "src/front/static/query/js/query-reasoning.js"
MCP_TAB_HTML = (
    REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"
)

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

    def test_build_is_status_open_but_view_mode_disabled(self):
        css = _css()
        assert (
            "body:is(.read-only-version, .role-viewer, .read-only-locked) "
            "#syncStartBtn" not in css
        )
        assert (
            "body:is(.role-viewer, .read-only-locked) #syncStartBtn"
            in css
        )

    def test_cohort_materialise_is_disabled(self):
        assert _gated(_css(), "#cohortMaterializeBtn")

    def test_reasoning_materialise_is_status_open_but_view_mode_disabled(self):
        css = _css()
        assert (
            "body:is(.read-only-version, .role-viewer, .read-only-locked) "
            "#runMaterializeBtn" not in css
        )
        assert (
            "body:is(.role-viewer, .read-only-locked) #runMaterializeBtn"
            in css
        )

    def test_mcp_policy_controls_are_disabled(self):
        """Editing the published tool set is a domain write, not navigation."""
        css = _css()
        assert _gated(css, ".js-mcp-tool")
        assert _gated(css, ".js-mcp-context")
        assert _gated(css, "#mcpToolsSelectAll")

    def test_mcp_controls_are_gated_by_class_not_by_generated_id(self):
        """Regression: ``query_graphql`` escaped the read-only lockdown.

        The tool checkboxes are ``#mcpTool_<tool>``, so the generic form rule's
        ``[id*="query"]`` exemption — meant for query editors — matched
        ``#mcpTool_query_graphql`` and left it clickable on a read-only domain.
        Gating on the class keeps the lockdown independent of tool names, which
        also covers a future tool containing "search" or "filter".
        """
        css = _css()
        html = MCP_TAB_HTML.read_text(encoding="utf-8")
        # The id that triggered the bug is still the one the template emits.
        assert 'id="mcpTool_{{ tool.name }}"' in html
        assert "js-mcp-tool" in html
        # No MCP rule may lean on an id built from the tool name. Only the
        # selector list counts — the comment above the rule names the offending
        # id on purpose, to explain why the gate is class-based.
        for rule in css.split("}"):
            if ".js-mcp-tool" not in rule:
                continue
            selectors = rule.rsplit("*/", 1)[-1].split("{")[0]
            assert "mcpTool_" not in selectors


class TestJsGuards:
    def test_unmap_all_refuses_when_cannot_edit(self):
        js = INFO_JS.read_text(encoding="utf-8")
        assert "canEditOntology" in js.split("async function confirmResetMappings")[1][
            :500
        ]

    def test_graph_refresh_permission_is_independent_of_lifecycle_status(self):
        js = PERMISSIONS_JS.read_text(encoding="utf-8")
        helper = js.split("function canRefreshGraph")[1].split(
            "window.OB =", maxsplit=1
        )[0]
        assert "hasDomainRole('builder')" in helper
        assert "editLockMode" in helper
        assert "versionStatus" not in helper
        assert "isActiveVersion" not in helper

    def test_build_refuses_when_cannot_refresh_graph(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        start = js.split("async function startTripleStoreSync")[1][:600]
        assert "canRefreshGraph" in start
        assert "Build is unavailable" in start

    def test_build_readiness_uses_graph_refresh_permission(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        assert "canBuild" in js
        assert "canRefreshGraph" in js

    def test_published_build_skips_domain_save(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        start = js.split("async function startTripleStoreSync")[1][:1800]
        assert "canEditOntology" in start
        assert "if (canEdit)" in start
        assert "_showSaveBeforeBuildDialog" in start
        assert "doDomainSave" in start

    def test_cohort_materialise_refuses_when_cannot_edit(self):
        js = COHORT_JS.read_text(encoding="utf-8")
        assert "canEditOntology" in js.split("openMaterializeModal()")[1][:400]
        assert "canEditOntology" in js.split("async materialize()")[1][:400]

    def test_reasoning_materialise_refuses_when_cannot_refresh_graph(self):
        js = REASONING_JS.read_text(encoding="utf-8")
        assert "canRefreshGraph" in js.split("async runMaterialize()")[1][:400]
