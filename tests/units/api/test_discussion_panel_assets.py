"""
Contract tests for the Discussion panel front-end assets.

These tests fetch the served assets through the app's ``/static`` mount and
assert the wiring is present, so an accidental removal/rename of a key hook
is caught by CI.

They are deliberately token-level (not behavioural) — they guard that the
contract between the panel and the rest of the app stays intact:

* comment bodies are rendered as markdown (via the global ``marked``).
"""

from __future__ import annotations

import pytest

PANEL_JS = "/static/global/js/comments-panel.js"
ONTOLOGY_INIT_JS = "/static/ontology/js/ontology-init.js"
MAPPING_INIT_JS = "/static/mapping/js/mapping-init.js"
COLLAB_JS = "/static/domain/js/domain-collaboration.js"
REVIEW_CSS = "/static/global/css/review-modals.css"


def _static(client, path: str) -> str:
    """Fetch a served static asset, asserting it is reachable."""
    resp = client.get(path)
    assert resp.status_code == 200, f"GET {path} returned {resp.status_code}"
    return resp.text


@pytest.fixture
def panel_js(client) -> str:
    return _static(client, PANEL_JS)


class TestDiscussionMarkdownRendering:
    """Comment bodies render markdown (not raw source) via the global marked."""

    def test_panel_defines_markdown_renderer(self, panel_js):
        assert "function renderMarkdown" in panel_js

    def test_renderer_uses_global_marked(self, panel_js):
        assert "window.marked" in panel_js
        assert "marked.parse" in panel_js

    def test_renderer_has_plaintext_fallback(self, panel_js):
        # When marked is unavailable it must still escape + line-break, never
        # inject raw text as HTML.
        assert "replace(/\\n/g, '<br>')" in panel_js

    def test_bubble_renders_body_as_markdown(self, panel_js):
        # The comment bubble pipes the parsed body through the renderer into a
        # markdown-styled container rather than escaping it verbatim.
        assert "oc-md" in panel_js
        assert "renderMarkdown(parsed.text)" in panel_js

    def test_markdown_styles_present(self, client):
        css = _static(client, REVIEW_CSS)
        assert ".oc-md" in css


class TestDiscussionDomainScope:
    """The panel is a single domain-wide thread with no tagging UI."""

    def test_no_anchor_in_requests(self, panel_js):
        # Comments are domain-wide: the panel must not send the (removed)
        # anchor_type / anchor_ref to the /comments API.
        assert "anchor_type" not in panel_js
        assert "anchor_ref" not in panel_js
        assert "anchorType" not in panel_js
        assert "anchorRef" not in panel_js

    def test_no_kind_badge_separator(self, panel_js):
        # The header no longer renders the "Class/Domain/Mapping" kind badge
        # that separated discussions by selection.
        assert "bg-secondary-subtle text-dark border me-1" not in panel_js

    def test_tag_picker_removed_from_compose(self, panel_js):
        # No entity/relationship tag widget when writing a comment/reply.
        assert "tagWidgetHtml" not in panel_js
        assert "data-oc-tag-select" not in panel_js
        assert "data-oc-tagbar" not in panel_js

    def test_no_tag_encoding_on_post(self, panel_js):
        # New comments post the raw body — tags are no longer embedded.
        assert "encodeBody" not in panel_js
        assert "collectTags" not in panel_js


class TestDiscussionTimelineMarkdown:
    """Domain → Discussions timeline renders comment bodies as markdown."""

    def test_timeline_defines_markdown_renderer(self, client):
        js = _static(client, COLLAB_JS)
        assert "function renderMarkdown" in js
        assert "window.marked" in js
        assert "marked.parse" in js

    def test_timeline_entry_renders_markdown(self, client):
        js = _static(client, COLLAB_JS)
        # Timeline entry pipes the parsed body through the renderer into a
        # markdown-styled container rather than escaping it verbatim.
        assert "renderMarkdown(parsed.text)" in js
        assert "oc-md" in js

    def test_timeline_markdown_styles_apply(self, client):
        # The Domain page loads review-modals.css, and the `.oc-md` reset is
        # unscoped so it applies to the timeline's `.oc-tl-text.oc-md`.
        css = _static(client, REVIEW_CSS)
        assert ".oc-md {" in css
