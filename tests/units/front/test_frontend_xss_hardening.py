"""Frontend XSS hardening and dead-code contracts.

* One shared, DOMPurify-sanitised ``renderMarkdown`` in ``utils.js``; the
  comment / chat / audit renderers must not call ``marked.parse`` directly.
* ``escapeHtml`` escapes quotes so it is safe in attribute contexts, and
  ``utils.js`` is its single global definition.
* Untrusted ontology / metadata values are escaped before ``innerHTML``.
* Verified-dead JS stays deleted.
"""

import json
import re
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src/front"
STATIC = FRONT / "static"
TEMPLATES = FRONT / "templates"
UTILS_JS = STATIC / "global/js/utils.js"

MARKDOWN_CONSUMERS = [
    "global/js/comments-panel.js",
    "domain/js/domain-collaboration.js",
    "query/js/query-chat.js",
    "ontology/js/ontology-assistant.js",
    "domain/js/domain-audit.js",
]

MARKED_URL = "https://cdn.jsdelivr.net/npm/marked@15.0.12/marked.min.js"
DOMPURIFY_URL = "https://cdn.jsdelivr.net/npm/dompurify@3.4.17/dist/purify.min.js"

pytestmark = pytest.mark.unit


def _read(rel: str | Path) -> str:
    path = rel if isinstance(rel, Path) else STATIC / rel
    if not path.exists():
        pytest.fail(f"{path.relative_to(REPO_ROOT)} does not exist")
    return path.read_text(encoding="utf-8")


def _top_level_function(js: str, name: str) -> str:
    """Source of a top-level ``function <name>`` up to its closing brace."""
    match = re.search(rf"^function {name}\(.*?^\}}", js, re.S | re.M)
    assert match, f"function {name} not found"
    return match.group(0)


# ---------------------------------------------------------------------------
# Shared Markdown renderer
# ---------------------------------------------------------------------------


def test_utils_defines_sanitised_render_markdown():
    body = _top_level_function(_read(UTILS_JS), "renderMarkdown")
    assert "DOMPurify.sanitize(" in body
    assert "marked.parse(" in body
    assert "escapeHtml(src).replace(/\\n/g, '<br>')" in body


def test_render_markdown_is_exported_on_window():
    assert "window.renderMarkdown = renderMarkdown;" in _read(UTILS_JS)


@pytest.mark.parametrize("rel", MARKDOWN_CONSUMERS)
def test_markdown_consumers_use_shared_renderer(rel):
    js = _read(rel)
    assert "marked.parse" not in js
    assert "function renderMarkdown" not in js
    assert "function renderMd" not in js
    assert "renderMarkdown(" in js


def _script_tag(html: str, url: str) -> str:
    match = re.search(rf'<script[^>]*src="{re.escape(url)}"[^>]*>', html)
    assert match, f"{url} is not loaded"
    return match.group(0)


@pytest.mark.parametrize("url", [MARKED_URL, DOMPURIFY_URL])
def test_base_loads_pinned_libraries_with_sri(url):
    tag = _script_tag(_read(TEMPLATES / "base.html"), url)
    assert re.search(r'integrity="sha384-[A-Za-z0-9+/=]{64}"', tag)
    assert 'crossorigin="anonymous"' in tag


def test_no_unpinned_marked_anywhere():
    for path in TEMPLATES.rglob("*.html"):
        html = path.read_text(encoding="utf-8")
        assert "npm/marked/" not in html, path.name


def test_ontology_page_does_not_reload_marked():
    assert "marked" not in _read(TEMPLATES / "ontology.html")


# ---------------------------------------------------------------------------
# escapeHtml / escapeAttr
# ---------------------------------------------------------------------------


def test_escape_html_replaces_quotes_explicitly():
    js = _read(UTILS_JS)
    body = _top_level_function(js, "escapeHtml")
    assert "textContent" not in body
    assert "'\"': '&quot;'" in js
    assert "\"'\": '&#39;'" in js


def test_escape_attr_is_global_in_utils():
    js = _read(UTILS_JS)
    assert "function escapeAttr(" in js
    assert "window.escapeAttr = escapeAttr;" in js


def test_home_does_not_redefine_escape_helpers():
    js = _read("home/js/home.js")
    assert "function escapeHtml" not in js
    assert "function escapeAttr" not in js


NODE = shutil.which("node")


@pytest.mark.skipif(NODE is None, reason="node is not installed")
def test_escape_and_render_markdown_behaviour():
    js = _read(UTILS_JS)
    escapes = re.search(r"^const _HTML_ESCAPES = .*?;$", js, re.M).group(0)
    source = "\n".join([
        escapes,
        _top_level_function(js, "escapeHtml"),
        _top_level_function(js, "renderMarkdown"),
    ])
    script = textwrap.dedent(
        """
        const assert = require('assert');
        global.window = {};
        eval(SOURCE + '; global.escapeHtml = escapeHtml; global.renderMarkdown = renderMarkdown;');

        assert.strictEqual(escapeHtml(`<a href="x" title='y'>&</a>`),
            '&lt;a href=&quot;x&quot; title=&#39;y&#39;&gt;&amp;&lt;/a&gt;');
        assert.strictEqual(escapeHtml(null), '');

        // No marked / DOMPurify: escaped plain text with <br>.
        assert.strictEqual(renderMarkdown('<img src=x onerror=1>\\nok'),
            '&lt;img src=x onerror=1&gt;<br>ok');

        // marked without DOMPurify must not return raw HTML.
        window.marked = { parse: (s) => s };
        assert.strictEqual(renderMarkdown('<b>x</b>'), '&lt;b&gt;x&lt;/b&gt;');

        // Both present: marked output always goes through DOMPurify.
        let sanitised = null;
        window.DOMPurify = { sanitize: (h) => { sanitised = h; return 'CLEAN'; } };
        assert.strictEqual(renderMarkdown('<b>x</b>'), 'CLEAN');
        assert.strictEqual(sanitised, '<b>x</b>');
        """
    ).replace("SOURCE", json.dumps(source))
    result = subprocess.run([NODE, "-e", script], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Untrusted values escaped before innerHTML
# ---------------------------------------------------------------------------


def test_import_reports_escape_untrusted_values():
    js = _read("ontology/js/ontology-information.js")
    for needle in (
        "${escapeHtml(filename)}",
        "${escapeHtml(onto.info.label || 'Unnamed')}",
        "${escapeHtml(cls.name)}",
        "${escapeHtml(cls.label || cls.name)}",
        "${escapeHtml(a.name)}",
        "${escapeHtml(prop.name)}",
    ):
        assert js.count(needle) >= 2, needle  # OWL + RDFS reports
    assert "<strong>File:</strong> ${filename}" not in js
    assert "<td><strong>${cls.name}</strong></td>" not in js


def test_shared_panels_escape_entity_and_relationship_fields():
    js = _read("ontology/js/ontology-shared-panels.js")
    for needle in (
        'id="sharedEntityName" value="${escapeHtml(cls?.name',
        'id="sharedEntityLabel" value="${escapeHtml(cls?.label',
        "${escapeHtml(cls?.comment || cls?.description || '')}</textarea>",
        'id="sharedRelName" value="${escapeHtml(prop?.name',
        'id="sharedRelLabel" value="${escapeHtml(prop?.label',
        "${escapeHtml(prop?.comment || prop?.description || '')}</textarea>",
        "${escapeHtml(error.message)}",
        "${escapeHtml(table.comment)}",
        'value="${escapeHtml(col.name)}"',
        "${escapeHtml(col.name)}</span>",
    ):
        assert needle in js, needle
    assert 'value="${cls?.name || \'\'}"' not in js
    assert "${table.comment}</small>" not in js


def test_entity_import_conflict_controls_escape_name():
    js = _read("ontology/js/ontology-entity-import.js")
    assert "const name = escapeHtml(e.name);" in js
    assert 'data-rename="${name}"' in js


# ---------------------------------------------------------------------------
# Dead code stays deleted
# ---------------------------------------------------------------------------

DEAD_FUNCTIONS = {
    "query/js/query-d3graph.js": [
        "resetD3Graph", "refreshVisualization", "unpinAllNodes", "zoomD3Graph",
        "toggleVisualFiltersPanel", "selectAllVisualFilters", "clearAllVisualFilters",
        "checkEntityLimit",
    ],
    "query/js/query-loaders.js": ["isValidEntityType", "displayCachedVisualization"],
    "domain/js/domain-information.js": [
        "rollbackVersion", "onVersionChange", "updateRegistryLocationDisplay", "updateGraphPaths",
    ],
    "domain/js/domain.js": ["loadDomainFromFile", "saveDomainFromSettings"],
    "global/js/navbar.js": ["handleDomainFile"],
    "ontology/js/ontology-wizard.js": ["getSelectedDocumentNames", "renderAgentStepsLog", "_truncate"],
    "global/js/mapping-information.js": ["showValidationResult", "showValidationSuccessModal"],
    "global/js/utils.js": ["loadVolumes"],
    "ontology/js/ontology-information.js": ["showValidationSummary"],
    "query/js/query-sync.js": ["checkConfigChanges"],
    "ontology/js/ontology-pitfalls.js": ["_showDepsWarning"],
    "global/js/comments-panel.js": ["userIsComposing"],
}


@pytest.mark.parametrize(
    "rel,name",
    [(rel, name) for rel, names in DEAD_FUNCTIONS.items() for name in names],
)
def test_dead_function_removed(rel, name):
    assert not re.search(rf"\bfunction {name}\(", _read(rel))


def test_legacy_uc_file_browser_removed():
    assert not (STATIC / "home/js/uc-file-browser-modal.js").exists()
    assert "uc-file-browser-modal.js" not in _read(TEMPLATES / "base.html")
    partial = _read(TEMPLATES / "partials/layout/uc_file_browser_modal.html")
    assert 'id="ucFileBrowserModal"' not in partial
    assert 'id="ucFileDialogModal"' in partial  # still used by uc_file_dialog.js


def test_no_quote_unsafe_escape_helper_is_redefined():
    """A local textContent->innerHTML escaper shadows the quote-safe global."""
    pattern = re.compile(
        r"function\s+escapeHtml\s*\([^)]*\)\s*\{[^}]*textContent[^}]*innerHTML",
        re.DOTALL,
    )
    offenders = [
        str(path.relative_to(STATIC))
        for path in STATIC.rglob("*.js")
        if pattern.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []
