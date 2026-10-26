"""Structural contracts for Domain → Audit trail."""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
AUDIT_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_audit.html"
AUDIT_JS = REPO_ROOT / "src/front/static/domain/js/domain-audit.js"
AUDIT_CSS = REPO_ROOT / "src/front/static/domain/css/domain-audit.css"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_audit_uses_card_integrated_ontology_and_mapping_tabs():
    html = _read(AUDIT_HTML)
    assert '<div class="card h-100">' in html
    assert '<div class="card-body p-0 ob-tabs-wrap">' in html
    assert 'class="nav nav-tabs ob-tabs nav-fill" id="auditTabs"' in html
    assert 'id="audit-tab-ontology"' in html
    assert 'id="audit-tab-mapping"' in html
    assert 'id="audit-pane-ontology"' in html
    assert 'id="audit-pane-mapping"' in html
    assert 'id="auditOntologyBody"' in html
    assert 'id="auditMappingBody"' in html
    assert "tab-content p-3" in html
    assert 'id="auditFilter"' not in html


def test_audit_tabs_have_icons_and_aria():
    html = _read(AUDIT_HTML)
    assert 'aria-controls="audit-pane-ontology"' in html
    assert 'aria-controls="audit-pane-mapping"' in html
    ontology = html[html.index('id="audit-tab-ontology"') :]
    assert "<i class=" in ontology[:400]
    mapping = html[html.index('id="audit-tab-mapping"') :]
    assert "<i class=" in mapping[:400]


def test_audit_version_filter_has_no_inline_style():
    html = _read(AUDIT_HTML)
    start = html.index('id="auditVersionFilter"')
    tag = html[html.rindex("<select", 0, start) : html.index(">", start) + 1]
    assert "style=" not in tag


def test_audit_js_splits_ontology_and_mapping_and_renders_before_value():
    js = _read(AUDIT_JS)
    assert "auditOntologyBody" in js
    assert "auditMappingBody" in js
    assert "meta.before" in js or 'meta || {}).before' in js
    assert "meta.after" in js or 'meta || {}).after' in js
    assert " → " in js
    assert "audit-field" in js
    assert "audit-old" in js
    assert "audit-new" in js
    assert "auditFilter" not in js
    assert "collapseChanges" in js
    change_fn = js[js.index("function changeItem") : js.index("function buildItem")]
    assert "audit-title" not in change_fn
    assert "badge bg-secondary" not in change_fn
    assert "function missingVal" in js
    assert "return 'relationship'" in js or 'return "relationship"' in js
    assert "class_removed: { icon: 'trash'" in js
    assert "audit-marker-remove" in js
    assert ".audit-marker-remove" in _read(AUDIT_CSS)
    assert "var(--db-status-danger)" in _read(AUDIT_CSS)


def test_audit_rows_and_badges_are_compact():
    css = _read(AUDIT_CSS)
    marker = re.search(
        r"\.audit-marker\s*\{([^}]+)\}", css, re.DOTALL
    )
    assert marker, "missing .audit-marker rule"
    block = marker.group(1)
    assert re.search(r"height:\s*20px", block)
    assert re.search(r"width:\s*20px", block)
    assert ".audit-timeline .badge" in css
    badge = re.search(
        r"\.audit-timeline\s+\.badge\s*\{([^}]+)\}", css, re.DOTALL
    )
    assert badge, "missing compact badge rule"
    assert re.search(r"line-height:\s*1(?:\.2)?", badge.group(1))
    assert "display: grid" not in css
    assert "flex: 0 0 8rem" not in css
    diffs = re.search(r"\.audit-diff-row\s*\{([^}]+)\}", css, re.DOTALL)
    assert diffs, "missing .audit-diff-row rule"
    assert "white-space: nowrap" in diffs.group(1)
    js = _read(AUDIT_JS)
    row_fn = js[js.index("function diffRowHtml") : js.index("function changeDetail")]
    assert "bits.join(' ')" in row_fn
    assert "#auditTabs .nav-link" in css
    tabs = re.search(r"#auditTabs\s+\.nav-link\s*\{([^}]+)\}", css, re.DOTALL)
    assert tabs and "text-align: left" in tabs.group(1)
