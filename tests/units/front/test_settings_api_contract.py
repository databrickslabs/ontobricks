"""Contracts for the curated Settings → API reference."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = REPO_ROOT / "src/front/templates/partials/dtwin/_query_api.html"
SCRIPT = REPO_ROOT / "src/front/static/query/js/query-api.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_domains_card_documents_backend_and_graph_availability():
    template = _read(TEMPLATE)
    domains_anchor = template.index('data-try-endpoint="/api/v1/domains"')
    card_start = template.rindex('<div class="card', 0, domains_anchor)
    domains_card = template[card_start:domains_anchor]

    assert '"graph_backend": "lakebase"' in domains_card
    assert '"graph_backend": "none"' in domains_card
    assert '"has_graph": true' in domains_card
    assert '"has_graph": false' in domains_card
    assert '"mcp_policy":' in domains_card


def test_page_has_domain_status_and_graph_capability_markers():
    template = _read(TEMPLATE)
    assert 'id="apiDomainStatus"' in template
    assert template.count('data-requires-graph="ready"') >= 7


def test_graphql_copy_requires_a_materialized_graph():
    template = _read(TEMPLATE)
    assert "materialized graph" in template
    assert "marked <em>Active</em>" not in template


def test_domain_selector_uses_the_external_contract():
    script = _read(SCRIPT)
    assert "fetch('/api/v1/domains'" in script
    assert "opt.dataset.graphBackend = p.graph_backend" in script
    assert "opt.dataset.hasGraph = String(Boolean(p.has_graph))" in script
    assert "fetch('/settings/registry/domains'" not in script


def test_versions_are_limited_to_published_versions():
    script = _read(SCRIPT)
    assert "data.versions.filter(v => v.is_published)" in script
    assert "latest PUBLISHED" in script


def test_domain_capabilities_control_graph_actions():
    script = _read(SCRIPT)
    assert "function syncApiDomainCapabilities()" in script
    assert "option.dataset.graphBackend === 'none'" in script
    assert "option.dataset.hasGraph === 'true'" in script
    assert "document.querySelectorAll('[data-requires-graph]')" in script
    assert "control.disabled = hasSelection && !hasGraph" in script
    assert "clearApiResponses();" in script
