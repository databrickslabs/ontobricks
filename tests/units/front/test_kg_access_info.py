"""UI contract for the Knowledge Graph tab's Data Access & Permissions block.

The Domain → Information → Knowledge Graph tab explains, per backend, whose
identity a graph read runs under (OBO vs service principal) and what a read
requires (UC grants and/or Team membership). This locks that copy so the OBO
model stays documented in the UI where users pick a backend.
"""

from pathlib import Path

import pytest
from jinja2 import Environment

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
INFO_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"
INFO_JS = REPO_ROOT / "src/front/static/domain/js/domain-information.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _access_block() -> str:
    html = _read(INFO_HTML)
    anchor = html.index('id="graphAccessInfoSection"')
    start = html.rindex("<div", 0, anchor)
    # The section closes right before the MCP tab comment.
    end = html.index("<!-- ======== MCP Tab ======== -->", anchor)
    return html[start:end]


def _render_block(**domain) -> str:
    return Environment(autoescape=True).from_string(_access_block()).render(domain=domain)


class TestTheAccessInfoBlockExists:
    def test_the_section_is_present(self):
        assert 'id="graphAccessInfoSection"' in _read(INFO_HTML)

    def test_it_hides_for_the_no_backend_domain(self):
        html = _read(INFO_HTML)
        anchor = html.index('id="graphAccessInfoSection"')
        # The section wrapper carries the graphless d-none guard.
        wrapper = html[html.rindex("<div", 0, anchor) : html.index(">", anchor)]
        tmpl = Environment(autoescape=True).from_string(wrapper + ">")
        assert "d-none" in tmpl.render(domain={"graph_backend": "none"})
        assert "d-none" not in tmpl.render(domain={"graph_backend": "lakebase"})


class TestEachBackendHasItsOwnDetailPanel:
    def test_there_is_one_detail_panel_per_backend(self):
        block = _access_block()
        for backend in ("databricks", "lakebase", "neo4j"):
            assert f'data-backend="{backend}"' in block
        assert block.count("graph-access-detail") >= 3


class TestOnlyTheSelectedBackendDetailIsShown:
    def test_lakehouse_selected_shows_only_the_obo_panel(self):
        rendered = _render_block(graph_backend="databricks")
        # Extract each panel and check which carries d-none.
        assert 'data-backend="databricks"' in rendered
        # The databricks panel must be visible; lakebase/neo4j hidden.
        assert self._is_visible(rendered, "databricks")
        assert not self._is_visible(rendered, "lakebase")
        assert not self._is_visible(rendered, "neo4j")

    def test_lakebase_selected_shows_only_the_lakebase_panel(self):
        rendered = _render_block(graph_backend="lakebase")
        assert self._is_visible(rendered, "lakebase")
        assert not self._is_visible(rendered, "databricks")
        assert not self._is_visible(rendered, "neo4j")

    def test_neo4j_selected_shows_only_the_neo4j_panel(self):
        rendered = _render_block(graph_backend="neo4j")
        assert self._is_visible(rendered, "neo4j")
        assert not self._is_visible(rendered, "databricks")
        assert not self._is_visible(rendered, "lakebase")

    def test_default_backend_is_lakebase(self):
        rendered = _render_block()  # no graph_backend set
        assert self._is_visible(rendered, "lakebase")

    @staticmethod
    def _is_visible(rendered: str, backend: str) -> bool:
        anchor = rendered.index(f'data-backend="{backend}"')
        div_open = rendered.rindex("<div", 0, anchor)
        div_tag = rendered[div_open : rendered.index(">", anchor)]
        return "d-none" not in div_tag


class TestTheDetailCopyMatchesTheAuthModel:
    def test_lakehouse_panel_documents_obo_and_uc_grants(self):
        block = _access_block()
        anchor = block.index('data-backend="databricks"')
        panel = block[anchor : block.index('data-backend="lakebase"')]
        assert "on-behalf-of (OBO)" in panel
        assert "Unity Catalog grants" in panel
        assert "signed-in user" in panel

    def test_lakebase_panel_documents_no_obo_and_service_principal(self):
        block = _access_block()
        anchor = block.index('data-backend="lakebase"')
        panel = block[anchor : block.index('data-backend="neo4j"')]
        assert "no OBO" in panel
        assert "service principal" in panel

    def test_neo4j_panel_documents_no_obo_and_the_bolt_profile(self):
        block = _access_block()
        panel = block[block.index('data-backend="neo4j"') :]
        assert "no OBO" in panel
        assert "Bolt profile" in panel

    def test_it_documents_the_team_gate_and_admin_bypass(self):
        block = _access_block()
        assert "Team membership" in block
        assert "Viewer" in block
        assert "administrators bypass" in block


class TestTheJsAdaptsTheDetailToTheSelectedBackend:
    def test_the_section_hides_live_for_no_backend(self):
        assert "graphAccessInfoSection" in _read(INFO_JS)

    def test_the_js_shows_only_the_matching_backend_detail(self):
        js = _read(INFO_JS)
        assert ".graph-access-detail" in js
        assert "el.dataset.backend !== backend" in js
