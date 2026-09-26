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
    end = html.index("</div>\n                    </div>", anchor)
    return html[start:end]


class TestTheAccessInfoBlockExists:
    def test_the_section_is_present(self):
        assert 'id="graphAccessInfoSection"' in _read(INFO_HTML)

    def test_it_hides_for_the_no_backend_domain(self):
        block = _access_block()
        tmpl = Environment(autoescape=True).from_string(block)
        assert "d-none" in tmpl.render(domain={"graph_backend": "none"})
        assert "d-none" not in tmpl.render(domain={"graph_backend": "lakebase"})


class TestEachBackendIsDocumented:
    def test_lakehouse_uses_obo_and_the_user_identity(self):
        block = _access_block()
        assert "Lakehouse" in block
        assert "Unity Catalog" in block
        assert "signed-in user" in block

    def test_lakebase_and_neo4j_run_as_the_service_principal_no_obo(self):
        block = _access_block()
        assert "Lakebase" in block
        assert "Neo4j" in block
        assert "service principal" in block
        assert "Bolt profile" in block

    def test_it_states_the_obo_answer_for_each_backend(self):
        block = _access_block()
        # One "Yes" (Lakehouse) and two "No" (Lakebase, Neo4j).
        assert block.count(">Yes") == 1
        assert block.count(">No") == 2

    def test_it_documents_the_team_gate_and_admin_bypass(self):
        block = _access_block()
        assert "Team membership" in block
        assert "Viewer" in block
        assert "administrators bypass" in block

    def test_it_notes_build_stays_on_the_service_principal(self):
        block = _access_block()
        assert "build" in block.lower()
        assert "never silently falls" in block


class TestTheBlockHidesLiveForNoBackend:
    def test_the_js_toggles_the_section_with_the_other_graph_only_sections(self):
        assert "graphAccessInfoSection" in _read(INFO_JS)
