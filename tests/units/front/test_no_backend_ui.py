"""UI contracts for the "No Backend" (ontology-only) domain type.

Four things must hold for a No Backend domain, none visible from the backend
tests:

1. The Graph Backend selector offers ``none`` and preselects it when stored.
2. The MCP tools carry a ``data-requires-graph`` marker and the Information
   JS unchecks + locks the graph tools (and reveals the notice) for ``none``.
3. The navbar disables the Mapping and Knowledge Graph tabs for ``none``.
4. The Mapping and Knowledge Graph pages redirect a ``none`` domain away.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
INFO_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"
INFO_JS = REPO_ROOT / "src/front/static/domain/js/domain-information.js"
DOMAIN_JS = REPO_ROOT / "src/front/static/domain/js/domain.js"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
MAPPING_ROUTE = REPO_ROOT / "src/front/routes/mapping.py"
DTWIN_ROUTE = REPO_ROOT / "src/front/routes/dtwin.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _select_markup() -> str:
    html = _read(INFO_HTML)
    anchor = html.index('id="domainGraphBackend"')
    start = html.rindex("<select", 0, anchor)
    end = html.index("</select>", anchor) + len("</select>")
    return html[start:end]


def _render_select(**domain) -> str:
    from jinja2 import Environment

    template = Environment(autoescape=True).from_string(_select_markup())
    return template.render(domain=domain)


class TestTheGraphBackendSelector:
    def test_it_offers_the_no_backend_option(self):
        rendered = _render_select(graph_backend="lakebase")
        assert '<option value="none"' in rendered
        assert "No Backend" in rendered

    def test_a_stored_no_backend_choice_is_preselected(self):
        rendered = _render_select(graph_backend="none")
        assert '<option value="none" selected>' in rendered
        assert '<option value="lakebase" selected' not in rendered

    def test_the_ontology_only_notice_exists_and_hides_by_default(self):
        html = _read(INFO_HTML)
        assert 'id="noBackendNotice"' in html
        # Hidden unless the stored backend is none.
        from jinja2 import Environment

        anchor = html.index('id="noBackendNotice"')
        block_start = html.rindex("<div", 0, anchor)
        block = html[block_start : html.index("</div>", anchor) + len("</div>")]
        tmpl = Environment(autoescape=True).from_string(block)
        assert "d-none" in tmpl.render(domain={"graph_backend": "lakebase"})
        assert "d-none" not in tmpl.render(domain={"graph_backend": "none"})


class TestTheMcpToolsCarryTheGraphMarker:
    def test_each_tool_checkbox_declares_whether_it_needs_a_graph(self):
        html = _read(INFO_HTML)
        assert "data-requires-graph=" in html
        assert "tool.requires_graph" in html


class TestTheInformationJsLocksGraphTools:
    def test_the_constraint_helper_exists(self):
        assert "function applyGraphlessConstraints()" in _read(INFO_JS)

    def test_it_targets_graph_tools_and_the_none_backend(self):
        js = _read(INFO_JS)
        start = js.index("function applyGraphlessConstraints()")
        body = js[start : start + 900]
        assert "'none'" in body
        assert 'data-requires-graph="true"' in body
        assert "el.disabled" in body
        assert "el.checked" in body
        assert "noBackendNotice" in body

    def test_it_runs_on_change_init_and_rehydration(self):
        js = _read(INFO_JS)
        assert (
            "graphBackendEl.addEventListener('change', applyGraphlessConstraints);"
            in js
        )
        # Once on init, once after the /domain/info rehydrate.
        assert js.count("applyGraphlessConstraints();") >= 2


class TestTheMcpTabRehydratesUnderTheConstraint:
    def test_apply_policy_reapplies_the_graphless_lock(self):
        body = _read(DOMAIN_JS)
        start = body.index("function applyMcpPolicy(")
        assert "applyGraphlessConstraints" in body[start : start + 600]

    def test_select_all_never_rechecks_a_locked_tool(self):
        body = _read(DOMAIN_JS)
        start = body.index("function initMcpPolicyTab()")
        assert "if (!el.disabled)" in body[start : start + 500]


class TestTheNavbarDisablesGraphFeatures:
    def test_the_mapping_and_kg_subnav_tabs_are_marked(self):
        html = _read(BASE_HTML)
        mapping = html[html.index('id="subnavMappingDropdown"') - 300 :
                       html.index('id="subnavMappingDropdown"')]
        kg = html[html.index('id="subnavKgDropdown"') - 300 :
                  html.index('id="subnavKgDropdown"')]
        assert "nav-requires-graph" in mapping
        assert "nav-requires-graph" in kg

    def test_the_navbar_defines_and_calls_the_gate(self):
        js = _read(NAVBAR_JS)
        assert "function updateMenusForGraphBackend(" in js
        assert "updateMenusForGraphBackend(" in js.split(
            "function applyDomainInfo", 1
        )[1].split("function updateMenusForGraphBackend", 1)[0]

    def test_the_gate_keys_off_the_none_backend(self):
        js = _read(NAVBAR_JS)
        start = js.index("function updateMenusForGraphBackend(")
        body = js[start : start + 900]
        assert "'none'" in body
        assert "nav-requires-graph" in body
        assert "nav-disabled" in body


class TestThePagesRedirectAGraphlessDomain:
    @pytest.mark.parametrize("route", [MAPPING_ROUTE, DTWIN_ROUTE])
    def test_the_page_route_guards_on_the_graphless_backend(self, route):
        src = _read(route)
        assert "is_graphless_backend" in src
        assert "RedirectResponse" in src
        assert "/domain/?section=information" in src
