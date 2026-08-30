"""UI contracts for the "No Backend" (ontology-only) domain type.

Four things must hold for a No Backend domain, none visible from the backend
tests:

1. The Graph Backend selector offers ``none`` and preselects it when stored.
2. The MCP tools carry a ``data-requires-graph`` marker and the Information
   JS unchecks + locks the graph tools (and reveals the notice) for ``none``.
3. The navbar disables the Mapping and Knowledge Graph tabs for ``none``.
4. The Mapping and Knowledge Graph pages redirect a ``none`` domain away.
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
INFO_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"
DOMAIN_HTML = REPO_ROOT / "src/front/templates/domain.html"
INFO_CSS = REPO_ROOT / "src/front/static/domain/css/domain-information.css"
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"
INFO_JS = REPO_ROOT / "src/front/static/domain/js/domain-information.js"
DOMAIN_JS = REPO_ROOT / "src/front/static/domain/js/domain.js"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
UTILS_JS = REPO_ROOT / "src/front/static/global/js/utils.js"
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


def _backend_cards_markup() -> str:
    html = _read(INFO_HTML)
    anchor = html.index('id="domainGraphBackendCards"')
    start = html.rindex("<fieldset", 0, anchor)
    end = html.index("</fieldset>", anchor) + len("</fieldset>")
    return html[start:end]


def _render_backend_cards(**domain) -> str:
    from jinja2 import Environment

    template = Environment(autoescape=True).from_string(_backend_cards_markup())
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


class TestTheGraphBackendCards:
    @pytest.mark.parametrize(
        ("value", "label"),
        [
            ("lakebase", "Lakebase"),
            ("databricks", "Lakehouse"),
            ("neo4j", "Neo4j"),
            ("none", "No Backend"),
        ],
    )
    def test_each_backend_is_a_native_radio_card(self, value, label):
        rendered = _render_backend_cards(graph_backend=value)
        assert 'name="domainGraphBackendChoice"' in rendered
        assert f'value="{value}"' in rendered
        assert re.search(rf'value="{value}"\s+checked', rendered)
        assert label in rendered

    def test_available_products_use_their_real_brand_assets(self):
        cards = _backend_cards_markup()
        assert "ob-icon-postgresql" in cards
        assert "ob-icon-lakehouse" in cards
        assert "ob-icon-neo4j" in cards
        assert "bi-slash-circle" in cards

    def test_the_compatibility_select_is_not_visible(self):
        select = _select_markup()
        assert "d-none" in select
        assert 'aria-hidden="true"' in select
        assert 'tabindex="-1"' in select

    def test_the_tab_body_uses_the_standard_surface(self):
        html = _read(INFO_HTML)
        anchor = html.index('id="domainInfoTabContent"')
        content_tag = html[html.rindex("<div", 0, anchor) : anchor]
        assert "tab-content" in content_tag
        assert "ob-tab-content" in content_tag
        assert 'class="ob-tabs-wrap"' in html

    def test_the_domain_page_loads_the_focused_stylesheet(self):
        assert "domain/css/domain-information.css" in _read(DOMAIN_HTML)

    def test_card_styles_cover_selection_focus_disabling_and_responsiveness(self):
        css = _read(INFO_CSS)
        assert ".domain-backend-grid" in css
        grid_rule = css[css.index(".domain-backend-grid {") :]
        grid_rule = grid_rule[: grid_rule.index("}")]
        assert "clear: left" in grid_rule
        assert ".domain-backend-option:checked + .domain-backend-card" in css
        assert ".domain-backend-option:focus-visible + .domain-backend-card" in css
        assert ".domain-backend-option:disabled + .domain-backend-card" in css
        assert "@media" in css


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

    def test_it_hides_graph_only_sections_for_no_backend(self):
        html = _read(INFO_HTML)
        js = _read(INFO_JS)
        for section_id in (
            "graphBackendMigrationNotice",
            "dualKnowledgeGraphSection",
            "tripleStoreGatewaySection",
        ):
            assert f'id="{section_id}"' in html
            assert section_id in js
        assert (
            html.count(
                "{% if domain.graph_backend == 'none' %}d-none{% endif %}"
            )
            >= 3
        )

    def test_it_runs_on_change_init_and_rehydration(self):
        js = _read(INFO_JS)
        assert (
            "graphBackendEl.addEventListener('change', applyGraphlessConstraints);"
            in js
        )
        # Once on init, once after the /domain/info rehydrate.
        assert js.count("applyGraphlessConstraints();") >= 2


class TestTheBackendCardsStayInSync:
    def test_card_changes_flow_through_the_existing_select_pipeline(self):
        js = _read(INFO_JS)
        assert "function initGraphBackendCards()" in js
        assert "backendSelect.value = option.value" in js
        assert "backendSelect.dispatchEvent(new Event('change'" in js

    def test_programmatic_backend_updates_refresh_the_cards(self):
        js = _read(INFO_JS)
        assert "function syncGraphBackendCards()" in js
        hydration = js.index(
            "graphBackendEl.value = infoData.info.graph_backend"
        )
        assert "syncGraphBackendCards();" in js[hydration : hydration + 250]


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


class TestTheAddDomainDialogDefaultsToLakehouse:
    """The New Domain dialog no longer asks for a backend: new domains default
    to Lakehouse and the dialog explains the choices can be changed later in
    Domain → Information."""

    def _dialog(self) -> str:
        src = _read(UTILS_JS)
        start = src.index("function showNewDomainDialog")
        end = src.index("window.showNewDomainDialog", start)
        return src[start:end]

    def test_the_dialog_has_no_backend_dropdown(self):
        dialog = self._dialog()
        assert "${modalId}_backend" not in dialog
        assert '<option value="lakebase"' not in dialog

    def test_the_dialog_defaults_the_backend_to_lakehouse(self):
        assert "graph_backend: 'databricks'" in self._dialog()

    def test_the_dialog_explains_the_choices_and_where_to_change_them(self):
        dialog = self._dialog()
        assert "Domain → Information" in dialog
        for choice in ("Lakehouse", "Lakebase", "Neo4j", "No Backend"):
            assert choice in dialog

    def test_domain_new_forwards_the_default_backend_to_the_api(self):
        js = _read(NAVBAR_JS)
        start = js.index("async function domainNew(")
        body = js[start : js.index("async function domainSave(", start)]
        assert "payload.graph_backend = input.graph_backend" in body


class TestThePagesRedirectAGraphlessDomain:
    @pytest.mark.parametrize("route", [MAPPING_ROUTE, DTWIN_ROUTE])
    def test_the_page_route_guards_on_the_graphless_backend(self, route):
        src = _read(route)
        assert "is_graphless_backend" in src
        assert "RedirectResponse" in src
        assert "/domain/?section=information" in src
