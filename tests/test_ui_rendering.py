"""
Layer 1 UI Tests -- HTML Rendering Verification

Fetches each page via the Starlette TestClient and parses HTML with the
stdlib html.parser to verify DOM expectations (no third-party HTML parser).
"""
from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple

import pytest


class _TagCollector(HTMLParser):
    """Collect opening tags as (name, attrs_dict); attrs lowercased by parser."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: List[Tuple[str, Dict[str, str]]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        d: Dict[str, str] = {}
        for k, v in attrs:
            d[k] = v if v is not None else ""
        self.tags.append((tag, d))


def _html(client, path: str) -> str:
    resp = client.get(path)
    assert resp.status_code == 200, f"GET {path} returned {resp.status_code}"
    return resp.text


def _tags(html: str) -> List[Tuple[str, Dict[str, str]]]:
    p = _TagCollector()
    p.feed(html)
    return p.tags


def _class_tokens(cls: str) -> List[str]:
    return [x for x in cls.split() if x]


def _has_class(attrs: Dict[str, str], name: str) -> bool:
    cls = attrs.get("class", "")
    return name in _class_tokens(cls)


def _find(
    tags: List[Tuple[str, Dict[str, str]]],
    tag: Optional[str] = None,
    id_: Optional[str] = None,
    class_: Optional[str] = None,
    attr: Optional[Tuple[str, str]] = None,
) -> Optional[Dict[str, str]]:
    aname, aval = attr if attr else (None, None)
    for t, a in tags:
        if tag is not None and t != tag:
            continue
        if id_ is not None and a.get("id") != id_:
            continue
        if class_ is not None and not _has_class(a, class_):
            continue
        if aname is not None and a.get(aname) != aval:
            continue
        return a
    return None


def _script_srcs(html: str) -> List[str]:
    return re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, flags=re.I)


def _title_text(html: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
    return (m.group(1).strip() if m else "") or ""


# =====================================================
# BASE TEMPLATE (shared across all pages)
# =====================================================


class TestBaseTemplate:
    """Verify elements inherited from base.html on every page."""

    @pytest.mark.parametrize("path", ["/", "/settings", "/ontology", "/mapping", "/domain", "/dtwin/", "/about"])
    def test_has_navbar(self, client, path):
        html = _html(client, path)
        assert _find(_tags(html), tag="nav", class_="navbar") is not None

    @pytest.mark.parametrize("path", ["/", "/settings", "/ontology", "/mapping", "/domain", "/dtwin/", "/about"])
    def test_has_brand_link(self, client, path):
        html = _html(client, path)
        tags = _tags(html)
        brand = _find(tags, tag="a", class_="navbar-brand")
        assert brand is not None
        assert brand.get("href") == "/"
        assert "OntoBricks" in html

    @pytest.mark.parametrize("path", ["/", "/settings", "/ontology"])
    def test_has_notification_container(self, client, path):
        html = _html(client, path)
        assert _find(_tags(html), id_="notifCenterDropdown") is not None

    @pytest.mark.parametrize("path", ["/", "/settings", "/ontology"])
    def test_bootstrap_script_loaded(self, client, path):
        html = _html(client, path)
        assert any("bootstrap" in src for src in _script_srcs(html))

    @pytest.mark.parametrize("path", ["/", "/ontology"])
    def test_utils_js_loaded(self, client, path):
        html = _html(client, path)
        assert any("utils.js" in src for src in _script_srcs(html))

    def test_navbar_has_domain_dropdown(self, client):
        html = _html(client, "/")
        assert _find(_tags(html), id_="domainDropdown") is not None

    def test_navbar_has_digital_twin_dropdown(self, client):
        html = _html(client, "/")
        assert _find(_tags(html), id_="digitaltwinDropdown") is not None

    def test_navbar_has_ontology_link_under_domain(self, client):
        """Ontology appears as a sub-item under the Domain dropdown (navbar_hidden)."""
        html = _html(client, "/")
        assert "/ontology/" in html

    def test_navbar_has_mapping_link_under_domain(self, client):
        """Mapping appears as a sub-item under the Domain dropdown (navbar_hidden)."""
        html = _html(client, "/")
        assert "/mapping/" in html

    def test_navbar_has_settings_link(self, client):
        html = _html(client, "/")
        tags = _tags(html)
        assert any(t == "a" and a.get("href") == "/settings" for t, a in tags)
        assert "Settings" in html

    def test_navbar_has_warehouse_status(self, client):
        html = _html(client, "/")
        assert _find(_tags(html), id_="warehouseStatusLink") is not None

    def test_navbar_has_task_tracker(self, client):
        html = _html(client, "/")
        assert _find(_tags(html), id_="taskTrackerToggle") is not None


# =====================================================
# HOME PAGE
# =====================================================


class TestHomePage:
    def test_title_contains_home(self, client):
        html = _html(client, "/")
        title = _title_text(html)
        assert "Home" in title or "OntoBricks" in title

    def test_hero_section(self, client):
        html = _html(client, "/")
        assert _find(_tags(html), class_="home-hero") is not None
        assert "OntoBricks" in html

    def test_domain_panel(self, client):
        html = _html(client, "/")
        assert _find(_tags(html), id_="sessionPanel") is not None
        assert _find(_tags(html), id_="homeDomainName") is not None

    def test_stat_items(self, client):
        html = _html(client, "/")
        assert _find(_tags(html), id_="classCount") is not None
        assert _find(_tags(html), id_="propCount") is not None
        assert _find(_tags(html), id_="mappingCount") is not None

    def test_quick_links(self, client):
        html = _html(client, "/")
        tags = _tags(html)
        hrefs = [a.get("href", "") for t, a in tags if t == "a" and _has_class(a, "quick-link-sm")]
        assert "/settings" in hrefs
        assert "/about" in hrefs


# =====================================================
# SETTINGS PAGE
# =====================================================


class TestSettingsPage:
    def test_title(self, client):
        html = _html(client, "/settings")
        assert "Settings" in _title_text(html)

    def test_tabs_present(self, client):
        html = _html(client, "/settings")
        tags = _tags(html)
        assert _find(tags, id_="tab-databricks") is not None
        assert _find(tags, id_="tab-global") is not None

    def test_host_display(self, client):
        html = _html(client, "/settings")
        assert _find(_tags(html), id_="currentHostDisplay") is not None

    def test_token_status(self, client):
        html = _html(client, "/settings")
        assert _find(_tags(html), id_="tokenStatus") is not None

    def test_warehouse_select(self, client):
        html = _html(client, "/settings")
        assert _find(_tags(html), id_="settingsWarehouseSelect") is not None

    def test_test_connection_button(self, client):
        html = _html(client, "/settings")
        btn = _find(_tags(html), id_="btnTestConnection")
        assert btn is not None
        assert "Test Connection" in html

    def test_save_all_button(self, client):
        html = _html(client, "/settings")
        assert _find(_tags(html), id_="btnSaveAllSettings") is not None

    def test_registry_pane_moved_to_registry_page(self, client):
        html = _html(client, "/registry/")
        assert _find(_tags(html), id_="registryDomainsSection") is not None

    def test_schedule_on_registry_page(self, client):
        html = _html(client, "/registry/")
        assert _find(_tags(html), id_="schedulesTableContainer") is not None

    def test_api_on_registry_page(self, client):
        html = _html(client, "/registry/")
        assert _find(_tags(html), id_="apiEndpointCards") is not None


# =====================================================
# ONTOLOGY PAGE
# =====================================================


class TestOntologyPage:
    def test_title(self, client):
        html = _html(client, "/ontology")
        assert "Ontology" in _title_text(html)

    def test_sidebar_present(self, client):
        html = _html(client, "/ontology")
        assert _find(_tags(html), class_="sidebar-nav") is not None

    @pytest.mark.parametrize(
        "section",
        [
            "information",
            "import",
            "wizard",
            "map",
            "design",
            "entities",
            "relationships",
            "dataquality",
            "swrl",
            "axioms",
            "owl",
        ],
    )
    def test_sidebar_has_section_link(self, client, section):
        html = _html(client, "/ontology")
        tags = _tags(html)
        found = any(t == "a" and a.get("data-section") == section for t, a in tags)
        assert found, f"Sidebar link for section '{section}' not found"

    @pytest.mark.parametrize(
        "section_id",
        [
            "information-section",
            "import-section",
            "wizard-section",
            "map-section",
            "design-section",
            "entities-section",
            "relationships-section",
            "dataquality-section",
            "swrl-section",
            "axioms-section",
        ],
    )
    def test_section_div_exists(self, client, section_id):
        html = _html(client, "/ontology")
        assert _find(_tags(html), id_=section_id) is not None, f"Section div #{section_id} not found"

    def test_ontoviz_script_loaded(self, client):
        html = _html(client, "/ontology")
        assert any("ontoviz.js" in src for src in _script_srcs(html))

    def test_ontology_core_script_loaded(self, client):
        html = _html(client, "/ontology")
        assert any("ontology-core.js" in src for src in _script_srcs(html))

    def test_wizard_tab(self, client):
        html = _html(client, "/ontology")
        assert _find(_tags(html), id_="wizard-tab-metadata") is not None

    def test_wizard_select_all_checkbox(self, client):
        html = _html(client, "/ontology")
        assert _find(_tags(html), id_="wizardSelectAllCheckbox") is not None


# =====================================================
# MAPPING PAGE
# =====================================================


class TestMappingPage:
    def test_title(self, client):
        html = _html(client, "/mapping")
        assert "Mapping" in _title_text(html)

    def test_sidebar_present(self, client):
        html = _html(client, "/mapping")
        assert _find(_tags(html), class_="sidebar-nav") is not None

    @pytest.mark.parametrize("section", ["information", "design", "manual", "autoassign", "r2rml", "sparksql"])
    def test_sidebar_has_section_link(self, client, section):
        html = _html(client, "/mapping")
        tags = _tags(html)
        found = any(t == "a" and a.get("data-section") == section for t, a in tags)
        assert found, f"Sidebar link for section '{section}' not found"

    def test_mapping_core_script_loaded(self, client):
        html = _html(client, "/mapping")
        assert any("mapping-core.js" in src for src in _script_srcs(html))


# =====================================================
# DOMAIN PAGE
# =====================================================


class TestDomainPage:
    def test_title(self, client):
        html = _html(client, "/domain")
        assert "Domain" in _title_text(html)

    def test_sidebar_present(self, client):
        html = _html(client, "/domain")
        assert _find(_tags(html), class_="sidebar-nav") is not None

    @pytest.mark.parametrize(
        "section",
        ["information", "metadata", "documents", "validation", "owl-content", "r2rml"],
    )
    def test_sidebar_has_section_link(self, client, section):
        html = _html(client, "/domain")
        tags = _tags(html)
        found = any(t == "a" and a.get("data-section") == section for t, a in tags)
        assert found, f"Sidebar link for section '{section}' not found"

    @pytest.mark.parametrize("section_id", ["information-section", "metadata-section", "validation-section"])
    def test_section_div_exists(self, client, section_id):
        html = _html(client, "/domain")
        assert _find(_tags(html), id_=section_id) is not None


# =====================================================
# DIGITAL TWIN PAGE
# =====================================================


class TestDigitalTwinPage:
    def test_title(self, client):
        html = _html(client, "/dtwin/")
        assert "Digital Twin" in _title_text(html)

    def test_sidebar_present(self, client):
        html = _html(client, "/dtwin/")
        assert _find(_tags(html), class_="sidebar-nav") is not None

    @pytest.mark.parametrize("section", ["dataquality", "sigmagraph", "graphql", "reasoning"])
    def test_sidebar_has_section_link(self, client, section):
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        found = any(t == "a" and a.get("data-section") == section for t, a in tags)
        assert found, f"Sidebar link for section '{section}' not found"

    def test_sigmagraph_section_present(self, client):
        html = _html(client, "/dtwin/")
        assert _find(_tags(html), id_="sigmagraph-section") is not None

    def test_sigma_script_loaded(self, client):
        html = _html(client, "/dtwin/")
        assert any("sigma" in src.lower() for src in _script_srcs(html))


# =====================================================
# ABOUT PAGE
# =====================================================


class TestAboutPage:
    def test_renders(self, client):
        html = _html(client, "/about")
        assert _find(_tags(html), tag="nav", class_="navbar") is not None

    def test_has_content(self, client):
        html = _html(client, "/about")
        assert "OntoBricks" in html
