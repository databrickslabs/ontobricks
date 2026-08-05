"""Contract tests for the top-right breadcrumb (navbar).

The breadcrumb must show only the current main menu (Ontology, Mapping,
Domain, Knowledge Graph, Registry, Settings) and the sub menu (sidebar
section) selected within it — no Registry/Domain ancestor crumbs.
"""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
BREADCRUMB_JS = REPO_ROOT / "src/front/static/global/js/breadcrumb.js"


def test_build_crumbs_has_no_ancestor_pushing_logic():
    js = BREADCRUMB_JS.read_text(encoding="utf-8")
    start = js.index("_buildCrumbs(path)")
    end = js.index("_updateChromeHeight()", start)
    block = js[start:end]
    # No more Registry / Domain ancestor crumbs, no hierarchy lookup.
    assert "Registry" not in block
    assert "_getDomainName" not in block
    assert "_HIERARCHY" not in block


def test_hierarchy_constant_and_domain_name_helper_removed():
    js = BREADCRUMB_JS.read_text(encoding="utf-8")
    assert "_HIERARCHY" not in js
    assert "_getDomainName" not in js
    assert "currentDomainName" not in js


def test_build_crumbs_returns_single_matched_route_crumb():
    js = BREADCRUMB_JS.read_text(encoding="utf-8")
    assert "return [{ label: matched.label, icon: matched.icon, href: path }];" in js


def test_init_only_hides_breadcrumb_when_route_unmatched():
    js = BREADCRUMB_JS.read_text(encoding="utf-8")
    # Previously hid whenever there were <= 1 crumbs (i.e. whenever there was
    # no Registry/Domain ancestor) — now must only hide on a true no-match.
    assert "if (crumbs.length === 0) return;" in js
    assert "if (crumbs.length <= 1) return;" not in js


def test_route_map_still_covers_all_main_menus():
    js = BREADCRUMB_JS.read_text(encoding="utf-8")
    for route, label in [
        ("/registry/", "Registry"),
        ("/domain/", "Domain"),
        ("/ontology/", "Ontology"),
        ("/mapping/", "Mapping"),
        ("/dtwin/", "Knowledge Graph"),
        ("/settings", "Settings"),
    ]:
        assert f"'{route}':" in js
        assert f"label: '{label}'" in js
