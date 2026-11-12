"""UI contracts for Domain → General → Instance IRI scope."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
INFO_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"
INFO_JS = REPO_ROOT / "src/front/static/domain/js/domain-information.js"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
DOMAIN_JS = REPO_ROOT / "src/front/static/domain/js/domain.js"
LOADERS_JS = REPO_ROOT / "src/front/static/query/js/query-loaders.js"
D3GRAPH_JS = REPO_ROOT / "src/front/static/query/js/query-d3graph.js"
SELECT_ID = "domainIdentityScope"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _render(**domain) -> str:
    from jinja2 import Environment

    html = _read(INFO_HTML)
    start = html.rindex("<div", 0, html.index('id="identityScopeSection"'))
    end = html.index("</select>", start) + len("</select>")
    return Environment(autoescape=True).from_string(html[start:end]).render(domain=domain)


def test_select_offers_both_scopes():
    markup = _render()
    assert f'id="{SELECT_ID}"' in markup
    assert 'value="class"' in markup and 'value="hierarchy"' in markup


def test_class_is_selected_by_default():
    markup = _render()
    assert 'data-saved-value="class"' in markup


def test_saved_hierarchy_is_selected():
    markup = _render(identity_scope="hierarchy")
    assert 'data-saved-value="hierarchy"' in markup


@pytest.mark.parametrize("path", [NAVBAR_JS, DOMAIN_JS])
def test_save_payload_sends_identity_scope(path):
    js = _read(path)
    assert "identity_scope:" in js and SELECT_ID in js


def test_info_refresh_syncs_identity_scope():
    js = _read(INFO_JS)
    assert SELECT_ID in js and "infoData.info.identity_scope" in js


def test_loaders_keep_parent_and_expose_specificity_helper():
    js = _read(LOADERS_JS)
    assert "parent: cls.parent" in js
    assert "function isMoreSpecificType(" in js


def test_graph_parser_keeps_the_most_specific_type():
    js = _read(D3GRAPH_JS)
    assert "isMoreSpecificType(object, entity.typeUri)" in js
