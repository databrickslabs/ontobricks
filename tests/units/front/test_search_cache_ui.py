"""UI contracts for Domain → General → Backend Search cache."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
INFO_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"
INFO_JS = REPO_ROOT / "src/front/static/domain/js/domain-information.js"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
DOMAIN_JS = REPO_ROOT / "src/front/static/domain/js/domain.js"

SECTION_ID = "searchCacheSection"
CHECK_ID = "domainGraphCacheEnabled"
FIELD = "graph_cache_enabled"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _section_markup() -> str:
    html = _read(INFO_HTML)
    anchor = html.index(f'id="{SECTION_ID}"')
    start = html.rindex("<div", 0, anchor)
    end = html.index(f'id="{CHECK_ID}"', anchor)
    end = html.index(">", end) + 1
    return html[start:end]


def _render(**domain) -> str:
    from jinja2 import Environment

    return Environment(autoescape=True).from_string(_section_markup()).render(
        domain=domain
    )


class TestVisibility:
    def test_shown_for_lakebase(self):
        assert "d-none" not in _render(graph_backend="lakebase")

    def test_shown_for_lakehouse(self):
        assert "d-none" not in _render(graph_backend="databricks")

    @pytest.mark.parametrize("backend", ["neo4j", "none"])
    def test_hidden_for_other_backends(self, backend):
        assert "d-none" in _render(graph_backend=backend)

    def test_switch_id_present(self):
        assert f'id="{CHECK_ID}"' in _render(graph_backend="lakebase")

    def test_default_checked(self):
        assert "checked" in _render(graph_backend="lakebase")


class TestWiring:
    def test_toggle_function_exists(self):
        js = _read(INFO_JS)
        assert "function toggleSearchCacheSection()" in js
        assert "graphBackendEl.addEventListener('change', toggleSearchCacheSection);" in js

    def test_dirty_flag(self):
        assert "cacheElInit.dataset.userEdited = '1'" in _read(INFO_JS)

    def test_status_line_helper(self):
        js = _read(INFO_JS)
        assert "function refreshSearchCacheStatusLine" in js
        assert "Off — live reads" in js

    def test_navbar_payload_sends_field(self):
        assert FIELD in _read(NAVBAR_JS)

    def test_domain_js_fallback_sends_field(self):
        assert FIELD in _read(DOMAIN_JS)
