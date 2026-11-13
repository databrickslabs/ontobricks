"""Front contracts for Context → Pages (Genie search → Generate selection)."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MENU = REPO_ROOT / "src/front/config/menu_config.json"
DOMAIN_HTML = REPO_ROOT / "src/front/templates/domain.html"
PAGES_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_pages.html"
PAGES_JS = REPO_ROOT / "src/front/static/domain/js/domain-pages.js"
PAGES_CSS = REPO_ROOT / "src/front/static/domain/css/domain-pages.css"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_menu_has_pages_under_metadata():
    import json

    menus = {m["id"]: m for m in json.loads(_read(MENU))["menus"]}
    domain = menus["domain"]
    meta = next(g for g in domain["groups"] if g["id"] == "domain-metadata")
    ids = [i["id"] for i in meta["items"]]
    assert "documents" in ids
    assert "pages" in ids
    assert ids.index("pages") > ids.index("documents")
    pages = next(i for i in meta["items"] if i["id"] == "pages")
    assert pages["label"] == "Pages"
    assert pages["requires"] == "domain_saved"
    assert pages["icon"] == "bi-journal-richtext"


def test_domain_shell_includes_pages_section_and_assets():
    html = _read(DOMAIN_HTML)
    assert 'id="pages-section"' in html
    assert "_domain_pages.html" in html
    assert "domain/js/domain-pages.js" in html
    assert "domain/css/domain-pages.css" in html


def test_pages_partial_has_search_first_controls():
    html = _read(PAGES_HTML)
    for cid in (
        "pagesSearchForm",
        "pagesSearchInput",
        "pagesSearchBtn",
        "pagesDomainFilter",
        "pagesStateFilter",
        "pagesSearchBody",
        "pagesSearchMoreBtn",
        "pagesSelectedList",
        "pagesSaveSelectionBtn",
        "pagesClearSelectionBtn",
        "pagesPreviewModal",
    ):
        assert f'id="{cid}"' in html
    # No browse-all table on load — search engine + curated list.
    assert "pagesBrowseBody" not in html
    assert "pagesRefreshBtn" not in html
    assert "Genie Pages search" in html
    assert "Pages for Ontology Generate" in html
    assert "Ontology → Generate" in html or "Ontology &rarr; Generate" in html
    # Must not use a native <form> submit (reloads /domain/ → General).
    assert "<form" not in html.lower()
    assert 'type="submit"' not in html
    assert 'id="pagesSearchBtn"' in html and 'type="button"' in html


def test_pages_js_search_first_no_autoload_browse():
    js = _read(PAGES_JS)
    assert "/domain/pages/list" in js
    assert "/domain/pages/selection" in js
    assert "/domain/pages/preview" in js
    assert "PagesManager" in js
    assert "runSearch" in js
    assert "Save selection" in js
    # Must not auto-load the full page catalog on init.
    assert "reloadBrowse" not in js
    assert "this.loadSelection().then(() => this.reloadBrowse())" not in js
    assert "runSearch" in js
    assert "data-pages-add" in js
    assert "pagesSearchBtn" in js
    assert "keydown" in js


def test_pages_preview_renders_markdown_body_as_html():
    """Preview converts Markdown body to HTML via renderMarkdown / marked+DOMPurify."""
    js = _read(PAGES_JS)
    assert "_renderPageHtml(p.body)" in js
    assert "renderMarkdown" in js
    assert "marked.parse" in js
    assert "DOMPurify.sanitize" in js
    assert "_esc(p.body" not in js


def test_pages_selected_list_shows_loading_spinner():
    html = _read(PAGES_HTML)
    assert "Loading selected pages" in html
    assert "spinner-border" in html
    js = _read(PAGES_JS)
    assert "_showSelectedLoading" in js


def test_pages_js_has_no_typographic_quotes():
    """Smart quotes inside JS string literals broke parsing of the whole file."""
    js = _read(PAGES_JS)
    for ch in ("\u2018", "\u2019", "\u201c", "\u201d"):
        assert ch not in js


def test_domain_shell_loads_timed_task_log_before_query_sync():
    html = _read(DOMAIN_HTML)
    assert "query/js/timed-task-log.js" in html
    assert html.index("query/js/timed-task-log.js") < html.index("query/js/query-sync.js")


def test_pages_title_is_preview_hyperlink_instead_of_preview_button():
    """Page name itself carries data-pages-preview; standalone Preview button removed."""
    js = _read(PAGES_JS)
    assert 'class="fw-semibold text-decoration-none pages-title-link" data-pages-preview=' in js
    assert ">Preview</button>" not in js


def test_pages_css_exists():
    css = _read(PAGES_CSS)
    assert ".pages-search-results" in css
    assert ".pages-preview-body" in css
    assert ".pages-title-link" in css
