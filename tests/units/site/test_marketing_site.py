"""Source contracts for the GitHub Pages marketing site under site/."""

from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SITE = REPO / "site"

REQUIRED_PAGES = [
    "index.html",
    "features/index.html",
    "get-started/index.html",
    "docs/index.html",
    "about/index.html",
    "404.html",
]

REQUIRED_ASSETS = [
    "assets/css/site.css",
    "assets/js/site.js",
    "assets/img/ontobricks-icon.svg",
]


def test_required_files_exist():
    missing = [p for p in REQUIRED_PAGES + REQUIRED_ASSETS if not (SITE / p).is_file()]
    assert missing == [], f"Missing site files: {missing}"


def test_css_defines_ob_tokens():
    css = (SITE / "assets/css/site.css").read_text(encoding="utf-8")
    for token in ("--ob-primary", "--ob-bg", "--ob-text", "--ob-font-display", "--ob-font-body"):
        assert token in css, f"Missing CSS token {token}"


def test_home_has_hero_and_ctas():
    html = (SITE / "index.html").read_text(encoding="utf-8")
    assert "Knowledge Graph Builder for Databricks" in html
    assert "github.com/databrickslabs/ontobricks" in html
    assert "get-started" in html


def test_pages_use_relative_assets():
    for page in REQUIRED_PAGES:
        if page == "404.html":
            continue
        html = (SITE / page).read_text(encoding="utf-8")
        assert "assets/css/site.css" in html
        assert "assets/js/site.js" in html
        assert 'href="/assets/' not in html
        assert 'src="/assets/' not in html


def test_site_js_has_nav_and_motion_hooks():
    js = (SITE / "assets/js/site.js").read_text(encoding="utf-8")
    assert "nav-toggle" in js or "data-nav-toggle" in js
    assert "prefers-reduced-motion" in js


def test_about_has_labs_disclaimer():
    html = (SITE / "about/index.html").read_text(encoding="utf-8")
    assert "AS-IS" in html or "as-is" in html.lower()
    assert "Service Level Agreements" in html or "SLA" in html


def test_docs_hub_links_github_markdown():
    html = (SITE / "docs/index.html").read_text(encoding="utf-8")
    assert "github.com/databrickslabs/ontobricks/blob/" in html
    assert "architecture.md" in html or "features.md" in html
