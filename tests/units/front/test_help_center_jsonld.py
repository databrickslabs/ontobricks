"""Help Center and About page must advertise W3C JSON-LD support."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_HELP = Path("src/front/templates/partials/layout/help_modal.html")
_ABOUT = Path("src/front/templates/about.html")


def test_glossary_defines_jsonld():
    html = _HELP.read_text(encoding="utf-8")
    assert "<dt>JSON-LD</dt>" in html
    assert "@context" in html
    assert "describe_ontology" in html


def test_w3c_resources_link_to_jsonld_spec():
    html = _HELP.read_text(encoding="utf-8")
    assert "https://www.w3.org/TR/json-ld11/" in html
    assert "JSON-LD 1.1" in html


def test_faq_mentions_jsonld_owl_serialization():
    html = _HELP.read_text(encoding="utf-8")
    assert "JSON-LD" in html
    assert "Turtle or <strong>JSON-LD</strong>" in html


def test_about_page_lists_jsonld_capability_and_spec():
    html = _ABOUT.read_text(encoding="utf-8")
    assert "JSON-LD" in html
    assert "https://www.w3.org/TR/json-ld11/" in html
