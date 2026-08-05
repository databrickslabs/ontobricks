"""Help Center static copy must name all Graph DB backends, including Neo4j."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_HELP = Path("src/front/templates/partials/layout/help_modal.html")


def _help_html() -> str:
    return _HELP.read_text(encoding="utf-8")


def test_faq_lists_neo4j_as_graph_backend():
    html = _help_html()
    assert "Which triple store backends are supported?" in html
    assert 'id="faq3"' in html
    # FAQ body must name all three Graph DB engines, not Lakebase-only.
    assert "Neo4j" in html
    assert "Lakebase" in html
    assert "Lakehouse" in html
    assert "currently <strong>Lakebase</strong> Postgres" not in html
    assert 'data-help-doc="neo4j-requirements"' in html


def test_glossary_defines_neo4j():
    html = _help_html()
    assert "<dt>Neo4j</dt>" in html
    assert "typed property graph" in html


def test_resources_link_to_neo4j_guide():
    html = _help_html()
    assert 'data-help-doc="neo4j-requirements"' in html
    assert ">Neo4j Backend<" in html
