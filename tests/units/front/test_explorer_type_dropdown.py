"""The Explorer filter's entity-type list must not recount the graph on open.

It used to call ``/dtwin/sync/stats?refresh=true``, which bypasses the stats
cache and recomputes every aggregate (total, distinct subjects, type and
predicate distributions) over the whole triple store each time the Filter
panel opens: seconds to minutes on a large graph. It now fills from the
ontology, then narrows from the cached stats, which every build clears.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"


def _fn(source: str, name: str) -> str:
    """The body of a named function, up to the next declaration at its level."""
    header = "function " + name
    start = source.index(header)
    rest = source[start + len(header) :]
    ends = [
        i
        for i in (rest.find("\n    function "), rest.find("\n    async function "))
        if i != -1
    ]
    return rest[: min(ends)] if ends else rest


@pytest.fixture(scope="module")
def source() -> str:
    return JS.read_text(encoding="utf-8")


def test_type_list_does_not_bypass_the_stats_cache(source):
    assert "refresh=true" not in _fn(source, "_populateFilterEntityTypes")


def test_type_list_reads_the_cached_stats(source):
    assert "fetch('/dtwin/sync/stats'," in _fn(source, "_populateFilterEntityTypes")


def test_type_list_fills_from_the_ontology_first(source):
    body = _fn(source, "_populateFilterEntityTypes")
    assert body.index("_ontologyEntityTypes()") < body.index("/dtwin/sync/stats")


def test_ontology_types_come_from_the_loaded_ontology(source):
    assert "/ontology/get-loaded-ontology" in _fn(source, "_ontologyEntityTypes")


def test_re_rendering_keeps_the_users_selection(source):
    body = _fn(source, "_renderStatsDropdown")
    assert "var previous = sel.value;" in body and "sel.value = previous;" in body
