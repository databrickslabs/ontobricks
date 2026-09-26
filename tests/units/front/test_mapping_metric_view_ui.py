"""UI contract for metric-view data sources in the domain metadata picker.

UC Metric Views load alongside tables/views (``object_kind`` on each entry).
The picker and the loaded-sources list must surface that kind so users can tell
a metric view apart from a plain table before mapping it — a metric view is
queried with ``MEASURE()`` + ``GROUP BY``, never ``SELECT *``.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
METADATA_JS = REPO_ROOT / "src/front/static/domain/js/domain-metadata.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestTheObjectKindHelperExists:
    def test_there_is_a_single_kind_meta_helper(self):
        js = _read(METADATA_JS)
        assert "function objectKindMeta(" in js

    def test_it_recognises_the_three_kinds(self):
        js = _read(METADATA_JS)
        helper = js[js.index("function objectKindMeta(") :]
        helper = helper[: helper.index("\n}\n")]
        assert "'metric_view'" in helper
        assert "'view'" in helper
        # table is the default branch
        assert "case 'table':" in helper or "default:" in helper


class TestMetricViewsAreVisuallyDistinct:
    def test_metric_views_carry_a_dedicated_badge(self):
        js = _read(METADATA_JS)
        helper = js[js.index("function objectKindMeta(") :]
        helper = helper[: helper.index("\n}\n")]
        assert "Metric View" in helper
        # plain tables get no badge
        assert "badge: ''" in helper


class TestBothListsUseTheKindHelper:
    def test_the_import_picker_row_uses_the_helper(self):
        js = _read(METADATA_JS)
        # picker builds rows over allAvailableTables
        picker = js[js.index("allAvailableTables.forEach(table =>") :]
        picker = picker[: picker.index("tbody.innerHTML")]
        assert "objectKindMeta(table.object_kind)" in picker

    def test_the_loaded_sources_row_uses_the_helper(self):
        js = _read(METADATA_JS)
        loaded = js[js.index("metadata.tables.forEach((table, index)") :]
        loaded = loaded[: loaded.index("tbody.innerHTML")]
        assert "objectKindMeta(table.object_kind)" in loaded
