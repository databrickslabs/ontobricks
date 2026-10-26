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
METADATA_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_metadata.html"


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


FRONT_ROOT = REPO_ROOT / "src/front"
_COMMENT_PREFIXES = ("//", "*", "/*", "<!--", "#")


def test_no_ui_label_still_says_data_source():
    """Sources include tables, views and metric views — the UI calls them data assets."""
    offenders = []
    for path in FRONT_ROOT.rglob("*"):
        if path.suffix not in {".html", ".js", ".json"} or not path.is_file():
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith(_COMMENT_PREFIXES):
                continue
            lowered = line.lower()
            if "data source" in lowered:
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")
    assert not offenders, offenders


class TestAddAssetsCopy:
    def test_the_catalog_schema_modal_confirms_with_add_assets(self):
        html = _read(METADATA_HTML)
        confirm = html[html.index('id="loadMetadataConfirmBtn"') :]
        confirm = confirm[: confirm.index("</button>")]
        assert "Add assets" in confirm
        assert "Load Tables" not in confirm
        assert "Add Tables" not in confirm

    def test_the_import_picker_counts_assets_not_tables(self):
        js = _read(METADATA_JS)
        body = js[js.index("function updateImportSelectionCount(") :]
        body = body[: body.index("\n}\n")]
        assert "Add ${selectedCount} asset" in body
        assert "Import ${selectedCount} Table" not in body
