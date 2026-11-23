"""Original Graph readiness proposes Auto-Map when Mapping is not ready."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
SYNC_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sync.html"
SYNC_JS = REPO_ROOT / "src/front/static/query/js/query-sync.js"
DBX_JS = REPO_ROOT / "src/front/static/query/js/query-databricks-build.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_the_certified_backend_panel_carries_no_mapping_readiness():
    """Mapping readiness gates the Original build; it lives on the Original page only."""
    html = _read(SYNC_HTML)
    for marker in ("syncReadinessCard", "syncMappingReadyBadge", "syncAutoMapBtn", "syncNotReadyAlert"):
        assert marker not in html


def test_databricks_readiness_offers_automap_when_not_ready():
    js = _read(DBX_JS)
    assert "dbxBuildAutoMapBtn" in js
    assert "AutoAssignModule.open" in js
    assert 'href="/dtwin/?section=design">Mapping</a>' in js
    assert "Click <strong>Build</strong>" in js
    # Button sits on the Mapping row, not only in the alert.
    assert "Mapping</span>" in js or "bi-diagram-2" in js
    assert js.index("dbxBuildAutoMapBtn") < js.index("Click <strong>Build</strong>")
