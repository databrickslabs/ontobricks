"""Build readiness proposes Auto-Map when Mapping is not ready."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
SYNC_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sync.html"
SYNC_JS = REPO_ROOT / "src/front/static/query/js/query-sync.js"
DBX_JS = REPO_ROOT / "src/front/static/query/js/query-databricks-build.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_lakebase_readiness_has_automap_button_next_to_mapping():
    html = _read(SYNC_HTML)
    row = html[html.index("syncMappingReadyBadge") : html.index("syncNotReadyAlert")]
    assert 'id="syncAutoMapBtn"' in row
    assert 'href="/mapping/?section=autoassign"' in row
    assert "bi-lightning-charge" in row
    assert ">Auto-Map<" in row or "Auto-Map\n" in row


def test_lakebase_not_ready_alert_lists_automap_then_build():
    html = _read(SYNC_HTML)
    alert = html[html.index("syncNotReadyAlert") : html.index("<!-- Knowledge Graph Target -->")]
    assert "<ol" in alert
    assert "Auto-Map" in alert
    assert 'href="/mapping/?section=design">Studio</a>' in alert
    assert alert.index("Auto-Map") < alert.index("Build")
    assert "materialize the graph" in alert


def test_apply_readiness_shows_automap_only_when_mapping_not_ready():
    js = _read(SYNC_JS)
    body = js[js.index("function _applyReadiness(") :]
    body = body[: body.index("\nfunction ")]
    assert "syncAutoMapBtn" in body
    assert "canEditOntology" in body
    assert "assignmentReady || !canOfferAutoMap" in body


def test_databricks_readiness_offers_automap_when_not_ready():
    js = _read(DBX_JS)
    assert "dbxBuildAutoMapBtn" in js or "/mapping/?section=autoassign" in js
    assert "Run <strong>Auto-Map</strong>" in js
    assert 'href="/mapping/?section=design">Studio</a>' in js
    assert "Click <strong>Build</strong>" in js
    # Button sits on the Mapping row, not only in the alert.
    assert "Mapping</span>" in js or "bi-diagram-2" in js
    assert js.index("/mapping/?section=autoassign") < js.index("Run <strong>Auto-Map</strong>")
