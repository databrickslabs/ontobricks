"""Explorer visualizes materialized clusters; it does not run detection."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
EXPLORER = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"


def test_explorer_has_no_detect_controls():
    html = EXPLORER.read_text(encoding="utf-8")
    assert "sgClusterAlgorithm" not in html
    assert "detectClusters" not in html
    assert "detectClustersBackend" not in html
    assert "sgClusterColorBy" in html
    assert "Intelligence → Community" in html or "Intelligence &rarr; Community" in html


def test_sigma_hydrates_from_current_endpoint():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "/dtwin/clusters/current" in js
    assert "_ensureMaterializedClusters" in js
    # Memberships are looked up for drawn nodes only, never loaded wholesale.
    assert "/dtwin/clusters/assignments" in js
    assert "cl.members" not in js
    assert "function _detectClusters" not in js
    assert "detectClusters:" not in js
    assert "detectClustersBackend:" not in js
