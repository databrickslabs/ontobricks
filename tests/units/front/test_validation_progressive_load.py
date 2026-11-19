"""Context → General loads per section with spinners (no full-page wait)."""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_validation.html"
JS = REPO_ROOT / "src/front/static/domain/js/domain-validation.js"
API = REPO_ROOT / "src/api/routers/internal/home.py"
SERVICE = REPO_ROOT / "src/back/objects/domain/HomeService.py"


def test_general_page_has_no_full_page_blocking_overlay():
    html = HTML.read_text(encoding="utf-8")
    assert "validationLoadingOverlay" not in html
    assert "validation-loading-overlay" not in html


def test_general_page_sections_expose_per_section_spinners():
    html = HTML.read_text(encoding="utf-8")
    for section_id in (
        "validationAssetsSpinner",
        "validationOntologySpinner",
        "validationMappingSpinner",
        "validationGraphSpinner",
        "domainHealthBannerSpinner",
    ):
        assert f'id="{section_id}"' in html


def test_validation_js_loads_overview_documents_and_graph_in_parallel():
    js = JS.read_text(encoding="utf-8")
    assert "/validate/detailed?parts=" in js
    assert "fetchValidationPart('overview')" in js
    assert "fetchValidationPart('documents')" in js
    assert "fetchValidationPart('graph')" in js
    assert "Promise.all" in js
    assert "hideValidationLoadingOverlay" not in js
    assert "validationLoadingOverlay" not in js


def test_validate_detailed_accepts_parts_query_param():
    api = API.read_text(encoding="utf-8")
    assert "parts" in api
    assert re.search(
        r"async def validate_detailed\([\s\S]*?parts",
        api,
    )
    service = SERVICE.read_text(encoding="utf-8")
    assert "parts" in service
    assert '"overview"' in service and '"documents"' in service and '"graph"' in service
