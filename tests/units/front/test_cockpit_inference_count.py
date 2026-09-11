"""UI contracts for the Cockpit materialized-inference metric."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
HTML = ROOT / "src/front/templates/partials/domain/_domain_validation.html"
JS = ROOT / "src/front/static/domain/js/domain-validation.js"


def test_cockpit_exposes_materialized_inference_metric():
    html = HTML.read_text(encoding="utf-8")
    assert 'id="psDtMaterializedInferenceCount"' in html
    assert "Materialized inferences" in html
    assert "data-materialized-inference-count" in html


def test_cockpit_loads_lightweight_inference_status():
    js = JS.read_text(encoding="utf-8")
    assert "loadMaterializedInferenceCount()" in js
    assert "'/dtwin/reasoning/inferred'" in js
    assert "materialized_inference_count" in js
    assert "'N/A'" in js
