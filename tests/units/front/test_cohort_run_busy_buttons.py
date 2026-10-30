"""Preview and Materialise show an in-button spinner and stay disabled while the request runs."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
COHORT_JS = REPO_ROOT / "src/front/static/query/js/query-cohorts.js"
RUN_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_cohorts.html"
MODALS = REPO_ROOT / "src/front/templates/partials/_cohort_modals.html"


def _js() -> str:
    return COHORT_JS.read_text(encoding="utf-8")


def test_preview_and_materialize_buttons_have_stable_ids():
    run = RUN_HTML.read_text(encoding="utf-8")
    assert 'id="cohortPreviewBtn"' in run
    assert 'id="cohortMaterializeBtn"' in run
    assert 'id="cohortMaterializeNowBtn"' in MODALS.read_text(encoding="utf-8")


def test_preview_and_materialize_set_busy_spinner_then_restore():
    src = _js()
    assert "spinner-border spinner-border-sm" in src
    assert "this._setBusy('cohortPreviewBtn'" in src
    assert "this._setBusy('cohortMaterializeNowBtn'" in src
    preview = src.split("async preview()", 1)[1].split("async materialize()", 1)[0]
    materialize = src.split("async materialize()", 1)[1].split(
        "// ---- Path trace", 1
    )[0]
    assert "finally" in preview
    assert "finally" in materialize
    assert "_setBusy('cohortPreviewBtn'" in preview
    assert "_setBusy('cohortMaterializeNowBtn'" in materialize
    assert "aria-busy" in src
