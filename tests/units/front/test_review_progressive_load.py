"""Contract: Context → Review loads piece-by-piece with per-section spinners."""

from pathlib import Path
import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_review.html"
JS = REPO_ROOT / "src/front/static/domain/js/domain-review.js"


def test_review_page_exposes_per_section_slots_with_spinners():
    html = HTML.read_text(encoding="utf-8")
    for slot_id in (
        "reviewBannerSlot",
        "reviewLifecycleSlot",
        "reviewReadiness",
        "reviewTeam",
        "reviewTimelineSlot",
    ):
        assert f'id="{slot_id}"' in html
    assert "spinner-border" in html
    # No single full-page blocking body spinner wrapping everything
    assert 'id="reviewBody"' in html


def test_review_js_fills_sections_progressively_in_parallel():
    js = JS.read_text(encoding="utf-8")
    assert "paintReviewSkeleton" in js or "reviewBannerSlot" in js
    assert "fetchOnce('/domain/version-status')" in js or 'fetchOnce("/domain/version-status")' in js
    assert "Promise.all" in js
    assert "loadReadiness()" in js
    assert "loadTeam()" in js
    # Must not wipe the whole body with one spinner before anything paints
    assert "Loading review status..." not in js or "reviewBannerSlot" in js
    assert "reviewLifecycleSlot" in js
    assert "reviewTimelineSlot" in js


def test_lifecycle_paints_optimistically_from_version_status():
    """Lifecycle stage is painted from version-status before review detail returns."""
    js = JS.read_text(encoding="utf-8")
    assert "function lifecyclePreview(status)" in js
    assert "function paintLifecyclePreview(status)" in js
    # Preview is painted right after the version-status check, before the detail fetch
    vs_check = js.index("showNotSavedMessage();\n                return;")
    paint = js.index("paintLifecyclePreview(vs.status);")
    detail = js.index("loadReviewDetail().catch(")
    assert vs_check < paint < detail
    # Footers show a loading placeholder until the detail arrives
    assert "pending: true" in js
    assert "if (pending)" in js
    assert "Loading details…" in js
