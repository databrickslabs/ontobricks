"""Read-only/viewer gating for the staged Generate wizard's Stage 2
(Review) / Stage 3 (Complete) mutation controls.

Plan task 5 of ``staged-ontology-generate`` review fix: a read-only-version
/ role-viewer / edit-lock-blocked caller could still include/exclude/edit/
remove candidate entities, discard the draft, continue to completion, or
retry a failed completion — none of those write surfaces were covered by
the centralized declarative gate in ``permissions.css`` (see its module
docstring). The pane and its data stay fully visible; only the controls
that mutate the draft are neutralised, matching every other write surface
gated in this file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PERMISSIONS_CSS = REPO_ROOT / "src/front/static/global/css/permissions.css"
HTML = REPO_ROOT / "src/front/templates/partials/ontology/_ontology_wizard.html"
REVIEW_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-wizard-review.js"

pytestmark = pytest.mark.unit


def _css() -> str:
    return PERMISSIONS_CSS.read_text(encoding="utf-8")


def _gated(css: str, selector: str) -> bool:
    """True when *selector* sits in a read-only *disable* rule (pointer-events:
    none), not merely the display:none hide rule. Mirrors the helper in
    ``test_view_mode_write_gates.py``."""
    idx = 0
    while True:
        pos = css.find(selector, idx)
        if pos < 0:
            return False
        rule_start = css.rfind("body:is(.read-only-version", 0, pos)
        brace = css.find("{", pos)
        props = css[brace : css.find("}", brace) + 1] if brace >= 0 else ""
        if rule_start >= 0 and "pointer-events: none" in props:
            return True
        idx = pos + len(selector)


@pytest.mark.parametrize(
    "action",
    [
        "wizard-review-add-candidate-toggle",
        "wizard-review-add-candidate-submit",
        "wizard-review-remove-candidate",
        "wizard-review-remove-alt-label",
        "wizard-review-add-evidence",
        "wizard-review-remove-evidence",
        "wizard-review-discard",
        "wizard-review-continue",
        "wizard-review-restart",
        "wizard-complete-retry",
        "wizard-complete-discard",
    ],
)
def test_every_review_and_complete_mutation_action_is_gated(action: str):
    css = _css()
    selector = f'[data-action="{action}"]'
    assert _gated(css, selector), f"{action} is not disabled on a read-only domain"


def test_gate_uses_important_to_win_over_btn_primary_styling():
    css = _css()
    block_start = css.index('[data-action="wizard-review-add-candidate-toggle"]')
    block = css[block_start : css.index("}", block_start) + 1]
    assert "!important" in block


def test_every_gated_action_actually_exists_in_the_wizard_markup_or_js():
    """Regression guard: a selector gated here but renamed/removed in the
    markup/JS would silently stop protecting anything."""
    html = HTML.read_text(encoding="utf-8")
    js = REVIEW_JS.read_text(encoding="utf-8")
    haystack = html + js
    for action in (
        "wizard-review-add-candidate-toggle",
        "wizard-review-add-candidate-submit",
        "wizard-review-remove-candidate",
        "wizard-review-remove-alt-label",
        "wizard-review-add-evidence",
        "wizard-review-remove-evidence",
        "wizard-review-discard",
        "wizard-review-continue",
        "wizard-review-restart",
        "wizard-complete-retry",
        "wizard-complete-discard",
    ):
        assert action in haystack, f"gated action {action} no longer exists"


def test_view_itself_stays_visible_not_hidden_by_the_gate():
    """The gate must disable controls, not hide the review/complete pane's
    own content — locked anchors, candidates, and the checklist must
    remain visible/inspectable for read-only callers."""
    css = _css()
    assert "#wizardLockedAnchorsList" not in css
    assert "#wizardCandidatesList" not in css
    assert "#wizardCompleteChecklist" not in css
