"""Inferred-tab provenance badge: if_then renders as the friendly "Simple" label."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
REASONING_JS = (
    ROOT / "src/front/static/query/js/query-reasoning.js"
).read_text(encoding="utf-8")


def test_provenance_label_maps_if_then_to_simple():
    # The friendly-label map exists and renames the rule families.
    labels = REASONING_JS[REASONING_JS.index("_PROVENANCE_LABELS:"):REASONING_JS.index("_provenanceLabel(")]
    assert "if_then: 'Simple'" in labels
    assert "sparql: 'Complex'" in labels


def test_inferred_badge_uses_provenance_label():
    # The Inferred-tab badge goes through the label helper, not the raw string.
    assert "_provenanceLabel(t.provenance)" in REASONING_JS
    # The raw provenance is no longer rendered directly in the badge.
    assert "badge bg-secondary\">${this._esc(t.provenance)}" not in REASONING_JS
