"""Contract tests for relationship arrow direction in the Mapping designer.

A property flagged ``direction: 'reverse'`` emits its triples target → source
(see ``R2RMLGenerator._generate_relationship``), so both canvases must draw the
arrowhead at the source end. The Ontology map does this with a
``auto-start-reverse`` marker plus a ``reverse`` CSS class; the Mapping designer
must behave identically, only in its own mapped / unmapped / excluded colours.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"
DESIGN_CSS = REPO_ROOT / "src/front/static/mapping/css/mapping-design.css"
ONTOLOGY_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-map.css"

START_MARKERS = (
    "mapping-arrow-start-mapped",
    "mapping-arrow-start-unmapped",
    "mapping-arrow-start-excluded",
)


def _design_js() -> str:
    return DESIGN_JS.read_text(encoding="utf-8")


def _design_css() -> str:
    return DESIGN_CSS.read_text(encoding="utf-8")


class TestStartMarkers:
    def test_one_start_marker_per_link_state(self):
        source = _design_js()
        for marker in START_MARKERS:
            assert marker in source, f"{marker} must be defined in <defs>"

    def test_start_markers_are_rotated(self):
        """Without auto-start-reverse the head points away from the node."""
        assert "'auto-start-reverse'" in _design_js()


class TestReverseClass:
    def test_reverse_links_carry_the_class(self):
        assert "d.direction === 'reverse' ? ' reverse' : ''" in _design_js()

    def test_reverse_is_additive_to_the_mapping_state_class(self):
        """A reverse link is still mapped/unmapped/excluded — both must show."""
        source = _design_js()
        idx = source.index("d.direction === 'reverse' ? ' reverse' : ''")
        assert "d.mapped ? 'mapped' : 'unmapped'" in source[idx - 200 : idx]


class TestReverseCss:
    def test_every_state_swaps_end_for_start(self):
        css = _design_css()
        for state, marker in (
            ("mapped", "mapping-arrow-start-mapped"),
            ("unmapped", "mapping-arrow-start-unmapped"),
            ("excluded", "mapping-arrow-start-excluded"),
        ):
            rule = f".mapping-map-link.reverse.{state}"
            assert rule in css, f"{rule} must exist"
            block = css[css.index(rule) : css.index(rule) + 220]
            assert "marker-end: none" in block
            assert f"url(#{marker})" in block

    def test_excluded_override_stays_important(self):
        """The base .excluded rule sets marker-end with !important."""
        css = _design_css()
        block = css[css.index(".mapping-map-link.reverse.excluded") :][:220]
        assert "marker-end: none !important" in block

    def test_matches_the_ontology_map_contract(self):
        assert ".map-link.reverse" in ONTOLOGY_CSS.read_text(encoding="utf-8")
