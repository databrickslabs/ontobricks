"""Contract tests for the entity external-link badge in the Ontology Designer.

Guards that the ``hasExternal`` boolean flag is computed and threaded into
*all four* entity-construction sites in ``ontology-design.js``, and that the
badge markup and CSS are consistent with the computed flag.
"""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DESIGN_JS = REPO_ROOT / "src/front/static/global/js/ontology-design.js"
ONTOVIZ_JS = REPO_ROOT / "src/front/static/global/ontoviz/ontoviz.js"
ONTOVIZ_CSS = REPO_ROOT / "src/front/static/global/ontoviz/css/ontoviz-entity.css"

# The canonical hasExternal boolean expression — common to all four sites
# (three use the object-literal form `hasExternal: !!(...)`, one uses the
# assignment form `entity.hasExternal = !!(...)` — both contain this substring).
_HAS_EXTERNAL_BOOL = (
    "!!(cls.dashboard || cls.dataset || "
    "(cls.actions || []).length || (cls.bridges || []).length)"
)


def test_has_external_present_at_all_four_construction_sites():
    """The hasExternal boolean expression must appear exactly four times in ontology-design.js.

    Three sites are inside loadOntologyIntoDesigner; the fourth is inside
    _buildFreshDesignLayout (added in the final-review fix pass).
    """
    js = DESIGN_JS.read_text(encoding="utf-8")
    count = js.count(_HAS_EXTERNAL_BOOL)
    assert count == 4, (
        f"Expected 4 occurrences of the hasExternal boolean expression in ontology-design.js, "
        f"found {count}. A fourth site (_buildFreshDesignLayout) may be missing."
    )


def test_badge_markup_class_present_in_ontoviz():
    """Badge span class must be present in the ontoviz renderer."""
    js = ONTOVIZ_JS.read_text(encoding="utf-8")
    assert "ovz-entity-external-badge" in js


def test_badge_icon_class_present_in_ontoviz():
    """Badge must use the Bootstrap Icon bi-link-45deg."""
    js = ONTOVIZ_JS.read_text(encoding="utf-8")
    assert "bi-link-45deg" in js


def test_badge_has_accessible_name():
    """Badge span must carry an aria-label for screen readers."""
    js = ONTOVIZ_JS.read_text(encoding="utf-8")
    assert 'aria-label="Has external configuration"' in js


def test_badge_icon_is_aria_hidden():
    """Decorative inner icon must be hidden from assistive technology."""
    js = ONTOVIZ_JS.read_text(encoding="utf-8")
    assert 'aria-hidden="true"' in js


def test_badge_css_rule_present():
    """The .ovz-entity-external-badge CSS class must be defined."""
    css = ONTOVIZ_CSS.read_text(encoding="utf-8")
    assert ".ovz-entity-external-badge" in css


def test_ontology_version_fingerprint_includes_external_fields():
    """_getOntologyVersion() fingerprint must reference dashboard, dataset, actions, bridges."""
    js = DESIGN_JS.read_text(encoding="utf-8")
    assert "function _getOntologyVersion()" in js
    # Find the function body by locating text near the fingerprint.
    assert "c.dashboard ? '1' : '0'" in js
    assert "c.dataset ? '1' : '0'" in js
    assert "(c.actions || []).length" in js
    assert "(c.bridges || []).length" in js
