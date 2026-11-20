"""Contract: Ontology → Data Quality header exposes a primary "+ Add" button.

"+ Add" is a standalone primary button placed left of the discussion button,
outside the Auto-generate / Cleanup / SHACL button group.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DQ_HTML = (
    ROOT / "src/front/templates/partials/ontology/_ontology_dataquality.html"
).read_text(encoding="utf-8")


def _header() -> str:
    start = DQ_HTML.index('class="section-header')
    end = DQ_HTML.index("</div>\n</div>", start)
    return DQ_HTML[start:end]


def test_add_button_is_primary_and_labelled_add():
    assert "DataQualityModule.openAddModal()" in DQ_HTML
    assert "<i class=\"bi bi-plus-lg me-1\"></i> Add\n" in DQ_HTML
    assert "Add Rule" not in DQ_HTML
    add_start = DQ_HTML.index("DataQualityModule.openAddModal()")
    button_open = DQ_HTML.rindex("<button", 0, add_start)
    assert 'class="btn btn-sm btn-primary ontology-edit-btn"' in DQ_HTML[button_open:add_start]


def test_add_button_is_outside_the_group_and_left_of_discuss():
    header = _header()
    add_pos = header.index("DataQualityModule.openAddModal()")
    discuss_pos = header.index("onto-discuss-btn")
    group_pos = header.index("btn-group")
    assert group_pos < add_pos < discuss_pos
    # The action group must be closed before the Add button starts.
    assert "</div>" in header[group_pos:add_pos]
    assert "btn-group" not in header[add_pos:discuss_pos]
