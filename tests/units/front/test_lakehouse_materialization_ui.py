"""UI contracts for the Lakehouse Materialization option.

The setting decides whether a domain's ``…_data`` is a Delta table or a
pass-through view over the R2RML gateway. Three things have to hold for the
option to be usable at all, and none of them is visible from the backend
tests:

1. The picker only appears for the Lakehouse backend — Lakebase and Neo4j
   ignore the setting, so offering it there would promise something the
   resolver refuses to honour.
2. Both save paths send the field. ``domain.js`` and the pre-UC-save
   auto-save in ``navbar.js`` post to the same ``/domain/info``; a field
   missing from either silently reverts the user's choice.
3. The pages that name the object adapt to the mode. Calling a view a
   "Delta table" and its live count a stored row count hides the cost of the
   mode from whoever chose it.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
INFO_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"
INFO_JS = REPO_ROOT / "src/front/static/domain/js/domain-information.js"
DOMAIN_JS = REPO_ROOT / "src/front/static/domain/js/domain.js"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"
BUILD_JS = REPO_ROOT / "src/front/static/query/js/query-databricks-build.js"
BUILD_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_databricks_build.html"

SELECT_ID = "domainLakehouseMaterialization"
SECTION_ID = "lakehouseMaterializationSection"
FIELD = "lakehouse_materialization"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _function_body(source: str, signature: str, span: int = 2500) -> str:
    start = source.index(signature)
    return source[start : start + span]


def _picker_markup() -> str:
    """The section's opening tag through its ``</select>``.

    That slice holds every conditional in the block, so rendering it answers
    what the page actually does rather than what its source text contains.
    """
    html = _read(INFO_HTML)
    anchor = html.index(f'id="{SECTION_ID}"')
    start = html.rindex("<div", 0, anchor)
    end = html.index("</select>", anchor) + len("</select>")
    return html[start:end]


def _render_picker(**domain) -> str:
    from jinja2 import Environment

    template = Environment(autoescape=True).from_string(_picker_markup())
    return template.render(domain=domain)


class TestThePicker:
    def test_the_select_offers_exactly_the_two_supported_modes(self):
        rendered = _render_picker(graph_backend="databricks")
        assert f'id="{SELECT_ID}"' in rendered
        assert '<option value="table"' in rendered
        assert '<option value="view"' in rendered

    def test_the_section_is_shown_for_lakehouse(self):
        assert "d-none" not in _render_picker(graph_backend="databricks")

    @pytest.mark.parametrize("backend", ["lakebase", "neo4j"])
    def test_the_section_is_hidden_for_backends_that_ignore_it(self, backend):
        """Offering it would promise something the resolver refuses to honour."""
        assert "d-none" in _render_picker(graph_backend=backend)

    def test_a_stored_view_only_choice_is_preselected(self):
        rendered = _render_picker(
            graph_backend="databricks", lakehouse_materialization="view"
        )
        assert '<option value="view" selected' in rendered
        assert '<option value="table" selected' not in rendered

    def test_the_default_selection_is_the_materialized_table(self):
        """A domain saved before the option existed must not render view-only."""
        rendered = _render_picker(graph_backend="databricks")
        assert '<option value="table" selected' in rendered
        assert '<option value="view" selected' not in rendered

    def test_the_saved_value_baseline_defaults_to_table(self):
        """``dataset.savedValue`` drives re-selection after a refresh."""
        assert 'data-saved-value="table"' in _render_picker(graph_backend="databricks")

    def test_the_help_text_names_what_stays_a_table(self):
        """Users need to know inferred triples and Analytics are unaffected."""
        html = _read(INFO_HTML)
        block_start = html.index(f'id="{SECTION_ID}"')
        block = html[block_start : block_start + 2500]
        assert "Inferred triples" in block
        assert "Graph Analytics" in block


class TestTheToggleIsWired:
    def test_the_toggle_runs_on_every_backend_change(self):
        js = _read(INFO_JS)
        assert "function toggleLakehouseMaterializationSection()" in js
        assert (
            "graphBackendEl.addEventListener('change', toggleLakehouseMaterializationSection);"
            in js
        )

    def test_the_toggle_keys_off_the_lakehouse_backend_value(self):
        body = _function_body(
            _read(INFO_JS), "function toggleLakehouseMaterializationSection()", span=400
        )
        assert "'databricks'" in body
        assert "d-none" in body

    def test_the_toggle_also_runs_on_init_and_after_rehydration(self):
        """Two calls beyond the listener: page load, and the /domain/info reload."""
        js = _read(INFO_JS)
        assert js.count("toggleLakehouseMaterializationSection();") >= 2

    def test_rehydration_does_not_overwrite_an_unsaved_choice(self):
        js = _read(INFO_JS)
        start = js.index("const materializationEl = document.getElementById")
        block = js[start : start + 500]
        assert "dataset.userEdited" in block
        assert f"infoData.info.{FIELD}" in block


class TestBothSavePathsSendTheField:
    def test_the_shared_payload_builder_includes_the_field(self):
        body = _function_body(
            _read(NAVBAR_JS), "function buildDomainInfoPayload(", span=2500
        )
        assert f"{FIELD}:" in body

    def test_the_payload_resets_the_field_off_lakehouse(self):
        """A domain that leaves Lakehouse must not keep a view-only setting."""
        body = _function_body(
            _read(NAVBAR_JS), "function buildDomainInfoPayload(", span=2500
        )
        start = body.index(f"{FIELD}:")
        clause = body[start : start + 300]
        assert "'databricks'" in clause
        assert "'table'" in clause

    def test_the_explicit_save_fallback_includes_the_field(self):
        """``saveDomainInfo`` falls back to its own literal when the builder is absent."""
        body = _function_body(_read(DOMAIN_JS), "async function saveDomainInfo(", span=3000)
        assert f"{FIELD}:" in body
        assert "'databricks'" in body

    def test_a_successful_save_updates_the_selector_baseline(self):
        """Otherwise a later rehydration re-selects the pre-save value."""
        body = _function_body(_read(DOMAIN_JS), "async function saveDomainInfo(", span=4000)
        assert f"materializationEl.dataset.savedValue = domainInfoPayload.{FIELD}" in body


class TestThePagesNameTheObjectCorrectly:
    def test_the_settings_health_card_no_longer_depends_on_materialization(self):
        js = _read(SETTINGS_JS)
        assert "data.materialization === 'view'" not in js
        assert "live query" not in js

    def test_the_build_page_labels_its_target_from_the_mode(self):
        html = _read(BUILD_HTML)
        js = _read(BUILD_JS)
        # The label must be addressable for the JS to rewrite it.
        assert 'id="dbxBuildDataTableLabel"' in html
        assert "'Target view'" in js
        assert "'Target Delta table'" in js

    def test_the_build_page_status_card_adapts_to_the_mode(self):
        js = _read(BUILD_JS)
        assert "data.materialization === 'view'" in js
        assert "'View status'" in js
        assert "'Delta table status'" in js


class TestTheBuildPageNamesTheStorageKind:
    """Knowledge Graph → Build must say which of the two objects it builds.

    Same FQN, very different cost and freshness: a materialized copy is a
    point-in-time snapshot refreshed only by a build, a view is always live.
    Whoever lands on the page has to be able to tell which one they have
    without opening Domain → Information.
    """

    def test_the_badge_exists_and_is_addressable(self):
        html = _read(BUILD_HTML)
        assert 'id="dbxBuildStorageBadge"' in html
        assert 'id="dbxBuildStorageNote"' in html
        assert 'id="dbxBuildSubtitle"' in html

    def test_the_badge_names_both_kinds_explicitly(self):
        body = _function_body(
            _read(BUILD_JS), "function _applyDbxStorageKind(", span=2600
        )
        assert "VIEW" in body
        assert "TABLE" in body
        assert "no data copy" in body
        assert "materialized copy" in body

    def test_the_badge_is_driven_by_the_endpoint_field(self):
        js = _read(BUILD_JS)
        assert "_applyDbxStorageKind(isViewMode)" in js
        assert "const isViewMode = data.materialization === 'view';" in js

    def test_the_subtitle_stops_promising_a_copy_in_view_mode(self):
        body = _function_body(
            _read(BUILD_JS), "function _applyDbxStorageKind(", span=2600
        )
        assert "Expose mapped triples through a governed Unity Catalog view" in body
        assert "Materialize mapped triples into a governed Delta table" in body

    def test_the_note_explains_what_a_build_actually_does(self):
        body = _function_body(
            _read(BUILD_JS), "function _applyDbxStorageKind(", span=2600
        )
        assert "Build refreshes the gateway definition" in body
        assert "Inferred triples keep their own Delta table" in body
        assert "point-in-time snapshot" in body

    def test_the_badge_uses_a_stylesheet_class_not_an_inline_size(self):
        """`.cursor/11-frontend-design` forbids inline style on templates."""
        html = _read(BUILD_HTML)
        css = (REPO_ROOT / "src/front/static/query/css/query-sync.css").read_text(
            encoding="utf-8"
        )
        assert "dbx-storage-badge" in html
        assert ".dbx-storage-badge" in css
        badge_line = next(
            line for line in html.splitlines() if 'id="dbxBuildStorageBadge"' in line
        )
        assert "style=" not in badge_line
