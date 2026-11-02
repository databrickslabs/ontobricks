"""UI contracts for the per-domain Materialization mode picker.

The picker chooses how a rebuild treats the mapped triples (full rebuild or one
of the incremental modes). Three things have to hold for the choice to stick,
and none of them is visible from the backend tests:

1. The picker is shown for every backend. Unlike the Lakehouse access type it
   is not tied to one engine, so it must not live inside that section.
2. Both save paths send the field. ``domain.js`` and the pre-UC-save auto-save
   in ``navbar.js`` post to the same ``/domain/info``; a field missing from
   either silently reverts the user's choice.
3. Real-time can be chosen; its source fields show only for that mode.
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
INFO_HTML = REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"
INFO_JS = REPO_ROOT / "src/front/static/domain/js/domain-information.js"
DOMAIN_JS = REPO_ROOT / "src/front/static/domain/js/domain.js"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"

SELECT_ID = "domainMaterializationMode"
SECTION_ID = "materializationModeSection"
LAKEHOUSE_SECTION_ID = "lakehouseMaterializationSection"
FIELD = "materialization_mode"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _function_body(source: str, signature: str, span: int = 2500) -> str:
    start = source.index(signature)
    return source[start : start + span]


def _picker_markup() -> str:
    """The section's opening tag through its ``</select>``."""
    html = _read(INFO_HTML)
    anchor = html.index(f'id="{SECTION_ID}"')
    start = html.rindex("<div", 0, anchor)
    end = html.index("</select>", anchor) + len("</select>")
    return html[start:end]


def _render_picker(**domain) -> str:
    from jinja2 import Environment

    template = Environment(autoescape=True).from_string(_picker_markup())
    return template.render(domain=domain)


def _option(rendered: str, value: str) -> str:
    match = re.search(rf'<option value="{value}"[^>]*>[^<]*</option>', rendered)
    assert match, f"option {value!r} not rendered"
    return match.group(0)


class TestThePicker:
    def test_the_select_is_an_editable_form_select(self):
        rendered = _render_picker()
        assert f'id="{SELECT_ID}"' in rendered
        assert "form-select" in rendered
        assert "domain-editable" in rendered

    def test_the_options_are_in_ladder_order(self):
        rendered = _render_picker()
        values = re.findall(r'<option value="([^"]+)"', rendered)
        assert values == ["full", "incremental", "microbatch", "realtime"]

    def test_the_option_labels(self):
        rendered = _render_picker()
        assert "Full rebuild" in _option(rendered, "full")
        assert "Incremental" in _option(rendered, "incremental")
        assert "Micro-batch" in _option(rendered, "microbatch")
        assert "Real-time" in _option(rendered, "realtime")

    @pytest.mark.parametrize("value", ["full", "incremental", "microbatch", "realtime"])
    def test_every_option_is_enabled(self, value):
        assert "disabled" not in _option(_render_picker(), value)

    @pytest.mark.parametrize(
        "backend", ["lakebase", "databricks", "neo4j", "none", None]
    )
    def test_the_section_is_visible_for_every_backend(self, backend):
        assert "d-none" not in _render_picker(graph_backend=backend)

    def test_the_default_selection_is_full_rebuild(self):
        rendered = _render_picker()
        assert "selected" in _option(rendered, "full")
        for value in ("incremental", "microbatch", "realtime"):
            assert "selected" not in _option(rendered, value)

    @pytest.mark.parametrize("value", ["incremental", "microbatch"])
    def test_a_stored_choice_is_preselected(self, value):
        rendered = _render_picker(materialization_mode=value)
        assert "selected" in _option(rendered, value)
        assert "selected" not in _option(rendered, "full")

    def test_a_stored_realtime_choice_is_not_silently_replaced(self):
        """A stored realtime choice is re-selected after a refresh."""
        rendered = _render_picker(materialization_mode="realtime")
        assert "selected" in _option(rendered, "realtime")
        assert "selected" not in _option(rendered, "full")

    def test_the_saved_value_baseline_defaults_to_full(self):
        """``dataset.savedValue`` drives re-selection after a refresh."""
        assert 'data-saved-value="full"' in _render_picker()

    def test_the_saved_value_baseline_follows_the_stored_choice(self):
        rendered = _render_picker(materialization_mode="incremental")
        assert 'data-saved-value="incremental"' in rendered

    def test_the_help_text_names_the_fallback(self):
        html = _read(INFO_HTML)
        block_start = html.index(f'id="{SECTION_ID}"')
        block = html[block_start : block_start + 2500]
        assert "Full rebuild" in block
        assert "falls back" in block

    def test_the_markup_has_no_inline_style_or_script(self):
        markup = _picker_markup()
        assert "style=" not in markup
        assert "<script" not in markup


class TestThePickerIsNotInsideTheLakehouseSection:
    def test_the_picker_follows_the_lakehouse_section(self):
        html = _read(INFO_HTML)
        assert html.index(f'id="{LAKEHOUSE_SECTION_ID}"') < html.index(
            f'id="{SECTION_ID}"'
        )

    def test_the_lakehouse_section_closes_before_the_picker_opens(self):
        """A child of the Lakehouse block would be hidden on every other backend."""
        html = _read(INFO_HTML)
        lakehouse = html.index(f'id="{LAKEHOUSE_SECTION_ID}"')
        picker = html.rindex("<div", 0, html.index(f'id="{SECTION_ID}"'))
        between = html[lakehouse:picker]
        # The slice starts inside the Lakehouse section's own opening tag, so a
        # sibling leaves exactly one more closing tag than opening tags.
        assert between.count("</div>") - between.count("<div") == 1


class TestTheFormTracksEdits:
    def test_an_edit_marks_the_control_dirty(self):
        js = _read(INFO_JS)
        start = js.index("const materializationModeElInit = document.getElementById")
        block = js[start : start + 400]
        assert "'domainMaterializationMode'" in block
        assert "dataset.userEdited" in block

    def test_rehydration_does_not_overwrite_an_unsaved_choice(self):
        js = _read(INFO_JS)
        start = js.index("const materializationModeEl = document.getElementById")
        block = js[start : start + 600]
        assert "dataset.userEdited" in block
        assert f"infoData.info.{FIELD}" in block
        assert "dataset.savedValue" in block


class TestBothSavePathsSendTheField:
    def test_the_shared_payload_builder_includes_the_field(self):
        body = _function_body(
            _read(NAVBAR_JS), "function buildDomainInfoPayload(", span=3500
        )
        assert "'domainMaterializationMode'" in body
        assert f"{FIELD}:" in body

    def test_the_payload_sends_the_field_for_every_backend(self):
        """Unlike the Lakehouse access type, the mode is not reset off one backend."""
        body = _function_body(
            _read(NAVBAR_JS), "function buildDomainInfoPayload(", span=3500
        )
        start = body.index(f"{FIELD}:")
        clause = body[start:].splitlines()[0]
        assert "graphBackendEl" not in clause

    def test_the_explicit_save_fallback_includes_the_field(self):
        """``saveDomainInfo`` falls back to its own literal when the builder is absent."""
        body = _function_body(_read(DOMAIN_JS), "async function saveDomainInfo(", span=3500)
        assert "'domainMaterializationMode'" in body
        assert f"{FIELD}:" in body

    def test_a_successful_save_updates_the_selector_baseline(self):
        """Otherwise a later rehydration re-selects the pre-save value."""
        body = _function_body(_read(DOMAIN_JS), "async function saveDomainInfo(", span=4500)
        assert (
            f"materializationModeEl.dataset.savedValue = domainInfoPayload.{FIELD}"
            in body
        )


class TestTheRealtimeSource:
    def _section(self, realtime_source=None, **domain) -> str:
        from jinja2 import Environment

        from back.core.graphdb import RealtimeSource

        html = _read(INFO_HTML)
        start = html.rindex("<fieldset", 0, html.index('id="realtimeSourceSection"'))
        end = html.index("</fieldset>", start) + len("</fieldset>")
        source = RealtimeSource.coerce(realtime_source)
        return Environment(autoescape=True).from_string(html[start:end]).render(
            domain=domain,
            realtime_source=source,
            realtime_kinds=RealtimeSource.kind_options(source.get("kind", "")),
        )

    def test_the_fields_show_only_for_the_realtime_mode(self):
        hidden = self._section(materialization_mode="microbatch")
        shown = self._section(materialization_mode="realtime")
        assert 'class="border rounded p-2 mt-2 d-none"' in hidden
        assert "d-none" not in shown.split(">", 1)[0]

    def test_the_stored_names_are_rendered(self):
        rt = {"kind": "kinesis", "bootstrap_servers": "us-west-2", "topic": "orders", "secret_scope": "sc", "secret_key": "k"}

        rendered = self._section(materialization_mode="realtime", realtime_source=rt)

        assert 'value="orders"' in rendered and 'value="sc"' in rendered
        assert '<option value="kinesis" selected>' in rendered

    def test_kinesis_is_offered_as_experimental_and_kafka_is_the_default(self):
        rendered = self._section(materialization_mode="realtime")

        assert ">Kinesis (experimental)</option>" in rendered
        assert '<option value="kafka" selected>' in rendered

    def test_the_route_not_the_template_builds_the_kind_options(self):
        html = _read(INFO_HTML)
        assert "{% set rt" not in html and "('kinesis', 'Kinesis')" not in html
        route = _read(REPO_ROOT / "src/front/routes/domain.py")
        assert '"realtime_kinds": RealtimeSource.kind_options(' in route

    def test_no_secret_value_field_exists(self):
        section = self._section(materialization_mode="realtime").lower()
        assert "password" not in section and 'type="password"' not in section

    def test_the_payload_sends_the_names_and_the_mode_toggle_is_wired(self):
        body = _function_body(_read(NAVBAR_JS), "function buildDomainInfoPayload(", span=4500)
        assert "realtime_source:" in body and "'realtimeSourceSecretKey'" in body
        js = _read(INFO_JS)
        assert "addEventListener('change', toggleRealtimeSourceSection)" in js
