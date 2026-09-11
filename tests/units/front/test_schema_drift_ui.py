"""Contract tests for the schema-drift surfaces in the Mapping designer.

Drift is advisory: the mapping is well-formed, the upstream table moved. The UI
must therefore read as a warning everywhere it appears (canvas, panels,
Diagnostics) and must never prevent the designer from loading when the drift
check itself is unavailable.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"
DIAGNOSTICS_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-diagnostics.js"
DESIGN_CSS = REPO_ROOT / "src/front/static/mapping/css/mapping-design.css"


def _design() -> str:
    return DESIGN_JS.read_text(encoding="utf-8")


def _diagnostics() -> str:
    return DIAGNOSTICS_JS.read_text(encoding="utf-8")


def _brace_block_after(source: str, start: int) -> str:
    i = source.index("{", start) + 1
    depth = 1
    body_start = i
    while i < len(source) and depth > 0:
        if source[i] == "{":
            depth += 1
        elif source[i] == "}":
            depth -= 1
        i += 1
    return source[body_start : i - 1] if depth == 0 else ""


def _function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?:async\s+)?function\s+{re.escape(name)}\s*\([^)]*\)\s*\{{", source
    )
    assert match, f"{name}() must exist"
    return _brace_block_after(source, match.end() - 1)


class TestDriftFetch:
    def test_calls_the_dedicated_drift_endpoint(self):
        """Not /mapping/diagnostics — that runs SELECT probes and row counts."""
        body = _function_body(_design(), "loadSchemaDrift")
        assert "/mapping/schema-drift" in body
        assert "/mapping/diagnostics" not in body

    def test_failure_leaves_the_designer_usable(self):
        body = _function_body(_design(), "loadSchemaDrift")
        assert "catch" in body
        assert "throw" not in body
        assert body.count("MappingDriftState.entities = {}") >= 1, (
            "an error must reset the state rather than leave it stale"
        )

    def test_unsuccessful_response_yields_empty_state(self):
        body = _function_body(_design(), "loadSchemaDrift")
        assert "data.success && data.entities" in body
        assert "data.success && data.relationships" in body

    def test_fetched_without_blocking_the_spinner(self):
        """Drift is advisory, so it must never block the designer spinner.

        The previous approach used Promise.all which forced the spinner to wait
        ~19 s for the warehouse query.  The new approach fires loadSchemaDrift()
        as a fire-and-forget background call so the spinner hides as soon as the
        fast session-local loadMapLayout() resolves.
        """
        source = _design()
        # Schema-drift is NOT in the blocking await path
        assert "Promise.all([loadMapLayout(), loadSchemaDrift()])" not in source
        # Schema-drift fires in the background via .then()
        assert "loadSchemaDrift().then(" in source
        # Map layout is still awaited (session-local, fast)
        assert "await loadMapLayout()" in source

    def test_state_holds_both_entities_and_relationships(self):
        source = _design()
        match = re.search(r"const MappingDriftState = \{(.*?)\};", source, re.S)
        assert match, "MappingDriftState must be declared"
        assert "entities" in match.group(1)
        assert "relationships" in match.group(1)


class TestCanvasMarker:
    def test_nodes_carry_their_drifted_columns(self):
        assert "MappingDriftState.entities[cls.uri]?.columns || []" in _design()

    def test_marker_only_on_affected_nodes(self):
        source = _design()
        assert "nodeElements.filter(d => d.driftedColumns.length > 0)" in source
        assert "mapping-map-node-drift" in source

    def test_marker_does_not_replace_the_mapping_status_ring(self):
        """A drifted entity is still mapped/unmapped/excluded — both must show."""
        source = _design()
        assert "mappingStatus: mappingStatus" in source
        drift_idx = source.index("nodeElements.filter(d => d.driftedColumns")
        assert "mapping-map-node-hitarea" in source[:drift_idx], (
            "the status ring is drawn independently of the drift marker"
        )

    def test_tooltip_names_the_missing_columns(self):
        source = _design()
        assert "Schema drift — missing in source:" in source
        assert "d.driftedColumns.join(', ')" in source

    def test_tooltip_unchanged_when_there_is_no_drift(self):
        source = _design()
        assert "d.driftedColumns.length" in source
        assert "statusLabels[d.mappingStatus]" in source

    def test_css_defines_the_marker(self):
        css = DESIGN_CSS.read_text(encoding="utf-8")
        assert ".mapping-map-node-drift" in css
        assert "pointer-events: none" in css


class TestPanelBanners:
    def test_entity_panel_banner_lists_missing_columns(self):
        source = _design()
        assert "MappingDriftState.entities[classUri]?.columns || []" in source
        assert "epDrift.length ?" in source
        assert "Source schema changed." in source

    def test_relationship_panel_banner_lists_missing_columns(self):
        source = _design()
        assert (
            "MappingDriftState.relationships[ontologyProperty.uri]?.columns || []"
            in source
        )
        assert "rpDrift.length ?" in source

    def test_banners_use_warning_styling_not_danger(self):
        source = _design()
        for var in ("epDrift", "rpDrift"):
            idx = source.index(f"${{{var}.length ?")
            block = source[idx : idx + 500]
            assert "alert-warning" in block, f"{var} banner must be a warning"
            assert "alert-danger" not in block

    def test_entity_banner_tells_the_user_what_to_do(self):
        source = _design()
        assert "Remap them or restore the column in the source table." in source

    def test_no_banner_when_there_is_no_drift(self):
        """Both banners are conditional, so a clean mapping renders as before."""
        source = _design()
        assert source.count("Source schema changed.") == 2
        assert "epDrift.length ?" in source and "rpDrift.length ?" in source

    def test_column_header_badge_marks_drifted_columns(self):
        body = _function_body(_design(), "renderEntityPanelGrid")
        assert "EntityPanelState.driftedColumns.has(col)" in body
        assert "bg-warning" in body

    def test_header_badge_is_additive_to_the_mapping_badge(self):
        """A drifted ID column must still show its ID badge."""
        body = _function_body(_design(), "renderEntityPanelGrid")
        idx = body.index("EntityPanelState.driftedColumns.has(col)")
        assert "badge +=" in body[idx : idx + 200], (
            "the drift badge must append to, not overwrite, the existing badge"
        )

    def test_panel_state_is_seeded_per_entity(self):
        source = _design()
        assert "EntityPanelState.driftedColumns = driftedColumnsForEntity(classUri)" in (
            source
        )

    def test_lookup_helper_returns_a_set(self):
        body = _function_body(_design(), "driftedColumnsForEntity")
        assert "new Set(" in body
        assert "MappingDriftState.entities[classUri]?.columns || []" in body


class TestDiagnosticsRendering:
    def test_drift_checks_are_recognised_by_prefix(self):
        body = _function_body(_diagnostics(), "_isDrift")
        assert "schema_drift:" in body

    def test_drift_rows_get_a_distinguishing_icon(self):
        body = _function_body(_diagnostics(), "_renderChecks")
        assert "bi-database-exclamation" in body
        assert "diag-check-drift" in body

    def test_drift_row_label_strips_the_prefix_and_annotates(self):
        body = _function_body(_diagnostics(), "_renderChecks")
        assert "replace('schema_drift:', '')" in body
        assert "(schema drift)" in body

    def test_non_drift_rows_keep_the_status_icon(self):
        body = _function_body(_diagnostics(), "_renderChecks")
        assert "_icon(c.status)" in body

    def test_header_badge_counts_drifted_columns(self):
        body = _function_body(_diagnostics(), "_driftBadge")
        assert ".filter(_isDrift).length" in body
        assert "drifted" in body

    def test_header_badge_hidden_when_clean(self):
        body = _function_body(_diagnostics(), "_driftBadge")
        assert "if (!n) return ''" in body

    def test_badge_shown_on_both_entities_and_relationships(self):
        source = _diagnostics()
        assert "_driftBadge(ent.checks)" in source
        assert "_driftBadge(rel.checks)" in source

    def test_badge_reads_as_a_warning_not_an_error(self):
        body = _function_body(_diagnostics(), "_driftBadge")
        assert "bg-warning" in body
        assert "bg-danger" not in body
