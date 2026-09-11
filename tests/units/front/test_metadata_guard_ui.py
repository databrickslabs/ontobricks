"""Contract tests for the Data Sources deletion guard and refresh diff preview.

Both flows are confirmation gates in front of destructive/irreversible metadata
writes, so what matters is the *wiring*: the impact endpoint is consulted before
the confirm, the save is genuinely gated behind the user's answer, and neither
gate can wedge the page when its pre-flight fails.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
METADATA_JS = REPO_ROOT / "src/front/static/domain/js/domain-metadata.js"
UTILS_JS = REPO_ROOT / "src/front/static/global/js/utils.js"


def _js() -> str:
    return METADATA_JS.read_text(encoding="utf-8")


def _brace_block_after(source: str, start: int) -> str:
    """Return the inner text of the ``{ ... }`` block opened at or after *start*."""
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
    assert match, f"{name}() must exist in domain-metadata.js"
    return _brace_block_after(source, match.end() - 1)


# ── Item 1: deletion guard ─────────────────────────────────────────────────────

class TestRemovalImpactPreflight:
    def test_calls_the_removal_impact_endpoint(self):
        body = _function_body(_js(), "fetchRemovalImpact")
        assert "/domain/metadata/removal-impact" in body
        assert "table_names" in body

    def test_skips_the_round_trip_when_nothing_is_being_removed(self):
        body = _function_body(_js(), "fetchRemovalImpact")
        assert "if (!identifiers.length) return {}" in body

    def test_failed_preflight_degrades_instead_of_blocking(self):
        """A removal must never become impossible because the check errored."""
        body = _function_body(_js(), "fetchRemovalImpact")
        assert "catch" in body
        assert body.count("return {}") >= 2, (
            "both the error path and the unsuccessful-response path must "
            "fall back to an empty impact map"
        )
        assert "throw" not in body


class TestRemovalImpactDialog:
    def test_no_impact_keeps_the_plain_confirm(self):
        body = _function_body(_js(), "confirmRemovalWithImpact")
        assert "if (!affectedTables) return showConfirmDialog(plain)" in body

    def test_impact_escalates_to_a_danger_detail_dialog(self):
        body = _function_body(_js(), "confirmRemovalWithImpact")
        assert "title: 'Data Sources Still In Use'" in body
        assert "headerClass: 'bg-danger text-white'" in body
        assert "detailHtml: buildRemovalImpactHtml(impact)" in body
        assert "confirmText: 'Remove Anyway'" in body

    def test_dialog_warns_that_generated_artefacts_are_cleared(self):
        body = _function_body(_js(), "confirmRemovalWithImpact")
        assert "R2RML" in body

    def test_referrer_counts_are_deduplicated(self):
        """One mapping referencing two doomed tables is still one mapping."""
        body = _function_body(_js(), "confirmRemovalWithImpact")
        assert "new Set(Object.values(impact).flat()).size" in body

    def test_detail_html_escapes_table_and_referrer_names(self):
        body = _function_body(_js(), "buildRemovalImpactHtml")
        assert "escapeHtml(table)" in body
        assert "escapeHtml(r)" in body


class TestRemovalCallSites:
    def test_remove_selected_tables_goes_through_the_guard(self):
        body = _function_body(_js(), "removeSelectedTables")
        assert "confirmRemovalWithImpact(" in body
        assert "tablesToRemove.map(t => t.full_name || t.name)" in body

    def test_clear_metadata_goes_through_the_guard(self):
        body = _function_body(_js(), "clearMetadata")
        assert "confirmRemovalWithImpact(allTables" in body
        assert "metadataCache?.tables || []" in body

    def test_both_call_sites_still_abort_on_decline(self):
        for name in ("removeSelectedTables", "clearMetadata"):
            body = _function_body(_js(), name)
            assert "if (!confirmed) return;" in body, f"{name} must honour cancel"

    def test_guard_runs_before_the_destructive_request(self):
        for name, endpoint in (
            ("removeSelectedTables", "/domain/metadata/save"),
            ("clearMetadata", "/domain/metadata/clear"),
        ):
            body = _function_body(_js(), name)
            assert body.index("confirmRemovalWithImpact") < body.index(endpoint), (
                f"{name} must confirm before calling {endpoint}"
            )


# ── Item 2: metadata refresh diff preview ──────────────────────────────────────

class TestColumnDiffRendering:
    def test_renders_every_change_kind(self):
        body = _function_body(_js(), "buildColumnDiffHtml")
        for key in ("added", "removed", "type_changed"):
            assert f"tableDiff.{key}" in body
        assert "Added" in body and "Removed" in body and "Type changed" in body

    def test_type_change_shows_both_types(self):
        body = _function_body(_js(), "buildColumnDiffHtml")
        assert "c.old_type" in body and "c.new_type" in body

    def test_removals_are_listed_first(self):
        """Dropped columns are the consequential half of the diff."""
        body = _function_body(_js(), "buildColumnDiffHtml")
        assert "[...removed, ...typeChanged, ...added]" in body

    def test_unchanged_columns_are_summarised_not_listed(self):
        body = _function_body(_js(), "buildColumnDiffHtml")
        assert "unchangedCount" in body
        assert "unchanged" in body

    def test_escapes_table_and_column_names(self):
        body = _function_body(_js(), "buildColumnDiffHtml")
        assert "escapeHtml(table)" in body
        assert "escapeHtml(name)" in body


class TestDiffConfirmDialog:
    def test_uses_a_large_review_dialog(self):
        body = _function_body(_js(), "confirmMetadataDiff")
        assert "title: 'Review Metadata Changes'" in body
        assert "size: 'modal-lg'" in body
        assert "detailHtml: buildColumnDiffHtml(diff)" in body

    def test_cancel_is_labelled_as_a_discard(self):
        body = _function_body(_js(), "confirmMetadataDiff")
        assert "confirmText: 'Apply Changes'" in body
        assert "cancelText: 'Discard'" in body

    def test_dropped_columns_get_an_extra_warning(self):
        body = _function_body(_js(), "confirmMetadataDiff")
        assert "droppedCount" in body
        assert "diagnostics" in body.lower()

    def test_change_count_spans_all_three_kinds(self):
        body = _function_body(_js(), "confirmMetadataDiff")
        for key in ("added", "removed", "type_changed"):
            assert f"d.{key} || []" in body


class TestSaveIsGatedOnTheDiff:
    def _monitor(self) -> str:
        return _function_body(_js(), "monitorMetadataTask")

    def test_reads_the_diff_from_the_task_result(self):
        assert "task.result?.diff || {}" in self._monitor()

    def test_an_empty_diff_saves_without_prompting(self):
        """A no-op refresh must not make the user click through a modal."""
        assert "Object.keys(diff).length && !await confirmMetadataDiff(diff)" in (
            self._monitor()
        )

    def test_declining_skips_the_save_entirely(self):
        body = self._monitor()
        gate = body.index("confirmMetadataDiff(diff)")
        save = body.index("/domain/metadata/save")
        between = body[gate:save]
        assert "break;" in between, (
            "the decline branch must break out before reaching the save call"
        )
        assert "Metadata changes discarded" in between

    def test_declining_reloads_the_stored_state(self):
        """Discarding must leave the UI showing what is actually persisted."""
        body = self._monitor()
        gate = body.index("confirmMetadataDiff(diff)")
        assert "loadMetadataStatus()" in body[gate : body.index("/domain/metadata/save")]

    def test_confirming_still_persists_via_the_save_endpoint(self):
        body = self._monitor()
        assert "/domain/metadata/save" in body
        assert "JSON.stringify({ tables })" in body


# ── Shared dialog affordance ───────────────────────────────────────────────────

class TestConfirmDialogDetailSupport:
    def _confirm_body(self) -> str:
        source = UTILS_JS.read_text(encoding="utf-8")
        start = source.index("function showConfirmDialog")
        return source[start : source.index("function showInfoDialog", start)]

    def test_accepts_the_new_options(self):
        body = self._confirm_body()
        for option in ("detailHtml", "size", "headerClass"):
            assert f"{option} =" in body, f"{option} must be a destructured option"

    def test_detail_panel_is_scrollable_and_only_rendered_when_given(self):
        body = self._confirm_body()
        assert re.search(r"detailBlock\s*=\s*detailHtml\s*\n?\s*\?", body), (
            "the detail panel must be conditional on detailHtml being supplied"
        )
        assert "overflow-y: auto" in body
        assert "${detailBlock}" in body, "the panel must be interpolated into the body"

    def test_defaults_keep_the_previous_markup(self):
        """Existing callers pass none of the new options."""
        body = self._confirm_body()
        assert "detailHtml = ''" in body
        assert "size = ''" in body
        assert "headerClass = ''" in body

    def test_dark_header_gets_a_light_close_button(self):
        body = self._confirm_body()
        assert "btn-close-white" in body
        assert "headerClass.includes('text-white')" in body

    def test_still_stacks_above_an_open_modal(self):
        assert "showStackedModal(modalEl)" in self._confirm_body()
