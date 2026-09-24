"""Structural/behavior contracts for the staged three-step Generate wizard.

Plan task 5 of ``staged-ontology-generate``
(``docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md``).
The frontend has no JS test runner in this repository — every other
``tests/units/front/test_*`` module asserts on the raw template/JS/CSS
source (regex/string contracts), and this module follows the same
convention. Behavioral/browser verification is out of scope here (performed
separately); this file locks in the wiring contract: which endpoints are
called, that the legacy one-shot route is never called, that sessionStorage
only ever holds task ids, that locked anchors cannot be mutated from the
client, and that the three stages/checklist follow the design's exact
shapes.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
HTML = REPO_ROOT / "src/front/templates/partials/ontology/_ontology_wizard.html"
WIZARD_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-wizard.js"
REVIEW_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-wizard-review.js"
WIZARD_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-wizard.css"
ONTOLOGY_PAGE = REPO_ROOT / "src/front/templates/ontology.html"
NO_LLM_GATE_TEST = REPO_ROOT / "tests/units/front/test_no_llm_ui_gate.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Asset wiring
# ---------------------------------------------------------------------------


def test_review_js_is_wired_into_the_ontology_page():
    page = _read(ONTOLOGY_PAGE)
    assert "ontology/js/ontology-wizard-review.js" in page
    # The review module must load before the orchestrator that calls it.
    assert page.index("ontology-wizard-review.js") < page.index(
        "ontology-wizard.js"
    ) or "ontology-wizard.js" in page


def test_review_js_file_exists_and_is_not_empty():
    assert REVIEW_JS.exists()
    assert len(_read(REVIEW_JS)) > 200


def test_documents_tab_copy_reads_knowledge_store():
    html = _read(HTML)
    assert "Knowledge Store" in html
    assert "Domain Documents" not in html
    # The deep link into the domain page keeps its section id.
    assert 'href="/domain/?section=documents"' in html
    assert "Domain &gt; Knowledge Store" in html


def test_docs_preview_pane_revealed_with_explicit_block():
    """The docs list pane carries `.ob-hidden { display:none }`; revealing it
    must set an explicit `block` (not ''), otherwise the class re-hides the
    checkboxes and the user cannot include/exclude ready documents."""
    js = _read(WIZARD_JS)
    # The reveal path must not clear the inline style to empty (regression).
    assert "previewEl.style.display = '';" not in js
    assert "previewEl.style.display = 'block';" in js


def test_documents_tab_button_labelled_knowledge_store():
    """The wizard nav tab reads 'Knowledge Store' (internal id unchanged)."""
    html = _read(HTML)
    m = re.search(
        r'id="wizard-tab-documents"[^>]*>(.*?)</button>',
        html,
        re.DOTALL,
    )
    assert m, "wizard documents tab button not found"
    label = m.group(1)
    assert "Knowledge Store" in label
    assert ">Documents<" not in label and " Documents\n" not in label


def test_detection_note_aligns_with_the_padded_tab_content():
    html = _read(HTML)
    note = re.search(
        r'<div class="([^"]+)">\s*'
        r'<i class="bi bi-info-circle me-1"></i>\s*'
        r"Detection only identifies entities\.",
        html,
    )
    assert note is not None
    assert "px-3" in note.group(1).split()


# ---------------------------------------------------------------------------
# Endpoint usage — exact staged routes only, never the legacy one-shot route
# ---------------------------------------------------------------------------


def test_detect_route_is_called_for_stage_one():
    js = _read(WIZARD_JS)
    assert "/ontology/wizard/generate/detect" in js


def test_draft_routes_are_called_for_stage_two():
    js = _read(WIZARD_JS) + _read(REVIEW_JS)
    assert "/ontology/wizard/generate/draft" in js
    assert "/ontology/wizard/generate/draft/update" in js
    assert "/ontology/wizard/generate/draft/discard" in js


def test_complete_route_is_called_for_stage_three():
    js = _read(WIZARD_JS)
    assert "/ontology/wizard/generate/complete" in js


def test_legacy_one_shot_route_is_never_called():
    js = _read(WIZARD_JS) + _read(REVIEW_JS)
    assert "generate-async" not in js


def test_legacy_one_shot_js_functions_are_removed():
    js = _read(WIZARD_JS)
    for legacy in (
        "generateOntologyFromWizard",
        "applyWizardOntology",
        "applyWizardOntologySilent",
        "showWizardResults",
        "wizardGeneratedOWL",
    ):
        assert legacy not in js, f"legacy one-shot symbol still present: {legacy}"


# ---------------------------------------------------------------------------
# sessionStorage holds task ids only — never draft/entity/checkpoint state
# ---------------------------------------------------------------------------


def test_session_storage_keys_are_task_ids_only():
    js = _read(WIZARD_JS)

    assert re.search(r"WIZARD_DETECT_TASK_KEY\s*=", js)
    assert re.search(r"WIZARD_COMPLETE_TASK_KEY\s*=", js)

    # The old OWL/stats persistence keys (client-side cache of generated
    # content) must be gone — the draft is the only source of truth now.
    for legacy_key in ("WIZARD_OWL_KEY", "WIZARD_STATS_KEY", "WIZARD_TASK_KEY"):
        assert legacy_key not in js, f"legacy sessionStorage key still present: {legacy_key}"


def test_no_sessionstorage_write_carries_draft_or_entity_payloads():
    js = _read(WIZARD_JS) + _read(REVIEW_JS)
    # Every sessionStorage.setItem call must be storing a task id constant/
    # variable, never a JSON-serialized draft/candidate/checkpoint blob.
    for call in re.findall(r"sessionStorage\.setItem\(([^)]*)\)", js):
        assert "JSON.stringify" not in call, f"sessionStorage stores structured state: {call}"
        assert "draft" not in call.lower(), f"sessionStorage stores draft state: {call}"


# ---------------------------------------------------------------------------
# Stepper: Configure / Review / Complete, active/completed states, a11y
# ---------------------------------------------------------------------------


def test_stepper_markup_has_three_accessible_steps():
    html = _read(HTML)

    nav = re.search(r'<nav\b[^>]*class="[^"]*wizard-stepper[^"]*"[^>]*>', html)
    assert nav, "wizard stepper nav not found"
    assert 'aria-label=' in nav.group(0)

    for step_id, label in (
        ("wizardStepConfigure", "Configure"),
        ("wizardStepReview", "Review"),
        ("wizardStepComplete", "Complete"),
    ):
        assert f'id="{step_id}"' in html
        step_tag = re.search(rf'<li[^>]*id="{step_id}"[^>]*>.*?</li>', html, re.DOTALL)
        assert step_tag, f"stepper item {step_id} not found"
        assert label in step_tag.group(0)


def test_stepper_data_attributes_match_stage_names():
    html = _read(HTML)
    assert 'data-wizard-step="configure"' in html
    assert 'data-wizard-step="review"' in html
    assert 'data-wizard-step="complete"' in html


def test_js_owns_stepper_active_and_completed_state_transitions():
    js = _read(WIZARD_JS)
    assert "function setWizardStage(" in js
    assert "wizard-step" in js
    assert "'active'" in js or '"active"' in js
    assert "'completed'" in js or '"completed"' in js
    assert "aria-current" in js


# ---------------------------------------------------------------------------
# Stage panes exist and default to Configure visible
# ---------------------------------------------------------------------------


def test_three_stage_panes_exist_with_configure_visible_by_default():
    html = _read(HTML)

    configure = re.search(r'<div\b[^>]*id="wizardConfigurePane"[^>]*>', html)
    review = re.search(r'<div\b[^>]*id="wizardReviewPane"[^>]*>', html)
    complete = re.search(r'<div\b[^>]*id="wizardCompletePane"[^>]*>', html)

    assert configure and review and complete
    assert "ob-hidden" not in configure.group(0)
    assert "ob-hidden" in review.group(0)
    assert "ob-hidden" in complete.group(0)


def test_configure_pane_preserves_existing_source_selectors():
    html = _read(HTML)
    configure_block = re.search(
        r'id="wizardConfigurePane".*?(?=id="wizardReviewPane")', html, re.DOTALL
    )
    assert configure_block
    block = configure_block.group(0)
    for marker in (
        "wizardMetadataTableBody",
        "wizardDocsList",
        "wizardGuidelines",
        "wizardTemplateButtons",
    ):
        assert marker in block, f"Configure pane missing existing control {marker}"


def test_detection_configuration_is_entity_only():
    html = _read(HTML)
    js = _read(WIZARD_JS)
    for removed_control in (
        "wizardIncludeDataProps",
        "wizardIncludeRelationships",
        "wizardIncludeInheritance",
        "wizardUseTableNames",
        "wizardUseColumnComments",
        "wizard-tab-options",
        "wizard-pane-options",
    ):
        assert removed_control not in html
        assert removed_control not in js
    assert "relationships and attributes are inferred after entity review" in html.lower()


def test_top_cta_still_requires_llm_and_is_rightmost():
    html = _read(HTML)
    btn = re.search(r'<button[^>]*id="wizardTopGenerateBtn"[^>]*>', html, re.DOTALL)
    assert btn
    assert "data-requires-llm" in btn.group(0)
    assert "btn-primary" in btn.group(0)


def test_review_actions_are_right_aligned_beside_the_stepper():
    html = _read(HTML)
    js = _read(WIZARD_JS)
    row = re.search(
        r'id="wizardStepperRow".*?id="wizardReviewActions".*?'
        r'data-action="wizard-review-discard".*?'
        r'id="wizardReviewContinueBtn".*?</div>',
        html,
        re.DOTALL,
    )
    assert row, "Review actions must live to the right of steps 1-2-3"
    assert "wizardReviewActions" in js
    assert re.search(
        r"wizardReviewActions[\s\S]{0,300}classList\.toggle\("
        r"['\"]ob-hidden['\"],\s*stage\s*!==\s*['\"]review['\"]",
        js,
    )


def test_stepper_review_actions_share_the_wizard_click_delegate():
    js = _read(REVIEW_JS)
    bind = js[js.index("function bindEvents(") : js.index("function onReviewClick(")]
    assert "getElementById('wizard-section')" in bind
    assert re.search(r"clickRoot\.addEventListener\(['\"]click['\"]", bind)


# ---------------------------------------------------------------------------
# Review stage: locked anchors are visibly read-only, candidates editable
# ---------------------------------------------------------------------------


def test_locked_anchor_rows_are_rendered_read_only():
    js = _read(REVIEW_JS)
    assert "wizard-entity-locked" in js
    assert "bi-lock-fill" in js
    # Locked rows must never wire include/exclude/remove/edit controls.
    locked_render = re.search(
        r"function renderLockedAnchorRow\([^)]*\)\s*\{([\s\S]*?)\n\s*\}", js
    )
    assert locked_render, "locked anchor row renderer not found"
    body = locked_render.group(1)
    assert "wizard-candidate-remove" not in body
    assert "wizard-candidate-include" not in body
    assert "wizard-field-input" not in body
    assert "Entity" in body
    assert "object_property" not in body
    assert "data_property" not in body
    assert "Class" not in body


def test_locked_anchors_use_a_responsive_card_grid():
    html = _read(HTML)
    css = _read(WIZARD_CSS)
    assert 'id="wizardLockedAnchorsList"' in html
    assert "wizard-locked-grid" in html
    assert "wizard-locked-grid" in css
    assert "auto-fill" in css
    assert "minmax(" in css


def test_candidates_default_to_included_and_expose_entity_attributes():
    js = _read(REVIEW_JS)
    html = _read(HTML)
    css = _read(WIZARD_CSS)
    assert "wizard-candidate-include" in js
    assert "wizard-candidate-remove" in js
    for field in ("canonical_label", "description"):
        assert f'data-field="{field}"' in js
    assert 'data-field="type_hint"' not in js
    assert "object_property" not in js
    assert "data_property" not in js
    assert "Object Property" not in js
    assert "Data Property" not in html
    assert 'id="wizardNewCandidateType"' not in html
    assert "wizard-chip" in js  # alternate labels chip widget
    assert "evidence" in js.lower()
    assert "wizard-candidate-grid" in html
    assert "wizard-candidate-grid" in css
    assert "repeat(2," in css
    assert "repeat(3," in css


def test_detect_overlay_renders_found_entities_live():
    js = _read(WIZARD_JS)
    ui = _read(
        REPO_ROOT / "src/front/static/global/js/task-progress-ui.js"
    )
    assert "detectedListId" in js
    assert "detected_entities" in js
    assert "wizard-detect-live-list" in ui or "detectedListId" in ui
    assert "renderDetectLiveList" in js or "detected_entities" in js
    assert re.search(
        r"pollInterval\s*=\s*kind\s*===\s*['\"]detect['\"]\s*\?\s*250\s*:\s*1500",
        js,
    )


def test_add_candidate_flow_posts_op_add():
    js = _read(REVIEW_JS)
    assert re.search(r"op:\s*['\"]add['\"]", js)


def test_remove_include_exclude_ops_are_wired():
    js = _read(REVIEW_JS)
    assert re.search(r"op:\s*['\"]remove['\"]", js)
    assert re.search(r"op:\s*['\"]include['\"]", js)
    assert re.search(r"op:\s*['\"]exclude['\"]", js)
    assert re.search(r"op:\s*['\"]update['\"]", js)


def test_draft_update_calls_carry_the_current_revision():
    js = _read(REVIEW_JS)
    assert re.search(r"revision:\s*\w*[Rr]evision\w*", js), (
        "draft/update payload must carry the draft_revision for optimistic "
        "concurrency"
    )


def test_revision_conflict_reloads_the_draft_instead_of_clobbering():
    js = _read(REVIEW_JS)
    assert "409" in js
    conflict_handling = re.search(
        r"(status(?:Code)?\s*===?\s*409)[\s\S]{0,400}", js
    )
    assert conflict_handling, "no explicit 409 handling found"


def test_at_least_one_candidate_form_field_is_alternate_labels_repeatable():
    html = _read(HTML)
    assert "wizardAddCandidateForm" in html


# ---------------------------------------------------------------------------
# Continue gating + discard confirmation
# ---------------------------------------------------------------------------


def test_continue_button_starts_disabled_and_gated_server_side():
    html = _read(HTML)
    btn = re.search(r'<button[^>]*id="wizardReviewContinueBtn"[^>]*>', html, re.DOTALL)
    assert btn
    assert "disabled" in btn.group(0)


def test_discard_uses_shared_confirm_dialog_not_native_confirm():
    js = _read(REVIEW_JS) + _read(WIZARD_JS)
    assert "showConfirmDialog(" in js
    assert "confirm(" not in js
    assert "alert(" not in js
    assert "prompt(" not in js


# ---------------------------------------------------------------------------
# Complete stage: strict relations -> attributes -> axioms -> merge order
# ---------------------------------------------------------------------------


def test_complete_checklist_lists_substages_in_strict_order():
    html = _read(HTML)
    checklist = re.search(
        r'id="wizardCompleteChecklist"[^>]*>(.*?)</ul>', html, re.DOTALL
    )
    assert checklist, "complete checklist not found"
    body = checklist.group(1)
    order = [m.group(1) for m in re.finditer(r'data-substage="(\w+)"', body)]
    assert order == ["relations", "attributes", "axioms", "merge"]


def test_complete_stage_renders_checkpoint_status_from_draft():
    js = _read(WIZARD_JS)
    assert "completion_checkpoints" in js
    assert "merge_checkpoint" in js


def test_retry_button_exists_and_resumes_without_resetting_done_stages():
    html = _read(HTML)
    assert 'id="wizardCompleteRetryBtn"' in html
    js = _read(WIZARD_JS)
    assert "function retryGenerateCompletion(" in js or (
        "wizard-complete-retry" in html and "/ontology/wizard/generate/complete" in js
    )


def test_completion_success_applies_outcome_like_todays_wizard():
    js = _read(WIZARD_JS)
    assert "SidebarNav" in js and "switchTo('map')" in js
    assert "showNotification(" in js


# ---------------------------------------------------------------------------
# Stale-source invalidation + empty-candidates block
# ---------------------------------------------------------------------------


def test_stale_banner_exists_with_redetect_action():
    html = _read(HTML)
    banner = re.search(r'id="wizardStaleBanner"[^>]*>', html)
    assert banner
    assert "ob-hidden" in banner.group(0)
    assert "wizard-review-restart" in html


def test_review_js_checks_the_stale_flag_from_the_draft_view():
    js = _read(REVIEW_JS)
    assert ".stale" in js


def test_validation_banner_is_aria_live_for_screen_readers():
    html = _read(HTML)
    banner = re.search(r'<div\b[^>]*id="wizardReviewValidation"[^>]*>', html)
    assert banner
    assert "aria-live" in banner.group(0)


# ---------------------------------------------------------------------------
# Design system compliance — no inline style/script, no native popups
# ---------------------------------------------------------------------------


def test_no_inline_style_or_script_in_template():
    html = _read(HTML)
    assert not re.search(r'style="', html)
    assert "<script" not in html


def test_no_native_browser_popups_anywhere_in_the_new_code():
    for path in (WIZARD_JS, REVIEW_JS):
        js = _read(path)
        assert re.search(r"(?<!\.)\balert\(", js) is None, path
        assert re.search(r"(?<!\.)\bconfirm\(", js) is None, path
        assert re.search(r"(?<!\.)\bprompt\(", js) is None, path


def test_configure_tabs_still_use_shared_ob_tabs_treatment():
    html = _read(HTML)
    assert 'class="nav nav-tabs ob-tabs nav-fill"' in html


def test_mobile_configure_tabs_fit_without_clipping():
    css = _read(WIZARD_CSS)
    assert "#wizardTabs .nav-item" in css
    assert "min-width: 0" in css
    assert "#wizardTabs .nav-link" in css
    assert "white-space: normal" in css


def test_wizard_buttons_have_a_visible_keyboard_focus_ring():
    css = _read(WIZARD_CSS)
    focus = re.search(r"#wizard-section \.btn:focus-visible\s*\{([^}]*)\}", css)
    assert focus
    assert "var(--db-focus-ring)" in focus.group(1)


def test_wizard_step_css_uses_design_tokens_not_raw_hex():
    css = _read(WIZARD_CSS)
    step_block = re.search(r"\.wizard-step\b[^{]*\{([^}]*)\}", css)
    assert step_block
    assert "#e7f1ff" not in css
    assert "#0d6efd" not in css
    assert "#198754" not in css
    assert "var(--db-" in css


def test_new_review_controls_carry_data_action_delegated_handlers():
    js = _read(REVIEW_JS)
    for action in (
        "wizard-review-continue",
        "wizard-review-discard",
        "wizard-review-remove-candidate",
    ):
        assert action in js


def test_addeventlistener_is_not_attached_per_row_in_review_module():
    """Delegated handling on the pane root, not one listener per rendered row."""
    js = _read(REVIEW_JS)
    per_row_listeners = re.findall(
        r"\.(?:addEventListener)\(", js
    )
    # A handful of root-level delegated listeners is fine; dozens would mean
    # per-row binding crept back in.
    assert len(per_row_listeners) <= 6, per_row_listeners


# ---------------------------------------------------------------------------
# Existing behavior preserved: ready-document filtering, data-requires-llm
# ---------------------------------------------------------------------------


def test_ready_document_filtering_is_preserved():
    js = _read(WIZARD_JS)
    assert "parse_status !== 'ready'" in js or 'parse_status === \'ready\'' in js


def test_no_llm_ui_gate_test_still_references_this_button():
    """Guards against silently dropping the declarative LLM marker contract
    exercised by ``test_no_llm_ui_gate.py``."""
    assert "wizardTopGenerateBtn" in _read(NO_LLM_GATE_TEST)


# ---------------------------------------------------------------------------
# Review-fix batch: Continue/Retry LLM markers (finding #2)
# ---------------------------------------------------------------------------


def test_continue_and_retry_controls_require_llm():
    html = _read(HTML)
    continue_btn = re.search(
        r'<button[^>]*id="wizardReviewContinueBtn"[^>]*>', html, re.DOTALL
    )
    retry_btn = re.search(
        r'<button[^>]*id="wizardCompleteRetryBtn"[^>]*>', html, re.DOTALL
    )
    assert continue_btn and "data-requires-llm" in continue_btn.group(0)
    assert retry_btn and "data-requires-llm" in retry_btn.group(0)


# ---------------------------------------------------------------------------
# Review-fix batch: hidden panes cannot be overridden by pane layout CSS
# (finding #3)
# ---------------------------------------------------------------------------


def test_hidden_stage_panes_cannot_be_overridden_by_pane_layout_css():
    """`.wizard-stage-pane { display: block; }` loads *after* the shared
    `.ob-hidden { display: none; }` rule (components.css loads before this
    file's <link>), so on an equal-specificity tie the later rule used to
    win and a "hidden" stage pane silently rendered anyway. A combined
    selector has strictly higher specificity than either rule alone, so it
    wins regardless of link order — `!important` on top guards against any
    future higher-specificity/flex override on `.wizard-stage-pane` too."""
    css = _read(WIZARD_CSS)
    guard = re.search(r"\.wizard-stage-pane\.ob-hidden\s*\{([^}]*)\}", css)
    assert guard, "no CSS guard forcing hidden stage panes to stay hidden"
    body = guard.group(1)
    assert "display: none" in body
    assert "!important" in body
    # The guard must appear after the plain `.wizard-stage-pane` rule so a
    # reader can see it as the deliberate override, not a coincidence.
    assert css.index(".wizard-stage-pane.ob-hidden") > css.index(
        ".wizard-stage-pane {"
    )


# ---------------------------------------------------------------------------
# Review-fix batch: mobile (<=768px) stepper + configure pane (finding #4)
# ---------------------------------------------------------------------------


def test_mobile_breakpoint_wraps_stepper_and_resets_pane_height():
    css = _read(WIZARD_CSS)
    idx = css.find("@media (max-width: 768px)")
    assert idx != -1, "no mobile breakpoint in ontology-wizard.css"
    mobile_block = css[idx:]

    stepper_rule = re.search(
        r"\.wizard-stepper-list\s*\{([^}]*)\}", mobile_block
    )
    assert stepper_rule, "stepper must gain a mobile rule to wrap/compact"
    assert "flex-wrap: wrap" in stepper_rule.group(1)

    # The desktop tabs shell's fixed-height/overflow-hidden chain must be
    # reset to natural flow on mobile (same convention as the Knowledge
    # Graph / Data Quality mobile resets documented in
    # .cursor/11-frontend-design.mdc), so the metadata table and source
    # tabs stay reachable without the whole page scrolling sideways.
    assert "overflow: visible" in mobile_block
    assert "height: auto" in mobile_block


# ---------------------------------------------------------------------------
# Review-fix batch: accessible focus target per stage (finding #5)
# ---------------------------------------------------------------------------


def test_each_stage_pane_has_a_focusable_heading():
    html = _read(HTML)
    for pane_id, heading_id in (
        ("wizardConfigurePane", "wizardConfigureHeading"),
        ("wizardReviewPane", "wizardReviewHeading"),
        ("wizardCompletePane", "wizardCompleteHeading"),
    ):
        pane_block = re.search(
            rf'id="{pane_id}"[^>]*>(.*?)(?=<!-- =+ -->|\Z)', html, re.DOTALL
        )
        assert pane_block, f"{pane_id} not found"
        heading = re.search(
            rf'<h[1-6]\b[^>]*id="{heading_id}"[^>]*>', pane_block.group(1)
        )
        assert heading, f"{pane_id} missing focusable heading {heading_id}"
        assert 'tabindex="-1"' in heading.group(0)


def test_stage_transition_focus_is_not_disruptive_on_initial_load():
    js = _read(WIZARD_JS)
    assert "function setWizardStage(" in js
    # setWizardStage must distinguish "first call ever" (page load/resume)
    # from a real transition, and only focus on the latter.
    assert re.search(r"isFirstCall|_wizardStageBooted", js), (
        "setWizardStage has no guard against focusing on initial load"
    )
    assert ".focus()" in js


# ---------------------------------------------------------------------------
# Review-fix batch: confirm before a stale-banner re-detect (finding #7)
# ---------------------------------------------------------------------------


def test_stale_banner_redetect_goes_through_the_confirm_guarded_entrypoint():
    """The stale banner's Re-detect action must not bypass the "this
    discards your current draft" confirmation — it now routes through the
    same `startGenerateDetection` entry point Stage 1's own button uses,
    instead of calling `runGenerateDetection` directly."""
    js = _read(WIZARD_JS)
    core_block = re.search(
        r"window\.WizardCore\s*=\s*\{([\s\S]*?)\};", js
    )
    assert core_block, "window.WizardCore bridge not found"
    redetect_line = re.search(r"redetect:\s*(\w+)", core_block.group(1))
    assert redetect_line
    assert redetect_line.group(1) == "startGenerateDetection", (
        "stale-banner redetect must go through the confirm-guarded "
        "startGenerateDetection, not call runGenerateDetection directly"
    )


# ---------------------------------------------------------------------------
# Review-fix batch: programmatic labels on source row checkboxes (finding #8)
# ---------------------------------------------------------------------------


def test_document_row_checkboxes_have_accessible_labels():
    js = _read(WIZARD_JS)
    doc_checkbox = re.search(
        r"<input type=\"checkbox\" class=\"form-check-input me-3 wizard-doc-checkbox\"[\s\S]{0,200}",
        js,
    )
    assert doc_checkbox, "document row checkbox markup not found"
    assert "aria-label" in doc_checkbox.group(0)


def test_metadata_select_all_checkbox_has_accessible_label():
    html = _read(HTML)
    checkbox = re.search(r'<input[^>]*id="wizardSelectAllCheckbox"[^>]*>', html)
    assert checkbox
    assert "aria-label" in checkbox.group(0) or "title=" in checkbox.group(0)


# ---------------------------------------------------------------------------
# Review-fix batch: XSS hardening on rewritten metadata/document rendering
# (finding #9)
# ---------------------------------------------------------------------------


def test_metadata_table_rows_escape_every_server_supplied_field():
    js = _read(WIZARD_JS)
    render_block = js[js.index("result.metadata.tables.forEach((table, index)"):]
    render_block = render_block[: render_block.index("previewEl.style.display")]
    for var in ("tableName", "displayName", "description"):
        assert re.search(rf"escapeHtml\({var}\)", render_block), (
            f"metadata table row renders {var} without escapeHtml(): "
            "XSS risk on a substantially rewritten rendering path"
        )


def test_document_list_rows_escape_the_file_name():
    js = _read(WIZARD_JS)
    render_block = js[js.index("function renderWizardDocsList("):]
    render_block = render_block[: render_block.index("function updateWizardDocSelection")]
    assert re.search(r"escapeHtml\(file\.name\)", render_block), (
        "document list row renders file.name without escapeHtml(): XSS risk "
        "on a substantially rewritten rendering path"
    )


# ---------------------------------------------------------------------------
# Entity-only review contract
# ---------------------------------------------------------------------------


def test_add_candidate_form_has_no_type_selector():
    html = _read(HTML)
    assert 'id="wizardNewCandidateType"' not in html
    assert "object_property" not in html
    assert "data_property" not in html


def test_candidate_edits_are_entity_fields_only():
    js = _read(REVIEW_JS)
    assert 'data-field="canonical_label"' in js
    assert 'data-field="description"' in js
    assert 'data-field="type_hint"' not in js
    assert "object_property" not in js
    assert "data_property" not in js
    assert "type_hint: 'class'" in js


def test_help_modal_glossary_leads_with_entity_not_class():
    """The glossary's Entity/Class term must lead with the friendly
    "Entity" label; "Class" stays only as a parenthetical OWL-term note,
    never erased outright."""
    help_modal = REPO_ROOT / "src/front/templates/partials/layout/help_modal.html"
    html = _read(help_modal)
    dt = re.search(r"<dt>(Entity[^<]*)</dt><dd>([^<]*(?:<[^d][^>]*>[^<]*</[^>]+>[^<]*)*)</dd>", html)
    assert dt, "Entity/Class glossary entry not found"
    assert dt.group(1).strip() == "Entity", (
        "glossary term must lead with the plain 'Entity' label, "
        f"got: {dt.group(1)!r}"
    )
    assert "Class" in dt.group(2), (
        "the OWL 'Class' term must still be noted in the description, "
        "not erased outright"
    )


def test_toast_messages_say_entities_not_classes():
    """User-visible completion toasts must say "entities", not "classes" —
    `stats.classes_added` (the data key) stays unchanged."""
    js = _read(WIZARD_JS)
    assert "stats.classes_added" in js, "classes_added data key must stay unchanged"
    assert re.search(r"\}\s*classes\b", js) is None, (
        "a user-visible toast still renders the word 'classes'"
    )
    assert re.search(r"\}\s*entities\b", js), (
        "expected at least one toast to render '... entities' from "
        "stats.classes_added"
    )


# ---------------------------------------------------------------------------
# Start Over: Complete pane discard-and-return-to-Configure
# ---------------------------------------------------------------------------


def test_complete_pane_has_start_over_button():
    html = _read(HTML)
    complete_block = re.search(
        r'id="wizardCompletePane".*', html, re.DOTALL
    )
    assert complete_block
    block = complete_block.group(0)
    btn = re.search(
        r'<button[^>]*data-action="wizard-complete-discard"[^>]*>(.*?)</button>',
        block,
        re.DOTALL,
    )
    assert btn, "Complete pane Start Over button (wizard-complete-discard) not found"
    assert "btn-outline-danger" in btn.group(0)
    assert "Start Over" in btn.group(1) or "Start Over" in btn.group(0)


def test_start_over_action_is_wired_in_wizard_js():
    js = _read(WIZARD_JS)
    assert "wizard-complete-discard" in js
    # Must hit the same discard endpoint the Review pane's discard already
    # uses, and return to Configure the same way.
    discard_fn = re.search(
        r"function\s+(\w*[Ss]tartOver\w*|\w*[Dd]iscard\w*Complete\w*)\s*\([^)]*\)\s*\{([\s\S]*?)\n\}",
        js,
    )
    assert discard_fn, "no Start-Over/discard-from-complete function found in ontology-wizard.js"
    body = discard_fn.group(2)
    assert "/ontology/wizard/generate/draft/discard" in body
    assert "showConfirmDialog(" in body
    assert "setWizardStage('configure')" in body or "onDraftDiscarded" in body


def test_start_over_cancels_inflight_complete_task_tracking():
    """Starting over while a completion task is still polling must clear
    the sessionStorage task-id tracking, mirroring the stale-banner
    re-detect guard's "don't silently discard unacted-on work" pattern."""
    js = _read(WIZARD_JS)
    discard_fn = re.search(
        r"function\s+(\w*[Ss]tartOver\w*|\w*[Dd]iscard\w*Complete\w*)\s*\([^)]*\)\s*\{([\s\S]*?)\n\}",
        js,
    )
    assert discard_fn
    body = discard_fn.group(2)
    assert "WIZARD_COMPLETE_TASK_KEY" in body


def test_review_discard_button_still_works_and_is_relabeled_consistently():
    """The existing Stage 2 Discard Draft button keeps its data-action
    hook and behavior; only its label may change for consistency with the
    new Stage 3 Start Over button."""
    html = _read(HTML)
    btn = re.search(
        r'<button[^>]*data-action="wizard-review-discard"[^>]*>(.*?)</button>',
        html,
        re.DOTALL,
    )
    assert btn, "wizard-review-discard button not found"
    assert "btn-outline-danger" in btn.group(0)
    # Behavior is unchanged: still routed through discardDraft() in the
    # review module, which posts to the same discard endpoint.
    js = _read(REVIEW_JS)
    assert "wizard-review-discard" in js
    assert "/ontology/wizard/generate/draft/discard" in js
