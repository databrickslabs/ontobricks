"""The entity/relationship panel is shared by two hosts, so ownership is tracked.

The Designer right panel (`#panelBody`) and the Manual Mapping bottom panel
(`#manualPanelBody`) both render the same markup, which uses page-global `ep*` /
`rp*` element ids. Two consequences the code must respect:

1. `runEntityPanelQuery()` / `runRelPanelQuery()` discard their response unless
   `currentPanelType` matches. Manual Mapping never set it, so the preview query
   returned, was dropped, and the Mapping tab spun forever (GitHub issue #145).
2. Only one host may hold the markup at a time — a leftover copy in the other
   host shadows the live one on every `getElementById` lookup.
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"
MANUAL_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-manual.js"


def _design() -> str:
    return DESIGN_JS.read_text(encoding="utf-8")


def _manual() -> str:
    return MANUAL_JS.read_text(encoding="utf-8")


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


def _method_body(source: str, name: str) -> str:
    match = re.search(
        rf"{re.escape(name)}\s*:\s*(?:async\s+)?function\s*\([^)]*\)\s*\{{", source
    )
    assert match, f"{name}() must exist"
    return _brace_block_after(source, match.end() - 1)


class TestPanelOwnershipIsClaimed:
    def test_entity_panel_claims_its_host(self):
        body = _function_body(_design(), "loadEntityPanelContent")
        assert "claimMappingPanel(panelBody, 'entity'" in body

    def test_relationship_panel_claims_its_host(self):
        body = _function_body(_design(), "loadRelationshipPanelContent")
        assert "claimMappingPanel(panelBody, 'relationship'" in body

    def test_claim_happens_before_the_panel_is_initialised(self):
        """initEntityPanel() schedules the preview query that the guard filters."""
        body = _function_body(_design(), "loadEntityPanelContent")
        assert body.index("claimMappingPanel(") < body.index("initEntityPanel(")

    def test_claim_records_the_host_that_received_the_markup(self):
        body = _function_body(_design(), "claimMappingPanel")
        assert "panelBody?.id" in body
        assert "currentPanelHostId = hostId" in body

    def test_claim_evicts_the_other_host(self):
        """Two live copies would make every ep*/rp* id lookup ambiguous."""
        body = _function_body(_design(), "claimMappingPanel")
        assert "currentPanelHostId !== hostId" in body
        assert "closeActiveMappingPanel()" in body


class TestPanelOwnershipIsReleased:
    def test_release_clears_ownership_and_invalidates_in_flight_queries(self):
        body = _function_body(_design(), "releaseMappingPanel")
        assert "currentPanelHostId = null" in body
        assert "currentPanelType = null" in body
        assert "clearTimeout(EntityPanelState._autoLoadTimer)" in body
        assert "EntityPanelState._generation++" in body

    def test_designer_close_delegates_to_release(self):
        body = _function_body(_design(), "closeMappingPanel")
        assert "releaseMappingPanel()" in body
        assert "currentPanelType = null" not in body, "must not duplicate the reset"

    def test_manual_close_releases_the_shared_panel(self):
        body = _method_body(_manual(), "closePanel")
        assert "releaseMappingPanel()" in body

    def test_manual_close_survives_a_missing_manual_section(self):
        """closeActiveMappingPanel() can reach it from the Designer side."""
        body = _method_body(_manual(), "closePanel")
        assert "if (container) container.classList.remove" in body
        assert "document.getElementById('manualSavePanelBtn').disabled" not in body
        assert "document.getElementById('manualPanelItemName').textContent" not in body


class TestActiveHostIsClosed:
    def test_close_dispatches_to_the_owning_host(self):
        body = _function_body(_design(), "closeActiveMappingPanel")
        assert "currentPanelHostId === 'manualPanelBody'" in body
        assert "ManualModule.closePanel()" in body
        assert "closeMappingPanel()" in body

    def test_auto_map_closes_the_host_that_owns_the_panel(self):
        """Manual Mapping has its own Auto-Map button sharing this poll loop."""
        body = _function_body(_design(), "_pollAndSaveResult")
        assert "closeActiveMappingPanel()" in body


class TestStaleResponseGuardStillApplies:
    """The guards are why ownership must be claimed — pin them to the fix."""

    def test_entity_query_drops_responses_from_another_panel(self):
        body = _function_body(_design(), "runEntityPanelQuery")
        assert "currentPanelType !== 'entity'" in body

    def test_relationship_query_drops_responses_from_another_panel(self):
        body = _function_body(_design(), "runRelPanelQuery")
        assert "currentPanelType !== 'relationship'" in body


class TestManualApplyUsesLivePanelState:
    """Manual Apply must persist the column selections owned by the shared panel."""

    def test_entity_apply_uses_state_instead_of_removed_summary_elements(self):
        body = _method_body(_manual(), "saveMapping")
        assert "EntityPanelState.idColumn" in body
        assert "EntityPanelState.labelColumn" in body
        assert "ontology_class_label:" in body
        assert "document.getElementById('epSummaryId')" not in body
        assert "document.getElementById('epSummaryLabel')" not in body

    def test_relationship_apply_uses_state_instead_of_removed_summary_elements(self):
        body = _method_body(_manual(), "saveMapping")
        assert "RelPanelState.sourceIdColumn" in body
        assert "RelPanelState.targetIdColumn" in body
        assert "document.getElementById('rpSummarySource')" not in body
        assert "document.getElementById('rpSummaryTarget')" not in body
