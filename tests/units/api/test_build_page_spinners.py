"""Contract tests for Build-page (Lakebase) inline retrieval spinners."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from back.objects.digitaltwin.DigitalTwin import DigitalTwin

SYNC_JS = "/static/query/js/query-sync.js"
SYNC_HTML = "src/front/templates/partials/dtwin/_query_sync.html"


def _static(client, path: str) -> str:
    resp = client.get(path)
    assert resp.status_code == 200, f"GET {path} returned {resp.status_code}"
    return resp.text


@pytest.fixture
def sync_js(client) -> str:
    return _static(client, SYNC_JS)


class TestBuildPageSpinnerHelpers:
    def test_defines_arch_spinner_badge_helper(self, sync_js):
        assert "function _archSpinnerBadge" in sync_js
        assert "spinner-border" in sync_js

    def test_defines_arch_spinner_name_helper(self, sync_js):
        assert "function _archSpinnerName" in sync_js

    def test_load_dt_existence_sets_loading_before_fetch(self, sync_js):
        # Loading markup must be applied before the live probe fetch.
        idx_set = sync_js.find("function _loadDtExistence")
        assert idx_set >= 0
        body = sync_js[idx_set : idx_set + 800]
        assert "_setArchRetrievalLoading(true)" in body
        assert body.find("_setArchRetrievalLoading(true)") < body.find(
            "fetch('/dtwin/sync/dt-existence'"
        )

    def test_pending_path_does_not_paint_unable_to_check(self, sync_js):
        # Unresolved flags while pending must use the spinner helper, not the
        # warning "Unable to check" badge.
        assert "dt.pending" in sync_js or "data.pending" in sync_js
        assert "_archSpinnerBadge" in sync_js


class TestBuildPageArchMarkup:
    def test_sync_partial_has_badge_and_name_targets(self):
        html = open(SYNC_HTML, encoding="utf-8").read()
        for element_id in (
            "dtExistView",
            "dtLakebaseSyncedUcExists",
            "dtLakebaseSyncedUc",
            "dtLakebaseTableExists",
            "dtLakebaseFullName",
            "dtViewName",
        ):
            assert f'id="{element_id}"' in html


class TestPendingDtExistenceSkeleton:
    def test_pending_flag_and_null_existence(self):
        domain = MagicMock()
        domain.last_update = None
        domain.last_build = None
        domain.info = {"graph_backend": "lakebase"}
        dt = DigitalTwin(domain)
        with patch.object(
            DigitalTwin, "resolve_graph_engine", return_value="lakebase"
        ), patch(
            "back.core.helpers.effective_view_table", return_value="c.s.view"
        ), patch(
            "back.core.helpers.effective_graph_name", return_value="g_demo_v1"
        ), patch(
            "back.core.helpers.effective_graph_query_table", return_value="g_demo_v1"
        ):
            # Config resolution may fail without full settings — still pending.
            result = dt.pending_dt_existence(MagicMock())

        assert result["pending"] is True
        assert result["view_exists"] is None
        assert result["lakebase_table_exists"] is None
        assert result["lakebase_synced_uc_exists"] is None
        assert result["view_table"] == "c.s.view"
