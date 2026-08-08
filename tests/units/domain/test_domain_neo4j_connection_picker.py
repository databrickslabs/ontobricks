"""Contract tests: Domain → Information Neo4j connection picker wiring."""

from __future__ import annotations

import pytest

INFO_JS = "/static/domain/js/domain-information.js"
DOMAIN_JS = "/static/domain/js/domain.js"
NAVBAR_JS = "/static/global/js/navbar.js"


def _static(client, path: str) -> str:
    resp = client.get(path)
    assert resp.status_code == 200, f"GET {path} returned {resp.status_code}"
    return resp.text


@pytest.fixture
def info_js(client) -> str:
    return _static(client, INFO_JS)


@pytest.fixture
def domain_js(client) -> str:
    return _static(client, DOMAIN_JS)


@pytest.fixture
def navbar_js(client) -> str:
    return _static(client, NAVBAR_JS)


class TestNeo4jConnectionPickerWiring:
    def test_options_load_without_user_action(self, info_js):
        """The dropdown must fill itself, not wait for the refresh button."""
        assert "function syncNeo4jConnectionSection" in info_js
        assert "/settings/graph-engine/neo4j-connections" in info_js
        idx = info_js.find("function syncNeo4jConnectionSection")
        body = info_js[idx : idx + 400]
        assert "loadNeo4jDatabases()" in body

    def test_backend_change_repopulates(self, info_js):
        assert "'change', syncNeo4jConnectionSection" in info_js

    def test_saved_value_read_after_fetch(self, info_js):
        """Late /domain/info hydration must not be clobbered by the fetch."""
        idx = info_js.find("async function _loadNeo4jConnectionOptions")
        assert idx >= 0
        body = info_js[idx : idx + 1600]
        fetch_idx = body.find("await fetch(")
        saved_idx = body.find("const saved =")
        assert fetch_idx >= 0 and saved_idx > fetch_idx

    def test_concurrent_loads_are_deduped(self, info_js):
        assert "_neo4jConnectionsLoading" in info_js

    def test_refresh_button_forces_reload(self, info_js):
        assert "loadNeo4jDatabases(true)" in info_js

    def test_save_blocks_empty_connection_for_neo4j(self, domain_js):
        idx = domain_js.find("async function saveDomainInfo")
        assert idx >= 0
        body = domain_js[idx : idx + 2500]
        guard = body.find("validateDomainInfoForm")
        payload = body.find("const domainInfoPayload")
        assert guard >= 0 and guard < payload

    def test_save_refreshes_saved_value_baseline(self, domain_js):
        idx = domain_js.find("Domain info saved successfully!")
        assert idx >= 0
        body = domain_js[idx : idx + 500]
        assert "dataset.savedValue" in body


class TestDomainInfoPayloadSharing:
    """The UC-save path must send the same fields as the form's own save.

    Regression: ``saveDomainInfoBeforeSave`` built its own payload that
    included ``graph_backend`` but omitted ``neo4j_connection``, so the
    server fell back to the previously stored name and the user's pick in the
    Knowledge Graph tab was silently discarded.
    """

    def test_shared_builder_includes_neo4j_connection(self, navbar_js):
        idx = navbar_js.find("function buildDomainInfoPayload")
        assert idx >= 0
        body = navbar_js[idx : idx + 2000]
        assert "neo4j_connection" in body
        assert "graph_backend" in body
        assert "domainNeo4jDatabase" in body

    def test_uc_save_path_uses_shared_builder(self, navbar_js):
        idx = navbar_js.find("async function saveDomainInfoBeforeSave")
        assert idx >= 0
        body = navbar_js[idx : idx + 1200]
        assert "buildDomainInfoPayload()" in body
        # No second, hand-rolled field list that could drift again.
        assert "graph_backend:" not in body

    def test_form_save_uses_shared_builder(self, domain_js):
        idx = domain_js.find("async function saveDomainInfo")
        assert idx >= 0
        body = domain_js[idx : idx + 2500]
        assert "buildDomainInfoPayload()" in body

    def test_uc_save_is_guarded(self, navbar_js):
        assert "function validateDomainInfoForm" in navbar_js
        idx = navbar_js.find("async function domainSave()")
        assert idx >= 0
        body = navbar_js[idx : idx + 400]
        assert "validateDomainInfoForm()" in body
        assert body.find("validateDomainInfoForm()") < body.find(
            "saveDomainInfoBeforeSave()"
        )

    def test_non_neo4j_backend_clears_connection(self, navbar_js):
        idx = navbar_js.find("function buildDomainInfoPayload")
        body = navbar_js[idx : idx + 2000]
        assert "graphBackendEl.value === 'neo4j'" in body


class TestNeo4jFieldEditRaceGuard:
    """A late ``/domain/info`` hydration must never clobber a fresh user edit.

    Regression: the DOMContentLoaded handler renders Graph Backend / Neo4j
    Connection correctly from the server-side Jinja template, then re-fetches
    the same data in a ``Promise.all`` gated behind the (often multi-second,
    real-workspace) LLM-endpoints call and unconditionally overwrote both
    fields once it resolved. A user who picked a new backend/connection before
    that fetch resolved had their pick silently reverted to the stale value —
    Save then reported success while persisting the wrong connection.
    """

    def test_change_marks_graph_backend_dirty(self, info_js):
        idx = info_js.find("const graphBackendEl = document.getElementById('domainGraphBackend');")
        assert idx >= 0
        body = info_js[idx : idx + 1400]
        assert "graphBackendEl.dataset.userEdited = '1'" in body

    def test_change_marks_neo4j_connection_dirty(self, info_js):
        idx = info_js.find("neo4jDbElInit")
        assert idx >= 0
        body = info_js[idx : idx + 1400]
        assert "neo4jDbElInit.dataset.userEdited = '1'" in body

    def test_late_fetch_skips_dirty_graph_backend(self, info_js):
        idx = info_js.find("graphBackendEl.value = infoData.info.graph_backend")
        assert idx >= 0
        # The guard must be on the same `if` as the assignment, i.e. appear
        # just before it, not merely somewhere earlier in the file.
        preceding = info_js[max(0, idx - 250) : idx]
        assert "!graphBackendEl.dataset.userEdited" in preceding

    def test_late_fetch_skips_dirty_neo4j_connection(self, info_js):
        idx = info_js.find("neo4jDbEl.value = saved;")
        assert idx >= 0
        preceding = info_js[max(0, idx - 400) : idx]
        assert "!neo4jDbEl.dataset.userEdited" in preceding

    def test_dirty_listeners_attached_before_the_slow_fetch(self, info_js):
        """The dirty flag must be wired before ``Promise.all`` starts, or a
        user could edit during the race window before listeners exist."""
        listener_idx = info_js.find("graphBackendEl.dataset.userEdited = '1'")
        fetch_idx = info_js.find("Promise.all([")
        assert 0 <= listener_idx < fetch_idx
