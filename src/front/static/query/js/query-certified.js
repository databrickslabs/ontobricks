/**
 * Graph → Management → Build: the Certified Graph part of the page.
 *
 * Fills the Certified Graph node of the summary diagram (backend icon,
 * target, Certified / Not built badge) and the Certified Graph section of the
 * Build tab (served relation, Lakebase / Neo4j backend panel). The Build
 * itself (both graphs) is driven by query-databricks-build.js.
 */
(function () {
    'use strict';

    // Targets with a backend copy, described by the Lakebase / Neo4j panel.
    const PANEL_TARGETS = ['lakebase', 'neo4j'];
    const TARGET_LABELS = {
        lakehouse_virtual: 'Lakehouse (virtual)',
        lakehouse_materialized: 'Lakehouse (materialized)',
        lakebase: 'Lakebase',
        neo4j: 'Neo4j',
        none: 'No graph backend',
    };

    function _el(id) {
        return document.getElementById(id);
    }

    function _renderBadge(status) {
        const badge = _el('certStateBadge');
        badge.className = 'badge dt-arch-badge ' + (status.certified ? 'text-bg-success' : 'text-bg-secondary');
        badge.textContent = status.certified ? 'Certified' : 'Not built';
    }

    function _renderServedLine(status, usesPanel) {
        const line = _el('certServedLine');
        line.replaceChildren();
        line.classList.toggle('d-none', usesPanel || !status.served_relation);
        if (usesPanel || !status.served_relation) return;
        const code = document.createElement('code');
        code.textContent = status.served_relation;
        line.append('Served from ', code, ' (' + (TARGET_LABELS[status.target] || status.target) + ').');
    }

    function _render(status) {
        const usesPanel = PANEL_TARGETS.indexOf(status.target) !== -1;
        if (typeof _setBackendBrandIcon === 'function') {
            _setBackendBrandIcon(_el('certBackendIcon'), status.backend);
        }
        _el('certFlowTarget').textContent = TARGET_LABELS[status.target] || 'Context backend';
        _renderBadge(status);
        _renderServedLine(status, usesPanel);
        _el('sync-lakebase-panel').classList.toggle('d-none', !usesPanel);
        // The Cache tab refresh needs a built Certified Graph (query-sync.js).
        window.__certifiedBuilt = !!status.certified;
        if (typeof _updateSyncAdjacencyActionState === 'function') _updateSyncAdjacencyActionState();
    }

    function _showError(message) {
        _el('certLoadErrorMessage').textContent = message || '';
        _el('certLoadError').classList.toggle('d-none', !message);
    }

    async function loadCertifiedStatus() {
        try {
            const data = await apiRequest('/dtwin/certified/status');
            _showError('');
            _render(data);
        } catch (err) {
            _showError('Could not load the Certified Graph state: ' + err.message);
        }
    }

    window.loadCertifiedStatus = loadCertifiedStatus;

    document.addEventListener('DOMContentLoaded', function () {
        if (!_el('buildFlowCertified')) return;
        const refresh = _el('dbxBuildRefreshBtn');
        if (refresh) refresh.addEventListener('click', loadCertifiedStatus);
        if (new URLSearchParams(window.location.search).get('section') === 'sync') {
            loadCertifiedStatus();
        }
    });

    document.addEventListener('sidebarSectionChanged', function (e) {
        if (e.detail && e.detail.section === 'sync') loadCertifiedStatus();
    });
})();
