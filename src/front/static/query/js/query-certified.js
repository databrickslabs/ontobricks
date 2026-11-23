/**
 * Graph → Management → Certified Graph.
 *
 * Shows whether the Certified Graph is built, whether the version is locked
 * (IN-REVIEW / PUBLISHED) and, for a Lakebase / Neo4j backend, the backend
 * panel. The Build runs from the Original Graph page (it builds both graphs).
 */
(function () {
    'use strict';

    // Targets with a backend copy, described by the Lakebase / Neo4j panel.
    const PANEL_TARGETS = ['lakebase', 'neo4j'];

    function _el(id) {
        return document.getElementById(id);
    }

    function _renderBadge(status) {
        const badge = _el('certStateBadge');
        if (!badge) return;
        badge.className = 'badge ' + (status.certified ? 'text-bg-success' : 'text-bg-secondary');
        badge.textContent = status.certified ? 'Certified' : 'Not built';
    }

    function _showBackendPanel(show) {
        _el('sync-lakebase-panel').classList.toggle('d-none', !show);
        const actions = _el('sync-lakebase-actions');
        actions.classList.toggle('d-none', !show);
        actions.classList.toggle('d-flex', show);
    }

    function _render(status) {
        if (typeof _setBackendBrandIcon === 'function') {
            _setBackendBrandIcon(_el('certBackendIcon'), status.backend);
        }
        _el('certReadOnlyNotice').classList.toggle('d-none', status.editable !== false);
        _renderBadge(status);
        _showBackendPanel(PANEL_TARGETS.indexOf(status.target) !== -1);
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
        if (!_el('certified-section')) return;
        _el('certRefreshBtn').addEventListener('click', loadCertifiedStatus);
        if (new URLSearchParams(window.location.search).get('section') === 'certified') {
            loadCertifiedStatus();
        }
    });

    document.addEventListener('sidebarSectionChanged', function (e) {
        if (e.detail && e.detail.section === 'certified') loadCertifiedStatus();
    });
})();
