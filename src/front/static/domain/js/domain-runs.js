/**
 * OntoBricks - domain-runs.js
 * Run history loaders for:
 *   - Graph → Management → Build → Runs (build runs only)
 *   - Intelligence → Discovery → Analytics → Runs (analytics runs only)
 *
 * Rendering comes from RunsRender (global/js/runs-render.js), shared with
 * Settings → Automation → Runs. This file owns fetching, element ids, load
 * latches, and the Build / Analytics page tab chrome (?tab=runs).
 */

let _buildRunsLoaded = false;
let _analyticsRunsLoaded = false;
let _runsCache = [];
let _analyticsRunsCache = [];

function _setRunsCount(el, n) {
    if (!el) return;
    el.textContent = n > 0 ? n.toLocaleString() + (n === 1 ? ' run' : ' runs') : '';
}

// Free-text filter bound to the shared .ob-filter-bar (see list-filter.js).
// Bound lazily so the inputs only need to exist when a table is rendered.
let _buildRunsFilter = null;
let _analyticsRunsFilter = null;

function _buildRunsFilterApi() {
    if (!_buildRunsFilter) {
        _buildRunsFilter = ObListFilter.bind(
            document.getElementById('runsFilter'),
            document.getElementById('runsTableBody'),
            document.getElementById('runsCount'),
            'run');
    }
    return _buildRunsFilter;
}

function _analyticsRunsFilterApi() {
    if (!_analyticsRunsFilter) {
        _analyticsRunsFilter = ObListFilter.bind(
            document.getElementById('analyticsRunsFilter'),
            document.getElementById('analyticsRunsTableBody'),
            document.getElementById('analyticsRunsCount'),
            'run');
    }
    return _analyticsRunsFilter;
}

function _renderRunsTable() {
    const empty = document.getElementById('runsEmpty');
    const wrapper = document.getElementById('runsTableWrapper');
    const tbody = document.getElementById('runsTableBody');
    if (!tbody) return;

    if (_runsCache.length === 0) {
        _setRunsCount(document.getElementById('runsCount'), 0);
        wrapper.style.display = 'none';
        empty.style.display = '';
        return;
    }
    empty.style.display = 'none';
    tbody.innerHTML = '';
    _runsCache.forEach(function (run, idx) {
        const row = document.createElement('tr');
        row.innerHTML = RunsRender.buildRunCells(run, idx, {
            handler: 'showRunDetails'
        });
        tbody.appendChild(row);
    });
    wrapper.style.display = '';
    const f = _buildRunsFilterApi();
    if (f) f.apply();
}

function _renderAnalyticsRunsTable() {
    const empty = document.getElementById('analyticsRunsEmpty');
    const wrapper = document.getElementById('analyticsRunsTableWrapper');
    const tbody = document.getElementById('analyticsRunsTableBody');
    if (!tbody) return;

    if (_analyticsRunsCache.length === 0) {
        _setRunsCount(document.getElementById('analyticsRunsCount'), 0);
        wrapper.style.display = 'none';
        empty.style.display = '';
        return;
    }
    empty.style.display = 'none';
    tbody.innerHTML = '';
    _analyticsRunsCache.forEach(function (run, idx) {
        const row = document.createElement('tr');
        row.innerHTML = RunsRender.analyticsRunCells(run, idx, {
            handler: 'showAnalyticsRunDetails'
        });
        tbody.appendChild(row);
    });
    wrapper.style.display = '';
    const f = _analyticsRunsFilterApi();
    if (f) f.apply();
}

async function _loadBuildRuns() {
    const loading = document.getElementById('runsLoading');
    const empty = document.getElementById('runsEmpty');
    const error = document.getElementById('runsError');
    const wrapper = document.getElementById('runsTableWrapper');
    if (!loading) return false;

    _setRunsCount(document.getElementById('runsCount'), 0);
    loading.style.display = '';
    empty.style.display = 'none';
    error.style.display = 'none';
    wrapper.style.display = 'none';

    try {
        const response = await fetch('/domain/build-runs', { credentials: 'same-origin' });
        const data = await response.json();
        loading.style.display = 'none';

        if (!data.success) {
            document.getElementById('runsErrorMessage').textContent =
                data.message || 'Failed to load build runs';
            error.style.display = '';
            return false;
        }
        _runsCache = data.runs || [];
        _renderRunsTable();
        return true;
    } catch (err) {
        loading.style.display = 'none';
        document.getElementById('runsErrorMessage').textContent = err.message;
        error.style.display = '';
        return false;
    }
}

async function _loadAnalyticsRuns() {
    const loading = document.getElementById('analyticsRunsLoading');
    const empty = document.getElementById('analyticsRunsEmpty');
    const error = document.getElementById('analyticsRunsError');
    const wrapper = document.getElementById('analyticsRunsTableWrapper');
    if (!loading) return false;

    _setRunsCount(document.getElementById('analyticsRunsCount'), 0);
    loading.style.display = '';
    empty.style.display = 'none';
    error.style.display = 'none';
    wrapper.style.display = 'none';

    try {
        const response = await fetch('/dtwin/metrics/history', { credentials: 'same-origin' });
        const data = await response.json();
        loading.style.display = 'none';

        if (!data.success) {
            document.getElementById('analyticsRunsErrorMessage').textContent =
                data.message || 'Failed to load analytics runs';
            error.style.display = '';
            return false;
        }
        _analyticsRunsCache = data.runs || [];
        _renderAnalyticsRunsTable();
        return true;
    } catch (err) {
        loading.style.display = 'none';
        document.getElementById('analyticsRunsErrorMessage').textContent = err.message;
        error.style.display = '';
        return false;
    }
}

async function loadBuildRuns() {
    _buildRunsLoaded = await _loadBuildRuns();
    return _buildRunsLoaded;
}

async function loadAnalyticsRuns() {
    _analyticsRunsLoaded = await _loadAnalyticsRuns();
    return _analyticsRunsLoaded;
}

// Back-compat alias used by older Refresh handlers / tests.
async function loadDomainRuns() {
    if (document.getElementById('runsTableBody')) {
        await loadBuildRuns();
    }
    if (document.getElementById('analyticsRunsTableBody')) {
        await loadAnalyticsRuns();
    }
}

function showRunDetails(idx) {
    showRunDetailsObj(_runsCache[idx]);
}

function showRunDetailsObj(run) {
    if (!run) return;

    const body = document.getElementById('runDetailsBody');
    if (!body) {
        console.error('[domain-runs] #runDetailsBody not found — modal missing from page template.');
        return;
    }
    body.innerHTML = RunsRender.buildRunDetailsHtml(run);

    const modalEl = document.getElementById('runDetailsModal');
    const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
    modal.show();
}

window.showRunDetailsObj = showRunDetailsObj;
window.loadBuildRuns = loadBuildRuns;
window.loadAnalyticsRuns = loadAnalyticsRuns;
window.loadDomainRuns = loadDomainRuns;

function showAnalyticsRunDetails(idx) {
    const run = _analyticsRunsCache[idx];
    if (!run) return;

    const body = document.getElementById('analyticsRunDetailsBody');
    if (!body) {
        console.error('[domain-runs] #analyticsRunDetailsBody not found — modal missing from page template.');
        return;
    }
    body.innerHTML = RunsRender.analyticsRunDetailsHtml(run);
    bootstrap.Modal.getOrCreateInstance(
        document.getElementById('analyticsRunDetailsModal')
    ).show();
}

window.showAnalyticsRunDetails = showAnalyticsRunDetails;

// ---- Original Graph page tabs (Build | Runs) --------------------------------
// Header actions (Refresh / Purge / Build) stay visible on every tab. The
// Cache and Objects tabs live on the Certified Graph page.

function _normalizeBuildPageTab(tab) {
    return tab === 'runs' ? 'runs' : 'build';
}

function _syncBuildPageTab(tab) {
    const name = _normalizeBuildPageTab(tab);
    const actions = document.getElementById('buildHeaderActions');
    if (actions) actions.classList.remove('d-none');
    const url = new URL(window.location.href);
    if (url.searchParams.get('section') === 'sync' || !url.searchParams.get('section')) {
        if (url.searchParams.get('tab') !== name) {
            url.searchParams.set('tab', name);
            if (!url.searchParams.get('section')) {
                url.searchParams.set('section', 'sync');
            }
            window.history.replaceState(window.history.state, '', url);
        }
    }
    if (name === 'runs' && !_buildRunsLoaded) {
        loadBuildRuns();
    }
}

function _showBuildPageTab(name) {
    const tab = _normalizeBuildPageTab(name);
    const btnId = ({
        runs: 'buildTabRunsBtn',
        build: 'buildTabBuildBtn',
    })[tab];
    const btn = document.getElementById(btnId);
    if (btn && typeof bootstrap !== 'undefined' && bootstrap.Tab) {
        try { bootstrap.Tab.getOrCreateInstance(btn).show(); } catch (_) { /* noop */ }
    }
    _syncBuildPageTab(tab);
}

function _initBuildPageTabs() {
    if (!document.getElementById('buildPageTabs')) return;
    document.getElementById('buildTabBuildBtn')?.addEventListener(
        'shown.bs.tab', () => _syncBuildPageTab('build'),
    );
    document.getElementById('buildTabRunsBtn')?.addEventListener(
        'shown.bs.tab', () => _syncBuildPageTab('runs'),
    );
}

// ---- Analytics page tabs (Analytics | Runs) ----------------------------

function _syncAnalyticsPageTab(tab) {
    const name = tab === 'runs' ? 'runs' : 'analytics';
    document.querySelectorAll('[data-analytics-page-actions="analytics"]').forEach(function (el) {
        el.classList.toggle('d-none', name === 'runs');
    });
    const url = new URL(window.location.href);
    const urlTab = name === 'runs' ? 'runs' : 'analytics';
    if (url.searchParams.get('tab') !== urlTab) {
        url.searchParams.set('tab', urlTab);
        if (!url.searchParams.get('section')) {
            url.searchParams.set('section', 'analytics');
        }
        window.history.replaceState(window.history.state, '', url);
    }
    if (name === 'runs' && !_analyticsRunsLoaded) {
        loadAnalyticsRuns();
    }
}

function _showAnalyticsPageTab(name) {
    const tab = name === 'runs' ? 'runs' : 'analytics';
    const btn = document.getElementById(
        tab === 'runs' ? 'analyticsPageTabRunsBtn' : 'analyticsPageTabMainBtn',
    );
    if (btn && typeof bootstrap !== 'undefined' && bootstrap.Tab) {
        try { bootstrap.Tab.getOrCreateInstance(btn).show(); } catch (_) { /* noop */ }
    }
    _syncAnalyticsPageTab(tab);
}

function _initAnalyticsPageTabs() {
    if (!document.getElementById('analyticsPageTabs')) return;
    document.getElementById('analyticsPageTabMainBtn')?.addEventListener(
        'shown.bs.tab', () => _syncAnalyticsPageTab('analytics'),
    );
    document.getElementById('analyticsPageTabRunsBtn')?.addEventListener(
        'shown.bs.tab', () => _syncAnalyticsPageTab('runs'),
    );
}

document.addEventListener('sidebarSectionChanged', function (e) {
    const section = e.detail && e.detail.section;
    if (section === 'sync') {
        const tab = new URLSearchParams(window.location.search).get('tab');
        _showBuildPageTab(tab);
    }
    if (section === 'analytics') {
        const tab = new URLSearchParams(window.location.search).get('tab');
        _showAnalyticsPageTab(tab === 'runs' ? 'runs' : 'analytics');
    }
});

document.addEventListener('DOMContentLoaded', function () {
    _initBuildPageTabs();
    _initAnalyticsPageTabs();

    const params = new URLSearchParams(window.location.search);
    const section = params.get('section') || '';
    const tab = params.get('tab');

    if (document.getElementById('buildPageTabs')
            && (section === 'sync' || document.getElementById('sync-section')?.classList.contains('active'))) {
        _showBuildPageTab(tab);
    }
    if (document.getElementById('analyticsPageTabs')
            && (section === 'analytics' || document.getElementById('analytics-section')?.classList.contains('active'))) {
        _showAnalyticsPageTab(tab === 'runs' ? 'runs' : 'analytics');
    }
});
