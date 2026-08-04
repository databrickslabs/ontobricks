/**
 * OntoBricks - domain-runs.js
 * Run history for Knowledge Graph > Management > Runs.
 *
 * Two independent tables: build runs from the registry's build_runs trace
 * (GET /domain/build-runs) and analytics runs from graph_analytics_runs
 * (GET /dtwin/metrics/history). They share no columns, so they are rendered
 * separately, and each load is isolated so one endpoint failing still leaves
 * the other table on screen.
 *
 * All rendering comes from RunsRender (global/js/runs-render.js), shared with
 * the cross-domain Runs page in Settings. This file owns only the fetching,
 * the element ids and the load latch.
 */

let _runsLoaded = false;
let _runsCache = [];
let _analyticsRunsCache = [];

function _renderRunsTable() {
    const empty = document.getElementById('runsEmpty');
    const wrapper = document.getElementById('runsTableWrapper');
    const tbody = document.getElementById('runsTableBody');
    if (!tbody) return;

    if (_runsCache.length === 0) {
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
}

function _renderAnalyticsRunsTable() {
    const empty = document.getElementById('analyticsRunsEmpty');
    const wrapper = document.getElementById('analyticsRunsTableWrapper');
    const tbody = document.getElementById('analyticsRunsTableBody');
    if (!tbody) return;

    if (_analyticsRunsCache.length === 0) {
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
}

// Returns true iff the fetch produced a renderable result (including a
// genuinely empty run list — an empty domain is not a load failure), false
// on any error path. loadDomainRuns() uses this to decide whether the
// overall load may be latched via _runsLoaded.
async function _loadBuildRuns() {
    const loading = document.getElementById('runsLoading');
    const empty = document.getElementById('runsEmpty');
    const error = document.getElementById('runsError');
    const wrapper = document.getElementById('runsTableWrapper');
    if (!loading) return false;

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

// Same success/failure contract as _loadBuildRuns() above.
async function _loadAnalyticsRuns() {
    const loading = document.getElementById('analyticsRunsLoading');
    const empty = document.getElementById('analyticsRunsEmpty');
    const error = document.getElementById('analyticsRunsError');
    const wrapper = document.getElementById('analyticsRunsTableWrapper');
    if (!loading) return false;

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

// Deliberately sequential awaits rather than a combinator that settles on the
// first rejection: each loader owns its own error handling, and neither is
// allowed to blank the other's table.
//
// _runsLoaded latches only when BOTH loaders actually succeeded, so the
// sidebarSectionChanged listener below will retry on re-entry after a
// failure instead of re-showing a stale error forever. A successful load
// that happens to return zero rows for one or both tables still latches:
// "no runs yet" is a real, current answer, not a failure to be retried on
// every single visit to the section.
async function loadDomainRuns() {
    const buildOk = await _loadBuildRuns();
    const analyticsOk = await _loadAnalyticsRuns();
    _runsLoaded = buildOk && analyticsOk;
}

function showRunDetails(idx) {
    showRunDetailsObj(_runsCache[idx]);
}

// Render the build-run details popup for a run object directly (so other
// views, e.g. the Audit trail timeline, can reuse the same modal without
// depending on this file's internal _runsCache).
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

document.addEventListener('sidebarSectionChanged', function (e) {
    if (e.detail && e.detail.section === 'runs' && !_runsLoaded) {
        loadDomainRuns();
    }
});

document.addEventListener('DOMContentLoaded', function () {
    if (document.getElementById('runs-section') &&
        document.getElementById('runs-section').classList.contains('active')) {
        loadDomainRuns();
    }
});
