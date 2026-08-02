/**
 * OntoBricks - domain-runs.js
 * Run history for Knowledge Graph > Management > Runs.
 *
 * Two independent tables: build runs from the registry's build_runs trace
 * (GET /domain/build-runs) and analytics runs from graph_analytics_runs
 * (GET /dtwin/metrics/history). They share no columns, so they are rendered
 * separately, and each load is isolated so one endpoint failing still leaves
 * the other table on screen.
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
        row.innerHTML =
            '<td class="text-end text-muted small">' + _esc(run.id || (idx + 1)) + '</td>'
            + '<td class="small">' + _fmtTs(run.started_at) + '</td>'
            + '<td class="text-center"><span class="badge bg-secondary">v' + _esc(run.version || '?') + '</span></td>'
            + '<td class="text-center">' + _statusBadge(run.status) + '</td>'
            + '<td class="text-end">' + _esc((Number(run.triple_count) || 0).toLocaleString()) + '</td>'
            + '<td class="text-center">'
            + '<button class="btn btn-sm btn-outline-primary" onclick="showRunDetails(' + idx + ')" title="View run details">'
            + '<i class="bi bi-eye"></i></button></td>';
        tbody.appendChild(row);
    });
    wrapper.style.display = '';
}

function _esc(s) {
    if (typeof escapeHtml === 'function') return escapeHtml(s == null ? '' : String(s));
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
        return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
}

function _fmtTs(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d.getTime())) return _esc(iso);
    return d.toLocaleString();
}

function _fmtDuration(secs) {
    const s = Number(secs) || 0;
    if (s < 1) return (s * 1000).toFixed(0) + ' ms';
    if (s < 60) return s.toFixed(1) + ' s';
    const m = Math.floor(s / 60);
    const r = Math.round(s % 60);
    return m + 'm ' + r + 's';
}

function _statusBadge(status) {
    const st = (status || '').toLowerCase();
    if (st === 'success') return '<span class="badge bg-success"><i class="bi bi-check-circle me-1"></i>Success</span>';
    if (st === 'error') return '<span class="badge bg-danger"><i class="bi bi-x-circle me-1"></i>Error</span>';
    if (st === 'cancelled') return '<span class="badge bg-warning text-dark"><i class="bi bi-slash-circle me-1"></i>Cancelled</span>';
    return '<span class="badge bg-secondary">' + _esc(status || 'unknown') + '</span>';
}

function _analyticsStatusBadge(status) {
    const st = (status || '').toLowerCase();
    if (st === 'completed') return '<span class="badge bg-success"><i class="bi bi-check-circle me-1"></i>Completed</span>';
    if (st === 'failed') return '<span class="badge bg-danger"><i class="bi bi-x-circle me-1"></i>Failed</span>';
    return '<span class="badge bg-secondary">' + _esc(status || 'unknown') + '</span>';
}

// Named _runLocalName, not _localName: this file declares its helpers at
// global scope and query-chat.js — loaded on the same page — has a
// _localName of its own. That one is currently inside an IIFE, so there is
// no clash today, but the prefix means there never can be.
function _runLocalName(uri) {
    const s = String(uri == null ? '' : uri);
    const cut = Math.max(s.lastIndexOf('#'), s.lastIndexOf('/'));
    return cut >= 0 ? s.slice(cut + 1) : s;
}

function _analyticsScope(classFilter) {
    const list = classFilter || [];
    if (!list.length) return '<span class="text-muted">All types</span>';
    const first = _esc(_runLocalName(list[0]));
    if (list.length === 1) return first;
    return first + ' <span class="text-muted">+' + (list.length - 1) + '</span>';
}

function _fmtMillis(ms) {
    const n = Number(ms) || 0;
    if (n < 1000) return n + ' ms';
    return _fmtDuration(n / 1000);
}

// A failed run stores zeros for every metric. Printing them would read as a
// graph with no nodes rather than a run that never produced numbers, so
// failed rows dash their metric cells out.
function _analyticsRunRow(run, idx) {
    const failed = (run.status || '').toLowerCase() === 'failed';
    const dash = '<span class="text-muted">&mdash;</span>';
    const num = function (v) { return _esc((Number(v) || 0).toLocaleString()); };

    return '<td class="small">' + _fmtTs(run.computed_at) + '</td>'
        + '<td class="small">' + _analyticsScope(run.class_filter) + '</td>'
        + '<td class="text-center"><span class="badge bg-secondary">v' + _esc(run.version || '?') + '</span></td>'
        + '<td class="text-center">' + _analyticsStatusBadge(run.status) + '</td>'
        + '<td class="text-end">' + (failed ? dash : num(run.node_count)) + '</td>'
        + '<td class="text-end">' + (failed ? dash : num(run.edge_count)) + '</td>'
        + '<td class="text-end">' + (failed ? dash : num(run.connected_components)) + '</td>'
        + '<td class="text-end">' + (failed ? dash : _esc((Number(run.avg_degree) || 0).toFixed(2))) + '</td>'
        + '<td class="text-end font-monospace">' + (failed ? dash : _esc((Number(run.density) || 0).toFixed(6))) + '</td>'
        + '<td class="text-end">' + _esc(_fmtMillis(run.duration_ms)) + '</td>'
        + '<td class="text-center">'
        + '<button class="btn btn-sm btn-outline-primary" onclick="showAnalyticsRunDetails(' + idx + ')" title="View analytics run details">'
        + '<i class="bi bi-eye"></i></button></td>';
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
        row.innerHTML = _analyticsRunRow(run, idx);
        tbody.appendChild(row);
    });
    wrapper.style.display = '';
}

function _kindBadge(kind) {
    const k = (kind || '').toLowerCase();
    const map = {
        session: ['bg-primary', 'bi-person-workspace', 'Session'],
        api: ['bg-info text-dark', 'bi-hdd-network', 'API'],
        scheduled: ['bg-dark', 'bi-clock', 'Scheduled']
    };
    const cfg = map[k] || ['bg-secondary', 'bi-question-circle', kind || '—'];
    return '<span class="badge ' + cfg[0] + '"><i class="bi ' + cfg[1] + ' me-1"></i>' + _esc(cfg[2]) + '</span>';
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

function _kv(label, value) {
    return '<div class="col-sm-6 mb-2">'
        + '<div class="text-muted small">' + _esc(label) + '</div>'
        + '<div class="fw-semibold text-break" style="overflow-wrap:anywhere;word-break:break-word;">' + value + '</div>'
        + '</div>';
}

function _statsTable(obj) {
    const keys = Object.keys(obj || {});
    if (keys.length === 0) return '<p class="text-muted small mb-0">No data.</p>';
    let rows = '';
    keys.forEach(function (k) {
        const label = k.replace(/_/g, ' ').replace(/\b\w/g, function (c) { return c.toUpperCase(); });
        rows += '<tr><td class="small text-muted">' + _esc(label) + '</td>'
            + '<td class="text-end fw-semibold small">' + _esc(obj[k]) + '</td></tr>';
    });
    return '<table class="table table-sm mb-0"><tbody>' + rows + '</tbody></table>';
}

function _phaseTable(phases) {
    const keys = Object.keys(phases || {});
    if (keys.length === 0) return '<p class="text-muted small mb-0">No phase timings recorded.</p>';
    let rows = '';
    keys.forEach(function (k) {
        rows += '<tr><td class="small">' + _esc(k) + '</td>'
            + '<td class="text-end small">' + _fmtDuration(phases[k]) + '</td></tr>';
    });
    return '<table class="table table-sm mb-0">'
        + '<thead class="table-light"><tr><th class="small">Step</th><th class="text-end small">Duration</th></tr></thead>'
        + '<tbody>' + rows + '</tbody></table>';
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
    const stats = run.stats || {};

    let html = '';

    // Summary
    html += '<div class="row g-2 mb-3">';
    html += _kv('Run ID', _esc(run.id || '—'));
    html += _kv('Status', _statusBadge(run.status));
    html += _kv('Version', '<span class="badge bg-secondary">v' + _esc(run.version || '?') + '</span>');
    html += _kv('Trigger', _kindBadge(run.build_kind));
    html += _kv('Started', _fmtTs(run.started_at));
    html += _kv('Finished', _fmtTs(run.finished_at));
    html += _kv('Duration', _fmtDuration(run.duration_s));
    html += _kv('Graph Engine', _esc(run.graph_engine || '—'));
    html += '</div>';

    if (run.message) {
        html += '<div class="alert alert-light border small mb-3"><i class="bi bi-chat-left-text me-1"></i>' + _esc(run.message) + '</div>';
    }
    if (run.error) {
        html += '<div class="alert alert-danger small mb-3"><i class="bi bi-exclamation-octagon me-1"></i>' + _esc(run.error) + '</div>';
    }

    // Build outputs
    html += '<h6 class="mt-2 mb-2"><i class="bi bi-hdd-stack me-1"></i>Build Output</h6>';
    html += '<div class="row g-2 mb-3">';
    html += _kv('Triples', _esc((run.triple_count || 0).toLocaleString()));
    html += _kv('Entities', _esc(run.entity_count || 0));
    html += _kv('Relationships', _esc(run.relationship_count || 0));
    html += _kv('SQL Size', _esc((run.sql_chars || 0).toLocaleString()) + ' chars');
    html += _kv('Sync Mode', _esc(run.sync_mode || '—'));
    html += _kv('Graph Name', _esc(run.graph_name || '—'));
    html += _kv('View / Table', _esc(run.view_table || '—'));
    html += _kv('Task ID', '<span class="font-monospace small">' + _esc(run.task_id || '—') + '</span>');
    html += '</div>';

    // Phase timings
    html += '<h6 class="mt-2 mb-2"><i class="bi bi-stopwatch me-1"></i>Phase Timings</h6>';
    html += _phaseTable(run.phase_times);

    // Ontology + mapping stats
    html += '<div class="row mt-3">';
    html += '<div class="col-md-6">'
        + '<h6 class="mb-2"><i class="bi bi-bezier2 me-1"></i>Ontology</h6>'
        + _statsTable(stats.ontology) + '</div>';
    html += '<div class="col-md-6">'
        + '<h6 class="mb-2"><i class="bi bi-shuffle me-1"></i>Mapping</h6>'
        + _statsTable(stats.mapping) + '</div>';
    html += '</div>';

    body.innerHTML = html;

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

    const failed = (run.status || '').toLowerCase() === 'failed';
    const scope = (run.class_filter || []);
    const dash = '<span class="text-muted">&mdash;</span>';
    const num = function (v) { return _esc((Number(v) || 0).toLocaleString()); };

    let html = '<div class="row g-2 mb-3">';
    html += _kv('Status', _analyticsStatusBadge(run.status));
    html += _kv('Version', '<span class="badge bg-secondary">v' + _esc(run.version || '?') + '</span>');
    html += _kv('When', _fmtTs(run.computed_at));
    html += _kv('Duration', _esc(_fmtMillis(run.duration_ms)));
    html += '</div>';

    if (run.error) {
        html += '<div class="alert alert-danger small mb-3"><i class="bi bi-exclamation-octagon me-1"></i>'
            + _esc(run.error) + '</div>';
    }

    html += '<h6 class="mt-2 mb-2"><i class="bi bi-diagram-3 me-1"></i>Scope</h6>';
    if (!scope.length) {
        html += '<p class="text-muted small mb-3">All entity types.</p>';
    } else {
        html += '<ul class="small mb-3">' + scope.map(function (uri) {
            return '<li><span class="fw-semibold">' + _esc(_runLocalName(uri)) + '</span> '
                + '<span class="text-muted font-monospace" style="font-size:0.75rem">' + _esc(uri) + '</span></li>';
        }).join('') + '</ul>';
    }

    html += '<h6 class="mt-2 mb-2"><i class="bi bi-bar-chart me-1"></i>Graph</h6>';
    html += '<div class="row g-2 mb-3">';
    html += _kv('Nodes', failed ? dash : num(run.node_count));
    html += _kv('Edges', failed ? dash : num(run.edge_count));
    html += _kv('Connected Components', failed ? dash : num(run.connected_components));
    html += _kv('Avg Degree', failed ? dash : _esc((Number(run.avg_degree) || 0).toFixed(2)));
    html += _kv('Density', failed ? dash : _esc((Number(run.density) || 0).toFixed(6)));
    html += _kv('Task ID', '<span class="font-monospace small">' + _esc(run.task_id || '—') + '</span>');
    html += '</div>';

    body.innerHTML = html;
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
