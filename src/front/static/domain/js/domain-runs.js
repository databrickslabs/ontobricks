/**
 * OntoBricks - domain-runs.js
 * Build-run history for the Domain > Management > Runs submenu.
 *
 * Reads the per-domain build trace recorded in the registry
 * (build_runs table) via GET /domain/build-runs and renders a list
 * plus a details popup for each run.
 */

let _runsLoaded = false;
let _runsCache = [];
let _runsVersionSel = '';  // '' = all versions

function _populateRunsVersions(versions, current) {
    const sel = document.getElementById('runsVersionFilter');
    if (!sel) return;
    _runsVersionSel = (current && versions.indexOf(current) !== -1) ? current : '';
    sel.innerHTML = '<option value="">All versions</option>' +
        versions.map(function (v) {
            return '<option value="' + _esc(v) + '"' + (v === _runsVersionSel ? ' selected' : '') +
                '>v' + _esc(v) + (v === current ? ' (current)' : '') + '</option>';
        }).join('');
    sel.value = _runsVersionSel;
    if (!sel.dataset.wired) {
        sel.addEventListener('change', function (e) {
            _runsVersionSel = e.target.value;
            _renderRunsTable();
        });
        sel.dataset.wired = '1';
    }
}

function _renderRunsTable() {
    const empty = document.getElementById('runsEmpty');
    const wrapper = document.getElementById('runsTableWrapper');
    const tbody = document.getElementById('runsTableBody');
    if (!tbody) return;

    const rows = _runsVersionSel
        ? _runsCache.filter(function (r) { return String(r.version == null ? '' : r.version) === _runsVersionSel; })
        : _runsCache.slice();

    if (rows.length === 0) {
        wrapper.style.display = 'none';
        empty.style.display = '';
        return;
    }
    empty.style.display = 'none';
    tbody.innerHTML = '';
    rows.forEach(function (run) {
        const idx = _runsCache.indexOf(run);
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

async function loadDomainRuns() {
    const loading = document.getElementById('runsLoading');
    const empty = document.getElementById('runsEmpty');
    const error = document.getElementById('runsError');
    const wrapper = document.getElementById('runsTableWrapper');
    const tbody = document.getElementById('runsTableBody');

    if (!loading || !tbody) return;

    loading.style.display = '';
    empty.style.display = 'none';
    error.style.display = 'none';
    wrapper.style.display = 'none';

    try {
        const response = await fetch('/domain/build-runs', { credentials: 'same-origin' });
        const data = await response.json();

        loading.style.display = 'none';

        if (!data.success) {
            document.getElementById('runsErrorMessage').textContent = data.message || 'Failed to load build runs';
            error.style.display = '';
            return;
        }

        _runsCache = data.runs || [];
        _populateRunsVersions(data.versions || [], data.current_version || '');

        if (_runsCache.length === 0) {
            empty.style.display = '';
            return;
        }

        _renderRunsTable();
        _runsLoaded = true;
    } catch (err) {
        loading.style.display = 'none';
        document.getElementById('runsErrorMessage').textContent = err.message;
        error.style.display = '';
    }
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
