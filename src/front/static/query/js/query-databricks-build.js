/**
 * Databricks triple-store build page — R2RML VIEW into a Delta TABLE, or into
 * a pass-through VIEW when the domain uses view-only materialization.
 */

const DBX_BUILD_TASK_KEY = 'ontobricks_databricks_build_task';

let dbxBuildReady = false;
let dbxBuildRunning = false;

function _tsxBackend() {
    try {
        const el = document.getElementById('triplestore-config');
        if (!el) return 'lakebase';
        return JSON.parse(el.textContent || '{}').triple_store_backend || 'lakebase';
    } catch (_) {
        return 'lakebase';
    }
}

function _dbxEscape(text) {
    const d = document.createElement('div');
    d.textContent = text == null ? '' : String(text);
    return d.innerHTML;
}

function _dbxNotify(message, type) {
    if (typeof showNotification === 'function') {
        showNotification(message, type);
    }
}

function _showDbxBuildResult(kind, html) {
    const alert = document.getElementById('dbxBuildResultAlert');
    if (!alert) return;
    const classes = {
        success: 'alert-success',
        warning: 'alert-warning',
        error: 'alert-danger',
        info: 'alert-info',
    };
    alert.className = 'alert small mb-3 ' + (classes[kind] || classes.info);
    alert.innerHTML = html;
    alert.classList.remove('d-none');
}

function _hideDbxBuildResult() {
    const alert = document.getElementById('dbxBuildResultAlert');
    if (alert) alert.classList.add('d-none');
}

function _resetDbxProgressBar() {
    const bar = document.getElementById('dbxBuildProgressBar');
    if (!bar) return;
    bar.className = 'progress-bar progress-bar-striped progress-bar-animated';
    bar.style.width = '0%';
    bar.textContent = '0%';
}

function _finishDbxProgressBar(status) {
    const bar = document.getElementById('dbxBuildProgressBar');
    if (!bar) return;
    bar.classList.remove('progress-bar-striped', 'progress-bar-animated');
    if (status === 'failed') {
        bar.classList.add('bg-danger');
    } else if (status === 'cancelled') {
        bar.classList.add('bg-secondary');
    } else if (status === 'completed') {
        bar.classList.add('bg-success');
    }
}

function _taskStepMessage(task) {
    if (!task) return '';
    const steps = task.steps || [];
    const idx = task.current_step != null ? task.current_step : 0;
    const step = steps[idx];
    if (step && step.description) return step.description;
    return task.message || '';
}

function applyTripleStoreBackendPanels() {
    const backend = _tsxBackend();
    const lakePanel = document.getElementById('sync-lakebase-panel');
    const dbxPanel = document.getElementById('sync-databricks-panel');
    const isDbx = backend === 'databricks';

    if (lakePanel) lakePanel.classList.toggle('d-none', isDbx);
    if (dbxPanel) dbxPanel.classList.toggle('d-none', !isDbx);
}

/**
 * Name the storage kind of ``…_data`` everywhere the page refers to it.
 *
 * The two modes produce objects with the same name and very different cost
 * and freshness characteristics, so "Target Delta table" over a pass-through
 * view is actively misleading: it implies a copy exists, that the triple
 * count is stored, and that a rebuild is needed after the source changes.
 */
function _applyDbxStorageKind(isViewMode) {
    const label = document.getElementById('dbxBuildDataTableLabel');
    if (label) {
        label.textContent = isViewMode ? 'Target view' : 'Target Delta table';
    }

    const badge = document.getElementById('dbxBuildStorageBadge');
    if (badge) {
        // Info tint for the view: it is the deliberate, non-default choice.
        // The materialized table is the default, so it stays neutral —
        // primary red is reserved for high-signal moments.
        badge.className = 'badge dbx-storage-badge ' + (isViewMode
            ? 'bg-info bg-opacity-10 text-info border border-info'
            : 'bg-secondary bg-opacity-10 text-secondary border');
        badge.innerHTML = isViewMode
            ? '<i class="bi bi-eye me-1"></i>VIEW · no data copy'
            : '<i class="bi bi-table me-1"></i>TABLE · materialized copy';
        badge.title = isViewMode
            ? 'Views only: ..._data is a SELECT over the gateway view, so it '
              + 'always reflects the current source data.'
            : 'Materialized: ..._data is a Delta copy of the gateway view, '
              + 'refreshed only by a build.';
    }

    const note = document.getElementById('dbxBuildStorageNote');
    if (note) {
        note.textContent = isViewMode
            ? 'Views only — Build refreshes the gateway definition; no triples '
              + 'are copied. Inferred triples keep their own Delta table.'
            : 'Materialized — Build copies the gateway output into this Delta '
              + 'table, so it is a point-in-time snapshot of the source.';
        note.classList.remove('d-none');
    }

    const subtitle = document.getElementById('dbxBuildSubtitle');
    if (subtitle) {
        subtitle.textContent = isViewMode
            ? 'Expose mapped triples through a governed Unity Catalog view — no data copy'
            : 'Materialize mapped triples into a governed Delta table in Unity Catalog';
    }
}

async function loadDatabricksBuildInfo() {
    const overlay = document.getElementById('dbxBuildLoadingOverlay');
    if (overlay) overlay.classList.remove('d-none');
    try {
        const resp = await fetch('/dtwin/databricks-build/info', { credentials: 'same-origin' });
        const data = await resp.json();
        if (!data.success) return;

        const isViewMode = data.materialization === 'view';

        const viewEl = document.getElementById('dbxBuildViewTable');
        const dataEl = document.getElementById('dbxBuildDataTable');
        if (viewEl) viewEl.textContent = data.view_table || '—';
        if (dataEl) dataEl.textContent = data.data_table || '—';
        _applyDbxStorageKind(isViewMode);

        const r = data.readiness || {};
        dbxBuildReady = !!r.mapping_valid;
        const readinessCard = document.getElementById('dbxBuildReadinessCard');
        if (readinessCard) {
            readinessCard.innerHTML =
                '<h6 class="fw-semibold mb-2"><i class="bi bi-clipboard-check me-1"></i>Readiness</h6>' +
                '<div class="small">Mapping: ' + (r.mapping_valid ? '<span class="text-success">Ready</span>' : '<span class="text-warning">Not ready</span>') +
                '</div>';
        }

        const ts = data.triplestore_status || {};
        const count = ts.count != null ? ts.count : 0;
        const statusCard = document.getElementById('dbxBuildStatusCard');
        if (statusCard) {
            const exists = ts.exists ? 'Yes' : 'No';
            const icon = isViewMode ? 'bi-eye' : 'bi-table';
            const title = isViewMode ? 'View status' : 'Delta table status';
            statusCard.innerHTML =
                '<h6 class="fw-semibold mb-2"><i class="bi ' + icon + ' me-1"></i>' + title + '</h6>' +
                '<div class="small">Exists: ' + exists + ' · Triples: <strong>' + count + '</strong></div>' +
                (isViewMode
                    ? '<div class="small text-muted mt-1">No triples are copied — this count is a live query.</div>'
                    : '');
        }

        const btn = document.getElementById('dbxBuildStartBtn');
        if (btn) btn.disabled = !dbxBuildReady || dbxBuildRunning;

        const statusText = document.getElementById('dbxBuildStatusText');
        if (statusText) {
            // has_data is tri-state: null means the probe could not reach the
            // engine, which is not the same as an empty graph.
            statusText.textContent = ts.has_data === null
                ? 'Status unavailable'
                : (ts.has_data ? count + ' triples loaded' : 'No data yet');
        }
    } catch (e) {
        console.error('[DatabricksBuild] info failed', e);
    } finally {
        if (overlay) overlay.classList.add('d-none');
    }
}

function _apiErrorMessage(data, fallback) {
    if (!data || typeof data !== 'object') return fallback;
    return data.message || data.detail || fallback;
}

async function startDatabricksBuild() {
    if (!dbxBuildReady || dbxBuildRunning) return;
    const btn = document.getElementById('dbxBuildStartBtn');
    if (btn) btn.disabled = true;
    dbxBuildRunning = true;
    _hideDbxBuildResult();
    _resetDbxProgressBar();
    try {
        const resp = await fetch('/dtwin/databricks-build/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: '{}',
        });
        const data = await resp.json();
        if (!resp.ok || !data.success || !data.task_id) {
            throw new Error(_apiErrorMessage(data, 'Build could not start'));
        }
        sessionStorage.setItem(DBX_BUILD_TASK_KEY, data.task_id);
        pollDatabricksBuildTask(data.task_id);
    } catch (e) {
        dbxBuildRunning = false;
        if (btn) btn.disabled = !dbxBuildReady;
        const msg = e.message || String(e);
        _showDbxBuildResult('error', '<strong>Build could not start.</strong> ' + _dbxEscape(msg));
        _dbxNotify('Build failed to start: ' + msg, 'error');
    }
}

function _finishDbxBuild(task) {
    sessionStorage.removeItem(DBX_BUILD_TASK_KEY);
    dbxBuildRunning = false;
    const btn = document.getElementById('dbxBuildStartBtn');
    if (btn) btn.disabled = !dbxBuildReady;
    _finishDbxProgressBar(task.status);

    if (task.status === 'failed') {
        const err = task.error || task.message || 'Unknown error';
        _showDbxBuildResult(
            'error',
            '<strong>Build failed.</strong><div class="mt-1">' + _dbxEscape(err) + '</div>'
        );
        _dbxNotify('Delta build failed: ' + err, 'error');
        return;
    }

    if (task.status === 'cancelled') {
        _showDbxBuildResult('warning', '<strong>Build cancelled.</strong>');
        _dbxNotify('Build cancelled', 'warning');
        return;
    }

    const triples = task.result && task.result.triple_count;
    const msg = task.message || 'Build completed';
    if (triples === 0) {
        _showDbxBuildResult(
            'warning',
            '<strong>Build finished with warnings.</strong><div class="mt-1">' + _dbxEscape(msg) + '</div>'
        );
        _dbxNotify(msg, 'warning');
    } else {
        _showDbxBuildResult(
            'success',
            '<strong>Build succeeded.</strong><div class="mt-1">' + _dbxEscape(msg) + '</div>'
        );
        _dbxNotify(msg, 'success');
    }
}

function pollDatabricksBuildTask(taskId) {
    const progressArea = document.getElementById('dbxBuildProgressArea');
    const bar = document.getElementById('dbxBuildProgressBar');
    const step = document.getElementById('dbxBuildProgressStep');
    if (progressArea) progressArea.classList.remove('d-none');

    const timer = setInterval(async () => {
        try {
            const resp = await fetch('/tasks/' + encodeURIComponent(taskId), { credentials: 'same-origin' });
            const data = await resp.json();
            if (!resp.ok || !data.success || !data.task) {
                throw new Error(_apiErrorMessage(data, 'Task not found'));
            }

            const task = data.task;
            const pct = task.progress != null ? task.progress : 0;
            if (bar) {
                bar.style.width = pct + '%';
                bar.textContent = pct + '%';
            }
            if (step) {
                step.textContent = _taskStepMessage(task);
            }

            if (task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled') {
                clearInterval(timer);
                _finishDbxBuild(task);
                await loadDatabricksBuildInfo();
            }
        } catch (e) {
            clearInterval(timer);
            sessionStorage.removeItem(DBX_BUILD_TASK_KEY);
            dbxBuildRunning = false;
            const btn = document.getElementById('dbxBuildStartBtn');
            if (btn) btn.disabled = !dbxBuildReady;
            _finishDbxProgressBar('failed');
            const msg = e.message || String(e);
            _showDbxBuildResult(
                'error',
                '<strong>Could not monitor build.</strong><div class="mt-1">' + _dbxEscape(msg) + '</div>'
            );
            _dbxNotify('Build monitoring failed: ' + msg, 'error');
            console.warn('[DatabricksBuild] poll error', e);
        }
    }, 1500);
}

document.addEventListener('DOMContentLoaded', function () {
    applyTripleStoreBackendPanels();

    document.getElementById('dbxBuildRefreshBtn')?.addEventListener('click', loadDatabricksBuildInfo);
    document.getElementById('dbxBuildStartBtn')?.addEventListener('click', startDatabricksBuild);

    document.addEventListener('sidebarSectionChanged', function (e) {
        if (e.detail?.section === 'sync' && _tsxBackend() === 'databricks') {
            loadDatabricksBuildInfo();
        }
    });

    const resumed = sessionStorage.getItem(DBX_BUILD_TASK_KEY);
    if (resumed) {
        dbxBuildRunning = true;
        pollDatabricksBuildTask(resumed);
    }

    if (_tsxBackend() === 'databricks') {
        loadDatabricksBuildInfo();
    }
});
