/**
 * Databricks triple-store build page — R2RML VIEW into a Delta TABLE, or into
 * a pass-through VIEW when the domain uses view-only materialization.
 */

const DBX_BUILD_TASK_KEY = 'ontobricks_databricks_build_task';
const DBX_ADJ_REFRESH_TASK_KEY = 'ontobricks_databricks_adjacency_refresh_task';
const DBX_BUILD_FAST_POLL_MS = 300;
const DBX_BUILD_FINAL_POLL_MS = 1000;

let dbxBuildReady = false;
let dbxBuildRunning = false;
let dbxAdjacencyRefreshRunning = false;
let dbxGraphHasData = false;
let dbxBuildInfoLoaded = false;

const _dbxTimedBuildLog = TimedTaskLog.create({
    cardId: "dbxBuildLogCard",
    listId: "dbxBuildLogList",
    totalId: "dbxBuildLogTotal",
    badgeId: "dbxBuildLogBadge",
    exportButtonId: "dbxBuildLogExport",
    title: "OntoBricks — Lakehouse Graph Build Log",
    filenamePrefix: "lakehouse-graph-build",
});

function showDbxBuildLog() {
    _dbxTimedBuildLog.show();
}

function hideDbxBuildLog() {
    _dbxTimedBuildLog.hide();
}

function renderDbxBuildLog(task) {
    _dbxTimedBuildLog.render(task);
}

function exportDbxBuildLog() {
    _dbxTimedBuildLog.export();
}

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

function _dbxBuildPollDelay(task) {
    const steps = Array.isArray(task?.steps) ? task.steps : [];
    const finalStep = Math.max(steps.length - 1, 0);
    return Number(task?.current_step || 0) >= finalStep
        ? DBX_BUILD_FINAL_POLL_MS
        : DBX_BUILD_FAST_POLL_MS;
}

function applyTripleStoreBackendPanels() {
    const backend = _tsxBackend();
    const lakePanel = document.getElementById('sync-lakebase-panel');
    const dbxPanel = document.getElementById('sync-databricks-panel');
    const isDbx = backend === 'databricks';

    if (lakePanel) lakePanel.classList.toggle('d-none', isDbx);
    if (dbxPanel) dbxPanel.classList.toggle('d-none', !isDbx);
}

function _updateDbxAdjacencyButton() {
    const btn = document.getElementById('dbxAdjacencyRefreshBtn');
    if (!btn) return;
    const backend = String(_tsxBackend() || 'lakebase').toLowerCase();
    const visible = backend === 'databricks';
    if (backend !== 'databricks') {
        btn.classList.add('d-none');
        btn.disabled = true;
        return;
    }
    btn.classList.toggle('d-none', !visible);
    btn.disabled = !dbxBuildReady
        || !dbxGraphHasData
        || dbxBuildRunning
        || dbxAdjacencyRefreshRunning;
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

function _setDbxBuildInitialState(state) {
    const loading = document.getElementById('dbxBuildLoadingState');
    const error = document.getElementById('dbxBuildLoadError');
    const content = document.getElementById('dbxBuildContent');
    loading?.classList.toggle('d-none', state !== 'loading');
    error?.classList.toggle('d-none', state !== 'error');
    content?.classList.toggle('d-none', state !== 'ready');
}

async function loadDatabricksBuildInfo() {
    const isInitialLoad = !dbxBuildInfoLoaded;
    if (isInitialLoad) {
        _setDbxBuildInitialState('loading');
    }
    try {
        const resp = await fetch('/dtwin/databricks-build/info', { credentials: 'same-origin' });
        const data = await resp.json();
        if (!resp.ok || !data.success) {
            throw new Error(_apiErrorMessage(data, 'Could not load triple store information'));
        }

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
        dbxGraphHasData = !!(ts.has_data);
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
        if (btn) btn.disabled = !dbxBuildReady || dbxBuildRunning || dbxAdjacencyRefreshRunning;
        _updateDbxAdjacencyButton();

        const statusText = document.getElementById('dbxBuildStatusText');
        if (statusText) {
            // has_data is tri-state: null means the probe could not reach the
            // engine, which is not the same as an empty graph.
            statusText.textContent = ts.has_data === null
                ? 'Status unavailable'
                : (ts.has_data ? count + ' triples loaded' : 'No data yet');
        }
        if (isInitialLoad) {
            dbxBuildInfoLoaded = true;
            _setDbxBuildInitialState('ready');
        }
    } catch (e) {
        if (isInitialLoad && !dbxBuildInfoLoaded) {
            _setDbxBuildInitialState('error');
        }
        console.error('[DatabricksBuild] info failed', e);
    }
}

function _apiErrorMessage(data, fallback) {
    if (!data || typeof data !== 'object') return fallback;
    return data.message || data.detail || fallback;
}

async function startDatabricksBuild() {
    if (!dbxBuildReady || dbxBuildRunning || dbxAdjacencyRefreshRunning) return;
    const btn = document.getElementById('dbxBuildStartBtn');
    if (btn) btn.disabled = true;
    dbxBuildRunning = true;
    _hideDbxBuildResult();
    _resetDbxProgressBar();
    showDbxBuildLog();
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
        _updateDbxAdjacencyButton();
        const msg = e.message || String(e);
        _showDbxBuildResult('error', '<strong>Build could not start.</strong> ' + _dbxEscape(msg));
        _dbxNotify('Build failed to start: ' + msg, 'error');
    }
}

async function startDatabricksAdjacencyRefresh() {
    if (!dbxBuildReady || dbxBuildRunning || dbxAdjacencyRefreshRunning || !dbxGraphHasData) {
        return;
    }
    const btn = document.getElementById('dbxAdjacencyRefreshBtn');
    if (btn) btn.disabled = true;
    dbxAdjacencyRefreshRunning = true;
    _hideDbxBuildResult();
    _resetDbxProgressBar();

    const progressArea = document.getElementById('dbxBuildProgressArea');
    const step = document.getElementById('dbxBuildProgressStep');
    if (progressArea) progressArea.classList.remove('d-none');
    if (step) step.textContent = 'Starting adjacency refresh...';

    try {
        const resp = await fetch('/dtwin/adjacency/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: '{}',
        });
        const data = await resp.json();
        if (!resp.ok || !data.success || !data.task_id) {
            throw new Error(_apiErrorMessage(data, 'Adjacency refresh could not start'));
        }
        sessionStorage.setItem(DBX_ADJ_REFRESH_TASK_KEY, data.task_id);
        pollDatabricksAdjacencyTask(data.task_id);
    } catch (e) {
        dbxAdjacencyRefreshRunning = false;
        _finishDbxProgressBar('failed');
        const msg = e.message || String(e);
        _showDbxBuildResult(
            'error',
            '<strong>Adjacency refresh could not start.</strong><div class="mt-1">' + _dbxEscape(msg) + '</div>'
        );
        _dbxNotify('Adjacency refresh failed to start: ' + msg, 'error');
        _updateDbxAdjacencyButton();
    }
}

function pollDatabricksAdjacencyTask(taskId) {
    const progressArea = document.getElementById('dbxBuildProgressArea');
    const bar = document.getElementById('dbxBuildProgressBar');
    const step = document.getElementById('dbxBuildProgressStep');
    if (progressArea) progressArea.classList.remove('d-none');

    async function pollOnce() {
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
            if (step) step.textContent = _taskStepMessage(task);

            if (task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled') {
                sessionStorage.removeItem(DBX_ADJ_REFRESH_TASK_KEY);
                dbxAdjacencyRefreshRunning = false;
                _finishDbxProgressBar(task.status);
                if (task.status === 'completed') {
                    _showDbxBuildResult(
                        'success',
                        '<strong>Adjacency refresh succeeded.</strong><div class="mt-1">' + _dbxEscape(task.message || '') + '</div>'
                    );
                    _dbxNotify('Adjacency refreshed successfully', 'success');
                } else if (task.status === 'failed') {
                    const err = task.error || task.message || 'Unknown error';
                    _showDbxBuildResult(
                        'error',
                        '<strong>Adjacency refresh failed.</strong><div class="mt-1">' + _dbxEscape(err) + '</div>'
                    );
                    _dbxNotify('Adjacency refresh failed: ' + err, 'error');
                } else {
                    _showDbxBuildResult('warning', '<strong>Adjacency refresh cancelled.</strong>');
                    _dbxNotify('Adjacency refresh cancelled', 'warning');
                }
                if (typeof refreshTasks === 'function') refreshTasks();
                await loadDatabricksBuildInfo();
                return;
            }
            setTimeout(pollOnce, 1500);
        } catch (e) {
            sessionStorage.removeItem(DBX_ADJ_REFRESH_TASK_KEY);
            dbxAdjacencyRefreshRunning = false;
            _finishDbxProgressBar('failed');
            const msg = e.message || String(e);
            _showDbxBuildResult(
                'error',
                '<strong>Could not monitor adjacency refresh.</strong><div class="mt-1">' + _dbxEscape(msg) + '</div>'
            );
            _dbxNotify('Adjacency refresh monitoring failed: ' + msg, 'error');
            _updateDbxAdjacencyButton();
        }
    }

    pollOnce();
}

function _finishDbxBuild(task) {
    sessionStorage.removeItem(DBX_BUILD_TASK_KEY);
    dbxBuildRunning = false;
    renderDbxBuildLog(task);
    const btn = document.getElementById('dbxBuildStartBtn');
    if (btn) btn.disabled = !dbxBuildReady;
    _updateDbxAdjacencyButton();
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

    async function pollOnce() {
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
            renderDbxBuildLog(task);

            if (task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled') {
                _finishDbxBuild(task);
                await loadDatabricksBuildInfo();
                return;
            }
            setTimeout(pollOnce, _dbxBuildPollDelay(task));
        } catch (e) {
            sessionStorage.removeItem(DBX_BUILD_TASK_KEY);
            dbxBuildRunning = false;
            const btn = document.getElementById('dbxBuildStartBtn');
            if (btn) btn.disabled = !dbxBuildReady;
            _updateDbxAdjacencyButton();
            _finishDbxProgressBar('failed');
            const msg = e.message || String(e);
            _showDbxBuildResult(
                'error',
                '<strong>Could not monitor build.</strong><div class="mt-1">' + _dbxEscape(msg) + '</div>'
            );
            _dbxNotify('Build monitoring failed: ' + msg, 'error');
            console.warn('[DatabricksBuild] poll error', e);
        }
    }

    pollOnce();
}

async function checkAndResumeDatabricksTask(taskKey, onRunningTask, onTerminalTask) {
    const taskId = sessionStorage.getItem(taskKey);
    if (!taskId) return false;
    try {
        const resp = await fetch('/tasks/' + encodeURIComponent(taskId), { credentials: 'same-origin' });
        const data = await resp.json();
        if (!resp.ok || !data.success || !data.task) {
            sessionStorage.removeItem(taskKey);
            return false;
        }
        const task = data.task;
        if (task.status === 'running' || task.status === 'pending') {
            onRunningTask(taskId, task);
            return true;
        }
        sessionStorage.removeItem(taskKey);
        if (typeof onTerminalTask === 'function') onTerminalTask(task);
        return false;
    } catch (_) {
        sessionStorage.removeItem(taskKey);
        return false;
    }
}

document.addEventListener('DOMContentLoaded', async function () {
    applyTripleStoreBackendPanels();
    _updateDbxAdjacencyButton();

    document.getElementById('dbxBuildRefreshBtn')?.addEventListener('click', loadDatabricksBuildInfo);
    document.getElementById('dbxBuildLoadRetry')?.addEventListener('click', loadDatabricksBuildInfo);
    document.getElementById('dbxBuildStartBtn')?.addEventListener('click', startDatabricksBuild);
    document.getElementById('dbxAdjacencyRefreshBtn')?.addEventListener('click', startDatabricksAdjacencyRefresh);
    document.getElementById('dbxBuildLogHide')?.addEventListener('click', hideDbxBuildLog);
    document.getElementById('dbxBuildLogExport')?.addEventListener('click', exportDbxBuildLog);

    document.addEventListener('sidebarSectionChanged', function (e) {
        if (e.detail?.section === 'sync' && _tsxBackend() === 'databricks') {
            loadDatabricksBuildInfo();
        }
    });

    await checkAndResumeDatabricksTask(
        DBX_BUILD_TASK_KEY,
        function (taskId, task) {
            dbxBuildRunning = true;
            showDbxBuildLog();
            renderDbxBuildLog(task);
            pollDatabricksBuildTask(taskId);
        },
        function (task) {
            _finishDbxBuild(task);
        }
    );
    await checkAndResumeDatabricksTask(
        DBX_ADJ_REFRESH_TASK_KEY,
        function (taskId) {
            dbxAdjacencyRefreshRunning = true;
            pollDatabricksAdjacencyTask(taskId);
        }
    );

    if (_tsxBackend() === 'databricks') {
        loadDatabricksBuildInfo();
    }
});
