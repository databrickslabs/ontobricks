/**
 * Graph → Management → Certified Graph.
 *
 * Shows whether the version is locked (IN-REVIEW / PUBLISHED), the backend
 * panel (Lakebase / Neo4j) and the certified build runs; the Build button
 * starts a certified build and follows its task.
 */
(function () {
    'use strict';

    const POLL_MS = 1500;
    // Targets with a backend copy: the Lakebase / Neo4j panel shows the
    // architecture and the live build log, and runs the build.
    const PANEL_TARGETS = ['lakebase', 'neo4j'];

    let _status = null;
    let _runs = [];
    let _running = false;

    function _el(id) {
        return document.getElementById(id);
    }

    function _renderRuns(runs) {
        _runs = runs || [];
        const body = _el('certRunsBody');
        if (!body) return;
        body.replaceChildren();
        _runs.forEach(function (run, idx) {
            const row = document.createElement('tr');
            row.innerHTML = RunsRender.buildRunCells(run, idx, { handler: 'showCertifiedRunDetails' });
            body.appendChild(row);
        });
        _el('certRunsEmpty').classList.toggle('d-none', _runs.length > 0);
        _el('certRunsWrapper').classList.toggle('d-none', _runs.length === 0);
    }

    function _renderBadge(status) {
        const badge = _el('certStateBadge');
        if (!badge) return;
        let label = 'Not built';
        let tone = 'text-bg-secondary';
        if (_running) {
            label = 'Building';
            tone = 'text-bg-primary';
        } else if (status.certified) {
            label = 'Certified';
            tone = 'text-bg-success';
        }
        badge.className = 'badge ' + tone;
        badge.textContent = label;
    }

    function _render(status) {
        _status = status;
        if (typeof _setBackendBrandIcon === 'function') {
            _setBackendBrandIcon(_el('certBackendIcon'), status.backend);
        }
        _el('certReadOnlyNotice').classList.toggle('d-none', status.editable !== false);
        _renderBadge(status);
        _renderRuns(status.runs);
        _showBackendPanel(_usesPanel());
        _syncButton();
    }

    function _usesPanel() {
        return !!_status && PANEL_TARGETS.indexOf(_status.target) !== -1;
    }

    function _showBackendPanel(show) {
        _el('sync-lakebase-panel').classList.toggle('d-none', !show);
        const actions = _el('sync-lakebase-actions');
        actions.classList.toggle('d-none', !show);
        actions.classList.toggle('d-flex', show);
    }

    function _syncButton() {
        const btn = _el('certBuildStartBtn');
        if (!btn) return;
        const ready = _status && _status.editable && _status.target !== 'none';
        btn.disabled = _running || !ready;
    }

    function _showError(message) {
        const box = _el('certLoadError');
        _el('certLoadErrorMessage').textContent = message || '';
        box.classList.toggle('d-none', !message);
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

    function _showProgress(pct, step) {
        _el('certProgressArea').classList.remove('d-none');
        const bar = _el('certProgressBar');
        bar.style.width = pct + '%';
        bar.textContent = pct + '%';
        _el('certProgressStep').textContent = step || '';
    }

    function _stepMessage(task) {
        const steps = task.steps || [];
        const step = steps[task.current_step != null ? task.current_step : 0];
        return (step && step.description) || task.message || '';
    }

    async function _poll(taskId) {
        const resp = await fetch('/tasks/' + encodeURIComponent(taskId), { credentials: 'same-origin' });
        const data = await resp.json();
        if (!resp.ok || !data.success || !data.task) {
            throw new Error((data && (data.message || data.detail)) || 'Task not found');
        }
        const task = data.task;
        _showProgress(task.progress || 0, _stepMessage(task));
        if (task.status === 'completed') return task;
        if (task.status === 'failed' || task.status === 'cancelled') {
            throw new Error(task.error || task.message || 'Certified build failed');
        }
        await new Promise(function (resolve) { setTimeout(resolve, POLL_MS); });
        return _poll(taskId);
    }

    async function startCertifiedBuild() {
        if (_running) return;
        _running = true;
        _syncButton();
        if (_status) _renderBadge(_status);
        if (_usesPanel() && typeof startCertifiedGraphBuild === 'function') {
            try {
                await startCertifiedGraphBuild();
            } finally {
                _running = false;
                await loadCertifiedStatus();
            }
            return;
        }
        _showProgress(0, 'Starting the certified build...');
        try {
            const data = await apiRequest('/dtwin/certified-build/start', { method: 'POST', body: '{}' });
            const task = await _poll(data.task_id);
            showNotification(task.message || 'Certified graph built', 'success');
        } catch (err) {
            showNotification(err.message, 'error');
        } finally {
            _running = false;
            _el('certProgressArea').classList.add('d-none');
            await loadCertifiedStatus();
        }
    }

    window.loadCertifiedStatus = loadCertifiedStatus;

    window.showCertifiedRunDetails = function (idx) {
        if (typeof showRunDetailsObj === 'function') showRunDetailsObj(_runs[idx]);
    };

    document.addEventListener('DOMContentLoaded', function () {
        if (!_el('certified-section')) return;
        _el('certRefreshBtn').addEventListener('click', loadCertifiedStatus);
        const btn = _el('certBuildStartBtn');
        if (btn) btn.addEventListener('click', startCertifiedBuild);
        if (new URLSearchParams(window.location.search).get('section') === 'certified') {
            loadCertifiedStatus();
        }
    });

    document.addEventListener('sidebarSectionChanged', function (e) {
        const section = e.detail && e.detail.section;
        if (section === 'certified' && !_running) loadCertifiedStatus();
    });
})();
