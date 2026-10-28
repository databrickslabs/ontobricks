/**
 * Simulation page — shared state, API helper, Scenarios + Runs sections,
 * and background-run polling. Workbench / Results live in their own modules.
 */
const SimulationPage = {
    config: {},
    scenarios: [],
    pollTimer: null,
    runningId: null,
    _pollBusy: false,
    POLL_MS: 2000,
    TASK_KEY: 'ontobricks_simulation_task',
    LAST_KEY: 'ontobricks_simulation_open',
    _openingEditor: false,

    init() {
        const cfgEl = document.getElementById('simulation-config');
        try {
            this.config = cfgEl ? JSON.parse(cfgEl.textContent || '{}') : {};
        } catch (_) {
            this.config = {};
        }
        this._bind();
        this.loadScenarios();
        this._resumeTask();
        document.addEventListener('sidebarSectionChanged', (e) => {
            const section = e.detail && e.detail.section;
            if (section === 'runs') this.loadRuns();
        });
        const scenariosLink = document.querySelector('.sidebar-nav .nav-link[data-section="scenarios"]');
        if (scenariosLink) {
            scenariosLink.addEventListener('click', () => this.showList({ replace: true }));
        }
        window.addEventListener('popstate', () => this.applyScenarioView({ fromPop: true }));
        setTimeout(() => this.applyScenarioView({ replace: true }), 0);
    },

    _bind() {
        const on = (id, fn) => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('click', fn);
        };
        on('simRefreshBtn', () => this.loadScenarios());
        on('simRunsRefreshBtn', () => this.loadRuns());
        on('simNewBtn', () => this.newScenario());
        const body = document.getElementById('simScenariosBody');
        if (body) {
            body.addEventListener('click', (e) => {
                const btn = e.target.closest('[data-action]');
                if (!btn) return;
                if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button) return;
                e.preventDefault();
                const id = btn.dataset.id;
                const action = btn.dataset.action;
                if (action === 'open') this.openScenario(id);
                else if (action === 'run') this.startRun(id);
                else if (action === 'results') this.showResults(id);
                else if (action === 'purge') this.purgeScenario(id);
                else if (action === 'delete') this.deleteScenario(id);
            });
        }
    },

    // -- API ---------------------------------------------------------------

    async api(url, options = {}) {
        const opts = { credentials: 'same-origin', ...options };
        if (opts.body && typeof opts.body !== 'string') {
            opts.body = JSON.stringify(opts.body);
            opts.headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
        }
        const resp = await fetch(url, opts);
        let data = {};
        try {
            data = await resp.json();
        } catch (_) {
            data = {};
        }
        if (!resp.ok || data.success === false) {
            throw new Error(data.message || data.detail || `Request failed (${resp.status})`);
        }
        return data;
    },

    // -- Helpers -----------------------------------------------------------

    esc(value) {
        return window.escapeHtml ? window.escapeHtml(value) : String(value == null ? '' : value);
    },

    shortUri(uri) {
        const s = String(uri || '');
        const hashAt = s.lastIndexOf('#');
        if (hashAt >= 0 && hashAt < s.length - 1) return s.substring(hashAt + 1);
        const path = s.split('?')[0].replace(/\/+$/, '');
        const cut = Math.max(path.lastIndexOf('/'), path.lastIndexOf(':'));
        return cut >= 0 && cut < path.length - 1 ? path.substring(cut + 1) : s;
    },

    isUri(value) {
        const s = String(value || '');
        return /^https?:\/\//i.test(s) || s.startsWith('urn:');
    },

    termLabel(value, label) {
        const text = String(label || '').trim();
        const raw = String(value || '');
        if (text && text !== raw) return text;
        return this.isUri(raw) ? this.shortUri(raw) : raw;
    },

    uriCell(uri, label) {
        const shown = (label && label !== uri) ? label : this.shortUri(uri);
        return `<span class="sim-uri" title="${this.esc(uri)}">${this.esc(shown)}</span>`;
    },

    async resolveEntityLabel(uri) {
        const key = String(uri || '').trim();
        if (!key) return '';
        this._labelCache = this._labelCache || {};
        if (this._labelCache[key]) return this._labelCache[key];
        try {
            const data = await this.api('/dtwin/sync/filter', {
                method: 'POST',
                body: {
                    phase: 'preview',
                    field: 'id',
                    match_type: 'exact',
                    value: key,
                    include_inferred: true,
                },
            });
            const seed = (data.seeds || []).find((s) => s.uri === key) || (data.seeds || [])[0];
            const label = (seed && seed.label) ? String(seed.label) : '';
            if (label) this._labelCache[key] = label;
            return label;
        } catch (_) {
            return '';
        }
    },

    fmtDate(iso) {
        if (!iso) return '–';
        const d = new Date(iso);
        return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
    },

    statusBadge(status) {
        const map = {
            draft: 'bg-secondary',
            running: 'bg-info text-dark',
            ready: 'bg-success',
            success: 'bg-success',
            failed: 'bg-danger',
            error: 'bg-danger',
        };
        const cls = map[status] || 'bg-secondary';
        return `<span class="badge ${cls}">${this.esc(status || 'draft')}</span>`;
    },

    scopeLabel(scope) {
        const s = scope || {};
        if (s.type === 'cohort') return `Cohort · ${this.esc(s.focus)}`;
        if (s.type === 'class') return `Class · ${this.uriCell(s.focus, s.focus_label)}`;
        return `${this.uriCell(s.focus, s.focus_label)} <span class="text-muted">· ${this.esc(s.hops)} hops</span>`;
    },

    // -- Scenarios ---------------------------------------------------------

    _loadingRow(colspan, label) {
        return `<tr><td colspan="${colspan}" class="text-center text-muted py-4">`
            + '<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>'
            + `${this.esc(label)}</td></tr>`;
    },

    async loadScenarios() {
        const body = document.getElementById('simScenariosBody');
        const refresh = document.getElementById('simRefreshBtn');
        if (body) body.innerHTML = this._loadingRow(8, 'Loading scenarios…');
        if (refresh) refresh.disabled = true;
        try {
            const data = await this.api('/simulation/list');
            this.scenarios = data.simulations || [];
            this.renderScenarios();
        } catch (err) {
            this.scenarios = [];
            if (body) {
                body.innerHTML = `<tr><td colspan="8" class="text-center text-muted py-4">${this.esc(err.message)}</td></tr>`;
            }
        } finally {
            if (refresh) refresh.disabled = false;
        }
        if (window.SimulationResults && SimulationResults.selectedId) SimulationResults.load();
    },

    renderScenarios() {
        const body = document.getElementById('simScenariosBody');
        if (!body) return;
        if (!this.scenarios.length) {
            body.innerHTML = '<tr><td colspan="8" class="text-center text-muted py-4">'
                + '<i class="bi bi-sliders2-vertical d-block fs-3 mb-2"></i>'
                + 'No scenarios yet — create one, or use <em>Simulate from here</em> on an entity in the Knowledge Graph.'
                + '</td></tr>';
            return;
        }
        body.innerHTML = this.scenarios.map((s) => {
            const spec = s.spec || {};
            const sum = s.diff_summary || {};
            const ready = s.status === 'ready';
            const running = s.status === 'running';
            return `<tr>
                <td><a href="/simulation/?section=scenarios&simulation=${this.esc(s.id)}" data-action="open" data-id="${this.esc(s.id)}" class="fw-semibold">${this.esc(s.label || s.id)}</a></td>
                <td class="small">${this.scopeLabel(spec.scope)}</td>
                <td class="text-end">${(spec.patches || []).length}</td>
                <td>${this.statusBadge(s.status)}</td>
                <td class="text-end text-success">${ready ? '+' + (sum.added_count || 0) : '–'}</td>
                <td class="text-end text-danger">${ready ? '−' + (sum.removed_count || 0) : '–'}</td>
                <td class="small text-muted">${this.fmtDate(s.last_run_at)}</td>
                <td class="text-end text-nowrap">
                    <button type="button" class="btn btn-sm btn-outline-secondary" data-action="results" data-id="${this.esc(s.id)}" title="Results" ${ready ? '' : 'disabled'}><i class="bi bi-clipboard-data"></i></button>
                    <button type="button" class="btn btn-sm btn-outline-primary" data-action="run" data-id="${this.esc(s.id)}" title="Run" ${running ? 'disabled' : ''}><i class="bi bi-play-fill"></i></button>
                    <button type="button" class="btn btn-sm btn-outline-warning" data-action="purge" data-id="${this.esc(s.id)}" title="Purge sandbox" ${running || s.status === 'draft' ? 'disabled' : ''}><i class="bi bi-eraser"></i></button>
                    <button type="button" class="btn btn-sm btn-outline-danger" data-action="delete" data-id="${this.esc(s.id)}" title="Delete" ${running ? 'disabled' : ''}><i class="bi bi-trash"></i></button>
                </td>
            </tr>`;
        }).join('');
    },

    openScenario(id, { tab } = {}) {
        this._openEditor({ id, tab, sync: true });
    },

    showResults(id) {
        this._openEditor({ id, tab: 'results', sync: true });
    },

    newScenario({ entity, label, sync = true, replace = false } = {}) {
        this._showEditorPane();
        if (window.SimulationWorkbench) {
            if (entity) SimulationWorkbench.seedFromEntity(entity, label);
            else {
                SimulationWorkbench.reset();
                SimulationWorkbench.showTab('general');
            }
        }
        if (sync) this.syncScenarioUrl({ simulation: 'new', entity, label }, { replace });
    },

    showList({ replace = false } = {}) {
        this._toggleEditor(false);
        this.syncScenarioUrl({}, { replace });
        this.loadScenarios();
    },

    rememberScenario(id) {
        if (!id || id === 'new') return;
        try { sessionStorage.setItem(this.LAST_KEY, id); } catch (_) { /* ignore */ }
    },

    syncScenarioUrl({ simulation, tab, entity, label } = {}, { replace = false } = {}) {
        const url = new URL(window.location.href);
        url.hash = '';
        url.searchParams.set('section', 'scenarios');
        const setOrDel = (key, value) => {
            const text = String(value || '').trim();
            if (text) url.searchParams.set(key, text);
            else url.searchParams.delete(key);
        };
        setOrDel('simulation', simulation);
        setOrDel('tab', tab);
        setOrDel('entity', entity);
        setOrDel('label', label);
        const state = { section: 'scenarios', simulation: simulation || null, tab: tab || null };
        const method = replace ? 'replaceState' : 'pushState';
        if (window.history && window.history[method]) window.history[method](state, '', url);
    },

    applyScenarioView({ replace = false, fromPop = false } = {}) {
        const params = new URLSearchParams(window.location.search);
        const rawSection = (params.get('section') || '').trim();
        if (rawSection && rawSection !== 'scenarios' && rawSection !== 'workbench' && rawSection !== 'results') {
            return;
        }
        const simId = (params.get('simulation') || '').trim();
        const entity = (params.get('entity') || '').trim();
        const tab = (params.get('tab') || '').trim() === 'results' || rawSection === 'results'
            ? 'results' : '';
        const label = (params.get('label') || '').trim();
        const last = (() => {
            try { return sessionStorage.getItem(this.LAST_KEY) || ''; } catch (_) { return ''; }
        })();

        if (simId && simId !== 'new') {
            this._openEditor({
                id: simId,
                tab,
                sync: rawSection !== 'scenarios' && !fromPop,
                replace: true,
            });
            return;
        }
        if (simId === 'new' || entity) {
            this.newScenario({
                entity,
                label,
                sync: rawSection !== 'scenarios' && !fromPop,
                replace: true,
            });
            return;
        }
        if ((rawSection === 'workbench' || rawSection === 'results') && last) {
            this._openEditor({ id: last, tab, sync: !fromPop, replace: true });
            return;
        }
        this._toggleEditor(false);
        if (rawSection === 'workbench' || rawSection === 'results') {
            this.syncScenarioUrl({}, { replace: true });
        }
    },

    _showEditorPane() {
        this._openingEditor = true;
        if (window.SidebarNav && SidebarNav.getActiveSection() !== 'scenarios') {
            SidebarNav._showSection('scenarios');
        }
        this._toggleEditor(true);
        this._openingEditor = false;
    },

    _toggleEditor(on) {
        const list = document.getElementById('simScenariosList');
        const editor = document.getElementById('simScenariosEditor');
        if (list) list.classList.toggle('d-none', on);
        if (editor) editor.classList.toggle('d-none', !on);
    },

    _openEditor({ id, tab, sync = false, replace = false } = {}) {
        if (!id) return;
        this._showEditorPane();
        this.rememberScenario(id);
        if (sync) this.syncScenarioUrl({ simulation: id, tab }, { replace });
        const openTab = () => {
            if (window.SimulationWorkbench && tab) SimulationWorkbench.showTab(tab);
            if (tab === 'results' && window.SimulationResults) SimulationResults.select(id);
        };
        if (window.SimulationWorkbench) {
            if (SimulationWorkbench.currentId !== id) {
                Promise.resolve(SimulationWorkbench.load(id)).then(openTab);
            } else {
                openTab();
            }
        }
    },

    async purgeScenario(id) {
        const sim = this.scenarios.find((s) => s.id === id);
        const label = this.esc(sim ? sim.label : id);
        const ok = await showConfirmDialog({
            title: 'Purge simulation sandbox?',
            message: `Drop the sandbox after-picture for "<strong>${label}</strong>"? `
                + 'The scenario definition is kept as a draft so you can run it again.',
            confirmText: 'Purge',
            cancelText: 'Cancel',
            confirmClass: 'btn-warning',
            icon: 'eraser',
        });
        if (!ok) return;
        try {
            await this.api(`/simulation/${encodeURIComponent(id)}/purge`, { method: 'POST' });
            showNotification('Simulation sandbox purged', 'success');
            this.loadScenarios();
        } catch (err) {
            showNotification(`Purge failed: ${err.message}`, 'error');
        }
    },

    async deleteScenario(id) {
        const sim = this.scenarios.find((s) => s.id === id);
        const ok = await showDeleteConfirm(this.esc(sim ? sim.label : id), 'scenario');
        if (!ok) return;
        try {
            await this.api(`/simulation/${encodeURIComponent(id)}`, { method: 'DELETE' });
            showNotification('Scenario deleted', 'success');
            if (window.SimulationWorkbench && SimulationWorkbench.currentId === id) {
                this.showList({ replace: true });
            }
            this.loadScenarios();
        } catch (err) {
            showNotification(`Delete failed: ${err.message}`, 'error');
        }
    },

    // -- Runs (background task) -------------------------------------------

    async startRun(id) {
        if (this.pollTimer) {
            showNotification('A simulation is already running', 'warning');
            return;
        }
        try {
            const data = await this.api(`/simulation/${encodeURIComponent(id)}/run`, { method: 'POST' });
            sessionStorage.setItem(this.TASK_KEY, JSON.stringify({ taskId: data.task_id, simId: id }));
            this._poll(data.task_id, id);
            if (typeof refreshTasks === 'function') refreshTasks();
            showNotification('Simulation started...', 'info');
            this.loadScenarios();
        } catch (err) {
            showNotification(`Simulation could not start: ${err.message}`, 'error');
        }
    },

    _resumeTask() {
        let saved = null;
        try {
            saved = JSON.parse(sessionStorage.getItem(this.TASK_KEY) || 'null');
        } catch (_) {
            saved = null;
        }
        if (saved && saved.taskId) this._poll(saved.taskId, saved.simId);
    },

    _setProgress(visible, pct, msg, { done = false, failed = false } = {}) {
        const box = document.getElementById('simWbProgress');
        if (!box) return;
        box.classList.toggle('d-none', !visible);
        const bar = document.getElementById('simWbProgressBar');
        const pctEl = document.getElementById('simWbProgressPct');
        const msgEl = document.getElementById('simWbProgressMsg');
        const doneBox = document.getElementById('simWbProgressDone');
        if (bar) {
            bar.style.width = `${pct || 0}%`;
            bar.setAttribute('aria-valuenow', String(pct || 0));
            bar.classList.toggle('bg-success', done);
            bar.classList.toggle('bg-danger', failed);
        }
        if (pctEl) pctEl.textContent = `${pct || 0}%`;
        if (msgEl && msg) msgEl.textContent = msg;
        if (doneBox) doneBox.classList.toggle('d-none', !done);
        const runBtn = document.getElementById('simWbRunBtn');
        if (runBtn && this.config.supported) runBtn.disabled = visible && !done && !failed;
    },

    _stopPolling({ keepProgress = false } = {}) {
        if (this.pollTimer) clearInterval(this.pollTimer);
        this.pollTimer = null;
        sessionStorage.removeItem(this.TASK_KEY);
        if (!keepProgress) {
            this.runningId = null;
            this._setProgress(false, 0, '');
        }
    },

    _poll(taskId, simId) {
        this.runningId = simId;
        this._setProgress(true, 0, 'Starting simulation…');
        const tick = async () => {
            if (this._pollBusy) return;
            this._pollBusy = true;
            try {
                const resp = await fetch(`/tasks/${encodeURIComponent(taskId)}`, { credentials: 'same-origin' });
                const data = await resp.json();
                if (!data.success || !data.task) {
                    this._stopPolling();
                    return;
                }
                const task = data.task;
                if (task.status === 'completed') {
                    this._stopPolling({ keepProgress: true });
                    this._setProgress(true, 100, task.message || 'Simulation complete', { done: true });
                    if (typeof refreshTasks === 'function') refreshTasks();
                    showNotification(task.message || 'Simulation complete — open Results', 'success');
                    await this.loadScenarios();
                    if (window.SimulationWorkbench) SimulationWorkbench.refreshArtifacts();
                    this.showResults(simId);
                } else if (task.status === 'failed' || task.status === 'cancelled') {
                    this._stopPolling({ keepProgress: true });
                    if (typeof refreshTasks === 'function') refreshTasks();
                    this._setProgress(
                        true,
                        Math.round(task.progress || 0),
                        `Simulation failed: ${task.error || task.message || 'unknown error'}`,
                        { failed: true },
                    );
                    showNotification(`Simulation failed: ${task.error || task.message || 'unknown error'}`, 'error');
                    this.loadScenarios();
                } else {
                    this._setProgress(true, Math.round(task.progress || 0), task.message || 'Running…');
                }
            } catch (err) {
                console.error('Simulation poll error:', err);
            } finally {
                this._pollBusy = false;
            }
        };
        tick();
        this.pollTimer = setInterval(tick, this.POLL_MS);
    },

    // -- Runs section ------------------------------------------------------

    async loadRuns() {
        const body = document.getElementById('simRunsBody');
        if (!body) return;
        try {
            const data = await this.api('/simulation/runs?limit=100');
            const runs = data.runs || [];
            if (!runs.length) {
                body.innerHTML = '<tr><td colspan="6" class="text-center text-muted py-4">No runs yet.</td></tr>';
                return;
            }
            const labels = Object.fromEntries(this.scenarios.map((s) => [s.id, s.label]));
            body.innerHTML = runs.map((r) => `<tr>
                <td class="small">${this.fmtDate(r.started_at)}</td>
                <td>${this.esc(labels[r.simulation_id] || r.simulation_id)}</td>
                <td>${this.statusBadge(r.status)}</td>
                <td class="small ${r.error ? 'text-danger' : ''}">${this.esc(r.error || r.message || '')}</td>
                <td class="text-end small">${Number(r.duration_s || 0).toFixed(1)} s</td>
                <td class="small text-muted">${this.esc(r.owner || '')}</td>
            </tr>`).join('');
        } catch (err) {
            body.innerHTML = `<tr><td colspan="6" class="text-center text-muted py-4">${this.esc(err.message)}</td></tr>`;
        }
    },
};

window.SimulationPage = SimulationPage;
document.addEventListener('DOMContentLoaded', () => SimulationPage.init());
