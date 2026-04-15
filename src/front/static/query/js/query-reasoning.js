/**
 * Inference Module — OWL 2 RL, SWRL, and graph reasoning UI
 */
const ReasoningModule = {
    currentTaskId: null,
    pollTimer: null,

    PAGE_SIZE: 15,
    _inferredData: [],
    _inferredPage: 0,

    async init() {
        document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
            new bootstrap.Tooltip(el, { html: true });
        });
        this.checkAndResumeTask();
    },

    toggleAllOptions(checked) {
        const ids = [
            'reasoningTbox', 'reasoningSwrl', 'reasoningGraph',
            'reasoningDecisionTables', 'reasoningSparqlRules',
            'reasoningAggregateRules',
        ];
        ids.forEach(id => {
            const el = document.getElementById(id);
            if (el) {
                el.checked = checked;
                this._syncTile(el);
            }
        });
    },

    _syncTile(checkbox) {
        const tile = checkbox.closest('.reasoning-tile');
        if (tile) tile.classList.toggle('active', checkbox.checked);
    },

    _syncDeltaInput() {
        const deltaChecked = document.getElementById('materializeDelta')?.checked;
        const grp = document.getElementById('materializeDeltaGroup');
        if (grp) grp.style.display = deltaChecked ? '' : 'none';
    },

    setRunning(running) {
        const btn = document.getElementById('runReasoningBtn');
        const checkboxes = document.querySelectorAll(
            '#reasoningTbox, #reasoningSwrl, #reasoningGraph, ' +
            '#reasoningDecisionTables, #reasoningSparqlRules, #reasoningAggregateRules'
        );
        if (running) {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span> Running inference…';
        } else {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-play-fill me-1"></i> Run Inference';
        }
        checkboxes.forEach(el => {
            el.disabled = running;
            const item = el.closest('.list-group-item');
            if (item) {
                item.classList.toggle('disabled', running);
                item.style.opacity = running ? '0.6' : '';
            }
        });
    },

    async runReasoning() {
        const tbox = document.getElementById('reasoningTbox').checked;
        const swrl = document.getElementById('reasoningSwrl').checked;
        const graph = document.getElementById('reasoningGraph').checked;
        const decision_tables = document.getElementById('reasoningDecisionTables')?.checked || false;
        const sparql_rules = document.getElementById('reasoningSparqlRules')?.checked || false;
        const aggregate_rules = document.getElementById('reasoningAggregateRules')?.checked || false;

        if (!tbox && !swrl && !graph && !decision_tables && !sparql_rules && !aggregate_rules) {
            if (typeof showNotification === 'function')
                showNotification('Select at least one inference phase.', 'warning');
            return;
        }

        this.setRunning(true);
        this.showProgress('Starting inference...');

        try {
            const response = await fetch('/dtwin/reasoning/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    tbox, swrl, graph,
                    decision_tables, sparql_rules, aggregate_rules
                })
            });
            const data = await response.json();
            if (data.success && data.task_id) {
                this.currentTaskId = data.task_id;
                sessionStorage.setItem('ontobricks_reasoning_task', data.task_id);
                this.pollTask(data.task_id);
            } else {
                this.hideProgress();
                this.setRunning(false);
                if (typeof showNotification === 'function')
                    showNotification(data.message || 'Failed to start inference.', 'error');
            }
        } catch (error) {
            this.hideProgress();
            this.setRunning(false);
            console.error('Reasoning start error:', error);
        }
    },

    pollTask(taskId) {
        if (this.pollTimer) clearInterval(this.pollTimer);
        this.pollTimer = setInterval(async () => {
            try {
                const resp = await fetch(`/tasks/${taskId}`);
                const data = await resp.json();

                if (!data.success || !data.task) {
                    clearInterval(this.pollTimer);
                    this.pollTimer = null;
                    sessionStorage.removeItem('ontobricks_reasoning_task');
                    this.hideProgress();
                    this.setRunning(false);
                    return;
                }

                const task = data.task;

                if (task.status === 'completed') {
                    clearInterval(this.pollTimer);
                    this.pollTimer = null;
                    sessionStorage.removeItem('ontobricks_reasoning_task');
                    this.hideProgress();
                    this.setRunning(false);
                    this.onTaskCompleted(task.result);
                    if (typeof showNotification === 'function')
                        showNotification('Inference completed!', 'success');
                } else if (task.status === 'failed') {
                    clearInterval(this.pollTimer);
                    this.pollTimer = null;
                    sessionStorage.removeItem('ontobricks_reasoning_task');
                    this.hideProgress();
                    this.setRunning(false);
                    if (typeof showNotification === 'function')
                        showNotification(task.message || 'Inference failed.', 'error');
                } else {
                    const pct = task.progress || 0;
                    this.updateProgress(pct, task.current_step_description || task.message || 'Running...');
                }
            } catch (e) {
                console.error('Reasoning poll error:', e);
            }
        }, 2000);
    },

    checkAndResumeTask() {
        const saved = sessionStorage.getItem('ontobricks_reasoning_task');
        if (saved) {
            this.currentTaskId = saved;
            this.setRunning(true);
            this.showProgress('Resuming inference task...');
            this.pollTask(saved);
        }
    },

    onTaskCompleted(result) {
        if (!result) return;

        const inferred = result.inferred_triples || [];
        const stats = result.stats || {};

        const statParts = [];
        if (stats.total_duration_seconds) statParts.push(`Duration: ${stats.total_duration_seconds}s`);
        if (stats.total_inferred) statParts.push(`Inferred: ${stats.total_inferred}`);
        document.getElementById('reasoningStats').textContent = statParts.join(' | ');

        this._inferredData = inferred;
        this._inferredPage = 0;

        this._renderExecutionReport(stats, result);

        document.getElementById('reasoningInitMessage').style.display = 'none';
        document.getElementById('reasoningReportContent').style.display = 'block';
        this._updateTabBadges(inferred.length);

        this._showMaterializePanel(result);

        const reportTab = document.getElementById('tab-report');
        if (reportTab) bootstrap.Tab.getOrCreateInstance(reportTab).show();

        this._refreshInferredPane();
    },

    _localName(uri) {
        if (!uri) return '';
        const idx = Math.max(uri.lastIndexOf('#'), uri.lastIndexOf('/'));
        return idx >= 0 ? uri.substring(idx + 1) : uri;
    },

    _esc(s) {
        if (!s) return '';
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    },

    searchInKnowledgeGraph(subjectUri) {
        const term = this._localName(subjectUri);
        if (!term) return;
        const link = document.querySelector('[data-section="sigmagraph"]');
        if (!link) return;
        link.click();
        setTimeout(() => {
            const valInput = document.getElementById('sgFilterValue');
            const depthSlider = document.getElementById('sgFilterDepth');
            const depthLabel = document.getElementById('sgFilterDepthValue');
            const matchSel = document.getElementById('sgFilterMatchType');
            if (valInput) valInput.value = term;
            if (depthSlider) depthSlider.value = '3';
            if (depthLabel) depthLabel.textContent = '3';
            if (matchSel) matchSel.value = 'contains';
            if (typeof SigmaGraph !== 'undefined') {
                SigmaGraph.setHighlightTerm(term);
                SigmaGraph.executeGraphFilter();
            }
        }, 400);
    },

    _updateTabBadges(inferredCount) {
        const bInf = document.getElementById('tabBadgeInferred');
        if (bInf) bInf.textContent = inferredCount > 0 ? inferredCount : '';
    },

    // ── Inferred Triples pagination ──────────────────────────────

    _refreshInferredPane() {
        const data = this._inferredData;
        const tableWrap = document.getElementById('inferredTriplesTable');
        const emptyMsg = document.getElementById('inferredTriplesEmpty');
        if (!tableWrap || !emptyMsg) return;

        if (!data || data.length === 0) {
            tableWrap.style.display = 'none';
            emptyMsg.style.display = '';
        } else {
            emptyMsg.style.display = 'none';
            tableWrap.style.display = '';
            this._renderInferredPage();
        }
    },

    _renderInferredPage() {
        const data = this._inferredData;
        if (!data || data.length === 0) return;
        const page = this._inferredPage;
        const ps = this.PAGE_SIZE;
        const total = data.length;
        const totalPages = Math.ceil(total / ps);
        const start = page * ps;
        const slice = data.slice(start, start + ps);

        const tbody = document.getElementById('inferredTriplesBody');
        if (!tbody) return;
        tbody.innerHTML = '';
        for (const t of slice) {
            const tr = document.createElement('tr');
            tr.innerHTML =
                `<td class="small text-truncate" style="max-width:180px" title="${this._esc(t.subject)}">${this._esc(this._localName(t.subject))}</td>` +
                `<td class="small text-truncate" style="max-width:180px" title="${this._esc(t.predicate)}">${this._esc(this._localName(t.predicate))}</td>` +
                `<td class="small text-truncate" style="max-width:180px" title="${this._esc(t.object)}">${this._esc(this._localName(t.object))}</td>` +
                `<td class="small"><span class="badge bg-secondary">${this._esc(t.provenance)}</span></td>`;
            tbody.appendChild(tr);
        }

        this._updatePagination('inferred', page, totalPages, start, ps, total);
    },

    pageInferred(delta) {
        const totalPages = Math.ceil((this._inferredData || []).length / this.PAGE_SIZE);
        const next = this._inferredPage + delta;
        if (next < 0 || next >= totalPages) return;
        this._inferredPage = next;
        this._renderInferredPage();
    },

    // ── Shared pagination helper ─────────────────────────────────

    _updatePagination(prefix, page, totalPages, start, ps, total) {
        const nav = document.getElementById(`${prefix}PaginationNav`);
        const info = document.getElementById(`${prefix}PageInfo`);
        const prevLi = document.getElementById(`${prefix}PrevLi`);
        const nextLi = document.getElementById(`${prefix}NextLi`);
        if (!nav) return;

        if (totalPages <= 1) {
            nav.setAttribute('style', 'display:none!important');
            return;
        }
        nav.removeAttribute('style');
        if (info) info.textContent = `${start + 1}–${Math.min(start + ps, total)} of ${total}`;
        if (prevLi) prevLi.classList.toggle('disabled', page === 0);
        if (nextLi) nextLi.classList.toggle('disabled', page >= totalPages - 1);
    },

    // ── Execution Report ─────────────────────────────────────────

    _renderExecutionReport(stats, result) {
        const phases = [
            { key: 'tbox',            label: 'T-Box (OWL 2 RL)',      icon: 'bi-diagram-3' },
            { key: 'swrl',            label: 'SWRL Inference',          icon: 'bi-code-square' },
            { key: 'graph',           label: 'Graph Inference',         icon: 'bi-share' },
            { key: 'decision_tables', label: 'Decision Tables',        icon: 'bi-table' },
            { key: 'sparql_rules',    label: 'SPARQL CONSTRUCT Rules', icon: 'bi-braces' },
            { key: 'aggregate_rules', label: 'Aggregate Rules',        icon: 'bi-calculator' },
        ];

        const tbody = document.getElementById('reasoningReportBody');
        if (!tbody) return;
        tbody.innerHTML = '';
        let totalInferred = 0;
        let anyError = false;

        for (const phase of phases) {
            const k = phase.key;
            const skipped  = stats[`${k}_skipped`] || (typeof stats[k] === 'object' && stats[k]?.skipped);
            const error    = stats[`${k}_error`] || '';
            const duration = stats[`${k}_duration_seconds`] || stats[`${k}_duration`] || '';
            const inf      = stats[`${k}_inferred_count`] || 0;
            const reason   = stats[`${k}_reason`] || stats[`${k}_skip_reason`] || '';

            let statusBadge, details;

            if (skipped) {
                statusBadge = '<span class="badge bg-secondary">Skipped</span>';
                details = reason || 'Not executed';
            } else if (error) {
                statusBadge = '<span class="badge bg-danger">Error</span>';
                details = `<span class="text-danger">${this._esc(error)}</span>`;
                anyError = true;
            } else {
                statusBadge = '<span class="badge bg-success">OK</span>';
                details = inf > 0 ? `${inf} inferred` : 'No new findings';
            }

            totalInferred += inf;

            const durationStr = duration ? `${duration}s` : '—';
            const tr = document.createElement('tr');
            tr.innerHTML = `<td><i class="bi ${phase.icon} me-1 text-muted"></i>${phase.label}</td>`
                + `<td>${statusBadge}</td>`
                + `<td class="text-muted">${durationStr}</td>`
                + `<td>${details}</td>`;
            tbody.appendChild(tr);
        }

        const totalDuration = stats.total_duration_seconds ? `${stats.total_duration_seconds}s` : '—';
        const totalStatus = anyError
            ? '<span class="badge bg-danger">Error</span>'
            : '<span class="badge bg-success">OK</span>';
        document.getElementById('reportTotalStatus').innerHTML = totalStatus;
        document.getElementById('reportTotalDuration').textContent = totalDuration;
        const ruleInferred = stats.total_inferred != null ? stats.total_inferred : totalInferred;
        const schemaInferred = (stats.tbox_inferred_count || 0) + (stats.graph_inferred_count || 0);
        let detailText = `${ruleInferred} inferred`;
        if (schemaInferred > 0)
            detailText += ` (+ ${schemaInferred} schema/structural)`;
        document.getElementById('reportTotalDetails').textContent = detailText;

        const ts = result.last_run || new Date().toISOString();
        const d = new Date(ts);
        document.getElementById('reportTimestamp').textContent =
            `Executed at ${d.toLocaleString()}`;
    },

    // ── Materialise (post-run) ───────────────────────────────────

    _showMaterializePanel(result) {
        const panel = document.getElementById('materializePanel');
        const area = document.getElementById('materializeResultArea');
        if (!panel) return;
        const inferred = result.inferred_triples || [];
        if (inferred.length > 0) {
            panel.style.display = '';
            if (area) { area.style.display = 'none'; area.innerHTML = ''; }
        } else {
            panel.style.display = 'none';
        }
    },

    async runMaterialize() {
        const deltaChecked = document.getElementById('materializeDelta')?.checked || false;
        const graphChecked = document.getElementById('materializeGraph')?.checked || false;
        const tableName = (document.getElementById('materializeTableName')?.value || '').trim();

        if (!deltaChecked && !graphChecked) {
            if (typeof showNotification === 'function')
                showNotification('Select at least one target (Delta table or Graph).', 'warning');
            return;
        }
        if (deltaChecked && tableName.split('.').length !== 3) {
            if (typeof showNotification === 'function')
                showNotification('Delta table must be fully qualified: catalog.schema.table', 'warning');
            return;
        }
        if (!this.currentTaskId) {
            if (typeof showNotification === 'function')
                showNotification('No inference results available. Run inference first.', 'warning');
            return;
        }

        const btn = document.getElementById('runMaterializeBtn');
        const area = document.getElementById('materializeResultArea');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Materialising…';

        try {
            const resp = await fetch('/dtwin/reasoning/materialize', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    task_id: this.currentTaskId,
                    materialize_delta: deltaChecked,
                    materialize_graph: graphChecked,
                    materialize_table: tableName
                })
            });
            const data = await resp.json();
            if (!data.success) {
                if (typeof showNotification === 'function')
                    showNotification(data.message || 'Materialisation failed.', 'error');
                if (area) { area.style.display = 'none'; }
                return;
            }

            const lines = [];
            if (data.materialize_table) {
                if (data.materialize_error) {
                    lines.push(`<span class="badge bg-danger">Error</span> Delta: ${this._esc(data.materialize_error)}`);
                } else {
                    lines.push(`<span class="badge bg-success">OK</span> ${data.materialize_count} triples &rarr; <code>${this._esc(data.materialize_table)}</code>`);
                }
            }
            if (data.materialize_graph_count != null || data.materialize_graph_error) {
                if (data.materialize_graph_error) {
                    lines.push(`<span class="badge bg-danger">Error</span> Graph: ${this._esc(data.materialize_graph_error)}`);
                } else {
                    lines.push(`<span class="badge bg-success">OK</span> ${data.materialize_graph_count} triples appended to graph`);
                }
            }
            if (area && lines.length) {
                area.innerHTML = lines.map(l => `<div class="small">${l}</div>`).join('');
                area.style.display = '';
            }
            if (typeof showNotification === 'function')
                showNotification('Materialisation completed!', 'success');
            btn.disabled = true;
            btn.innerHTML = '<i class="bi bi-check-circle me-1"></i>Materialised';
            return;
        } catch (err) {
            console.error('Materialise error:', err);
            if (typeof showNotification === 'function')
                showNotification('Materialisation request failed.', 'error');
        }
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-database-add me-1"></i>Materialise';
    },

    // ── Progress helpers ─────────────────────────────────────────

    showProgress(message) {
        document.getElementById('reasoningProgressArea').style.display = 'block';
        document.getElementById('reasoningProgressStep').textContent = message;
        document.getElementById('reasoningProgressBar').style.width = '0%';
        document.getElementById('reasoningProgressBar').textContent = '0%';
    },

    updateProgress(pct, message) {
        const bar = document.getElementById('reasoningProgressBar');
        bar.style.width = `${pct}%`;
        bar.textContent = `${pct}%`;
        bar.setAttribute('aria-valuenow', pct);
        if (message) document.getElementById('reasoningProgressStep').textContent = message;
    },

    hideProgress() {
        document.getElementById('reasoningProgressArea').style.display = 'none';
    }
};

document.addEventListener('DOMContentLoaded', () => {
    if (typeof ReasoningModule !== 'undefined') ReasoningModule.init();
});
