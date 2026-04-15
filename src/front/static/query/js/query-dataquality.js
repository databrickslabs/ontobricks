/**
 * OntoBricks — query-dataquality.js
 * SHACL-driven Data Quality execution module for the Digital Twin menu.
 */
window.DQExecModule = {
    results: [],
    _pollTimer: null,

    CATEGORIES: [
        'completeness', 'cardinality', 'uniqueness',
        'consistency', 'conformance', 'structural',
    ],

    init() {
        this._initBackendToggle();
        this._updateTripleStoreLabel();
    },

    _syncTile(checkbox) {
        const tile = checkbox.closest('.reasoning-tile');
        if (tile) tile.classList.toggle('active', checkbox.checked);
    },

    toggleAllDimensions(checked) {
        document.querySelectorAll('#dqDimCompleteness, #dqDimCardinality, #dqDimUniqueness, #dqDimConsistency, #dqDimConformance, #dqDimStructural')
            .forEach(el => { el.checked = checked; this._syncTile(el); });
    },

    _getSelectedDimensions() {
        const dims = [];
        document.querySelectorAll('[data-dimension]').forEach(el => {
            if (el.checked) dims.push(el.dataset.dimension);
        });
        return dims;
    },

    _setDimensionsDisabled(disabled) {
        document.querySelectorAll('[data-dimension]').forEach(el => { el.disabled = disabled; });
    },

    _initBackendToggle() {
        const group = document.getElementById('dqBackendToggle');
        if (!group) return;
        const cfg = window.__TRIPLESTORE_CONFIG || {};
        const viewBtn = group.querySelector('[data-backend="view"]');
        const graphBtn = group.querySelector('[data-backend="graph"]');

        if (!cfg.view_table && viewBtn) {
            viewBtn.classList.add('disabled');
            viewBtn.setAttribute('title', 'No Delta VIEW configured');
        }
        if (!cfg.graph_name && graphBtn) {
            graphBtn.classList.add('disabled');
            graphBtn.setAttribute('title', 'No LadybugDB graph available');
        }

        if (!cfg.graph_name && cfg.view_table) {
            graphBtn.classList.remove('active');
            viewBtn.classList.add('active');
        }

        group.querySelectorAll('button').forEach(btn => {
            btn.addEventListener('click', () => {
                if (btn.classList.contains('disabled')) return;
                group.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._updateTripleStoreLabel();
            });
        });
    },

    _getSelectedBackend() {
        const active = document.querySelector('#dqBackendToggle button.active');
        return active ? active.dataset.backend : 'view';
    },

    _updateTripleStoreLabel() {
        const el = document.getElementById('dqExecTripleStoreTable');
        if (!el) return;
        const cfg = window.__TRIPLESTORE_CONFIG || {};
        const backend = this._getSelectedBackend();
        if (backend === 'view') {
            el.textContent = cfg.view_table || 'Not configured';
        } else {
            el.textContent = cfg.graph_name || 'Not configured';
        }
    },

    _getTripleStoreTable() {
        const cfg = window.__TRIPLESTORE_CONFIG || {};
        const backend = this._getSelectedBackend();
        return backend === 'view' ? (cfg.view_table || '') : (cfg.graph_name || '');
    },

    async runAllChecks() {
        const backend = this._getSelectedBackend();
        const table = this._getTripleStoreTable();
        if (!table) {
            showNotification(
                backend === 'view'
                    ? 'Delta VIEW is not configured. Set it up in Domain Settings and build first.'
                    : 'LadybugDB graph is not available. Build the Digital Twin first.',
                'warning'
            );
            return;
        }

        const dimensions = this._getSelectedDimensions();
        if (!dimensions.length) {
            showNotification('Select at least one data quality dimension to run.', 'warning');
            return;
        }

        this._setDimensionsDisabled(true);

        document.getElementById('dqExecInitMessage').style.display = 'none';
        document.getElementById('dqExecResults').style.display = 'none';
        document.getElementById('dqExecProgressArea').style.display = '';
        document.getElementById('dqExecProgressBar').style.width = '0%';
        document.getElementById('dqExecProgressBar').textContent = '0%';
        document.getElementById('dqExecProgressStep').textContent = 'Starting data quality checks...';

        try {
            const resp = await fetch('/dtwin/dataquality/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({
                    triplestore_table: table,
                    backend: backend,
                    dimensions: dimensions,
                    violation_limit: parseInt(document.getElementById('dqViolationLimit')?.value || '10'),
                }),
            });
            const data = await resp.json();
            if (data.success && data.task_id) {
                this._pollTask(data.task_id);
            } else {
                this._showError(data.message || 'Failed to start checks');
            }
        } catch (e) {
            console.error('[DQExec] Error:', e);
            this._showError('Error starting data quality checks');
        }
    },

    _pollTask(taskId) {
        if (this._pollTimer) clearInterval(this._pollTimer);
        this._pollTimer = setInterval(async () => {
            try {
                const resp = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
                const data = await resp.json();
                const task = data.task;
                if (!task) return;

                if (task.status === 'running' || task.status === 'pending') {
                    const pct = task.progress || 0;
                    const bar = document.getElementById('dqExecProgressBar');
                    bar.style.width = pct + '%';
                    bar.textContent = pct + '%';
                    document.getElementById('dqExecProgressStep').textContent = task.message || 'Running...';
                } else if (task.status === 'completed') {
                    clearInterval(this._pollTimer);
                    this._pollTimer = null;
                    this._showResults(task.result || {});
                } else if (task.status === 'failed') {
                    clearInterval(this._pollTimer);
                    this._pollTimer = null;
                    this._showError(task.message || 'Checks failed');
                }
            } catch (e) {
                console.error('[DQExec] Poll error:', e);
            }
        }, 1500);
    },

    _showResults(result) {
        document.getElementById('dqExecProgressArea').style.display = 'none';
        this._setDimensionsDisabled(false);

        const reportTab = document.getElementById('dq-tab-report');
        if (reportTab) bootstrap.Tab.getOrCreateInstance(reportTab).show();

        this.results = result.results || [];
        let firstWithResults = null;

        this.CATEGORIES.forEach(cat => {
            const container = document.querySelector(`.dq-exec-result-list[data-category="${cat}"]`);
            if (!container) return;
            const catResults = this.results.filter(r => r.category === cat);
            const scoreEl = document.querySelector(`.dq-cat-score[data-cat-score="${cat}"]`);
            const countEl = document.querySelector(`.dq-cat-count[data-cat-count="${cat}"]`);
            const accordionItem = document.getElementById(`dqExecResults${cat.charAt(0).toUpperCase() + cat.slice(1)}`);
            const collapseEl = document.getElementById(`dqCollapse${cat.charAt(0).toUpperCase() + cat.slice(1)}`);

            if (countEl) countEl.textContent = catResults.length;

            if (!catResults.length) {
                container.innerHTML = '<div class="text-muted small py-1">No rules in this category</div>';
                if (scoreEl) scoreEl.innerHTML = '';
                if (accordionItem) accordionItem.classList.add('opacity-50');
                if (collapseEl) collapseEl.classList.remove('show');
                return;
            }

            if (accordionItem) accordionItem.classList.remove('opacity-50');
            if (!firstWithResults) firstWithResults = collapseEl;

            const rows = catResults.map(r => this._renderResultRow(r)).join('');
            container.innerHTML = `<table class="table table-borderless table-sm align-middle mb-0 dq-results-table"><tbody>${rows}</tbody></table>`;
            if (scoreEl) scoreEl.innerHTML = this._categoryScoreBadge(catResults);
        });

        if (firstWithResults) {
            firstWithResults.classList.add('show');
            const btn = firstWithResults.previousElementSibling?.querySelector('.accordion-button');
            if (btn) btn.classList.remove('collapsed');
        }

        document.getElementById('dqExecResults').style.display = '';
        this._renderGauges();
    },

    _renderResultRow(r) {
        const icon = r.status === 'success' ? 'bi-check-circle-fill text-success'
            : r.status === 'error' ? 'bi-x-circle-fill text-danger'
            : r.status === 'warning' ? 'bi-exclamation-circle-fill text-warning'
            : 'bi-info-circle-fill text-info';
        const violCount = (r.violations && r.violations.length) || 0;
        const shapeId = r.shape_id || '';
        const sqlBtn = r.sql
            ? `<button class="btn btn-outline-secondary btn-sm py-0 px-1" onclick="DQExecModule.showSql('${this._escAttr(shapeId)}')" title="View SQL"><i class="bi bi-code-slash"></i></button>`
            : '';
        const violBtn = violCount > 0
            ? `<button class="btn btn-outline-danger btn-sm py-0 px-1" onclick="DQExecModule.showViolations('${this._escAttr(shapeId)}')" title="View violations"><i class="bi bi-list-ul"></i> ${violCount}</button>`
            : '';

        const hasPop = r.pass_pct != null && r.total_population > 0;
        const pct = hasPop ? r.pass_pct : (violCount === 0 ? 100 : null);
        const pctDisplay = pct != null ? `${pct}%` : '-';
        const pctClass = pct == null ? 'text-muted' : pct === 100 ? 'text-success' : pct >= 80 ? 'text-warning fw-bold' : 'text-danger fw-bold';
        const goodCount = r.total_population > 0 ? (r.total_population - violCount) : 0;
        const pctTitle = (r.total_population > 0) ? `${goodCount} of ${r.total_population} entities pass (${violCount} violation${violCount !== 1 ? 's' : ''})` : '';

        return `<tr class="dq-row-${r.status}" data-shape-id="${shapeId}">
            <td class="dq-col-pct ${pctClass}" title="${pctTitle}">${pctDisplay}</td>
            <td class="dq-col-status"><i class="bi ${icon}"></i></td>
            <td class="dq-col-name small fw-medium">${this._escHtml(r.name || 'Check')}</td>
            <td class="dq-col-msg text-muted small">${this._escHtml(r.message || '')}</td>
            <td class="dq-col-actions text-end text-nowrap">${sqlBtn} ${violBtn}</td>
        </tr>`;
    },

    showSql(shapeId) {
        const r = this.results.find(r => r.shape_id === shapeId);
        if (!r || !r.sql) return;
        document.getElementById('dqExecQueryCode').textContent = r.sql;
        document.getElementById('dqExecQueryViewer').style.display = '';
    },

    hideQueryViewer() {
        document.getElementById('dqExecQueryViewer').style.display = 'none';
    },

    showViolations(shapeId) {
        const r = this.results.find(r => r.shape_id === shapeId);
        if (!r || !r.violations || !r.violations.length) return;

        const header = document.getElementById('dqExecViolationsHeader');
        const body = document.getElementById('dqExecViolationsBody');
        document.getElementById('dqExecViolationsTitle').innerHTML =
            `<i class="bi bi-exclamation-triangle me-2"></i>${this._escHtml(r.name || 'Violations')}`;
        let countText = `${r.violations.length} violation${r.violations.length !== 1 ? 's' : ''}`;
        if (r.violation_total != null && r.violation_total > r.violations.length) {
            countText += ` shown (${r.violation_total} total)`;
        } else {
            countText += ' found';
        }
        if (r.pass_pct != null && r.total_population > 0) {
            countText += ` — ${r.pass_pct}% pass on ${r.total_population} entities`;
        }
        document.getElementById('dqExecViolationsCount').textContent = countText;

        const sample = r.violations[0] || {};
        const cols = Array.isArray(sample) ? sample.map((_, i) => `col_${i}`) : Object.keys(sample);
        const subjectCol = ['s', 'focus_node', 'subject'].find(k => cols.includes(k)) || cols[0];

        header.innerHTML = cols.map(c => `<th>${this._escHtml(c)}</th>`).join('')
            + '<th class="text-center" style="width:40px"></th>';

        body.innerHTML = '';
        r.violations.forEach(row => {
            const vals = Array.isArray(row) ? row : cols.map(c => row[c] ?? '');
            const subjectVal = Array.isArray(row) ? vals[0] : (row[subjectCol] || '');
            const tr = document.createElement('tr');
            tr.innerHTML = vals.map(v => `<td class="small">${this._escHtml(String(v))}</td>`).join('')
                + '<td class="small text-center">'
                + '<a href="#" class="text-primary dq-kg-link" title="Search in Knowledge Graph">'
                + '<i class="bi bi-diagram-3"></i></a></td>';
            tr.querySelector('.dq-kg-link').addEventListener('click', (e) => {
                e.preventDefault();
                bootstrap.Modal.getInstance(document.getElementById('dqExecViolationsModal'))?.hide();
                this.searchInKnowledgeGraph(String(subjectVal));
            });
            body.appendChild(tr);
        });

        new bootstrap.Modal(document.getElementById('dqExecViolationsModal')).show();
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
            if (valInput) {
                valInput.value = term;
                valInput.dispatchEvent(new Event('input', { bubbles: true }));
            }
            if (depthSlider) depthSlider.value = '3';
            if (typeof SigmaGraph !== 'undefined' && SigmaGraph.executeGraphFilter) {
                SigmaGraph.executeGraphFilter();
            }
        }, 400);
    },

    _localName(uri) {
        if (!uri) return '';
        const i = Math.max(uri.lastIndexOf('#'), uri.lastIndexOf('/'));
        return i >= 0 ? uri.substring(i + 1) : uri;
    },

    _gaugeCharts: {},

    _categoryScore(results) {
        const scored = (results || []).filter(r => r.status === 'success' || r.status === 'error');
        if (!scored.length) return null;
        const scores = scored.map(r => {
            if (r.pass_pct != null && r.total_population > 0) return r.pass_pct;
            const violCount = (r.violations && r.violations.length) || 0;
            return violCount === 0 ? 100 : 0;
        });
        return Math.round(scores.reduce((a, b) => a + b, 0) / scores.length * 10) / 10;
    },

    _renderGauges() {
        const gaugeMap = {
            completeness: 'gaugeCompleteness',
            cardinality:  'gaugeCardinality',
            uniqueness:   'gaugeUniqueness',
            consistency:  'gaugeConsistency',
            conformance:  'gaugeConformance',
            structural:   'gaugeStructural',
        };

        let hasAny = false;
        for (const [cat, canvasId] of Object.entries(gaugeMap)) {
            const catResults = this.results.filter(r => r.category === cat);
            const score = this._categoryScore(catResults);
            if (score != null) hasAny = true;
            this._drawGauge(canvasId, score);
        }
        document.getElementById('dqGaugesArea').style.display = hasAny ? '' : 'none';
    },

    _drawGauge(canvasId, score) {
        if (this._gaugeCharts[canvasId]) {
            this._gaugeCharts[canvasId].destroy();
            delete this._gaugeCharts[canvasId];
        }
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        const ctx = canvas.getContext('2d');

        if (score == null) {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            canvas.parentElement.style.opacity = '0.3';
            return;
        }
        canvas.parentElement.style.opacity = '1';

        const val = Math.max(0, Math.min(100, score));
        const color = val === 100 ? '#198754' : val >= 80 ? '#ffc107' : '#dc3545';
        const remaining = 100 - val;

        this._gaugeCharts[canvasId] = new Chart(ctx, {
            type: 'doughnut',
            data: {
                datasets: [{
                    data: [val, remaining],
                    backgroundColor: [color, '#e9ecef'],
                    borderWidth: 0,
                    circumference: 180,
                    rotation: 270,
                }]
            },
            options: {
                responsive: false,
                cutout: '70%',
                plugins: {
                    legend: { display: false },
                    tooltip: { enabled: false },
                },
                layout: { padding: 0 },
            },
            plugins: [{
                id: 'gaugeLabel',
                afterDraw(chart) {
                    const { ctx: c, width, height } = chart;
                    const cx = width / 2;
                    const cy = height - 6;
                    c.save();
                    c.textAlign = 'center';
                    c.textBaseline = 'bottom';
                    c.font = 'bold 15px system-ui, sans-serif';
                    c.fillStyle = color;
                    c.fillText(`${val}%`, cx, cy);
                    c.restore();
                }
            }],
        });
    },

    _categoryScoreBadge(results) {
        const avg = this._categoryScore(results);
        if (avg == null) return '';
        const cls = avg === 100 ? 'bg-success' : avg >= 80 ? 'bg-warning text-dark' : 'bg-danger';
        return `<span class="badge ${cls} ms-2">${avg}%</span>`;
    },

    _showError(msg) {
        document.getElementById('dqExecProgressArea').style.display = 'none';
        this._setDimensionsDisabled(false);
        const reportTab = document.getElementById('dq-tab-report');
        if (reportTab) bootstrap.Tab.getOrCreateInstance(reportTab).show();
        document.getElementById('dqExecInitMessage').style.display = '';
        document.getElementById('dqExecInitMessage').innerHTML =
            `<i class="bi bi-exclamation-triangle text-danger fs-1 d-block mb-2"></i><p class="text-danger">${this._escHtml(msg)}</p>`;
    },

    _escHtml(s) {
        const d = document.createElement('div');
        d.textContent = s || '';
        return d.innerHTML;
    },

    _escAttr(s) {
        return (s || '').replace(/'/g, "\\'").replace(/"/g, '&quot;');
    },
};
