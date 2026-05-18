/**
 * Inference Module — OWL 2 RL, SWRL, and graph reasoning UI
 */
const ReasoningModule = {
    currentTaskId: null,
    pollTimer: null,

    PAGE_SIZE: 15,
    _inferredData: [],
    _inferredPage: 0,
    _rulesCache: {},
    _rulesLoaded: false,

    // rule type → { tileCheckboxId, listAttr }
    _RULE_TILE_MAP: {
        swrl:             { checkboxId: 'reasoningSwrl',           listAttr: 'swrl' },
        decision_tables:  { checkboxId: 'reasoningDecisionTables', listAttr: 'decision_tables' },
        sparql_rules:     { checkboxId: 'reasoningSparqlRules',    listAttr: 'sparql_rules' },
        aggregate_rules:  { checkboxId: 'reasoningAggregateRules', listAttr: 'aggregate_rules' },
    },

    async init() {
        document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
            new bootstrap.Tooltip(el, { html: true });
        });
        this.checkAndResumeTask();
        this._loadRules();
    },

    // ── Per-rule loading ─────────────────────────────────────────

    async _loadRules() {
        if (this._rulesLoaded) return;
        this._rulesLoaded = true;
        try {
            const [swrlResp, dtResp, sparqlResp, aggResp] = await Promise.all([
                fetch('/ontology/swrl/list', { credentials: 'same-origin' }),
                fetch('/ontology/rules/decision_tables/list', { credentials: 'same-origin' }),
                fetch('/ontology/rules/sparql_rules/list', { credentials: 'same-origin' }),
                fetch('/ontology/rules/aggregate_rules/list', { credentials: 'same-origin' }),
            ]);
            const [swrlData, dtData, sparqlData, aggData] = await Promise.all([
                swrlResp.json(), dtResp.json(), sparqlResp.json(), aggResp.json(),
            ]);
            this._rulesCache = {
                swrl:            swrlData.rules || [],
                decision_tables: dtData.rules || [],
                sparql_rules:    sparqlData.rules || [],
                aggregate_rules: aggData.rules || [],
            };
            this._renderRuleCheckboxes();
        } catch (e) {
            console.error('[Reasoning] Failed to load rules:', e);
        }
    },

    _renderRuleCheckboxes() {
        for (const [type, cfg] of Object.entries(this._RULE_TILE_MAP)) {
            const rules = this._rulesCache[type] || [];
            const container = document.querySelector(`[data-r-rulelist="${cfg.listAttr}"]`);
            const toggleBtn = document.querySelector(`[data-r-expand="${cfg.listAttr}"]`);
            if (!container || !rules.length) continue;

            if (toggleBtn) {
                const countSpan = toggleBtn.querySelector('.dq-rules-count');
                if (countSpan) countSpan.textContent = `${rules.length} rule${rules.length !== 1 ? 's' : ''}`;
                toggleBtn.classList.remove('d-none');
            }

            container.innerHTML = rules.map((r, i) => `
                <div class="dq-rule-item">
                    <input type="checkbox" class="form-check-input r-rule-cb"
                           id="rRule_${type}_${i}"
                           data-r-rule-name="${this._escAttr(r.name || '')}"
                           data-r-rule-type="${type}"
                           checked>
                    <label class="form-check-label small" for="rRule_${type}_${i}"
                           title="${this._escAttr(r.name || '')}">
                        ${this._esc(r.name || `Rule ${i + 1}`)}
                    </label>
                </div>
            `).join('');

            container.querySelectorAll('.r-rule-cb').forEach(cb => {
                cb.addEventListener('change', () => this._onRuleCheckChange(type));
            });
        }
    },

    _toggleRuleList(type) {
        const cfg = this._RULE_TILE_MAP[type];
        if (!cfg) return;
        const container = document.querySelector(`[data-r-rulelist="${cfg.listAttr}"]`);
        const btn = document.querySelector(`[data-r-expand="${cfg.listAttr}"]`);
        if (!container) return;
        const isHidden = container.classList.contains('d-none');
        container.classList.toggle('d-none', !isHidden);
        if (btn) {
            const icon = btn.querySelector('.dq-expand-chevron');
            if (icon) icon.classList.toggle('dq-chevron-open', isHidden);
        }
    },

    _onRuleCheckChange(type) {
        const cfg = this._RULE_TILE_MAP[type];
        if (!cfg) return;
        const tileCb = document.getElementById(cfg.checkboxId);
        const container = document.querySelector(`[data-r-rulelist="${cfg.listAttr}"]`);
        if (!tileCb || !container) return;
        const ruleCbs = [...container.querySelectorAll('.r-rule-cb')];
        if (!ruleCbs.length) return;
        const checkedCount = ruleCbs.filter(cb => cb.checked).length;
        if (checkedCount === ruleCbs.length) {
            tileCb.checked = true;
            tileCb.indeterminate = false;
        } else if (checkedCount === 0) {
            tileCb.checked = false;
            tileCb.indeterminate = false;
        } else {
            tileCb.checked = false;
            tileCb.indeterminate = true;
        }
        const tile = tileCb.closest('.reasoning-tile');
        if (tile) tile.classList.toggle('active', tileCb.checked || tileCb.indeterminate);
    },

    _getSelectedRuleNames(type) {
        const cfg = this._RULE_TILE_MAP[type];
        if (!cfg) return [];
        const names = [];
        document.querySelectorAll(`.r-rule-cb[data-r-rule-type="${type}"]:checked`).forEach(cb => {
            if (cb.dataset.rRuleName) names.push(cb.dataset.rRuleName);
        });
        return names;
    },

    _escAttr(s) {
        return (s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
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
                el.indeterminate = false;
                this._syncTile(el);
            }
        });
    },

    _syncTile(checkbox) {
        const tile = checkbox.closest('.reasoning-tile');
        if (tile) tile.classList.toggle('active', checkbox.checked);
        checkbox.indeterminate = false;
        // Cascade to child rule checkboxes for enumerable tiles
        const typeMap = {
            reasoningSwrl:           'swrl',
            reasoningDecisionTables: 'decision_tables',
            reasoningSparqlRules:    'sparql_rules',
            reasoningAggregateRules: 'aggregate_rules',
        };
        const type = typeMap[checkbox.id];
        if (!type) return;
        const cfg = this._RULE_TILE_MAP[type];
        if (!cfg) return;
        const container = document.querySelector(`[data-r-rulelist="${cfg.listAttr}"]`);
        if (container) {
            container.querySelectorAll('.r-rule-cb').forEach(cb => {
                cb.checked = checkbox.checked;
            });
        }
    },

    _syncDeltaInput() {
        const deltaChecked = document.getElementById('materializeDelta')?.checked;
        const grp = document.getElementById('materializeDeltaGroup');
        if (grp) grp.classList.toggle('d-none', !deltaChecked);
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
        document.querySelectorAll('.r-rule-cb').forEach(el => { el.disabled = running; });
        document.querySelectorAll('[data-r-expand]').forEach(el => { el.disabled = running; });
    },

    async runReasoning() {
        const tboxEl   = document.getElementById('reasoningTbox');
        const swrlEl   = document.getElementById('reasoningSwrl');
        const graphEl  = document.getElementById('reasoningGraph');
        const dtEl     = document.getElementById('reasoningDecisionTables');
        const sparqlEl = document.getElementById('reasoningSparqlRules');
        const aggEl    = document.getElementById('reasoningAggregateRules');

        const tbox           = tboxEl?.checked || false;
        const swrl           = swrlEl?.checked || swrlEl?.indeterminate || false;
        const graph          = graphEl?.checked || false;
        const decision_tables = dtEl?.checked || dtEl?.indeterminate || false;
        const sparql_rules   = sparqlEl?.checked || sparqlEl?.indeterminate || false;
        const aggregate_rules = aggEl?.checked || aggEl?.indeterminate || false;

        if (!tbox && !swrl && !graph && !decision_tables && !sparql_rules && !aggregate_rules) {
            if (typeof showNotification === 'function')
                showNotification('Select at least one inference phase.', 'warning');
            return;
        }

        const payload = { tbox, swrl, graph, decision_tables, sparql_rules, aggregate_rules };

        // Attach per-rule name filters for enumerable phases (only when rules are loaded)
        if (this._rulesLoaded) {
            const swrlNames = this._getSelectedRuleNames('swrl');
            const dtNames   = this._getSelectedRuleNames('decision_tables');
            const sparqlNames = this._getSelectedRuleNames('sparql_rules');
            const aggNames  = this._getSelectedRuleNames('aggregate_rules');
            if (swrlNames.length)   payload.swrl_rule_names        = swrlNames;
            if (dtNames.length)     payload.decision_table_names   = dtNames;
            if (sparqlNames.length) payload.sparql_rule_names      = sparqlNames;
            if (aggNames.length)    payload.aggregate_rule_names   = aggNames;
        }

        this.setRunning(true);
        this.showProgress('Starting inference...');

        try {
            const response = await fetch('/dtwin/reasoning/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
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

        document.getElementById('reasoningInitMessage').classList.add('d-none');
        document.getElementById('reasoningReportContent').classList.remove('d-none');
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
            tableWrap.classList.add('d-none');
            emptyMsg.classList.remove('d-none');
        } else {
            emptyMsg.classList.add('d-none');
            tableWrap.classList.remove('d-none');
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
            nav.classList.add('d-none');
            return;
        }
        nav.classList.remove('d-none');
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
            panel.classList.remove('d-none');
            if (area) { area.classList.add('d-none'); area.innerHTML = ''; }
        } else {
            panel.classList.add('d-none');
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
                if (area) { area.classList.add('d-none'); }
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
                } else if (data.materialize_graph_count === 0) {
                    lines.push(`<span class="badge bg-warning text-dark">Warning</span> 0 triples appended to graph — inferred triples may use non-HTTP(S) URIs (e.g. <code>urn:</code>) which are excluded from the graph write.`);
                } else {
                    lines.push(`<span class="badge bg-success">OK</span> ${data.materialize_graph_count} triples appended to graph`);
                }
            }
            if (area && lines.length) {
                area.innerHTML = lines.map(l => `<div class="small">${l}</div>`).join('');
                area.classList.remove('d-none');
            }
            if (typeof showNotification === 'function')
                showNotification('Materialisation completed!', 'success');
            btn.disabled = true;
            btn.innerHTML = '<i class="bi bi-check-circle me-1"></i>Materialised';

            if (graphChecked && data.materialize_graph_count > 0) {
                if (typeof SigmaGraph !== 'undefined' && typeof SigmaGraph.refreshCurrentExpansion === 'function') {
                    const refreshed = await SigmaGraph.refreshCurrentExpansion();
                    if (!refreshed) {
                        // No active filter expansion — fall back to silently reloading the full
                        // triple store so the graph chart reflects the newly materialised triples.
                        if (typeof loadTripleStore === 'function') {
                            await loadTripleStore({ silent: true, navigate: false });
                        } else if (area) {
                            const hint = document.createElement('div');
                            hint.className = 'small text-muted mt-1';
                            hint.innerHTML = '<i class="bi bi-info-circle me-1"></i>Open the <strong>Knowledge Graph</strong> tab and reload to see the new triples.';
                            area.appendChild(hint);
                        }
                    }
                }
            }
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
        document.getElementById('reasoningProgressArea').classList.remove('d-none');
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
        document.getElementById('reasoningProgressArea').classList.add('d-none');
    }
};

document.addEventListener('DOMContentLoaded', () => {
    if (typeof ReasoningModule !== 'undefined') ReasoningModule.init();
});
