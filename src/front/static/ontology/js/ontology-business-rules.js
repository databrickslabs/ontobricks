/**
 * OntoBricks — ontology-business-rules.js
 * Tabbed Business Rules manager: coordinates SWRL (delegated), Decision Tables,
 * SPARQL CONSTRUCT, and Aggregate Rules.
 */
window.BusinessRulesModule = {
    _initialized: false,
    _brStaticUiBound: false,
    _dtEditorDelegBound: false,
    _ontologyClasses: [],
    _ontologyProperties: [],

    // Rule data per tab
    dtRules: [],
    sparqlRules: [],
    aggRules: [],

    // Decision-table editor transient state
    _dtColumns: [],
    _dtRows: [],

    // ── Initialization ───────────────────────────────────────

    async init() {
        if (this._initialized) {
            this._refreshAllBadges();
            return;
        }
        this._initialized = true;

        this._bindBrStaticUiOnce();
        this._ensureDtEditorDelegation();

        await this._loadOntologyItems();

        const loaders = [
            this._loadRules('decision_tables'),
            this._loadRules('sparql_rules'),
            this._loadRules('aggregate_rules'),
        ];
        if (typeof SwrlModule !== 'undefined') {
            SwrlModule.loadOntologyItems();
            loaders.push(SwrlModule.loadRules());
        }
        await Promise.all(loaders);
        this._refreshAllBadges();
    },

    _bindBrStaticUiOnce() {
        if (this._brStaticUiBound) return;
        const root = document.getElementById('swrl-section');
        if (!root) return;
        this._brStaticUiBound = true;

        root.addEventListener('click', (e) => {
            const t = e.target.closest('[data-br-action]');
            if (!t || !root.contains(t)) return;
            const act = t.getAttribute('data-br-action');
            if (act === 'add-rule') this.addRuleForActiveTab();
            else if (act === 'dt-add-column') this.dtAddColumn();
            else if (act === 'dt-add-row') this.dtAddRow();
            else if (act === 'dt-save') this.dtSave();
            else if (act === 'sparql-validate') this.sparqlValidate();
            else if (act === 'sparql-save') this.sparqlSave();
            else if (act === 'agg-save') this.aggSave();
        });

        root.addEventListener('change', (e) => {
            const el = e.target;
            if (!root.contains(el)) return;
            if (el.id === 'dtTargetClass') this._dtTargetClassChanged();
            else if (el.id === 'dtOutputAction') this._dtSyncOutputValue();
            else if (el.id === 'aggTargetClass') this._aggTargetClassChanged();
            else if (el.name === 'dtRowLogic') this._dtRowLogicChanged();
        });
    },

    _ensureDtEditorDelegation() {
        if (this._dtEditorDelegBound) return;
        const modal = document.getElementById('dtEditorModal');
        if (!modal) return;
        this._dtEditorDelegBound = true;

        modal.addEventListener('change', (e) => {
            const t = e.target;
            if (!modal.contains(t)) return;
            if (t.matches && t.matches('select[data-dt-hdr-col]')) {
                this._dtColChanged(parseInt(t.getAttribute('data-dt-hdr-col'), 10), t.value);
                return;
            }
            if (t.matches && t.matches('select.dt-op-select')) {
                const ri = parseInt(t.getAttribute('data-dt-r'), 10);
                const ci = parseInt(t.getAttribute('data-dt-c'), 10);
                this._dtCellChanged(ri, ci, 'op', t.value);
                return;
            }
            if (t.matches && t.matches('input[data-dt-cell-val]')) {
                const ri = parseInt(t.getAttribute('data-dt-r'), 10);
                const ci = parseInt(t.getAttribute('data-dt-c'), 10);
                this._dtCellChanged(ri, ci, 'value', t.value);
            }
        });

        modal.addEventListener('click', (e) => {
            const rm = e.target.closest('[data-dt-remove-row]');
            if (rm && modal.contains(rm)) {
                this._dtRemoveRow(parseInt(rm.getAttribute('data-dt-remove-row'), 10));
            }
        });
    },

    // ── Ontology items (classes / properties for dropdowns) ──

    async _loadOntologyItems() {
        try {
            const resp = await fetch('/ontology/get-loaded-ontology');
            const data = await resp.json();
            if (data.success && data.ontology) {
                this._ontologyClasses = data.ontology.classes || [];
                this._ontologyProperties = data.ontology.properties || [];
            }
        } catch (e) {
            console.error('BusinessRulesModule: failed to load ontology items', e);
        }
    },

    _populateClassSelect(selectId, selected) {
        const sel = document.getElementById(selectId);
        if (!sel) return;
        const first = sel.options[0];
        sel.innerHTML = '';
        sel.appendChild(first);
        for (const c of this._ontologyClasses) {
            const opt = document.createElement('option');
            opt.value = c.name || c.label || '';
            opt.textContent = c.name || c.label || '';
            sel.appendChild(opt);
        }
        if (selected) sel.value = selected;
    },

    _localName(uri) {
        if (!uri) return '';
        const i = Math.max(uri.lastIndexOf('#'), uri.lastIndexOf('/'));
        return i >= 0 ? uri.substring(i + 1) : uri;
    },

    _populatePropertySelect(selectId, selected, filterClassName) {
        const sel = document.getElementById(selectId);
        if (!sel) return;
        const first = sel.options[0];
        sel.innerHTML = '';
        sel.appendChild(first);

        const seen = new Set();
        const addProp = (name, type) => {
            const key = (name || '').toLowerCase();
            if (!key || seen.has(key)) return;
            seen.add(key);
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = type ? `${name} (${type})` : name;
            sel.appendChild(opt);
        };

        if (filterClassName) {
            const clsLower = filterClassName.toLowerCase();

            const cls = this._ontologyClasses.find(c =>
                (c.name || '').toLowerCase() === clsLower ||
                this._localName(c.uri || '').toLowerCase() === clsLower
            );
            if (cls && cls.dataProperties) {
                cls.dataProperties.forEach(dp => {
                    addProp(dp.name || dp.localName || this._localName(dp.uri), dp.type || '');
                });
            }

            for (const p of this._ontologyProperties) {
                const dom = (p.domain || '').toLowerCase();
                const domLocal = this._localName(dom).toLowerCase();
                if (dom === clsLower || domLocal === clsLower) {
                    addProp(p.name || p.label || '', p.type || '');
                }
            }
        } else {
            for (const p of this._ontologyProperties) {
                addProp(p.name || p.label || '', p.type || '');
            }
        }

        if (selected) sel.value = selected;
    },

    _getPropertiesForClass(className) {
        if (!className) return this._ontologyProperties;
        const clsLower = className.toLowerCase();
        const seen = new Set();
        const result = [];
        const add = (p) => {
            const key = (p.name || p.label || '').toLowerCase();
            if (!key || seen.has(key)) return;
            seen.add(key);
            result.push(p);
        };

        const cls = this._ontologyClasses.find(c =>
            (c.name || '').toLowerCase() === clsLower ||
            this._localName(c.uri || '').toLowerCase() === clsLower
        );
        if (cls && cls.dataProperties) {
            cls.dataProperties.forEach(dp => {
                add({ name: dp.name || dp.localName || this._localName(dp.uri), type: dp.type || '' });
            });
        }

        for (const p of this._ontologyProperties) {
            const dom = (p.domain || '').toLowerCase();
            const domLocal = this._localName(dom).toLowerCase();
            if (dom === clsLower || domLocal === clsLower) {
                add(p);
            }
        }
        return result;
    },

    // ── Generic CRUD helpers ─────────────────────────────────

    async _loadRules(ruleType) {
        try {
            const resp = await fetch(`/ontology/rules/${ruleType}/list`);
            const data = await resp.json();
            if (!data.success) return;
            switch (ruleType) {
                case 'decision_tables': this.dtRules = data.rules || []; this._renderDtList(); break;
                case 'sparql_rules': this.sparqlRules = data.rules || []; this._renderSparqlList(); break;
                case 'aggregate_rules': this.aggRules = data.rules || []; this._renderAggList(); break;
            }
        } catch (e) {
            console.error(`BusinessRulesModule: failed to load ${ruleType}`, e);
        }
    },

    async _saveRule(ruleType, rule, index) {
        try {
            const resp = await fetch(`/ontology/rules/${ruleType}/save`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rule, index }),
            });
            const data = await resp.json();
            if (data.success) {
                if (typeof showNotification === 'function') showNotification('Rule saved', 'success');
                await this._loadRules(ruleType);
                this._refreshAllBadges();
            } else {
                if (typeof showNotification === 'function') showNotification(data.message || 'Save failed', 'error');
            }
            return data;
        } catch (e) {
            console.error(`BusinessRulesModule: save ${ruleType} failed`, e);
            if (typeof showNotification === 'function') showNotification('Save failed', 'error');
        }
    },

    async _deleteRule(ruleType, index) {
        if (!confirm('Delete this rule?')) return;
        try {
            const resp = await fetch(`/ontology/rules/${ruleType}/delete`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ index }),
            });
            const data = await resp.json();
            if (data.success) {
                if (typeof showNotification === 'function') showNotification('Rule deleted', 'success');
                await this._loadRules(ruleType);
                this._refreshAllBadges();
            }
        } catch (e) {
            console.error(`BusinessRulesModule: delete ${ruleType} failed`, e);
        }
    },

    async _toggleEnabled(ruleType, index) {
        let rules;
        switch (ruleType) {
            case 'decision_tables': rules = this.dtRules; break;
            case 'sparql_rules': rules = this.sparqlRules; break;
            case 'aggregate_rules': rules = this.aggRules; break;
        }
        if (!rules || !rules[index]) return;
        const rule = { ...rules[index], enabled: !rules[index].enabled };
        await this._saveRule(ruleType, rule, index);
    },

    // ── Tab routing ─────────────────────────────────────────

    _activeTab() {
        const active = document.querySelector('#brTabs .nav-link.active');
        if (!active) return 'swrl';
        const target = active.getAttribute('data-bs-target') || '';
        if (target.includes('dt')) return 'dt';
        if (target.includes('sparql')) return 'sparql';
        if (target.includes('agg')) return 'agg';
        return 'swrl';
    },

    addRuleForActiveTab() {
        switch (this._activeTab()) {
            case 'swrl': if (typeof SwrlModule !== 'undefined') SwrlModule.addRule(); break;
            case 'dt': this.dtOpenEditor(-1); break;
            case 'sparql': this.sparqlOpenEditor(-1); break;
            case 'agg': this.aggOpenEditor(-1); break;
        }
    },

    // ── Badge updates ───────────────────────────────────────

    _refreshAllBadges() {
        const swrlCount = (typeof SwrlModule !== 'undefined' && SwrlModule.rules) ? SwrlModule.rules.length : 0;
        this._setBadge('brBadgeSwrl', swrlCount);
        this._setBadge('brBadgeDt', this.dtRules.length);
        this._setBadge('brBadgeSparql', this.sparqlRules.length);
        this._setBadge('brBadgeAgg', this.aggRules.length);
    },

    _setBadge(id, count) {
        const el = document.getElementById(id);
        if (el) el.textContent = count;
    },

    // ── Shared rendering helpers ────────────────────────────

    _esc(s) {
        if (!s) return '';
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    },

    _renderRuleCard(container, { name, meta, enabled, onEdit, onDelete, onToggle }) {
        const card = document.createElement('div');
        card.className = 'br-rule-card d-flex align-items-center gap-2';
        if (enabled === false) card.style.opacity = '0.55';

        const enabledBadge = enabled === false
            ? '<span class="badge bg-secondary me-1 br-disabled-badge">disabled</span>'
            : '';

        card.innerHTML =
            `<div class="flex-grow-1">` +
            `  <div class="br-rule-name">${enabledBadge}${this._esc(name)}</div>` +
            `  <div class="br-rule-meta">${meta}</div>` +
            `</div>` +
            `<div class="btn-group btn-group-sm br-rule-actions">` +
            `  <button class="btn btn-outline-secondary" title="${enabled !== false ? 'Disable' : 'Enable'}">` +
            `    <i class="bi ${enabled !== false ? 'bi-toggle-on text-success' : 'bi-toggle-off text-danger'}"></i>` +
            `  </button>` +
            `  <button class="btn btn-outline-secondary" title="Edit"><i class="bi bi-pencil"></i></button>` +
            `  <button class="btn btn-outline-danger" title="Delete"><i class="bi bi-trash"></i></button>` +
            `</div>`;
        const btns = card.querySelectorAll('button');
        btns[0].addEventListener('click', onToggle);
        btns[1].addEventListener('click', onEdit);
        btns[2].addEventListener('click', onDelete);
        container.appendChild(card);
    },

    // ═══════════════════════════════════════════════════════════
    // DECISION TABLES
    // ═══════════════════════════════════════════════════════════

    _renderDtList() {
        const container = document.getElementById('dtList');
        const empty = document.getElementById('dtEmpty');
        if (!container) return;
        container.querySelectorAll('.br-rule-card').forEach(c => c.remove());
        if (this.dtRules.length === 0) { if (empty) empty.classList.remove('d-none'); return; }
        if (empty) empty.classList.add('d-none');

        this.dtRules.forEach((dt, i) => {
            this._renderRuleCard(container, {
                name: dt.name || `Table ${i + 1}`,
                meta: `${this._esc(dt.target_class || '?')} · ${(dt.input_columns || []).length} col · ${(dt.rows || []).length} rows (${(dt.row_logic || 'or').toUpperCase()}) · ${dt.hit_policy || 'first'}`,
                enabled: dt.enabled,
                onEdit: () => this.dtOpenEditor(i),
                onDelete: () => this._deleteRule('decision_tables', i),
                onToggle: () => this._toggleEnabled('decision_tables', i),
            });
        });
    },

    dtOpenEditor(index) {
        const isNew = index < 0;
        const dt = isNew ? {} : { ...this.dtRules[index] };
        document.getElementById('dtEditIndex').value = index;
        document.getElementById('dtName').value = dt.name || '';
        document.getElementById('dtHitPolicy').value = dt.hit_policy || 'first';
        document.getElementById('dtEditorTitle').innerHTML = `<i class="bi bi-table me-2"></i>${isNew ? 'Add' : 'Edit'} Decision Table`;

        const logic = dt.row_logic || 'or';
        document.getElementById(logic === 'and' ? 'dtRowLogicAnd' : 'dtRowLogicOr').checked = true;
        this._dtUpdateLogicHint();

        this._populateClassSelect('dtTargetClass', dt.target_class || '');
        const targetCls = dt.target_class || '';
        this._populatePropertySelect('dtOutputProperty', (dt.output_column || {}).property || '', targetCls);
        document.getElementById('dtOutputAction').value = (dt.output_column || {}).action || 'set_value';
        document.getElementById('dtOutputValue').value = (dt.output_column || {}).value || '';
        this._dtSyncOutputValue();

        this._dtColumns = (dt.input_columns || []).map(c => ({ ...c }));
        this._dtRows = (dt.rows || []).map(r => ({ conditions: [...(r.conditions || [])], action_value: r.action_value || '' }));
        if (this._dtColumns.length === 0) {
            this._dtColumns.push({ property: '', label: '' });
            this._dtRows.push({ conditions: [{ op: 'eq', value: '' }], action_value: '' });
        }
        this._dtRenderGrid();
        new bootstrap.Modal(document.getElementById('dtEditorModal')).show();
    },

    _dtGetRowLogic() {
        return document.getElementById('dtRowLogicAnd')?.checked ? 'and' : 'or';
    },

    _dtRowLogicChanged() {
        this._dtUpdateLogicHint();
        this._dtRenderGrid();
    },

    _dtUpdateLogicHint() {
        const hint = document.getElementById('dtRowLogicHint');
        if (!hint) return;
        const isAnd = this._dtGetRowLogic() === 'and';
        hint.innerHTML = isAnd
            ? 'Rows match if <strong>all</strong> rows fire'
            : 'Rows match if <strong>any</strong> row fires';
    },

    _dtRenderGrid() {
        const thead = document.getElementById('dtGridHead');
        const tbody = document.getElementById('dtGridBody');
        if (!thead || !tbody) return;

        const targetCls = (document.getElementById('dtTargetClass') || {}).value || '';
        const filteredProps = this._getPropertiesForClass(targetCls);

        let hdr = '<tr><th class="text-center small text-muted dt-grid-num-col">#</th>';
        this._dtColumns.forEach((col, ci) => {
            hdr += `<th class="dt-col-header">` +
                `<select class="form-select form-select-sm" data-dt-hdr-col="${ci}">` +
                `<option value="">Property…</option>`;
            for (const p of filteredProps) {
                const pName = p.name || p.label || '';
                hdr += `<option value="${this._esc(pName)}" ${pName === col.property ? 'selected' : ''}>${this._esc(pName)}</option>`;
            }
            hdr += `</select></th>`;
        });
        hdr += '<th class="dt-grid-action-col"></th></tr>';
        thead.innerHTML = hdr;

        const colCount = this._dtColumns.length + 2;
        const logic = this._dtGetRowLogic();
        const logicLabel = logic.toUpperCase();
        const logicClass = logic === 'and' ? 'bg-success' : 'bg-primary';

        tbody.innerHTML = '';
        this._dtRows.forEach((row, ri) => {
            if (ri > 0) {
                tbody.innerHTML +=
                    `<tr class="dt-logic-row"><td colspan="${colCount}" class="text-center py-0 dt-logic-row-cell">` +
                    `<span class="badge ${logicClass} bg-opacity-75 dt-logic-badge">${logicLabel}</span>` +
                    `</td></tr>`;
            }
            let tr = `<tr><td class="text-center small text-muted">${ri + 1}</td>`;
            this._dtColumns.forEach((_, ci) => {
                const cond = (row.conditions || [])[ci] || { op: 'eq', value: '' };
                tr += `<td>` +
                    `<div class="d-flex gap-1">` +
                    `<select class="form-select form-select-sm dt-op-select" data-dt-r="${ri}" data-dt-c="${ci}">` +
                    this._dtOpOptions(cond.op) +
                    `</select>` +
                    `<input type="text" class="form-control form-control-sm dt-val-input" value="${this._esc(cond.value)}" data-dt-cell-val data-dt-r="${ri}" data-dt-c="${ci}">` +
                    `</div></td>`;
            });
            tr += `<td class="text-center"><i class="bi bi-x-circle dt-remove-btn" data-dt-remove-row="${ri}" title="Remove row" role="button" tabindex="0"></i></td>`;
            tr += '</tr>';
            tbody.innerHTML += tr;
        });
    },

    _dtOpOptions(selected) {
        const ops = [
            ['eq', '='], ['neq', '≠'], ['gt', '>'], ['gte', '≥'],
            ['lt', '<'], ['lte', '≤'], ['startsWith', 'starts'], ['endsWith', 'ends'],
            ['contains', 'contains'], ['any', 'any'],
        ];
        return ops.map(([v, l]) => `<option value="${v}" ${v === selected ? 'selected' : ''}>${l}</option>`).join('');
    },

    _dtTargetClassChanged() {
        const cls = (document.getElementById('dtTargetClass') || {}).value || '';
        this._populatePropertySelect('dtOutputProperty', '', cls);
        this._dtColumns.forEach(c => { c.property = ''; });
        this._dtRenderGrid();
    },

    _dtColChanged(ci, val) { this._dtColumns[ci].property = val; },
    _dtCellChanged(ri, ci, field, val) {
        if (!this._dtRows[ri].conditions[ci]) this._dtRows[ri].conditions[ci] = { op: 'eq', value: '' };
        this._dtRows[ri].conditions[ci][field] = val;
    },

    _dtSyncOutputValue() {
        const action = document.getElementById('dtOutputAction')?.value;
        const grp = document.getElementById('dtOutputValueGroup');
        if (grp) {
            if (action === 'set_value') grp.classList.remove('d-none');
            else grp.classList.add('d-none');
        }
    },

    dtAddColumn() {
        this._dtColumns.push({ property: '', label: '' });
        this._dtRows.forEach(r => r.conditions.push({ op: 'eq', value: '' }));
        this._dtRenderGrid();
    },
    dtAddRow() {
        this._dtRows.push({ conditions: this._dtColumns.map(() => ({ op: 'eq', value: '' })), action_value: '' });
        this._dtRenderGrid();
    },
    _dtRemoveColumn(ci) {
        if (this._dtColumns.length <= 1) return;
        this._dtColumns.splice(ci, 1);
        this._dtRows.forEach(r => r.conditions.splice(ci, 1));
        this._dtRenderGrid();
    },
    _dtRemoveRow(ri) {
        if (this._dtRows.length <= 1) return;
        this._dtRows.splice(ri, 1);
        this._dtRenderGrid();
    },

    async dtSave() {
        const index = parseInt(document.getElementById('dtEditIndex').value, 10);
        const rule = {
            name: document.getElementById('dtName').value.trim(),
            target_class: document.getElementById('dtTargetClass').value,
            hit_policy: document.getElementById('dtHitPolicy').value,
            row_logic: this._dtGetRowLogic(),
            input_columns: this._dtColumns.map(c => ({ property: c.property, label: c.label || c.property })),
            output_column: {
                property: document.getElementById('dtOutputProperty').value,
                action: document.getElementById('dtOutputAction').value,
                value: (document.getElementById('dtOutputValue')?.value || '').trim(),
            },
            rows: this._dtRows,
            enabled: true,
        };
        if (index >= 0 && this.dtRules[index]) rule.enabled = this.dtRules[index].enabled;

        const result = await this._saveRule('decision_tables', rule, index);
        if (result && result.success) bootstrap.Modal.getInstance(document.getElementById('dtEditorModal'))?.hide();
    },

    // ═══════════════════════════════════════════════════════════
    // SPARQL CONSTRUCT RULES
    // ═══════════════════════════════════════════════════════════

    _renderSparqlList() {
        const container = document.getElementById('sparqlList');
        const empty = document.getElementById('sparqlEmpty');
        if (!container) return;
        container.querySelectorAll('.br-rule-card').forEach(c => c.remove());
        if (this.sparqlRules.length === 0) { if (empty) empty.classList.remove('d-none'); return; }
        if (empty) empty.classList.add('d-none');

        this.sparqlRules.forEach((r, i) => {
            const querySnip = (r.query || '').substring(0, 80).replace(/\n/g, ' ');
            this._renderRuleCard(container, {
                name: r.name || `SPARQL Rule ${i + 1}`,
                meta: this._esc(querySnip) + (r.query && r.query.length > 80 ? '…' : ''),
                enabled: r.enabled,
                onEdit: () => this.sparqlOpenEditor(i),
                onDelete: () => this._deleteRule('sparql_rules', i),
                onToggle: () => this._toggleEnabled('sparql_rules', i),
            });
        });
    },

    sparqlOpenEditor(index) {
        const isNew = index < 0;
        const r = isNew ? {} : { ...this.sparqlRules[index] };
        document.getElementById('sparqlEditIndex').value = index;
        document.getElementById('sparqlName').value = r.name || '';
        document.getElementById('sparqlDescription').value = r.description || '';
        document.getElementById('sparqlQuery').value = r.query || '';
        document.getElementById('sparqlEditorTitle').innerHTML = `<i class="bi bi-braces me-2"></i>${isNew ? 'Add' : 'Edit'} SPARQL Rule`;
        const svm = document.getElementById('sparqlValidationMsg');
        if (svm) svm.classList.add('d-none');
        new bootstrap.Modal(document.getElementById('sparqlEditorModal')).show();
    },

    async sparqlValidate() {
        const query = document.getElementById('sparqlQuery').value.trim();
        const name = document.getElementById('sparqlName').value.trim();
        const msgEl = document.getElementById('sparqlValidationMsg');
        try {
            const resp = await fetch('/ontology/rules/sparql_rules/validate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rule: { name, query } }),
            });
            const data = await resp.json();
            msgEl.classList.remove('d-none');
            if (data.valid) {
                msgEl.className = 'alert alert-success small py-2';
                msgEl.textContent = 'Query syntax is valid.';
            } else {
                msgEl.className = 'alert alert-danger small py-2';
                msgEl.textContent = (data.errors || []).join('; ') || 'Validation failed';
            }
        } catch (e) {
            msgEl.classList.remove('d-none');
            msgEl.className = 'alert alert-danger small py-2';
            msgEl.textContent = 'Validation request failed';
        }
    },

    async sparqlSave() {
        const index = parseInt(document.getElementById('sparqlEditIndex').value, 10);
        const rule = {
            name: document.getElementById('sparqlName').value.trim(),
            description: document.getElementById('sparqlDescription').value.trim(),
            query: document.getElementById('sparqlQuery').value,
            enabled: true,
        };
        if (index >= 0 && this.sparqlRules[index]) rule.enabled = this.sparqlRules[index].enabled;

        const result = await this._saveRule('sparql_rules', rule, index);
        if (result && result.success) bootstrap.Modal.getInstance(document.getElementById('sparqlEditorModal'))?.hide();
    },

    // ═══════════════════════════════════════════════════════════
    // AGGREGATE RULES
    // ═══════════════════════════════════════════════════════════

    _renderAggList() {
        const container = document.getElementById('aggList');
        const empty = document.getElementById('aggEmpty');
        if (!container) return;
        container.querySelectorAll('.br-rule-card').forEach(c => c.remove());
        if (this.aggRules.length === 0) { if (empty) empty.classList.remove('d-none'); return; }
        if (empty) empty.classList.add('d-none');

        const opLabels = { lt: '<', gt: '>', eq: '=', lte: '≤', gte: '≥', neq: '≠' };
        this.aggRules.forEach((r, i) => {
            const func = (r.aggregate_function || 'count').toUpperCase();
            const op = opLabels[r.operator] || r.operator || '?';
            this._renderRuleCard(container, {
                name: r.name || `Aggregate Rule ${i + 1}`,
                meta: `${this._esc(r.target_class || '?')} · ${func}(${this._esc(r.aggregate_property || r.group_by_property || '*')}) ${op} ${r.threshold ?? '?'}`,
                enabled: r.enabled,
                onEdit: () => this.aggOpenEditor(i),
                onDelete: () => this._deleteRule('aggregate_rules', i),
                onToggle: () => this._toggleEnabled('aggregate_rules', i),
            });
        });
    },

    aggOpenEditor(index) {
        const isNew = index < 0;
        const r = isNew ? {} : { ...this.aggRules[index] };
        document.getElementById('aggEditIndex').value = index;
        document.getElementById('aggName').value = r.name || '';
        document.getElementById('aggFunction').value = r.aggregate_function || 'count';
        document.getElementById('aggOperator').value = r.operator || 'gt';
        document.getElementById('aggThreshold').value = r.threshold ?? '';
        document.getElementById('aggEditorTitle').innerHTML = `<i class="bi bi-calculator me-2"></i>${isNew ? 'Add' : 'Edit'} Aggregate Rule`;

        const targetCls = r.target_class || '';
        this._populateClassSelect('aggTargetClass', targetCls);
        this._populateClassSelect('aggResultClass', r.result_class || '');
        this._populatePropertySelect('aggGroupBy', r.group_by_property || '', targetCls);
        this._populatePropertySelect('aggAggProp', r.aggregate_property || '', targetCls);
        new bootstrap.Modal(document.getElementById('aggEditorModal')).show();
    },

    _aggTargetClassChanged() {
        const cls = (document.getElementById('aggTargetClass') || {}).value || '';
        this._populatePropertySelect('aggGroupBy', '', cls);
        this._populatePropertySelect('aggAggProp', '', cls);
    },

    async aggSave() {
        const index = parseInt(document.getElementById('aggEditIndex').value, 10);
        const rule = {
            name: document.getElementById('aggName').value.trim(),
            target_class: document.getElementById('aggTargetClass').value,
            group_by_property: document.getElementById('aggGroupBy').value,
            aggregate_property: document.getElementById('aggAggProp').value,
            aggregate_function: document.getElementById('aggFunction').value,
            operator: document.getElementById('aggOperator').value,
            threshold: parseFloat(document.getElementById('aggThreshold').value) || 0,
            result_class: document.getElementById('aggResultClass').value || '',
            enabled: true,
        };
        if (index >= 0 && this.aggRules[index]) rule.enabled = this.aggRules[index].enabled;

        const result = await this._saveRule('aggregate_rules', rule, index);
        if (result && result.success) bootstrap.Modal.getInstance(document.getElementById('aggEditorModal'))?.hide();
    },

};
