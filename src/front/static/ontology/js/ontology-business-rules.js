/**
 * OntoBricks — ontology-business-rules.js
 * Tabbed Business Rules manager: coordinates Simple (IF/THEN, delegated to
 * IfThenModule) and Complex (SPARQL CONSTRUCT) rules.
 */
window.BusinessRulesModule = {
    _initialized: false,
    _brStaticUiBound: false,
    _ontologyClasses: [],
    _ontologyProperties: [],

    // Rule data per tab
    sparqlRules: [],

    // ── Initialization ───────────────────────────────────────

    async init() {
        if (this._initialized) {
            this._refreshAllBadges();
            return;
        }
        this._initialized = true;

        this._bindBrStaticUiOnce();

        await this._loadOntologyItems();

        const loaders = [
            this._loadRules('sparql_rules'),
        ];
        if (typeof IfThenModule !== 'undefined') {
            IfThenModule.loadOntologyItems();
            loaders.push(IfThenModule.loadRules());
        }
        await Promise.all(loaders);
        this._refreshAllBadges();
    },

    _bindBrStaticUiOnce() {
        if (this._brStaticUiBound) return;
        const root = document.getElementById('if-then-section');
        if (!root) return;
        this._brStaticUiBound = true;

        root.addEventListener('click', (e) => {
            const t = e.target.closest('[data-br-action]');
            if (!t || !root.contains(t)) return;
            const act = t.getAttribute('data-br-action');
            if (act === 'add-rule') this.addRuleForActiveTab();
            else if (act === 'auto-generate') this.autoGenerate();
            else if (act === 'suggest-select-all') this.selectAllSuggestions(true);
            else if (act === 'suggest-select-none') this.selectAllSuggestions(false);
            else if (act === 'suggest-accept') this.acceptSuggestions();
            else if (act === 'sparql-validate') this.sparqlValidate();
            else if (act === 'sparql-save') this.sparqlSave();
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
                case 'sparql_rules': this.sparqlRules = data.rules || []; this._renderSparqlList(); break;
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
        const confirmed = await showConfirmDialog({
            title: 'Delete Rule',
            message: 'Are you sure you want to delete this rule?',
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash',
        });
        if (!confirmed) return;
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
            case 'sparql_rules': rules = this.sparqlRules; break;
        }
        if (!rules || !rules[index]) return;
        const rule = { ...rules[index], enabled: !rules[index].enabled };
        await this._saveRule(ruleType, rule, index);
    },

    // ── Tab routing ─────────────────────────────────────────

    _activeTab() {
        const active = document.querySelector('#brTabs .nav-link.active');
        if (!active) return 'if_then';
        const target = active.getAttribute('data-bs-target') || '';
        if (target.includes('sparql')) return 'sparql';
        return 'if_then';
    },

    addRuleForActiveTab() {
        switch (this._activeTab()) {
            case 'if_then': if (typeof IfThenModule !== 'undefined') IfThenModule.addRule(); break;
            case 'sparql': this.sparqlOpenEditor(-1); break;
        }
    },

    // ── Badge updates ───────────────────────────────────────

    _refreshAllBadges() {
        const ifThenCount = (typeof IfThenModule !== 'undefined' && IfThenModule.rules) ? IfThenModule.rules.length : 0;
        this._setBadge('brBadgeIfThen', ifThenCount);
        this._setBadge('brBadgeSparql', this.sparqlRules.length);
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

    _renderRuleCard(container, { name, meta, enabled, badge, onEdit, onDelete, onToggle }) {
        const card = document.createElement('div');
        const isEnabled = enabled !== false;
        card.className = `ob-rule-card br-rule-card mb-2${isEnabled ? '' : ' opacity-50'}`;
        const badgeHtml = badge || (
            `<span class="badge bg-warning text-dark bg-opacity-75 small">` +
            `<i class="bi bi-braces me-1"></i>Complex</span>`
        );
        const metaHtml = meta
            ? `<div class="text-muted small mt-1"><code>${meta}</code></div>`
            : '';
        const toggleHtml = onToggle
            ? `<button class="btn btn-outline-secondary btn-sm" title="${isEnabled ? 'Disable' : 'Enable'}">` +
              `<i class="bi ${isEnabled ? 'bi-toggle-on text-success' : 'bi-toggle-off text-danger'}"></i>` +
              `</button>`
            : '';

        card.innerHTML =
            `<div class="d-flex align-items-start justify-content-between">` +
            `  <div class="flex-grow-1">` +
            `    <div class="d-flex align-items-center gap-2 mb-1">${badgeHtml}</div>` +
            `    <div class="small fw-medium">${this._esc(name)}</div>` +
            `    ${metaHtml}` +
            `  </div>` +
            `  <div class="btn-group btn-group-sm ms-2 flex-shrink-0">` +
            `    ${toggleHtml}` +
            `    <button class="btn btn-outline-secondary btn-sm" title="Edit"><i class="bi bi-pencil"></i></button>` +
            `    <button class="btn btn-outline-danger btn-sm" title="Delete"><i class="bi bi-trash"></i></button>` +
            `  </div>` +
            `</div>`;
        const btns = [...card.querySelectorAll('button')];
        let i = 0;
        if (onToggle) btns[i++].addEventListener('click', onToggle);
        if (onEdit) btns[i++].addEventListener('click', onEdit);
        if (onDelete) btns[i].addEventListener('click', onDelete);
        container.appendChild(card);
    },

    // ═══════════════════════════════════════════════════════════
    // SPARQL CONSTRUCT RULES  (UI label: "Complex")
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
                name: r.name || `Complex Rule ${i + 1}`,
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
        document.getElementById('sparqlEditorTitle').innerHTML = `<i class="bi bi-braces me-2"></i>${isNew ? 'Add' : 'Edit'} Complex Rule`;
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
                msgEl.textContent = 'Query is valid and compiles to SQL.';
            } else {
                msgEl.className = 'alert alert-danger small py-2';
                msgEl.textContent = data.message || 'Validation failed';
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
    // AUTO-GENERATE (agent) — suggest + review + accept
    // ═══════════════════════════════════════════════════════════

    _suggestions: { if_then_rules: [], sparql_rules: [] },
    _genTaskActive: false,

    SUGGEST_SECTIONS: [
        { key: 'if_then_rules', label: 'Simple', icon: 'bi-code-square', badge: 'bg-primary' },
        { key: 'sparql_rules', label: 'Complex', icon: 'bi-braces', badge: 'bg-warning text-dark' },
    ],

    async autoGenerate() {
        if (this._genTaskActive) return;

        const loadingEl = document.getElementById('brSuggestLoading');
        const errorEl = document.getElementById('brSuggestError');
        const emptyEl = document.getElementById('brSuggestEmpty');
        const listEl = document.getElementById('brSuggestList');
        const acceptBtn = document.getElementById('brSuggestAcceptBtn');
        const progressEl = document.getElementById('brSuggestProgress');

        if (loadingEl) loadingEl.style.display = '';
        if (errorEl) errorEl.style.display = 'none';
        if (emptyEl) emptyEl.style.display = 'none';
        if (listEl) listEl.style.display = 'none';
        if (acceptBtn) acceptBtn.classList.add('d-none');
        if (progressEl) progressEl.textContent = 'Generating business rules…';

        this._suggestions = { if_then_rules: [], sparql_rules: [] };
        new bootstrap.Modal(document.getElementById('brSuggestModal')).show();

        this._genTaskActive = true;
        try {
            const resp = await fetch('/ontology/business-rules/generate-async', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ options: {}, guidelines: '', documents: [] }),
            });
            const data = await resp.json();
            if (!data.success || !data.task_id) {
                this._showSuggestError(data.message || 'Could not start generation');
                return;
            }
            const result = await this._pollGenTask(data.task_id, progressEl);
            if (result === null) return;
            this._suggestions = {
                if_then_rules: result.if_then_rules || [],
                sparql_rules: result.sparql_rules || [],
            };
            this._renderSuggestions();
        } catch (e) {
            console.error('[BusinessRules] Auto-generate error:', e);
            this._showSuggestError('Error generating rules: ' + (e.message || e));
        } finally {
            this._genTaskActive = false;
        }
    },

    async _pollGenTask(taskId, progressEl) {
        const sleep = (ms) => new Promise(r => setTimeout(r, ms));
        for (let i = 0; i < 150; i++) {
            await sleep(2000);
            let task;
            try {
                const resp = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
                const data = await resp.json();
                if (!data.success) { this._showSuggestError('Task not found'); return null; }
                task = data.task;
            } catch (e) {
                continue;
            }
            if (progressEl && task.message) progressEl.textContent = task.message;
            if (task.status === 'completed') return task.result || {};
            if (task.status === 'failed') {
                this._showSuggestError(task.error || 'Generation failed');
                return null;
            }
        }
        this._showSuggestError('Generation timed out');
        return null;
    },

    _showSuggestError(msg) {
        const loadingEl = document.getElementById('brSuggestLoading');
        const errorEl = document.getElementById('brSuggestError');
        if (loadingEl) loadingEl.style.display = 'none';
        if (errorEl) { errorEl.style.display = ''; errorEl.textContent = msg; }
    },

    _suggestMeta(key, rule) {
        switch (key) {
            case 'if_then_rules':
                return `${this._esc(rule.antecedent || '')} → ${this._esc(rule.consequent || '')}`;
            case 'sparql_rules': {
                const snip = (rule.query || '').substring(0, 90).replace(/\n/g, ' ');
                return this._esc(snip) + ((rule.query || '').length > 90 ? '…' : '');
            }
            default:
                return '';
        }
    },

    _renderSuggestions() {
        const loadingEl = document.getElementById('brSuggestLoading');
        const emptyEl = document.getElementById('brSuggestEmpty');
        const listEl = document.getElementById('brSuggestList');
        const acceptBtn = document.getElementById('brSuggestAcceptBtn');
        const itemsEl = document.getElementById('brSuggestItems');
        const countEl = document.getElementById('brSuggestCount');
        if (loadingEl) loadingEl.style.display = 'none';

        const total = this.SUGGEST_SECTIONS.reduce((n, s) => n + (this._suggestions[s.key] || []).length, 0);
        if (total === 0) {
            if (emptyEl) emptyEl.style.display = '';
            return;
        }
        if (listEl) listEl.style.display = '';
        if (acceptBtn) acceptBtn.classList.remove('d-none');
        if (countEl) countEl.textContent = `${total} rule(s) proposed`;

        let html = '';
        for (const sec of this.SUGGEST_SECTIONS) {
            const rules = this._suggestions[sec.key] || [];
            if (rules.length === 0) continue;
            html += `<div class="br-suggest-section mb-3">` +
                `<div class="br-suggest-section-title small fw-semibold text-muted mb-2">` +
                `<span class="badge ${sec.badge} bg-opacity-75 me-1"><i class="bi ${sec.icon} me-1"></i>${sec.label}</span>` +
                `${rules.length} rule(s)</div>`;
            rules.forEach((rule, i) => {
                html += `<label for="brSug_${sec.key}_${i}" class="d-flex align-items-start gap-3 border rounded px-3 py-2 mb-2 br-suggest-item" style="cursor:pointer">` +
                    `<input class="form-check-input flex-shrink-0 mt-1" type="checkbox" id="brSug_${sec.key}_${i}" data-br-sug-key="${sec.key}" data-br-sug-idx="${i}" checked>` +
                    `<div class="flex-grow-1 min-w-0">` +
                    `<div class="small fw-medium text-body">${this._esc(rule.name || '(unnamed)')}</div>` +
                    (rule.description ? `<div class="text-muted small">${this._esc(rule.description)}</div>` : '') +
                    `<div class="text-muted small mt-1"><code class="text-secondary">${this._suggestMeta(sec.key, rule)}</code></div>` +
                    `</div></label>`;
            });
            html += `</div>`;
        }
        if (itemsEl) itemsEl.innerHTML = html;
    },

    selectAllSuggestions(checked) {
        document.querySelectorAll('#brSuggestItems [data-br-sug-idx]').forEach(el => { el.checked = checked; });
    },

    async acceptSuggestions() {
        const payload = { if_then_rules: [], sparql_rules: [] };
        let selectedCount = 0;
        document.querySelectorAll('#brSuggestItems [data-br-sug-idx]').forEach(el => {
            if (!el.checked) return;
            const key = el.getAttribute('data-br-sug-key');
            const idx = parseInt(el.getAttribute('data-br-sug-idx'), 10);
            const rule = (this._suggestions[key] || [])[idx];
            if (rule) { payload[key].push(rule); selectedCount++; }
        });

        if (selectedCount === 0) {
            if (typeof showNotification === 'function') showNotification('No rules selected', 'warning');
            return;
        }

        try {
            const resp = await fetch('/ontology/business-rules/accept-suggestions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify(payload),
            });
            const data = await resp.json();
            if (!data.success) {
                if (typeof showNotification === 'function') showNotification(data.message || 'Accept failed', 'error');
                return;
            }
            await this._loadRules('sparql_rules');
            if (typeof IfThenModule !== 'undefined') await IfThenModule.loadRules();
            this._refreshAllBadges();
            bootstrap.Modal.getInstance(document.getElementById('brSuggestModal'))?.hide();

            const added = data.added_total || 0;
            const rejected = (data.rejected || []).length;
            const duplicates = data.duplicates_total || (data.duplicates || []).length || 0;
            let msg = `Added ${added} rule(s)`;
            if (duplicates) msg += ` · ${duplicates} skipped (duplicate)`;
            if (rejected) msg += ` · ${rejected} skipped (invalid)`;
            if (typeof showNotification === 'function') {
                showNotification(msg, (rejected || duplicates) ? 'warning' : 'success');
            }
        } catch (e) {
            console.error('[BusinessRules] Accept error:', e);
            if (typeof showNotification === 'function') showNotification('Error accepting rules: ' + (e.message || e), 'error');
        }
    },

};
