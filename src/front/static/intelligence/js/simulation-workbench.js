/**
 * Simulation Workbench — edit one scenario (scope, hypothetical facts,
 * reasoning phases), save it and run it.
 */
const SimulationWorkbench = {
    currentId: null,
    status: '',
    patches: [],
    _cohortsLoaded: false,
    _focusLabel: '',
    _focusLookupTimer: null,
    _busy: {},

    init() {
        const on = (id, evt, fn) => {
            const el = document.getElementById(id);
            if (el) el.addEventListener(evt, fn);
        };
        on('simWbAddPatchBtn', 'click', () => this.openFactWizard());
        on('simWbFactPromptBtn', 'click', () => this.proposeFacts());
        on('simWbFactPromptClearBtn', 'click', () => this._clearFactPrompt());
        on('simWbFactProgressHide', 'click', () => this._hideFactProgress());
        on('simWbSaveBtn', 'click', () => this.save());
        on('simWbRunBtn', 'click', () => this.run());
        on('simWbBackBtn', 'click', () => {
            if (window.SimulationPage) SimulationPage.showList();
        });
        on('simWbPurgeBtn', 'click', () => this.purgeScenario());
        on('simWbPatchSelectAll', 'change', (e) => this._selectAllPatches(e.target.checked));
        on('simWbPatchRemoveSelectedBtn', 'click', () => this._removeSelectedPatches());
        on('simWbResetBtn', 'click', () => {
            if (window.SimulationPage) SimulationPage.newScenario();
            else {
                this.reset();
                this.showTab('general');
            }
        });
        on('simWbScopeType', 'change', () => this._syncScopeUi());
        on('simWbFocusSearchBtn', 'click', () => this.openScopeEntityPicker());
        on('simWbFocus', 'change', () => this._onFocusUriChanged());
        on('simWbFocus', 'input', () => {
            clearTimeout(this._focusLookupTimer);
            this._focusLookupTimer = setTimeout(() => this._onFocusUriChanged(), 400);
        });

        const body = document.getElementById('simWbPatchesBody');
        if (body) {
            body.addEventListener('click', (e) => {
                const edit = e.target.closest('[data-edit]');
                if (edit) {
                    this.openFactWizard({ idx: Number(edit.dataset.edit) });
                    return;
                }
                const btn = e.target.closest('[data-remove]');
                if (!btn) return;
                this.patches.splice(Number(btn.dataset.remove), 1);
                this.renderPatches();
            });
            body.addEventListener('change', (e) => {
                const cb = e.target.closest('[data-select]');
                if (!cb) return;
                const idx = Number(cb.dataset.select);
                if (this.patches[idx]) this.patches[idx]._selected = cb.checked;
                this._syncPatchSelectionUi();
            });
        }

        this.reset();
        if (window.SimulationRules) SimulationRules.bind();
        const tabs = document.getElementById('simWbTabs');
        if (tabs) {
            tabs.addEventListener('shown.bs.tab', (e) => {
                const results = e.target && e.target.id === 'simWb-tab-results';
                if (results && window.SimulationResults) SimulationResults.onShow();
                this._syncTabLoading();
                if (window.SimulationPage && this.currentId) {
                    SimulationPage.syncScenarioUrl(
                        { simulation: this.currentId, tab: results ? 'results' : '' },
                        { replace: true },
                    );
                }
            });
        }
    },

    seedFromEntity(entity, urlLabel) {
        this.reset();
        this.showTab('general');
        this._set('simWbScopeType', 'entity');
        this._set('simWbFocus', entity);
        this._applyFocusLabel(urlLabel, entity);
        this._set('simWbLabel', `What if: ${this._focusLabel || SimulationPage.shortUri(entity)}`);
        this._syncScopeUi();
        this.addPatch({
            op: 'set',
            subject: entity,
            subject_label: this._focusLabel || SimulationPage.shortUri(entity),
            predicate: '',
            object: '',
        });
        if (!urlLabel) this._onFocusUriChanged();
    },

    _el(id) {
        return document.getElementById(id);
    },

    setTabBusy(tab, busy) {
        if (!this._busy) this._busy = {};
        const n = (this._busy[tab] || 0) + (busy ? 1 : -1);
        this._busy[tab] = Math.max(0, n);
        this._syncTabLoading();
    },

    _activeTabName() {
        const active = document.querySelector('#simWbTabs .nav-link.active');
        if (!active || !active.id) return 'general';
        return active.id.replace('simWb-tab-', '');
    },

    _syncTabLoading() {
        const overlay = this._el('simWbTabLoading');
        if (!overlay) return;
        const tab = this._activeTabName();
        const busy = (this._busy[tab] || 0) > 0;
        overlay.classList.toggle('d-none', !busy);
        overlay.setAttribute('aria-hidden', busy ? 'false' : 'true');
        const label = this._el('simWbTabLoadingLabel');
        if (label) {
            label.textContent = {
                general: 'Loading scenario…',
                facts: 'Loading facts…',
                reasoning: 'Loading rules…',
                results: 'Loading results…',
            }[tab] || 'Loading…';
        }
    },

    _set(id, value) {
        const el = this._el(id);
        if (el) el.value = value == null ? '' : value;
    },

    _val(id) {
        const el = this._el(id);
        return el ? String(el.value || '').trim() : '';
    },

    async _loadCohorts() {
        if (this._cohortsLoaded) return;
        this._cohortsLoaded = true;
        const select = this._el('simWbCohort');
        if (!select) return;
        this.setTabBusy('general', true);
        try {
            const data = await SimulationPage.api('/dtwin/cohorts/rules');
            const rules = data.rules || [];
            select.innerHTML = rules.length
                ? rules.map((r) => `<option value="${SimulationPage.esc(r.id)}">${SimulationPage.esc(r.label || r.name || r.id)}</option>`).join('')
                : '<option value="">No cohort rules defined</option>';
        } catch (err) {
            select.innerHTML = `<option value="">${SimulationPage.esc(err.message)}</option>`;
        } finally {
            this.setTabBusy('general', false);
        }
    },

    _syncScopeUi() {
        const type = this._val('simWbScopeType') || 'entity';
        const focus = this._el('simWbFocus');
        const focusGroup = this._el('simWbFocusGroup');
        const searchBtn = this._el('simWbFocusSearchBtn');
        const cohort = this._el('simWbCohort');
        const label = this._el('simWbFocusLabel');
        const hops = this._el('simWbHopsGroup');
        const isCohort = type === 'cohort';
        if (focusGroup) focusGroup.classList.toggle('d-none', isCohort);
        else if (focus) focus.classList.toggle('d-none', isCohort);
        if (searchBtn) searchBtn.classList.toggle('d-none', type !== 'entity');
        if (cohort) cohort.classList.toggle('d-none', !isCohort);
        if (hops) hops.classList.toggle('d-none', type !== 'entity');
        if (label) {
            label.textContent = { entity: 'Entity', cohort: 'Cohort rule' }[type];
            label.setAttribute('for', isCohort ? 'simWbCohort' : 'simWbFocus');
        }
        if (isCohort) this._loadCohorts();
        this._paintFocusCaption();
    },

    _applyFocusLabel(label, uri) {
        const local = SimulationPage.shortUri(uri);
        const text = String(label || '').trim();
        this._focusLabel = (text && text !== uri && text !== local) ? text : (text || '');
        this._paintFocusCaption();
    },

    _paintFocusCaption() {
        const cap = this._el('simWbFocusCaption');
        if (!cap) return;
        const type = this._val('simWbScopeType') || 'entity';
        const uri = this._val('simWbFocus');
        const local = SimulationPage.shortUri(uri);
        const show = type === 'entity' && this._focusLabel && this._focusLabel !== uri && this._focusLabel !== local;
        cap.textContent = show ? this._focusLabel : '';
        cap.classList.toggle('d-none', !show);
    },

    async _onFocusUriChanged() {
        if ((this._val('simWbScopeType') || 'entity') !== 'entity') return;
        const uri = this._val('simWbFocus');
        if (!uri) {
            this._applyFocusLabel('', '');
            return;
        }
        const label = await SimulationPage.resolveEntityLabel(uri);
        if (this._val('simWbFocus') !== uri) return;
        this._applyFocusLabel(label, uri);
        const name = this._el('simWbLabel');
        if (name && /^What if: /.test(name.value) && this._focusLabel) {
            name.value = `What if: ${this._focusLabel}`;
        }
    },

    reset() {
        this.currentId = null;
        this.status = 'draft';
        this.patches = [];
        this._syncTabLoading();
        const cfg = SimulationPage.config || {};
        this._set('simWbLabel', '');
        this._set('simWbDescription', '');
        this._set('simWbFactPrompt', '');
        this._set('simWbScopeType', 'entity');
        this._set('simWbFocus', '');
        this._applyFocusLabel('', '');
        this._set('simWbHops', cfg.default_hops != null ? cfg.default_hops : 2);
        this._set('simWbCap', cfg.entity_cap || 5000);
        this._applyPhases(null);
        const title = this._el('simWbTitle');
        if (title) title.textContent = '— new scenario';
        this._syncScopeUi();
        this.renderPatches();
        this.paintArtifacts(null, this.status);
        if (window.SimulationResults) SimulationResults.select(null);
    },

    showTab(name) {
        const btn = this._el(`simWb-tab-${name}`);
        if (btn) btn.click();
    },

    async load(id) {
        this.setTabBusy('general', true);
        try {
            const data = await SimulationPage.api(`/simulation/${encodeURIComponent(id)}`);
            const sim = data.simulation || {};
            const spec = sim.spec || {};
            const scope = spec.scope || {};
            this.currentId = sim.id;
            this.status = sim.status || 'draft';
            this._set('simWbLabel', spec.label || sim.label);
            this._set('simWbDescription', spec.description || '');
            this._set('simWbFactPrompt', spec.fact_prompt || '');
            const scopeType = scope.type === 'cohort' ? 'cohort' : 'entity';
            this._set('simWbScopeType', scopeType);
            this._syncScopeUi();
            if (scopeType === 'cohort') {
                await this._loadCohorts();
                this._set('simWbCohort', scope.focus);
            } else {
                this._set('simWbFocus', scope.focus);
                this._applyFocusLabel(scope.focus_label || '', scope.focus);
                if (!scope.focus_label) this._onFocusUriChanged();
            }
            this._set('simWbHops', scope.hops);
            this._set('simWbCap', scope.cap);
            this.patches = (spec.patches || []).map((p) => ({ ...p }));
            this._applyPhases(spec.phases, spec.rule_names);
            const title = this._el('simWbTitle');
            if (title) title.textContent = `— ${sim.label || sim.id}`;
            await this._resolvePatchLabels();
            this.paintArtifacts(sim.artifacts, sim.status);
        } catch (err) {
            showNotification(`Could not open scenario: ${err.message}`, 'error');
        } finally {
            this.setTabBusy('general', false);
        }
    },

    _applyPhases(phases, ruleNames) {
        document.querySelectorAll('#simWbPhases [data-phase]').forEach((cb) => {
            if (cb.disabled) {
                cb.checked = false;
                cb.indeterminate = false;
                return;
            }
            const key = cb.dataset.phase;
            cb.checked = phases ? !!phases[key] : (key === 'if_then' || key === 'graph');
            cb.indeterminate = false;
        });
        if (window.SimulationRules) SimulationRules.apply(ruleNames || null);
    },

    // -- Patches -----------------------------------------------------------

    async proposeFacts() {
        const prompt = this._val('simWbFactPrompt');
        if (!prompt) {
            showNotification('Describe the facts to add.', 'warning');
            return;
        }
        const btn = this._el('simWbFactPromptBtn');
        const idle = '<i class="bi bi-stars me-1"></i>Propose facts';
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>Proposing…';
        }
        this._resetFactProgress();
        this._appendFactProgress('Starting…', { current: true });
        try {
            const data = await this._streamProposeFacts({
                prompt,
                simulation_id: this.currentId || '',
                scope: {
                    type: this._val('simWbScopeType'),
                    focus: this._val('simWbFocus') || this._val('simWbCohort'),
                    focus_label: this._focusLabel || '',
                },
                existing_patches: this.patches.map((p) => ({
                    op: p.op,
                    subject: p.subject,
                    predicate: p.predicate,
                    object: p.object,
                })),
            });
            this._completeFactProgress();
            const merged = data.patches || [];
            const replyHtml = this._factReplyHtml(data.reply);
            if (this._samePatchList(this.patches, merged)) {
                showNotification(
                    replyHtml || 'No new facts proposed. Existing facts already cover this.',
                    'warning',
                    10000,
                );
            } else {
                this.patches = merged.map((row) => ({ ...row }));
                await this._resolvePatchLabels();
                if (replyHtml) showNotification(replyHtml, 'success', 10000);
            }
            await this._persistLastFactPrompt();
        } catch (err) {
            this._failFactProgress();
            showNotification(err.message, 'error');
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = idle;
            }
        }
    },

    _resetFactProgress() {
        const box = this._el('simWbFactProgress');
        const list = this._el('simWbFactProgressSteps');
        if (list) list.replaceChildren();
        if (box) box.classList.remove('d-none');
    },

    _hideFactProgress() {
        const box = this._el('simWbFactProgress');
        if (box) box.classList.add('d-none');
    },

    _patchFactKey(patch) {
        const row = patch || {};
        return [row.op, row.subject, row.predicate, row.object, row.datatype || ''].join('\0');
    },

    _samePatchList(left, right) {
        if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) {
            return false;
        }
        return left.every((row, i) => this._patchFactKey(row) === this._patchFactKey(right[i]));
    },

    _appendFactProgress(label, { current = false } = {}) {
        const list = this._el('simWbFactProgressSteps');
        if (!list) return;
        const last = list.lastElementChild;
        const same = last && last.textContent.trim() === String(label || '').trim();
        if (same && last.classList.contains('is-current')) return;
        this._completeFactProgress();
        const li = document.createElement('li');
        li.className = current ? 'is-current' : 'is-done';
        const icon = current
            ? '<span class="sim-fact-progress-icon spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>'
            : '<i class="sim-fact-progress-icon bi bi-check2" aria-hidden="true"></i>';
        li.innerHTML = `${icon}<span>${SimulationPage.esc(label)}</span>`;
        list.appendChild(li);
        list.scrollTop = list.scrollHeight;
    },

    _completeFactProgress() {
        const list = this._el('simWbFactProgressSteps');
        if (!list) return;
        list.querySelectorAll('li.is-current').forEach((li) => {
            li.classList.remove('is-current');
            li.classList.add('is-done');
            const icon = li.querySelector('.sim-fact-progress-icon');
            if (icon) {
                const check = document.createElement('i');
                check.className = 'sim-fact-progress-icon bi bi-check2';
                check.setAttribute('aria-hidden', 'true');
                icon.replaceWith(check);
            }
        });
    },

    _failFactProgress() {
        const list = this._el('simWbFactProgressSteps');
        if (!list) return;
        list.querySelectorAll('li.is-current').forEach((li) => {
            li.classList.remove('is-current');
            const icon = li.querySelector('.sim-fact-progress-icon');
            if (icon) {
                const fail = document.createElement('i');
                fail.className = 'sim-fact-progress-icon bi bi-x-lg text-danger';
                fail.setAttribute('aria-hidden', 'true');
                icon.replaceWith(fail);
            }
        });
    },

    async _streamProposeFacts(body) {
        const resp = await fetch('/simulation/facts/agent/stream', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!resp.ok || !resp.body) {
            let data = {};
            try { data = await resp.json(); } catch (_) { data = {}; }
            throw new Error(data.message || data.detail || `Request failed (${resp.status})`);
        }
        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        try {
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                const parts = buffer.split('\n\n');
                buffer = parts.pop();
                for (const part of parts) {
                    const line = part.trim();
                    if (!line.startsWith('data:')) continue;
                    let event;
                    try { event = JSON.parse(line.slice(5).trim()); } catch (_) { continue; }
                    if (event.type === 'step') {
                        this._appendFactProgress(event.label || event.tool_name || 'Working', { current: true });
                    } else if (event.type === 'done') {
                        return event;
                    } else if (event.type === 'error') {
                        throw new Error(event.message || 'Could not propose hypothetical facts');
                    }
                }
            }
        } finally {
            reader.releaseLock();
        }
        throw new Error('The proposal stream ended without a result.');
    },

    async _persistLastFactPrompt() {
        if (!this.currentId) return;
        const body = this.collect();
        if (this._validate(body)) return;
        try {
            await this._putSave(body, { quiet: true });
        } catch (_) { /* textarea still holds the last prompt */ }
    },

    _factReplyHtml(reply) {
        const src = (reply == null ? '' : String(reply)).trim();
        if (!src) return '';
        const html = (typeof renderMarkdown === 'function')
            ? renderMarkdown(src)
            : SimulationPage.esc(src).replace(/\n/g, '<br>');
        return `<div class="oc-md">${html}</div>`;
    },

    addPatch(patch) {
        const focus = this._val('simWbScopeType') === 'entity' ? this._val('simWbFocus') : '';
        const row = patch || {
            op: 'set',
            subject: focus,
            subject_label: focus ? (this._focusLabel || SimulationPage.shortUri(focus)) : '',
            predicate: '',
            object: '',
        };
        this.patches.push(row);
        this.renderPatches();
        if (!patch || !row.subject_label) this._resolvePatchLabels();
    },

    renderPatches() {
        const body = this._el('simWbPatchesBody');
        const count = this._el('simWbPatchCount');
        if (count) count.textContent = String(this.patches.length);
        if (!body) return;
        if (!this.patches.length) {
            body.innerHTML = '<p class="text-center text-muted py-3 small mb-0">No hypothetical facts yet — add one to describe what changes.</p>';
            this._syncPatchSelectionUi();
            return;
        }
        body.innerHTML = this.patches.map((p, i) => `<article class="card sim-patch-card">
            <div class="card-body">
                <div class="d-flex align-items-center gap-2">
                    <input class="form-check-input flex-shrink-0" type="checkbox" data-select="${i}"
                           ${p._selected ? 'checked' : ''} aria-label="Select fact">
                    ${this._patchFieldsHtml(p, i)}
                    <div class="d-flex gap-1 flex-shrink-0">
                        <button type="button" class="btn btn-sm btn-outline-secondary" data-edit="${i}"
                                title="Edit fact" aria-label="Edit fact">
                            <i class="bi bi-pencil"></i>
                        </button>
                        <button type="button" class="btn btn-sm btn-outline-danger" data-remove="${i}"
                                title="Remove fact" aria-label="Remove fact">
                            <i class="bi bi-x-lg"></i>
                        </button>
                    </div>
                </div>
            </div>
        </article>`).join('');
        this._syncPatchSelectionUi();
    },

    _clearFactPrompt() {
        this._set('simWbFactPrompt', '');
        const box = this._el('simWbFactPrompt');
        if (box) box.focus();
    },

    _selectAllPatches(checked) {
        this.patches.forEach((row) => { row._selected = !!checked; });
        this.renderPatches();
    },

    _syncPatchSelectionUi() {
        const n = this.patches.filter((row) => row._selected).length;
        const btn = this._el('simWbPatchRemoveSelectedBtn');
        const count = this._el('simWbPatchSelectedCount');
        const all = this._el('simWbPatchSelectAll');
        if (count) count.textContent = String(n);
        if (btn) btn.classList.toggle('d-none', n === 0);
        if (all) {
            all.disabled = this.patches.length === 0;
            all.checked = this.patches.length > 0 && n === this.patches.length;
            all.indeterminate = n > 0 && n < this.patches.length;
        }
    },

    async _removeSelectedPatches() {
        const n = this.patches.filter((row) => row._selected).length;
        if (!n) return;
        const ok = await showConfirmDialog({
            title: 'Remove selected facts?',
            message: `Remove ${n} hypothetical fact${n === 1 ? '' : 's'} from this scenario?`,
            confirmText: 'Remove',
            cancelText: 'Cancel',
            confirmClass: 'btn-danger',
            icon: 'trash',
        });
        if (!ok) return;
        this.patches = this.patches.filter((row) => !row._selected);
        this.renderPatches();
    },

    async purgeScenario() {
        if (!this.currentId || !window.SimulationPage) return;
        await SimulationPage.purgeScenario(this.currentId);
    },

    _syncPurgeBtn() {
        const btn = this._el('simWbPurgeBtn');
        if (!btn) return;
        const running = this.status === 'running'
            || (window.SimulationPage && SimulationPage.runningId === this.currentId);
        const draft = !this.currentId || this.status === 'draft' || !this.status;
        btn.disabled = running || draft;
    },

    _patchFieldsHtml(p, i) {
        return `${this._opIcon(p.op)}
                    <div class="row g-2 sim-patch-fields">
                        <div class="col-12 col-lg-4">${this._termCell('subject', i, p, 'Subject')}</div>
                        <div class="col-12 col-lg-4">${this._termCell('predicate', i, p, 'Predicate')}</div>
                        <div class="col-12 col-lg-4">${this._termCell('object', i, p, 'Object / value')}</div>
                    </div>`;
    },

    _opIcon(op) {
        const catalog = {
            add: { icon: 'bi-plus-circle', label: 'Add' },
            remove: { icon: 'bi-dash-circle', label: 'Remove' },
            set: { icon: 'bi-arrow-repeat', label: 'Set' },
        };
        const meta = catalog[op] || catalog.set;
        return `<span class="sim-patch-op" title="${meta.label}" aria-label="${meta.label}">
            <i class="bi ${meta.icon}" aria-hidden="true"></i>
        </span>`;
    },

    _termCell(field, idx, patch, placeholder) {
        const esc = (v) => SimulationPage.esc(v);
        const uri = patch[field] || '';
        const isUri = SimulationPage.isUri(uri);
        const idShown = isUri ? SimulationPage.shortUri(uri) : uri;
        const label = String(patch[`${field}_label`] || '').trim();
        const caption = (label && label !== idShown && label !== uri) ? label : '';
        const shown = idShown || placeholder;
        return `<div class="sim-term">
            ${this._termKindHtml(field, patch, uri, isUri, caption)}
            <div class="sim-term-id" title="${esc(uri)}">${esc(shown)}</div>
        </div>`;
    },

    _termKindHtml(field, patch, uri, isUri, caption) {
        const esc = (v) => SimulationPage.esc(v);
        const labelBit = caption
            ? ` <span class="sim-term-caption">(${esc(caption)})</span>`
            : '';
        if (field === 'predicate') {
            if (!uri) return '';
            const kind = patch.predicate_kind || this._predicateKind(uri);
            const relationship = kind === 'relationship';
            const icon = relationship ? 'bi-arrow-left-right' : 'bi-tag';
            const text = relationship ? 'Relationship' : 'Attribute';
            const range = patch.predicate_range_label || '';
            return `<div class="sim-term-kind">
                <i class="bi ${icon}" aria-hidden="true"></i>
                <span>${esc(text)}${range ? ` → ${esc(range)}` : ''}${labelBit}</span>
            </div>`;
        }
        if (isUri) {
            const typeName = patch[`${field}_type`] || '';
            const emoji = patch[`${field}_emoji`] || this._emojiForType(
                typeName, patch[`${field}_type_uri`],
            );
            const shownType = typeName || 'Entity';
            return `<div class="sim-term-kind">
                <span class="sim-term-emoji" aria-hidden="true">${esc(emoji)}</span>
                <span>${esc(shownType)}${labelBit}</span>
            </div>`;
        }
        if (field === 'object' && uri) {
            return `<div class="sim-term-kind">
                <i class="bi bi-fonts" aria-hidden="true"></i>
                <span>Value${labelBit}</span>
            </div>`;
        }
        return '';
    },

    async _ensureOntology() {
        if (window.SimulationFactWizard && typeof SimulationFactWizard._ensureOntology === 'function') {
            await SimulationFactWizard._ensureOntology();
        }
    },

    _ontologyClasses() {
        return (window.SimulationFactWizard && SimulationFactWizard._ontology
            && SimulationFactWizard._ontology.classes) || [];
    },

    _ontologyProperties() {
        return (window.SimulationFactWizard && SimulationFactWizard._ontology
            && SimulationFactWizard._ontology.properties) || [];
    },

    _emojiForType(type, typeUri) {
        const cls = window.SimulationFactWizard
            && typeof SimulationFactWizard._findClass === 'function'
            ? SimulationFactWizard._findClass(type, typeUri)
            : null;
        return (cls && (cls.emoji || cls.icon)) || '📦';
    },

    _classTitle(type, typeUri) {
        const cls = window.SimulationFactWizard
            && typeof SimulationFactWizard._findClass === 'function'
            ? SimulationFactWizard._findClass(type, typeUri)
            : null;
        if (cls) return cls.label || cls.name || type || SimulationPage.shortUri(typeUri);
        return type || SimulationPage.shortUri(typeUri) || '';
    },

    _predicateKind(uri) {
        const prop = this._findProperty(uri);
        if (!prop) return 'attribute';
        if (window.SimulationFactWizard && typeof SimulationFactWizard._isObjectProp === 'function') {
            return SimulationFactWizard._isObjectProp(prop) ? 'relationship' : 'attribute';
        }
        return String(prop.type || '').indexOf('Object') >= 0 ? 'relationship' : 'attribute';
    },

    _findProperty(uri) {
        const raw = String(uri || '');
        if (!raw) return null;
        const local = SimulationPage.shortUri(raw).toLowerCase();
        return this._ontologyProperties().find((p) => {
            const puri = String(p.uri || p.iri || '');
            return puri === raw || SimulationPage.shortUri(puri).toLowerCase() === local;
        }) || null;
    },

    _decoratePredicate(patch) {
        const uri = patch.predicate || '';
        if (!uri) return;
        const prop = this._findProperty(uri);
        const kind = this._predicateKind(uri);
        patch.predicate_kind = kind;
        if (prop) {
            patch.predicate_label = prop.label || prop.name || SimulationPage.shortUri(uri);
            if (kind === 'relationship' && prop.range) {
                patch.predicate_range_label = this._classTitle(prop.range, prop.range)
                    || SimulationPage.shortUri(prop.range);
            }
        } else if (!patch.predicate_label) {
            patch.predicate_label = SimulationPage.shortUri(uri);
        }
    },

    async _resolvePatchLabels() {
        this.setTabBusy('facts', true);
        try {
            await this._ensureOntology();
            const jobs = [];
            this.patches.forEach((patch, i) => {
                this._decoratePredicate(patch);
                ['subject', 'object'].forEach((field) => {
                    const value = patch[field];
                    if (!value) return;
                    if (!SimulationPage.isUri(value)) {
                        if (!patch[`${field}_label`]) patch[`${field}_label`] = value;
                        return;
                    }
                    jobs.push(
                        SimulationPage.resolveEntitySeed(value).then((seed) => {
                            if (!this.patches[i] || this.patches[i][field] !== value) return;
                            const row = this.patches[i];
                            row[`${field}_label`] = seed.label || SimulationPage.shortUri(value);
                            if (seed.type) {
                                row[`${field}_type`] = this._classTitle(seed.type, seed.type_uri) || seed.type;
                                row[`${field}_type_uri`] = seed.type_uri || '';
                                row[`${field}_emoji`] = this._emojiForType(seed.type, seed.type_uri);
                            }
                        }),
                    );
                });
            });
            if (jobs.length) await Promise.all(jobs);
            this.renderPatches();
        } finally {
            this.setTabBusy('facts', false);
        }
    },

    openScopeEntityPicker() {
        if ((this._val('simWbScopeType') || 'entity') !== 'entity') return;
        if (!window.SimulationFactWizard) return;
        SimulationFactWizard.open({
            pick: 'scope',
            patch: { subject: this._val('simWbFocus') },
        });
    },

    applyScopeEntity(uri, label) {
        const subject = String(uri || '').trim();
        if (!subject) return;
        this._set('simWbScopeType', 'entity');
        this._set('simWbFocus', subject);
        this._applyFocusLabel(label, subject);
        const name = this._el('simWbLabel');
        if (name && (!name.value.trim() || /^What if: /.test(name.value))) {
            name.value = `What if: ${this._focusLabel || SimulationPage.shortUri(subject)}`;
        }
        this._syncScopeUi();
    },

    openFactWizard({ idx = null, startField = 'subject' } = {}) {
        if (idx == null) {
            const focus = this._val('simWbScopeType') === 'entity' ? this._val('simWbFocus') : '';
            if (window.SimulationFactWizard) {
                SimulationFactWizard.open({
                    idx: null,
                    startField,
                    patch: { op: 'set', subject: focus, predicate: '', object: '' },
                });
            }
            return;
        }
        if (window.SimulationFactWizard) {
            SimulationFactWizard.open({
                idx,
                startField,
                patch: this.patches[idx] || { op: 'set', subject: '', predicate: '', object: '' },
            });
        }
    },

    paintArtifacts(artifacts, status) {
        if (status) this.status = status;
        this._syncPurgeBtn();
        const box = this._el('simWbArtifacts');
        const badge = this._el('simWbArtifactStatus');
        const data = artifacts || {};
        const state = status || data.status || '';
        const objects = data.objects || [];
        const counts = data.counts || {};
        const created = state === 'ready';
        if (badge) {
            badge.textContent = created ? 'created' : (state || 'none');
            badge.className = `badge ${created ? 'bg-success' : 'bg-secondary'}`;
        }
        if (!box) return;
        if (!objects.length) {
            box.innerHTML = '<p class="text-muted small mb-0">Save and run the scenario to create the sandbox table and Explorer overlay view.</p>';
            return;
        }
        const esc = (v) => SimulationPage.esc(v);
        const roles = {
            sandbox: {
                label: 'Sandbox triples',
                hint: objKind => objKind === 'graph'
                    ? 'Private Neo4j graph of the in-scope triples. Hypothetical facts and scenario reasoning write here. Explorer overlays this marker graph onto the live neighbourhood.'
                    : 'Private copy of the in-scope triples. Hypothetical facts and scenario reasoning write here. Explorer never reads this table directly; it uses the overlay view below.',
            },
            before_picture: {
                label: 'Before picture',
                hint: () => 'Seeded sandbox triples captured before patches and reasoning. Results Added/Removed are computed against this companion.',
            },
            origin: {
                label: 'Fact origins',
                hint: () => 'Tags each added triple as your fact or a reasoning consequence for the Results badges.',
            },
            explorer_inferred: {
                label: 'Explorer overlay',
                hint: () => 'Live inferred graph UNION the sandbox. Sandbox (subject, predicate) pairs replace live ones; other live facts stay visible. API, MCP and Explorer always read this view. Scenario-run inferences appear because they were written into the sandbox.',
            },
        };
        const rows = objects.map((obj) => {
            const role = roles[obj.role] || {};
            const label = role.label || obj.role || '';
            const hint = typeof role.hint === 'function'
                ? role.hint(obj.kind)
                : (role.hint || '');
            const items = created && obj.role === 'sandbox' && counts.triples != null
                ? `${Number(counts.triples).toLocaleString()} triples`
                : '—';
            return `<tr>
                <td><span class="badge bg-secondary bg-opacity-25 text-dark">${esc(obj.kind || 'object')}</span></td>
                <td class="sim-col-role text-nowrap">${esc(label)}</td>
                <td class="sim-artifact-name-cell">
                    <code class="sim-artifact-name" title="${esc(obj.name || '')}">${esc(obj.name || '')}</code>
                    ${hint ? `<p class="form-text small mb-0 sim-artifact-hint">${esc(hint)}</p>` : ''}
                </td>
                <td class="text-muted text-nowrap">${items}</td>
            </tr>`;
        }).join('');
        let stats = '';
        if (created) {
            const cell = (label, value) => `<div class="sim-artifact-stat">`
                + `<span class="text-muted">${esc(label)}</span>`
                + `<strong>${Number(value || 0).toLocaleString()}</strong></div>`;
            stats = `<div class="sim-artifact-counts small mt-3">
                ${cell('Entities', counts.entities)}
                ${cell('Triples', counts.triples)}
                ${cell('Added', counts.added)}
                ${cell('Removed', counts.removed)}
                ${cell('Inferred', counts.inferred)}
                ${cell('Touched', counts.touched)}
            </div>`;
        } else {
            stats = '<p class="text-muted small mb-0 mt-2">Run the scenario to materialise these objects.</p>';
        }
        box.innerHTML = `<div class="table-responsive"><table class="table table-sm align-middle mb-0 sim-table sim-artifact-table">
            <thead><tr><th>Kind</th><th>Role</th><th>Name</th><th>Items</th></tr></thead>
            <tbody>${rows}</tbody>
        </table></div>${stats}`;
    },

    async refreshArtifacts() {
        if (!this.currentId) {
            this.paintArtifacts(null);
            return;
        }
        try {
            const data = await SimulationPage.api(`/simulation/${encodeURIComponent(this.currentId)}`);
            const sim = data.simulation || {};
            this.status = sim.status || this.status;
            this.paintArtifacts(sim.artifacts, sim.status);
        } catch (_) {
            /* keep the last catalog if the refresh fails */
        }
    },

    applyFactFromWizard(patch, idx) {
        const row = {
            op: patch.op || 'set',
            subject: patch.subject || '',
            predicate: patch.predicate || '',
            object: patch.object || '',
            subject_label: patch.subject_label || '',
            predicate_label: patch.predicate_label || '',
            object_label: patch.object_label || '',
            subject_type: patch.subject_type || '',
            subject_type_uri: patch.subject_type_uri || '',
            subject_emoji: patch.subject_emoji || '',
            predicate_kind: patch.predicate_kind || '',
            predicate_range_label: patch.predicate_range_label || '',
            object_type: patch.object_type || '',
            object_type_uri: patch.object_type_uri || '',
            object_emoji: patch.object_emoji || '',
        };
        if (idx == null || !this.patches[idx]) this.patches.push(row);
        else this.patches[idx] = row;
        this.renderPatches();
        this._resolvePatchLabels();
    },

    // -- Save / run --------------------------------------------------------

    collect() {
        const type = this._val('simWbScopeType') || 'entity';
        const phases = window.SimulationRules
            ? SimulationRules.collectPhases()
            : {};
        if (!Object.keys(phases).length) {
            document.querySelectorAll('#simWbPhases [data-phase]').forEach((cb) => {
                phases[cb.dataset.phase] = cb.checked;
            });
        }
        return {
            label: this._val('simWbLabel'),
            description: this._val('simWbDescription'),
            fact_prompt: this._val('simWbFactPrompt'),
            scope: {
                type,
                focus: type === 'cohort' ? this._val('simWbCohort') : this._val('simWbFocus'),
                hops: Number(this._val('simWbHops') || 0),
                cap: Number(this._val('simWbCap') || 0),
                focus_label: type === 'entity' ? this._focusLabel : '',
            },
            patches: this.patches
                .filter((p) => (p.subject || p.predicate || p.object))
                .map((p) => ({
                    op: p.op,
                    subject: String(p.subject || '').trim(),
                    predicate: String(p.predicate || '').trim(),
                    object: String(p.object || ''),
                })),
            phases,
            rule_names: window.SimulationRules
                ? SimulationRules.collectRuleNames(phases)
                : {},
        };
    },

    _validate(body) {
        if (!body.label) return 'Give the scenario a name';
        if (!body.scope.focus) return 'Pick the scope focus (entity or cohort)';
        const bad = body.patches.find((p) => !p.subject || !p.predicate || (p.op !== 'remove' && p.object === ''));
        if (bad) return 'Every fact needs a subject, a predicate and (except Remove) a value';
        return '';
    },

    async save({ quiet = false } = {}) {
        const body = this.collect();
        const problem = this._validate(body);
        if (problem) {
            showNotification(problem, 'warning');
            return null;
        }
        try {
            return await this._putSave(body, { quiet });
        } catch (err) {
            if (!this.currentId || !/is running/i.test(err.message || '')) {
                showNotification(`Save failed: ${err.message}`, 'error');
                return null;
            }
            try {
                await SimulationPage.api(
                    `/simulation/${encodeURIComponent(this.currentId)}/abort`,
                    { method: 'POST' },
                );
                if (typeof SimulationPage._stopPolling === 'function') {
                    SimulationPage._stopPolling();
                }
                const id = await this._putSave(body, { quiet: true });
                showNotification('Previous run was still marked running — scenario saved', 'warning');
                return id;
            } catch (retryErr) {
                showNotification(`Save failed: ${retryErr.message}`, 'error');
                return null;
            }
        }
    },

    async _putSave(body, { quiet = false } = {}) {
        const data = this.currentId
            ? await SimulationPage.api(`/simulation/${encodeURIComponent(this.currentId)}`, { method: 'PUT', body })
            : await SimulationPage.api('/simulation/create', { method: 'POST', body });
        const sim = data.simulation || {};
        this.currentId = sim.id;
        const title = this._el('simWbTitle');
        if (title) title.textContent = `— ${sim.label || sim.id}`;
        this.paintArtifacts(sim.artifacts, sim.status);
        if (window.SimulationPage) {
            SimulationPage.rememberScenario(sim.id);
            SimulationPage.syncScenarioUrl({ simulation: sim.id }, { replace: true });
        }
        if (!quiet) showNotification('Scenario saved', 'success');
        SimulationPage.loadScenarios();
        return sim.id;
    },

    async run() {
        const id = await this.save({ quiet: true });
        if (id) SimulationPage.startRun(id);
    },
};

window.SimulationWorkbench = SimulationWorkbench;
document.addEventListener('DOMContentLoaded', () => SimulationWorkbench.init());
