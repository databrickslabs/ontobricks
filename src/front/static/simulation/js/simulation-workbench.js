/**
 * Simulation Workbench — edit one scenario (scope, hypothetical facts,
 * reasoning phases), save it and run it.
 */
const SimulationWorkbench = {
    currentId: null,
    patches: [],
    _cohortsLoaded: false,
    _focusLabel: '',
    _focusLookupTimer: null,

    init() {
        const on = (id, evt, fn) => {
            const el = document.getElementById(id);
            if (el) el.addEventListener(evt, fn);
        };
        on('simWbAddPatchBtn', 'click', () => this.addPatch());
        on('simWbFactWizardBtn', 'click', () => this.openFactWizard());
        on('simWbViewResultsBtn', 'click', () => {
            const id = this.currentId || (window.SimulationPage && SimulationPage.runningId);
            if (id && window.SimulationPage) SimulationPage.showResults(id);
        });
        on('simWbSaveBtn', 'click', () => this.save());
        on('simWbRunBtn', 'click', () => this.run());
        on('simWbResetBtn', 'click', () => this.reset());
        on('simWbScopeType', 'change', () => this._syncScopeUi());
        on('simWbFocus', 'change', () => this._onFocusUriChanged());
        on('simWbFocus', 'input', () => {
            clearTimeout(this._focusLookupTimer);
            this._focusLookupTimer = setTimeout(() => this._onFocusUriChanged(), 400);
        });

        const body = document.getElementById('simWbPatchesBody');
        if (body) {
            body.addEventListener('input', (e) => this._onPatchEdit(e));
            body.addEventListener('change', (e) => this._onPatchEdit(e));
            body.addEventListener('click', (e) => {
                const lookup = e.target.closest('[data-lookup]');
                if (lookup) {
                    this.openFactWizard({
                        idx: Number(lookup.dataset.idx),
                        startField: lookup.dataset.lookup,
                    });
                    return;
                }
                const btn = e.target.closest('[data-remove]');
                if (!btn) return;
                this.patches.splice(Number(btn.dataset.remove), 1);
                this.renderPatches();
            });
        }

        this.reset();
        const params = new URLSearchParams(window.location.search);
        const entity = params.get('entity');
        if (entity) {
            const urlLabel = (params.get('label') || '').trim();
            this._set('simWbScopeType', 'entity');
            this._set('simWbFocus', entity);
            this._applyFocusLabel(urlLabel, entity);
            this._set('simWbLabel', `What if: ${this._focusLabel || SimulationPage.shortUri(entity)}`);
            this._syncScopeUi();
            this.addPatch({ op: 'set', subject: entity, predicate: '', object: '' });
            if (!urlLabel) this._onFocusUriChanged();
        }
    },

    _el(id) {
        return document.getElementById(id);
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
        try {
            const data = await SimulationPage.api('/dtwin/cohorts/rules');
            const rules = data.rules || [];
            select.innerHTML = rules.length
                ? rules.map((r) => `<option value="${SimulationPage.esc(r.id)}">${SimulationPage.esc(r.label || r.name || r.id)}</option>`).join('')
                : '<option value="">No cohort rules defined</option>';
        } catch (err) {
            select.innerHTML = `<option value="">${SimulationPage.esc(err.message)}</option>`;
        }
    },

    _syncScopeUi() {
        const type = this._val('simWbScopeType') || 'entity';
        const focus = this._el('simWbFocus');
        const cohort = this._el('simWbCohort');
        const label = this._el('simWbFocusLabel');
        const hops = this._el('simWbHopsGroup');
        const isCohort = type === 'cohort';
        if (focus) focus.classList.toggle('d-none', isCohort);
        if (cohort) cohort.classList.toggle('d-none', !isCohort);
        if (hops) hops.classList.toggle('d-none', type !== 'entity');
        if (label) {
            label.textContent = { entity: 'Entity', cohort: 'Cohort rule', class: 'Class URI' }[type];
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
        this.patches = [];
        const cfg = SimulationPage.config || {};
        this._set('simWbLabel', '');
        this._set('simWbDescription', '');
        this._set('simWbScopeType', 'entity');
        this._set('simWbFocus', '');
        this._applyFocusLabel('', '');
        this._set('simWbHops', cfg.default_hops != null ? cfg.default_hops : 2);
        this._set('simWbCap', cfg.class_cap || 5000);
        this._applyPhases(null);
        const title = this._el('simWbTitle');
        if (title) title.textContent = '— new scenario';
        this._syncScopeUi();
        this.renderPatches();
    },

    async load(id) {
        try {
            const data = await SimulationPage.api(`/simulation/${encodeURIComponent(id)}`);
            const sim = data.simulation || {};
            const spec = sim.spec || {};
            const scope = spec.scope || {};
            this.currentId = sim.id;
            this._set('simWbLabel', spec.label || sim.label);
            this._set('simWbDescription', spec.description || '');
            this._set('simWbScopeType', scope.type || 'entity');
            this._syncScopeUi();
            if (scope.type === 'cohort') {
                await this._loadCohorts();
                this._set('simWbCohort', scope.focus);
            } else {
                this._set('simWbFocus', scope.focus);
                this._applyFocusLabel(scope.focus_label || '', scope.focus);
                if (scope.type === 'entity' && !scope.focus_label) this._onFocusUriChanged();
            }
            this._set('simWbHops', scope.hops);
            this._set('simWbCap', scope.cap);
            this.patches = (spec.patches || []).map((p) => ({ ...p }));
            this._applyPhases(spec.phases);
            const title = this._el('simWbTitle');
            if (title) title.textContent = `— ${sim.label || sim.id}`;
            this.renderPatches();
        } catch (err) {
            showNotification(`Could not open scenario: ${err.message}`, 'error');
        }
    },

    _applyPhases(phases) {
        document.querySelectorAll('#simWbPhases [data-phase]').forEach((cb) => {
            if (cb.disabled) {
                cb.checked = false;
                return;
            }
            const key = cb.dataset.phase;
            cb.checked = phases ? !!phases[key] : (key === 'swrl' || key === 'graph');
        });
    },

    // -- Patches -----------------------------------------------------------

    addPatch(patch) {
        const focus = this._val('simWbScopeType') === 'entity' ? this._val('simWbFocus') : '';
        this.patches.push(patch || { op: 'set', subject: focus, predicate: '', object: '' });
        this.renderPatches();
    },

    _onPatchEdit(e) {
        const field = e.target.dataset.field;
        const idx = Number(e.target.dataset.idx);
        if (!field || Number.isNaN(idx) || !this.patches[idx]) return;
        this.patches[idx][field] = e.target.value;
    },

    renderPatches() {
        const body = this._el('simWbPatchesBody');
        const count = this._el('simWbPatchCount');
        if (count) count.textContent = String(this.patches.length);
        if (!body) return;
        if (!this.patches.length) {
            body.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-3 small">No hypothetical facts yet — add one to describe what changes.</td></tr>';
            return;
        }
        body.innerHTML = this.patches.map((p, i) => `<tr>
            <td>
                <select class="form-select form-select-sm" data-field="op" data-idx="${i}" aria-label="Operation">
                    <option value="set" ${p.op === 'set' ? 'selected' : ''}>Set</option>
                    <option value="add" ${p.op === 'add' ? 'selected' : ''}>Add</option>
                    <option value="remove" ${p.op === 'remove' ? 'selected' : ''}>Remove</option>
                </select>
            </td>
            <td>${this._lookupInput('subject', i, p.subject, 'Subject URI')}</td>
            <td>${this._lookupInput('predicate', i, p.predicate, 'Predicate URI')}</td>
            <td>${this._lookupInput('object', i, p.object, 'Object URI or literal')}</td>
            <td class="text-end"><button type="button" class="btn btn-sm btn-outline-danger" data-remove="${i}" title="Remove fact"><i class="bi bi-x-lg"></i></button></td>
        </tr>`).join('');
    },

    _lookupInput(field, idx, value, placeholder) {
        const esc = (v) => SimulationPage.esc(v);
        return `<div class="input-group input-group-sm">
            <input type="text" class="form-control" data-field="${field}" data-idx="${idx}"
                   value="${esc(value)}" placeholder="${esc(placeholder)}" aria-label="${esc(placeholder)}">
            <button type="button" class="btn btn-outline-secondary" data-lookup="${field}" data-idx="${idx}"
                    title="Find existing ${esc(field)}"><i class="bi bi-search"></i></button>
        </div>`;
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

    applyFactFromWizard(patch, idx) {
        const row = {
            op: patch.op || 'set',
            subject: patch.subject || '',
            predicate: patch.predicate || '',
            object: patch.object || '',
        };
        if (idx == null || !this.patches[idx]) this.patches.push(row);
        else this.patches[idx] = row;
        this.renderPatches();
    },

    // -- Save / run --------------------------------------------------------

    collect() {
        const type = this._val('simWbScopeType') || 'entity';
        const phases = {};
        document.querySelectorAll('#simWbPhases [data-phase]').forEach((cb) => {
            phases[cb.dataset.phase] = cb.checked;
        });
        return {
            label: this._val('simWbLabel'),
            description: this._val('simWbDescription'),
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
        };
    },

    _validate(body) {
        if (!body.label) return 'Give the scenario a name';
        if (!body.scope.focus) return 'Pick the scope focus (entity, cohort or class)';
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
            const data = this.currentId
                ? await SimulationPage.api(`/simulation/${encodeURIComponent(this.currentId)}`, { method: 'PUT', body })
                : await SimulationPage.api('/simulation/create', { method: 'POST', body });
            const sim = data.simulation || {};
            this.currentId = sim.id;
            const title = this._el('simWbTitle');
            if (title) title.textContent = `— ${sim.label || sim.id}`;
            if (!quiet) showNotification('Scenario saved', 'success');
            SimulationPage.loadScenarios();
            return sim.id;
        } catch (err) {
            showNotification(`Save failed: ${err.message}`, 'error');
            return null;
        }
    },

    async run() {
        const id = await this.save({ quiet: true });
        if (id) SimulationPage.startRun(id);
    },
};

window.SimulationWorkbench = SimulationWorkbench;
document.addEventListener('DOMContentLoaded', () => SimulationWorkbench.init());
