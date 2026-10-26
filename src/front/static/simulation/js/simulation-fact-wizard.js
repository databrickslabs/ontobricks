/**
 * Three-step picker for a hypothetical fact: subject from the live graph,
 * predicate from the ontology, object from entities or a typed literal.
 */
const SimulationFactWizard = {
    _idx: null,
    _step: 1,
    _ontology: { classes: [], properties: [] },
    _subjectType: '',
    _subjectTypeUri: '',
    _pickedSubjectUri: '',
    _pickedLabel: '',
    _pickMode: '',
    _predIsObject: false,
    _predRange: '',
    _busy: false,

    init() {
        const on = (id, evt, fn) => {
            const el = document.getElementById(id);
            if (el) el.addEventListener(evt, fn);
        };
        on('simFactSearchBtn', 'click', () => this._searchSubjects());
        on('simFactObjectSearchBtn', 'click', () => this._searchObjects());
        on('simFactSearch', 'keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); this._searchSubjects(); } });
        on('simFactObjectSearch', 'keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); this._searchObjects(); } });
        on('simFactPredFilter', 'input', () => this._renderPredicates());
        on('simFactSubjectUri', 'input', () => {
            if (this._val('simFactSubjectUri') !== this._pickedSubjectUri) {
                this._subjectType = '';
                this._subjectTypeUri = '';
            }
        });
        on('simFactBackBtn', 'click', () => this._go(this._step - 1));
        on('simFactNextBtn', 'click', () => this._go(this._step + 1));
        on('simFactApplyBtn', 'click', () => this._apply());
        on('simFactOp', 'change', () => this._syncOpUi());
        document.getElementById('simFactWizardTabs')?.addEventListener('click', (e) => {
            const tab = e.target.closest('[data-step]');
            if (tab) this._go(Number(tab.dataset.step));
        });
        document.getElementById('simFactSubjectHits')?.addEventListener('click', (e) => {
            const btn = e.target.closest('[data-uri]');
            if (!btn) return;
            this._pickSubject(
                btn.dataset.uri,
                btn.dataset.type || '',
                btn.dataset.typeUri || '',
                btn.dataset.label || '',
                btn,
            );
        });
        document.getElementById('simFactSubjectHits')?.addEventListener('dblclick', (e) => {
            const btn = e.target.closest('[data-uri]');
            if (!btn || this._pickMode !== 'scope') return;
            this._pickSubject(
                btn.dataset.uri,
                btn.dataset.type || '',
                btn.dataset.typeUri || '',
                btn.dataset.label || '',
                btn,
            );
            this._apply();
        });
        document.getElementById('simFactPredHits')?.addEventListener('click', (e) => {
            const btn = e.target.closest('[data-uri]');
            if (!btn) return;
            this._pickPredicate(btn.dataset.uri, btn.dataset.kind === 'object', btn.dataset.range || '', btn);
        });
        document.getElementById('simFactObjectHits')?.addEventListener('click', (e) => {
            const btn = e.target.closest('[data-uri]');
            if (!btn) return;
            this._set('simFactObjectValue', btn.dataset.uri);
            this._markActive('simFactObjectHits', btn);
        });
    },

    async open({ idx = null, startField = 'subject', patch = null, pick = '' } = {}) {
        this._idx = idx;
        this._pickMode = pick === 'scope' ? 'scope' : '';
        this._subjectType = '';
        this._subjectTypeUri = '';
        this._pickedSubjectUri = '';
        this._pickedLabel = '';
        this._predIsObject = false;
        this._predRange = '';
        await this._ensureOntology();
        const p = patch || {};
        this._set('simFactOp', p.op || 'set');
        this._set('simFactSubjectUri', p.subject || '');
        this._set('simFactPredicateUri', p.predicate || '');
        this._set('simFactObjectValue', p.object || '');
        this._set('simFactSearch', p.subject ? this._local(p.subject) : '');
        this._set('simFactObjectSearch', '');
        this._set('simFactPredFilter', '');
        const subjectHits = this._el('simFactSubjectHits');
        const objectHits = this._el('simFactObjectHits');
        if (subjectHits) subjectHits.innerHTML = '';
        if (objectHits) objectHits.innerHTML = '';
        this._fillClassSelect();
        this._syncPickMode();
        const start = this._pickMode === 'scope'
            ? 1
            : ({ subject: 1, predicate: 2, object: 3 }[startField] || 1);
        await this._go(start);
        bootstrap.Modal.getOrCreateInstance(this._el('simFactWizardModal')).show();
    },

    _el(id) { return document.getElementById(id); },

    _set(id, value) {
        const el = this._el(id);
        if (el) el.value = value == null ? '' : value;
    },

    _val(id) {
        const el = this._el(id);
        return el ? String(el.value || '').trim() : '';
    },

    async _ensureOntology() {
        if (this._ontology.classes.length || this._ontology.properties.length) return;
        try {
            const data = await SimulationPage.api('/ontology/get-loaded-ontology');
            const ont = data.ontology || {};
            this._ontology = {
                classes: ont.classes || [],
                properties: ont.properties || [],
            };
        } catch (_) {
            this._ontology = { classes: [], properties: [] };
        }
    },

    _fillClassSelect() {
        const sel = this._el('simFactClass');
        if (!sel) return;
        const current = sel.value;
        const classes = this._ontology.classes.slice().sort((a, b) =>
            String(a.label || a.name || '').localeCompare(String(b.label || b.name || ''))
        );
        sel.innerHTML = '<option value="">Any class</option>' + classes.map((c) => {
            const uri = this._assetUri(c);
            if (!uri) return '';
            return `<option value="${SimulationPage.esc(uri)}">${SimulationPage.esc(c.label || c.name || uri)}</option>`;
        }).join('');
        if (current) sel.value = current;
    },

    _assetUri(item) {
        if (!item) return '';
        return String(item.uri || item.iri || '').trim();
    },

    _local(uri) {
        const s = String(uri || '');
        const i = Math.max(s.lastIndexOf('#'), s.lastIndexOf('/'));
        return i >= 0 ? s.slice(i + 1) : s;
    },

    _isObjectProp(p) {
        const type = String(p.type || '');
        if (type) return type === 'ObjectProperty' || type === 'owl:ObjectProperty';
        const range = String(p.range || '').toLowerCase();
        if (!range || range.startsWith('xsd:') || range.includes('http://www.w3.org/2001/xmlschema')) {
            return false;
        }
        return this._ontology.classes.some((c) => {
            const name = String(c.name || '').toLowerCase();
            const uri = this._assetUri(c).toLowerCase();
            return range === name || range === uri || range === this._local(uri).toLowerCase();
        });
    },

    _classKeys(cls) {
        const keys = new Set();
        if (!cls) return keys;
        [cls.name, cls.label, this._assetUri(cls)].forEach((v) => {
            const s = String(v || '').trim();
            if (!s) return;
            keys.add(s.toLowerCase());
            keys.add(this._local(s).toLowerCase());
        });
        return keys;
    },

    _findClass(type, typeUri) {
        const typeKey = String(type || '').toLowerCase();
        const uriKey = String(typeUri || '').toLowerCase();
        return (this._ontology.classes || []).find((c) => {
            const keys = this._classKeys(c);
            return (typeKey && keys.has(typeKey)) || (uriKey && keys.has(uriKey));
        }) || null;
    },

    _declaringKeys() {
        const keys = new Set();
        const start = this._findClass(this._subjectType, this._subjectTypeUri);
        const addClass = (cls) => {
            this._classKeys(cls).forEach((k) => keys.add(k));
        };
        if (start) {
            addClass(start);
            const seen = new Set([String(start.name || '')]);
            let parent = start.parent || '';
            while (parent && !seen.has(parent)) {
                seen.add(parent);
                const ancestor = this._findClass(parent, parent);
                if (!ancestor) {
                    keys.add(parent.toLowerCase());
                    keys.add(this._local(parent).toLowerCase());
                    break;
                }
                addClass(ancestor);
                parent = ancestor.parent || '';
            }
            return keys;
        }
        [this._subjectType, this._subjectTypeUri].forEach((v) => {
            const s = String(v || '').trim();
            if (!s) return;
            keys.add(s.toLowerCase());
            keys.add(this._local(s).toLowerCase());
        });
        return keys;
    },

    _domainMatches(p) {
        const keys = this._declaringKeys();
        if (!keys.size) return false;
        const domain = String(p.domain || '').trim();
        if (!domain) return false;
        return keys.has(domain.toLowerCase()) || keys.has(this._local(domain).toLowerCase());
    },

    async _ensureSubjectType() {
        if (this._subjectType || this._subjectTypeUri) return;
        const classUri = this._val('simFactClass');
        if (classUri) {
            const cls = this._findClass('', classUri);
            this._subjectType = cls ? (cls.name || this._local(classUri)) : this._local(classUri);
            this._subjectTypeUri = cls ? (this._assetUri(cls) || classUri) : classUri;
            return;
        }
        const uri = this._val('simFactSubjectUri');
        if (!uri) return;
        try {
            const data = await SimulationPage.api('/dtwin/sync/filter', {
                method: 'POST',
                body: {
                    phase: 'preview',
                    field: 'id',
                    match_type: 'exact',
                    value: uri,
                    include_inferred: true,
                },
            });
            const seed = (data.seeds || []).find((s) => s.uri === uri) || (data.seeds || [])[0];
            if (seed) {
                this._subjectType = seed.type || '';
                this._subjectTypeUri = seed.type_uri || '';
            }
        } catch (_) {
            /* list stays empty until a typed subject is resolved */
        }
    },

    _allPredicates() {
        const seen = new Set();
        const out = [];
        const push = (uri, label, kind, range, domain) => {
            if (!uri || seen.has(uri)) return;
            seen.add(uri);
            out.push({ uri, label, kind, range, domain });
        };
        (this._ontology.properties || []).forEach((p) => {
            push(
                this._assetUri(p),
                p.label || p.name || this._local(this._assetUri(p)),
                this._isObjectProp(p) ? 'object' : 'data',
                p.range || '',
                p.domain || '',
            );
        });
        (this._ontology.classes || []).forEach((c) => {
            const domain = c.name || this._assetUri(c);
            const attrs = [].concat(c.attributes || [], c.dataProperties || []);
            attrs.forEach((a) => {
                const uri = this._assetUri(a) || a.name;
                push(uri, a.label || a.name || this._local(uri), 'data', a.range || a.datatype || '', domain);
            });
        });
        out.sort((a, b) => String(a.label).localeCompare(String(b.label)));
        return out;
    },

    _renderPredicates() {
        const list = this._el('simFactPredHits');
        if (!list) return;
        const needle = this._val('simFactPredFilter').toLowerCase();
        const selected = this._val('simFactPredicateUri');
        const rows = this._allPredicates().filter((p) => {
            if (!this._domainMatches(p)) return false;
            if (!needle) return true;
            return `${p.label} ${p.uri}`.toLowerCase().includes(needle);
        });
        if (!rows.length) {
            const msg = this._declaringKeys().size
                ? 'No ontology properties declared on this subject\'s class.'
                : 'Pick a subject from search so we can list properties of its class.';
            list.innerHTML = `<div class="list-group-item small text-muted">${msg}</div>`;
            return;
        }
        list.innerHTML = rows.map((p) => {
            const active = p.uri === selected ? ' active' : '';
            const kind = p.kind === 'object' ? 'relationship' : 'attribute';
            return `<button type="button" class="list-group-item list-group-item-action small${active}"
                data-uri="${SimulationPage.esc(p.uri)}" data-kind="${p.kind}" data-range="${SimulationPage.esc(p.range)}">
                <strong>${SimulationPage.esc(p.label)}</strong>
                <span class="badge bg-secondary bg-opacity-25 text-dark ms-1">${kind}</span>
                <div class="text-muted text-break">${SimulationPage.esc(p.uri)}</div>
            </button>`;
        }).join('');
    },

    _pickSubject(uri, type, typeUri, label, btn) {
        this._set('simFactSubjectUri', uri);
        this._pickedSubjectUri = uri;
        this._pickedLabel = label || '';
        this._subjectType = type;
        this._subjectTypeUri = typeUri;
        this._markActive('simFactSubjectHits', btn);
        this._renderPredicates();
    },

    _pickPredicate(uri, isObject, range, btn) {
        this._set('simFactPredicateUri', uri);
        this._predIsObject = !!isObject;
        this._predRange = range || '';
        this._markActive('simFactPredHits', btn);
        this._syncObjectHint();
    },

    _markActive(listId, btn) {
        const list = this._el(listId);
        if (!list) return;
        list.querySelectorAll('.list-group-item').forEach((el) => el.classList.toggle('active', el === btn));
    },

    _op() {
        return this._val('simFactOp') || 'set';
    },

    _opCopy() {
        const range = this._predRange ? this._local(this._predRange) : '';
        const rel = this._predIsObject
            ? (range
                ? `This predicate links to ${range}. `
                : 'This predicate is a relationship. ')
            : 'This predicate holds a literal. ';
        const catalog = {
            set: {
                intro: 'Set replaces every current value of a predicate on the subject. Pick the subject, lock the operation, then choose the predicate and the new value.',
                pred: 'Choose a predicate declared on this subject\'s class (including inherited).',
                object: rel + 'Enter the new value. Every existing object of this predicate on the subject is cleared first.',
                objectLabel: 'New value',
                objectPlaceholder: 'Replacement URI or literal',
                apply: 'Apply set',
            },
            add: {
                intro: 'Add asserts an extra triple. Existing values of the predicate are kept. Pick the subject, lock the operation, then choose the predicate and the value to add.',
                pred: 'Choose a predicate declared on this subject\'s class for the extra triple.',
                object: rel + 'Enter the additional value. Other objects of this predicate stay in place.',
                objectLabel: 'Value to add',
                objectPlaceholder: 'New URI or literal',
                apply: 'Apply add',
            },
            remove: {
                intro: 'Remove retracts one exact triple. Pick the subject, lock the operation, then choose the predicate and the object to delete.',
                pred: 'Choose a predicate declared on this subject\'s class to retract.',
                object: rel + 'Pick or type the object to retract. Leave empty only when the triple has an empty object.',
                objectLabel: 'Value to retract',
                objectPlaceholder: 'Object of the triple to remove',
                apply: 'Apply remove',
            },
        };
        return catalog[this._op()] || catalog.set;
    },

    _syncOpUi() {
        if (this._pickMode === 'scope') {
            this._syncPickMode();
            return;
        }
        const copy = this._opCopy();
        const intro = this._el('simFactIntro');
        const predHint = this._el('simFactPredHint');
        const objHint = this._el('simFactObjectHint');
        const objLabel = this._el('simFactObjectValueLabel');
        const objInput = this._el('simFactObjectValue');
        const apply = this._el('simFactApplyBtn');
        const op = this._el('simFactOp');
        if (intro) intro.textContent = copy.intro;
        if (predHint) predHint.textContent = copy.pred;
        if (objHint) objHint.textContent = copy.object;
        if (objLabel) objLabel.textContent = copy.objectLabel;
        if (objInput) objInput.placeholder = copy.objectPlaceholder;
        if (apply) {
            apply.innerHTML = `<i class="bi bi-check2 me-1"></i>${SimulationPage.esc(copy.apply)}`;
        }
        if (op) op.disabled = this._step !== 1;
        this._el('simFactObjectSearchWrap')?.classList.remove('d-none');
    },

    _syncObjectHint() {
        this._syncOpUi();
    },

    async _go(step) {
        if (this._pickMode === 'scope') {
            this._step = 1;
            this._el('simFactStepSubject')?.classList.remove('d-none');
            this._el('simFactStepPredicate')?.classList.add('d-none');
            this._el('simFactStepObject')?.classList.add('d-none');
            this._syncPickMode();
            return;
        }
        const next = Math.min(3, Math.max(1, Number(step) || 1));
        if (next > this._step) {
            if (next >= 2 && !this._val('simFactSubjectUri')) {
                showNotification('Pick or type a subject URI first.', 'warning');
                return;
            }
            if (next >= 3 && !this._val('simFactPredicateUri')) {
                showNotification('Pick or type a predicate URI first.', 'warning');
                return;
            }
        }
        if (next >= 2) await this._ensureSubjectType();
        this._step = next;
        this._el('simFactStepSubject')?.classList.toggle('d-none', this._step !== 1);
        this._el('simFactStepPredicate')?.classList.toggle('d-none', this._step !== 2);
        this._el('simFactStepObject')?.classList.toggle('d-none', this._step !== 3);
        [1, 2, 3].forEach((n) => {
            const tab = this._el(['simFactTabSubject', 'simFactTabPredicate', 'simFactTabObject'][n - 1]);
            if (tab) tab.classList.toggle('active', n === this._step);
        });
        const back = this._el('simFactBackBtn');
        if (back) back.disabled = this._step === 1;
        this._el('simFactNextBtn')?.classList.toggle('d-none', this._step === 3);
        this._el('simFactApplyBtn')?.classList.toggle('d-none', this._step !== 3);
        this._syncOpUi();
        if (this._step === 2) this._renderPredicates();
    },

    _setBusy(id, busy, idleHtml, label) {
        const btn = this._el(id);
        if (!btn) return;
        if (busy) {
            if (btn.dataset.idleHtml == null) btn.dataset.idleHtml = btn.innerHTML;
            btn.disabled = true;
            btn.setAttribute('aria-busy', 'true');
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>'
                + SimulationPage.esc(label);
            return;
        }
        btn.removeAttribute('aria-busy');
        btn.innerHTML = btn.dataset.idleHtml || idleHtml;
        delete btn.dataset.idleHtml;
        btn.disabled = false;
    },

    async _searchSubjects() {
        await this._runSearch({
            btnId: 'simFactSearchBtn',
            idle: '<i class="bi bi-search me-1"></i>Search',
            listId: 'simFactSubjectHits',
            value: this._val('simFactSearch'),
            entityType: this._val('simFactClass'),
            onPickAttr: true,
        });
    },

    async _searchObjects() {
        const rangeUri = this._resolveRangeUri(this._predRange);
        await this._runSearch({
            btnId: 'simFactObjectSearchBtn',
            idle: '<i class="bi bi-search me-1"></i>Search',
            listId: 'simFactObjectHits',
            value: this._val('simFactObjectSearch'),
            entityType: rangeUri,
        });
    },

    _resolveRangeUri(range) {
        if (!range) return '';
        const hit = this._ontology.classes.find((c) => {
            const uri = this._assetUri(c);
            const name = String(c.name || '');
            return uri === range || name === range || this._local(uri) === range;
        });
        return hit ? this._assetUri(hit) : range;
    },

    async _runSearch({ btnId, idle, listId, value, entityType }) {
        if (this._busy) return;
        if (!value && !entityType) {
            showNotification('Enter a search term or pick a class.', 'warning');
            return;
        }
        const list = this._el(listId);
        if (!list) return;
        this._busy = true;
        this._setBusy(btnId, true, idle, 'Searching…');
        try {
            const data = await SimulationPage.api('/dtwin/sync/filter', {
                method: 'POST',
                body: {
                    phase: 'preview',
                    entity_type: entityType || '',
                    field: 'any',
                    match_type: 'contains',
                    value: value || '',
                    include_inferred: true,
                },
            });
            const seeds = data.seeds || [];
            if (!seeds.length) {
                list.innerHTML = '<div class="list-group-item small text-muted">No matching entities in the live graph.</div>';
                return;
            }
            const cap = data.capped ? ` <span class="text-muted">(first ${seeds.length})</span>` : '';
            list.innerHTML = `<div class="list-group-item small text-muted">${seeds.length} matches${cap}</div>`
                + seeds.map((s) => `<button type="button" class="list-group-item list-group-item-action small"
                    data-uri="${SimulationPage.esc(s.uri)}"
                    data-type="${SimulationPage.esc(s.type || '')}"
                    data-type-uri="${SimulationPage.esc(s.type_uri || '')}"
                    data-label="${SimulationPage.esc(s.label || this._local(s.uri))}">
                    <strong>${SimulationPage.esc(s.label || this._local(s.uri))}</strong>
                    <span class="badge bg-secondary bg-opacity-25 text-dark ms-1">${SimulationPage.esc(s.type || 'Unknown')}</span>
                    <div class="text-muted text-break">${SimulationPage.esc(s.uri)}</div>
                </button>`).join('');
        } catch (err) {
            list.innerHTML = `<div class="list-group-item small text-danger">${SimulationPage.esc(err.message)}</div>`;
        } finally {
            this._busy = false;
            this._setBusy(btnId, false, idle);
        }
    },

    _syncPickMode() {
        const pick = this._pickMode === 'scope';
        this._el('simFactOpWrap')?.classList.toggle('d-none', pick);
        this._el('simFactWizardTabs')?.classList.toggle('d-none', pick);
        const title = this._el('simFactWizardTitle');
        if (title) {
            title.innerHTML = pick
                ? '<i class="bi bi-search me-2"></i>Choose neighbourhood entity'
                : '<i class="bi bi-magic me-2"></i>Hypothetical fact';
        }
        const intro = this._el('simFactIntro');
        if (intro && pick) {
            intro.textContent = 'Search the live graph for the entity whose neighbourhood becomes the simulation slice.';
        }
        const apply = this._el('simFactApplyBtn');
        if (apply && pick) {
            apply.innerHTML = '<i class="bi bi-check2 me-1"></i>Use this entity';
        }
        this._el('simFactNextBtn')?.classList.toggle('d-none', pick);
        this._el('simFactBackBtn')?.classList.toggle('d-none', pick);
        this._el('simFactApplyBtn')?.classList.toggle('d-none', !pick && this._step !== 3);
    },

    _apply() {
        const subject = this._val('simFactSubjectUri');
        if (this._pickMode === 'scope') {
            if (!subject) {
                showNotification('Pick or type a subject URI first.', 'warning');
                return;
            }
            if (window.SimulationWorkbench) {
                SimulationWorkbench.applyScopeEntity(
                    subject,
                    this._pickedLabel || this._local(subject),
                );
            }
            bootstrap.Modal.getInstance(this._el('simFactWizardModal'))?.hide();
            return;
        }
        const op = this._val('simFactOp') || 'set';
        const predicate = this._val('simFactPredicateUri');
        const object = this._el('simFactObjectValue') ? this._el('simFactObjectValue').value : '';
        if (!subject || !predicate) {
            showNotification('Subject and predicate are required.', 'warning');
            return;
        }
        if (op !== 'remove' && object === '') {
            showNotification('This operation needs an object value.', 'warning');
            return;
        }
        if (window.SimulationWorkbench) {
            SimulationWorkbench.applyFactFromWizard(
                { op, subject, predicate, object },
                this._idx,
            );
        }
        bootstrap.Modal.getInstance(this._el('simFactWizardModal'))?.hide();
    },
};

window.SimulationFactWizard = SimulationFactWizard;
document.addEventListener('DOMContentLoaded', () => SimulationFactWizard.init());
