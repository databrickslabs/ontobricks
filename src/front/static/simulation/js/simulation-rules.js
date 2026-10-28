/**
 * Per-rule picker for the simulation workbench — same lists as
 * Knowledge Graph → Inference (SWRL, decision tables, SPARQL, aggregates).
 */
const SimulationRules = {
    TYPES: ['swrl', 'decision_tables', 'sparql_rules', 'aggregate_rules'],
    _cache: {},
    _loaded: false,
    _pending: null,

    bind() {
        if (this._bound) return;
        const root = document.getElementById('simWbPhases');
        if (!root) return;
        this._bound = true;
        root.addEventListener('change', (e) => {
            const phase = e.target.closest('[data-phase]');
            if (phase && !e.target.classList.contains('sim-rule-cb')) {
                this._onPhaseToggle(phase);
                return;
            }
            const rule = e.target.closest('.sim-rule-cb');
            if (rule) this._onRuleToggle(rule.dataset.simRuleType);
        });
        root.addEventListener('click', (e) => {
            const btn = e.target.closest('[data-sim-expand]');
            if (!btn) return;
            e.preventDefault();
            e.stopPropagation();
            this._toggleList(btn.dataset.simExpand);
        });
        const all = document.getElementById('simWbRulesAll');
        const none = document.getElementById('simWbRulesNone');
        if (all) all.addEventListener('click', () => this.toggleAll(true));
        if (none) none.addEventListener('click', () => this.toggleAll(false));
        this.load();
    },

    async load() {
        if (this._loaded) {
            this.apply(this._pending);
            return;
        }
        try {
            const [swrl, tables, sparql, aggregate] = await Promise.all([
                SimulationPage.api('/ontology/swrl/list'),
                SimulationPage.api('/ontology/rules/decision_tables/list'),
                SimulationPage.api('/ontology/rules/sparql_rules/list'),
                SimulationPage.api('/ontology/rules/aggregate_rules/list'),
            ]);
            this._cache = {
                swrl: swrl.rules || [],
                decision_tables: tables.rules || [],
                sparql_rules: sparql.rules || [],
                aggregate_rules: aggregate.rules || [],
            };
        } catch (err) {
            console.error('[Simulation] Could not load reasoning rules:', err);
            this._cache = { swrl: [], decision_tables: [], sparql_rules: [], aggregate_rules: [] };
        }
        this._loaded = true;
        this._render();
        this.apply(this._pending);
    },

    apply(ruleNames) {
        this._pending = ruleNames || null;
        if (!this._loaded) return;
        this.TYPES.forEach((type) => {
            const selected = this._pending && this._pending[type];
            const phaseOn = this._phaseOn(type);
            document.querySelectorAll(`.sim-rule-cb[data-sim-rule-type="${type}"]`).forEach((cb) => {
                const name = cb.dataset.simRuleName || '';
                if (!phaseOn) cb.checked = false;
                else if (!selected || !selected.length) cb.checked = true;
                else cb.checked = selected.includes(name);
            });
            this._syncPhaseFromRules(type);
        });
        this._syncTiles();
    },

    collectPhases() {
        const phases = {};
        document.querySelectorAll('#simWbPhases [data-phase]').forEach((cb) => {
            phases[cb.dataset.phase] = !!(cb.checked || cb.indeterminate);
        });
        return phases;
    },

    collectRuleNames(phases) {
        const out = {};
        this.TYPES.forEach((type) => {
            if (!phases[type]) return;
            const names = this._selectedNames(type);
            if (names.length) out[type] = names;
        });
        return out;
    },

    toggleAll(checked) {
        document.querySelectorAll('#simWbPhases [data-phase]').forEach((cb) => {
            if (cb.disabled) return;
            cb.checked = checked;
            cb.indeterminate = false;
            this._cascade(cb.dataset.phase, checked);
        });
        this._syncTiles();
    },

    _render() {
        this.TYPES.forEach((type) => {
            const rules = this._cache[type] || [];
            const list = document.querySelector(`[data-sim-rulelist="${type}"]`);
            const toggle = document.querySelector(`[data-sim-expand="${type}"]`);
            if (!list || !rules.length) return;
            if (toggle) {
                const count = toggle.querySelector('.dq-rules-count');
                if (count) count.textContent = `${rules.length}`;
                toggle.classList.remove('d-none');
            }
            const esc = (v) => SimulationPage.esc(v);
            list.innerHTML = rules.map((rule, i) => {
                const name = rule.name || `Rule ${i + 1}`;
                const id = `simRule_${type}_${i}`;
                return `<div class="dq-rule-item">
                    <input type="checkbox" class="form-check-input sim-rule-cb" id="${esc(id)}"
                           data-sim-rule-name="${esc(name)}" data-sim-rule-type="${esc(type)}" checked>
                    <label class="form-check-label small" for="${esc(id)}" title="${esc(name)}">${esc(name)}</label>
                </div>`;
            }).join('');
        });
    },

    _toggleList(type) {
        const list = document.querySelector(`[data-sim-rulelist="${type}"]`);
        const btn = document.querySelector(`[data-sim-expand="${type}"]`);
        if (!list) return;
        const show = list.classList.contains('d-none');
        list.classList.toggle('d-none', !show);
        const icon = btn && btn.querySelector('.dq-expand-chevron');
        if (icon) icon.classList.toggle('dq-chevron-open', show);
    },

    _onPhaseToggle(checkbox) {
        checkbox.indeterminate = false;
        this._cascade(checkbox.dataset.phase, checkbox.checked);
        this._syncTiles();
    },

    _onRuleToggle(type) {
        this._syncPhaseFromRules(type);
        this._syncTiles();
    },

    _cascade(type, checked) {
        if (!this.TYPES.includes(type)) return;
        document.querySelectorAll(`.sim-rule-cb[data-sim-rule-type="${type}"]`).forEach((cb) => {
            cb.checked = checked;
        });
    },

    _syncPhaseFromRules(type) {
        const phase = document.querySelector(`#simWbPhases [data-phase="${type}"]`);
        if (!phase) return;
        const boxes = [...document.querySelectorAll(`.sim-rule-cb[data-sim-rule-type="${type}"]`)];
        if (!boxes.length) return;
        const n = boxes.filter((cb) => cb.checked).length;
        if (n === boxes.length) {
            phase.checked = true;
            phase.indeterminate = false;
        } else if (n === 0) {
            phase.checked = false;
            phase.indeterminate = false;
        } else {
            phase.checked = false;
            phase.indeterminate = true;
        }
    },

    _syncTiles() {
        document.querySelectorAll('#simWbPhases [data-phase]').forEach((cb) => {
            const tile = cb.closest('.reasoning-tile');
            if (tile) tile.classList.toggle('active', !!(cb.checked || cb.indeterminate));
        });
    },

    _phaseOn(type) {
        const cb = document.querySelector(`#simWbPhases [data-phase="${type}"]`);
        return !!(cb && (cb.checked || cb.indeterminate));
    },

    _selectedNames(type) {
        return [...document.querySelectorAll(`.sim-rule-cb[data-sim-rule-type="${type}"]:checked`)]
            .map((cb) => cb.dataset.simRuleName)
            .filter(Boolean);
    },
};

window.SimulationRules = SimulationRules;
