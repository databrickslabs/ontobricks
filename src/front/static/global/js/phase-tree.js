/**
 * Tri-state tree for inference phases and their rules
 * (markup: templates/partials/layout/_phase_tree.html).
 *
 * Phase checkboxes carry `data-phase`; rule checkboxes carry
 * `data-tree-rule="<phase>"` + `data-rule-name`. A phase is "on" when it is
 * checked or indeterminate.
 */
class PhaseTree {
    static RULE_TYPES = ['if_then', 'sparql_rules'];

    constructor(root) {
        this.root = root;
        this.root.addEventListener('change', (e) => this._onChange(e.target));
        this.root.addEventListener('click', (e) => this._onClick(e));
    }

    renderRules(type, rules) {
        const list = this.root.querySelector(`[data-tree-children="${type}"]`);
        const twisty = this.root.querySelector(`[data-tree-toggle="${type}"]`);
        if (!list) return;
        const leafIcon = list.dataset.leafIcon || 'bi-dot';
        const phaseOn = this._isOn(type);
        list.innerHTML = (rules || []).map((rule, i) => {
            const name = rule.name || `Rule ${i + 1}`;
            const id = `${this.root.id}_${type}_${i}`;
            const off = rule.enabled === false
                ? '<span class="badge text-bg-light border ob-tree-badge">off</span>' : '';
            return `<li class="ob-tree-node" role="treeitem">
                <div class="ob-tree-row">
                    <input type="checkbox" class="form-check-input" id="${PhaseTree.esc(id)}"
                           data-tree-rule="${PhaseTree.esc(type)}" data-rule-name="${PhaseTree.esc(name)}"
                           ${phaseOn ? 'checked' : ''}>
                    <label class="ob-tree-label" for="${PhaseTree.esc(id)}"
                           title="${PhaseTree.esc(rule.description || name)}">
                        <i class="bi ${leafIcon} ob-tree-icon"></i>
                        <span class="ob-tree-title">${PhaseTree.esc(name)}</span>
                    </label>
                    ${off}
                </div>
            </li>`;
        }).join('');
        if (twisty) twisty.disabled = !list.children.length;
        this.sync();
    }

    /** `{phase: bool}` for every phase in the tree. */
    phases() {
        const out = {};
        this._phaseInputs().forEach((cb) => {
            out[cb.dataset.phase] = !!(cb.checked || cb.indeterminate);
        });
        return out;
    }

    /** `{phase: [rule names]}` for enabled phases with at least one rule ticked. */
    ruleNames(phases = this.phases()) {
        const out = {};
        PhaseTree.RULE_TYPES.forEach((type) => {
            if (!phases[type]) return;
            const names = this._rules(type).filter((cb) => cb.checked)
                .map((cb) => cb.dataset.ruleName).filter(Boolean);
            if (names.length) out[type] = names;
        });
        return out;
    }

    /** Tick rules from a saved selection; an empty selection means every rule. */
    applyRules(ruleNames) {
        PhaseTree.RULE_TYPES.forEach((type) => {
            const selected = ruleNames && ruleNames[type];
            const phaseOn = this._isOn(type);
            this._rules(type).forEach((cb) => {
                if (!phaseOn) cb.checked = false;
                else if (!selected || !selected.length) cb.checked = true;
                else cb.checked = selected.includes(cb.dataset.ruleName);
            });
            this._syncPhase(type);
        });
        this.sync();
    }

    setAll(checked) {
        this._phaseInputs().forEach((cb) => {
            if (cb.disabled) return;
            cb.checked = checked;
            cb.indeterminate = false;
            this._cascade(cb.dataset.phase, checked);
        });
        this.sync();
    }

    expand(type, open) {
        const node = this.root.querySelector(`[data-tree-node="${type}"]`);
        const list = this.root.querySelector(`[data-tree-children="${type}"]`);
        if (!node || !list || !list.children.length) return;
        const show = open === undefined ? list.hidden : open;
        list.hidden = !show;
        node.setAttribute('aria-expanded', String(show));
    }

    setBusy(busy) {
        this.root.classList.toggle('is-busy', busy);
        this.root.querySelectorAll('input, button').forEach((el) => {
            if (el.hasAttribute('data-tree-locked')) return;
            if (el.matches('[data-tree-toggle]')) {
                const list = this.root.querySelector(`[data-tree-children="${el.dataset.treeToggle}"]`);
                el.disabled = busy || !(list && list.children.length);
                return;
            }
            el.disabled = busy;
        });
    }

    /** Repaint "on" accents and the selected/total badges. */
    sync() {
        this._phaseInputs().forEach((cb) => {
            const type = cb.dataset.phase;
            const node = cb.closest('[data-tree-node]');
            if (node) node.classList.toggle('is-on', !!(cb.checked || cb.indeterminate));
            const badge = this.root.querySelector(`[data-tree-count="${type}"]`);
            const rules = this._rules(type);
            if (badge && rules.length) {
                const n = rules.filter((r) => r.checked).length;
                badge.textContent = `${n}/${rules.length} rule${rules.length !== 1 ? 's' : ''}`;
            }
        });
    }

    _onChange(target) {
        if (target.matches('[data-phase]')) {
            target.indeterminate = false;
            this._cascade(target.dataset.phase, target.checked);
        } else if (target.matches('[data-tree-rule]')) {
            this._syncPhase(target.dataset.treeRule);
        } else {
            return;
        }
        this.sync();
    }

    _onClick(e) {
        const twisty = e.target.closest('[data-tree-toggle]');
        if (twisty) {
            this.expand(twisty.dataset.treeToggle);
            return;
        }
        const action = e.target.closest('[data-tree-action]');
        if (!action) return;
        const kind = action.dataset.treeAction;
        if (kind === 'all' || kind === 'none') {
            this.setAll(kind === 'all');
        } else {
            PhaseTree.RULE_TYPES.forEach((type) => this.expand(type, kind === 'expand'));
        }
    }

    _cascade(type, checked) {
        this._rules(type).forEach((cb) => { cb.checked = checked; });
    }

    _syncPhase(type) {
        const phase = this._phaseInput(type);
        const rules = this._rules(type);
        if (!phase || !rules.length || phase.disabled) return;
        const n = rules.filter((cb) => cb.checked).length;
        phase.checked = n === rules.length;
        phase.indeterminate = n > 0 && n < rules.length;
    }

    _isOn(type) {
        const cb = this._phaseInput(type);
        return !!(cb && (cb.checked || cb.indeterminate));
    }

    _phaseInput(type) {
        return this.root.querySelector(`[data-phase="${type}"]`);
    }

    _phaseInputs() {
        return [...this.root.querySelectorAll('[data-phase]')];
    }

    _rules(type) {
        return [...this.root.querySelectorAll(`[data-tree-rule="${type}"]`)];
    }

    static esc(value) {
        return String(value ?? '')
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }
}

window.PhaseTree = PhaseTree;
