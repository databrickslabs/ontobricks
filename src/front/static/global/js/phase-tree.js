/**
 * Tri-state tree used by Graph → Inference, Simulation → Reasoning and
 * Graph → Data quality (markup: templates/partials/layout/_phase_tree.html).
 *
 * A node checkbox carries `data-phase="<key>"`. A node that expands carries a
 * child list `data-tree-children="<key>"`, and its rule checkboxes carry
 * `data-tree-rule="<key>"` + `data-rule-value="<value>"`. A node is "on" when
 * it is checked or indeterminate.
 */
class PhaseTree {
    constructor(root) {
        this.root = root;
        this.root.addEventListener('change', (e) => this._onChange(e.target));
        this.root.addEventListener('click', (e) => this._onClick(e));
    }

    /**
     * Rule items for a child list. Each item is
     * `{value, label, title?, badge?: {text, cls?, title?}}`; every field is
     * escaped here, so callers pass plain strings.
     */
    renderRules(key, items) {
        const list = this._children(key);
        if (!list) return;
        const leafIcon = list.dataset.leafIcon || 'bi-dot';
        const phaseOn = this._isOn(key);
        const esc = PhaseTree.esc;
        list.innerHTML = (items || []).map((item, i) => {
            const id = `${this.root.id}_${key}_${i}`;
            const badge = item.badge
                ? `<span class="badge ob-tree-badge ${esc(item.badge.cls || 'text-bg-light border')}"`
                    + ` title="${esc(item.badge.title || '')}">${esc(item.badge.text)}</span>`
                : '';
            return `<li class="ob-tree-node" role="treeitem">
                <div class="ob-tree-row">
                    <input type="checkbox" class="form-check-input" id="${esc(id)}"
                           data-tree-rule="${esc(key)}" data-rule-value="${esc(item.value)}"
                           ${phaseOn ? 'checked' : ''}>
                    <label class="ob-tree-label" for="${esc(id)}" title="${esc(item.title || item.label)}">
                        <i class="bi ${esc(leafIcon)} ob-tree-icon"></i>
                        <span class="ob-tree-title">${item.link
                            ? `<a href="#" class="ob-tree-rule-link" data-rule-link="${esc(key)}" data-rule-value="${esc(item.value)}">${esc(item.label)}</a>`
                            : esc(item.label)}</span>
                    </label>
                    ${badge}
                </div>
            </li>`;
        }).join('');
        const twisty = this.root.querySelector(`[data-tree-toggle="${key}"]`);
        if (twisty) twisty.disabled = !list.children.length;
        this.sync();
    }

    /** Rule items from `{name, description, enabled}` records (IF-THEN, SPARQL). */
    static fromNamedRules(rules) {
        return (rules || []).map((r, i) => {
            const name = r.name || `Rule ${i + 1}`;
            return {
                value: r.name || '',
                label: name,
                title: r.description || name,
                link: true,
                badge: r.enabled === false ? { text: 'off' } : null,
            };
        });
    }

    /** Keys of the nodes that have a rule list. */
    expandableKeys() {
        return [...this.root.querySelectorAll('[data-tree-children]')]
            .map((el) => el.dataset.treeChildren);
    }

    /** `{key: bool}` for every node. */
    phases() {
        const out = {};
        this._phaseInputs().forEach((cb) => {
            out[cb.dataset.phase] = !!(cb.checked || cb.indeterminate);
        });
        return out;
    }

    /** `{key: [values]}` for on nodes with at least one rule ticked. */
    selectedRules(phases = this.phases()) {
        const out = {};
        this.expandableKeys().forEach((key) => {
            if (!phases[key]) return;
            const values = this._checkedValues(key);
            if (values.length) out[key] = values;
        });
        return out;
    }

    /** Every ticked rule value, whatever its node is doing. */
    checkedValues() {
        return [...this.root.querySelectorAll('[data-tree-rule]:checked')]
            .map((cb) => cb.dataset.ruleValue)
            .filter(Boolean);
    }

    /** Tick rules from a saved `{key: [values]}`; an empty selection means every rule. */
    applyRules(selection) {
        this.expandableKeys().forEach((key) => {
            const picked = selection && selection[key];
            const phaseOn = this._isOn(key);
            this._rules(key).forEach((cb) => {
                if (!phaseOn) cb.checked = false;
                else if (!picked || !picked.length) cb.checked = true;
                else cb.checked = picked.includes(cb.dataset.ruleValue);
            });
            this._syncPhase(key);
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

    expand(key, open) {
        const node = this.root.querySelector(`[data-tree-node="${key}"]`);
        const list = this._children(key);
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
                const list = this._children(el.dataset.treeToggle);
                el.disabled = busy || !(list && list.children.length);
                return;
            }
            el.disabled = busy;
        });
    }

    /** Repaint the "on" accent and the `n/total rules` badges. */
    sync() {
        this._phaseInputs().forEach((cb) => {
            const key = cb.dataset.phase;
            const node = cb.closest('[data-tree-node]');
            if (node) node.classList.toggle('is-on', !!(cb.checked || cb.indeterminate));
            const badge = this.root.querySelector(`[data-tree-count="${key}"]`);
            const rules = this._rules(key);
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
        // Rule link: open the rule instead of toggling its checkbox. Only
        // preventDefault() here, so the root listener still runs and the
        // label's checkbox activation is cancelled.
        const ruleLink = e.target.closest('[data-rule-link]');
        if (ruleLink && this.root.contains(ruleLink)) {
            e.preventDefault();
            this.root.dispatchEvent(new CustomEvent('ob:rule-open', {
                bubbles: true,
                detail: { key: ruleLink.dataset.ruleLink, value: ruleLink.dataset.ruleValue },
            }));
            return;
        }
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
            this.expandableKeys().forEach((key) => this.expand(key, kind === 'expand'));
        }
    }

    _cascade(key, checked) {
        this._rules(key).forEach((cb) => { cb.checked = checked; });
    }

    _syncPhase(key) {
        const phase = this._phaseInput(key);
        const rules = this._rules(key);
        if (!phase || !rules.length || phase.disabled) return;
        const n = rules.filter((cb) => cb.checked).length;
        phase.checked = n === rules.length;
        phase.indeterminate = n > 0 && n < rules.length;
    }

    _checkedValues(key) {
        return this._rules(key).filter((cb) => cb.checked)
            .map((cb) => cb.dataset.ruleValue).filter(Boolean);
    }

    _isOn(key) {
        const cb = this._phaseInput(key);
        return !!(cb && (cb.checked || cb.indeterminate));
    }

    _children(key) {
        return this.root.querySelector(`[data-tree-children="${key}"]`);
    }

    _phaseInput(key) {
        return this.root.querySelector(`[data-phase="${key}"]`);
    }

    _phaseInputs() {
        return [...this.root.querySelectorAll('[data-phase]')];
    }

    _rules(key) {
        return [...this.root.querySelectorAll(`[data-tree-rule="${key}"]`)];
    }

    static esc(value) {
        return String(value ?? '')
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }
}

window.PhaseTree = PhaseTree;
