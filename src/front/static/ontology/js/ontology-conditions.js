/**
 * OntoBricks — ontology-conditions.js
 * Reusable property/operator/value condition rows for the Ontology page.
 *
 * A condition is `{ property, property_uri, op, value }`. Rows live in the DOM
 * and are read back with `collect()`, so callers keep no mirror state.
 *
 * Operator names match the decision-table vocabulary used by the backend
 * (`DT_OP_SQL`), plus `exists` / `notExists` for relationships.
 */
window.ConditionRowsModule = {
    OPERATORS: [
        { op: 'eq', label: '=' },
        { op: 'neq', label: '\u2260' },
        { op: 'gt', label: '>' },
        { op: 'gte', label: '\u2265' },
        { op: 'lt', label: '<' },
        { op: 'lte', label: '\u2264' },
        { op: 'startsWith', label: 'starts with' },
        { op: 'endsWith', label: 'ends with' },
        { op: 'contains', label: 'contains' },
    ],

    EXISTENCE_OPERATORS: [
        { op: 'exists', label: 'exists' },
        { op: 'notExists', label: 'does not exist' },
    ],

    isExistenceOp(op) {
        return op === 'exists' || op === 'notExists';
    },

    /**
     * Render `rows` into `container`.
     *
     * @param {HTMLElement} container
     * @param {Array} rows       conditions to display
     * @param {Object} options   { properties: [{uri, name, isRelationship}], onChange }
     */
    render(container, rows, options) {
        if (!container) return;
        const opts = options || {};
        const properties = opts.properties || [];
        container.innerHTML = (rows || []).map(
            (row, index) => this._rowHtml(row, index, properties)
        ).join('');
        this._bindOnce(container, opts);
    },

    /** Append an empty row, defaulting to the first available property. */
    addRow(container, options) {
        const rows = this.collect(container);
        const properties = (options || {}).properties || [];
        const first = properties[0];
        rows.push({
            property: first ? first.name : '',
            property_uri: first ? first.uri : '',
            op: first && first.isRelationship ? 'exists' : 'eq',
            value: '',
        });
        this.render(container, rows, options);
        if (options && options.onChange) options.onChange(rows);
    },

    /**
     * Read the rows currently held in the DOM.
     *
     * Incomplete rows are kept so indexes stay aligned with the rendered ones;
     * callers persisting conditions should drop rows with no `property_uri`.
     */
    collect(container) {
        if (!container) return [];
        return Array.from(container.querySelectorAll('[data-cond-row]')).map(row => {
            const propSel = row.querySelector('[data-cond-field="property"]');
            const opSel = row.querySelector('[data-cond-field="op"]');
            const valInput = row.querySelector('[data-cond-field="value"]');
            const selected = propSel ? propSel.options[propSel.selectedIndex] : null;
            const op = opSel ? opSel.value : 'eq';
            return {
                property: selected ? (selected.dataset.name || '') : '',
                property_uri: propSel ? propSel.value : '',
                op,
                value: this.isExistenceOp(op) ? '' : (valInput ? valInput.value : ''),
            };
        });
    },

    /** One-line human summary, e.g. `status = active AND amount > 1000`. */
    summarize(rows, logic) {
        const joiner = logic === 'or' ? ' OR ' : ' AND ';
        return (rows || []).map(c => {
            const label = this._operatorLabel(c.op);
            return this.isExistenceOp(c.op)
                ? `${c.property} ${label}`
                : `${c.property} ${label} ${c.value}`;
        }).join(joiner);
    },

    _operatorLabel(op) {
        const all = this.OPERATORS.concat(this.EXISTENCE_OPERATORS);
        const found = all.find(o => o.op === op);
        return found ? found.label : op;
    },

    _rowHtml(row, index, properties) {
        const propOptions = properties.map(p => {
            const selected = p.uri === row.property_uri ? 'selected' : '';
            return `<option value="${this._esc(p.uri)}" data-name="${this._esc(p.name)}"` +
                ` data-relationship="${p.isRelationship ? '1' : '0'}" ${selected}>` +
                `${this._esc(p.name)}</option>`;
        }).join('');

        const selectedProp = properties.find(p => p.uri === row.property_uri);
        const isRelationship = !!(selectedProp && selectedProp.isRelationship);
        // A relationship only offers existence operators, so a comparison
        // carried over from a previously selected attribute falls back to one.
        const op = isRelationship && !this.isExistenceOp(row.op) ? 'exists' : row.op;
        const isExistence = this.isExistenceOp(op);
        const opOptions = this._operatorOptions(op, isRelationship);

        return `
        <div class="d-flex align-items-center gap-1 mb-1" data-cond-row data-cond-index="${index}">
            <select class="form-select form-select-sm" data-cond-field="property" style="max-width:12rem">
                <option value="">Select property...</option>
                ${propOptions}
            </select>
            <select class="form-select form-select-sm" data-cond-field="op" style="max-width:9rem">
                ${opOptions}
            </select>
            <input type="text" class="form-control form-control-sm ${isExistence ? 'd-none' : ''}"
                   data-cond-field="value" placeholder="value" value="${this._esc(row.value || '')}">
            <button type="button" class="btn btn-sm btn-link text-danger p-0 px-1"
                    data-cond-remove="${index}" title="Remove condition">
                <i class="bi bi-x-lg"></i>
            </button>
        </div>`;
    },

    _operatorOptions(selected, isRelationship) {
        const available = isRelationship
            ? this.EXISTENCE_OPERATORS
            : this.OPERATORS.concat(this.EXISTENCE_OPERATORS);
        return available.map(o =>
            `<option value="${o.op}" ${o.op === selected ? 'selected' : ''}>${o.label}</option>`
        ).join('');
    },

    _bindOnce(container, options) {
        if (container.dataset.condBound === '1') return;
        container.dataset.condBound = '1';

        const notify = () => {
            if (options.onChange) options.onChange(this.collect(container));
        };

        container.addEventListener('change', (e) => {
            const field = e.target.getAttribute('data-cond-field');
            if (!field) return;
            if (field === 'property' || field === 'op') {
                this.render(container, this.collect(container), options);
            }
            notify();
        });

        container.addEventListener('input', (e) => {
            if (e.target.getAttribute('data-cond-field') === 'value') notify();
        });

        container.addEventListener('click', (e) => {
            const button = e.target.closest('[data-cond-remove]');
            if (!button) return;
            const index = parseInt(button.getAttribute('data-cond-remove'), 10);
            const rows = this.collect(container);
            rows.splice(index, 1);
            this.render(container, rows, options);
            notify();
        });
    },

    _esc(str) {
        return String(str === undefined || str === null ? '' : str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    },
};
