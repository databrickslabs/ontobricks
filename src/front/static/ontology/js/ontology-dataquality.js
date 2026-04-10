/**
 * OntoBricks — ontology-dataquality.js
 * SHACL-based Data Quality module for the Ontology menu.
 */
window.DataQualityModule = {
    shapes: [],
    ontologyClasses: [],
    ontologyProperties: [],
    _shaclPanelOpen: false,

    CATEGORIES: [
        { id: 'completeness', label: 'Completeness', icon: 'bi-check2-all',   badge: 'bg-info' },
        { id: 'cardinality',  label: 'Cardinality',  icon: 'bi-123',          badge: 'bg-primary' },
        { id: 'uniqueness',   label: 'Uniqueness',   icon: 'bi-fingerprint',  badge: 'bg-warning text-dark' },
        { id: 'consistency',  label: 'Consistency',   icon: 'bi-link-45deg',   badge: 'bg-success' },
        { id: 'conformance',  label: 'Conformance',   icon: 'bi-regex',        badge: 'bg-danger' },
        { id: 'structural',   label: 'Structural',    icon: 'bi-diagram-3',    badge: 'bg-dark' },
    ],

    CONSTRAINT_FIELDS: {
        completeness: `
            <div class="alert alert-light small py-2 mb-3">
                <i class="bi bi-info-circle"></i> The selected property must exist and not be null for every entity of the target class.
            </div>`,
        cardinality: `
            <div class="alert alert-light small py-2 mb-3">
                <i class="bi bi-info-circle"></i> Set min and/or max number of values for the property.
            </div>
            <div class="row mb-3">
                <div class="col-6">
                    <label class="form-label">Min count</label>
                    <input type="number" class="form-control" id="dqParamMinCount" min="0" value="">
                </div>
                <div class="col-6">
                    <label class="form-label">Max count</label>
                    <input type="number" class="form-control" id="dqParamMaxCount" min="0" value="">
                </div>
            </div>`,
        uniqueness: `
            <div class="alert alert-light small py-2 mb-3">
                <i class="bi bi-info-circle"></i> Each value of this property must be unique across all instances.
            </div>
            <div class="mb-3">
                <label class="form-label">Uniqueness type</label>
                <select class="form-select" id="dqParamUniquenessType">
                    <option value="functional">Functional (max 1 value per subject)</option>
                    <option value="inverseFunctional">Inverse Functional (each value maps to one subject)</option>
                </select>
            </div>`,
        consistency: `
            <div class="alert alert-light small py-2 mb-3">
                <i class="bi bi-info-circle"></i> Values of this property must be of a specific type.
            </div>
            <div class="mb-3">
                <label class="form-label">Constraint type</label>
                <select class="form-select" id="dqParamConsistencyType" onchange="DataQualityModule.onConsistencyTypeChange()">
                    <option value="sh:class">Target class (sh:class)</option>
                    <option value="sh:datatype">Datatype (sh:datatype)</option>
                </select>
            </div>
            <div class="mb-3" id="dqConsistencyValueGroup">
                <label class="form-label">Target class</label>
                <select class="form-select" id="dqParamTargetType">
                    <option value="">Select...</option>
                </select>
            </div>
            <div class="mb-3" id="dqConsistencyDatatypeGroup" style="display:none">
                <label class="form-label">Datatype</label>
                <select class="form-select" id="dqParamDatatype">
                    <option value="string">xsd:string</option>
                    <option value="integer">xsd:integer</option>
                    <option value="decimal">xsd:decimal</option>
                    <option value="float">xsd:float</option>
                    <option value="double">xsd:double</option>
                    <option value="boolean">xsd:boolean</option>
                    <option value="date">xsd:date</option>
                    <option value="dateTime">xsd:dateTime</option>
                </select>
            </div>`,
        conformance: `
            <div class="alert alert-light small py-2 mb-3">
                <i class="bi bi-info-circle"></i> Values must match a pattern, range, or set of allowed values.
            </div>
            <div class="mb-3">
                <label class="form-label">Constraint type</label>
                <select class="form-select" id="dqParamConformanceType" onchange="DataQualityModule.onConformanceTypeChange()">
                    <option value="sh:pattern">Regex pattern (sh:pattern)</option>
                    <option value="sh:hasValue">Fixed value (sh:hasValue)</option>
                    <option value="sh:minInclusive">Numeric range</option>
                    <option value="sh:minLength">String length</option>
                </select>
            </div>
            <div id="dqConformancePatternGroup">
                <div class="mb-3">
                    <label class="form-label">Pattern (regex)</label>
                    <input type="text" class="form-control font-monospace" id="dqParamPattern" placeholder="^[A-Z].*">
                </div>
            </div>
            <div id="dqConformanceValueGroup" style="display:none">
                <div class="mb-3">
                    <label class="form-label">Value</label>
                    <input type="text" class="form-control" id="dqParamHasValue" placeholder="Expected value">
                </div>
            </div>
            <div id="dqConformanceRangeGroup" style="display:none">
                <div class="row mb-3">
                    <div class="col-6">
                        <label class="form-label">Min (inclusive)</label>
                        <input type="number" class="form-control" id="dqParamRangeMin" step="any">
                    </div>
                    <div class="col-6">
                        <label class="form-label">Max (inclusive)</label>
                        <input type="number" class="form-control" id="dqParamRangeMax" step="any">
                    </div>
                </div>
            </div>
            <div id="dqConformanceLengthGroup" style="display:none">
                <div class="row mb-3">
                    <div class="col-6">
                        <label class="form-label">Min length</label>
                        <input type="number" class="form-control" id="dqParamLenMin" min="0">
                    </div>
                    <div class="col-6">
                        <label class="form-label">Max length</label>
                        <input type="number" class="form-control" id="dqParamLenMax" min="0">
                    </div>
                </div>
            </div>`,
        structural: `
            <div class="alert alert-light small py-2 mb-3">
                <i class="bi bi-info-circle"></i> Graph-level structural rules.
            </div>
            <div class="mb-3">
                <label class="form-label">Rule type</label>
                <select class="form-select" id="dqParamStructuralType">
                    <option value="noOrphans">No orphan entities</option>
                    <option value="closed">Closed shape (no unexpected properties)</option>
                </select>
            </div>`,
    },

    init() {
        this.loadShapes();
        this.loadOntologyItems();
    },

    async loadShapes() {
        try {
            const resp = await fetch('/ontology/dataquality/list', { credentials: 'same-origin' });
            const data = await resp.json();
            if (data.success) {
                this.shapes = data.shapes || [];
                this.renderAll();
            }
        } catch (e) {
            console.error('[DataQuality] Load failed:', e);
        }
    },

    async loadOntologyItems() {
        try {
            const resp = await fetch('/ontology/get-loaded-ontology', { credentials: 'same-origin' });
            const data = await resp.json();
            if (data.success && data.ontology) {
                this.ontologyClasses = data.ontology.classes || [];
                this.ontologyProperties = data.ontology.properties || [];
            }
        } catch (e) {
            console.log('[DataQuality] Could not load ontology items:', e);
        }
    },

    renderAll() {
        const counts = {};
        this.CATEGORIES.forEach(c => { counts[c.id] = 0; });

        this.shapes.forEach(s => {
            const cat = s.category || 'conformance';
            if (counts[cat] !== undefined) counts[cat]++;
        });

        this.CATEGORIES.forEach(c => {
            const badge = document.getElementById('dqBadge' + c.id.charAt(0).toUpperCase() + c.id.slice(1));
            if (badge) badge.textContent = counts[c.id];
            const list = document.getElementById('dqList' + c.id.charAt(0).toUpperCase() + c.id.slice(1));
            if (list) {
                const catShapes = this.shapes.filter(s => s.category === c.id);
                if (catShapes.length) {
                    list.innerHTML = catShapes.map(s => this._renderShapeCard(s)).join('');
                } else {
                    list.innerHTML = `<div class="text-muted small text-center py-3">No ${c.label.toLowerCase()} rules</div>`;
                }
            }
        });
    },

    _renderShapeCard(shape) {
        const cat = this.CATEGORIES.find(c => c.id === shape.category) || this.CATEGORIES[0];
        const enabled = shape.enabled !== false;
        const opacity = enabled ? '' : 'opacity-50';

        return `
        <div class="dq-shape-card ${opacity} mb-2" data-shape-id="${shape.id}">
            <div class="d-flex align-items-start justify-content-between">
                <div class="flex-grow-1">
                    <div class="d-flex align-items-center gap-2 mb-1">
                        <span class="badge ${cat.badge} bg-opacity-75 small">
                            <i class="bi ${cat.icon} me-1"></i>${cat.label}
                        </span>
                        ${shape.target_class ? `<span class="text-muted small">${shape.target_class}</span>` : ''}
                        ${shape.property_path ? `<span class="text-muted small">· ${shape.property_path}</span>` : ''}
                    </div>
                    <div class="small fw-medium">${this._escHtml(shape.label || shape.message || shape.id)}</div>
                    <div class="text-muted small mt-1">
                        <code>${shape.shacl_type}</code>
                        ${shape.parameters ? ' — ' + this._escHtml(JSON.stringify(shape.parameters).slice(0, 80)) : ''}
                    </div>
                </div>
                <div class="btn-group btn-group-sm ms-2 flex-shrink-0">
                    <button class="btn btn-outline-secondary btn-sm" onclick="DataQualityModule.toggleShape('${shape.id}')"
                            title="${enabled ? 'Disable' : 'Enable'}">
                        <i class="bi ${enabled ? 'bi-toggle-on text-success' : 'bi-toggle-off text-danger'}"></i>
                    </button>
                    <button class="btn btn-outline-secondary btn-sm" onclick="DataQualityModule.editShape('${shape.id}')"
                            title="Edit">
                        <i class="bi bi-pencil"></i>
                    </button>
                    <button class="btn btn-outline-danger btn-sm" onclick="DataQualityModule.deleteShape('${shape.id}')"
                            title="Delete">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </div>
        </div>`;
    },

    _escHtml(str) {
        const div = document.createElement('div');
        div.textContent = str || '';
        return div.innerHTML;
    },

    // --- Modal helpers ---

    _getActiveTabCategory() {
        const activeTab = document.querySelector('#dqTabs .nav-link.active');
        return (activeTab && activeTab.dataset.dqCategory) || 'completeness';
    },

    openAddModal() {
        const category = this._getActiveTabCategory();
        document.getElementById('dqEditShapeId').value = '';
        document.getElementById('dqShapeModalTitle').innerHTML =
            '<i class="bi bi-shield-plus me-2"></i>Add Data Quality Rule';
        document.getElementById('dqCategory').value = category;
        document.getElementById('dqMessage').value = '';
        this.onCategoryChange();
        this._populateTargetClassSelect();
        this._updateShaclPreview();
        new bootstrap.Modal(document.getElementById('dqShapeModal')).show();
    },

    editShape(shapeId) {
        const shape = this.shapes.find(s => s.id === shapeId);
        if (!shape) return;
        document.getElementById('dqEditShapeId').value = shapeId;
        document.getElementById('dqShapeModalTitle').innerHTML =
            '<i class="bi bi-pencil me-2"></i>Edit Data Quality Rule';
        document.getElementById('dqCategory').value = shape.category || 'completeness';
        document.getElementById('dqMessage').value = shape.message || '';
        this.onCategoryChange();
        this._populateTargetClassSelect(shape.target_class_uri);

        setTimeout(() => {
            this._populatePropertySelect(shape.target_class, shape.property_uri);
            this._fillParamsFromShape(shape);
            this._updateShaclPreview();
        }, 100);

        new bootstrap.Modal(document.getElementById('dqShapeModal')).show();
    },

    CATEGORY_HINTS: {
        completeness: 'Ensures every entity has the required properties filled in. Catches missing or null values.',
        cardinality: 'Validates that properties have the correct number of values (e.g. exactly one email, at most 3 phone numbers).',
        uniqueness: 'Checks that property values are not duplicated where they should be unique (e.g. one value per subject).',
        consistency: 'Verifies that relationships point to entities of the correct type (e.g. a "worksFor" must target a Company).',
        conformance: 'Validates that values match expected formats or patterns (e.g. regex, allowed values).',
        structural: 'Graph-level rules that check the overall structure (e.g. no orphan entities, all entities must have a label).',
    },

    onCategoryChange() {
        const cat = document.getElementById('dqCategory').value;
        const fields = document.getElementById('dqConstraintFields');
        fields.innerHTML = this.CONSTRAINT_FIELDS[cat] || '';

        const hint = document.getElementById('dqCategoryHint');
        if (hint) hint.textContent = this.CATEGORY_HINTS[cat] || '';

        const propGroup = document.getElementById('dqPropertyGroup');
        const targetGroup = document.getElementById('dqTargetClassGroup');
        if (cat === 'structural') {
            propGroup.style.display = 'none';
            targetGroup.style.display = 'none';
        } else {
            propGroup.style.display = '';
            targetGroup.style.display = '';
        }

        const clsSel = document.getElementById('dqTargetClass');
        const clsOpt = clsSel.options[clsSel.selectedIndex];
        const clsName = clsOpt ? (clsOpt.dataset.name || '') : '';
        this._populatePropertySelect(clsName);

        this._bindParamChangeListeners();
        this._autoFillMessage();
    },

    onTargetClassChange() {
        const sel = document.getElementById('dqTargetClass');
        const opt = sel.options[sel.selectedIndex];
        const clsName = opt ? opt.dataset.name : '';
        this._populatePropertySelect(clsName);
        this._autoFillMessage();
    },

    onConsistencyTypeChange() {
        const type = document.getElementById('dqParamConsistencyType').value;
        const clsGrp = document.getElementById('dqConsistencyValueGroup');
        const dtGrp = document.getElementById('dqConsistencyDatatypeGroup');
        if (type === 'sh:datatype') {
            clsGrp.style.display = 'none';
            dtGrp.style.display = '';
        } else {
            clsGrp.style.display = '';
            dtGrp.style.display = 'none';
        }
        this._autoFillMessage();
    },

    onConformanceTypeChange() {
        const type = document.getElementById('dqParamConformanceType').value;
        document.getElementById('dqConformancePatternGroup').style.display = type === 'sh:pattern' ? '' : 'none';
        document.getElementById('dqConformanceValueGroup').style.display = type === 'sh:hasValue' ? '' : 'none';
        document.getElementById('dqConformanceRangeGroup').style.display = type === 'sh:minInclusive' ? '' : 'none';
        document.getElementById('dqConformanceLengthGroup').style.display = type === 'sh:minLength' ? '' : 'none';
        this._autoFillMessage();
    },

    _bindParamChangeListeners() {
        const ids = [
            'dqParamMinCount', 'dqParamMaxCount',
            'dqParamUniquenessType', 'dqParamConsistencyType', 'dqParamTargetType', 'dqParamDatatype',
            'dqParamConformanceType', 'dqParamPattern', 'dqParamHasValue',
            'dqParamRangeMin', 'dqParamRangeMax', 'dqParamLenMin', 'dqParamLenMax',
            'dqParamStructuralType',
        ];
        ids.forEach(id => {
            const el = document.getElementById(id);
            if (!el) return;
            const evt = el.tagName === 'SELECT' ? 'change' : 'input';
            el.addEventListener(evt, () => this._autoFillMessage());
        });

        const msgEl = document.getElementById('dqMessage');
        if (msgEl) msgEl.addEventListener('input', () => this._updateShaclPreview());
        const propEl = document.getElementById('dqProperty');
        if (propEl) propEl.addEventListener('change', () => { this._autoFillMessage(); });
    },

    _updateShaclPreview() {
        const el = document.getElementById('dqShapePreview');
        if (!el) return;

        const cat = document.getElementById('dqCategory').value;
        const targetSel = document.getElementById('dqTargetClass');
        const targetOpt = targetSel.options[targetSel.selectedIndex];
        const clsUri = targetSel.value || '';
        const clsName = targetOpt ? (targetOpt.dataset.name || '') : '';
        const propSel = document.getElementById('dqProperty');
        const propUri = propSel.value || '';
        const propName = propSel.options[propSel.selectedIndex]?.dataset?.name || '';
        const severity = 'sh:Violation';
        const message = document.getElementById('dqMessage').value || '';

        if (!clsUri && cat !== 'structural') {
            el.innerHTML = '<span class="text-muted fst-italic">Configure the rule to see SHACL output</span>';
            return;
        }

        const { shacl_type, parameters } = this._collectParams(cat);
        const lines = [];
        const prefix = (clsUri || '').replace(/#[^#]*$/, '#');

        lines.push('@prefix sh:   <http://www.w3.org/ns/shacl#> .');
        lines.push('@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .');
        if (prefix) lines.push(`@prefix :     <${prefix}> .`);
        lines.push('');

        const shapeLocal = clsName || 'Target';
        if (cat === 'structural' && shacl_type === 'sh:sparql') {
            lines.push(`<${shapeLocal}Shape> a sh:NodeShape ;`);
            const sel = parameters['sh:select'] || '';
            lines.push(`    sh:sparql [`);
            lines.push(`        sh:select """${sel}""" ;`);
            if (message) lines.push(`        sh:message "${this._escTurtle(message)}" ;`);
            lines.push(`        sh:severity ${severity}`);
            lines.push(`    ] .`);
        } else if (cat === 'structural' && shacl_type === 'sh:closed') {
            lines.push(`:${shapeLocal}Shape a sh:NodeShape ;`);
            if (clsUri) lines.push(`    sh:targetClass <${clsUri}> ;`);
            lines.push(`    sh:closed true .`);
        } else if (shacl_type === 'sh:sparql') {
            lines.push(`:${shapeLocal}Shape a sh:NodeShape ;`);
            lines.push(`    sh:targetClass <${clsUri}> ;`);
            const sel = parameters['sh:select'] || '';
            lines.push(`    sh:sparql [`);
            lines.push(`        sh:select """${sel}""" ;`);
            if (message) lines.push(`        sh:message "${this._escTurtle(message)}" ;`);
            lines.push(`        sh:severity ${severity}`);
            lines.push(`    ] .`);
        } else {
            lines.push(`:${shapeLocal}Shape a sh:NodeShape ;`);
            lines.push(`    sh:targetClass <${clsUri}> ;`);
            lines.push(`    sh:property [`);
            if (propUri) lines.push(`        sh:path <${propUri}> ;`);
            else if (propName) lines.push(`        sh:path :${propName} ;`);

            for (const [k, v] of Object.entries(parameters)) {
                if (k === 'sh:class' || k === 'sh:node') {
                    lines.push(`        ${k} <${v}> ;`);
                } else if (k === 'sh:datatype') {
                    const xsd = v.includes(':') ? v : `xsd:${v}`;
                    lines.push(`        sh:datatype ${xsd} ;`);
                } else if (typeof v === 'number') {
                    lines.push(`        ${k} ${v} ;`);
                } else {
                    lines.push(`        ${k} "${this._escTurtle(String(v))}" ;`);
                }
            }
            if (message) lines.push(`        sh:message "${this._escTurtle(message)}" ;`);
            lines.push(`        sh:severity ${severity}`);
            lines.push(`    ] .`);
        }

        el.textContent = lines.join('\n');
    },

    _escTurtle(s) {
        return (s || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"');
    },

    _autoFillMessage() {
        const cat = document.getElementById('dqCategory').value;
        const clsSel = document.getElementById('dqTargetClass');
        const clsOpt = clsSel.options[clsSel.selectedIndex];
        const cls = clsOpt ? (clsOpt.dataset.name || '') : '';
        const propSel = document.getElementById('dqProperty');
        const propOpt = propSel.options[propSel.selectedIndex];
        const prop = propOpt ? (propOpt.dataset.name || propOpt.text || '') : '';
        let msg = '';

        if (cat === 'completeness') {
            if (cls && prop) msg = `${cls}.${prop} must not be empty`;
        } else if (cat === 'cardinality') {
            const min = document.getElementById('dqParamMinCount')?.value;
            const max = document.getElementById('dqParamMaxCount')?.value;
            if (cls && prop) {
                const parts = [];
                if (min) parts.push(`at least ${min}`);
                if (max) parts.push(`at most ${max}`);
                msg = `${cls}.${prop} must have ${parts.join(' and ') || '...'} value(s)`;
            }
        } else if (cat === 'uniqueness') {
            const type = document.getElementById('dqParamUniquenessType')?.value || 'functional';
            if (prop) {
                msg = type === 'functional'
                    ? `${prop}: at most one value per subject`
                    : `${prop}: each value maps to at most one subject`;
            }
        } else if (cat === 'consistency') {
            const cType = document.getElementById('dqParamConsistencyType')?.value || 'sh:class';
            if (cType === 'sh:datatype') {
                const dt = document.getElementById('dqParamDatatype')?.value || '';
                if (cls && prop) msg = `${cls}.${prop} must be of type ${dt || '...'}`;
            } else {
                const tgtSel = document.getElementById('dqParamTargetType');
                const tgt = tgtSel ? (tgtSel.options[tgtSel.selectedIndex]?.text || '') : '';
                if (cls && prop) msg = `${cls}.${prop} values must be of type ${tgt || '...'}`;
            }
        } else if (cat === 'conformance') {
            const cType = document.getElementById('dqParamConformanceType')?.value || 'sh:pattern';
            if (cType === 'sh:pattern') {
                const pat = document.getElementById('dqParamPattern')?.value || '';
                if (cls && prop) msg = `${cls}.${prop} must match pattern "${pat || '...'}"`;
            } else if (cType === 'sh:hasValue') {
                const val = document.getElementById('dqParamHasValue')?.value || '';
                if (cls && prop) msg = `${cls}.${prop} must equal "${val || '...'}"`;
            } else if (cType === 'sh:minInclusive') {
                const min = document.getElementById('dqParamRangeMin')?.value || '';
                const max = document.getElementById('dqParamRangeMax')?.value || '';
                if (cls && prop) msg = `${cls}.${prop} must be between ${min || '...'} and ${max || '...'}`;
            } else if (cType === 'sh:minLength') {
                const min = document.getElementById('dqParamLenMin')?.value || '';
                const max = document.getElementById('dqParamLenMax')?.value || '';
                if (cls && prop) msg = `${cls}.${prop} length must be between ${min || '...'} and ${max || '...'}`;
            }
        } else if (cat === 'structural') {
            const sType = document.getElementById('dqParamStructuralType')?.value || 'noOrphans';
            msg = sType === 'closed'
                ? 'Entities must not have unexpected properties'
                : 'Every entity must have at least one relationship (no orphans)';
        }

        document.getElementById('dqMessage').value = msg;
        this._updateShaclPreview();
    },

    _populateTargetClassSelect(selectedUri) {
        const sel = document.getElementById('dqTargetClass');
        sel.innerHTML = '<option value="">Select entity...</option>';
        this.ontologyClasses.forEach(cls => {
            const emoji = cls.emoji ? cls.emoji + ' ' : '';
            const uri = cls.uri || cls.name;
            const selected = selectedUri && uri === selectedUri ? 'selected' : '';
            sel.innerHTML += `<option value="${uri}" data-name="${cls.name}" ${selected}>${emoji}${cls.name}</option>`;
        });

        if (document.getElementById('dqCategory').value === 'consistency') {
            const tgt = document.getElementById('dqParamTargetType');
            if (tgt) {
                tgt.innerHTML = '<option value="">Select...</option>';
                this.ontologyClasses.forEach(cls => {
                    const uri = cls.uri || cls.name;
                    tgt.innerHTML += `<option value="${uri}">${cls.name}</option>`;
                });
            }
        }
    },

    _populatePropertySelect(className, selectedUri) {
        const cat = document.getElementById('dqCategory').value;
        const relationshipsOnly = cat === 'cardinality';
        const attributesOnly = cat === 'completeness' || cat === 'conformance' || cat === 'uniqueness';
        const sel = document.getElementById('dqProperty');
        const placeholder = relationshipsOnly ? 'Select relationship...'
            : attributesOnly ? 'Select attribute...'
            : 'Select property...';
        sel.innerHTML = `<option value="">${placeholder}</option>`;

        const label = document.getElementById('dqPropertyLabel');
        if (label) {
            label.textContent = relationshipsOnly ? '2. Relationship'
                : attributesOnly ? '2. Attribute'
                : '2. Property';
        }

        const localName = (uri) => {
            if (!uri) return '';
            const i = Math.max(uri.lastIndexOf('#'), uri.lastIndexOf('/'));
            return i >= 0 ? uri.substring(i + 1) : uri;
        };
        const matchUri = (a, b) => {
            if (!a || !b) return false;
            if (a === b) return true;
            return localName(a).toLowerCase() === localName(b).toLowerCase();
        };

        const seenNames = new Set();
        const seenUris  = new Set();
        const addProp = (uri, name) => {
            const nameKey = (name || '').toLowerCase();
            const uriKey  = (uri  || '').toLowerCase();
            const localKey = localName(uriKey).toLowerCase();
            if (nameKey && seenNames.has(nameKey)) return;
            if (localKey && seenNames.has(localKey)) return;
            if (uriKey && seenUris.has(uriKey)) return;
            if (nameKey) seenNames.add(nameKey);
            if (localKey && localKey !== nameKey) seenNames.add(localKey);
            if (uriKey) seenUris.add(uriKey);
            const selected = selectedUri && matchUri(uri, selectedUri) ? 'selected' : '';
            sel.innerHTML += `<option value="${uri}" data-name="${name}" ${selected}>${name}</option>`;
        };

        const clsLower = (className || '').toLowerCase();

        const isObjectProp = (p) => {
            if (p.type) return p.type === 'ObjectProperty' || p.type === 'owl:ObjectProperty';
            if (p.range) {
                const r = p.range.toLowerCase();
                if (r.startsWith('xsd:') || r.includes('string') || r.includes('integer') ||
                    r.includes('decimal') || r.includes('date') || r.includes('boolean') ||
                    r.includes('float') || r.includes('double') || r.includes('time')) return false;
            }
            return true;
        };

        const allProps = this.ontologyProperties.filter(p => {
            if (!className) return true;
            const dom = (p.domain || '').toLowerCase();
            return dom === clsLower || !dom;
        });

        if (attributesOnly) {
            if (className) {
                const cls = this.ontologyClasses.find(c =>
                    (c.name || '').toLowerCase() === clsLower ||
                    localName(c.uri || '').toLowerCase() === clsLower
                );
                if (cls && cls.dataProperties) {
                    cls.dataProperties.forEach(dp => {
                        addProp(dp.uri || dp.name || dp.localName, dp.name || dp.localName || localName(dp.uri));
                    });
                }
            }
            addProp('http://www.w3.org/2000/01/rdf-schema#label', 'rdfs:label');
        } else if (relationshipsOnly) {
            allProps.filter(isObjectProp).forEach(p => addProp(p.uri || p.name, p.name));
        } else {
            allProps.forEach(p => addProp(p.uri || p.name, p.name));

            if (className) {
                const cls = this.ontologyClasses.find(c =>
                    (c.name || '').toLowerCase() === clsLower ||
                    localName(c.uri || '').toLowerCase() === clsLower
                );
                if (cls && cls.dataProperties) {
                    cls.dataProperties.forEach(dp => {
                        addProp(dp.uri || dp.name || dp.localName, dp.name || dp.localName || localName(dp.uri));
                    });
                }
            }

            addProp('http://www.w3.org/2000/01/rdf-schema#label', 'rdfs:label');
        }

        if (selectedUri && sel.selectedIndex <= 0) {
            const fallbackName = localName(selectedUri);
            sel.innerHTML += `<option value="${selectedUri}" data-name="${fallbackName}" selected>${fallbackName}</option>`;
        }
    },

    _fillParamsFromShape(shape) {
        const params = shape.parameters || {};
        const cat = shape.category;
        if (cat === 'completeness') {
            // No params to fill — always sh:minCount = 1
        } else if (cat === 'cardinality') {
            const elMin = document.getElementById('dqParamMinCount');
            const elMax = document.getElementById('dqParamMaxCount');
            if (elMin) elMin.value = params['sh:minCount'] ?? '';
            if (elMax) elMax.value = params['sh:maxCount'] ?? '';
        } else if (cat === 'conformance') {
            if (params['sh:pattern']) {
                const el = document.getElementById('dqParamConformanceType');
                if (el) el.value = 'sh:pattern';
                this.onConformanceTypeChange();
                const pat = document.getElementById('dqParamPattern');
                if (pat) pat.value = params['sh:pattern'];
            } else if (params['sh:hasValue']) {
                const el = document.getElementById('dqParamConformanceType');
                if (el) el.value = 'sh:hasValue';
                this.onConformanceTypeChange();
                const v = document.getElementById('dqParamHasValue');
                if (v) v.value = params['sh:hasValue'];
            }
        }
    },

    // --- CRUD ---

    async saveShape() {
        const editId = document.getElementById('dqEditShapeId').value;
        const category = document.getElementById('dqCategory').value;
        const targetSel = document.getElementById('dqTargetClass');
        const targetOpt = targetSel.options[targetSel.selectedIndex];
        const propSel = document.getElementById('dqProperty');
        const propOpt = propSel.options[propSel.selectedIndex];

        const shape = {
            id: editId || undefined,
            category,
            target_class: targetOpt ? (targetOpt.dataset.name || '') : '',
            target_class_uri: targetSel.value || '',
            property_path: propOpt ? (propOpt.dataset.name || '') : '',
            property_uri: propSel.value || '',
            severity: 'sh:Violation',
            message: document.getElementById('dqMessage').value,
            enabled: true,
        };

        const { shacl_type, parameters } = this._collectParams(category);
        shape.shacl_type = shacl_type;
        shape.parameters = parameters;
        shape.label = shape.message || `${shacl_type} on ${shape.target_class}.${shape.property_path}`;

        try {
            const resp = await fetch('/ontology/dataquality/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ shape }),
            });
            if (!resp.ok) {
                const text = await resp.text();
                showNotification('Server error (' + resp.status + '): ' + (text || 'unknown'), 'error');
                return;
            }
            const data = await resp.json();
            if (data.success) {
                this.shapes = data.shapes || [];
                this.renderAll();
                bootstrap.Modal.getInstance(document.getElementById('dqShapeModal'))?.hide();
                showNotification('Rule saved', 'success');
            } else {
                showNotification(data.message || 'Save failed', 'error');
            }
        } catch (e) {
            console.error('[DataQuality] Save error:', e);
            showNotification('Error saving rule: ' + (e.message || e), 'error');
        }
    },

    _collectParams(category) {
        let shacl_type = 'sh:minCount';
        const parameters = {};

        if (category === 'completeness') {
            shacl_type = 'sh:minCount';
            parameters['sh:minCount'] = 1;
        } else if (category === 'cardinality') {
            const min = document.getElementById('dqParamMinCount')?.value;
            const max = document.getElementById('dqParamMaxCount')?.value;
            if (min !== '' && min !== undefined) parameters['sh:minCount'] = parseInt(min, 10);
            if (max !== '' && max !== undefined) parameters['sh:maxCount'] = parseInt(max, 10);
            shacl_type = parameters['sh:minCount'] !== undefined ? 'sh:minCount' : 'sh:maxCount';
        } else if (category === 'uniqueness') {
            const type = document.getElementById('dqParamUniquenessType')?.value || 'functional';
            if (type === 'functional') {
                shacl_type = 'sh:maxCount';
                parameters['sh:maxCount'] = 1;
            } else {
                shacl_type = 'sh:sparql';
                const propUri = document.getElementById('dqProperty')?.value || '';
                parameters['sh:select'] =
                    `SELECT $this WHERE { $this <${propUri}> ?val . ?other <${propUri}> ?val . FILTER ($this != ?other) }`;
            }
        } else if (category === 'consistency') {
            const cType = document.getElementById('dqParamConsistencyType')?.value || 'sh:class';
            if (cType === 'sh:class') {
                shacl_type = 'sh:class';
                parameters['sh:class'] = document.getElementById('dqParamTargetType')?.value || '';
            } else {
                shacl_type = 'sh:datatype';
                parameters['sh:datatype'] = document.getElementById('dqParamDatatype')?.value || 'string';
            }
        } else if (category === 'conformance') {
            const cType = document.getElementById('dqParamConformanceType')?.value || 'sh:pattern';
            if (cType === 'sh:pattern') {
                shacl_type = 'sh:pattern';
                parameters['sh:pattern'] = document.getElementById('dqParamPattern')?.value || '';
            } else if (cType === 'sh:hasValue') {
                shacl_type = 'sh:hasValue';
                parameters['sh:hasValue'] = document.getElementById('dqParamHasValue')?.value || '';
            } else if (cType === 'sh:minInclusive') {
                shacl_type = 'sh:minInclusive';
                const min = document.getElementById('dqParamRangeMin')?.value;
                const max = document.getElementById('dqParamRangeMax')?.value;
                if (min) parameters['sh:minInclusive'] = parseFloat(min);
                if (max) parameters['sh:maxInclusive'] = parseFloat(max);
            } else if (cType === 'sh:minLength') {
                shacl_type = 'sh:minLength';
                const min = document.getElementById('dqParamLenMin')?.value;
                const max = document.getElementById('dqParamLenMax')?.value;
                if (min) parameters['sh:minLength'] = parseInt(min, 10);
                if (max) parameters['sh:maxLength'] = parseInt(max, 10);
            }
        } else if (category === 'structural') {
            const sType = document.getElementById('dqParamStructuralType')?.value || 'noOrphans';
            if (sType === 'closed') {
                shacl_type = 'sh:closed';
            } else {
                shacl_type = 'sh:sparql';
                parameters['sh:select'] =
                    'SELECT $this WHERE { $this a ?type . FILTER NOT EXISTS { $this ?p ?o . FILTER (?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>) } FILTER NOT EXISTS { ?s ?p2 $this . FILTER (?p2 != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>) } }';
            }
        }

        return { shacl_type, parameters };
    },

    async deleteShape(shapeId) {
        const confirmed = await showConfirmDialog({
            title: 'Delete Rule',
            message: 'Delete this data quality rule?',
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash',
        });
        if (!confirmed) return;
        try {
            const resp = await fetch('/ontology/dataquality/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ id: shapeId }),
            });
            const data = await resp.json();
            if (data.success) {
                this.shapes = data.shapes || [];
                this.renderAll();
            }
        } catch (e) {
            console.error('[DataQuality] Delete error:', e);
        }
    },

    async toggleShape(shapeId) {
        const shape = this.shapes.find(s => s.id === shapeId);
        if (!shape) return;
        shape.enabled = !shape.enabled;
        try {
            await fetch('/ontology/dataquality/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ shape }),
            });
            this.renderAll();
        } catch (e) {
            console.error('[DataQuality] Toggle error:', e);
        }
    },

    // --- SHACL panel ---

    toggleShaclPanel() {
        this._shaclPanelOpen = !this._shaclPanelOpen;
        document.getElementById('dqShaclPanel').style.display = this._shaclPanelOpen ? '' : 'none';
        if (this._shaclPanelOpen) this.refreshTurtle();
    },

    async refreshTurtle() {
        try {
            const resp = await fetch('/ontology/dataquality/turtle', { credentials: 'same-origin' });
            const data = await resp.json();
            if (data.success) {
                document.getElementById('dqShaclEditor').value = data.turtle || '';
            }
        } catch (e) {
            console.error('[DataQuality] Turtle refresh error:', e);
        }
    },

    exportShacl() {
        window.location.href = '/ontology/dataquality/export';
    },

    showImportModal() {
        document.getElementById('dqImportFile').value = '';
        document.getElementById('dqImportText').value = '';
        new bootstrap.Modal(document.getElementById('dqImportModal')).show();
    },

    async doImport() {
        let turtle = document.getElementById('dqImportText').value.trim();
        const fileInput = document.getElementById('dqImportFile');
        if (fileInput.files.length > 0 && !turtle) {
            turtle = await fileInput.files[0].text();
        }
        if (!turtle) {
            showNotification('Please provide SHACL Turtle content', 'warning');
            return;
        }
        try {
            const resp = await fetch('/ontology/dataquality/import', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ turtle }),
            });
            if (!resp.ok) {
                const text = await resp.text();
                showNotification('Server error (' + resp.status + '): ' + (text || 'unknown'), 'error');
                return;
            }
            const data = await resp.json();
            if (data.success) {
                this.shapes = data.shapes || [];
                this.renderAll();
                bootstrap.Modal.getInstance(document.getElementById('dqImportModal'))?.hide();
                showNotification('Imported ' + (data.imported_count || 0) + ' shapes', 'success');
            } else {
                showNotification(data.message || 'Import failed', 'error');
            }
        } catch (e) {
            console.error('[DataQuality] Import error:', e);
            showNotification('Error importing SHACL: ' + (e.message || e), 'error');
        }
    },
};
