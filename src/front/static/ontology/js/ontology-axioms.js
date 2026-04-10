/**
 * OntoBricks - ontology-axioms.js
 * Extracted from ontology templates per code_instructions.txt
 */

// EXPRESSIONS & AXIOMS MODULE
// =====================================================
window.AxiomsModule = {
    axioms: [],
    expressions: [],
    editingIndex: -1,
    editingCollection: 'axioms',
    classes: [],
    properties: [],

    EXPRESSION_TYPES: new Set(['unionOf', 'intersectionOf', 'complementOf', 'oneOf']),

    isExpression(type) {
        return this.EXPRESSION_TYPES.has(type);
    },

    _activateTab(collection) {
        const tabId = collection === 'expressions' ? 'expressions-tab' : 'axioms-tab';
        const tabEl = document.getElementById(tabId);
        if (tabEl) new bootstrap.Tab(tabEl).show();
    },
    
    init: function() {
        this.loadAxioms();
        this.loadOntologyItems();
    },
    
    async loadAxioms() {
        try {
            const response = await fetch('/ontology/axioms/list');
            const data = await response.json();
            if (data.success) {
                this.axioms = data.axioms || [];
                this.expressions = data.expressions || [];
                this.renderAll();
            }
        } catch (error) {
            console.error('Error loading axioms:', error);
        }
    },
    
    async loadOntologyItems() {
        try {
            const response = await fetch('/ontology/get-loaded-ontology');
            const data = await response.json();
            if (data.success && data.ontology) {
                this.classes = data.ontology.classes || [];
                this.properties = data.ontology.properties || [];
                this.populateSelects();
            }
        } catch (error) {
            console.log('Could not load ontology items:', error);
        }
    },
    
    populateSelects() {
        this.populateSelect('axiomSubject', this.classes, 'class');
        this.populateSelect('axiomObject', this.classes, 'class');
    },
    
    populateSelect(id, items, type) {
        const select = document.getElementById(id);
        if (!select) return;
        
        select.innerHTML = '<option value="">Select...</option>';
        items.forEach(item => {
            select.innerHTML += `<option value="${item.uri || item.name}">${item.name}</option>`;
        });
    },
    
    onTypeChange() {
        const type = document.getElementById('axiomType').value;
        
        document.getElementById('subjectGroup').style.display = 'block';
        document.getElementById('objectGroup').style.display = 'block';
        document.getElementById('individualsGroup').style.display = 'none';
        document.getElementById('chainGroup').style.display = 'none';
        document.getElementById('addObjectBtn').style.display = 'none';
        
        const isPropertyType = ['equivalentProperty', 'inverseOf', 'propertyChain', 'disjointProperties'].includes(type);
        
        if (isPropertyType) {
            document.getElementById('subjectLabel').textContent = 'Subject Property';
            document.getElementById('objectLabel').textContent = 'Object Property';
            this.populateSelect('axiomSubject', this.properties, 'property');
            this.populateSelect('axiomObject', this.properties, 'property');
        } else {
            document.getElementById('subjectLabel').textContent = 'Subject Class';
            document.getElementById('objectLabel').textContent = 'Object Class(es)';
            this.populateSelect('axiomSubject', this.classes, 'class');
            this.populateSelect('axiomObject', this.classes, 'class');
        }
        
        if (['unionOf', 'intersectionOf', 'disjointWith', 'disjointUnion', 'disjointProperties'].includes(type)) {
            document.getElementById('addObjectBtn').style.display = 'inline-block';
        }
        
        if (type === 'oneOf') {
            document.getElementById('objectGroup').style.display = 'none';
            document.getElementById('individualsGroup').style.display = 'block';
        }
        
        if (type === 'propertyChain') {
            document.getElementById('objectGroup').style.display = 'none';
            document.getElementById('chainGroup').style.display = 'block';
            this.initChainSelects();
        }

        // Update the save button style to match context
        const saveBtn = document.getElementById('axiomSaveBtn');
        if (saveBtn) {
            saveBtn.className = this.isExpression(type)
                ? 'btn btn-primary'
                : 'btn btn-success';
        }
    },
    
    addObjectSelect() {
        const container = document.getElementById('objectSelectContainer');
        const newSelect = document.createElement('select');
        newSelect.className = 'form-select mb-2 additional-object';
        
        const type = document.getElementById('axiomType').value;
        const isPropertyType = ['equivalentProperty', 'inverseOf', 'disjointProperties'].includes(type);
        const items = isPropertyType ? this.properties : this.classes;
        
        newSelect.innerHTML = '<option value="">Select...</option>';
        items.forEach(item => {
            newSelect.innerHTML += `<option value="${item.uri || item.name}">${item.name}</option>`;
        });
        
        container.appendChild(newSelect);
    },
    
    initChainSelects() {
        const container = document.getElementById('chainSelectContainer');
        container.innerHTML = '';
        this.addChainSelect();
        this.addChainSelect();
    },
    
    addChainSelect() {
        const container = document.getElementById('chainSelectContainer');
        const div = document.createElement('div');
        div.className = 'input-group mb-2';
        
        let selectHtml = '<select class="form-select chain-select"><option value="">Select property...</option>';
        this.properties.forEach(prop => {
            selectHtml += `<option value="${prop.uri || prop.name}">${prop.name}</option>`;
        });
        selectHtml += '</select><span class="input-group-text">∘</span>';
        
        div.innerHTML = selectHtml;
        container.appendChild(div);
    },
    
    getAxiomTypeLabel(type) {
        const labels = {
            'equivalentClass': 'Equivalent (≡)',
            'disjointWith': 'Disjoint',
            'disjointUnion': 'Disjoint Union',
            'unionOf': 'Union (⊔)',
            'intersectionOf': 'Intersection (⊓)',
            'complementOf': 'Complement (¬)',
            'oneOf': 'One Of',
            'equivalentProperty': 'Equivalent Props',
            'inverseOf': 'Inverse Of',
            'propertyChain': 'Property Chain',
            'disjointProperties': 'Disjoint Props'
        };
        return labels[type] || type;
    },
    
    getAxiomTypeBadge(type) {
        if (['unionOf', 'intersectionOf', 'complementOf', 'oneOf'].includes(type)) return 'bg-primary';
        if (['equivalentClass', 'equivalentProperty'].includes(type)) return 'bg-success';
        if (['disjointWith', 'disjointUnion', 'disjointProperties'].includes(type)) return 'bg-danger';
        return 'bg-info';
    },

    // ── Rendering ────────────────────────────────────────

    renderAll() {
        const exprItems = this.expressions.map((item, index) => ({ ...item, _index: index, _collection: 'expressions' }));
        const axiomItems = this.axioms.map((item, index) => ({ ...item, _index: index, _collection: 'axioms' }));

        this._renderList('expressionsList', 'noExpressionsMessage', 'expressionsCount', exprItems);
        this._renderList('axiomsList', 'noAxiomsMessage', 'axiomsCount', axiomItems);
    },

    _renderList(containerId, emptyMsgId, countBadgeId, items) {
        const container = document.getElementById(containerId);
        const noMsg = document.getElementById(emptyMsgId);
        const countBadge = document.getElementById(countBadgeId);
        if (!container) return;

        if (countBadge) countBadge.textContent = items.length > 0 ? items.length : '';

        if (items.length === 0) {
            if (noMsg) noMsg.style.display = 'block';
            const cards = container.querySelectorAll('.axiom-card');
            cards.forEach(c => c.remove());
            return;
        }

        if (noMsg) noMsg.style.display = 'none';
        const canEdit = window.isActiveVersion !== false;

        let html = '';
        for (const item of items) {
            const index = item._index;
            const collection = item._collection || 'axioms';
            const badge = this.getAxiomTypeBadge(item.type);
            const actionButtons = canEdit ? `
                <div class="btn-group btn-group-sm ontology-edit-btn">
                    <button class="btn btn-outline-secondary" onclick="AxiomsModule.editAxiom(${index}, '${collection}')" title="Edit">
                        <i class="bi bi-pencil"></i>
                    </button>
                    <button class="btn btn-outline-danger" onclick="AxiomsModule.deleteAxiom(${index}, '${collection}')" title="Delete">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            ` : `
                <button class="btn btn-sm btn-outline-secondary" onclick="AxiomsModule.viewAxiom(${index}, '${collection}')" title="View Details">
                    <i class="bi bi-eye"></i> View
                </button>
            `;

            html += `
                <div class="card mb-2 axiom-card">
                    <div class="card-body py-2 px-3">
                        <div class="d-flex justify-content-between align-items-start">
                            <div class="flex-grow-1">
                                <span class="badge ${badge} me-2">${this.getAxiomTypeLabel(item.type)}</span>
                                <strong>${this.escapeHtml(item.subject)}</strong>
                                ${item.objects && item.objects.length > 0 ?
                                    `<span class="text-muted mx-2">${this.getAxiomSymbol(item.type)}</span>
                                     <span class="text-primary">${item.objects.map(o => this.escapeHtml(o)).join(', ')}</span>` : ''}
                                ${item.individuals ? `<span class="text-success ms-2">{${this.escapeHtml(item.individuals)}}</span>` : ''}
                                ${item.chain ? `<span class="text-info ms-2">${item.chain.map(c => this.escapeHtml(c)).join(' ∘ ')}</span>` : ''}
                                ${item.description ? `<small class="text-muted d-block">${this.escapeHtml(item.description)}</small>` : ''}
                            </div>
                            ${actionButtons}
                        </div>
                    </div>
                </div>
            `;
        }

        const noMsgHtml = noMsg ? noMsg.outerHTML : '';
        container.innerHTML = html + noMsgHtml;
    },
    
    getAxiomSymbol(type) {
        const symbols = {
            'equivalentClass': '≡',
            'equivalentProperty': '≡',
            'disjointWith': '⊥',
            'unionOf': '⊔',
            'intersectionOf': '⊓',
            'complementOf': '¬',
            'inverseOf': '⁻¹'
        };
        return symbols[type] || '→';
    },

    // ── Add / Edit / View ────────────────────────────────

    addForActiveTab() {
        const axiomsTab = document.getElementById('axioms-tab');
        if (axiomsTab && axiomsTab.classList.contains('active')) {
            this.addAxiom();
        } else {
            this.addExpression();
        }
    },

    _resetModal() {
        document.getElementById('axiomType').value = '';
        document.getElementById('axiomSubject').value = '';
        document.getElementById('axiomObject').value = '';
        document.getElementById('axiomIndividuals').value = '';
        document.getElementById('axiomDescription').value = '';
        document.querySelectorAll('.additional-object').forEach(el => el.remove());
        this.onTypeChange();
    },

    addExpression() {
        this.editingIndex = -1;
        this.editingCollection = 'expressions';
        this._activateTab('expressions');
        this._resetModal();
        document.getElementById('axiomModalTitle').innerHTML =
            '<i class="bi bi-collection me-2"></i>Add Expression';
        document.getElementById('axiomModalHint').innerHTML =
            '<i class="bi bi-lightbulb me-1 text-primary"></i>' +
            '<strong>Expression</strong> &mdash; define how a class is <em>composed</em> from other classes ' +
            '(e.g. Pet = Cat &cup; Dog &cup; Fish).';
        const saveBtn = document.getElementById('axiomSaveBtn');
        if (saveBtn) saveBtn.className = 'btn btn-primary';

        // Show only expression options, hide axiom options
        const sel = document.getElementById('axiomType');
        for (const og of sel.querySelectorAll('optgroup')) {
            og.style.display = og.label.toLowerCase().startsWith('expression') ? '' : 'none';
        }

        new bootstrap.Modal(document.getElementById('axiomModal')).show();
    },

    addAxiom() {
        this.editingIndex = -1;
        this.editingCollection = 'axioms';
        this._activateTab('axioms');
        this._resetModal();
        document.getElementById('axiomModalTitle').innerHTML =
            '<i class="bi bi-signpost-split me-2"></i>Add Axiom';
        document.getElementById('axiomModalHint').innerHTML =
            '<i class="bi bi-lightbulb me-1 text-success"></i>' +
            '<strong>Axiom</strong> &mdash; assert a logical <em>fact</em> about classes or properties ' +
            '(e.g. Cat &perp; Dog, or hasParent &#8315;&sup1; hasChild).';
        const saveBtn = document.getElementById('axiomSaveBtn');
        if (saveBtn) saveBtn.className = 'btn btn-success';

        // Show only axiom options, hide expression options
        const sel = document.getElementById('axiomType');
        for (const og of sel.querySelectorAll('optgroup')) {
            og.style.display = og.label.toLowerCase().startsWith('expression') ? 'none' : '';
        }

        new bootstrap.Modal(document.getElementById('axiomModal')).show();
    },
    
    editAxiom(index, collection = 'axioms') {
        this.editingIndex = index;
        this.editingCollection = collection;
        const list = collection === 'expressions' ? this.expressions : this.axioms;
        const axiom = list[index];
        const isExpr = this.isExpression(axiom.type);

        // Show all optgroups when editing
        const sel = document.getElementById('axiomType');
        for (const og of sel.querySelectorAll('optgroup')) {
            og.style.display = '';
        }

        document.getElementById('axiomModalTitle').innerHTML = isExpr
            ? '<i class="bi bi-collection me-2"></i>Edit Expression'
            : '<i class="bi bi-signpost-split me-2"></i>Edit Axiom';
        document.getElementById('axiomModalHint').innerHTML = isExpr
            ? '<i class="bi bi-lightbulb me-1 text-primary"></i><strong>Expression</strong> &mdash; defines how a class is composed from other classes.'
            : '<i class="bi bi-lightbulb me-1 text-success"></i><strong>Axiom</strong> &mdash; asserts a logical fact about classes or properties.';

        document.getElementById('axiomType').value = axiom.type;
        this.onTypeChange();
        
        document.getElementById('axiomSubject').value = axiom.subject || '';
        document.getElementById('axiomObject').value = axiom.objects?.[0] || '';
        document.getElementById('axiomIndividuals').value = axiom.individuals || '';
        document.getElementById('axiomDescription').value = axiom.description || '';
        
        if (axiom.objects && axiom.objects.length > 1) {
            for (let i = 1; i < axiom.objects.length; i++) {
                this.addObjectSelect();
                const selects = document.querySelectorAll('.additional-object');
                selects[selects.length - 1].value = axiom.objects[i];
            }
        }
        
        new bootstrap.Modal(document.getElementById('axiomModal')).show();
    },
    
    viewAxiom(index, collection = 'axioms') {
        this.editingIndex = index;
        this.editingCollection = collection;
        const list = collection === 'expressions' ? this.expressions : this.axioms;
        const axiom = list[index];
        const isExpr = this.isExpression(axiom.type);

        // Show all optgroups
        const sel = document.getElementById('axiomType');
        for (const og of sel.querySelectorAll('optgroup')) {
            og.style.display = '';
        }

        document.getElementById('axiomModalTitle').innerHTML = isExpr
            ? '<i class="bi bi-eye me-2"></i>View Expression'
            : '<i class="bi bi-eye me-2"></i>View Axiom';
        document.getElementById('axiomModalHint').innerHTML = isExpr
            ? '<i class="bi bi-info-circle me-1 text-primary"></i>This is an <strong>expression</strong> &mdash; a class composition.'
            : '<i class="bi bi-info-circle me-1 text-success"></i>This is an <strong>axiom</strong> &mdash; a logical assertion.';

        document.getElementById('axiomType').value = axiom.type;
        this.onTypeChange();
        
        document.getElementById('axiomSubject').value = axiom.subject || '';
        document.getElementById('axiomObject').value = axiom.objects?.[0] || '';
        document.getElementById('axiomIndividuals').value = axiom.individuals || '';
        document.getElementById('axiomDescription').value = axiom.description || '';
        
        if (axiom.objects && axiom.objects.length > 1) {
            for (let i = 1; i < axiom.objects.length; i++) {
                this.addObjectSelect();
                const selects = document.querySelectorAll('.additional-object');
                selects[selects.length - 1].value = axiom.objects[i];
            }
        }
        
        const modal = document.getElementById('axiomModal');
        modal.querySelectorAll('input, textarea, select').forEach(el => {
            el.disabled = true;
        });
        modal.querySelectorAll('.modal-footer .btn-primary, .modal-footer .btn-success').forEach(btn => {
            btn.style.display = 'none';
        });
        modal.querySelectorAll('#addObjectBtn, #chainGroup button').forEach(btn => {
            btn.style.display = 'none';
        });
        
        new bootstrap.Modal(modal).show();
    },
    
    async saveAxiom() {
        const type = document.getElementById('axiomType').value;
        const subject = document.getElementById('axiomSubject').value;
        
        if (!type) {
            showNotification('Please select a type', 'warning');
            return;
        }
        if (!subject && type !== 'propertyChain') {
            showNotification('Please select a subject', 'warning');
            return;
        }
        
        const objects = [];
        const mainObject = document.getElementById('axiomObject').value;
        if (mainObject) objects.push(mainObject);
        document.querySelectorAll('.additional-object').forEach(sel => {
            if (sel.value) objects.push(sel.value);
        });
        
        const chain = [];
        document.querySelectorAll('.chain-select').forEach(sel => {
            if (sel.value) chain.push(sel.value);
        });
        
        const axiom = {
            type: type,
            subject: subject,
            objects: objects.length > 0 ? objects : null,
            individuals: document.getElementById('axiomIndividuals').value || null,
            chain: chain.length > 0 ? chain : null,
            description: document.getElementById('axiomDescription').value || null
        };
        
        const collection = this.isExpression(type) ? 'expressions' : this.editingCollection;

        try {
            const response = await fetch('/ontology/axioms/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ axiom, index: this.editingIndex, collection })
            });
            const data = await response.json();
            
            if (data.success) {
                bootstrap.Modal.getInstance(document.getElementById('axiomModal')).hide();
                this._activateTab(collection);
                this.loadAxioms();
                if (typeof autoGenerateOwl === 'function') autoGenerateOwl();
            } else {
                showNotification('Error saving: ' + data.message, 'error');
            }
        } catch (error) {
            showNotification('Error: ' + error.message, 'error');
        }
    },
    
    async deleteAxiom(index, collection = 'axioms') {
        const list = collection === 'expressions' ? this.expressions : this.axioms;
        const item = list[index];
        const kind = collection === 'expressions' ? 'expression' : 'axiom';
        const confirmed = await showConfirmDialog({
            title: `Delete ${kind.charAt(0).toUpperCase() + kind.slice(1)}`,
            message: `Are you sure you want to delete this ${kind}?`,
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash'
        });
        if (!confirmed) return;
        
        try {
            const response = await fetch('/ontology/axioms/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ index, collection })
            });
            const data = await response.json();
            
            if (data.success) {
                this.loadAxioms();
                if (typeof autoGenerateOwl === 'function') autoGenerateOwl();
            } else {
                showNotification('Error deleting: ' + data.message, 'error');
            }
        } catch (error) {
            showNotification('Error: ' + error.message, 'error');
        }
    },
    
    escapeHtml(text) { return escapeHtml(text); }
};

// Initialize when section becomes active
document.addEventListener('DOMContentLoaded', function() {
    if (document.getElementById('axioms-section')?.classList.contains('active')) {
        AxiomsModule.init();
    }
    
    const modal = document.getElementById('axiomModal');
    if (modal) {
        modal.addEventListener('hidden.bs.modal', function() {
            modal.querySelectorAll('input, textarea, select').forEach(el => {
                el.disabled = false;
            });
            modal.querySelectorAll('.modal-footer .btn-primary, .modal-footer .btn-success').forEach(btn => {
                btn.style.display = '';
            });
            modal.querySelectorAll('#addObjectBtn, #chainGroup button').forEach(btn => {
                btn.style.display = '';
            });
            // Restore all optgroups
            const sel = document.getElementById('axiomType');
            if (sel) {
                for (const og of sel.querySelectorAll('optgroup')) {
                    og.style.display = '';
                }
            }
            document.getElementById('axiomModalTitle').textContent = 'Add';
            document.querySelectorAll('.additional-object').forEach(el => el.remove());
        });
    }
});
