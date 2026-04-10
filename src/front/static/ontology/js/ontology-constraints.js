/**
 * OntoBricks - ontology-constraints.js
 * Extracted from ontology templates per code_instructions.txt
 */

// PROPERTY CONSTRAINTS MODULE - REDESIGNED
// =====================================================
window.ConstraintsModule = {
    constraints: [],
    ontologyProperties: [],
    ontologyClasses: [],
    globalRules: {},
    editingIndex: -1,
    editingType: null,

    _isObjectProperty(prop) {
        if (prop.type) {
            return prop.type === 'ObjectProperty' || prop.type === 'owl:ObjectProperty';
        }
        if (prop.range) {
            const range = prop.range.toLowerCase();
            if (range.startsWith('xsd:') || range.includes('string') || range.includes('integer') ||
                range.includes('decimal') || range.includes('date') || range.includes('boolean') ||
                range.includes('float') || range.includes('double') || range.includes('time')) {
                return false;
            }
            const classes = (this.ontologyClasses || []).map(c => (c.name || '').toLowerCase());
            if (classes.includes(range.toLowerCase())) return true;
        }
        if (prop.domain && !prop.range) return false;
        if (Array.isArray(prop.properties) && prop.properties.length > 0) return false;
        return false;
    },
    
    init: function() {
        this.loadConstraints();
        this.loadOntologyItems();
    },
    
    async loadConstraints() {
        try {
            const response = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
            const data = await response.json();
            console.log('[Constraints] Loaded from server:', data);
            if (data.success) {
                this.constraints = data.constraints || [];
                console.log('[Constraints] Total constraints:', this.constraints.length, this.constraints);
                this.renderConstraintsList();
            }
        } catch (error) {
            console.error('Error loading constraints:', error);
        }
    },
    
    async loadOntologyItems() {
        try {
            const response = await fetch('/ontology/get-loaded-ontology', { credentials: 'same-origin' });
            const data = await response.json();
            if (data.success && data.ontology) {
                this.ontologyClasses = data.ontology.classes || [];
                this.ontologyProperties = data.ontology.properties || [];
                this.populateSelects();
            }
        } catch (error) {
            console.log('Could not load ontology items:', error);
        }
    },
    
    populateSelects() {
        // Populate all class/entity selects
        const classSelectIds = ['valueEntity'];
        classSelectIds.forEach(id => {
            const select = document.getElementById(id);
            if (select) {
                select.innerHTML = '<option value="">Select entity...</option>';
                this.ontologyClasses.forEach(cls => {
                    const emoji = cls.emoji ? cls.emoji + ' ' : '';
                    select.innerHTML += `<option value="${cls.uri || cls.name}" data-name="${cls.name}">${emoji}${cls.name}</option>`;
                });
            }
        });
        
        // Populate relationship select (object properties only)
        const cardSelect = document.getElementById('cardinalityProperty');
        if (cardSelect) {
            cardSelect.innerHTML = '<option value="">Select relationship...</option>';
            this.ontologyProperties
                .filter(prop => this._isObjectProperty(prop))
                .forEach(prop => {
                    let domainRange = '';
                    if (prop.domain && prop.range) {
                        const dir = prop.direction || 'forward';
                        domainRange = dir === 'reverse'
                            ? ` (${prop.range} → ${prop.domain})`
                            : ` (${prop.domain} → ${prop.range})`;
                    }
                    const option = document.createElement('option');
                    option.value = prop.uri || prop.name;
                    option.textContent = `${prop.name}${domainRange}`;
                    option.dataset.domain = prop.domain || '';
                    option.dataset.range = prop.range || '';
                    option.dataset.name = prop.name || '';
                    cardSelect.appendChild(option);
                });
        }

        // Populate entity value property select (all properties)
        const evSelect = document.getElementById('entityValueProperty');
        if (evSelect) {
            evSelect.innerHTML = '<option value="">Select relationship...</option>';
            this.ontologyProperties.forEach(prop => {
                let domainRange = '';
                if (prop.domain && prop.range) {
                    const dir = prop.direction || 'forward';
                    domainRange = dir === 'reverse'
                        ? ` (${prop.range} → ${prop.domain})`
                        : ` (${prop.domain} → ${prop.range})`;
                }
                const option = document.createElement('option');
                option.value = prop.uri || prop.name;
                option.textContent = `${prop.name}${domainRange}`;
                option.dataset.domain = prop.domain || '';
                option.dataset.name = prop.name || '';
                evSelect.appendChild(option);
            });
        }
        
        // Entity change handler for attributes
        const entitySelect = document.getElementById('attributeEntity');
        if (entitySelect) {
            entitySelect.addEventListener('change', () => this.loadAttributesForEntity());
        }
    },
    
    loadAttributesForEntity() {
        const entitySelect = document.getElementById('attributeEntity');
        const attrSelect = document.getElementById('attributeName');
        const selectedName = entitySelect.options[entitySelect.selectedIndex]?.dataset.name;
        
        attrSelect.innerHTML = '<option value="">Select attribute...</option>';
        
        if (!selectedName) return;
        
        // Find the class and its attributes
        const cls = this.ontologyClasses.find(c => c.name === selectedName);
        if (cls && cls.attributes) {
            cls.attributes.forEach(attr => {
                attrSelect.innerHTML += `<option value="${attr.name || attr.id}">${attr.name || attr.id}</option>`;
            });
        }
    },
    
    // =====================================================
    // CARDINALITY MODAL
    // =====================================================
    openCardinalityModal(editIndex = -1) {
        this.editingIndex = editIndex;
        this.editingType = 'cardinality';
        
        // Reset all fields
        document.getElementById('cardinalityProperty').value = '';
        document.getElementById('cardinalityType').value = '';
        document.getElementById('cardinalityValue').value = 1;
        document.getElementById('cardinalityOptions').style.display = 'none';
        
        // Reset all checkboxes and restore generic labels
        ['Functional', 'InverseFunctional', 'Transitive', 'Symmetric', 'Asymmetric', 'Reflexive', 'Irreflexive'].forEach(type => {
            const checkbox = document.getElementById('card' + type);
            if (checkbox) checkbox.checked = false;
        });
        this._updatePropertyLabels('relates to', 'A', 'B');
        
        if (editIndex >= 0) {
            const c = this.constraints[editIndex];
            document.getElementById('cardinalityValue').value = c.cardinalityValue || 1;
            
            // Find and select the matching property in the dropdown
            const propSelect = document.getElementById('cardinalityProperty');
            const savedProperty = c.property || '';
            
            // Try to find a matching option
            let found = false;
            for (const option of propSelect.options) {
                const optionValue = option.value;
                const optionName = option.dataset.name || '';
                
                // Match by value (URI), name, or local part of URI
                const savedLocalName = savedProperty.includes('#') ? savedProperty.split('#').pop() : 
                                       savedProperty.includes('/') ? savedProperty.split('/').pop() : savedProperty;
                const optionLocalName = optionValue.includes('#') ? optionValue.split('#').pop() :
                                        optionValue.includes('/') ? optionValue.split('/').pop() : optionValue;
                
                if (optionValue === savedProperty || 
                    optionName === savedProperty || 
                    optionLocalName === savedLocalName ||
                    optionName === savedLocalName) {
                    propSelect.value = optionValue;
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                console.warn('[Constraints] Could not find matching property for:', savedProperty);
                propSelect.value = '';
            }
            
            // Load relationship properties and set cardinality type
            this.loadRelationshipProperties();
            
            // Set cardinality type after loading (it might be a property characteristic, not cardinality)
            const cardinalityTypes = ['minCardinality', 'maxCardinality', 'exactCardinality'];
            if (cardinalityTypes.includes(c.type)) {
                document.getElementById('cardinalityType').value = c.type;
            }
        }
        
        new bootstrap.Modal(document.getElementById('cardinalityModal')).show();
    },
    
    loadRelationshipProperties() {
        const propSelect = document.getElementById('cardinalityProperty');
        const property = propSelect.value;
        const optionsDiv = document.getElementById('cardinalityOptions');
        
        if (!property) {
            optionsDiv.style.display = 'none';
            return;
        }
        
        optionsDiv.style.display = 'block';
        
        const selected = propSelect.options[propSelect.selectedIndex];
        const relName = selected.dataset.name || property;
        const domain = selected.dataset.domain || 'X';
        const range = selected.dataset.range || 'Y';
        this._updatePropertyLabels(relName, domain, range);
        
        // Check existing constraints for this property
        const propConstraints = this.constraints.filter(c => {
            const cProp = c.property || '';
            const cLocal = cProp.includes('#') ? cProp.split('#').pop() : cProp.includes('/') ? cProp.split('/').pop() : cProp;
            const propLocal = property.includes('#') ? property.split('#').pop() : property.includes('/') ? property.split('/').pop() : property;
            return cProp === property || cLocal === propLocal;
        });
        const enabledTypes = propConstraints.map(c => c.type);
        
        // Set cardinality type if exists
        const cardinalityTypes = ['minCardinality', 'maxCardinality', 'exactCardinality'];
        const existingCardinality = propConstraints.find(c => cardinalityTypes.includes(c.type));
        if (existingCardinality) {
            document.getElementById('cardinalityType').value = existingCardinality.type;
            document.getElementById('cardinalityValue').value = existingCardinality.cardinalityValue || 1;
        } else {
            document.getElementById('cardinalityType').value = '';
            document.getElementById('cardinalityValue').value = 1;
        }
        
        // Set property characteristics checkboxes
        ['Functional', 'InverseFunctional', 'Transitive', 'Symmetric', 'Asymmetric', 'Reflexive', 'Irreflexive'].forEach(type => {
            const checkbox = document.getElementById('card' + type);
            if (checkbox) {
                checkbox.checked = enabledTypes.includes(type.toLowerCase());
            }
        });
    },

    _updatePropertyLabels(rel, domain, range) {
        const d = domain, r = range, p = rel;
        const labels = {
            cardFunctional:       `Each <b>${d}</b> has at most one <b>${r}</b> via <b>${p}</b>`,
            cardInverseFunctional:`Each <b>${r}</b> is linked to at most one <b>${d}</b> via <b>${p}</b>`,
            cardTransitive:       `If <b>${d}</b> ${p} <b>${r}</b> and <b>${r}</b> ${p} <b>${r}\'</b>, then infer <b>${d}</b> ${p} <b>${r}\'</b>`,
            cardSymmetric:        `If <b>${d}</b> ${p} <b>${r}</b>, then infer <b>${r}</b> ${p} <b>${d}</b>`,
            cardAsymmetric:       `If <b>${d}</b> ${p} <b>${r}</b>, then <b>${r}</b> ${p} <b>${d}</b> cannot exist`,
            cardReflexive:        `Every <b>${d}</b> ${p} itself`,
            cardIrreflexive:      `No <b>${d}</b> can ${p} itself`
        };
        for (const [id, html] of Object.entries(labels)) {
            const el = document.querySelector(`#${id} + label small`);
            if (el) el.innerHTML = html;
        }
    },
    
    async saveCardinality() {
        const property = document.getElementById('cardinalityProperty').value;
        if (!property) {
            showNotification('Please select a relationship', 'warning');
            return;
        }
        
        const propSelect = document.getElementById('cardinalityProperty');
        const selectedOption = propSelect.options[propSelect.selectedIndex];
        const domainClass = this.ontologyClasses.find(c => c.name === selectedOption.dataset.domain);
        const className = domainClass ? (domainClass.uri || domainClass.name) : selectedOption.dataset.domain;
        
        // Helper to get local name for comparison
        const getLocalName = (uri) => {
            if (!uri) return '';
            if (uri.includes('#')) return uri.split('#').pop();
            if (uri.includes('/')) return uri.split('/').pop();
            return uri;
        };
        const propertyLocal = getLocalName(property);
        
        // Save cardinality constraint if selected
        const cardinalityType = document.getElementById('cardinalityType').value;
        const cardinalityValue = parseInt(document.getElementById('cardinalityValue').value) || 0;
        const cardinalityTypes = ['minCardinality', 'maxCardinality', 'exactCardinality'];
        
        // Find existing cardinality constraint for this property
        const existingCardIdx = this.constraints.findIndex(c => {
            const cLocal = getLocalName(c.property);
            return cardinalityTypes.includes(c.type) && (c.property === property || cLocal === propertyLocal);
        });
        
        if (cardinalityType) {
            // Add or update cardinality constraint
            const constraint = {
                type: cardinalityType,
                property: property,
                className: className,
                cardinalityValue: cardinalityValue
            };
            await this.saveConstraintToServer(constraint, existingCardIdx >= 0 ? existingCardIdx : -1);
        } else if (existingCardIdx >= 0) {
            // Remove existing cardinality if none selected
            await this.deleteConstraintFromServer(existingCardIdx);
        }
        
        // Save property characteristics
        const characteristics = ['functional', 'inverseFunctional', 'transitive', 'symmetric', 'asymmetric', 'reflexive', 'irreflexive'];
        
        for (const char of characteristics) {
            const checkboxId = 'card' + char.charAt(0).toUpperCase() + char.slice(1);
            const isChecked = document.getElementById(checkboxId)?.checked;
            
            // Find existing constraint for this property and characteristic
            const existingIdx = this.constraints.findIndex(c => {
                const cLocal = getLocalName(c.property);
                return c.type === char && (c.property === property || cLocal === propertyLocal);
            });
            
            if (isChecked && existingIdx < 0) {
                // Add new constraint
                await this.saveConstraintToServer({ type: char, property: property }, -1);
            } else if (!isChecked && existingIdx >= 0) {
                // Remove existing constraint
                await this.deleteConstraintFromServer(existingIdx);
            }
        }
        
        bootstrap.Modal.getInstance(document.getElementById('cardinalityModal')).hide();
        this.loadConstraints();
    },
    
    // =====================================================
    // VALUE CONSTRAINT MODAL
    // =====================================================
    openValueModal(editIndex = -1) {
        this.editingIndex = editIndex;
        this.editingType = 'value';
        
        // Reset attribute selector
        const attrSelect = document.getElementById('valueAttribute');
        attrSelect.innerHTML = '<option value="">Select entity first...</option>';
        attrSelect.disabled = true;
        
        if (editIndex >= 0) {
            const c = this.constraints[editIndex];
            document.getElementById('valueEntity').value = c.className || '';
            // Load attributes first, then set the attribute value
            this.loadValueAttributes().then(() => {
                document.getElementById('valueAttribute').value = c.attributeName || '';
            });
            document.getElementById('valueCheckType').value = c.checkType || 'startsWith';
            document.getElementById('valueCheckValue').value = c.checkValue || '';
            document.getElementById('valueCaseSensitive').checked = c.caseSensitive || false;
        } else {
            document.getElementById('valueEntity').value = '';
            document.getElementById('valueCheckType').value = 'startsWith';
            document.getElementById('valueCheckValue').value = '';
            document.getElementById('valueCaseSensitive').checked = false;
        }
        
        this.onValueCheckTypeChange();
        new bootstrap.Modal(document.getElementById('valueModal')).show();
    },
    
    loadValueAttributes() {
        return new Promise((resolve) => {
            const entitySelect = document.getElementById('valueEntity');
            const attrSelect = document.getElementById('valueAttribute');
            const selectedValue = entitySelect.value;
            const selectedOption = entitySelect.options[entitySelect.selectedIndex];
            const entityName = selectedOption?.dataset?.name || '';
            
            // Reset attributes
            attrSelect.innerHTML = '<option value="">Select attribute...</option>';
            
            if (!selectedValue || !entityName) {
                attrSelect.disabled = true;
                resolve();
                return;
            }
            
            // Add Label as first option
            attrSelect.innerHTML = '<option value="label">Label (rdfs:label)</option>';
            
            // Find the class and add its data properties (attributes)
            const cls = this.ontologyClasses.find(c => c.name === entityName);
            if (cls) {
                // Check dataProperties (entity attributes)
                const dataProps = cls.dataProperties || [];
                dataProps.forEach(prop => {
                    const propName = prop.name || prop.localName || prop;
                    if (propName && typeof propName === 'string') {
                        attrSelect.innerHTML += `<option value="${propName}">${propName}</option>`;
                    }
                });
            }
            
            attrSelect.disabled = false;
            resolve();
        });
    },
    
    onValueCheckTypeChange() {
        const checkType = document.getElementById('valueCheckType').value;
        const inputGroup = document.getElementById('valueInputGroup');
        const caseSensitiveGroup = document.getElementById('valueCaseSensitiveGroup');
        
        // Hide value input for "not empty" check
        if (checkType === 'notNull') {
            inputGroup.style.display = 'none';
            caseSensitiveGroup.style.display = 'none';
        } else {
            inputGroup.style.display = 'block';
            caseSensitiveGroup.style.display = 'block';
        }
    },
    
    async saveValue() {
        const className = document.getElementById('valueEntity').value;
        const attributeName = document.getElementById('valueAttribute').value;
        const checkType = document.getElementById('valueCheckType').value;
        
        if (!className) {
            showNotification('Please select an entity', 'warning');
            return;
        }
        if (!attributeName) {
            showNotification('Please select an attribute', 'warning');
            return;
        }
        
        const constraint = {
            type: 'valueCheck',
            className: className,
            attributeName: attributeName,
            checkType: checkType,
            checkValue: document.getElementById('valueCheckValue').value || null,
            caseSensitive: document.getElementById('valueCaseSensitive').checked
        };
        
        // Validate required values (except for notNull)
        if (checkType !== 'notNull' && !constraint.checkValue) {
            showNotification('Please enter a value to check against', 'warning');
            return;
        }
        
        await this.saveConstraintToServer(constraint);
        bootstrap.Modal.getInstance(document.getElementById('valueModal')).hide();
    },
    
    // =====================================================
    // ATTRIBUTE VALUES MODAL
    // =====================================================
    openAttributeValuesModal(editIndex = -1) {
        this.editingIndex = editIndex;
        this.editingType = 'attributeValue';
        this.onAttributeTypeChange();
        
        if (editIndex >= 0) {
            const c = this.constraints[editIndex];
            document.getElementById('attributeConstraintType').value = c.attributeConstraintType || 'datatype';
            document.getElementById('attributeEntity').value = c.className || '';
            this.loadAttributesForEntity();
            setTimeout(() => {
                document.getElementById('attributeName').value = c.attributeName || '';
            }, 100);
            document.getElementById('attributeDatatype').value = c.datatype || 'xsd:string';
            document.getElementById('attributePattern').value = c.pattern || '';
            document.getElementById('attributeMinValue').value = c.minValue || '';
            document.getElementById('attributeMaxValue').value = c.maxValue || '';
            document.getElementById('attributeMinLength').value = c.minLength || '';
            document.getElementById('attributeMaxLength').value = c.maxLength || '';
            document.getElementById('attributeEnumValues').value = c.enumValues || '';
            this.onAttributeTypeChange();
        } else {
            document.getElementById('attributeConstraintType').value = 'datatype';
            document.getElementById('attributeEntity').value = '';
            document.getElementById('attributeName').innerHTML = '<option value="">Select attribute...</option>';
        }
        
        new bootstrap.Modal(document.getElementById('attributeValuesModal')).show();
    },
    
    onAttributeTypeChange() {
        const type = document.getElementById('attributeConstraintType').value;
        document.getElementById('attributeDatatypeGroup').style.display = type === 'datatype' ? 'block' : 'none';
        document.getElementById('attributePatternGroup').style.display = type === 'pattern' ? 'block' : 'none';
        document.getElementById('attributeRangeGroup').style.display = type === 'range' ? 'block' : 'none';
        document.getElementById('attributeLengthGroup').style.display = type === 'length' ? 'block' : 'none';
        document.getElementById('attributeEnumGroup').style.display = type === 'enum' ? 'block' : 'none';
    },
    
    async saveAttributeValue() {
        const className = document.getElementById('attributeEntity').value;
        const attrName = document.getElementById('attributeName').value;
        const constraintType = document.getElementById('attributeConstraintType').value;
        
        if (!className || !attrName) {
            showNotification('Please select an entity and attribute', 'warning');
            return;
        }
        
        const constraint = {
            type: 'attributeConstraint',
            attributeConstraintType: constraintType,
            className: className,
            attributeName: attrName,
            datatype: constraintType === 'datatype' ? document.getElementById('attributeDatatype').value : null,
            pattern: constraintType === 'pattern' ? document.getElementById('attributePattern').value : null,
            minValue: constraintType === 'range' ? document.getElementById('attributeMinValue').value : null,
            maxValue: constraintType === 'range' ? document.getElementById('attributeMaxValue').value : null,
            minLength: constraintType === 'length' ? document.getElementById('attributeMinLength').value : null,
            maxLength: constraintType === 'length' ? document.getElementById('attributeMaxLength').value : null,
            enumValues: constraintType === 'enum' ? document.getElementById('attributeEnumValues').value : null
        };
        
        await this.saveConstraintToServer(constraint);
        bootstrap.Modal.getInstance(document.getElementById('attributeValuesModal')).hide();
    },
    
    // =====================================================
    // GLOBAL CONSTRAINTS MODAL
    // =====================================================
    openGlobalModal() {
        // Load existing global rules
        this.globalRules = {};
        this.constraints.forEach(c => {
            if (c.type === 'globalRule') {
                this.globalRules[c.ruleName] = true;
            }
        });
        
        document.getElementById('globalNoOrphans').checked = !!this.globalRules['noOrphans'];
        document.getElementById('globalRequireLabels').checked = !!this.globalRules['requireLabels'];
        document.getElementById('globalUniqueIds').checked = !!this.globalRules['uniqueIds'];
        
        new bootstrap.Modal(document.getElementById('globalModal')).show();
    },
    
    toggleGlobalRule(ruleName) {
        this.globalRules[ruleName] = document.getElementById('global' + ruleName.charAt(0).toUpperCase() + ruleName.slice(1)).checked;
    },
    
    async saveGlobalConstraints() {
        // Save global rules only
        const rules = ['noOrphans', 'requireLabels', 'uniqueIds'];
        for (const rule of rules) {
            const checkboxId = 'global' + rule.charAt(0).toUpperCase() + rule.slice(1);
            const isChecked = document.getElementById(checkboxId)?.checked;
            const existingIdx = this.constraints.findIndex(c => c.type === 'globalRule' && c.ruleName === rule);
            
            if (isChecked && existingIdx < 0) {
                await this.saveConstraintToServer({ type: 'globalRule', ruleName: rule }, -1);
            } else if (!isChecked && existingIdx >= 0) {
                await this.deleteConstraintFromServer(existingIdx);
            }
        }
        
        bootstrap.Modal.getInstance(document.getElementById('globalModal')).hide();
        this.loadConstraints();
    },
    
    // =====================================================
    // COMMON FUNCTIONS
    // =====================================================
    async saveConstraintToServer(constraint, index = null) {
        try {
            const response = await fetch('/ontology/constraints/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ constraint, index: index !== null ? index : this.editingIndex }),
                credentials: 'same-origin'
            });
            const data = await response.json();
            
            if (data.success) {
                this.loadConstraints();
                if (typeof autoGenerateOwl === 'function') autoGenerateOwl();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (error) {
            showNotification('Error: ' + error.message, 'error');
        }
    },
    
    async deleteConstraintFromServer(index) {
        try {
            await fetch('/ontology/constraints/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ index }),
                credentials: 'same-origin'
            });
        } catch (error) {
            console.error('Error deleting constraint:', error);
        }
    },
    
    async deleteConstraint(index) {
        const confirmed = await showConfirmDialog({
            title: 'Delete Constraint',
            message: 'Are you sure you want to delete this constraint?',
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash'
        });
        if (!confirmed) return;
        await this.deleteConstraintFromServer(index);
        this.loadConstraints();
        if (typeof autoGenerateOwl === 'function') autoGenerateOwl();
    },
    
    editConstraint(index) {
        const c = this.constraints[index];
        const cardinalityTypes = ['minCardinality', 'maxCardinality', 'exactCardinality'];
        const valueCheckTypes = ['valueCheck', 'entityValueCheck', 'entityLabelCheck'];
        const propertyChars = ['functional', 'inverseFunctional', 'transitive', 'symmetric', 'asymmetric', 'reflexive', 'irreflexive'];
        
        if (cardinalityTypes.includes(c.type) || propertyChars.includes(c.type)) {
            // Both cardinality and property characteristics are now in the Cardinality modal
            this.openCardinalityModal(index);
        } else if (valueCheckTypes.includes(c.type)) {
            this.openValueModal(index);
        } else if (c.type === 'attributeConstraint') {
            showNotification('Attribute constraint editing not yet available. Please delete and recreate.', 'info');
        } else if (c.type === 'globalRule') {
            this.openGlobalModal();
        }
    },
    
    viewConstraint(index) {
        const c = this.constraints[index];
        const cardinalityTypes = ['minCardinality', 'maxCardinality', 'exactCardinality'];
        const valueCheckTypes = ['valueCheck', 'entityValueCheck', 'entityLabelCheck'];
        const propertyChars = ['functional', 'inverseFunctional', 'transitive', 'symmetric', 'asymmetric', 'reflexive', 'irreflexive'];
        
        if (cardinalityTypes.includes(c.type) || propertyChars.includes(c.type)) {
            this.openCardinalityModal(index);
            this._setModalReadOnly('cardinalityModal', 'Relationship Constraint');
        } else if (valueCheckTypes.includes(c.type)) {
            this.openValueModal(index);
            this._setModalReadOnly('valueModal', 'Value Constraint');
        } else if (c.type === 'attributeConstraint') {
            showNotification('Attribute constraint viewing not yet available.', 'info');
        } else if (c.type === 'globalRule') {
            this.openGlobalModal();
            this._setModalReadOnly('globalModal', 'Global Constraints');
        }
    },
    
    _setModalReadOnly(modalId, title) {
        setTimeout(() => {
            const modal = document.getElementById(modalId);
            if (!modal) return;
            
            // Update title
            const modalTitle = modal.querySelector('.modal-title');
            if (modalTitle) {
                modalTitle.innerHTML = `<i class="bi bi-eye"></i> View ${title}`;
            }
            
            // Disable all inputs
            modal.querySelectorAll('input, textarea, select').forEach(el => {
                el.disabled = true;
            });
            
            // Hide save buttons
            modal.querySelectorAll('.modal-footer .btn-primary').forEach(btn => {
                btn.style.display = 'none';
            });
        }, 100);
    },
    
    renderConstraintsList() {
        const container = document.getElementById('constraintsList');
        
        if (this.constraints.length === 0) {
            container.innerHTML = '<div class="text-muted small">No constraints defined yet</div>';
            return;
        }
        
        container.innerHTML = '';
        
        // Group constraints by category
        const categories = {
            cardinality: { title: '🔢 Cardinality', items: [], color: 'secondary' },
            value: { title: '✓ Value', items: [], color: 'secondary' },
            global: { title: '🌐 Global', items: [], color: 'secondary' }
        };
        
        this.constraints.forEach((c, index) => {
            const cardinalityTypes = ['minCardinality', 'maxCardinality', 'exactCardinality'];
            const valueTypes = ['valueCheck', 'entityValueCheck', 'entityLabelCheck', 'attributeConstraint'];
            const propertyChars = ['functional', 'inverseFunctional', 'transitive', 'symmetric', 'asymmetric', 'reflexive', 'irreflexive'];
            
            if (cardinalityTypes.includes(c.type)) {
                categories.cardinality.items.push({ constraint: c, index });
            } else if (valueTypes.includes(c.type)) {
                categories.value.items.push({ constraint: c, index });
            } else if (propertyChars.includes(c.type) || c.type === 'globalRule') {
                categories.global.items.push({ constraint: c, index });
            } else {
                // Log any uncategorized constraints for debugging
                console.warn('[Constraints] Uncategorized constraint type:', c.type, c);
            }
        });
        
        for (const [key, cat] of Object.entries(categories)) {
            if (cat.items.length === 0) continue;
            
            const section = document.createElement('div');
            section.className = 'mb-3';
            section.innerHTML = `<h6 class="text-${cat.color} mb-2">${cat.title}</h6>`;
            
            cat.items.forEach(({ constraint: c, index }) => {
                // Check if editing is allowed (active version only)
                const canEdit = window.isActiveVersion !== false;
                const actionButtons = canEdit ? `
                    <div class="btn-group btn-group-sm ontology-edit-btn">
                        <button class="btn btn-outline-primary" onclick="ConstraintsModule.editConstraint(${index})"><i class="bi bi-pencil"></i></button>
                        <button class="btn btn-outline-danger" onclick="ConstraintsModule.deleteConstraint(${index})"><i class="bi bi-trash"></i></button>
                    </div>
                ` : `
                    <button class="btn btn-sm btn-outline-secondary" onclick="ConstraintsModule.viewConstraint(${index})">
                        <i class="bi bi-eye"></i> View
                    </button>
                `;
                
                const card = document.createElement('div');
                card.className = 'card mb-2';
                card.innerHTML = `
                    <div class="card-body p-2">
                        <div class="d-flex justify-content-between align-items-center">
                            <div>
                                ${this.formatConstraintDisplay(c)}
                            </div>
                            ${actionButtons}
                        </div>
                    </div>
                `;
                section.appendChild(card);
            });
            
            container.appendChild(section);
        }
    },
    
    formatConstraintDisplay(c) {
        const getLocalName = (uri) => {
            if (!uri) return '';
            const idx = Math.max(uri.lastIndexOf('#'), uri.lastIndexOf('/'));
            return idx >= 0 ? uri.substring(idx + 1) : uri;
        };
        
        const labels = {
            'minCardinality': 'Min',
            'maxCardinality': 'Max',
            'exactCardinality': 'Exact',
            'functional': 'Functional',
            'inverseFunctional': 'Inverse Functional',
            'transitive': 'Transitive',
            'symmetric': 'Symmetric',
            'asymmetric': 'Asymmetric',
            'reflexive': 'Reflexive',
            'irreflexive': 'Irreflexive',
            'globalRule': 'Rule',
            'attributeConstraint': 'Attribute'
        };
        
        const checkTypeLabels = {
            'startsWith': 'starts with',
            'endsWith': 'ends with',
            'contains': 'contains',
            'equals': '=',
            'notEquals': '≠',
            'matches': 'matches',
            'notNull': 'not empty'
        };
        
        if (c.type === 'globalRule') {
            const ruleLabels = {
                'noOrphans': 'No Orphan Entities',
                'requireLabels': 'Require Labels',
                'uniqueIds': 'Unique Identifiers'
            };
            return `<span class="badge bg-secondary me-2">${ruleLabels[c.ruleName] || c.ruleName}</span>`;
        }
        
        if (c.type === 'valueCheck' || c.type === 'entityValueCheck') {
            const className = getLocalName(c.className);
            const attrName = c.attributeName || 'label';
            const checkLabel = checkTypeLabels[c.checkType] || c.checkType;
            const valueDisplay = c.checkType !== 'notNull' ? (c.checkValue || '') : '';
            return `<strong>${className}</strong>.<code>${attrName}</code> <span class="badge bg-secondary">${checkLabel}</span> ${valueDisplay ? `"${valueDisplay}"` : ''}`;
        }
        
        if (c.type === 'entityLabelCheck') {
            const className = getLocalName(c.className);
            const checkLabel = checkTypeLabels[c.checkType] || c.checkType;
            const valueDisplay = c.checkType !== 'notNull' ? (c.checkValue || '') : '';
            return `<strong>${className}</strong>.<code>label</code> <span class="badge bg-secondary">${checkLabel}</span> ${valueDisplay ? `"${valueDisplay}"` : ''}`;
        }
        
        if (c.type === 'attributeConstraint') {
            const attrLabel = c.attributeName || 'Unknown';
            const typeLabel = c.attributeConstraintType || 'constraint';
            return `<strong>${getLocalName(c.className)}</strong>.<code>${attrLabel}</code> <span class="badge bg-secondary">${typeLabel}</span>`;
        }
        
        const propName = getLocalName(c.property);
        const className = getLocalName(c.className);
        const label = labels[c.type] || c.type;
        
        if (['minCardinality', 'maxCardinality', 'exactCardinality'].includes(c.type)) {
            // Show: ClassName.relationship [Min/Max/Exact: N]
            return `<strong>${className}</strong>.<code>${propName}</code> <span class="badge bg-secondary">${label}: ${c.cardinalityValue}</span>`;
        }
        
        // For property characteristics (functional, transitive, etc.)
        return `<code>${propName}</code> <span class="badge bg-secondary">${label}</span>`;
    }
};

// Initialize when section becomes active
document.addEventListener('DOMContentLoaded', function() {
    if (document.getElementById('constraints-section')?.classList.contains('active')) {
        ConstraintsModule.init();
    }
    
    // Reset modals when closed (re-enable inputs after view mode)
    ['cardinalityModal', 'valueModal', 'globalModal'].forEach(modalId => {
        const modal = document.getElementById(modalId);
        if (modal) {
            modal.addEventListener('hidden.bs.modal', function() {
                // Re-enable all inputs
                modal.querySelectorAll('input, textarea, select').forEach(el => {
                    el.disabled = false;
                });
                // Show save buttons
                modal.querySelectorAll('.modal-footer .btn-primary').forEach(btn => {
                    btn.style.display = '';
                });
                // Reset title
                const titleMap = {
                    'cardinalityModal': '<i class="bi bi-123"></i> Relationship Constraints',
                    'valueModal': '<i class="bi bi-check2-square"></i> Value Constraint',
                    'globalModal': '<i class="bi bi-globe"></i> Global Constraints'
                };
                const modalTitle = modal.querySelector('.modal-title');
                if (modalTitle && titleMap[modalId]) {
                    modalTitle.innerHTML = titleMap[modalId];
                }
            });
        }
    });
});
