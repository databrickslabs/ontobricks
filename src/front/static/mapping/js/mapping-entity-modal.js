/**
 * OntoBricks - mapping-entity-modal.js
 * Extracted from mapping templates per code_instructions.txt
 */

// ==========================================================================
// ENTITY MAPPING MODAL SCRIPTS
// ==========================================================================

// Store available columns from query
let entityQueryColumns = [];

// ==========================================================================
// ENTITY COLUMN MAPPING STATE
// ==========================================================================
const EntityColumnMappingState = {
    columns: [],
    rows: [],
    idColumn: null,
    labelColumn: null,
    attributeMappings: {},  // { attributeName: columnName }
    entityAttributes: [],   // Available attributes from ontology
    
    reset: function() {
        this.columns = [];
        this.rows = [];
        this.idColumn = null;
        this.labelColumn = null;
        this.attributeMappings = {};
        this.entityAttributes = [];
    },
    
    setMapping: function(column, mappingType, attributeName = null) {
        // Clear previous mapping for this column
        if (this.idColumn === column) this.idColumn = null;
        if (this.labelColumn === column) this.labelColumn = null;
        Object.keys(this.attributeMappings).forEach(attr => {
            if (this.attributeMappings[attr] === column) {
                delete this.attributeMappings[attr];
            }
        });
        
        // Set new mapping
        if (mappingType === 'id') {
            this.idColumn = column;
        } else if (mappingType === 'label') {
            this.labelColumn = column;
        } else if (mappingType === 'attribute' && attributeName) {
            this.attributeMappings[attributeName] = column;
        }
        
        this.updateValidation();
        this.renderMappingTable();
    },
    
    getMappingForColumn: function(column) {
        if (this.idColumn === column) return { type: 'id', label: 'ID' };
        if (this.labelColumn === column) return { type: 'label', label: 'Label' };
        for (const [attr, col] of Object.entries(this.attributeMappings)) {
            if (col === column) return { type: 'attribute', label: attr };
        }
        return null;
    },
    
    updateValidation: function() {
        const statusEl = document.getElementById('idMappingStatus');
        const saveBtn = document.getElementById('saveEntityMapping');
        
        if (this.idColumn) {
            statusEl.innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> ID: ' + this.idColumn + '</span>';
            saveBtn.disabled = false;
        } else {
            statusEl.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> ID column required</span>';
            saveBtn.disabled = true;
        }
    },
    
    renderMappingTable: function() {
        const headerRow = document.getElementById('entityResultsMappingHeader');
        const tbody = document.getElementById('entityResultsMappingBody');
        const self = this;
        
        // Build header with mapping dropdowns
        headerRow.innerHTML = this.columns.map(col => {
            const mapping = this.getMappingForColumn(col);
            let badgeHtml = '';
            
            if (mapping) {
                if (mapping.type === 'id') {
                    badgeHtml = `<span class="badge bg-primary mapping-badge"><i class="bi bi-key"></i> ID</span>`;
                } else if (mapping.type === 'label') {
                    badgeHtml = `<span class="badge bg-info mapping-badge"><i class="bi bi-tag"></i> Label</span>`;
                } else if (mapping.type === 'attribute') {
                    badgeHtml = `<span class="badge bg-secondary mapping-badge"><i class="bi bi-list"></i> ${mapping.label}</span>`;
                }
            } else {
                badgeHtml = `<span class="badge bg-light text-muted mapping-badge border">Click to map</span>`;
            }
            
            // Build dropdown menu
            const attrOptions = this.entityAttributes.map(attr => {
                const attrName = attr.name || attr.localName || attr;
                const isSelected = this.attributeMappings[attrName] === col;
                return `<div class="dropdown-item ${isSelected ? 'active' : ''}" data-action="attribute" data-attr="${attrName}">
                    <i class="bi bi-list"></i> ${attrName}
                </div>`;
            }).join('');
            
            return `
                <th data-column="${col}">
                    <span class="column-name">${col}</span>
                    <div class="column-mapping">${badgeHtml}</div>
                    <div class="mapping-dropdown">
                        <div class="dropdown-item ${this.idColumn === col ? 'active' : ''}" data-action="id">
                            <i class="bi bi-key text-primary"></i> Set as ID
                        </div>
                        <div class="dropdown-item ${this.labelColumn === col ? 'active' : ''}" data-action="label">
                            <i class="bi bi-tag text-info"></i> Set as Label
                        </div>
                        <hr class="my-1">
                        ${attrOptions.length > 0 ? attrOptions : '<div class="dropdown-item text-muted disabled">No attributes defined</div>'}
                        <hr class="my-1">
                        <div class="dropdown-item text-danger" data-action="clear">
                            <i class="bi bi-x-circle"></i> Clear mapping
                        </div>
                    </div>
                </th>
            `;
        }).join('');
        
        // Build data rows
        if (this.rows.length > 0) {
            tbody.innerHTML = this.rows.map(row => {
                return '<tr>' + this.columns.map(col => {
                    const val = row[col];
                    return `<td>${val !== null && val !== undefined ? val : '<em class="text-muted">null</em>'}</td>`;
                }).join('') + '</tr>';
            }).join('');
        } else {
            tbody.innerHTML = `<tr><td colspan="${this.columns.length}" class="text-center text-muted">No data</td></tr>`;
        }
        
        // Attach click handlers to headers
        headerRow.querySelectorAll('th').forEach(th => {
            const col = th.dataset.column;
            renderMappingTableDropdown(th, col, self, (column, action, attr) => {
                if (action === 'id') {
                    self.setMapping(column, 'id');
                } else if (action === 'label') {
                    self.setMapping(column, 'label');
                } else if (action === 'attribute') {
                    self.setMapping(column, 'attribute', attr);
                } else if (action === 'clear') {
                    self.setMapping(column, 'none');
                }
            });
        });
    }
};

// Alias for backward compatibility
const ColumnMappingState = EntityColumnMappingState;

// ==========================================================================
// TAB NAVIGATOR
// ==========================================================================
const entityTabNav = new TabNavigator({
    tabsContainerId: 'entityMappingTabs',
    modalId: 'addEntityMappingModal',
    tabs: [
        { id: 'wizard-tab', paneId: 'wizard-pane', disabled: false },
        { id: 'step1-tab', paneId: 'step1-pane', disabled: false },
        { id: 'results-tab', paneId: 'results-pane', disabled: true }
    ]
});

// Helper functions for backward compatibility
function enableEntityTab(tabId) { entityTabNav.enableTab(tabId); }
function switchToEntityTab(tabId) { entityTabNav.switchTo(tabId); }
function resetEntityTabs() { 
    entityTabNav.reset('wizard-tab'); 
    EntityColumnMappingState.reset();
}

// ==========================================================================
// SQL WIZARD (Metadata-based)
// ==========================================================================
const EntityWizard = new SQLWizardBase({
    llmEndpointId: 'entityWizardLlmEndpoint',
    tableListId: 'entityWizardTableList',
    promptId: 'entityWizardPrompt',
    promptOverlayId: 'entityWizardPromptOverlay',
    schemaInfoId: 'entityWizardSchemaInfo',
    tableCountId: 'entityWizardTableCount',
    selectedCountId: 'entityWizardSelectedCount',
    limitId: 'entityWizardLimit',
    generateBtnId: 'entityGenerateSqlBtn',
    resultId: 'entityWizardResult',
    generatedSqlId: 'entityWizardGeneratedSql',
    warningId: 'entityWizardWarning',
    warningTextId: 'entityWizardWarningText',
    errorId: 'entityWizardError',
    errorTextId: 'entityWizardErrorText',
    copyBtnId: 'entityCopySqlBtn',
    useBtnId: 'entityUseGeneratedSqlBtn',
    refreshBtnId: 'refreshEntityEndpointsBtn',
    noMetadataWarningId: 'entityWizardNoMetadata',
    mappingType: 'entity',  // Entity mapping type for context
    onUseSql: function(sql) {
        document.getElementById('modalEntitySqlQuery').value = sql;
        switchToEntityTab('step1-tab');
        setTimeout(() => {
            document.getElementById('testEntityQueryBtn').click();
        }, 100);
    }
});

// Setup wizard event listeners
EntityWizard.setupEventListeners();

// Extension method for entity-specific initialization
EntityWizard.initWithEntity = function(entityName) {
    this.init(entityName ? `List all the ${entityName}` : '');
};

// ==========================================================================
// MODAL FUNCTIONS
// ==========================================================================

function openEntityMappingModal(classUri, className) {
    document.getElementById('modalEntityName').textContent = className;
    document.getElementById('modalEntityClass').value = classUri;
    
    resetEntityModal();
    EntityWizard.initWithEntity(className);
    
    const existingMapping = MappingState.config.entities.find(m => m.ontology_class === classUri);
    
    if (existingMapping) {
        document.getElementById('modalEntitySqlQuery').value = existingMapping.sql_query || '';
        
        if (existingMapping.sql_query) {
            // Pre-load column mapping state from existing mapping
            const savedColumns = [];
            if (existingMapping.attribute_mappings) {
                Object.values(existingMapping.attribute_mappings).forEach(col => {
                    if (col && !savedColumns.includes(col)) savedColumns.push(col);
                });
            }
            if (existingMapping.id_column && !savedColumns.includes(existingMapping.id_column)) {
                savedColumns.push(existingMapping.id_column);
            }
            if (existingMapping.label_column && !savedColumns.includes(existingMapping.label_column)) {
                savedColumns.push(existingMapping.label_column);
            }
            
            EntityColumnMappingState.columns = savedColumns;
            EntityColumnMappingState.idColumn = existingMapping.id_column || null;
            EntityColumnMappingState.labelColumn = existingMapping.label_column || null;
            EntityColumnMappingState.attributeMappings = existingMapping.attribute_mappings ? { ...existingMapping.attribute_mappings } : {};
            
            const classInfo = MappingState.loadedOntology?.classes?.find(c => c.uri === classUri);
            EntityColumnMappingState.entityAttributes = classInfo?.dataProperties || [];
            
            enableEntityTab('results-tab');
            document.getElementById('saveEntityMapping').disabled = false;
            showMappingSummary();
            // Open Mapping tab for existing mappings
            switchToEntityTab('results-tab');
            document.getElementById('entityQueryStatus').innerHTML = '<span class="text-muted"><i class="bi bi-info-circle"></i> Run query to see results & edit mappings</span>';
        }
    } else {
        document.getElementById('modalEntitySqlQuery').value = '';
        setTimeout(() => switchToEntityTab('wizard-tab'), 100);
    }
    
    const modal = new bootstrap.Modal(document.getElementById('addEntityMappingModal'));
    modal.show();
}

function resetEntityModal() {
    entityQueryColumns = [];
    document.getElementById('entityQueryStatus').textContent = '';
    document.getElementById('saveEntityMapping').disabled = true;
    EntityColumnMappingState.reset();
    document.getElementById('entityResultsMappingHeader').innerHTML = '';
    document.getElementById('entityResultsMappingBody').innerHTML = '';
    document.getElementById('entityQueryResultCount').textContent = '0 rows';
    document.getElementById('idMappingStatus').innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> ID not set</span>';
    document.getElementById('mappingSummaryView').style.display = 'block';
    document.getElementById('mappingGridView').style.display = 'none';
    const resultsTab = document.getElementById('results-tab');
    if (resultsTab) resultsTab.disabled = true;
    resetEntityTabs();
}

function showMappingSummary() {
    document.getElementById('mappingSummaryView').style.display = 'block';
    document.getElementById('mappingGridView').style.display = 'none';
    
    const idCol = EntityColumnMappingState.idColumn;
    const labelCol = EntityColumnMappingState.labelColumn;
    const attrMappings = EntityColumnMappingState.attributeMappings;
    
    document.getElementById('summaryIdColumn').textContent = idCol || 'Not set';
    document.getElementById('summaryIdColumn').className = idCol ? 'text-dark' : 'text-muted';
    
    document.getElementById('summaryLabelColumn').textContent = labelCol || 'Not set';
    document.getElementById('summaryLabelColumn').className = labelCol ? 'text-dark' : 'text-muted';
    
    const attrSection = document.getElementById('summaryAttributesSection');
    const attrList = document.getElementById('summaryAttributesList');
    const attrEntries = Object.entries(attrMappings);
    
    if (attrEntries.length > 0) {
        attrSection.style.display = 'block';
        attrList.innerHTML = attrEntries.map(([attr, col]) => 
            `<span class="badge bg-light text-dark border"><i class="bi bi-list"></i> ${attr} → ${col}</span>`
        ).join('');
    } else {
        attrSection.style.display = 'none';
    }
}

function showMappingGrid() {
    document.getElementById('mappingSummaryView').style.display = 'none';
    document.getElementById('mappingGridView').style.display = 'block';
}

function viewEntityMapping(classUri, className) {
    openEntityMappingModal(classUri, className);
    
    setTimeout(() => {
        const modal = document.getElementById('addEntityMappingModal');
        if (!modal) return;
        
        document.querySelector('#addEntityMappingModal .modal-title').innerHTML = 
            '<i class="bi bi-eye"></i> View Entity Mapping: <span id="modalEntityName">' + className + '</span>';
        
        modal.querySelectorAll('input, textarea, select').forEach(el => el.disabled = true);
        
        ['saveEntityMapping', 'testEntityQueryBtn', 'entityGenerateSqlBtn', 'entityUseGeneratedSqlBtn', 'goToSqlFromSummaryBtn'].forEach(id => {
            const btn = document.getElementById(id);
            if (btn) btn.style.display = 'none';
        });
        
        const wizardTab = document.getElementById('wizard-tab');
        if (wizardTab) wizardTab.disabled = true;
        
        enableEntityTab('results-tab');
        switchToEntityTab('results-tab');
        showMappingSummary();
        
        const summaryBtn = document.getElementById('goToSqlFromSummaryBtn');
        if (summaryBtn) {
            summaryBtn.innerHTML = '<i class="bi bi-lock"></i> Read-only mode';
            summaryBtn.className = 'btn btn-outline-secondary btn-sm';
            summaryBtn.disabled = true;
            summaryBtn.style.display = 'inline-block';
        }
    }, 100);
}

async function removeEntityMappingByClass(classUri) {
    const mapping = MappingState.config.entities.find(m => m.ontology_class === classUri);
    if (!mapping) return;
    
    const confirmed = await showConfirmDialog({
        title: 'Remove Mapping',
        message: `Are you sure you want to remove the mapping for "<strong>${mapping.ontology_class_label}</strong>"?<br><br>The entity will move back to the unmapped list.`,
        confirmText: 'Remove',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
    if (confirmed) {
        const idx = MappingState.config.entities.indexOf(mapping);
        MappingState.config.entities.splice(idx, 1);
        autoSaveMappings();
        showNotification(`Mapping for "${mapping.ontology_class_label}" removed`, 'info', 3000);
    }
}

// ==========================================================================
// EVENT LISTENERS
// ==========================================================================

// Test Query button
document.getElementById('testEntityQueryBtn')?.addEventListener('click', async function() {
    const sqlQuery = document.getElementById('modalEntitySqlQuery').value.trim();
    
    if (!sqlQuery) {
        showNotification('Please enter a SQL query first', 'warning');
        return;
    }
    
    const btn = this;
    const originalText = btn.innerHTML;
    btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Running...';
    btn.disabled = true;
    document.getElementById('entityQueryStatus').textContent = 'Executing query...';
    
    try {
        const response = await fetch('/mapping/test-query', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({query: sqlQuery}),
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success) {
            EntityColumnMappingState.columns = result.columns;
            EntityColumnMappingState.rows = result.rows || [];
            
            const classUri = document.getElementById('modalEntityClass').value;
            const classInfo = MappingState.loadedOntology?.classes?.find(c => c.uri === classUri);
            EntityColumnMappingState.entityAttributes = classInfo?.dataProperties || [];
            
            const existingMapping = MappingState.config.entities.find(m => m.ontology_class === classUri);
            if (existingMapping) {
                if (existingMapping.id_column && result.columns.includes(existingMapping.id_column)) {
                    EntityColumnMappingState.idColumn = existingMapping.id_column;
                }
                if (existingMapping.label_column && result.columns.includes(existingMapping.label_column)) {
                    EntityColumnMappingState.labelColumn = existingMapping.label_column;
                }
                if (existingMapping.attribute_mappings) {
                    Object.entries(existingMapping.attribute_mappings).forEach(([attr, col]) => {
                        if (result.columns.includes(col)) {
                            EntityColumnMappingState.attributeMappings[attr] = col;
                        }
                    });
                }
            }
            
            document.getElementById('entityQueryResultCount').textContent = `${result.row_count} row(s)`;
            
            EntityColumnMappingState.renderMappingTable();
            EntityColumnMappingState.updateValidation();
            showMappingGrid();
            
            enableEntityTab('results-tab');
            switchToEntityTab('results-tab');
            
            document.getElementById('entityQueryStatus').innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> Query successful</span>';
            showNotification(`Query returned ${result.row_count} row(s) with ${result.columns.length} column(s)`, 'success', 3000);
        } else {
            document.getElementById('entityQueryStatus').innerHTML = `<span class="text-danger"><i class="bi bi-x-circle"></i> ${result.message}</span>`;
            showNotification('Query failed: ' + result.message, 'error');
        }
    } catch (error) {
        document.getElementById('entityQueryStatus').innerHTML = `<span class="text-danger"><i class="bi bi-x-circle"></i> Error: ${error.message}</span>`;
        showNotification('Error testing query: ' + error.message, 'error');
    } finally {
        btn.innerHTML = originalText;
        btn.disabled = false;
    }
});

// Navigation buttons
document.getElementById('backToSqlBtn')?.addEventListener('click', () => switchToEntityTab('step1-tab'));

document.getElementById('goToSqlFromSummaryBtn')?.addEventListener('click', function() {
    switchToEntityTab('step1-tab');
    const sqlQuery = document.getElementById('modalEntitySqlQuery').value.trim();
    if (sqlQuery) {
        document.getElementById('testEntityQueryBtn').click();
    }
});

// Save entity mapping
document.getElementById('saveEntityMapping')?.addEventListener('click', function() {
    const ontologyClass = document.getElementById('modalEntityClass').value;
    const sqlQueryRaw = document.getElementById('modalEntitySqlQuery').value.trim();
    
    // Strip LIMIT clause - it's only for preview, not for the final saved query
    const sqlQuery = stripLimitClause(sqlQueryRaw);
    
    if (!sqlQuery) {
        showNotification('Please provide a SQL query', 'warning');
        return;
    }
    
    if (!EntityColumnMappingState.idColumn) {
        showNotification('Please select an ID column', 'warning');
        switchToEntityTab('results-tab');
        return;
    }
    
    const classInfo = MappingState.loadedOntology.classes.find(c => c.uri === ontologyClass);
    const classLabel = classInfo ? (classInfo.label || classInfo.name) : 'Unknown';
    
    const existingIndex = MappingState.config.entities.findIndex(m => m.ontology_class === ontologyClass);
    
    const newMapping = {
        ontology_class: ontologyClass,
        ontology_class_label: classLabel,
        sql_query: sqlQuery,
        id_column: EntityColumnMappingState.idColumn,
        label_column: EntityColumnMappingState.labelColumn || null,
        attribute_mappings: { ...EntityColumnMappingState.attributeMappings }
    };
    
    if (existingIndex >= 0) {
        MappingState.config.entities[existingIndex] = newMapping;
        showNotification(`Mapping for "${classLabel}" updated successfully`, 'success', 2000);
    } else {
        MappingState.config.entities.push(newMapping);
        showNotification(`Mapping created for "${classLabel}"`, 'success', 2000);
    }
    
    autoSaveMappings();
    resetEntityModal();
    
    const modal = bootstrap.Modal.getInstance(document.getElementById('addEntityMappingModal'));
    if (modal) modal.hide();
});

// Reset modal when closed
document.getElementById('addEntityMappingModal')?.addEventListener('hidden.bs.modal', function() {
    const modal = this;
    
    modal.querySelectorAll('input, textarea, select').forEach(el => el.disabled = false);
    
    ['saveEntityMapping', 'testEntityQueryBtn', 'entityGenerateSqlBtn', 'entityUseGeneratedSqlBtn', 'goToSqlFromSummaryBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) btn.style.display = '';
    });
    
    const wizardTab = document.getElementById('wizard-tab');
    if (wizardTab) wizardTab.disabled = false;
    
    const summaryBtn = document.getElementById('goToSqlFromSummaryBtn');
    if (summaryBtn) {
        summaryBtn.innerHTML = '<i class="bi bi-play-fill"></i> Run Query to Edit Mappings';
        summaryBtn.className = 'btn btn-dark btn-sm';
        summaryBtn.disabled = false;
    }
    
    document.querySelector('#addEntityMappingModal .modal-title').innerHTML = 
        '<i class="bi bi-box"></i> Map Entity: <span id="modalEntityName"></span>';
});

// ==========================================================================
// EXPOSE FUNCTIONS TO GLOBAL SCOPE
// ==========================================================================
window.openEntityMappingModal = openEntityMappingModal;
window.viewEntityMapping = viewEntityMapping;
window.removeEntityMappingByClass = removeEntityMappingByClass;
