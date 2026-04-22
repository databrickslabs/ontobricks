/**
 * OntoBricks - mapping-relationship-modal.js
 * Extracted from mapping templates per code_instructions.txt
 */

// ==========================================================================
// RELATIONSHIP MAPPING MODAL SCRIPTS
// ==========================================================================

// Store available columns from query
let relQueryColumns = [];

// ==========================================================================
// RELATIONSHIP COLUMN MAPPING STATE
// ==========================================================================
const RelColumnMappingState = {
    columns: [],
    rows: [],
    sourceIdColumn: null,
    targetIdColumn: null,
    attributeMappings: {},  // { attributeName: columnName }
    relationshipAttributes: [],   // Available attributes from ontology
    
    reset: function() {
        this.columns = [];
        this.rows = [];
        this.sourceIdColumn = null;
        this.targetIdColumn = null;
        this.attributeMappings = {};
        this.relationshipAttributes = [];
    },
    
    setMapping: function(column, mappingType, attributeName = null) {
        // Clear previous mapping for this column
        if (this.sourceIdColumn === column) this.sourceIdColumn = null;
        if (this.targetIdColumn === column) this.targetIdColumn = null;
        Object.keys(this.attributeMappings).forEach(attr => {
            if (this.attributeMappings[attr] === column) {
                delete this.attributeMappings[attr];
            }
        });
        
        // Set new mapping
        if (mappingType === 'source') {
            this.sourceIdColumn = column;
        } else if (mappingType === 'target') {
            this.targetIdColumn = column;
        } else if (mappingType === 'attribute' && attributeName) {
            this.attributeMappings[attributeName] = column;
        }
        
        this.updateValidation();
        this.renderMappingTable();
    },
    
    getMappingForColumn: function(column) {
        if (this.sourceIdColumn === column) return { type: 'source', label: 'Source ID' };
        if (this.targetIdColumn === column) return { type: 'target', label: 'Target ID' };
        for (const [attr, col] of Object.entries(this.attributeMappings)) {
            if (col === column) return { type: 'attribute', label: attr };
        }
        return null;
    },
    
    updateValidation: function() {
        const statusEl = document.getElementById('relMappingStatus');
        const saveBtn = document.getElementById('saveRelationshipMapping');
        
        if (this.sourceIdColumn && this.targetIdColumn) {
            statusEl.innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> Source: ' + this.sourceIdColumn + ' | Target: ' + this.targetIdColumn + '</span>';
            saveBtn.disabled = false;
        } else if (this.sourceIdColumn) {
            statusEl.innerHTML = '<span class="text-warning"><i class="bi bi-exclamation-circle"></i> Target ID required</span>';
            saveBtn.disabled = true;
        } else if (this.targetIdColumn) {
            statusEl.innerHTML = '<span class="text-warning"><i class="bi bi-exclamation-circle"></i> Source ID required</span>';
            saveBtn.disabled = true;
        } else {
            statusEl.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> Source & Target required</span>';
            saveBtn.disabled = true;
        }
    },
    
    renderMappingTable: function() {
        const headerRow = document.getElementById('relResultsMappingHeader');
        const tbody = document.getElementById('relResultsMappingBody');
        const self = this;
        
        // Build header with mapping dropdowns
        headerRow.innerHTML = this.columns.map(col => {
            const mapping = this.getMappingForColumn(col);
            let badgeHtml = '';
            
            if (mapping) {
                if (mapping.type === 'source') {
                    badgeHtml = `<span class="badge bg-primary mapping-badge"><i class="bi bi-box-arrow-right"></i> Source</span>`;
                } else if (mapping.type === 'target') {
                    badgeHtml = `<span class="badge bg-success mapping-badge"><i class="bi bi-box-arrow-in-right"></i> Target</span>`;
                } else if (mapping.type === 'attribute') {
                    badgeHtml = `<span class="badge bg-secondary mapping-badge"><i class="bi bi-list"></i> ${mapping.label}</span>`;
                }
            } else {
                badgeHtml = `<span class="badge bg-light text-muted mapping-badge border">Click to map</span>`;
            }
            
            // Build dropdown menu
            const attrOptions = this.relationshipAttributes.map(attr => {
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
                        <div class="dropdown-item ${this.sourceIdColumn === col ? 'active' : ''}" data-action="source">
                            <i class="bi bi-box-arrow-right text-primary"></i> Set as Source ID
                        </div>
                        <div class="dropdown-item ${this.targetIdColumn === col ? 'active' : ''}" data-action="target">
                            <i class="bi bi-box-arrow-in-right text-success"></i> Set as Target ID
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
                if (action === 'source') {
                    self.setMapping(column, 'source');
                } else if (action === 'target') {
                    self.setMapping(column, 'target');
                } else if (action === 'attribute') {
                    self.setMapping(column, 'attribute', attr);
                } else if (action === 'clear') {
                    self.setMapping(column, 'none');
                }
            });
        });
    }
};

// ==========================================================================
// TAB NAVIGATOR
// ==========================================================================
const relTabNav = new TabNavigator({
    tabsContainerId: 'relMappingTabs',
    modalId: 'addRelationshipMappingModal',
    tabs: [
        { id: 'rel-wizard-tab', paneId: 'rel-wizard-pane', disabled: false },
        { id: 'rel-sql-tab', paneId: 'rel-sql-pane', disabled: false },
        { id: 'rel-mapping-tab', paneId: 'rel-mapping-pane', disabled: true }
    ]
});

// Helper functions
function enableRelTab(tabId) { relTabNav.enableTab(tabId); }
function switchToRelTab(tabId) { relTabNav.switchTo(tabId); }
function resetRelTabs() { 
    relTabNav.reset('rel-wizard-tab'); 
    RelColumnMappingState.reset();
}

// ==========================================================================
// SQL WIZARD
// ==========================================================================
const RelWizard = new SQLWizardBase({
    llmEndpointId: 'relWizardLlmEndpoint',
    tableListId: 'relWizardTableList',
    promptId: 'relWizardPrompt',
    promptOverlayId: 'relWizardPromptOverlay',
    schemaInfoId: 'relWizardSchemaInfo',
    tableCountId: 'relWizardTableCount',
    selectedCountId: 'relWizardSelectedCount',
    limitId: 'relWizardLimit',
    generateBtnId: 'relGenerateSqlBtn',
    resultId: 'relWizardResult',
    generatedSqlId: 'relWizardGeneratedSql',
    warningId: 'relWizardWarning',
    warningTextId: 'relWizardWarningText',
    errorId: 'relWizardError',
    errorTextId: 'relWizardErrorText',
    copyBtnId: 'relCopySqlBtn',
    useBtnId: 'relUseGeneratedSqlBtn',
    refreshBtnId: 'refreshRelEndpointsBtn',
    noMetadataWarningId: 'relWizardNoMetadata',
    mappingType: 'relationship',  // Specialized prompts for relationship mapping
    onUseSql: function(sql) {
        document.getElementById('modalRelSqlQuery').value = sql;
        switchToRelTab('rel-sql-tab');
        setTimeout(() => {
            document.getElementById('testQueryBtn').click();
        }, 100);
    }
});

// Setup wizard event listeners
RelWizard.setupEventListeners();

// Extension method for relationship-specific initialization
RelWizard.initWithRelationship = function(sourceEntity, targetEntity) {
    this.init(sourceEntity && targetEntity ? 
        `Provide the relationship identifiers and information between ${sourceEntity} and ${targetEntity}` : '');
};

// ==========================================================================
// HELPER FUNCTIONS
// ==========================================================================

function getLocalName(uri) {
    if (!uri) return '';
    return uri.split('#').pop().split('/').pop();
}

function populateRelationshipClassSelects() {
    const sourceSelect = document.getElementById('modalRelSourceClass');
    const targetSelect = document.getElementById('modalRelTargetClass');
    
    sourceSelect.innerHTML = '<option value="">Select source class...</option>';
    targetSelect.innerHTML = '<option value="">Select target class...</option>';
    
    MappingState.config.entities.forEach(mapping => {
        const optionSource = document.createElement('option');
        optionSource.value = mapping.table || mapping.ontology_class_label;
        optionSource.textContent = `${mapping.ontology_class_label}`;
        optionSource.setAttribute('data-catalog', mapping.catalog || '');
        optionSource.setAttribute('data-schema', mapping.schema || '');
        optionSource.setAttribute('data-class', mapping.ontology_class);
        sourceSelect.appendChild(optionSource);
        
        const optionTarget = document.createElement('option');
        optionTarget.value = mapping.table || mapping.ontology_class_label;
        optionTarget.textContent = `${mapping.ontology_class_label}`;
        optionTarget.setAttribute('data-catalog', mapping.catalog || '');
        optionTarget.setAttribute('data-schema', mapping.schema || '');
        optionTarget.setAttribute('data-class', mapping.ontology_class);
        targetSelect.appendChild(optionTarget);
    });
}

function updateSourceEntityInfo(tableValue) {
    const mapping = MappingState.config.entities.find(m => 
        m.table === tableValue || m.ontology_class_label === tableValue
    );
    if (mapping) {
        document.getElementById('sourceTableInfo').textContent = mapping.sql_query || `${mapping.catalog}.${mapping.schema}.${mapping.table}`;
        document.getElementById('sourcePKInfo').textContent = mapping.id_column;
    }
}

function updateTargetEntityInfo(tableValue) {
    const mapping = MappingState.config.entities.find(m => 
        m.table === tableValue || m.ontology_class_label === tableValue
    );
    if (mapping) {
        document.getElementById('targetTableInfo').textContent = mapping.sql_query || `${mapping.catalog}.${mapping.schema}.${mapping.table}`;
        document.getElementById('targetPKInfo').textContent = mapping.id_column;
    }
}

// ==========================================================================
// MODAL FUNCTIONS
// ==========================================================================

function openRelationshipMappingModal(propertyUri, propertyName) {
    document.getElementById('modalPropertyName').textContent = propertyName;
    document.getElementById('modalPropertyUri').value = propertyUri;
    
    populateRelationshipClassSelects();
    resetRelationshipModal();
    
    // Get ontology property info to determine source/target entities
    const propertyInfo = MappingState.loadedOntology.properties.find(p => p.uri === propertyUri);
    
    // Pre-select source and target entities from ontology domain/range
    if (propertyInfo) {
        const domainUri = propertyInfo.domain;
        const rangeUri = propertyInfo.range;
        const direction = propertyInfo.direction || 'forward';
        
        // Determine actual source and target based on direction
        const sourceUri = direction === 'reverse' ? rangeUri : domainUri;
        const targetUri = direction === 'reverse' ? domainUri : rangeUri;
        
        // Find matching entity mappings
        const sourceMapping = MappingState.config.entities.find(m => 
            m.ontology_class === sourceUri || 
            m.ontology_class_label === getLocalName(sourceUri)
        );
        const targetMapping = MappingState.config.entities.find(m => 
            m.ontology_class === targetUri || 
            m.ontology_class_label === getLocalName(targetUri)
        );
        
        // Set entity displays
        const sourceSelect = document.getElementById('modalRelSourceClass');
        const targetSelect = document.getElementById('modalRelTargetClass');
        const sourceDisplay = document.getElementById('sourceEntityDisplay');
        const targetDisplay = document.getElementById('targetEntityDisplay');
        
        if (sourceMapping) {
            sourceSelect.value = sourceMapping.table || sourceMapping.ontology_class_label;
            updateSourceEntityInfo(sourceSelect.value);
            if (sourceDisplay) {
                sourceDisplay.textContent = sourceMapping.ontology_class_label || getLocalName(sourceUri);
            }
        } else {
            if (sourceDisplay) {
                sourceDisplay.textContent = getLocalName(sourceUri) + ' (not mapped)';
                sourceDisplay.classList.add('text-warning');
            }
        }
        
        if (targetMapping) {
            targetSelect.value = targetMapping.table || targetMapping.ontology_class_label;
            updateTargetEntityInfo(targetSelect.value);
            if (targetDisplay) {
                targetDisplay.textContent = targetMapping.ontology_class_label || getLocalName(targetUri);
            }
        } else {
            if (targetDisplay) {
                targetDisplay.textContent = getLocalName(targetUri) + ' (not mapped)';
                targetDisplay.classList.add('text-warning');
            }
        }
        
        // Load relationship attributes
        RelColumnMappingState.relationshipAttributes = propertyInfo.properties || [];
    }
    
    // Initialize wizard with relationship context
    RelWizard.initWithRelationship(
        document.getElementById('sourceEntityDisplay').textContent,
        document.getElementById('targetEntityDisplay').textContent
    );
    
    const existingMapping = MappingState.config.relationships.find(m => m.property === propertyUri);
    
    if (existingMapping) {
        document.getElementById('modalRelSqlQuery').value = existingMapping.sql_query || '';
        
        if (existingMapping.sql_query && existingMapping.source_id_column && existingMapping.target_id_column) {
            // Pre-load column mapping state from existing mapping
            const savedColumns = [];
            if (existingMapping.attribute_mappings) {
                Object.values(existingMapping.attribute_mappings).forEach(col => {
                    if (col && !savedColumns.includes(col)) savedColumns.push(col);
                });
            }
            if (existingMapping.source_id_column && !savedColumns.includes(existingMapping.source_id_column)) {
                savedColumns.push(existingMapping.source_id_column);
            }
            if (existingMapping.target_id_column && !savedColumns.includes(existingMapping.target_id_column)) {
                savedColumns.push(existingMapping.target_id_column);
            }
            
            RelColumnMappingState.columns = savedColumns;
            RelColumnMappingState.sourceIdColumn = existingMapping.source_id_column || null;
            RelColumnMappingState.targetIdColumn = existingMapping.target_id_column || null;
            RelColumnMappingState.attributeMappings = existingMapping.attribute_mappings ? { ...existingMapping.attribute_mappings } : {};
            
            enableRelTab('rel-mapping-tab');
            document.getElementById('saveRelationshipMapping').disabled = false;
            showRelMappingSummary();
            // Open Mapping tab for existing mappings
            switchToRelTab('rel-mapping-tab');
            document.getElementById('queryStatus').innerHTML = '<span class="text-muted"><i class="bi bi-info-circle"></i> Run query to see results & edit mappings</span>';
        }
    } else {
        document.getElementById('modalRelSqlQuery').value = '';
        setTimeout(() => switchToRelTab('rel-wizard-tab'), 100);
    }
    
    const modal = new bootstrap.Modal(document.getElementById('addRelationshipMappingModal'));
    modal.show();
}

function resetRelationshipModal() {
    relQueryColumns = [];
    document.getElementById('queryStatus').textContent = '';
    document.getElementById('saveRelationshipMapping').disabled = true;
    RelColumnMappingState.reset();
    document.getElementById('relResultsMappingHeader').innerHTML = '';
    document.getElementById('relResultsMappingBody').innerHTML = '';
    document.getElementById('relQueryResultCount').textContent = '0 rows';
    document.getElementById('relMappingStatus').innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> Source & Target required</span>';
    document.getElementById('relMappingSummaryView').style.display = 'block';
    document.getElementById('relMappingGridView').style.display = 'none';
    
    const sourceDisplay = document.getElementById('sourceEntityDisplay');
    const targetDisplay = document.getElementById('targetEntityDisplay');
    if (sourceDisplay) {
        sourceDisplay.textContent = 'Source';
        sourceDisplay.classList.remove('text-warning');
    }
    if (targetDisplay) {
        targetDisplay.textContent = 'Target';
        targetDisplay.classList.remove('text-warning');
    }
    
    const mappingTab = document.getElementById('rel-mapping-tab');
    if (mappingTab) mappingTab.disabled = true;
    
    resetRelTabs();
}

function showRelMappingSummary() {
    document.getElementById('relMappingSummaryView').style.display = 'block';
    document.getElementById('relMappingGridView').style.display = 'none';
    
    const srcCol = RelColumnMappingState.sourceIdColumn;
    const tgtCol = RelColumnMappingState.targetIdColumn;
    const attrMappings = RelColumnMappingState.attributeMappings;
    
    document.getElementById('relSummarySourceColumn').textContent = srcCol || 'Not set';
    document.getElementById('relSummarySourceColumn').className = srcCol ? 'text-dark' : 'text-muted';
    
    document.getElementById('relSummaryTargetColumn').textContent = tgtCol || 'Not set';
    document.getElementById('relSummaryTargetColumn').className = tgtCol ? 'text-dark' : 'text-muted';
    
    const attrSection = document.getElementById('relSummaryAttributesSection');
    const attrList = document.getElementById('relSummaryAttributesList');
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

function showRelMappingGrid() {
    document.getElementById('relMappingSummaryView').style.display = 'none';
    document.getElementById('relMappingGridView').style.display = 'block';
}

function viewRelationshipMapping(propertyUri, propertyName) {
    openRelationshipMappingModal(propertyUri, propertyName);
    
    setTimeout(() => {
        const modal = document.getElementById('addRelationshipMappingModal');
        if (!modal) return;
        
        document.querySelector('#addRelationshipMappingModal .modal-title').innerHTML = 
            '<i class="bi bi-eye"></i> View Relationship Mapping: <span id="modalPropertyName">' + propertyName + '</span>';
        
        modal.querySelectorAll('input, textarea, select').forEach(el => el.disabled = true);
        
        ['saveRelationshipMapping', 'testQueryBtn', 'relGenerateSqlBtn', 'relUseGeneratedSqlBtn', 'relGoToSqlFromSummaryBtn'].forEach(id => {
            const btn = document.getElementById(id);
            if (btn) btn.style.display = 'none';
        });
        
        const wizardTab = document.getElementById('rel-wizard-tab');
        if (wizardTab) wizardTab.disabled = true;
        
        enableRelTab('rel-mapping-tab');
        switchToRelTab('rel-mapping-tab');
        showRelMappingSummary();
        
        const summaryBtn = document.getElementById('relGoToSqlFromSummaryBtn');
        if (summaryBtn) {
            summaryBtn.innerHTML = '<i class="bi bi-lock"></i> Read-only mode';
            summaryBtn.className = 'btn btn-outline-secondary btn-sm';
            summaryBtn.disabled = true;
            summaryBtn.style.display = 'inline-block';
        }
    }, 100);
}

async function removeRelationshipMappingByProperty(propertyUri) {
    const mapping = MappingState.config.relationships.find(m => m.property === propertyUri);
    if (!mapping) return;
    
    const confirmed = await showConfirmDialog({
        title: 'Remove Mapping',
        message: `Are you sure you want to remove the mapping for "<strong>${mapping.property_label}</strong>"?<br><br>The property will move back to the unmapped list.`,
        confirmText: 'Remove',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
    if (confirmed) {
        const idx = MappingState.config.relationships.indexOf(mapping);
        MappingState.config.relationships.splice(idx, 1);
        autoSaveMappings();
        showNotification(`Mapping for "${mapping.property_label}" removed`, 'info', 3000);
    }
}

// ==========================================================================
// EVENT LISTENERS
// ==========================================================================

// Test Query button
document.getElementById('testQueryBtn')?.addEventListener('click', async function() {
    const sqlQuery = document.getElementById('modalRelSqlQuery').value.trim();
    
    if (!sqlQuery) {
        showNotification('Please enter a SQL query first', 'warning');
        return;
    }
    
    const btn = this;
    const originalText = btn.innerHTML;
    btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Running...';
    btn.disabled = true;
    document.getElementById('queryStatus').textContent = 'Executing query...';
    
    try {
        const response = await fetch('/mapping/test-query', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({query: sqlQuery}),
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success) {
            RelColumnMappingState.columns = result.columns;
            RelColumnMappingState.rows = result.rows || [];
            
            const propertyUri = document.getElementById('modalPropertyUri').value;
            const existingMapping = MappingState.config.relationships.find(m => m.property === propertyUri);
            
            if (existingMapping) {
                if (existingMapping.source_id_column && result.columns.includes(existingMapping.source_id_column)) {
                    RelColumnMappingState.sourceIdColumn = existingMapping.source_id_column;
                }
                if (existingMapping.target_id_column && result.columns.includes(existingMapping.target_id_column)) {
                    RelColumnMappingState.targetIdColumn = existingMapping.target_id_column;
                }
                if (existingMapping.attribute_mappings) {
                    Object.entries(existingMapping.attribute_mappings).forEach(([attr, col]) => {
                        if (result.columns.includes(col)) {
                            RelColumnMappingState.attributeMappings[attr] = col;
                        }
                    });
                }
            }
            
            const propertyInfo = MappingState.loadedOntology?.properties?.find(p => p.uri === propertyUri);
            RelColumnMappingState.relationshipAttributes = propertyInfo?.properties || [];
            
            document.getElementById('relQueryResultCount').textContent = `${result.row_count} row(s)`;
            
            RelColumnMappingState.renderMappingTable();
            RelColumnMappingState.updateValidation();
            showRelMappingGrid();
            
            enableRelTab('rel-mapping-tab');
            switchToRelTab('rel-mapping-tab');
            
            document.getElementById('queryStatus').innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> Query successful</span>';
            showNotification(`Query returned ${result.row_count} row(s) with ${result.columns.length} column(s)`, 'success', 3000);
        } else {
            const reason = [result.message, result.detail].filter(Boolean).join(' — ');
            document.getElementById('queryStatus').innerHTML = `<span class="text-danger"><i class="bi bi-x-circle"></i> ${reason || 'Unknown error'}</span>`;
            showNotification('Query failed: ' + (reason || 'Unknown error'), 'error');
        }
    } catch (error) {
        document.getElementById('queryStatus').innerHTML = `<span class="text-danger"><i class="bi bi-x-circle"></i> Error: ${error.message}</span>`;
        showNotification('Error testing query: ' + error.message, 'error');
    } finally {
        btn.innerHTML = originalText;
        btn.disabled = false;
    }
});

// Navigation buttons
document.getElementById('relBackToSqlBtn')?.addEventListener('click', () => switchToRelTab('rel-sql-tab'));

document.getElementById('relGoToSqlFromSummaryBtn')?.addEventListener('click', function() {
    switchToRelTab('rel-sql-tab');
    const sqlQuery = document.getElementById('modalRelSqlQuery').value.trim();
    if (sqlQuery) {
        document.getElementById('testQueryBtn').click();
    }
});

// Save relationship mapping
document.getElementById('saveRelationshipMapping')?.addEventListener('click', function() {
    const propertyUri = document.getElementById('modalPropertyUri').value;
    const sqlQueryRaw = document.getElementById('modalRelSqlQuery').value.trim();
    
    // Strip LIMIT clause - it's only for preview, not for the final saved query
    const sqlQuery = stripLimitClause(sqlQueryRaw);
    
    if (!sqlQuery) {
        showNotification('Please provide a SQL query', 'warning');
        return;
    }
    
    if (!RelColumnMappingState.sourceIdColumn || !RelColumnMappingState.targetIdColumn) {
        showNotification('Please select both Source ID and Target ID columns', 'warning');
        switchToRelTab('rel-mapping-tab');
        return;
    }
    
    const sourceOption = document.getElementById('modalRelSourceClass').selectedOptions[0];
    const targetOption = document.getElementById('modalRelTargetClass').selectedOptions[0];
    const propertyInfo = MappingState.loadedOntology.properties.find(p => p.uri === propertyUri);
    const propertyLabel = propertyInfo ? (propertyInfo.label || propertyInfo.name) : 'Unknown';
    
    const sourceClassLabel = sourceOption ? sourceOption.textContent : document.getElementById('sourceEntityDisplay').textContent;
    const targetClassLabel = targetOption ? targetOption.textContent : document.getElementById('targetEntityDisplay').textContent;
    
    const direction = propertyInfo ? (propertyInfo.direction || 'forward') : 'forward';
    
    const existingIndex = MappingState.config.relationships.findIndex(m => m.property === propertyUri);
    
    const newMapping = {
        source_table: sourceOption?.value || '',
        source_catalog: sourceOption?.getAttribute('data-catalog') || '',
        source_schema: sourceOption?.getAttribute('data-schema') || '',
        source_class: sourceOption?.getAttribute('data-class') || '',
        source_class_label: sourceClassLabel,
        source_id_column: RelColumnMappingState.sourceIdColumn,
        target_table: targetOption?.value || '',
        target_catalog: targetOption?.getAttribute('data-catalog') || '',
        target_schema: targetOption?.getAttribute('data-schema') || '',
        target_class: targetOption?.getAttribute('data-class') || '',
        target_class_label: targetClassLabel,
        target_id_column: RelColumnMappingState.targetIdColumn,
        property: propertyUri,
        property_label: propertyLabel,
        sql_query: sqlQuery,
        attribute_mappings: { ...RelColumnMappingState.attributeMappings },
        direction: direction
    };
    
    if (existingIndex >= 0) {
        MappingState.config.relationships[existingIndex] = newMapping;
        showNotification(`Mapping for "${propertyLabel}" updated successfully`, 'success', 2000);
    } else {
        MappingState.config.relationships.push(newMapping);
        showNotification(`Mapping created for "${propertyLabel}"`, 'success', 2000);
    }
    
    autoSaveMappings();
    resetRelationshipModal();
    
    const modal = bootstrap.Modal.getInstance(document.getElementById('addRelationshipMappingModal'));
    if (modal) modal.hide();
});

// Reset modal when closed
document.getElementById('addRelationshipMappingModal')?.addEventListener('hidden.bs.modal', function() {
    const modal = this;
    
    modal.querySelectorAll('input, textarea, select').forEach(el => el.disabled = false);
    
    ['saveRelationshipMapping', 'testQueryBtn', 'relGenerateSqlBtn', 'relUseGeneratedSqlBtn', 'relGoToSqlFromSummaryBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) btn.style.display = '';
    });
    
    const wizardTab = document.getElementById('rel-wizard-tab');
    if (wizardTab) wizardTab.disabled = false;
    
    const summaryBtn = document.getElementById('relGoToSqlFromSummaryBtn');
    if (summaryBtn) {
        summaryBtn.innerHTML = '<i class="bi bi-play-fill"></i> Run Query to Edit Mappings';
        summaryBtn.className = 'btn btn-dark btn-sm';
        summaryBtn.disabled = false;
    }
    
    document.querySelector('#addRelationshipMappingModal .modal-title').innerHTML = 
        '<i class="bi bi-arrow-left-right"></i> Map Relationship: <span id="modalPropertyName"></span>';
});

// ==========================================================================
// EXPOSE FUNCTIONS TO GLOBAL SCOPE
// ==========================================================================
window.openRelationshipMappingModal = openRelationshipMappingModal;
window.viewRelationshipMapping = viewRelationshipMapping;
window.removeRelationshipMappingByProperty = removeRelationshipMappingByProperty;
window.populateRelationshipClassSelects = populateRelationshipClassSelects;
