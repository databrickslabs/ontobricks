/**
 * OntoBricks - Manual Mapping Module
 * Tree view for manual mapping management with bottom panel
 * Reuses the same panel content functions from mapping-design.js
 */

window.ManualModule = {
    initialized: false,
    currentItem: null, // { type: 'entity'|'relationship', uri: string, name: string }
    
    /**
     * Initialize the module
     */
    init: function() {
        // Always refresh if MappingState has ontology data
        // This handles the case where init was called before data was ready
        if (this.initialized && MappingState.loadedOntology) {
            this.refresh();
            return;
        }
        
        // Check if MappingState is ready
        if (!MappingState.initialized || !MappingState.loadedOntology) {
            console.log('ManualModule: Waiting for MappingState to be ready...');
            // Don't mark as initialized yet
            return;
        }
        
        this.initialized = true;
        this.refresh();
    },
    
    /**
     * Refresh the tree view
     */
    refresh: function() {
        this.buildTree();
    },
    
    /**
     * Build and render the mapping tree
     */
    buildTree: function() {
        const container = document.getElementById('manualAssignmentTree');
        if (!container) return;
        
        const ontology = MappingState.loadedOntology || {};
        const classes = ontology.classes || [];
        const allProperties = ontology.properties || [];
        const entityMappings = MappingState.config?.entities || [];
        const relationshipMappings = MappingState.config?.relationships || [];
        
        // Filter to only show ObjectProperties (relationships), not DatatypeProperties (attributes)
        const properties = allProperties.filter(prop => {
            // If type is specified, check if it's ObjectProperty
            if (prop.type) {
                return prop.type === 'ObjectProperty' || prop.type === 'owl:ObjectProperty';
            }
            // If no type specified, check if range looks like a class (not xsd: datatype)
            if (prop.range) {
                const range = prop.range.toLowerCase();
                // Exclude if range is an XSD datatype
                if (range.startsWith('xsd:') || range.includes('string') || range.includes('integer') || 
                    range.includes('decimal') || range.includes('date') || range.includes('boolean') ||
                    range.includes('float') || range.includes('double') || range.includes('time')) {
                    return false;
                }
            }
            return true;
        });
        
        if (classes.length === 0 && properties.length === 0) {
            container.innerHTML = '<div class="text-muted small">No ontology items found. Please ensure an ontology is loaded.</div>';
            return;
        }
        
        // Build set of excluded entity names for inherited relationship exclusion
        const excludedClassNames = new Set(
            classes.filter(c => c.excluded).map(c => c.name || c.localName || '')
        );

        // Categorize entities
        const assignedEntities = [];
        const notAssignedEntities = [];
        const excludedEntities = [];
        
        classes.forEach(cls => {
            const mapping = entityMappings.find(m => m.ontology_class === cls.uri);
            const item = {
                uri: cls.uri,
                name: cls.name || cls.localName || cls.uri.split('#').pop(),
                label: cls.label || cls.name,
                emoji: cls.emoji || '📦',
                mapped: !!mapping && !!mapping.sql_query
            };
            if (cls.excluded) {
                excludedEntities.push(item);
            } else if (item.mapped) {
                assignedEntities.push(item);
            } else {
                notAssignedEntities.push(item);
            }
        });
        
        // Categorize relationships
        const assignedRelationships = [];
        const notAssignedRelationships = [];
        const excludedRelationships = [];
        
        properties.forEach(prop => {
            const mapping = relationshipMappings.find(m => m.property === prop.uri);
            const isExcluded = prop.excluded || excludedClassNames.has(prop.domain) || excludedClassNames.has(prop.range);
            const item = {
                uri: prop.uri,
                name: prop.name || prop.localName || prop.uri.split('#').pop(),
                label: prop.label || prop.name,
                domain: prop.domain,
                range: prop.range,
                direction: prop.direction || 'forward',
                mapped: !!mapping && !!mapping.sql_query
            };
            if (isExcluded) {
                excludedRelationships.push(item);
            } else if (item.mapped) {
                assignedRelationships.push(item);
            } else {
                notAssignedRelationships.push(item);
            }
        });
        
        // Sort alphabetically
        const sortByName = (a, b) => a.name.localeCompare(b.name);
        assignedEntities.sort(sortByName);
        notAssignedEntities.sort(sortByName);
        excludedEntities.sort(sortByName);
        assignedRelationships.sort(sortByName);
        notAssignedRelationships.sort(sortByName);
        excludedRelationships.sort(sortByName);
        
        // Build tree HTML
        let html = '<ul class="manual-tree-list">';
        
        // Assigned root node
        const assignedTotal = assignedEntities.length + assignedRelationships.length;
        html += this.renderRootNode('assigned', '✅ Mapped', assignedTotal, assignedEntities, assignedRelationships);
        
        // Not Assigned root node
        const notAssignedTotal = notAssignedEntities.length + notAssignedRelationships.length;
        html += this.renderRootNode('not-assigned', '❌ Not Mapped', notAssignedTotal, notAssignedEntities, notAssignedRelationships);

        // Excluded root node
        const excludedTotal = excludedEntities.length + excludedRelationships.length;
        html += this.renderRootNode('excluded', '🚫 Excluded', excludedTotal, excludedEntities, excludedRelationships);
        
        html += '</ul>';
        
        container.innerHTML = html;
        
        // Add event listeners
        this.attachEventListeners(container);
    },
    
    /**
     * Render a root node (Assigned or Not Assigned)
     */
    renderRootNode: function(status, label, totalCount, entities, relationships) {
        let html = `
            <li class="manual-tree-node root-node ${status}" data-status="${status}">
                <div class="manual-tree-node-content">
                    <button type="button" class="manual-tree-toggle" data-expanded="true">
                        <i class="bi bi-chevron-down"></i>
                    </button>
                    <span class="manual-tree-label">
                        <span class="item-name">${label}</span>
                        <span class="manual-count-badge">${totalCount}</span>
                    </span>
                </div>
                <div class="manual-tree-children">
                    <ul class="manual-tree-list nested">
        `;
        
        // Entities category
        html += this.renderCategoryNode('entities', 'Entities', entities.length, entities, status);
        
        // Relationships category
        html += this.renderCategoryNode('relationships', 'Relationships', relationships.length, relationships, status);
        
        html += `
                    </ul>
                </div>
            </li>
        `;
        
        return html;
    },
    
    /**
     * Render a category node (Entities or Relationships)
     */
    renderCategoryNode: function(category, label, count, items, parentStatus) {
        const icon = category === 'entities' ? 'bi-box' : 'bi-arrow-left-right';
        
        let html = `
            <li class="manual-tree-node category-node" data-category="${category}">
                <div class="manual-tree-node-content">
                    <button type="button" class="manual-tree-toggle" data-expanded="true">
                        <i class="bi bi-chevron-down"></i>
                    </button>
                    <span class="manual-tree-label">
                        <i class="bi ${icon}"></i>
                        <span class="item-name">${label}</span>
                        <span class="manual-count-badge">${count}</span>
                    </span>
                </div>
                <div class="manual-tree-children">
                    <ul class="manual-tree-list nested">
        `;
        
        if (items.length === 0) {
            html += `<li class="manual-tree-node"><div class="manual-tree-node-content text-muted small ps-4">No items</div></li>`;
        } else {
            items.forEach(item => {
                html += this.renderItemNode(item, category, parentStatus);
            });
        }
        
        html += `
                    </ul>
                </div>
            </li>
        `;
        
        return html;
    },
    
    /**
     * Render an item node (Entity or Relationship)
     */
    renderItemNode: function(item, category, parentStatus) {
        const type = category === 'entities' ? 'entity' : 'relationship';
        const icon = category === 'entities' ? 'bi-box' : 'bi-arrow-left-right';
        const emoji = item.emoji || '';
        let statusBadge;
        if (parentStatus === 'excluded') {
            statusBadge = '<span class="manual-status-badge excluded">Excluded</span>';
        } else if (parentStatus === 'assigned') {
            statusBadge = '<span class="manual-status-badge assigned">Mapped</span>';
        } else {
            statusBadge = '<span class="manual-status-badge not-assigned">Not Mapped</span>';
        }
        
        let displayName = item.name;
        if (type === 'relationship' && item.domain && item.range) {
            const getLocalName = (uri) => uri ? uri.split('#').pop().split('/').pop() : '';
            const source = item.direction === 'reverse' ? getLocalName(item.range) : getLocalName(item.domain);
            const target = item.direction === 'reverse' ? getLocalName(item.domain) : getLocalName(item.range);
            displayName += ` <small class="text-muted">(${source} → ${target})</small>`;
        }
        
        const escapedName = item.name.replace(/"/g, '&quot;');
        const escapedLabel = (item.label || item.name).replace(/"/g, '&quot;');
        
        return `
            <li class="manual-tree-node item-node ${type}-item" data-type="${type}" data-uri="${item.uri}" data-name="${escapedName}" data-label="${escapedLabel}">
                <div class="manual-tree-node-content">
                    <span class="manual-tree-spacer"></span>
                    <span class="manual-tree-label">
                        ${emoji ? `<span class="emoji-inline">${emoji}</span>` : `<i class="bi ${icon}"></i>`}
                        <span class="item-name">${displayName}</span>
                        ${statusBadge}
                    </span>
                </div>
            </li>
        `;
    },
    
    /**
     * Attach event listeners to the tree
     */
    attachEventListeners: function(container) {
        // Toggle buttons
        container.querySelectorAll('.manual-tree-toggle').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleNode(btn);
            });
        });
        
        // Item clicks
        container.querySelectorAll('.manual-tree-node.item-node .manual-tree-node-content').forEach(content => {
            content.addEventListener('click', (e) => {
                e.stopPropagation();
                const node = content.closest('.manual-tree-node');
                const type = node.dataset.type;
                const uri = node.dataset.uri;
                const name = node.dataset.name;
                const label = node.dataset.label;
                this.selectItem(type, uri, name, label, content);
            });
        });
    },
    
    /**
     * Toggle tree node expand/collapse
     */
    toggleNode: function(btn) {
        const isExpanded = btn.dataset.expanded === 'true';
        const node = btn.closest('.manual-tree-node');
        const children = node.querySelector('.manual-tree-children');
        
        if (isExpanded) {
            btn.dataset.expanded = 'false';
            btn.innerHTML = '<i class="bi bi-chevron-right"></i>';
            if (children) children.classList.add('collapsed');
        } else {
            btn.dataset.expanded = 'true';
            btn.innerHTML = '<i class="bi bi-chevron-down"></i>';
            if (children) children.classList.remove('collapsed');
        }
    },
    
    /**
     * Expand all nodes
     */
    expandAll: function() {
        const container = document.getElementById('manualAssignmentTree');
        if (!container) return;
        
        container.querySelectorAll('.manual-tree-toggle').forEach(btn => {
            btn.dataset.expanded = 'true';
            btn.innerHTML = '<i class="bi bi-chevron-down"></i>';
        });
        
        container.querySelectorAll('.manual-tree-children').forEach(el => {
            el.classList.remove('collapsed');
        });
    },
    
    /**
     * Collapse all nodes
     */
    collapseAll: function() {
        const container = document.getElementById('manualAssignmentTree');
        if (!container) return;
        
        container.querySelectorAll('.manual-tree-toggle').forEach(btn => {
            btn.dataset.expanded = 'false';
            btn.innerHTML = '<i class="bi bi-chevron-right"></i>';
        });
        
        container.querySelectorAll('.manual-tree-children').forEach(el => {
            el.classList.add('collapsed');
        });
    },
    
    /**
     * Select an item and open the panel
     */
    selectItem: function(type, uri, name, label, contentElement) {
        // Clear previous selection
        document.querySelectorAll('#manualAssignmentTree .manual-tree-node-content.selected').forEach(el => {
            el.classList.remove('selected');
        });
        
        // Mark as selected
        contentElement.classList.add('selected');
        
        // Store current item
        this.currentItem = { type, uri, name, label };
        
        // Open panel with content
        this.openPanel(type, uri, name, label);
    },
    
    /**
     * Open the panel for an item - reuses the Designer panel content functions
     */
    openPanel: function(type, uri, name, label) {
        const container = document.getElementById('manual-container');
        const titleSpan = document.getElementById('manualPanelItemName');
        const titleIcon = document.querySelector('#manualPanelTitle i');
        const panelBody = document.getElementById('manualPanelBody');
        
        // Update title
        titleSpan.textContent = label || name;
        titleIcon.className = type === 'entity' ? 'bi bi-box' : 'bi bi-arrow-left-right';
        
        // Load content using the same functions from mapping-design.js
        if (type === 'entity') {
            // Find the class in ontology
            const ontologyClass = MappingState.loadedOntology?.classes?.find(c => c.uri === uri);
            if (ontologyClass && typeof loadEntityPanelContent === 'function') {
                // Use the Designer's panel content function with our panel body
                loadEntityPanelContent(uri, label || name, panelBody);
                // Re-initialize Bootstrap tabs after content is loaded
                this.initBootstrapTabs(panelBody);
            } else {
                panelBody.innerHTML = '<div class="alert alert-warning">Entity not found in ontology</div>';
            }
        } else {
            // Find the property in ontology
            const ontologyProperty = MappingState.loadedOntology?.properties?.find(p => p.uri === uri);
            if (ontologyProperty && typeof loadRelationshipPanelContent === 'function') {
                // Use the Designer's panel content function with our panel body
                loadRelationshipPanelContent(ontologyProperty, panelBody);
                // Re-initialize Bootstrap tabs after content is loaded
                this.initBootstrapTabs(panelBody);
            } else {
                panelBody.innerHTML = '<div class="alert alert-warning">Relationship not found in ontology</div>';
            }
        }
        
        // Show panel (bottom)
        container.classList.add('panel-open');
        
        // Enable save button listener
        this.setupSaveButton();
    },
    
    /**
     * Initialize Bootstrap tabs within the panel
     */
    initBootstrapTabs: function(panelBody) {
        // Give DOM time to update, then initialize Bootstrap tabs
        setTimeout(() => {
            // Initialize all tab buttons
            panelBody.querySelectorAll('[data-bs-toggle="tab"]').forEach(tabEl => {
                // Force Bootstrap to recognize the tab
                new bootstrap.Tab(tabEl);
            });
        }, 50);
    },
    
    /**
     * Setup save button to work with the panel content
     */
    setupSaveButton: function() {
        const saveBtn = document.getElementById('manualSavePanelBtn');
        if (!saveBtn) return;
        
        // Enable save button when SQL query changes in the panel (not in view mode)
        if (window.isActiveVersion !== false) {
            setTimeout(() => {
                const sqlInputs = document.querySelectorAll('#manualPanelBody textarea, #manualPanelBody input[type="text"]');
                sqlInputs.forEach(input => {
                    input.addEventListener('input', () => {
                        saveBtn.disabled = false;
                    });
                });
            }, 100);
        }
    },
    
    /**
     * Close the panel
     */
    closePanel: function() {
        const container = document.getElementById('manual-container');
        container.classList.remove('panel-open');
        
        // Clear selection
        document.querySelectorAll('#manualAssignmentTree .manual-tree-node-content.selected').forEach(el => {
            el.classList.remove('selected');
        });
        
        this.currentItem = null;
        document.getElementById('manualSavePanelBtn').disabled = true;
        
        // Reset panel body to placeholder
        const panelBody = document.getElementById('manualPanelBody');
        if (panelBody) {
            panelBody.innerHTML = `
                <div class="text-muted small text-center py-4">
                    <i class="bi bi-hand-index"></i> Click on an entity or relationship above to configure its mapping
                </div>
            `;
        }
        document.getElementById('manualPanelItemName').textContent = 'Select an item above';
    },
    
    /**
     * Reset the panel: remove the mapping entirely (unmap the entity/relationship)
     */
    resetPanel: function() {
        if (!this.currentItem) return;
        
        const { type, uri, name, label } = this.currentItem;
        const displayName = label || name;
        
        if (type === 'entity') {
            // Remove entity mapping from state
            const existingIndex = MappingState.config.entities.findIndex(m => m.ontology_class === uri);
            if (existingIndex >= 0) {
                MappingState.config.entities.splice(existingIndex, 1);
                showNotification(`Mapping for "${displayName}" removed`, 'success', 2000);
                autoSaveMappings();
            } else {
                showNotification(`No mapping to remove for "${displayName}"`, 'info', 1500);
            }
        } else {
            // Remove relationship mapping from state
            const existingIndex = MappingState.config.relationships.findIndex(m => m.property === uri);
            if (existingIndex >= 0) {
                MappingState.config.relationships.splice(existingIndex, 1);
                showNotification(`Mapping for "${displayName}" removed`, 'success', 2000);
                autoSaveMappings();
            } else {
                showNotification(`No mapping to remove for "${displayName}"`, 'info', 1500);
            }
        }
        
        // Close panel and refresh the tree
        this.closePanel();
        this.refresh();
    },
    
    /**
     * Auto-Map: Launch the agent and return immediately. Result is saved in the background.
     */
    autoMap: async function() {
        if (!this.currentItem) {
            showNotification('Please select an item first', 'warning');
            return;
        }

        const targetUri = this.currentItem.uri;
        const targetType = this.currentItem.type;
        const item = (targetType === 'entity')
            ? _buildAgentEntityItem(targetUri)
            : _buildAgentRelItem(targetUri);
        if (!item) { showNotification('Item not found in ontology', 'warning'); return; }

        const btn = document.getElementById('manualAutoMapBtn');
        const spinner = btn?.querySelector('.spinner-border');
        spinner?.classList.remove('d-none');
        if (btn) btn.disabled = true;

        try {
            const taskId = await _startSingleAutoAssign(targetType, item);
            if (!taskId) return;
            showNotification(`Agent is working on "${item.name}"…`, 'info', 15000);
            _pollAndSaveResult(taskId, targetType, targetUri, item.name);
        } catch (error) {
            console.error('[Manual Auto-Map] Error:', error);
            showNotification('Auto-Map failed: ' + error.message, 'error');
        } finally {
            spinner?.classList.add('d-none');
            if (btn) btn.disabled = (window.isActiveVersion === false);
        }
    },
    
    /**
     * Save the current mapping - uses the correct API endpoints
     */
    saveMapping: async function() {
        if (!this.currentItem) return;
        
        const { type, uri, name, label } = this.currentItem;
        const saveBtn = document.getElementById('manualSavePanelBtn');
        const originalHtml = saveBtn.innerHTML;
        saveBtn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
        saveBtn.disabled = true;
        
        try {
            if (type === 'entity') {
                // Get values from the panel (same IDs as Designer)
                const sqlQuery = document.getElementById('epSqlQuery')?.value?.trim();
                const idColumn = document.getElementById('epSummaryId')?.textContent;
                const labelColumn = document.getElementById('epSummaryLabel')?.textContent;
                
                if (!sqlQuery) {
                    showNotification('Please enter a SQL query first', 'warning');
                    saveBtn.innerHTML = originalHtml;
                    saveBtn.disabled = false;
                    return;
                }
                
                const mapping = {
                    ontology_class: uri,
                    class_name: label || name,
                    sql_query: sqlQuery,
                    id_column: idColumn && idColumn !== 'Not set' ? idColumn : null,
                    label_column: labelColumn && labelColumn !== 'Not set' ? labelColumn : null
                };
                
                const response = await fetch('/mapping/entity/add', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(mapping),
                    credentials: 'same-origin'
                });
                
                const result = await response.json();
                if (!result.success) {
                    throw new Error(result.message || 'Failed to save');
                }
                
                // Update local state
                const existingIdx = MappingState.config.entities.findIndex(m => m.ontology_class === uri);
                if (existingIdx >= 0) {
                    MappingState.config.entities[existingIdx] = result.mapping || mapping;
                } else {
                    MappingState.config.entities.push(result.mapping || mapping);
                }
            } else {
                // Get values from the panel (same IDs as Designer)
                const sqlQuery = document.getElementById('rpSqlQuery')?.value?.trim();
                const sourceColumn = document.getElementById('rpSummarySource')?.textContent;
                const targetColumn = document.getElementById('rpSummaryTarget')?.textContent;
                
                if (!sqlQuery) {
                    showNotification('Please enter a SQL query first', 'warning');
                    saveBtn.innerHTML = originalHtml;
                    saveBtn.disabled = false;
                    return;
                }
                
                const mapping = {
                    property: uri,
                    property_name: label || name,
                    sql_query: sqlQuery,
                    source_id_column: sourceColumn && sourceColumn !== 'Not set' ? sourceColumn : null,
                    target_id_column: targetColumn && targetColumn !== 'Not set' ? targetColumn : null
                };
                
                const response = await fetch('/mapping/relationship/add', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(mapping),
                    credentials: 'same-origin'
                });
                
                const result = await response.json();
                if (!result.success) {
                    throw new Error(result.message || 'Failed to save');
                }
                
                // Update local state
                const existingIdx = MappingState.config.relationships.findIndex(m => m.property === uri);
                if (existingIdx >= 0) {
                    MappingState.config.relationships[existingIdx] = result.mapping || mapping;
                } else {
                    MappingState.config.relationships.push(result.mapping || mapping);
                }
            }
            
            // Refresh the tree to reflect changes
            this.refresh();
            
            showNotification('Mapping saved successfully', 'success', 2000);
        } catch (error) {
            showNotification('Error: ' + error.message, 'error');
        } finally {
            saveBtn.innerHTML = originalHtml;
            saveBtn.disabled = true;
        }
    }
};

// Initialize when manual section becomes active
document.addEventListener('sectionChange', function(e) {
    if (e.detail?.section === 'manual') {
        ManualModule.init();
    }
});

/**
 * Toolbar / panel controls: data-action delegation (no inline onclick in templates).
 */
(function initManualActionDelegation() {
    function bind() {
        const root = document.getElementById('manual-section');
        if (!root || root.dataset.manualActionsBound === '1') return;
        root.dataset.manualActionsBound = '1';
        root.addEventListener('click', function (e) {
            const el = e.target.closest('[data-action]');
            if (!el || !root.contains(el)) return;
            const action = el.dataset.action;
            switch (action) {
                case 'manual-expand-all':
                    ManualModule.expandAll();
                    break;
                case 'manual-collapse-all':
                    ManualModule.collapseAll();
                    break;
                case 'manual-refresh':
                    ManualModule.refresh();
                    break;
                case 'manual-reset-panel':
                    ManualModule.resetPanel();
                    break;
                case 'manual-auto-map':
                    ManualModule.autoMap();
                    break;
                case 'manual-close-panel':
                    ManualModule.closePanel();
                    break;
                case 'manual-save-mapping':
                    ManualModule.saveMapping();
                    break;
                default:
                    break;
            }
        });
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bind);
    } else {
        bind();
    }
})();
