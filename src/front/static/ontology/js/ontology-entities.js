/**
 * OntoBricks - ontology-entities.js
 * Extracted from ontology templates per code_instructions.txt
 */

// ENTITIES SECTION - Class Hierarchy Tree
// =====================================================

/**
 * Update classes list - delegates to hierarchy tree
 */
function updateClassesList() {
    updateClassHierarchyTree();
}

/**
 * Build and render the class hierarchy tree
 */
function updateClassHierarchyTree() {
    const container = document.getElementById('classHierarchyTree');
    if (!container) return;
    
    const classes = OntologyState.config.classes || [];
    
    if (classes.length === 0) {
        container.innerHTML = '<div class="text-muted small">No classes defined yet</div>';
        return;
    }
    
    // Build hierarchy structure
    const hierarchy = buildClassHierarchy(classes);
    
    // Render tree
    container.innerHTML = renderHierarchyTree(hierarchy, 0);
    
    // Add event listeners to toggle buttons
    container.querySelectorAll('.tree-toggle').forEach(btn => {
        btn.addEventListener('click', function(e) {
            e.stopPropagation();
            toggleTreeNode(this);
        });
    });
}

/**
 * Build hierarchy from flat class list
 */
function buildClassHierarchy(classes) {
    const classMap = new Map();
    classes.forEach(cls => {
        classMap.set(cls.name, { ...cls, children: [] });
    });
    
    const roots = [];
    
    classMap.forEach((cls, name) => {
        if (cls.parent && classMap.has(cls.parent)) {
            classMap.get(cls.parent).children.push(cls);
        } else {
            roots.push(cls);
        }
    });
    
    const sortByName = (a, b) => (a.name || '').localeCompare(b.name || '');
    roots.sort(sortByName);
    
    function sortChildren(node) {
        if (node.children && node.children.length > 0) {
            node.children.sort(sortByName);
            node.children.forEach(sortChildren);
        }
    }
    roots.forEach(sortChildren);
    
    return roots;
}

/**
 * Render hierarchy tree as HTML
 * Note: Attributes are NOT shown in the tree - they are displayed in the right panel when an entity is selected
 */
function renderHierarchyTree(nodes, level) {
    if (!nodes || nodes.length === 0) return '';
    
    const canEdit = window.isActiveVersion !== false;
    
    let html = '<ul class="class-hierarchy-list' + (level === 0 ? ' root-level' : '') + '">';
    
    nodes.forEach(node => {
        const hasChildren = node.children && node.children.length > 0;
        const emoji = node.emoji || OntologyState.defaultClassEmoji;
        const childCount = hasChildren ? ` <span class="badge bg-secondary badge-sm">${node.children.length}</span>` : '';
        const attrCount = (node.dataProperties || []).length;
        const attrBadge = attrCount > 0 ? ` <span class="badge bg-info badge-sm" title="${attrCount} attribute(s) - click to view"><i class="bi bi-tag-fill"></i> ${attrCount}</span>` : '';
        
        const deleteBtn = canEdit ? 
            `<button type="button" class="btn btn-sm btn-link text-danger p-0 ms-2 tree-delete-btn" 
                     onclick="event.stopPropagation(); removeClassByName('${node.name}')" title="Delete ${node.name}">
                <i class="bi bi-trash"></i>
            </button>` : '';
        
        // Build children HTML (subclasses only - no attributes in tree)
        let childrenHtml = '';
        if (hasChildren) {
            childrenHtml = renderHierarchyTree(node.children, level + 1);
        }
        
        // Expandable content only if has child classes
        const expandableContent = hasChildren ? 
            `<div class="tree-children">${childrenHtml}</div>` : '';
        
        html += `
            <li class="tree-node${hasChildren ? ' has-children' : ''}" data-class-name="${node.name}">
                <div class="tree-node-content">
                    ${hasChildren ? 
                        `<button type="button" class="tree-toggle btn btn-sm btn-link p-0" data-expanded="true">
                            <i class="bi bi-chevron-down"></i>
                        </button>` : 
                        `<span class="tree-spacer"></span>`
                    }
                    <span class="tree-node-label" onclick="editClassByName('${node.name}')" title="Click to view details and attributes">
                        <span class="emoji-inline">${emoji}</span>
                        <strong>${node.name}</strong>
                        ${node.label && node.label !== node.name ? `<small class="text-muted ms-1">(${node.label})</small>` : ''}
                        ${childCount}
                        ${attrBadge}
                    </span>
                    ${deleteBtn}
                </div>
                ${expandableContent}
            </li>
        `;
    });
    
    html += '</ul>';
    return html;
}

/**
 * Find class index by name
 */
function findClassIndexByName(className) {
    return OntologyState.config.classes.findIndex(cls => cls.name === className);
}

/**
 * Remove class by name
 */
async function removeClassByName(className) {
    const idx = findClassIndexByName(className);
    if (idx >= 0) {
        await removeClass(idx);
    }
}

/**
 * Find all items that will be affected when deleting an entity
 * @param {string} className - Name of the entity being deleted
 * @returns {Object} - Object containing affected relationships and mappings
 */
async function findAffectedItems(className) {
    const affected = {
        relationships: [],
        entityMappings: [],
        relationshipMappings: []
    };
    
    // Find relationships where this entity is domain or range
    const properties = OntologyState.config.properties || [];
    properties.forEach(prop => {
        if (prop.domain === className || prop.range === className) {
            affected.relationships.push(prop);
        }
    });
    
    // Load mappings from session to check for affected mappings
    try {
        const response = await fetch('/mapping/load', { credentials: 'same-origin' });
        const result = await response.json();
        const mappingConfig = result.config || result;
        
        if (mappingConfig) {
            // Find entity mappings for this class
            const entityMappings = mappingConfig.entities || [];
            entityMappings.forEach(mapping => {
                const mappingClass = mapping.ontology_class || mapping.class_uri || '';
                // Check if mapping references this class (by name or by URI ending with class name)
                if (mappingClass === className || 
                    mappingClass.endsWith('#' + className) || 
                    mappingClass.endsWith('/' + className)) {
                    affected.entityMappings.push(mapping);
                }
            });
            
            // Find relationship mappings for affected relationships
            const relationshipMappings = mappingConfig.relationships || [];
            const affectedRelNames = affected.relationships.map(r => r.name);
            relationshipMappings.forEach(mapping => {
                const propUri = mapping.property || '';
                // Check if mapping references any affected relationship
                affectedRelNames.forEach(relName => {
                    if (propUri === relName || 
                        propUri.endsWith('#' + relName) || 
                        propUri.endsWith('/' + relName)) {
                        affected.relationshipMappings.push(mapping);
                    }
                });
            });
        }
    } catch (error) {
        console.error('Error loading mappings for cascade check:', error);
    }
    
    return affected;
}

/**
 * Show cascade delete confirmation dialog
 * @param {string} className - Name of entity being deleted
 * @param {Object} affected - Affected items from findAffectedItems()
 * @returns {Promise<boolean>} - true if user confirms, false otherwise
 */
function showCascadeDeleteConfirm(className, affected) {
    return new Promise((resolve) => {
        // Remove existing modal if any
        const existingModal = document.getElementById('cascadeDeleteModal');
        if (existingModal) existingModal.remove();
        
        let resolved = false;
        
        // Build the list of affected items
        let affectedHtml = '';
        
        // Relationships section
        if (affected.relationships.length > 0) {
            affectedHtml += `
                <div class="mb-3">
                    <h6 class="text-danger"><i class="bi bi-arrow-left-right me-1"></i>Relationships to be deleted (${affected.relationships.length})</h6>
                    <ul class="list-unstyled ms-3 small">
                        ${affected.relationships.map(r => {
                            const connection = r.domain && r.range ? ` (${r.domain} → ${r.range})` : '';
                            return `<li><i class="bi bi-x-circle text-danger me-1"></i><strong>${r.name}</strong>${connection}</li>`;
                        }).join('')}
                    </ul>
                </div>
            `;
        }
        
        // Entity mappings section
        if (affected.entityMappings.length > 0) {
            affectedHtml += `
                <div class="mb-3">
                    <h6 class="text-warning"><i class="bi bi-database me-1"></i>Entity Mappings to be deleted (${affected.entityMappings.length})</h6>
                    <ul class="list-unstyled ms-3 small">
                        ${affected.entityMappings.map(m => {
                            const table = m.source_table || m.table_name || 'Unknown table';
                            return `<li><i class="bi bi-x-circle text-warning me-1"></i>${m.ontology_class || m.class_uri} → <code>${table}</code></li>`;
                        }).join('')}
                    </ul>
                </div>
            `;
        }
        
        // Relationship mappings section
        if (affected.relationshipMappings.length > 0) {
            affectedHtml += `
                <div class="mb-3">
                    <h6 class="text-warning"><i class="bi bi-link-45deg me-1"></i>Relationship Mappings to be deleted (${affected.relationshipMappings.length})</h6>
                    <ul class="list-unstyled ms-3 small">
                        ${affected.relationshipMappings.map(m => {
                            const propName = m.property_label || m.property || 'Unknown';
                            return `<li><i class="bi bi-x-circle text-warning me-1"></i>${propName}</li>`;
                        }).join('')}
                    </ul>
                </div>
            `;
        }
        
        // If no affected items, show simple message
        if (!affectedHtml) {
            affectedHtml = '<p class="text-muted">No related items will be affected.</p>';
        }
        
        const totalAffected = affected.relationships.length + affected.entityMappings.length + affected.relationshipMappings.length;
        const warningClass = totalAffected > 0 ? 'bg-danger' : 'bg-warning';
        
        const modalHtml = `
            <div class="modal fade" id="cascadeDeleteModal" tabindex="-1">
                <div class="modal-dialog modal-dialog-centered modal-lg">
                    <div class="modal-content">
                        <div class="modal-header ${warningClass} text-white">
                            <h5 class="modal-title">
                                <i class="bi bi-exclamation-triangle me-2"></i>
                                Confirm Delete Entity
                            </h5>
                            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            <p>Are you sure you want to delete the entity <strong>"${className}"</strong>?</p>
                            
                            ${totalAffected > 0 ? `
                                <div class="alert alert-danger">
                                    <i class="bi bi-exclamation-triangle me-2"></i>
                                    <strong>Warning:</strong> This will also delete ${totalAffected} related item(s):
                                </div>
                            ` : ''}
                            
                            <div class="border rounded p-3 bg-light" style="max-height: 300px; overflow-y: auto;">
                                ${affectedHtml}
                            </div>
                            
                            <p class="mt-3 mb-0 text-muted small">
                                <i class="bi bi-info-circle me-1"></i>
                                This action cannot be undone.
                            </p>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                            <button type="button" class="btn btn-danger" id="confirmCascadeDelete">
                                <i class="bi bi-trash me-1"></i>Delete Entity${totalAffected > 0 ? ` & ${totalAffected} Related Items` : ''}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        const modalEl = document.getElementById('cascadeDeleteModal');
        const modal = new bootstrap.Modal(modalEl);
        
        // Handle confirm button
        document.getElementById('confirmCascadeDelete').addEventListener('click', () => {
            resolved = true;
            modal.hide();
            resolve(true);
        });
        
        // Handle cancel/close
        modalEl.addEventListener('hidden.bs.modal', () => {
            modalEl.remove();
            if (!resolved) {
                resolve(false);
            }
        }, { once: true });
        
        modal.show();
    });
}

/**
 * Remove class by index with cascade delete
 */
async function removeClass(idx) {
    const className = OntologyState.config.classes[idx]?.name;
    if (!className) return;
    
    // Find all affected items
    const affected = await findAffectedItems(className);
    
    // Show cascade delete confirmation
    const confirmed = await showCascadeDeleteConfirm(className, affected);
    if (!confirmed) return;
    
    // Delete affected relationships from ontology
    if (affected.relationships.length > 0) {
        const relNamesToDelete = affected.relationships.map(r => r.name);
        OntologyState.config.properties = OntologyState.config.properties.filter(
            prop => !relNamesToDelete.includes(prop.name)
        );
    }
    
    // Delete the entity
    OntologyState.config.classes.splice(idx, 1);
    
    // Update UI lists
    updateClassesList();
    if (typeof updatePropertiesList === 'function') {
        updatePropertiesList();
    }
    
    // Save ontology changes
    await window.saveConfigToSession();
    await autoGenerateOwl();
    
    // Delete affected mappings if any exist
    if (affected.entityMappings.length > 0 || affected.relationshipMappings.length > 0) {
        try {
            // Load current mapping config
            const response = await fetch('/mapping/load', { credentials: 'same-origin' });
            const result = await response.json();
            const mappingConfig = result.config || result;
            
            if (mappingConfig) {
                // Remove affected entity mappings
                if (affected.entityMappings.length > 0) {
                    const affectedClassNames = affected.entityMappings.map(m => m.ontology_class || m.class_uri);
                    mappingConfig.entities = (mappingConfig.entities || []).filter(m => {
                        const mappingClass = m.ontology_class || m.class_uri || '';
                        return !affectedClassNames.some(name => 
                            mappingClass === name || 
                            mappingClass.endsWith('#' + name) || 
                            mappingClass.endsWith('/' + name)
                        );
                    });
                }
                
                // Remove affected relationship mappings
                if (affected.relationshipMappings.length > 0) {
                    const affectedRelNames = affected.relationships.map(r => r.name);
                    mappingConfig.relationships = (mappingConfig.relationships || []).filter(m => {
                        const propUri = m.property || '';
                        return !affectedRelNames.some(name => 
                            propUri === name || 
                            propUri.endsWith('#' + name) || 
                            propUri.endsWith('/' + name)
                        );
                    });
                }
                
                // Save updated mapping config
                await fetch('/mapping/save', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(mappingConfig),
                    credentials: 'same-origin'
                });
            }
        } catch (error) {
            console.error('Error updating mappings after cascade delete:', error);
        }
    }
    
    const relatedCount = affected.relationships.length + affected.entityMappings.length + affected.relationshipMappings.length;
    if (relatedCount > 0) {
        showNotification(`Entity and ${relatedCount} related item(s) deleted successfully`, 'success', 3000);
    } else {
        showNotification('Entity deleted successfully', 'success', 2000);
    }
}

/**
 * Toggle tree node expand/collapse
 */
function toggleTreeNode(toggleBtn) {
    const isExpanded = toggleBtn.dataset.expanded === 'true';
    const treeNode = toggleBtn.closest('.tree-node');
    const childrenContainer = treeNode.querySelector('.tree-children');
    
    if (isExpanded) {
        toggleBtn.dataset.expanded = 'false';
        toggleBtn.innerHTML = '<i class="bi bi-chevron-right"></i>';
        if (childrenContainer) childrenContainer.style.display = 'none';
    } else {
        toggleBtn.dataset.expanded = 'true';
        toggleBtn.innerHTML = '<i class="bi bi-chevron-down"></i>';
        if (childrenContainer) childrenContainer.style.display = 'block';
    }
}

/**
 * Expand all tree nodes
 */
function expandAllClasses() {
    const container = document.getElementById('classHierarchyTree');
    if (!container) return;
    
    container.querySelectorAll('.tree-toggle').forEach(btn => {
        btn.dataset.expanded = 'true';
        btn.innerHTML = '<i class="bi bi-chevron-down"></i>';
    });
    
    container.querySelectorAll('.tree-children').forEach(el => {
        el.style.display = 'block';
    });
}

/**
 * Collapse all tree nodes
 */
function collapseAllClasses() {
    const container = document.getElementById('classHierarchyTree');
    if (!container) return;
    
    container.querySelectorAll('.tree-toggle').forEach(btn => {
        btn.dataset.expanded = 'false';
        btn.innerHTML = '<i class="bi bi-chevron-right"></i>';
    });
    
    container.querySelectorAll('.tree-children').forEach(el => {
        el.style.display = 'none';
    });
}
