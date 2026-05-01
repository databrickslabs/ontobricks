/**
 * OntoBricks - ontology-relationships.js
 * Extracted from ontology templates per code_instructions.txt
 */

// RELATIONSHIPS SECTION - Property List
// =====================================================

/**
 * Update properties list
 * Only shows ObjectProperties (relationships), not DatatypeProperties (attributes)
 */
function updatePropertiesList() {
    const list = document.getElementById('propertiesList');
    if (!list) return;
    
    const allProperties = OntologyState.config.properties || [];
    
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
    
    if (properties.length === 0) {
        list.innerHTML = '<div class="text-muted small">No relationships defined yet</div>';
        return;
    }
    
    // Mirrors ontology-entities.js — viewer-role and inactive-version
    // both must hide write affordances (delete, toggle direction, …).
    const canEdit = (window.OB && typeof window.OB.canEditOntology === 'function')
        ? window.OB.canEditOntology()
        : window.isActiveVersion !== false;
    
    let html = '<ul class="relationship-list">';
    
    properties.forEach((prop) => {
        // Find the original index in allProperties for actions
        const idx = allProperties.findIndex(p => p.name === prop.name);
        let directionBadge = '';
        if (prop.domain && prop.range) {
            const dir = prop.direction || 'forward';
            if (dir === 'reverse') {
                directionBadge = `<span class="badge bg-secondary badge-sm ms-2" title="Reverse direction">←</span>`;
            } else {
                directionBadge = `<span class="badge bg-secondary badge-sm ms-2" title="Forward direction">→</span>`;
            }
        }
        
        let connectionDisplay = '';
        if (prop.domain && prop.range) {
            const dir = prop.direction || 'forward';
            if (dir === 'reverse') {
                connectionDisplay = `<small class="text-muted ms-2">(${prop.range} → ${prop.domain})</small>`;
            } else {
                connectionDisplay = `<small class="text-muted ms-2">(${prop.domain} → ${prop.range})</small>`;
            }
        }
        
        const toggleBtn = canEdit ? 
            `<button type="button" class="btn btn-sm btn-link text-secondary p-0 ms-2 tree-action-btn" 
                     onclick="event.stopPropagation(); togglePropertyDirection(${idx})" title="Toggle Direction">
                <i class="bi bi-arrow-left-right"></i>
            </button>` : '';
        
        const deleteBtn = canEdit ? 
            `<button type="button" class="btn btn-sm btn-link text-danger p-0 ms-1 tree-action-btn" 
                     onclick="event.stopPropagation(); removePropertyByName('${prop.name}')" title="Delete ${prop.name}">
                <i class="bi bi-trash"></i>
            </button>` : '';
        
        html += `
            <li class="tree-node" data-property-name="${prop.name}">
                <div class="tree-node-content">
                    <span class="tree-spacer"></span>
                    <span class="tree-node-label" onclick="editPropertyByName('${prop.name}')" title="Click to edit">
                        <i class="bi bi-arrow-left-right text-primary"></i>
                        <strong>${prop.name}</strong>
                        ${directionBadge}
                        ${connectionDisplay}
                    </span>
                    ${toggleBtn}
                    ${deleteBtn}
                </div>
            </li>
        `;
    });
    
    html += '</ul>';
    list.innerHTML = html;
}

/**
 * Find property index by name
 */
function findPropertyIndexByName(propertyName) {
    return OntologyState.config.properties.findIndex(prop => prop.name === propertyName);
}

/**
 * Remove property by name
 *
 * Defense-in-depth: see ``removeClassByName``.
 */
async function removePropertyByName(propertyName) {
    if (window.OB && typeof window.OB.canEditOntology === 'function'
        && !window.OB.canEditOntology()) {
        return;
    }
    const idx = findPropertyIndexByName(propertyName);
    if (idx >= 0) {
        await removeProperty(idx);
    }
}

/**
 * Remove property by index
 */
async function removeProperty(idx) {
    const propertyName = OntologyState.config.properties[idx]?.name || 'this relationship';
    const confirmed = await showDeleteConfirm(propertyName, 'relationship');
    if (!confirmed) return;
    OntologyState.config.properties.splice(idx, 1);
    updatePropertiesList();
    
    await window.saveConfigToSession();
    await autoGenerateOwl();
    
    showNotification('Relationship deleted successfully', 'success', 2000);
}

/**
 * Toggle property direction
 */
async function togglePropertyDirection(idx) {
    const prop = OntologyState.config.properties[idx];
    if (!prop) return;
    
    prop.direction = (prop.direction === 'reverse') ? 'forward' : 'reverse';
    
    updatePropertiesList();
    
    await window.saveConfigToSession();
    await autoGenerateOwl();
}
