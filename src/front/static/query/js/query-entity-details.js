/**
 * OntoBricks - query-entity-details.js
 * Entity and relationship detail panel, mapping lookup utilities.
 * Extracted from query.js per code_instructions.txt
 */

// =====================================================
// ENTITY DETAILS PANEL
// =====================================================

async function showEntityDetails(entity) {
    const container = document.getElementById('entityDetailsContent');
    if (!container) return;
    
    // Ensure entity mappings are loaded
    if (Object.keys(entityMappings).length === 0) {
        console.log('[EntityDetails] Entity mappings not loaded, loading now...');
        await loadEntityMappings();
        console.log('[EntityDetails] After loading, entityMappings keys:', Object.keys(entityMappings));
    }
    
    // Get icon for this entity
    const icon = getEntityIcon(entity);
    const displayLabel = getDisplayLabel(entity);
    
    // Find the mapping for this entity type
    // Try multiple strategies: by type, by typeUri, and by extracting class from entity URI
    const typeLower = (entity.type || '').toLowerCase();
    let entityMapping = entityMappings[typeLower] || findMappingByType(entity.type);
    
    // If not found by type, try by typeUri
    if (!entityMapping && entity.typeUri) {
        entityMapping = findMappingByType(entity.typeUri);
    }
    
    // If still not found, try extracting class name from entity URI
    if (!entityMapping && entity.id) {
        entityMapping = findMappingByUri(entity.id);
    }
    
    console.log('=== Entity Mapping Lookup Debug ===');
    console.log('  entity.id:', entity.id);
    console.log('  entity.type:', entity.type);
    console.log('  entity.typeUri:', entity.typeUri);
    console.log('  entity.instanceId:', entity.instanceId);
    const extractedClass = extractClassFromUri(entity.id);
    console.log('  Extracted class from URI:', extractedClass);
    console.log('  entityMappings keys:', Object.keys(entityMappings));
    console.log('  Found mapping:', entityMapping ? 'yes' : 'no');
    if (entityMapping) {
        console.log('  Mapping className:', entityMapping.className);
    } else {
        console.log('  FAILED TO FIND MAPPING - trying manual checks:');
        console.log('    typeLower lookup:', typeLower, '->', entityMappings[typeLower] ? 'found' : 'not found');
        console.log('    findMappingByType result:', findMappingByType(entity.type) ? 'found' : 'not found');
        console.log('    findMappingByType(typeUri) result:', findMappingByType(entity.typeUri) ? 'found' : 'not found');
        console.log('    findMappingByUri result:', findMappingByUri(entity.id) ? 'found' : 'not found');
        if (extractedClass) {
            console.log('    Direct lookup of extracted class:', extractedClass.toLowerCase(), '->', entityMappings[extractedClass.toLowerCase()] ? 'found' : 'not found');
        }
    }
    console.log('=== End Mapping Debug ===')
    
    // Find all relationships involving this entity
    const outgoingRels = d3LinksData.filter(l => {
        const sourceId = typeof l.source === 'object' ? l.source.id : l.source;
        return sourceId === entity.id;
    });
    
    const incomingRels = d3LinksData.filter(l => {
        const targetId = typeof l.target === 'object' ? l.target.id : l.target;
        return targetId === entity.id;
    });
    
    // Get ontology class info - prefer mapping's class name over entity.type
    // entity.type might be a data value (like "residential") instead of the ontology class (like "Meter")
    let classInfo = null;
    let ontologyTypeName = 'Unknown';
    
    console.log('=== Ontology Type Name Resolution ===');
    console.log('  entityMapping found:', !!entityMapping);
    if (entityMapping) {
        console.log('  entityMapping.className:', entityMapping.className);
        console.log('  entityMapping.classUri:', entityMapping.classUri);
    }
    
    // Priority 1: Use entity mapping's class name (most reliable)
    if (entityMapping) {
        if (entityMapping.className) {
            ontologyTypeName = entityMapping.className;
            classInfo = findOntologyClass(entityMapping.className) || findOntologyClass(entityMapping.classUri);
            console.log('  Using Priority 1a (entityMapping.className):', ontologyTypeName);
        } else if (entityMapping.classUri) {
            // Extract class name from classUri if className is empty
            // e.g., "https://databricks-ontology.com/MyOntology#Subscription" → "Subscription"
            const uriParts = entityMapping.classUri.split(/[#\/]/);
            ontologyTypeName = uriParts[uriParts.length - 1] || 'Unknown';
            classInfo = findOntologyClass(entityMapping.classUri);
            console.log('  Using Priority 1b (extracted from entityMapping.classUri):', ontologyTypeName);
        }
    }
    // Priority 2: Try the full type URI stored on entity
    else if (entity.typeUri) {
        classInfo = findOntologyClass(entity.typeUri);
        if (classInfo) {
            ontologyTypeName = classInfo.name;
            console.log('  Using Priority 2 (entity.typeUri -> classInfo.name):', ontologyTypeName);
        } else {
            // Extract local part from URI
            ontologyTypeName = entity.typeUri.split('#').pop().split('/').pop() || entity.type || 'Unknown';
            console.log('  Using Priority 2 (entity.typeUri fallback):', ontologyTypeName);
        }
    }
    // Priority 3: Try extracting class name from entity URI (e.g., #Meter_123 → Meter)
    else if (entity.id) {
        const extractedClass = extractClassFromUri(entity.id);
        console.log('  Priority 3: extractedClass from URI:', extractedClass);
        if (extractedClass) {
            classInfo = findOntologyClass(extractedClass);
            if (classInfo) {
                ontologyTypeName = classInfo.name;
                console.log('  Using Priority 3 (extractedClass -> classInfo.name):', ontologyTypeName);
            } else {
                // Use extracted class name even without full ontology info
                ontologyTypeName = extractedClass;
                console.log('  Using Priority 3 (extractedClass directly):', ontologyTypeName);
            }
        }
    }
    // Priority 4: Try entity.type (might be class name or might be data value - least reliable)
    if (ontologyTypeName === 'Unknown' && entity.type) {
        classInfo = findOntologyClass(entity.type);
        if (classInfo) {
            ontologyTypeName = classInfo.name;
            console.log('  Using Priority 4 (entity.type -> classInfo.name):', ontologyTypeName);
        } else {
            console.log('  Priority 4: entity.type does not match ontology class, keeping Unknown');
        }
        // Don't use entity.type as display name if it doesn't match an ontology class
        // It's likely a data value, not the class name
    }
    console.log('  Final ontologyTypeName:', ontologyTypeName);
    console.log('=== End Type Resolution ===')
    
    const ontologyTypeEmoji = classInfo?.emoji || entityMapping?.emoji || icon;
    
    // Collect all attribute values - from entity.attributes, query results, and mappings
    const allAttributes = new Map();
    
    // Get valid attribute names for this entity type from the mapping
    const validAttributeNames = new Set();
    if (entityMapping) {
        if (entityMapping.idColumn) validAttributeNames.add(entityMapping.idColumn.toLowerCase());
        if (entityMapping.labelColumn) validAttributeNames.add(entityMapping.labelColumn.toLowerCase());
        if (entityMapping.attributeMappings) {
            Object.entries(entityMapping.attributeMappings).forEach(([attrName, colName]) => {
                validAttributeNames.add(attrName.toLowerCase());
                validAttributeNames.add(colName.toLowerCase());
            });
        }
        // Also add standard names
        validAttributeNames.add('label');
        validAttributeNames.add('name');
    }
    
    // Add from entity.attributes (captured during graph processing)
    console.log('=== Entity Details Debug ===');
    console.log('Entity ID:', entity.id);
    console.log('Entity type:', entity.type);
    console.log('Entity label:', entity.label);
    console.log('Valid attribute names for type:', Array.from(validAttributeNames));
    console.log('Entity attributes:', JSON.stringify(entity.attributes, null, 2));
    
    // Normalize function for fuzzy attribute matching
    const normalizeAttr = s => s.toLowerCase().replace(/[_-]/g, '');
    const validNormalized = new Set(Array.from(validAttributeNames).map(normalizeAttr));
    
    if (entity.attributes) {
        for (const [key, value] of Object.entries(entity.attributes)) {
            if (value) {
                // If we have a mapping, only add attributes that are valid for this type
                if (entityMapping && validAttributeNames.size > 0) {
                    const keyLower = key.toLowerCase();
                    const keyNormalized = normalizeAttr(key);
                    // Check normalized match as well (first_name matches FirstName)
                    const isValid = validAttributeNames.has(keyLower) || 
                                   validNormalized.has(keyNormalized) ||
                                   Array.from(validAttributeNames).some(v => keyLower.includes(v) || v.includes(keyLower)) ||
                                   Array.from(validNormalized).some(v => keyNormalized.includes(v) || v.includes(keyNormalized));
                    if (isValid) {
                        allAttributes.set(key, value);
                        console.log(`  Added from entity (valid): ${key} = ${value}`);
                    } else {
                        console.log(`  Skipped from entity (not valid for ${entity.type}): ${key} = ${value}`);
                    }
                } else {
                    allAttributes.set(key, value);
                    console.log(`  Added from entity: ${key} = ${value}`);
                }
            }
        }
    }
    
    // Add from query results
    const queryAttributes = getEntityAttributes(entity.id);
    console.log('Query attributes found:', queryAttributes.length);
    
    for (const attr of queryAttributes) {
        if (attr.value && !allAttributes.has(attr.predicate)) {
            // If we have a mapping, only add attributes that are valid for this type
            if (entityMapping && validAttributeNames.size > 0) {
                const keyLower = attr.predicate.toLowerCase();
                const keyNormalized = normalizeAttr(attr.predicate);
                // Check normalized match as well (first_name matches FirstName)
                const isValid = validAttributeNames.has(keyLower) || 
                               validNormalized.has(keyNormalized) ||
                               Array.from(validAttributeNames).some(v => keyLower.includes(v) || v.includes(keyLower)) ||
                               Array.from(validNormalized).some(v => keyNormalized.includes(v) || v.includes(keyNormalized));
                if (isValid) {
                    allAttributes.set(attr.predicate, attr.value);
                    console.log(`  Added from query (valid): ${attr.predicate} = ${attr.value}`);
                } else {
                    console.log(`  Skipped from query (not valid for ${entity.type}): ${attr.predicate} = ${attr.value}`);
                }
            } else {
                allAttributes.set(attr.predicate, attr.value);
                console.log(`  Added from query: ${attr.predicate} = ${attr.value}`);
            }
        }
    }
    
    console.log('Total valid attributes:', allAttributes.size);
    console.log('=== End Debug ===');
    
    // Determine special attribute names to exclude from custom attributes
    const specialAttrNames = new Set();
    let actualIdValue = entity.instanceId;
    let actualLabelValue = entity.label;
    let dashboardUrl = entityMapping?.dashboard || classInfo?.dashboard || null;
    let dashboardParams = entityMapping?.dashboardParams || classInfo?.dashboardParams || {};
    
    console.log('[Dashboard] Source check:');
    console.log('  entityMapping?.dashboard:', entityMapping?.dashboard);
    console.log('  entityMapping?.dashboardParams:', JSON.stringify(entityMapping?.dashboardParams));
    console.log('  classInfo?.dashboard:', classInfo?.dashboard);
    console.log('  classInfo?.dashboardParams:', JSON.stringify(classInfo?.dashboardParams));
    console.log('  Final dashboardUrl:', dashboardUrl);
    console.log('  Final dashboardParams:', JSON.stringify(dashboardParams));
    
    if (entityMapping) {
        if (entityMapping.idColumn) {
            specialAttrNames.add(entityMapping.idColumn.toLowerCase());
            specialAttrNames.add(normalizeAttr(entityMapping.idColumn));
            // Get actual ID value
            actualIdValue = findAttributeValue(allAttributes, entityMapping.idColumn) || entity.instanceId;
        }
        if (entityMapping.labelColumn) {
            specialAttrNames.add(entityMapping.labelColumn.toLowerCase());
            specialAttrNames.add(normalizeAttr(entityMapping.labelColumn));
            // Get actual Label value
            actualLabelValue = findAttributeValue(allAttributes, entityMapping.labelColumn) || entity.label;
        }
    }
    // Also add common variations
    specialAttrNames.add('id');
    specialAttrNames.add('label');
    specialAttrNames.add('name');
    specialAttrNames.add('dashboard');
    
    // Build the details HTML - now that we have all values
    let html = `
        <div class="entity-detail-header">
            <span class="entity-detail-icon">${ontologyTypeEmoji}</span>
            <div class="entity-detail-title">
                <h6>${escapeHtml(displayLabel)}</h6>
                <small title="${escapeHtml(entity.id)}">${truncateUri(entity.id)}</small>
            </div>
        </div>
        
        <div class="entity-detail-section">
            <h6><i class="bi bi-card-list"></i> Entity Info</h6>
            <div class="entity-detail-item">
                <span class="detail-key"><i class="bi bi-box text-primary"></i> Type</span>
                <span class="detail-value">${escapeHtml(ontologyTypeName)}</span>
            </div>
            <div class="entity-detail-item">
                <span class="detail-key"><i class="bi bi-key-fill text-warning"></i> ID</span>
                <span class="detail-value">${escapeHtml(actualIdValue || 'N/A')}</span>
            </div>
        </div>
    `;
    
    // Filter attributes to only show custom ones (exclude ID, Label, Dashboard)
    const customAttributes = new Map();
    
    if (entityMapping && entityMapping.attributeMappings) {
        // With mapping: show mapped attributes in order, excluding special ones
        for (const [attrName, columnName] of Object.entries(entityMapping.attributeMappings)) {
            const attrNameLower = attrName.toLowerCase();
            const columnNameLower = columnName.toLowerCase();
            
            // Skip if this is a special attribute
            if (specialAttrNames.has(attrNameLower) || specialAttrNames.has(columnNameLower) ||
                specialAttrNames.has(normalizeAttr(attrName)) || specialAttrNames.has(normalizeAttr(columnName))) {
                continue;
            }
            
            const value = findAttributeValue(allAttributes, columnName) || findAttributeValue(allAttributes, attrName);
            if (value) {
                customAttributes.set(attrName, value);
            }
        }
    } else {
        // Without mapping: show all attributes except special ones
        for (const [key, value] of allAttributes.entries()) {
            const keyLower = key.toLowerCase();
            const keyNormalized = normalizeAttr(key);
            
            // Skip if this is a special attribute
            if (specialAttrNames.has(keyLower) || specialAttrNames.has(keyNormalized)) {
                continue;
            }
            
            customAttributes.set(key, value);
        }
    }
    
    // Render Attributes section (custom attributes only)
    if (customAttributes.size > 0) {
        html += `
            <div class="entity-detail-section">
                <h6><i class="bi bi-tags"></i> Attributes</h6>
        `;
        
        for (const [key, value] of customAttributes.entries()) {
            html += `
                <div class="entity-detail-item">
                    <span class="detail-key"><i class="bi bi-card-text text-secondary"></i> ${escapeHtml(key)}</span>
                    <span class="detail-value">${escapeHtml(value)}</span>
                </div>
            `;
        }
        html += `</div>`;
    } else {
        html += `
            <div class="entity-detail-section">
                <h6><i class="bi bi-tags"></i> Attributes</h6>
                <p class="small text-muted mb-0">No custom attributes found for this entity.</p>
            </div>
        `;
    }
    
    // Dashboard section (separate, at the bottom of attributes area)
    if (dashboardUrl) {
        console.log('[Dashboard] Building URL with params:', { dashboardUrl, dashboardParams, actualIdValue });
        
        // Build parameter values from entity attributes
        // dashboardParams format: { paramKeyword: { attribute, datasetId, pageId, widgetId } }
        const paramValues = {};
        for (const [paramKeyword, mapping] of Object.entries(dashboardParams)) {
            // Handle both old and new format
            const attrName = typeof mapping === 'object' ? mapping.attribute : mapping;
            const pageId = typeof mapping === 'object' ? mapping.pageId : '';
            const widgetId = typeof mapping === 'object' ? mapping.widgetId : '';
            
            let value = null;
            if (attrName === '__ID__') {
                value = actualIdValue;
                console.log(`[Dashboard] Param ${paramKeyword} = ID value: ${actualIdValue}`);
            } else {
                // Find attribute value
                value = findAttributeValue(allAttributes, attrName);
                console.log(`[Dashboard] Param ${paramKeyword} = attribute ${attrName}: ${value}`);
            }
            
            if (value) {
                paramValues[paramKeyword] = {
                    value: value,
                    pageId: pageId,
                    widgetId: widgetId
                };
            }
        }
        
        console.log('[Dashboard] Final paramValues:', JSON.stringify(paramValues));
        
        // Build dashboard URL with mapped parameters
        const dashboardUrlWithParams = buildDashboardUrl(dashboardUrl, actualIdValue, paramValues);
        
        html += `
            <div class="entity-detail-section">
                <h6><i class="bi bi-speedometer2"></i> Dashboard</h6>
                <div class="entity-detail-item">
                    <button onclick="openDashboardModal('${escapeHtml(dashboardUrlWithParams)}', '${escapeHtml(ontologyTypeName)}', '${escapeHtml(actualIdValue || '')}')" 
                            class="btn btn-sm btn-outline-info w-100" title="Open dashboard">
                        <i class="bi bi-speedometer2 me-1"></i>View Dashboard
                    </button>
                </div>
            </div>
        `;
    }

    // Cross-domain bridges
    const bridges = entityMapping?.bridges || classInfo?.bridges || [];
    if (bridges.length > 0) {
        html += `
            <div class="entity-detail-section">
                <h6><i class="bi bi-signpost-2"></i> Bridges (${bridges.length})</h6>
        `;
        for (const bridge of bridges) {
            const tgtDom = bridge.target_domain || bridge.target_project || '';
            const targetEntityUri = actualIdValue
                ? (bridge.target_class_uri || '') + '#' + actualIdValue
                : (bridge.target_class_uri || '');
            const resolveUrl = '/resolve?uri=' + encodeURIComponent(targetEntityUri) +
                '&domain=' + encodeURIComponent(tgtDom);
            const tooltip = escapeHtml(bridge.label || 'Navigate to ' + bridge.target_class_name + ' in ' + tgtDom);
            html += `
                <div class="entity-detail-item">
                    <a href="${escapeHtml(resolveUrl)}" class="btn btn-sm btn-outline-primary w-100 text-start" title="${tooltip}">
                        <i class="bi bi-signpost-2 me-1"></i>
                        <span class="fw-semibold">${escapeHtml(bridge.target_class_name || '')}</span>
                        <small class="text-muted ms-1"><i class="bi bi-folder2-open ms-1 me-1"></i>${escapeHtml(tgtDom)}</small>
                        <i class="bi bi-box-arrow-up-right ms-auto float-end mt-1"></i>
                    </a>
                </div>
            `;
        }
        html += `</div>`;
    }
    
    // Outgoing relationships
    if (outgoingRels.length > 0) {
        html += `
            <div class="entity-detail-section">
                <h6><i class="bi bi-arrow-right-circle"></i> Outgoing (${outgoingRels.length})</h6>
        `;
        for (const rel of outgoingRels) {
            const targetId = typeof rel.target === 'object' ? rel.target.id : rel.target;
            const targetNode = d3NodesData.find(n => n.id === targetId);
            const targetLabel = targetNode ? getDisplayLabel(targetNode) : extractEntityLabel(targetId);
            const targetIcon = targetNode ? getEntityIcon(targetNode) : '🔷';
            
            html += `
                <div class="entity-relationship-item">
                    <span class="rel-direction">→</span>
                    <span class="rel-predicate">${escapeHtml(rel.predicate)}</span>
                    <span class="rel-direction">→</span>
                    <span class="rel-target" onclick="selectEntityById('${escapeHtml(targetId)}')">${targetIcon} ${escapeHtml(targetLabel)}</span>
                </div>
            `;
        }
        html += `</div>`;
    }
    
    // Incoming relationships
    if (incomingRels.length > 0) {
        html += `
            <div class="entity-detail-section">
                <h6><i class="bi bi-arrow-left-circle"></i> Incoming (${incomingRels.length})</h6>
        `;
        for (const rel of incomingRels) {
            const sourceId = typeof rel.source === 'object' ? rel.source.id : rel.source;
            const sourceNode = d3NodesData.find(n => n.id === sourceId);
            const sourceLabel = sourceNode ? getDisplayLabel(sourceNode) : extractEntityLabel(sourceId);
            const sourceIcon = sourceNode ? getEntityIcon(sourceNode) : '🔷';
            
            html += `
                <div class="entity-relationship-item">
                    <span class="rel-target" onclick="selectEntityById('${escapeHtml(sourceId)}')">${sourceIcon} ${escapeHtml(sourceLabel)}</span>
                    <span class="rel-direction">→</span>
                    <span class="rel-predicate">${escapeHtml(rel.predicate)}</span>
                    <span class="rel-direction">→</span>
                </div>
            `;
        }
        html += `</div>`;
    }
    
    // Full URI section
    html += `
        <div class="entity-detail-section">
            <h6><i class="bi bi-link-45deg"></i> Full URI</h6>
            <div class="small text-muted" style="word-break: break-all;">${escapeHtml(entity.id)}</div>
        </div>
    `;
    
    container.innerHTML = html;
}

function clearEntityDetails() {
    const container = document.getElementById('entityDetailsContent');
    if (container) {
        container.innerHTML = `
            <div class="entity-details-placeholder">
                <i class="bi bi-cursor"></i>
                <p class="small mb-0">Click on an entity or<br>relationship to view details</p>
            </div>
        `;
    }
}

function showRelationshipDetails(relationship) {
    const container = document.getElementById('entityDetailsContent');
    if (!container) return;
    
    // Get source and target nodes
    const sourceNode = typeof relationship.source === 'object' ? relationship.source : 
        d3NodesData.find(n => n.id === relationship.source);
    const targetNode = typeof relationship.target === 'object' ? relationship.target : 
        d3NodesData.find(n => n.id === relationship.target);
    
    // Get predicate local name
    const predicateUri = relationship.predicate || '';
    const predicateLabel = predicateUri.includes('#') ? predicateUri.split('#').pop() : 
        predicateUri.includes('/') ? predicateUri.split('/').pop() : predicateUri;
    
    // Get icons for source and target
    const sourceIcon = sourceNode ? getEntityIcon(sourceNode) : '📦';
    const targetIcon = targetNode ? getEntityIcon(targetNode) : '📦';
    const sourceLabel = sourceNode ? getDisplayLabel(sourceNode) : 'Unknown';
    const targetLabel = targetNode ? getDisplayLabel(targetNode) : 'Unknown';
    
    let html = `
        <div class="entity-detail-header">
            <span class="entity-detail-icon">🔗</span>
            <div class="entity-detail-title">
                <h6>${escapeHtml(predicateLabel)}</h6>
                <small>Relationship</small>
            </div>
        </div>
    `;
    
    // Relationship info section (same styling as entity info)
    html += `
        <div class="entity-detail-section">
            <h6><i class="bi bi-card-list"></i> Relationship Info</h6>
            <div class="entity-detail-item">
                <span class="detail-key">Name</span>
                <span class="detail-value">${escapeHtml(predicateLabel)}</span>
            </div>
            <div class="entity-detail-item">
                <span class="detail-key">URI</span>
                <span class="detail-value small" style="word-break: break-all;">${escapeHtml(predicateUri)}</span>
            </div>
        </div>
    `;
    
    // Look up relationship mapping info if available
    const mappingAttrs = [];
    if (typeof relationshipMappings !== 'undefined' && relationshipMappings) {
        const predLower = predicateLabel.toLowerCase();
        const relMapping = relationshipMappings[predLower] || 
            Object.values(relationshipMappings).find(m => 
                m.predicate && m.predicate.toLowerCase().includes(predLower)
            );
        if (relMapping) {
            if (relMapping.sourceTable) mappingAttrs.push({ key: 'Source Table', value: relMapping.sourceTable });
            if (relMapping.targetTable) mappingAttrs.push({ key: 'Target Table', value: relMapping.targetTable });
            if (relMapping.joinColumn) mappingAttrs.push({ key: 'Join Column', value: relMapping.joinColumn });
            if (relMapping.sourceColumn) mappingAttrs.push({ key: 'Source Column', value: relMapping.sourceColumn });
            if (relMapping.targetColumn) mappingAttrs.push({ key: 'Target Column', value: relMapping.targetColumn });
        }
    }
    
    // Mapping info section (if available)
    if (mappingAttrs.length > 0) {
        html += `
            <div class="entity-detail-section">
                <h6><i class="bi bi-database"></i> Mapping Info</h6>
        `;
        for (const attr of mappingAttrs) {
            html += `
                <div class="entity-detail-item">
                    <span class="detail-key">${escapeHtml(attr.key)}</span>
                    <span class="detail-value">${escapeHtml(attr.value)}</span>
                </div>
            `;
        }
        html += `</div>`;
    }
    
    // Source entity section
    html += `
        <div class="entity-detail-section">
            <h6><i class="bi bi-box-arrow-right"></i> Source Entity</h6>
            <div class="entity-relationship-item" style="cursor: pointer;" onclick="selectEntityById('${escapeHtml(sourceNode?.id || '')}')">
                <span class="me-2">${sourceIcon}</span>
                <span class="rel-target">${escapeHtml(sourceLabel)}</span>
            </div>
        </div>
    `;
    
    // Target entity section
    html += `
        <div class="entity-detail-section">
            <h6><i class="bi bi-box-arrow-in-right"></i> Target Entity</h6>
            <div class="entity-relationship-item" style="cursor: pointer;" onclick="selectEntityById('${escapeHtml(targetNode?.id || '')}')">
                <span class="me-2">${targetIcon}</span>
                <span class="rel-target">${escapeHtml(targetLabel)}</span>
            </div>
        </div>
    `;
    
    // Visual representation
    html += `
        <div class="entity-detail-section">
            <h6><i class="bi bi-diagram-3"></i> Triple Pattern</h6>
            <div class="p-2 bg-light rounded small">
                <div class="d-flex align-items-center justify-content-between flex-wrap gap-1">
                    <span class="badge bg-success">${sourceIcon} ${escapeHtml(sourceLabel)}</span>
                    <i class="bi bi-arrow-right text-muted"></i>
                    <span class="badge bg-primary">${escapeHtml(predicateLabel)}</span>
                    <i class="bi bi-arrow-right text-muted"></i>
                    <span class="badge bg-info">${targetIcon} ${escapeHtml(targetLabel)}</span>
                </div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

function selectEntityById(entityId) {
    const entity = d3NodesData.find(n => n.id === entityId);
    if (entity) {
        showEntityDetails(entity);
        
        // Clear all selections and reset all hitarea strokes
        d3.selectAll('.d3-node').classed('selected', false).classed('pinned', false);
        d3.selectAll('.d3-node-hitarea').attr('stroke', 'none').attr('fill', 'transparent');
        d3.selectAll('.d3-link').classed('selected', false);
        d3.selectAll('.d3-link-hitarea')
            .classed('selected', false)
            .attr('fill', '#e9ecef')
            .attr('stroke', '#999')
            .attr('stroke-width', 1.5);
        
        // Highlight the node in the graph
        d3.selectAll('.d3-node').each(function(d) {
            if (d.id === entityId) {
                d3.select(this).classed('selected', true);
                d3.select(this).select('.d3-node-hitarea').attr('stroke', '#0d6efd').attr('stroke-width', 3);
            }
        });
    }
}

function getEntityAttributes(entityId) {
    // Get attributes from the last query results
    const attributes = [];
    const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
    const RDFS_LABEL = 'http://www.w3.org/2000/01/rdf-schema#label';
    
    if (lastQueryResults && lastQueryResults.results) {
        const columns = lastQueryResults.columns || [];
        const subjectCol = columns.find(c => c.toLowerCase() === 'subject' || c === 's') || columns[0];
        const predicateCol = columns.find(c => c.toLowerCase() === 'predicate' || c === 'p') || columns[1];
        const objectCol = columns.find(c => c.toLowerCase() === 'object' || c === 'o') || columns[2];
        
        for (const row of lastQueryResults.results) {
            const subject = row[subjectCol];
            const predicate = predicateCol ? row[predicateCol] : '';
            const object = objectCol ? row[objectCol] : '';
            
            if (subject === entityId && predicate && object) {
                // Skip type and label predicates (already shown)
                if (predicate === RDF_TYPE || predicate.endsWith('#type') || predicate.endsWith('/type')) continue;
                if (predicate === RDFS_LABEL || predicate.endsWith('#label') || predicate.endsWith('/label')) continue;
                
                // Skip if object is a URI (it's a relationship, not an attribute)
                if (object.startsWith('http://') || object.startsWith('https://')) continue;
                
                attributes.push({
                    predicate: extractEntityLabel(predicate),
                    value: object
                });
            }
        }
    }
    
    return attributes;
}

function truncateUri(uri) {
    if (!uri || uri.length <= 50) return uri;
    return uri.substring(0, 25) + '...' + uri.substring(uri.length - 22);
}

function findMappingByType(entityType) {
    if (!entityType) return null;
    
    const typeLower = entityType.toLowerCase();
    const typeLocalName = typeLower.split('#').pop().split('/').pop();
    
    // Try direct match
    if (entityMappings[typeLower]) return entityMappings[typeLower];
    if (entityMappings[typeLocalName]) return entityMappings[typeLocalName];
    
    // Try partial match
    for (const [key, mapping] of Object.entries(entityMappings)) {
        if (key.includes(typeLocalName) || typeLocalName.includes(key)) {
            return mapping;
        }
    }
    
    return null;
}

/**
 * Extract the ontology class name from an entity URI.
 * URIs typically follow patterns like:
 * - https://databricks-ontology.com/MyOntology/Meter/MTR000282 → "Meter" (path-based)
 * - http://ontobricks.org/ontology#Meter/12345 → "Meter" (R2RML format)
 * - http://ontobricks.org/ontology#Meter_12345 → "Meter"
 * - http://ontobricks.org/ontology#Customer_67890 → "Customer"
 */
function extractClassFromUri(uri) {
    if (!uri) return null;
    
    // Split by both # and / to analyze the URI structure
    const segments = uri.split(/[#\/]/);
    
    // Filter out empty segments and common prefixes
    const meaningfulSegments = segments.filter(s => 
        s && s.length > 0 && 
        !s.includes(':') && // Skip protocol parts like "https:"
        !s.includes('.') && // Skip domain parts like "databricks-ontology.com"
        s !== 'http' && s !== 'https'
    );
    
    console.log('  extractClassFromUri segments:', meaningfulSegments);
    
    if (meaningfulSegments.length >= 2) {
        // The pattern is typically: .../ClassName/InstanceID
        // So the class is second-to-last meaningful segment
        const lastSegment = meaningfulSegments[meaningfulSegments.length - 1];
        const secondLastSegment = meaningfulSegments[meaningfulSegments.length - 2];
        
        // Check if second-to-last looks like a class name (starts with capital letter or is a word)
        // and last segment looks like an ID (contains numbers or is alphanumeric code)
        if (/^[A-Za-z][A-Za-z0-9]*$/.test(secondLastSegment) && 
            (/\d/.test(lastSegment) || /^[A-Z]{2,}/.test(lastSegment))) {
            return secondLastSegment;
        }
    }
    
    // Fallback: try the last meaningful segment
    if (meaningfulSegments.length >= 1) {
        const lastSegment = meaningfulSegments[meaningfulSegments.length - 1];
        
        // Pattern: ClassName_ID
        const underscoreMatch = lastSegment.match(/^([A-Za-z][A-Za-z0-9]*)_/);
        if (underscoreMatch) {
            return underscoreMatch[1];
        }
        
        // Pattern: ClassNameNNNN (letters followed by numbers)
        const letterNumberMatch = lastSegment.match(/^([A-Za-z]+)\d+$/);
        if (letterNumberMatch) {
            return letterNumberMatch[1];
        }
        
        // If it looks like a class name (not an ID)
        if (/^[A-Za-z][A-Za-z0-9]*$/.test(lastSegment) && !/^\d+$/.test(lastSegment) && !/^[A-Z]{2,}\d+$/.test(lastSegment)) {
            return lastSegment;
        }
    }
    
    return null;
}

/**
 * Find entity mapping by entity URI (extracting class name from URI pattern)
 */
function findMappingByUri(entityUri) {
    const className = extractClassFromUri(entityUri);
    if (!className) return null;
    
    const classLower = className.toLowerCase();
    console.log('[findMappingByUri] Looking for class:', classLower, 'in keys:', Object.keys(entityMappings));
    
    // Try direct match
    if (entityMappings[classLower]) {
        console.log('[findMappingByUri] Direct match found for:', classLower);
        return entityMappings[classLower];
    }
    
    // Try partial/fuzzy match (handles typos like "subscribtion" vs "subscription")
    for (const [key, mapping] of Object.entries(entityMappings)) {
        // Check if either contains the other
        if (key.includes(classLower) || classLower.includes(key)) {
            console.log('[findMappingByUri] Partial match found:', key, 'for', classLower);
            return mapping;
        }
        
        // Fuzzy match: check if they're similar (within 2 character difference)
        if (isSimilar(key, classLower, 2)) {
            console.log('[findMappingByUri] Fuzzy match found:', key, 'for', classLower);
            return mapping;
        }
    }
    
    console.log('[findMappingByUri] No match found for:', classLower);
    return null;
}

/**
 * Check if two strings are similar (Levenshtein distance <= maxDist)
 */
function isSimilar(str1, str2, maxDist) {
    if (Math.abs(str1.length - str2.length) > maxDist) return false;
    
    // Simple character difference count
    let diff = 0;
    const longer = str1.length > str2.length ? str1 : str2;
    const shorter = str1.length > str2.length ? str2 : str1;
    
    for (let i = 0; i < shorter.length; i++) {
        if (shorter[i] !== longer[i]) diff++;
        if (diff > maxDist) return false;
    }
    diff += longer.length - shorter.length;
    return diff <= maxDist;
}
