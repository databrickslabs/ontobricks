// =====================================================
// ONTOLOGY DESIGN - OntoViz Integration
// =====================================================

let designerInitialized = false;
let autoSaveTimeout = null;
let isLoadingData = false;  // Flag to prevent auto-save during data loading
let isViewOnlyMode = true;  // Default to view-only mode
let layoutDirty = false;    // Track unsaved layout changes
let ontologyVersionAtLoad = null;  // Track ontology version when design was last loaded

/**
 * Resolve an entity name (from a property's domain/range) to an ID in the entity map.
 * Handles exact match, URI-based local name extraction, and case-insensitive fallback.
 */
function _resolveEntityId(entityIdMap, entityNameLower, name) {
    if (!name) return undefined;
    // 1. Direct exact match
    if (entityIdMap.has(name)) return entityIdMap.get(name);
    // 2. Extract local name from URI (e.g. "http://example.org/onto#MyClass" → "MyClass")
    if (name.includes('#') || (name.includes('/') && name.includes(':'))) {
        const localName = name.split('#').pop().split('/').pop();
        if (localName && entityIdMap.has(localName)) return entityIdMap.get(localName);
    }
    // 3. Case-insensitive fallback
    return entityNameLower.get(name.toLowerCase());
}

/**
 * Toggle between View and Edit modes
 * @param {boolean} viewOnly - true for view-only mode, false for edit mode
 */
function setDesignerViewMode(viewOnly) {
    isViewOnlyMode = viewOnly;
    
    // Update button states
    const viewBtn = document.getElementById('viewModeBtn');
    const editBtn = document.getElementById('editModeBtn');
    const descEl = document.getElementById('designModeDescription');
    
    if (viewBtn && editBtn) {
        if (viewOnly) {
            viewBtn.classList.add('active');
            editBtn.classList.remove('active');
        } else {
            viewBtn.classList.remove('active');
            editBtn.classList.add('active');
        }
    }
    
    // Update description text
    if (descEl) {
        if (viewOnly) {
            descEl.textContent = 'View your ontology visually. Click "Edit" to modify entities and relationships.';
        } else {
            descEl.textContent = 'Design your ontology by creating entities (classes) and relationships (properties) visually. Changes are saved automatically.';
        }
    }
    
    // Update OntoViz mode
    if (ontologyDesigner) {
        ontologyDesigner.setViewOnly(viewOnly);
    }
    
    console.log('[DESIGN] Mode changed to:', viewOnly ? 'View Only' : 'Edit');
}

/**
 * Initialize mode toggle buttons
 */
function initDesignerModeToggle() {
    const viewBtn = document.getElementById('viewModeBtn');
    const editBtn = document.getElementById('editModeBtn');
    
    if (viewBtn) {
        viewBtn.addEventListener('click', () => setDesignerViewMode(true));
    }
    
    if (editBtn) {
        editBtn.addEventListener('click', () => {
            if (window.isActiveVersion === false) return;
            setDesignerViewMode(false);
        });
    }
    
    // Set initial state (view-only by default)
    setDesignerViewMode(true);
}

// Initialize mode toggle when DOM is ready
document.addEventListener('DOMContentLoaded', initDesignerModeToggle);

/**
 * Show/hide the loading overlay for ontology designer
 */
function showOntologyDesignerLoading(show) {
    const loadingEl = document.getElementById('ontologyDesignerLoading');
    if (loadingEl) {
        loadingEl.style.display = show ? 'flex' : 'none';
    }
}

/**
 * Show/hide the empty state overlay
 */
function showOntologyDesignerEmptyState(show) {
    const emptyEl = document.getElementById('ontologyDesignerEmptyState');
    if (emptyEl) {
        emptyEl.style.display = show ? 'flex' : 'none';
    }
}

/**
 * Initialize the ontology designer (OntoViz)
 */
async function initOntologyDesigner() {
    // Show loading overlay
    showOntologyDesignerLoading(true);
    
    // First check if we have any views
    const hasViews = await refreshViewList();
    
    if (!hasViews) {
        // No views - show empty state and don't load anything
        showOntologyDesignerLoading(false);
        showOntologyDesignerEmptyState(true);
        return;
    }
    
    // Hide empty state since we have views
    showOntologyDesignerEmptyState(false);
    
    if (designerInitialized && ontologyDesigner) {
        // Already initialized — check if ontology data changed while we were away
        const currentVersion = _getOntologyVersion();
        if (ontologyVersionAtLoad === currentVersion) {
            // Ontology unchanged: keep the in-memory layout as-is (no reload needed)
            console.log('[DESIGN] Ontology unchanged — preserving current layout');
            showOntologyDesignerLoading(false);
            return;
        }
        
        // Ontology changed (entities/relationships edited in another section): reload
        console.log('[DESIGN] Ontology changed — reloading layout');
        try {
            await loadOntologyIntoDesigner(false);
        } catch (error) {
            console.error('Error refreshing ontology designer:', error);
        } finally {
            setTimeout(() => showOntologyDesignerLoading(false), 300);
        }
        return;
    }
    
    // Initialize the canvas then load the current view's data
    await initializeOntoVizCanvas();
    
    try {
        await loadOntologyIntoDesigner(false);
    } catch (error) {
        console.error('Error loading ontology into designer:', error);
    } finally {
        setTimeout(() => showOntologyDesignerLoading(false), 300);
    }
}

/**
 * Compute a lightweight version fingerprint of the ontology data.
 * Used to detect whether entities/relationships changed while the design section was hidden.
 */
function _getOntologyVersion() {
    if (typeof OntologyState === 'undefined' || !OntologyState.config) return null;
    const classes = OntologyState.config.classes || [];
    const props = OntologyState.config.properties || [];
    // Encode class count + names + property count + names into a simple fingerprint
    const parts = [
        classes.length,
        classes.map(c => c.name + ':' + (c.dataProperties || []).length + ':' + (c.parent || '')).join(','),
        props.length,
        props.map(p => p.name + ':' + p.type + ':' + (p.domain || '') + ':' + (p.range || '')).join(',')
    ];
    return parts.join('|');
}

/**
 * Initialize the OntoViz canvas (separate function for reuse)
 */
async function initializeOntoVizCanvas() {
    const canvas = document.getElementById('ontology-designer-canvas');
    if (!canvas) {
        console.error('Design canvas not found');
        showOntologyDesignerLoading(false);
        return;
    }
    
    // Check if editing is allowed (active version only)
    const canEdit = window.isActiveVersion !== false;
    
    // Initialize OntoViz with simplified properties (name only, no types or keys)
    // Start in view-only mode by default
    ontologyDesigner = new OntoViz('#ontology-designer-canvas', {
        showToolbar: canEdit,  // Hide toolbar for inactive versions
        showMinimap: false,
        showStatusBar: true,
        showPropertyTypes: false,  // Hide property types - ontology uses simple attribute names
        showPropertyKeys: false,   // Hide primary key toggles - not relevant for ontologies
        snapToGrid: true,
        gridSize: 24,
        viewOnly: isViewOnlyMode,  // Start in view-only mode by default
        onEntityCreate: canEdit ? function(entity) {
            console.log('Entity created:', entity.name);
            scheduleAutoSave();
        } : null,
        onEntityUpdate: canEdit ? function(entity) {
            console.log('Entity updated:', entity.name);
            scheduleAutoSave();
        } : null,
        onEntityDelete: canEdit ? function(entity) {
            console.log('Entity deleted:', entity.name);
            scheduleAutoSave();
        } : null,
        onRelationshipCreate: canEdit ? function(rel) {
            console.log('Relationship created:', rel.name);
            scheduleAutoSave();
        } : null,
        onRelationshipUpdate: canEdit ? function(rel, changeInfo) {
            console.log('Relationship updated:', rel.name, changeInfo);
            // If relationship was renamed, update references in mappings, constraints, axioms
            if (changeInfo && changeInfo.renamed && changeInfo.oldName) {
                console.log(`Relationship renamed from "${changeInfo.oldName}" to "${rel.name}"`);
                fetch('/ontology/update-relationship-references', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        old_name: changeInfo.oldName,
                        new_name: rel.name
                    })
                })
                .then(resp => resp.json())
                .then(result => {
                    if (result.success && result.updates) {
                        const total = result.updates.mappings_updated + 
                                      result.updates.constraints_updated + 
                                      result.updates.axioms_updated;
                        if (total > 0 && typeof showNotification === 'function') {
                            showNotification(result.message, 'info', 3000);
                        }
                    }
                })
                .catch(err => console.error('Error updating references:', err));
            }
            scheduleAutoSave();
        } : null,
        onRelationshipDelete: canEdit ? function(rel) {
            console.log('Relationship deleted:', rel.name);
            scheduleAutoSave();
        } : null,
        onInheritanceCreate: canEdit ? function(inh) {
            console.log('Inheritance created:', inh.sourceEntityId, '->', inh.targetEntityId);
            scheduleAutoSave();
        } : null,
        onInheritanceUpdate: canEdit ? function(inh) {
            console.log('Inheritance updated:', inh.id);
            scheduleAutoSave();
        } : null,
        onInheritanceDelete: canEdit ? function(inh) {
            console.log('Inheritance deleted:', inh.id);
            scheduleAutoSave();
        } : null,
        onVisibilityChange: canEdit ? function(type, id, visible) {
            console.log('Visibility changed:', type, id, '->', visible);
            scheduleAutoSave();
        } : null,
        onSelectionChange: function(selection) {
            // Could update a properties panel here
        },
        onLayoutChange: function(reason) {
            console.log('[DESIGN] Layout changed via:', reason);
            scheduleAutoSave();
        }
    });
    
    designerInitialized = true;
    console.log('Ontology Designer initialized');
}

/**
 * Schedule an auto-save with debouncing
 * Skips if data is currently being loaded to prevent overwriting ontology during load
 */
function scheduleAutoSave() {
    // Don't auto-save while loading data - this prevents overwriting ontology during view switches
    if (isLoadingData) {
        console.log('[AUTO-SAVE] Skipped - data loading in progress');
        return;
    }
    
    layoutDirty = true;
    
    if (autoSaveTimeout) {
        clearTimeout(autoSaveTimeout);
    }
    autoSaveTimeout = setTimeout(async () => {
        // Double-check flag in case loading started during the timeout
        if (isLoadingData) {
            console.log('[AUTO-SAVE] Skipped - data loading in progress');
            return;
        }
        await syncDesignToOntology(false);
        layoutDirty = false;
    }, 500); // Save 500ms after last change
}

/**
 * Flush any pending auto-save immediately and save layout.
 * Called before navigating away from the design section.
 */
async function flushDesignLayout() {
    if (autoSaveTimeout) {
        clearTimeout(autoSaveTimeout);
        autoSaveTimeout = null;
    }
    
    if (!ontologyDesigner || isLoadingData) return;
    
    if (layoutDirty) {
        console.log('[FLUSH] Saving pending layout changes');
        await saveDesignLayoutOnly();
        layoutDirty = false;
    }
}

/**
 * Save only the visual layout (positions, anchors, visibility) without full ontology sync.
 * Lighter than syncDesignToOntology — used for position-only changes and pre-navigation saves.
 */
async function saveDesignLayoutOnly() {
    if (!ontologyDesigner) return;
    const design = ontologyDesigner.toJSON();
    await saveDesignLayout(design);
    console.log('[LAYOUT] Layout saved (positions only)');
}

/**
 * Beacon-based layout save for page unload.
 * navigator.sendBeacon guarantees delivery even during page teardown.
 */
function saveDesignLayoutBeacon() {
    if (!ontologyDesigner || !layoutDirty || isLoadingData) return;
    
    try {
        const design = ontologyDesigner.toJSON();
        const cleanedDesign = {
            entities: (design.entities || []).map(entity => ({
                id: entity.id,
                name: entity.name,
                x: entity.x,
                y: entity.y,
                properties: entity.properties,
                color: entity.color
            })),
            relationships: design.relationships,
            inheritances: design.inheritances,
            visibility: design.visibility
        };
        
        const blob = new Blob(
            [JSON.stringify(cleanedDesign)],
            { type: 'application/json' }
        );
        navigator.sendBeacon('/domain/design-views/save-current', blob);
        layoutDirty = false;
        console.log('[BEACON] Layout saved via sendBeacon');
    } catch (error) {
        console.warn('[BEACON] Failed to save layout:', error);
    }
}

window.addEventListener('beforeunload', saveDesignLayoutBeacon);
window.addEventListener('pagehide', saveDesignLayoutBeacon);

/**
 * Sync design to OntologyState and optionally save to session
 */
async function syncDesignToOntology(showFeedback = false) {
    if (!ontologyDesigner) return;
    
    const design = ontologyDesigner.toJSON();
    
    // Build a map of entity ID to entity name for inheritance lookup
    const entityIdToName = new Map();
    design.entities.forEach(e => entityIdToName.set(e.id, e.name));
    
    // Build inheritance parent map: childName -> parentName
    // In inheritance links: direction 'forward' means source is parent, target is child
    const childToParentMap = new Map();
    if (design.inheritances && design.inheritances.length > 0) {
        design.inheritances.forEach(inh => {
            let parentId, childId;
            if (inh.direction === 'forward') {
                parentId = inh.sourceEntityId;
                childId = inh.targetEntityId;
            } else {
                parentId = inh.targetEntityId;
                childId = inh.sourceEntityId;
            }
            
            const parentName = entityIdToName.get(parentId);
            const childName = entityIdToName.get(childId);
            
            if (parentName && childName) {
                childToParentMap.set(childName, parentName);
            }
        });
    }
    
    // Build lookup maps from existing OntologyState so we preserve URIs and
    // other metadata that the visual designer doesn't carry (uri, label, dashboard, etc.)
    const existingClassByName = new Map();
    (OntologyState.config?.classes || []).forEach(c => existingClassByName.set(c.name, c));
    
    const existingPropByName = new Map();
    (OntologyState.config?.properties || []).forEach(p => {
        if (p.type === 'ObjectProperty') existingPropByName.set(p.name, p);
    });
    
    // Convert design to ontology format (including parent from inheritance)
    const classes = design.entities.map(entity => {
        const existing = existingClassByName.get(entity.name) || {};
        
        // Get own properties
        const ownProperties = entity.properties.map(prop => ({
            name: prop.name,
            localName: prop.name,
            inherited: false
        }));
        
        // Get inherited properties from the OntoViz designer
        const inheritedProps = ontologyDesigner.getInheritedProperties(entity.id);
        const inheritedProperties = inheritedProps.map(prop => ({
            name: prop.name,
            localName: prop.name,
            inherited: true,
            inheritedFrom: prop.inheritedFrom
        }));
        
        const classObj = {
            uri: existing.uri || undefined,
            name: entity.name,
            localName: entity.name,
            label: existing.label || entity.name,
            comment: existing.comment || entity.description || '',
            emoji: entity.icon || '📦',
            description: entity.description || existing.comment || '',
            dashboard: existing.dashboard || '',
            dashboardParams: existing.dashboardParams || {},
            dataProperties: [...ownProperties, ...inheritedProperties]
        };
        
        // Set parent: prefer inheritance link from designer, fall back to existing parent
        const parentName = childToParentMap.get(entity.name);
        if (parentName) {
            classObj.parent = parentName;
        } else if (existing.parent) {
            classObj.parent = existing.parent;
        }
        
        return classObj;
    });
    
    const objectProperties = design.relationships.map(rel => {
        const sourceEntity = design.entities.find(e => e.id === rel.sourceEntityId);
        const targetEntity = design.entities.find(e => e.id === rel.targetEntityId);
        const existing = existingPropByName.get(rel.name) || {};
        return {
            uri: existing.uri || undefined,
            name: rel.name,
            localName: rel.name,
            label: existing.label || rel.name,
            type: 'ObjectProperty',
            domain: sourceEntity ? sourceEntity.name : null,
            range: targetEntity ? targetEntity.name : null,
            direction: rel.direction || 'forward',
            properties: rel.properties || []
        };
    });
    
    // Update OntologyState.config
    if (typeof OntologyState !== 'undefined' && OntologyState.config) {
        const existingDataProperties = (OntologyState.config.properties || []).filter(p => p.type !== 'ObjectProperty');
        
        OntologyState.config.classes = classes;
        OntologyState.config.properties = [...existingDataProperties, ...objectProperties];
        
        // Save to session (await to ensure it completes)
        await saveOntologyToSession(showFeedback);
        
        // Also save the design layout for domain persistence
        await saveDesignLayout(design);
        
        // Regenerate OWL content to reflect changes
        if (typeof autoGenerateOwl === 'function') {
            autoGenerateOwl();
        }
        
        const inheritanceCount = design.inheritances ? design.inheritances.length : 0;
        console.log('Design synced:', classes.length + ' classes, ' + objectProperties.length + ' relationships, ' + inheritanceCount + ' inheritances');
    }
}

/**
 * Save design layout to the current view
 * Only saves layout-specific data (positions, visibility)
 * Icon and description are NOT saved - they come from the ontology
 */
async function saveDesignLayout(design) {
    try {
        // Strip icon and description from entities - they come from ontology
        const cleanedDesign = {
            ...design,
            entities: (design.entities || []).map(entity => ({
                id: entity.id,
                name: entity.name,
                x: entity.x,
                y: entity.y,
                properties: entity.properties,
                color: entity.color
                // icon and description are intentionally NOT saved
            })),
            relationships: design.relationships,
            inheritances: design.inheritances,
            visibility: design.visibility
        };
        
        await fetch('/domain/design-views/save-current', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(cleanedDesign),
            credentials: 'same-origin'
        });
    } catch (error) {
        console.log('Could not save design layout:', error);
    }
}

// =====================================================
// DESIGN VIEWS MANAGEMENT
// =====================================================

let currentDesignView = 'default';

/**
 * Initialize view management UI
 */
async function initViewManagement() {
    // Load available views
    await refreshViewList();
    
    // Bind view selector change
    document.getElementById('designViewSelector')?.addEventListener('change', async function() {
        const viewName = this.value;
        if (viewName && viewName !== currentDesignView) {
            await switchToView(viewName);
        }
    });
    
    // Bind create view button
    document.getElementById('createViewBtn')?.addEventListener('click', showCreateViewDialog);
    
    // Bind rename view button
    document.getElementById('renameViewBtn')?.addEventListener('click', showRenameViewDialog);
    
    // Bind delete view button
    document.getElementById('deleteViewBtn')?.addEventListener('click', showDeleteViewDialog);
}

/**
 * Refresh the view list dropdown
 */
async function refreshViewList() {
    try {
        const response = await fetch('/domain/design-views', { credentials: 'same-origin' });
        const data = await response.json();
        
        if (data.success) {
            const selector = document.getElementById('designViewSelector');
            const hasViews = data.views && data.views.length > 0;
            
            if (selector) {
                if (hasViews) {
                    selector.innerHTML = data.views.map(v => 
                        `<option value="${v}" ${v === data.current_view ? 'selected' : ''}>${v}</option>`
                    ).join('');
                    selector.disabled = false;
                } else {
                    selector.innerHTML = '<option value="">-- No views --</option>';
                    selector.disabled = true;
                }
            }
            currentDesignView = hasViews ? data.current_view : null;
            
            // Update delete/rename/create button state
            const deleteBtn = document.getElementById('deleteViewBtn');
            const renameBtn = document.getElementById('renameViewBtn');
            const createBtn = document.getElementById('createViewBtn');
            const createGroupBtn = document.getElementById('createGroupFromViewBtn');
            if (window.isActiveVersion === false) {
                if (deleteBtn) deleteBtn.disabled = true;
                if (renameBtn) renameBtn.disabled = true;
                if (createBtn) createBtn.disabled = true;
                if (createGroupBtn) createGroupBtn.disabled = true;
            } else {
                if (deleteBtn) deleteBtn.disabled = !hasViews || data.views.length <= 1;
                if (renameBtn) renameBtn.disabled = !hasViews;
            }
            
            // Show/hide empty state
            showOntologyDesignerEmptyState(!hasViews);
            
            return hasViews;
        }
    } catch (error) {
        console.log('Could not load views:', error);
    }
    return false;
}

/**
 * Switch to a different view
 */
async function switchToView(viewName) {
    showOntologyDesignerLoading(true);
    
    // Flush any pending layout changes for the current view before switching
    await flushDesignLayout();
    
    try {
        const response = await fetch('/domain/design-views/switch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: viewName }),
            credentials: 'same-origin'
        });
        const data = await response.json();
        
        if (data.success) {
            currentDesignView = data.current_view;
            
            // Load the view's layout into the designer
            if (ontologyDesigner && data.layout) {
                if (data.layout.entities && data.layout.entities.length > 0) {
                    await loadOntologyIntoDesigner(false);
                } else {
                    await loadFromOntologyFresh();
                }
            }
            
            console.log(`[DESIGN] Switched to view "${viewName}"`);
        } else {
            showNotification(data.message || 'Failed to switch view', 'error');
        }
    } catch (error) {
        console.error('Error switching view:', error);
        showNotification('Error switching view: ' + error.message, 'error');
    } finally {
        // Always hide loading
        setTimeout(() => showOntologyDesignerLoading(false), 300);
    }
}

/**
 * Load fresh from ontology (for new/empty views)
 * All entities are hidden by default - user selects which ones to show via palette
 */
async function loadFromOntologyFresh() {
    if (!ontologyDesigner || typeof OntologyState === 'undefined') return;
    
    // Set flag to prevent auto-save during loading
    isLoadingData = true;
    console.log('[LOAD FRESH] Starting fresh data load - auto-save disabled');
    
    // Always clear the canvas first for new views
    ontologyDesigner.clear();
    
    const classes = OntologyState.config?.classes || [];
    const properties = OntologyState.config?.properties || [];
    
    // If no classes, just show empty canvas
    if (classes.length === 0) {
        isLoadingData = false;
        return;
    }
    
    const entityIdMap = new Map();
    const entityNameLower = new Map();

    const classCount = classes.length;
    const circleRadius = Math.max(300, classCount * 45);
    const circleCX = circleRadius + 100;
    const circleCY = circleRadius + 100;

    const entities = classes.map((cls, idx) => {
        const id = `entity_${Date.now()}_${idx}`;
        const name = cls.name || cls.localName;
        entityIdMap.set(name, id);
        entityNameLower.set(name.toLowerCase(), id);
        const ownProperties = (cls.dataProperties || []).filter(dp => !dp.inherited);
        const angle = (2 * Math.PI * idx) / classCount - Math.PI / 2;
        return {
            id: id,
            name: name,
            type: cls.label || name,
            icon: cls.emoji || OntologyState.defaultClassEmoji || '📦',
            description: cls.description || '',
            properties: ownProperties.map(dp => ({
                name: dp.name || dp.localName || dp,
                type: 'string'
            })),
            x: Math.round(circleCX + circleRadius * Math.cos(angle)),
            y: Math.round(circleCY + circleRadius * Math.sin(angle))
        };
    });
    
    let freshSkipped = 0;
    const relationships = [];
    properties.forEach((prop, idx) => {
        if (prop.type === 'ObjectProperty' || (prop.domain && prop.range)) {
            const sourceId = _resolveEntityId(entityIdMap, entityNameLower, prop.domain);
            const targetId = _resolveEntityId(entityIdMap, entityNameLower, prop.range);
            if (sourceId && targetId) {
                relationships.push({
                    id: `rel_${Date.now()}_${idx}`,
                    name: prop.name,
                    sourceEntityId: sourceId,
                    targetEntityId: targetId,
                    sourceAnchor: 'right',
                    targetAnchor: 'left'
                });
            } else {
                freshSkipped++;
                console.warn(`[DESIGN-FRESH] Relationship "${prop.name}" skipped: domain="${prop.domain}" (${sourceId ? 'found' : 'NOT FOUND'}), range="${prop.range}" (${targetId ? 'found' : 'NOT FOUND'})`);
            }
        }
    });
    if (freshSkipped > 0) {
        console.warn(`[DESIGN-FRESH] ${freshSkipped}/${properties.length} relationship(s) skipped — entity names: [${Array.from(entityIdMap.keys()).join(', ')}]`);
    }
    
    // Build inheritances from ontology parent-child relationships
    const inheritances = [];
    classes.forEach((cls, idx) => {
        const parentName = cls.parent || cls.parentClass;
        if (parentName) {
            const childId = entityIdMap.get(cls.name);
            const parentId = entityIdMap.get(parentName);
            if (childId && parentId) {
                inheritances.push({
                    id: `inh_${Date.now()}_${idx}`,
                    sourceEntityId: parentId,
                    targetEntityId: childId,
                    direction: 'forward'
                });
            } else if (!parentId) {
                console.warn(`[DESIGN] Cannot create inheritance link: parent "${parentName}" not found for "${cls.name}"`);
            }
        }
    });
    
    // Hide ALL entities and relationships by default for new views
    const hiddenEntities = entities.map(e => e.name);
    const hiddenRelationships = relationships.map(r => {
        const srcEntity = entities.find(e => e.id === r.sourceEntityId);
        const tgtEntity = entities.find(e => e.id === r.targetEntityId);
        return { name: r.name, source: srcEntity?.name || '', target: tgtEntity?.name || '' };
    });
    
    const layout = { 
        entities, 
        relationships, 
        inheritances,
        visibility: {
            hiddenEntities: hiddenEntities,
            hiddenRelationships: hiddenRelationships,
            hiddenInheritances: []
        }
    };
    
    // Load with all entities hidden (OntoViz now hides during render)
    ontologyDesigner.fromJSON(layout, { autoLayout: false, center: false, animate: false });
    
    // Re-enable auto-save after loading completes
    setTimeout(() => {
        isLoadingData = false;
        layoutDirty = false;
        ontologyVersionAtLoad = _getOntologyVersion();
        console.log('[LOAD FRESH] Data load complete - auto-save re-enabled');
    }, 600);
}

/**
 * Show create view dialog
 */
function showCreateViewDialog() {
    if (window.isActiveVersion === false) return;
    const existingModal = document.getElementById('createViewModal');
    if (existingModal) existingModal.remove();
    
    // Only show copy option if there's a current view
    const copyOptionHtml = currentDesignView ? `
        <div class="form-check">
            <input type="checkbox" class="form-check-input" id="copyFromCurrent">
            <label class="form-check-label" for="copyFromCurrent">
                Copy layout from current view
            </label>
        </div>
    ` : '';
    
    const modalHtml = `
        <div class="modal fade" id="createViewModal" tabindex="-1">
            <div class="modal-dialog modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-plus-circle me-2"></i>Create New View</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="mb-3">
                            <label class="form-label">View Name</label>
                            <input type="text" class="form-control" id="newViewName" placeholder="Enter view name">
                        </div>
                        ${copyOptionHtml}
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="button" class="btn btn-primary" id="confirmCreateView">Create View</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('createViewModal'));
    
    document.getElementById('confirmCreateView').addEventListener('click', async () => {
        const name = document.getElementById('newViewName').value.trim();
        const copyCheckbox = document.getElementById('copyFromCurrent');
        const copyFrom = (copyCheckbox && copyCheckbox.checked) ? currentDesignView : null;
        
        if (!name) {
            showNotification('Please enter a view name', 'warning');
            return;
        }
        
        try {
            const response = await fetch('/domain/design-views/create', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name, copy_from: copyFrom }),
                credentials: 'same-origin'
            });
            const data = await response.json();
            
            if (data.success) {
                modal.hide();
                await refreshViewList();
                showNotification(`View "${name}" created`, 'success', 2000);
                
                // Hide empty state and ensure designer is initialized
                showOntologyDesignerEmptyState(false);
                
                // Initialize designer if not yet done
                if (!designerInitialized) {
                    await initializeOntoVizCanvas();
                }
                
                // Switch to the new view
                document.getElementById('designViewSelector').value = name;
                await switchToView(name);
            } else {
                showNotification(data.message || 'Failed to create view', 'error');
            }
        } catch (error) {
            showNotification('Error creating view: ' + error.message, 'error');
        }
    });
    
    modal.show();
    document.getElementById('newViewName').focus();
}

/**
 * Show rename view dialog
 */
function showRenameViewDialog() {
    if (window.isActiveVersion === false) return;
    const existingModal = document.getElementById('renameViewModal');
    if (existingModal) existingModal.remove();
    
    const modalHtml = `
        <div class="modal fade" id="renameViewModal" tabindex="-1">
            <div class="modal-dialog modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-pencil me-2"></i>Rename View</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="mb-3">
                            <label class="form-label">Current Name</label>
                            <input type="text" class="form-control" value="${currentDesignView}" disabled>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">New Name</label>
                            <input type="text" class="form-control" id="renameViewName" placeholder="Enter new name">
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="button" class="btn btn-primary" id="confirmRenameView">Rename</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('renameViewModal'));
    
    document.getElementById('confirmRenameView').addEventListener('click', async () => {
        const newName = document.getElementById('renameViewName').value.trim();
        
        if (!newName) {
            showNotification('Please enter a new name', 'warning');
            return;
        }
        
        try {
            const response = await fetch('/domain/design-views/rename', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ old_name: currentDesignView, new_name: newName }),
                credentials: 'same-origin'
            });
            const data = await response.json();
            
            if (data.success) {
                modal.hide();
                currentDesignView = newName;
                await refreshViewList();
                showNotification(`View renamed to "${newName}"`, 'success', 2000);
            } else {
                showNotification(data.message || 'Failed to rename view', 'error');
            }
        } catch (error) {
            showNotification('Error renaming view: ' + error.message, 'error');
        }
    });
    
    modal.show();
    document.getElementById('renameViewName').focus();
}

/**
 * Show delete view dialog
 */
function showDeleteViewDialog() {
    if (window.isActiveVersion === false) return;
    const existingModal = document.getElementById('deleteViewModal');
    if (existingModal) existingModal.remove();
    
    const modalHtml = `
        <div class="modal fade" id="deleteViewModal" tabindex="-1">
            <div class="modal-dialog modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header bg-danger text-white">
                        <h5 class="modal-title"><i class="bi bi-trash me-2"></i>Delete View</h5>
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <p>Are you sure you want to delete the view <strong>"${currentDesignView}"</strong>?</p>
                        <p class="text-muted mb-0">This action cannot be undone. The entities and relationships will still exist in the ontology.</p>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="button" class="btn btn-danger" id="confirmDeleteView">Delete View</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('deleteViewModal'));
    
    document.getElementById('confirmDeleteView').addEventListener('click', async () => {
        try {
            const response = await fetch('/domain/design-views/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: currentDesignView }),
                credentials: 'same-origin'
            });
            const data = await response.json();
            
            if (data.success) {
                modal.hide();
                currentDesignView = data.current_view;
                await refreshViewList();
                showNotification('View deleted', 'success', 2000);
                
                // Load the new current view
                await switchToView(data.current_view);
            } else {
                showNotification(data.message || 'Failed to delete view', 'error');
            }
        } catch (error) {
            showNotification('Error deleting view: ' + error.message, 'error');
        }
    });
    
    modal.show();
}

// Initialize view management when design section is loaded
document.addEventListener('DOMContentLoaded', initViewManagement);

/**
 * Load design layout from the current view
 */
async function loadDesignLayoutFromProject() {
    try {
        const response = await fetch('/domain/design-views/current', { credentials: 'same-origin' });
        const data = await response.json();
        
        // Update current view name
        if (data.current_view) {
            currentDesignView = data.current_view;
        }
        if (data.success && data.layout) {
            return data.layout;
        }
    } catch (error) {
        console.log('Could not load design layout:', error);
    }
    return null;
}

/**
 * Save OntologyState.config to session via API (using global function)
 */
async function saveOntologyToSession(showFeedback = false) {
    // Use the global save function from ontology-core.js
    const success = await window.saveConfigToSession();
    if (success && showFeedback) {
        showSaveIndicator('Saved!');
    }
    return success;
}

/**
 * Show a brief save indicator
 * Styles defined in /static/css/ontology.css
 */
function showSaveIndicator(message) {
    let indicator = document.getElementById('design-save-indicator');
    if (!indicator) {
        indicator = document.createElement('div');
        indicator.id = 'design-save-indicator';
        document.body.appendChild(indicator);
    }
    indicator.textContent = message;
    indicator.style.opacity = '1';
    setTimeout(() => {
        indicator.style.opacity = '0';
    }, 2000);
}

/**
 * Load entities and relationships from OntologyState into the designer
 * @param {boolean} showAlert - Whether to show an alert after loading
 * @returns {boolean} - Whether data was loaded
 */
async function loadOntologyIntoDesigner(showAlert = true) {
    if (!ontologyDesigner) {
        if (showAlert) {
            showNotification('Designer not initialized. Please switch to the Design tab first.', 'warning');
        }
        return false;
    }
    
    // Set flag to prevent auto-save during loading
    isLoadingData = true;
    console.log('[LOAD] Starting data load - auto-save disabled');
    
    // First, try to load from saved design layout (domain persistence)
    const savedLayout = await loadDesignLayoutFromProject();
    
    // If we have a saved layout AND ontology data, merge them
    // The saved layout provides positions/anchors, the ontology provides the current data (including properties)
    if (savedLayout && savedLayout.entities && savedLayout.entities.length > 0 && 
        typeof OntologyState !== 'undefined' && OntologyState.config) {
        
        const classes = OntologyState.config.classes || [];
        const properties = OntologyState.config.properties || [];
        
        // Create lookup maps
        const savedEntityMap = new Map();
        savedLayout.entities.forEach(e => savedEntityMap.set(e.name, e));
        
        // Build saved-entity-ID → name map so we can create composite keys for relationships
        const savedIdToName = new Map();
        savedLayout.entities.forEach(e => { if (e.id && e.name) savedIdToName.set(e.id, e.name); });
        
        // Key relationships by name|source|target to handle duplicate names correctly
        const savedRelMap = new Map();
        savedLayout.relationships.forEach(r => {
            const srcName = savedIdToName.get(r.sourceEntityId) || '';
            const tgtName = savedIdToName.get(r.targetEntityId) || '';
            savedRelMap.set(`${r.name}|${srcName}|${tgtName}`, r);
            // Also store by name-only as fallback for legacy layouts
            if (!savedRelMap.has(`name:${r.name}`)) {
                savedRelMap.set(`name:${r.name}`, r);
            }
        });
        
        // Count new entities (no saved position) so we can spread them in a circle
        let newEntityIdx = 0;
        const newEntityCount = classes.filter(cls => !savedEntityMap.has(cls.name)).length;
        const savedPositions = savedLayout.entities.filter(e => e.x != null && e.y != null);
        const avgX = savedPositions.length > 0
            ? savedPositions.reduce((s, e) => s + e.x, 0) / savedPositions.length
            : 400;
        const avgY = savedPositions.length > 0
            ? savedPositions.reduce((s, e) => s + e.y, 0) / savedPositions.length
            : 300;
        const spreadRadius = Math.max(300, newEntityCount * 50);

        // Merge: use ontology data but preserve layout positions from saved layout
        const mergedLayout = {
            entities: classes.map(cls => {
                const savedEntity = savedEntityMap.get(cls.name);
                const ownProperties = (cls.dataProperties || []).filter(dp => !dp.inherited);
                let posX, posY;
                if (savedEntity?.x != null) {
                    posX = savedEntity.x;
                    posY = savedEntity.y;
                } else {
                    const angle = (2 * Math.PI * newEntityIdx) / Math.max(1, newEntityCount) - Math.PI / 2;
                    posX = Math.round(avgX + spreadRadius * Math.cos(angle));
                    posY = Math.round(avgY + spreadRadius * Math.sin(angle));
                    newEntityIdx++;
                }
                return {
                    id: savedEntity?.id || undefined,
                    name: cls.name || cls.localName,
                    icon: cls.emoji || cls.icon || '📦',
                    description: cls.description || '',
                    x: posX,
                    y: posY,
                    properties: ownProperties.map(dp => ({
                        name: dp.name || dp.localName || 'attribute'
                    }))
                };
            }),
            relationships: []
        };
        
        // Build entity ID map for relationships (with case-insensitive index for robust matching)
        const entityIdMap = new Map();
        const entityNameLower = new Map();
        mergedLayout.entities.forEach((e, idx) => {
            if (!e.id) e.id = 'entity_' + idx;
            entityIdMap.set(e.name, e.id);
            entityNameLower.set(e.name.toLowerCase(), e.id);
        });
        
        // Merge relationships - use ontology properties but preserve layout info
        let skippedRels = 0;
        properties.forEach(prop => {
            if (prop.type === 'ObjectProperty' || (prop.domain && prop.range)) {
                const sourceId = _resolveEntityId(entityIdMap, entityNameLower, prop.domain);
                const targetId = _resolveEntityId(entityIdMap, entityNameLower, prop.range);
                
                if (sourceId && targetId) {
                    // Composite key lookup first, then fallback to name-only
                    const compositeKey = `${prop.name}|${prop.domain}|${prop.range}`;
                    const savedRel = savedRelMap.get(compositeKey) || savedRelMap.get(`name:${prop.name}`);
                    mergedLayout.relationships.push({
                        id: undefined,  // always generate fresh IDs to avoid collisions
                        name: prop.name || prop.localName || 'relates_to',
                        sourceEntityId: sourceId,
                        targetEntityId: targetId,
                        direction: prop.direction || 'forward',
                        properties: prop.properties || [],
                        labelOffsetX: savedRel?.labelOffsetX ?? 0,
                        labelOffsetY: savedRel?.labelOffsetY ?? 0,
                        sourceAnchor: savedRel?.sourceAnchor ?? 'auto',
                        targetAnchor: savedRel?.targetAnchor ?? 'auto'
                    });
                } else {
                    skippedRels++;
                    console.warn(`[DESIGN] Relationship "${prop.name}" skipped: domain="${prop.domain}" (${sourceId ? 'found' : 'NOT FOUND'}), range="${prop.range}" (${targetId ? 'found' : 'NOT FOUND'})`);
                }
            }
        });
        if (skippedRels > 0) {
            console.warn(`[DESIGN] ${skippedRels}/${properties.length} relationship(s) skipped — entity names: [${Array.from(entityIdMap.keys()).join(', ')}]`);
        }
        console.log(`[DESIGN] Relationship summary: ${mergedLayout.relationships.length} included, ${skippedRels} skipped out of ${properties.length} total properties`);
        
        // Build inheritances from ontology parent-child relationships (authoritative source)
        // This ensures parent attributes from the ontology are always reflected
        mergedLayout.inheritances = [];
        const addedInheritancePairs = new Set(); // Track child->parent pairs to avoid duplicates
        
        classes.forEach((cls, idx) => {
            const parentName = cls.parent || cls.parentClass;
            if (parentName) {
                const childId = entityIdMap.get(cls.name);
                const parentId = entityIdMap.get(parentName);
                if (childId && parentId) {
                    const pairKey = `${childId}->${parentId}`;
                    if (!addedInheritancePairs.has(pairKey)) {
                        mergedLayout.inheritances.push({
                            id: `inh_merge_${Date.now()}_${idx}`,
                            sourceEntityId: parentId,
                            targetEntityId: childId,
                            direction: 'forward'
                        });
                        addedInheritancePairs.add(pairKey);
                    }
                } else if (!parentId) {
                    console.warn(`[DESIGN] Cannot create inheritance link: parent "${parentName}" not found for "${cls.name}"`);
                }
            }
        });
        
        // Also include any additional inheritances from saved layout that aren't in the ontology
        // (This handles cases where layout has inheritances that haven't been synced to ontology yet)
        if (savedLayout.inheritances && savedLayout.inheritances.length > 0) {
            // Map old entity IDs to new entity IDs based on entity names
            const savedEntityNameToNewId = new Map();
            savedLayout.entities.forEach(savedEntity => {
                const newId = entityIdMap.get(savedEntity.name);
                if (newId && savedEntity.id) {
                    savedEntityNameToNewId.set(savedEntity.id, newId);
                }
            });
            
            savedLayout.inheritances.forEach(inh => {
                const mappedSourceId = savedEntityNameToNewId.get(inh.sourceEntityId) || inh.sourceEntityId;
                const mappedTargetId = savedEntityNameToNewId.get(inh.targetEntityId) || inh.targetEntityId;
                
                // Only include if both entities still exist and this pair wasn't already added from ontology
                const sourceExists = Array.from(entityIdMap.values()).includes(mappedSourceId);
                const targetExists = Array.from(entityIdMap.values()).includes(mappedTargetId);
                const pairKey = `${mappedTargetId}->${mappedSourceId}`; // child->parent
                
                if (sourceExists && targetExists && !addedInheritancePairs.has(pairKey)) {
                    mergedLayout.inheritances.push({
                        id: inh.id,
                        sourceEntityId: mappedSourceId,
                        targetEntityId: mappedTargetId,
                        direction: inh.direction || 'forward'
                    });
                    addedInheritancePairs.add(pairKey);
                }
            });
        }
        
        // Include visibility state from saved layout
        if (savedLayout.visibility) {
            mergedLayout.visibility = savedLayout.visibility;
            const hidRels = savedLayout.visibility.hiddenRelationships || [];
            const hidEnts = savedLayout.visibility.hiddenEntities || [];
            console.log(`[DESIGN] Visibility: ${hidEnts.length} hidden entities, ${hidRels.length} hidden relationships`);
        }
        
        const inhCount = mergedLayout.inheritances ? mergedLayout.inheritances.length : 0;
        console.log('Loading merged layout:', mergedLayout.entities.length + ' entities, ' + mergedLayout.relationships.length + ' relationships, ' + inhCount + ' inheritances');
        ontologyDesigner.fromJSON(mergedLayout, { autoLayout: false, center: false, animate: false });
        
        // Re-enable auto-save after loading completes
        setTimeout(() => {
            isLoadingData = false;
            layoutDirty = false;
            ontologyVersionAtLoad = _getOntologyVersion();
            console.log('[LOAD] Data load complete - auto-save re-enabled');
        }, 600);  // Wait longer than auto-save debounce (500ms)
        return true;
    }
    
    // Fallback: load just from saved layout if no ontology data
    // But still try to get icons from OntologyState if available
    if (savedLayout && savedLayout.entities && savedLayout.entities.length > 0) {
        // Try to enrich with icons from OntologyState
        if (typeof OntologyState !== 'undefined' && OntologyState.config?.classes) {
            const classMap = new Map();
            OntologyState.config.classes.forEach(cls => {
                classMap.set(cls.name || cls.localName, cls);
            });
            
            savedLayout.entities.forEach(entity => {
                const cls = classMap.get(entity.name);
                if (cls) {
                    entity.icon = cls.emoji || cls.icon || entity.icon || '📦';
                    entity.description = cls.description || entity.description || '';
                }
            });
        }
        
        const inhCount = savedLayout.inheritances ? savedLayout.inheritances.length : 0;
        console.log('Loading from saved design layout:', savedLayout.entities.length + ' entities, ' + inhCount + ' inheritances');
        ontologyDesigner.fromJSON(savedLayout, { autoLayout: false, center: false, animate: false });
        
        // Re-enable auto-save after loading completes
        setTimeout(() => {
            isLoadingData = false;
            layoutDirty = false;
            ontologyVersionAtLoad = _getOntologyVersion();
            console.log('[LOAD] Data load complete - auto-save re-enabled');
        }, 600);
        return true;
    }
    
    // Get current ontology data from OntologyState.config (correct path)
    if (typeof OntologyState !== 'undefined' && OntologyState.config) {
        const classes = OntologyState.config.classes || [];
        const properties = OntologyState.config.properties || [];
        
        console.log('Loading ontology into designer:', { classes: classes.length, properties: properties.length });
        
        if (classes.length === 0) {
            if (showAlert) {
                showNotification('No classes defined in the ontology. Please add classes in the Entities tab first.', 'info');
            }
            console.log('No classes to load into designer');
            isLoadingData = false;
            return false;
        }
        
        ontologyDesigner.clear();
        
        // Create entity map for relationship linking
        const entityMap = new Map();
        
        // Calculate canvas center for initial positioning
        const canvas = document.getElementById('ontology-designer-canvas');
        const canvasWidth = canvas ? canvas.clientWidth : 800;
        const canvasHeight = canvas ? canvas.clientHeight : 600;
        
        // Calculate grid layout dimensions
        const cols = Math.min(4, classes.length);
        const rows = Math.ceil(classes.length / cols);
        const entityWidth = 200;
        const entityHeight = 150;
        const gapX = 100;
        const gapY = 80;
        const gridWidth = cols * entityWidth + (cols - 1) * gapX;
        const gridHeight = rows * entityHeight + (rows - 1) * gapY;
        
        // Starting position to center the grid
        const startX = (canvasWidth - gridWidth) / 2 + entityWidth / 2;
        const startY = (canvasHeight - gridHeight) / 2 + entityHeight / 2;
        
        // Add classes as entities - positioned in centered grid
        classes.forEach((cls, index) => {
            // Get data properties for this class (simplified - names only)
            const dataProps = (cls.dataProperties || []).map(dp => ({
                name: dp.name || dp.localName || 'attribute'
            }));
            
            // Calculate grid position (centered)
            const col = index % cols;
            const row = Math.floor(index / cols);
            const x = startX + col * (entityWidth + gapX);
            const y = startY + row * (entityHeight + gapY);
            
            const entity = ontologyDesigner.addEntity({
                name: cls.name || cls.localName || `Class_${index}`,
                icon: cls.emoji || cls.icon || '📦',
                description: cls.description || '',
                x: x,
                y: y,
                properties: dataProps
            });
            
            // Map by multiple keys for flexible lookup
            if (cls.uri) entityMap.set(cls.uri, entity);
            if (cls.name) entityMap.set(cls.name, entity);
            if (cls.localName) entityMap.set(cls.localName, entity);
        });
        
        // Add object properties as relationships
        let relCount = 0;
        properties.forEach(prop => {
            // Check if it's an object property (has domain and range that are classes)
            const isObjectProperty = prop.type === 'ObjectProperty' || 
                                    (prop.domain && prop.range && 
                                     entityMap.has(prop.domain) && entityMap.has(prop.range));
            
            if (isObjectProperty) {
                const sourceEntity = entityMap.get(prop.domain);
                const targetEntity = entityMap.get(prop.range);
                
                if (sourceEntity && targetEntity) {
                    ontologyDesigner.addRelationship({
                        sourceEntityId: sourceEntity.id,
                        targetEntityId: targetEntity.id,
                        name: prop.name || prop.localName || 'relates_to',
                        direction: prop.direction || 'forward',
                        properties: prop.properties || []
                    });
                    relCount++;
                }
            }
        });
        
        // Add inheritance links from class parent relationships
        let inhCount = 0;
        classes.forEach(cls => {
            if (cls.parent) {
                const childEntity = entityMap.get(cls.name) || entityMap.get(cls.localName);
                const parentEntity = entityMap.get(cls.parent);
                
                if (childEntity && parentEntity) {
                    ontologyDesigner.addInheritance({
                        sourceEntityId: parentEntity.id,
                        targetEntityId: childEntity.id,
                        direction: 'forward'
                    });
                    inhCount++;
                } else if (!parentEntity) {
                    console.warn(`[DESIGN] Cannot create inheritance link: parent "${cls.parent}" not found for "${cls.name}"`);
                }
            }
        });
        
        // Only center for first-time-ever loads with no saved layout.
        // All other paths (merge, saved-only) preserve exact saved positions.
        setTimeout(() => {
            ontologyDesigner.centerDiagram({ animate: false });
            ontologyDesigner._updateRelationshipPaths();
        }, 100);
        
        console.log('Loaded ' + classes.length + ' classes, ' + relCount + ' relationships, and ' + inhCount + ' inheritances into designer');
        
        if (showAlert) {
            showNotification('Imported ' + classes.length + ' classes, ' + relCount + ' relationships, and ' + inhCount + ' inheritances from ontology', 'success');
        }
        
        // Re-enable auto-save after loading and layout resolution completes
        setTimeout(() => {
            isLoadingData = false;
            layoutDirty = false;
            ontologyVersionAtLoad = _getOntologyVersion();
            console.log('[LOAD] Data load complete - auto-save re-enabled');
        }, 600);
        return true;
    } else {
        if (showAlert) {
            showNotification('No ontology loaded. Please load an ontology first from the Information tab.', 'warning');
        }
        console.log('OntologyState not available');
        isLoadingData = false;
        return false;
    }
}

/**
 * Create a Knowledge Graph group from the current business view's entities.
 */
function createGroupFromView() {
    if (window.isActiveVersion === false) return;
    if (!ontologyDesigner) {
        showNotification('No view is loaded.', 'warning');
        return;
    }

    const design = ontologyDesigner.toJSON();
    const entities = design.entities || [];
    if (entities.length === 0) {
        showNotification('This view has no entities to group.', 'warning');
        return;
    }

    const memberNames = entities.map(e => e.name).filter(Boolean);
    const viewName = currentDesignView || 'default';
    const defaultGroupName = viewName.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '');

    const existingModal = document.getElementById('createGroupFromViewModal');
    if (existingModal) existingModal.remove();

    const entityListHtml = memberNames.map(
        n => '<span class="badge bg-secondary me-1 mb-1">' + n + '</span>'
    ).join('');

    const modalHtml = `
        <div class="modal fade" id="createGroupFromViewModal" tabindex="-1">
            <div class="modal-dialog modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-collection me-2"></i>Create Group from View</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="mb-3">
                            <label class="form-label">Group Name</label>
                            <input type="text" class="form-control" id="groupFromViewName"
                                   value="${defaultGroupName}" placeholder="URI-safe identifier">
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Label</label>
                            <input type="text" class="form-control" id="groupFromViewLabel"
                                   value="${viewName}" placeholder="Display label">
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Color</label>
                            <input type="color" class="form-control form-control-color" id="groupFromViewColor" value="#4A90D9">
                        </div>
                        <div>
                            <label class="form-label">Members (${memberNames.length} entities from this view)</label>
                            <div class="border rounded p-2" style="max-height:180px;overflow-y:auto;">
                                ${entityListHtml}
                            </div>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="button" class="btn btn-warning" id="confirmCreateGroupFromView">
                            <i class="bi bi-collection me-1"></i>Create Group
                        </button>
                    </div>
                </div>
            </div>
        </div>`;

    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('createGroupFromViewModal'));

    document.getElementById('confirmCreateGroupFromView').addEventListener('click', async () => {
        const name = document.getElementById('groupFromViewName').value.trim();
        const label = document.getElementById('groupFromViewLabel').value.trim();
        const color = document.getElementById('groupFromViewColor').value;

        if (!name) {
            showNotification('Group name is required.', 'warning');
            return;
        }

        try {
            const response = await fetch('/ontology/groups/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    group: { name, label: label || name, description: 'Created from business view: ' + viewName, color, icon: '', members: memberNames },
                    index: -1
                }),
                credentials: 'same-origin'
            });
            const data = await response.json();

            if (data.success) {
                modal.hide();
                showNotification('Group "' + name + '" created with ' + memberNames.length + ' members.', 'success', 4000);
            } else {
                showNotification(data.message || 'Failed to create group.', 'error');
            }
        } catch (err) {
            console.error('[Design] create group error:', err);
            showNotification('Error creating group: ' + err.message, 'error');
        }
    });

    modal.show();
    document.getElementById('groupFromViewName').select();
}
