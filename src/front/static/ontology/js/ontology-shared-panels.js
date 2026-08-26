/**
 * OntoBricks - ontology-shared-panels.js
 * Extracted from ontology templates per code_instructions.txt
 */

// =====================================================
// MAPPING LINK HELPER
// =====================================================

let _cachedMappingConfig = null;

async function _fetchMappingConfig() {
    if (_cachedMappingConfig) return _cachedMappingConfig;
    try {
        const resp = await fetch('/mapping/load', { credentials: 'same-origin' });
        const data = await resp.json();
        if (data.success && data.config) {
            _cachedMappingConfig = data.config;
            return _cachedMappingConfig;
        }
    } catch (e) { /* ignore */ }
    return null;
}

function _renderAssignmentLink(containerId, type, name) {
    const container = document.getElementById(containerId) ||
        (sharedPanelCurrentSection && sharedPanelCurrentSection.querySelector('#' + containerId));
    if (!container) return;
    container.innerHTML = '';

    _fetchMappingConfig().then(config => {
        if (!config) return;
        const endsWith = (uri, n) => uri && (uri === n || uri.endsWith('#' + n) || uri.endsWith('/' + n));
        let hasAssignment = false;
        if (type === 'entity') {
            const classUri = OntologyState.config.classes.find(c => c.name === name)?.uri || '';
            hasAssignment = (config.entities || []).some(
                m => m.ontology_class === classUri || endsWith(m.ontology_class, name)
            );
        } else {
            const propUri = OntologyState.config.properties.find(p => p.name === name)?.uri || '';
            hasAssignment = (config.relationships || []).some(
                m => m.property === propUri || endsWith(m.property, name)
            );
        }
        if (hasAssignment) {
            const params = new URLSearchParams({ section: 'design', select: name, type: type });
            container.innerHTML = `
                <div class="mb-2 small">
                    <a href="/mapping/?${params.toString()}" title="View mapping for ${escapeHtml(name)}">
                        <i class="bi bi-link-45deg"></i> View Mapping
                    </a>
                </div>`;
        }
    });
}

// =====================================================
// SHARED SPLIT PANEL - Entity & Relationship Editing
// =====================================================

// Emoji categories sourced from global EmojiPicker module (emoji-picker.js)

// Current editing state
let sharedPanelEditType = null;
let sharedPanelEditIndex = -1;
let sharedPanelOriginalName = null;
let sharedPanelOwnAttributes = [];
let sharedPanelInheritedAttributes = [];
let sharedPanelViewOnly = false;
let sharedPanelOnSaveCallback = null;
let sharedPanelCurrentSection = null;
let sharedPanelElement = null;  // Reference to the current panel DOM element for scoped queries
let sharedPanelDashboardUrl = null;  // Dashboard URL for the entity
let sharedPanelDashboardParams = {};  // Dashboard parameter mappings { paramName: attributeName }
let sharedPanelBridges = [];  // Cross-domain entity bridges
let sharedPanelDataset = null;  // Linked Unity Catalog dataset { catalog, schema, asset, type, fullName, key_column, description }
// Linked Unity Catalog function actions. Each entry is
// { catalog, schema, function, fullName, description, returns_table }.
// The bound function must take exactly one parameter: the ID of the entity
// being acted on — it is passed automatically at invocation time.
let sharedPanelActions = [];
// Virtual attribute declarations. One entry per bound Unity Catalog function:
// { catalog, schema, function, fullName, description, returns_table,
//   attributes: [{ name, column, label, dataType }] }.
// Values are never stored here — they are computed on demand by the consumers
// (Graph Explorer, MCP), never at design time.
let sharedPanelVirtualAttributes = [];
let sharedPanelDirty = false;

// Remembered active tab per panel type — persists across entity/relationship selections
let _entityPanelActiveTab = 'details';
let _relPanelActiveTab = 'details';

// Panel resize state
let isResizing = false;
let panelStartWidth = 420;

/**
 * Get or create the detail panel in the current active section
 */
function getOrCreateDetailPanel() {
    // Find the active section that supports detail panel
    let activeSection = document.querySelector('#map-section.active, #entities-section.active, #relationships-section.active');
    
    // Fallback: check for visible sections by computed style
    if (!activeSection) {
        const sections = ['map-section', 'entities-section', 'relationships-section'];
        for (const id of sections) {
            const section = document.getElementById(id);
            if (section) {
                const style = window.getComputedStyle(section);
                if (style.display !== 'none') {
                    activeSection = section;
                    break;
                }
            }
        }
    }
    
    if (!activeSection) {
        console.log('[SharedPanel] No active section found');
        return null;
    }
    
    console.log('[SharedPanel] Active section:', activeSection.id);
    sharedPanelCurrentSection = activeSection;
    
    // Use the specific container for each section
    let panelContainer = activeSection;
    // The panel is a sibling of the content pane, not a child of it, so the
    // Designer hosts it on the wrapper rather than on the canvas itself.
    const containerMap = {
        'map-section': 'ontology-map-wrapper',
        'entities-section': 'ontology-entities-container',
        'relationships-section': 'ontology-relationships-container'
    };
    
    const containerId = containerMap[activeSection.id];
    if (containerId) {
        const container = document.getElementById(containerId);
        if (container) {
            panelContainer = container;
            console.log('[SharedPanel] Using', containerId, 'as panel container');
        }
    }
    
    // Check if panel already exists in this container
    let panel = panelContainer.querySelector('.shared-detail-panel');
    if (panel) {
        console.log('[SharedPanel] Panel already exists');
        return panel;
    }
    
    return createDetailPanel(panelContainer);
}

/**
 * Build the handle + panel pair inside a container and wire it up.
 * Returns the existing panel if the container already has one.
 */
function createDetailPanel(panelContainer) {
    const existing = panelContainer.querySelector('.shared-detail-panel');
    if (existing) return existing;

    console.log('[SharedPanel] Creating new panel');

    // Create resize handle
    const resizeHandle = document.createElement('div');
    resizeHandle.className = 'detail-panel-resize-handle';
    resizeHandle.title = 'Drag to resize panels';
    resizeHandle.innerHTML = '<div class="resize-handle-bar"></div>';

    // Create panel
    const panelDiv = document.createElement('div');
    panelDiv.className = 'shared-detail-panel';

    panelDiv.innerHTML = `
        <div class="panel-header">
            <h6 id="sharedPanelTitle"><i class="bi bi-box"></i> <span id="sharedPanelItemName">Select Item</span></h6>
        </div>
        <div class="panel-body" id="sharedPanelBody"></div>
    `;

    // Append to container (either section or ontology-map-wrapper for Map)
    panelContainer.appendChild(resizeHandle);
    panelContainer.appendChild(panelDiv);

    // Add the class to enable split layout
    panelContainer.classList.add('has-detail-panel');

    setupResizeHandle(panelContainer);

    renderPanelPlaceholder(panelDiv);

    return panelDiv;
}

/**
 * Create the panel in every section that hosts one, before the views render.
 * D3 sizes the Designer canvas from `container.clientWidth` exactly once, so a
 * panel appearing later would leave the graph centred on the wrong axis.
 */
function ensureDetailPanels() {
    ['ontology-map-wrapper',
     'ontology-entities-container',
     'ontology-relationships-container'].forEach(id => {
        const container = document.getElementById(id);
        if (container) createDetailPanel(container);
    });
}

/**
 * Reset a panel to its "nothing selected" state.
 */
function renderPanelPlaceholder(panel) {
    const target = panel || sharedPanelElement;
    if (!target) return;

    target.classList.add('is-empty');

    const title = target.querySelector('#sharedPanelItemName');
    if (title) title.textContent = 'Select Item';

    const body = target.querySelector('#sharedPanelBody');
    if (body) {
        body.innerHTML = `
            <div class="panel-placeholder">
                <i class="bi bi-cursor"></i>
                <p class="small mb-0">Click on an entity or<br>relationship to view details</p>
            </div>
        `;
    }
}

/**
 * Switch between form tabs inside the panel body.
 * Persists the selection so the same tab is restored when another
 * entity or relationship is opened.
 */
function switchFormTab(tabLink) {
    const form = tabLink.closest('form') || tabLink.closest('.panel-body');
    if (!form) return;
    const tabName = tabLink.dataset.formTab;

    form.querySelectorAll('.form-tabs-nav .form-tab-link').forEach(link => {
        link.classList.toggle('active', link.dataset.formTab === tabName);
    });
    form.querySelectorAll('.form-tab-pane').forEach(pane => {
        pane.classList.toggle('active', pane.dataset.formTabContent === tabName);
    });

    if (form.id === 'sharedEntityForm') {
        _entityPanelActiveTab = tabName;
    } else if (form.id === 'sharedRelationshipForm') {
        _relPanelActiveTab = tabName;
    }
}

/**
 * Mark the panel as dirty. Called by mutation helpers and DOM events.
 */
function markPanelDirty() {
    sharedPanelDirty = true;
}

/**
 * Attach dirty tracking to the panel body.
 * Listens for input/change (typing, selects) and click (button-driven mutations).
 * Call this after every renderEntityForm / renderRelationshipForm.
 */
function attachDirtyTracking() {
    sharedPanelDirty = false;
    const body = sharedPanelElement?.querySelector('#sharedPanelBody');
    if (!body) return;
    body.addEventListener('input', markPanelDirty);
    body.addEventListener('change', markPanelDirty);
    body.addEventListener('click', (e) => {
        if (e.target.closest('button')) markPanelDirty();
    });
}

/**
 * Prompt the user if there are unsaved changes, then close.
 */
async function guardedCloseSharedPanel() {
    if (sharedPanelDirty) {
        await saveSharedPanelItem();
    }
    closeSharedPanel();
}

/**
 * Setup resize handle for the panel
 */
function setupResizeHandle(section) {
    const handle = section.querySelector('.detail-panel-resize-handle');
    const panel = section.querySelector('.shared-detail-panel');
    if (!handle || !panel) return;
    
    handle.addEventListener('mousedown', (e) => {
        isResizing = true;
        panelStartWidth = panel.offsetWidth;
        handle.classList.add('active');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
        e.preventDefault();
    });
    
    document.addEventListener('mousemove', (e) => {
        if (!isResizing) return;
        
        const sectionRect = section.getBoundingClientRect();
        const newWidth = sectionRect.right - e.clientX;

        const clampedWidth = Math.max(200, Math.min(900, newWidth));
        panel.style.width = clampedWidth + 'px';
    });
    
    document.addEventListener('mouseup', () => {
        if (isResizing) {
            isResizing = false;
            const handle = document.querySelector('.detail-panel-resize-handle.active');
            if (handle) handle.classList.remove('active');
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        }
    });
}

/**
 * Get an element by ID scoped to the current panel.
 * Falls back to document.getElementById if no panel reference exists.
 * This prevents issues when multiple panels exist in the DOM (e.g., Map + Entities).
 */
function panelGetById(id) {
    if (sharedPanelElement) {
        return sharedPanelElement.querySelector('#' + id);
    }
    return document.getElementById(id);
}

/**
 * If the panel is open and dirty, prompt the user.
 * Returns true if it's safe to proceed (saved or discarded), false to abort.
 */
async function checkDirtyBeforeSwitch() {
    if (!sharedPanelDirty) return true;
    await saveSharedPanelItem();
    return true;
}

/**
 * Open the shared panel
 */
function openSharedPanel() {
    console.log('[SharedPanel] openSharedPanel called');
    const panel = getOrCreateDetailPanel();
    if (!panel) {
        console.log('[SharedPanel] getOrCreateDetailPanel returned null');
        return;
    }
    
    // Store reference to the current panel element for scoped queries
    sharedPanelElement = panel;

    panel.classList.remove('is-empty');

    // Callers set the header synchronously but render the body in an async
    // step. Drop the placeholder now so it can't sit under a populated header.
    panel.querySelector('.panel-placeholder')?.remove();
}

/**
 * Close the shared panel
 */
function closeSharedPanel() {
    // The panel stays in the layout, so "close" means drop the selection and
    // fall back to the placeholder.
    document.querySelectorAll('.shared-detail-panel').forEach(panel => {
        renderPanelPlaceholder(panel);
    });

    // Reset state
    sharedPanelEditType = null;
    sharedPanelEditIndex = -1;
    sharedPanelOriginalName = null;
    sharedPanelOwnAttributes = [];
    sharedPanelInheritedAttributes = [];
    sharedPanelViewOnly = false;
    sharedPanelOnSaveCallback = null;
    sharedPanelElement = null;
    sharedPanelDashboardUrl = null;
    sharedPanelDashboardParams = {};
    sharedPanelBridges = [];
    sharedPanelDataset = null;
    sharedPanelActions = [];
    sharedPanelDirty = false;
}

/**
 * Get inherited properties from parent entity (recursive)
 */
function getSharedInheritedProperties(parentName, visited = new Set()) {
    if (!parentName || visited.has(parentName)) return [];
    visited.add(parentName);
    
    const parentEntity = OntologyState.config.classes.find(c => c.name === parentName);
    if (!parentEntity) return [];
    
    const parentProps = (parentEntity.dataProperties || []).map(p => ({
        name: p.name || p.localName || p,
        inheritedFrom: parentName
    }));
    
    const grandparentProps = getSharedInheritedProperties(parentEntity.parent, visited);
    
    return [...grandparentProps, ...parentProps];
}

// =====================================================
// ENTITY PANEL FUNCTIONS
// =====================================================

// True when the caller may mutate the loaded ontology — falls through
// to ``window.OB.canEditOntology`` so the rule (active version + domain
// role >= editor) lives in one place. Older bundles without
// ``window.OB`` keep the legacy "active version" behaviour.
function _canEditOntologyPanel() {
    if (window.OB && typeof window.OB.canEditOntology === 'function') {
        return window.OB.canEditOntology();
    }
    return window.isActiveVersion !== false;
}

async function openEntityPanel(options = {}) {
    if (!_canEditOntologyPanel()) {
        console.log('[SharedPanel] openEntityPanel suppressed (read-only / viewer)');
        return;
    }
    await checkDirtyBeforeSwitch();
    console.log('[SharedPanel] openEntityPanel called');
    sharedPanelEditType = 'entity';
    sharedPanelEditIndex = -1;
    sharedPanelOriginalName = null;
    sharedPanelOwnAttributes = [];
    sharedPanelInheritedAttributes = [];
    sharedPanelViewOnly = false;
    sharedPanelOnSaveCallback = options.onSave || null;
    sharedPanelDashboardUrl = null;  // Reset dashboard for new entity
    sharedPanelDashboardParams = {};  // Reset dashboard parameter mappings
    sharedPanelBridges = [];  // Reset bridges for new entity
    sharedPanelDataset = null;  // Reset dataset for new entity
    sharedPanelActions = [];  // Reset UC function actions for new entity
    sharedPanelVirtualAttributes = [];  // Reset virtual attributes for new entity
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) {
        console.log('[SharedPanel] Panel not found after openSharedPanel');
        return;
    }
    
    panel.querySelector('#sharedPanelTitle').innerHTML = '<i class="bi bi-plus-circle"></i> <span id="sharedPanelItemName">Add Entity</span>';
    await renderEntityForm(panel, null);
    attachDirtyTracking();
}

async function openEntityPanelForEdit(idx, options = {}) {
    // Viewers / older-version readers fall through to the view-only
    // panel so the icon button, attribute add/remove, dashboard assign
    // and bridge add controls (rendered behind the ``!viewOnly`` guard
    // in renderEntityForm / renderSharedEntityAttributes / …) are not
    // even emitted into the DOM. Saves us a per-button CSS sweep and
    // keeps a single source of truth.
    if (!_canEditOntologyPanel()) {
        return openEntityPanelForView(idx, options);
    }
    await checkDirtyBeforeSwitch();
    console.log('[SharedPanel] openEntityPanelForEdit called, idx:', idx);
    const cls = OntologyState.config.classes[idx];
    if (!cls) {
        console.log('[SharedPanel] Class not found at index:', idx);
        return;
    }
    
    sharedPanelEditType = 'entity';
    sharedPanelEditIndex = idx;
    sharedPanelOriginalName = cls.name;
    sharedPanelViewOnly = false;
    sharedPanelOnSaveCallback = options.onSave || null;
    sharedPanelDashboardUrl = cls.dashboard || null;  // Load existing dashboard URL
    sharedPanelDashboardParams = cls.dashboardParams || {};  // Load existing parameter mappings
    sharedPanelBridges = cls.bridges ? JSON.parse(JSON.stringify(cls.bridges)) : [];
    sharedPanelDataset = cls.dataset ? JSON.parse(JSON.stringify(cls.dataset)) : null;
    sharedPanelActions = cls.actions ? JSON.parse(JSON.stringify(cls.actions)) : [];
    sharedPanelVirtualAttributes = cls.virtualAttributes ? JSON.parse(JSON.stringify(cls.virtualAttributes)) : [];

    console.log('[SharedPanel] Edit - Loaded class:', cls.name, 'dataProperties:', (cls.dataProperties || []).length);
    
    sharedPanelInheritedAttributes = getSharedInheritedProperties(cls.parent);
    const inheritedNames = new Set(sharedPanelInheritedAttributes.map(a => a.name));
    sharedPanelOwnAttributes = (cls.dataProperties || [])
        .map(p => ({ name: p.name || p.localName || p }))
        .filter(a => !inheritedNames.has(a.name));
    
    // Jump straight to a specific tab (e.g. Designer context-menu shortcuts)
    if (options.activeTab) _entityPanelActiveTab = options.activeTab;
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) {
        console.log('[SharedPanel] Panel not found after openSharedPanel');
        return;
    }
    
    const emoji = cls.emoji || OntologyState.defaultClassEmoji || '📦';
    panel.querySelector('#sharedPanelTitle').innerHTML = `<i class="bi bi-pencil"></i> ${emoji} <span id="sharedPanelItemName">${cls.name}</span>`;
    await renderEntityForm(panel, cls);
    attachDirtyTracking();
}

async function openEntityPanelForView(idx, options = {}) {
    await checkDirtyBeforeSwitch();
    const cls = OntologyState.config.classes[idx];
    if (!cls) return;
    
    sharedPanelEditType = 'entity';
    sharedPanelEditIndex = idx;
    sharedPanelViewOnly = true;
    sharedPanelOnSaveCallback = null;
    sharedPanelDashboardUrl = cls.dashboard || null;  // Load existing dashboard URL
    sharedPanelDashboardParams = cls.dashboardParams || {};  // Load existing parameter mappings
    sharedPanelBridges = cls.bridges ? JSON.parse(JSON.stringify(cls.bridges)) : [];
    sharedPanelDataset = cls.dataset ? JSON.parse(JSON.stringify(cls.dataset)) : null;
    sharedPanelActions = cls.actions ? JSON.parse(JSON.stringify(cls.actions)) : [];
    sharedPanelVirtualAttributes = cls.virtualAttributes ? JSON.parse(JSON.stringify(cls.virtualAttributes)) : [];

    sharedPanelInheritedAttributes = getSharedInheritedProperties(cls.parent);
    const inheritedNames = new Set(sharedPanelInheritedAttributes.map(a => a.name));
    sharedPanelOwnAttributes = (cls.dataProperties || [])
        .map(p => ({ name: p.name || p.localName || p }))
        .filter(a => !inheritedNames.has(a.name));
    
    // Jump straight to a specific tab (e.g. Designer context-menu shortcuts)
    if (options.activeTab) _entityPanelActiveTab = options.activeTab;
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) return;
    
    const emoji = cls.emoji || OntologyState.defaultClassEmoji || '📦';
    panel.querySelector('#sharedPanelTitle').innerHTML = `<i class="bi bi-eye"></i> ${emoji} <span id="sharedPanelItemName">${cls.name}</span>`;
    await renderEntityForm(panel, cls, true);
}

async function renderEntityForm(panel, cls, viewOnly = false) {
    const body = panel.querySelector('#sharedPanelBody');
    
    const parentOptions = OntologyState.config.classes
        .filter(c => !cls || c.name !== cls.name)
        .map(c => `<option value="${c.name}" ${cls && cls.parent === c.name ? 'selected' : ''}>${c.emoji || '📦'} ${c.name}</option>`)
        .join('');
    
    // Options for disjoint/equivalent (exclude current class)
    const otherClassOptions = OntologyState.config.classes
        .filter(c => !cls || c.name !== cls.name)
        .map(c => `<option value="${c.name}">${c.emoji || '📦'} ${c.name}</option>`)
        .join('');
    
    const emoji = cls?.emoji || OntologyState.defaultClassEmoji || '📦';
    const disabled = viewOnly ? 'disabled' : '';
    
    // Load constraints from server (the ONLY source of truth for constraints)
    let disjointWith = [];
    let equivalentTo = [];
    
    if (cls?.name) {
        try {
            const response = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
            const data = await response.json();
            if (data.success && data.constraints) {
                const classConstraints = data.constraints.filter(c => c.className === cls.name);
                classConstraints.forEach(c => {
                    if (c.type === 'disjointWith') disjointWith = c.disjointClasses || [];
                    if (c.type === 'equivalentTo') equivalentTo = c.equivalentClasses || [];
                });
            }
        } catch (error) {
            console.error('[SharedPanel] Error loading entity constraints:', error);
        }
    }

    let groupOptions = '<option value="">-- None --</option>';
    let currentGroup = '';
    try {
        const gRes = await fetch('/ontology/groups/list', { credentials: 'same-origin' });
        const gData = await gRes.json();
        if (gData.success && gData.groups) {
            gData.groups.forEach(g => {
                const isMember = (g.members || []).includes(cls?.name);
                if (isMember) currentGroup = g.name;
                const sel = isMember ? 'selected' : '';
                const icon = g.icon || '';
                groupOptions += `<option value="${g.name}" ${sel}>${icon} ${g.label || g.name}</option>`;
            });
        }
    } catch (e) {
        console.warn('[SharedPanel] Could not load groups:', e);
    }
    
    const _eTab = _entityPanelActiveTab || 'details';
    body.innerHTML = `
        <div id="sharedEntityAssignmentLink"></div>
        <form id="sharedEntityForm">
            <ul class="form-tabs-nav">
                <li><a class="form-tab-link ${_eTab === 'details' ? 'active' : ''}" data-form-tab="details" href="#" onclick="event.preventDefault(); switchFormTab(this)"><i class="bi bi-info-circle me-1"></i>Details</a></li>
                <li><a class="form-tab-link ${_eTab === 'attributes' ? 'active' : ''}" data-form-tab="attributes" href="#" onclick="event.preventDefault(); switchFormTab(this)"><i class="bi bi-tags me-1"></i>Attributes</a></li>
                <li><a class="form-tab-link ${_eTab === 'actions' ? 'active' : ''}" data-form-tab="actions" href="#" onclick="event.preventDefault(); switchFormTab(this)"><i class="bi bi-lightning me-1"></i>References</a></li>
                <li><a class="form-tab-link ${_eTab === 'constraints' ? 'active' : ''}" data-form-tab="constraints" href="#" onclick="event.preventDefault(); switchFormTab(this)"><i class="bi bi-sliders me-1"></i>Constraints</a></li>
            </ul>

            <div class="form-tab-pane ${_eTab === 'details' ? 'active' : ''}" data-form-tab-content="details">
                <div class="mb-3 p-2 bg-light rounded border">
                    <label for="sharedEntityParent" class="form-label"><i class="bi bi-diagram-2"></i> Inherits From</label>
                    <select class="form-select form-select-sm" id="sharedEntityParent" ${disabled} onchange="onSharedEntityParentChange()">
                        <option value="">-- None --</option>
                        ${parentOptions}
                    </select>
                </div>
                <div class="mb-3 p-2 bg-light rounded border">
                    <label for="sharedEntityGroup" class="form-label"><i class="bi bi-collection"></i> Group</label>
                    <select class="form-select form-select-sm" id="sharedEntityGroup" ${disabled}
                        data-entity="${(cls?.name || '').replace(/"/g, '&quot;')}"
                        data-prev-group="${currentGroup.replace(/"/g, '&quot;')}"
                        onchange="onSharedEntityGroupChange(this.dataset.entity, this.value, this.dataset.prevGroup)">
                        ${groupOptions}
                    </select>
                </div>
                <div class="mb-3">
                    <label for="sharedEntityName" class="form-label">Name <span class="text-danger">*</span></label>
                    <input type="text" class="form-control form-control-sm" id="sharedEntityName" value="${cls?.name || ''}" ${disabled} required>
                </div>
                <div class="mb-3">
                    <label for="sharedEntityLabel" class="form-label">Label</label>
                    <input type="text" class="form-control form-control-sm" id="sharedEntityLabel" value="${cls?.label || cls?.name || ''}" ${disabled} placeholder="Defaults to name if empty">
                </div>
                <div class="mb-3">
                    <label class="form-label">Icon</label>
                    <div class="input-group input-group-sm">
                        <span class="input-group-text" id="sharedEntityEmojiPreview">${emoji}</span>
                        <input type="text" class="form-control" id="sharedEntityIcon" value="${emoji}" ${disabled} maxlength="2" style="width: 45px;">
                        <button type="button" class="btn btn-outline-secondary" id="sharedEntityEmojiBtn" ${disabled}><i class="bi bi-emoji-smile"></i></button>
                    </div>
                    <div id="sharedEntityEmojiPickerMount"></div>
                </div>
                <div class="mb-3">
                    <label for="sharedEntityDescription" class="form-label">Description</label>
                    <textarea class="form-control form-control-sm" id="sharedEntityDescription" rows="2" ${disabled}>${cls?.comment || cls?.description || ''}</textarea>
                </div>
            </div>

            <div class="form-tab-pane ${_eTab === 'attributes' ? 'active' : ''}" data-form-tab-content="attributes">
                <div class="d-flex justify-content-end gap-1 mb-2">
                    ${!viewOnly ? `
                        <button type="button" class="btn btn-sm btn-outline-secondary py-0 px-1" onclick="openMetadataAttributePicker()" title="Add from data sources"><i class="bi bi-database"></i></button>
                        <button type="button" class="btn btn-sm btn-outline-primary py-0 px-1" onclick="addSharedEntityAttribute()" title="Add manually"><i class="bi bi-plus"></i></button>
                    ` : ''}
                </div>
                <div id="sharedEntityAttributes" class="border rounded p-2 panel-box"></div>
                <div class="mt-3">
                    <label class="form-label d-flex justify-content-between align-items-center">
                        <span><i class="bi bi-magic me-1"></i>Virtual Attributes</span>
                        ${!viewOnly ? '<button type="button" class="btn btn-sm btn-outline-primary py-0 px-1" onclick="openVirtualAttributeSelectorModal()"><i class="bi bi-plus"></i> Add</button>' : ''}
                    </label>
                    <div id="sharedEntityVirtualAttributes" class="border rounded p-2 panel-box">
                        <div id="sharedEntityVirtualAttributesContent">
                            <small class="text-muted">No virtual attributes</small>
                        </div>
                    </div>
                    <div class="form-text small">Computed on demand by a Unity Catalog function taking exactly one parameter: the ID of the entity. Not mapped, not stored in the graph, and not queryable in SPARQL or GraphQL.</div>
                </div>
            </div>

            <div class="form-tab-pane ${_eTab === 'actions' ? 'active' : ''}" data-form-tab-content="actions">
                <div class="mb-3">
                    <label class="form-label d-flex justify-content-between align-items-center">
                        <span><i class="bi bi-speedometer2 me-1"></i>Dashboard</span>
                        ${!viewOnly ? '<button type="button" class="btn btn-sm btn-outline-primary py-0 px-1" onclick="openDashboardSelectorModal()"><i class="bi bi-link-45deg"></i> Assign</button>' : ''}
                    </label>
                    <div id="sharedEntityDashboard" class="border rounded p-2" style="background: #ffffff;">
                        <div id="sharedEntityDashboardContent">
                            <small class="text-muted">No dashboard assigned</small>
                        </div>
                    </div>
                </div>
                <div class="mb-3">
                    <label class="form-label d-flex justify-content-between align-items-center">
                        <span><i class="bi bi-table me-1"></i>Dataset</span>
                        ${!viewOnly ? '<button type="button" class="btn btn-sm btn-outline-primary py-0 px-1" onclick="openDatasetSelectorModal()"><i class="bi bi-search"></i> Select</button>' : ''}
                    </label>
                    <div id="sharedEntityDataset" class="border rounded p-2" style="background: #ffffff;">
                        <div id="sharedEntityDatasetContent">
                            <small class="text-muted">No dataset assigned</small>
                        </div>
                    </div>
                </div>
                <div class="mb-3">
                    <label class="form-label d-flex justify-content-between align-items-center">
                        <span><i class="bi bi-lightning-charge me-1"></i>Actions</span>
                        ${!viewOnly ? '<button type="button" class="btn btn-sm btn-outline-primary py-0 px-1" onclick="openActionSelectorModal()"><i class="bi bi-plus"></i> Add</button>' : ''}
                    </label>
                    <div id="sharedEntityActions" class="border rounded p-2" style="background: #ffffff;">
                        <div id="sharedEntityActionsContent">
                            <small class="text-muted">No actions assigned</small>
                        </div>
                    </div>
                    <div class="form-text small">Each action runs a Unity Catalog function that takes exactly one parameter: the ID of the entity.</div>
                </div>
                <div class="mb-3">
                    <label class="form-label d-flex justify-content-between align-items-center">
                        <span><i class="bi bi-signpost-2 me-1"></i>Bridges</span>
                        ${!viewOnly ? '<button type="button" class="btn btn-sm btn-outline-primary py-0 px-1" onclick="openBridgeSelectorModal()"><i class="bi bi-plus"></i> Add</button>' : ''}
                    </label>
                    <div id="sharedEntityBridges" class="border rounded p-2" style="background: #ffffff;">
                        <div id="sharedEntityBridgesContent">
                            <small class="text-muted">No bridges to other domains</small>
                        </div>
                    </div>
                </div>
            </div>

            <div class="form-tab-pane ${_eTab === 'constraints' ? 'active' : ''}" data-form-tab-content="constraints">
                <div class="mb-3">
                    <label class="form-label small text-muted mb-1" title="Entities that share no instances with this entity">
                        <i class="bi bi-x-circle me-1"></i>Disjoint With
                    </label>
                    <select class="form-select form-select-sm" id="sharedEntityDisjointWith" ${disabled} multiple size="3" 
                            title="Select entities that cannot share instances with this entity">
                        ${otherClassOptions}
                    </select>
                    <div class="form-text small">No instance can belong to both this entity and the selected entities</div>
                </div>
                <div class="mb-3">
                    <label class="form-label small text-muted mb-1" title="Entities that have exactly the same instances as this entity">
                        <i class="bi bi-arrows-angle-expand me-1"></i>Equivalent To
                    </label>
                    <select class="form-select form-select-sm" id="sharedEntityEquivalentTo" ${disabled} multiple size="3"
                            title="Select entities that are equivalent to this entity">
                        ${otherClassOptions}
                    </select>
                    <div class="form-text small">Entities that have exactly the same instances</div>
                </div>
            </div>
        </form>
    `;
    
    // Set multi-select values for constraints
    if (disjointWith.length > 0) {
        const disjointSelect = panelGetById('sharedEntityDisjointWith');
        if (disjointSelect) {
            Array.from(disjointSelect.options).forEach(opt => {
                opt.selected = disjointWith.includes(opt.value);
            });
        }
    }
    if (equivalentTo.length > 0) {
        const equivalentSelect = panelGetById('sharedEntityEquivalentTo');
        if (equivalentSelect) {
            Array.from(equivalentSelect.options).forEach(opt => {
                opt.selected = equivalentTo.includes(opt.value);
            });
        }
    }
    
    renderSharedEntityAttributes(viewOnly);
    renderSharedEntityVirtualAttributes(viewOnly);
    renderSharedEntityDashboard(viewOnly);
    renderSharedEntityDataset(viewOnly);
    renderSharedEntityActions(viewOnly);
    renderSharedEntityBridges(viewOnly);
    if (!viewOnly) {
        var _btnEl = panelGetById('sharedEntityEmojiBtn');
        if (_btnEl) {
            EmojiPicker.create({
                triggerEl:   _btnEl,
                previewEl:   panelGetById('sharedEntityEmojiPreview'),
                inputEl:     panelGetById('sharedEntityIcon'),
                containerEl: panelGetById('sharedEntityEmojiPickerMount')
            });
        }
    }

    if (cls?.name) {
        _renderAssignmentLink('sharedEntityAssignmentLink', 'entity', cls.name);
    }
}

function renderSharedEntityAttributes(viewOnly = false) {
    const container = panelGetById('sharedEntityAttributes');
    if (!container) return;
    
    let html = '';
    
    if (sharedPanelInheritedAttributes.length > 0) {
        html += '<div class="mb-1"><small class="text-muted fw-bold"><i class="bi bi-diagram-2"></i> Inherited</small></div>';
        html += sharedPanelInheritedAttributes.map(attr => `
            <div class="d-flex align-items-center gap-1 mb-1 opacity-75">
                <input type="text" class="form-control form-control-sm bg-light" value="${attr.name || ''}" disabled readonly style="font-size:0.75rem;">
                <span class="badge bg-secondary" style="font-size:0.6rem;"><i class="bi bi-lock"></i></span>
            </div>
        `).join('');
        if (sharedPanelOwnAttributes.length > 0) html += '<hr class="my-1">';
    }
    
    if (sharedPanelOwnAttributes.length > 0) {
        if (sharedPanelInheritedAttributes.length > 0) {
            html += '<div class="mb-1"><small class="text-muted fw-bold"><i class="bi bi-tag"></i> Own</small></div>';
        }
        html += sharedPanelOwnAttributes.map((attr, idx) => `
            <div class="d-flex align-items-center gap-1 mb-1">
                <input type="text" class="form-control form-control-sm" value="${attr.name || ''}" ${viewOnly ? 'disabled' : ''} onchange="updateSharedEntityAttribute(${idx}, this.value)" style="font-size:0.75rem;">
                ${!viewOnly ? `<button type="button" class="btn btn-sm btn-outline-danger py-0 px-1" onclick="removeSharedEntityAttribute(${idx})"><i class="bi bi-x"></i></button>` : ''}
            </div>
        `).join('');
    }
    
    if (sharedPanelInheritedAttributes.length === 0 && sharedPanelOwnAttributes.length === 0) {
        html = '<small class="text-muted">No attributes</small>';
    }
    
    container.innerHTML = html;
}

function addSharedEntityAttribute() {
    sharedPanelOwnAttributes.push({ name: '' });
    markPanelDirty();
    renderSharedEntityAttributes(false);
    setTimeout(() => {
        const inputs = document.querySelectorAll('#sharedEntityAttributes input:not([disabled])');
        if (inputs.length > 0) inputs[inputs.length - 1].focus();
    }, 50);
}

function updateSharedEntityAttribute(idx, value) {
    if (sharedPanelOwnAttributes[idx]) sharedPanelOwnAttributes[idx].name = value.trim();
    markPanelDirty();
}

function removeSharedEntityAttribute(idx) {
    sharedPanelOwnAttributes.splice(idx, 1);
    markPanelDirty();
    renderSharedEntityAttributes(false);
}

function onSharedEntityParentChange() {
    const parentName = panelGetById('sharedEntityParent')?.value;
    sharedPanelInheritedAttributes = getSharedInheritedProperties(parentName);
    renderSharedEntityAttributes(false);
}

function onSharedEntityGroupChange(entityName, newGroup, previousGroup) {
    if (!entityName) return;
    const requests = [];
    if (previousGroup && previousGroup !== newGroup) {
        requests.push(fetch('/ontology/groups/members', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: previousGroup, remove: [entityName] })
        }));
    }
    if (newGroup) {
        requests.push(fetch('/ontology/groups/members', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: newGroup, add: [entityName] })
        }));
    }
    Promise.all(requests)
        .then(responses => Promise.all(responses.map(r => r.json())))
        .then(results => {
            const failed = results.find(r => !r.success);
            if (failed) {
                showNotification(failed.message || 'Failed to update group membership', 'error');
            } else {
                const label = newGroup || 'none';
                showNotification(`${entityName} is now in group: ${label}`, 'success');
            }
            const sel = panelGetById('sharedEntityGroup');
            if (sel) sel.dataset.prevGroup = newGroup;
        })
        .catch(err => {
            console.error('[SharedPanel] Group update failed:', err);
            showNotification('Could not update group membership', 'error');
        });
}

// Emoji picker functions removed — now uses global EmojiPicker.create() (emoji-picker.js)

// =====================================================
// DASHBOARD FUNCTIONS
// =====================================================

/**
 * Render the dashboard section in the entity form
 */
function renderSharedEntityDashboard(viewOnly = false) {
    const container = panelGetById('sharedEntityDashboardContent');
    if (!container) return;
    
    if (sharedPanelDashboardUrl) {
        const dashboardName = extractDashboardNameFromUrl(sharedPanelDashboardUrl);
        
        // Build parameter mappings display
        const mappings = Object.entries(sharedPanelDashboardParams || {});
        let mappingsHtml = '';
        if (mappings.length > 0) {
            mappingsHtml = `
                <div class="mt-2 pt-2 border-top">
                    <small class="text-muted d-block mb-1"><i class="bi bi-link-45deg me-1"></i>Parameter Mappings:</small>
                    ${mappings.map(([param, mapping]) => {
                        // Handle both old format (string) and new format (object)
                        const attrName = typeof mapping === 'object' ? mapping.attribute : mapping;
                        const displayName = attrName === '__ID__' ? 'Entity ID' : attrName;
                        return `
                            <div class="d-flex align-items-center gap-1 small">
                                <span class="badge bg-secondary">${escapeHtml(param)}</span>
                                <i class="bi bi-arrow-right text-muted"></i>
                                <span class="badge bg-primary">${escapeHtml(displayName)}</span>
                            </div>
                        `;
                    }).join('')}
                </div>
            `;
        }
        
        container.innerHTML = `
            <div class="d-flex align-items-center gap-2">
                <a href="${escapeHtml(sharedPanelDashboardUrl)}" target="_blank" class="text-decoration-none flex-grow-1" title="Open dashboard">
                    <i class="bi bi-speedometer2 text-primary"></i>
                    <span class="ms-1">${escapeHtml(dashboardName)}</span>
                    <i class="bi bi-box-arrow-up-right ms-1 small"></i>
                </a>
                ${!viewOnly ? `<button type="button" class="btn btn-sm btn-outline-danger py-0 px-1" onclick="removeSharedEntityDashboard()" title="Remove dashboard"><i class="bi bi-x"></i></button>` : ''}
            </div>
            ${mappingsHtml}
        `;
    } else {
        container.innerHTML = '<small class="text-muted">No dashboard assigned</small>';
    }
}

/**
 * Extract a readable name from the dashboard URL
 */
function extractDashboardNameFromUrl(url) {
    if (!url) return 'Unknown Dashboard';
    try {
        // Try to extract dashboard ID from the URL
        const urlObj = new URL(url);
        const pathParts = urlObj.pathname.split('/');
        const dashId = pathParts[pathParts.length - 1] || pathParts[pathParts.length - 2];
        return dashId ? `Dashboard ${dashId.substring(0, 8)}...` : 'Dashboard';
    } catch {
        return 'Dashboard';
    }
}

/**
 * Remove the assigned dashboard
 */
function removeSharedEntityDashboard() {
    sharedPanelDashboardUrl = null;
    sharedPanelDashboardParams = {};
    markPanelDirty();
    renderSharedEntityDashboard(false);
}

// =====================================================
// UNITY CATALOG DATASET
// =====================================================

/**
 * Render the linked Unity Catalog dataset in the entity form
 */
function renderSharedEntityDataset(viewOnly = false) {
    const container = panelGetById('sharedEntityDatasetContent');
    if (!container) return;

    const ds = sharedPanelDataset;
    if (ds && ds.asset) {
        const isView = (ds.type || '').toUpperCase() === 'VIEW';
        const badge = isView
            ? '<span class="badge bg-info text-dark">View</span>'
            : '<span class="badge bg-secondary">Table</span>';
        const fullName = ds.fullName || `${ds.catalog}.${ds.schema}.${ds.asset}`;
        const keyColHtml = !viewOnly
            ? `<div class="mt-2">
                 <label class="form-label form-label-sm mb-1" style="font-size:0.8rem;">
                   Key column <small class="text-muted">(used to match node ID)</small>
                 </label>
                 <div class="d-flex gap-2">
                   <select class="form-select form-select-sm"
                           id="datasetKeyColumnSelect"
                           onchange="onDatasetKeyColumnChange(this.value)"
                           disabled>
                     <option value="">Loading columns…</option>
                   </select>
                   <button type="button"
                           class="btn btn-sm btn-outline-secondary d-none"
                           id="datasetKeyColumnRetry"
                           onclick="retryDatasetKeyColumns()">Retry</button>
                 </div>
               </div>`
            : (ds.key_column
                ? `<div class="mt-1"><small class="text-muted">Key: <code>${escapeHtml(ds.key_column)}</code></small></div>`
                : '');
        const descHtml = !viewOnly
            ? `<div class="mt-2">
                 <div class="d-flex align-items-center justify-content-between mb-1">
                   <label class="form-label form-label-sm mb-0" style="font-size:0.8rem;" for="datasetDescriptionInput">
                     Description <small class="text-muted">(purpose of this dataset)</small>
                   </label>
                   <button type="button"
                           class="btn btn-sm btn-outline-secondary py-0 px-1"
                           id="datasetDescriptionFromUcBtn"
                           title="Fill from Data Sources table description"
                           onclick="loadDatasetDescriptionFromDataSource()">
                     <i class="bi bi-database-down"></i>
                   </button>
                 </div>
                 <textarea class="form-control form-control-sm" rows="2"
                           id="datasetDescriptionInput"
                           oninput="onDatasetDescriptionChange(this.value)">${escapeHtml(ds.description || '')}</textarea>
               </div>`
            : (ds.description
                ? `<div class="mt-1"><small class="text-muted">${escapeHtml(ds.description)}</small></div>`
                : '');
        container.innerHTML = `
            <div class="d-flex align-items-center gap-2">
                <i class="bi bi-table text-primary"></i>
                <div class="flex-grow-1">
                    <div class="fw-semibold">${escapeHtml(ds.asset)} ${badge}</div>
                    <small class="text-muted">${escapeHtml(fullName)}</small>
                </div>
                ${!viewOnly ? `<button type="button" class="btn btn-sm btn-outline-danger py-0 px-1" onclick="removeSharedEntityDataset()" title="Remove dataset"><i class="bi bi-x"></i></button>` : ''}
            </div>
            ${keyColHtml}
            ${descHtml}
        `;
        if (!viewOnly) {
            void _loadDatasetKeyColumns(ds);
        }
    } else {
        container.innerHTML = '<small class="text-muted">No dataset assigned</small>';
    }
}

function _setDatasetKeyColumnState(label, retry = false) {
    const select = panelGetById('datasetKeyColumnSelect');
    const retryButton = panelGetById('datasetKeyColumnRetry');
    if (select) {
        select.disabled = true;
        select.innerHTML = `<option value="">${escapeHtml(label)}</option>`;
    }
    if (retryButton) retryButton.classList.toggle('d-none', !retry);
}

function _populateDatasetKeyColumnSelect(columns) {
    const select = panelGetById('datasetKeyColumnSelect');
    const retryButton = panelGetById('datasetKeyColumnRetry');
    if (!select || !sharedPanelDataset) return;

    const names = columns
        .map(column => String(column?.name || '').trim())
        .filter(Boolean);
    if (!names.length) {
        _setDatasetKeyColumnState('No columns found', true);
        return;
    }

    const current = sharedPanelDataset.key_column || '';
    const options = [new Option('Select a key column…', '')];
    if (current && !names.includes(current)) {
        options.push(new Option(`${current} (missing)`, current, true, true));
    }
    for (const name of names) {
        const selected = name === current;
        options.push(new Option(name, name, selected, selected));
    }
    select.replaceChildren(...options);
    select.disabled = false;
    if (retryButton) retryButton.classList.add('d-none');
}

async function _loadDatasetKeyColumns(dataset, force = false) {
    const datasetKey = _datasetKey(dataset);
    if (!datasetKey || !dataset?.catalog || !dataset?.schema || !dataset?.asset) {
        _setDatasetKeyColumnState('No columns found', true);
        return;
    }

    if (!force && _datasetColumnCache.has(datasetKey)) {
        _populateDatasetKeyColumnSelect(_datasetColumnCache.get(datasetKey));
        return;
    }

    _setDatasetKeyColumnState('Loading columns…');
    try {
        const response = await fetch('/mapping/table-columns', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                catalog: dataset.catalog,
                schema: dataset.schema,
                table: dataset.asset,
            }),
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        const columns = Array.isArray(data.columns) ? data.columns : [];
        if (columns.length) {
            _datasetColumnCache.set(datasetKey, columns);
        }
        if (_isCurrentDataset(datasetKey)) {
            _populateDatasetKeyColumnSelect(columns);
        }
    } catch (error) {
        console.error('[Dataset] Error loading columns:', error);
        if (_isCurrentDataset(datasetKey)) {
            _setDatasetKeyColumnState('Failed to load columns', true);
        }
    }
}

function retryDatasetKeyColumns() {
    if (!sharedPanelDataset) return;
    void _loadDatasetKeyColumns(sharedPanelDataset, true);
}

function onDatasetKeyColumnChange(value) {
    if (!sharedPanelDataset) return;
    sharedPanelDataset.key_column = value || null;
    markPanelDirty();
}

function onDatasetDescriptionChange(value) {
    if (!sharedPanelDataset) return;
    sharedPanelDataset.description = value.trim() || null;
    markPanelDirty();
}

async function loadDatasetDescriptionFromDataSource() {
    if (!sharedPanelDataset?.catalog || !sharedPanelDataset?.schema || !sharedPanelDataset?.asset) {
        showNotification('No dataset selected', 'warning', 2500);
        return;
    }
    const btn = panelGetById('datasetDescriptionFromUcBtn');
    const datasetKey = _datasetKey(sharedPanelDataset);
    const targetFullName = (
        sharedPanelDataset.fullName
        || `${sharedPanelDataset.catalog}.${sharedPanelDataset.schema}.${sharedPanelDataset.asset}`
    ).toLowerCase();
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
    }
    try {
        const resp = await fetch('/domain/metadata', { credentials: 'same-origin' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        if (!_isCurrentDataset(datasetKey)) return;
        if (!data.success || !data.has_metadata || !data.metadata?.tables?.length) {
            showNotification('No Data Sources loaded for this domain', 'info', 3000);
            return;
        }
        const match = data.metadata.tables.find(table => {
            const fullName = String(table.full_name || '').toLowerCase();
            const shortName = String(table.name || '').toLowerCase();
            return fullName === targetFullName
                || shortName === targetFullName
                || shortName === String(sharedPanelDataset.asset || '').toLowerCase();
        });
        if (!match) {
            showNotification('Dataset not found in Data Sources', 'info', 3000);
            return;
        }
        const comment = String(match.comment || match.description || '').trim();
        if (!comment) {
            showNotification('No table description in Data Sources for this dataset', 'info', 3000);
            return;
        }
        onDatasetDescriptionChange(comment);
        const input = panelGetById('datasetDescriptionInput');
        if (input) input.value = comment;
        showNotification('Description loaded from Data Sources', 'success', 2500);
    } catch (err) {
        console.error('[Dataset] Error loading Data Sources description:', err);
        if (_isCurrentDataset(datasetKey)) {
            showNotification('Failed to load Data Sources description', 'danger', 3000);
        }
    } finally {
        if (btn && _isCurrentDataset(datasetKey)) {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-database-down"></i>';
        }
    }
}

/**
 * Remove the assigned dataset
 */
function removeSharedEntityDataset() {
    sharedPanelDataset = null;
    markPanelDirty();
    renderSharedEntityDataset(false);
}

// Selector state
let _datasetSelCatalog = '';
let _datasetSelSchema = '';
let _datasetAllAssets = [];
const _datasetColumnCache = new Map();

function _datasetKey(dataset) {
    if (!dataset) return '';
    return `${dataset.catalog || ''}.${dataset.schema || ''}.${dataset.asset || ''}`;
}

function _isCurrentDataset(datasetKey) {
    return Boolean(sharedPanelDataset && _datasetKey(sharedPanelDataset) === datasetKey);
}

/**
 * Open the dataset selector modal (catalog -> schema -> table/view).
 */
async function openDatasetSelectorModal() {
    const modalId = 'datasetSelectorModal';
    const existing = document.getElementById(modalId);
    if (existing) existing.remove();

    _datasetSelCatalog = '';
    _datasetSelSchema = '';
    _datasetAllAssets = [];

    const modalHtml = `
        <div class="modal fade" id="${modalId}" tabindex="-1" data-bs-backdrop="static">
            <div class="modal-dialog modal-lg modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-table me-2"></i>Select a Unity Catalog Table or View</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="row g-2 mb-3">
                            <div class="col-md-6">
                                <label class="form-label small fw-semibold">Catalog</label>
                                <select class="form-select form-select-sm" id="datasetCatalogSelect" onchange="_datasetOnCatalogChange()">
                                    <option value="">Loading...</option>
                                </select>
                            </div>
                            <div class="col-md-6">
                                <label class="form-label small fw-semibold">Schema</label>
                                <select class="form-select form-select-sm" id="datasetSchemaSelect" onchange="_datasetOnSchemaChange()" disabled>
                                    <option value="">Select a catalog first</option>
                                </select>
                            </div>
                        </div>
                        <input type="text" class="form-control form-control-sm mb-2" id="datasetAssetSearch" placeholder="Search tables and views..." oninput="_filterDatasetAssets()" disabled>
                        <div id="datasetAssetList" class="list-group" style="max-height: 320px; overflow-y: auto;">
                            <div class="text-muted p-3 text-center">Select a catalog and schema to list tables and views</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;

    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById(modalId));
    modal.show();

    // Pre-select the current dataset's catalog/schema when editing
    const preCatalog = sharedPanelDataset?.catalog || '';
    const preSchema = sharedPanelDataset?.schema || '';

    try {
        const resp = await fetch('/settings/catalogs', { credentials: 'same-origin' });
        const data = await resp.json();
        const sel = document.getElementById('datasetCatalogSelect');
        if (!sel) return;
        const catalogs = data.catalogs || [];
        sel.innerHTML = '<option value="">Select a catalog...</option>' +
            catalogs.map(c => `<option value="${escapeHtml(c)}"${c === preCatalog ? ' selected' : ''}>${escapeHtml(c)}</option>`).join('');
        if (preCatalog) {
            _datasetSelCatalog = preCatalog;
            await _datasetLoadSchemas(preCatalog, preSchema);
        }
    } catch (err) {
        console.error('[Dataset] Error loading catalogs:', err);
        const sel = document.getElementById('datasetCatalogSelect');
        if (sel) sel.innerHTML = '<option value="">Failed to load catalogs</option>';
    }
}

async function _datasetOnCatalogChange() {
    const catalog = document.getElementById('datasetCatalogSelect')?.value || '';
    _datasetSelCatalog = catalog;
    _datasetSelSchema = '';
    _datasetAllAssets = [];
    const searchInput = document.getElementById('datasetAssetSearch');
    if (searchInput) searchInput.disabled = true;
    const list = document.getElementById('datasetAssetList');
    if (list) list.innerHTML = '<div class="text-muted p-3 text-center">Select a schema to list tables and views</div>';
    if (catalog) {
        await _datasetLoadSchemas(catalog);
    } else {
        const schemaSel = document.getElementById('datasetSchemaSelect');
        if (schemaSel) {
            schemaSel.disabled = true;
            schemaSel.innerHTML = '<option value="">Select a catalog first</option>';
        }
    }
}

async function _datasetLoadSchemas(catalog, preSchema = '') {
    const schemaSel = document.getElementById('datasetSchemaSelect');
    if (!schemaSel) return;
    schemaSel.disabled = true;
    schemaSel.innerHTML = '<option value="">Loading...</option>';
    try {
        const resp = await fetch(`/settings/schemas/${encodeURIComponent(catalog)}`, { credentials: 'same-origin' });
        const data = await resp.json();
        const schemas = data.schemas || [];
        schemaSel.innerHTML = '<option value="">Select a schema...</option>' +
            schemas.map(s => `<option value="${escapeHtml(s)}"${s === preSchema ? ' selected' : ''}>${escapeHtml(s)}</option>`).join('');
        schemaSel.disabled = false;
        if (preSchema) {
            _datasetSelSchema = preSchema;
            await _datasetLoadAssets(catalog, preSchema);
        }
    } catch (err) {
        console.error('[Dataset] Error loading schemas:', err);
        schemaSel.innerHTML = '<option value="">Failed to load schemas</option>';
    }
}

async function _datasetOnSchemaChange() {
    const schema = document.getElementById('datasetSchemaSelect')?.value || '';
    _datasetSelSchema = schema;
    if (schema && _datasetSelCatalog) {
        await _datasetLoadAssets(_datasetSelCatalog, schema);
    } else {
        const list = document.getElementById('datasetAssetList');
        if (list) list.innerHTML = '<div class="text-muted p-3 text-center">Select a schema to list tables and views</div>';
    }
}

async function _datasetLoadAssets(catalog, schema) {
    const list = document.getElementById('datasetAssetList');
    if (list) list.innerHTML = '<div class="text-center py-3"><div class="spinner-border spinner-border-sm text-primary"></div> Loading tables and views...</div>';
    try {
        const resp = await fetch(`/settings/uc-assets?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`, { credentials: 'same-origin' });
        const data = await resp.json();
        _datasetAllAssets = (data.success && data.assets) ? data.assets : [];
        const searchInput = document.getElementById('datasetAssetSearch');
        if (searchInput) searchInput.disabled = _datasetAllAssets.length === 0;
        if (!_datasetAllAssets.length) {
            if (list) list.innerHTML = '<div class="text-muted p-3 text-center">No tables or views found in this schema</div>';
            return;
        }
        _renderDatasetAssetList(_datasetAllAssets);
    } catch (err) {
        console.error('[Dataset] Error loading assets:', err);
        if (list) list.innerHTML = '<div class="text-danger p-2"><i class="bi bi-exclamation-triangle"></i> Failed to load tables and views</div>';
    }
}

function _renderDatasetAssetList(assets) {
    const list = document.getElementById('datasetAssetList');
    if (!list) return;
    list.innerHTML = assets.map(a => {
        const isView = (a.table_type || '').toUpperCase() === 'VIEW';
        const badge = isView
            ? '<span class="badge bg-info text-dark">View</span>'
            : '<span class="badge bg-secondary">Table</span>';
        const icon = isView ? 'bi-eye' : 'bi-table';
        return `
            <button type="button" class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                    onclick='_datasetSelectAsset(${JSON.stringify(a).replace(/'/g, "&#39;")})'>
                <i class="bi ${icon} text-primary"></i>
                <div class="flex-grow-1">
                    <div class="fw-semibold">${escapeHtml(a.name)} ${badge}</div>
                    ${a.comment ? `<small class="text-muted">${escapeHtml(String(a.comment).substring(0, 80))}</small>` : ''}
                </div>
                <i class="bi bi-chevron-right text-muted"></i>
            </button>
        `;
    }).join('');
}

function _filterDatasetAssets() {
    const q = (document.getElementById('datasetAssetSearch')?.value || '').toLowerCase();
    const filtered = _datasetAllAssets.filter(a =>
        (a.name || '').toLowerCase().includes(q) ||
        (a.comment || '').toLowerCase().includes(q)
    );
    _renderDatasetAssetList(filtered);
}

function _datasetSelectAsset(asset) {
    const previousDescription = Object.prototype.hasOwnProperty.call(
        sharedPanelDataset || {},
        'description'
    )
        ? (sharedPanelDataset.description || '')
        : null;
    const defaultDescription = String(asset.comment || '').trim();
    sharedPanelDataset = {
        catalog: _datasetSelCatalog,
        schema: _datasetSelSchema,
        asset: asset.name,
        type: (asset.table_type || '').toUpperCase() === 'VIEW' ? 'VIEW' : 'TABLE',
        fullName: asset.full_name || `${_datasetSelCatalog}.${_datasetSelSchema}.${asset.name}`,
        key_column: null,
        description: previousDescription !== null ? previousDescription : (defaultDescription || null),
    };
    markPanelDirty();
    renderSharedEntityDataset(false);
    closeDatasetSelectorModal();
    showNotification(`Dataset linked: ${sharedPanelDataset.fullName}`, 'success', 3000);
}

function closeDatasetSelectorModal() {
    const modal = document.getElementById('datasetSelectorModal');
    if (modal) {
        const bsModal = bootstrap.Modal.getInstance(modal);
        if (bsModal) bsModal.hide();
        modal.addEventListener('hidden.bs.modal', () => modal.remove(), { once: true });
    }
    _datasetSelCatalog = '';
    _datasetSelSchema = '';
    _datasetAllAssets = [];
}

// =====================================================
// UNITY CATALOG FUNCTION PICKERS — shared cascade
// =====================================================
// Both the Actions picker and the Virtual Attributes picker walk the same
// catalog -> schema -> function cascade. Only the eligibility rules and what
// they do with the chosen function differ, so the three fetches live here.

/**
 * Fill a catalog <select> with the accessible Unity Catalog catalogs.
 */
async function _ucFillCatalogSelect(selectId) {
    try {
        const resp = await fetch('/settings/catalogs', { credentials: 'same-origin' });
        const data = await resp.json();
        const sel = document.getElementById(selectId);
        if (!sel) return;
        const catalogs = data.catalogs || [];
        sel.innerHTML = '<option value="">Select a catalog...</option>' +
            catalogs.map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('');
    } catch (err) {
        console.error('[UCPicker] Error loading catalogs:', err);
        const sel = document.getElementById(selectId);
        if (sel) sel.innerHTML = '<option value="">Failed to load catalogs</option>';
    }
}

/**
 * Fill a schema <select> with the schemas of *catalog*.
 */
async function _ucFillSchemaSelect(catalog, selectId) {
    const schemaSel = document.getElementById(selectId);
    if (!schemaSel) return;
    schemaSel.disabled = true;
    schemaSel.innerHTML = '<option value="">Loading...</option>';
    try {
        const resp = await fetch(`/settings/schemas/${encodeURIComponent(catalog)}`, { credentials: 'same-origin' });
        const data = await resp.json();
        const schemas = data.schemas || [];
        schemaSel.innerHTML = '<option value="">Select a schema...</option>' +
            schemas.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
        schemaSel.disabled = false;
    } catch (err) {
        console.error('[UCPicker] Error loading schemas:', err);
        schemaSel.innerHTML = '<option value="">Failed to load schemas</option>';
    }
}

/**
 * Return the user-defined functions of catalog.schema, or [] on failure.
 * Each entry carries param_count, returns_table, return_type and
 * return_columns.
 */
async function _ucFetchFunctions(catalog, schema) {
    const resp = await fetch(
        `/settings/uc-functions?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`,
        { credentials: 'same-origin' }
    );
    const data = await resp.json();
    return (data.success && data.functions) ? data.functions : [];
}

/**
 * Number of values a function returns: one RETURNS TABLE column each, or 1 for
 * a scalar. Null when the metastore did not report the columns of a table
 * function, which is not the same as returning nothing.
 *
 * @param {Object} fn Entry from /settings/uc-functions.
 * @returns {?number} Output count, or null when unknown.
 */
function _ucOutputCount(fn) {
    if (!fn.returns_table) return 1;
    const columns = fn.return_columns || [];
    return columns.length > 0 ? columns.length : null;
}

/**
 * Badge stating how many values a function returns, for the picker list.
 * Empty when the count is unknown rather than guessing at it.
 */
function _ucOutputBadge(fn) {
    const count = _ucOutputCount(fn);
    if (count === null) {
        return '<span class="badge bg-light text-muted border" ' +
            'title="Unity Catalog did not report this function\'s result columns">' +
            'outputs unknown</span>';
    }
    return `<span class="badge bg-light text-dark border" title="Returns ${count} value${count === 1 ? '' : 's'}">` +
        `${count} output${count === 1 ? '' : 's'}</span>`;
}

/**
 * Discard a dynamically inserted picker modal once Bootstrap has hidden it.
 */
function _ucClosePickerModal(modalId) {
    const modal = document.getElementById(modalId);
    if (!modal) return;
    const bsModal = bootstrap.Modal.getInstance(modal);
    if (bsModal) bsModal.hide();
    modal.addEventListener('hidden.bs.modal', () => modal.remove(), { once: true });
}

// =====================================================
// UNITY CATALOG FUNCTION ACTIONS
// =====================================================

/**
 * Render the linked Unity Catalog function actions in the entity form
 */
function renderSharedEntityActions(viewOnly = false) {
    const container = panelGetById('sharedEntityActionsContent');
    if (!container) return;

    if (!sharedPanelActions.length) {
        container.innerHTML = '<small class="text-muted">No actions assigned</small>';
        return;
    }

    container.innerHTML = sharedPanelActions.map((action, idx) => {
        const fullName = action.fullName
            || `${action.catalog || ''}.${action.schema || ''}.${action.function || ''}`;
        const badge = action.returns_table
            ? '<span class="badge bg-info text-dark">Table</span>'
            : '<span class="badge bg-secondary">Scalar</span>';
        const descHtml = !viewOnly
            ? `<textarea class="form-control form-control-sm mt-1" rows="2"
                         id="actionDescriptionInput-${idx}"
                         placeholder="What does this action do?"
                         oninput="onActionDescriptionChange(${idx}, this.value)">${escapeHtml(action.description || '')}</textarea>`
            : (action.description
                ? `<small class="text-muted d-block ms-3">${escapeHtml(action.description)}</small>`
                : '');
        return `
            <div class="${idx > 0 ? 'mt-2 pt-2 border-top' : ''}">
                <div class="d-flex align-items-center gap-2">
                    <i class="bi bi-lightning-charge text-warning"></i>
                    <div class="flex-grow-1">
                        <div class="fw-semibold">${escapeHtml(action.function || '')} ${badge}</div>
                        <small class="text-muted">${escapeHtml(fullName)}</small>
                    </div>
                    ${!viewOnly ? `<button type="button" class="btn btn-sm btn-outline-danger py-0 px-1" onclick="removeSharedEntityAction(${idx})" title="Remove action"><i class="bi bi-x"></i></button>` : ''}
                </div>
                ${descHtml}
            </div>
        `;
    }).join('');
}

function onActionDescriptionChange(index, value) {
    const action = sharedPanelActions[index];
    if (!action) return;
    action.description = value.trim() || null;
    markPanelDirty();
}

/**
 * Remove an action by index
 */
function removeSharedEntityAction(index) {
    sharedPanelActions.splice(index, 1);
    markPanelDirty();
    renderSharedEntityActions(false);
}

// Action selector state
let _actionSelCatalog = '';
let _actionSelSchema = '';
let _actionAllFunctions = [];

/**
 * Open the action selector modal (catalog -> schema -> function).
 */
async function openActionSelectorModal() {
    const modalId = 'actionSelectorModal';
    const existing = document.getElementById(modalId);
    if (existing) existing.remove();

    _actionSelCatalog = '';
    _actionSelSchema = '';
    _actionAllFunctions = [];

    const modalHtml = `
        <div class="modal fade" id="${modalId}" tabindex="-1" data-bs-backdrop="static">
            <div class="modal-dialog modal-lg modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-lightning-charge me-2"></i>Select a Unity Catalog Function</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="alert alert-info py-2 px-3 small">
                            <i class="bi bi-info-circle me-1"></i>
                            The function must accept <strong>exactly one parameter</strong>: the ID of the
                            entity to act on. Functions with a different signature cannot be selected.
                        </div>
                        <div class="row g-2 mb-3">
                            <div class="col-md-6">
                                <label class="form-label small fw-semibold">Catalog</label>
                                <select class="form-select form-select-sm" id="actionCatalogSelect" onchange="_actionOnCatalogChange()">
                                    <option value="">Loading...</option>
                                </select>
                            </div>
                            <div class="col-md-6">
                                <label class="form-label small fw-semibold">Schema</label>
                                <select class="form-select form-select-sm" id="actionSchemaSelect" onchange="_actionOnSchemaChange()" disabled>
                                    <option value="">Select a catalog first</option>
                                </select>
                            </div>
                        </div>
                        <input type="text" class="form-control form-control-sm mb-2" id="actionFunctionSearch" placeholder="Search functions..." oninput="_filterActionFunctions()" disabled>
                        <div id="actionFunctionList" class="list-group" style="max-height: 320px; overflow-y: auto;">
                            <div class="text-muted p-3 text-center">Select a catalog and schema to list functions</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;

    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById(modalId));
    modal.show();

    await _ucFillCatalogSelect('actionCatalogSelect');
}

async function _actionOnCatalogChange() {
    const catalog = document.getElementById('actionCatalogSelect')?.value || '';
    _actionSelCatalog = catalog;
    _actionSelSchema = '';
    _actionAllFunctions = [];
    const searchInput = document.getElementById('actionFunctionSearch');
    if (searchInput) searchInput.disabled = true;
    const list = document.getElementById('actionFunctionList');
    if (list) list.innerHTML = '<div class="text-muted p-3 text-center">Select a schema to list functions</div>';
    if (catalog) {
        await _ucFillSchemaSelect(catalog, 'actionSchemaSelect');
    } else {
        const schemaSel = document.getElementById('actionSchemaSelect');
        if (schemaSel) {
            schemaSel.disabled = true;
            schemaSel.innerHTML = '<option value="">Select a catalog first</option>';
        }
    }
}

async function _actionOnSchemaChange() {
    const schema = document.getElementById('actionSchemaSelect')?.value || '';
    _actionSelSchema = schema;
    if (schema && _actionSelCatalog) {
        await _actionLoadFunctions(_actionSelCatalog, schema);
    } else {
        const list = document.getElementById('actionFunctionList');
        if (list) list.innerHTML = '<div class="text-muted p-3 text-center">Select a schema to list functions</div>';
    }
}

async function _actionLoadFunctions(catalog, schema) {
    const list = document.getElementById('actionFunctionList');
    if (list) list.innerHTML = '<div class="text-center py-3"><div class="spinner-border spinner-border-sm text-primary"></div> Loading functions...</div>';
    try {
        _actionAllFunctions = await _ucFetchFunctions(catalog, schema);
        const searchInput = document.getElementById('actionFunctionSearch');
        if (searchInput) searchInput.disabled = _actionAllFunctions.length === 0;
        if (!_actionAllFunctions.length) {
            if (list) list.innerHTML = '<div class="text-muted p-3 text-center">No functions found in this schema</div>';
            return;
        }
        _renderActionFunctionList(_actionAllFunctions);
    } catch (err) {
        console.error('[Action] Error loading functions:', err);
        if (list) list.innerHTML = '<div class="text-danger p-2"><i class="bi bi-exclamation-triangle"></i> Failed to load functions</div>';
    }
}

function _renderActionFunctionList(functions) {
    const list = document.getElementById('actionFunctionList');
    if (!list) return;
    list.innerHTML = functions.map(fn => {
        // Only single-parameter functions are eligible: the one argument is the entity ID.
        const eligible = Number(fn.param_count) === 1;
        const badge = fn.returns_table
            ? '<span class="badge bg-info text-dark">Table</span>'
            : '<span class="badge bg-secondary">Scalar</span>';
        const params = (fn.input_params || []).join(', ');
        const hint = eligible
            ? `<small class="text-muted">${escapeHtml(fn.comment ? String(fn.comment).substring(0, 80) : `(${params})`)}</small>`
            : `<small class="text-warning"><i class="bi bi-exclamation-triangle me-1"></i>Needs exactly 1 parameter (has ${Number(fn.param_count) || 0})</small>`;
        return `
            <button type="button" class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                    ${eligible ? `onclick='_actionSelectFunction(${JSON.stringify(fn).replace(/'/g, "&#39;")})'` : 'disabled'}>
                <i class="bi bi-lightning-charge text-warning"></i>
                <div class="flex-grow-1">
                    <div class="fw-semibold">${escapeHtml(fn.name)} ${badge} ${_ucOutputBadge(fn)}</div>
                    ${hint}
                </div>
                ${eligible ? '<i class="bi bi-chevron-right text-muted"></i>' : ''}
            </button>
        `;
    }).join('');
}

function _filterActionFunctions() {
    const q = (document.getElementById('actionFunctionSearch')?.value || '').toLowerCase();
    const filtered = _actionAllFunctions.filter(fn =>
        (fn.name || '').toLowerCase().includes(q) ||
        (fn.comment || '').toLowerCase().includes(q)
    );
    _renderActionFunctionList(filtered);
}

function _actionSelectFunction(fn) {
    const fullName = fn.full_name || `${_actionSelCatalog}.${_actionSelSchema}.${fn.name}`;
    if (sharedPanelActions.some(a => a.fullName === fullName)) {
        showNotification(`Action already assigned: ${fullName}`, 'warning', 3000);
        return;
    }
    sharedPanelActions.push({
        catalog: _actionSelCatalog,
        schema: _actionSelSchema,
        function: fn.name,
        fullName,
        description: String(fn.comment || '').trim() || null,
        returns_table: Boolean(fn.returns_table),
    });
    markPanelDirty();
    renderSharedEntityActions(false);
    closeActionSelectorModal();
    showNotification(`Action added: ${fullName} — add a description`, 'success', 3000);
}

function closeActionSelectorModal() {
    _ucClosePickerModal('actionSelectorModal');
}

// =====================================================
// VIRTUAL ATTRIBUTES
// =====================================================
// A virtual attribute is not mapped and not stored in the graph: a Unity
// Catalog function computes it on demand. One function yields one attribute
// per RETURNS TABLE column, or a single one when it is scalar. Nothing is
// computed here — at design time there is no entity instance to bind the ID to.

/**
 * Alias used for the single value of a scalar function, which has no result
 * column name of its own. Must match SCALAR_RESULT_COLUMN server-side.
 */
const VA_SCALAR_COLUMN = 'result';

/**
 * Render the declared virtual attribute groups in the entity form.
 */
function renderSharedEntityVirtualAttributes(viewOnly = false) {
    const container = panelGetById('sharedEntityVirtualAttributesContent');
    if (!container) return;

    if (!sharedPanelVirtualAttributes.length) {
        container.innerHTML = '<small class="text-muted">No virtual attributes</small>';
        return;
    }

    container.innerHTML = sharedPanelVirtualAttributes.map((group, idx) => {
        const fullName = group.fullName
            || `${group.catalog || ''}.${group.schema || ''}.${group.function || ''}`;
        const badge = group.returns_table
            ? '<span class="badge bg-info text-dark">Table</span>'
            : '<span class="badge bg-secondary">Scalar</span>';
        const descHtml = !viewOnly
            ? `<textarea class="form-control form-control-sm mt-1" rows="2"
                         id="vaDescriptionInput-${idx}"
                         placeholder="What does this function compute?"
                         oninput="onVirtualAttributeDescriptionChange(${idx}, this.value)">${escapeHtml(group.description || '')}</textarea>`
            : (group.description
                ? `<small class="text-muted d-block ms-3">${escapeHtml(group.description)}</small>`
                : '');
        const attrsHtml = (group.attributes || []).map((attr, aIdx) => {
            const type = attr.dataType
                ? `<span class="badge bg-light text-muted border ms-1">${escapeHtml(attr.dataType)}</span>`
                : '';
            const labelHtml = !viewOnly
                ? `<input type="text" class="form-control form-control-sm mt-1"
                          value="${escapeHtml(attr.label || attr.name || '')}"
                          placeholder="Display label"
                          oninput="onVirtualAttributeLabelChange(${idx}, ${aIdx}, this.value)">`
                : '';
            return `
                <div class="${aIdx > 0 ? 'mt-2' : ''}">
                    <div class="small"><code>${escapeHtml(attr.name || '')}</code>${type}</div>
                    ${labelHtml}
                </div>
            `;
        }).join('');
        return `
            <div class="va-group">
                <div class="d-flex align-items-center gap-2">
                    <i class="bi bi-magic text-primary"></i>
                    <div class="flex-grow-1">
                        <div class="fw-semibold">${escapeHtml(group.function || '')} ${badge}</div>
                        <small class="text-muted">${escapeHtml(fullName)}</small>
                    </div>
                    ${!viewOnly ? `<button type="button" class="btn btn-sm btn-outline-danger py-0 px-1" onclick="removeSharedEntityVirtualAttribute(${idx})" title="Remove this function and its attributes"><i class="bi bi-x"></i></button>` : ''}
                </div>
                ${descHtml}
                <div class="va-group-attrs">${attrsHtml}</div>
            </div>
        `;
    }).join('');
}

function onVirtualAttributeDescriptionChange(index, value) {
    const group = sharedPanelVirtualAttributes[index];
    if (!group) return;
    group.description = value.trim() || null;
    markPanelDirty();
}

function onVirtualAttributeLabelChange(index, attrIndex, value) {
    const attr = sharedPanelVirtualAttributes[index]?.attributes?.[attrIndex];
    if (!attr) return;
    attr.label = value.trim() || attr.name;
    markPanelDirty();
}

/**
 * Remove a whole group. A single attribute cannot be removed on its own: it is
 * part of the function's signature, so dropping one would make the mapping
 * from result column to attribute ambiguous.
 */
async function removeSharedEntityVirtualAttribute(index) {
    const group = sharedPanelVirtualAttributes[index];
    if (!group) return;
    const count = (group.attributes || []).length;
    const confirmed = await showConfirmDialog({
        title: 'Remove virtual attributes',
        message: `Remove <strong>${escapeHtml(group.function || group.fullName)}</strong> and its ${count} virtual attribute${count === 1 ? '' : 's'}?`,
        confirmText: 'Remove',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
    if (!confirmed) return;
    sharedPanelVirtualAttributes.splice(index, 1);
    markPanelDirty();
    renderSharedEntityVirtualAttributes(false);
}

/**
 * Names already taken on this class: mapped attributes (own + inherited) and
 * the other virtual attributes. The Graph Explorer and the MCP render both
 * families in one namespace, so a collision has to be resolved at declaration
 * time rather than surfacing as two rows with the same name.
 */
function _vaTakenNames(exceptGroupIndex = -1) {
    const taken = new Set();
    sharedPanelOwnAttributes.forEach(a => a.name && taken.add(a.name));
    sharedPanelInheritedAttributes.forEach(a => a.name && taken.add(a.name));
    sharedPanelVirtualAttributes.forEach((group, idx) => {
        if (idx === exceptGroupIndex) return;
        (group.attributes || []).forEach(a => a.name && taken.add(a.name));
    });
    return taken;
}

/**
 * Derive the virtual attributes of *fn*, suffixing any colliding name.
 * Returns { attributes, renamed } where renamed lists "old -> new" pairs.
 */
function _vaDeriveAttributes(fn) {
    const taken = _vaTakenNames();
    const columns = fn.returns_table
        ? (fn.return_columns || [])
        : [{ name: fn.name, data_type: fn.return_type || '' }];
    const attributes = [];
    const renamed = [];
    columns.forEach(col => {
        const rawName = String(col.name || '').trim();
        if (!rawName) return;
        let name = rawName;
        let suffix = 2;
        while (taken.has(name)) {
            name = `${rawName}_${suffix}`;
            suffix += 1;
        }
        if (name !== rawName) renamed.push(`${rawName} → ${name}`);
        taken.add(name);
        attributes.push({
            name,
            column: fn.returns_table ? rawName : VA_SCALAR_COLUMN,
            label: name,
            dataType: String(col.data_type || '').trim() || null,
        });
    });
    return { attributes, renamed };
}

// Virtual attribute selector state
let _vaSelCatalog = '';
let _vaSelSchema = '';
let _vaAllFunctions = [];

/**
 * Open the virtual attribute selector modal (catalog -> schema -> function).
 */
async function openVirtualAttributeSelectorModal() {
    const modalId = 'virtualAttributeSelectorModal';
    const existing = document.getElementById(modalId);
    if (existing) existing.remove();

    _vaSelCatalog = '';
    _vaSelSchema = '';
    _vaAllFunctions = [];

    const modalHtml = `
        <div class="modal fade" id="${modalId}" tabindex="-1" data-bs-backdrop="static">
            <div class="modal-dialog modal-lg modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-magic me-2"></i>Select a Unity Catalog Function</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="alert alert-info py-2 px-3 small">
                            <i class="bi bi-info-circle me-1"></i>
                            The function must accept <strong>exactly one parameter</strong>: the ID of the
                            entity. One virtual attribute is created per returned column.
                        </div>
                        <div class="row g-2 mb-3">
                            <div class="col-md-6">
                                <label class="form-label small fw-semibold">Catalog</label>
                                <select class="form-select form-select-sm" id="vaCatalogSelect" onchange="_vaOnCatalogChange()">
                                    <option value="">Loading...</option>
                                </select>
                            </div>
                            <div class="col-md-6">
                                <label class="form-label small fw-semibold">Schema</label>
                                <select class="form-select form-select-sm" id="vaSchemaSelect" onchange="_vaOnSchemaChange()" disabled>
                                    <option value="">Select a catalog first</option>
                                </select>
                            </div>
                        </div>
                        <input type="text" class="form-control form-control-sm mb-2" id="vaFunctionSearch" placeholder="Search functions..." oninput="_filterVaFunctions()" disabled>
                        <div id="vaFunctionList" class="list-group va-function-list">
                            <div class="text-muted p-3 text-center">Select a catalog and schema to list functions</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;

    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById(modalId));
    modal.show();

    await _ucFillCatalogSelect('vaCatalogSelect');
}

async function _vaOnCatalogChange() {
    const catalog = document.getElementById('vaCatalogSelect')?.value || '';
    _vaSelCatalog = catalog;
    _vaSelSchema = '';
    _vaAllFunctions = [];
    const searchInput = document.getElementById('vaFunctionSearch');
    if (searchInput) searchInput.disabled = true;
    const list = document.getElementById('vaFunctionList');
    if (list) list.innerHTML = '<div class="text-muted p-3 text-center">Select a schema to list functions</div>';
    if (catalog) {
        await _ucFillSchemaSelect(catalog, 'vaSchemaSelect');
    } else {
        const schemaSel = document.getElementById('vaSchemaSelect');
        if (schemaSel) {
            schemaSel.disabled = true;
            schemaSel.innerHTML = '<option value="">Select a catalog first</option>';
        }
    }
}

async function _vaOnSchemaChange() {
    const schema = document.getElementById('vaSchemaSelect')?.value || '';
    _vaSelSchema = schema;
    if (schema && _vaSelCatalog) {
        await _vaLoadFunctions(_vaSelCatalog, schema);
    } else {
        const list = document.getElementById('vaFunctionList');
        if (list) list.innerHTML = '<div class="text-muted p-3 text-center">Select a schema to list functions</div>';
    }
}

async function _vaLoadFunctions(catalog, schema) {
    const list = document.getElementById('vaFunctionList');
    if (list) list.innerHTML = '<div class="text-center py-3"><div class="spinner-border spinner-border-sm text-primary"></div> Loading functions...</div>';
    try {
        _vaAllFunctions = await _ucFetchFunctions(catalog, schema);
        const searchInput = document.getElementById('vaFunctionSearch');
        if (searchInput) searchInput.disabled = _vaAllFunctions.length === 0;
        if (!_vaAllFunctions.length) {
            if (list) list.innerHTML = '<div class="text-muted p-3 text-center">No functions found in this schema</div>';
            return;
        }
        _renderVaFunctionList(_vaAllFunctions);
    } catch (err) {
        console.error('[VirtualAttribute] Error loading functions:', err);
        if (list) list.innerHTML = '<div class="text-danger p-2"><i class="bi bi-exclamation-triangle"></i> Failed to load functions</div>';
    }
}

function _renderVaFunctionList(functions) {
    const list = document.getElementById('vaFunctionList');
    if (!list) return;
    const assigned = new Set(sharedPanelVirtualAttributes.map(g => g.fullName));
    list.innerHTML = functions.map(fn => {
        const fullName = fn.full_name || `${_vaSelCatalog}.${_vaSelSchema}.${fn.name}`;
        // Same single-parameter rule as actions: the one argument is the entity ID.
        const singleParam = Number(fn.param_count) === 1;
        const alreadyAssigned = assigned.has(fullName);
        const eligible = singleParam && !alreadyAssigned;
        const badge = fn.returns_table
            ? '<span class="badge bg-info text-dark">Table</span>'
            : '<span class="badge bg-secondary">Scalar</span>';
        const returns = fn.returns_table
            ? (fn.return_columns || []).map(c => `${c.name}${c.data_type ? ` ${c.data_type}` : ''}`).join(', ')
            : (fn.return_type || '');
        let hint;
        if (!singleParam) {
            hint = `<small class="text-warning"><i class="bi bi-exclamation-triangle me-1"></i>Needs exactly 1 parameter (has ${Number(fn.param_count) || 0})</small>`;
        } else if (alreadyAssigned) {
            hint = '<small class="text-warning"><i class="bi bi-check2-circle me-1"></i>Already assigned to this entity</small>';
        } else {
            // The output count is what the user is really choosing here: it is
            // the number of virtual attributes the function will create.
            const count = _ucOutputCount(fn);
            const creates = count === null
                ? ''
                : ` — creates ${count} attribute${count === 1 ? '' : 's'}`;
            hint = `<small class="text-muted">returns: ${escapeHtml(returns || 'unknown')}${creates}</small>`;
        }
        return `
            <button type="button" class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                    ${eligible ? `onclick='_vaSelectFunction(${JSON.stringify(fn).replace(/'/g, "&#39;")})'` : 'disabled'}>
                <i class="bi bi-magic text-primary"></i>
                <div class="flex-grow-1">
                    <div class="fw-semibold">${escapeHtml(fn.name)} ${badge} ${_ucOutputBadge(fn)}</div>
                    ${hint}
                </div>
                ${eligible ? '<i class="bi bi-chevron-right text-muted"></i>' : ''}
            </button>
        `;
    }).join('');
}

function _filterVaFunctions() {
    const q = (document.getElementById('vaFunctionSearch')?.value || '').toLowerCase();
    const filtered = _vaAllFunctions.filter(fn =>
        (fn.name || '').toLowerCase().includes(q) ||
        (fn.comment || '').toLowerCase().includes(q)
    );
    _renderVaFunctionList(filtered);
}

function _vaSelectFunction(fn) {
    const fullName = fn.full_name || `${_vaSelCatalog}.${_vaSelSchema}.${fn.name}`;
    if (sharedPanelVirtualAttributes.some(g => g.fullName === fullName)) {
        showNotification(`Function already assigned: ${fullName}`, 'warning', 3000);
        return;
    }
    const { attributes, renamed } = _vaDeriveAttributes(fn);
    if (!attributes.length) {
        showNotification(
            `${fn.name} exposes no usable return column — cannot create virtual attributes`,
            'warning', 4000
        );
        return;
    }
    sharedPanelVirtualAttributes.push({
        catalog: _vaSelCatalog,
        schema: _vaSelSchema,
        function: fn.name,
        fullName,
        description: String(fn.comment || '').trim() || null,
        returns_table: Boolean(fn.returns_table),
        attributes,
    });
    markPanelDirty();
    renderSharedEntityVirtualAttributes(false);
    closeVirtualAttributeSelectorModal();
    if (renamed.length) {
        showNotification(
            `Name already used on this entity, renamed: ${renamed.join(', ')}`,
            'warning', 5000
        );
    } else {
        showNotification(
            `${attributes.length} virtual attribute${attributes.length === 1 ? '' : 's'} added from ${fn.name}`,
            'success', 3000
        );
    }
}

function closeVirtualAttributeSelectorModal() {
    _ucClosePickerModal('virtualAttributeSelectorModal');
}

// =====================================================
// CROSS-PROJECT BRIDGES
// =====================================================

/**
 * Render the bridges section in the entity form
 */
function renderSharedEntityBridges(viewOnly = false) {
    const container = panelGetById('sharedEntityBridgesContent');
    if (!container) return;

    if (sharedPanelBridges.length > 0) {
        container.innerHTML = sharedPanelBridges.map((bridge, idx) => `
            <div class="d-flex align-items-center gap-2 ${idx > 0 ? 'mt-1 pt-1 border-top' : ''}">
                <div class="flex-grow-1">
                    <small class="d-block">
                        <i class="bi bi-signpost-2 text-info"></i>
                        <span class="fw-semibold ms-1">${escapeHtml(bridge.target_class_name || '')}</span>
                    </small>
                    <small class="text-muted d-block ms-3">
                        <i class="bi bi-box me-1"></i>${escapeHtml(bridge.target_domain || bridge.target_project || '')}
                        ${bridge.label ? ` &mdash; ${escapeHtml(bridge.label)}` : ''}
                    </small>
                </div>
                ${!viewOnly ? `<button type="button" class="btn btn-sm btn-outline-danger py-0 px-1" onclick="removeSharedEntityBridge(${idx})" title="Remove bridge"><i class="bi bi-x"></i></button>` : ''}
            </div>
        `).join('');
    } else {
        container.innerHTML = '<small class="text-muted">No bridges to other domains</small>';
    }
}

/**
 * Remove a bridge by index
 */
function removeSharedEntityBridge(index) {
    sharedPanelBridges.splice(index, 1);
    markPanelDirty();
    renderSharedEntityBridges(false);
}

/**
 * Open the bridge selector modal (two-step: domain then class)
 */
async function openBridgeSelectorModal() {
    const modalId = 'bridgeSelectorModal';

    const existing = document.getElementById(modalId);
    if (existing) existing.remove();

    const modalHtml = `
        <div class="modal fade" id="${modalId}" tabindex="-1" data-bs-backdrop="static">
            <div class="modal-dialog modal-lg modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">
                            <i class="bi bi-signpost-2 me-2"></i>Add Bridge to Another Domain
                        </h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div id="bridgeSelectorLoading" class="text-center py-4">
                            <div class="spinner-border spinner-border-sm text-primary" role="status"></div>
                            <span class="ms-2">Loading registry domains...</span>
                        </div>
                        <div id="bridgeSelectorContent" style="display: none;">
                            <!-- Step 1: Project selection -->
                            <div id="bridgeStepDomain">
                                <label class="form-label fw-semibold">Select a domain</label>
                                <div id="bridgeDomainList" class="list-group" style="max-height: 300px; overflow-y: auto;"></div>
                            </div>
                            <!-- Step 2: Class selection (hidden initially) -->
                            <div id="bridgeStepClass" style="display: none;">
                                <div class="d-flex align-items-center gap-2 mb-3">
                                    <button type="button" class="btn btn-sm btn-outline-secondary" onclick="_bridgeBackToDomains()">
                                        <i class="bi bi-arrow-left"></i>
                                    </button>
                                    <span class="fw-semibold" id="bridgeSelectedDomainName"></span>
                                </div>
                                <label class="form-label fw-semibold">Select a target entity</label>
                                <input type="text" class="form-control form-control-sm mb-2" id="bridgeClassSearch" placeholder="Search classes..." oninput="_filterBridgeClasses()">
                                <div id="bridgeClassList" class="list-group" style="max-height: 280px; overflow-y: auto;"></div>
                            </div>
                            <!-- Step 3: Label (hidden initially) -->
                            <div id="bridgeStepLabel" style="display: none;">
                                <div class="d-flex align-items-center gap-2 mb-3">
                                    <button type="button" class="btn btn-sm btn-outline-secondary" onclick="_bridgeBackToClasses()">
                                        <i class="bi bi-arrow-left"></i>
                                    </button>
                                    <span id="bridgeSummary" class="fw-semibold"></span>
                                </div>
                                <div class="mb-3">
                                    <label for="bridgeLabelInput" class="form-label">Label <small class="text-muted">(optional)</small></label>
                                    <input type="text" class="form-control form-control-sm" id="bridgeLabelInput" placeholder="e.g. Same as Client in DomainB">
                                </div>
                                <button type="button" class="btn btn-primary w-100" onclick="_bridgeConfirm()">
                                    <i class="bi bi-check-lg me-1"></i>Add Bridge
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;

    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById(modalId));
    modal.show();

    try {
        const resp = await fetch('/ontology/bridges/domains', { credentials: 'same-origin' });
        const data = await resp.json();
        document.getElementById('bridgeSelectorLoading').style.display = 'none';
        document.getElementById('bridgeSelectorContent').style.display = '';

        const list = document.getElementById('bridgeDomainList');
        const bridgeRows = data.domains || data.projects || [];
        if (!data.success || !bridgeRows.length) {
            list.innerHTML = '<div class="text-muted p-3 text-center">No other domains found in the registry</div>';
            return;
        }

        list.innerHTML = bridgeRows.map(p => `
            <button type="button" class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                    onclick="_bridgeSelectDomain('${escapeHtml(p.name)}')">
                <i class="bi bi-box text-primary"></i>
                <div class="flex-grow-1">
                    <div class="fw-semibold">${escapeHtml(p.name)}</div>
                    ${p.description ? `<small class="text-muted">${escapeHtml(p.description)}</small>` : ''}
                </div>
                <i class="bi bi-chevron-right text-muted"></i>
            </button>
        `).join('');
    } catch (err) {
        console.error('[Bridges] Error loading domains:', err);
        document.getElementById('bridgeSelectorLoading').innerHTML =
            '<div class="text-danger"><i class="bi bi-exclamation-triangle"></i> Failed to load domains</div>';
    }
}

let _bridgePendingDomain = '';
let _bridgePendingClass = null;
let _bridgeAllClasses = [];

async function _bridgeSelectDomain(domainName) {
    _bridgePendingDomain = domainName;
    document.getElementById('bridgeStepDomain').style.display = 'none';
    document.getElementById('bridgeStepClass').style.display = '';
    document.getElementById('bridgeSelectedDomainName').textContent = domainName;

    const list = document.getElementById('bridgeClassList');
    list.innerHTML = '<div class="text-center py-3"><div class="spinner-border spinner-border-sm text-primary"></div> Loading classes...</div>';

    try {
        const resp = await fetch(`/ontology/bridges/domains/${encodeURIComponent(domainName)}/classes`, { credentials: 'same-origin' });
        const data = await resp.json();
        _bridgeAllClasses = (data.success && data.classes) ? data.classes : [];

        if (!_bridgeAllClasses.length) {
            list.innerHTML = '<div class="text-muted p-3 text-center">No classes found in this domain</div>';
            return;
        }
        _renderBridgeClassList(_bridgeAllClasses);
    } catch (err) {
        console.error('[Bridges] Error loading classes:', err);
        list.innerHTML = '<div class="text-danger p-2"><i class="bi bi-exclamation-triangle"></i> Failed to load classes</div>';
    }
}

function _renderBridgeClassList(classes) {
    const list = document.getElementById('bridgeClassList');
    list.innerHTML = classes.map(c => `
        <button type="button" class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                onclick='_bridgeSelectClass(${JSON.stringify(c).replace(/'/g, "&#39;")})'>
            <span>${c.emoji || '📦'}</span>
            <div class="flex-grow-1">
                <div class="fw-semibold">${escapeHtml(c.name)}</div>
                ${c.description ? `<small class="text-muted">${escapeHtml(c.description.substring(0, 80))}</small>` : ''}
            </div>
            <i class="bi bi-chevron-right text-muted"></i>
        </button>
    `).join('');
}

function _filterBridgeClasses() {
    const q = (document.getElementById('bridgeClassSearch')?.value || '').toLowerCase();
    const filtered = _bridgeAllClasses.filter(c =>
        (c.name || '').toLowerCase().includes(q) ||
        (c.label || '').toLowerCase().includes(q) ||
        (c.description || '').toLowerCase().includes(q)
    );
    _renderBridgeClassList(filtered);
}

function _bridgeSelectClass(cls) {
    _bridgePendingClass = cls;
    document.getElementById('bridgeStepClass').style.display = 'none';
    document.getElementById('bridgeStepLabel').style.display = '';
    document.getElementById('bridgeSummary').innerHTML =
        `<i class="bi bi-box me-1"></i>${escapeHtml(_bridgePendingDomain)} <i class="bi bi-arrow-right mx-1"></i> ${cls.emoji || '📦'} ${escapeHtml(cls.name)}`;
}

function _bridgeBackToDomains() {
    document.getElementById('bridgeStepClass').style.display = 'none';
    document.getElementById('bridgeStepDomain').style.display = '';
    _bridgePendingDomain = '';
    _bridgePendingClass = null;
    _bridgeAllClasses = [];
}

function _bridgeBackToClasses() {
    document.getElementById('bridgeStepLabel').style.display = 'none';
    document.getElementById('bridgeStepClass').style.display = '';
    _bridgePendingClass = null;
}

function _bridgeConfirm() {
    if (!_bridgePendingDomain || !_bridgePendingClass) return;

    const duplicate = sharedPanelBridges.some(b =>
        (b.target_domain || b.target_project) === _bridgePendingDomain && b.target_class_uri === _bridgePendingClass.uri
    );
    if (duplicate) {
        showNotification('This bridge already exists', 'warning');
        return;
    }

    const label = (document.getElementById('bridgeLabelInput')?.value || '').trim();

    sharedPanelBridges.push({
        target_domain: _bridgePendingDomain,
        target_class_uri: _bridgePendingClass.uri,
        target_class_name: _bridgePendingClass.name,
        label: label
    });

    markPanelDirty();
    renderSharedEntityBridges(false);
    closeBridgeSelectorModal();
    showNotification(`Bridge added to ${_bridgePendingClass.name} in ${_bridgePendingDomain}`, 'success', 3000);
}

function closeBridgeSelectorModal() {
    const modal = document.getElementById('bridgeSelectorModal');
    if (modal) {
        const bsModal = bootstrap.Modal.getInstance(modal);
        if (bsModal) bsModal.hide();
        modal.addEventListener('hidden.bs.modal', () => modal.remove(), { once: true });
    }
    _bridgePendingDomain = '';
    _bridgePendingClass = null;
    _bridgeAllClasses = [];
}


/**
 * Open the dashboard selector modal
 */
async function openDashboardSelectorModal() {
    const modalId = 'dashboardSelectorModal';
    
    // Remove existing modal if any
    const existing = document.getElementById(modalId);
    if (existing) existing.remove();
    
    // Create modal HTML
    const modalHtml = `
        <div class="modal fade" id="${modalId}" tabindex="-1" data-bs-backdrop="static">
            <div class="modal-dialog modal-lg modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">
                            <i class="bi bi-speedometer2 me-2"></i>Select Dashboard
                        </h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div id="dashboardSelectorLoading" class="text-center py-4">
                            <div class="ob-loading-spinner">
                                <svg class="ob-spinner-svg" viewBox="0 0 80 80" fill="none">
                                    <g class="ob-ring">
                                        <g stroke="#CBD5E1" stroke-width="1.2" opacity="0.5">
                                            <line x1="40" y1="10" x2="61" y2="19"/><line x1="61" y1="19" x2="70" y2="40"/>
                                            <line x1="70" y1="40" x2="61" y2="61"/><line x1="61" y1="61" x2="40" y2="70"/>
                                            <line x1="40" y1="70" x2="19" y2="61"/><line x1="19" y1="61" x2="10" y2="40"/>
                                            <line x1="10" y1="40" x2="19" y2="19"/><line x1="19" y1="19" x2="40" y2="10"/>
                                        </g>
                                        <circle cx="40" cy="10" r="5" fill="#FF3621"/><circle cx="61" cy="19" r="5" fill="#6366F1"/>
                                        <circle cx="70" cy="40" r="5" fill="#4ECDC4"/><circle cx="61" cy="61" r="5" fill="#F59E0B"/>
                                        <circle cx="40" cy="70" r="5" fill="#FF3621"/><circle cx="19" cy="61" r="5" fill="#6366F1"/>
                                        <circle cx="10" cy="40" r="5" fill="#4ECDC4"/><circle cx="19" cy="19" r="5" fill="#F59E0B"/>
                                    </g>
                                    <g transform="translate(40,40)">
                                        <g class="ob-center">
                                            <path d="M0-12 L10-6 L0 0 L-10-6Z" fill="#FF3621"/>
                                            <path d="M0-5 L10 1 L0 7 L-10 1Z" fill="#FF3621" opacity="0.85"/>
                                            <path d="M0 2 L10 8 L0 14 L-10 8Z" fill="#FF3621" opacity="0.7"/>
                                        </g>
                                    </g>
                                </svg>
                                <span class="ob-spinner-label">Loading dashboards from Databricks...</span>
                            </div>
                        </div>
                        <div id="dashboardSelectorContent" style="display: none;">
                            <div class="mb-3">
                                <input type="text" class="form-control" id="dashboardSearchInput" placeholder="Search dashboards...">
                            </div>
                            <div id="dashboardSelectorList" class="list-group" style="max-height: 400px; overflow-y: auto;">
                                <!-- Dashboard list will be populated here -->
                            </div>
                            <div id="dashboardSelectorEmpty" class="text-center py-4 text-muted" style="display: none;">
                                <i class="bi bi-inbox fs-1"></i>
                                <p class="mt-2">No dashboards found</p>
                            </div>
                            <div id="dashboardSelectorError" class="alert alert-warning mt-3" style="display: none;">
                                <i class="bi bi-exclamation-triangle me-2"></i>
                                <span id="dashboardSelectorErrorMsg"></span>
                            </div>
                        </div>
                        <div class="mt-3 pt-3 border-top">
                            <label class="form-label small text-muted">Or enter dashboard URL manually:</label>
                            <div class="input-group">
                                <input type="url" class="form-control form-control-sm" id="dashboardManualUrl" placeholder="https://...">
                                <button type="button" class="btn btn-outline-primary btn-sm" onclick="applyManualDashboardUrl()">Apply</button>
                            </div>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    // Add modal to page
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    
    const modalEl = document.getElementById(modalId);
    const modal = new bootstrap.Modal(modalEl);
    
    // Clean up on close
    modalEl.addEventListener('hidden.bs.modal', () => {
        setTimeout(() => modalEl.remove(), 100);
    });
    
    // Show modal
    modal.show();
    
    // Load dashboards
    await loadDashboardsForSelector(modal);
}

/**
 * Load dashboards from the server and populate the selector
 */
async function loadDashboardsForSelector(modal) {
    const loadingEl = document.getElementById('dashboardSelectorLoading');
    const contentEl = document.getElementById('dashboardSelectorContent');
    const listEl = document.getElementById('dashboardSelectorList');
    const emptyEl = document.getElementById('dashboardSelectorEmpty');
    const errorEl = document.getElementById('dashboardSelectorError');
    const errorMsgEl = document.getElementById('dashboardSelectorErrorMsg');
    const searchInput = document.getElementById('dashboardSearchInput');
    
    try {
        const response = await fetch('/ontology/dashboards/list', { credentials: 'same-origin' });
        const data = await response.json();
        
        loadingEl.style.display = 'none';
        contentEl.style.display = 'block';
        
        if (!data.success) {
            errorEl.style.display = 'block';
            errorMsgEl.textContent = data.message || 'Failed to load dashboards';
            return;
        }
        
        const dashboards = data.dashboards || [];
        
        if (dashboards.length === 0) {
            emptyEl.style.display = 'block';
            return;
        }
        
        // Store dashboards for filtering
        window._dashboardSelectorData = dashboards;
        
        // Render dashboards
        renderDashboardList(dashboards, listEl, modal);
        
        // Setup search
        searchInput.addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase();
            const filtered = dashboards.filter(d => 
                d.name.toLowerCase().includes(query) || 
                (d.path && d.path.toLowerCase().includes(query))
            );
            renderDashboardList(filtered, listEl, modal);
            emptyEl.style.display = filtered.length === 0 ? 'block' : 'none';
        });
        
    } catch (error) {
        console.error('[Dashboard] Error loading dashboards:', error);
        loadingEl.style.display = 'none';
        contentEl.style.display = 'block';
        errorEl.style.display = 'block';
        errorMsgEl.textContent = 'Error connecting to Databricks: ' + error.message;
    }
}

/**
 * Render the dashboard list
 */
function renderDashboardList(dashboards, container, modal) {
    container.innerHTML = dashboards.map(dash => `
        <a href="#" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center" 
           onclick="selectDashboard('${dash.url}', '${dash.name.replace(/'/g, "\\'")}', '${dash.id || ''}'); return false;">
            <div>
                <div class="fw-semibold">
                    <i class="bi bi-speedometer2 me-2 text-primary"></i>${escapeHtml(dash.name)}
                </div>
                <small class="text-muted">${escapeHtml(dash.path || dash.type || '')}</small>
            </div>
            <span class="badge bg-${dash.type === 'lakeview' ? 'primary' : 'secondary'}">${dash.type === 'lakeview' ? 'AI/BI' : 'Legacy'}</span>
        </a>
    `).join('');
}

/**
 * Select a dashboard from the list
 */
async function selectDashboard(url, name, dashboardId) {
    sharedPanelDashboardUrl = url;
    sharedPanelDashboardParams = {};
    markPanelDirty();
    
    // If we have a dashboard ID, fetch parameters and show mapping UI
    if (dashboardId && dashboardId.trim() !== '') {
        await showDashboardParameterMapping(dashboardId, name);
    } else {
        // No dashboard ID (manual URL or legacy dashboard) - just close modal
        closeDashboardSelectorModal();
        renderSharedEntityDashboard(false);
        showNotification(`Dashboard "${name}" assigned`, 'success', 2000);
    }
}

/**
 * Close the dashboard selector modal
 */
function closeDashboardSelectorModal() {
    const modalEl = document.getElementById('dashboardSelectorModal');
    if (modalEl) {
        const modal = bootstrap.Modal.getInstance(modalEl);
        if (modal) modal.hide();
    }
}

/**
 * Show dashboard parameter mapping UI
 */
async function showDashboardParameterMapping(dashboardId, dashboardName) {
    const loadingEl = document.getElementById('dashboardSelectorLoading');
    const contentEl = document.getElementById('dashboardSelectorContent');
    
    // Show loading while fetching parameters
    if (loadingEl) loadingEl.style.display = 'block';
    if (contentEl) contentEl.style.display = 'none';
    
    try {
        const response = await fetch(`/ontology/dashboards/${encodeURIComponent(dashboardId)}/parameters`);
        const data = await response.json();
        
        console.log('[Dashboard] Full API response:', data);
        if (data.debug) {
            console.log('[Dashboard] Raw datasets:', JSON.stringify(data.debug.datasets, null, 2));
            console.log('[Dashboard] Raw pages:', JSON.stringify(data.debug.pages, null, 2));
        }
        
        if (!data.success) {
            // Continue without parameters
            closeDashboardSelectorModal();
            renderSharedEntityDashboard(false);
            showNotification(`Dashboard "${dashboardName}" assigned (no parameters found)`, 'success', 2000);
            return;
        }
        
        const parameters = data.parameters || [];
        console.log('[Dashboard] Extracted parameters:', parameters);
        
        if (parameters.length === 0) {
            // No parameters - just assign the dashboard
            closeDashboardSelectorModal();
            renderSharedEntityDashboard(false);
            showNotification(`Dashboard "${dashboardName}" assigned`, 'success', 2000);
            return;
        }
        
        // Update embed URL if provided
        if (data.embed_url) {
            sharedPanelDashboardUrl = data.embed_url;
        }
        
        // Show parameter mapping UI
        showParameterMappingUI(parameters, dashboardName);
        
    } catch (error) {
        console.error('[Dashboard] Error fetching parameters:', error);
        closeDashboardSelectorModal();
        renderSharedEntityDashboard(false);
        showNotification(`Dashboard "${dashboardName}" assigned`, 'success', 2000);
    }
}

/**
 * Show the parameter mapping UI in the modal
 */
function showParameterMappingUI(parameters, dashboardName) {
    const loadingEl = document.getElementById('dashboardSelectorLoading');
    const contentEl = document.getElementById('dashboardSelectorContent');
    
    if (loadingEl) loadingEl.style.display = 'none';
    if (contentEl) contentEl.style.display = 'block';
    
    // Get available attributes for mapping (ID + own attributes)
    const availableAttrs = [
        { name: 'ID', value: '__ID__', description: 'Entity ID' },
        ...sharedPanelOwnAttributes.filter(a => a.name).map(a => ({
            name: a.name,
            value: a.name,
            description: 'Attribute'
        }))
    ];
    
    // Build attribute options HTML
    const attrOptionsHtml = `
        <option value="">-- Not Mapped --</option>
        ${availableAttrs.map(attr => `
            <option value="${attr.value}">${attr.name} (${attr.description})</option>
        `).join('')}
    `;
    
    // Build parameter mapping HTML
    const paramMappingHtml = parameters.map(param => `
        <div class="d-flex align-items-center gap-2 mb-2 p-2 border rounded bg-light">
            <div class="flex-grow-1">
                <div class="fw-semibold small">${escapeHtml(param.name)}</div>
                <small class="text-muted">${param.type || 'parameter'}${param.dataset ? ` (${escapeHtml(param.dataset)})` : ''}</small>
            </div>
            <i class="bi bi-arrow-right text-muted"></i>
            <select class="form-select form-select-sm" style="width: 180px;" 
                    data-param-name="${escapeHtml(param.name)}" 
                    data-param-keyword="${escapeHtml(param.keyword || param.name)}"
                    data-dataset-id="${escapeHtml(param.datasetId || '')}"
                    data-page-id="${escapeHtml(param.pageId || '')}"
                    data-widget-id="${escapeHtml(param.widgetId || '')}"
                    onchange="updateDashboardParamMapping(this)">
                ${attrOptionsHtml}
            </select>
        </div>
    `).join('');
    
    // Update modal content
    contentEl.innerHTML = `
        <div class="alert alert-info mb-3">
            <i class="bi bi-info-circle me-2"></i>
            <strong>${escapeHtml(dashboardName)}</strong> has ${parameters.length} parameter(s).
            Map them to entity attributes to filter the dashboard data.
        </div>
        
        <h6 class="mb-3"><i class="bi bi-link-45deg me-2"></i>Parameter Mapping</h6>
        
        <div class="mb-3">
            ${paramMappingHtml}
        </div>
        
        ${parameters.length === 0 ? '<p class="text-muted small">No parameters found in this dashboard.</p>' : ''}
        
        <div class="d-flex justify-content-end gap-2 mt-4 pt-3 border-top">
            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
            <button type="button" class="btn btn-primary" onclick="applyDashboardWithMappings('${escapeHtml(dashboardName)}')">
                <i class="bi bi-check-lg me-1"></i>Apply
            </button>
        </div>
    `;
}

/**
 * Update dashboard parameter mapping when user changes selection
 */
function updateDashboardParamMapping(selectElement) {
    const paramName = selectElement.dataset.paramName;
    const paramKeyword = selectElement.dataset.paramKeyword || paramName;
    const datasetId = selectElement.dataset.datasetId || '';
    const pageId = selectElement.dataset.pageId || '';
    const widgetId = selectElement.dataset.widgetId || '';
    const attrValue = selectElement.value;
    
    if (attrValue) {
        // Store the attribute mapping and IDs for URL building
        // Embed URL format: f_{pageId}~{widgetId}=value
        sharedPanelDashboardParams[paramKeyword] = {
            attribute: attrValue,
            datasetId: datasetId,
            pageId: pageId,
            widgetId: widgetId
        };
    } else {
        delete sharedPanelDashboardParams[paramKeyword];
    }
    
    console.log('[Dashboard] Parameter mappings:', sharedPanelDashboardParams);
}

/**
 * Apply dashboard mapping with parameter mappings
 */
function applyDashboardWithMappings(dashboardName) {
    closeDashboardSelectorModal();
    markPanelDirty();
    renderSharedEntityDashboard(false);

    const mappingCount = Object.keys(sharedPanelDashboardParams).length;
    if (mappingCount > 0) {
        showNotification(`Dashboard "${dashboardName}" assigned with ${mappingCount} parameter mapping(s)`, 'success', 3000);
    } else {
        showNotification(`Dashboard "${dashboardName}" assigned`, 'success', 2000);
    }
}

/**
 * Apply a manually entered dashboard URL
 */
function applyManualDashboardUrl() {
    const input = document.getElementById('dashboardManualUrl');
    const url = input?.value.trim();
    
    if (!url) {
        showNotification('Please enter a dashboard URL', 'warning');
        return;
    }
    
    // Basic URL validation
    try {
        new URL(url);
    } catch {
        showNotification('Please enter a valid URL', 'warning');
        return;
    }
    
    selectDashboard(url, 'Custom Dashboard');
}

async function saveSharedEntity(options = {}) {
    const name = panelGetById('sharedEntityName')?.value.trim();
    const labelRaw = panelGetById('sharedEntityLabel')?.value.trim();
    const label = labelRaw || name;
    const icon = panelGetById('sharedEntityIcon')?.value.trim();
    const parent = panelGetById('sharedEntityParent')?.value;
    const description = panelGetById('sharedEntityDescription')?.value.trim();
    
    // Get constraint values from the form
    const disjointWithSelect = panelGetById('sharedEntityDisjointWith');
    const equivalentToSelect = panelGetById('sharedEntityEquivalentTo');
    const disjointWith = disjointWithSelect ? Array.from(disjointWithSelect.selectedOptions).map(opt => opt.value) : [];
    const equivalentTo = equivalentToSelect ? Array.from(equivalentToSelect.selectedOptions).map(opt => opt.value) : [];
    
    if (!name) { showNotification('Please enter an entity name', 'warning'); return; }

    // Same stale-index guard as saveSharedRelationship: skip when only non-name fields changed.
    const duplicateEntity = (name !== sharedPanelOriginalName || sharedPanelEditIndex < 0)
        && (OntologyState.config.classes || []).some((c, i) => c.name === name && i !== sharedPanelEditIndex);
    if (duplicateEntity) { showNotification(`An entity named "${name}" already exists`, 'warning'); return; }
    
    const validAttributes = sharedPanelOwnAttributes.filter(a => a.name?.trim()).map(a => ({ name: a.name.trim(), localName: a.name.trim() }));
    
    console.log('[SharedPanel] Saving - sharedPanelDashboardParams:', JSON.stringify(sharedPanelDashboardParams));
    
    const classData = { 
        name, 
        localName: name, 
        label, 
        emoji: icon, 
        parent: parent || undefined, 
        description, 
        comment: description, 
        dataProperties: validAttributes,
        dashboard: sharedPanelDashboardUrl || undefined,
        dashboardParams: Object.keys(sharedPanelDashboardParams).length > 0 ? sharedPanelDashboardParams : undefined,
        bridges: sharedPanelBridges.length > 0 ? sharedPanelBridges : undefined,
        dataset: sharedPanelDataset || undefined,
        actions: sharedPanelActions.length > 0 ? sharedPanelActions : undefined,
        virtualAttributes: sharedPanelVirtualAttributes.length > 0 ? sharedPanelVirtualAttributes : undefined
    };
    
    console.log('[SharedPanel] Saving - classData.dashboardParams:', JSON.stringify(classData.dashboardParams));
    
    if (sharedPanelEditIndex >= 0) {
        const existing = OntologyState.config.classes[sharedPanelEditIndex] || {};
        const oldName = existing.name;
        // Preserve server-assigned URI so the backend prune doesn't orphan mappings.
        if (existing.uri) classData.uri = existing.uri;
        OntologyState.config.classes[sharedPanelEditIndex] = classData;
        if (oldName !== name) {
            OntologyState.config.classes.forEach(c => { if (c.parent === oldName) c.parent = name; });
            OntologyState.config.properties.forEach(p => { if (p.domain === oldName) p.domain = name; if (p.range === oldName) p.range = name; });
        }
        showNotification('Entity updated', 'success', 2000);
    } else {
        OntologyState.config.classes.push(classData);
        showNotification('Entity added', 'success', 2000);
    }
    
    await window.saveConfigToSession({ keepalive: options.keepalive });
    
    // Save entity constraints to the ONLY storage location: session_data/ontology/constraints
    await saveEntityConstraintsToServer(name, disjointWith, equivalentTo);
    
    await autoGenerateOwl();
    if (sharedPanelOnSaveCallback) sharedPanelOnSaveCallback();
    if (typeof updateClassesList === 'function') updateClassesList();
    
    // Refresh ConstraintsModule if loaded
    if (typeof ConstraintsModule !== 'undefined' && ConstraintsModule.loadConstraints) {
        ConstraintsModule.loadConstraints();
    }
    
    closeSharedPanel();
}

/**
 * Save entity constraints to server-side storage (session_data/ontology/constraints)
 * This is the ONLY place constraints are stored
 */
async function saveEntityConstraintsToServer(className, disjointWith, equivalentTo) {
    try {
        // First, get existing constraints to find indices
        const listResponse = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
        const listData = await listResponse.json();
        let existingConstraints = listData.success ? (listData.constraints || []) : [];
        
        // Handle disjointWith constraints
        const existingDisjointIdx = existingConstraints.findIndex(c => c.className === className && c.type === 'disjointWith');
        if (disjointWith.length > 0) {
            // Add or update disjointWith constraint
            const constraint = {
                type: 'disjointWith',
                className: className,
                disjointClasses: disjointWith
            };
            await fetch('/ontology/constraints/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ constraint, index: existingDisjointIdx >= 0 ? existingDisjointIdx : -1 }),
                credentials: 'same-origin'
            });
            // Refresh list
            const refreshResponse = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
            const refreshData = await refreshResponse.json();
            existingConstraints = refreshData.constraints || [];
        } else if (existingDisjointIdx >= 0) {
            // Remove constraint if it exists but no classes are selected
            await fetch('/ontology/constraints/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ index: existingDisjointIdx }),
                credentials: 'same-origin'
            });
            // Refresh list
            const refreshResponse = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
            const refreshData = await refreshResponse.json();
            existingConstraints = refreshData.constraints || [];
        }
        
        // Handle equivalentTo constraints
        const existingEquivalentIdx = existingConstraints.findIndex(c => c.className === className && c.type === 'equivalentTo');
        if (equivalentTo.length > 0) {
            // Add or update equivalentTo constraint
            const constraint = {
                type: 'equivalentTo',
                className: className,
                equivalentClasses: equivalentTo
            };
            await fetch('/ontology/constraints/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ constraint, index: existingEquivalentIdx >= 0 ? existingEquivalentIdx : -1 }),
                credentials: 'same-origin'
            });
        } else if (existingEquivalentIdx >= 0) {
            // Remove constraint if it exists but no classes are selected
            await fetch('/ontology/constraints/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ index: existingEquivalentIdx }),
                credentials: 'same-origin'
            });
        }
    } catch (error) {
        console.error('[SharedPanel] Error saving entity constraints to server:', error);
    }
}

// =====================================================
// RELATIONSHIP PANEL FUNCTIONS
// =====================================================

async function openRelationshipPanel(options = {}) {
    if (!_canEditOntologyPanel()) {
        console.log('[SharedPanel] openRelationshipPanel suppressed (read-only / viewer)');
        return;
    }
    await checkDirtyBeforeSwitch();
    console.log('[SharedPanel] openRelationshipPanel called');
    sharedPanelEditType = 'relationship';
    sharedPanelEditIndex = -1;
    sharedPanelOriginalName = null;
    sharedPanelViewOnly = false;
    sharedPanelOnSaveCallback = options.onSave || null;
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) {
        console.log('[SharedPanel] Panel not found after openSharedPanel');
        return;
    }
    
    panel.querySelector('#sharedPanelTitle').innerHTML = '<i class="bi bi-plus-circle"></i> <span id="sharedPanelItemName">Add Relationship</span>';
    await renderRelationshipForm(panel, null);
    attachDirtyTracking();
}

async function openRelationshipPanelForEdit(idx, options = {}) {
    // Mirror ``openEntityPanelForEdit`` — viewers/inactive-version
    // readers fall through to the read-only panel so add/remove
    // controls are never rendered.
    if (!_canEditOntologyPanel()) {
        return openRelationshipPanelForView(idx, options);
    }
    await checkDirtyBeforeSwitch();
    console.log('[SharedPanel] openRelationshipPanelForEdit called, idx:', idx);
    const prop = OntologyState.config.properties[idx];
    if (!prop) {
        console.log('[SharedPanel] Property not found at index:', idx);
        return;
    }
    
    sharedPanelEditType = 'relationship';
    sharedPanelEditIndex = idx;
    sharedPanelOriginalName = prop.name;
    sharedPanelViewOnly = false;
    sharedPanelOnSaveCallback = options.onSave || null;
    
    // Jump straight to a specific tab (e.g. Designer context-menu shortcuts)
    if (options.activeTab) _relPanelActiveTab = options.activeTab;
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) {
        console.log('[SharedPanel] Panel not found after openSharedPanel');
        return;
    }
    
    panel.querySelector('#sharedPanelTitle').innerHTML = `<i class="bi bi-pencil"></i> <span id="sharedPanelItemName">${prop.name}</span>`;
    await renderRelationshipForm(panel, prop);
    attachDirtyTracking();
}

async function openRelationshipPanelForView(idx, options = {}) {
    await checkDirtyBeforeSwitch();
    const prop = OntologyState.config.properties[idx];
    if (!prop) return;
    
    sharedPanelEditType = 'relationship';
    sharedPanelEditIndex = idx;
    sharedPanelViewOnly = true;
    sharedPanelOnSaveCallback = null;
    
    // Jump straight to a specific tab (e.g. Designer context-menu shortcuts)
    if (options.activeTab) _relPanelActiveTab = options.activeTab;
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) return;
    
    panel.querySelector('#sharedPanelTitle').innerHTML = `<i class="bi bi-eye"></i> <span id="sharedPanelItemName">${prop.name}</span>`;
    await renderRelationshipForm(panel, prop, true);
}

/**
 * One clickable card of the graphical direction picker: the two endpoint chips
 * with the arrow between them, drawn the way the canvas will draw the link.
 * @param {string} value - 'forward' | 'reverse'
 * @param {string} icon - Bootstrap Icons class for the arrow
 * @param {boolean} viewOnly - render as a non-interactive preview
 * @returns {string} HTML
 */
function _relDirectionOption(value, icon, viewOnly) {
    return `
        <button type="button" class="rel-direction-option" data-direction="${value}"
                role="radio" aria-checked="false" ${viewOnly ? 'disabled' : ''}>
            <span class="rel-direction-node" data-rel-node="domain">Source</span>
            <span class="rel-direction-arrow">
                <span class="rel-direction-name" data-rel-node="name"></span>
                <i class="bi ${icon}"></i>
            </span>
            <span class="rel-direction-node" data-rel-node="range">Target</span>
        </button>
    `;
}

/**
 * Select *direction* in the picker and mirror it into the hidden input that
 * saveSharedRelationship() reads.
 * @param {string} direction - 'forward' | 'reverse'
 */
function setSharedRelDirection(direction) {
    const value = direction === 'reverse' ? 'reverse' : 'forward';
    const input = panelGetById('sharedRelDirection');
    if (input) input.value = value;
    panelGetById('sharedRelDirectionPicker')?.querySelectorAll('.rel-direction-option')
        .forEach(option => {
            const selected = option.dataset.direction === value;
            option.classList.toggle('selected', selected);
            option.setAttribute('aria-checked', String(selected));
        });
}

/**
 * Keep the picker's endpoint chips in sync with the Source / Target / Name
 * fields, so both cards always read as the relationship being edited.
 */
function refreshSharedRelDirectionLabels() {
    const picker = panelGetById('sharedRelDirectionPicker');
    if (!picker) return;
    const text = {
        domain: panelGetById('sharedRelDomain')?.value || 'Source',
        range: panelGetById('sharedRelRange')?.value || 'Target',
        name: panelGetById('sharedRelName')?.value.trim() || ''
    };
    Object.entries(text).forEach(([role, label]) => {
        picker.querySelectorAll(`[data-rel-node="${role}"]`).forEach(el => {
            el.textContent = label;
        });
    });
}

async function renderRelationshipForm(panel, prop, viewOnly = false) {
    const body = panel.querySelector('#sharedPanelBody');
    const classOptions = OntologyState.config.classes.map(c => `<option value="${c.name}">${c.emoji || '📦'} ${c.name}</option>`).join('');
    const disabled = viewOnly ? 'disabled' : '';
    
    // Load constraints from server (the ONLY source of truth for constraints)
    let minCard = '';
    let maxCard = '';
    let isFunctional = false;
    let isInverseFunctional = false;
    let isSymmetric = false;
    let isTransitive = false;
    
    if (prop?.name) {
        try {
            const response = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
            const data = await response.json();
            if (data.success && data.constraints) {
                const propConstraints = data.constraints.filter(c => c.property === prop.name);
                propConstraints.forEach(c => {
                    if (c.type === 'minCardinality') minCard = c.cardinalityValue ?? '';
                    if (c.type === 'maxCardinality') maxCard = c.cardinalityValue ?? '';
                    if (c.type === 'functional') isFunctional = true;
                    if (c.type === 'inverseFunctional') isInverseFunctional = true;
                    if (c.type === 'symmetric') isSymmetric = true;
                    if (c.type === 'transitive') isTransitive = true;
                });
            }
        } catch (error) {
            console.error('[SharedPanel] Error loading constraints:', error);
        }
    }
    
    const _relTabValid = ['details', 'constraints'];
    const _rTab = _relTabValid.includes(_relPanelActiveTab) ? _relPanelActiveTab : 'details';
    body.innerHTML = `
        <div id="sharedRelAssignmentLink"></div>
        <form id="sharedRelationshipForm">
            <ul class="form-tabs-nav">
                <li><a class="form-tab-link ${_rTab === 'details' ? 'active' : ''}" data-form-tab="details" href="#" onclick="event.preventDefault(); switchFormTab(this)"><i class="bi bi-info-circle me-1"></i>Details</a></li>
                <li><a class="form-tab-link ${_rTab === 'constraints' ? 'active' : ''}" data-form-tab="constraints" href="#" onclick="event.preventDefault(); switchFormTab(this)"><i class="bi bi-sliders me-1"></i>Constraints</a></li>
            </ul>

            <div class="form-tab-pane ${_rTab === 'details' ? 'active' : ''}" data-form-tab-content="details">
                <div class="mb-3">
                    <label for="sharedRelName" class="form-label">Name <span class="text-danger">*</span></label>
                    <input type="text" class="form-control form-control-sm" id="sharedRelName" value="${prop?.name || ''}" ${disabled} required>
                </div>
                <div class="mb-3">
                    <label for="sharedRelLabel" class="form-label">Label</label>
                    <input type="text" class="form-control form-control-sm" id="sharedRelLabel" value="${prop?.label || prop?.name || ''}" ${disabled} placeholder="Defaults to name if empty">
                </div>
                <div class="mb-3">
                    <label for="sharedRelDomain" class="form-label">Source (Domain) <span class="text-danger">*</span></label>
                    <select class="form-select form-select-sm" id="sharedRelDomain" ${disabled} required>
                        <option value="">-- Select --</option>${classOptions}
                    </select>
                </div>
                <div class="mb-3">
                    <label for="sharedRelRange" class="form-label">Target (Range) <span class="text-danger">*</span></label>
                    <select class="form-select form-select-sm" id="sharedRelRange" ${disabled} required>
                        <option value="">-- Select --</option>${classOptions}
                    </select>
                </div>
                <div class="mb-3">
                    <label class="form-label">Direction</label>
                    <input type="hidden" id="sharedRelDirection" value="${prop?.direction || 'forward'}">
                    <div class="rel-direction-picker" id="sharedRelDirectionPicker" role="radiogroup" aria-label="Relationship direction">
                        ${_relDirectionOption('forward', 'bi-arrow-right', viewOnly)}
                        ${_relDirectionOption('reverse', 'bi-arrow-left', viewOnly)}
                    </div>
                    <div class="form-text small mt-1">Which way the triple is written — the canvas draws the arrowhead on the side it points to</div>
                </div>
                <div class="mb-3">
                    <label for="sharedRelDescription" class="form-label">Description</label>
                    <textarea class="form-control form-control-sm" id="sharedRelDescription" rows="2" ${disabled}>${prop?.comment || prop?.description || ''}</textarea>
                </div>
            </div>

            <div class="form-tab-pane ${_rTab === 'constraints' ? 'active' : ''}" data-form-tab-content="constraints">
                <div class="mb-3">
                    <label class="form-label small text-muted mb-1">Cardinality</label>
                    <div class="row g-2">
                        <div class="col-6">
                            <div class="input-group input-group-sm">
                                <span class="input-group-text">Min</span>
                                <input type="number" class="form-control" id="sharedRelMinCard" 
                                       value="${minCard}" min="0" placeholder="0" ${disabled}>
                            </div>
                            <div class="form-text small">Minimum number of values required per subject (0 = optional)</div>
                        </div>
                        <div class="col-6">
                            <div class="input-group input-group-sm">
                                <span class="input-group-text">Max</span>
                                <input type="number" class="form-control" id="sharedRelMaxCard" 
                                       value="${maxCard}" min="0" placeholder="*" ${disabled}>
                            </div>
                            <div class="form-text small">Maximum number of values allowed; leave empty for unlimited (*)</div>
                        </div>
                    </div>
                </div>
                <div class="mb-2">
                    <label class="form-label small text-muted mb-1">Property Characteristics</label>
                    <div class="form-check mb-2">
                        <input class="form-check-input" type="checkbox" id="sharedRelFunctional" 
                               ${isFunctional ? 'checked' : ''} ${disabled}>
                        <label class="form-check-label small fw-semibold" for="sharedRelFunctional">Functional</label>
                        <div class="form-text small mt-0">Each subject can have at most one value for this relationship (forces Max cardinality to 1)</div>
                    </div>
                    <div class="form-check mb-2">
                        <input class="form-check-input" type="checkbox" id="sharedRelInverseFunctional" 
                               ${isInverseFunctional ? 'checked' : ''} ${disabled}>
                        <label class="form-check-label small fw-semibold" for="sharedRelInverseFunctional">Inverse Functional</label>
                        <div class="form-text small mt-0">Each target value can be linked back to at most one subject (this relationship is one-to-one on the target side)</div>
                    </div>
                    <div class="form-check mb-2">
                        <input class="form-check-input" type="checkbox" id="sharedRelSymmetric" 
                               ${isSymmetric ? 'checked' : ''} ${disabled}>
                        <label class="form-check-label small fw-semibold" for="sharedRelSymmetric">Symmetric</label>
                        <div class="form-text small mt-0">If A is related to B, then B is automatically related to A too (the relationship reads the same in both directions)</div>
                    </div>
                    <div class="form-check">
                        <input class="form-check-input" type="checkbox" id="sharedRelTransitive" 
                               ${isTransitive ? 'checked' : ''} ${disabled}>
                        <label class="form-check-label small fw-semibold" for="sharedRelTransitive">Transitive</label>
                        <div class="form-text small mt-0">If A is related to B, and B is related to C, then A is automatically related to C (chains propagate through the relationship)</div>
                    </div>
                </div>
            </div>
        </form>
    `;
    
    if (prop) {
        panelGetById('sharedRelDomain').value = prop.domain || '';
        panelGetById('sharedRelRange').value = prop.range || '';
    }

    setSharedRelDirection(prop?.direction || 'forward');
    refreshSharedRelDirectionLabels();
    if (!viewOnly) {
        panelGetById('sharedRelDirectionPicker')?.addEventListener('click', (event) => {
            const option = event.target.closest('.rel-direction-option');
            if (option) setSharedRelDirection(option.dataset.direction);
        });
        ['sharedRelDomain', 'sharedRelRange', 'sharedRelName'].forEach(id => {
            panelGetById(id)?.addEventListener('input', refreshSharedRelDirectionLabels);
        });
    }
    
    // Add event listener to sync Functional checkbox with Max cardinality
    const functionalCheckbox = panelGetById('sharedRelFunctional');
    const maxCardInput = panelGetById('sharedRelMaxCard');
    if (functionalCheckbox && maxCardInput && !viewOnly) {
        functionalCheckbox.addEventListener('change', function() {
            if (this.checked) {
                maxCardInput.value = '1';
            }
        });
        maxCardInput.addEventListener('input', function() {
            if (this.value === '1') {
                functionalCheckbox.checked = true;
            } else if (this.value !== '1' && this.value !== '') {
                functionalCheckbox.checked = false;
            }
        });
    }

    if (prop?.name) {
        _renderAssignmentLink('sharedRelAssignmentLink', 'relationship', prop.name);
    }
}

async function saveSharedRelationship(options = {}) {
    const name = panelGetById('sharedRelName')?.value.trim();
    const labelRaw = panelGetById('sharedRelLabel')?.value.trim();
    const label = labelRaw || name;
    const domain = panelGetById('sharedRelDomain')?.value;
    const range = panelGetById('sharedRelRange')?.value;
    const direction = panelGetById('sharedRelDirection')?.value;
    const comment = panelGetById('sharedRelDescription')?.value.trim();
    
    // Get constraint values from the form
    const minCardValue = panelGetById('sharedRelMinCard')?.value.trim();
    const maxCardValue = panelGetById('sharedRelMaxCard')?.value.trim();
    const isFunctional = panelGetById('sharedRelFunctional')?.checked || false;
    const isInverseFunctional = panelGetById('sharedRelInverseFunctional')?.checked || false;
    const isSymmetric = panelGetById('sharedRelSymmetric')?.checked || false;
    const isTransitive = panelGetById('sharedRelTransitive')?.checked || false;
    
    if (!name) { showNotification('Please enter a relationship name', 'warning'); return; }

    // When the name is unchanged (direction/domain/range edit) the entry cannot be a
    // duplicate of itself — skip the index-based check, which can be stale when
    // syncDesignToOntology has reordered the properties array since the panel opened.
    const duplicateRel = (name !== sharedPanelOriginalName || sharedPanelEditIndex < 0)
        && (OntologyState.config.properties || []).some((p, i) => p.name === name && i !== sharedPanelEditIndex);
    if (duplicateRel) { showNotification(`A relationship named "${name}" already exists`, 'warning'); return; }

    if (!domain) { showNotification('Please select a source entity', 'warning'); return; }
    if (!range) { showNotification('Please select a target entity', 'warning'); return; }
    
    // Build constraints object for server-side storage ONLY
    const constraints = {};
    if (minCardValue !== '' && minCardValue !== '0') {
        constraints.minCardinality = parseInt(minCardValue, 10);
    }
    if (maxCardValue !== '') {
        constraints.maxCardinality = parseInt(maxCardValue, 10);
    }
    if (isFunctional) constraints.functional = true;
    if (isInverseFunctional) constraints.inverseFunctional = true;
    if (isSymmetric) constraints.symmetric = true;
    if (isTransitive) constraints.transitive = true;
    
    const propertyData = { 
        name, 
        localName: name, 
        label, 
        comment, 
        description: comment, 
        type: 'ObjectProperty', 
        domain, 
        range, 
        direction
        // NOTE: constraints are stored ONLY in session_data/ontology/constraints, not here
    };
    const isRename = sharedPanelEditIndex >= 0 && sharedPanelOriginalName && sharedPanelOriginalName !== name;
    
    if (sharedPanelEditIndex >= 0) {
        const existingProp = OntologyState.config.properties[sharedPanelEditIndex] || {};
        // Preserve server-assigned URI so the backend prune doesn't orphan mappings.
        if (existingProp.uri) propertyData.uri = existingProp.uri;
        OntologyState.config.properties[sharedPanelEditIndex] = propertyData;
        showNotification('Relationship updated', 'success', 2000);
    } else {
        OntologyState.config.properties.push(propertyData);
        showNotification('Relationship added', 'success', 2000);
    }
    
    await window.saveConfigToSession({ keepalive: options.keepalive });
    if (isRename) {
        try { await fetch('/ontology/update-relationship-references', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ old_name: sharedPanelOriginalName, new_name: name }) }); } catch (e) {}
    }
    
    // Save constraints to the ONLY storage location: session_data/ontology/constraints
    await saveRelationshipConstraintsToServer(name, domain, constraints);
    
    await autoGenerateOwl();
    if (sharedPanelOnSaveCallback) sharedPanelOnSaveCallback();
    if (typeof updatePropertiesList === 'function') updatePropertiesList();
    
    // Refresh ConstraintsModule if loaded
    if (typeof ConstraintsModule !== 'undefined' && ConstraintsModule.loadConstraints) {
        ConstraintsModule.loadConstraints();
    }

    // Targeted in-place canvas updates — immediately flip the arrow in whichever
    // view is currently active. Both calls are no-ops when the view was never opened.
    const lookupName = sharedPanelOriginalName || name;
    if (typeof refreshRelationshipInDesigner === 'function') {
        refreshRelationshipInDesigner(lookupName, direction);
    }
    if (typeof refreshMapLinkDirection === 'function') {
        refreshMapLinkDirection(lookupName, direction);
    }

    closeSharedPanel();
}

/**
 * Save relationship constraints to server-side storage (session_data/ontology/constraints)
 * This is the ONLY place constraints are stored
 */
async function saveRelationshipConstraintsToServer(propertyName, domainClass, constraints) {
    try {
        // First, get existing constraints to find indices
        const listResponse = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
        const listData = await listResponse.json();
        const existingConstraints = listData.success ? (listData.constraints || []) : [];
        
        // Define constraint types
        const cardinalityTypes = ['minCardinality', 'maxCardinality'];
        const characteristicTypes = ['functional', 'inverseFunctional', 'symmetric', 'transitive'];
        
        // Process cardinality constraints
        for (const cardType of cardinalityTypes) {
            const existingIdx = existingConstraints.findIndex(c => c.property === propertyName && c.type === cardType);
            const hasValue = constraints[cardType] !== undefined && constraints[cardType] !== null;
            
            if (hasValue) {
                // Add or update constraint
                const constraint = {
                    type: cardType,
                    property: propertyName,
                    className: domainClass,
                    cardinalityValue: constraints[cardType]
                };
                await fetch('/ontology/constraints/save', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ constraint, index: existingIdx >= 0 ? existingIdx : -1 }),
                    credentials: 'same-origin'
                });
                // Re-fetch to get updated indices after save
                if (existingIdx < 0) {
                    const refreshResponse = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
                    const refreshData = await refreshResponse.json();
                    existingConstraints.length = 0;
                    existingConstraints.push(...(refreshData.constraints || []));
                }
            } else if (existingIdx >= 0) {
                // Remove constraint if it exists but value is now empty
                await fetch('/ontology/constraints/delete', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ index: existingIdx }),
                    credentials: 'same-origin'
                });
                // Re-fetch to get updated indices after delete
                const refreshResponse = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
                const refreshData = await refreshResponse.json();
                existingConstraints.length = 0;
                existingConstraints.push(...(refreshData.constraints || []));
            }
        }
        
        // Process characteristic constraints (functional, symmetric, etc.)
        for (const charType of characteristicTypes) {
            const existingIdx = existingConstraints.findIndex(c => c.property === propertyName && c.type === charType);
            const isEnabled = constraints[charType] === true;
            
            if (isEnabled && existingIdx < 0) {
                // Add new characteristic constraint
                await fetch('/ontology/constraints/save', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ constraint: { type: charType, property: propertyName }, index: -1 }),
                    credentials: 'same-origin'
                });
            } else if (!isEnabled && existingIdx >= 0) {
                // Remove characteristic constraint
                await fetch('/ontology/constraints/delete', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ index: existingIdx }),
                    credentials: 'same-origin'
                });
                // Re-fetch to get updated indices
                const refreshResponse = await fetch('/ontology/constraints/list', { credentials: 'same-origin' });
                const refreshData = await refreshResponse.json();
                existingConstraints.length = 0;
                existingConstraints.push(...(refreshData.constraints || []));
            }
        }
    } catch (error) {
        console.error('[SharedPanel] Error saving constraints to server:', error);
    }
}

// =====================================================
// SAVE HANDLER
// =====================================================

async function saveSharedPanelItem(options = {}) {
    if (sharedPanelViewOnly) return;
    if (sharedPanelEditType === 'entity') await saveSharedEntity(options);
    else if (sharedPanelEditType === 'relationship') await saveSharedRelationship(options);
    sharedPanelDirty = false;
}

/**
 * Commit the panel's pending edit while the page is being torn down.
 *
 * The panel is an edit buffer: closing it, switching section or opening
 * another item all flush it first. Leaving the page did not, so a pending
 * edit — a removed virtual attribute, a renamed entity — was dropped, and the
 * registry auto-save on unload then persisted the state that never heard about
 * it. This closes that gap.
 *
 * Deliberately not awaited: only the synchronous prefix of the save can run
 * before teardown. That prefix commits the change to `OntologyState.config`
 * and dispatches the session save with `keepalive`, which is the part that
 * must not be lost. The tail (constraints, OWL regeneration, notifications)
 * is skipped — the OWL is regenerated from the config on the next load.
 */
function flushSharedPanelOnUnload() {
    if (!sharedPanelDirty || sharedPanelViewOnly) return;
    try {
        saveSharedPanelItem({ keepalive: true });
        console.log('[UNLOAD] Pending panel edit flushed to session');
    } catch (err) {
        console.warn('[UNLOAD] Could not flush the pending panel edit:', err);
    }
    sharedPanelDirty = false;
}

window.addEventListener('beforeunload', flushSharedPanelOnUnload);
window.addEventListener('pagehide', flushSharedPanelOnUnload);
window.flushSharedPanelOnUnload = flushSharedPanelOnUnload;

document.addEventListener('DOMContentLoaded', () => {
    ensureDetailPanels();
});

// =====================================================
// COMPATIBILITY FUNCTIONS
// =====================================================

function editClassByName(className, activeTab) {
    const idx = OntologyState.config.classes.findIndex(cls => cls.name === className);
    if (idx >= 0) {
        const opts = { onSave: () => { if (typeof initOntologyMap === 'function' && document.getElementById('map-section')?.classList.contains('active')) initOntologyMap(); } };
        if (activeTab) opts.activeTab = activeTab;
        if (_canEditOntologyPanel()) openEntityPanelForEdit(idx, opts);
        else openEntityPanelForView(idx, opts);
    }
}

function editClass(idx, activeTab) {
    const opts = { onSave: () => { if (typeof initOntologyMap === 'function' && document.getElementById('map-section')?.classList.contains('active')) initOntologyMap(); } };
    if (activeTab) opts.activeTab = activeTab;
    openEntityPanelForEdit(idx, opts);
}
function viewClass(idx) { openEntityPanelForView(idx); }

function editPropertyByName(propertyName, activeTab) {
    const idx = OntologyState.config.properties.findIndex(prop => prop.name === propertyName);
    if (idx >= 0) {
        const opts = { onSave: () => { if (typeof initOntologyMap === 'function' && document.getElementById('map-section')?.classList.contains('active')) initOntologyMap(); } };
        if (activeTab) opts.activeTab = activeTab;
        if (_canEditOntologyPanel()) openRelationshipPanelForEdit(idx, opts);
        else openRelationshipPanelForView(idx, opts);
    }
}

function editProperty(idx, activeTab) {
    const opts = { onSave: () => { if (typeof initOntologyMap === 'function' && document.getElementById('map-section')?.classList.contains('active')) initOntologyMap(); } };
    if (activeTab) opts.activeTab = activeTab;
    openRelationshipPanelForEdit(idx, opts);
}
function viewProperty(idx) { openRelationshipPanelForView(idx); }


// ===========================================
// Metadata Attribute Picker
// ===========================================

let metaAttrPickerModal = null;
let metaAttrMetadata = null;
let metaAttrSelectedColumns = {};

/**
 * Convert a column name to camelCase attribute name.
 * e.g. "street_address" -> "streetAddress", "Contract ID" -> "contractId", "POSTAL_CODE" -> "postalCode"
 */
function columnToCamelCase(name) {
    if (!name) return '';
    // Replace underscores and hyphens with spaces, then split
    const words = name.replace(/[_\-]+/g, ' ').trim().split(/\s+/);
    if (words.length === 0) return '';
    // First word lowercase, rest title-case
    return words.map((word, i) => {
        if (i === 0) return word.toLowerCase();
        return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    }).join('');
}

/**
 * Open the metadata attribute picker modal.
 * Fetches domain metadata and shows table list.
 */
async function openMetadataAttributePicker() {
    if (!metaAttrPickerModal) {
        metaAttrPickerModal = new bootstrap.Modal(document.getElementById('metadataAttributePickerModal'));
    }
    
    // Reset state
    metaAttrSelectedColumns = {};
    document.getElementById('metaAttrStep1').style.display = 'block';
    document.getElementById('metaAttrStep2').style.display = 'none';
    document.getElementById('metaAttrFooter').style.display = 'none';
    
    const tableList = document.getElementById('metaAttrTableList');
    tableList.innerHTML = '<div class="text-muted small p-2"><i class="bi bi-hourglass-split me-1"></i>Loading data sources...</div>';
    
    metaAttrPickerModal.show();
    
    try {
        const response = await fetch('/domain/metadata', { credentials: 'same-origin' });
        const data = await response.json();
        
        if (!data.success || !data.has_metadata || !data.metadata?.tables?.length) {
            tableList.innerHTML = '<div class="text-muted small p-2"><i class="bi bi-exclamation-circle me-1"></i>No data sources loaded. Load data sources in Domain settings first.</div>';
            return;
        }
        
        metaAttrMetadata = data.metadata;
        renderMetaAttrTableList();
        
    } catch (error) {
        tableList.innerHTML = `<div class="text-danger small p-2"><i class="bi bi-exclamation-triangle me-1"></i>${error.message}</div>`;
    }
}

/**
 * Render the table selection list (Step 1).
 */
function renderMetaAttrTableList() {
    const tableList = document.getElementById('metaAttrTableList');
    const tables = metaAttrMetadata?.tables || [];
    
    if (tables.length === 0) {
        tableList.innerHTML = '<div class="text-muted small p-2">No tables available</div>';
        return;
    }
    
    // Get existing attribute names to show which columns are already added
    const existingAttrs = new Set(sharedPanelOwnAttributes.map(a => a.name.toLowerCase()));
    const inheritedAttrs = new Set(sharedPanelInheritedAttributes.map(a => a.name.toLowerCase()));
    const allAttrs = new Set([...existingAttrs, ...inheritedAttrs]);
    
    let html = '';
    for (const table of tables) {
        const colCount = table.columns?.length || 0;
        const tableFqn = table.full_name || table.name;
        const fqnParts = tableFqn.split('.');
        const displayName = fqnParts.length === 3 ? fqnParts[2] : tableFqn;
        const dataSource = fqnParts.length >= 2 ? `${fqnParts[0]}.${fqnParts[1]}` : '';
        const alreadyCount = table.columns ? table.columns.filter(c => allAttrs.has(columnToCamelCase(c.name).toLowerCase())).length : 0;
        
        html += `
            <a href="#" class="list-group-item list-group-item-action py-2 px-2" onclick="metaAttrSelectTable('${tableFqn.replace(/'/g, "\\'")}'); return false;">
                <div class="d-flex justify-content-between align-items-center">
                    <span class="small fw-semibold"><i class="bi bi-table me-1 text-primary"></i>${displayName}</span>
                    <span class="badge bg-secondary">${colCount} col</span>
                </div>
                ${dataSource ? `<small class="text-muted d-block" style="font-size: 0.7rem;">${dataSource}</small>` : ''}
                ${table.comment ? `<small class="text-muted d-block" style="font-size: 0.7rem;">${table.comment}</small>` : ''}
                ${alreadyCount > 0 ? `<small class="text-info" style="font-size: 0.65rem;">${alreadyCount} column(s) already as attributes</small>` : ''}
            </a>
        `;
    }
    
    tableList.innerHTML = html;
}

/**
 * Select a table and show its columns (Step 2).
 */
function metaAttrSelectTable(tableName) {
    const tables = metaAttrMetadata?.tables || [];
    const table = tables.find(t => (t.full_name || t.name) === tableName);
    
    if (!table || !table.columns?.length) {
        showNotification('No columns found for this table', 'warning');
        return;
    }
    
    // Get existing attribute names
    const existingAttrs = new Set(sharedPanelOwnAttributes.map(a => a.name.toLowerCase()));
    const inheritedAttrs = new Set(sharedPanelInheritedAttributes.map(a => a.name.toLowerCase()));
    const allAttrs = new Set([...existingAttrs, ...inheritedAttrs]);
    
    // Reset selections
    metaAttrSelectedColumns = {};
    
    // Switch to step 2
    document.getElementById('metaAttrStep1').style.display = 'none';
    document.getElementById('metaAttrStep2').style.display = 'block';
    document.getElementById('metaAttrFooter').style.display = 'flex';
    document.getElementById('metaAttrSelectedTable').textContent = tableName;
    
    const colList = document.getElementById('metaAttrColumnList');
    let html = '';
    
    for (const col of table.columns) {
        const camelName = columnToCamelCase(col.name);
        const alreadyExists = allAttrs.has(camelName.toLowerCase());
        const disabled = alreadyExists ? 'disabled' : '';
        const labelClass = alreadyExists ? 'text-muted' : '';
        const badge = alreadyExists ? '<span class="badge bg-info ms-1" style="font-size: 0.6rem;">exists</span>' : '';
        
        html += `
            <label class="list-group-item list-group-item-action py-1 px-2 d-flex align-items-start gap-2 ${labelClass}" style="cursor: ${alreadyExists ? 'default' : 'pointer'};">
                <input class="form-check-input mt-1 metaAttrColCheck" type="checkbox" value="${col.name}" 
                       data-camel="${camelName}" ${disabled}
                       onchange="metaAttrToggleColumn('${col.name.replace(/'/g, "\\'")}', this.checked)">
                <div class="flex-grow-1">
                    <div class="small">
                        <span class="fw-semibold">${col.name}</span>
                        <i class="bi bi-arrow-right mx-1 text-muted" style="font-size: 0.65rem;"></i>
                        <code class="text-primary" style="font-size: 0.75rem;">${camelName}</code>
                        ${badge}
                    </div>
                    <div style="font-size: 0.65rem;" class="text-muted">
                        ${col.type || ''}${col.comment ? ' — ' + col.comment : ''}
                    </div>
                </div>
            </label>
        `;
    }
    
    colList.innerHTML = html;
    metaAttrUpdateSelectionCount();
}

/**
 * Go back to table list (Step 1).
 */
function metaAttrBackToTables() {
    document.getElementById('metaAttrStep1').style.display = 'block';
    document.getElementById('metaAttrStep2').style.display = 'none';
    document.getElementById('metaAttrFooter').style.display = 'none';
    metaAttrSelectedColumns = {};
}

/**
 * Toggle a column selection.
 */
function metaAttrToggleColumn(colName, checked) {
    if (checked) {
        metaAttrSelectedColumns[colName] = true;
    } else {
        delete metaAttrSelectedColumns[colName];
    }
    metaAttrUpdateSelectionCount();
}

/**
 * Toggle all columns.
 */
function metaAttrToggleAll(checked) {
    const checkboxes = document.querySelectorAll('.metaAttrColCheck:not(:disabled)');
    checkboxes.forEach(cb => {
        cb.checked = checked;
        metaAttrToggleColumn(cb.value, checked);
    });
}

/**
 * Update the selection count display.
 */
function metaAttrUpdateSelectionCount() {
    const count = Object.keys(metaAttrSelectedColumns).length;
    const countEl = document.getElementById('metaAttrSelectionCount');
    if (countEl) countEl.textContent = `${count} selected`;
    
    // Update select-all checkbox state
    const allCheckboxes = document.querySelectorAll('.metaAttrColCheck:not(:disabled)');
    const allChecked = allCheckboxes.length > 0 && Array.from(allCheckboxes).every(cb => cb.checked);
    const someChecked = Array.from(allCheckboxes).some(cb => cb.checked);
    const selectAllCb = document.getElementById('metaAttrSelectAll');
    if (selectAllCb) {
        selectAllCb.checked = allChecked;
        selectAllCb.indeterminate = someChecked && !allChecked;
    }
}

/**
 * Apply selected columns as attributes (camelCase).
 */
function metaAttrApplySelection() {
    const selectedNames = Object.keys(metaAttrSelectedColumns);
    
    if (selectedNames.length === 0) {
        showNotification('No columns selected', 'warning');
        return;
    }
    
    // Get existing attribute names to avoid duplicates
    const existingAttrs = new Set(sharedPanelOwnAttributes.map(a => a.name.toLowerCase()));
    
    let addedCount = 0;
    for (const colName of selectedNames) {
        const camelName = columnToCamelCase(colName);
        if (camelName && !existingAttrs.has(camelName.toLowerCase())) {
            sharedPanelOwnAttributes.push({ name: camelName });
            existingAttrs.add(camelName.toLowerCase());
            addedCount++;
        }
    }
    
    // Re-render attributes
    renderSharedEntityAttributes(false);
    
    // Close modal
    if (metaAttrPickerModal) {
        metaAttrPickerModal.hide();
    }
    
    if (addedCount > 0) {
        showNotification(`Added ${addedCount} attribute(s) from data sources`, 'success', 2000);
    } else {
        showNotification('All selected columns already exist as attributes', 'info', 2000);
    }
}
