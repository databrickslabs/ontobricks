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

// Emoji categories for icon picker
const sharedEmojiCategories = {
    'People & Roles': ['👤', '👥', '👨', '👩', '👶', '👴', '👵', '🧑', '👨‍💼', '👩‍💼', '👨‍🔬', '👩‍🔬', '👨‍💻', '👩‍💻', '👨‍🏫', '👩‍🏫', '👨‍⚕️', '👩‍⚕️', '🧑‍🤝‍🧑', '👪'],
    'Business & Work': ['🏢', '🏭', '🏬', '🏛️', '💼', '📊', '📈', '📉', '💰', '💵', '💳', '🏦', '📋', '📁', '📂', '🗂️', '📝', '✏️', '📌', '📎'],
    'Technology': ['💻', '🖥️', '⌨️', '🖱️', '📱', '📲', '☎️', '🔌', '💾', '💿', '📀', '🔧', '🔩', '⚙️', '🔬', '🔭', '📡', '🤖', '🔋', '💡'],
    'Data & Documents': ['📄', '📃', '📑', '📰', '📚', '📖', '📒', '📓', '📔', '📕', '📗', '📘', '📙', '🗃️', '🗄️', '📦', '📫', '📬', '📭', '📮'],
    'Nature & Science': ['🌍', '🌎', '🌏', '🌐', '🌳', '🌲', '🌴', '🌵', '🌾', '🌻', '🔥', '💧', '⚡', '🌈', '☀️', '🌙', '⭐', '🌟', '💎', '🔮'],
    'Objects & Things': ['🏠', '🏡', '🚗', '🚕', '🚌', '✈️', '🚀', '🛸', '⚓', '🎯', '🎨', '🎭', '🎪', '🎬', '🎮', '🎲', '🧩', '🔑', '🗝️', '🔒'],
    'Symbols': ['❤️', '💙', '💚', '💛', '💜', '🖤', '🤍', '🤎', '⭕', '❌', '✅', '❎', '➕', '➖', '➗', '✖️', '💯', '🔴', '🟠', '🟢'],
    'Arrows & Shapes': ['⬆️', '⬇️', '⬅️', '➡️', '↗️', '↘️', '↙️', '↖️', '↕️', '↔️', '🔄', '🔃', '🔀', '🔁', '🔂', '▶️', '⏸️', '⏹️', '🔷', '🔶']
};

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
let sharedPanelDirty = false;

// Panel resize state
let isResizing = false;
let panelStartWidth = 380;

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
    const containerMap = {
        'map-section': 'ontology-map-container',
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
    
    console.log('[SharedPanel] Creating new panel');
    
    // Create resize handle
    const resizeHandle = document.createElement('div');
    resizeHandle.className = 'detail-panel-resize-handle';
    resizeHandle.innerHTML = '<div class="resize-bar"></div>';
    
    // Create panel
    const panelDiv = document.createElement('div');
    panelDiv.className = 'shared-detail-panel';
    
    const isMapSection = activeSection.id === 'map-section';
    
    if (isMapSection) {
        const hideAssistant = window.isActiveVersion === false;
        panelDiv.classList.add('shared-detail-panel-tabbed');
        panelDiv.innerHTML = `
            <div class="panel-tabs-header">
                <ul class="nav nav-tabs panel-nav-tabs" role="tablist">
                    <li class="nav-item">
                        <a class="nav-link active" data-panel-tab="details" role="tab" href="#">
                            <i class="bi bi-info-circle me-1"></i>Details
                        </a>
                    </li>
                    <li class="nav-item" ${hideAssistant ? 'style="display:none"' : ''}>
                        <a class="nav-link" data-panel-tab="assistant" role="tab" href="#">
                            <i class="bi bi-chat-dots me-1"></i>AI Assistant
                        </a>
                    </li>
                </ul>
                <button type="button" class="btn btn-outline-secondary btn-sm panel-close-btn" id="sharedClosePanelBtn" title="Close">
                    <i class="bi bi-x-lg"></i>
                </button>
            </div>
            <div class="panel-tab-pane active" data-panel-tab-content="details">
                <div class="panel-header">
                    <h6 id="sharedPanelTitle"><i class="bi bi-box"></i> <span id="sharedPanelItemName">Edit</span></h6>
                </div>
                <div class="panel-body" id="sharedPanelBody"></div>
                <div class="panel-footer" id="sharedPanelFooter">
                    <button type="button" class="btn btn-secondary btn-sm" id="sharedCancelPanelBtn">Cancel</button>
                    <button type="button" class="btn btn-dark btn-sm" id="sharedSavePanelBtn">
                        <i class="bi bi-check-circle"></i> Save
                    </button>
                </div>
            </div>
            <div class="panel-tab-pane" data-panel-tab-content="assistant">
                <div class="assistant-messages" id="assistantMessages">
                    <div class="assistant-welcome">
                        <div class="assistant-welcome-icon"><img src="/static/global/img/favicon.svg" alt="OntoBricks" width="40" height="40"></div>
                        <p>Modify your ontology with natural language:</p>
                        <div class="assistant-suggestions">
                            <button class="btn btn-sm btn-outline-primary assistant-suggestion" data-message="Show me all entities and their attributes">
                                <i class="bi bi-list-ul me-1"></i>List entities
                            </button>
                            <button class="btn btn-sm btn-outline-primary assistant-suggestion" data-message="Show me all relationships">
                                <i class="bi bi-arrow-left-right me-1"></i>List relationships
                            </button>
                            <button class="btn btn-sm btn-outline-danger assistant-suggestion" data-message="Remove all the entities that have no relationship and no inheritance">
                                <i class="bi bi-trash me-1"></i>Clean orphans
                            </button>
                        </div>
                    </div>
                </div>
                <div class="assistant-input-area">
                    <div class="assistant-input-wrapper">
                        <textarea id="assistantInput" class="form-control" placeholder="Ask me to modify your ontology…" rows="1"></textarea>
                        <button id="assistantSendBtn" class="btn btn-primary" title="Send message" disabled>
                            <i class="bi bi-send-fill"></i>
                        </button>
                    </div>
                    <div class="assistant-input-hint text-muted small mt-1">
                        <kbd>Enter</kbd> to send &middot; <button class="btn btn-link btn-sm p-0 text-muted" id="assistantClearBtn" style="font-size: 0.75rem; text-decoration: none;">Clear chat</button>
                    </div>
                </div>
            </div>
        `;
    } else {
        panelDiv.innerHTML = `
            <div class="panel-header">
                <h6 id="sharedPanelTitle"><i class="bi bi-box"></i> <span id="sharedPanelItemName">Edit</span></h6>
                <button type="button" class="btn btn-outline-secondary btn-sm panel-close-btn" id="sharedClosePanelBtn" title="Close">
                    <i class="bi bi-x-lg"></i>
                </button>
            </div>
            <div class="panel-body" id="sharedPanelBody"></div>
            <div class="panel-footer" id="sharedPanelFooter">
                <button type="button" class="btn btn-secondary btn-sm" id="sharedCancelPanelBtn">Cancel</button>
                <button type="button" class="btn btn-dark btn-sm" id="sharedSavePanelBtn">
                    <i class="bi bi-check-circle"></i> Save
                </button>
            </div>
        `;
    }
    
    // Append to container (either section or ontology-map-container for Map)
    panelContainer.appendChild(resizeHandle);
    panelContainer.appendChild(panelDiv);
    
    // Add the class to enable split layout
    panelContainer.classList.add('has-detail-panel');
    
    // Setup event listeners for this panel instance
    setupPanelListeners(panelContainer);
    setupResizeHandle(panelContainer);
    
    return panelDiv;
}

/**
 * Setup panel event listeners
 */
function setupPanelListeners(section) {
    section.querySelector('#sharedClosePanelBtn')?.addEventListener('click', guardedCloseSharedPanel);
    section.querySelector('#sharedCancelPanelBtn')?.addEventListener('click', guardedCloseSharedPanel);
    section.querySelector('#sharedSavePanelBtn')?.addEventListener('click', saveSharedPanelItem);
    
    // Tab switching for tabbed panels (map section)
    section.querySelectorAll('[data-panel-tab]').forEach(tabLink => {
        if (tabLink.tagName === 'A') {
            tabLink.addEventListener('click', (e) => {
                e.preventDefault();
                switchPanelTab(section, tabLink.dataset.panelTab);
            });
        }
    });
}

/**
 * Switch between tabs in a tabbed panel.
 */
function switchPanelTab(container, tabName) {
    const panel = container.querySelector('.shared-detail-panel-tabbed');
    if (!panel) return;
    
    panel.querySelectorAll('.panel-nav-tabs .nav-link').forEach(link => {
        link.classList.toggle('active', link.dataset.panelTab === tabName);
    });
    
    panel.querySelectorAll('.panel-tab-pane').forEach(pane => {
        pane.classList.toggle('active', pane.dataset.panelTabContent === tabName);
    });
    
    // Update toggle button state
    const toggleBtn = document.getElementById('mapToggleAssistant');
    if (toggleBtn) {
        toggleBtn.classList.toggle('active', tabName === 'assistant');
    }
    
    // Re-initialize assistant when switching to its tab
    if (tabName === 'assistant' && typeof window.initOntologyAssistant === 'function') {
        window.initOntologyAssistant();
    }
}

/**
 * Open the right panel on the AI Assistant tab (called from toolbar button).
 */
function openAssistantPanel() {
    const mapContainer = document.getElementById('ontology-map-container');
    if (!mapContainer) return;
    
    const panel = mapContainer.querySelector('.shared-detail-panel-tabbed');
    const isOpen = mapContainer.classList.contains('panel-open');
    
    if (isOpen && panel) {
        const assistantTab = panel.querySelector('.panel-nav-tabs .nav-link[data-panel-tab="assistant"]');
        if (assistantTab && assistantTab.classList.contains('active')) {
            guardedCloseSharedPanel();
            return;
        }
        switchPanelTab(mapContainer, 'assistant');
        return;
    }
    
    // Panel doesn't exist yet or isn't open — create/open it
    // Temporarily set sharedPanelEditType so the panel opens properly
    const prevType = sharedPanelEditType;
    sharedPanelEditType = sharedPanelEditType || '_assistant';
    openSharedPanel();
    sharedPanelEditType = prevType;
    
    // Switch to assistant tab
    const freshPanel = mapContainer.querySelector('.shared-detail-panel-tabbed');
    if (freshPanel) {
        switchPanelTab(mapContainer, 'assistant');
        
        // Set empty state on details tab when opened directly to assistant
        const body = freshPanel.querySelector('#sharedPanelBody');
        if (body && !body.innerHTML.trim()) {
            body.innerHTML = '<div class="text-center text-muted py-4"><i class="bi bi-hand-index fs-3 d-block mb-2 opacity-50"></i><p class="small mb-0">Click an entity or relationship on the map to see its details here.</p></div>';
        }
    }
    
    if (typeof window.initOntologyAssistant === 'function') {
        window.initOntologyAssistant();
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
        const save = await showConfirmDialog({
            title: 'Unsaved Changes',
            message: 'You have unsaved changes. Do you want to save before closing?',
            confirmText: 'Save',
            cancelText: 'Discard',
            confirmClass: 'btn-primary',
            icon: 'exclamation-triangle'
        });
        if (save) {
            await saveSharedPanelItem();
            return;
        }
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
        
        // Clamp width between min and max
        const clampedWidth = Math.max(280, Math.min(500, newWidth));
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
    const save = await showConfirmDialog({
        title: 'Unsaved Changes',
        message: 'You have unsaved changes. Do you want to save before continuing?',
        confirmText: 'Save',
        cancelText: 'Discard',
        confirmClass: 'btn-primary',
        icon: 'exclamation-triangle'
    });
    if (save) {
        await saveSharedPanelItem();
    } else {
        sharedPanelDirty = false;
    }
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
    
    // Get the container (either section or map-container for Map section)
    const container = panel.parentElement;
    // For tabbed panels the details tab content is a child, so walk up
    const effectiveContainer = panel.closest('.has-detail-panel') || container;
    console.log('[SharedPanel] Adding panel-open class to container:', effectiveContainer?.id || effectiveContainer?.className);
    effectiveContainer?.classList.add('panel-open');
    
    // If this is a tabbed panel and we're opening for entity/relationship details, switch to Details tab
    if (panel.classList.contains('shared-detail-panel-tabbed') && sharedPanelEditType && sharedPanelEditType !== '_assistant') {
        switchPanelTab(effectiveContainer, 'details');
    }
}

/**
 * Close the shared panel
 */
function closeSharedPanel() {
    // Find all containers that might have the panel-open class
    const containers = [
        sharedPanelCurrentSection,
        document.getElementById('ontology-map-container'),
        document.getElementById('ontology-entities-container'),
        document.getElementById('ontology-relationships-container')
    ];
    
    containers.forEach(container => {
        if (container) {
            container.classList.remove('panel-open');
        }
    });
    
    // Reset assistant toggle button state
    const toggleBtn = document.getElementById('mapToggleAssistant');
    if (toggleBtn) toggleBtn.classList.remove('active');
    
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

async function openEntityPanel(options = {}) {
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
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) {
        console.log('[SharedPanel] Panel not found after openSharedPanel');
        return;
    }
    
    panel.querySelector('#sharedPanelTitle').innerHTML = '<i class="bi bi-plus-circle"></i> <span id="sharedPanelItemName">Add Entity</span>';
    panel.querySelector('#sharedSavePanelBtn').style.display = '';
    
    await renderEntityForm(panel, null);
    attachDirtyTracking();
}

async function openEntityPanelForEdit(idx, options = {}) {
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
    
    console.log('[SharedPanel] Edit - Loaded class:', cls.name, 'dataProperties:', (cls.dataProperties || []).length);
    
    sharedPanelInheritedAttributes = getSharedInheritedProperties(cls.parent);
    const inheritedNames = new Set(sharedPanelInheritedAttributes.map(a => a.name));
    sharedPanelOwnAttributes = (cls.dataProperties || [])
        .map(p => ({ name: p.name || p.localName || p }))
        .filter(a => !inheritedNames.has(a.name));
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) {
        console.log('[SharedPanel] Panel not found after openSharedPanel');
        return;
    }
    
    const emoji = cls.emoji || OntologyState.defaultClassEmoji || '📦';
    panel.querySelector('#sharedPanelTitle').innerHTML = `<i class="bi bi-pencil"></i> ${emoji} <span id="sharedPanelItemName">${cls.name}</span>`;
    panel.querySelector('#sharedSavePanelBtn').style.display = '';
    
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
    
    sharedPanelInheritedAttributes = getSharedInheritedProperties(cls.parent);
    const inheritedNames = new Set(sharedPanelInheritedAttributes.map(a => a.name));
    sharedPanelOwnAttributes = (cls.dataProperties || [])
        .map(p => ({ name: p.name || p.localName || p }))
        .filter(a => !inheritedNames.has(a.name));
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) return;
    
    const emoji = cls.emoji || OntologyState.defaultClassEmoji || '📦';
    panel.querySelector('#sharedPanelTitle').innerHTML = `<i class="bi bi-eye"></i> ${emoji} <span id="sharedPanelItemName">${cls.name}</span>`;
    panel.querySelector('#sharedSavePanelBtn').style.display = 'none';
    
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
    
    body.innerHTML = `
        <div id="sharedEntityAssignmentLink"></div>
        <form id="sharedEntityForm">
            <div class="mb-3 p-2 bg-light rounded border">
                <label for="sharedEntityParent" class="form-label"><i class="bi bi-diagram-2"></i> Inherits From</label>
                <select class="form-select form-select-sm" id="sharedEntityParent" ${disabled} onchange="onSharedEntityParentChange()">
                    <option value="">-- None --</option>
                    ${parentOptions}
                </select>
            </div>
            
            <div class="mb-3">
                <label for="sharedEntityName" class="form-label">Name <span class="text-danger">*</span></label>
                <input type="text" class="form-control form-control-sm" id="sharedEntityName" value="${cls?.name || ''}" ${disabled} required>
            </div>
            
            <div class="mb-3">
                <label class="form-label">Icon</label>
                <div class="input-group input-group-sm">
                    <span class="input-group-text" id="sharedEntityEmojiPreview">${emoji}</span>
                    <input type="text" class="form-control" id="sharedEntityIcon" value="${emoji}" ${disabled} maxlength="2" style="width: 45px;">
                    <button type="button" class="btn btn-outline-secondary" id="sharedEntityEmojiBtn" ${disabled}><i class="bi bi-emoji-smile"></i></button>
                </div>
                <div id="sharedEntityEmojiPicker" class="emoji-picker-container mt-2" style="display: none;">
                    <div class="card">
                        <div class="card-header py-1">
                            <div class="d-flex justify-content-between align-items-center">
                                <small class="fw-bold">Select Icon</small>
                                <button type="button" class="btn-close btn-sm" onclick="closeSharedEmojiPicker()"></button>
                            </div>
                        </div>
                        <div class="card-body p-2">
                            <input type="text" class="form-control form-control-sm mb-2" id="sharedEntityEmojiSearch" placeholder="Search...">
                            <div id="sharedEntityEmojiGrid" class="emoji-picker-grid"></div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="mb-3">
                <label for="sharedEntityDescription" class="form-label">Description</label>
                <textarea class="form-control form-control-sm" id="sharedEntityDescription" rows="2" ${disabled}>${cls?.comment || cls?.description || ''}</textarea>
            </div>
            
            <div class="mb-3">
                <label class="form-label d-flex justify-content-between align-items-center">
                    <span>Attributes</span>
                    ${!viewOnly ? `<div class="d-flex gap-1">
                        <button type="button" class="btn btn-sm btn-outline-secondary py-0 px-1" onclick="openMetadataAttributePicker()" title="Add from metadata"><i class="bi bi-database"></i></button>
                        <button type="button" class="btn btn-sm btn-outline-primary py-0 px-1" onclick="addSharedEntityAttribute()" title="Add manually"><i class="bi bi-plus"></i></button>
                    </div>` : ''}
                </label>
                <div id="sharedEntityAttributes" class="border rounded p-2" style="background: #f8f9fa; overflow-y: auto;"></div>
            </div>
            
            <!-- Dashboard Section -->
            <div class="mb-3">
                <label class="form-label d-flex justify-content-between align-items-center">
                    <span><i class="bi bi-speedometer2 me-1"></i>Dashboard</span>
                    ${!viewOnly ? '<button type="button" class="btn btn-sm btn-outline-primary py-0 px-1" onclick="openDashboardSelectorModal()"><i class="bi bi-link-45deg"></i> Assign</button>' : ''}
                </label>
                <div id="sharedEntityDashboard" class="border rounded p-2" style="background: #f8f9fa;">
                    <div id="sharedEntityDashboardContent">
                        <small class="text-muted">No dashboard assigned</small>
                    </div>
                </div>
            </div>
            
            <!-- Constraints Section -->
            <div class="mt-4 pt-3 border-top">
                <label class="form-label fw-semibold">
                    <i class="bi bi-sliders me-1"></i>Constraints
                </label>
                
                <!-- Disjoint With -->
                <div class="mb-3">
                    <label class="form-label small text-muted mb-1" title="Classes that share no instances with this class">
                        <i class="bi bi-x-circle me-1"></i>Disjoint With
                    </label>
                    <select class="form-select form-select-sm" id="sharedEntityDisjointWith" ${disabled} multiple size="3" 
                            title="Select classes that cannot share instances with this class">
                        ${otherClassOptions}
                    </select>
                    <div class="form-text small">No instance can belong to both this class and the selected classes</div>
                </div>
                
                <!-- Equivalent To -->
                <div class="mb-3">
                    <label class="form-label small text-muted mb-1" title="Classes that have exactly the same instances as this class">
                        <i class="bi bi-arrows-angle-expand me-1"></i>Equivalent To
                    </label>
                    <select class="form-select form-select-sm" id="sharedEntityEquivalentTo" ${disabled} multiple size="3"
                            title="Select classes that are equivalent to this class">
                        ${otherClassOptions}
                    </select>
                    <div class="form-text small">Classes that have exactly the same instances</div>
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
    renderSharedEntityDashboard(viewOnly);
    if (!viewOnly) initSharedEmojiPicker();

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

function closeSharedEmojiPicker() {
    const picker = panelGetById('sharedEntityEmojiPicker');
    if (picker) picker.style.display = 'none';
}

function initSharedEmojiPicker() {
    const btn = panelGetById('sharedEntityEmojiBtn');
    const picker = panelGetById('sharedEntityEmojiPicker');
    const grid = panelGetById('sharedEntityEmojiGrid');
    const search = panelGetById('sharedEntityEmojiSearch');
    const preview = panelGetById('sharedEntityEmojiPreview');
    const input = panelGetById('sharedEntityIcon');
    
    if (!btn || !picker || !grid) return;
    
    function renderEmojis(filter = '') {
        grid.innerHTML = '';
        for (const [category, emojis] of Object.entries(sharedEmojiCategories)) {
            if (filter && !category.toLowerCase().includes(filter.toLowerCase())) continue;
            const div = document.createElement('div');
            div.className = 'mb-2';
            div.innerHTML = `<small class="text-muted fw-bold">${category}</small>`;
            const row = document.createElement('div');
            row.className = 'd-flex flex-wrap gap-1 mt-1';
            emojis.forEach(emoji => {
                const b = document.createElement('button');
                b.type = 'button';
                b.className = 'btn btn-light btn-sm emoji-btn';
                b.textContent = emoji;
                b.onclick = () => { preview.textContent = emoji; input.value = emoji; picker.style.display = 'none'; };
                row.appendChild(b);
            });
            div.appendChild(row);
            grid.appendChild(div);
        }
    }
    
    btn.onclick = () => {
        picker.style.display = picker.style.display === 'none' ? 'block' : 'none';
        if (picker.style.display === 'block') { renderEmojis(); search.value = ''; }
    };
    search.oninput = (e) => renderEmojis(e.target.value);
    input.oninput = () => { preview.textContent = input.value || OntologyState.defaultClassEmoji; };
}

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

async function saveSharedEntity() {
    const name = panelGetById('sharedEntityName')?.value.trim();
    const icon = panelGetById('sharedEntityIcon')?.value.trim();
    const parent = panelGetById('sharedEntityParent')?.value;
    const description = panelGetById('sharedEntityDescription')?.value.trim();
    
    // Get constraint values from the form
    const disjointWithSelect = panelGetById('sharedEntityDisjointWith');
    const equivalentToSelect = panelGetById('sharedEntityEquivalentTo');
    const disjointWith = disjointWithSelect ? Array.from(disjointWithSelect.selectedOptions).map(opt => opt.value) : [];
    const equivalentTo = equivalentToSelect ? Array.from(equivalentToSelect.selectedOptions).map(opt => opt.value) : [];
    
    if (!name) { showNotification('Please enter an entity name', 'warning'); return; }
    
    const validAttributes = sharedPanelOwnAttributes.filter(a => a.name?.trim()).map(a => ({ name: a.name.trim(), localName: a.name.trim() }));
    
    // Label is the same as name - NO constraints field in class data
    console.log('[SharedPanel] Saving - sharedPanelDashboardParams:', JSON.stringify(sharedPanelDashboardParams));
    
    const classData = { 
        name, 
        localName: name, 
        label: name, 
        emoji: icon, 
        parent: parent || undefined, 
        description, 
        comment: description, 
        dataProperties: validAttributes,
        dashboard: sharedPanelDashboardUrl || undefined,  // Dashboard URL
        dashboardParams: Object.keys(sharedPanelDashboardParams).length > 0 ? sharedPanelDashboardParams : undefined  // Dashboard parameter mappings
        // NOTE: constraints are stored ONLY in session_data/ontology/constraints, not here
    };
    
    console.log('[SharedPanel] Saving - classData.dashboardParams:', JSON.stringify(classData.dashboardParams));
    
    if (sharedPanelEditIndex >= 0) {
        const oldName = OntologyState.config.classes[sharedPanelEditIndex].name;
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
    
    await window.saveConfigToSession();
    
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
    panel.querySelector('#sharedSavePanelBtn').style.display = '';
    
    await renderRelationshipForm(panel, null);
    attachDirtyTracking();
}

async function openRelationshipPanelForEdit(idx, options = {}) {
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
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) {
        console.log('[SharedPanel] Panel not found after openSharedPanel');
        return;
    }
    
    panel.querySelector('#sharedPanelTitle').innerHTML = `<i class="bi bi-pencil"></i> <span id="sharedPanelItemName">${prop.name}</span>`;
    panel.querySelector('#sharedSavePanelBtn').style.display = '';
    
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
    
    openSharedPanel();
    
    const panel = sharedPanelCurrentSection?.querySelector('.shared-detail-panel');
    if (!panel) return;
    
    panel.querySelector('#sharedPanelTitle').innerHTML = `<i class="bi bi-eye"></i> <span id="sharedPanelItemName">${prop.name}</span>`;
    panel.querySelector('#sharedSavePanelBtn').style.display = 'none';
    
    await renderRelationshipForm(panel, prop, true);
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
    
    body.innerHTML = `
        <div id="sharedRelAssignmentLink"></div>
        <form id="sharedRelationshipForm">
            <div class="mb-3">
                <label for="sharedRelName" class="form-label">Name <span class="text-danger">*</span></label>
                <input type="text" class="form-control form-control-sm" id="sharedRelName" value="${prop?.name || ''}" ${disabled} required>
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
                <label for="sharedRelDirection" class="form-label">Direction</label>
                <select class="form-select form-select-sm" id="sharedRelDirection" ${disabled}>
                    <option value="forward">Forward →</option>
                    <option value="reverse">Reverse ←</option>
                </select>
            </div>
            <div class="mb-3">
                <label for="sharedRelDescription" class="form-label">Description</label>
                <textarea class="form-control form-control-sm" id="sharedRelDescription" rows="2" ${disabled}>${prop?.comment || prop?.description || ''}</textarea>
            </div>
            
            <!-- Constraints Section -->
            <div class="mt-4 pt-3 border-top">
                <label class="form-label fw-semibold">
                    <i class="bi bi-sliders me-1"></i>Constraints
                </label>
                
                <!-- Cardinality -->
                <div class="mb-3">
                    <label class="form-label small text-muted mb-1">Cardinality</label>
                    <div class="row g-2">
                        <div class="col-6">
                            <div class="input-group input-group-sm">
                                <span class="input-group-text" title="Minimum cardinality">Min</span>
                                <input type="number" class="form-control" id="sharedRelMinCard" 
                                       value="${minCard}" min="0" placeholder="0" ${disabled}>
                            </div>
                        </div>
                        <div class="col-6">
                            <div class="input-group input-group-sm">
                                <span class="input-group-text" title="Maximum cardinality">Max</span>
                                <input type="number" class="form-control" id="sharedRelMaxCard" 
                                       value="${maxCard}" min="0" placeholder="*" ${disabled}>
                            </div>
                        </div>
                    </div>
                    <div class="form-text small">Leave Max empty for unlimited (*)</div>
                </div>
                
                <!-- Property Characteristics -->
                <div class="mb-2">
                    <label class="form-label small text-muted mb-1">Property Characteristics</label>
                    <div class="d-flex flex-wrap gap-2">
                        <div class="form-check form-check-inline">
                            <input class="form-check-input" type="checkbox" id="sharedRelFunctional" 
                                   ${isFunctional ? 'checked' : ''} ${disabled}>
                            <label class="form-check-label small" for="sharedRelFunctional" 
                                   title="Each subject can have at most one value for this property">
                                Functional
                            </label>
                        </div>
                        <div class="form-check form-check-inline">
                            <input class="form-check-input" type="checkbox" id="sharedRelInverseFunctional" 
                                   ${isInverseFunctional ? 'checked' : ''} ${disabled}>
                            <label class="form-check-label small" for="sharedRelInverseFunctional"
                                   title="Each value can be linked to at most one subject">
                                Inverse Functional
                            </label>
                        </div>
                    </div>
                    <div class="d-flex flex-wrap gap-2 mt-1">
                        <div class="form-check form-check-inline">
                            <input class="form-check-input" type="checkbox" id="sharedRelSymmetric" 
                                   ${isSymmetric ? 'checked' : ''} ${disabled}>
                            <label class="form-check-label small" for="sharedRelSymmetric"
                                   title="If A relates to B, then B also relates to A">
                                Symmetric
                            </label>
                        </div>
                        <div class="form-check form-check-inline">
                            <input class="form-check-input" type="checkbox" id="sharedRelTransitive" 
                                   ${isTransitive ? 'checked' : ''} ${disabled}>
                            <label class="form-check-label small" for="sharedRelTransitive"
                                   title="If A relates to B and B relates to C, then A relates to C">
                                Transitive
                            </label>
                        </div>
                    </div>
                </div>
            </div>
        </form>
    `;
    
    if (prop) {
        panelGetById('sharedRelDomain').value = prop.domain || '';
        panelGetById('sharedRelRange').value = prop.range || '';
        panelGetById('sharedRelDirection').value = prop.direction || 'forward';
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

async function saveSharedRelationship() {
    const name = panelGetById('sharedRelName')?.value.trim();
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
    
    // Label is the same as name - NO constraints field in property data
    const propertyData = { 
        name, 
        localName: name, 
        label: name, 
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
        OntologyState.config.properties[sharedPanelEditIndex] = propertyData;
        showNotification('Relationship updated', 'success', 2000);
    } else {
        OntologyState.config.properties.push(propertyData);
        showNotification('Relationship added', 'success', 2000);
    }
    
    await window.saveConfigToSession();
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

async function saveSharedPanelItem() {
    if (sharedPanelViewOnly) return;
    if (sharedPanelEditType === 'entity') await saveSharedEntity();
    else if (sharedPanelEditType === 'relationship') await saveSharedRelationship();
    sharedPanelDirty = false;
}

// =====================================================
// COMPATIBILITY FUNCTIONS
// =====================================================

function editClassByName(className) {
    const idx = OntologyState.config.classes.findIndex(cls => cls.name === className);
    if (idx >= 0) {
        const canEdit = window.isActiveVersion !== false;
        if (canEdit) openEntityPanelForEdit(idx, { onSave: () => { if (typeof initOntologyMap === 'function' && document.getElementById('map-section')?.classList.contains('active')) initOntologyMap(); } });
        else openEntityPanelForView(idx);
    }
}

function editClass(idx) { openEntityPanelForEdit(idx, { onSave: () => { if (typeof initOntologyMap === 'function' && document.getElementById('map-section')?.classList.contains('active')) initOntologyMap(); } }); }
function viewClass(idx) { openEntityPanelForView(idx); }

function editPropertyByName(propertyName) {
    const idx = OntologyState.config.properties.findIndex(prop => prop.name === propertyName);
    if (idx >= 0) {
        const canEdit = window.isActiveVersion !== false;
        if (canEdit) openRelationshipPanelForEdit(idx, { onSave: () => { if (typeof initOntologyMap === 'function' && document.getElementById('map-section')?.classList.contains('active')) initOntologyMap(); } });
        else openRelationshipPanelForView(idx);
    }
}

function editProperty(idx) { openRelationshipPanelForEdit(idx, { onSave: () => { if (typeof initOntologyMap === 'function' && document.getElementById('map-section')?.classList.contains('active')) initOntologyMap(); } }); }
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
 * Fetches project metadata and shows table list.
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
    tableList.innerHTML = '<div class="text-muted small p-2"><i class="bi bi-hourglass-split me-1"></i>Loading metadata...</div>';
    
    metaAttrPickerModal.show();
    
    try {
        const response = await fetch('/project/metadata', { credentials: 'same-origin' });
        const data = await response.json();
        
        if (!data.success || !data.has_metadata || !data.metadata?.tables?.length) {
            tableList.innerHTML = '<div class="text-muted small p-2"><i class="bi bi-exclamation-circle me-1"></i>No metadata loaded. Load metadata in Project settings first.</div>';
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
        const tableName = table.full_name || table.name;
        // Count how many columns are already attributes
        const alreadyCount = table.columns ? table.columns.filter(c => allAttrs.has(columnToCamelCase(c.name).toLowerCase())).length : 0;
        
        html += `
            <a href="#" class="list-group-item list-group-item-action py-2 px-2" onclick="metaAttrSelectTable('${tableName.replace(/'/g, "\\'")}'); return false;">
                <div class="d-flex justify-content-between align-items-center">
                    <span class="small fw-semibold"><i class="bi bi-table me-1 text-primary"></i>${tableName}</span>
                    <span class="badge bg-secondary">${colCount} col</span>
                </div>
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
        showNotification(`Added ${addedCount} attribute(s) from metadata`, 'success', 2000);
    } else {
        showNotification('All selected columns already exist as attributes', 'info', 2000);
    }
}
