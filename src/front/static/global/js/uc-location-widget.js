/**
 * OntoBricks - UC Location Widget
 * Shared component for selecting Catalog.Schema across the application.
 * 
 * Usage:
 * 1. Include the widget HTML in your template:
 *    <div id="myWidget" class="uc-location-widget" data-callback="onLocationChanged"></div>
 * 
 * 2. Initialize the widget:
 *    UCLocationWidget.init('myWidget', { onSelect: (catalog, schema) => { ... } });
 * 
 * 3. Or use the helper to create a widget programmatically:
 *    UCLocationWidget.create(container, 'myWidgetId', { onSelect: callback });
 */

const UCLocationWidget = (function() {
    // Cache for catalogs and schemas to avoid repeated API calls
    let catalogsCache = null;
    let schemasCache = {};
    let projectLocationCache = null;
    
    // Widget instances
    const widgets = {};
    
    /**
     * Load project UC location (cached)
     * Falls back to metadata location if project UC location is not set
     */
    async function loadProjectLocation(forceRefresh = false) {
        if (projectLocationCache && projectLocationCache.catalog && !forceRefresh) {
            return projectLocationCache;
        }
        
        try {
            const response = await fetch('/project/info', { credentials: 'same-origin' });
            const data = await response.json();
            if (data.success && data.registry && data.registry.catalog && data.registry.schema) {
                projectLocationCache = data.registry;
                return projectLocationCache;
            }
            
            // Fallback: try to get from metadata if project UC location is not set
            // Extract catalog/schema from first table's full_name
            const metadataResponse = await fetch('/project/metadata', { credentials: 'same-origin' });
            const metadataData = await metadataResponse.json();
            if (metadataData.success && metadataData.has_metadata && metadataData.metadata) {
                const metadata = metadataData.metadata;
                const tables = metadata.tables || [];
                if (tables.length > 0 && tables[0].full_name) {
                    const parts = tables[0].full_name.split('.');
                    if (parts.length >= 2) {
                        projectLocationCache = {
                            catalog: parts[0],
                            schema: parts[1],
                            volume: ''
                        };
                        return projectLocationCache;
                    }
                }
            }
        } catch (error) {
            console.error('[UCLocationWidget] Error loading project location:', error);
        }
        return { catalog: '', schema: '', volume: '' };
    }
    
    /**
     * Load catalogs (cached)
     */
    async function loadCatalogs(forceRefresh = false) {
        if (catalogsCache && !forceRefresh) {
            return catalogsCache;
        }
        
        try {
            const response = await fetch('/settings/catalogs', { credentials: 'same-origin' });
            const data = await response.json();
            if (data.catalogs) {
                catalogsCache = data.catalogs;
                return catalogsCache;
            }
        } catch (error) {
            console.error('[UCLocationWidget] Error loading catalogs:', error);
        }
        return [];
    }
    
    /**
     * Load schemas for a catalog (cached)
     */
    async function loadSchemas(catalog, forceRefresh = false) {
        if (!catalog) return [];
        
        if (schemasCache[catalog] && !forceRefresh) {
            return schemasCache[catalog];
        }
        
        try {
            const response = await fetch(`/settings/schemas/${catalog}`, { credentials: 'same-origin' });
            const data = await response.json();
            if (data.schemas) {
                schemasCache[catalog] = data.schemas;
                return schemasCache[catalog];
            }
        } catch (error) {
            console.error('[UCLocationWidget] Error loading schemas:', error);
        }
        return [];
    }
    
    /**
     * Create the widget HTML
     */
    function createWidgetHTML(widgetId, options = {}) {
        const label = options.label || 'Unity Catalog Location';
        const showLabel = options.showLabel !== false;
        
        return `
            <div class="uc-location-widget-container">
                ${showLabel ? `<label class="form-label">${label}</label>` : ''}
                <div class="input-group">
                    <span class="input-group-text bg-light">
                        <i class="bi bi-database"></i>
                    </span>
                    <input type="text" class="form-control" id="${widgetId}_display" 
                           readonly placeholder="No location set" 
                           style="background-color: #f8f9fa; cursor: pointer;"
                           onclick="UCLocationWidget.openModal('${widgetId}')">
                    <button type="button" class="btn btn-outline-secondary" 
                            onclick="UCLocationWidget.openModal('${widgetId}')"
                            title="Change Catalog.Schema">
                        <i class="bi bi-pencil-square"></i>
                    </button>
                </div>
                <input type="hidden" id="${widgetId}_catalog" value="">
                <input type="hidden" id="${widgetId}_schema" value="">
            </div>
        `;
    }
    
    /**
     * Create and show the selection modal
     */
    function createModal() {
        // Check if modal already exists
        if (document.getElementById('ucLocationModal')) {
            return;
        }
        
        const modalHtml = `
            <div class="modal fade" id="ucLocationModal" tabindex="-1">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">
                                <i class="bi bi-database me-2"></i>Select Catalog & Schema
                            </h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            <!-- Use Project Location Option -->
                            <div id="ucLocationProjectOption" class="border rounded d-flex align-items-center mb-3 p-2 bg-light" style="display: none !important;">
                                <div class="flex-grow-1">
                                    <strong><i class="bi bi-folder2"></i> Project Location:</strong>
                                    <span id="ucLocationProjectValue" class="fw-semibold ms-1">-</span>
                                </div>
                                <button type="button" class="btn btn-sm btn-primary" id="ucLocationUseProjectBtn">
                                    Use This
                                </button>
                            </div>
                            
                            <div class="row g-3">
                                <!-- Catalog Selection -->
                                <div class="col-12">
                                    <label for="ucLocationModalCatalog" class="form-label">Catalog</label>
                                    <select class="form-select" id="ucLocationModalCatalog">
                                        <option value="">Loading catalogs...</option>
                                    </select>
                                </div>
                                
                                <!-- Schema Selection -->
                                <div class="col-12">
                                    <label for="ucLocationModalSchema" class="form-label">Schema</label>
                                    <select class="form-select" id="ucLocationModalSchema" disabled>
                                        <option value="">Select catalog first</option>
                                    </select>
                                </div>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                            <button type="button" class="btn btn-primary" id="ucLocationModalConfirm" disabled>
                                <i class="bi bi-check-lg"></i> Select
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        
        // Add event listeners
        document.getElementById('ucLocationModalCatalog').addEventListener('change', onCatalogChange);
        document.getElementById('ucLocationModalSchema').addEventListener('change', onSchemaChange);
        document.getElementById('ucLocationModalConfirm').addEventListener('click', onConfirm);
        document.getElementById('ucLocationUseProjectBtn').addEventListener('click', onUseProjectLocation);
    }
    
    /**
     * Handle catalog change in modal
     */
    async function onCatalogChange() {
        const catalogSelect = document.getElementById('ucLocationModalCatalog');
        const schemaSelect = document.getElementById('ucLocationModalSchema');
        const confirmBtn = document.getElementById('ucLocationModalConfirm');
        
        const catalog = catalogSelect.value;
        
        // Reset schema
        schemaSelect.innerHTML = '<option value="">Loading schemas...</option>';
        schemaSelect.disabled = true;
        confirmBtn.disabled = true;
        
        if (!catalog) {
            schemaSelect.innerHTML = '<option value="">Select catalog first</option>';
            return;
        }
        
        // Load schemas
        const schemas = await loadSchemas(catalog);
        
        if (schemas.length > 0) {
            schemaSelect.innerHTML = '<option value="">Select schema...</option>';
            schemas.forEach(schema => {
                schemaSelect.innerHTML += `<option value="${schema}">${schema}</option>`;
            });
            schemaSelect.disabled = false;
        } else {
            schemaSelect.innerHTML = '<option value="">No schemas available</option>';
        }
    }
    
    /**
     * Handle schema change in modal
     */
    function onSchemaChange() {
        const catalog = document.getElementById('ucLocationModalCatalog').value;
        const schema = document.getElementById('ucLocationModalSchema').value;
        const confirmBtn = document.getElementById('ucLocationModalConfirm');
        
        confirmBtn.disabled = !(catalog && schema);
    }
    
    /**
     * Handle use project location button
     */
    function onUseProjectLocation() {
        if (!projectLocationCache) return;
        
        const catalogSelect = document.getElementById('ucLocationModalCatalog');
        const schemaSelect = document.getElementById('ucLocationModalSchema');
        
        // Set catalog
        catalogSelect.value = projectLocationCache.catalog;
        
        // Trigger schema load then set schema
        onCatalogChange().then(() => {
            setTimeout(() => {
                schemaSelect.value = projectLocationCache.schema;
                onSchemaChange();
            }, 100);
        });
    }
    
    /**
     * Handle confirm button
     */
    function onConfirm() {
        const catalog = document.getElementById('ucLocationModalCatalog').value;
        const schema = document.getElementById('ucLocationModalSchema').value;
        
        if (!catalog || !schema) return;
        
        // Get the current widget ID from modal data
        const modal = document.getElementById('ucLocationModal');
        const widgetId = modal.dataset.currentWidget;
        
        if (widgetId && widgets[widgetId]) {
            setWidgetValue(widgetId, catalog, schema);
            
            // Call the onSelect callback
            if (widgets[widgetId].onSelect) {
                widgets[widgetId].onSelect(catalog, schema);
            }
        }
        
        // Close modal
        bootstrap.Modal.getInstance(modal).hide();
    }
    
    /**
     * Set widget display value
     */
    function setWidgetValue(widgetId, catalog, schema) {
        const displayInput = document.getElementById(`${widgetId}_display`);
        const catalogInput = document.getElementById(`${widgetId}_catalog`);
        const schemaInput = document.getElementById(`${widgetId}_schema`);
        
        if (displayInput) {
            displayInput.value = catalog && schema ? `${catalog}.${schema}` : '';
            displayInput.placeholder = catalog && schema ? '' : 'No location set';
        }
        if (catalogInput) catalogInput.value = catalog || '';
        if (schemaInput) schemaInput.value = schema || '';
    }
    
    /**
     * Get widget current value
     */
    function getWidgetValue(widgetId) {
        const catalogInput = document.getElementById(`${widgetId}_catalog`);
        const schemaInput = document.getElementById(`${widgetId}_schema`);
        
        return {
            catalog: catalogInput ? catalogInput.value : '',
            schema: schemaInput ? schemaInput.value : ''
        };
    }
    
    /**
     * Open the selection modal for a widget
     */
    async function openModal(widgetId) {
        createModal();
        
        const modal = document.getElementById('ucLocationModal');
        modal.dataset.currentWidget = widgetId;
        
        const catalogSelect = document.getElementById('ucLocationModalCatalog');
        const schemaSelect = document.getElementById('ucLocationModalSchema');
        const confirmBtn = document.getElementById('ucLocationModalConfirm');
        const projectOption = document.getElementById('ucLocationProjectOption');
        const projectValue = document.getElementById('ucLocationProjectValue');
        
        // Reset state
        catalogSelect.innerHTML = '<option value="">Loading catalogs...</option>';
        schemaSelect.innerHTML = '<option value="">Select catalog first</option>';
        schemaSelect.disabled = true;
        confirmBtn.disabled = true;
        
        // Show modal
        const bsModal = new bootstrap.Modal(modal);
        bsModal.show();
        
        // Load project location and catalogs in parallel
        const [projectLoc, catalogs] = await Promise.all([
            loadProjectLocation(),
            loadCatalogs()
        ]);
        
        // Show project option if available
        if (projectLoc && projectLoc.catalog && projectLoc.schema) {
            projectOption.style.display = 'flex';
            projectValue.textContent = `${projectLoc.catalog}.${projectLoc.schema}`;
        } else {
            projectOption.style.display = 'none';
        }
        
        // Populate catalogs
        if (catalogs.length > 0) {
            catalogSelect.innerHTML = '<option value="">Select catalog...</option>';
            catalogs.forEach(catalog => {
                catalogSelect.innerHTML += `<option value="${catalog}">${catalog}</option>`;
            });
            
            // Pre-select current value if any
            const currentValue = getWidgetValue(widgetId);
            if (currentValue.catalog) {
                catalogSelect.value = currentValue.catalog;
                await onCatalogChange();
                if (currentValue.schema) {
                    schemaSelect.value = currentValue.schema;
                    onSchemaChange();
                }
            }
        } else {
            catalogSelect.innerHTML = '<option value="">No catalogs available</option>';
        }
    }
    
    /**
     * Initialize a widget
     */
    async function init(widgetId, options = {}) {
        const container = document.getElementById(widgetId);
        if (!container) {
            console.error(`[UCLocationWidget] Container not found: ${widgetId}`);
            return;
        }
        
        // Store widget options
        widgets[widgetId] = {
            container: container,
            onSelect: options.onSelect || null,
            autoLoadProject: options.autoLoadProject !== false
        };
        
        // Create widget HTML if container is empty
        if (!container.innerHTML.trim()) {
            container.innerHTML = createWidgetHTML(widgetId, options);
        }
        
        // Auto-load project location if enabled
        if (widgets[widgetId].autoLoadProject) {
            const projectLoc = await loadProjectLocation();
            if (projectLoc && projectLoc.catalog && projectLoc.schema) {
                setWidgetValue(widgetId, projectLoc.catalog, projectLoc.schema);
            }
        }
        
        return widgets[widgetId];
    }
    
    /**
     * Create a widget programmatically
     */
    async function create(containerElement, widgetId, options = {}) {
        containerElement.id = widgetId;
        containerElement.innerHTML = createWidgetHTML(widgetId, options);
        return await init(widgetId, options);
    }
    
    /**
     * Refresh project location cache
     */
    async function refreshProjectLocation() {
        projectLocationCache = null;
        return await loadProjectLocation(true);
    }
    
    /**
     * Clear all caches
     */
    function clearCache() {
        catalogsCache = null;
        schemasCache = {};
        projectLocationCache = null;
    }
    
    // Public API
    return {
        init,
        create,
        openModal,
        getValue: getWidgetValue,
        setValue: setWidgetValue,
        refreshProjectLocation,
        clearCache,
        loadProjectLocation,
        loadCatalogs,
        loadSchemas
    };
})();

// Make available globally
window.UCLocationWidget = UCLocationWidget;
