/**
 * OntoBricks - Navbar JavaScript
 * Shared navigation bar functionality for all pages
 */

function showDomainLoading(label) {
    const el = document.getElementById('domainLoadingOverlay');
    if (!el) return;
    const lbl = document.getElementById('domainLoadingLabel');
    if (lbl) lbl.textContent = label || 'Loading domain...';
    el.classList.remove('d-none');
}

function hideDomainLoading() {
    const el = document.getElementById('domainLoadingOverlay');
    if (el) el.classList.add('d-none');
}

// Initialize navbar when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    initNavbar();
});

/**
 * Initialize all navbar components.
 *
 * Uses the consolidated /navbar/state endpoint so a single HTTP
 * round-trip (with a 15 s sessionStorage TTL cache) replaces four
 * separate requests.  Only /settings/permissions/me stays separate
 * because it involves slower external permission checks.
 */
function initNavbar() {
    loadNavbarState();
    showAdminNavItems();
}

/**
 * Load the consolidated navbar state in a single round-trip and
 * apply domain info and warehouse icon to the DOM.
 * Validation indicators have moved to the Domain Validation page.
 */
async function loadNavbarState() {
    try {
        const state = await fetchCached('/navbar/state', 15000);
        applyDomainInfo(state.domain || {});
        applyWarehouseIcon(state.warehouse || {});
    } catch (error) {
        console.error('Error loading navbar state:', error);
        updateDomainMenuVisibility(false);
        updateMenusForDomainStatus(false);
    }
}


async function showAdminNavItems() {
    try {
        const me = await fetchCached('/settings/permissions/me', 60000);
        if (me.is_app_admin === true || me.role === 'admin') {
            document.querySelectorAll('.admin-only-nav').forEach(el => {
                el.style.display = '';
            });
        }
    } catch (_) { /* non-admin or not in app mode */ }
}

/**
 * Refresh all three workflow indicators (Ontology, Mapping, Digital Twin)
 * in the top navbar. Call this single function after any change that could
 * affect one or more of these statuses.
 *
 * Invalidates the sessionStorage cache so the next call hits the server.
 */
async function refreshNavbarIndicators() {
    fetchCachedInvalidate('/navbar/state');
    await loadNavbarState();
}

window.refreshNavbarIndicators = refreshNavbarIndicators;

/**
 * Apply warehouse icon colour from pre-fetched data (no extra HTTP call).
 */
function applyWarehouseIcon(warehouse) {
    const icon = document.getElementById('warehouseStatusIcon');
    const link = document.getElementById('warehouseStatusLink');
    if (!icon) return;
    if (warehouse.warehouse_id) {
        icon.style.color = 'var(--bs-success)';
        if (link) link.title = 'SQL Warehouse configured – click to change';
    } else {
        icon.style.color = 'var(--bs-danger)';
        if (link) link.title = 'No SQL Warehouse selected – click to configure';
    }
}

/**
 * Apply domain name and menu visibility from pre-fetched data.
 */
function applyDomainInfo(data) {
    const currentDomainNameEl = document.getElementById('currentDomainName');
    const domainSectionName = document.getElementById('domainSectionName');

    const stats = data.stats || {};
    const hasContent = (stats.entities > 0) || (stats.entity_mappings > 0);
    const hasCustomName = data.info && data.info.name && data.info.name !== 'NewProject' && data.info.name !== 'NewDomain';
    const hasDomain = hasCustomName || hasContent;

    const domainName = (data.info && data.info.name) ? data.info.name : 'NewDomain';
    const version = (data.info && data.info.version) || '1';

    if (currentDomainNameEl) {
        if (hasDomain) {
            currentDomainNameEl.textContent = `${domainName} V${version}`;
        } else {
            currentDomainNameEl.textContent = 'Domain';
        }
    }

    if (domainSectionName) {
        domainSectionName.textContent = domainName;
    }

    updateDomainMenuVisibility(hasDomain);

    const hasRegistry = data.registry && data.registry.catalog && data.domain_folder;
    updateMenusForDomainStatus(hasRegistry);
}

/**
 * Legacy helper: load domain name by fetching the consolidated state.
 * Kept for callers outside navbar.js (e.g. after domain save).
 */
async function loadDomainName() {
    fetchCachedInvalidate('/navbar/state');
    fetchOnceInvalidate('/domain/info');
    await loadNavbarState();
}

/**
 * Enable/disable navigation menus based on whether the domain is saved to UC
 * @param {boolean} isSaved - Whether the domain is saved to Unity Catalog
 */
function updateMenusForDomainStatus(isSaved) {
    // Top navbar links that require a saved domain
    const navLinks = document.querySelectorAll('.nav-requires-domain');
    navLinks.forEach(link => {
        if (isSaved) {
            link.classList.remove('nav-disabled');
            link.removeAttribute('title');
        } else {
            link.classList.add('nav-disabled');
            link.setAttribute('title', 'Save domain to Unity Catalog first');
        }
    });
    
    // Dropdown menu items that require a saved domain
    const dropdownItems = document.querySelectorAll('.dropdown-requires-domain');
    dropdownItems.forEach(item => {
        if (isSaved) {
            item.classList.remove('disabled');
            item.style.pointerEvents = '';
            item.style.opacity = '';
            item.removeAttribute('title');
        } else {
            item.classList.add('disabled');
            item.style.pointerEvents = 'none';
            item.style.opacity = '0.5';
            item.setAttribute('title', 'Save domain to Unity Catalog first');
        }
    });
    
    // Sidebar links that require a saved domain
    const sidebarLinks = document.querySelectorAll('.sidebar-requires-domain');
    sidebarLinks.forEach(link => {
        if (isSaved) {
            link.classList.remove('sidebar-disabled');
            link.removeAttribute('title');
        } else {
            link.classList.add('sidebar-disabled');
            link.setAttribute('title', 'Save domain to Unity Catalog first');
        }
    });
    
    // Buttons that require a saved domain (e.g., New Version button)
    const buttons = document.querySelectorAll('.btn-requires-domain');
    buttons.forEach(btn => {
        if (isSaved) {
            btn.disabled = false;
            btn.classList.remove('disabled');
            btn.removeAttribute('title');
        } else {
            btn.disabled = true;
            btn.classList.add('disabled');
            btn.setAttribute('title', 'Save domain to Unity Catalog first');
        }
    });
    
    // Show/hide new domain message if applicable
    updateNewDomainMessage(!isSaved);
}

/**
 * Show a message for new domains
 */
function updateNewDomainMessage(showMessage) {
    // Check if we're on the domain information section
    const domainSettingsSection = document.getElementById('information-section');
    if (!domainSettingsSection) return;
    
    // Remove existing message if any
    const existingMsg = document.getElementById('newDomainMessage');
    if (existingMsg) existingMsg.remove();
    
    if (showMessage) {
        // Add message after the section header
        const sectionHeader = domainSettingsSection.querySelector('.section-header');
        if (sectionHeader) {
            const msgHtml = `
                <div id="newDomainMessage" class="alert alert-info d-flex align-items-center mt-3" role="alert">
                    <i class="bi bi-info-circle-fill me-2 fs-5"></i>
                    <div>
                        <strong>New Domain</strong> - Please fill in the <strong>Domain Name</strong> and <strong>Base URI</strong>, 
                        then click <strong>Save Domain to Unity Catalog</strong> to enable all features.
                    </div>
                </div>
            `;
            sectionHeader.insertAdjacentHTML('afterend', msgHtml);
        }
    }
}

/**
 * Called after the domain is saved to UC to enable menus
 */
function enableMenusAfterSave() {
    updateMenusForDomainStatus(true);
}

/**
 * Update domain dropdown menu visibility based on domain state
 * Note: All menu items are now always visible
 */
function updateDomainMenuVisibility(hasDomain) {
    // All domain menu items are now always visible
    // This function is kept for compatibility but does nothing
}


// Legacy aliases kept for callers in other modules
window.refreshOntologyStatus = refreshNavbarIndicators;
window.refreshDigitalTwinStatus = refreshNavbarIndicators;

// showNotification is provided by utils.js via NotificationCenter


// ==========================================
// Domain lifecycle (navbar)
// ==========================================

/**
 * Start a new domain (clears current data)
 */
async function domainNew() {
    const confirmed = await showConfirmDialog({
        title: 'New Domain',
        message: 'Start a new domain? This will clear all current ontology, design, and mapping data.',
        confirmText: 'Start New',
        confirmClass: 'btn-warning',
        icon: 'file-earmark-plus'
    });
    if (!confirmed) return;
    
    try {
        const response = await fetch('/domain/clear', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('New domain started', 'success');
            // Navigate to Domain settings page
            setTimeout(() => {
                window.location.href = '/domain/#information';
            }, 1000);
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        console.error('Error creating new domain:', error);
        showNotification('Failed to create new domain: ' + error.message, 'error');
    }
}

/**
 * Save domain to Unity Catalog Volume
 * Volume path uses the domain folder; file name = version number
 */
async function domainSave() {
    try {
        // First, save domain info from form if on the domain page
        await saveDomainInfoBeforeSave();
        
        // Save the current design layout if on the design page
        if (typeof ontologyDesigner !== 'undefined' && ontologyDesigner) {
            try {
                const layoutData = ontologyDesigner.toJSON();
                
                // Strip icon and description from entities - they come from ontology
                const cleanedLayout = {
                    ...layoutData,
                    entities: (layoutData.entities || []).map(entity => ({
                        id: entity.id,
                        name: entity.name,
                        x: entity.x,
                        y: entity.y,
                        properties: entity.properties,
                        color: entity.color
                        // icon and description are intentionally NOT saved
                    })),
                    relationships: layoutData.relationships,
                    inheritances: layoutData.inheritances,
                    visibility: layoutData.visibility
                };
                
                await fetch('/domain/design-views/save-current', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(cleanedLayout),
                    credentials: 'same-origin'
                });
            } catch (e) {
                console.log('No design layout to save or not on design page');
            }
        }
        
        // Show catalog/schema selection dialog
        showDomainSaveDialog();
    } catch (error) {
        console.error('Error preparing save:', error);
        showNotification('Failed to prepare save: ' + error.message, 'error');
    }
}

/**
 * Save domain info from form fields before saving to UC
 */
async function saveDomainInfoBeforeSave() {
    // Check if we're on a page with domain info form fields
    const nameEl = document.getElementById('domainName');
    const descEl = document.getElementById('domainDescription');
    const authorEl = document.getElementById('domainAuthor');
    const baseUriEl = document.getElementById('domainBaseUri');
    const llmEndpointEl = document.getElementById('domainLlmEndpoint');
    const versionEl = document.getElementById('domainVersionSelect');

    // If any form fields exist, save the domain info
    if (nameEl || descEl || authorEl || baseUriEl || llmEndpointEl) {
        const deltaCatalog = document.getElementById('triplestoreLocationWidget_catalog')?.value || '';
        const deltaSchema = document.getElementById('triplestoreLocationWidget_schema')?.value || '';
        const deltaTableName = document.getElementById('domainTriplestoreTableName')?.value.trim() || '';
        const hasDelta = deltaCatalog || deltaSchema || deltaTableName;

        const domainInfoPayload = {
            name: nameEl ? nameEl.value.trim() : undefined,
            description: descEl ? descEl.value.trim() : undefined,
            author: authorEl ? authorEl.value.trim() : undefined,
            base_uri: baseUriEl ? baseUriEl.value.trim() : undefined,
            base_uri_auto: (typeof _baseUriAutoMode !== 'undefined') ? _baseUriAutoMode : undefined,
            llm_endpoint: llmEndpointEl ? llmEndpointEl.value : undefined,
            
            version: versionEl ? versionEl.value : undefined,
            delta: hasDelta ? {
                catalog: deltaCatalog,
                schema: deltaSchema,
                table_name: deltaTableName,
            } : undefined,
        };
        
        // Remove undefined values
        Object.keys(domainInfoPayload).forEach(key => {
            if (domainInfoPayload[key] === undefined) delete domainInfoPayload[key];
        });
        
        // Only save if we have something to save
        if (Object.keys(domainInfoPayload).length > 0) {
            console.log('[Domain] Auto-saving domain info before UC save:', domainInfoPayload);
            try {
                await fetch('/domain/info', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(domainInfoPayload),
                    credentials: 'same-origin'
                });
            } catch (e) {
                console.warn('Could not auto-save domain info:', e);
            }
        }
    }
}

/**
 * Show confirmation dialog before saving to the registry.
 */
async function showDomainSaveDialog() {
    const modalHtml = `
        <div class="modal fade" id="domainSaveModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-cloud-upload"></i> Save Domain to Registry</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div id="saveRegistryInfo" class="mb-3">
                            <span class="spinner-border spinner-border-sm me-1"></span> Checking registry...
                        </div>
                        <div class="alert alert-info small">
                            <i class="bi bi-info-circle"></i>
                            <strong>Domain:</strong> <span id="saveDomainName">Loading...</span><br>
                            <strong>Version:</strong> <span id="saveDomainVersion">Loading...</span>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="button" class="btn btn-primary" id="btnConfirmSave" disabled>
                            <i class="bi bi-cloud-upload"></i> Save
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `;

    const existingModal = document.getElementById('domainSaveModal');
    if (existingModal) existingModal.remove();
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('domainSaveModal'));
    modal.show();

    try {
        const infoData = await fetchOnce('/domain/info');
        if (infoData.success) {
            document.getElementById('saveDomainName').textContent = infoData.info.name || 'NewDomain';
            document.getElementById('saveDomainVersion').textContent = infoData.info.version || '1';
        }
    } catch (_) { /* ignore */ }

    // Check registry
    try {
        const regResp = await fetch('/settings/registry', { credentials: 'same-origin' });
        const reg = await regResp.json();
        const infoDiv = document.getElementById('saveRegistryInfo');
        if (reg.configured) {
            infoDiv.innerHTML = `<div class="alert alert-success small mb-0"><i class="bi bi-archive me-1"></i> Registry: <strong>${reg.catalog}.${reg.schema}.${reg.volume}</strong></div>`;
            document.getElementById('btnConfirmSave').disabled = false;
        } else {
            infoDiv.innerHTML = '<div class="alert alert-warning small mb-0"><i class="bi bi-exclamation-triangle me-1"></i> Registry not configured. <a href="/settings">Go to Settings</a></div>';
        }
    } catch (e) {
        document.getElementById('saveRegistryInfo').innerHTML = '<div class="alert alert-danger small mb-0">Error checking registry</div>';
    }

    document.getElementById('btnConfirmSave').addEventListener('click', async () => {
        modal.hide();
        await doDomainSave();
    });
}

async function doDomainSave() {
    try {
        showNotification('Saving domain to registry...', 'info', 5000);
        const response = await fetch('/domain/save-to-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({}),
            credentials: 'same-origin'
        });
        const data = await response.json();
        if (data.success) {
            showNotification(data.message || 'Domain saved successfully!', 'success');
            enableMenusAfterSave();
            await refreshNavbarIndicators();
            loadDomainName();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

/**
 * Load domain from Unity Catalog Volume
 */
async function domainLoad() {
    // Check if warehouse is selected first
    try {
        const configResponse = await fetch('/settings/current', { credentials: 'same-origin' });
        const configData = await configResponse.json();
        
        if (!configData.warehouse_id) {
            showWarehouseRequiredDialog();
            return;
        }
    } catch (error) {
        console.error('Error checking warehouse:', error);
    }
    
    showDomainLoadDialog();
}

/**
 * Show dialog asking user to select a SQL Warehouse first
 */
function showWarehouseRequiredDialog() {
    const modalHtml = `
        <div class="modal fade" id="warehouseRequiredModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header bg-warning">
                        <h5 class="modal-title"><i class="bi bi-exclamation-triangle"></i> SQL Warehouse Required</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <p>To load a domain from Unity Catalog, you need to select a SQL Warehouse first.</p>
                        <p class="text-muted mb-0">Go to <strong>Settings</strong> to select an available SQL Warehouse.</p>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                        <a href="/settings" class="btn btn-primary">
                            <i class="bi bi-gear"></i> Go to Settings
                        </a>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    // Remove existing modal if present
    const existingModal = document.getElementById('warehouseRequiredModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    // Add modal to document
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    
    // Show the modal
    const modal = new bootstrap.Modal(document.getElementById('warehouseRequiredModal'));
    modal.show();
}


/**
 * Show dialog to pick a domain and version from the registry.
 */
function showDomainLoadDialog() {
    const modalHtml = `
        <div class="modal fade" id="domainLoadModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-cloud-download"></i> Load Domain from Registry</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div id="loadRegistryInfo" class="mb-3">
                            <span class="spinner-border spinner-border-sm me-1"></span> Checking registry...
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Domain</label>
                            <select class="form-select" id="loadDomainSelect" disabled>
                                <option value="">Loading domains...</option>
                            </select>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Version</label>
                            <select class="form-select" id="loadVersionSelect" disabled>
                                <option value="">Select domain first</option>
                            </select>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="button" class="btn btn-success" id="btnConfirmLoad" disabled>
                            <i class="bi bi-cloud-download"></i> Load
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `;

    const existingModal = document.getElementById('domainLoadModal');
    if (existingModal) existingModal.remove();
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('domainLoadModal'));
    modal.show();

    loadDomainsFromRegistry();

    document.getElementById('btnConfirmLoad').addEventListener('click', async () => {
        const domainSlug = document.getElementById('loadDomainSelect').value;
        const version = document.getElementById('loadVersionSelect').value;
        if (!domainSlug || !version) {
            showNotification('Please select a domain and version', 'warning');
            return;
        }
        modal.hide();
        await doDomainLoad(domainSlug, version);
    });
}

async function loadDomainsFromRegistry() {
    const infoDiv = document.getElementById('loadRegistryInfo');
    const domainSelect = document.getElementById('loadDomainSelect');
    const versionSelect = document.getElementById('loadVersionSelect');
    try {
        const regResp = await fetch('/settings/registry', { credentials: 'same-origin' });
        const reg = await regResp.json();
        if (!reg.configured) {
            infoDiv.innerHTML = '<div class="alert alert-warning small mb-0"><i class="bi bi-exclamation-triangle me-1"></i> Registry not configured. <a href="/settings">Go to Settings</a></div>';
            return;
        }
        infoDiv.innerHTML = `<div class="alert alert-success small mb-0"><i class="bi bi-archive me-1"></i> Registry: <strong>${reg.catalog}.${reg.schema}.${reg.volume}</strong></div>`;

        domainSelect.innerHTML = '<option value="">Loading domains...</option>';
        const domainListResp = await fetch('/domain/list-projects', { credentials: 'same-origin' });
        const domainListData = await domainListResp.json();

        domainSelect.innerHTML = '<option value="">Select domain...</option>';
        domainSelect.disabled = false;

        const _domainList = domainListData.domains || domainListData.projects || [];
        if (domainListData.success && _domainList.length > 0) {
            _domainList.forEach(d => {
                domainSelect.innerHTML += `<option value="${d}">${d}</option>`;
            });
        } else {
            domainSelect.innerHTML = '<option value="">No domains found</option>';
        }

        domainSelect.onchange = () => loadVersionsForDomainFromRegistry(domainSelect.value);
    } catch (e) {
        infoDiv.innerHTML = '<div class="alert alert-danger small mb-0">Error loading registry</div>';
    }
}

async function loadVersionsForDomainFromRegistry(domainName) {
    const versionSelect = document.getElementById('loadVersionSelect');
    document.getElementById('btnConfirmLoad').disabled = true;

    if (!domainName) {
        versionSelect.disabled = true;
        versionSelect.innerHTML = '<option value="">Select domain first</option>';
        return;
    }

    try {
        versionSelect.innerHTML = '<option value="">Loading versions...</option>';
        const resp = await fetch(`/domain/list-versions?domain_name=${encodeURIComponent(domainName)}`, { credentials: 'same-origin' });
        const data = await resp.json();

        versionSelect.innerHTML = '<option value="">Select version...</option>';
        versionSelect.disabled = false;

        if (data.success && data.versions && data.versions.length > 0) {
            const sorted = data.versions.sort((a, b) => b.localeCompare(a, undefined, { numeric: true }));
            versionSelect.innerHTML = '';
            sorted.forEach((ver, idx) => {
                const label = idx === 0 ? `${ver} (Latest)` : `${ver} (Read-Only)`;
                versionSelect.innerHTML += `<option value="${ver}">${label}</option>`;
            });
            versionSelect.value = sorted[0];
            document.getElementById('btnConfirmLoad').disabled = false;
        } else {
            versionSelect.innerHTML = '<option value="">No versions found</option>';
        }

        versionSelect.onchange = () => {
            document.getElementById('btnConfirmLoad').disabled = !versionSelect.value;
        };
    } catch (e) {
        versionSelect.innerHTML = '<option value="">Error loading versions</option>';
    }
}

async function doDomainLoad(domainSlug, version) {
    showDomainLoading(`Loading ${domainSlug}...`);
    try {
        const response = await fetch('/domain/load-from-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain: domainSlug, version }),
            credentials: 'same-origin'
        });
        const data = await response.json();
        if (data.success) {
            showNotification(data.message || 'Domain loaded successfully!', 'success');
            setTimeout(() => location.reload(), 1000);
        } else {
            hideDomainLoading();
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        hideDomainLoading();
        showNotification('Error: ' + error.message, 'error');
    }
}

/**
 * Handle the selected domain export file - parse and check for versions
 */
async function handleDomainFile(input) {
    const file = input.files[0];
    if (!file) return;
    
    try {
        // Read the file content first
        const content = await file.text();
        let importedDomain;
        
        try {
            importedDomain = JSON.parse(content);
        } catch (e) {
            showNotification('Error: Invalid JSON in domain file', 'error');
            input.value = '';
            return;
        }
        
        // Check if file has versions
        if (importedDomain.versions && Object.keys(importedDomain.versions).length > 0) {
            const versions = Object.keys(importedDomain.versions).sort().reverse();
            
            if (versions.length > 1) {
                // Show version selection dialog
                showVersionSelectionDialog(versions, importedDomain, async (selectedVersion) => {
                    await importDomainWithVersion(importedDomain, selectedVersion);
                });
            } else {
                // Only one version, load it directly
                await importDomainWithVersion(importedDomain, versions[0]);
            }
        } else {
            // Legacy format without versions - load directly
            await importDomainDirect(importedDomain);
        }
    } catch (error) {
        console.error('Error loading domain file:', error);
        showNotification('Failed to load domain: ' + error.message, 'error');
    }
    
    // Reset file input
    input.value = '';
}

/**
 * Show version selection dialog
 */
function showVersionSelectionDialog(versions, importedDomain, onSelect) {
    // Create modal if it doesn't exist
    let modal = document.getElementById('versionSelectModal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'versionSelectModal';
        modal.className = 'modal fade';
        modal.tabIndex = -1;
        modal.innerHTML = `
            <div class="modal-dialog modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-clock-history me-2"></i>Select Version</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <p class="text-muted mb-3">This domain contains multiple versions. Select the version to load:</p>
                        <div id="versionSelectList" class="list-group"></div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
    }
    
    // Populate version list
    const listEl = document.getElementById('versionSelectList');
    listEl.innerHTML = '';
    
    versions.forEach((version, index) => {
        const versionData = importedDomain.versions[version];
        const classCount = versionData.ontology?.classes?.length || 0;
        const propCount = versionData.ontology?.properties?.length || 0;
        const mappingCount = versionData.assignment?.entities?.length || 0;
        
        const item = document.createElement('a');
        item.href = '#';
        item.className = 'list-group-item list-group-item-action d-flex justify-content-between align-items-center';
        if (index === 0) item.classList.add('active');
        
        item.innerHTML = `
            <div>
                <strong>Version ${version}</strong>
                <small class="text-muted d-block">${classCount} entities, ${propCount} relationships, ${mappingCount} assignments</small>
            </div>
            <i class="bi bi-chevron-right"></i>
        `;
        
        item.onclick = (e) => {
            e.preventDefault();
            bootstrap.Modal.getInstance(modal).hide();
            onSelect(version);
        };
        
        listEl.appendChild(item);
    });
    
    // Show modal
    const bsModal = new bootstrap.Modal(modal);
    bsModal.show();
}

/**
 * Import domain export with a specific version
 */
async function importDomainWithVersion(importedDomain, version) {
    showDomainLoading(`Loading version ${version}...`);
    try {
        const response = await fetch('/domain/import', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain: importedDomain, version: version }),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        handleImportResponse(data);
    } catch (error) {
        hideDomainLoading();
        console.error('Error importing domain:', error);
        showNotification('Failed to import domain: ' + error.message, 'error');
    }
}

/**
 * Import domain export directly (legacy format)
 */
async function importDomainDirect(importedDomain) {
    showDomainLoading('Loading domain...');
    try {
        const response = await fetch('/domain/import', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain: importedDomain }),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        handleImportResponse(data);
    } catch (error) {
        hideDomainLoading();
        console.error('Error importing domain:', error);
        showNotification('Failed to import domain: ' + error.message, 'error');
    }
}

/**
 * Handle import response
 */
function handleImportResponse(data) {
    if (data.success) {
        let msg = 'Domain loaded';
        if (data.version) msg += ` (v${data.version})`;
        msg += '! ' + data.stats.entities + ' entities, ' + 
               data.stats.relationships + ' relationships, ' + 
               data.stats.mappings + ' assignments';
        if (data.generated) {
            const genParts = [];
            if (data.generated.owl) genParts.push('OWL');
            if (data.generated.r2rml) genParts.push('R2RML');
            if (genParts.length > 0) {
                msg += ' (' + genParts.join(' & ') + ' generated)';
            }
        }
        showNotification(msg, 'success');
        setTimeout(() => location.reload(), 1500);
    } else {
        hideDomainLoading();
        showNotification('Error loading domain: ' + data.message, 'error');
    }
}

// Expose domain actions globally
window.domainNew = domainNew;
window.domainSave = domainSave;
window.domainLoad = domainLoad;
window.showDomainSaveDialog = showDomainSaveDialog;
window.showDomainLoadDialog = showDomainLoadDialog;
window.doDomainSave = doDomainSave;
window.doDomainLoad = doDomainLoad;
window.checkDomainSavedStatus = loadDomainName;
window.enableMenusAfterSave = enableMenusAfterSave;
window.updateMenusForDomainStatus = updateMenusForDomainStatus;
window.showDomainLoading = showDomainLoading;
window.hideDomainLoading = hideDomainLoading;
