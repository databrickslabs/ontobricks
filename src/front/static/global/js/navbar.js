/**
 * OntoBricks - Navbar JavaScript
 * Shared navigation bar functionality for all pages
 */

function showProjectLoading(label) {
    const el = document.getElementById('projectLoadingOverlay');
    if (!el) return;
    const lbl = document.getElementById('projectLoadingLabel');
    if (lbl) lbl.textContent = label || 'Loading project...';
    el.classList.remove('d-none');
}

function hideProjectLoading() {
    const el = document.getElementById('projectLoadingOverlay');
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
 * apply project info and warehouse icon to the DOM.
 * Validation indicators have moved to the Project Validation page.
 */
async function loadNavbarState() {
    try {
        const state = await fetchCached('/navbar/state', 15000);
        applyProjectInfo(state.project || {});
        applyWarehouseIcon(state.warehouse || {});
    } catch (error) {
        console.error('Error loading navbar state:', error);
        updateProjectMenuVisibility(false);
        updateMenusForProjectStatus(false);
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
 * Apply project name and menu visibility from pre-fetched data.
 */
function applyProjectInfo(data) {
    const projectNameEl = document.getElementById('currentProjectName');
    const projectSectionName = document.getElementById('projectSectionName');

    const stats = data.stats || {};
    const hasContent = (stats.entities > 0) || (stats.entity_mappings > 0);
    const hasCustomName = data.info && data.info.name && data.info.name !== 'NewProject';
    const hasProject = hasCustomName || hasContent;

    const projectName = (data.info && data.info.name) ? data.info.name : 'NewProject';
    const version = (data.info && data.info.version) || '1';

    if (projectNameEl) {
        if (hasProject) {
            projectNameEl.textContent = `${projectName} V${version}`;
        } else {
            projectNameEl.textContent = 'Project';
        }
    }

    if (projectSectionName) {
        projectSectionName.textContent = projectName;
    }

    updateProjectMenuVisibility(hasProject);

    const hasRegistry = data.registry && data.registry.catalog && data.project_folder;
    updateMenusForProjectStatus(hasRegistry);
}

/**
 * Legacy helper: load project name by fetching the consolidated state.
 * Kept for callers outside navbar.js (e.g. after project save).
 */
async function loadProjectName() {
    fetchCachedInvalidate('/navbar/state');
    fetchOnceInvalidate('/project/info');
    await loadNavbarState();
}

/**
 * Enable/disable navigation menus based on whether project is saved to UC
 * @param {boolean} isSaved - Whether the project is saved to Unity Catalog
 */
function updateMenusForProjectStatus(isSaved) {
    // Top navbar links that require project
    const navLinks = document.querySelectorAll('.nav-requires-project');
    navLinks.forEach(link => {
        if (isSaved) {
            link.classList.remove('nav-disabled');
            link.removeAttribute('title');
        } else {
            link.classList.add('nav-disabled');
            link.setAttribute('title', 'Save project to Unity Catalog first');
        }
    });
    
    // Dropdown menu items that require project
    const dropdownItems = document.querySelectorAll('.dropdown-requires-project');
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
            item.setAttribute('title', 'Save project to Unity Catalog first');
        }
    });
    
    // Sidebar links that require project
    const sidebarLinks = document.querySelectorAll('.sidebar-requires-project');
    sidebarLinks.forEach(link => {
        if (isSaved) {
            link.classList.remove('sidebar-disabled');
            link.removeAttribute('title');
        } else {
            link.classList.add('sidebar-disabled');
            link.setAttribute('title', 'Save project to Unity Catalog first');
        }
    });
    
    // Buttons that require project (e.g., New Version button)
    const buttons = document.querySelectorAll('.btn-requires-project');
    buttons.forEach(btn => {
        if (isSaved) {
            btn.disabled = false;
            btn.classList.remove('disabled');
            btn.removeAttribute('title');
        } else {
            btn.disabled = true;
            btn.classList.add('disabled');
            btn.setAttribute('title', 'Save project to Unity Catalog first');
        }
    });
    
    // Show/hide new project message if applicable
    updateNewProjectMessage(!isSaved);
}

/**
 * Show a message for new projects
 */
function updateNewProjectMessage(showMessage) {
    // Check if we're on the project page
    const projectSettingsSection = document.getElementById('information-section');
    if (!projectSettingsSection) return;
    
    // Remove existing message if any
    const existingMsg = document.getElementById('newProjectMessage');
    if (existingMsg) existingMsg.remove();
    
    if (showMessage) {
        // Add message after the section header
        const sectionHeader = projectSettingsSection.querySelector('.section-header');
        if (sectionHeader) {
            const msgHtml = `
                <div id="newProjectMessage" class="alert alert-info d-flex align-items-center mt-3" role="alert">
                    <i class="bi bi-info-circle-fill me-2 fs-5"></i>
                    <div>
                        <strong>New Project</strong> - Please fill in the <strong>Project Name</strong> and <strong>Base URI</strong>, 
                        then click <strong>Save Project to Unity Catalog</strong> to enable all features.
                    </div>
                </div>
            `;
            sectionHeader.insertAdjacentHTML('afterend', msgHtml);
        }
    }
}

/**
 * Called after project is saved to UC to enable menus
 */
function enableMenusAfterSave() {
    updateMenusForProjectStatus(true);
}

/**
 * Update project dropdown menu visibility based on project state
 * Note: All menu items are now always visible
 */
function updateProjectMenuVisibility(hasProject) {
    // All project menu items are now always visible
    // This function is kept for compatibility but does nothing
}


// Legacy aliases kept for callers in other modules
window.refreshOntologyStatus = refreshNavbarIndicators;
window.refreshDigitalTwinStatus = refreshNavbarIndicators;

// showNotification is provided by utils.js via NotificationCenter


// ==========================================
// Project Management Functions
// ==========================================

/**
 * Start a new project (clears current data)
 */
async function projectNew() {
    const confirmed = await showConfirmDialog({
        title: 'New Project',
        message: 'Start a new project? This will clear all current ontology, design, and mapping data.',
        confirmText: 'Start New',
        confirmClass: 'btn-warning',
        icon: 'file-earmark-plus'
    });
    if (!confirmed) return;
    
    try {
        const response = await fetch('/project/clear', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('New project started', 'success');
            // Navigate to Project settings page
            setTimeout(() => {
                window.location.href = '/project/#information';
            }, 1000);
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        console.error('Error creating new project:', error);
        showNotification('Failed to create new project: ' + error.message, 'error');
    }
}

/**
 * Save project to Unity Catalog Volume
 * Volume name = Project name, File name = version number
 */
async function projectSave() {
    try {
        // First, save project info from form if on the project page
        await saveProjectInfoBeforeSave();
        
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
                
                await fetch('/project/design-views/save-current', {
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
        showProjectSaveDialog();
    } catch (error) {
        console.error('Error preparing save:', error);
        showNotification('Failed to prepare save: ' + error.message, 'error');
    }
}

/**
 * Save project info from form fields before saving to UC
 */
async function saveProjectInfoBeforeSave() {
    // Check if we're on a page with project info form fields
    const nameEl = document.getElementById('projectName');
    const descEl = document.getElementById('projectDescription');
    const authorEl = document.getElementById('projectAuthor');
    const baseUriEl = document.getElementById('projectBaseUri');
    const llmEndpointEl = document.getElementById('projectLlmEndpoint');
    const versionEl = document.getElementById('projectVersionSelect');

    // If any form fields exist, save the project info
    if (nameEl || descEl || authorEl || baseUriEl || llmEndpointEl) {
        const deltaCatalog = document.getElementById('triplestoreLocationWidget_catalog')?.value || '';
        const deltaSchema = document.getElementById('triplestoreLocationWidget_schema')?.value || '';
        const deltaTableName = document.getElementById('projectTriplestoreTableName')?.value.trim() || '';
        const hasDelta = deltaCatalog || deltaSchema || deltaTableName;

        const projectInfo = {
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
        Object.keys(projectInfo).forEach(key => {
            if (projectInfo[key] === undefined) delete projectInfo[key];
        });
        
        // Only save if we have something to save
        if (Object.keys(projectInfo).length > 0) {
            console.log('[Project] Auto-saving project info before UC save:', projectInfo);
            try {
                await fetch('/project/info', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(projectInfo),
                    credentials: 'same-origin'
                });
            } catch (e) {
                console.warn('Could not auto-save project info:', e);
            }
        }
    }
}

/**
 * Show confirmation dialog before saving to the registry.
 */
async function showProjectSaveDialog() {
    const modalHtml = `
        <div class="modal fade" id="projectSaveModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-cloud-upload"></i> Save Project to Registry</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div id="saveRegistryInfo" class="mb-3">
                            <span class="spinner-border spinner-border-sm me-1"></span> Checking registry...
                        </div>
                        <div class="alert alert-info small">
                            <i class="bi bi-info-circle"></i>
                            <strong>Project:</strong> <span id="saveProjectName">Loading...</span><br>
                            <strong>Version:</strong> <span id="saveProjectVersion">Loading...</span>
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

    const existingModal = document.getElementById('projectSaveModal');
    if (existingModal) existingModal.remove();
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('projectSaveModal'));
    modal.show();

    try {
        const infoData = await fetchOnce('/project/info');
        if (infoData.success) {
            document.getElementById('saveProjectName').textContent = infoData.info.name || 'NewProject';
            document.getElementById('saveProjectVersion').textContent = infoData.info.version || '1';
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
        await doProjectSave();
    });
}

async function doProjectSave() {
    try {
        showNotification('Saving project to registry...', 'info', 5000);
        const response = await fetch('/project/save-to-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({}),
            credentials: 'same-origin'
        });
        const data = await response.json();
        if (data.success) {
            showNotification(data.message || 'Project saved successfully!', 'success');
            enableMenusAfterSave();
            await refreshNavbarIndicators();
            loadProjectName();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

/**
 * Load project from Unity Catalog Volume
 */
async function projectLoad() {
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
    
    showProjectLoadDialog();
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
                        <p>To load a project from Unity Catalog, you need to select a SQL Warehouse first.</p>
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
 * Show dialog to pick a project and version from the registry.
 */
function showProjectLoadDialog() {
    const modalHtml = `
        <div class="modal fade" id="projectLoadModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-cloud-download"></i> Load Project from Registry</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div id="loadRegistryInfo" class="mb-3">
                            <span class="spinner-border spinner-border-sm me-1"></span> Checking registry...
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Project</label>
                            <select class="form-select" id="loadProjectSelect" disabled>
                                <option value="">Loading projects...</option>
                            </select>
                        </div>
                        <div class="mb-3">
                            <label class="form-label">Version</label>
                            <select class="form-select" id="loadVersionSelect" disabled>
                                <option value="">Select project first</option>
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

    const existingModal = document.getElementById('projectLoadModal');
    if (existingModal) existingModal.remove();
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('projectLoadModal'));
    modal.show();

    loadProjectsFromRegistry();

    document.getElementById('btnConfirmLoad').addEventListener('click', async () => {
        const project = document.getElementById('loadProjectSelect').value;
        const version = document.getElementById('loadVersionSelect').value;
        if (!project || !version) {
            showNotification('Please select a project and version', 'warning');
            return;
        }
        modal.hide();
        await doProjectLoad(project, version);
    });
}

async function loadProjectsFromRegistry() {
    const infoDiv = document.getElementById('loadRegistryInfo');
    const projectSelect = document.getElementById('loadProjectSelect');
    const versionSelect = document.getElementById('loadVersionSelect');
    try {
        const regResp = await fetch('/settings/registry', { credentials: 'same-origin' });
        const reg = await regResp.json();
        if (!reg.configured) {
            infoDiv.innerHTML = '<div class="alert alert-warning small mb-0"><i class="bi bi-exclamation-triangle me-1"></i> Registry not configured. <a href="/settings">Go to Settings</a></div>';
            return;
        }
        infoDiv.innerHTML = `<div class="alert alert-success small mb-0"><i class="bi bi-archive me-1"></i> Registry: <strong>${reg.catalog}.${reg.schema}.${reg.volume}</strong></div>`;

        projectSelect.innerHTML = '<option value="">Loading projects...</option>';
        const projResp = await fetch('/project/list-projects', { credentials: 'same-origin' });
        const projData = await projResp.json();

        projectSelect.innerHTML = '<option value="">Select project...</option>';
        projectSelect.disabled = false;

        if (projData.success && projData.projects && projData.projects.length > 0) {
            projData.projects.forEach(p => {
                projectSelect.innerHTML += `<option value="${p}">${p}</option>`;
            });
        } else {
            projectSelect.innerHTML = '<option value="">No projects found</option>';
        }

        projectSelect.onchange = () => loadVersionsFromRegistry(projectSelect.value);
    } catch (e) {
        infoDiv.innerHTML = '<div class="alert alert-danger small mb-0">Error loading registry</div>';
    }
}

async function loadVersionsFromRegistry(projectName) {
    const versionSelect = document.getElementById('loadVersionSelect');
    document.getElementById('btnConfirmLoad').disabled = true;

    if (!projectName) {
        versionSelect.disabled = true;
        versionSelect.innerHTML = '<option value="">Select project first</option>';
        return;
    }

    try {
        versionSelect.innerHTML = '<option value="">Loading versions...</option>';
        const resp = await fetch(`/project/list-versions?project_name=${encodeURIComponent(projectName)}`, { credentials: 'same-origin' });
        const data = await resp.json();

        versionSelect.innerHTML = '<option value="">Select version...</option>';
        versionSelect.disabled = false;

        if (data.success && data.versions && data.versions.length > 0) {
            const sorted = data.versions.sort((a, b) => b.localeCompare(a, undefined, { numeric: true }));
            versionSelect.innerHTML = '';
            sorted.forEach((ver, idx) => {
                const label = idx === 0 ? `${ver} (Latest - Active)` : `${ver} (Inactive)`;
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

async function doProjectLoad(project, version) {
    showProjectLoading(`Loading ${project}...`);
    try {
        const response = await fetch('/project/load-from-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ project, version }),
            credentials: 'same-origin'
        });
        const data = await response.json();
        if (data.success) {
            showNotification(data.message || 'Project loaded successfully!', 'success');
            setTimeout(() => location.reload(), 1000);
        } else {
            hideProjectLoading();
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        hideProjectLoading();
        showNotification('Error: ' + error.message, 'error');
    }
}

/**
 * Handle the selected project file - parse and check for versions
 */
async function handleProjectFile(input) {
    const file = input.files[0];
    if (!file) return;
    
    try {
        // Read the file content first
        const content = await file.text();
        let projectData;
        
        try {
            projectData = JSON.parse(content);
        } catch (e) {
            showNotification('Error: Invalid JSON in project file', 'error');
            input.value = '';
            return;
        }
        
        // Check if file has versions
        if (projectData.versions && Object.keys(projectData.versions).length > 0) {
            const versions = Object.keys(projectData.versions).sort().reverse();
            
            if (versions.length > 1) {
                // Show version selection dialog
                showVersionSelectionDialog(versions, projectData, async (selectedVersion) => {
                    await importProjectWithVersion(projectData, selectedVersion);
                });
            } else {
                // Only one version, load it directly
                await importProjectWithVersion(projectData, versions[0]);
            }
        } else {
            // Legacy format without versions - load directly
            await importProjectDirect(projectData);
        }
    } catch (error) {
        console.error('Error loading project:', error);
        showNotification('Failed to load project: ' + error.message, 'error');
    }
    
    // Reset file input
    input.value = '';
}

/**
 * Show version selection dialog
 */
function showVersionSelectionDialog(versions, projectData, onSelect) {
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
                        <p class="text-muted mb-3">This project contains multiple versions. Select the version to load:</p>
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
        const versionData = projectData.versions[version];
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
 * Import project with a specific version
 */
async function importProjectWithVersion(projectData, version) {
    showProjectLoading(`Loading version ${version}...`);
    try {
        const response = await fetch('/project/import', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ project: projectData, version: version }),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        handleImportResponse(data);
    } catch (error) {
        hideProjectLoading();
        console.error('Error importing project:', error);
        showNotification('Failed to import project: ' + error.message, 'error');
    }
}

/**
 * Import project directly (legacy format)
 */
async function importProjectDirect(projectData) {
    showProjectLoading('Loading project...');
    try {
        const response = await fetch('/project/import', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ project: projectData }),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        handleImportResponse(data);
    } catch (error) {
        hideProjectLoading();
        console.error('Error importing project:', error);
        showNotification('Failed to import project: ' + error.message, 'error');
    }
}

/**
 * Handle import response
 */
function handleImportResponse(data) {
    if (data.success) {
        let msg = 'Project loaded';
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
        hideProjectLoading();
        showNotification('Error loading project: ' + data.message, 'error');
    }
}

// Make project functions globally available
window.projectNew = projectNew;
window.projectSave = projectSave;
window.projectLoad = projectLoad;
window.showProjectSaveDialog = showProjectSaveDialog;
window.showProjectLoadDialog = showProjectLoadDialog;
window.doProjectSave = doProjectSave;
window.doProjectLoad = doProjectLoad;
window.checkProjectSavedStatus = loadProjectName;
window.enableMenusAfterSave = enableMenusAfterSave;
window.updateMenusForProjectStatus = updateMenusForProjectStatus;
window.showProjectLoading = showProjectLoading;
window.hideProjectLoading = hideProjectLoading;
