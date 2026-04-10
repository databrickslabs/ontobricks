/**
 * OntoBricks - project.js
 * Extracted from project templates per code_instructions.txt
 */

document.body.classList.add('full-width-layout');

let _defaultBaseUriDomain = '';
let _baseUriAutoMode = true;

/**
 * Strip non-alphanumeric chars and force CamelCase on the project name.
 * Each "word" (sequence of letters/digits after a non-alnum char or at the
 * start) gets its first letter uppercased.
 */
function enforceCamelCase(value) {
    const stripped = value.replace(/[^a-zA-Z0-9]/g, ' ');
    return stripped
        .split(/\s+/)
        .filter(Boolean)
        .map(w => w.charAt(0).toUpperCase() + w.slice(1))
        .join('');
}

// Configure sidebar navigation
window.SIDEBAR_NAV_MANUAL_INIT = true;
document.addEventListener('DOMContentLoaded', function() {
    // Check URL for section parameter
    const urlParams = new URLSearchParams(window.location.search);
    const initialSection = urlParams.get('section');
    
    SidebarNav.init({
        onSectionChange: function(section, targetSection) {
            // Load metadata when switching to metadata section
            if (section === 'metadata' && typeof initMetadataSection === 'function') {
                initMetadataSection();
            }
            // Load validation details when switching to validation section
            if (section === 'validation') {
                loadValidationDetails();
            }
            // Load versions when switching to versions section
            if (section === 'versions' && typeof loadVersionsList === 'function') {
                loadVersionsList();
            }
            // Load OWL content when switching to owl-content section
            if (section === 'owl-content' && typeof window.loadOwlContent === 'function') {
                window.loadOwlContent();
            }
            // Load R2RML content when switching to r2rml section
            if (section === 'r2rml' && typeof window.loadR2RMLContent === 'function') {
                window.loadR2RMLContent();
            }
        }
    });
    
    // Load initial data
    loadProjectInfo();
    
    // If section parameter was passed, navigate to that section
    if (initialSection) {
        const link = document.querySelector(`[data-section="${initialSection}"]`);
        if (link) {
            link.click();
        }
    }
});

async function loadProjectInfo() {
    try {
        const data = await fetchOnce('/project/info');
        
        if (data.success && data.info) {
            const nameEl = document.getElementById('projectName');
            const descEl = document.getElementById('projectDescription');
            const authorEl = document.getElementById('projectAuthor');
            const baseUriEl = document.getElementById('projectBaseUri');
            const autoToggle = document.getElementById('baseUriCustomToggle');
            
            if (nameEl) nameEl.value = data.info.name || 'NewProject';
            if (descEl) descEl.value = data.info.description || '';
            if (authorEl) authorEl.value = data.info.author || '';
            
            if (authorEl && !authorEl.value) {
                loadCurrentUserAsAuthor(authorEl);
            }
            
            await loadDefaultBaseUriDomain();
            
            // Determine auto/manual mode:
            // - explicit flag wins
            // - missing flag + existing base_uri → manual (preserve user value)
            // - missing flag + empty base_uri → auto
            if (data.info.base_uri_auto !== undefined) {
                _baseUriAutoMode = data.info.base_uri_auto !== false;
            } else {
                _baseUriAutoMode = !data.info.base_uri;
            }
            
            if (autoToggle) autoToggle.checked = !_baseUriAutoMode;
            
            if (_baseUriAutoMode) {
                updateAutoBaseUri();
            } else if (baseUriEl && data.info.base_uri) {
                baseUriEl.value = data.info.base_uri;
            }
            
            applyBaseUriMode();
            
            // Cache registry for triplestore defaults
            if (data.registry) {
                window._projectRegistry = data.registry;
            }

            // Populate triplestore hidden fields from API response
            if (data.delta) {
                var catEl = document.getElementById('triplestoreLocationWidget_catalog');
                var schEl = document.getElementById('triplestoreLocationWidget_schema');
                var dispEl = document.getElementById('triplestoreCatalogSchemaDisplay');
                if (catEl && data.delta.catalog) catEl.value = data.delta.catalog;
                if (schEl && data.delta.schema) schEl.value = data.delta.schema;
                if (dispEl && data.delta.catalog && data.delta.schema) {
                    dispEl.value = data.delta.catalog + '.' + data.delta.schema;
                }
            }

            // Apply registry catalog.schema as triplestore default (only if not already set)
            if (typeof applyRegistryAsTriplestoreDefault === 'function') {
                applyRegistryAsTriplestoreDefault();
            }
            
            // Compute derived triplestore names (view + snapshot)
            if (typeof syncDerivedTriplestoreNames === 'function') {
                syncDerivedTriplestoreNames();
            }
            
            // Enforce CamelCase and update derived fields when project name changes
            if (nameEl) {
                nameEl.addEventListener('input', function() {
                    const pos = nameEl.selectionStart;
                    const cleaned = enforceCamelCase(nameEl.value);
                    if (cleaned !== nameEl.value) {
                        nameEl.value = cleaned;
                        nameEl.setSelectionRange(
                            Math.min(pos, cleaned.length),
                            Math.min(pos, cleaned.length)
                        );
                    }
                    if (_baseUriAutoMode) updateAutoBaseUri();
                    if (typeof syncDerivedTriplestoreNames === 'function') {
                        syncDerivedTriplestoreNames();
                    }
                });
            }
        }
    } catch (error) {
        console.error('Error loading project info:', error);
    }
}

/**
 * Fetch the default base URI domain from Settings (cached in _defaultBaseUriDomain).
 */
async function loadDefaultBaseUriDomain() {
    try {
        const response = await fetch('/settings/get-base-uri', { credentials: 'same-origin' });
        const result = await response.json();
        if (result.success && result.base_uri) {
            _defaultBaseUriDomain = result.base_uri.trim();
        }
    } catch (error) {
        console.log('Could not load default base URI domain:', error);
    }
}

/**
 * Auto-fill the Author field with the current Databricks user email (fire-and-forget).
 */
function loadCurrentUserAsAuthor(authorEl) {
    fetch('/project/current-user', { credentials: 'same-origin' })
        .then(r => r.json())
        .then(data => {
            if (data.success && data.email && !authorEl.value) {
                authorEl.value = data.email;
            }
        })
        .catch(() => {});
}

/**
 * Compute and set the Base URI from the default domain and project name.
 * Format: {domain}/{ProjectName}#
 */
function updateAutoBaseUri() {
    if (!_baseUriAutoMode) return;
    const baseUriEl = document.getElementById('projectBaseUri');
    const nameEl = document.getElementById('projectName');
    if (!baseUriEl || !_defaultBaseUriDomain) return;
    
    let domain = _defaultBaseUriDomain.replace(/[/#]+$/, '');
    const projectName = (nameEl ? nameEl.value.trim() : '') || 'MyProject';
    baseUriEl.value = domain + '/' + projectName + '#';
}

/**
 * Toggle the Base URI field between auto (readonly, computed) and manual (editable).
 * Called when the "Custom" checkbox changes and after version-status UI updates.
 */
function applyBaseUriMode() {
    const baseUriEl = document.getElementById('projectBaseUri');
    const autoToggle = document.getElementById('baseUriCustomToggle');
    const hintEl = document.getElementById('baseUriHint');
    if (!baseUriEl) return;
    
    _baseUriAutoMode = autoToggle ? !autoToggle.checked : true;
    
    if (_baseUriAutoMode) {
        baseUriEl.readOnly = true;
        baseUriEl.style.backgroundColor = '#e9ecef';
        updateAutoBaseUri();
        if (hintEl) hintEl.innerHTML = 'Auto-generated from <a href="/settings">Settings</a> default domain and project name.';
    } else {
        baseUriEl.readOnly = false;
        baseUriEl.style.backgroundColor = '';
        if (hintEl) hintEl.innerHTML = 'Enter a custom base URI for all ontology entities.';
    }
}

async function saveProjectInfo() {
    const nameEl = document.getElementById('projectName');
    const descEl = document.getElementById('projectDescription');
    const authorEl = document.getElementById('projectAuthor');
    const versionEl = document.getElementById('projectVersionSelect');
    const baseUriEl = document.getElementById('projectBaseUri');
    const llmEndpointEl = document.getElementById('projectLlmEndpoint');

    if (!nameEl || !descEl || !authorEl) {
        showNotification('Form fields not found', 'error');
        return;
    }

    const projName = nameEl.value.trim();
    if (!projName) {
        showNotification('Project name is required', 'warning');
        return;
    }
    if (!/^[A-Z][a-zA-Z0-9]*$/.test(projName)) {
        showNotification('Project Name must be CamelCase and alphanumeric only', 'warning');
        return;
    }

    const projectInfo = {
        name: projName,
        version: versionEl ? versionEl.value : '1',
        description: descEl.value.trim(),
        author: authorEl.value.trim(),
        base_uri: baseUriEl ? baseUriEl.value.trim() : '',
        base_uri_auto: _baseUriAutoMode,
        llm_endpoint: llmEndpointEl ? llmEndpointEl.value : '',
        
        delta: {
            catalog: document.getElementById('triplestoreLocationWidget_catalog')?.value || '',
            schema: document.getElementById('triplestoreLocationWidget_schema')?.value || '',
            table_name: document.getElementById('projectTriplestoreTableName')?.value.trim() || '',
        },
    };
    
    try {
        showNotification('Saving project info...', 'info', 1000);
        
        const response = await fetch('/project/info', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(projectInfo),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('Project info saved successfully!', 'success');
            // Update the navbar project name
            const projectNameEl = document.getElementById('currentProjectName');
            if (projectNameEl) {
                projectNameEl.textContent = projectInfo.name || 'Project';
            }
        } else {
            showNotification('Error: ' + (data.message || 'Failed to save'), 'error');
        }
    } catch (error) {
        console.error('Save error:', error);
        showNotification('Failed to save: ' + error.message, 'error');
    }
}

// Note: saveProjectToLocalFile is defined in _project_actions.html

function loadProjectFromFile() {
    document.getElementById('projectFileInput').click();
}

async function handleProjectFileUpload(input) {
    const file = input.files[0];
    if (!file) return;
    
    if (typeof showProjectLoading === 'function') showProjectLoading('Loading project...');
    try {
        const formData = new FormData();
        formData.append('file', file);
        
        const response = await fetch('/project/import', {
            method: 'POST',
            body: formData,
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('Project loaded successfully!', 'success');
            setTimeout(() => location.reload(), 1000);
        } else {
            if (typeof hideProjectLoading === 'function') hideProjectLoading();
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        if (typeof hideProjectLoading === 'function') hideProjectLoading();
        showNotification('Failed to load: ' + error.message, 'error');
    }
    
    input.value = '';
}

async function newProject() {
    const confirmed = await showConfirmDialog({
        title: 'New Project',
        message: 'Start a new project? This will clear all current data.',
        confirmText: 'Start New',
        confirmClass: 'btn-warning',
        icon: 'file-earmark-plus'
    });
    if (!confirmed) return;
    
    try {
        const response = await fetch('/project/clear', {
            method: 'POST',
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('New project started!', 'success');
            setTimeout(() => location.reload(), 1000);
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Failed: ' + error.message, 'error');
    }
}

/**
 * Save project from settings page with validation
 * For new projects, requires Project Name and Base URI
 */
async function saveProjectFromSettings() {
    const nameEl = document.getElementById('projectName');
    const baseUriEl = document.getElementById('projectBaseUri');
    
    // Get values
    const projectName = nameEl ? nameEl.value.trim() : '';
    const baseUri = baseUriEl ? baseUriEl.value.trim() : '';
    
    // Clear previous validation states
    if (nameEl) nameEl.classList.remove('is-invalid');
    if (baseUriEl) baseUriEl.classList.remove('is-invalid');
    
    // Validate required fields
    let hasErrors = false;
    
    if (!projectName) {
        if (nameEl) {
            nameEl.classList.add('is-invalid');
            nameEl.focus();
        }
        showNotification('Project Name is required', 'warning');
        hasErrors = true;
    } else if (!/^[A-Z][a-zA-Z0-9]*$/.test(projectName)) {
        if (nameEl) {
            nameEl.classList.add('is-invalid');
            if (!hasErrors) nameEl.focus();
        }
        showNotification('Project Name must be CamelCase and alphanumeric only (e.g. MyOntologyProject)', 'warning');
        hasErrors = true;
    }
    
    if (!baseUri) {
        if (baseUriEl) {
            baseUriEl.classList.add('is-invalid');
            if (!hasErrors) baseUriEl.focus();
        }
        if (!hasErrors) {
            showNotification('Base URI is required', 'warning');
        } else {
            showNotification('Project Name and Base URI are required', 'warning');
        }
        hasErrors = true;
    }
    
    // Validate Base URI format
    if (baseUri && !isValidUri(baseUri)) {
        if (baseUriEl) {
            baseUriEl.classList.add('is-invalid');
            if (!hasErrors) baseUriEl.focus();
        }
        showNotification('Base URI must be a valid URI (e.g., https://example.org/ontology#)', 'warning');
        hasErrors = true;
    }
    
    if (hasErrors) {
        return;
    }
    
    // Remove validation states on input
    if (nameEl) {
        nameEl.addEventListener('input', () => nameEl.classList.remove('is-invalid'), { once: true });
    }
    if (baseUriEl) {
        baseUriEl.addEventListener('input', () => baseUriEl.classList.remove('is-invalid'), { once: true });
    }
    
    // Save project info to session and open Unity Catalog save dialog
    // (saveProjectInfo only saves to session; projectSave opens the UC dialog for actual persistence)
    if (typeof projectSave === 'function') {
        await projectSave();
    } else {
        await saveProjectInfo();
        showNotification('Project info saved. Use the menu "Save Project" to persist to Unity Catalog.', 'info', 4000);
    }
}

/**
 * Validate URI format
 */
function isValidUri(uri) {
    try {
        // Check basic URI patterns
        if (!uri.match(/^https?:\/\//i) && !uri.match(/^urn:/i)) {
            return false;
        }
        return true;
    } catch (e) {
        return false;
    }
}
