/**
 * OntoBricks - domain-information.js
 * Extracted from domain templates per code_instructions.txt
 */

let currentDomainFolder = null;

// Load available LLM endpoints
async function loadLlmEndpoints() {
    const select = document.getElementById('domainLlmEndpoint');
    if (!select) return;
    
    const savedValue = select.dataset.savedValue || '';

    try {
        const response = await fetch('/mapping/wizard/llm-endpoints', { credentials: 'same-origin' });
        const data = await response.json();
        
        select.innerHTML = '<option value="">-- Select an LLM endpoint --</option>';
        
        if (data.success && data.endpoints && data.endpoints.length > 0) {
            data.endpoints.forEach(endpoint => {
                const option = document.createElement('option');
                option.value = endpoint.name;
                option.textContent = endpoint.name;
                select.appendChild(option);
            });
        } else {
            const option = document.createElement('option');
            option.value = '';
            option.textContent = 'No endpoints available';
            option.disabled = true;
            select.appendChild(option);
        }

        if (savedValue) {
            setSelectedLlmEndpoint(savedValue);
        }
    } catch (error) {
        console.error('Error loading LLM endpoints:', error);
    }
}

// Set the selected LLM endpoint
function setSelectedLlmEndpoint(endpointName) {
    const select = document.getElementById('domainLlmEndpoint');
    if (!select || !endpointName) return;

    select.value = endpointName;
    if (select.value !== endpointName) {
        const option = document.createElement('option');
        option.value = endpointName;
        option.textContent = endpointName;
        select.appendChild(option);
        select.value = endpointName;
    }
}

// Rollback to the saved version (discard all local changes)
async function rollbackVersion() {
    const versionSelect = document.getElementById('domainVersionSelect');
    const currentVersion = versionSelect ? versionSelect.value : '1';
    
    try {
        // Determine the domain folder from version-status or domain info
        let domainFolder = currentDomainFolder;
        if (!domainFolder) {
            const statusData = await fetchOnce('/domain/version-status');
            const folder = statusData.success && (statusData.domain_folder || statusData.project_folder);
            if (folder) {
                domainFolder = folder;
                currentDomainFolder = domainFolder;
            }
        }

        if (!domainFolder) {
            showNotification('Domain must be saved to the registry first to rollback', 'warning');
            return;
        }

        const confirmed = await showConfirmDialog({
            title: 'Rollback Version',
            message: `This will reload version ${currentVersion} from Unity Catalog and discard ALL unsaved changes. Are you sure?`,
            confirmText: 'Rollback',
            confirmClass: 'btn-warning',
            icon: 'arrow-counterclockwise'
        });
        if (!confirmed) return;

        showNotification('Rolling back to saved version...', 'info', 3000);

        const response = await fetch('/domain/load-from-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                domain: domainFolder,
                version: currentVersion
            }),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(`Rolled back to version ${currentVersion} successfully!`, 'success');
            // Reload page to refresh all data
            window.location.reload();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        console.error('Rollback error:', error);
        showNotification('Error: ' + error.message, 'error');
    }
}

// Create a new version
async function createNewVersion() {
    try {
        const confirmed = await showConfirmDialog({
            title: 'Create New Version',
            message: 'This will copy the current version and increment the version number. Continue?',
            confirmText: 'Create Version',
            confirmClass: 'btn-primary',
            icon: 'plus-circle'
        });
        if (!confirmed) return;
        
        showNotification('Creating new version...', 'info', 2000);
        
        const response = await fetch('/domain/create-version', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(`Version ${data.new_version} created successfully!`, 'success');
            // Add new version to dropdown and select it
            const versionSelect = document.getElementById('domainVersionSelect');
            const newOption = document.createElement('option');
            newOption.value = data.new_version;
            newOption.textContent = `v${data.new_version}`;
            // Insert at the beginning (latest first)
            versionSelect.insertBefore(newOption, versionSelect.firstChild);
            versionSelect.value = data.new_version;
            // Reload page to refresh status
            window.location.reload();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

// Handle version change
async function onVersionChange(version) {
    if (!currentDomainFolder) {
        showNotification('Domain must be saved to Unity Catalog first', 'warning');
        return;
    }
    
    const confirmed = await showConfirmDialog({
        title: 'Switch Version',
        message: `Load version ${version}? Unsaved changes will be lost.`,
        confirmText: 'Load Version',
        confirmClass: 'btn-primary',
        icon: 'arrow-repeat'
    });
    
    if (!confirmed) {
        // Reset select to current version
        const statusResponse = await fetch('/domain/version-status', { credentials: 'same-origin' });
        const statusData = await statusResponse.json();
        if (statusData.success) {
            document.getElementById('domainVersionSelect').value = statusData.version;
        }
        return;
    }
    
    try {
        showNotification('Loading version...', 'info', 3000);
        
        const response = await fetch('/domain/load-from-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                domain: currentDomainFolder,
                version: version
            }),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(data.message || 'Version loaded successfully!', 'success');
            // Reload page to refresh all data
            window.location.reload();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

// Update the version display label and hidden input
function populateVersionDropdown(versions, currentVersion) {
    const versionHidden = document.getElementById('domainVersionSelect');
    const versionDisplay = document.getElementById('domainVersionDisplay');

    if (versionHidden) versionHidden.value = currentVersion;
    if (versionDisplay) versionDisplay.value = `v${currentVersion}`;
}

// No-op: version selector has been replaced by a read-only label
function enableVersionSelector() {}

// Update UI based on version status (latest = editable, older = read-only)
function updateVersionStatusUI(isActive, version, hasRegistry) {
    const domainNameInput = document.getElementById('domainName');
    const domainNameHint = document.getElementById('domainNameHint');

    const editableFields = document.querySelectorAll('.domain-editable:not(#domainName)');
    const baseUriToggle = document.getElementById('baseUriCustomToggle');

    const versionDisplay = document.getElementById('domainVersionDisplay');
    const versionHidden = document.getElementById('domainVersionSelect');
    if (versionDisplay) versionDisplay.value = `v${version}`;
    if (versionHidden) versionHidden.value = version;

    if (hasRegistry && domainNameInput) {
        domainNameInput.disabled = true;
        domainNameInput.readOnly = true;
        domainNameInput.style.backgroundColor = '#e9ecef';
        if (domainNameHint) {
            domainNameHint.innerHTML = '<i class="bi bi-lock"></i> Locked (used as folder name in the registry)';
        }
    }
    
    if (isActive) {
        editableFields.forEach(el => {
            el.disabled = false;
            if (el.tagName === 'BUTTON') {
                el.classList.remove('disabled');
            }
        });
        if (baseUriToggle) baseUriToggle.disabled = false;
    } else {
        editableFields.forEach(el => {
            el.disabled = true;
            if (el.tagName === 'BUTTON') {
                el.classList.add('disabled');
            }
        });
        if (baseUriToggle) baseUriToggle.disabled = true;
        
        if (domainNameInput) {
            domainNameInput.disabled = true;
        }
    }
    
    if (typeof applyBaseUriMode === 'function') {
        applyBaseUriMode();
    }
}

function updateRegistryLocationDisplay(registry, domainFolder) {
    const ucRow = document.getElementById('ucLocationRow');
    const displayEl = document.getElementById('registryLocationDisplay');
    if (!ucRow || !displayEl) return;

    if (registry && registry.catalog && domainFolder) {
        displayEl.textContent = `${registry.catalog}.${registry.schema}.${registry.volume}/domains/${domainFolder}`;
        ucRow.style.display = 'flex';
    }
}

// Fetch and update version status on page load
document.addEventListener('DOMContentLoaded', async function() {
    try {
        // Initialize triplestore widget immediately (no API calls, values from template)
        initTriplestoreWidget();

        // Update derived graph paths when the Triple Store tab is shown
        const tsTab = document.getElementById('tab-triplestore');
        if (tsTab) {
            tsTab.addEventListener('shown.bs.tab', () => updateGraphPaths());
        }

        // Load LLM endpoints and version status in parallel
        const [, statusData, infoData] = await Promise.all([
            loadLlmEndpoints(),
            fetchOnce('/domain/version-status').catch(() => null),
            fetchOnce('/domain/info').catch(() => null)
        ]);

        if (statusData && statusData.success) {
            updateVersionStatusUI(statusData.is_active, statusData.version, statusData.has_registry);
            populateVersionDropdown(statusData.available_versions, statusData.version);
            syncDerivedTriplestoreNames();
            const sf = statusData.domain_folder || statusData.project_folder;
            if (sf) {
                currentDomainFolder = sf;
            }
        }

        if (infoData && infoData.success) {
            const inf = infoData.domain_folder || infoData.project_folder;
            if (inf && !currentDomainFolder) {
                currentDomainFolder = inf;
            }
            if (infoData.info && infoData.info.llm_endpoint) {
                setSelectedLlmEndpoint(infoData.info.llm_endpoint);
            }
            if (infoData.registry) {
                window._domainRegistry = infoData.registry;
                applyRegistryAsTriplestoreDefault();
            }
        }
        
        // Re-enable version selector after a short delay to override any global disabling
        setTimeout(enableVersionSelector, 100);
    } catch (e) {
        console.log('Could not fetch domain status:', e);
    }
});


/**
 * Sanitize a name for use as a Delta table identifier.
 */
function sanitizeTableName(name) {
    return name.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
}

/**
 * Initialize the triplestore display from template-rendered values.
 * The view name is always derived from the domain name (read-only).
 */
function initTriplestoreWidget() {
    syncDerivedTriplestoreNames();
    updateGraphPaths();
}

/**
 * Recompute the VIEW name and snapshot table name from the current
 * domain name and catalog.schema.  Both fields are read-only.
 */
function syncDerivedTriplestoreNames() {
    var nameEl = document.getElementById('domainName');
    var domainName = nameEl ? nameEl.value.trim() : '';
    var safeName = domainName ? sanitizeTableName(domainName) : '';

    var versionEl = document.getElementById('domainVersionSelect');
    var version = versionEl ? versionEl.value.trim() : '1';
    var versionSuffix = '_V' + (version || '1');

    var tableNameEl = document.getElementById('domainTriplestoreTableName');
    if (tableNameEl && safeName) {
        tableNameEl.value = 'triplestore_' + safeName + versionSuffix;
    }

    var catalog = (document.getElementById('triplestoreLocationWidget_catalog') || {}).value || '';
    var schema  = (document.getElementById('triplestoreLocationWidget_schema')  || {}).value || '';

    var snapshotEl = document.getElementById('domainSnapshotTableName');
    if (snapshotEl) {
        if (catalog && schema && safeName) {
            snapshotEl.value = catalog + '.' + schema + '._ob_snapshot_' + safeName + versionSuffix;
        } else {
            snapshotEl.value = '';
        }
    }
}

function updateGraphPaths() {
    const nameEl = document.getElementById('domainName');
    const domainSlug = nameEl ? nameEl.value.trim().toLowerCase() : '';
    const versionEl = document.getElementById('domainVersionSelect');
    const version = versionEl ? versionEl.value.trim() : '1';
    const versionSuffix = '_V' + (version || '1');
    const localEl = document.getElementById('ladybugLocalPath');
    if (localEl && domainSlug) {
        localEl.textContent = '/tmp/ontobricks/' + domainSlug + versionSuffix + '.lbug';
    }
    syncDerivedTriplestoreNames();
}

/**
 * Open the UCLocationWidget modal to change catalog.schema for the triplestore.
 * The widget is initialized lazily on first click (no SQL warehouse call at page load).
 */
let triplestoreWidgetReady = false;
async function openTriplestoreLocationModal() {
    if (!triplestoreWidgetReady) {
        // Lazy-register the widget so UCLocationWidget.openModal can find it
        if (typeof UCLocationWidget !== 'undefined') {
            // Register widget without auto-loading domain registry location
            UCLocationWidget.init('triplestoreLocationWidget', {
                label: '',
                showLabel: false,
                autoLoadDomain: false,
                onSelect: function(catalog, schema) {
                    var display = document.getElementById('triplestoreCatalogSchemaDisplay');
                    if (display) display.value = catalog + '.' + schema;
                    document.getElementById('triplestoreLocationWidget_catalog').value = catalog;
                    document.getElementById('triplestoreLocationWidget_schema').value = schema;
                    syncDerivedTriplestoreNames();
                }
            });
        }
        triplestoreWidgetReady = true;
    }
    UCLocationWidget.openModal('triplestoreLocationWidget');
}

/**
 * No-op kept for backward compatibility (modal callback references it).
 * Delta catalog, schema, and table_name are read directly from their
 * respective form fields when saving.
 */
function updateTriplestoreHiddenField() {
    // Values are read directly from the individual form elements at save time.
}

/**
 * Generate the default triplestore table name from the domain name.
 */
function generateDefaultTriplestoreTableName() {
    applyRegistryAsTriplestoreDefault();
    syncDerivedTriplestoreNames();
}

/**
 * If the triplestore catalog.schema is not set, copy from the registry location.
 * Always refreshes derived names (view + snapshot) afterwards.
 */
function applyRegistryAsTriplestoreDefault() {
    const catalog = document.getElementById('triplestoreLocationWidget_catalog')?.value || '';

    if (!catalog) {
        const reg = window._domainRegistry;
        if (reg && reg.catalog && reg.schema) {
            document.getElementById('triplestoreLocationWidget_catalog').value = reg.catalog;
            document.getElementById('triplestoreLocationWidget_schema').value = reg.schema;
            const display = document.getElementById('triplestoreCatalogSchemaDisplay');
            if (display) display.value = reg.catalog + '.' + reg.schema;
        }
    }

    syncDerivedTriplestoreNames();
}
