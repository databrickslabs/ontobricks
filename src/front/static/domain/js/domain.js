/**
 * OntoBricks - domain.js
 * Extracted from domain templates per code_instructions.txt
 */

document.body.classList.add('full-width-layout');

let _defaultBaseUriDomain = '';
let _baseUriAutoMode = true;

/**
 * Strip non-alphanumeric chars and force CamelCase on the domain name.
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
    loadDomainInfo();

    var domainFileInput = document.getElementById('domainFileInput');
    if (domainFileInput) {
        domainFileInput.addEventListener('change', function () {
            handleDomainFileUpload(domainFileInput);
        });
    }
    
    // If section parameter was passed, navigate to that section
    if (initialSection) {
        const link = document.querySelector(`[data-section="${initialSection}"]`);
        if (link) {
            link.click();
        }
    }
});

async function loadDomainInfo() {
    try {
        const data = await fetchOnce('/domain/info');
        
        if (data.success && data.info) {
            const nameEl = document.getElementById('domainName');
            const descEl = document.getElementById('domainDescription');
            const authorEl = document.getElementById('domainAuthor');
            const baseUriEl = document.getElementById('domainBaseUri');
            const autoToggle = document.getElementById('baseUriCustomToggle');
            
            if (nameEl) nameEl.value = data.info.name || 'NewDomain';
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
                window._domainRegistry = data.registry;
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
            
            if (nameEl) {
                let _nameCheckTimer = null;
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
                    clearTimeout(_nameCheckTimer);
                    _nameCheckTimer = setTimeout(() => checkDomainNameAvailability(nameEl), 500);
                });
            }
        }
    } catch (error) {
        console.error('Error loading domain info:', error);
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
    fetch('/domain/current-user', { credentials: 'same-origin' })
        .then(r => r.json())
        .then(data => {
            if (data.success && data.email && !authorEl.value) {
                authorEl.value = data.email;
            }
        })
        .catch(() => {});
}

/**
 * Compute and set the Base URI from the default domain and domain name.
 * Format: {defaultDomain}/{DomainName}#
 */
function updateAutoBaseUri() {
    if (!_baseUriAutoMode) return;
    const baseUriEl = document.getElementById('domainBaseUri');
    const nameEl = document.getElementById('domainName');
    if (!baseUriEl || !_defaultBaseUriDomain) return;
    
    let defaultDomain = _defaultBaseUriDomain.replace(/[/#]+$/, '');
    const domainName = (nameEl ? nameEl.value.trim() : '') || 'MyDomain';
    baseUriEl.value = defaultDomain + '/' + domainName + '#';
}

/**
 * Toggle the Base URI field between auto (readonly, computed) and manual (editable).
 * Called when the "Custom" checkbox changes and after version-status UI updates.
 */
function applyBaseUriMode() {
    const baseUriEl = document.getElementById('domainBaseUri');
    const autoToggle = document.getElementById('baseUriCustomToggle');
    const hintEl = document.getElementById('baseUriHint');
    if (!baseUriEl) return;
    
    _baseUriAutoMode = autoToggle ? !autoToggle.checked : true;
    
    if (_baseUriAutoMode) {
        baseUriEl.readOnly = true;
        baseUriEl.style.backgroundColor = '#e9ecef';
        updateAutoBaseUri();
        if (hintEl) hintEl.innerHTML = 'Auto-generated from <a href="/settings">Settings</a> default domain and domain name.';
    } else {
        baseUriEl.readOnly = false;
        baseUriEl.style.backgroundColor = '';
        if (hintEl) hintEl.innerHTML = 'Enter a custom base URI for all ontology entities.';
    }
}

/**
 * Check whether the domain name is already taken in the registry.
 * Adds/removes an .is-invalid class and a small hint beneath the input.
 */
async function checkDomainNameAvailability(nameEl) {
    const name = (nameEl ? nameEl.value.trim() : '');
    const hintId = 'domainNameDuplicateHint';
    let hint = document.getElementById(hintId);
    if (!name) {
        if (hint) hint.remove();
        return;
    }
    try {
        const resp = await fetch(`/domain/check-name?name=${encodeURIComponent(name)}`, { credentials: 'same-origin' });
        const data = await resp.json();
        if (data.success && data.available === false) {
            nameEl.classList.add('is-invalid');
            if (!hint) {
                hint = document.createElement('div');
                hint.id = hintId;
                hint.className = 'invalid-feedback';
                hint.style.display = 'block';
                nameEl.parentNode.appendChild(hint);
            }
            hint.textContent = `A domain named "${data.folder}" already exists in the registry.`;
        } else {
            nameEl.classList.remove('is-invalid');
            if (hint) hint.remove();
        }
    } catch (_) {
        if (hint) hint.remove();
    }
}

async function saveDomainInfo() {
    const nameEl = document.getElementById('domainName');
    const descEl = document.getElementById('domainDescription');
    const authorEl = document.getElementById('domainAuthor');
    const versionEl = document.getElementById('domainVersionSelect');
    const baseUriEl = document.getElementById('domainBaseUri');
    const llmEndpointEl = document.getElementById('domainLlmEndpoint');

    if (!nameEl || !descEl || !authorEl) {
        showNotification('Form fields not found', 'error');
        return;
    }

    const domainName = nameEl.value.trim();
    if (!domainName) {
        showNotification('Domain name is required', 'warning');
        return;
    }
    if (!/^[A-Z][a-zA-Z0-9]*$/.test(domainName)) {
        showNotification('Domain Name must be CamelCase and alphanumeric only', 'warning');
        return;
    }

    const domainInfoPayload = {
        name: domainName,
        version: versionEl ? versionEl.value : '1',
        description: descEl.value.trim(),
        author: authorEl.value.trim(),
        base_uri: baseUriEl ? baseUriEl.value.trim() : '',
        base_uri_auto: _baseUriAutoMode,
        llm_endpoint: llmEndpointEl ? llmEndpointEl.value : '',
        
        delta: {
            catalog: document.getElementById('triplestoreLocationWidget_catalog')?.value || '',
            schema: document.getElementById('triplestoreLocationWidget_schema')?.value || '',
            table_name: document.getElementById('domainTriplestoreTableName')?.value.trim() || '',
        },
    };
    
    try {
        showNotification('Saving domain info...', 'info', 1000);
        
        const response = await fetch('/domain/info', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(domainInfoPayload),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('Domain info saved successfully!', 'success');
            // Update the navbar domain name
            const currentDomainNameEl = document.getElementById('currentDomainName');
            if (currentDomainNameEl) {
                currentDomainNameEl.textContent = domainInfoPayload.name || 'Domain';
            }
        } else {
            showNotification('Error: ' + (data.message || 'Failed to save'), 'error');
        }
    } catch (error) {
        console.error('Save error:', error);
        showNotification('Failed to save: ' + error.message, 'error');
    }
}

// Note: save-to-local helpers live in domain template partials when present.

function loadDomainFromFile() {
    document.getElementById('domainFileInput').click();
}

async function handleDomainFileUpload(input) {
    const file = input.files[0];
    if (!file) return;
    
    if (typeof showDomainLoading === 'function') showDomainLoading('Loading domain...');
    try {
        const formData = new FormData();
        formData.append('file', file);
        
        const response = await fetch('/domain/import', {
            method: 'POST',
            body: formData,
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('Domain loaded successfully!', 'success');
            setTimeout(() => location.reload(), 1000);
        } else {
            if (typeof hideDomainLoading === 'function') hideDomainLoading();
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        if (typeof hideDomainLoading === 'function') hideDomainLoading();
        showNotification('Failed to load: ' + error.message, 'error');
    }
    
    input.value = '';
}

async function newDomain() {
    const confirmed = await showConfirmDialog({
        title: 'New Domain',
        message: 'Start a new domain? This will clear all current data.',
        confirmText: 'Start New',
        confirmClass: 'btn-warning',
        icon: 'file-earmark-plus'
    });
    if (!confirmed) return;
    
    try {
        const response = await fetch('/domain/clear', {
            method: 'POST',
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('New domain started!', 'success');
            setTimeout(() => location.reload(), 1000);
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Failed: ' + error.message, 'error');
    }
}

/**
 * Save domain from settings page with validation
 * For new domains, requires Domain Name and Base URI
 */
async function saveDomainFromSettings() {
    const nameEl = document.getElementById('domainName');
    const baseUriEl = document.getElementById('domainBaseUri');
    
    // Get values
    const domainName = nameEl ? nameEl.value.trim() : '';
    const baseUri = baseUriEl ? baseUriEl.value.trim() : '';
    
    // Clear previous validation states
    if (nameEl) nameEl.classList.remove('is-invalid');
    if (baseUriEl) baseUriEl.classList.remove('is-invalid');
    
    // Validate required fields
    let hasErrors = false;
    
    if (!domainName) {
        if (nameEl) {
            nameEl.classList.add('is-invalid');
            nameEl.focus();
        }
        showNotification('Domain Name is required', 'warning');
        hasErrors = true;
    } else if (!/^[A-Z][a-zA-Z0-9]*$/.test(domainName)) {
        if (nameEl) {
            nameEl.classList.add('is-invalid');
            if (!hasErrors) nameEl.focus();
        }
        showNotification('Domain Name must be CamelCase and alphanumeric only (e.g. MyOntologyDomain)', 'warning');
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
            showNotification('Domain Name and Base URI are required', 'warning');
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
    
    // Save domain info to session and open Unity Catalog save dialog
    // (saveDomainInfo only saves to session; domainSave opens the UC dialog for actual persistence)
    if (typeof domainSave === 'function') {
        await domainSave();
    } else {
        await saveDomainInfo();
        showNotification('Domain info saved. Use the menu "Save Domain" to persist to Unity Catalog.', 'info', 4000);
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
