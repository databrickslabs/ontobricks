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
            if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
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
            if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
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
            if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
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
        refreshDtNamesFromForm();

        // Re-derive the DT names whenever the Triple Store tab is shown
        // (panel is initially hidden) and whenever the user commits a
        // change to the domain name (on blur / Enter, NOT per-keystroke).
        const tsTab = document.getElementById('tab-triplestore');
        if (tsTab) {
            tsTab.addEventListener('shown.bs.tab', refreshDtNamesFromForm);
        }
        const nameEl = document.getElementById('domainName');
        if (nameEl) {
            nameEl.addEventListener('change', refreshDtNamesFromForm);
            nameEl.addEventListener('blur', refreshDtNamesFromForm);
            // Duplicate-name guard runs on blur as well as the
            // existing debounced ``input`` hook in domain.js, so the
            // user gets immediate feedback when committing the field.
            // The check itself, the inline ``invalid-feedback`` hint,
            // and the ``is-invalid`` class are owned by domain.js.
            if (typeof checkDomainNameAvailability === 'function') {
                nameEl.addEventListener('blur', () => checkDomainNameAvailability(nameEl));
            }
        }
        const versionEl = document.getElementById('domainVersionSelect');
        if (versionEl) {
            versionEl.addEventListener('change', refreshDtNamesFromForm);
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
        }

        // The DT panel reads catalog/schema from the dropdown rendering of
        // version-status; rerun once after status is applied so the FQNs
        // line up with the real registry config.
        refreshDtNamesFromForm();
        
        // Re-enable version selector after a short delay to override any global disabling
        setTimeout(enableVersionSelector, 100);
    } catch (e) {
        console.log('Could not fetch domain status:', e);
    } finally {
        // Hand-off from navbar.js domainNew(): hide the overlay once the
        // initial info round-trips have resolved.
        try {
            if (sessionStorage.getItem('ob_creating_new_domain') === '1') {
                sessionStorage.removeItem('ob_creating_new_domain');
                if (typeof hideDomainLoading === 'function') hideDomainLoading();
            }
        } catch (e) {}
    }
});


/**
 * Lowercase slug with non ``[a-z0-9_]`` replaced by ``_`` — matches
 * the triple-store / snapshot *table* naming helpers in the backend.
 * The **registry folder** slug uses ``sanitize_domain_folder`` instead
 * (non-alphanumerics stripped, not replaced); with CamelCase-only
 * domain names the two coincide; they can diverge if validation ever
 * loosens.
 */
function _safeDomainSlug(name) {
    return (name || '').toLowerCase().replace(/[^a-z0-9_]/g, '_');
}

/**
 * Best-effort split of an existing FQN ("catalog.schema.table") into
 * its catalog/schema parts so we can keep the registry prefix while
 * rewriting the table portion. Returns ``null`` when the value is
 * empty or not a 3-part dotted name (i.e. registry not configured).
 */
function _splitFqnPrefix(fqn) {
    const parts = (fqn || '').split('.');
    if (parts.length === 3 && parts[0] && parts[1]) {
        return { catalog: parts[0], schema: parts[1] };
    }
    return null;
}

/**
 * Recompute the Triple-Store FQN, Snapshot FQN, and local Ladybug
 * path from the current domain name + version inputs. Mirrors the
 * backend naming rules so the user sees what UC objects *will be*
 * called once they save the domain — without round-tripping to the
 * server. Bound to the domain-name ``change``/``blur`` event so it
 * runs once per committed name change, not on every keystroke.
 */
function refreshDtNamesFromForm() {
    const nameEl = document.getElementById('domainName');
    const versionEl = document.getElementById('domainVersionSelect');
    const name = nameEl ? nameEl.value.trim() : '';
    const safe = _safeDomainSlug(name);
    const version = versionEl ? versionEl.value.trim() : '1';
    const v = version || '1';
    const safeVersion = _safeDomainSlug(v);

    const tsEl = document.getElementById('domainTriplestoreFullName');
    if (tsEl) {
        const prefix = _splitFqnPrefix(tsEl.value);
        const tsName = 'triplestore_' + safe + '_V' + v;
        tsEl.value = prefix ? prefix.catalog + '.' + prefix.schema + '.' + tsName : (safe ? tsName : '');
    }

    const snapEl = document.getElementById('domainSnapshotTableName');
    if (snapEl) {
        const prefix = _splitFqnPrefix(snapEl.value);
        const snapName = '_ob_snapshot_' + safe + '_v' + safeVersion;
        snapEl.value = prefix ? prefix.catalog + '.' + prefix.schema + '.' + snapName : (safe ? snapName : '');
    }

    const localEl = document.getElementById('ladybugLocalPath');
    if (localEl && safe) {
        localEl.textContent = '/tmp/ontobricks/' + safe + '_V' + v + '.lbug';
    }
}

// Backwards-compatible alias: callers and tests still reference the
// old name; keep it working as a thin wrapper over the new function.
function updateGraphPaths() {
    refreshDtNamesFromForm();
}
