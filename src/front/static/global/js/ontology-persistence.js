/**
 * Ontology session / registry persistence helpers.
 *
 * Extracted from the former OntoViz design module so unload beacons and the
 * subnav dirty indicator keep working after the Design canvas was removed.
 */

let registryDirty = false;
let unloadOntologyFlushed = false;

/**
 * Mark ontology edits as not yet persisted to the registry.
 */
function markOntologyRegistryDirty() {
    registryDirty = true;
    if (typeof setDomainDirty === 'function') setDomainDirty('ontology', true);
}

/**
 * Auto-save design changes to the registry (silent, no confirmation).
 * Called when the user leaves a dirty ontology section or navigates away.
 */
async function autoSaveToRegistry() {
    if (!registryDirty) return;
    try {
        console.log('[AUTO-SAVE] Saving ontology changes to registry...');
        if (typeof showNotification === 'function') {
            showNotification('Auto-saving to registry…', 'info', 3000);
        }
        const response = await fetch('/domain/save-to-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({}),
            credentials: 'same-origin'
        });
        const data = await response.json();
        if (data.success) {
            registryDirty = false;
            if (typeof setDomainDirty === 'function') setDomainDirty('ontology', false);
            console.log('[AUTO-SAVE] Registry save successful');
            if (typeof showNotification === 'function') {
                showNotification('Ontology auto-saved to registry', 'success', 2500);
            }
        } else {
            console.warn('[AUTO-SAVE] Registry save failed:', data.message);
        }
    } catch (e) {
        console.warn('[AUTO-SAVE] Registry save error:', e);
    }
}

/**
 * Reset the registry-dirty flag (called after an explicit manual save).
 */
function clearRegistryDirty() {
    registryDirty = false;
    if (typeof setDomainDirty === 'function') setDomainDirty('ontology', false);
}

/**
 * Fire-and-forget registry save on page unload using fetch keepalive.
 * Flushes the shared panel first so a pending edit is in the session before
 * save-to-uc runs.
 */
function saveRegistryOnUnload() {
    if (typeof flushSharedPanelOnUnload === 'function') flushSharedPanelOnUnload();
    if (!registryDirty) return;
    if (unloadOntologyFlushed) return;
    try {
        fetch('/domain/save-to-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({}),
            credentials: 'same-origin',
            keepalive: true
        });
        console.log('[UNLOAD] Registry save request sent (keepalive)');
    } catch (e) {
        console.warn('[UNLOAD] Registry save on unload failed:', e);
    }
}

window.addEventListener('beforeunload', saveRegistryOnUnload);
window.addEventListener('pagehide', saveRegistryOnUnload);

window.markOntologyRegistryDirty = markOntologyRegistryDirty;
window.autoSaveToRegistry = autoSaveToRegistry;
window.clearRegistryDirty = clearRegistryDirty;
window.saveRegistryOnUnload = saveRegistryOnUnload;
