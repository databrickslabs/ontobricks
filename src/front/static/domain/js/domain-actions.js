/**
 * OntoBricks - domain-actions.js
 * Extracted from domain templates per code_instructions.txt
 *
 * Note: createNewVersion() lives in domain-information.js (single source of truth).
 * New Version (#btnAddVersion) must stay enabled regardless of lifecycle status
 * and ontology/mapping readiness — branching is always allowed from the tip.
 */

// Update version status display.
// ``editable`` is true only when the loaded version is DRAFT (regardless of
// whether it is the latest version) — older DRAFT versions are editable.
function updateVersionStatus(editable, version, isLatest) {
    const alert = document.getElementById('versionStatusAlert');
    const text = document.getElementById('versionStatusText');
    const saveBtn = document.getElementById('menuSaveDomain');
    // Branching a new version is independent of DRAFT/read-only and of
    // ontology/mapping validation — never disable this control here.
    const versionBtn = document.getElementById('btnAddVersion');

    if (editable) {
        if (alert) alert.className = 'alert alert-success mb-0';
        if (text) text.innerHTML = `<strong>Version ${version}</strong> is a <strong>Draft</strong>. You can modify ontology and mappings.`;
        if (saveBtn) saveBtn.disabled = false;
    } else {
        if (alert) alert.className = 'alert alert-warning mb-0';
        if (text) text.innerHTML = `<strong>Version ${version}</strong> is <strong>read-only</strong>. Set it back to Draft to make changes.`;
        if (saveBtn) saveBtn.disabled = true;
    }
    if (versionBtn) {
        versionBtn.disabled = false;
        versionBtn.classList.remove('disabled');
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', async function() {
    try {
        const data = await fetchOnce('/domain/version-status');
        if (data.success) {
            const editable = (data.status || 'DRAFT') === 'DRAFT';
            updateVersionStatus(editable, data.version, data.is_latest);
        }
    } catch (e) {
        console.log('Could not fetch version status');
    }
});
