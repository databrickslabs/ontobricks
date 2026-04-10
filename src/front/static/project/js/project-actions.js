/**
 * OntoBricks - project-actions.js
 * Extracted from project templates per code_instructions.txt
 *
 * Note: createNewVersion() lives in project-information.js (single source of truth).
 */

// Update version status display
function updateVersionStatus(isActive, version, isLatest) {
    const alert = document.getElementById('versionStatusAlert');
    const text = document.getElementById('versionStatusText');
    const saveBtn = document.getElementById('btnSaveProject');
    const versionBtn = document.getElementById('btnCreateVersion');
    
    if (isActive) {
        alert.className = 'alert alert-success mb-0';
        text.innerHTML = `<strong>Version ${version}</strong> is the <strong>Active</strong> version. You can modify ontology and mappings.`;
        if (saveBtn) saveBtn.disabled = false;
        if (versionBtn) versionBtn.disabled = false;
    } else {
        alert.className = 'alert alert-warning mb-0';
        text.innerHTML = `<strong>Version ${version}</strong> is <strong>Inactive</strong> (read-only). Load the latest version to make changes.`;
        if (saveBtn) saveBtn.disabled = true;
        if (versionBtn) versionBtn.disabled = true;
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', async function() {
    try {
        const data = await fetchOnce('/project/version-status');
        if (data.success) {
            updateVersionStatus(data.is_active, data.version, data.is_latest);
        }
    } catch (e) {
        console.log('Could not fetch version status');
    }
});
