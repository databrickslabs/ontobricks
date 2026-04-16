/**
 * OntoBricks - mapping-r2rml.js
 * Extracted from mapping templates per code_instructions.txt
 */

// R2RML SECTION - R2RML Generation and Preview
// =====================================================

function hideR2RMLLoadingOverlay() {
    const overlay = document.getElementById('r2rmlLoadingOverlay');
    if (overlay) overlay.style.display = 'none';
}

// Load existing R2RML from MappingState, session, or generate new one
async function generateR2RMLPreview() {
    const r2rmlPreview = document.getElementById('r2rmlPreview');
    
    // If textarea already has content, don't reload
    if (r2rmlPreview.value && r2rmlPreview.value.trim()) {
        console.log('R2RML Section: Content already in textarea');
        hideR2RMLLoadingOverlay();
        return;
    }
    
    // Check if we have R2RML in MappingState (from auto-generation)
    if (MappingState.r2rmlContent && MappingState.r2rmlContent.trim()) {
        console.log('R2RML Section: Using content from MappingState');
        r2rmlPreview.value = MappingState.r2rmlContent;
        hideR2RMLLoadingOverlay();
        return;
    }
    
    if (!MappingState.config.entities || MappingState.config.entities.length === 0) {
        r2rmlPreview.value = '';
        r2rmlPreview.placeholder = 'No entity mappings configured yet. Go to "Entities" section to create mappings first.';
        hideR2RMLLoadingOverlay();
        return;
    }
    
    try {
        // Try to load existing R2RML from session
        const loadResponse = await fetch('/mapping/load', { credentials: 'same-origin' });
        const loadResult = await loadResponse.json();
        
        if (loadResult.r2rml_output && loadResult.r2rml_output.trim()) {
            console.log('R2RML Section: Loaded existing R2RML from session');
            r2rmlPreview.value = loadResult.r2rml_output;
            MappingState.r2rmlContent = loadResult.r2rml_output;
            return;
        }
        
        // If no saved R2RML, generate new one
        console.log('R2RML Section: No saved R2RML, generating new...');
        const response = await fetch('/mapping/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success && result.r2rml) {
            r2rmlPreview.value = result.r2rml;
            MappingState.r2rmlContent = result.r2rml;
        } else {
            r2rmlPreview.value = '';
        }
    } catch (error) {
        console.error('Error loading R2RML:', error);
        r2rmlPreview.value = '';
    } finally {
        hideR2RMLLoadingOverlay();
    }
}

// Force regenerate R2RML (can be called when mappings change)
async function forceRegenerateR2RML() {
    const r2rmlPreview = document.getElementById('r2rmlPreview');
    
    if (!MappingState.config.entities || MappingState.config.entities.length === 0) {
        r2rmlPreview.value = '';
        return;
    }
    
    try {
        const response = await fetch('/mapping/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success && result.r2rml) {
            r2rmlPreview.value = result.r2rml;
        }
    } catch (error) {
        console.error('Error generating R2RML:', error);
    }
}

// Expose for global use
window.forceRegenerateR2RML = forceRegenerateR2RML;

// Attach button handlers once the DOM is fully parsed (the script loads
// before the R2RML partial HTML, so elements don't exist at parse time).
function _initR2RMLButtons() {
    // Regenerate R2RML (manual trigger)
    document.getElementById('regenerateR2RMLBtn')?.addEventListener('click', async function() {
        const btn = this;
        btn.disabled = true;
        btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Generating...';

        try {
            const r2rmlPreview = document.getElementById('r2rmlPreview');
            r2rmlPreview.value = '';
            r2rmlPreview.placeholder = 'Regenerating R2RML...';

            await forceRegenerateR2RML();

            if (r2rmlPreview.value) {
                showNotification('R2RML regenerated successfully', 'success', 3000);
            }
        } catch (error) {
            showNotification('Error regenerating R2RML: ' + error.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Regenerate';
        }
    });

    // Copy R2RML to clipboard
    document.getElementById('copyR2RMLBtn')?.addEventListener('click', function() {
        const r2rmlText = document.getElementById('r2rmlPreview').value;
        if (!r2rmlText) {
            showNotification('No R2RML content to copy', 'warning');
            return;
        }
        navigator.clipboard.writeText(r2rmlText).then(() => {
            showNotification('R2RML copied to clipboard', 'success', 2000);
        }).catch(err => {
            showNotification('Failed to copy: ' + err.message, 'error');
        });
    });

    // Download R2RML as file
    document.getElementById('downloadR2RMLBtn')?.addEventListener('click', async function() {
        const r2rmlText = document.getElementById('r2rmlPreview').value;
        if (!r2rmlText) {
            showNotification('No R2RML content to download', 'warning');
            return;
        }

        let domainName = 'mapping';
        try {
            const response = await fetch('/domain/info', { credentials: 'same-origin' });
            const data = await response.json();
            if (data.success && data.info?.name) {
                domainName = data.info.name;
            }
        } catch (e) {}

        const filename = domainName.replace(/\s+/g, '_') + '_r2rml.ttl';
        const blob = new Blob([r2rmlText], { type: 'text/turtle' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
        showNotification('R2RML file downloaded: ' + filename, 'success', 3000);
    });
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _initR2RMLButtons);
} else {
    _initR2RMLButtons();
}
