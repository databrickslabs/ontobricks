/**
 * OntoBricks - domain-r2rml.js
 * Extracted from domain templates per code_instructions.txt
 */

// R2RML SECTION - Load, Regenerate, Copy & Download
// =====================================================

// Make function globally accessible
window.loadR2RMLContent = async function() {
    const r2rmlPreview = document.getElementById('r2rmlPreview');
    if (!r2rmlPreview) {
        console.error('R2RML preview textarea not found');
        return;
    }
    
    console.log('Loading R2RML content...');
    r2rmlPreview.placeholder = 'Loading R2RML content...';
    
    try {
        // Try to load existing R2RML from session
        const loadResponse = await fetch('/mapping/load', { credentials: 'same-origin' });
        const loadResult = await loadResponse.json();
        console.log('Mapping load response:', loadResult);
        
        // Check for existing R2RML content (available at root level)
        if (loadResult.r2rml_output && loadResult.r2rml_output.trim()) {
            r2rmlPreview.value = loadResult.r2rml_output;
            return;
        }
        
        // Check if we have mappings (inside config object)
        const mappings = loadResult.config?.entities || [];
        
        if (mappings.length > 0) {
            console.log('Generating R2RML from mappings...');
            // Generate R2RML from backend
            const response = await fetch('/mapping/generate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin'
            });
            
            const result = await response.json();
            console.log('R2RML generate response:', result);
            
            if (result.success && result.r2rml) {
                r2rmlPreview.value = result.r2rml;
            } else {
                r2rmlPreview.value = '';
                r2rmlPreview.placeholder = result.message || 'Failed to generate R2RML';
            }
        } else {
            r2rmlPreview.value = '';
            r2rmlPreview.placeholder = 'No entity mappings configured yet. Go to "Mapping" page to create mappings first.';
        }
    } catch (error) {
        console.error('Error loading R2RML:', error);
        r2rmlPreview.value = '';
        r2rmlPreview.placeholder = 'Error loading R2RML content: ' + error.message;
    }
}

// Regenerate R2RML (manual trigger)
document.getElementById('regenerateR2RMLBtn')?.addEventListener('click', async function() {
    const btn = this;
    btn.disabled = true;
    btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Generating...';
    
    try {
        const response = await fetch('/mapping/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        const r2rmlPreview = document.getElementById('r2rmlPreview');
        
        if (result.success && result.r2rml) {
            r2rmlPreview.value = result.r2rml;
            showNotification('R2RML regenerated successfully', 'success', 3000);
        } else {
            showNotification(result.message || 'Error regenerating R2RML', 'error');
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
    
    // Get domain name for filename
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
