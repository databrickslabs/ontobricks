/**
 * OntoBricks - ontology-owl-content.js
 * Extracted from ontology templates per code_instructions.txt
 */

// OWL CONTENT SECTION - Regenerate, Copy & Download
// =====================================================

// Regenerate OWL (manual trigger)
document.getElementById('regenerateOwl')?.addEventListener('click', async function() {
    const btn = this;
    btn.disabled = true;
    btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Generating...';
    
    try {
        await autoGenerateOwl();
        showNotification('OWL regenerated successfully', 'success', 3000);
    } catch (error) {
        showNotification('Error regenerating OWL: ' + error.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Regenerate';
    }
});

// Copy OWL to clipboard
document.getElementById('copyOwl')?.addEventListener('click', function() {
    const owlText = document.getElementById('owlPreview').value;
    if (!owlText || owlText.startsWith('<!-- OWL GENERATION ERROR -->')) {
        showNotification('No valid OWL content to copy', 'warning');
        return;
    }
    navigator.clipboard.writeText(owlText).then(() => {
        showNotification('OWL copied to clipboard', 'success', 3000);
    });
});

// Download OWL as file
document.getElementById('downloadOwl')?.addEventListener('click', function() {
    const owlText = document.getElementById('owlPreview').value;
    if (!owlText || owlText.startsWith('<!-- OWL GENERATION ERROR -->')) {
        showNotification('No valid OWL content to download', 'warning');
        return;
    }
    
    const filename = (OntologyState.config.name || 'ontology').replace(/\s+/g, '_') + '.ttl';
    const blob = new Blob([owlText], { type: 'text/turtle' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
    showNotification('OWL file downloaded: ' + filename, 'success', 3000);
});
