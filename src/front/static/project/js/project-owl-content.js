/**
 * OntoBricks - project-owl-content.js
 * Extracted from project templates per code_instructions.txt
 */

// OWL CONTENT SECTION - Load, Regenerate, Copy & Download
// =====================================================

// Make function globally accessible
window.loadOwlContent = async function() {
    const owlPreview = document.getElementById('owlPreview');
    if (!owlPreview) {
        console.error('OWL preview textarea not found');
        return;
    }
    
    console.log('Loading OWL content...');
    owlPreview.placeholder = 'Loading OWL content...';
    
    try {
        // Use the export-owl endpoint which generates OWL from the project's ontology
        const response = await fetch('/ontology/export-owl', { credentials: 'same-origin' });
        const result = await response.json();
        console.log('OWL export response:', result);
        
        if (result.success && result.owl_content) {
            owlPreview.value = result.owl_content;
        } else {
            owlPreview.value = '';
            owlPreview.placeholder = result.message || 'No ontology configured yet. Add entities in the Ontology page first.';
        }
    } catch (error) {
        console.error('Error loading OWL:', error);
        owlPreview.value = '';
        owlPreview.placeholder = 'Error loading OWL content: ' + error.message;
    }
}

// Regenerate OWL (manual trigger)
document.getElementById('regenerateOwlBtn')?.addEventListener('click', async function() {
    const btn = this;
    btn.disabled = true;
    btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Generating...';
    
    try {
        const owlPreview = document.getElementById('owlPreview');
        owlPreview.value = '';
        owlPreview.placeholder = 'Regenerating OWL...';
        
        await window.loadOwlContent();
        
        if (owlPreview.value) {
            showNotification('OWL regenerated successfully', 'success', 3000);
        }
    } catch (error) {
        showNotification('Error regenerating OWL: ' + error.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Regenerate';
    }
});

// Copy OWL to clipboard
document.getElementById('copyOwlBtn')?.addEventListener('click', function() {
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
document.getElementById('downloadOwlBtn')?.addEventListener('click', async function() {
    const owlText = document.getElementById('owlPreview').value;
    if (!owlText || owlText.startsWith('<!-- OWL GENERATION ERROR -->')) {
        showNotification('No valid OWL content to download', 'warning');
        return;
    }
    
    // Get project name for filename
    let projectName = 'ontology';
    try {
        const response = await fetch('/project/info', { credentials: 'same-origin' });
        const data = await response.json();
        if (data.success && data.info?.name) {
            projectName = data.info.name;
        }
    } catch (e) {}
    
    const filename = projectName.replace(/\s+/g, '_') + '.ttl';
    const blob = new Blob([owlText], { type: 'text/turtle' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
    showNotification('OWL file downloaded: ' + filename, 'success', 3000);
});
