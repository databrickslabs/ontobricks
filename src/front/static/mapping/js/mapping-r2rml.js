/**
 * OntoBricks - mapping-r2rml.js
 * R2RML export for Mapping Studio (toolbar "Export" button).
 * The former Mapping → General → R2RML preview tab was removed; import lives
 * in mapping-import.js.
 */

// Force regenerate R2RML from the current mappings and return the Turtle text.
async function forceRegenerateR2RML() {
    if (!MappingState.config.entities || MappingState.config.entities.length === 0) {
        return '';
    }

    try {
        const response = await fetch('/mapping/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });

        const result = await response.json();

        if (result.success && result.r2rml) {
            MappingState.r2rmlContent = result.r2rml;
            return result.r2rml;
        }
    } catch (error) {
        console.error('Error generating R2RML:', error);
    }
    return '';
}

// Expose for global use
window.forceRegenerateR2RML = forceRegenerateR2RML;

async function _exportR2RMLText(r2rmlText) {
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
}

// Studio toolbar: regenerate then export
function _initR2RMLExportButton() {
    document.getElementById('mappingExportR2RMLBtn')?.addEventListener('click', async function() {
        const btn = this;
        btn.disabled = true;
        const original = btn.innerHTML;
        btn.innerHTML = '<i class="bi bi-hourglass-split me-1"></i> Exporting...';
        try {
            const generated = await forceRegenerateR2RML();
            await _exportR2RMLText(generated || MappingState.r2rmlContent || '');
        } catch (error) {
            showNotification('Error exporting R2RML: ' + error.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = original;
        }
    });
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _initR2RMLExportButton);
} else {
    _initR2RMLExportButton();
}
