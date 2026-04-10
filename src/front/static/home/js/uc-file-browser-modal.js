/**
 * OntoBricks - uc-file-browser-modal.js
 * Extracted from home templates per code_instructions.txt
 */

let ucBrowserCallback = null;
let ucBrowserExtensions = ['.ttl', '.owl', '.rdf'];

/**
 * Legacy: Open the UC File Browser modal
 * @param {Object} options - Configuration options
 * @param {string} options.title - Modal title
 * @param {Array} options.extensions - File extensions to filter
 * @param {Function} options.onSelect - Callback when file is selected (receives file info)
 */
function openUCFileBrowser(options = {}) {
    // Redirect to new UCFileDialog
    UCFileDialog.open({
        mode: 'load',
        title: options.title,
        extensions: options.extensions,
        onSelect: options.onSelect
    });
}

function formatFileSize(bytes) {
    if (!bytes) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}
