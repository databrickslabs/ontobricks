/**
 * OntoBricks - ontology-import.js
 * Import section for the Ontology page - OWL, RDFS, FIBO, CDISC, IOF
 */

// IMPORT SECTION - Import OWL, RDFS, FIBO, CDISC, IOF
// =====================================================

// --- OWL Import ---
document.getElementById('importOwlLocalBtn').addEventListener('click', function() {
    document.getElementById('importOwlFileInput').click();
});

document.getElementById('importOwlUCBtn').addEventListener('click', function() {
    UCFileDialog.open({
        mode: 'load',
        title: 'Import OWL from Unity Catalog',
        extensions: ['.ttl', '.owl', '.rdf'],
        onSelect: async function(fileInfo) {
            await parseAndLoadOwlFromProject(fileInfo.content, fileInfo.filename);
        }
    });
});

document.getElementById('importOwlFileInput').addEventListener('change', async function(e) {
    const file = e.target.files[0];
    if (!file) return;
    
    try {
        showNotification('Importing OWL file...', 'info', 2000);
        const content = await file.text();
        await parseAndLoadOwlFromProject(content, file.name);
    } catch (error) {
        showNotification('Error reading file: ' + error.message, 'error');
    }
    this.value = '';
});

async function parseAndLoadOwlFromProject(content, filename) {
    try {
        const response = await fetch('/ontology/parse-owl', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ content: content }),
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success) {
            const stats = result.stats || {};
            const classCount = stats.classes || 0;
            const propCount = stats.properties || 0;
            
            showNotification(`OWL imported: ${classCount} classes, ${propCount} relationships from ${filename}`, 'success');
            
            // Reload ontology state from session so UI reflects the import
            if (typeof loadOntologyFromSession === 'function') {
                await loadOntologyFromSession();
            }
            if (typeof refreshOntologyStatus === 'function') refreshOntologyStatus();
        } else {
            showNotification('Error parsing OWL: ' + result.message, 'error');
        }
    } catch (error) {
        showNotification('Error importing OWL: ' + error.message, 'error');
    }
}

// --- RDFS Import ---
document.getElementById('importRdfsLocalBtn').addEventListener('click', function() {
    document.getElementById('importRdfsFileInput').click();
});

document.getElementById('importRdfsUCBtn').addEventListener('click', function() {
    UCFileDialog.open({
        mode: 'load',
        title: 'Import RDFS from Unity Catalog',
        extensions: ['.ttl', '.rdf', '.xml', '.rdfs', '.n3', '.nt'],
        onSelect: async function(fileInfo) {
            await parseAndLoadRdfsFromProject(fileInfo.content, fileInfo.filename);
        }
    });
});

document.getElementById('importRdfsFileInput').addEventListener('change', async function(e) {
    const file = e.target.files[0];
    if (!file) return;
    
    try {
        showNotification('Importing RDFS file...', 'info', 2000);
        const content = await file.text();
        await parseAndLoadRdfsFromProject(content, file.name);
    } catch (error) {
        showNotification('Error reading file: ' + error.message, 'error');
    }
    this.value = '';
});

async function parseAndLoadRdfsFromProject(content, filename) {
    try {
        const response = await fetch('/ontology/parse-rdfs', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ content: content }),
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success) {
            const stats = result.stats || {};
            const classCount = stats.classes || 0;
            const propCount = stats.properties || 0;
            
            showNotification(`RDFS imported: ${classCount} classes, ${propCount} relationships from ${filename}`, 'success');
            
            // Reload ontology state from session so UI reflects the import
            if (typeof loadOntologyFromSession === 'function') {
                await loadOntologyFromSession();
            }
            if (typeof refreshOntologyStatus === 'function') refreshOntologyStatus();
        } else {
            showNotification('Error parsing RDFS: ' + result.message, 'error');
        }
    } catch (error) {
        showNotification('Error importing RDFS: ' + error.message, 'error');
    }
}

// --- FIBO Import ---
document.getElementById('importFiboBtn').addEventListener('click', async function() {
    // Collect selected domains (FND is always included)
    const domains = ['FND'];
    document.querySelectorAll('.fibo-domain-cb:checked').forEach(function(cb) {
        if (!domains.includes(cb.value)) {
            domains.push(cb.value);
        }
    });

    if (domains.length === 1) {
        // Only FND selected – still valid but ask if intentional
        const proceed = confirm(
            'Only Foundations (FND) is selected. This imports core concepts only.\n\n' +
            'Select additional domains (BE, FBC, etc.) for a richer ontology.\n\nProceed with FND only?'
        );
        if (!proceed) return;
    }

    const btn = document.getElementById('importFiboBtn');
    const progress = document.getElementById('fiboImportProgress');
    const statusEl = document.getElementById('fiboImportStatus');

    // Disable button and show progress
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Importing...';
    progress.classList.remove('d-none');
    statusEl.textContent = 'Fetching FIBO modules from spec.edmcouncil.org... This may take 15-30 seconds.';

    try {
        const response = await fetch('/ontology/import-fibo', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ domains: domains }),
            credentials: 'same-origin'
        });

        const result = await response.json();

        if (result.success) {
            const stats = result.stats || {};
            showNotification(
                'FIBO imported: ' + (stats.classes || 0) + ' classes, ' +
                (stats.properties || 0) + ' relationships' +
                (stats.modules_failed > 0
                    ? ' (' + stats.modules_failed + ' modules unavailable)'
                    : ''),
                'success'
            );
            statusEl.textContent = 'Import complete!';

            // Show warning if some modules failed
            if (result.failed && result.failed.length > 0) {
                console.warn('[FIBO] Unavailable modules:', result.failed);
                showNotification(
                    result.failed.length + ' module(s) could not be fetched. See console for details.',
                    'warning'
                );
            }

            // Reload ontology state from session so UI reflects the import
            if (typeof loadOntologyFromSession === 'function') {
                await loadOntologyFromSession();
            }
            if (typeof refreshOntologyStatus === 'function') refreshOntologyStatus();
        } else {
            showNotification('FIBO import failed: ' + result.message, 'error');
            statusEl.textContent = 'Import failed.';
        }
    } catch (error) {
        showNotification('FIBO import error: ' + error.message, 'error');
        statusEl.textContent = 'Import error.';
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-download"></i> Import Selected Domains';
        setTimeout(function() { progress.classList.add('d-none'); }, 3000);
    }
});

// --- CDISC Import ---
document.getElementById('importCdiscBtn').addEventListener('click', async function() {
    // Collect selected domains (SCHEMAS is always included)
    const domains = ['SCHEMAS'];
    document.querySelectorAll('.cdisc-domain-cb:checked').forEach(function(cb) {
        if (!domains.includes(cb.value)) {
            domains.push(cb.value);
        }
    });

    if (domains.length === 1) {
        // Only SCHEMAS selected – still valid but ask if intentional
        const proceed = confirm(
            'Only Schemas are selected. This imports the meta-model only.\n\n' +
            'Select additional standards (SDTM, CDASH, etc.) for a richer ontology.\n\nProceed with Schemas only?'
        );
        if (!proceed) return;
    }

    const btn = document.getElementById('importCdiscBtn');
    const progress = document.getElementById('cdiscImportProgress');
    const statusEl = document.getElementById('cdiscImportStatus');

    // Disable button and show progress
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Importing...';
    progress.classList.remove('d-none');
    statusEl.textContent = 'Fetching CDISC modules from GitHub... This may take 15-30 seconds.';

    try {
        const response = await fetch('/ontology/import-cdisc', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ domains: domains }),
            credentials: 'same-origin'
        });

        const result = await response.json();

        if (result.success) {
            showNotification(result.message || 'CDISC import complete', 'success');
            statusEl.textContent = 'Import complete!';

            // Show warning if some modules failed
            if (result.failed && result.failed.length > 0) {
                console.warn('[CDISC] Unavailable modules:', result.failed);
                showNotification(
                    result.failed.length + ' module(s) could not be fetched. See console for details.',
                    'warning'
                );
            }

            // Reload ontology state from session so UI reflects the import
            if (typeof loadOntologyFromSession === 'function') {
                await loadOntologyFromSession();
            }
            if (typeof refreshOntologyStatus === 'function') refreshOntologyStatus();
        } else {
            showNotification('CDISC import failed: ' + result.message, 'error');
            statusEl.textContent = 'Import failed.';
        }
    } catch (error) {
        showNotification('CDISC import error: ' + error.message, 'error');
        statusEl.textContent = 'Import error.';
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-download"></i> Import Selected Standards';
        setTimeout(function() { progress.classList.add('d-none'); }, 3000);
    }
});

// --- IOF Import ---
document.getElementById('importIofBtn').addEventListener('click', async function() {
    // Collect selected domains (CORE is always included)
    const domains = ['CORE'];
    document.querySelectorAll('.iof-domain-cb:checked').forEach(function(cb) {
        if (!domains.includes(cb.value)) {
            domains.push(cb.value);
        }
    });

    if (domains.length === 1) {
        const proceed = confirm(
            'Only Core is selected. This imports foundational manufacturing concepts only.\n\n' +
            'Select additional domains (Maintenance, Supply Chain) for a richer ontology.\n\nProceed with Core only?'
        );
        if (!proceed) return;
    }

    const btn = document.getElementById('importIofBtn');
    const progress = document.getElementById('iofImportProgress');
    const statusEl = document.getElementById('iofImportStatus');

    // Disable button and show progress
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Importing...';
    progress.classList.remove('d-none');
    statusEl.textContent = 'Fetching IOF modules from GitHub... This may take 15-30 seconds.';

    try {
        const response = await fetch('/ontology/import-iof', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ domains: domains }),
            credentials: 'same-origin'
        });

        const result = await response.json();

        if (result.success) {
            const stats = result.stats || {};
            showNotification(
                'IOF imported: ' + (stats.classes || 0) + ' classes, ' +
                (stats.properties || 0) + ' relationships' +
                (stats.modules_failed > 0
                    ? ' (' + stats.modules_failed + ' modules unavailable)'
                    : ''),
                'success'
            );
            statusEl.textContent = 'Import complete!';

            // Show warning if some modules failed
            if (result.failed && result.failed.length > 0) {
                console.warn('[IOF] Unavailable modules:', result.failed);
                showNotification(
                    result.failed.length + ' module(s) could not be fetched. See console for details.',
                    'warning'
                );
            }

            // Reload ontology state from session so UI reflects the import
            if (typeof loadOntologyFromSession === 'function') {
                await loadOntologyFromSession();
            }
            if (typeof refreshOntologyStatus === 'function') refreshOntologyStatus();
        } else {
            showNotification('IOF import failed: ' + result.message, 'error');
            statusEl.textContent = 'Import failed.';
        }
    } catch (error) {
        showNotification('IOF import error: ' + error.message, 'error');
        statusEl.textContent = 'Import error.';
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-download"></i> Import Selected Domains';
        setTimeout(function() { progress.classList.add('d-none'); }, 3000);
    }
});

