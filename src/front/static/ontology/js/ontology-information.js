/**
 * OntoBricks - ontology-information.js
 * Extracted from ontology templates per code_instructions.txt
 */

// INFORMATION SECTION - Event Handlers
// =====================================================

// Ontology name is readonly (derived from domain name lowercase).
// No input listener needed — the value is set by loadOntologyFromSession.

// Reset ontology
document.getElementById('resetOntology')?.addEventListener('click', async function() {
    const confirmed = await showConfirmDialog({
        title: 'Reset Ontology',
        message: 'Are you sure you want to reset the ontology? All entities and relationships will be cleared. <strong>This action cannot be undone.</strong>',
        confirmText: 'Reset',
        confirmClass: 'btn-danger',
        icon: 'exclamation-triangle'
    });
    if (!confirmed) return;
    
    try {
        const response = await fetch('/ontology/reset', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (!result.success) {
            showNotification('Error resetting: ' + result.message, 'error');
            return;
        }
        
        await fetch('/clear-ontology', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            credentials: 'same-origin'
        });
        
        let defaultName = 'myontology';
        try {
            const projResp = await fetch('/domain/info', { credentials: 'same-origin' });
            const projData = await projResp.json();
            if (projData.success && projData.info && projData.info.name) {
                defaultName = projData.info.name.toLowerCase();
            }
        } catch (_) { /* keep fallback */ }

        OntologyState.config = {
            name: defaultName,
            base_uri: OntologyState.config.base_uri,
            classes: [],
            properties: []
        };
        
        document.getElementById('ontologyName').value = defaultName;
        loadBaseUriFromDomain();
        
        if (typeof updateClassesList === 'function') updateClassesList();
        if (typeof updatePropertiesList === 'function') updatePropertiesList();
        document.getElementById('owlPreview').value = '';
        
        if (OntologyState.network) {
            OntologyState.network.setData({ nodes: [], edges: [] });
        }
        
        showNotification('Ontology has been reset. All entities and relationships cleared.', 'success', 3000);
        
        if (typeof window.refreshOntologyStatus === 'function') {
            window.refreshOntologyStatus();
        }
    } catch (error) {
        console.error('Error clearing session:', error);
        showNotification('Error during reset: ' + error.message, 'error');
    }
});

// Auto-validate Ontology (called after every change)
async function autoValidateOntology() {
    const statusEl = document.getElementById('ontologyValidationStatus');
    if (!statusEl) return;
    
    try {
        // Call the centralized validation endpoint
        const response = await fetch('/validate/ontology', { credentials: 'same-origin' });
        const result = await response.json();
        
        // Update the validation status display
        if (result.valid) {
            // Get stats from result or from OntologyState
            const stats = result.stats || {
                classes: OntologyState.config?.classes?.length || 0,
                properties: OntologyState.config?.properties?.length || 0
            };
            statusEl.innerHTML = `
                <span class="badge bg-success">
                    <i class="bi bi-check-circle-fill"></i> Valid
                </span>
                <small class="text-muted ms-2">${stats.classes} entities, ${stats.properties} relationships</small>
            `;
        } else if (result.issues && result.issues.length > 0) {
            const issueText = result.issues[0];
            statusEl.innerHTML = `
                <span class="badge bg-danger">
                    <i class="bi bi-x-circle-fill"></i> Invalid
                </span>
                <small class="text-danger ms-2">${issueText}</small>
            `;
        } else {
            statusEl.innerHTML = `
                <span class="badge bg-warning text-dark">
                    <i class="bi bi-exclamation-triangle-fill"></i> Incomplete
                </span>
                <small class="text-muted ms-2">Add entities to create a valid ontology</small>
            `;
        }
        
        // Update navbar indicators
        if (typeof window.refreshOntologyStatus === 'function') {
            window.refreshOntologyStatus();
        }
        
        return result;
    } catch (error) {
        console.error('Auto-validation error:', error);
        statusEl.innerHTML = `
            <span class="badge bg-secondary">
                <i class="bi bi-question-circle"></i> Unknown
            </span>
        `;
        return null;
    }
}

// Make it globally available
window.autoValidateOntology = autoValidateOntology;

// Run auto-validation on page load
document.addEventListener('DOMContentLoaded', function() {
    setTimeout(autoValidateOntology, 500);
});

// Show validation summary in a modal
function showValidationSummary(config, issues, warnings) {
    const hasIssues = issues.length > 0;
    const hasWarnings = warnings.length > 0;
    const isValid = !hasIssues;
    
    let modalHtml = `
        <div class="modal fade" id="validationModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header ${isValid ? 'bg-success' : 'bg-danger'} text-white">
                        <h5 class="modal-title">
                            <i class="bi bi-${isValid ? 'check-circle' : 'x-circle'}"></i> 
                            Ontology Validation ${isValid ? 'Successful' : 'Failed'}
                        </h5>
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <h6><i class="bi bi-info-circle"></i> Summary</h6>
                        <table class="table table-sm">
                            <tr><td><strong>Name:</strong></td><td>${config.name}</td></tr>
                            <tr><td><strong>Base URI:</strong></td><td><code>${config.base_uri}</code></td></tr>
                            <tr><td><strong>Entities:</strong></td><td>${config.classes.length}</td></tr>
                            <tr><td><strong>Relationships:</strong></td><td>${config.properties.length}</td></tr>
                        </table>
    `;
    
    if (hasIssues) {
        modalHtml += `
                        <div class="alert alert-danger mt-3">
                            <h6><i class="bi bi-x-circle"></i> Issues</h6>
                            <ul class="mb-0">
                                ${issues.map(i => `<li>${i}</li>`).join('')}
                            </ul>
                        </div>
        `;
    }
    
    if (hasWarnings) {
        modalHtml += `
                        <div class="alert alert-warning mt-3">
                            <h6><i class="bi bi-exclamation-triangle"></i> Warnings</h6>
                            <ul class="mb-0">
                                ${warnings.map(w => `<li>${w}</li>`).join('')}
                            </ul>
                        </div>
        `;
    }
    
    if (isValid && !hasWarnings) {
        modalHtml += `
                        <div class="alert alert-success mt-3">
                            <i class="bi bi-check-circle"></i> Ontology is valid and ready to use!
                        </div>
        `;
    }
    
    modalHtml += `
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    const existingModal = document.getElementById('validationModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('validationModal'));
    modal.show();
}

// Import OWL Button - shows dropdown for import source
document.getElementById('importOwlBtn')?.addEventListener('click', function(e) {
    // Create dropdown menu if not exists
    let dropdown = document.getElementById('importOwlDropdown');
    if (!dropdown) {
        dropdown = document.createElement('div');
        dropdown.id = 'importOwlDropdown';
        dropdown.className = 'dropdown-menu show';
        dropdown.style.cssText = 'position: absolute; z-index: 1050;';
        dropdown.innerHTML = `
            <a class="dropdown-item" href="#" id="importOwlLocal">
                <i class="bi bi-hdd"></i> Import from Local File
            </a>
            <a class="dropdown-item" href="#" id="importOwlUC">
                <i class="bi bi-cloud-download"></i> Import from Unity Catalog
            </a>
        `;
        this.parentElement.appendChild(dropdown);
        
        // Position it below the button
        const rect = this.getBoundingClientRect();
        dropdown.style.top = (this.offsetTop + this.offsetHeight) + 'px';
        dropdown.style.left = this.offsetLeft + 'px';
        
        // Add event listeners
        document.getElementById('importOwlLocal').addEventListener('click', function(e) {
            e.preventDefault();
            dropdown.remove();
            document.getElementById('owlFileInput').click();
        });
        
        document.getElementById('importOwlUC').addEventListener('click', function(e) {
            e.preventDefault();
            dropdown.remove();
            importOwlFromUC();
        });
        
        // Close dropdown when clicking outside
        setTimeout(() => {
            document.addEventListener('click', function closeDropdown(event) {
                if (!dropdown.contains(event.target) && event.target !== document.getElementById('importOwlBtn')) {
                    dropdown.remove();
                    document.removeEventListener('click', closeDropdown);
                }
            });
        }, 100);
    } else {
        dropdown.remove();
    }
});

// Handle local file import
document.getElementById('owlFileInput')?.addEventListener('change', async function(e) {
    const file = e.target.files[0];
    if (!file) return;
    
    try {
        showNotification('Importing OWL file...', 'info', 2000);
        const content = await file.text();
        await parseAndLoadOwl(content, file.name);
    } catch (error) {
        showNotification('Error reading file: ' + error.message, 'error');
    }
    
    // Reset input
    this.value = '';
});

// Import from Unity Catalog
function importOwlFromUC() {
    UCFileDialog.open({
        mode: 'load',
        title: 'Import OWL from Unity Catalog',
        extensions: ['.ttl', '.owl', '.rdf'],
        onSelect: async function(fileInfo) {
            await parseAndLoadOwl(fileInfo.content, fileInfo.filename);
        }
    });
}

// Parse and load OWL content
async function parseAndLoadOwl(content, filename) {
    try {
        const response = await fetch('/ontology/parse-owl', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ content: content }),
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success) {
            const onto = result.ontology;
            
            // Separate ObjectProperties (relationships) from DatatypeProperties (attributes)
            const objectProperties = onto.properties.filter(p => p.type === 'ObjectProperty');
            const datatypeProperties = onto.properties.filter(p => p.type === 'DatatypeProperty' || p.type === 'Property');
            
            // Build a map of class name -> attributes from DatatypeProperties
            const classAttributesMap = {};
            const orphanAttributes = [];
            
            datatypeProperties.forEach(prop => {
                if (prop.domain) {
                    if (!classAttributesMap[prop.domain]) {
                        classAttributesMap[prop.domain] = [];
                    }
                    classAttributesMap[prop.domain].push({
                        name: prop.name,
                        localName: prop.name,
                        label: prop.label || prop.name,
                        description: prop.comment || '',
                        range: prop.range || 'string'
                    });
                } else {
                    orphanAttributes.push(prop);
                }
            });
            
            // Ontology name is always derived from the domain name (lowercase)
            let owlOntologyName = 'loadedontology';
            try {
                const piResp = await fetch('/domain/info', { credentials: 'same-origin' });
                const piData = await piResp.json();
                if (piData.success && piData.info && piData.info.name) {
                    owlOntologyName = piData.info.name.toLowerCase();
                }
            } catch (_) { /* keep fallback */ }
            OntologyState.config.name = owlOntologyName;
            OntologyState.config.base_uri = onto.info.namespace || onto.info.uri || OntologyState.baseUriDomain + '/LoadedOntology#';
            
            // Build classes with their attributes
            OntologyState.config.classes = onto.classes.map(cls => ({
                name: cls.name,
                label: cls.label || cls.name,
                description: cls.comment || '',
                parent: cls.parent || '',
                emoji: cls.emoji || OntologyState.defaultClassEmoji,
                dataProperties: classAttributesMap[cls.name] || []
            }));
            
            // Only store ObjectProperties as relationships
            OntologyState.config.properties = objectProperties.map(prop => ({
                name: prop.name,
                label: prop.label || prop.name,
                description: prop.comment || '',
                type: 'ObjectProperty',
                domain: prop.domain || '',
                range: prop.range || ''
            }));
            
            document.getElementById('ontologyName').value = OntologyState.config.name;
            document.getElementById('baseUri').value = OntologyState.config.base_uri;
            
            if (typeof updateClassesList === 'function') updateClassesList();
            if (typeof updatePropertiesList === 'function') updatePropertiesList();
            
            // Save loaded config to session immediately
            await window.saveConfigToSession();
            
            // Also update base_uri in domain settings (since it's managed there)
            try {
                await fetch('/domain/info', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ base_uri: OntologyState.config.base_uri }),
                    credentials: 'same-origin'
                });
            } catch (e) {
                console.log('Could not update domain base URI:', e);
            }
            
            await autoGenerateOwl();
            
            // Show detailed import report
            showOwlImportReport(filename, onto, {
                classes: onto.classes,
                objectProperties: objectProperties,
                datatypeProperties: datatypeProperties,
                orphanAttributes: orphanAttributes,
                classAttributesMap: classAttributesMap
            });
        } else {
            showNotification('Error parsing OWL: ' + result.message, 'error');
        }
    } catch (error) {
        showNotification('Error importing OWL: ' + error.message, 'error');
    }
}

// Show OWL import report modal
function showOwlImportReport(filename, onto, details) {
    // Remove existing modal if any
    const existingModal = document.getElementById('owlImportReportModal');
    if (existingModal) existingModal.remove();
    
    // Count attributes assigned to classes
    let totalAttributesAssigned = 0;
    Object.values(details.classAttributesMap).forEach(attrs => {
        totalAttributesAssigned += attrs.length;
    });
    
    // Build class details HTML
    let classDetailsHtml = '';
    details.classes.forEach(cls => {
        const attrs = details.classAttributesMap[cls.name] || [];
        classDetailsHtml += `
            <tr>
                <td><strong>${cls.name}</strong></td>
                <td>${cls.label || cls.name}</td>
                <td>${cls.parent || '<span class="text-muted">-</span>'}</td>
                <td>${attrs.length > 0 ? attrs.map(a => `<span class="badge bg-secondary me-1">${a.name}</span>`).join('') : '<span class="text-muted">none</span>'}</td>
            </tr>
        `;
    });
    
    // Build relationships HTML
    let relationshipsHtml = '';
    if (details.objectProperties.length > 0) {
        details.objectProperties.forEach(prop => {
            relationshipsHtml += `
                <tr>
                    <td><strong>${prop.name}</strong></td>
                    <td>${prop.domain || '<span class="text-muted">?</span>'}</td>
                    <td>${prop.range || '<span class="text-muted">?</span>'}</td>
                </tr>
            `;
        });
    } else {
        relationshipsHtml = '<tr><td colspan="3" class="text-muted text-center">No relationships found</td></tr>';
    }
    
    // Build orphan attributes HTML (if any)
    let orphanHtml = '';
    if (details.orphanAttributes.length > 0) {
        orphanHtml = `
            <div class="alert alert-warning mt-3">
                <i class="bi bi-exclamation-triangle me-2"></i>
                <strong>${details.orphanAttributes.length} attribute(s) without domain class:</strong>
                <ul class="mb-0 mt-2">
                    ${details.orphanAttributes.map(a => `<li>${a.name} (range: ${a.range || 'unspecified'})</li>`).join('')}
                </ul>
            </div>
        `;
    }
    
    const modalHtml = `
        <div class="modal fade" id="owlImportReportModal" tabindex="-1">
            <div class="modal-dialog modal-lg modal-dialog-scrollable">
                <div class="modal-content">
                    <div class="modal-header bg-primary text-white">
                        <h5 class="modal-title"><i class="bi bi-check-circle me-2"></i>OWL Import Report</h5>
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="alert alert-info">
                            <i class="bi bi-file-earmark-text me-2"></i>
                            <strong>File:</strong> ${filename}
                            <br>
                            <strong>Ontology:</strong> ${onto.info.label || 'Unnamed'} 
                            <small class="text-muted">(${onto.info.namespace || onto.info.uri || 'no namespace'})</small>
                        </div>
                        
                        <!-- Summary -->
                        <div class="row text-center mb-4">
                            <div class="col-3">
                                <div class="card bg-primary text-white">
                                    <div class="card-body py-2">
                                        <h3 class="mb-0">${details.classes.length}</h3>
                                        <small>Entities</small>
                                    </div>
                                </div>
                            </div>
                            <div class="col-3">
                                <div class="card bg-info text-white">
                                    <div class="card-body py-2">
                                        <h3 class="mb-0">${details.objectProperties.length}</h3>
                                        <small>Relationships</small>
                                    </div>
                                </div>
                            </div>
                            <div class="col-3">
                                <div class="card bg-secondary text-white">
                                    <div class="card-body py-2">
                                        <h3 class="mb-0">${totalAttributesAssigned}</h3>
                                        <small>Attributes</small>
                                    </div>
                                </div>
                            </div>
                            <div class="col-3">
                                <div class="card ${details.orphanAttributes.length > 0 ? 'bg-warning' : 'bg-success'} text-white">
                                    <div class="card-body py-2">
                                        <h3 class="mb-0">${details.orphanAttributes.length}</h3>
                                        <small>Warnings</small>
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Entities Table -->
                        <h6><i class="bi bi-diagram-3 me-2"></i>Imported Entities</h6>
                        <div class="table-responsive" style="max-height: 250px; overflow-y: auto;">
                            <table class="table table-sm table-striped">
                                <thead class="table-light sticky-top">
                                    <tr>
                                        <th>Name</th>
                                        <th>Label</th>
                                        <th>Parent</th>
                                        <th>Attributes</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${classDetailsHtml || '<tr><td colspan="4" class="text-muted text-center">No entities found</td></tr>'}
                                </tbody>
                            </table>
                        </div>
                        
                        <!-- Relationships Table -->
                        <h6 class="mt-4"><i class="bi bi-arrow-left-right me-2"></i>Imported Relationships</h6>
                        <div class="table-responsive" style="max-height: 200px; overflow-y: auto;">
                            <table class="table table-sm table-striped">
                                <thead class="table-light sticky-top">
                                    <tr>
                                        <th>Name</th>
                                        <th>Domain (From)</th>
                                        <th>Range (To)</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${relationshipsHtml}
                                </tbody>
                            </table>
                        </div>
                        
                        ${orphanHtml}
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-primary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('owlImportReportModal'));
    modal.show();
}

// =====================================================
// RDFS IMPORT HANDLERS
// =====================================================

// Import RDFS Button - shows dropdown for import source
document.getElementById('importRdfsBtn')?.addEventListener('click', function(e) {
    // Create dropdown menu if not exists
    let dropdown = document.getElementById('importRdfsDropdown');
    if (!dropdown) {
        dropdown = document.createElement('div');
        dropdown.id = 'importRdfsDropdown';
        dropdown.className = 'dropdown-menu show';
        dropdown.style.cssText = 'position: absolute; z-index: 1050;';
        dropdown.innerHTML = `
            <a class="dropdown-item" href="#" id="importRdfsLocal">
                <i class="bi bi-hdd"></i> Import from Local File
            </a>
            <a class="dropdown-item" href="#" id="importRdfsUC">
                <i class="bi bi-cloud-download"></i> Import from Unity Catalog
            </a>
        `;
        this.parentElement.appendChild(dropdown);
        
        // Position it below the button
        const rect = this.getBoundingClientRect();
        dropdown.style.top = (this.offsetTop + this.offsetHeight) + 'px';
        dropdown.style.left = this.offsetLeft + 'px';
        
        // Add event listeners
        document.getElementById('importRdfsLocal').addEventListener('click', function(e) {
            e.preventDefault();
            dropdown.remove();
            document.getElementById('rdfsFileInput').click();
        });
        
        document.getElementById('importRdfsUC').addEventListener('click', function(e) {
            e.preventDefault();
            dropdown.remove();
            importRdfsFromUC();
        });
        
        // Close dropdown when clicking outside
        setTimeout(() => {
            document.addEventListener('click', function closeDropdown(event) {
                if (!dropdown.contains(event.target) && event.target !== document.getElementById('importRdfsBtn')) {
                    dropdown.remove();
                    document.removeEventListener('click', closeDropdown);
                }
            });
        }, 100);
    } else {
        dropdown.remove();
    }
});

// Handle local RDFS file import
document.getElementById('rdfsFileInput')?.addEventListener('change', async function(e) {
    const file = e.target.files[0];
    if (!file) return;
    
    try {
        showNotification('Importing RDFS file...', 'info', 2000);
        const content = await file.text();
        await parseAndLoadRdfs(content, file.name);
    } catch (error) {
        showNotification('Error reading file: ' + error.message, 'error');
    }
    
    // Reset input
    this.value = '';
});

// Import RDFS from Unity Catalog
function importRdfsFromUC() {
    UCFileDialog.open({
        mode: 'load',
        title: 'Import RDFS from Unity Catalog',
        extensions: ['.ttl', '.rdf', '.rdfs', '.n3', '.nt'],
        onSelect: async function(fileInfo) {
            await parseAndLoadRdfs(fileInfo.content, fileInfo.filename);
        }
    });
}

// Parse and load RDFS content
async function parseAndLoadRdfs(content, filename) {
    try {
        const response = await fetch('/ontology/parse-rdfs', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ content: content }),
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success) {
            const onto = result.ontology;
            
            // Separate ObjectProperties (relationships) from DatatypeProperties (attributes)
            const objectProperties = onto.properties.filter(p => p.type === 'ObjectProperty');
            const datatypeProperties = onto.properties.filter(p => p.type === 'DatatypeProperty' || p.type === 'Property');
            
            // Build a map of class name -> attributes from DatatypeProperties
            const classAttributesMap = {};
            const orphanAttributes = []; // Attributes without a valid domain class
            
            datatypeProperties.forEach(prop => {
                if (prop.domain) {
                    if (!classAttributesMap[prop.domain]) {
                        classAttributesMap[prop.domain] = [];
                    }
                    classAttributesMap[prop.domain].push({
                        name: prop.name,
                        localName: prop.name,
                        label: prop.label || prop.name,
                        description: prop.description || '',
                        range: prop.range || 'string'
                    });
                } else {
                    orphanAttributes.push(prop);
                }
            });
            
            // Ontology name is always derived from the domain name (lowercase)
            let rdfsOntologyName = 'loadedvocabulary';
            try {
                const piResp2 = await fetch('/domain/info', { credentials: 'same-origin' });
                const piData2 = await piResp2.json();
                if (piData2.success && piData2.info && piData2.info.name) {
                    rdfsOntologyName = piData2.info.name.toLowerCase();
                }
            } catch (_) { /* keep fallback */ }
            OntologyState.config.name = rdfsOntologyName;
            OntologyState.config.base_uri = onto.info.namespace || onto.info.uri || OntologyState.baseUriDomain + '/LoadedVocabulary#';
            
            // Build classes with their attributes
            OntologyState.config.classes = onto.classes.map(cls => ({
                name: cls.name,
                label: cls.label || cls.name,
                description: cls.description || cls.comment || '',
                parent: cls.parent || '',
                emoji: cls.emoji || OntologyState.defaultClassEmoji,
                dataProperties: classAttributesMap[cls.name] || []
            }));
            
            // Only store ObjectProperties as relationships
            OntologyState.config.properties = objectProperties.map(prop => ({
                name: prop.name,
                label: prop.label || prop.name,
                description: prop.description || prop.comment || '',
                type: 'ObjectProperty',
                domain: prop.domain || '',
                range: prop.range || ''
            }));
            
            document.getElementById('ontologyName').value = OntologyState.config.name;
            document.getElementById('baseUri').value = OntologyState.config.base_uri;
            
            if (typeof updateClassesList === 'function') updateClassesList();
            if (typeof updatePropertiesList === 'function') updatePropertiesList();
            
            // Save loaded config to session immediately
            await window.saveConfigToSession();
            
            // Also update base_uri in domain settings (since it's managed there)
            try {
                await fetch('/domain/info', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ base_uri: OntologyState.config.base_uri }),
                    credentials: 'same-origin'
                });
            } catch (e) {
                console.log('Could not update domain base URI:', e);
            }
            
            await autoGenerateOwl();
            
            // Show detailed import report
            showRdfsImportReport(filename, onto, {
                classes: onto.classes,
                objectProperties: objectProperties,
                datatypeProperties: datatypeProperties,
                orphanAttributes: orphanAttributes,
                classAttributesMap: classAttributesMap
            });
        } else {
            showNotification('Error parsing RDFS: ' + result.message, 'error');
        }
    } catch (error) {
        showNotification('Error importing RDFS: ' + error.message, 'error');
    }
}

// Show RDFS import report modal
function showRdfsImportReport(filename, onto, details) {
    // Remove existing modal if any
    const existingModal = document.getElementById('rdfsImportReportModal');
    if (existingModal) existingModal.remove();
    
    // Count attributes assigned to classes
    let totalAttributesAssigned = 0;
    Object.values(details.classAttributesMap).forEach(attrs => {
        totalAttributesAssigned += attrs.length;
    });
    
    // Build class details HTML
    let classDetailsHtml = '';
    details.classes.forEach(cls => {
        const attrs = details.classAttributesMap[cls.name] || [];
        classDetailsHtml += `
            <tr>
                <td><strong>${cls.name}</strong></td>
                <td>${cls.label || cls.name}</td>
                <td>${cls.parent || '<span class="text-muted">-</span>'}</td>
                <td>${attrs.length > 0 ? attrs.map(a => `<span class="badge bg-secondary me-1">${a.name}</span>`).join('') : '<span class="text-muted">none</span>'}</td>
            </tr>
        `;
    });
    
    // Build relationships HTML
    let relationshipsHtml = '';
    if (details.objectProperties.length > 0) {
        details.objectProperties.forEach(prop => {
            relationshipsHtml += `
                <tr>
                    <td><strong>${prop.name}</strong></td>
                    <td>${prop.domain || '<span class="text-muted">?</span>'}</td>
                    <td>${prop.range || '<span class="text-muted">?</span>'}</td>
                </tr>
            `;
        });
    } else {
        relationshipsHtml = '<tr><td colspan="3" class="text-muted text-center">No relationships found</td></tr>';
    }
    
    // Build orphan attributes HTML (if any)
    let orphanHtml = '';
    if (details.orphanAttributes.length > 0) {
        orphanHtml = `
            <div class="alert alert-warning mt-3">
                <i class="bi bi-exclamation-triangle me-2"></i>
                <strong>${details.orphanAttributes.length} attribute(s) without domain class:</strong>
                <ul class="mb-0 mt-2">
                    ${details.orphanAttributes.map(a => `<li>${a.name} (range: ${a.range || 'unspecified'})</li>`).join('')}
                </ul>
            </div>
        `;
    }
    
    const modalHtml = `
        <div class="modal fade" id="rdfsImportReportModal" tabindex="-1">
            <div class="modal-dialog modal-lg modal-dialog-scrollable">
                <div class="modal-content">
                    <div class="modal-header bg-success text-white">
                        <h5 class="modal-title"><i class="bi bi-check-circle me-2"></i>RDFS Import Report</h5>
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="alert alert-info">
                            <i class="bi bi-file-earmark-text me-2"></i>
                            <strong>File:</strong> ${filename}
                            <br>
                            <strong>Ontology:</strong> ${onto.info.label || 'Unnamed'} 
                            <small class="text-muted">(${onto.info.namespace || onto.info.uri || 'no namespace'})</small>
                        </div>
                        
                        <!-- Summary -->
                        <div class="row text-center mb-4">
                            <div class="col-3">
                                <div class="card bg-primary text-white">
                                    <div class="card-body py-2">
                                        <h3 class="mb-0">${details.classes.length}</h3>
                                        <small>Entities</small>
                                    </div>
                                </div>
                            </div>
                            <div class="col-3">
                                <div class="card bg-info text-white">
                                    <div class="card-body py-2">
                                        <h3 class="mb-0">${details.objectProperties.length}</h3>
                                        <small>Relationships</small>
                                    </div>
                                </div>
                            </div>
                            <div class="col-3">
                                <div class="card bg-secondary text-white">
                                    <div class="card-body py-2">
                                        <h3 class="mb-0">${totalAttributesAssigned}</h3>
                                        <small>Attributes</small>
                                    </div>
                                </div>
                            </div>
                            <div class="col-3">
                                <div class="card ${details.orphanAttributes.length > 0 ? 'bg-warning' : 'bg-success'} text-white">
                                    <div class="card-body py-2">
                                        <h3 class="mb-0">${details.orphanAttributes.length}</h3>
                                        <small>Warnings</small>
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Entities Table -->
                        <h6><i class="bi bi-diagram-3 me-2"></i>Imported Entities</h6>
                        <div class="table-responsive" style="max-height: 250px; overflow-y: auto;">
                            <table class="table table-sm table-striped">
                                <thead class="table-light sticky-top">
                                    <tr>
                                        <th>Name</th>
                                        <th>Label</th>
                                        <th>Parent</th>
                                        <th>Attributes</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${classDetailsHtml || '<tr><td colspan="4" class="text-muted text-center">No entities found</td></tr>'}
                                </tbody>
                            </table>
                        </div>
                        
                        <!-- Relationships Table -->
                        <h6 class="mt-4"><i class="bi bi-arrow-left-right me-2"></i>Imported Relationships</h6>
                        <div class="table-responsive" style="max-height: 200px; overflow-y: auto;">
                            <table class="table table-sm table-striped">
                                <thead class="table-light sticky-top">
                                    <tr>
                                        <th>Name</th>
                                        <th>Domain (From)</th>
                                        <th>Range (To)</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${relationshipsHtml}
                                </tbody>
                            </table>
                        </div>
                        
                        ${orphanHtml}
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-primary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('rdfsImportReportModal'));
    modal.show();
}

// Export OWL Button - shows dropdown for export destination
document.getElementById('exportOwlBtn')?.addEventListener('click', function(e) {
    // Create dropdown menu if not exists
    let dropdown = document.getElementById('exportOwlDropdown');
    if (!dropdown) {
        dropdown = document.createElement('div');
        dropdown.id = 'exportOwlDropdown';
        dropdown.className = 'dropdown-menu show';
        dropdown.style.cssText = 'position: absolute; z-index: 1050;';
        dropdown.innerHTML = `
            <a class="dropdown-item" href="#" id="exportOwlLocal">
                <i class="bi bi-hdd"></i> Export to Local File
            </a>
            <a class="dropdown-item" href="#" id="exportOwlUC">
                <i class="bi bi-cloud-upload"></i> Export to Unity Catalog
            </a>
        `;
        this.parentElement.appendChild(dropdown);
        
        // Position it below the button
        dropdown.style.top = (this.offsetTop + this.offsetHeight) + 'px';
        dropdown.style.left = this.offsetLeft + 'px';
        
        // Add event listeners
        document.getElementById('exportOwlLocal').addEventListener('click', function(e) {
            e.preventDefault();
            dropdown.remove();
            exportOwlToLocal();
        });
        
        document.getElementById('exportOwlUC').addEventListener('click', function(e) {
            e.preventDefault();
            dropdown.remove();
            exportOwlToUC();
        });
        
        // Close dropdown when clicking outside
        setTimeout(() => {
            document.addEventListener('click', function closeDropdown(event) {
                if (!dropdown.contains(event.target) && event.target !== document.getElementById('exportOwlBtn')) {
                    dropdown.remove();
                    document.removeEventListener('click', closeDropdown);
                }
            });
        }, 100);
    } else {
        dropdown.remove();
    }
});

// Export to local file
function exportOwlToLocal() {
    const owlContent = document.getElementById('owlPreview').value;
    if (!owlContent) {
        showNotification('No OWL content to export. Please create an ontology first.', 'warning');
        return;
    }
    
    const filename = (OntologyState.config.name || 'ontology').replace(/\s+/g, '_').toLowerCase() + '.ttl';
    
    const blob = new Blob([owlContent], { type: 'text/turtle' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showNotification(`OWL exported: ${filename}`, 'success');
}

// Export to Unity Catalog
function exportOwlToUC() {
    const owlContent = document.getElementById('owlPreview').value;
    if (!owlContent) {
        showNotification('No OWL content to export. Please create an ontology first.', 'warning');
        return;
    }
    
    const defaultFilename = (OntologyState.config.name || 'ontology').replace(/\s+/g, '_').toLowerCase() + '.ttl';
    
    UCFileDialog.open({
        mode: 'save',
        title: 'Export OWL to Unity Catalog',
        extensions: ['.ttl'],
        defaultFilename: defaultFilename,
        onSave: async function(location) {
            try {
                const response = await fetch('/ontology/save-to-uc', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        path: location.path,
                        content: owlContent
                    }),
                    credentials: 'same-origin'
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification(`OWL exported to ${location.filename}`, 'success');
                } else {
                    showNotification('Error exporting: ' + result.message, 'error');
                }
            } catch (error) {
                showNotification('Error exporting OWL: ' + error.message, 'error');
            }
        }
    });
}
