// =====================================================
// INFORMATION SECTION - Mapping Status & Actions
// =====================================================

/**
 * Filter to only get ObjectProperties (relationships), not DatatypeProperties (attributes)
 */
function filterObjectProperties(allProperties) {
    if (!allProperties) return [];
    return allProperties.filter(prop => {
        if (prop.type) {
            return prop.type === 'ObjectProperty' || prop.type === 'owl:ObjectProperty';
        }
        if (prop.range) {
            const range = prop.range.toLowerCase();
            if (range.startsWith('xsd:') || range.includes('string') || range.includes('integer') || 
                range.includes('decimal') || range.includes('date') || range.includes('boolean') ||
                range.includes('float') || range.includes('double') || range.includes('time')) {
                return false;
            }
        }
        return true;
    });
}

// Update metadata prerequisite status
async function updateMetadataStatus() {
    const prereqAlert = document.getElementById('metadataPrerequisite');
    const checkIcon = document.getElementById('metadataCheckIcon');
    const checkStatus = document.getElementById('metadataCheckStatus');
    if (!prereqAlert) return;

    try {
        const response = await fetch('/domain/metadata', { credentials: 'same-origin' });
        const data = await response.json();

        if (data.success && data.has_metadata && data.metadata?.tables?.length) {
            prereqAlert.className = 'alert alert-success';
            checkIcon.innerHTML = '<i class="bi bi-check-circle-fill me-2"></i>';
            checkStatus.textContent = `${data.metadata.tables.length} table(s) loaded`;
        } else {
            prereqAlert.className = 'alert alert-warning';
            checkIcon.innerHTML = '<i class="bi bi-exclamation-triangle-fill me-2"></i>';
            checkStatus.innerHTML = '<strong>No data sources loaded</strong> — Data sources are required before creating mappings. Please load data sources first.';
        }
    } catch (e) {
        prereqAlert.className = 'alert alert-danger';
        checkIcon.innerHTML = '<i class="bi bi-x-circle-fill me-2"></i>';
        checkStatus.textContent = 'Error checking data sources status';
    }
}

// Update ontology prerequisite status
function updateTaxonomyStatus() {
    const placeholder = document.getElementById('mappingLoadingPlaceholder');
    const content = document.getElementById('mappingLoadedContent');
    if (placeholder) placeholder.style.display = 'none';
    if (content) content.classList.remove('d-none');

    updateMetadataStatus();

    const prereqAlert = document.getElementById('ontologyPrerequisite');
    const checkIcon = document.getElementById('ontologyCheckIcon');
    const checkStatus = document.getElementById('ontologyCheckStatus');
    
    if (MappingState.loadedOntology) {
        prereqAlert.className = 'alert alert-success';
        checkIcon.innerHTML = '<i class="bi bi-check-circle-fill me-2"></i>';
        const classCount = MappingState.loadedOntology.classes?.length || 0;
        const propCount = filterObjectProperties(MappingState.loadedOntology.properties).length;
        checkStatus.textContent = `${MappingState.loadedOntology.info?.label || 'Loaded'} (${classCount} classes, ${propCount} relationships)`;
        
        // Enable menu items
        updateMenuState(true);
    } else {
        prereqAlert.className = 'alert alert-warning';
        checkIcon.innerHTML = '<i class="bi bi-exclamation-triangle me-2"></i>';
        checkStatus.textContent = 'No ontology loaded - Please load an ontology first';
        
        // Disable menu items
        updateMenuState(false);
    }
}

// Update sidebar menu state based on ontology loading
function updateMenuState(enabled) {
    const menuItems = document.querySelectorAll('.sidebar-nav .nav-link[data-section="entities"], .sidebar-nav .nav-link[data-section="relationships"], .sidebar-nav .nav-link[data-section="r2rml"]');
    menuItems.forEach(item => {
        if (enabled) {
            item.classList.remove('disabled');
            item.style.pointerEvents = 'auto';
            item.style.opacity = '1';
        } else {
            item.classList.add('disabled');
            item.style.pointerEvents = 'none';
            item.style.opacity = '0.5';
        }
    });
}

var _mappingGauges = {};

function _drawMappingGauge(canvasId, score) {
    if (_mappingGauges[canvasId]) {
        _mappingGauges[canvasId].destroy();
        delete _mappingGauges[canvasId];
    }
    var canvas = document.getElementById(canvasId);
    if (!canvas) return;
    var ctx = canvas.getContext('2d');

    if (score == null) {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        canvas.parentElement.style.opacity = '0.3';
        return;
    }
    canvas.parentElement.style.opacity = '1';

    var val = Math.max(0, Math.min(100, Math.round(score)));
    var color = val === 100 ? '#198754' : val >= 80 ? '#ffc107' : '#dc3545';
    var remaining = 100 - val;

    _mappingGauges[canvasId] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            datasets: [{
                data: [val, remaining],
                backgroundColor: [color, '#e9ecef'],
                borderWidth: 0,
                circumference: 180,
                rotation: 270,
            }]
        },
        options: {
            responsive: false,
            cutout: '70%',
            plugins: { legend: { display: false }, tooltip: { enabled: false } },
            layout: { padding: 0 },
        },
        plugins: [{
            id: 'mappingGaugeLabel',
            afterDraw: function(chart) {
                var c = chart.ctx, w = chart.width, h = chart.height;
                var cx = w / 2, cy = h - 4;
                c.save();
                c.textAlign = 'center';
                c.textBaseline = 'bottom';
                c.font = 'bold 14px system-ui, sans-serif';
                c.fillStyle = color;
                c.fillText(val + '%', cx, cy);
                c.restore();
            }
        }],
    });
}

function updateMappingCompletionStatus() {
    const placeholder = document.getElementById('mappingLoadingPlaceholder');
    const content = document.getElementById('mappingLoadedContent');
    if (placeholder) placeholder.style.display = 'none';
    if (content) content.classList.remove('d-none');

    const entityCountEl = document.getElementById('entityMappingCount');
    const relationshipCountEl = document.getElementById('relationshipMappingCount');
    const attributeCountEl = document.getElementById('attributeMappingCount');
    const statusBadge = document.getElementById('mappingStatusBadge');
    const statusMessage = document.getElementById('mappingStatusMessage');
    
    if (!MappingState.loadedOntology) {
        entityCountEl.textContent = '0 / 0';
        relationshipCountEl.textContent = '0 / 0';
        if (attributeCountEl) attributeCountEl.textContent = '0 / 0';
        _drawMappingGauge('gaugeMapEntities', null);
        _drawMappingGauge('gaugeMapAttributes', null);
        _drawMappingGauge('gaugeMapRelationships', null);
        statusBadge.textContent = 'No Ontology';
        statusBadge.className = 'badge bg-secondary';
        statusMessage.innerHTML = '<i class="bi bi-exclamation-triangle text-warning"></i> Please load an ontology first from the Ontology page.';
        renderSummaryDetailLists({}, []);
        return;
    }
    
    // Filter out excluded classes and properties
    const allClasses = MappingState.loadedOntology.classes || [];
    const activeClasses = allClasses.filter(c => !c.excluded);
    const excludedClassUris = new Set(allClasses.filter(c => c.excluded).map(c => c.uri));
    const excludedClassNames = new Set(
        allClasses.filter(c => c.excluded).map(c => c.name || c.localName)
    );
    
    const allObjectProperties = filterObjectProperties(MappingState.loadedOntology.properties);
    const activeProperties = allObjectProperties.filter(p =>
        !p.excluded
        && !excludedClassNames.has(p.domain) && !excludedClassNames.has(p.range)
    );
    
    const totalClasses = activeClasses.length;
    const totalProperties = activeProperties.length;
    
    // Count entity mappings that match existing non-excluded ontology classes
    let mappedClasses = 0;
    const mappingByClass = {};
    if (MappingState.config.entities && activeClasses.length > 0) {
        const classUris = new Set(activeClasses.map(c => c.uri));
        MappingState.config.entities.forEach(m => {
            const uri = m.ontology_class || m.class_uri;
            if (classUris.has(uri)) {
                mappedClasses++;
                mappingByClass[uri] = m;
            }
        });
    }
    
    // Count relationship mappings that match existing non-excluded ObjectProperties
    let mappedProperties = 0;
    if (MappingState.config.relationships && activeProperties.length > 0) {
        const propUris = new Set(activeProperties.map(p => p.uri));
        mappedProperties = MappingState.config.relationships.filter(m => 
            propUris.has(m.property)
        ).length;
    }
    
    // Count attribute mappings across all mapped non-excluded entities
    let totalAttributes = 0;
    let mappedAttributes = 0;
    for (const cls of activeClasses) {
        const dataProps = cls.dataProperties || [];
        if (dataProps.length === 0) continue;
        totalAttributes += dataProps.length;
        const em = mappingByClass[cls.uri];
        if (em) {
            const attrMap = em.attribute_mappings || {};
            for (const dp of dataProps) {
                const attrName = dp.name || dp.localName || '';
                if (attrName && attrMap[attrName]) {
                    mappedAttributes++;
                }
            }
        }
    }
    
    // Update counts
    entityCountEl.textContent = `${mappedClasses} / ${totalClasses}`;
    relationshipCountEl.textContent = `${mappedProperties} / ${totalProperties}`;
    if (attributeCountEl) attributeCountEl.textContent = `${mappedAttributes} / ${totalAttributes}`;

    // Draw gauges
    const entityPct = totalClasses > 0 ? (mappedClasses / totalClasses) * 100 : null;
    const attrPct = totalAttributes > 0 ? (mappedAttributes / totalAttributes) * 100 : null;
    const relPct = totalProperties > 0 ? (mappedProperties / totalProperties) * 100 : null;
    _drawMappingGauge('gaugeMapEntities', entityPct);
    _drawMappingGauge('gaugeMapAttributes', attrPct);
    _drawMappingGauge('gaugeMapRelationships', relPct);

    // Populate entity/relationship detail lists
    renderSummaryDetailLists(mappingByClass, allObjectProperties, excludedClassNames);
    
    // Determine status
    const totalItems = totalClasses + totalProperties + totalAttributes;
    const mappedItems = mappedClasses + mappedProperties + mappedAttributes;
    const entitiesComplete = mappedClasses >= totalClasses;
    const propsComplete = mappedProperties >= totalProperties;
    const attrsComplete = mappedAttributes >= totalAttributes;
    const isComplete = entitiesComplete && propsComplete && attrsComplete && totalItems > 0;
    const isValidated = MappingState.mappingValidated && isComplete;
    
    if (isValidated) {
        statusBadge.textContent = 'Valid';
        statusBadge.className = 'badge bg-success';
        statusMessage.innerHTML = '<i class="bi bi-check-circle text-success"></i> All mappings complete. R2RML is ready.';
    } else if (isComplete) {
        statusBadge.textContent = 'Complete';
        statusBadge.className = 'badge bg-success';
        statusMessage.innerHTML = '<i class="bi bi-check-circle text-success"></i> All entities, relationships, and attributes are mapped.';
    } else if (mappedItems > 0) {
        statusBadge.textContent = 'In Progress';
        statusBadge.className = 'badge bg-info';
        
        const missing = [];
        if (mappedClasses < totalClasses) missing.push(`${totalClasses - mappedClasses} entities`);
        if (mappedProperties < totalProperties) missing.push(`${totalProperties - mappedProperties} relationships`);
        if (mappedAttributes < totalAttributes) missing.push(`${totalAttributes - mappedAttributes} attributes`);
        statusMessage.innerHTML = `<i class="bi bi-hourglass-split text-info"></i> Mapping in progress. Still need: ${missing.join(', ')}.`;
    } else {
        statusBadge.textContent = 'Not Started';
        statusBadge.className = 'badge bg-secondary';
        statusMessage.innerHTML = '<i class="bi bi-info-circle"></i> No mappings yet. Go to Entities to start mapping classes to tables.';
    }
}

/**
 * Render entity and relationship detail lists in the summary grid.
 */
function renderSummaryDetailLists(mappingByClass, objectProperties, excludedClassNames) {
    const entityListEl = document.getElementById('entityDetailList');
    const relListEl = document.getElementById('relationshipDetailList');
    if (!entityListEl || !relListEl) return;

    excludedClassNames = excludedClassNames || new Set();
    const classes = MappingState.loadedOntology?.classes || [];

    // --- Entity list ---
    if (classes.length === 0) {
        entityListEl.innerHTML = '<div class="text-muted small fst-italic">No entities defined</div>';
    } else {
        const rows = classes.map((cls, idx) => {
            const name = cls.label || cls.name || cls.localName || cls.uri;
            const isExcluded = !!cls.excluded;
            const em = mappingByClass[cls.uri];
            const dataProps = cls.dataProperties || [];
            let attrMapped = 0;
            if (em) {
                const attrMap = em.attribute_mappings || {};
                for (const dp of dataProps) {
                    if (attrMap[dp.name || dp.localName || '']) attrMapped++;
                }
            }
            const isMapped = !!em;

            const cbId = `excludeEntity_${idx}`;
            const checked = isExcluded ? '' : 'checked';
            const checkbox = `<input class="form-check-input exclude-cb" type="checkbox" id="${cbId}" ${checked} data-uri="${cls.uri}" data-type="entity" title="${isExcluded ? 'Include in mapping' : 'Exclude from mapping'}">`;

            const badge = isExcluded
                ? '<span class="badge bg-light text-secondary border">excluded</span>'
                : isMapped
                    ? `<span class="badge bg-success-subtle text-success">${attrMapped}/${dataProps.length} attr</span>`
                    : '<span class="badge bg-secondary-subtle text-secondary">not mapped</span>';

            const rowClass = isExcluded ? 'summary-detail-row excluded' : 'summary-detail-row';
            return `<div class="${rowClass}">
                ${checkbox}
                <span class="summary-detail-name" title="${cls.uri}">${isExcluded ? `<s>${name}</s>` : name}</span>
                ${badge}
            </div>`;
        });
        entityListEl.innerHTML = rows.join('');
        _bindExcludeCheckboxes(entityListEl);
    }

    // --- Relationship list ---
    if (objectProperties.length === 0) {
        relListEl.innerHTML = '<div class="text-muted small fst-italic">No relationships defined</div>';
    } else {
        const mappedPropUris = new Set(
            (MappingState.config.relationships || []).map(m => m.property)
        );
        const rows = objectProperties.map((prop, idx) => {
            const name = prop.label || prop.name || prop.localName || prop.uri;
            const isParentExcluded = excludedClassNames.has(prop.domain) || excludedClassNames.has(prop.range);
            const isExcluded = !!prop.excluded || isParentExcluded;

            const isMapped = mappedPropUris.has(prop.uri);
            const badge = isExcluded
                ? '<span class="badge bg-light text-secondary border">excluded</span>'
                : isMapped
                    ? '<span class="badge bg-success-subtle text-success">mapped</span>'
                    : '<span class="badge bg-secondary-subtle text-secondary">not mapped</span>';

            const cbId = `excludeRel_${idx}`;
            const checked = isExcluded ? '' : 'checked';
            const disabled = isParentExcluded ? 'disabled' : '';
            const cbTitle = isParentExcluded
                ? 'Excluded because a connected entity is excluded'
                : isExcluded ? 'Include in mapping' : 'Exclude from mapping';
            const checkbox = `<input class="form-check-input exclude-cb" type="checkbox" id="${cbId}" ${checked} ${disabled} data-uri="${prop.uri}" data-type="relationship" title="${cbTitle}">`;

            const rowClass = isExcluded ? 'summary-detail-row excluded' : 'summary-detail-row';
            return `<div class="${rowClass}">
                ${checkbox}
                <span class="summary-detail-name" title="${prop.uri}">${isExcluded ? `<s>${name}</s>` : name}</span>
                ${badge}
            </div>`;
        });
        relListEl.innerHTML = rows.join('');
        _bindExcludeCheckboxes(relListEl);
    }
}

/**
 * Bind change events on exclude checkboxes inside a container.
 */
function _bindExcludeCheckboxes(container) {
    container.querySelectorAll('.exclude-cb').forEach(cb => {
        cb.addEventListener('change', function() {
            const uri = this.dataset.uri;
            const itemType = this.dataset.type;
            const excluded = !this.checked;
            if (typeof window.toggleEntityExclusion === 'function') {
                window.toggleEntityExclusion(uri, excluded, itemType);
            }
        });
    });
}

// Show validation result modal (kept for potential future use)
function showValidationResult(issues, warnings) {
    let modalHtml = `
        <div class="modal fade" id="validationResultModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">
                            <i class="bi bi-x-circle text-danger"></i> Validation Incomplete
                        </h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="alert alert-danger">
                            <h6><i class="bi bi-exclamation-triangle"></i> Issues Found</h6>
                            <ul class="mb-0">
                                ${issues.map(i => `<li>${i}</li>`).join('')}
                            </ul>
                        </div>
                        <p class="text-muted small mb-0">
                            <i class="bi bi-lightbulb"></i> Complete all mappings before validating.
                        </p>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    const existingModal = document.getElementById('validationResultModal');
    if (existingModal) existingModal.remove();
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('validationResultModal'));
    modal.show();
}

// Show validation success modal
function showValidationSuccessModal(stats) {
    const modalHtml = `
        <div class="modal fade" id="validationSuccessModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header bg-success text-white">
                        <h5 class="modal-title">
                            <i class="bi bi-patch-check-fill"></i> Mapping Validated!
                        </h5>
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="text-center mb-3">
                            <i class="bi bi-check-circle-fill text-success" style="font-size: 4rem;"></i>
                        </div>
                        <div class="alert alert-success">
                            <h6><i class="bi bi-check-circle"></i> Success</h6>
                            <p class="mb-0">All mappings validated. R2RML has been generated.</p>
                        </div>
                        <table class="table table-sm">
                            <tr><td><strong>Entity Mappings:</strong></td><td>${stats?.entities || 0}</td></tr>
                            <tr><td><strong>Relationship Mappings:</strong></td><td>${stats?.relationships || 0}</td></tr>
                        </table>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                        <button type="button" class="btn btn-outline-secondary" onclick="SidebarNav.switchTo('r2rml'); bootstrap.Modal.getInstance(document.getElementById('validationSuccessModal')).hide();">
                            <i class="bi bi-file-earmark-code"></i> View R2RML
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    const existingModal = document.getElementById('validationSuccessModal');
    if (existingModal) existingModal.remove();
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modal = new bootstrap.Modal(document.getElementById('validationSuccessModal'));
    modal.show();
}

// Reset all mappings
async function confirmResetMappings() {
    const confirmed = await showConfirmDialog({
        title: 'Reset All Mappings',
        message: 'This will delete <strong>all entity and relationship mappings</strong>.<br><br>Are you sure you want to continue?',
        confirmText: 'Reset All',
        confirmClass: 'btn-danger',
        icon: 'exclamation-triangle'
    });
    if (confirmed) {
        resetAllMappings();
    }
}

async function resetAllMappings() {
    const btn = document.getElementById('resetMappingsBtn');
    const originalHTML = btn.innerHTML;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
    btn.disabled = true;
    
    try {
        MappingState.config = {
            entities: [],
            relationships: []
        };
        
        await fetch('/mapping/save', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(MappingState.config),
            credentials: 'same-origin'
        });
        
        await fetch('/mapping/clear', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        
        const r2rmlPreview = document.getElementById('r2rmlPreview');
        if (r2rmlPreview) r2rmlPreview.value = '';
        
        MappingState.r2rmlAutoGenerated = false;
        MappingState.mappingValidated = false;
        
        updateMappingCompletionStatus();
        
        showNotification('✅ All mappings reset.', 'success');
        
    } catch (error) {
        console.error('Error resetting:', error);
        showNotification('Error: ' + error.message, 'error');
    } finally {
        btn.innerHTML = originalHTML;
        btn.disabled = false;
    }
}

// =====================================================
// IMPORT R2RML
// =====================================================

// Import R2RML Button - shows dropdown
function setupImportExportButtons() {
    const importBtn = document.getElementById('importR2RMLBtn');
    if (importBtn) {
        importBtn.addEventListener('click', function(e) {
            let dropdown = document.getElementById('importR2RMLDropdown');
            if (!dropdown) {
                dropdown = document.createElement('div');
                dropdown.id = 'importR2RMLDropdown';
                dropdown.className = 'dropdown-menu show';
                dropdown.style.cssText = 'position: absolute; z-index: 1050;';
                dropdown.innerHTML = `
                    <a class="dropdown-item" href="#" id="importR2RMLLocal">
                        <i class="bi bi-hdd"></i> Import from Local File
                    </a>
                    <a class="dropdown-item" href="#" id="importR2RMLUC">
                        <i class="bi bi-cloud-download"></i> Import from Unity Catalog
                    </a>
                `;
                this.parentElement.appendChild(dropdown);
                
                // Position it below the button
                dropdown.style.top = (this.offsetTop + this.offsetHeight) + 'px';
                dropdown.style.left = this.offsetLeft + 'px';
                
                // Add event listeners
                document.getElementById('importR2RMLLocal').addEventListener('click', function(e) {
                    e.preventDefault();
                    dropdown.remove();
                    document.getElementById('r2rmlFileInput').click();
                });
                
                document.getElementById('importR2RMLUC').addEventListener('click', function(e) {
                    e.preventDefault();
                    dropdown.remove();
                    importR2RMLFromUC();
                });
                
                // Close dropdown when clicking outside
                setTimeout(() => {
                    document.addEventListener('click', function closeDropdown(event) {
                        if (!dropdown.contains(event.target) && event.target !== importBtn) {
                            dropdown.remove();
                            document.removeEventListener('click', closeDropdown);
                        }
                    });
                }, 100);
            } else {
                dropdown.remove();
            }
        });
    }
    
    // Export R2RML Button - shows dropdown
    const exportBtn = document.getElementById('exportR2RMLBtn');
    if (exportBtn) {
        exportBtn.addEventListener('click', function(e) {
            let dropdown = document.getElementById('exportR2RMLDropdown');
            if (!dropdown) {
                dropdown = document.createElement('div');
                dropdown.id = 'exportR2RMLDropdown';
                dropdown.className = 'dropdown-menu show';
                dropdown.style.cssText = 'position: absolute; z-index: 1050;';
                dropdown.innerHTML = `
                    <a class="dropdown-item" href="#" id="exportR2RMLLocal">
                        <i class="bi bi-hdd"></i> Export to Local File
                    </a>
                    <a class="dropdown-item" href="#" id="exportR2RMLUC">
                        <i class="bi bi-cloud-upload"></i> Export to Unity Catalog
                    </a>
                `;
                this.parentElement.appendChild(dropdown);
                
                // Position it below the button
                dropdown.style.top = (this.offsetTop + this.offsetHeight) + 'px';
                dropdown.style.left = this.offsetLeft + 'px';
                
                // Add event listeners
                document.getElementById('exportR2RMLLocal').addEventListener('click', function(e) {
                    e.preventDefault();
                    dropdown.remove();
                    exportR2RMLToLocal();
                });
                
                document.getElementById('exportR2RMLUC').addEventListener('click', function(e) {
                    e.preventDefault();
                    dropdown.remove();
                    exportR2RMLToUC();
                });
                
                // Close dropdown when clicking outside
                setTimeout(() => {
                    document.addEventListener('click', function closeDropdown(event) {
                        if (!dropdown.contains(event.target) && event.target !== exportBtn) {
                            dropdown.remove();
                            document.removeEventListener('click', closeDropdown);
                        }
                    });
                }, 100);
            } else {
                dropdown.remove();
            }
        });
    }
    
    // Handle local file import
    const fileInput = document.getElementById('r2rmlFileInput');
    if (fileInput) {
        fileInput.addEventListener('change', async function(e) {
            const file = e.target.files[0];
            if (!file) return;
            
            try {
                showNotification('Importing R2RML file...', 'info', 2000);
                const content = await file.text();
                await parseAndLoadR2RML(content, file.name);
            } catch (error) {
                showNotification('Error reading file: ' + error.message, 'error');
            }
            
            this.value = '';
        });
    }
}

// Import from Unity Catalog
function importR2RMLFromUC() {
    UCFileDialog.open({
        mode: 'load',
        title: 'Import R2RML from Unity Catalog',
        extensions: ['.ttl', '.rdf'],
        onSelect: async function(fileInfo) {
            await parseAndLoadR2RML(fileInfo.content, fileInfo.filename);
        }
    });
}

// Parse and load R2RML content
async function parseAndLoadR2RML(content, filename) {
    try {
        const response = await fetch('/mapping/parse-r2rml', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ content: content }),
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success) {
            // Update mapping state with parsed mappings
            if (result.entities) {
                MappingState.config.entities = result.entities;
            }
            if (result.relationships) {
                MappingState.config.relationships = result.relationships;
            }
            
            // Update R2RML preview
            const r2rmlPreview = document.getElementById('r2rmlPreview');
            if (r2rmlPreview) r2rmlPreview.value = content;
            
            // Save to session
            await fetch('/mapping/save', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(MappingState.config),
                credentials: 'same-origin'
            });
            
            if (MappingState.config.entities.length > 0) {
                MappingState.mappingValidated = true;
                MappingState.r2rmlAutoGenerated = true;
            }
            
            // Refresh UI
            updateMappingCompletionStatus();
            
            // Update navbar status indicator
            if (typeof window.refreshOntologyStatus === 'function') {
                window.refreshOntologyStatus();
            }
            
            showNotification(`R2RML imported: ${filename} (${result.entities?.length || 0} entities, ${result.relationships?.length || 0} relationships)`, 'success');
        } else {
            showNotification('Error parsing R2RML: ' + result.message, 'error');
        }
    } catch (error) {
        showNotification('Error importing R2RML: ' + error.message, 'error');
    }
}

// =====================================================
// EXPORT R2RML
// =====================================================

// Get R2RML content - generate if needed
async function getR2RMLContent() {
    // First check if we have content in the preview
    let r2rmlContent = document.getElementById('r2rmlPreview')?.value;
    
    if (r2rmlContent && r2rmlContent.trim()) {
        return r2rmlContent;
    }
    
    // Check if we have mappings to generate R2RML from
    if (!MappingState.config.entities || MappingState.config.entities.length === 0) {
        return null;
    }
    
    // Generate R2RML from backend
    try {
        showNotification('Generating R2RML...', 'info', 2000);
        const response = await fetch('/mapping/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success && result.r2rml) {
            // Update the preview textarea too
            const preview = document.getElementById('r2rmlPreview');
            if (preview) preview.value = result.r2rml;
            return result.r2rml;
        }
    } catch (error) {
        console.error('Error generating R2RML:', error);
    }
    
    return null;
}

// Export to local file
async function exportR2RMLToLocal() {
    const r2rmlContent = await getR2RMLContent();
    
    if (!r2rmlContent) {
        showNotification('No R2RML content to export. Configure entity mappings first.', 'warning');
        return;
    }
    
    const name = MappingState.loadedOntology?.info?.label || 'mapping';
    const filename = name.replace(/\s+/g, '_').toLowerCase() + '_r2rml.ttl';
    
    const blob = new Blob([r2rmlContent], { type: 'text/turtle' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showNotification(`R2RML exported: ${filename}`, 'success');
}

// Export to Unity Catalog
async function exportR2RMLToUC() {
    const r2rmlContent = await getR2RMLContent();
    
    if (!r2rmlContent) {
        showNotification('No R2RML content to export. Configure entity mappings first.', 'warning');
        return;
    }
    
    const name = MappingState.loadedOntology?.info?.label || 'mapping';
    const defaultFilename = name.replace(/\s+/g, '_').toLowerCase() + '_r2rml.ttl';
    
    UCFileDialog.open({
        mode: 'save',
        title: 'Export R2RML to Unity Catalog',
        extensions: ['.ttl'],
        defaultFilename: defaultFilename,
        onSave: async function(location) {
            try {
                const response = await fetch('/write-volume-file', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        file_path: location.path,
                        content: r2rmlContent
                    }),
                    credentials: 'same-origin'
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showNotification(`R2RML exported to ${location.filename}`, 'success');
                } else {
                    showNotification('Error exporting: ' + result.message, 'error');
                }
            } catch (error) {
                showNotification('Error exporting R2RML: ' + error.message, 'error');
            }
        }
    });
}

// Initialize Import/Export buttons and wire reset button
setupImportExportButtons();
var _resetBtn = document.getElementById('resetMappingsBtn');
if (_resetBtn) {
    _resetBtn.addEventListener('click', function() { confirmResetMappings(); });
}

// Expose functions to global scope for mapping-core.js
window.updateTaxonomyStatus = updateTaxonomyStatus;
window.updateMappingCompletionStatus = updateMappingCompletionStatus;
window.confirmResetMappings = confirmResetMappings;
window.resetAllMappings = resetAllMappings;
