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
    const prereqAlerts = document.querySelectorAll('#metadataPrerequisite');
    const checkIcons = document.querySelectorAll('#metadataCheckIcon');
    const checkStatuses = document.querySelectorAll('#metadataCheckStatus');
    if (!prereqAlerts.length) return;

    try {
        const response = await fetch('/domain/metadata', { credentials: 'same-origin' });
        const data = await response.json();

        if (data.success && data.has_metadata && data.metadata?.tables?.length) {
            prereqAlerts.forEach(el => el.className = 'alert alert-success');
            checkIcons.forEach(el => el.innerHTML = '<i class="bi bi-check-circle-fill me-2"></i>');
            checkStatuses.forEach(el => el.textContent = `${data.metadata.tables.length} table(s) loaded`);
        } else {
            prereqAlerts.forEach(el => el.className = 'alert alert-warning');
            checkIcons.forEach(el => el.innerHTML = '<i class="bi bi-exclamation-triangle-fill me-2"></i>');
            checkStatuses.forEach(el => el.innerHTML = '<strong>No data assets loaded</strong> — Data assets are required before creating mappings. Please load data assets first.');
        }
    } catch (e) {
        prereqAlerts.forEach(el => el.className = 'alert alert-danger');
        checkIcons.forEach(el => el.innerHTML = '<i class="bi bi-x-circle-fill me-2"></i>');
        checkStatuses.forEach(el => el.textContent = 'Error checking data assets status');
    }
}

// Update ontology prerequisite status
function updateTaxonomyStatus() {
    const placeholder = document.getElementById('mappingLoadingPlaceholder');
    const content = document.getElementById('mappingLoadedContent');
    if (placeholder) placeholder.style.display = 'none';
    if (content) content.classList.remove('d-none');

    updateMetadataStatus();

    const prereqAlerts = document.querySelectorAll('#ontologyPrerequisite');
    const checkIcons = document.querySelectorAll('#ontologyCheckIcon');
    const checkStatuses = document.querySelectorAll('#ontologyCheckStatus');
    
    if (MappingState.loadedOntology) {
        prereqAlerts.forEach(el => el.className = 'alert alert-success');
        checkIcons.forEach(el => el.innerHTML = '<i class="bi bi-check-circle-fill me-2"></i>');
        const classCount = MappingState.loadedOntology.classes?.length || 0;
        const propCount = filterObjectProperties(MappingState.loadedOntology.properties).length;
        checkStatuses.forEach(el => el.textContent = `${MappingState.loadedOntology.info?.label || 'Loaded'} (${classCount} classes, ${propCount} relationships)`);
        
        // Enable menu items
        updateMenuState(true);
    } else {
        prereqAlerts.forEach(el => el.className = 'alert alert-warning');
        checkIcons.forEach(el => el.innerHTML = '<i class="bi bi-exclamation-triangle me-2"></i>');
        checkStatuses.forEach(el => el.textContent = 'No ontology loaded - Please load an ontology first');
        
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
    var canvases = document.querySelectorAll('#' + canvasId);
    if (!canvases.length) return;

    canvases.forEach(function(canvas, idx) {
        var key = canvasId + '_' + idx;
        if (_mappingGauges[key]) {
            _mappingGauges[key].destroy();
            delete _mappingGauges[key];
        }
        var ctx = canvas.getContext('2d');

        if (score == null) {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            if (canvas.parentElement) canvas.parentElement.style.opacity = '0.3';
            return;
        }
        if (canvas.parentElement) canvas.parentElement.style.opacity = '1';

        var val = Math.max(0, Math.min(100, Math.round(score)));
        var color = val === 100 ? '#198754' : val >= 80 ? '#ffc107' : '#dc3545';
        var remaining = 100 - val;

        _mappingGauges[key] = new Chart(ctx, {
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
    });
}

function updateMappingCompletionStatus() {
    const placeholder = document.getElementById('mappingLoadingPlaceholder');
    const content = document.getElementById('mappingLoadedContent');
    if (placeholder) placeholder.style.display = 'none';
    if (content) content.classList.remove('d-none');

    const entityCountEls = document.querySelectorAll('#entityMappingCount');
    const relationshipCountEls = document.querySelectorAll('#relationshipMappingCount');
    const attributeCountEls = document.querySelectorAll('#attributeMappingCount');
    const statusBadges = document.querySelectorAll('#mappingStatusBadge');
    const statusMessages = document.querySelectorAll('#mappingStatusMessage');
    
    if (!MappingState.loadedOntology) {
        entityCountEls.forEach(el => el.textContent = '0 / 0');
        relationshipCountEls.forEach(el => el.textContent = '0 / 0');
        attributeCountEls.forEach(el => el.textContent = '0 / 0');
        _drawMappingGauge('gaugeMapEntities', null);
        _drawMappingGauge('gaugeMapAttributes', null);
        _drawMappingGauge('gaugeMapRelationships', null);
        statusBadges.forEach(el => {
            el.textContent = 'No Ontology';
            el.className = 'badge bg-secondary';
        });
        statusMessages.forEach(el => {
            el.innerHTML = '<i class="bi bi-exclamation-triangle text-warning"></i> Please load an ontology first from the Ontology page.';
        });
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
    
    // Build lookup maps (same pattern as Auto-Map page)
    // Only count entities / relationships that have a real SQL query — stubs
    // (excluded_attributes only, no sql_query) must not inflate the mapped count.
    const entityMappings = MappingState.config.entities || [];
    const relationshipMappings = MappingState.config.relationships || [];

    const assignedEntityUris = new Set(
        entityMappings.filter(m => m.sql_query).map(m => m.ontology_class || m.class_uri)
    );
    const assignedRelUris = new Set(
        relationshipMappings.filter(m => m.sql_query).map(m => m.property)
    );

    // Build mappingByClass from ALL config entries (needed for attribute lookup)
    const mappingByClass = {};
    entityMappings.forEach(m => {
        const uri = m.ontology_class || m.class_uri;
        if (uri) mappingByClass[uri] = m;
    });

    // Count from the ONTOLOGY side (same direction as Auto-Map) so any URI
    // mismatch or stub entry can't inflate the denominator.
    const mappedClasses = activeClasses.filter(c => assignedEntityUris.has(c.uri)).length;
    const mappedProperties = activeProperties.filter(p => assignedRelUris.has(p.uri)).length;
    
    // Count attribute mappings across all mapped non-excluded entities.
    // Excluded attributes (excluded_attributes in the mapping) are not counted.
    let totalAttributes = 0;
    let mappedAttributes = 0;
    let excludedAttrCount = 0;
    for (const cls of activeClasses) {
        const dataProps = cls.dataProperties || [];
        if (dataProps.length === 0) continue;
        const em = mappingByClass[cls.uri];
        const exclAttrs = new Set((em && em.excluded_attributes) || []);
        excludedAttrCount += exclAttrs.size;
        const attrMap = (em && em.attribute_mappings) || {};
        for (const dp of dataProps) {
            const attrName = dp.name || dp.localName || '';
            if (!attrName) continue;
            if (exclAttrs.has(attrName)) continue;   // excluded — skip
            totalAttributes++;
            if (attrMap[attrName]) mappedAttributes++;
        }
    }

    // Reusable helper: render "· N excl." badge
    const _excl = (n) => n > 0
        ? ` <span class="text-warning-emphasis" title="${n} excluded" style="font-size:0.72rem;">· ${n} excl.</span>`
        : '';

    // Count excluded entities / relationships for badges
    const excludedEntityCount = allClasses.filter(c => c.excluded).length;
    const excludedRelCount = allObjectProperties.filter(p =>
        p.excluded || excludedClassNames.has(p.domain) || excludedClassNames.has(p.range)
    ).length;

    // Update counts
    const entityCountStr = `${mappedClasses} / ${totalClasses}${_excl(excludedEntityCount)}`;
    const relCountStr = `${mappedProperties} / ${totalProperties}${_excl(excludedRelCount)}`;
    const attrCountStr = `${mappedAttributes} / ${totalAttributes}${_excl(excludedAttrCount)}`;
    entityCountEls.forEach(el => { el.innerHTML = entityCountStr; });
    relationshipCountEls.forEach(el => { el.innerHTML = relCountStr; });
    attributeCountEls.forEach(el => { el.innerHTML = attrCountStr; });

    // Draw gauges
    const entityPct = totalClasses > 0 ? (mappedClasses / totalClasses) * 100 : null;
    const attrPct = totalAttributes > 0 ? (mappedAttributes / totalAttributes) * 100 : null;
    const relPct = totalProperties > 0 ? (mappedProperties / totalProperties) * 100 : null;
    _drawMappingGauge('gaugeMapEntities', entityPct);
    _drawMappingGauge('gaugeMapAttributes', attrPct);
    _drawMappingGauge('gaugeMapRelationships', relPct);

    // Populate entity/relationship detail lists
    renderSummaryDetailLists(mappingByClass, allObjectProperties, excludedClassNames);
    
    // Determine status (only included attrs count towards completeness)
    const totalItems = totalClasses + totalProperties + totalAttributes;
    const mappedItems = mappedClasses + mappedProperties + mappedAttributes;
    const entitiesComplete = mappedClasses >= totalClasses;
    const propsComplete = mappedProperties >= totalProperties;
    const attrsComplete = mappedAttributes >= totalAttributes;
    const isComplete = entitiesComplete && propsComplete && attrsComplete && totalItems > 0;
    const isValidated = MappingState.mappingValidated && isComplete;
    
    if (isValidated) {
        statusBadges.forEach(el => { el.textContent = 'Valid'; el.className = 'badge bg-success'; });
        statusMessages.forEach(el => { el.innerHTML = '<i class="bi bi-check-circle text-success"></i> All mappings complete. R2RML is ready.'; });
    } else if (isComplete) {
        statusBadges.forEach(el => { el.textContent = 'Complete'; el.className = 'badge bg-success'; });
        statusMessages.forEach(el => { el.innerHTML = '<i class="bi bi-check-circle text-success"></i> All entities, relationships, and attributes are mapped.'; });
    } else if (mappedItems > 0) {
        statusBadges.forEach(el => { el.textContent = 'In Progress'; el.className = 'badge bg-info'; });
        
        const missing = [];
        if (mappedClasses < totalClasses) missing.push(`${totalClasses - mappedClasses} entities`);
        if (mappedProperties < totalProperties) missing.push(`${totalProperties - mappedProperties} relationships`);
        if (mappedAttributes < totalAttributes) missing.push(`${totalAttributes - mappedAttributes} attributes`);
        statusMessages.forEach(el => { el.innerHTML = `<i class="bi bi-hourglass-split text-info"></i> Mapping in progress. Still need: ${missing.join(', ')}.`; });
    } else {
        statusBadges.forEach(el => { el.textContent = 'Not Started'; el.className = 'badge bg-secondary'; });
        statusMessages.forEach(el => { el.innerHTML = '<i class="bi bi-info-circle"></i> No mappings yet. Go to Entities to start mapping classes to tables.'; });
    }
}

/**
 * Render entity and relationship detail lists in the summary grid.
 */
function renderSummaryDetailLists(mappingByClass, objectProperties, excludedClassNames) {
    const entityListEls = document.querySelectorAll('#entityDetailList');
    const relListEls = document.querySelectorAll('#relationshipDetailList');
    if (!entityListEls.length || !relListEls.length) return;

    excludedClassNames = excludedClassNames || new Set();
    const classes = MappingState.loadedOntology?.classes || [];

    // --- Entity list ---
    if (classes.length === 0) {
        entityListEls.forEach(el => { el.innerHTML = '<div class="text-muted small fst-italic">No entities defined</div>'; });
    } else {
        const rows = classes.map((cls, idx) => {
            const name = cls.label || cls.name || cls.localName || cls.uri;
            const isExcluded = !!cls.excluded;
            const em = mappingByClass[cls.uri];
            const dataProps = cls.dataProperties || [];
            const exclAttrs = new Set((em && em.excluded_attributes) || []);
            // Only count included attributes (same logic as gauges)
            const includedProps = dataProps.filter(dp => {
                const n = dp.name || dp.localName || '';
                return n && !exclAttrs.has(n);
            });
            let attrMapped = 0;
            if (em) {
                const attrMap = em.attribute_mappings || {};
                for (const dp of includedProps) {
                    if (attrMap[dp.name || dp.localName || '']) attrMapped++;
                }
            }
            const isMapped = !!(em && em.sql_query);
            const exclCount = exclAttrs.size;

            const cbId = `excludeEntity_${idx}`;
            const checked = isExcluded ? '' : 'checked';
            const checkbox = `<input class="form-check-input exclude-cb" type="checkbox" id="${cbId}" ${checked} data-uri="${cls.uri}" data-type="entity" title="${isExcluded ? 'Include in mapping' : 'Exclude from mapping'}">`;

            const exclBit = exclCount > 0 ? ` <span class="text-warning-emphasis" style="font-size:0.7rem;" title="${exclCount} attr(s) excluded">· ${exclCount} excl.</span>` : '';
            const badge = isExcluded
                ? '<span class="badge bg-light text-secondary border">excluded</span>'
                : isMapped
                    ? `<span class="badge bg-success-subtle text-success">${attrMapped}/${includedProps.length} attr${exclBit}</span>`
                    : '<span class="badge bg-secondary-subtle text-secondary">not mapped</span>';

            const rowClass = isExcluded ? 'summary-detail-row excluded' : 'summary-detail-row';
            return `<div class="${rowClass}">
                ${checkbox}
                <span class="summary-detail-name" title="${cls.uri}">${isExcluded ? `<s>${name}</s>` : name}</span>
                ${badge}
            </div>`;
        });
        entityListEls.forEach(el => {
            el.innerHTML = rows.join('');
            _bindExcludeCheckboxes(el);
        });
    }

    // --- Relationship list ---
    if (objectProperties.length === 0) {
        relListEls.forEach(el => { el.innerHTML = '<div class="text-muted small fst-italic">No relationships defined</div>'; });
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
        relListEls.forEach(el => {
            el.innerHTML = rows.join('');
            _bindExcludeCheckboxes(el);
        });
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

// Reset all mappings
async function confirmResetMappings() {
    // View / viewer / lock mode: CSS already greys the button; refuse here
    // so a synthetic click or a race before the body class lands cannot wipe
    // mappings on a non-editable version.
    if (window.OB && typeof window.OB.canEditOntology === 'function'
            && !window.OB.canEditOntology()) {
        if (typeof showNotification === 'function') {
            showNotification(
                'Unmap all is unavailable — this version is read-only.',
                'warning'
            );
        }
        return;
    }
    const confirmed = await showConfirmDialog({
        title: 'Unmap All',
        message: 'This will delete <strong>all entity and relationship mappings</strong>.<br>Hidden (excluded) entities, relationships and attributes stay hidden.<br><br>Are you sure you want to continue?',
        confirmText: 'Unmap all',
        confirmClass: 'btn-danger',
        icon: 'exclamation-triangle'
    });
    if (confirmed) {
        resetAllMappings();
    }
}

async function resetAllMappings() {
    const btn = document.getElementById('resetMappingsDesignBtn');
    const originalHTML = btn ? btn.innerHTML : '';
    if (btn) {
        btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
        btn.disabled = true;
    }
    
    try {
        // Only the mappings are deleted. Hidden (excluded) entities/relationships
        // and excluded attributes are the user's curation of the ontology, not
        // mappings: wiping them turned every hidden item red ("not mapped").
        // The server keeps them, and we must NOT push an empty config to
        // /mapping/save first - it replaces the lists wholesale.
        const clearResp = await fetch('/mapping/clear?keep_exclusions=true', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        const cleared = await clearResp.json();
        if (cleared && cleared.success === false) {
            throw new Error(cleared.message || 'Reset failed');
        }

        // Adopt what the server kept (the exclusion stubs) and re-stamp the
        // ontology objects, whose `excluded` flags are derived from that config.
        const loadResp = await fetch('/mapping/load', { credentials: 'same-origin' });
        const loaded = await loadResp.json();
        const kept = (loaded && loaded.config) || {};
        MappingState.config = {
            entities: kept.entities || [],
            relationships: kept.relationships || []
        };
        if (typeof _stampExcludedFlags === 'function') {
            _stampExcludedFlags();
        }
        
        MappingState.r2rmlAutoGenerated = false;
        MappingState.mappingValidated = false;
        
        updateMappingCompletionStatus();
        
        showNotification('✅ All mappings reset.', 'success');
        
    } catch (error) {
        console.error('Error resetting:', error);
        showNotification('Error: ' + error.message, 'error');
    } finally {
        if (btn) {
            btn.innerHTML = originalHTML;
            btn.disabled = false;
        }
    }
}

function _initMappingInformationButtons() {
    // "Unmap all" lives in the Mapping Studio header (#resetMappingsDesignBtn);
    // it opens the confirm dialog before clearing mappings.
    var _btn = document.getElementById('resetMappingsDesignBtn');
    if (_btn && !_btn.dataset.obResetWired) {
        _btn.addEventListener('click', function() { confirmResetMappings(); });
        _btn.dataset.obResetWired = '1';
    }
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _initMappingInformationButtons);
} else {
    _initMappingInformationButtons();
}

// Expose functions to global scope for mapping-core.js
window.updateTaxonomyStatus = updateTaxonomyStatus;
window.updateMappingCompletionStatus = updateMappingCompletionStatus;
window.confirmResetMappings = confirmResetMappings;
window.resetAllMappings = resetAllMappings;
