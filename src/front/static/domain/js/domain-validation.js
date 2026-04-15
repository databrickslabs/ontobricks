/**
 * OntoBricks - domain-validation.js
 * Populates the Project Validation dashboard.
 */

function hideValidationLoadingOverlay() {
    const overlay = document.getElementById('validationLoadingOverlay');
    if (overlay) overlay.style.display = 'none';
}

async function loadValidationDetails() {
    try {
        const response = await fetch('/validate/detailed', { credentials: 'same-origin' });
        const data = await response.json();

        updateHealthBanner(data);
        updateWarehouseTile(data);
        updateOntologyTile(data);
        updateMappingTile(data);
        updateDtwinTile(data);
        updateMetadataTile(data);
        updateDocumentsTile(data);
        updateVersionTiles(data);
        updateOntologyCard(data);
        updateMappingCard(data);
        updateDtwinCard(data);
        updateMissingItems(data);
    } catch (error) {
        console.error('Error loading validation:', error);
        const banner = document.getElementById('domainHealthBanner');
        if (banner) {
            banner.style.display = '';
            banner.className = 'validation-health-banner health-error mb-4';
            banner.innerHTML = '<i class="bi bi-exclamation-triangle health-icon"></i><div>Error loading validation status: ' + error.message + '</div>';
        }
    } finally {
        hideValidationLoadingOverlay();
    }
}

/* ── Health banner ────────────────────────────── */
function updateHealthBanner(data) {
    const banner = document.getElementById('domainHealthBanner');
    if (!banner) return;
    banner.style.display = '';

    const ready = data.ontology_valid && data.mapping_valid;
    const dtwin = data.dtwin || {};
    const warehouse = data.warehouse || {};
    const hasWarehouse = !!warehouse.warehouse_id;

    if (ready && dtwin.indicator === 'green' && hasWarehouse) {
        banner.className = 'validation-health-banner health-ready mb-4';
        banner.innerHTML = '<i class="bi bi-check-circle-fill health-icon"></i>' +
            '<div><strong>Domain fully operational</strong> — Ontology, Mapping, Warehouse and Digital Twin are all healthy.</div>';
        return;
    }

    const issues = [];
    if (!hasWarehouse) issues.push('SQL Warehouse not configured');
    if (!data.ontology_valid) issues.push('Ontology invalid');
    if (!data.mapping_valid) issues.push('Mapping incomplete');
    if (dtwin.indicator === 'red') issues.push('Digital Twin not built');
    else if (dtwin.indicator === 'orange') issues.push('Digital Twin partially ready');

    if (!data.ontology_valid || !data.mapping_valid || !hasWarehouse) {
        banner.className = 'validation-health-banner health-error mb-4';
        banner.innerHTML = '<i class="bi bi-exclamation-triangle-fill health-icon"></i>' +
            '<div><strong>Action needed</strong> — ' + issues.join(' · ') + '</div>';
    } else {
        banner.className = 'validation-health-banner health-warning mb-4';
        banner.innerHTML = '<i class="bi bi-exclamation-circle health-icon"></i>' +
            '<div><strong>Almost ready</strong> — ' + issues.join(' · ') + '</div>';
    }
}

/* ── Tile helpers ─────────────────────────────── */
function setTile(id, state, status) {
    const tile = document.getElementById(id);
    if (!tile) return;
    tile.className = 'validation-tile tile-' + state;
    const statusEl = tile.querySelector('.validation-tile-status');
    if (statusEl) statusEl.textContent = status;
}

function updateWarehouseTile(data) {
    const wh = data.warehouse || {};
    if (wh.warehouse_id) {
        setTile('warehouseTile', 'success', 'Connected');
    } else {
        setTile('warehouseTile', 'danger', 'Not configured');
    }
}

function updateOntologyTile(data) {
    if (data.ontology_valid) {
        const cls = (data.ontology_stats || {}).classes || 0;
        setTile('ontologyTile', 'success', cls + ' entities — Valid');
    } else {
        const issues = data.ontology_issues || [];
        setTile('ontologyTile', 'danger', issues[0] || 'Invalid');
    }
}

function updateMappingTile(data) {
    if (data.mapping_valid) {
        setTile('mappingTile', 'success', 'Complete');
    } else {
        const stats = data.mapping_stats || {};
        const total = (stats.total_classes || 0) + (stats.total_properties || 0);
        const mapped = (stats.entities || 0) + (stats.relationships || 0);
        if (total > 0) {
            const pct = Math.round((mapped / total) * 100);
            setTile('mappingTile', pct >= 50 ? 'warning' : 'danger', pct + '% complete');
        } else {
            setTile('mappingTile', 'muted', 'No items to map');
        }
    }
}

function updateDtwinTile(data) {
    const dt = data.dtwin || {};
    if (dt.indicator === 'green') {
        const tripleLabel = dt.count ? dt.count.toLocaleString() + ' triples' : 'Active';
        setTile('dtwinTile', 'success', tripleLabel);
    } else if (dt.indicator === 'orange') {
        setTile('dtwinTile', 'warning', 'Partially ready');
    } else {
        setTile('dtwinTile', 'danger', 'Not built');
    }
}

function updateMetadataTile(data) {
    var count = data.metadata_table_count;
    if (count > 0) {
        setTile('metadataTile', 'success', count + ' table' + (count !== 1 ? 's' : '') + ' imported');
    } else {
        setTile('metadataTile', 'muted', 'No tables imported');
    }
}

function updateDocumentsTile(data) {
    var count = data.document_count;
    if (count === null || count === undefined) {
        setTile('documentsTile', 'muted', 'Not checked yet');
    } else if (count > 0) {
        setTile('documentsTile', 'success', count + ' document' + (count !== 1 ? 's' : ''));
    } else {
        setTile('documentsTile', 'muted', 'No documents');
    }
}

/* ── Version tiles ────────────────────────────── */
function updateVersionTiles(data) {
    var version = data.domain_version || '1';
    setTile('currentVersionTile', 'success', 'v' + version);

    fetch('/domain/version-status', { credentials: 'same-origin' })
        .then(function(r) { return r.json(); })
        .then(function(vs) {
            if (vs.success && vs.available_versions) {
                var count = vs.available_versions.length;
                setTile('versionsTile', 'success', count + ' version' + (count !== 1 ? 's' : ''));
                var latest = vs.available_versions[0] || version;
                setTile('activeVersionTile', vs.is_active ? 'success' : 'warning',
                    'v' + latest + (vs.is_active ? '' : ' (read-only)'));
            } else {
                setTile('versionsTile', 'muted', '1 version');
                setTile('activeVersionTile', 'success', 'v' + version);
            }
        })
        .catch(function() {
            setTile('versionsTile', 'muted', '1 version');
            setTile('activeVersionTile', 'muted', 'v' + version);
        });
}

/* ── Ontology detail cards (split: entities + relationships) ── */
function updateOntologyCard(data) {
    const wrapper = document.getElementById('ontologyValidationCard');
    const card = wrapper ? wrapper.querySelector('.card') || wrapper : wrapper;
    const badge = document.getElementById('ontologyValidationBadge');
    const issuesDiv = document.getElementById('ontologyIssues');
    const warningsDiv = document.getElementById('ontologyWarnings');

    const relCard = document.getElementById('ontologyRelCard');
    const relBadge = document.getElementById('ontologyRelValidationBadge');

    document.getElementById('ontologyClassCount').textContent = (data.ontology_stats || {}).classes || 0;
    document.getElementById('ontologyPropertyCount').textContent = (data.ontology_stats || {}).properties || 0;

    var validClass = data.ontology_valid ? 'validation-card-valid' : 'validation-card-invalid';
    var badgeClass = data.ontology_valid ? 'badge bg-success' : 'badge bg-danger';
    var badgeText = data.ontology_valid ? 'Valid' : 'Invalid';

    if (card) card.className = 'card validation-detail-card h-100 ' + validClass;
    if (badge) { badge.className = badgeClass; badge.textContent = badgeText; }
    if (relCard) relCard.className = 'card validation-detail-card h-100 ' + validClass;
    if (relBadge) { relBadge.className = badgeClass; relBadge.textContent = badgeText; }

    const issues = data.ontology_issues || [];
    issuesDiv.innerHTML = issues.length > 0
        ? '<div class="alert alert-danger py-2 mb-2 small"><strong><i class="bi bi-exclamation-circle me-1"></i>Issues:</strong><ul class="mb-0 ps-3 mt-1">' + issues.map(function(i) { return '<li>' + i + '</li>'; }).join('') + '</ul></div>'
        : '';

    const warnings = data.ontology_warnings || [];
    warningsDiv.innerHTML = warnings.length > 0
        ? '<div class="alert alert-warning py-2 mb-0 small"><strong><i class="bi bi-exclamation-triangle me-1"></i>Warnings:</strong><ul class="mb-0 ps-3 mt-1">' + warnings.map(function(w) { return '<li>' + w + '</li>'; }).join('') + '</ul></div>'
        : '';
}

/* ── Mapping detail cards (split: entities, relationships, completion) ── */
function updateMappingCard(data) {
    var wrapper = document.getElementById('mappingValidationCard');
    var card = wrapper ? wrapper.querySelector('.card') || wrapper : wrapper;
    var badge = document.getElementById('mappingValidationBadge');
    var relCard = document.getElementById('mappingRelCard');
    var relBadge = document.getElementById('mappingRelValidationBadge');
    var compCard = document.getElementById('mappingCompletionCard');
    var progressBar = document.getElementById('mappingProgressBar');
    var completionPercent = document.getElementById('mappingCompletionPercent');
    var issuesDiv = document.getElementById('mappingIssues');
    var warningsDiv = document.getElementById('mappingWarnings');

    var stats = data.mapping_stats || {};
    var entityMappings = stats.entities || 0;
    var totalClasses = stats.total_classes || 0;
    var relMappings = stats.relationships || 0;
    var totalProperties = stats.total_properties || 0;
    var ignoredEntities = stats.ignored_entities || 0;
    var ignoredRelationships = stats.ignored_relationships || 0;

    document.getElementById('mappedEntitiesCount').textContent = entityMappings;
    document.getElementById('totalEntitiesCount').textContent = totalClasses;
    document.getElementById('mappedRelationshipsCount').textContent = relMappings;
    document.getElementById('totalRelationshipsCount').textContent = totalProperties;

    var ignoredEntitiesEl = document.getElementById('ignoredEntitiesLabel');
    if (ignoredEntitiesEl) {
        if (ignoredEntities > 0) {
            ignoredEntitiesEl.innerHTML = '<i class="bi bi-eye-slash me-1"></i>' + ignoredEntities + ' ignored';
            ignoredEntitiesEl.style.display = '';
        } else {
            ignoredEntitiesEl.style.display = 'none';
        }
    }
    var ignoredRelsEl = document.getElementById('ignoredRelationshipsLabel');
    if (ignoredRelsEl) {
        if (ignoredRelationships > 0) {
            ignoredRelsEl.innerHTML = '<i class="bi bi-eye-slash me-1"></i>' + ignoredRelationships + ' ignored';
            ignoredRelsEl.style.display = '';
        } else {
            ignoredRelsEl.style.display = 'none';
        }
    }

    var percent = 0;
    var totalItems = totalClasses + totalProperties;
    if (totalItems > 0) percent = Math.round(((entityMappings + relMappings) / totalItems) * 100);
    if (progressBar) {
        progressBar.style.width = percent + '%';
        progressBar.className = 'progress-bar' + (percent === 100 ? ' bg-success' : percent >= 50 ? ' bg-warning' : ' bg-danger');
    }
    if (completionPercent) completionPercent.textContent = percent + '%';

    var validClass = data.mapping_valid ? 'validation-card-valid' : 'validation-card-invalid';
    var badgeClass = data.mapping_valid ? 'badge bg-success' : 'badge bg-danger';
    var badgeText = data.mapping_valid ? 'Complete' : 'Incomplete';

    if (card) card.className = 'card validation-detail-card h-100 ' + validClass;
    if (badge) { badge.className = badgeClass; badge.textContent = badgeText; }
    if (relCard) relCard.className = 'card validation-detail-card h-100 ' + validClass;
    if (relBadge) { relBadge.className = badgeClass; relBadge.textContent = badgeText; }
    if (compCard) compCard.className = 'card validation-detail-card h-100 ' + validClass;

    var issues = data.mapping_issues || [];
    issuesDiv.innerHTML = issues.length > 0
        ? '<div class="alert alert-danger py-2 mb-2 small"><strong><i class="bi bi-exclamation-circle me-1"></i>Issues:</strong><ul class="mb-0 ps-3 mt-1">' + issues.map(function(i) { return '<li>' + i + '</li>'; }).join('') + '</ul></div>'
        : '';

    var warnings = data.mapping_warnings || [];
    warningsDiv.innerHTML = warnings.length > 0
        ? '<div class="alert alert-warning py-2 mb-0 small"><strong><i class="bi bi-exclamation-triangle me-1"></i>Warnings:</strong><ul class="mb-0 ps-3 mt-1">' + warnings.map(function(w) { return '<li>' + w + '</li>'; }).join('') + '</ul></div>'
        : '';
}

/* ── Digital Twin detail card ─────────────────── */

/** Reusable existence badge (same pattern as query-sync.js _badge) */
function _dtBadge(flag, okText, failText, unknownText) {
    if (flag === true)
        return '<span class="badge bg-success bg-opacity-10 text-success border border-success"><i class="bi bi-check-circle-fill me-1"></i>' + okText + '</span>';
    if (flag === false)
        return '<span class="badge bg-danger bg-opacity-10 text-danger border border-danger"><i class="bi bi-x-circle-fill me-1"></i>' + failText + '</span>';
    return '<span class="badge bg-secondary bg-opacity-10 text-secondary border"><i class="bi bi-dash-circle me-1"></i>' + (unknownText || 'N/A') + '</span>';
}

function _formatTimestamp(iso) {
    if (!iso) return null;
    try {
        var d = new Date(iso);
        return d.toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch (_) { return iso; }
}

function updateDtwinCard(data) {
    var card = document.getElementById('dtwinValidationCard');
    var badge = document.getElementById('dtwinValidationBadge');
    var dt = data.dtwin || {};

    // Card border + badge
    if (dt.indicator === 'green') {
        card.className = 'card validation-detail-card mb-4 validation-card-valid';
        badge.className = 'badge bg-success';
        badge.textContent = 'Active';
    } else if (dt.indicator === 'orange') {
        card.className = 'card validation-detail-card mb-4 validation-card-warning';
        badge.className = 'badge bg-warning text-dark';
        badge.textContent = 'Partial';
    } else {
        card.className = 'card validation-detail-card mb-4 validation-card-invalid';
        badge.className = 'badge bg-danger';
        badge.textContent = 'Not Built';
    }

    // Rebuild warning
    var warning = document.getElementById('psDtRebuildWarning');
    if (warning) {
        if (dt.needs_rebuild) {
            warning.classList.remove('d-none');
            warning.classList.add('d-flex');
        } else {
            warning.classList.remove('d-flex');
            warning.classList.add('d-none');
        }
    }

    // Timestamps
    var lastBuiltEl = document.getElementById('psDtLastBuilt');
    if (lastBuiltEl) {
        var fmtBuilt = _formatTimestamp(dt.last_built);
        if (fmtBuilt) {
            lastBuiltEl.innerHTML = '<i class="bi bi-check-circle-fill text-success me-1"></i>' + fmtBuilt;
        } else {
            lastBuiltEl.innerHTML = '<span class="text-muted">Never built</span>';
        }
    }
    var lastUpdateEl = document.getElementById('psDtLastUpdate');
    if (lastUpdateEl) {
        var fmtUpdate = _formatTimestamp(dt.last_update);
        if (fmtUpdate) {
            lastUpdateEl.innerHTML = '<i class="bi bi-info-circle-fill text-info me-1"></i>' + fmtUpdate;
        } else {
            lastUpdateEl.innerHTML = '<span class="text-muted">No changes yet</span>';
        }
    }

    // Zero-Copy VIEW card
    var viewEl = document.getElementById('psDtExistView');
    if (viewEl) viewEl.innerHTML = _dtBadge(dt.view_exists, 'Exists', 'Not found', 'Not configured');

    var zcCard = document.getElementById('psDtZeroCopyCard');
    if (zcCard) {
        if (dt.view_exists === true) zcCard.className = 'border rounded p-3 h-100 border-success';
        else if (dt.view_exists === false) zcCard.className = 'border rounded p-3 h-100 border-danger';
        else zcCard.className = 'border rounded p-3 h-100';
    }

    var viewNameEl = document.getElementById('psDtViewName');
    if (viewNameEl) viewNameEl.textContent = dt.view_table || 'Not configured';

    // Snapshot
    var snapshotArea = document.getElementById('psDtSnapshotArea');
    if (snapshotArea && dt.snapshot_table) {
        snapshotArea.style.display = '';
        var snapshotName = document.getElementById('psDtSnapshotName');
        if (snapshotName) snapshotName.textContent = dt.snapshot_table;
        var snapshotBadge = document.getElementById('psDtExistSnapshot');
        if (snapshotBadge) snapshotBadge.innerHTML = _dtBadge(dt.snapshot_exists, 'Exists', 'Not created', 'N/A');
    } else if (snapshotArea) {
        snapshotArea.style.display = 'none';
    }

    // Graph DB card
    var localEl = document.getElementById('psDtExistLocal');
    if (localEl) localEl.innerHTML = _dtBadge(dt.local_lbug_exists, 'Loaded', 'Not loaded', 'N/A');

    var graphCard = document.getElementById('psDtGraphCard');
    if (graphCard) {
        if (dt.local_lbug_exists === true) graphCard.className = 'border rounded p-3 h-100 border-success';
        else if (dt.local_lbug_exists === false) graphCard.className = 'border rounded p-3 h-100 border-danger';
        else graphCard.className = 'border rounded p-3 h-100';
    }

    var regEl = document.getElementById('psDtExistRegistry');
    if (regEl) regEl.innerHTML = _dtBadge(dt.registry_lbug_exists, 'Archived', 'Not archived', 'Not configured');

    // Triple count
    var tripleArea = document.getElementById('psDtTripleArea');
    var tripleVal = document.getElementById('psDtTripleValue');
    if (tripleArea) {
        var count = dt.triple_count || dt.count || 0;
        if (count > 0) {
            tripleArea.style.display = '';
            if (tripleVal) tripleVal.textContent = count.toLocaleString();
        } else {
            tripleArea.style.display = 'none';
        }
    }
}

/* ── Missing items ────────────────────────────── */
function updateMissingItems(data) {
    const card = document.getElementById('missingItemsCard');
    const entitiesList = document.getElementById('unmappedEntitiesList');
    const relationshipsList = document.getElementById('unmappedRelationshipsList');

    const unmappedEntities = data.unmapped_entities || [];
    const unmappedRelationships = data.unmapped_relationships || [];

    if (unmappedEntities.length === 0 && unmappedRelationships.length === 0) {
        card.style.display = 'none';
        return;
    }

    card.style.display = 'block';

    entitiesList.innerHTML = unmappedEntities.length > 0
        ? unmappedEntities.map(function(e) {
            return '<li class="list-group-item d-flex justify-content-between align-items-center py-1"><span><i class="bi bi-box text-muted me-1"></i> ' + (e.label || e.name) + '</span><a href="/mapping/" class="btn btn-outline-primary btn-sm py-0">Assign</a></li>';
        }).join('')
        : '<li class="list-group-item text-success py-1"><i class="bi bi-check me-1"></i> All entities assigned</li>';

    relationshipsList.innerHTML = unmappedRelationships.length > 0
        ? unmappedRelationships.map(function(r) {
            return '<li class="list-group-item d-flex justify-content-between align-items-center py-1"><span><i class="bi bi-arrow-left-right text-muted me-1"></i> ' + (r.label || r.name) + '</span><a href="/mapping/" class="btn btn-outline-primary btn-sm py-0">Assign</a></li>';
        }).join('')
        : '<li class="list-group-item text-success py-1"><i class="bi bi-check me-1"></i> All relationships assigned</li>';
}

/* ── Bootstrap on section activation ──────────── */
document.addEventListener('DOMContentLoaded', function() {
    var urlParams = new URLSearchParams(window.location.search);
    if (urlParams.get('section') === 'validation') {
        loadValidationDetails();
    }
});
