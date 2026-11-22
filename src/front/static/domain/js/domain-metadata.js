/**
 * OntoBricks - domain-metadata.js
 * Extracted from domain templates per code_instructions.txt
 */

let tableSelections = {}; // Track selected tables by name
let currentEditingTableIndex = null; // Track which table is being edited in the modal
let importTableSelections = {}; // Track tables selected for import in the modal
let allAvailableTables = []; // All tables available in UC schema
let addDataAssetsModal = null; // Bootstrap instance of #addDataAssetsModal
let addDataAssetsRequestId = 0; // Ignores stale list-tables responses
let dataAssetsImportLimit = 40;

// Visual metadata for a data-source object kind (table | view | metric_view).
// Returns the row icon, its color class, and an optional inline badge.
function objectKindMeta(objectKind) {
    switch (objectKind) {
        case 'metric_view':
            return {
                icon: 'bi-graph-up-arrow',
                iconClass: 'text-info',
                badge: '<span class="badge bg-info ms-1">Metric View</span>',
            };
        case 'view':
            return {
                icon: 'bi-eye',
                iconClass: 'text-secondary',
                badge: '<span class="badge bg-secondary ms-1">View</span>',
            };
        case 'table':
        default:
            return { icon: 'bi-table', iconClass: 'text-primary', badge: '' };
    }
}

const _metadataGauges = {};

function _drawMetadataGauge(canvasId, score) {
    if (_metadataGauges[canvasId]) {
        _metadataGauges[canvasId].destroy();
        delete _metadataGauges[canvasId];
    }
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    const val = Math.max(0, Math.min(100, Math.round(score)));
    const color = val === 100 ? '#198754' : val >= 80 ? '#ffc107' : '#dc3545';
    const remaining = 100 - val;

    _metadataGauges[canvasId] = new Chart(ctx, {
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
            id: 'metadataGaugeLabel',
            afterDraw(chart) {
                const c = chart.ctx, w = chart.width, h = chart.height;
                c.save();
                c.textAlign = 'center';
                c.textBaseline = 'bottom';
                c.font = 'bold 14px system-ui, sans-serif';
                c.fillStyle = color;
                c.fillText(val + '%', w / 2, h - 4);
                c.restore();
            }
        }],
    });
}

function updateMetadataGauges(metadata) {
    const tables = metadata?.tables || [];
    if (tables.length === 0) return;

    let tablesWithDesc = 0;
    let totalColumns = 0;
    let columnsWithDesc = 0;

    for (const t of tables) {
        if (t.comment && t.comment.trim()) tablesWithDesc++;
        for (const col of (t.columns || [])) {
            totalColumns++;
            if (col.comment && col.comment.trim()) columnsWithDesc++;
        }
    }

    const tableScore = Math.round((tablesWithDesc / tables.length) * 100);
    const colScore = totalColumns > 0 ? Math.round((columnsWithDesc / totalColumns) * 100) : 0;

    _drawMetadataGauge('gaugeTableDesc', tableScore);
    _drawMetadataGauge('gaugeColumnDesc', colScore);

    const tableDetail = document.getElementById('gaugeTableDescDetail');
    if (tableDetail) tableDetail.textContent = `${tablesWithDesc} / ${tables.length} tables`;

    const colDetail = document.getElementById('gaugeColumnDescDetail');
    if (colDetail) colDetail.textContent = `${columnsWithDesc} / ${totalColumns} columns`;
}

// Async task tracking
const METADATA_LOAD_TASK_KEY = 'ontobricks_metadata_load_task';
const METADATA_UPDATE_TASK_KEY = 'ontobricks_metadata_update_task';

// Initialize metadata section
async function initMetadataSection() {
    await loadMetadataStatus();
}

// Open the single "Add Data Assets" dialog (catalog, schema, tables, import)
async function showLoadMetadataModal() {
    addDataAssetsModal = bootstrap.Modal.getOrCreateInstance(document.getElementById('addDataAssetsModal'));
    resetAddDataAssetsListing();
    const schemaSelect = document.getElementById('addDataAssetsSchema');
    schemaSelect.innerHTML = '<option value="">Select catalog first</option>';
    schemaSelect.disabled = true;
    addDataAssetsModal.show();
    await _populateAddDataAssetsCatalogs();
}

// Fill the catalog <select>. Optionally keep the current value if it still exists.
async function _populateAddDataAssetsCatalogs(preferredCatalog = '') {
    const catalogSelect = document.getElementById('addDataAssetsCatalog');
    catalogSelect.innerHTML = '<option value="">Loading catalogs...</option>';
    const catalogs = await UCLocationWidget.loadCatalogs(true);
    catalogSelect.innerHTML = catalogs.length > 0
        ? '<option value="">Select catalog...</option>' + catalogs.map(_optionHtml).join('')
        : '<option value="">No catalogs available</option>';
    if (preferredCatalog && catalogs.includes(preferredCatalog)) {
        catalogSelect.value = preferredCatalog;
    }
}

function _optionHtml(value) {
    const safe = escapeHtml(value);
    return `<option value="${safe}">${safe}</option>`;
}

function getAddDataAssetsLocation() {
    return {
        catalog: document.getElementById('addDataAssetsCatalog')?.value || '',
        schema: document.getElementById('addDataAssetsSchema')?.value || '',
    };
}

// Show exactly one of: placeholder ('placeholder'), spinner ('listing') or table list ('list')
function _showAddAssetsState(state, placeholderText) {
    const placeholder = document.getElementById('addDataAssetsPlaceholder');
    const listing = document.getElementById('addDataAssetsListing');
    const list = document.getElementById('addDataAssetsList');
    if (placeholder) {
        placeholder.textContent = placeholderText || 'Choose a catalog and schema to list its tables.';
        placeholder.classList.toggle('d-none', state !== 'placeholder');
    }
    if (listing) listing.classList.toggle('d-none', state !== 'listing');
    if (list) list.classList.toggle('d-none', state !== 'list');
}

// Clear the table list and selection; invalidates any in-flight list-tables request
function resetAddDataAssetsListing() {
    addDataAssetsRequestId += 1;
    allAvailableTables = [];
    importTableSelections = {};
    const tbody = document.getElementById('tableSelectionBody');
    if (tbody) tbody.innerHTML = '';
    const permBanner = document.getElementById('tableSelectionPermBanner');
    if (permBanner) permBanner.classList.add('d-none');
    const info = document.getElementById('tableSelectionInfo');
    if (info) info.innerHTML = '';
    const filter = document.getElementById('tableSearchFilter');
    if (filter) filter.value = '';
    _showAddAssetsState('placeholder');
    updateImportSelectionCount();
}

// Catalog changed: reload its schemas and clear the table list
async function onAddDataAssetsCatalogChange() {
    const catalog = document.getElementById('addDataAssetsCatalog').value;
    const schemaSelect = document.getElementById('addDataAssetsSchema');
    resetAddDataAssetsListing();
    schemaSelect.disabled = true;

    if (!catalog) {
        schemaSelect.innerHTML = '<option value="">Select catalog first</option>';
        return;
    }

    schemaSelect.innerHTML = '<option value="">Loading schemas...</option>';
    const schemas = await UCLocationWidget.loadSchemas(catalog);
    // Ignore the answer if the user already picked another catalog
    if (document.getElementById('addDataAssetsCatalog').value !== catalog) return;

    if (schemas.length === 0) {
        schemaSelect.innerHTML = '<option value="">No schemas available</option>';
        return;
    }
    schemaSelect.innerHTML = '<option value="">Select schema...</option>' + schemas.map(_optionHtml).join('');
    schemaSelect.disabled = false;
}

// Schema changed: list its tables inline
function onAddDataAssetsSchemaChange() {
    const { catalog, schema } = getAddDataAssetsLocation();
    if (!catalog || !schema) {
        resetAddDataAssetsListing();
        return;
    }
    loadTablesForSelection(catalog, schema);
}

/**
 * Refresh: clear the UC location cache, reload catalogs/schemas, and re-list
 * tables for the currently selected location (if any).
 */
async function refreshAddDataAssets() {
    const { catalog, schema } = getAddDataAssetsLocation();
    const refreshBtn = document.getElementById('addDataAssetsRefreshBtn');
    if (refreshBtn) refreshBtn.disabled = true;
    try {
        UCLocationWidget.clearCache();
        await _populateAddDataAssetsCatalogs(catalog);
        if (!catalog) {
            const schemaSelect = document.getElementById('addDataAssetsSchema');
            schemaSelect.innerHTML = '<option value="">Select catalog first</option>';
            schemaSelect.disabled = true;
            resetAddDataAssetsListing();
            return;
        }
        await onAddDataAssetsCatalogChange();
        if (schema) {
            const schemaSelect = document.getElementById('addDataAssetsSchema');
            if ([...schemaSelect.options].some(o => o.value === schema)) {
                schemaSelect.value = schema;
                loadTablesForSelection(catalog, schema);
            }
        }
    } finally {
        if (refreshBtn) refreshBtn.disabled = false;
    }
}

function setMetadataListOverlay(visible, text) {
    const overlay = document.getElementById('metadataListOverlay');
    const label = document.getElementById('metadataListOverlayText');
    if (label && text) label.textContent = text;
    if (overlay) overlay.classList.toggle('d-none', !visible);
}

/**
 * Paint the asset list from session metadata, then refresh Rights in the
 * background so first paint is not blocked by warehouse SELECT probes.
 * @param {{listOverlay?: boolean, overlayText?: string, refreshRights?: boolean}} [opts]
 */
async function loadMetadataStatus(opts = {}) {
    const listOverlay = opts.listOverlay === true;
    const refreshRights = opts.refreshRights !== false;
    const statusDiv = document.getElementById('metadataStatus');
    const statusText = document.getElementById('metadataStatusText');
    const previewDiv = document.getElementById('metadataPreview');

    if (listOverlay) {
        setMetadataListOverlay(true, opts.overlayText || 'Reloading assets…');
        if (previewDiv) previewDiv.classList.remove('d-none');
    }
    
    if (statusDiv) statusDiv.className = 'mb-4';
    if (statusText && !listOverlay) {
        statusText.className = 'text-muted small d-flex align-items-center gap-2';
        statusText.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span><span>Loading data assets…</span>';
    }
    
    try {
        const response = await fetch('/domain/metadata', { credentials: 'same-origin' });
        const data = await response.json();
        
        if (data.success && data.has_metadata) {
            metadataCache = data.metadata;
            const tableCount = data.metadata.tables?.length || 0;
            
            // Collect distinct data-source locations from tables
            const locations = new Set();
            (data.metadata.tables || []).forEach(t => {
                const parts = (t.full_name || '').split('.');
                if (parts.length >= 2) locations.add(`${parts[0]}.${parts[1]}`);
            });
            const locationDisplay = locations.size === 1
                ? [...locations][0]
                : locations.size > 1 ? `${locations.size} locations` : '';
            
            if (statusDiv) statusDiv.className = 'mb-4';
            if (statusText) {
                statusText.className = 'small d-flex align-items-center gap-2';
                statusText.innerHTML = `
                    <i class="bi bi-check-circle-fill text-success"></i>
                    <span><strong>Data assets loaded:</strong> ${locationDisplay ? `<strong>${locationDisplay}</strong> — ` : ''}${tableCount} table${tableCount !== 1 ? 's' : ''}</span>
                `;
            }
            
            // Initialize table selections (unchecked = keep; checked = mark for removal)
            tableSelections = {};
            (data.metadata.tables || []).forEach(table => {
                tableSelections[table.name] = false;
            });
            
            // Show preview and Update Mappings button
            displayMetadataPreview(data.metadata);
            checkMetadataDescriptions(data.metadata);
            updateMetadataGauges(data.metadata);
            const updateMappingsBtn = document.getElementById('updateMappingsBtn');
            if (updateMappingsBtn) updateMappingsBtn.classList.remove('d-none');
            if (refreshRights) {
                refreshSelectPermissionsInBackground();
            }
        } else {
            metadataCache = null;
            tableSelections = {};
            if (statusDiv) statusDiv.className = 'mb-4';
            if (statusText) {
                statusText.className = 'text-muted small d-flex align-items-center gap-2';
                statusText.innerHTML = '<i class="bi bi-info-circle"></i><span>No data assets loaded</span>';
            }
            previewDiv.classList.add('d-none');
            const warningEl = document.getElementById('metadataDescriptionWarning');
            if (warningEl) warningEl.style.display = 'none';
            const updateMappingsBtn = document.getElementById('updateMappingsBtn');
            if (updateMappingsBtn) updateMappingsBtn.classList.add('d-none');
        }
    } catch (error) {
        console.error('Error loading metadata status:', error);
        if (statusDiv) statusDiv.className = 'mb-4';
        if (statusText) {
            statusText.className = 'text-danger small d-flex align-items-center gap-2';
            statusText.innerHTML = '<i class="bi bi-exclamation-triangle"></i><span>Error loading data assets status</span>';
        }
    } finally {
        if (listOverlay) setMetadataListOverlay(false);
    }
}

let _rightsRefreshSeq = 0;

/** Fill the Rights column via warehouse SELECT probes without blocking first paint. */
async function refreshSelectPermissionsInBackground() {
    const seq = ++_rightsRefreshSeq;
    const cells = document.querySelectorAll('.dm-rights-cell');
    cells.forEach(cell => {
        cell.innerHTML = '<span class="spinner-border spinner-border-sm text-muted" role="status" aria-hidden="true" title="Checking SELECT…"></span>';
    });
    try {
        const response = await fetch('/domain/metadata/check-select', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: '{}',
            credentials: 'same-origin',
        });
        const data = await response.json();
        if (seq !== _rightsRefreshSeq) return;
        if (!data.success) {
            cells.forEach(cell => {
                cell.innerHTML = renderSelectPermCell({});
            });
            return;
        }
        const byName = {};
        (data.tables || []).forEach(row => { byName[row.name] = row; });
        if (metadataCache && Array.isArray(metadataCache.tables)) {
            metadataCache.tables.forEach(table => {
                const row = byName[table.name];
                if (!row) return;
                table.can_select = row.can_select;
                table.select_error = row.select_error;
            });
        }
        document.querySelectorAll('.dm-rights-cell').forEach(cell => {
            const name = cell.dataset.table;
            const table = (metadataCache?.tables || []).find(t => t.name === name) || byName[name] || {};
            cell.innerHTML = renderSelectPermCell(table);
        });
    } catch (error) {
        if (seq !== _rightsRefreshSeq) return;
        console.error('[Metadata] Rights refresh failed:', error);
        document.querySelectorAll('.dm-rights-cell').forEach(cell => {
            cell.innerHTML = '<span class="text-muted" title="Could not check SELECT"><i class="bi bi-dash-circle"></i></span>';
        });
    }
}

/**
 * List the tables of catalog.schema inside the Add Data Assets dialog and
 * pre-select the new ones (up to the import limit).
 */
async function loadTablesForSelection(catalog, schema) {
    if (!catalog || !schema) return;

    resetAddDataAssetsListing();
    const requestId = ++addDataAssetsRequestId;
    _showAddAssetsState('listing');

    try {
        // Lightweight list of table names, metadata is loaded on import
        const response = await fetch('/domain/metadata/list-tables', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ catalog, schema }),
            credentials: 'same-origin'
        });
        const data = await response.json();
        if (requestId !== addDataAssetsRequestId) return; // superseded by a newer selection

        if (!data.success) {
            _showAddAssetsState('placeholder', 'Could not list tables.');
            showNotification('Error: ' + (data.message || 'Failed to list tables'), 'error');
            return;
        }

        allAvailableTables = data.tables || [];
        if (typeof data.import_limit === 'number' && data.import_limit > 0) {
            dataAssetsImportLimit = data.import_limit;
        }
        const perms = data.permissions || {};

        if (allAvailableTables.length === 0) {
            if (perms.permission_warning) {
                const msg = `No tables visible in ${catalog}.${schema}. ` + perms.permission_warning;
                showNotification(msg, 'error');
                _showAddAssetsState('placeholder', msg);
            } else {
                const msg = `No tables found in ${catalog}.${schema}`;
                showNotification(msg, 'warning');
                _showAddAssetsState('placeholder', msg);
            }
            return;
        }

        // Non-blocking warning when SELECT is denied
        if (perms.permission_warning) {
            showNotification(perms.permission_warning, 'warning');
        }

        // Initialize import selections (select new tables up to the import limit)
        importTableSelections = {};
        let remaining = _remainingImportSlots();
        allAvailableTables.forEach(table => {
            const take = !table.already_loaded && remaining > 0;
            importTableSelections[table.name] = take;
            if (take) remaining -= 1;
        });

        renderImportTable(catalog, schema, perms);
        _showAddAssetsState('list');
    } catch (error) {
        if (requestId !== addDataAssetsRequestId) return;
        _showAddAssetsState('placeholder', 'Could not list tables.');
        showNotification('Error: ' + error.message, 'error');
    }
}

// Render the info badges, permission banner and table rows of the Add Data Assets list
function renderImportTable(catalog, schema, perms) {
    const tbody = document.getElementById('tableSelectionBody');
    const infoSpan = document.getElementById('tableSelectionInfo');

    const newCount = allAvailableTables.filter(t => !t.already_loaded).length;
    const existingCount = allAvailableTables.filter(t => t.already_loaded).length;

    infoSpan.innerHTML = `<code>${catalog}.${schema}</code> - ${allAvailableTables.length} table(s) found`;
    if (existingCount > 0) {
        infoSpan.innerHTML += ` <span class="badge bg-info">${existingCount} already loaded</span>`;
    }
    const remaining = _remainingImportSlots();
    infoSpan.innerHTML += ` <span class="badge bg-secondary">Import limit ${dataAssetsImportLimit} (${remaining} slot${remaining !== 1 ? 's' : ''} left)</span>`;
    if (newCount > remaining) {
        infoSpan.innerHTML += ` <span class="badge bg-warning text-dark">${newCount - remaining} new asset(s) skipped</span>`;
    }

    // Permission status banner
    const permBanner = document.getElementById('tableSelectionPermBanner');
    if (permBanner) {
        if (!perms) {
            permBanner.classList.add('d-none');
        } else {
            const listOk  = perms.can_list_tables !== false;
            const selOk   = perms.can_select === true;
            const selUnk  = perms.can_select === null;

            const listBadge = listOk
                ? '<span class="badge bg-success me-1"><i class="bi bi-check-circle me-1"></i>List tables: OK</span>'
                : '<span class="badge bg-danger me-1"><i class="bi bi-x-circle me-1"></i>List tables: denied</span>';

            const selBadge = selUnk
                ? '<span class="badge bg-secondary me-1"><i class="bi bi-dash-circle me-1"></i>SELECT: n/a</span>'
                : selOk
                    ? '<span class="badge bg-success me-1"><i class="bi bi-check-circle me-1"></i>SELECT data: OK</span>'
                    : '<span class="badge bg-warning text-dark me-1"><i class="bi bi-exclamation-triangle me-1"></i>SELECT data: denied</span>';

            permBanner.innerHTML = `
                <div class="d-flex align-items-center flex-wrap gap-1">
                    <small class="text-muted me-1">Service principal permissions:</small>
                    ${listBadge}${selBadge}
                    ${perms.permission_warning
                        ? `<small class="text-warning ms-1"><i class="bi bi-exclamation-triangle me-1"></i>${perms.permission_warning}</small>`
                        : ''}
                </div>`;
            permBanner.classList.remove('d-none');
        }
    }
    
    // Build table rows — already-loaded assets have no checkbox (cannot reload)
    let html = '';
    allAvailableTables.forEach(table => {
        const isSelected = importTableSelections[table.name] === true;
        const statusBadge = table.already_loaded 
            ? '<span class="badge bg-info">Already loaded</span>' 
            : '<span class="badge bg-success">New</span>';
        const kind = objectKindMeta(table.object_kind);
        const checkboxCell = table.already_loaded
            ? '<td></td>'
            : `<td>
                    <input type="checkbox" class="form-check-input import-table-checkbox"
                           data-table="${escapeHtml(table.name)}"
                           ${isSelected ? 'checked' : ''}>
               </td>`;
        
        html += `
            <tr class="import-table-row ${table.already_loaded ? 'table-light' : ''}" data-table="${escapeHtml(table.name)}">
                ${checkboxCell}
                <td>
                    <i class="bi ${kind.icon} ${kind.iconClass} me-1"></i>
                    <strong>${escapeHtml(table.name)}</strong>
                    ${kind.badge}
                </td>
                <td>${statusBadge}</td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html || '<tr><td colspan="3" class="text-center text-muted">No tables found</td></tr>';
    
    updateImportSelectionCount();
    updateSelectAllImportCheckbox();
    
}

function _alreadyLoadedCount() {
    if (metadataCache && Array.isArray(metadataCache.tables)) {
        return metadataCache.tables.length;
    }
    return allAvailableTables.filter(t => t.already_loaded).length;
}

function _remainingImportSlots() {
    return Math.max(0, dataAssetsImportLimit - _alreadyLoadedCount());
}

/**
 * Decide which visible import rows to check. Already-loaded assets are never
 * selected (they have no checkbox). The remaining-slot cap applies to new assets.
 * @returns {{selections: Object<string, boolean>, skippedNew: number}}
 */
function computeSelectAllImportState(tables, visibleNames, isSelected, remainingSlots) {
    const selections = {};
    let remaining = remainingSlots;
    let skippedNew = 0;
    visibleNames.forEach(name => {
        const table = tables.find(t => t.name === name);
        if (table && table.already_loaded) {
            selections[name] = false;
            return;
        }
        let take = isSelected;
        if (take) {
            if (remaining <= 0) {
                take = false;
                skippedNew += 1;
            } else {
                remaining -= 1;
            }
        }
        selections[name] = take;
    });
    return { selections, skippedNew };
}

function _selectedNewCount() {
    return allAvailableTables.filter(t => !t.already_loaded && importTableSelections[t.name]).length;
}

function toggleImportTableSelection(tableName, isSelected) {
    const table = allAvailableTables.find(t => t.name === tableName);
    if (isSelected && table && !table.already_loaded && _selectedNewCount() >= _remainingImportSlots()) {
        importTableSelections[tableName] = false;
        const checkbox = document.querySelector(`.import-table-checkbox[data-table="${tableName}"]`);
        if (checkbox) checkbox.checked = false;
        showNotification(
            `Import limit is ${dataAssetsImportLimit} data assets (Settings → Global). Deselect another asset first.`,
            'warning'
        );
        updateImportSelectionCount();
        updateSelectAllImportCheckbox();
        return;
    }
    importTableSelections[tableName] = isSelected;
    updateImportSelectionCount();
    updateSelectAllImportCheckbox();
}

function selectAllImportTables(isSelected) {
    const visibleRows = document.querySelectorAll('.import-table-row:not(.d-none)');
    const visibleNames = Array.from(visibleRows).map(r => r.dataset.table);
    const { selections, skippedNew } = computeSelectAllImportState(
        allAvailableTables,
        visibleNames,
        isSelected,
        _remainingImportSlots()
    );
    visibleRows.forEach(row => {
        const tableName = row.dataset.table;
        const take = selections[tableName] === true;
        importTableSelections[tableName] = take;
        const checkbox = row.querySelector('.import-table-checkbox');
        if (checkbox) checkbox.checked = take;
    });
    if (isSelected && skippedNew > 0) {
        showNotification(
            `Import limit is ${dataAssetsImportLimit} data assets (Settings → Global). ${skippedNew} new asset(s) were left unselected.`,
            'warning'
        );
    }
    updateImportSelectionCount();
    updateSelectAllImportCheckbox();
}

function selectNewTablesOnly() {
    let remaining = _remainingImportSlots();
    allAvailableTables.forEach(table => {
        const take = !table.already_loaded && remaining > 0;
        importTableSelections[table.name] = take;
        if (take) remaining -= 1;
    });
    
    // Update checkboxes
    document.querySelectorAll('.import-table-checkbox').forEach(checkbox => {
        const tableName = checkbox.dataset.table;
        checkbox.checked = importTableSelections[tableName] === true;
    });
    
    updateImportSelectionCount();
    updateSelectAllImportCheckbox();
}

function filterImportTables(filter) {
    const lowerFilter = filter.toLowerCase();
    const rows = document.querySelectorAll('.import-table-row');
    
    rows.forEach(row => {
        const tableName = row.dataset.table.toLowerCase();
        if (tableName.includes(lowerFilter)) {
            row.classList.remove('d-none');
        } else {
            row.classList.add('d-none');
        }
    });
}

function updateImportSelectionCount() {
    const selectedCount = Object.values(importTableSelections).filter(v => v).length;
    const selectableCount = allAvailableTables.filter(t => !t.already_loaded).length;
    
    const countSpan = document.getElementById('selectedCountInfo');
    const importBtn = document.getElementById('importSelectedBtn');
    
    if (countSpan) {
        countSpan.textContent = `${selectedCount} of ${selectableCount} new table${selectableCount !== 1 ? 's' : ''} selected`;
    }
    
    if (importBtn) {
        importBtn.disabled = selectedCount === 0;
        importBtn.innerHTML = `<i class="bi bi-download"></i> Add ${selectedCount} asset${selectedCount !== 1 ? 's' : ''}`;
    }
}

function updateSelectAllImportCheckbox() {
    // Only rows that still have a checkbox (new / not already loaded)
    const visibleNames = Array.from(
        document.querySelectorAll('.import-table-row:not(.d-none) .import-table-checkbox')
    ).map(cb => cb.dataset.table);
    
    const checkbox = document.getElementById('selectAllImportCheckbox');
    if (!checkbox) return;
    if (visibleNames.length === 0) {
        checkbox.checked = false;
        checkbox.indeterminate = false;
        return;
    }
    const allSelected = visibleNames.every(name => importTableSelections[name] === true);
    const someSelected = visibleNames.some(name => importTableSelections[name] === true);
    checkbox.checked = allSelected;
    checkbox.indeterminate = someSelected && !allSelected;
}

async function importSelectedTables() {
    let selectedTables = Object.entries(importTableSelections)
        .filter(([, selected]) => selected)
        .map(([name]) => name)
        .filter(name => {
            const table = allAvailableTables.find(t => t.name === name);
            return table && !table.already_loaded;
        });
    
    if (selectedTables.length === 0) {
        showNotification('No new tables selected for import', 'warning');
        return;
    }

    const remaining = _remainingImportSlots();
    if (selectedTables.length > remaining) {
        showNotification(
            `Import limit is ${dataAssetsImportLimit}. Only the first ${remaining} selected asset(s) will be imported.`,
            'warning'
        );
        selectedTables = selectedTables.slice(0, remaining);
    }
    if (selectedTables.length === 0) {
        showNotification(
            `Data assets import limit of ${dataAssetsImportLimit} reached. Raise it in Settings → Global or remove existing assets.`,
            'warning'
        );
        return;
    }
    
    // Catalog and schema chosen in the Add Data Assets dialog
    const { catalog, schema } = getAddDataAssetsLocation();
    
    // Close the modal and show a spinner on the assets list while we import
    bootstrap.Modal.getOrCreateInstance(document.getElementById('addDataAssetsModal')).hide();
    const previewDiv = document.getElementById('metadataPreview');
    if (previewDiv) previewDiv.classList.remove('d-none');
    setMetadataListOverlay(true, 'Importing assets…');
    
    const loadBtn = document.getElementById('loadMetadataBtn');
    const progressDiv = document.getElementById('metadataProgress');
    const statusSpan = document.getElementById('metadataInitStatus');
    
    loadBtn.disabled = true;
    progressDiv.classList.remove('d-none');
    statusSpan.textContent = `Loading data assets for ${selectedTables.length} table(s)...`;
    
    try {
        // Start async task
        const response = await fetch('/domain/metadata/initialize-async', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                catalog, 
                schema,
                selected_tables: selectedTables 
            }),
            credentials: 'same-origin'
        });
        
        const startResult = await response.json();
        
        if (!startResult.success) {
            progressDiv.classList.add('d-none');
            loadBtn.disabled = false;
            setMetadataListOverlay(false);
            showNotification('Error: ' + startResult.message, 'error');
            return;
        }
        
        const taskId = startResult.task_id;
        sessionStorage.setItem(METADATA_LOAD_TASK_KEY, taskId);
        
        showNotification('Data assets loading started...', 'info');
        
        // Trigger refresh of task tracker
        if (typeof refreshTasks === 'function') refreshTasks();
        
        // Monitor the task (reload + rights happen on completion)
        await monitorMetadataTask(taskId, 'load', loadBtn, progressDiv, statusSpan);
        
    } catch (error) {
        progressDiv.classList.add('d-none');
        loadBtn.disabled = false;
        setMetadataListOverlay(false);
        showNotification('Error: ' + error.message, 'error');
    }
}

/**
 * Render the per-table column diff returned by a metadata refresh.
 * @param {Object} diff - table name -> {added, removed, type_changed, unchanged}
 * @returns {string} HTML
 */
function buildColumnDiffHtml(diff) {
    const row = (badge, name, detail) => `
        <tr>
            <td>${badge}</td>
            <td><code>${escapeHtml(name)}</code></td>
            <td class="text-muted small">${detail}</td>
        </tr>
    `;

    return Object.entries(diff).map(([table, tableDiff]) => {
        const added = (tableDiff.added || []).map(c => row(
            '<span class="badge bg-success">Added</span>',
            c.name,
            escapeHtml(c.type || '')
        ));
        const removed = (tableDiff.removed || []).map(c => row(
            '<span class="badge bg-danger">Removed</span>',
            c.name,
            escapeHtml(c.type || '')
        ));
        const typeChanged = (tableDiff.type_changed || []).map(c => row(
            '<span class="badge bg-warning text-dark">Type changed</span>',
            c.name,
            `${escapeHtml(c.old_type || '')} &rarr; ${escapeHtml(c.new_type || '')}`
        ));
        const unchangedCount = (tableDiff.unchanged || []).length;

        return `
            <div class="mb-3">
                <h6 class="mb-2"><i class="bi bi-table me-1"></i><code>${escapeHtml(table)}</code></h6>
                <table class="table table-sm table-borderless mb-1">
                    <tbody>${[...removed, ...typeChanged, ...added].join('')}</tbody>
                </table>
                <div class="small text-muted ms-1">${unchangedCount} column(s) unchanged</div>
            </div>
        `;
    }).join('');
}

/**
 * Ask the user to review a metadata refresh before it is persisted.
 * The refresh result lives only in the browser until /domain/metadata/save is
 * called, so declining simply discards it and leaves stored metadata intact.
 * @param {Object} diff - table name -> column diff
 * @returns {Promise<boolean>}
 */
function confirmMetadataDiff(diff) {
    const tableCount = Object.keys(diff).length;
    const changeCount = Object.values(diff).reduce((total, d) => total
        + (d.added || []).length
        + (d.removed || []).length
        + (d.type_changed || []).length, 0);
    const droppedCount = Object.values(diff).reduce(
        (total, d) => total + (d.removed || []).length, 0
    );

    return showConfirmDialog({
        title: 'Review Metadata Changes',
        size: 'modal-lg',
        icon: 'arrow-repeat',
        messageHtml: `
            Unity Catalog reports ${changeCount} column change(s) across
            ${tableCount} table(s).
            ${droppedCount > 0 ? `
                <div class="alert alert-warning mt-3 mb-0">
                    <i class="bi bi-exclamation-triangle me-2"></i>
                    ${droppedCount} column(s) no longer exist upstream. Any mapping
                    bound to them will break — run Mapping diagnostics after applying.
                </div>
            ` : ''}
        `,
        detailHtml: buildColumnDiffHtml(diff),
        confirmText: 'Apply Changes',
        confirmClass: 'btn-primary',
        cancelText: 'Discard'
    });
}

/**
 * Monitor a metadata async task until completion
 */
async function monitorMetadataTask(taskId, taskType, btn, progressDiv, statusSpan) {
    const pollInterval = 1500;
    
    while (true) {
        try {
            await new Promise(resolve => setTimeout(resolve, pollInterval));
            
            const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
            const data = await response.json();
            
            if (!data.success) {
                throw new Error('Task not found');
            }
            
            const task = data.task;
            
            // Update progress UI
            if (statusSpan && task.message) {
                statusSpan.textContent = task.message;
            }
            
            if (task.status === 'completed') {
                sessionStorage.removeItem(taskType === 'load' ? METADATA_LOAD_TASK_KEY : METADATA_UPDATE_TASK_KEY);
                
                if (progressDiv) progressDiv.classList.add('d-none');
                if (btn) btn.disabled = false;
                
                // A refresh that changed columns must be reviewed before it is
                // persisted — the merge is a full overwrite with no undo.
                const diff = task.result?.diff || {};
                if (Object.keys(diff).length && !await confirmMetadataDiff(diff)) {
                    showNotification('Metadata changes discarded', 'info');
                    await loadMetadataStatus({ listOverlay: true, overlayText: 'Reloading assets…' });
                    if (typeof refreshTasks === 'function') refreshTasks();
                    break;
                }
                
                // Save the updated metadata to the session
                if (task.result && task.result.metadata) {
                    try {
                        const tables = task.result.metadata.tables || [];
                        const saveResp = await fetch('/domain/metadata/save', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ tables }),
                            credentials: 'same-origin'
                        });
                        const saveData = await saveResp.json();
                        if (!saveData.success) {
                            console.warn('[Metadata] Save warning:', saveData.message);
                        }
                    } catch (saveErr) {
                        console.error('[Metadata] Save error:', saveErr);
                    }
                }
                
                showNotification(task.message || 'Data assets operation completed!', 'success');
                
                // Reload the assets list with a spinner, then rights in the background
                await loadMetadataStatus({
                    listOverlay: true,
                    overlayText: 'Reloading assets…',
                    refreshRights: true,
                });
                
                // Refresh task tracker
                if (typeof refreshTasks === 'function') refreshTasks();
                break;
                
            } else if (task.status === 'failed') {
                sessionStorage.removeItem(taskType === 'load' ? METADATA_LOAD_TASK_KEY : METADATA_UPDATE_TASK_KEY);
                
                if (progressDiv) progressDiv.classList.add('d-none');
                if (btn) btn.disabled = false;
                setMetadataListOverlay(false);
                
                showNotification('Failed: ' + (task.error || 'Unknown error'), 'error');
                if (typeof refreshTasks === 'function') refreshTasks();
                break;
                
            } else if (task.status === 'cancelled') {
                sessionStorage.removeItem(taskType === 'load' ? METADATA_LOAD_TASK_KEY : METADATA_UPDATE_TASK_KEY);
                
                if (progressDiv) progressDiv.classList.add('d-none');
                if (btn) btn.disabled = false;
                setMetadataListOverlay(false);
                
                showNotification('Operation was cancelled', 'warning');
                if (typeof refreshTasks === 'function') refreshTasks();
                break;
            }
        } catch (error) {
            console.error('[Metadata] Monitoring error:', error);
            sessionStorage.removeItem(taskType === 'load' ? METADATA_LOAD_TASK_KEY : METADATA_UPDATE_TASK_KEY);
            
            if (progressDiv) progressDiv.classList.add('d-none');
            if (btn) btn.disabled = false;
            setMetadataListOverlay(false);
            
            showNotification('Error monitoring task', 'error');
            break;
        }
    }
}

function displayMetadataPreview(metadata) {
    const previewDiv = document.getElementById('metadataPreview');
    const tbody = document.getElementById('metadataTablesBody');
    
    if (!metadata || !metadata.tables || metadata.tables.length === 0) {
        previewDiv.classList.add('d-none');
        return;
    }
    
    // Reset selections: all unchecked by default (checked = marked for removal)
    tableSelections = {};
    metadata.tables.forEach(table => {
        tableSelections[table.name] = false;
    });
    
    let html = '';
    metadata.tables.forEach((table, index) => {
        const columnCount = table.columns?.length || 0;
        const tableComment = table.comment || table.description || '';
        const isMarked = tableSelections[table.name] === true;
        const fullName = table.full_name || table.name;
        const parts = fullName.split('.');
        const displayName = parts.length === 3 ? parts[2] : fullName;
        const dataSource = parts.length >= 2 ? `${parts[0]}.${parts[1]}` : '';
        const selectCell = renderSelectPermCell(table);
        
        html += `
            <tr class="${isMarked ? 'table-danger' : ''}" data-table-index="${index}">
                <td>
                    <input type="checkbox" class="form-check-input table-checkbox" 
                           data-table="${table.name}" 
                           ${isMarked ? 'checked' : ''}>
                </td>
                <td class="meta-cursor-pointer" data-meta-action="table-details" data-table-index="${index}" title="${fullName}">
                    <i class="bi ${objectKindMeta(table.object_kind).icon} ${objectKindMeta(table.object_kind).iconClass} me-1"></i>
                    <strong>${displayName}</strong>
                    ${objectKindMeta(table.object_kind).badge}
                </td>
                <td class="meta-cursor-pointer" data-meta-action="open-ds-modal" data-table-index="${index}" title="Click to change data asset">
                    ${dataSource
                        ? `<code>${dataSource}</code>`
                        : '<span class="text-muted fst-italic">Click to set...</span>'}
                </td>
                <td class="meta-cursor-pointer" data-meta-action="table-details" data-table-index="${index}">
                    <span class="badge bg-secondary">${columnCount}</span>
                </td>
                <td class="text-center dm-rights-cell" data-table="${escapeHtml(table.name)}">${selectCell}</td>
                <td class="table-comment-cell meta-cursor-pointer" data-meta-action="table-details" data-table-index="${index}">
                    <span class="table-comment-text" id="tableComment_${index}">${tableComment || '<span class="text-muted fst-italic">Click to add description...</span>'}</span>
                </td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html || '<tr><td colspan="6" class="text-center text-muted">No tables found</td></tr>';
    previewDiv.classList.remove('d-none');
    
    updateSelectionCount();
    updateSelectAllCheckbox();
}

function renderSelectPermCell(table) {
    // can_select is filled asynchronously by refreshSelectPermissionsInBackground
    if (table.can_select === undefined || table.can_select === null) {
        return '<span class="text-muted" title="SELECT rights not checked yet">'
            + '<i class="bi bi-dash-circle"></i></span>';
    }
    if (table.can_select) {
        return '<span class="text-success" title="Service principal can SELECT this table">'
            + '<i class="bi bi-check-circle-fill"></i></span>';
    }
    const reason = (table.select_error || 'SELECT not permitted')
        .toString()
        .replace(/"/g, '&quot;');
    return `<span class="text-danger" title="${reason}"><i class="bi bi-x-circle-fill"></i></span>`;
}

function toggleTableSelection(tableName, isChecked) {
    tableSelections[tableName] = isChecked;
    
    // Update row styling: checked = marked for removal (red)
    const checkbox = document.querySelector(`input[data-table="${tableName}"]`);
    if (checkbox) {
        const row = checkbox.closest('tr');
        if (isChecked) {
            row.classList.add('table-danger');
        } else {
            row.classList.remove('table-danger');
        }
    }
    
    updateSelectionCount();
    updateSelectAllCheckbox();
}

function toggleAllTables(isChecked) {
    Object.keys(tableSelections).forEach(tableName => {
        tableSelections[tableName] = isChecked;
    });
    
    // Update all checkboxes: checked = marked for removal
    document.querySelectorAll('.table-checkbox').forEach(checkbox => {
        checkbox.checked = isChecked;
        const row = checkbox.closest('tr');
        if (isChecked) {
            row.classList.add('table-danger');
        } else {
            row.classList.remove('table-danger');
        }
    });
    
    updateSelectionCount();
}

function updateSelectionCount() {
    const markedCount = Object.values(tableSelections).filter(v => v).length;
    const totalCount = Object.keys(tableSelections).length;
    
    const countEl = document.getElementById('tableSelectionCount');
    const removeBtn = document.getElementById('removeTablesBtn');
    
    if (countEl) {
        countEl.textContent = `${totalCount} table${totalCount !== 1 ? 's' : ''}`;
    }
    
    // Show/hide remove button based on how many tables are checked for removal
    if (removeBtn) {
        if (markedCount > 0) {
            removeBtn.classList.remove('d-none');
            removeBtn.innerHTML = `<i class="bi bi-trash"></i> Remove ${markedCount} Selected Table${markedCount !== 1 ? 's' : ''}`;
        } else {
            removeBtn.classList.add('d-none');
        }
    }
}

function updateSelectAllCheckbox() {
    const allChecked = Object.values(tableSelections).every(v => v);
    const someChecked = Object.values(tableSelections).some(v => v);
    const checkbox = document.getElementById('selectAllTablesCheckbox');
    
    if (checkbox) {
        checkbox.checked = allChecked;
        checkbox.indeterminate = someChecked && !allChecked;
    }
}

/**
 * Ask the backend which entity/relationship mappings still reference the tables
 * about to be removed. Returns {} when nothing is affected, or the removal check
 * itself fails — a failed pre-flight must not block the removal.
 * @param {string[]} identifiers - full_name (or name) of each table being removed
 * @returns {Promise<Object<string, string[]>>} table identifier -> referrer labels
 */
async function fetchRemovalImpact(identifiers) {
    if (!identifiers.length) return {};
    try {
        const response = await fetch('/domain/metadata/removal-impact', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ table_names: identifiers }),
            credentials: 'same-origin'
        });
        const data = await response.json();
        return data.success ? (data.impact || {}) : {};
    } catch (error) {
        console.warn('Removal impact check failed, proceeding without detail', error);
        return {};
    }
}

/**
 * Build the affected-mappings detail block for the deletion-guard dialog.
 * @param {Object<string, string[]>} impact - from fetchRemovalImpact()
 * @returns {string} HTML
 */
function buildRemovalImpactHtml(impact) {
    return Object.entries(impact).map(([table, referrers]) => `
        <div class="mb-3">
            <h6 class="text-danger mb-1">
                <i class="bi bi-database-x me-1"></i><code>${escapeHtml(table)}</code>
            </h6>
            <ul class="list-unstyled ms-3 small mb-0">
                ${referrers.map(r => `
                    <li><i class="bi bi-exclamation-circle text-warning me-1"></i>${escapeHtml(r)}</li>
                `).join('')}
            </ul>
        </div>
    `).join('');
}

/**
 * Confirm a data-source removal, listing the mappings it would orphan.
 * Falls back to a plain confirm when nothing references the removed tables.
 * @param {string[]} identifiers - full_name (or name) of each table being removed
 * @param {Object} plain - showConfirmDialog options used when there is no impact
 * @returns {Promise<boolean>}
 */
async function confirmRemovalWithImpact(identifiers, plain) {
    const impact = await fetchRemovalImpact(identifiers);
    const affectedTables = Object.keys(impact).length;
    if (!affectedTables) return showConfirmDialog(plain);

    const mappingCount = new Set(Object.values(impact).flat()).size;
    return showConfirmDialog({
        ...plain,
        title: 'Data Assets Still In Use',
        size: 'modal-lg',
        headerClass: 'bg-danger text-white',
        icon: 'exclamation-triangle',
        messageHtml: `
            ${escapeHtml(plain.message)}
            <div class="alert alert-danger mt-3 mb-0">
                <i class="bi bi-exclamation-triangle me-2"></i>
                <strong>Warning:</strong> ${affectedTables} of these data asset(s)
                are still mapped by ${mappingCount} entity/relationship mapping(s).
                Removing them leaves those mappings pointing at a missing table,
                which will fail at build time. Generated R2RML/SQL will be cleared.
            </div>
        `,
        detailHtml: buildRemovalImpactHtml(impact),
        confirmText: 'Remove Anyway'
    });
}

async function removeSelectedTables() {
    if (!metadataCache) {
        showNotification('No data assets loaded', 'warning');
        return;
    }
    
    // Checked tables = marked for removal; unchecked = kept
    const tablesToRemove = (metadataCache.tables || []).filter(table => 
        tableSelections[table.name] === true
    );
    const tablesToKeep = (metadataCache.tables || []).filter(table => 
        tableSelections[table.name] !== true
    );
    const toRemoveCount = tablesToRemove.length;
    
    if (toRemoveCount === 0) {
        showNotification('No tables selected for removal', 'info');
        return;
    }
    
    // Confirm removal - special message if removing all, with an affected-mappings
    // guard when the doomed tables are still referenced by the assignment.
    const isRemovingAll = tablesToKeep.length === 0;
    const confirmed = await confirmRemovalWithImpact(
        tablesToRemove.map(t => t.full_name || t.name),
        {
            title: isRemovingAll ? 'Clear All Data Assets' : 'Remove Tables',
            message: isRemovingAll
                ? 'Are you sure you want to remove all tables? This will clear the data assets.'
                : `Are you sure you want to remove ${toRemoveCount} table${toRemoveCount !== 1 ? 's' : ''} from the data assets?`,
            confirmText: isRemovingAll ? 'Clear All' : 'Remove',
            confirmClass: 'btn-danger',
            icon: 'trash'
        }
    );
    
    if (!confirmed) return;
    
    try {
        // If removing all, use clear endpoint; otherwise save remaining tables
        if (isRemovingAll) {
            const response = await fetch('/domain/metadata/clear', {
                method: 'POST',
                credentials: 'same-origin'
            });
            
            const data = await response.json();
            
            if (data.success) {
                showNotification('Data assets cleared', 'success');
                metadataCache = null;
                tableSelections = {};
                await loadMetadataStatus();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } else {
            const response = await fetch('/domain/metadata/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    tables: tablesToKeep
                }),
                credentials: 'same-origin'
            });
            
            const data = await response.json();
            
            if (data.success) {
                showNotification(`Removed ${toRemoveCount} table${toRemoveCount !== 1 ? 's' : ''}. ${tablesToKeep.length} remaining.`, 'success');
                
                // Update cache with remaining tables
                metadataCache.tables = tablesToKeep;
                metadataCache.table_count = tablesToKeep.length;
                
                // Refresh display (resets checkboxes)
                displayMetadataPreview(metadataCache);
                await loadMetadataStatus();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

async function saveMetadataChanges(silent = false) {
    if (!metadataCache) {
        if (!silent) showNotification('No data assets to save', 'warning');
        return;
    }
    
    // Save all tables (including any comment/description changes)
    const allTables = metadataCache.tables || [];
    
    if (allTables.length === 0) {
        if (!silent) showNotification('No tables to save', 'warning');
        return;
    }
    
    try {
        const response = await fetch('/domain/metadata/save', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                tables: allTables
            }),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            if (!silent) showNotification(`Saved data assets for ${allTables.length} tables`, 'success');
            updateMetadataGauges(metadataCache);
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

function showTableDetails(tableIndex) {
    if (!metadataCache || !metadataCache.tables || !metadataCache.tables[tableIndex]) {
        return;
    }
    
    currentEditingTableIndex = tableIndex;
    const table = metadataCache.tables[tableIndex];
    
    const fullName = table.full_name || table.name;
    const parts = fullName.split('.');
    const displayName = parts.length === 3 ? parts[2] : fullName;
    document.getElementById('tableDetailsName').textContent = displayName;
    document.getElementById('tableDescriptionInput').value = table.comment || table.description || '';
    
    const tbody = document.getElementById('tableDetailsBody');
    let html = '';
    
    (table.columns || []).forEach((col, colIndex) => {
        const colName = col.col_name || col.name || '';
        const colType = col.data_type || col.type || '';
        const colComment = col.comment || '';
        
        html += `
            <tr>
                <td><code>${colName}</code></td>
                <td><span class="badge bg-info">${colType}</span></td>
                <td>
                    <input type="text" class="form-control form-control-sm column-comment-input" 
                           data-col-index="${colIndex}"
                           value="${colComment.replace(/"/g, '&quot;')}" 
                           placeholder="Enter column description...">
                </td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html || '<tr><td colspan="3" class="text-center text-muted">No columns found</td></tr>';
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('tableDetailsModal'));
    modal.show();
}

async function saveTableDetails() {
    if (currentEditingTableIndex === null || !metadataCache || !metadataCache.tables) {
        return;
    }
    
    const table = metadataCache.tables[currentEditingTableIndex];
    
    // Save table description
    const tableDescription = document.getElementById('tableDescriptionInput').value.trim();
    table.comment = tableDescription;
    table.description = tableDescription;
    
    // Save column comments
    const columnInputs = document.querySelectorAll('.column-comment-input');
    columnInputs.forEach(input => {
        const colIndex = parseInt(input.dataset.colIndex);
        if (table.columns && table.columns[colIndex]) {
            table.columns[colIndex].comment = input.value.trim();
        }
    });
    
    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('tableDetailsModal'));
    modal.hide();
    
    // Refresh the display
    displayMetadataPreview(metadataCache);
    checkMetadataDescriptions(metadataCache);

    // Persist immediately — the modal Save button is the only save affordance.
    await saveMetadataChanges(true);
    showNotification('Table details saved', 'success', 2000);
}

async function clearMetadata() {
    const allTables = (metadataCache?.tables || []).map(t => t.full_name || t.name);
    const confirmed = await confirmRemovalWithImpact(allTables, {
        title: 'Clear Data Assets',
        message: 'Are you sure you want to clear the loaded data assets?',
        confirmText: 'Clear',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
    
    if (!confirmed) return;
    
    try {
        const response = await fetch('/domain/metadata/clear', {
            method: 'POST',
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            metadataCache = null;
            tableSelections = {};
            currentEditingTableIndex = null;
            showNotification('Data assets cleared', 'success');
            await loadMetadataStatus();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

let changeDataSourceModal = null;
let dsWidgetInitialized = false;
let dsEditingTableIndex = null;

async function openChangeDataSourceModal(tableIndex) {
    if (!metadataCache || !metadataCache.tables || !metadataCache.tables[tableIndex]) {
        return;
    }
    
    dsEditingTableIndex = tableIndex;
    const table = metadataCache.tables[tableIndex];
    const fullName = table.full_name || table.name;
    const parts = fullName.split('.');
    const currentCatalog = parts.length >= 2 ? parts[0] : '';
    const currentSchema = parts.length >= 2 ? parts[1] : '';
    
    document.getElementById('dsModalTableName').textContent = table.name;
    document.getElementById('dsApplyToAll').checked = false;
    
    if (!changeDataSourceModal) {
        changeDataSourceModal = new bootstrap.Modal(document.getElementById('changeDataSourceModal'));
    }
    
    if (!dsWidgetInitialized) {
        await UCLocationWidget.init('changeDataSourceWidget', {
            label: '',
            showLabel: false,
            autoLoadDomain: false,
            onSelect: () => {}
        });
        dsWidgetInitialized = true;
    }
    
    if (currentCatalog && currentSchema) {
        UCLocationWidget.setValue('changeDataSourceWidget', currentCatalog, currentSchema);
    }
    
    changeDataSourceModal.show();
}

async function confirmDataSourceChange() {
    const location = UCLocationWidget.getValue('changeDataSourceWidget');
    const catalog = location.catalog;
    const schema = location.schema;
    
    if (!catalog || !schema) {
        showNotification('Please select a catalog and schema', 'warning');
        return;
    }
    
    const applyAll = document.getElementById('dsApplyToAll').checked;
    const table = metadataCache.tables[dsEditingTableIndex];
    
    if (changeDataSourceModal) {
        changeDataSourceModal.hide();
    }
    
    try {
        const response = await fetch('/domain/metadata/update-table-location', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                table_name: table.name,
                catalog: catalog,
                schema: schema,
                apply_all: applyAll
            }),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(data.message, 'success');
            await loadMetadataStatus();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

async function updateMappingsFromMetadata() {
    if (!metadataCache || !metadataCache.tables || metadataCache.tables.length === 0) {
        showNotification('No data assets loaded', 'warning');
        return;
    }
    
    const confirmed = await showConfirmDialog({
        title: 'Update Mappings',
        message: 'This will update entity and relationship mappings with the catalog.schema from each data asset. Continue?',
        confirmText: 'Update Mappings',
        confirmClass: 'btn-success',
        icon: 'arrow-right-circle'
    });
    
    if (!confirmed) return;
    
    const btn = document.getElementById('updateMappingsBtn');
    const originalHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Updating...';
    
    try {
        const response = await fetch('/domain/metadata/update-mappings', {
            method: 'POST',
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            const persisted = await doDomainSave();
            if (persisted) {
                showNotification(`${data.message}. Changes saved to registry.`, 'success');
            }
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = originalHtml;
    }
}

async function updateMetadataFromUC() {
    console.log('[Metadata] Update from UC called');
    
    if (!metadataCache || !metadataCache.tables || metadataCache.tables.length === 0) {
        showNotification('No data assets loaded to update', 'warning');
        return;
    }
    
    const allTableNames = (metadataCache.tables || []).map(t => t.name);
    const selectedNames = allTableNames.filter(n => tableSelections[n] === true);
    const tablesToUpdate = selectedNames.length > 0 ? selectedNames : allTableNames;
    const isSelectionUpdate = selectedNames.length > 0;
    
    if (tablesToUpdate.length === 0) {
        showNotification('No tables to update', 'warning');
        return;
    }
    
    const updateBtn = document.getElementById('updateMetadataBtn');
    const originalHtml = updateBtn.innerHTML;
    updateBtn.disabled = true;
    updateBtn.innerHTML = '<i class="bi bi-hourglass-split"></i> Updating...';
    setMetadataListOverlay(true, 'Updating assets…');
    
    try {
        // Start async task
        const response = await fetch('/domain/metadata/update-async', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ table_names: tablesToUpdate }),
            credentials: 'same-origin'
        });
        
        const startResult = await response.json();
        
        if (!startResult.success) {
            updateBtn.disabled = false;
            updateBtn.innerHTML = originalHtml;
            setMetadataListOverlay(false);
            showNotification('Error: ' + startResult.message, 'error');
            return;
        }
        
        const taskId = startResult.task_id;
        sessionStorage.setItem(METADATA_UPDATE_TASK_KEY, taskId);
        
        showNotification(
            isSelectionUpdate
                ? `Updating ${tablesToUpdate.length} selected table(s)...`
                : `Updating all ${tablesToUpdate.length} table(s)...`,
            'info'
        );
        
        // Trigger refresh of task tracker
        if (typeof refreshTasks === 'function') refreshTasks();
        
        // Monitor the task - use a simple status span
        const statusSpan = document.createElement('span');
        updateBtn.parentElement.appendChild(statusSpan);
        
        await monitorMetadataTask(taskId, 'update', updateBtn, null, statusSpan);
        
        // Restore button
        updateBtn.innerHTML = originalHtml;
        
        // Clean up temp status span
        if (statusSpan.parentElement) statusSpan.remove();
        
    } catch (error) {
        console.error('[Metadata] Update error:', error);
        updateBtn.disabled = false;
        updateBtn.innerHTML = originalHtml;
        setMetadataListOverlay(false);
        showNotification('Error: ' + error.message, 'error');
    }
}

/**
 * Replaces inline onclick/onchange/oninput from _domain_metadata.html
 * (entire partial is rendered inside #metadata-section, including modals).
 */
(function setupDomainMetadataTemplateBindings() {
    let bound = false;
    function bind() {
        if (bound) return;
        const root = document.getElementById('metadata-section');
        if (!root) return;
        bound = true;

        root.addEventListener('click', function (e) {
            const t = e.target.closest('[data-meta-action]');
            if (!t || !root.contains(t)) return;
            const act = t.getAttribute('data-meta-action');
            if (act === 'clear-metadata') clearMetadata();
            else if (act === 'show-load-modal') showLoadMetadataModal();
            else if (act === 'remove-selected-tables') removeSelectedTables();
            else if (act === 'update-mappings') updateMappingsFromMetadata();
            else if (act === 'update-from-uc') updateMetadataFromUC();
            else if (act === 'import-select-all') selectAllImportTables(t.getAttribute('data-meta-checked') === '1');
            else if (act === 'import-new-only') selectNewTablesOnly();
            else if (act === 'import-selected') importSelectedTables();
            else if (act === 'save-table-details') saveTableDetails();
            else if (act === 'add-assets-refresh') refreshAddDataAssets();
            else if (act === 'confirm-ds-change') confirmDataSourceChange();
            else if (act === 'table-details') showTableDetails(parseInt(t.getAttribute('data-table-index'), 10));
            else if (act === 'open-ds-modal') openChangeDataSourceModal(parseInt(t.getAttribute('data-table-index'), 10));
        });

        root.addEventListener('change', function (e) {
            const el = e.target;
            if (!root.contains(el)) return;
            if (el.id === 'addDataAssetsCatalog') onAddDataAssetsCatalogChange();
            else if (el.id === 'addDataAssetsSchema') onAddDataAssetsSchemaChange();
            else if (el.id === 'selectAllTablesCheckbox') toggleAllTables(el.checked);
            else if (el.id === 'selectAllImportCheckbox') selectAllImportTables(el.checked);
            else if (el.classList.contains('table-checkbox') && el.dataset.table) {
                toggleTableSelection(el.dataset.table, el.checked);
            } else if (el.classList.contains('import-table-checkbox') && el.dataset.table) {
                toggleImportTableSelection(el.dataset.table, el.checked);
            }
        });

        const tsf = document.getElementById('tableSearchFilter');
        if (tsf) tsf.addEventListener('input', function () { filterImportTables(tsf.value); });
    }
    document.addEventListener('DOMContentLoaded', bind);
})();

function checkMetadataDescriptions(metadata) {
    const warningEl = document.getElementById('metadataDescriptionWarning');
    if (!warningEl) return;

    const tables = metadata?.tables || [];
    if (tables.length === 0) {
        warningEl.style.display = 'none';
        return;
    }

    const tablesNoDesc = [];
    let columnsNoDescCount = 0;

    for (const t of tables) {
        const tName = t.full_name || t.name || '?';
        if (!t.comment || !t.comment.trim()) {
            tablesNoDesc.push(tName);
        }
        for (const col of (t.columns || [])) {
            if (!col.comment || !col.comment.trim()) {
                columnsNoDescCount++;
            }
        }
    }

    const totalMissing = tablesNoDesc.length + columnsNoDescCount;
    if (totalMissing === 0) {
        warningEl.style.display = 'none';
        return;
    }

    const collapseId = 'metaDescQualityDetails';
    const tableRows = tablesNoDesc.map(t =>
        `<li><i class="bi bi-table text-muted me-1"></i><code>${t}</code> — no table description</li>`
    ).join('');

    warningEl.innerHTML = `
        <div class="alert alert-warning mb-0 py-2">
            <div class="d-flex align-items-start gap-2">
                <i class="bi bi-exclamation-triangle-fill mt-1 flex-shrink-0"></i>
                <div class="flex-grow-1">
                    <strong>${tablesNoDesc.length} table(s) and ${columnsNoDescCount} column(s) are missing descriptions.</strong>
                    <span class="ms-1 text-muted small">Adding descriptions improves AI mapping accuracy.</span>
                    ${tablesNoDesc.length > 0 ? `
                    <button class="btn btn-link btn-sm py-0 px-1 ms-1 text-warning-emphasis text-decoration-none"
                            type="button" data-bs-toggle="collapse" data-bs-target="#${collapseId}">
                        <i class="bi bi-chevron-down" style="font-size:0.75rem;"></i> Show tables
                    </button>
                    <div class="collapse mt-2" id="${collapseId}">
                        <ul class="list-unstyled mb-0 small" style="max-height:160px;overflow-y:auto;">
                            ${tableRows}
                        </ul>
                    </div>` : ''}
                </div>
            </div>
        </div>`;
    warningEl.style.display = 'block';
}
