/**
 * OntoBricks - domain-metadata.js
 * Extracted from domain templates per code_instructions.txt
 */

let tableSelections = {}; // Track selected tables by name
let currentEditingTableIndex = null; // Track which table is being edited in the modal
let importTableSelections = {}; // Track tables selected for import in the modal
let allAvailableTables = []; // All tables available in UC schema
let loadMetadataModal = null; // Bootstrap modal instance
let loadMetadataWidgetInitialized = false; // Track if widget is initialized
let pendingLoadCatalog = ''; // Catalog selected in the load metadata modal
let pendingLoadSchema = ''; // Schema selected in the load metadata modal

// Async task tracking
const METADATA_LOAD_TASK_KEY = 'ontobricks_metadata_load_task';
const METADATA_UPDATE_TASK_KEY = 'ontobricks_metadata_update_task';

// Initialize metadata section
async function initMetadataSection() {
    await loadMetadataStatus();
}

// Show the load metadata location modal
async function showLoadMetadataModal() {
    // Initialize modal if not already
    if (!loadMetadataModal) {
        loadMetadataModal = new bootstrap.Modal(document.getElementById('loadMetadataLocationModal'));
    }
    
    // Initialize widget inside modal if not already done
    if (!loadMetadataWidgetInitialized) {
        await UCLocationWidget.init('loadMetadataLocationWidget', {
            label: 'Catalog.Schema',
            onSelect: (catalog, schema) => {
                console.log('[Metadata] Location selected:', catalog, schema);
            }
        });
        loadMetadataWidgetInitialized = true;
    }
    
    loadMetadataModal.show();
}

async function loadMetadataStatus() {
    const statusDiv = document.getElementById('metadataStatus');
    const statusText = document.getElementById('metadataStatusText');
    const previewDiv = document.getElementById('metadataPreview');
    
    statusDiv.className = 'alert alert-secondary mb-4';
    statusText.innerHTML = '<span class="spinner-border spinner-border-sm me-2" role="status"></span>Loading data sources…';
    
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
            
            statusDiv.className = 'alert alert-success mb-4';
            statusText.innerHTML = `
                <strong>Data sources loaded:</strong> 
                ${locationDisplay ? `<strong>${locationDisplay}</strong> - ` : ''}
                ${tableCount} table${tableCount !== 1 ? 's' : ''}
            `;
            
            // Initialize table selections (all selected by default)
            tableSelections = {};
            (data.metadata.tables || []).forEach(table => {
                tableSelections[table.name] = true;
            });
            
            // Show preview and Update Mappings button
            displayMetadataPreview(data.metadata);
            const updateMappingsBtn = document.getElementById('updateMappingsBtn');
            if (updateMappingsBtn) updateMappingsBtn.style.display = 'inline-block';
        } else {
            metadataCache = null;
            tableSelections = {};
            statusDiv.className = 'alert alert-secondary mb-4';
            statusText.innerHTML = '<i class="bi bi-info-circle"></i> No data sources loaded';
            previewDiv.style.display = 'none';
            const updateMappingsBtn = document.getElementById('updateMappingsBtn');
            if (updateMappingsBtn) updateMappingsBtn.style.display = 'none';
        }
    } catch (error) {
        console.error('Error loading metadata status:', error);
        statusDiv.className = 'alert alert-warning mb-4';
        statusText.innerHTML = '<i class="bi bi-exclamation-triangle"></i> Error loading data sources status';
    }
}

async function loadMetadataFromUC() {
    // Get values from the modal's UC Location widget
    const location = UCLocationWidget.getValue('loadMetadataLocationWidget');
    const catalog = location.catalog;
    const schema = location.schema;
    
    if (!catalog || !schema) {
        showNotification('Please select catalog and schema first', 'warning');
        return;
    }
    
    // Store for later use in importSelectedTables
    pendingLoadCatalog = catalog;
    pendingLoadSchema = schema;
    
    // Close the location modal
    if (loadMetadataModal) {
        loadMetadataModal.hide();
    }
    
    const loadBtn = document.getElementById('loadMetadataBtn');
    const progressDiv = document.getElementById('metadataProgress');
    const statusSpan = document.getElementById('metadataInitStatus');
    
    loadBtn.disabled = true;
    progressDiv.style.display = 'block';
    statusSpan.textContent = 'Fetching table list from Unity Catalog...';
    
    try {
        // Step 1: Get list of tables (lightweight)
        const response = await fetch('/domain/metadata/list-tables', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ catalog, schema }),
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        progressDiv.style.display = 'none';
        loadBtn.disabled = false;
        statusSpan.textContent = '';
        
        if (data.success) {
            // Store available tables
            allAvailableTables = data.tables || [];
            
            if (allAvailableTables.length === 0) {
                showNotification(`No tables found in ${catalog}.${schema}`, 'warning');
                return;
            }
            
            // Initialize import selections (select all new tables by default)
            importTableSelections = {};
            allAvailableTables.forEach(table => {
                // Pre-select tables that are not already loaded
                importTableSelections[table.name] = !table.already_loaded;
            });
            
            // Show the selection modal
            showTableSelectionModal(catalog, schema);
        } else {
            showNotification('Error: ' + (data.message || 'Failed to list tables'), 'error');
        }
        
    } catch (error) {
        progressDiv.style.display = 'none';
        loadBtn.disabled = false;
        statusSpan.textContent = '';
        showNotification('Error: ' + error.message, 'error');
    }
}

function showTableSelectionModal(catalog, schema) {
    const tbody = document.getElementById('tableSelectionBody');
    const infoSpan = document.getElementById('tableSelectionInfo');
    
    const newCount = allAvailableTables.filter(t => !t.already_loaded).length;
    const existingCount = allAvailableTables.filter(t => t.already_loaded).length;
    
    infoSpan.innerHTML = `<code>${catalog}.${schema}</code> - ${allAvailableTables.length} table(s) found`;
    if (existingCount > 0) {
        infoSpan.innerHTML += ` <span class="badge bg-info">${existingCount} already loaded</span>`;
    }
    
    // Build table rows
    let html = '';
    allAvailableTables.forEach(table => {
        const isSelected = importTableSelections[table.name] === true;
        const statusBadge = table.already_loaded 
            ? '<span class="badge bg-info">Already loaded</span>' 
            : '<span class="badge bg-success">New</span>';
        
        html += `
            <tr class="import-table-row ${table.already_loaded ? 'table-light' : ''}" data-table="${table.name}">
                <td>
                    <input type="checkbox" class="form-check-input import-table-checkbox" 
                           data-table="${table.name}" 
                           ${isSelected ? 'checked' : ''} 
                           onchange="toggleImportTableSelection('${table.name}', this.checked)">
                </td>
                <td>
                    <i class="bi bi-table text-primary me-1"></i>
                    <strong>${table.name}</strong>
                </td>
                <td>${statusBadge}</td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html || '<tr><td colspan="3" class="text-center text-muted">No tables found</td></tr>';
    
    updateImportSelectionCount();
    updateSelectAllImportCheckbox();
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('tableSelectionModal'));
    modal.show();
}

function toggleImportTableSelection(tableName, isSelected) {
    importTableSelections[tableName] = isSelected;
    updateImportSelectionCount();
    updateSelectAllImportCheckbox();
}

function selectAllImportTables(isSelected) {
    // Only affect visible (non-filtered) rows
    const visibleRows = document.querySelectorAll('.import-table-row:not([style*="display: none"])');
    visibleRows.forEach(row => {
        const tableName = row.dataset.table;
        importTableSelections[tableName] = isSelected;
        const checkbox = row.querySelector('.import-table-checkbox');
        if (checkbox) checkbox.checked = isSelected;
    });
    
    updateImportSelectionCount();
    updateSelectAllImportCheckbox();
}

function selectNewTablesOnly() {
    // Select only tables that are not already loaded
    allAvailableTables.forEach(table => {
        importTableSelections[table.name] = !table.already_loaded;
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
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });
}

function updateImportSelectionCount() {
    const selectedCount = Object.values(importTableSelections).filter(v => v).length;
    const totalCount = allAvailableTables.length;
    
    const countSpan = document.getElementById('selectedCountInfo');
    const importBtn = document.getElementById('importSelectedBtn');
    
    if (countSpan) {
        countSpan.textContent = `${selectedCount} of ${totalCount} tables selected`;
    }
    
    if (importBtn) {
        importBtn.disabled = selectedCount === 0;
        importBtn.innerHTML = `<i class="bi bi-download"></i> Import ${selectedCount} Table${selectedCount !== 1 ? 's' : ''}`;
    }
}

function updateSelectAllImportCheckbox() {
    const visibleRows = document.querySelectorAll('.import-table-row:not([style*="display: none"])');
    const visibleTableNames = Array.from(visibleRows).map(r => r.dataset.table);
    
    const allSelected = visibleTableNames.every(name => importTableSelections[name] === true);
    const someSelected = visibleTableNames.some(name => importTableSelections[name] === true);
    
    const checkbox = document.getElementById('selectAllImportCheckbox');
    if (checkbox) {
        checkbox.checked = allSelected;
        checkbox.indeterminate = someSelected && !allSelected;
    }
}

async function importSelectedTables() {
    const selectedTables = Object.entries(importTableSelections)
        .filter(([name, selected]) => selected)
        .map(([name]) => name);
    
    if (selectedTables.length === 0) {
        showNotification('No tables selected for import', 'warning');
        return;
    }
    
    // Use the catalog/schema stored when the load metadata modal was confirmed
    const catalog = pendingLoadCatalog;
    const schema = pendingLoadSchema;
    
    // Close the modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('tableSelectionModal'));
    modal.hide();
    
    const loadBtn = document.getElementById('loadMetadataBtn');
    const progressDiv = document.getElementById('metadataProgress');
    const statusSpan = document.getElementById('metadataInitStatus');
    
    loadBtn.disabled = true;
    progressDiv.style.display = 'block';
    statusSpan.textContent = `Loading data sources for ${selectedTables.length} table(s)...`;
    
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
            progressDiv.style.display = 'none';
            loadBtn.disabled = false;
            showNotification('Error: ' + startResult.message, 'error');
            return;
        }
        
        const taskId = startResult.task_id;
        sessionStorage.setItem(METADATA_LOAD_TASK_KEY, taskId);
        
        showNotification('Data sources loading started...', 'info');
        
        // Trigger refresh of task tracker
        if (typeof refreshTasks === 'function') refreshTasks();
        
        // Monitor the task
        await monitorMetadataTask(taskId, 'load', loadBtn, progressDiv, statusSpan);
        
    } catch (error) {
        progressDiv.style.display = 'none';
        loadBtn.disabled = false;
        showNotification('Error: ' + error.message, 'error');
    }
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
                
                if (progressDiv) progressDiv.style.display = 'none';
                if (btn) btn.disabled = false;
                
                // Auto-save the updated metadata to the session
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
                            console.warn('[Metadata] Auto-save warning:', saveData.message);
                        }
                    } catch (saveErr) {
                        console.error('[Metadata] Auto-save error:', saveErr);
                    }
                }
                
                showNotification(task.message || 'Data sources operation completed!', 'success');
                
                // Refresh metadata display
                await loadMetadataStatus();
                
                // Refresh task tracker
                if (typeof refreshTasks === 'function') refreshTasks();
                break;
                
            } else if (task.status === 'failed') {
                sessionStorage.removeItem(taskType === 'load' ? METADATA_LOAD_TASK_KEY : METADATA_UPDATE_TASK_KEY);
                
                if (progressDiv) progressDiv.style.display = 'none';
                if (btn) btn.disabled = false;
                
                showNotification('Failed: ' + (task.error || 'Unknown error'), 'error');
                if (typeof refreshTasks === 'function') refreshTasks();
                break;
                
            } else if (task.status === 'cancelled') {
                sessionStorage.removeItem(taskType === 'load' ? METADATA_LOAD_TASK_KEY : METADATA_UPDATE_TASK_KEY);
                
                if (progressDiv) progressDiv.style.display = 'none';
                if (btn) btn.disabled = false;
                
                showNotification('Operation was cancelled', 'warning');
                if (typeof refreshTasks === 'function') refreshTasks();
                break;
            }
        } catch (error) {
            console.error('[Metadata] Monitoring error:', error);
            sessionStorage.removeItem(taskType === 'load' ? METADATA_LOAD_TASK_KEY : METADATA_UPDATE_TASK_KEY);
            
            if (progressDiv) progressDiv.style.display = 'none';
            if (btn) btn.disabled = false;
            
            showNotification('Error monitoring task', 'error');
            break;
        }
    }
}

function displayMetadataPreview(metadata) {
    const previewDiv = document.getElementById('metadataPreview');
    const tbody = document.getElementById('metadataTablesBody');
    
    if (!metadata || !metadata.tables || metadata.tables.length === 0) {
        previewDiv.style.display = 'none';
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
        
        html += `
            <tr class="${isMarked ? 'table-danger' : ''}" data-table-index="${index}">
                <td onclick="event.stopPropagation();">
                    <input type="checkbox" class="form-check-input table-checkbox" 
                           data-table="${table.name}" 
                           ${isMarked ? 'checked' : ''} 
                           onchange="toggleTableSelection('${table.name}', this.checked)">
                </td>
                <td style="cursor: pointer;" onclick="showTableDetails(${index})" title="${fullName}">
                    <i class="bi bi-table text-primary me-1"></i>
                    <strong>${displayName}</strong>
                </td>
                <td style="cursor: pointer;" onclick="openChangeDataSourceModal(${index})" title="Click to change data source">
                    ${dataSource
                        ? `<code>${dataSource}</code>`
                        : '<span class="text-muted fst-italic">Click to set...</span>'}
                </td>
                <td style="cursor: pointer;" onclick="showTableDetails(${index})">
                    <span class="badge bg-secondary">${columnCount}</span>
                </td>
                <td class="table-comment-cell" onclick="editTableCommentInline(${index}, event)">
                    <span class="table-comment-text" id="tableComment_${index}">${tableComment || '<span class="text-muted fst-italic">Click to add description...</span>'}</span>
                </td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html || '<tr><td colspan="5" class="text-center text-muted">No tables found</td></tr>';
    previewDiv.style.display = 'block';
    
    updateSelectionCount();
    updateSelectAllCheckbox();
}

function editTableCommentInline(tableIndex, event) {
    event.stopPropagation();
    
    if (!metadataCache || !metadataCache.tables || !metadataCache.tables[tableIndex]) {
        return;
    }
    
    const table = metadataCache.tables[tableIndex];
    const cell = event.currentTarget;
    const currentComment = table.comment || table.description || '';
    
    // Replace cell content with input
    cell.innerHTML = `
        <input type="text" class="form-control form-control-sm" 
               value="${currentComment.replace(/"/g, '&quot;')}" 
               onblur="saveTableCommentInline(${tableIndex}, this.value)"
               onkeydown="if(event.key==='Enter'){this.blur();} if(event.key==='Escape'){cancelTableCommentEdit(${tableIndex});}"
               autofocus>
    `;
    
    const input = cell.querySelector('input');
    input.focus();
    input.select();
}

function saveTableCommentInline(tableIndex, newComment) {
    if (!metadataCache || !metadataCache.tables || !metadataCache.tables[tableIndex]) {
        return;
    }
    
    // Update the cache
    metadataCache.tables[tableIndex].comment = newComment;
    metadataCache.tables[tableIndex].description = newComment;
    
    // Update the display
    const commentSpan = document.getElementById(`tableComment_${tableIndex}`);
    const cell = commentSpan ? commentSpan.parentElement : document.querySelector(`tr[data-table-index="${tableIndex}"] .table-comment-cell`);
    
    if (cell) {
        cell.innerHTML = `<span class="table-comment-text" id="tableComment_${tableIndex}">${newComment || '<span class="text-muted fst-italic">Click to add description...</span>'}</span>`;
    }
    
    // Auto-save the comment change
    saveMetadataChanges(true);
}

function cancelTableCommentEdit(tableIndex) {
    displayMetadataPreview(metadataCache);
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
            removeBtn.style.display = 'inline-block';
            removeBtn.innerHTML = `<i class="bi bi-trash"></i> Remove ${markedCount} Selected Table${markedCount !== 1 ? 's' : ''}`;
        } else {
            removeBtn.style.display = 'none';
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

async function removeSelectedTables() {
    if (!metadataCache) {
        showNotification('No data sources loaded', 'warning');
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
    
    // Confirm removal - special message if removing all
    const isRemovingAll = tablesToKeep.length === 0;
    const confirmed = await showConfirmDialog({
        title: isRemovingAll ? 'Clear All Data Sources' : 'Remove Tables',
        message: isRemovingAll 
            ? 'Are you sure you want to remove all tables? This will clear the data sources.'
            : `Are you sure you want to remove ${toRemoveCount} table${toRemoveCount !== 1 ? 's' : ''} from the data sources?`,
        confirmText: isRemovingAll ? 'Clear All' : 'Remove',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
    
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
                showNotification('Data sources cleared', 'success');
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
        if (!silent) showNotification('No data sources to save', 'warning');
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
            if (!silent) showNotification(`Saved data sources for ${allTables.length} tables`, 'success');
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

function saveTableDetails() {
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
    
    showNotification('Table details updated. Click "Save Changes" to persist.', 'info', 2000);
}

async function clearMetadata() {
    const confirmed = await showConfirmDialog({
        title: 'Clear Data Sources',
        message: 'Are you sure you want to clear the loaded data sources?',
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
            showNotification('Data sources cleared', 'success');
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
        showNotification('No data sources loaded', 'warning');
        return;
    }
    
    const confirmed = await showConfirmDialog({
        title: 'Update Mappings',
        message: 'This will update entity and relationship mappings with the catalog.schema from each data source table. Continue?',
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
        
        btn.disabled = false;
        btn.innerHTML = originalHtml;
        
        if (data.success) {
            showNotification(data.message, 'success');
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        btn.disabled = false;
        btn.innerHTML = originalHtml;
        showNotification('Error: ' + error.message, 'error');
    }
}

async function updateMetadataFromUC() {
    console.log('[Metadata] Update from UC called');
    
    if (!metadataCache || !metadataCache.tables || metadataCache.tables.length === 0) {
        showNotification('No data sources loaded to update', 'warning');
        return;
    }
    
    // Update all loaded tables
    const tablesToUpdate = (metadataCache.tables || []).map(t => t.name);
    
    if (tablesToUpdate.length === 0) {
        showNotification('No tables to update', 'warning');
        return;
    }
    
    const updateBtn = document.getElementById('updateMetadataBtn');
    const originalHtml = updateBtn.innerHTML;
    updateBtn.disabled = true;
    updateBtn.innerHTML = '<i class="bi bi-hourglass-split"></i> Updating...';
    
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
            showNotification('Error: ' + startResult.message, 'error');
            return;
        }
        
        const taskId = startResult.task_id;
        sessionStorage.setItem(METADATA_UPDATE_TASK_KEY, taskId);
        
        showNotification(`Updating ${tablesToUpdate.length} table(s)...`, 'info');
        
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
        showNotification('Error: ' + error.message, 'error');
    }
}
