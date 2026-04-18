/**
 * OntoBricks - ontology-wizard.js
 * Ontology Wizard - AI-powered ontology generation from metadata
 */

// =====================================================
// WIZARD STATE
// =====================================================

let wizardMetadataCache = null;
let wizardGeneratedOWL = null;
let wizardSelectedTables = new Set();
let wizardSelectedDocs = new Set();
let wizardDocsCache = [];
let wizardCurrentTaskId = null;  // Track running task
let wizardNotificationShown = false;  // Prevent duplicate notifications

// Session storage keys for persisting state across navigation
const WIZARD_TASK_KEY = 'ontobricks_wizard_task';
const WIZARD_OWL_KEY = 'ontobricks_wizard_owl';
const WIZARD_STATS_KEY = 'ontobricks_wizard_stats';

// =====================================================
// WIZARD INITIALIZATION
// =====================================================

/**
 * Initialize the wizard section
 */
async function initOntologyWizard() {
    console.log('[Wizard] Initializing...');
    
    // Check if there's a running task from previous session
    const savedTaskId = sessionStorage.getItem(WIZARD_TASK_KEY);
    if (savedTaskId) {
        console.log('[Wizard] Found saved task:', savedTaskId);
        await checkAndResumeTask(savedTaskId);
    } else {
        // No running task - check for previously generated OWL
        const savedOWL = sessionStorage.getItem(WIZARD_OWL_KEY);
        if (savedOWL) {
            console.log('[Wizard] Restoring previously generated OWL');
            let stats = {};
            try {
                stats = JSON.parse(sessionStorage.getItem(WIZARD_STATS_KEY) || '{}');
            } catch (e) { /* ignore parse errors */ }
            showWizardResults({ owl_content: savedOWL, stats: stats });
        }
    }
    
    await loadWizardMetadata();
    await loadWizardDocuments();
    await loadWizardTemplatesFromServer();
}

/**
 * Check if a saved task is still running and resume monitoring
 */
async function checkAndResumeTask(taskId) {
    try {
        const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
        const data = await response.json();
        
        if (!data.success) {
            // Task not found, clear storage
            sessionStorage.removeItem(WIZARD_TASK_KEY);
            return;
        }
        
        const task = data.task;
        
        if (task.status === 'running' || task.status === 'pending') {
            // Task still running, resume monitoring
            console.log('[Wizard] Resuming task monitoring:', taskId);
            wizardCurrentTaskId = taskId;
            disableWizardForm(true);
            showWizardTaskProgress(task);
            
            // Continue polling
            monitorWizardTask(taskId);
        } else if (task.status === 'completed' && task.result) {
            // Task completed, show results
            console.log('[Wizard] Task completed, showing results');
            sessionStorage.removeItem(WIZARD_TASK_KEY);
            showWizardResults(task.result);
        } else if (task.status === 'failed') {
            // Task failed
            sessionStorage.removeItem(WIZARD_TASK_KEY);
            showNotification('Previous generation failed: ' + (task.error || 'Unknown error'), 'error');
        } else {
            // Task cancelled or other status
            sessionStorage.removeItem(WIZARD_TASK_KEY);
        }
    } catch (error) {
        console.error('[Wizard] Error checking task:', error);
        sessionStorage.removeItem(WIZARD_TASK_KEY);
    }
}

/**
 * Disable/enable the wizard form
 */
function disableWizardForm(disabled) {
    const form = document.getElementById('wizard-section');
    if (!form) return;
    
    // Disable all inputs and buttons
    const inputs = form.querySelectorAll('input, textarea, button, select');
    inputs.forEach(input => {
        if (disabled) {
            input.setAttribute('data-was-disabled', input.disabled);
            input.disabled = true;
        } else {
            const wasDisabled = input.getAttribute('data-was-disabled') === 'true';
            input.disabled = wasDisabled;
            input.removeAttribute('data-was-disabled');
        }
    });
    
    // Add/remove overlay
    let overlay = document.getElementById('wizardFormOverlay');
    if (disabled) {
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'wizardFormOverlay';
            overlay.className = 'wizard-form-overlay';
            overlay.innerHTML = `
                <div class="text-center" style="max-width: 400px;">
                    <div class="ob-loading-spinner">
                        <svg class="ob-spinner-svg" viewBox="0 0 80 80" fill="none">
                            <g class="ob-ring">
                                <g stroke="#CBD5E1" stroke-width="1.2" opacity="0.5">
                                    <line x1="40" y1="10" x2="61" y2="19"/><line x1="61" y1="19" x2="70" y2="40"/>
                                    <line x1="70" y1="40" x2="61" y2="61"/><line x1="61" y1="61" x2="40" y2="70"/>
                                    <line x1="40" y1="70" x2="19" y2="61"/><line x1="19" y1="61" x2="10" y2="40"/>
                                    <line x1="10" y1="40" x2="19" y2="19"/><line x1="19" y1="19" x2="40" y2="10"/>
                                </g>
                                <circle cx="40" cy="10" r="5" fill="#FF3621"/><circle cx="61" cy="19" r="5" fill="#6366F1"/>
                                <circle cx="70" cy="40" r="5" fill="#4ECDC4"/><circle cx="61" cy="61" r="5" fill="#F59E0B"/>
                                <circle cx="40" cy="70" r="5" fill="#FF3621"/><circle cx="19" cy="61" r="5" fill="#6366F1"/>
                                <circle cx="10" cy="40" r="5" fill="#4ECDC4"/><circle cx="19" cy="19" r="5" fill="#F59E0B"/>
                            </g>
                            <g transform="translate(40,40)">
                                <g class="ob-center">
                                    <path d="M0-12 L10-6 L0 0 L-10-6Z" fill="#FF3621"/>
                                    <path d="M0-5 L10 1 L0 7 L-10 1Z" fill="#FF3621" opacity="0.85"/>
                                    <path d="M0 2 L10 8 L0 14 L-10 8Z" fill="#FF3621" opacity="0.7"/>
                                </g>
                            </g>
                        </svg>
                        <span class="ob-spinner-label" id="wizardOverlayTitle">Generating ontology...</span>
                    </div>
                    <p id="wizardOverlayMessage" class="text-muted mt-2 mb-3 small">Your ontology is being generated...</p>
                    <div class="progress mb-2" style="height: 6px; max-width: 300px; margin: 0 auto;">
                        <div id="wizardOverlayProgress" class="progress-bar progress-bar-striped progress-bar-animated" style="width: 0%"></div>
                    </div>
                    <small id="wizardOverlayStep" class="text-muted"></small>
                </div>
            `;
            form.style.position = 'relative';
            form.appendChild(overlay);
        }
        overlay.style.display = 'flex';
    } else {
        if (overlay) {
            overlay.style.display = 'none';
        }
    }
}

/**
 * Show task progress in the overlay
 */
function showWizardTaskProgress(task) {
    const progressBar = document.getElementById('wizardOverlayProgress');
    const stepEl = document.getElementById('wizardOverlayStep');
    const messageEl = document.getElementById('wizardOverlayMessage');
    
    if (progressBar) {
        progressBar.style.width = task.progress + '%';
    }
    if (stepEl && task.steps && task.current_step < task.steps.length) {
        stepEl.textContent = task.steps[task.current_step].description;
    }
    if (messageEl) {
        messageEl.textContent = task.message || 'Processing...';
    }
}

/**
 * Monitor a wizard task until completion
 */
async function monitorWizardTask(taskId) {
    const pollInterval = 1500;
    
    while (true) {
        try {
            await sleep(pollInterval);
            
            const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
            const data = await response.json();
            
            if (!data.success) {
                throw new Error('Task not found');
            }
            
            const task = data.task;
            showWizardTaskProgress(task);
            
            if (task.status === 'completed') {
                sessionStorage.removeItem(WIZARD_TASK_KEY);
                wizardCurrentTaskId = null;
                disableWizardForm(false);
                
                if (task.result) {
                    wizardNotificationShown = true;
                    showWizardResults(task.result);
                }
                break;
            } else if (task.status === 'failed') {
                sessionStorage.removeItem(WIZARD_TASK_KEY);
                wizardCurrentTaskId = null;
                disableWizardForm(false);
                showNotification('Generation failed: ' + (task.error || 'Unknown error'), 'error');
                break;
            } else if (task.status === 'cancelled') {
                sessionStorage.removeItem(WIZARD_TASK_KEY);
                wizardCurrentTaskId = null;
                disableWizardForm(false);
                showNotification('Generation was cancelled', 'warning');
                break;
            }
        } catch (error) {
            console.error('[Wizard] Monitoring error:', error);
            sessionStorage.removeItem(WIZARD_TASK_KEY);
            wizardCurrentTaskId = null;
            disableWizardForm(false);
            showNotification('Error monitoring task', 'error');
            break;
        }
    }
    
    // Refresh task tracker
    if (typeof refreshTasks === 'function') {
        refreshTasks();
    }
}

/**
 * Show generation results and automatically apply the ontology (no user action required)
 */
async function showWizardResults(result) {
    wizardGeneratedOWL = result.owl_content;
    
    // Persist OWL and stats to sessionStorage so they survive page navigation
    try {
        sessionStorage.setItem(WIZARD_OWL_KEY, result.owl_content);
        sessionStorage.setItem(WIZARD_STATS_KEY, JSON.stringify(result.stats || {}));
    } catch (e) {
        console.warn('[Wizard] Could not persist OWL to sessionStorage:', e);
    }
    
    // Show preview briefly (stats only, OWL available for copy/download)
    document.getElementById('wizardOWLPreview').value = result.owl_content;
    
    const stats = result.stats || {};
    const agentSteps = result.agent_steps || [];
    const agentIter = result.agent_iterations || 0;

    let statsHtml = `
        <div class="d-flex gap-4 text-muted flex-wrap">
            <span><i class="bi bi-box me-1"></i><strong>${stats.classes || 0}</strong> entities</span>
            <span><i class="bi bi-arrow-left-right me-1"></i><strong>${stats.properties || 0}</strong> relationships</span>
            <span><i class="bi bi-list me-1"></i><strong>${stats.dataProperties || 0}</strong> attributes</span>`;
    if (agentIter) {
        statsHtml += `<span class="ms-auto"><i class="bi bi-robot me-1"></i>${agentIter} agent steps</span>`;
    }
    statsHtml += `</div>`;

    if (agentSteps.length) {
        statsHtml += renderAgentStepsLog(agentSteps);
    }

    document.getElementById('wizardPreviewStats').innerHTML = statsHtml;
    document.getElementById('wizardPreviewCard').style.display = 'block';
    
    const applied = await applyWizardOntologySilent();
    if (applied) {
        clearWizardPreview();
        if (typeof refreshOntologyStatus === 'function') {
            refreshOntologyStatus();
        }
        if (typeof loadOntologyFromSession === 'function') {
            await loadOntologyFromSession();
        }
        if (typeof SidebarNav !== 'undefined' && typeof SidebarNav.goTo === 'function') {
            SidebarNav.goTo('map');
        }
        showNotification('Ontology created successfully!', 'success');
    } else {
        // On failure, keep preview visible so user can retry via Apply button
        document.getElementById('wizardPreviewCard').scrollIntoView({ behavior: 'smooth' });
    }
}

/**
 * Apply the generated OWL to the domain ontology (no confirmation dialog)
 * @returns {Promise<boolean>} true if applied successfully
 */
async function applyWizardOntologySilent() {
    if (!wizardGeneratedOWL) return false;
    try {
        const response = await fetch('/ontology/parse-owl', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ content: wizardGeneratedOWL }),
            credentials: 'same-origin'
        });
        const result = await response.json();
        if (result.success) {
            return true;
        }
        showNotification('Error applying ontology: ' + (result.message || 'Unknown error'), 'error');
        return false;
    } catch (error) {
        console.error('[Wizard] Apply error:', error);
        showNotification('Error applying ontology: ' + error.message, 'error');
        return false;
    }
}

/**
 * Load metadata for the wizard
 */
async function loadWizardMetadata() {
    const statusEl = document.getElementById('wizardMetadataStatus');
    const previewEl = document.getElementById('wizardMetadataPreview');
    const noMetadataEl = document.getElementById('wizardNoMetadata');
    const tableBody = document.getElementById('wizardMetadataTableBody');
    
    try {
        const response = await fetch('/domain/metadata', { credentials: 'same-origin' });
        const result = await response.json();
        
        if (result.success && result.metadata && result.metadata.tables && result.metadata.tables.length > 0) {
            wizardMetadataCache = result.metadata;
            
            // Initialize selection - all tables selected by default
            wizardSelectedTables.clear();
            result.metadata.tables.forEach(table => {
                const tableName = table.full_name || table.name;
                wizardSelectedTables.add(tableName);
            });
            
            // Show stats
            const tableCount = result.metadata.tables.length;
            let totalColumns = 0;
            result.metadata.tables.forEach(t => {
                totalColumns += (t.columns || []).length;
            });
            
            statusEl.innerHTML = `
                <div class="d-flex align-items-center">
                    <i class="bi bi-check-circle-fill text-success me-2 fs-5"></i>
                    <div>
                        <strong>${tableCount} tables</strong> available with <strong>${totalColumns} columns</strong>
                    </div>
                </div>
            `;
            
            // Populate preview table with checkboxes
            tableBody.innerHTML = '';
            result.metadata.tables.forEach((table, index) => {
                const columnCount = (table.columns || []).length;
                const description = table.comment || table.description || '';
                const tableName = table.full_name || table.name;
                const displayName = tableName.split('.').pop(); // Get just the table name for display
                
                tableBody.innerHTML += `
                    <tr>
                        <td class="text-center">
                            <input type="checkbox" class="form-check-input wizard-table-checkbox" 
                                   data-table="${tableName}" id="wizardTable${index}" checked>
                        </td>
                        <td>
                            <label for="wizardTable${index}" class="mb-0 cursor-pointer">
                                <strong>${displayName}</strong>
                                <br><small class="text-muted">${tableName}</small>
                            </label>
                        </td>
                        <td class="text-center">${columnCount}</td>
                        <td class="small">${description || '<span class="text-muted">-</span>'}</td>
                    </tr>
                `;
            });
            
            previewEl.style.display = 'block';
            noMetadataEl.style.display = 'none';
            
            // Update selection count
            updateWizardSelectionCount();
            
            // Enable generate button
            const genBtn = document.getElementById('wizardTopGenerateBtn');
            if (genBtn) genBtn.disabled = false;
        } else {
            statusEl.innerHTML = `
                <div class="d-flex align-items-center text-muted">
                    <i class="bi bi-info-circle me-2 fs-5"></i>
                    <span>No data sources loaded — you can still generate from documents or guidelines</span>
                </div>
            `;
            previewEl.style.display = 'none';
            noMetadataEl.style.display = 'block';
        }
    } catch (error) {
        console.error('[Wizard] Error loading metadata:', error);
        statusEl.innerHTML = `
            <div class="d-flex align-items-center text-danger">
                <i class="bi bi-x-circle-fill me-2 fs-5"></i>
                <span>Error loading data sources: ${error.message}</span>
            </div>
        `;
        noMetadataEl.style.display = 'block';
    }
}

// =====================================================
// TABLE SELECTION
// =====================================================

/**
 * Update selection when a table checkbox is toggled
 */
function updateWizardTableSelection(checkbox) {
    const tableName = checkbox.dataset.table;
    if (checkbox.checked) {
        wizardSelectedTables.add(tableName);
    } else {
        wizardSelectedTables.delete(tableName);
    }
    updateWizardSelectAllCheckbox();
    updateWizardSelectionCount();
}

/**
 * Select or deselect all tables
 */
function selectAllWizardTables(selectAll) {
    const checkboxes = document.querySelectorAll('.wizard-table-checkbox');
    checkboxes.forEach(cb => {
        cb.checked = selectAll;
        const tableName = cb.dataset.table;
        if (selectAll) {
            wizardSelectedTables.add(tableName);
        } else {
            wizardSelectedTables.delete(tableName);
        }
    });
    updateWizardSelectAllCheckbox();
    updateWizardSelectionCount();
}

/**
 * Sync the header "select all" checkbox with current row state.
 * Checked when all selected, unchecked when none, indeterminate when partial.
 */
function updateWizardSelectAllCheckbox() {
    const headerCb = document.getElementById('wizardSelectAllCheckbox');
    if (!headerCb) return;
    const total = document.querySelectorAll('.wizard-table-checkbox').length;
    const checked = wizardSelectedTables.size;
    headerCb.checked = total > 0 && checked === total;
    headerCb.indeterminate = checked > 0 && checked < total;
}

/**
 * Update the selection count display
 */
function updateWizardSelectionCount() {
    const countEl = document.getElementById('wizardSelectedCount');
    const total = wizardMetadataCache?.tables?.length || 0;
    const selected = wizardSelectedTables.size;
    
    if (countEl) {
        countEl.innerHTML = `<i class="bi bi-check2-square me-1"></i><strong>${selected}</strong> of ${total} tables selected for generation`;
    }
}

/**
 * Get selected metadata (only selected tables)
 */
function getSelectedMetadata() {
    if (!wizardMetadataCache) return null;
    
    const selectedTables = wizardMetadataCache.tables.filter(table => {
        const tableName = table.full_name || table.name;
        return wizardSelectedTables.has(tableName);
    });
    
    return {
        ...wizardMetadataCache,
        tables: selectedTables
    };
}

// =====================================================
// TEMPLATE LOADING  (fetched from backend global_config)
// =====================================================

let wizardTemplates = {};

/**
 * Fetch wizard quick-templates from the backend and render buttons.
 */
async function loadWizardTemplatesFromServer() {
    try {
        const response = await fetch('/ontology/wizard/templates', { credentials: 'same-origin' });
        const data = await response.json();
        if (data.success && data.templates) {
            wizardTemplates = data.templates;
            renderWizardTemplateButtons(wizardTemplates);
        }
    } catch (error) {
        console.error('[Wizard] Failed to load templates:', error);
    }
}

/**
 * Render template buttons dynamically into the Quick Templates container.
 */
function renderWizardTemplateButtons(templates) {
    const container = document.getElementById('wizardTemplateButtons');
    if (!container) return;
    container.innerHTML = '';
    for (const [key, tpl] of Object.entries(templates)) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'btn btn-outline-secondary';
        btn.innerHTML = `<i class="bi bi-${tpl.icon || 'file-text'} me-1"></i>${tpl.label || key}`;
        btn.addEventListener('click', () => loadWizardTemplate(key));
        container.appendChild(btn);
    }
}

/**
 * Load a predefined template into the guidelines textarea
 */
function loadWizardTemplate(templateName) {
    const textarea = document.getElementById('wizardGuidelines');
    const tpl = wizardTemplates[templateName];
    if (tpl) {
        textarea.value = tpl.guidelines || '';
        showNotification(`Loaded ${(tpl.label || templateName).toUpperCase()} template`, 'info', 2000);
    }
}

// =====================================================
// ONTOLOGY GENERATION
// =====================================================

/**
 * Generate ontology from wizard inputs (async version)
 */
async function generateOntologyFromWizard() {
    const selectedMetadata = getSelectedMetadata();
    const guidelines = document.getElementById('wizardGuidelines').value.trim();
    const documents = getSelectedDocumentNames();

    const hasMetadata = selectedMetadata && selectedMetadata.tables && selectedMetadata.tables.length > 0;
    const hasGuidelines = guidelines.length > 0;
    const hasDocs = documents.length > 0;

    if (!hasMetadata && !hasGuidelines && !hasDocs) {
        showNotification('Please provide at least data sources, documents, or guidelines', 'warning');
        return;
    }

    const options = {
        includeDataProperties: document.getElementById('wizardIncludeDataProps').checked,
        includeRelationships: document.getElementById('wizardIncludeRelationships').checked,
        includeInheritance: document.getElementById('wizardIncludeInheritance').checked,
        useTableNames: document.getElementById('wizardUseTableNames').checked,
        useColumnComments: document.getElementById('wizardUseColumnComments').checked
    };
    
    try {
        // Start async task
        const response = await fetch('/ontology/wizard/generate-async', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                metadata: hasMetadata ? selectedMetadata : {},
                guidelines: guidelines,
                options: options,
                documents: documents
            }),
            credentials: 'same-origin'
        });
        
        const startResult = await response.json();
        
        if (!startResult.success) {
            showNotification('Error: ' + startResult.message, 'error');
            return;
        }
        
        const taskId = startResult.task_id;
        console.log('[Wizard] Task started:', taskId);
        
        // Clear any previously persisted OWL
        sessionStorage.removeItem(WIZARD_OWL_KEY);
        sessionStorage.removeItem(WIZARD_STATS_KEY);
        wizardGeneratedOWL = null;
        
        // Save task ID to session storage (persist across page navigation)
        sessionStorage.setItem(WIZARD_TASK_KEY, taskId);
        wizardCurrentTaskId = taskId;
        wizardNotificationShown = false;  // Reset notification flag for new task
        
        // Disable the form and show progress overlay
        disableWizardForm(true);
        
        // Trigger refresh of task tracker
        if (typeof refreshTasks === 'function') {
            refreshTasks();
        }
        
        showNotification('Ontology generation started. You can navigate away and come back.', 'info');
        
        // Start monitoring the task
        monitorWizardTask(taskId);
        
    } catch (error) {
        console.error('[Wizard] Generation error:', error);
        showNotification('Error starting generation: ' + error.message, 'error');
    }
}

/**
 * Helper function for delays
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// =====================================================
// PREVIEW ACTIONS
// =====================================================

/**
 * Copy generated OWL to clipboard
 */
function copyWizardOWL() {
    const owlContent = document.getElementById('wizardOWLPreview').value;
    if (owlContent) {
        navigator.clipboard.writeText(owlContent).then(() => {
            showNotification('OWL content copied to clipboard', 'success', 2000);
        }).catch(err => {
            showNotification('Failed to copy: ' + err.message, 'error');
        });
    }
}

/**
 * Download generated OWL as file
 */
function downloadWizardOWL() {
    const owlContent = document.getElementById('wizardOWLPreview').value;
    if (!owlContent) return;
    
    const blob = new Blob([owlContent], { type: 'text/turtle' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = 'generated_ontology.ttl';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showNotification('OWL file downloaded', 'success', 2000);
}

/**
 * Clear the preview
 */
function clearWizardPreview() {
    document.getElementById('wizardPreviewCard').style.display = 'none';
    document.getElementById('wizardOWLPreview').value = '';
    wizardGeneratedOWL = null;
    
    // Clear persisted OWL from sessionStorage
    sessionStorage.removeItem(WIZARD_OWL_KEY);
    sessionStorage.removeItem(WIZARD_STATS_KEY);
}

/**
 * Apply the generated ontology (import it)
 */
async function applyWizardOntology() {
    if (!wizardGeneratedOWL) {
        showNotification('No ontology to apply', 'warning');
        return;
    }
    
    // Confirm before applying (will replace existing ontology)
    const confirmed = await showConfirmDialog({
        title: 'Apply Generated Ontology',
        message: 'This will replace your current ontology with the generated one. Continue?',
        confirmText: 'Apply',
        confirmClass: 'btn-success',
        icon: 'check-lg'
    });
    
    if (!confirmed) return;
    
    try {
        // Use the existing parse-owl endpoint to import
        const response = await fetch('/ontology/parse-owl', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ content: wizardGeneratedOWL }),
            credentials: 'same-origin'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showNotification('Ontology applied successfully!', 'success');
            
            // Clear the preview
            clearWizardPreview();
            
            // Refresh ontology status
            if (typeof refreshOntologyStatus === 'function') {
                refreshOntologyStatus();
            }
            
            // Reload ontology state
            if (typeof loadOntologyFromSession === 'function') {
                await loadOntologyFromSession();
            }
            
            // Navigate to Map view to see the result
            if (typeof SidebarNav !== 'undefined' && typeof SidebarNav.goTo === 'function') {
                SidebarNav.goTo('map');
            }
        } else {
            showNotification('Error applying ontology: ' + result.message, 'error');
        }
    } catch (error) {
        console.error('[Wizard] Apply error:', error);
        showNotification('Error applying ontology: ' + error.message, 'error');
    }
}

// =====================================================
// DOCUMENTS SELECTION
// =====================================================

/**
 * Load documents from the domain volume.
 */
async function loadWizardDocuments() {
    const statusEl = document.getElementById('wizardDocsStatus');
    const previewEl = document.getElementById('wizardDocsPreview');
    const noDocsEl = document.getElementById('wizardNoDocs');

    try {
        const response = await fetch('/domain/documents/list', { credentials: 'same-origin' });
        const result = await response.json();

        if (result.success && result.files && result.files.length > 0) {
            wizardDocsCache = result.files;
            wizardSelectedDocs.clear();
            // Select all by default
            wizardDocsCache.forEach(f => wizardSelectedDocs.add(f.name));

            statusEl.innerHTML = `
                <div class="d-flex align-items-center">
                    <i class="bi bi-check-circle-fill text-success me-2 fs-5"></i>
                    <div><strong>${wizardDocsCache.length} document${wizardDocsCache.length > 1 ? 's' : ''}</strong> available</div>
                </div>`;
            renderWizardDocsList();
            previewEl.style.display = '';
            if (noDocsEl) noDocsEl.style.display = 'none';
        } else {
            statusEl.innerHTML = '';
            if (previewEl) previewEl.style.display = 'none';
            if (noDocsEl) noDocsEl.style.display = '';
        }
    } catch (err) {
        console.warn('[Wizard] Could not load documents:', err);
        statusEl.innerHTML = '';
        if (previewEl) previewEl.style.display = 'none';
        if (noDocsEl) noDocsEl.style.display = '';
    }
}

function renderWizardDocsList() {
    const container = document.getElementById('wizardDocsList');
    if (!container) return;

    const iconMap = {
        pdf: 'bi-file-earmark-pdf text-danger',
        doc: 'bi-file-earmark-word text-primary',
        docx: 'bi-file-earmark-word text-primary',
        xls: 'bi-file-earmark-excel text-success',
        xlsx: 'bi-file-earmark-excel text-success',
        csv: 'bi-file-earmark-spreadsheet text-success',
        txt: 'bi-file-earmark-text text-secondary',
        json: 'bi-file-earmark-code text-warning',
        xml: 'bi-file-earmark-code text-warning',
    };

    let html = '<div class="list-group list-group-flush">';
    wizardDocsCache.forEach((file, idx) => {
        const ext = (file.name.split('.').pop() || '').toLowerCase();
        const iconCls = iconMap[ext] || 'bi-file-earmark text-muted';
        const checked = wizardSelectedDocs.has(file.name) ? 'checked' : '';
        const size = file.size != null ? formatDocSize(file.size) : '';
        const docNameAttr = encodeURIComponent(file.name);
        html += `
            <div class="list-group-item list-group-item-action d-flex align-items-center py-2">
                <input type="checkbox" class="form-check-input me-3 wizard-doc-checkbox"
                       value="${file.name}" ${checked}>
                <i class="bi ${iconCls} me-2"></i>
                <span class="text-truncate flex-grow-1">${file.name}</span>
                ${size ? `<span class="small text-muted ms-2">${size}</span>` : ''}
                <button class="btn btn-sm btn-outline-primary py-0 px-1 ms-2" type="button"
                        data-action="wizard-doc-preview" data-doc-name="${docNameAttr}" title="Preview">
                    <i class="bi bi-eye"></i>
                </button>
            </div>`;
    });
    html += '</div>';
    container.innerHTML = html;
    updateWizardDocsCount();
}

function updateWizardDocSelection(checkbox) {
    if (checkbox.checked) {
        wizardSelectedDocs.add(checkbox.value);
    } else {
        wizardSelectedDocs.delete(checkbox.value);
    }
    updateWizardDocsCount();
}

function selectAllWizardDocs(selectAll) {
    wizardSelectedDocs.clear();
    if (selectAll) {
        wizardDocsCache.forEach(f => wizardSelectedDocs.add(f.name));
    }
    document.querySelectorAll('.wizard-doc-checkbox').forEach(cb => {
        cb.checked = selectAll;
    });
    updateWizardDocsCount();
}

function updateWizardDocsCount() {
    const el = document.getElementById('wizardDocsSelectedCount');
    if (!el) return;
    const total = wizardDocsCache.length;
    const selected = wizardSelectedDocs.size;
    el.textContent = `${selected} of ${total} document${total !== 1 ? 's' : ''} selected`;
}

function getSelectedDocumentNames() {
    return Array.from(wizardSelectedDocs);
}

function formatDocSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

// =====================================================
// AGENT STEPS LOG
// =====================================================

function renderAgentStepsLog(steps) {
    if (!steps || !steps.length) return '';

    const iconFor = {
        tool_call: 'bi-wrench text-primary',
        tool_result: 'bi-arrow-return-right text-success',
        output: 'bi-file-earmark-code text-dark',
    };

    let rows = steps.map(s => {
        const icon = iconFor[s.type] || 'bi-dot';
        const label = s.type === 'tool_call'
            ? `<strong>${s.tool}</strong>(${s.content || ''})`
            : s.type === 'tool_result'
                ? `<span class="text-muted">${s.tool} → ${_truncate(s.content, 80)}</span>`
                : `<em>Output produced</em>`;
        const dur = s.ms ? `<span class="text-muted">${s.ms}ms</span>` : '';
        return `<div class="d-flex align-items-start gap-2 py-1" style="font-size:0.82rem;">
                    <i class="bi ${icon}" style="margin-top:2px;"></i>
                    <span class="flex-grow-1 text-truncate">${label}</span>
                    ${dur}
                 </div>`;
    });

    return `
        <details class="mt-2">
            <summary class="small text-muted" style="cursor:pointer;">
                <i class="bi bi-robot me-1"></i>Agent activity log (${steps.length} steps)
            </summary>
            <div class="border rounded p-2 mt-1" style="max-height:180px; overflow-y:auto; background:#fafafa;">
                ${rows.join('')}
            </div>
        </details>`;
}

function _truncate(str, max) { return truncate(str, max); }

// =====================================================
// EXPOSE GLOBALLY
// =====================================================

window.initOntologyWizard = initOntologyWizard;
window.loadWizardTemplate = loadWizardTemplate;
window.generateOntologyFromWizard = generateOntologyFromWizard;
window.copyWizardOWL = copyWizardOWL;
window.downloadWizardOWL = downloadWizardOWL;
window.clearWizardPreview = clearWizardPreview;
window.applyWizardOntology = applyWizardOntology;
window.updateWizardTableSelection = updateWizardTableSelection;
window.selectAllWizardTables = selectAllWizardTables;
window.updateWizardDocSelection = updateWizardDocSelection;
window.selectAllWizardDocs = selectAllWizardDocs;

/**
 * Wizard UI: data-action click delegation and checkbox change handling (no inline handlers).
 */
(function initWizardActionDelegation() {
    function bind() {
        const root = document.getElementById('wizard-section');
        if (!root || root.dataset.wizardActionsBound === '1') return;
        root.dataset.wizardActionsBound = '1';

        root.addEventListener('click', function (e) {
            const el = e.target.closest('[data-action]');
            if (!el || !root.contains(el)) return;
            const action = el.dataset.action;
            switch (action) {
                case 'wizard-generate':
                    generateOntologyFromWizard();
                    break;
                case 'wizard-tables-bulk':
                    selectAllWizardTables(el.dataset.selectAll === 'true');
                    break;
                case 'wizard-docs-bulk':
                    selectAllWizardDocs(el.dataset.selectAll === 'true');
                    break;
                case 'wizard-copy-owl':
                    copyWizardOWL();
                    break;
                case 'wizard-download-owl':
                    downloadWizardOWL();
                    break;
                case 'wizard-discard-preview':
                    clearWizardPreview();
                    break;
                case 'wizard-apply-ontology':
                    applyWizardOntology();
                    break;
                case 'wizard-doc-preview': {
                    e.stopPropagation();
                    const raw = el.getAttribute('data-doc-name') || '';
                    let name = '';
                    try {
                        name = decodeURIComponent(raw);
                    } catch (err) {
                        name = raw;
                    }
                    if (name && typeof DocumentPreview !== 'undefined' && DocumentPreview.open) {
                        DocumentPreview.open(name);
                    }
                    break;
                }
                default:
                    break;
            }
        });

        root.addEventListener('change', function (e) {
            const t = e.target;
            if (t.id === 'wizardSelectAllCheckbox') {
                selectAllWizardTables(!!t.checked);
                return;
            }
            if (t.classList && t.classList.contains('wizard-table-checkbox')) {
                updateWizardTableSelection(t);
                return;
            }
            if (t.classList && t.classList.contains('wizard-doc-checkbox')) {
                updateWizardDocSelection(t);
            }
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bind);
    } else {
        bind();
    }
})();
