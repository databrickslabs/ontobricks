/**
 * OntoBricks - Auto-Map Module
 * Batch automatic mapping for all unassigned entities and relationships.
 * Uses async task processing for auto-mapping.
 */

// Session storage key for persisting task ID
const AUTO_ASSIGN_TASK_KEY = 'ontobricks_autoassign_task';

window.AutoAssignModule = {
    initialized: false,
    isRunning: false,
    isCancelled: false,
    results: [],
    currentTaskId: null,
    
    /**
     * Disable sidebar menus that should not be accessed while auto-map runs.
     */
    disableSidebarMenus: function() {
        document.querySelectorAll('.autoassign-disables').forEach(function(link) {
            link.classList.add('disabled');
            link.style.pointerEvents = 'none';
            link.style.opacity = '0.45';
            link.setAttribute('title', 'Auto-Map is running…');
        });
    },
    
    /**
     * Re-enable sidebar menus after auto-map finishes.
     */
    enableSidebarMenus: function() {
        document.querySelectorAll('.autoassign-disables').forEach(function(link) {
            link.classList.remove('disabled');
            link.style.pointerEvents = '';
            link.style.opacity = '';
            link.removeAttribute('title');
        });
    },
    
    /**
     * Refresh the mapping config from the server and update status cards.
     */
    refreshMappingConfig: async function() {
        try {
            const response = await fetch('/mapping/load', { credentials: 'same-origin' });
            const data = await response.json();
            if (data.success && data.config) {
                MappingState.config = data.config;
                console.log('[AutoAssign] Mapping config refreshed from server');
            }
        } catch (e) {
            console.error('[AutoAssign] Error refreshing config:', e);
        }
        this.updateStatus();
    },
    
    /**
     * Initialize the module
     */
    init: async function() {
        if (!MappingState.initialized || !MappingState.loadedOntology) {
            console.log('AutoAssignModule: Waiting for MappingState to be ready...');
            return;
        }
        
        // Always refresh mapping config from server to get current mapping state
        await this.refreshMappingConfig();
        
        // Check for running task from previous session
        const savedTaskId = sessionStorage.getItem(AUTO_ASSIGN_TASK_KEY);
        if (savedTaskId) {
            console.log('[AutoAssign] Found saved task:', savedTaskId);
            await this.checkAndResumeTask(savedTaskId);
        }
        
        this.initialized = true;
        this.updateStatus();
    },
    
    /**
     * Check if a saved task is still running and resume monitoring
     */
    checkAndResumeTask: async function(taskId) {
        try {
            const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
            const data = await response.json();
            
            if (!data.success) {
                sessionStorage.removeItem(AUTO_ASSIGN_TASK_KEY);
                return;
            }
            
            const task = data.task;
            
            if (task.status === 'running' || task.status === 'pending') {
                console.log('[AutoAssign] Resuming task monitoring:', taskId);
                this.currentTaskId = taskId;
                this.isRunning = true;
                this.disableSidebarMenus();
                this.showProgressUI();
                this.updateProgressFromTask(task);
                this.monitorTask(taskId);
            } else if (task.status === 'completed' && task.result) {
                console.log('[AutoAssign] Task completed, applying results');
                sessionStorage.removeItem(AUTO_ASSIGN_TASK_KEY);
                this.results = task.result.results || [];
                await this.saveMappingsFromTask(task.result);
                this.showReport();
                await this.refreshMappingConfig();
                this.enableSidebarMenus();
            } else if (task.status === 'failed') {
                sessionStorage.removeItem(AUTO_ASSIGN_TASK_KEY);
                showNotification('Previous auto-map failed: ' + (task.error || 'Unknown error'), 'error');
            } else {
                sessionStorage.removeItem(AUTO_ASSIGN_TASK_KEY);
            }
        } catch (error) {
            console.error('[AutoAssign] Error checking task:', error);
            sessionStorage.removeItem(AUTO_ASSIGN_TASK_KEY);
        }
    },
    
    /**
     * Filter to only get ObjectProperties (relationships), not DatatypeProperties (attributes)
     */
    filterObjectProperties: function(allProperties) {
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
    },
    
    /**
     * Update the status display showing unassigned counts
     */
    updateStatus: function() {
        const ontology = MappingState.loadedOntology || {};
        const allClasses = ontology.classes || [];
        const allProperties = this.filterObjectProperties(ontology.properties || []);
        
        // Filter out excluded entities and relationships
        const classes = allClasses.filter(c => !c.excluded);
        const excludedNames = new Set(allClasses.filter(c => c.excluded).map(c => c.name || c.localName));
        const properties = allProperties.filter(p =>
            !p.excluded && !excludedNames.has(p.domain) && !excludedNames.has(p.range)
        );
        
        const entityMappings = MappingState.config?.entities || [];
        const relationshipMappings = MappingState.config?.relationships || [];
        
        // Count unassigned entities (non-excluded only)
        const assignedEntityUris = new Set(entityMappings.filter(m => m.sql_query).map(m => m.ontology_class));
        const unassignedEntities = classes.filter(c => !assignedEntityUris.has(c.uri));
        
        // Count unassigned relationships (non-excluded only)
        const assignedRelUris = new Set(relationshipMappings.filter(m => m.sql_query).map(m => m.property));
        const unassignedRels = properties.filter(p => !assignedRelUris.has(p.uri));
        
        // Count entities with missing attributes (assigned but incomplete, non-excluded)
        const mappingByClass = {};
        entityMappings.forEach(m => {
            const uri = m.ontology_class || m.class_uri;
            if (uri) mappingByClass[uri] = m;
        });
        
        const entitiesWithMissingAttrs = classes.filter(cls => {
            if (!assignedEntityUris.has(cls.uri)) return false;
            const dataProps = cls.dataProperties || [];
            if (dataProps.length === 0) return false;
            const em = mappingByClass[cls.uri] || {};
            const attrMap = em.attribute_mappings || {};
            return dataProps.some(dp => {
                const name = dp.name || dp.localName || '';
                return name && !attrMap[name];
            });
        });
        
        // Update UI - show assigned / total (non-excluded)
        const assignedEntityCount = classes.length - unassignedEntities.length;
        const assignedRelCount = properties.length - unassignedRels.length;
        const entityCountEl = document.getElementById('autoAssignEntityCount');
        const relCountEl = document.getElementById('autoAssignRelCount');
        if (entityCountEl) {
            entityCountEl.textContent = `${assignedEntityCount} / ${classes.length}`;
            entityCountEl.className = assignedEntityCount === classes.length && classes.length > 0
                ? 'mb-1 text-success' : 'mb-1';
        }
        if (relCountEl) {
            relCountEl.textContent = `${assignedRelCount} / ${properties.length}`;
            relCountEl.className = assignedRelCount === properties.length && properties.length > 0
                ? 'mb-1 text-success' : 'mb-1';
        }
        
        // Update attribute card
        const attrCard = document.getElementById('autoAssignAttrCard');
        const attrCountEl = document.getElementById('autoAssignAttrCount');
        const reassignBtn = document.getElementById('reassignAttrsBtn');
        
        if (entitiesWithMissingAttrs.length > 0) {
            if (attrCard) attrCard.style.display = 'block';
            if (attrCountEl) attrCountEl.textContent = entitiesWithMissingAttrs.length;
            if (reassignBtn) reassignBtn.style.display = 'inline-block';
        } else {
            if (attrCard) attrCard.style.display = 'none';
            if (reassignBtn) reassignBtn.style.display = 'none';
        }
        
        const startBtn = document.getElementById('startAutoAssignBtn');
        if (startBtn) {
            const allAssigned = unassignedEntities.length === 0 && unassignedRels.length === 0;
            if (allAssigned && classes.length > 0) {
                startBtn.disabled = true;
                startBtn.innerHTML = '<i class="bi bi-check-circle me-2"></i> All Mapped';
                startBtn.classList.remove('btn-primary');
                startBtn.classList.add('btn-success');
            } else {
                startBtn.disabled = false;
                const count = unassignedEntities.length + unassignedRels.length;
                startBtn.innerHTML = `<i class="bi bi-lightning-charge me-2"></i> Auto-Map (${count})`;
                startBtn.classList.remove('btn-success');
                startBtn.classList.add('btn-primary');
            }
        }
    },
    
    /**
     * Show progress UI and disable the page with overlay
     */
    showProgressUI: function() {
        const progressSection = document.getElementById('autoAssignProgressSection');
        const reportSection = document.getElementById('autoAssignReportSection');
        const startBtn = document.getElementById('startAutoAssignBtn');
        const reassignBtn = document.getElementById('reassignAttrsBtn');
        const cancelBtn = document.getElementById('cancelAutoAssignBtn');
        
        if (progressSection) progressSection.style.display = 'block';
        if (reportSection) reportSection.style.display = 'none';
        if (startBtn) startBtn.style.display = 'none';
        if (reassignBtn) reassignBtn.style.display = 'none';
        if (cancelBtn) cancelBtn.style.display = 'inline-block';
    },
    
    /**
     * Hide progress UI and remove overlay
     */
    hideProgressUI: function() {
        const progressSection = document.getElementById('autoAssignProgressSection');
        const startBtn = document.getElementById('startAutoAssignBtn');
        const cancelBtn = document.getElementById('cancelAutoAssignBtn');
        
        if (progressSection) progressSection.style.display = 'none';
        if (startBtn) startBtn.style.display = 'inline-block';
        if (cancelBtn) cancelBtn.style.display = 'none';
    },
    
    /**
     * Update progress from task object (uses correct element IDs from HTML)
     */
    updateProgressFromTask: function(task) {
        const progressBar = document.getElementById('autoAssignProgressBar');
        const percentEl = document.getElementById('autoAssignProgressPercent');
        const statusEl = document.getElementById('autoAssignCurrentItem');
        const labelEl = document.getElementById('autoAssignProgressLabel');
        
        if (progressBar) {
            progressBar.style.width = (task.progress || 0) + '%';
        }
        if (percentEl) {
            percentEl.textContent = (task.progress || 0) + '%';
        }
        if (statusEl) {
            statusEl.innerHTML = '<i class="bi bi-hourglass-split"></i> <span>' + (task.message || 'Processing...') + '</span>';
        }
        if (labelEl) {
            labelEl.textContent = 'Processing...';
        }
        
        const res = task.result;
        if (res && res.live_stats) {
            const entityCountEl = document.getElementById('autoAssignEntityCount');
            const relCountEl = document.getElementById('autoAssignRelCount');
            if (entityCountEl) {
                entityCountEl.textContent = `${res.entities_assigned} / ${res.entities_total}`;
                entityCountEl.className = res.entities_assigned >= res.entities_total && res.entities_total > 0
                    ? 'mb-1 text-success' : 'mb-1';
            }
            if (relCountEl) {
                relCountEl.textContent = `${res.relationships_assigned} / ${res.relationships_total}`;
                relCountEl.className = res.relationships_assigned >= res.relationships_total && res.relationships_total > 0
                    ? 'mb-1 text-success' : 'mb-1';
            }
        }
    },
    
    /**
     * Save mappings from completed task result to the session (frontend-driven save).
     */
    saveMappingsFromTask: async function(taskResult) {
        // Apply mappings to local state
        if (taskResult.entity_mappings) {
            MappingState.config.entities = taskResult.entity_mappings;
        }
        if (taskResult.relationship_mappings) {
            MappingState.config.relationships = taskResult.relationship_mappings;
        }
        
        // Persist to session via the standard save endpoint
        try {
            if (typeof autoSaveMappings === 'function') {
                await autoSaveMappings();
                console.log('[AutoAssign] Mappings auto-saved to session');
            }
        } catch (saveErr) {
            console.error('[AutoAssign] Auto-save error:', saveErr);
        }
    },
    
    /**
     * Monitor task until completion (polls /tasks/{taskId})
     */
    monitorTask: async function(taskId) {
        const pollInterval = 1500;
        
        while (true) {
            try {
                await new Promise(r => setTimeout(r, pollInterval));
                
                const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
                const data = await response.json();
                
                if (!data.success) {
                    throw new Error('Task not found');
                }
                
                const task = data.task;
                this.updateProgressFromTask(task);
                
                if (task.status === 'completed') {
                    sessionStorage.removeItem(AUTO_ASSIGN_TASK_KEY);
                    this.currentTaskId = null;
                    this.isRunning = false;
                    
                    if (task.result) {
                        this.results = task.result.results || [];
                        await this.saveMappingsFromTask(task.result);
                    }
                    
                    this.showReport();
                    await this.refreshMappingConfig();
                    this.enableSidebarMenus();
                    
                    if (typeof refreshTasks === 'function') refreshTasks();
                    
                    showNotification('Auto-map completed!', 'success');
                    break;
                } else if (task.status === 'failed') {
                    sessionStorage.removeItem(AUTO_ASSIGN_TASK_KEY);
                    this.currentTaskId = null;
                    this.isRunning = false;
                    this.hideProgressUI();
                    this.enableSidebarMenus();
                    showNotification('Auto-map failed: ' + (task.error || 'Unknown error'), 'error');
                    break;
                } else if (task.status === 'cancelled') {
                    sessionStorage.removeItem(AUTO_ASSIGN_TASK_KEY);
                    this.currentTaskId = null;
                    this.isRunning = false;
                    this.hideProgressUI();
                    this.enableSidebarMenus();
                    showNotification('Auto-map was cancelled', 'warning');
                    break;
                }
            } catch (error) {
                console.error('[AutoAssign] Monitoring error:', error);
                sessionStorage.removeItem(AUTO_ASSIGN_TASK_KEY);
                this.currentTaskId = null;
                this.isRunning = false;
                this.hideProgressUI();
                this.enableSidebarMenus();
                showNotification('Error monitoring auto-map', 'error');
                break;
            }
        }
        
        if (typeof refreshTasks === 'function') refreshTasks();
    },
    
    /**
     * Start the auto-map process (async task via backend)
     */
    start: async function() {
        if (this.isRunning) return;
        
        const ontology = MappingState.loadedOntology || {};
        const classes = ontology.classes || [];
        const properties = this.filterObjectProperties(ontology.properties || []);
        
        if (classes.length === 0 && properties.length === 0) {
            showNotification('No ontology classes or properties found', 'info');
            return;
        }
        
        // Filter out excluded entities and relationships
        const activeClasses = classes.filter(c => !c.excluded);
        const excludedNames = new Set(classes.filter(c => c.excluded).map(c => c.name || c.localName));
        const activeProperties = properties.filter(p =>
            !p.excluded && !excludedNames.has(p.domain) && !excludedNames.has(p.range)
        );
        
        // Determine which items already have valid assignments
        const entityMappings = MappingState.config?.entities || [];
        const relationshipMappings = MappingState.config?.relationships || [];
        const assignedEntityUris = new Set(entityMappings.filter(m => m.sql_query).map(m => m.ontology_class));
        const assignedRelUris = new Set(relationshipMappings.filter(m => m.sql_query).map(m => m.property));
        
        // Only send unassigned entities and relationships to the backend
        const unassignedClasses = activeClasses.filter(c => !assignedEntityUris.has(c.uri));
        const unassignedProperties = activeProperties.filter(p => !assignedRelUris.has(p.uri));
        
        if (unassignedClasses.length === 0 && unassignedProperties.length === 0) {
            showNotification('All entities and relationships are already assigned', 'info');
            return;
        }
        
        const entities = unassignedClasses.map(entity => {
            const attributes = (entity.dataProperties || []).map(a => a.name || a.localName || a);
            return {
                uri: entity.uri,
                name: entity.label || entity.name || entity.localName,
                attributes: attributes
            };
        });
        
        const relationships = unassignedProperties.map(rel => ({
            uri: rel.uri,
            name: rel.label || rel.name || rel.localName,
            domain: rel.domain,
            range: rel.range,
            direction: rel.direction || 'forward'
        }));
        
        // Build schema context
        const schemaContext = typeof buildSchemaContext === 'function' ? buildSchemaContext() : {};
        
        try {
            // Start async task
            const response = await fetch('/mapping/auto-assign/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    entities: entities,
                    relationships: relationships,
                    schema_context: schemaContext
                }),
                credentials: 'same-origin'
            });
            
            const result = await response.json();
            
            if (!result.success) {
                showNotification('Error: ' + result.message, 'error');
                return;
            }
            
            // Save task ID and start monitoring
            this.currentTaskId = result.task_id;
            sessionStorage.setItem(AUTO_ASSIGN_TASK_KEY, result.task_id);
            this.isRunning = true;
            this.isCancelled = false;
            this.results = [];
            
            // Disable other menus while running
            this.disableSidebarMenus();
            
            // Show progress UI
            this.showProgressUI();
            
            if (typeof refreshTasks === 'function') refreshTasks();
            
            showNotification(`Auto-map started for ${entities.length} entities and ${relationships.length} relationships. You can navigate away and come back.`, 'info');
            
            // Start monitoring
            this.monitorTask(result.task_id);
            
        } catch (error) {
            console.error('[AutoAssign] Start error:', error);
            showNotification('Error starting auto-map: ' + error.message, 'error');
        }
    },
    
    /**
     * Start re-mapping for entities that have missing attributes.
     * These entities are already assigned but some attributes lack column mappings.
     * Re-running auto-map on them will regenerate the SQL and re-map attributes.
     */
    startPartial: async function() {
        if (this.isRunning) return;
        
        const ontology = MappingState.loadedOntology || {};
        const classes = (ontology.classes || []).filter(c => !c.excluded);
        const entityMappings = MappingState.config?.entities || [];
        
        // Build lookup
        const assignedEntityUris = new Set(entityMappings.filter(m => m.sql_query).map(m => m.ontology_class));
        const mappingByClass = {};
        entityMappings.forEach(m => {
            const uri = m.ontology_class || m.class_uri;
            if (uri) mappingByClass[uri] = m;
        });
        
        // Find non-excluded entities with missing attributes
        const partialEntities = classes.filter(cls => {
            if (!assignedEntityUris.has(cls.uri)) return false;
            const dataProps = cls.dataProperties || [];
            if (dataProps.length === 0) return false;
            const em = mappingByClass[cls.uri] || {};
            const attrMap = em.attribute_mappings || {};
            return dataProps.some(dp => {
                const name = dp.name || dp.localName || '';
                return name && !attrMap[name];
            });
        });
        
        if (partialEntities.length === 0) {
            showNotification('All attributes are already assigned', 'info');
            return;
        }
        
        // Build data for backend (same format as start())
        const entities = partialEntities.map(entity => {
            const attributes = (entity.dataProperties || []).map(a => a.name || a.localName || a);
            return {
                uri: entity.uri,
                name: entity.label || entity.name || entity.localName,
                attributes: attributes
            };
        });
        
        // Build schema context
        const schemaContext = typeof buildSchemaContext === 'function' ? buildSchemaContext() : {};
        
        try {
            const response = await fetch('/mapping/auto-assign/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    entities: entities,
                    relationships: [],
                    schema_context: schemaContext
                }),
                credentials: 'same-origin'
            });
            
            const result = await response.json();
            
            if (!result.success) {
                showNotification('Error: ' + result.message, 'error');
                return;
            }
            
            this.currentTaskId = result.task_id;
            sessionStorage.setItem(AUTO_ASSIGN_TASK_KEY, result.task_id);
            this.isRunning = true;
            this.isCancelled = false;
            this.results = [];
            
            this.showProgressUI();
            
            if (typeof refreshTasks === 'function') refreshTasks();
            
            showNotification(`Re-assigning ${partialEntities.length} entities with missing attributes...`, 'info');
            
            this.monitorTask(result.task_id);
            
        } catch (error) {
            console.error('[AutoAssign] StartPartial error:', error);
            showNotification('Error starting re-mapping: ' + error.message, 'error');
        }
    },
    
    /**
     * Cancel the running auto-map task
     */
    cancel: async function() {
        if (!this.isRunning || !this.currentTaskId) return;
        
        try {
            await fetch(`/tasks/${this.currentTaskId}/cancel`, {
                method: 'POST',
                credentials: 'same-origin'
            });
            const labelEl = document.getElementById('autoAssignProgressLabel');
            if (labelEl) labelEl.textContent = 'Cancelling...';
        } catch (e) {
            console.error('[AutoAssign] Cancel error:', e);
        }
        
        this.isCancelled = true;
    },
    
    /**
     * Show the report
     */
    showReport: function() {
        document.getElementById('autoAssignProgressSection').style.display = 'none';
        document.getElementById('autoAssignReportSection').style.display = 'block';
        document.getElementById('cancelAutoAssignBtn').style.display = 'none';
        document.getElementById('startAutoAssignBtn').style.display = 'inline-block';
        
        // Count results
        const successCount = this.results.filter(r => r.status === 'success').length;
        const failedCount = this.results.filter(r => r.status === 'failed').length;
        const skippedCount = this.results.filter(r => r.status === 'skipped').length;
        
        document.getElementById('reportSuccessCount').textContent = successCount;
        document.getElementById('reportFailedCount').textContent = failedCount;
        document.getElementById('reportSkippedCount').textContent = skippedCount;
        
        // Update badge
        const badge = document.getElementById('autoAssignReportBadge');
        if (this.isCancelled) {
            badge.textContent = 'Cancelled';
            badge.className = 'badge bg-warning';
        } else if (failedCount > 0) {
            badge.textContent = 'Completed with errors';
            badge.className = 'badge bg-warning';
        } else {
            badge.textContent = 'Complete';
            badge.className = 'badge bg-success';
        }
        
        // Build report table
        const tbody = document.getElementById('autoAssignReportBody');
        tbody.innerHTML = this.results.map(r => {
            const statusIcon = r.status === 'success' 
                ? '<i class="bi bi-check-circle-fill text-success"></i>'
                : r.status === 'skipped'
                    ? '<i class="bi bi-dash-circle-fill text-secondary"></i>'
                    : '<i class="bi bi-x-circle-fill text-danger"></i>';
            
            const typeIcon = r.type === 'entity' ? 'bi-box' : 'bi-arrow-left-right';
            const typeBadge = r.type === 'entity' 
                ? '<span class="badge bg-primary">Entity</span>'
                : '<span class="badge bg-purple" style="background-color: #6f42c1;">Relationship</span>';
            
            const details = r.status === 'success' 
                ? `<span class="text-success">${r.details || ''}</span>`
                : `<span class="text-danger">${r.error || ''}</span>`;
            
            return `
                <tr>
                    <td class="text-center">${statusIcon}</td>
                    <td>${typeBadge}</td>
                    <td><i class="bi ${typeIcon} me-1"></i>${r.name}</td>
                    <td class="small">${details}</td>
                </tr>
            `;
        }).join('');
        
        // Show notification
        if (successCount > 0) {
            showNotification(`Auto-mapped ${successCount} item(s) successfully`, 'success', 3000);
        }
    },
    
    /**
     * Reset to initial state
     */
    reset: function() {
        this.results = [];
        document.getElementById('autoAssignProgressSection').style.display = 'none';
        document.getElementById('autoAssignReportSection').style.display = 'none';
        document.getElementById('startAutoAssignBtn').style.display = 'inline-block';
        document.getElementById('cancelAutoAssignBtn').style.display = 'none';
        this.updateStatus();
    }
};

// Initialize when autoassign section becomes active
document.addEventListener('sectionChange', function(e) {
    if (e.detail?.section === 'autoassign') {
        // Add delay and retry logic
        const tryInit = (retries = 0) => {
            if (MappingState.initialized && MappingState.loadedOntology) {
                AutoAssignModule.init();
            } else if (retries < 10) {
                setTimeout(() => tryInit(retries + 1), 100);
            } else {
                console.warn('AutoAssignModule: Force init after timeout');
                AutoAssignModule.init();
            }
        };
        setTimeout(() => tryInit(0), 50);
    }
});
