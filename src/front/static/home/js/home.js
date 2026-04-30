/**
 * OntoBricks - home.js
 * Extracted from home templates per code_instructions.txt
 */


async function loadSessionStatus() {
    try {
        const response = await fetch('/session-status', { credentials: 'same-origin' });
        const data = await response.json();
        
        // Update domain name
        const domainName = data.domain_name || 'NewDomain';
        document.getElementById('homeDomainName').textContent = domainName;
        
        // Update class count
        const classCount = data.class_count || 0;
        document.getElementById('classCount').textContent = classCount;
        document.getElementById('taxonomyIcon').className = 'stat-icon ' + (classCount > 0 ? 'active' : 'inactive');
        
        // Update property count (fetch from ontology)
        const propResponse = await fetch('/ontology/get-loaded-ontology', { credentials: 'same-origin' });
        const propData = await propResponse.json();
        let propCount = 0;
        if (propData.success && propData.ontology) {
            propCount = propData.ontology.properties?.length || 0;
        }
        document.getElementById('propCount').textContent = propCount;
        document.getElementById('propsIcon').className = 'stat-icon ' + (propCount > 0 ? 'active' : 'inactive');
        
        // Update mapping count
        const entityMappings = data.entities || 0;
        const relMappings = data.relationships || 0;
        const totalMappings = entityMappings + relMappings;
        document.getElementById('mappingCount').textContent = totalMappings;
        document.getElementById('mappingIcon').className = 'stat-icon ' + (totalMappings > 0 ? 'active' : 'inactive');
        
        // Check R2RML status for workflow
        const hasR2rml = data.has_r2rml;
        
        // Update workflow status badges
        updateWorkflowStatus(classCount, totalMappings, hasR2rml);
        
    } catch (error) {
        console.error('Error loading session status:', error);
    }
}

function updateWorkflowStatus(classCount, mappingCount, hasR2rml) {
    const taxonomyStatus = document.getElementById('taxonomyCardStatus');
    const mappingStatus = document.getElementById('mappingCardStatus');
    const queryStatus = document.getElementById('queryCardStatus');
    
    if (classCount > 0) {
        taxonomyStatus.innerHTML = '<i class="bi bi-check-circle-fill"></i> Loaded';
        taxonomyStatus.className = 'card-status ready';
    } else {
        taxonomyStatus.innerHTML = '<i class="bi bi-circle"></i> Not started';
        taxonomyStatus.className = 'card-status pending';
    }
    
    if (mappingCount > 0) {
        mappingStatus.innerHTML = '<i class="bi bi-check-circle-fill"></i> Configured';
        mappingStatus.className = 'card-status ready';
    } else if (classCount > 0) {
        mappingStatus.innerHTML = '<i class="bi bi-arrow-right-circle"></i> Ready to assign';
        mappingStatus.className = 'card-status pending';
    } else {
        mappingStatus.innerHTML = '<i class="bi bi-circle"></i> Needs ontology first';
        mappingStatus.className = 'card-status pending';
    }
    
    if (hasR2rml || mappingCount > 0) {
        // R2RML is auto-generated when mappings exist
        queryStatus.innerHTML = '<i class="bi bi-check-circle-fill"></i> Ready to sync & explore';
        queryStatus.className = 'card-status ready';
    } else {
        queryStatus.innerHTML = '<i class="bi bi-circle"></i> Needs mapping first';
        queryStatus.className = 'card-status pending';
    }
}

// Project Management Functions
async function newDomain() {
    const confirmed = await showConfirmDialog({
        title: 'New Domain',
        message: 'Create a new domain? This will clear all current ontology and mapping data.',
        confirmText: 'Create New',
        confirmClass: 'btn-warning',
        icon: 'file-earmark-plus'
    });
    if (!confirmed) return;
    
    try {
        showDomainStatus('Creating new domain...', 'info');
        const response = await fetch('/reset-session', { method: 'POST', credentials: 'same-origin' });
        const result = await response.json();
        
        if (result.success) {
            showDomainStatus('New domain created', 'success');
            if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
            setTimeout(() => window.location.reload(), 1000);
        } else {
            showDomainStatus('Error: ' + result.message, 'error');
        }
    } catch (error) {
        showDomainStatus('Error: ' + error.message, 'error');
    }
}

function showDomainStatus(message, type) {
    const statusEl = document.getElementById('domainStatus');
    statusEl.className = 'domain-status ' + type;
    statusEl.innerHTML = `<i class="bi bi-${type === 'success' ? 'check-circle' : type === 'error' ? 'x-circle' : type === 'warning' ? 'exclamation-triangle' : 'hourglass-split'}"></i> ${message}`;
    statusEl.classList.remove('hidden-initial');
    
    if (type === 'success') {
        setTimeout(() => statusEl.classList.add('hidden-initial'), 5000);
    }
}

async function resetTaxonomy(event) {
    event.preventDefault();
    event.stopPropagation();
    
    const confirmed = await showConfirmDialog({
        title: 'Reset Ontology',
        message: 'This will clear the current ontology (classes and properties). Continue?',
        confirmText: 'Reset',
        confirmClass: 'btn-danger',
        icon: 'exclamation-triangle'
    });
    if (!confirmed) return;
    
    try {
        const response = await fetch('/ontology/reset', { method: 'POST', credentials: 'same-origin' });
        const result = await response.json();
        
        if (result.success) {
            showNotification('Ontology reset successfully', 'success');
            loadSessionStatus();
            if (typeof window.refreshOntologyStatus === 'function') {
                window.refreshOntologyStatus();
            }
        } else {
            showNotification('Error resetting ontology: ' + result.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

async function resetMapping(event) {
    event.preventDefault();
    event.stopPropagation();
    
    const confirmed = await showConfirmDialog({
        title: 'Reset Assignments',
        message: 'This will clear all assignments and R2RML data. Continue?',
        confirmText: 'Reset',
        confirmClass: 'btn-danger',
        icon: 'exclamation-triangle'
    });
    if (!confirmed) return;
    
    try {
        const response = await fetch('/mapping/reset', { method: 'POST', credentials: 'same-origin' });
        const result = await response.json();
        
        if (result.success) {
            showNotification('Mapping reset successfully', 'success');
            loadSessionStatus();
            if (typeof window.refreshOntologyStatus === 'function') {
                window.refreshOntologyStatus();
            }
        } else {
            showNotification('Error resetting mapping: ' + result.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

// Initialize home page on load
document.addEventListener('DOMContentLoaded', function() {
    loadSessionStatus();
    initHomeActionButtons();
});

/**
 * Wire domain / workflow buttons (data-action) — keeps home.html free of inline handlers.
 */
function initHomeActionButtons() {
    var root = document.getElementById('sessionPanel') || document.body;
    root.addEventListener('click', onHomePanelClick);
    var workflow = document.querySelector('.workflow-section');
    if (workflow && workflow !== root) {
        workflow.addEventListener('click', onHomePanelClick);
    }
}

function onHomePanelClick(e) {
    var el = e.target.closest('[data-action]');
    if (!el) return;
    var action = el.getAttribute('data-action');
    if (action === 'newDomain') {
        e.preventDefault();
        newDomain();
    } else if (action === 'domainSave') {
        e.preventDefault();
        domainSave();
    } else if (action === 'domainLoad') {
        e.preventDefault();
        domainLoad();
    } else if (action === 'resetTaxonomy') {
        e.preventDefault();
        resetTaxonomy(e);
    } else if (action === 'resetMapping') {
        e.preventDefault();
        resetMapping(e);
    }
}
