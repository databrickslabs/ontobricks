// =====================================================
// VERSION STATUS CHECK
// =====================================================

// Global variable to track if current version is active (editable)
window.isActiveVersion = true;

// Check version status and update global state
async function checkVersionStatus() {
    try {
        const data = await fetchOnce('/project/version-status');
        if (data.success) {
            window.isActiveVersion = data.is_active;
            if (!data.is_active) {
                showReadOnlyBanner();
                disableEditingForInactiveVersion();
            }
        }
    } catch (e) {
        console.log('Could not fetch version status');
    }
}

// Show a banner indicating read-only mode
function showReadOnlyBanner() {
    const existingBanner = document.getElementById('readOnlyBanner');
    if (existingBanner) return;
    
    // Create a spacer div that takes up space in the document flow
    const spacer = document.createElement('div');
    spacer.id = 'readOnlyBannerSpacer';
    
    // Create the fixed banner (styles defined in components.css)
    const banner = document.createElement('div');
    banner.id = 'readOnlyBanner';
    banner.className = 'alert alert-warning text-center mb-0 py-2';
    banner.innerHTML = '<i class="bi bi-lock"></i> <strong>Read-Only Mode:</strong> You are viewing an older version. Load the latest version to make changes.';
    
    // Insert banner after navbar
    const navbar = document.querySelector('.navbar');
    if (navbar) {
        navbar.after(spacer);
        navbar.after(banner);
    } else {
        document.body.insertBefore(spacer, document.body.firstChild);
        document.body.insertBefore(banner, document.body.firstChild);
    }
    
    // Also add a CSS class to body to allow CSS-based adjustments
    document.body.classList.add('read-only-mode');
}

// Disable editing elements for inactive versions
function disableEditingForInactiveVersion() {
    // Hide all ontology edit buttons (with class ontology-edit-btn)
    const ontologyEditBtns = document.querySelectorAll('.ontology-edit-btn');
    ontologyEditBtns.forEach(btn => {
        btn.style.display = 'none';
    });
    
    // Check if we're on query page (Digital Twin) - don't disable execute/run buttons there
    const isQueryPage = window.location.pathname.includes('/dtwin');
    
    // Disable all buttons that modify data (but not on query page)
    if (!isQueryPage) {
        const editButtons = document.querySelectorAll('button[onclick*="save"], button[onclick*="add"], button[onclick*="delete"], button[onclick*="create"], button[onclick*="update"], .btn-primary, .btn-success, .btn-danger');
        editButtons.forEach(btn => {
            if (!btn.classList.contains('btn-secondary') && !btn.getAttribute('data-bs-dismiss')) {
                btn.disabled = true;
                btn.title = 'Read-only: Load latest version to edit';
            }
        });
    }
    
    // Disable form inputs (except search/filter/query fields)
    const inputs = document.querySelectorAll('input:not([type="search"]):not([id*="search"]):not([id*="filter"]), textarea, select');
    inputs.forEach(input => {
        // Don't disable inputs on query page (Digital Twin) or query-related inputs
        if (isQueryPage) return;
        // Don't disable the version selector - it should always be usable
        if (input.id === 'projectVersionSelect') return;
        if (!input.id.includes('search') && !input.id.includes('filter') && !input.id.includes('query')) {
            input.disabled = true;
        }
    });
    
    // Explicitly re-enable version selector (it should always be usable to switch versions)
    const versionSelect = document.getElementById('projectVersionSelect');
    if (versionSelect) {
        versionSelect.disabled = false;
        versionSelect.removeAttribute('disabled');
    }
    
    // Disable Save Project menu item in navbar
    const saveMenuItem = document.getElementById('menuSaveProject');
    if (saveMenuItem) {
        saveMenuItem.classList.add('disabled');
        saveMenuItem.style.pointerEvents = 'none';
        saveMenuItem.style.opacity = '0.5';
        saveMenuItem.onclick = function(e) { e.preventDefault(); return false; };
        saveMenuItem.title = 'Read-only: Cannot save inactive version';
    }

    // --- View-mode specific restrictions per menu area ---
    hideViewModeSidebarItems();
    disableOntologyModelViewMode();
    disableOntologyBusinessViewsViewMode();
    disableMappingDesignerViewMode();
    disableMappingManualViewMode();
}

// Hide sidebar items flagged with view-mode-hidden
function hideViewModeSidebarItems() {
    document.querySelectorAll('.view-mode-hidden').forEach(link => {
        link.style.display = 'none';
    });
}

// Ontology > Model: no right-click popups, hide assistant tab & button
function disableOntologyModelViewMode() {
    const assistantBtn = document.getElementById('mapToggleAssistant');
    if (assistantBtn) {
        assistantBtn.style.display = 'none';
    }

    window._viewModeOntologyModelApplied = true;
}

// Ontology > Business Views: disable View/Edit toggle, disable Edit/Add/Delete buttons
function disableOntologyBusinessViewsViewMode() {
    const designModeToggle = document.getElementById('designModeToggle');
    if (designModeToggle) {
        designModeToggle.style.display = 'none';
    }

    const createViewBtn = document.getElementById('createViewBtn');
    if (createViewBtn) {
        createViewBtn.disabled = true;
        createViewBtn.title = 'Read-only: Load latest version to edit';
    }

    const renameViewBtn = document.getElementById('renameViewBtn');
    if (renameViewBtn) {
        renameViewBtn.disabled = true;
        renameViewBtn.title = 'Read-only: Load latest version to edit';
    }

    const deleteViewBtn = document.getElementById('deleteViewBtn');
    if (deleteViewBtn) {
        deleteViewBtn.disabled = true;
        deleteViewBtn.title = 'Read-only: Load latest version to edit';
    }
}

// Mapping > Designer: disable Unmap, Auto-Map, Save; no right-click popups
function disableMappingDesignerViewMode() {
    ['resetPanelBtn', 'autoMapPanelBtn', 'savePanelBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) {
            btn.disabled = true;
            btn.title = 'Read-only: Load latest version to edit';
        }
    });

    window._viewModeMappingDesignerApplied = true;
}

// Mapping > Manual: disable Unmap, Auto-Map, Save
function disableMappingManualViewMode() {
    ['manualResetBtn', 'manualAutoMapBtn', 'manualSavePanelBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) {
            btn.disabled = true;
            btn.title = 'Read-only: Load latest version to edit';
        }
    });
}

// Run on page load
document.addEventListener('DOMContentLoaded', checkVersionStatus);
