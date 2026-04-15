// =====================================================
// VERSION STATUS CHECK
// =====================================================

// Global variable to track if current version is active (editable)
window.isActiveVersion = true;

// Check version status and update global state
async function checkVersionStatus() {
    try {
        const data = await fetchOnce('/domain/version-status');
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
    banner.innerHTML = '<i class="bi bi-lock"></i> <strong>Read-Only:</strong> You are viewing an older version. Load the latest version to make changes.';
    
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

// IDs of buttons that must stay enabled even in read-only mode
const _READ_ONLY_ALLOW_IDS = new Set([
    'runDiagnosticsBtn',
    'docRefreshBtn',
    'syncStartBtn',
    'dtDropSnapshot',
    'dtReloadFromRegistry',
]);

// Disable editing elements for read-only (older) versions
function disableEditingForInactiveVersion() {
    const _RO = 'Read-only: Load latest version to edit';

    // Hide all ontology edit buttons (with class ontology-edit-btn)
    document.querySelectorAll('.ontology-edit-btn').forEach(btn => {
        btn.style.display = 'none';
    });

    // Check if we're on query page (Digital Twin) - don't disable execute/run buttons there
    const isQueryPage = window.location.pathname.includes('/dtwin');

    // Disable all buttons that modify data (but not on query page)
    if (!isQueryPage) {
        const editButtons = document.querySelectorAll(
            'button[onclick*="save"], button[onclick*="add"], button[onclick*="delete"], '
            + 'button[onclick*="create"], button[onclick*="update"], '
            + '.btn-primary, .btn-success, .btn-danger'
        );
        editButtons.forEach(btn => {
            if (_READ_ONLY_ALLOW_IDS.has(btn.id)) return;
            if (!btn.classList.contains('btn-secondary') && !btn.getAttribute('data-bs-dismiss')) {
                btn.disabled = true;
                btn.title = _RO;
            }
        });
    }

    // Disable form inputs (except search/filter/query fields)
    const inputs = document.querySelectorAll('input:not([type="search"]):not([id*="search"]):not([id*="filter"]), textarea, select');
    inputs.forEach(input => {
        if (isQueryPage) return;
        if (input.id === 'domainVersionSelect') return;
        if (!input.id.includes('search') && !input.id.includes('filter') && !input.id.includes('query')) {
            input.disabled = true;
        }
    });

    // Explicitly re-enable version selector
    const versionSelect = document.getElementById('domainVersionSelect');
    if (versionSelect) {
        versionSelect.disabled = false;
        versionSelect.removeAttribute('disabled');
    }

    // Disable Save Domain menu item in navbar
    const saveMenuItem = document.getElementById('menuSaveDomain');
    if (saveMenuItem) {
        saveMenuItem.classList.add('disabled');
        saveMenuItem.style.pointerEvents = 'none';
        saveMenuItem.style.opacity = '0.5';
        saveMenuItem.onclick = function(e) { e.preventDefault(); return false; };
        saveMenuItem.title = 'Read-only: Load the latest version to save';
    }

    // --- Area-specific restrictions ---
    hideViewModeSidebarItems();
    disableDomainMetadataViewMode();
    disableDomainDocumentsViewMode();
    disableOntologyModelViewMode();
    disableOntologyBusinessViewsViewMode();
    disableDataQualityViewMode();
    disableMappingDesignerViewMode();
    disableMappingManualViewMode();
    enableMappingDiagnostics();
    enableBuildSyncControls();
}

// Hide sidebar items flagged with view-mode-hidden
function hideViewModeSidebarItems() {
    document.querySelectorAll('.view-mode-hidden').forEach(link => {
        link.style.display = 'none';
    });
}

// Domain > Metadata: disable all modification buttons
function disableDomainMetadataViewMode() {
    const _RO = 'Read-only: Load latest version to edit';
    ['loadMetadataBtn', 'removeTablesBtn', 'updateMetadataBtn',
     'importSelectedBtn', 'loadMetadataConfirmBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) { btn.disabled = true; btn.title = _RO; }
    });
    // Disable the Reset button (btn-outline-danger with onclick="clearMetadata()")
    document.querySelectorAll('button[onclick*="clearMetadata"]').forEach(btn => {
        btn.disabled = true; btn.title = _RO;
    });
    // Disable inline table description editing and modal Save Changes
    document.querySelectorAll('button[onclick*="saveTableDetails"], button[onclick*="saveTableComment"]').forEach(btn => {
        btn.disabled = true; btn.title = _RO;
    });
}

// Domain > Documents: hide upload UI, keep file list viewable
function disableDomainDocumentsViewMode() {
    const dropZone = document.getElementById('docDropZone');
    if (dropZone) {
        dropZone.style.pointerEvents = 'none';
        dropZone.style.opacity = '0.45';
        dropZone.title = 'Read-only: Load latest version to upload';
    }
    const fileInput = document.getElementById('docFileInput');
    if (fileInput) fileInput.disabled = true;
    ['docUploadBtn', 'docClearQueueBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) { btn.disabled = true; }
    });
    // Disable delete buttons rendered dynamically in the file list
    const fileList = document.getElementById('docFileList');
    if (fileList) {
        new MutationObserver(() => {
            fileList.querySelectorAll('button[onclick*="deleteFile"]').forEach(btn => {
                btn.disabled = true;
                btn.title = 'Read-only: Load latest version to edit';
            });
        }).observe(fileList, { childList: true, subtree: true });
    }
}

// Ontology > Model: hide assistant and auto-map icons button
function disableOntologyModelViewMode() {
    const assistantBtn = document.getElementById('mapToggleAssistant');
    if (assistantBtn) assistantBtn.style.display = 'none';

    const autoIconsBtn = document.getElementById('mapAutoAssignIcons');
    if (autoIconsBtn) {
        autoIconsBtn.disabled = true;
        autoIconsBtn.style.display = 'none';
    }
}

// Ontology > Business Views: hide mode toggle, disable CRUD buttons
function disableOntologyBusinessViewsViewMode() {
    const _RO = 'Read-only: Load latest version to edit';

    const designModeToggle = document.getElementById('designModeToggle');
    if (designModeToggle) designModeToggle.style.display = 'none';

    ['createViewBtn', 'renameViewBtn', 'deleteViewBtn', 'createGroupFromViewBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) { btn.disabled = true; btn.title = _RO; }
    });
}

// Ontology > Data Quality: view-only (no toggle, edit, delete per rule)
function disableDataQualityViewMode() {
    _disableDqCards();
    // Watch for dynamically rendered cards (shapes load async)
    const container = document.getElementById('dataquality-section');
    if (container) {
        new MutationObserver(_disableDqCards).observe(container, { childList: true, subtree: true });
    }
}

function _disableDqCards() {
    const _RO = 'Read-only: Load latest version to edit';
    document.querySelectorAll('.dq-shape-card .btn-group button').forEach(btn => {
        btn.disabled = true;
        btn.title = _RO;
    });
}

// Mapping > Designer: disable Unmap, Auto-Map, Save
function disableMappingDesignerViewMode() {
    ['resetPanelBtn', 'autoMapPanelBtn', 'savePanelBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) {
            btn.disabled = true;
            btn.title = 'Read-only: Load latest version to edit';
        }
    });
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

// Mapping > Diagnostics: must remain usable in read-only mode
function enableMappingDiagnostics() {
    const btn = document.getElementById('runDiagnosticsBtn');
    if (btn) {
        btn.disabled = false;
        btn.removeAttribute('title');
    }
}

// Domain > Build: must remain fully usable in read-only mode
function enableBuildSyncControls() {
    ['syncStartBtn', 'dtDropSnapshot', 'dtReloadFromRegistry'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.disabled = false; el.removeAttribute('title'); }
    });
    ['buildModeIncremental', 'buildModeFull'].forEach(id => {
        const radio = document.getElementById(id);
        if (radio) { radio.disabled = false; }
    });
    // Re-enable the refresh button
    document.querySelectorAll('button[onclick*="checkTripleStoreStatus"]').forEach(btn => {
        btn.disabled = false;
        btn.removeAttribute('title');
    });
}

// Run on page load
document.addEventListener('DOMContentLoaded', checkVersionStatus);
