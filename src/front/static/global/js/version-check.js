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

// Return a context-aware tooltip for disabled edit controls.
// Viewers and older-version readers share the same DOM-level plumbing
// but need different wording so the user knows how to regain write access.
function _readOnlyTooltip() {
    return window.isDomainViewer
        ? 'Read-only: Viewer role — ask for editor/builder rights'
        : 'Read-only: Load latest version to edit';
}

// Selector matching every interactive design surface (mapping map,
// ontology map, Business Views OntoViz canvas, …) where right-click
// menus trigger writes. In read-only mode we block ``contextmenu`` on
// any descendant of these containers during the capture phase, which
// pre-empts the per-widget D3/Canvas handlers. This protects us
// against timing races where a handler reads ``window.isActiveVersion``
// before the viewer cascade in ``checkDomainRole`` has set it to
// ``false``, and it guarantees we also swallow the browser's default
// context menu (some handlers only call ``preventDefault`` inside
// conditional branches).
const _READ_ONLY_DESIGN_SURFACE_SELECTOR =
    '#mapping-map-container, #ontology-map-container, '
    + '.ovz-canvas, .ontoviz-container';

// Install a single document-level capture-phase contextmenu blocker
// that suppresses right-clicks on design surfaces while read-only is
// active. Idempotent.
function installReadOnlyContextMenuBlocker() {
    if (window._readOnlyContextMenuBlockerInstalled) return;
    window._readOnlyContextMenuBlockerInstalled = true;
    document.addEventListener('contextmenu', function(event) {
        if (window.isActiveVersion !== false) return;
        const target = event.target;
        if (target && target.closest
            && target.closest(_READ_ONLY_DESIGN_SURFACE_SELECTOR)) {
            event.preventDefault();
            event.stopPropagation();
            if (typeof event.stopImmediatePropagation === 'function') {
                event.stopImmediatePropagation();
            }
        }
    }, true);
}

// Disable editing elements for read-only (older) versions
function disableEditingForInactiveVersion() {
    const _RO = _readOnlyTooltip();

    installReadOnlyContextMenuBlocker();

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
        saveMenuItem.title = window.isDomainViewer
            ? 'Read-only: Viewer role cannot save'
            : 'Read-only: Load the latest version to save';
    }

    // --- Area-specific restrictions ---
    hideViewModeSidebarItems();
    disableDomainMetadataViewMode();
    disableDomainDocumentsViewMode();
    disableOntologyModelViewMode();
    disableOntologyBusinessViewsViewMode();
    disableDataQualityViewMode();
    disableOntologyImportViewMode();
    disableMappingDesignerViewMode();
    disableMappingManualViewMode();
    disableMappingImportViewMode();
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
    const _RO = _readOnlyTooltip();
    ['loadMetadataBtn', 'removeTablesBtn', 'updateMetadataBtn',
     'importSelectedBtn', 'loadMetadataConfirmBtn',
     'updateMappingsBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) { btn.disabled = true; btn.title = _RO; }
    });
    // Disable the Reset button. The button lives in the metadata header
    // and wires up via ``data-meta-action="clear-metadata"`` (see
    // ``domain-metadata.js``'s delegated handler at ``act === 'clear-metadata'``).
    // Also cover any legacy ``onclick="clearMetadata()"`` binding in case it
    // comes back in a partial template.
    document.querySelectorAll(
        'button[data-meta-action="clear-metadata"], '
        + 'button[onclick*="clearMetadata"]'
    ).forEach(btn => {
        btn.disabled = true; btn.title = _RO;
    });
    // Disable every other ``data-meta-action`` write (data-source swap,
    // description edit, table-details open, update-from-UC) so read-only
    // users cannot reach them even when the specific button id changes.
    document.querySelectorAll(
        '[data-meta-action="open-ds-modal"], '
        + '[data-meta-action="edit-comment"], '
        + '[data-meta-action="table-details"], '
        + '[data-meta-action="update-from-uc"], '
        + '[data-meta-action="remove-selected-tables"], '
        + '[data-meta-action="update-mappings"], '
        + '[data-meta-action="show-load-modal"]'
    ).forEach(el => {
        el.style.pointerEvents = 'none';
        el.style.opacity = '0.5';
        el.title = _RO;
    });
    // Disable inline table description editing and modal Save Changes
    document.querySelectorAll('button[onclick*="saveTableDetails"], button[onclick*="saveTableComment"]').forEach(btn => {
        btn.disabled = true; btn.title = _RO;
    });
}

// Ontology > Import: disable every import trigger so read-only users
// cannot overwrite the currently loaded ontology with a local file,
// a Unity Catalog file, or an industry-standard bundle (FIBO, CDISC,
// IOF, RDFS, OWL). All of these call POST/PUT endpoints that the
// backend PermissionMiddleware would already 403 for viewers, but
// disabling the buttons avoids confusing "write blocked" toasts for
// what is clearly a read-only flow.
function disableOntologyImportViewMode() {
    const _RO = _readOnlyTooltip();
    [
        'importOwlLocalBtn', 'importOwlUCBtn',
        'importRdfsLocalBtn', 'importRdfsUCBtn',
        'importFiboBtn', 'importCdiscBtn', 'importIofBtn',
    ].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) { btn.disabled = true; btn.title = _RO; }
    });
    // Disable the module checkbox selectors too — they only matter if
    // the import is going to run, which it can't.
    document.querySelectorAll(
        '.fibo-domain-cb, .cdisc-domain-cb, .iof-domain-cb'
    ).forEach(cb => { cb.disabled = true; });
    // Hidden file inputs that back the "From Local File" buttons.
    ['importOwlFileInput', 'importRdfsFileInput'].forEach(id => {
        const input = document.getElementById(id);
        if (input) input.disabled = true;
    });
}

// Mapping > Import: disable R2RML import so read-only users cannot
// overwrite the current mapping set.
function disableMappingImportViewMode() {
    const _RO = _readOnlyTooltip();
    ['importR2rmlLocalBtn', 'importR2rmlUCBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) { btn.disabled = true; btn.title = _RO; }
    });
    const fileInput = document.getElementById('importR2rmlFileInput');
    if (fileInput) fileInput.disabled = true;
}

// Domain > Documents: hide upload UI, keep file list viewable
function disableDomainDocumentsViewMode() {
    const dropZone = document.getElementById('docDropZone');
    if (dropZone) {
        dropZone.style.pointerEvents = 'none';
        dropZone.style.opacity = '0.45';
        dropZone.title = window.isDomainViewer
            ? 'Read-only: Viewer role cannot upload'
            : 'Read-only: Load latest version to upload';
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
                btn.title = _readOnlyTooltip();
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
    const _RO = _readOnlyTooltip();

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
    const _RO = _readOnlyTooltip();
    document.querySelectorAll('.dq-shape-card .btn-group button').forEach(btn => {
        btn.disabled = true;
        btn.title = _RO;
    });
}

// Mapping > Designer: disable Unmap, Auto-Map, Save
function disableMappingDesignerViewMode() {
    const _RO = _readOnlyTooltip();
    ['resetPanelBtn', 'autoMapPanelBtn', 'savePanelBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) {
            btn.disabled = true;
            btn.title = _RO;
        }
    });
}

// Mapping > Manual: disable Unmap, Auto-Map, Save
function disableMappingManualViewMode() {
    const _RO = _readOnlyTooltip();
    ['manualResetBtn', 'manualAutoMapBtn', 'manualSavePanelBtn'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) {
            btn.disabled = true;
            btn.title = _RO;
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

// =====================================================
// VIEWER ROLE CHECK
// Non-admin users with a ``viewer`` role on the current
// domain can read everything but cannot write. Surface
// that clearly in the UI (banner + disabled controls)
// rather than letting them type into forms that will
// silently 403 on save.
// =====================================================

window.isDomainViewer = false;

// Prefixes of routes that mutate domain state. Any non-GET request to
// one of these must be blocked for viewers, regardless of which UI
// element triggered it (some buttons are rendered dynamically and
// slip past the DOM-level disabling done by
// ``disableEditingForInactiveVersion``).
const _VIEWER_WRITE_PREFIXES = ['/ontology/', '/mapping/', '/dtwin/', '/domain/'];
// Exceptions: enumeration endpoints that live under those prefixes but
// do not mutate anything. They must stay reachable for viewers.
const _VIEWER_WRITE_EXCEPTIONS = new Set([
    '/domain/list-projects',
    '/domain/list-versions',
    '/domain/load-from-uc',
]);

function checkDomainRole() {
    // Roles are now stamped on <body> by base.html (data-app-role /
    // data-domain-role / data-app-mode) and surfaced via
    // window.OB.permissions, so this function no longer needs to
    // fetch /settings/permissions/me on every page load.
    const perms = (window.OB && window.OB.permissions) || null;
    if (!perms || !perms.isAppMode) return;
    if (perms.isAdmin) return;
    if (perms.domainRole === 'viewer') {
        window.isDomainViewer = true;
        // Cascade to the version-based read-only flag. Every ontology/
        // mapping widget (shared side-panel, OntoViz canvas, map context
        // menus, mapping-designer, mapping-manual, ...) already gates
        // editing on ``window.isActiveVersion === false``. Flipping it
        // here reuses that plumbing so viewers get the same treatment as
        // someone viewing an older version — no right-click actions, no
        // save button, no add/remove attribute, no dashboard/bridge
        // assignment, etc.
        window.isActiveVersion = false;
        showViewerReadOnlyBanner();
        disableEditingForInactiveVersion();
        installViewerFetchGuard();
    }
}

// Global fetch interceptor. For viewers, blocks writes (POST/PUT/PATCH/DELETE)
// to any domain-mutating route and surfaces a clear alert. The backend
// already returns 403 for these, but intercepting client-side gives
// immediate, unambiguous feedback and prevents partial UI state changes.
function installViewerFetchGuard() {
    if (window._viewerFetchGuardInstalled) return;
    window._viewerFetchGuardInstalled = true;
    const originalFetch = window.fetch.bind(window);
    window.fetch = function viewerGuardedFetch(input, init) {
        try {
            const method = ((init && init.method) ||
                (input && input.method) || 'GET').toString().toUpperCase();
            if (method !== 'GET' && method !== 'HEAD' && method !== 'OPTIONS') {
                const url = (typeof input === 'string') ? input :
                    (input && input.url) ? input.url : '';
                let pathOnly = url;
                try { pathOnly = new URL(url, window.location.origin).pathname; }
                catch (_) { /* best effort */ }
                const isMutating = _VIEWER_WRITE_PREFIXES.some(
                    p => pathOnly.startsWith(p)
                ) && !_VIEWER_WRITE_EXCEPTIONS.has(pathOnly);
                if (isMutating) {
                    console.warn(
                        '[viewer] blocked %s %s — read-only role',
                        method, pathOnly
                    );
                    flashViewerBlocked();
                    return Promise.resolve(new Response(
                        JSON.stringify({
                            success: false,
                            error: 'Viewer role does not allow write operations',
                        }),
                        { status: 403, headers: { 'Content-Type': 'application/json' } }
                    ));
                }
            }
        } catch (_) { /* fall through to real fetch */ }
        return originalFetch(input, init);
    };
}

function flashViewerBlocked() {
    const banner = document.getElementById('readOnlyBanner');
    if (!banner) {
        alert('Read-only: your role on this domain is viewer, so changes cannot be saved.');
        return;
    }
    banner.style.transition = 'background-color 0.2s ease';
    const prev = banner.style.backgroundColor;
    banner.style.backgroundColor = '#f8d7da';
    setTimeout(() => { banner.style.backgroundColor = prev; }, 600);
}

function showViewerReadOnlyBanner() {
    const existingBanner = document.getElementById('readOnlyBanner');
    if (existingBanner) return;

    const spacer = document.createElement('div');
    spacer.id = 'readOnlyBannerSpacer';

    const banner = document.createElement('div');
    banner.id = 'readOnlyBanner';
    banner.className = 'alert alert-info text-center mb-0 py-2';
    banner.innerHTML = '<i class="bi bi-eye"></i> <strong>Read-Only:</strong> '
        + 'You have viewer access on this domain. Ask a domain admin for '
        + 'editor or builder rights to make changes.';

    const navbar = document.querySelector('.navbar');
    if (navbar) {
        navbar.after(spacer);
        navbar.after(banner);
    } else {
        document.body.insertBefore(spacer, document.body.firstChild);
        document.body.insertBefore(banner, document.body.firstChild);
    }

    document.body.classList.add('read-only-mode');
}

// Run on page load (version check first, then role-based gate).
// checkDomainRole is now synchronous (reads window.OB.permissions),
// no await needed.
document.addEventListener('DOMContentLoaded', async () => {
    await checkVersionStatus();
    if (!window.isActiveVersion) return;
    checkDomainRole();
});
