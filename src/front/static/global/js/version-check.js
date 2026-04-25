// =====================================================
// VERSION STATUS CHECK
//
// On every page load we ask the backend whether the loaded domain is
// the active version. If it is not, we:
//   1. show a sticky read-only banner under the navbar,
//   2. add ``read-only-version`` to <body> — every gate in
//      permissions.css (form fields, write buttons, OntoViz controls,
//      …) keys off that class, so we no longer need a per-button JS
//      sweep,
//   3. install a single document-level capture-phase contextmenu
//      blocker for D3/Canvas design surfaces, where the right-click
//      menus would otherwise trigger writes that the backend would
//      then 403 with no UI feedback.
//
// Domain-role gating (viewer vs editor vs builder) is also done
// declaratively now — see ``permissions.css`` and
// ``window.OB.permissions``.
// =====================================================

window.isActiveVersion = true;

async function checkVersionStatus() {
    try {
        const data = await fetchOnce('/domain/version-status');
        if (data.success) {
            window.isActiveVersion = data.is_active;
            if (!data.is_active) {
                showReadOnlyBanner();
                document.body.classList.add('read-only-version');
                installReadOnlyContextMenuBlocker();
            }
        }
    } catch (e) {
        console.log('Could not fetch version status');
    }
}

// Sticky banner under the navbar.
function showReadOnlyBanner() {
    if (document.getElementById('readOnlyBanner')) return;

    const spacer = document.createElement('div');
    spacer.id = 'readOnlyBannerSpacer';

    const banner = document.createElement('div');
    banner.id = 'readOnlyBanner';
    banner.className = 'alert alert-warning text-center mb-0 py-2';
    banner.innerHTML = '<i class="bi bi-lock"></i> <strong>Read-Only:</strong> '
        + 'You are viewing an older version. Load the latest version to make changes.';

    const navbar = document.querySelector('.navbar');
    if (navbar) {
        navbar.after(spacer);
        navbar.after(banner);
    } else {
        document.body.insertBefore(spacer, document.body.firstChild);
        document.body.insertBefore(banner, document.body.firstChild);
    }
}

// Selector matching every interactive design surface (mapping map,
// ontology map, Business Views OntoViz canvas, …) where right-click
// menus trigger writes. In read-only mode we block ``contextmenu`` on
// any descendant of these containers during the capture phase, which
// pre-empts the per-widget D3/Canvas handlers and guarantees we also
// swallow the browser's default context menu.
const _READ_ONLY_DESIGN_SURFACE_SELECTOR =
    '#mapping-map-container, #ontology-map-container, '
    + '.ovz-canvas, .ontoviz-container';

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

document.addEventListener('DOMContentLoaded', () => {
    checkVersionStatus();
});
