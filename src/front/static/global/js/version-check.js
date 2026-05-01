// =====================================================
// VERSION STATUS CHECK
//
// On every page load we ask the backend whether the loaded domain is
// the active version. If it is not, we:
//   1. annotate the navbar role pill (rendered by ``permissions.js``)
//      with an "older version, read-only" note via
//      ``window.OB.annotateRoleNavBadge``,
//   2. add ``read-only-version`` to <body> — every gate in
//      permissions.css (form fields, write buttons, OntoViz controls,
//      …) keys off ``:is(.read-only-version, .role-viewer)``, so we
//      no longer need a per-button JS sweep,
//   3. install the shared capture-phase contextmenu blocker for
//      D3/Canvas design surfaces (defined once in ``permissions.js``
//      and reused here for older-version readers).
//
// Domain-role gating (viewer vs editor vs builder) is also done
// declaratively now — viewers get ``body.role-viewer`` from
// ``permissions.js`` synchronously at parse time, no round-trip
// required.
// =====================================================

window.isActiveVersion = true;

async function checkVersionStatus() {
    try {
        const data = await fetchOnce('/domain/version-status');
        if (data.success) {
            window.isActiveVersion = data.is_active;
            if (!data.is_active) {
                document.body.classList.add('read-only-version');
                if (window.OB && typeof window.OB.annotateRoleNavBadge === 'function') {
                    window.OB.annotateRoleNavBadge(
                        'You are viewing an older version of this domain. '
                        + 'Load the latest version to make changes.'
                    );
                }
                if (window.OB && typeof window.OB.installReadOnlyContextMenuBlocker === 'function') {
                    window.OB.installReadOnlyContextMenuBlocker();
                }
            }
        }
    } catch (e) {
        console.log('Could not fetch version status');
    }
}

document.addEventListener('DOMContentLoaded', () => {
    checkVersionStatus();
});
