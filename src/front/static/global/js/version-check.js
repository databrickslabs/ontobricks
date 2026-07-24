// =====================================================
// VERSION STATUS CHECK
//
// On every page load we ask the backend for the loaded version's
// lifecycle status. Design editing is allowed only while the status is
// DRAFT — regardless of whether the loaded version is the latest one.
// An older DRAFT version is fully editable; an IN-REVIEW / PUBLISHED
// version is read-only for design changes. When the version is not
// editable we:
//   1. annotate the navbar role pill (rendered by ``permissions.js``)
//      with a "read-only" note via ``window.OB.annotateRoleNavBadge``,
//   2. add ``read-only-version`` to <body> — every design-editing gate
//      in permissions.css (form fields, write buttons, OntoViz
//      controls, …) keys off ``:is(.read-only-version, .role-viewer)``,
//      so we no longer need a per-button JS sweep,
//   3. install the shared capture-phase contextmenu blocker for
//      D3/Canvas design surfaces (defined once in ``permissions.js``).
//
// KG Build/sync and reasoning materialise are data-refresh ops (they
// re-materialise triples using the frozen design) and are NOT gated on
// the lifecycle status — they remain usable on PUBLISHED / IN-REVIEW
// versions, still guarded by the builder role.
//
// Domain-role gating (viewer vs editor vs builder) is also done
// declaratively now — viewers get ``body.role-viewer`` from
// ``permissions.js`` synchronously at parse time, no round-trip
// required.
// =====================================================

// Global "editable" proxy used by every design-surface gate. It now means
// "the loaded version is editable" (i.e. DRAFT), not "is the latest
// version". Defaults to true so the UI stays editable until the async
// check runs.
window.isActiveVersion = true;
// Lifecycle status of the loaded version. Editing is only allowed while
// the status is DRAFT; IN-REVIEW / PUBLISHED versions are read-only.
// Defaults to 'DRAFT' so the UI stays editable until the async check runs.
window.versionStatus = 'DRAFT';

// Lifecycle status → Bootstrap badge classes + label (shared map).
const VERSION_STATUS_BADGE = {
    'DRAFT': { cls: 'bg-warning-subtle text-dark border-warning', icon: 'pencil', label: 'Draft' },
    'IN-REVIEW': { cls: 'bg-info-subtle text-dark border-info', icon: 'eye', label: 'In Review' },
    'PUBLISHED': { cls: 'bg-success-subtle text-dark border-success', icon: 'broadcast', label: 'Published' }
};

// Populate every ``.js-version-status-badge`` placeholder with the current
// lifecycle status. Lets any Jinja template surface the badge by simply
// dropping ``<span class="js-version-status-badge"></span>`` next to a
// ``v{{ current_version }}`` header. Status is a controlled enum (safe).
function renderVersionStatusBadges(status) {
    const cfg = VERSION_STATUS_BADGE[String(status || 'DRAFT').toUpperCase()]
        || VERSION_STATUS_BADGE['DRAFT'];
    document.querySelectorAll('.js-version-status-badge').forEach(el => {
        el.className = 'js-version-status-badge badge border ' + cfg.cls;
        el.style.fontSize = el.style.fontSize || '0.65rem';
        el.innerHTML = '<i class="bi bi-' + cfg.icon + ' me-1"></i>' + cfg.label;
        el.title = 'Lifecycle status: ' + cfg.label;
    });
}
window.OB = window.OB || {};
window.OB.renderVersionStatusBadges = renderVersionStatusBadges;

async function checkVersionStatus() {
    try {
        const data = await fetchOnce('/domain/version-status');
        if (data.success) {
            window.versionStatus = data.status || 'DRAFT';
            // Editability is driven solely by the lifecycle status: only a
            // DRAFT version is editable, regardless of whether it is the
            // latest version. Older DRAFT versions are fully editable.
            // ``isActiveVersion`` is kept as the global "editable" proxy that
            // every design-surface gate keys off — it now means "editable",
            // not "is latest".
            const editable = window.versionStatus === 'DRAFT';
            window.isActiveVersion = editable;
            renderVersionStatusBadges(window.versionStatus);

            if (!editable) {
                // ``read-only-version`` is the generic gate every selector
                // in permissions.css / ontoviz.css keys off, disabling all
                // design-editing surfaces with no duplication.
                document.body.classList.add('read-only-version');
                if (window.OB && typeof window.OB.annotateRoleNavBadge === 'function') {
                    window.OB.annotateRoleNavBadge(
                        'This version is ' + window.versionStatus +
                        ' (read-only). Set it back to DRAFT to make changes.'
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
    renderVersionStatusBadges(window.versionStatus);
    checkVersionStatus();
});
