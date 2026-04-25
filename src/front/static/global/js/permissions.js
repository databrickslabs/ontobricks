/**
 * OntoBricks - Permissions (server-injected, synchronous)
 *
 * The server stamps the caller's resolved roles on the <body> tag in
 * base.html (data-app-role / data-domain-role / data-app-mode), so the
 * page can gate UI elements without an extra HTTP round-trip on first
 * paint. This module reads those attributes once at parse time and:
 *
 *   1. exposes a frozen ``window.OB.permissions`` namespace consumed by
 *      navbar.js, version-check.js, registry.js, … (helper functions
 *      ``hasAppRole`` / ``hasDomainRole`` mirror the backend hierarchy);
 *
 *   2. when the caller has a *viewer* domain role and is not an app
 *      admin, stamps ``body.role-viewer`` so every gate in
 *      ``permissions.css`` (and the OntoViz / mapping / sidebar
 *      stylesheets) collapses write surfaces — buttons, dropdowns,
 *      form fields, OntoViz toolbars — exactly the way they do for an
 *      older inactive version. The backend still 403s any write
 *      attempt; this just removes the "click → toast error" round-trip
 *      we used to surface for viewers;
 *
 *   3. installs a single document-level capture-phase ``contextmenu``
 *      blocker on D3/Canvas design surfaces (mapping map, OntoViz
 *      canvas, …) and exposes it as
 *      ``window.OB.installReadOnlyContextMenuBlocker`` so
 *      ``version-check.js`` can reuse it for older-version readers;
 *
 *   4. renders a small red ROLE pill in the navbar (right side, before
 *      the warehouse / notifications icons) showing the caller's
 *      effective role — ADMIN / BUILDER / EDITOR / VIEWER — with a
 *      tooltip explaining what the role allows. ``version-check.js``
 *      appends an "older version, read-only" note to the tooltip when
 *      the loaded domain version is not the active one.
 *
 * Roles and the role hierarchy match
 * ``back/objects/registry/PermissionService.py``:
 *   none < viewer < editor < builder < admin
 * (ROLE_APP_USER intentionally has no level — it gates only via the
 *  app-scope check, mirroring the backend.)
 */
(function () {
    'use strict';

    const HIERARCHY = {
        none: 0,
        viewer: 1,
        editor: 2,
        builder: 3,
        admin: 4,
    };

    function level(role) {
        return HIERARCHY[(role || '').toLowerCase()] || 0;
    }

    const ds = (document.body && document.body.dataset) || {};
    const appRole = (ds.appRole || 'admin').toLowerCase();
    const domainRole = (ds.domainRole || 'admin').toLowerCase();
    const isAppMode = ds.appMode === 'true';

    const permissions = Object.freeze({
        appRole,
        domainRole,
        isAppMode,
        isAdmin: appRole === 'admin',
        isViewer: domainRole === 'viewer',
        isEditor: domainRole === 'editor',
        isBuilder: domainRole === 'builder',

        /**
         * True when the caller's app role is at least *role*.
         * Admins always satisfy any app gate.
         */
        hasAppRole(role) {
            return level(appRole) >= level(role);
        },

        /**
         * True when the caller's domain role is at least *role*.
         * Falls back to the app role so admins satisfy domain gates
         * even without a per-domain entry, matching the backend
         * ``require(scope='domain')`` dependency.
         */
        hasDomainRole(role) {
            const need = level(role);
            return level(domainRole) >= need || level(appRole) >= need;
        },
    });

    /*
     * Selector matching every interactive design surface (mapping map,
     * ontology map, Business Views OntoViz canvas, …) where right-
     * click menus would trigger writes. We block ``contextmenu`` on
     * descendants during the capture phase, which pre-empts the per-
     * widget D3/Canvas handlers and swallows the browser's default
     * menu — so we don't need to chase every individual handler.
     */
    const READ_ONLY_DESIGN_SURFACE_SELECTOR =
        '#mapping-map-container, #ontology-map-container, '
        + '.ovz-canvas, .ontoviz-container';

    function installReadOnlyContextMenuBlocker() {
        if (window._readOnlyContextMenuBlockerInstalled) return;
        window._readOnlyContextMenuBlockerInstalled = true;
        document.addEventListener('contextmenu', function (event) {
            const target = event.target;
            if (target && target.closest
                && target.closest(READ_ONLY_DESIGN_SURFACE_SELECTOR)) {
                event.preventDefault();
                event.stopPropagation();
                if (typeof event.stopImmediatePropagation === 'function') {
                    event.stopImmediatePropagation();
                }
            }
        }, true);
    }

    /*
     * Viewer-mode stamping: when a non-admin caller has a viewer role
     * on the loaded domain we want the same UI lockdown that
     * ``read-only-version`` already provides for inactive versions —
     * disabled write buttons, neutralised form fields, hidden OntoViz
     * toolbars, blocked design-surface context menus. The CSS gates
     * use ``:is(.read-only-version, .role-viewer)`` so this is a
     * single class flip rather than a per-button JS sweep.
     *
     * Only fires in app mode (Databricks Apps) and never for app
     * admins, who keep full UI access.
     */
    /*
     * Effective role tooltip: explains what the active role can and
     * cannot do, so hovering the navbar pill is enough to understand
     * the current permission scope without opening Settings.
     */
    const ROLE_TOOLTIPS = {
        admin: 'Admin — full access to all domains, settings, and registry.',
        builder: 'Builder — full read/write access on this domain '
            + '(ontology, mappings, twins, dashboards).',
        editor: 'Editor — can read and edit this domain\'s ontology and '
            + 'mappings.',
        viewer: 'Viewer — read-only access on this domain. Ask a domain '
            + 'admin for editor or builder rights to make changes.',
    };

    /*
     * The "effective" role drives both the badge text and the gating
     * decisions on the page:
     *
     *   - app admins always see ADMIN (they bypass per-domain gates,
     *     mirroring the backend ``require(scope='domain')`` fall-back);
     *   - everyone else shows their domain role (viewer/editor/builder).
     *
     * We resolve it once here and re-use it for the navbar pill and
     * the optional ``body.role-viewer`` stamp.
     */
    function effectiveRole() {
        if (permissions.isAdmin) return 'admin';
        if (['viewer', 'editor', 'builder'].includes(domainRole)) {
            return domainRole;
        }
        return '';
    }

    /*
     * Stamp the body class for viewers (so every gate in
     * ``permissions.css`` neutralises write surfaces) and reveal the
     * navbar role pill. Both run on every page load in app mode; in
     * local-dev mode we skip the pill since there's no real role
     * resolution and the user can do everything anyway.
     */
    function applyRoleIndicators() {
        if (!isAppMode) return;

        const role = effectiveRole();
        if (!role) return;

        if (role === 'viewer') {
            document.body.classList.add('role-viewer');
            installReadOnlyContextMenuBlocker();
        }

        showRoleNavBadge(role);
    }

    /*
     * Reveal the small role pill in the navbar (right side, before the
     * warehouse / notifications icons). The label is the role in
     * upper-case (ADMIN / BUILDER / EDITOR / VIEWER) and the tooltip
     * spells out what that role can do. ``version-check.js`` calls
     * ``annotateRoleNavBadge`` to append an older-version note when
     * the loaded domain version is not the active one.
     *
     * The DOM lives in ``base.html`` (``#roleNavBadgeItem``); we just
     * flip its display, fill in the label, and (re-)bind the tooltip.
     */
    function showRoleNavBadge(role) {
        const item = document.getElementById('roleNavBadgeItem');
        const label = document.getElementById('roleNavBadgeLabel');
        const badge = document.getElementById('roleNavBadge');
        if (!item || !label || !badge) return;

        label.textContent = role.toUpperCase();
        item.style.setProperty('display', 'flex', 'important');

        const tooltip = ROLE_TOOLTIPS[role] || role.toUpperCase();
        setBadgeTooltip(badge, tooltip);
    }

    /*
     * Append (or replace) extra context on the role pill's tooltip.
     * Used by ``version-check.js`` when the user is on an older,
     * read-only version: the role itself is unchanged but the user
     * can't actually write because the version isn't active.
     */
    function annotateRoleNavBadge(extra) {
        const badge = document.getElementById('roleNavBadge');
        if (!badge || !extra) return;
        const role = effectiveRole();
        const base = ROLE_TOOLTIPS[role] || '';
        const combined = base ? base + '<br><br>' + escapeHtml(extra) : escapeHtml(extra);
        setBadgeTooltip(badge, combined);
    }

    /*
     * (Re-)bind a Bootstrap tooltip on the role pill. Tooltips are
     * rendered as HTML so we can break the role description and the
     * inactive-version note across two lines. ``base`` callers go
     * through ``escapeHtml`` already (or are static strings we trust),
     * so we don't double-escape here.
     */
    function setBadgeTooltip(badge, tooltip) {
        badge.setAttribute('title', tooltip);
        badge.setAttribute('data-bs-original-title', tooltip);
        if (window.bootstrap && window.bootstrap.Tooltip) {
            const existing = window.bootstrap.Tooltip.getInstance(badge);
            if (existing) existing.dispose();
            new window.bootstrap.Tooltip(badge, { html: true });
        }
    }

    function escapeHtml(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    /*
     * One-stop helper for "can the caller mutate the loaded ontology?".
     * Combines the two read-only signals so write surfaces in
     * ``ontology-shared-panels.js`` / ``ontology-core.js`` only need a
     * single check instead of duplicating the logic per call site:
     *
     *   - ``window.isActiveVersion === false`` → older inactive
     *     version, set asynchronously by ``version-check.js``;
     *   - domain-role below editor → viewer (or none) on the current
     *     domain. Admins always satisfy domain gates per the backend
     *     ``require(scope='domain')`` fall-back.
     *
     * ``window.isActiveVersion`` defaults to ``true``, so this returns
     * ``true`` until ``checkVersionStatus`` has had a chance to run.
     * Viewer mode never had that delay because the role is stamped
     * synchronously on <body>.
     */
    function canEditOntology() {
        if (window.isActiveVersion === false) return false;
        return permissions.hasDomainRole('editor');
    }

    window.OB = window.OB || {};
    window.OB.permissions = permissions;
    window.OB.canEditOntology = canEditOntology;
    window.OB.installReadOnlyContextMenuBlocker = installReadOnlyContextMenuBlocker;
    window.OB.showRoleNavBadge = showRoleNavBadge;
    window.OB.annotateRoleNavBadge = annotateRoleNavBadge;

    // ``base.html`` loads this with ``defer`` so <body> is fully parsed
    // by now, but be safe in case the load order changes.
    if (document.body) {
        applyRoleIndicators();
    } else {
        document.addEventListener('DOMContentLoaded', applyRoleIndicators);
    }
})();
