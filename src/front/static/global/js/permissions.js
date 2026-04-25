/**
 * OntoBricks - Permissions (server-injected, synchronous)
 *
 * The server stamps the caller's resolved roles on the <body> tag in
 * base.html (data-app-role / data-domain-role / data-app-mode), so the
 * page can gate UI elements without an extra HTTP round-trip on first
 * paint. This module reads those attributes once at parse time and
 * exposes a frozen ``window.OB.permissions`` namespace consumed by:
 *
 *   - navbar.js               (admin-only nav items)
 *   - version-check.js        (viewer banner / domain-role checks)
 *   - registry.js, ...        (declarative data-requires gates)
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

    window.OB = window.OB || {};
    window.OB.permissions = permissions;
})();
