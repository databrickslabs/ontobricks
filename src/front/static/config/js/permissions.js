/**
 * OntoBricks - permissions.js
 * Permissions tab of the Settings page.
 *
 * Read-only view of the Databricks App principals.  Management of
 * who can use the app happens in the Databricks UI.  Per-domain roles
 * (Viewer / Editor / Builder) live in Registry → Teams.
 */
document.addEventListener('DOMContentLoaded', function () {

    loadPermissionsList();

    const btnRefresh = document.getElementById('btnRefreshPermissions');
    if (btnRefresh) {
        btnRefresh.addEventListener('click', loadPermissionsList);
    }

    async function loadPermissionsList() {
        const container = document.getElementById('permissionsTableContainer');
        if (!container) return;

        container.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span>' +
            ' Loading app principals...</div>';

        try {
            const resp = await fetch('/settings/permissions', {
                credentials: 'same-origin',
            });
            const data = await resp.json();

            if (!data.success) {
                container.innerHTML =
                    '<div class="alert alert-danger small mb-0">' +
                    (data.error || 'Failed to load app principals') +
                    '</div>';
                return;
            }

            const users = Array.isArray(data.users) ? data.users : [];
            const groups = Array.isArray(data.groups) ? data.groups : [];

            if (users.length === 0 && groups.length === 0) {
                container.innerHTML =
                    '<div class="alert alert-warning small mb-0">' +
                    '<i class="bi bi-exclamation-triangle me-1"></i>' +
                    ' No Databricks App principals found. Grant access in the' +
                    ' Databricks App permissions page.' +
                    '</div>';
                return;
            }

            const rows = [];

            for (const u of users) {
                const email = escapeHtml(u.email || '');
                const display = escapeHtml(u.display_name || u.email || '');
                const perm = escapeHtml(
                    u.permission_level || u.permission || 'CAN_USE'
                );
                rows.push(
                    '<tr>' +
                    '<td><i class="bi bi-person me-1 text-secondary"></i>' +
                    email + '</td>' +
                    '<td>' + display + '</td>' +
                    '<td><span class="badge bg-secondary-subtle text-secondary-emphasis">user</span></td>' +
                    '<td><code>' + perm + '</code></td>' +
                    '</tr>'
                );
            }

            for (const g of groups) {
                const name = escapeHtml(g.display_name || g.id || '');
                const perm = escapeHtml(
                    g.permission_level || g.permission || 'CAN_USE'
                );
                rows.push(
                    '<tr>' +
                    '<td><i class="bi bi-people me-1 text-secondary"></i>' +
                    name + '</td>' +
                    '<td>' + name + '</td>' +
                    '<td><span class="badge bg-info-subtle text-info-emphasis">group</span></td>' +
                    '<td><code>' + perm + '</code></td>' +
                    '</tr>'
                );
            }

            container.innerHTML =
                '<div class="table-responsive">' +
                '<table class="table table-sm table-hover align-middle mb-0">' +
                '<thead class="table-light">' +
                '<tr>' +
                '<th style="width: 30%;">Principal</th>' +
                '<th style="width: 30%;">Display Name</th>' +
                '<th style="width: 15%;">Type</th>' +
                '<th style="width: 25%;">App Permission</th>' +
                '</tr>' +
                '</thead>' +
                '<tbody>' + rows.join('') + '</tbody>' +
                '</table>' +
                '</div>';
        } catch (err) {
            console.error('Error loading app principals:', err);
            container.innerHTML =
                '<div class="alert alert-danger small mb-0">' +
                'Network error: ' + escapeHtml(String(err)) +
                '</div>';
        }
    }

    function escapeHtml(text) {
        if (text == null) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }
});
