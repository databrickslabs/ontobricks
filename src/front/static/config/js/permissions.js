/**
 * OntoBricks - permissions.js
 * Permissions tab logic for the Settings page.
 * Handles: loading current user role, showing/hiding the tab,
 * listing permission entries, adding/removing users and groups,
 * and inline role changes.
 */
document.addEventListener('DOMContentLoaded', function () {

    let currentUserRole = '';

    initPermissionsTab();

    // -----------------------------------------------------------------
    //  Bootstrap: load permissions list immediately (settings page is
    //  already admin-only via middleware)
    // -----------------------------------------------------------------

    function initPermissionsTab() {
        currentUserRole = 'admin';
        loadPermissionsList();
    }

    // -----------------------------------------------------------------
    //  Permission list
    // -----------------------------------------------------------------

    async function loadPermissionsList() {
        const container = document.getElementById('permissionsTableContainer');
        if (!container) return;

        container.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading permissions...</div>';

        try {
            const resp = await fetch('/settings/permissions', { credentials: 'same-origin' });
            const data = await resp.json();

            if (!data.success) {
                container.innerHTML =
                    '<div class="text-muted small py-3"><i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                    esc(data.message || 'Could not load permissions') + '</div>';
                return;
            }

            const perms = data.permissions || [];
            if (perms.length === 0) {
                container.innerHTML =
                    '<div class="text-muted small py-4 text-center">' +
                    '<i class="bi bi-shield me-1"></i> No permissions configured yet. Click <strong>Add</strong> to grant access.</div>';
                return;
            }

            let html =
                '<div class="table-responsive">' +
                '<table class="table table-sm table-hover align-middle mb-0">' +
                '<thead><tr>' +
                '<th class="ps-3">Principal</th>' +
                '<th>Display Name</th>' +
                '<th>Type</th>' +
                '<th style="width:10rem;">Role</th>' +
                '<th class="text-end pe-3" style="width:3rem;"></th>' +
                '</tr></thead><tbody>';

            perms.forEach(function (p) {
                const icon = p.principal_type === 'group' ? 'bi-people-fill' : 'bi-person-fill';
                const roleSelect =
                    '<select class="form-select form-select-sm perm-role-select" data-principal="' + esc(p.principal) + '" ' +
                    'data-type="' + esc(p.principal_type) + '" data-display="' + esc(p.display_name) + '">' +
                    '<option value="viewer"' + (p.role === 'viewer' ? ' selected' : '') + '>Viewer</option>' +
                    '<option value="editor"' + (p.role === 'editor' ? ' selected' : '') + '>Editor</option>' +
                    '</select>';

                html +=
                    '<tr>' +
                    '<td class="ps-3 fw-semibold text-nowrap"><i class="bi ' + icon + ' me-1 text-primary"></i>' + esc(p.principal) + '</td>' +
                    '<td class="text-muted text-truncate" style="max-width:200px;">' + esc(p.display_name || '') + '</td>' +
                    '<td><span class="badge bg-light text-dark border">' + esc(p.principal_type) + '</span></td>' +
                    '<td>' + roleSelect + '</td>' +
                    '<td class="text-end pe-3">' +
                    '<button type="button" class="btn btn-sm btn-outline-danger border-0 perm-delete-btn" ' +
                    'data-principal="' + esc(p.principal) + '" title="Remove"><i class="bi bi-trash"></i></button>' +
                    '</td></tr>';
            });

            html += '</tbody></table></div>';
            container.innerHTML = html;

            container.querySelectorAll('.perm-role-select').forEach(function (sel) {
                sel.addEventListener('change', function () {
                    changePermissionRole(
                        sel.dataset.principal,
                        sel.dataset.type,
                        sel.dataset.display,
                        sel.value
                    );
                });
            });

            container.querySelectorAll('.perm-delete-btn').forEach(function (btn) {
                btn.addEventListener('click', function (e) {
                    e.stopPropagation();
                    deletePermission(btn.dataset.principal);
                });
            });
        } catch (e) {
            console.error('Error loading permissions:', e);
            container.innerHTML =
                '<div class="text-danger small py-3"><i class="bi bi-x-circle me-1"></i> Error loading permissions</div>';
        }
    }

    // -----------------------------------------------------------------
    //  Change role inline
    // -----------------------------------------------------------------

    async function changePermissionRole(principal, principalType, displayName, newRole) {
        try {
            const resp = await fetch('/settings/permissions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({
                    principal: principal,
                    principal_type: principalType,
                    display_name: displayName,
                    role: newRole
                })
            });
            const data = await resp.json();
            if (data.success) {
                showNotification('Role updated for ' + principal, 'success', 2000);
            } else {
                showNotification('Error: ' + data.message, 'error');
                loadPermissionsList();
            }
        } catch (e) {
            showNotification('Error updating role: ' + e.message, 'error');
            loadPermissionsList();
        }
    }

    // -----------------------------------------------------------------
    //  Delete permission
    // -----------------------------------------------------------------

    async function deletePermission(principal) {
        const confirmed = await showConfirmDialog({
            title: 'Remove Permission',
            message: 'Remove access for "' + principal + '"? They will no longer be able to use OntoBricks.',
            confirmText: 'Remove',
            confirmClass: 'btn-danger',
            icon: 'trash'
        });
        if (!confirmed) return;

        try {
            const resp = await fetch('/settings/permissions/' + encodeURIComponent(principal), {
                method: 'DELETE',
                credentials: 'same-origin'
            });
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message, 'success', 2000);
                loadPermissionsList();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (e) {
            showNotification('Error removing permission: ' + e.message, 'error');
        }
    }

    // -----------------------------------------------------------------
    //  Add permission modal
    // -----------------------------------------------------------------

    document.getElementById('btnAddPermission')?.addEventListener('click', function () {
        document.getElementById('permPrincipalSearch').value = '';
        document.getElementById('permPrincipalValue').value = '';
        document.getElementById('permDisplayName').value = '';
        document.getElementById('permRole').value = 'viewer';
        document.getElementById('permPrincipalType').value = 'user';
        const results = document.getElementById('permSearchResults');
        if (results) { results.style.display = 'none'; results.innerHTML = ''; }
        const sel = document.getElementById('permSelectedPrincipal');
        if (sel) sel.style.display = 'none';

        const modal = new bootstrap.Modal(document.getElementById('addPermissionModal'));
        modal.show();
    });

    const searchInput = document.getElementById('permPrincipalSearch');
    const searchBtn = document.getElementById('btnSearchPrincipals');
    const resultsDiv = document.getElementById('permSearchResults');
    const typeSelect = document.getElementById('permPrincipalType');
    const selectedDiv = document.getElementById('permSelectedPrincipal');
    const selectedBadge = document.getElementById('permSelectedBadge');
    const clearBtn = document.getElementById('btnClearSelection');

    async function doSearch() {
        const query = searchInput ? searchInput.value.trim() : '';
        if (query.length < 2) {
            showNotification('Please enter at least 2 characters to search', 'warning');
            return;
        }
        const ptype = typeSelect ? typeSelect.value : 'user';

        if (resultsDiv) {
            resultsDiv.innerHTML = '<div class="text-center text-muted small py-2"><span class="spinner-border spinner-border-sm me-1"></span> Searching...</div>';
            resultsDiv.style.display = '';
        }

        try {
            const resp = await fetch('/settings/permissions/search?q=' + encodeURIComponent(query) + '&type=' + ptype, { credentials: 'same-origin' });
            const data = await resp.json();
            if (!data.success || !data.results || data.results.length === 0) {
                resultsDiv.innerHTML = '<div class="text-muted small py-2 px-2"><i class="bi bi-info-circle me-1"></i>No results found for "' + esc(query) + '"</div>';
                return;
            }
            renderSearchResults(data.results, ptype);
        } catch (e) {
            console.error('Error searching principals:', e);
            if (resultsDiv) resultsDiv.innerHTML = '<div class="text-danger small py-2 px-2">Search failed</div>';
        }
    }

    function renderSearchResults(results, ptype) {
        if (!resultsDiv) return;
        resultsDiv.innerHTML = '';

        results.forEach(function (item) {
            const isGroup = ptype === 'group';
            const principal = isGroup ? item.display_name : item.email;
            const display = item.display_name || principal;
            const icon = isGroup ? 'bi-people-fill' : 'bi-person-fill';
            const label = isGroup ? display : (item.email + (display !== item.email ? ' — ' + display : ''));
            const active = item.active !== false;

            const row = document.createElement('a');
            row.href = '#';
            row.className = 'list-group-item list-group-item-action small py-1 px-2 d-flex align-items-center border-0' + (active ? '' : ' text-muted');
            row.innerHTML = '<i class="bi ' + icon + ' me-2 text-primary"></i><span class="text-truncate">' + esc(label) + '</span>' +
                (active ? '' : '<span class="badge bg-light text-muted ms-auto border">inactive</span>');

            row.addEventListener('click', function (e) {
                e.preventDefault();
                selectPrincipal(principal, display, label);
            });
            resultsDiv.appendChild(row);
        });
        resultsDiv.style.display = '';
    }

    function selectPrincipal(principal, display, label) {
        document.getElementById('permPrincipalValue').value = principal;
        document.getElementById('permDisplayName').value = display;
        if (selectedBadge) selectedBadge.textContent = label;
        if (selectedDiv) selectedDiv.style.display = '';
        if (resultsDiv) resultsDiv.style.display = 'none';
        if (searchInput) searchInput.value = '';
    }

    if (clearBtn) {
        clearBtn.addEventListener('click', function () {
            document.getElementById('permPrincipalValue').value = '';
            document.getElementById('permDisplayName').value = '';
            if (selectedDiv) selectedDiv.style.display = 'none';
        });
    }

    if (searchBtn) {
        searchBtn.addEventListener('click', doSearch);
    }
    if (searchInput) {
        searchInput.addEventListener('keydown', function (e) {
            if (e.key === 'Enter') { e.preventDefault(); doSearch(); }
        });
    }

    if (typeSelect) {
        typeSelect.addEventListener('change', function () {
            if (searchInput) searchInput.value = '';
            document.getElementById('permPrincipalValue').value = '';
            document.getElementById('permDisplayName').value = '';
            if (resultsDiv) { resultsDiv.style.display = 'none'; resultsDiv.innerHTML = ''; }
            if (selectedDiv) selectedDiv.style.display = 'none';
        });
    }

    document.getElementById('btnApplyPermission')?.addEventListener('click', async function () {
        const principal = document.getElementById('permPrincipalValue').value || document.getElementById('permPrincipalSearch').value.trim();
        const principalType = document.getElementById('permPrincipalType').value;
        const displayName = document.getElementById('permDisplayName').value || principal;
        const role = document.getElementById('permRole').value;

        if (!principal) {
            showNotification('Please select a user or group', 'warning');
            return;
        }

        const btn = document.getElementById('btnApplyPermission');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Adding...';

        try {
            const resp = await fetch('/settings/permissions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ principal: principal, principal_type: principalType, display_name: displayName, role: role })
            });
            const data = await resp.json();

            if (data.success) {
                showNotification('Permission added for ' + principal, 'success', 2000);
                bootstrap.Modal.getInstance(document.getElementById('addPermissionModal'))?.hide();
                loadPermissionsList();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (e) {
            showNotification('Error adding permission: ' + e.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-check-circle me-1"></i> Add';
        }
    });

    // -----------------------------------------------------------------
    //  Refresh button
    // -----------------------------------------------------------------

    document.getElementById('btnRefreshPermissions')?.addEventListener('click', function () {
        loadPermissionsList();
    });

    // -----------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------

    function esc(str) {
        if (!str) return '';
        const d = document.createElement('div');
        d.textContent = str;
        return d.innerHTML;
    }
});
