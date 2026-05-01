/**
 * OntoBricks - domain-versions.js
 * Version list management for the Domain > Versions submenu.
 */

let _versionsLoaded = false;

async function loadVersionsList() {
    const loading = document.getElementById('versionsLoading');
    const empty = document.getElementById('versionsEmpty');
    const error = document.getElementById('versionsError');
    const wrapper = document.getElementById('versionsTableWrapper');
    const tbody = document.getElementById('versionsTableBody');

    if (!loading || !tbody) return;

    loading.style.display = '';
    empty.style.display = 'none';
    error.style.display = 'none';
    wrapper.style.display = 'none';

    try {
        const response = await fetch('/domain/versions-list', { credentials: 'same-origin' });
        const data = await response.json();

        loading.style.display = 'none';

        if (!data.success) {
            document.getElementById('versionsErrorMessage').textContent = data.message || 'Failed to load versions';
            error.style.display = '';
            return;
        }

        if (!data.versions || data.versions.length === 0) {
            empty.style.display = '';
            return;
        }

        tbody.innerHTML = '';
        data.versions.forEach(function (v) {
            const row = document.createElement('tr');
            if (v.is_current) row.classList.add('table-primary');

            const versionBadge = v.is_current
                ? '<span class="badge bg-primary"><i class="bi bi-check-circle me-1"></i>v' + escapeHtml(v.version) + '</span>'
                : '<span class="badge bg-secondary">v' + escapeHtml(v.version) + '</span>';

            // MCP/API "active" flag is read-only here — changing it is only
            // supported from Registry → Browse (see registry.js).
            const mcpBadge = v.mcp_enabled
                ? '<span class="badge bg-success" title="This version is exposed via API and MCP">'
                + '<i class="bi bi-broadcast me-1"></i>Active</span>'
                : '<span class="text-muted small" title="Not the API/MCP-active version">—</span>';

            const loadBtn = v.is_current
                ? ''
                : '<button class="btn btn-sm btn-outline-primary" onclick="loadVersionFromList(\''
                  + escapeHtml(v.version) + '\')" title="Load this version">'
                  + '<i class="bi bi-box-arrow-in-down"></i></button>';

            row.innerHTML = '<td class="text-center">' + versionBadge + '</td>'
                + '<td class="small">' + escapeHtml(v.description || '—') + '</td>'
                + '<td class="text-center">' + mcpBadge + '</td>'
                + '<td class="small text-muted">' + escapeHtml(v.author || '') + '</td>'
                + '<td class="text-center">' + loadBtn + '</td>';

            tbody.appendChild(row);
        });

        wrapper.style.display = '';
        _versionsLoaded = true;
    } catch (err) {
        loading.style.display = 'none';
        document.getElementById('versionsErrorMessage').textContent = err.message;
        error.style.display = '';
    }
}

async function loadVersionFromList(version) {
    const confirmed = await showConfirmDialog({
        title: 'Load Version',
        message: 'Load version ' + version + '? Unsaved changes will be lost.',
        confirmText: 'Load Version',
        confirmClass: 'btn-primary',
        icon: 'box-arrow-in-down'
    });
    if (!confirmed) return;

    try {
        showNotification('Loading version ' + version + '…', 'info', 3000);

        const statusData = await fetch('/domain/version-status', { credentials: 'same-origin' }).then(r => r.json());
        const domainFolder = (statusData && (statusData.domain_folder || statusData.project_folder)) || '';
        if (!domainFolder) {
            showNotification('Cannot determine domain folder', 'error');
            return;
        }

        const response = await fetch('/domain/load-from-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain: domainFolder, version: version }),
            credentials: 'same-origin'
        });
        const data = await response.json();

        if (data.success) {
            showNotification(data.message || 'Version loaded!', 'success');
            if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
            window.location.reload();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (err) {
        showNotification('Error: ' + err.message, 'error');
    }
}

async function addNewVersionFromList() {
    const confirmed = await showConfirmDialog({
        title: 'Create New Version',
        message: 'This will copy the current version and increment the version number. Continue?',
        confirmText: 'Create Version',
        confirmClass: 'btn-primary',
        icon: 'plus-circle'
    });
    if (!confirmed) return;

    try {
        showNotification('Creating new version…', 'info', 2000);

        const response = await fetch('/domain/create-version', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        const data = await response.json();

        if (data.success) {
            showNotification('Version ' + data.new_version + ' created!', 'success');
            if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
            window.location.reload();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (err) {
        showNotification('Error: ' + err.message, 'error');
    }
}

async function reloadLastSavedVersion() {
    const confirmed = await showConfirmDialog({
        title: 'Reload Saved Version',
        message: 'This will reload the current version from Unity Catalog and discard ALL unsaved changes. Are you sure?',
        confirmText: 'Reload',
        confirmClass: 'btn-warning',
        icon: 'arrow-counterclockwise'
    });
    if (!confirmed) return;

    try {
        showNotification('Reloading saved version…', 'info', 3000);

        const statusData = await fetch('/domain/version-status', { credentials: 'same-origin' }).then(r => r.json());
        const domainFolder = (statusData && (statusData.domain_folder || statusData.project_folder)) || '';
        const currentVersion = (statusData && statusData.version) || '1';

        if (!domainFolder) {
            showNotification('Domain must be saved to the registry first', 'warning');
            return;
        }

        const response = await fetch('/domain/load-from-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain: domainFolder, version: currentVersion }),
            credentials: 'same-origin'
        });
        const data = await response.json();

        if (data.success) {
            showNotification('Version ' + currentVersion + ' reloaded!', 'success');
            if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
            window.location.reload();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (err) {
        showNotification('Error: ' + err.message, 'error');
    }
}

function escapeHtml(text) {
    var div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
