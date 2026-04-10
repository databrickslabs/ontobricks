/**
 * OntoBricks - project-versions.js
 * Version list management for the Project > Versions submenu.
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
        const response = await fetch('/project/versions-list', { credentials: 'same-origin' });
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

            const statusBadge = v.is_active
                ? '<span class="badge bg-success"><i class="bi bi-unlock me-1"></i>Active</span>'
                : '<span class="badge bg-warning text-dark"><i class="bi bi-lock me-1"></i>Inactive</span>';

            const mcpSwitch = '<div class="form-check form-switch d-flex justify-content-center mb-0">'
                + '<input class="form-check-input version-mcp-toggle" type="checkbox"'
                + ' data-version="' + escapeHtml(v.version) + '"'
                + (v.mcp_enabled ? ' checked' : '')
                + ' title="API / MCP exposure (only one version at a time)">'
                + '</div>';

            const loadBtn = v.is_current
                ? ''
                : '<button class="btn btn-sm btn-outline-primary" onclick="loadVersionFromList(\''
                  + escapeHtml(v.version) + '\')" title="Load this version">'
                  + '<i class="bi bi-box-arrow-in-down"></i></button>';

            row.innerHTML = '<td class="text-center">' + versionBadge + '</td>'
                + '<td class="small">' + escapeHtml(v.description || '—') + '</td>'
                + '<td>' + mcpSwitch + '</td>'
                + '<td class="small text-muted">' + escapeHtml(v.author || '') + '</td>'
                + '<td class="text-center">' + statusBadge + '</td>'
                + '<td class="text-center">' + loadBtn + '</td>';

            tbody.appendChild(row);
        });

        wrapper.style.display = '';
        _versionsLoaded = true;

        tbody.querySelectorAll('.version-mcp-toggle').forEach(function (toggle) {
            toggle.addEventListener('change', function () {
                onMcpToggleChange(this);
            });
        });
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

        const statusData = await fetch('/project/version-status', { credentials: 'same-origin' }).then(r => r.json());
        const projectFolder = (statusData && statusData.project_folder) || '';
        if (!projectFolder) {
            showNotification('Cannot determine project folder', 'error');
            return;
        }

        const response = await fetch('/project/load-from-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ project: projectFolder, version: version }),
            credentials: 'same-origin'
        });
        const data = await response.json();

        if (data.success) {
            showNotification(data.message || 'Version loaded!', 'success');
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

        const response = await fetch('/project/create-version', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        const data = await response.json();

        if (data.success) {
            showNotification('Version ' + data.new_version + ' created!', 'success');
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

        const statusData = await fetch('/project/version-status', { credentials: 'same-origin' }).then(r => r.json());
        const projectFolder = (statusData && statusData.project_folder) || '';
        const currentVersion = (statusData && statusData.version) || '1';

        if (!projectFolder) {
            showNotification('Project must be saved to the registry first', 'warning');
            return;
        }

        const response = await fetch('/project/load-from-uc', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ project: projectFolder, version: currentVersion }),
            credentials: 'same-origin'
        });
        const data = await response.json();

        if (data.success) {
            showNotification('Version ' + currentVersion + ' reloaded!', 'success');
            window.location.reload();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (err) {
        showNotification('Error: ' + err.message, 'error');
    }
}

async function onMcpToggleChange(toggle) {
    const version = toggle.dataset.version;
    const enabled = toggle.checked;

    try {
        const resp = await fetch('/project/set-version-mcp', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ version: version, enabled: enabled }),
            credentials: 'same-origin'
        });
        const data = await resp.json();

        if (!data.success) {
            toggle.checked = !enabled;
            showNotification('Error: ' + data.message, 'error');
            return;
        }

        if (enabled) {
            document.querySelectorAll('.version-mcp-toggle').forEach(function (other) {
                if (other !== toggle) other.checked = false;
            });
        }

        showNotification('API/MCP ' + (enabled ? 'enabled' : 'disabled') + ' for v' + version, 'success');
    } catch (err) {
        toggle.checked = !enabled;
        showNotification('Error: ' + err.message, 'error');
    }
}

function escapeHtml(text) {
    var div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
