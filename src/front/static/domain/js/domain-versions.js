/**
 * OntoBricks - domain-versions.js
 * Version list management for the Domain > Versions submenu.
 */

const VERSION_STATUS_MAP = {
    'DRAFT': {
        cls: 'bg-warning-subtle text-dark border-warning',
        icon: 'pencil',
        label: 'Draft'
    },
    'IN-REVIEW': {
        cls: 'bg-info-subtle text-dark border-info',
        icon: 'eye',
        label: 'In Review'
    },
    'PUBLISHED': {
        cls: 'bg-success-subtle text-dark border-success',
        icon: 'broadcast',
        label: 'Published'
    }
};

function setVersionsState(visibleState) {
    const loading = document.getElementById('versionsLoading');
    const empty = document.getElementById('versionsEmpty');
    const error = document.getElementById('versionsError');
    const list = document.getElementById('versionsCardList');

    [
        ['loading', loading],
        ['empty', empty],
        ['error', error],
        ['list', list]
    ].forEach(function (entry) {
        if (entry[1]) entry[1].classList.toggle('ob-hidden', entry[0] !== visibleState);
    });
}

async function loadVersionsList(forceRefresh = false) {
    const list = document.getElementById('versionsCardList');
    const errorMessage = document.getElementById('versionsErrorMessage');
    if (!list || !errorMessage) return;

    setVersionsState('loading');

    try {
        const endpoint = '/domain/versions-list' + (forceRefresh ? '?refresh=true' : '');
        const response = await fetch(endpoint, { credentials: 'same-origin' });
        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.message || 'Failed to load versions');
        }

        if (!data.versions || data.versions.length === 0) {
            list.replaceChildren();
            setVersionsState('empty');
            return;
        }

        list.innerHTML = data.versions.map(function (version) {
            return renderVersionCard(version, data.domain_folder || '');
        }).join('');
        setVersionsState('list');
    } catch (err) {
        errorMessage.textContent = err.message || 'Failed to load versions';
        setVersionsState('error');
        showNotification(errorMessage.textContent, 'error');
    }
}

function renderVersionCard(version, domainFolder) {
    const status = VERSION_STATUS_MAP[String(version.status || 'DRAFT').toUpperCase()]
        || VERSION_STATUS_MAP.DRAFT;
    const transitions = (version.transitions || []).map(function (transition) {
        const disabled = transition.enabled ? '' : ' disabled';
        const reason = transition.blocked_reason
            ? ' title="' + escapeHtml(transition.blocked_reason) + '"'
            : '';
        return '<button type="button" class="btn btn-sm btn-outline-primary"'
            + ' data-action="transition"'
            + ' data-version="' + escapeHtml(version.version) + '"'
            + ' data-domain="' + escapeHtml(domainFolder) + '"'
            + ' data-target-status="' + escapeHtml(transition.target_status) + '"'
            + disabled + reason + '>'
            + escapeHtml(transition.label) + '</button>';
    }).join('');
    const load = version.is_current ? '' :
        '<button type="button" class="btn btn-sm btn-outline-primary"'
        + ' data-action="load" data-version="' + escapeHtml(version.version) + '">'
        + '<i class="bi bi-box-arrow-in-down me-1"></i>Load</button>';
    let deletion = '';
    if (version.delete_control_visible) {
        const disabled = version.can_delete ? '' : ' disabled';
        const title = escapeHtml(
            version.delete_block_reason || ('Delete version v' + version.version)
        );
        deletion = '<span tabindex="0" title="' + title + '">'
            + '<button type="button" class="btn btn-sm btn-outline-danger"'
            + ' data-action="delete" data-version="' + escapeHtml(version.version) + '"'
            + disabled + '><i class="bi bi-trash me-1"></i>Delete</button></span>';
    }
    return '<article class="card dm-version-card'
        + (version.is_current ? ' is-loaded' : '') + '" role="listitem"'
        + ' aria-labelledby="version-title-' + escapeHtml(version.version) + '">'
        + '<div class="card-body">'
        + '<div class="dm-version-card-header">'
        + '<h5 class="mb-0" id="version-title-' + escapeHtml(version.version) + '">'
        + 'v' + escapeHtml(version.version) + '</h5>'
        + '<span class="badge border ' + status.cls + '"><i class="bi bi-'
        + status.icon + ' me-1"></i>' + status.label + '</span>'
        + (version.is_current ? '<span class="badge bg-primary">Loaded</span>' : '')
        + (version.is_active ? '<span class="badge bg-secondary">Latest</span>' : '')
        + '</div>'
        + '<p class="dm-version-card-description mt-2 mb-2">'
        + escapeHtml(version.description || 'No description') + '</p>'
        + '<div class="dm-version-card-meta mb-3">'
        + '<span><i class="bi bi-person me-1"></i>'
        + escapeHtml(version.author || 'Unknown author') + '</span>'
        + '<span><i class="bi bi-clock me-1"></i>'
        + escapeHtml(version.last_update || 'No update date') + '</span>'
        + '<span><i class="bi bi-hammer me-1"></i>'
        + escapeHtml(version.last_build || 'Not built') + '</span>'
        + '</div>'
        + '<div class="dm-version-card-actions">'
        + '<div class="dm-version-action-group">' + transitions + '</div>'
        + '<div class="dm-version-action-group">' + load + deletion + '</div>'
        + '</div></div></article>';
}

async function transitionVersion(domainFolder, version, targetStatus) {
    const confirmed = await showConfirmDialog({
        title: 'Update Lifecycle Status',
        message: 'Change version v' + escapeHtml(version) + ' to '
            + escapeHtml(targetStatus) + '?',
        confirmText: 'Update Status',
        confirmClass: 'btn-primary',
        icon: 'arrow-repeat'
    });
    if (!confirmed) return;

    const response = await fetch('/domain/set-version-status', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            domain_name: domainFolder,
            version: version,
            status: targetStatus
        })
    });
    const data = await response.json();
    if (!response.ok || !data.success) {
        throw new Error(data.message || 'Version status update failed');
    }
    showNotification(data.message || 'Version status updated', 'success');
    await loadVersionsList(true);
}

async function deleteVersionFromList(version) {
    const confirmed = await showConfirmDialog({
        title: 'Delete Version',
        message: 'Permanently delete version v' + escapeHtml(version)
            + ' and its Knowledge Store content? This cannot be undone.',
        confirmText: 'Delete Version',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
    if (!confirmed) return;

    const response = await fetch(
        '/domain/versions/' + encodeURIComponent(version),
        { method: 'DELETE', credentials: 'same-origin' }
    );
    const data = await response.json();
    if (!response.ok || !data.success) {
        if (response.status === 409) await loadVersionsList(true);
        throw new Error(data.message || 'Version deletion failed');
    }
    showNotification(data.message, 'success');
    await loadVersionsList(true);
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
    return window.createNewDomainVersion();
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

document.addEventListener('DOMContentLoaded', function () {
    const versionsCardList = document.getElementById('versionsCardList');
    const reloadButton = document.getElementById('btnReloadVersion');
    const addButton = document.getElementById('btnAddVersion');
    const retryButton = document.getElementById('versionsRetryBtn');

    if (versionsCardList) {
        versionsCardList.addEventListener('click', async function (event) {
            const actionButton = event.target.closest('button[data-action]');
            if (!actionButton || actionButton.disabled) return;

            try {
                if (actionButton.dataset.action === 'load') {
                    await loadVersionFromList(actionButton.dataset.version);
                } else if (actionButton.dataset.action === 'transition') {
                    await transitionVersion(
                        actionButton.dataset.domain,
                        actionButton.dataset.version,
                        actionButton.dataset.targetStatus
                    );
                } else if (actionButton.dataset.action === 'delete') {
                    await deleteVersionFromList(actionButton.dataset.version);
                }
            } catch (err) {
                showNotification(err.message || 'Version action failed', 'error');
            }
        });
    }
    if (reloadButton) reloadButton.addEventListener('click', reloadLastSavedVersion);
    if (addButton) addButton.addEventListener('click', addNewVersionFromList);
    if (retryButton) {
        retryButton.addEventListener('click', function () {
            loadVersionsList(true);
        });
    }
});
