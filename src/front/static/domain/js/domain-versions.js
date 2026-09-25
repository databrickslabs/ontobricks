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

        const cards = data.versions.map(function (version) {
            return renderVersionCard(version, data.domain_folder || '');
        });
        list.replaceChildren(...cards);
        setVersionsState('list');
    } catch (err) {
        errorMessage.textContent = err.message || 'Failed to load versions';
        setVersionsState('error');
        showNotification(errorMessage.textContent, 'error');
    }
}

function createVersionElement(tagName, className = '', text = null) {
    const element = document.createElement(tagName);
    if (className) element.className = className;
    if (text !== null) element.textContent = String(text);
    return element;
}

function prependVersionIcon(element, iconName) {
    const icon = createVersionElement('i', 'bi bi-' + iconName + ' me-1');
    icon.setAttribute('aria-hidden', 'true');
    element.prepend(icon);
}

function createVersionActionButton(options) {
    const button = createVersionElement(
        'button',
        'btn btn-sm ' + options.buttonClass,
        options.label
    );
    button.type = 'button';
    button.dataset.action = options.action;
    button.dataset.version = String(options.version);
    if (options.domain !== undefined) {
        button.dataset.domain = String(options.domain);
    }
    if (options.targetStatus !== undefined) {
        button.dataset.targetStatus = String(options.targetStatus);
    }
    button.disabled = Boolean(options.disabled);
    if (options.icon) prependVersionIcon(button, options.icon);
    return button;
}

function wrapDisabledVersionAction(button, reason) {
    if (!button.disabled) return button;

    const wrapper = createVersionElement('span', 'dm-version-action-blocked');
    wrapper.setAttribute('tabindex', '0');
    wrapper.setAttribute('title', String(reason || 'This action is unavailable.'));
    wrapper.append(button);
    return wrapper;
}

function renderVersionCard(version, domainFolder) {
    const status = VERSION_STATUS_MAP[String(version.status || 'DRAFT').toUpperCase()]
        || VERSION_STATUS_MAP.DRAFT;
    const versionValue = String(version.version || '');
    const article = createVersionElement(
        'article',
        'card dm-version-card' + (version.is_current ? ' is-loaded' : '')
    );
    article.setAttribute('role', 'listitem');
    article.setAttribute('aria-label', 'Version ' + versionValue);

    const body = createVersionElement('div', 'card-body');
    const header = createVersionElement('div', 'dm-version-card-header');
    const heading = createVersionElement('h5', 'mb-0', 'v' + versionValue);
    const statusBadge = createVersionElement(
        'span',
        'badge border ' + status.cls,
        status.label
    );
    prependVersionIcon(statusBadge, status.icon);
    header.append(heading, statusBadge);
    if (version.is_current) {
        header.append(createVersionElement('span', 'badge bg-primary', 'Loaded'));
    }
    if (version.is_active) {
        header.append(createVersionElement('span', 'badge bg-secondary', 'Latest'));
    }

    const description = createVersionElement(
        'p',
        'dm-version-card-description mt-2 mb-2',
        version.description || 'No description'
    );
    const metadata = createVersionElement('div', 'dm-version-card-meta mb-3');
    [
        ['person', version.author || 'Unknown author'],
        ['clock', version.last_update || 'No update date'],
        ['hammer', version.last_build || 'Not built']
    ].forEach(function (item) {
        const value = createVersionElement('span', '', item[1]);
        prependVersionIcon(value, item[0]);
        metadata.append(value);
    });

    const actions = createVersionElement('div', 'dm-version-card-actions');
    const transitionGroup = createVersionElement('div', 'dm-version-action-group');
    (version.transitions || []).forEach(function (transition) {
        const button = createVersionActionButton({
            action: 'transition',
            version: versionValue,
            domain: domainFolder,
            targetStatus: transition.target_status,
            label: transition.label,
            buttonClass: 'btn-outline-primary',
            disabled: !transition.enabled
        });
        transitionGroup.append(
            wrapDisabledVersionAction(button, transition.blocked_reason)
        );
    });

    const directActions = createVersionElement('div', 'dm-version-action-group');
    if (!version.is_current) {
        directActions.append(createVersionActionButton({
            action: 'load',
            version: versionValue,
            label: 'Load',
            icon: 'box-arrow-in-down',
            buttonClass: 'btn-outline-primary',
            disabled: false
        }));
    }

    if (version.delete_control_visible) {
        const title = String(
            version.delete_block_reason || ('Delete version v' + version.version)
        );
        const deleteWrapper = createVersionElement('span');
        const deleteButton = createVersionActionButton({
            action: 'delete',
            version: versionValue,
            label: 'Delete',
            icon: 'trash',
            buttonClass: 'btn-outline-danger',
            disabled: !version.can_delete
        });
        if (deleteButton.disabled) {
            deleteWrapper.className = 'dm-version-action-blocked';
            deleteWrapper.setAttribute('tabindex', '0');
            deleteWrapper.setAttribute('title', title);
        } else {
            deleteButton.setAttribute('title', title);
        }
        deleteWrapper.append(deleteButton);
        directActions.append(deleteWrapper);
    }

    actions.append(transitionGroup, directActions);
    body.append(header, description, metadata, actions);
    article.append(body);
    return article;
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

async function deleteVersionFromList(version, triggerButton = null) {
    const confirmed = await showConfirmDialog({
        title: 'Delete Version',
        message: 'Permanently delete version v' + escapeHtml(version)
            + ' and its Knowledge Store content? This cannot be undone.',
        confirmText: 'Delete Version',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
    if (!confirmed) {
        if (triggerButton
            && triggerButton.isConnected
            && !triggerButton.disabled
            && typeof triggerButton.focus === 'function') {
            triggerButton.focus();
        }
        return;
    }

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
                    await deleteVersionFromList(
                        actionButton.dataset.version,
                        actionButton
                    );
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
