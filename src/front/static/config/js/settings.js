/**
 * OntoBricks - settings.js
 * Settings page JavaScript – tabbed layout with a single global Save button
 */

document.addEventListener('DOMContentLoaded', function () {

    let currentWarehouseId = null;
    let warehouseLocked = false;

    function escapeHtmlSettings(str) { return escapeHtml(str); }

    loadCurrentConfig();
    loadBaseUri();
    loadCurrentDefaultEmoji();
    loadRegistryCacheTtl();

    // =====================================================================
    //  DATABRICKS TAB
    // =====================================================================

    async function loadCurrentConfig() {
        try {
            const response = await fetch('/settings/current', { credentials: 'same-origin' });
            const data = await response.json();

            const tokenBadge = document.getElementById('tokenBadge');
            const authModeDisplay = document.getElementById('authModeDisplay');

            if (data.auth_mode === 'oauth') {
                tokenBadge.className = 'badge bg-success';
                tokenBadge.innerHTML = '<i class="bi bi-shield-check"></i> OAuth configured';
                authModeDisplay.textContent = data.token || '';
                document.getElementById('tokenHelp').textContent = 'Using OAuth Service Principal (Databricks Apps mode)';
            } else if ((data.auth_mode === 'token' || data.auth_mode === 'pat') && data.token) {
                tokenBadge.className = 'badge bg-success';
                tokenBadge.innerHTML = '<i class="bi bi-check-circle"></i> Token configured';
                authModeDisplay.textContent = '';
                document.getElementById('tokenHelp').textContent = data.from_env ? 'From environment variable' : 'From session';
            } else if (data.auth_mode === 'app') {
                tokenBadge.className = 'badge bg-success';
                tokenBadge.innerHTML = '<i class="bi bi-cloud-check"></i> Databricks App';
                authModeDisplay.textContent = '';
                document.getElementById('tokenHelp').textContent = 'Using Databricks Apps authentication';
            } else {
                tokenBadge.className = 'badge bg-danger';
                tokenBadge.innerHTML = '<i class="bi bi-x-circle"></i> Not configured';
                authModeDisplay.textContent = '';
                document.getElementById('tokenHelp').innerHTML = '<i class="bi bi-exclamation-triangle text-warning"></i> Set DATABRICKS_TOKEN or use Databricks Apps';
            }

            currentWarehouseId = data.warehouse_id;
            warehouseLocked = !!data.warehouse_locked;

            if (warehouseLocked) {
                const whSelect = document.getElementById('settingsWarehouseSelect');
                if (whSelect) {
                    whSelect.innerHTML = '<option value="' + escapeHtmlSettings(data.warehouse_id || '') + '" selected>'
                        + escapeHtmlSettings(data.warehouse_id || '(not set)') + '</option>';
                    whSelect.disabled = true;
                }
                const btnRefresh = document.getElementById('btnRefreshWarehouses');
                if (btnRefresh) btnRefresh.disabled = true;
                const whHelp = document.getElementById('warehouseHelp');
                if (whHelp) whHelp.innerHTML = '<i class="bi bi-lock-fill text-muted me-1"></i> Configured via Databricks App resource';
            } else {
                await loadWarehouseSelect(data.warehouse_id);
            }

            const hostDisplay = document.getElementById('currentHostDisplay');
            if (data.host) {
                hostDisplay.innerHTML = `<i class="bi bi-cloud text-success"></i> ${data.host}`;
            } else {
                hostDisplay.innerHTML = '<i class="bi bi-exclamation-circle text-warning"></i> Not configured';
            }

            if (data.from_env) {
                document.getElementById('envNotice').style.display = 'block';
            }
        } catch (error) {
            console.error('Error loading config:', error);
        }
    }

    async function loadWarehouseSelect(preselectId) {
        const select = document.getElementById('settingsWarehouseSelect');
        if (!select) return;

        try {
            const response = await fetch('/settings/warehouses', { credentials: 'same-origin' });
            const data = await response.json();

            select.innerHTML = '<option value="">-- Select a SQL Warehouse --</option>';

            if (data.warehouses && data.warehouses.length > 0) {
                data.warehouses.forEach(wh => {
                    const stateLabel = wh.state === 'RUNNING' ? ' (running)' : '';
                    const opt = document.createElement('option');
                    opt.value = wh.id;
                    opt.textContent = wh.name + stateLabel;
                    select.appendChild(opt);
                });
            } else if (data.error) {
                select.innerHTML = `<option value="">Error: ${data.error}</option>`;
            } else {
                select.innerHTML = '<option value="">No warehouses available</option>';
            }

            if (preselectId) {
                select.value = preselectId;
            }
        } catch (error) {
            console.error('Error loading warehouses:', error);
            select.innerHTML = '<option value="">Error loading warehouses</option>';
        }
    }

    document.getElementById('btnRefreshWarehouses')?.addEventListener('click', () => loadWarehouseSelect(currentWarehouseId));

    document.getElementById('btnTestConnection')?.addEventListener('click', async function () {
        const whId = document.getElementById('settingsWarehouseSelect').value || currentWarehouseId;
        const resultDiv = document.getElementById('connectionResult');

        if (!whId) {
            showNotification('Please select a SQL Warehouse first', 'warning');
            return;
        }

        resultDiv.style.display = 'block';
        resultDiv.innerHTML = '<div class="alert alert-info"><i class="bi bi-hourglass-split"></i> Testing connection...</div>';

        try {
            const response = await fetch('/settings/test-connection', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ warehouse_id: whId })
            });
            const result = await response.json();

            if (result.success) {
                resultDiv.innerHTML = `<div class="alert alert-success"><i class="bi bi-check-circle"></i> ${result.message}</div>`;
            } else {
                resultDiv.innerHTML = `<div class="alert alert-danger"><i class="bi bi-x-circle"></i> ${result.message}</div>`;
            }
        } catch (error) {
            resultDiv.innerHTML = `<div class="alert alert-danger"><i class="bi bi-x-circle"></i> Error: ${error.message}</div>`;
        }
    });

    // =====================================================================
    //  GLOBAL TAB – Base URI
    // =====================================================================

    async function loadBaseUri() {
        try {
            const response = await fetch('/settings/get-base-uri', { credentials: 'same-origin' });
            const result = await response.json();
            if (result.success && result.base_uri) {
                document.getElementById('baseUriDefault').value = result.base_uri;
            }
        } catch (error) {
            console.log('Using default base URI');
        }
    }

    // =====================================================================
    //  GLOBAL TAB – Registry Cache TTL
    // =====================================================================

    async function loadRegistryCacheTtl() {
        try {
            const resp = await fetch('/settings/get-registry-cache-ttl', { credentials: 'same-origin' });
            const result = await resp.json();
            if (result.success && result.registry_cache_ttl != null) {
                document.getElementById('registryCacheTtl').value = result.registry_cache_ttl;
            }
        } catch (error) {
            console.log('Using default registry cache TTL');
        }
    }

    // =====================================================================
    //  GLOBAL TAB – Default Emoji Picker (uses shared EmojiPicker module)
    // =====================================================================

    async function loadCurrentDefaultEmoji() {
        try {
            const response = await fetch('/settings/get-default-emoji', { credentials: 'same-origin' });
            const result = await response.json();
            if (result.success && result.emoji) {
                document.getElementById('currentDefaultEmoji').textContent = result.emoji;
            }
        } catch (error) {
            console.log('Using default emoji');
        }
    }

    async function selectDefaultEmoji(emoji) {
        try {
            const response = await fetch('/settings/set-default-emoji', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ emoji })
            });
            const result = await response.json();
            if (result.success) {
                document.getElementById('currentDefaultEmoji').textContent = emoji;
                showNotification('Default class icon updated to ' + emoji, 'success', 2000);
            } else {
                showNotification('Error: ' + result.message, 'error');
            }
        } catch (error) {
            showNotification('Error saving default emoji: ' + error.message, 'error');
        }
    }

    const changeBtn = document.getElementById('changeDefaultEmoji');
    if (changeBtn) {
        EmojiPicker.create({
            triggerEl:   changeBtn,
            previewEl:   document.getElementById('currentDefaultEmoji'),
            containerEl: document.getElementById('defaultEmojiPickerMount'),
            showSearch:  false,
            onSelect:    function (emoji) { selectDefaultEmoji(emoji); }
        });
    }

    // =====================================================================
    //  LADYBUGDB TAB
    // =====================================================================

    let ladybugFilesLoaded = false;

    document.getElementById('tab-ladybugdb')?.addEventListener('shown.bs.tab', () => {
        if (!ladybugFilesLoaded) loadLadybugFiles();
    });

    document.getElementById('btnRefreshLadybugFiles')?.addEventListener('click', () => loadLadybugFiles());

    async function loadLadybugFiles() {
        const container = document.getElementById('ladybugFilesContainer');
        if (!container) return;

        container.innerHTML = '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading files...</div>';

        try {
            const resp = await fetch('/settings/ladybugdb/files', { credentials: 'same-origin' });
            const data = await resp.json();

            if (!data.success) {
                container.innerHTML = '<div class="text-muted small py-3">' +
                    '<i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                    escapeHtmlSettings(data.message || 'Could not list files') + '</div>';
                return;
            }

            if (!data.files || data.files.length === 0) {
                container.innerHTML = '<div class="text-muted small py-3 text-center">' +
                    '<i class="bi bi-folder"></i> No files in <code>' +
                    escapeHtmlSettings(data.base_dir) + '</code></div>';
                ladybugFilesLoaded = true;
                return;
            }

            let html = '<div class="table-responsive">' +
                '<table class="table table-sm table-hover align-middle mb-0">' +
                '<thead><tr>' +
                    '<th class="ps-3">Name</th>' +
                    '<th class="text-end" style="width:7rem;">Size</th>' +
                    '<th class="text-end" style="width:13rem;">Last Modified</th>' +
                    '<th class="text-end pe-3" style="width:3rem;"></th>' +
                '</tr></thead><tbody>';

            data.files.forEach(f => {
                const icon = f.is_dir
                    ? '<i class="bi bi-folder-fill text-warning me-1"></i>'
                    : '<i class="bi bi-file-earmark me-1 text-secondary"></i>';
                const deleteBtn = '<button type="button" class="btn btn-sm btn-outline-danger border-0 ladybug-delete-btn" ' +
                    'data-name="' + escapeHtmlSettings(f.name) + '" title="Delete ' + escapeHtmlSettings(f.name) + '">' +
                    '<i class="bi bi-trash"></i></button>';
                html += '<tr>' +
                    '<td class="ps-3 font-monospace">' + icon + escapeHtmlSettings(f.name) + '</td>' +
                    '<td class="text-end text-muted small">' + escapeHtmlSettings(f.size_display) + '</td>' +
                    '<td class="text-end text-muted small">' + escapeHtmlSettings(f.modified_display) + '</td>' +
                    '<td class="text-end pe-3">' + deleteBtn + '</td>' +
                '</tr>';
            });

            html += '</tbody></table></div>';
            container.innerHTML = html;
            ladybugFilesLoaded = true;

            container.querySelectorAll('.ladybug-delete-btn').forEach(btn => {
                btn.addEventListener('click', () => deleteLadybugFile(btn.dataset.name));
            });
        } catch (e) {
            console.error('Error loading LadybugDB files:', e);
            container.innerHTML = '<div class="text-danger small py-3">' +
                '<i class="bi bi-x-circle me-1"></i> Error loading files: ' +
                escapeHtmlSettings(e.message) + '</div>';
        }
    }

    async function deleteLadybugFile(name) {
        const confirmed = await showConfirmDialog({
            title: 'Delete Graph File',
            message: 'Delete "' + name + '" from local storage? This cannot be undone.',
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash'
        });
        if (!confirmed) return;

        try {
            const resp = await fetch('/settings/ladybugdb/files/' + encodeURIComponent(name), {
                method: 'DELETE',
                credentials: 'same-origin'
            });
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message, 'success', 2000);
                await loadLadybugFiles();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (e) {
            showNotification('Error deleting file: ' + e.message, 'error');
        }
    }

    // =====================================================================
    //  GLOBAL SAVE BUTTON – saves Warehouse + Base URI
    // =====================================================================

    document.getElementById('btnSaveAllSettings')?.addEventListener('click', async function () {
        const btn = this;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Saving...';

        const errors = [];

        // 1. Save warehouse (skip when locked by Databricks App resource)
        const whId = document.getElementById('settingsWarehouseSelect').value;
        if (whId && !warehouseLocked) {
            try {
                const resp = await fetch('/settings/save', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin',
                    body: JSON.stringify({ warehouse_id: whId })
                });
                const r = await resp.json();
                if (r.success) currentWarehouseId = whId;
                else errors.push('Warehouse: ' + r.message);
            } catch (e) { errors.push('Warehouse: ' + e.message); }
        }

        // 2. Save base URI
        const baseUri = document.getElementById('baseUriDefault').value.trim();
        if (baseUri) {
            try {
                const resp = await fetch('/settings/save-base-uri', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin',
                    body: JSON.stringify({ base_uri: baseUri })
                });
                const r = await resp.json();
                if (!r.success) errors.push('Base URI: ' + r.message);
            } catch (e) { errors.push('Base URI: ' + e.message); }
        }

        // 3. Save registry cache TTL
        const ttlInput = document.getElementById('registryCacheTtl');
        if (ttlInput) {
            const ttl = parseInt(ttlInput.value, 10);
            if (!isNaN(ttl) && ttl >= 10) {
                try {
                    const resp = await fetch('/settings/save-registry-cache-ttl', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        credentials: 'same-origin',
                        body: JSON.stringify({ registry_cache_ttl: ttl })
                    });
                    const r = await resp.json();
                    if (!r.success) errors.push('Cache TTL: ' + r.message);
                } catch (e) { errors.push('Cache TTL: ' + e.message); }
            }
        }

        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-check-circle me-1"></i> Save';

        if (errors.length > 0) {
            showNotification('Some settings failed to save:\n' + errors.join('\n'), 'error');
        } else {
            showNotification('All settings saved', 'success', 2000);
        }
    });
});
