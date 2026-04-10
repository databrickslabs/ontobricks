/**
 * OntoBricks - settings.js
 * Settings page JavaScript – tabbed layout with a single global Save button
 */

document.addEventListener('DOMContentLoaded', function () {

    let currentWarehouseId = null;
    let warehouseLocked = false;
    let registryLocked = false;

    loadCurrentConfig();
    loadRegistryConfig();
    loadBaseUri();
    loadCurrentDefaultEmoji();

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
    //  REGISTRY TAB
    // =====================================================================

    let registryConfigured = false;
    let registryCfg = { catalog: '', schema: '', volume: 'OntoBricksRegistry', configured: false };

    async function loadRegistryConfig() {
        const label = document.getElementById('registryLocationLabel');

        try {
            const resp = await fetch('/settings/registry', { credentials: 'same-origin' });
            registryCfg = await resp.json();
            registryLocked = !!registryCfg.registry_locked;

            updateRegistryLabel();
            updateRegistryStatus(registryCfg);

            if (registryLocked) {
                const btnChange = document.getElementById('btnChangeRegistry');
                if (btnChange) btnChange.disabled = true;
                const regHelp = document.getElementById('registryHelp');
                if (regHelp) regHelp.innerHTML = '<i class="bi bi-lock-fill text-muted me-1"></i> Configured via Databricks App resource';

                const btnInit = document.getElementById('btnInitRegistry');
                if (btnInit) {
                    if (registryCfg.configured) {
                        btnInit.style.display = 'none';
                    } else {
                        btnInit.style.display = '';
                        btnInit.disabled = false;
                    }
                }
            }
        } catch (e) {
            console.error('Error loading registry config:', e);
            label.innerHTML = '<i class="bi bi-x-circle text-danger"></i> <span class="text-danger">Error loading config</span>';
        }
    }

    function updateRegistryLabel() {
        const label = document.getElementById('registryLocationLabel');
        const initBtn = document.getElementById('btnInitRegistry');

        if (registryCfg.catalog && registryCfg.schema) {
            const path = registryCfg.catalog + '.' + registryCfg.schema + '.' + (registryCfg.volume || 'OntoBricksRegistry');
            label.innerHTML = '<i class="bi bi-archive text-success me-1"></i> <strong>' + escapeHtmlSettings(path) + '</strong>';
            initBtn.style.display = registryCfg.configured ? 'none' : '';
        } else {
            label.innerHTML = '<i class="bi bi-exclamation-triangle text-warning me-1"></i> <span class="text-muted">Not configured</span>';
            initBtn.style.display = 'none';
        }
    }

    function updateRegistryStatus(cfg) {
        const div = document.getElementById('registryStatus');
        registryConfigured = !!cfg.configured;

        if (cfg.configured) {
            div.style.display = 'none';
            loadRegistryProjects();
        } else if (cfg.catalog && cfg.schema) {
            div.style.display = 'block';
            const msg = registryLocked
                ? 'Registry volume is set via Databricks App resource but not yet initialized. Click <strong>Initialize</strong> to set up the registry.'
                : 'Registry location set but not initialized yet. Click <strong>Initialize</strong> to create the volume.';
            div.innerHTML = '<div class="alert alert-warning small mb-0">' +
                '<i class="bi bi-exclamation-triangle me-1"></i> ' + msg + '</div>';
            const section = document.getElementById('registryProjectsSection');
            if (section) section.style.display = 'none';
        } else {
            div.style.display = 'block';
            div.innerHTML = '<div class="alert alert-warning small mb-0">' +
                '<i class="bi bi-exclamation-triangle me-1"></i> Registry not configured. Click <strong>Change</strong> to select a catalog, schema and volume.</div>';
            const section = document.getElementById('registryProjectsSection');
            if (section) section.style.display = 'none';
        }
    }

    // --- Change modal: lazy-load catalogs/schemas only when opened ---

    document.getElementById('btnChangeRegistry')?.addEventListener('click', () => {
        const modal = new bootstrap.Modal(document.getElementById('registryChangeModal'));
        modal.show();
        loadModalCatalogs();
    });

    async function loadModalCatalogs() {
        const catSelect = document.getElementById('registryCatalog');
        const schSelect = document.getElementById('registrySchema');

        catSelect.disabled = true;
        catSelect.innerHTML = '<option value="">Loading catalogs...</option>';
        schSelect.disabled = true;
        schSelect.innerHTML = '<option value="">Select catalog first</option>';

        try {
            const resp = await fetch('/settings/catalogs', { credentials: 'same-origin' });
            const data = await resp.json();

            catSelect.innerHTML = '<option value="">Select catalog...</option>';
            if (data.catalogs) {
                data.catalogs.forEach(c => {
                    catSelect.innerHTML += '<option value="' + c + '"' +
                        (c === registryCfg.catalog ? ' selected' : '') + '>' + c + '</option>';
                });
            }
            catSelect.disabled = false;

            catSelect.onchange = () => loadModalSchemas(catSelect.value, null);

            if (registryCfg.catalog) {
                await loadModalSchemas(registryCfg.catalog, registryCfg.schema);
            }
        } catch (e) {
            console.error('Error loading catalogs:', e);
            catSelect.innerHTML = '<option value="">Error loading catalogs</option>';
        }

        const volInput = document.getElementById('registryVolume');
        if (registryCfg.volume) volInput.value = registryCfg.volume;
    }

    async function loadModalSchemas(catalog, preselectSchema) {
        const schSelect = document.getElementById('registrySchema');
        if (!catalog) {
            schSelect.disabled = true;
            schSelect.innerHTML = '<option value="">Select catalog first</option>';
            return;
        }

        schSelect.disabled = true;
        schSelect.innerHTML = '<option value="">Loading schemas...</option>';

        try {
            const resp = await fetch('/settings/schemas?catalog=' + encodeURIComponent(catalog), { credentials: 'same-origin' });
            const data = await resp.json();

            schSelect.innerHTML = '<option value="">Select schema...</option>';
            schSelect.disabled = false;
            if (data.schemas) {
                data.schemas.forEach(s => {
                    schSelect.innerHTML += '<option value="' + s + '"' +
                        (s === preselectSchema ? ' selected' : '') + '>' + s + '</option>';
                });
            }
        } catch (e) {
            schSelect.innerHTML = '<option value="">Error loading schemas</option>';
        }
    }

    document.getElementById('btnApplyRegistry')?.addEventListener('click', async () => {
        const catalog = document.getElementById('registryCatalog').value;
        const schema = document.getElementById('registrySchema').value;
        const volume = document.getElementById('registryVolume').value.trim() || 'OntoBricksRegistry';

        if (!catalog || !schema) {
            showNotification('Please select both a catalog and a schema', 'warning');
            return;
        }

        const btn = document.getElementById('btnApplyRegistry');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Saving...';

        try {
            const resp = await fetch('/settings/registry', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ catalog, schema, volume })
            });
            const r = await resp.json();

            if (r.success) {
                registryCfg = { catalog, schema, volume, configured: r.configured !== undefined ? r.configured : registryCfg.configured };

                const cfgResp = await fetch('/settings/registry', { credentials: 'same-origin' });
                registryCfg = await cfgResp.json();

                updateRegistryLabel();
                updateRegistryStatus(registryCfg);
                bootstrap.Modal.getInstance(document.getElementById('registryChangeModal'))?.hide();
                showNotification('Registry location updated', 'success', 2000);
            } else {
                showNotification('Error: ' + r.message, 'error');
            }
        } catch (e) {
            showNotification('Error saving registry: ' + e.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-check-circle me-1"></i> Apply';
        }
    });

    // --- Registry project list ---

    async function loadRegistryProjects() {
        const section = document.getElementById('registryProjectsSection');
        const listDiv = document.getElementById('registryProjectsList');
        if (!section || !listDiv) return;

        section.style.display = 'block';
        listDiv.innerHTML = '<div class="text-center text-muted small py-3">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading projects...</div>';

        try {
            const [data, vsData] = await Promise.all([
                fetch('/settings/registry/projects', { credentials: 'same-origin' }).then(r => r.json()),
                fetchOnce('/project/version-status')
            ]);
            const currentFolder = (vsData.success && vsData.project_folder) ? vsData.project_folder : null;

            if (!data.success) {
                listDiv.innerHTML = '<div class="text-muted small py-3"><i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                    (data.message || 'Could not load projects') + '</div>';
                return;
            }

            if (!data.projects || data.projects.length === 0) {
                listDiv.innerHTML = '<div class="text-muted small py-3 text-center">' +
                    '<i class="bi bi-folder"></i> No projects in registry yet</div>';
                return;
            }

            let html = '<div class="table-responsive registry-project-table-wrapper">' +
                '<table class="table table-sm table-hover align-middle mb-0 registry-project-table">' +
                '<thead><tr>' +
                    '<th class="ps-3">Name</th>' +
                    '<th>Description</th>' +
                    '<th class="text-center" style="width:5rem;">Versions</th>' +
                    '<th class="text-end pe-3" style="width:3rem;"></th>' +
                '</tr></thead><tbody>';

            data.projects.forEach((p, idx) => {
                const desc = p.description
                    ? escapeHtmlSettings(p.description)
                    : '<span class="fst-italic text-muted">—</span>';
                const vCount = (p.versions || []).length;
                const hasVersions = vCount > 0;
                const rowId = 'reg-versions-' + idx;
                const isCurrent = currentFolder && p.name === currentFolder;
                const nameLabel = escapeHtmlSettings(p.name) +
                    (isCurrent ? ' <span class="badge bg-primary-subtle text-primary border ms-1" style="font-size:0.65rem;">current</span>' : '');
                const deleteBtn = isCurrent
                    ? '<button type="button" class="btn btn-sm border-0 text-muted" disabled title="Cannot delete the currently loaded project">' +
                          '<i class="bi bi-trash"></i></button>'
                    : '<button type="button" class="btn btn-sm btn-outline-danger border-0 registry-delete-btn" ' +
                          'data-project="' + escapeHtmlSettings(p.name) + '" title="Delete project and all versions">' +
                          '<i class="bi bi-trash"></i></button>';
                html += '<tr class="registry-project-row" data-target="' + rowId + '" style="cursor:pointer;">' +
                    '<td class="ps-3 fw-semibold text-nowrap">' +
                        '<i class="bi bi-chevron-right me-1 text-muted registry-chevron" style="font-size:0.7rem;transition:transform 0.15s;"></i>' +
                        '<i class="bi bi-folder2 me-1 text-primary"></i>' +
                        nameLabel +
                    '</td>' +
                    '<td class="text-muted text-truncate" style="max-width:300px;">' + desc + '</td>' +
                    '<td class="text-center"><span class="badge bg-secondary">' + vCount + '</span></td>' +
                    '<td class="text-end pe-3">' + deleteBtn + '</td>' +
                '</tr>';
                // Expandable version rows (hidden by default)
                if (hasVersions) {
                    html += '<tr id="' + rowId + '" class="registry-version-panel" style="display:none;">' +
                        '<td colspan="4" class="ps-5 pe-3 py-0">' +
                        '<div class="d-flex flex-wrap gap-1 py-2">';
                    p.versions.forEach(v => {
                        html += '<span class="badge bg-light text-dark border d-inline-flex align-items-center gap-1 registry-version-badge">' +
                            'v' + escapeHtmlSettings(v) +
                            '<button type="button" class="btn-close btn-close-sm registry-delete-version-btn" ' +
                                'data-project="' + escapeHtmlSettings(p.name) + '" data-version="' + escapeHtmlSettings(v) + '" ' +
                                'title="Delete version v' + escapeHtmlSettings(v) + '" style="font-size:0.55rem;margin-left:2px;"></button>' +
                        '</span>';
                    });
                    html += '</div></td></tr>';
                }
            });

            html += '</tbody></table></div>';
            listDiv.innerHTML = html;

            // Toggle version panel on project row click
            listDiv.querySelectorAll('.registry-project-row').forEach(row => {
                row.addEventListener('click', (e) => {
                    if (e.target.closest('.registry-delete-btn')) return;
                    const target = document.getElementById(row.dataset.target);
                    if (!target) return;
                    const chevron = row.querySelector('.registry-chevron');
                    const isOpen = target.style.display !== 'none';
                    target.style.display = isOpen ? 'none' : '';
                    if (chevron) chevron.style.transform = isOpen ? '' : 'rotate(90deg)';
                });
            });

            listDiv.querySelectorAll('.registry-delete-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    deleteRegistryProject(btn.dataset.project);
                });
            });

            listDiv.querySelectorAll('.registry-delete-version-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    deleteRegistryVersion(btn.dataset.project, btn.dataset.version);
                });
            });

        } catch (e) {
            console.error('Error loading registry projects:', e);
            listDiv.innerHTML = '<div class="text-danger small py-3">' +
                '<i class="bi bi-x-circle me-1"></i> Error loading projects</div>';
        }
    }

    async function deleteRegistryProject(projectName) {
        const confirmed = await showConfirmDialog({
            title: 'Delete Project',
            message: 'Delete project "' + projectName + '" and all its versions from the registry? This cannot be undone.',
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash'
        });
        if (!confirmed) return;

        try {
            const resp = await fetch('/settings/registry/projects/' + encodeURIComponent(projectName), {
                method: 'DELETE',
                credentials: 'same-origin'
            });
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message, 'success');
                loadRegistryProjects();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (e) {
            showNotification('Error deleting project: ' + e.message, 'error');
        }
    }

    async function deleteRegistryVersion(projectName, version) {
        const confirmed = await showConfirmDialog({
            title: 'Delete Version',
            message: 'Delete version v' + version + ' from project "' + projectName + '"? This cannot be undone.',
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash'
        });
        if (!confirmed) return;

        try {
            const resp = await fetch(
                '/settings/registry/projects/' + encodeURIComponent(projectName) + '/versions/' + encodeURIComponent(version),
                { method: 'DELETE', credentials: 'same-origin' }
            );
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message, 'success');
                loadRegistryProjects();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (e) {
            showNotification('Error deleting version: ' + e.message, 'error');
        }
    }

    function escapeHtmlSettings(str) { return escapeHtml(str); }

    document.getElementById('btnRefreshProjects')?.addEventListener('click', () => loadRegistryProjects());

    document.getElementById('btnInitRegistry')?.addEventListener('click', async () => {
        const btn = document.getElementById('btnInitRegistry');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Initializing...';
        try {
            const resp = await fetch('/settings/registry/initialize', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin'
            });
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message, 'success');
                registryConfigured = true;
                registryCfg.configured = true;
                updateRegistryLabel();
                updateRegistryStatus(registryCfg);
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (e) {
            showNotification('Error: ' + e.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-plus-circle me-1"></i> Initialize';
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
    //  GLOBAL TAB – Default Emoji Picker
    // =====================================================================

    const emojiCategories = {
        'People & Roles': ['👤', '👥', '👨', '👩', '👶', '👴', '👵', '🧑', '👨‍💼', '👩‍💼', '👨‍🔬', '👩‍🔬', '👨‍💻', '👩‍💻', '👨‍🏫', '👩‍🏫', '👨‍⚕️', '👩‍⚕️', '🧑‍🤝‍🧑', '👪'],
        'Business & Work': ['🏢', '🏭', '🏬', '🏛️', '💼', '📊', '📈', '📉', '💰', '💵', '💳', '🏦', '📋', '📁', '📂', '🗂️', '📝', '✏️', '📌', '📎'],
        'Technology': ['💻', '🖥️', '⌨️', '🖱️', '📱', '📲', '☎️', '🔌', '💾', '💿', '📀', '🔧', '🔩', '⚙️', '🔬', '🔭', '📡', '🤖', '🔋', '💡'],
        'Data & Documents': ['📄', '📃', '📑', '📰', '📚', '📖', '📒', '📓', '📔', '📕', '📗', '📘', '📙', '🗃️', '🗄️', '📦', '📫', '📬', '📭', '📮'],
        'Nature & Science': ['🌍', '🌎', '🌏', '🌐', '🌳', '🌲', '🌴', '🌵', '🌾', '🌻', '🔥', '💧', '⚡', '🌈', '☀️', '🌙', '⭐', '🌟', '💎', '🔮'],
        'Objects & Things': ['🏠', '🏡', '🚗', '🚕', '🚌', '✈️', '🚀', '🛸', '⚓', '🎯', '🎨', '🎭', '🎪', '🎬', '🎮', '🎲', '🧩', '🔑', '🗝️', '🔒'],
        'Symbols': ['❤️', '💙', '💚', '💛', '💜', '🖤', '🤍', '🤎', '⭕', '❌', '✅', '❎', '➕', '➖', '➗', '✖️', '💯', '🔴', '🟠', '🟢'],
        'Arrows & Shapes': ['⬆️', '⬇️', '⬅️', '➡️', '↗️', '↘️', '↙️', '↖️', '↕️', '↔️', '🔄', '🔃', '🔀', '🔁', '🔂', '▶️', '⏸️', '⏹️', '🔷', '🔶']
    };

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

    function initDefaultEmojiPicker() {
        const grid = document.getElementById('defaultEmojiGrid');
        for (const [category, emojis] of Object.entries(emojiCategories)) {
            const categoryDiv = document.createElement('div');
            categoryDiv.className = 'mb-2';
            categoryDiv.innerHTML = `<small class="text-muted fw-bold">${category}</small>`;

            const emojiRow = document.createElement('div');
            emojiRow.className = 'd-flex flex-wrap gap-1 mt-1';

            emojis.forEach(emoji => {
                const btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'btn btn-light btn-sm';
                btn.style.fontSize = '1.2rem';
                btn.style.padding = '0.25rem 0.5rem';
                btn.textContent = emoji;
                btn.title = emoji;
                btn.onclick = () => selectDefaultEmoji(emoji);
                emojiRow.appendChild(btn);
            });

            categoryDiv.appendChild(emojiRow);
            grid.appendChild(categoryDiv);
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
                document.getElementById('defaultEmojiPickerContainer').style.display = 'none';
                showNotification('Default class icon updated to ' + emoji, 'success', 2000);
            } else {
                showNotification('Error: ' + result.message, 'error');
            }
        } catch (error) {
            showNotification('Error saving default emoji: ' + error.message, 'error');
        }
    }

    document.getElementById('changeDefaultEmoji')?.addEventListener('click', function () {
        const picker = document.getElementById('defaultEmojiPickerContainer');
        if (picker.style.display === 'none') {
            picker.style.display = 'block';
            if (document.getElementById('defaultEmojiGrid').children.length === 0) {
                initDefaultEmojiPicker();
            }
        } else {
            picker.style.display = 'none';
        }
    });

    document.getElementById('closeDefaultEmojiPicker')?.addEventListener('click', function () {
        document.getElementById('defaultEmojiPickerContainer').style.display = 'none';
    });

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
    //  GLOBAL SAVE BUTTON – saves Warehouse + Registry + Base URI
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

        // 2. Registry is saved via the modal Apply button — no action needed here

        // 3. Save base URI
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

        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-check-circle me-1"></i> Save';

        if (errors.length > 0) {
            showNotification('Some settings failed to save:\n' + errors.join('\n'), 'error');
        } else {
            showNotification('All settings saved', 'success', 2000);
        }
    });
});
