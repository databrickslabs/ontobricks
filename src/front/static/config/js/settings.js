/**
 * OntoBricks - settings.js
 * Settings page JavaScript – sidebar layout; global Save persists all sections including triple store
 */

document.addEventListener('DOMContentLoaded', function () {

    let currentWarehouseId = null;
    let warehouseLocked = false;
    let graphDbLoaded = false;
    // Registry rebuilt on every loadLakebaseObjects call; keyed by domain base name.
    // Avoids embedding JSON in onclick HTML attributes (double quotes break the attribute).
    let _lkDomainRegistry = {};
    // UC/Lakeflow objects keyed by domain base name; populated by loadLakebaseSyncObjects.
    let _lkUCRegistry = {};

    function escapeHtmlSettings(str) { return escapeHtml(str); }

    loadCurrentConfig();
    loadBaseUri();
    loadCurrentDefaultEmoji();

    loadRegistryCacheTtl();
    loadEditLockTtl();
    loadNavbarLogo();

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
                hostDisplay.innerHTML = '<i class="bi bi-cloud text-success"></i> ' + escapeHtmlSettings(data.host);
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
                select.innerHTML = '<option value="">Error: ' + escapeHtmlSettings(data.error) + '</option>';
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
    //  GLOBAL TAB – Edit Lock Lease TTL (stored in seconds, shown in minutes)
    // =====================================================================

    async function loadEditLockTtl() {
        const input = document.getElementById('editLockTtlMin');
        if (!input) return;
        try {
            const resp = await fetch('/settings/edit-lock-ttl', { credentials: 'same-origin' });
            const result = await resp.json();
            if (result.success && result.edit_lock_ttl_s != null) {
                input.value = Math.round(result.edit_lock_ttl_s / 60);
            }
        } catch (error) {
            console.log('Using default edit-lock lease TTL');
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
    //  GLOBAL TAB – Application Logo (top-bar branding)
    // =====================================================================

    async function loadNavbarLogo() {
        try {
            const resp = await fetch('/settings/navbar-logo', { credentials: 'same-origin' });
            const result = await resp.json();
            if (!result.success) return;
            const previewEl = document.getElementById('navbarLogoPreview');
            if (previewEl && result.logo_url) previewEl.src = result.logo_url;
            const statusEl = document.getElementById('navbarLogoStatus');
            if (statusEl) {
                statusEl.textContent = result.is_custom ? 'Custom logo active' : 'Using default logo';
            }
        } catch (e) {
            console.log('Could not load navbar logo settings');
        }
    }

    const logoFileInput = document.getElementById('navbarLogoFile');
    const logoUploadBtn = document.getElementById('btnUploadNavbarLogo');
    const logoResetBtn  = document.getElementById('btnResetNavbarLogo');
    const logoPreviewEl = document.getElementById('navbarLogoPreview');
    const logoStatusEl  = document.getElementById('navbarLogoStatus');

    if (logoFileInput) {
        logoFileInput.addEventListener('change', () => {
            const file = logoFileInput.files && logoFileInput.files[0];
            if (logoUploadBtn) logoUploadBtn.disabled = !file;
            if (file && logoPreviewEl) {
                const reader = new FileReader();
                reader.onload = (ev) => { logoPreviewEl.src = ev.target.result; };
                reader.readAsDataURL(file);
            }
        });
    }

    if (logoUploadBtn) {
        logoUploadBtn.addEventListener('click', async () => {
            const file = logoFileInput && logoFileInput.files && logoFileInput.files[0];
            if (!file) return;
            const MAX = 1024 * 1024;
            if (file.size > MAX) {
                showNotification(`Image too large (${file.size} bytes); max ${MAX} bytes`, 'error');
                return;
            }
            logoUploadBtn.disabled = true;
            const original = logoUploadBtn.innerHTML;
            logoUploadBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Uploading...';
            try {
                const fd = new FormData();
                fd.append('file', file);
                const resp = await fetch('/settings/navbar-logo', {
                    method: 'POST',
                    body: fd,
                    credentials: 'same-origin'
                });
                const result = await resp.json();
                if (result.success) {
                    if (logoPreviewEl && result.logo_url) logoPreviewEl.src = result.logo_url;
                    if (logoStatusEl) logoStatusEl.textContent = 'Custom logo active';
                    if (logoFileInput) logoFileInput.value = '';
                    if (typeof fetchCachedInvalidate === 'function') {
                        fetchCachedInvalidate('/navbar/state');
                    }
                    const navImg = document.getElementById('brandLogoImg');
                    if (navImg && result.logo_url) navImg.src = result.logo_url;
                    showNotification('Application logo updated', 'success', 2500);
                } else {
                    showNotification('Error: ' + (result.message || 'upload failed'), 'error');
                }
            } catch (e) {
                showNotification('Error uploading logo: ' + e.message, 'error');
            } finally {
                logoUploadBtn.innerHTML = original;
                logoUploadBtn.disabled = !(logoFileInput && logoFileInput.files && logoFileInput.files[0]);
            }
        });
    }

    if (logoResetBtn) {
        logoResetBtn.addEventListener('click', async () => {
            const confirmed = await (typeof showConfirmDialog === 'function'
                ? showConfirmDialog({
                    title: 'Reset application logo',
                    message: 'Restore the default OntoBricks logo for all users?',
                    confirmText: 'Reset',
                    confirmClass: 'btn-warning',
                    icon: 'arrow-counterclockwise'
                })
                : Promise.resolve(window.confirm('Restore the default logo?')));
            if (!confirmed) return;
            logoResetBtn.disabled = true;
            try {
                const resp = await fetch('/settings/navbar-logo', {
                    method: 'DELETE',
                    credentials: 'same-origin'
                });
                const result = await resp.json();
                if (result.success) {
                    if (logoPreviewEl && result.logo_url) logoPreviewEl.src = result.logo_url;
                    if (logoStatusEl) logoStatusEl.textContent = 'Using default logo';
                    if (logoFileInput) logoFileInput.value = '';
                    if (logoUploadBtn) logoUploadBtn.disabled = true;
                    if (typeof fetchCachedInvalidate === 'function') {
                        fetchCachedInvalidate('/navbar/state');
                    }
                    const navImg = document.getElementById('brandLogoImg');
                    if (navImg && result.logo_url) navImg.src = result.logo_url;
                    showNotification('Application logo reset to default', 'success', 2500);
                } else {
                    showNotification('Error: ' + (result.message || 'reset failed'), 'error');
                }
            } catch (e) {
                showNotification('Error resetting logo: ' + e.message, 'error');
            } finally {
                logoResetBtn.disabled = false;
            }
        });
    }

    // =====================================================================
    //  GRAPH DB TAB – Graph Engine selector
    // =====================================================================

    /** Show Lakebase picker section from graph engine select. */
    function applyGraphDbEnginePanels() {
        const sel = document.getElementById('graphEngineSelect');
        const lakePanel = document.getElementById('lakebaseGraphPanel');
        if (!sel) return;
        const eng = sel.value;
        if (lakePanel) {
            lakePanel.style.display = eng === 'lakebase' ? 'block' : 'none';
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    function _setSelectLoading(sel, msg) {
        if (!sel) return;
        sel.innerHTML = '<option value="">' + escapeHtmlSettings(msg) + '</option>';
        sel.disabled = true;
    }

    function _setSelectError(sel, msg) {
        if (!sel) return;
        sel.innerHTML = '<option value="">' + escapeHtmlSettings(msg) + '</option>';
        sel.disabled = false;
    }

    function _getCurrentSchemaValue() {
        const schSel = document.getElementById('lakebaseGraphSchema');
        const schIn  = document.getElementById('lakebaseGraphSchemaInput');
        const btn    = document.getElementById('btnToggleLakebaseSchemaInput');
        if (btn && btn.dataset.mode === 'input') {
            return (schIn ? schIn.value : '').trim() || 'ontobricks_graph';
        }
        return (schSel ? schSel.value : '').trim() || 'ontobricks_graph';
    }

    // ── cascading pickers ─────────────────────────────────────────────────────

    async function loadLakebaseProjects() {
        const projSel   = document.getElementById('lakebaseProject');
        const branchSel = document.getElementById('lakebaseBranch');
        const dbSel     = document.getElementById('lakebaseGraphDb');
        const schSel    = document.getElementById('lakebaseGraphSchema');
        const btn       = document.getElementById('btnLoadLakebaseProjects');
        const help      = document.getElementById('lakebaseProjectHelp');
        if (!projSel) return;

        _setSelectLoading(projSel, 'Loading projects…');
        if (btn) btn.disabled = true;

        // read current configured values to restore selection after reload
        let cfgDb = '', cfgProject = '', cfgBranch = '';
        try {
            const o = JSON.parse(document.getElementById('graphEngineConfig')?.value || '{}');
            cfgDb      = o.database || '';
            cfgProject = o.lakebase_project || '';
            cfgBranch  = o.lakebase_branch  || '';
        } catch (_) {}

        try {
            const resp = await fetch('/settings/graph-engine/lakebase-projects', { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            if (!data.success || !data.projects.length) {
                _setSelectError(projSel, '(no projects found — check workspace auth)');
                if (help) help.textContent = data.message || 'Could not list projects.';
                return;
            }
            projSel.innerHTML = '<option value="">(select a project)</option>';
            let matched = false;
            for (const p of data.projects) {
                const opt = document.createElement('option');
                opt.value = p.name;
                opt.textContent = p.short_name + (p.state ? ' — ' + p.state : '');
                if (p.name === cfgProject || p.short_name === cfgProject) {
                    opt.selected = true;
                    matched = true;
                }
                projSel.appendChild(opt);
            }
            projSel.disabled = false;
            if (help) help.textContent = data.projects.length + ' project(s) found.';

            if (matched && projSel.value) {
                await loadLakebaseBranches(projSel.value, cfgBranch, cfgDb);
            }
        } catch (e) {
            _setSelectError(projSel, '(error — ' + (e.message || 'network') + ')');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    async function loadLakebaseBranches(projectPath, cfgBranch, cfgDb) {
        const branchSel = document.getElementById('lakebaseBranch');
        const help      = document.getElementById('lakebaseBranchHelp');
        if (!branchSel || !projectPath) return;

        _setSelectLoading(branchSel, 'Loading branches…');

        try {
            const resp = await fetch(
                '/settings/graph-engine/lakebase-branches?project=' + encodeURIComponent(projectPath),
                { credentials: 'same-origin' }
            );
            const data = resp.ok ? await resp.json() : {};
            if (!data.success || !data.branches.length) {
                _setSelectError(branchSel, '(no branches found)');
                if (help) help.textContent = data.message || 'No branches.';
                return;
            }
            branchSel.innerHTML = '<option value="">(select a branch)</option>';
            let matched = false;
            for (const b of data.branches) {
                const opt = document.createElement('option');
                opt.value = b.name;
                opt.textContent = b.short_name + (b.state ? ' — ' + b.state : '');
                if (b.name === cfgBranch || b.short_name === cfgBranch) {
                    opt.selected = true;
                    matched = true;
                }
                branchSel.appendChild(opt);
            }
            branchSel.disabled = false;
            if (help) help.textContent = data.branches.length + ' branch(es) found.';

            if (matched && branchSel.value) {
                await loadLakebasePgDatabases(branchSel.value, cfgDb);
            }
        } catch (e) {
            _setSelectError(branchSel, '(error — ' + (e.message || 'network') + ')');
        }
    }

    async function loadLakebasePgDatabases(branchPath, cfgDb) {
        const dbSel  = document.getElementById('lakebaseGraphDb');
        const schSel = document.getElementById('lakebaseGraphSchema');
        const help   = document.getElementById('lakebaseGraphDbHelp');
        if (!dbSel || !branchPath) return;

        _setSelectLoading(dbSel, 'Loading databases…');
        _setSelectLoading(schSel, '(select a database first)');

        // read current schema from config textarea so we can restore it
        let cfgSchema = 'ontobricks_graph';
        try {
            const o = JSON.parse(document.getElementById('graphEngineConfig')?.value || '{}');
            if (o.schema) cfgSchema = o.schema;
        } catch (_) {}

        try {
            const resp = await fetch(
                '/settings/graph-engine/lakebase-pg-databases?branch=' + encodeURIComponent(branchPath),
                { credentials: 'same-origin' }
            );
            const data = resp.ok ? await resp.json() : {};
            if (!data.success || !data.databases.length) {
                _setSelectError(dbSel, '(no databases found)');
                if (help) help.textContent = data.message || 'No databases on this branch.';
                return;
            }
            dbSel.innerHTML = '<option value="">(default — bound database)</option>';
            let matched = false;
            for (const name of data.databases) {
                const opt = document.createElement('option');
                opt.value = name;
                opt.textContent = name;
                if (name === cfgDb) { opt.selected = true; matched = true; }
                dbSel.appendChild(opt);
            }
            dbSel.disabled = false;
            if (help) help.textContent = data.databases.length + ' database(s) found.';

            if (matched && dbSel.value) {
                await loadLakebasePgSchemas(dbSel.value, cfgSchema, branchPath);
            }
        } catch (e) {
            _setSelectError(dbSel, '(error — ' + (e.message || 'network') + ')');
        }
    }

    async function loadLakebasePgSchemas(database, cfgSchema, branchPath) {
        const schSel = document.getElementById('lakebaseGraphSchema');
        const schIn  = document.getElementById('lakebaseGraphSchemaInput');
        const help   = document.getElementById('lakebaseGraphSchemaHelp');
        if (!schSel || !database) return;

        _setSelectLoading(schSel, 'Loading schemas…');

        try {
            const params = new URLSearchParams({ database });
            if (branchPath) params.set('branch_path', branchPath);
            const resp = await fetch(
                '/settings/graph-engine/lakebase-pg-schemas?' + params.toString(),
                { credentials: 'same-origin' }
            );
            const data = resp.ok ? await resp.json() : {};
            if (!data.success || !data.schemas.length) {
                _setSelectError(schSel, '(no schemas — ' + (data.message || 'empty database') + ')');
                if (help) help.textContent = 'No schemas found. Use the pencil to type one manually.';
                return;
            }
            schSel.innerHTML = '<option value="">(default — ontobricks_graph)</option>';
            let matched = false;
            for (const name of data.schemas) {
                const opt = document.createElement('option');
                opt.value = name;
                opt.textContent = name;
                if (name === cfgSchema) { opt.selected = true; matched = true; }
                schSel.appendChild(opt);
            }
            schSel.disabled = false;
            if (help) help.textContent = data.schemas.length + ' schema(s) found.';
            if (!matched && cfgSchema) {
                // schema configured but not listed (might not exist yet) — add as option
                const opt = document.createElement('option');
                opt.value = cfgSchema;
                opt.textContent = cfgSchema + ' (configured)';
                opt.selected = true;
                schSel.appendChild(opt);
            }
            if (schIn) schIn.value = schSel.value || cfgSchema;
        } catch (e) {
            _setSelectError(schSel, '(error — ' + (e.message || 'network') + ')');
        }
        mergeLakebasePanelIntoConfigTextarea();
    }

    // ── schema toggle (select ↔ manual input) ────────────────────────────────

    function _initSchemaToggle() {
        const btn   = document.getElementById('btnToggleLakebaseSchemaInput');
        const schSel = document.getElementById('lakebaseGraphSchema');
        const schIn  = document.getElementById('lakebaseGraphSchemaInput');
        if (!btn || !schSel || !schIn) return;

        btn.addEventListener('click', function () {
            const isSelect = btn.dataset.mode === 'select';
            if (isSelect) {
                // switch to manual input
                schSel.classList.add('d-none');
                schIn.classList.remove('d-none');
                schIn.value = schSel.value || 'ontobricks_graph';
                btn.dataset.mode = 'input';
                btn.title = 'Use dropdown';
                btn.innerHTML = '<i class="bi bi-list"></i>';
            } else {
                // switch back to select
                schIn.classList.add('d-none');
                schSel.classList.remove('d-none');
                btn.dataset.mode = 'select';
                btn.title = 'Type schema name manually';
                btn.innerHTML = '<i class="bi bi-pencil"></i>';
            }
            mergeLakebasePanelIntoConfigTextarea();
        });

        schIn.addEventListener('input', function () {
            mergeLakebasePanelIntoConfigTextarea();
            const ucSchDisplay = document.getElementById('lakebaseUcSchemaDisplay');
            if (ucSchDisplay) ucSchDisplay.value = this.value || '';
        });
        schIn.addEventListener('change', mergeLakebasePanelIntoConfigTextarea);
    }

    // ── merge / apply ─────────────────────────────────────────────────────────

    /** Merge Lakebase form fields + optional managed-sync options into the JSON textarea. */
    function mergeLakebasePanelIntoConfigTextarea() {
        const ta         = document.getElementById('graphEngineConfig');
        const dbSel      = document.getElementById('lakebaseGraphDb');
        const projSel    = document.getElementById('lakebaseProject');
        const branchSel  = document.getElementById('lakebaseBranch');
        const syncModeEl = document.getElementById('lakebaseSyncMode');
        if (!ta || !dbSel) return;
        let o = {};
        try { o = JSON.parse(ta.value || '{}'); } catch (_) { o = {}; }
        if (typeof o !== 'object' || Array.isArray(o)) o = {};

        o.database          = dbSel.value || '';
        o.schema            = _getCurrentSchemaValue();
        o.lakebase_project  = (projSel   ? projSel.value   : '') || '';
        o.lakebase_branch   = (branchSel ? branchSel.value : '') || '';

        const mode = (syncModeEl && syncModeEl.value === 'managed_synced') ? 'managed_synced' : 'app_managed';
        if (mode === 'managed_synced') {
            o.sync_mode = 'managed_synced';
            const stEl   = document.getElementById('lakebaseSyncTableMode');
            const toutEl = document.getElementById('lakebaseSyncTimeout');
            const ucCat  = document.getElementById('lakebaseUcCatalog');
            if (stEl) o.sync_table_mode = stEl.value || 'snapshot';
            if (toutEl) {
                const n = parseInt(toutEl.value, 10);
                o.sync_timeout_s = (!isNaN(n) && n > 0) ? n : 600;
            }
            const cat = (ucCat ? ucCat.value : '').trim();
            if (cat) o.sync_uc_catalog = cat; else delete o.sync_uc_catalog;
            // sync_uc_schema is always derived from the Postgres graph schema — never persisted
            delete o.sync_uc_schema;
        } else {
            o.sync_mode = 'app_managed';
            delete o.sync_table_mode;
            delete o.sync_timeout_s;
            delete o.sync_uc_catalog;
            delete o.sync_uc_schema;
        }
        ta.value = JSON.stringify(o, null, 2);
    }

    function toggleLakebaseManagedSyncPanel() {
        const sm    = document.getElementById('lakebaseSyncMode');
        const panel = document.getElementById('lakebaseManagedSyncPanel');
        if (!sm || !panel) return;
        panel.classList.toggle('d-none', sm.value !== 'managed_synced');
    }

    function updateLakebaseSyncModeHelp() {
        const sm = document.getElementById('lakebaseSyncMode');
        const v  = sm && sm.value === 'managed_synced' ? 'managed_synced' : 'app_managed';
        document.querySelectorAll('[data-lk-mode]').forEach(function (el) {
            el.classList.toggle('d-none', el.getAttribute('data-lk-mode') !== v);
        });
    }

    // ── UC catalog + schema pickers ───────────────────────────────────────────

    async function loadUcCatalogsForGraphEngine() {
        const catSel = document.getElementById('lakebaseUcCatalog');
        const msg    = document.getElementById('lakebaseUcCatalogLoadMsg');
        const btn    = document.getElementById('btnLoadUcCatalogs');
        if (!catSel) return;
        if (msg) { msg.classList.remove('d-none'); msg.className = 'form-text small mt-1 text-muted'; msg.textContent = 'Loading catalogs…'; }
        if (btn) btn.disabled = true;

        let cfgCat = '';
        try {
            const o = JSON.parse(document.getElementById('graphEngineConfig')?.value || '{}');
            cfgCat = o.sync_uc_catalog || '';
        } catch (_) {}

        try {
            const resp = await fetch('/settings/graph-engine/uc-catalogs', { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            if (data.success && Array.isArray(data.catalogs)) {
                catSel.innerHTML = '<option value="">(none — use Registry catalog)</option>';
                let matched = false;
                for (const name of data.catalogs) {
                    const opt = document.createElement('option');
                    opt.value = name;
                    opt.textContent = name;
                    if (name === cfgCat) { opt.selected = true; matched = true; }
                    catSel.appendChild(opt);
                }
                catSel.disabled = false;
                if (msg) { msg.className = 'form-text small mt-1 text-success'; msg.textContent = data.catalogs.length + ' catalog(s) loaded.'; }
                // no-op: UC schema is always derived from Postgres graph schema
            } else {
                if (msg) { msg.className = 'form-text small mt-1 text-warning'; msg.textContent = data.message || 'Could not list catalogs.'; }
                catSel.disabled = false;
            }
        } catch (e) {
            if (msg) { msg.className = 'form-text small mt-1 text-warning'; msg.textContent = e.message || 'Network error'; }
            catSel.disabled = false;
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    /**
     * Ensure `sel` has `value` selected, matching an existing option by exact
     * value or by short segment (last path component) so we reuse a real
     * cascade-loaded option when present, and only inject a synthetic option
     * when the value is genuinely absent. Keeps the field non-empty.
     */
    function _ensureSelectedOption(sel, value, label) {
        if (!sel || !value) return;
        const short = value.indexOf('/') >= 0 ? value.split('/').pop() : value;
        let opt = Array.from(sel.options).find((op) =>
            op.value === value ||
            op.value === short ||
            (op.value.indexOf('/') >= 0 && op.value.split('/').pop() === short)
        );
        if (!opt) {
            opt = document.createElement('option');
            opt.value = value;
            opt.textContent = label || short;
            sel.appendChild(opt);
        }
        sel.value = opt.value;
        sel.disabled = false;
    }

    /**
     * Guarantee the 4 Connection-tab fields (project, branch, database,
     * schema) always reflect the saved registry config — even when the live
     * workspace cascade can't list/match them (stale or unreachable project).
     */
    function prefillLakebaseConnectionFromConfig() {
        let o = {};
        try { o = JSON.parse(document.getElementById('graphEngineConfig')?.value || '{}'); } catch (_) {}
        // Connection tab — all 4 cascading selects
        _ensureSelectedOption(document.getElementById('lakebaseProject'),    o.lakebase_project || '');
        _ensureSelectedOption(document.getElementById('lakebaseBranch'),     o.lakebase_branch  || '');
        _ensureSelectedOption(document.getElementById('lakebaseGraphDb'),    o.database         || '');
        _ensureSelectedOption(document.getElementById('lakebaseGraphSchema'), o.schema          || '');
        const schIn = document.getElementById('lakebaseGraphSchemaInput');
        if (schIn && o.schema) schIn.value = o.schema;
        // Bulk loading tab — UC catalog (managed_synced mode)
        _ensureSelectedOption(document.getElementById('lakebaseUcCatalog'),  o.sync_uc_catalog  || '');
    }

    function applyLakebaseFormFromConfigTextarea() {
        const ta         = document.getElementById('graphEngineConfig');
        const syncModeEl = document.getElementById('lakebaseSyncMode');
        if (!ta) return;
        let o = {};
        try { o = JSON.parse(ta.value || '{}'); } catch (_) {}

        if (syncModeEl) syncModeEl.value = (o.sync_mode === 'managed_synced') ? 'managed_synced' : 'app_managed';

        const stEl   = document.getElementById('lakebaseSyncTableMode');
        if (stEl && o.sync_table_mode) stEl.value = o.sync_table_mode;

        const toutEl = document.getElementById('lakebaseSyncTimeout');
        if (toutEl && o.sync_timeout_s != null) toutEl.value = String(parseInt(o.sync_timeout_s, 10) || 600);

        // UC catalog — set value but don't reload options here (happens in loadUcCatalogsForGraphEngine)
        const ucCat = document.getElementById('lakebaseUcCatalog');
        if (ucCat && o.sync_uc_catalog != null) ucCat.value = String(o.sync_uc_catalog);

        // schema input mirror + UC schema display (always mirrors Postgres graph schema)
        const schIn = document.getElementById('lakebaseGraphSchemaInput');
        if (schIn && o.schema) schIn.value = o.schema;
        const ucSchDisplay = document.getElementById('lakebaseUcSchemaDisplay');
        if (ucSchDisplay) ucSchDisplay.value = o.schema || '';

        toggleLakebaseManagedSyncPanel();
        updateLakebaseSyncModeHelp();
    }

    async function loadLakebaseGraphHealth() {
        const msgEl = document.getElementById('lakebaseGraphHealthMessage');
        const dl = document.getElementById('lakebaseGraphHealthDl');
        const btn = document.getElementById('btnRefreshLakebaseGraphHealth');
        const engSel = document.getElementById('graphEngineSelect');
        if (!msgEl || !dl || engSel?.value !== 'lakebase') return;

        if (btn) btn.disabled = true;
        dl.innerHTML = '';
        msgEl.style.display = '';
        msgEl.className = 'small mb-2 text-muted';
        msgEl.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Checking Lakebase…';

        function row(label, value) {
            return '<dt class="col-sm-4 text-muted">' + escapeHtmlSettings(label) + '</dt>'
                + '<dd class="col-sm-8 font-monospace text-break">' + value + '</dd>';
        }

        try {
            const resp = await fetch('/settings/graph-engine/lakebase-health', { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            if (!data.success) {
                msgEl.className = 'small mb-2 text-warning';
                const m = data.message || data.reason || 'Health check failed';
                msgEl.innerHTML = '<i class="bi bi-exclamation-triangle me-1"></i>' + escapeHtmlSettings(m);
                if (data.host) {
                    dl.innerHTML = row('PGHOST', escapeHtmlSettings(String(data.host)));
                }
                return;
            }
            msgEl.className = 'small mb-2 ' + (data.schema_exists ? 'text-success' : 'text-warning');
            msgEl.innerHTML = '<i class="bi bi-' + (data.schema_exists ? 'check-circle' : 'exclamation-triangle') + ' me-1"></i>'
                + escapeHtmlSettings(data.message || 'OK');
            dl.innerHTML = (
                row('Bound host (PGHOST)', escapeHtmlSettings(String(data.host || '')))
                + row('Port', escapeHtmlSettings(String(data.port != null ? data.port : '')))
                + row('Graph database', escapeHtmlSettings(String(data.graph_database || '')))
                + row('Graph schema', escapeHtmlSettings(String(data.graph_schema || '')))
                + row('Schema exists', data.schema_exists ? 'yes' : 'no')
                + row('Tables in schema', escapeHtmlSettings(String(data.tables_in_schema != null ? data.tables_in_schema : '')))
                + '<dt class="col-sm-4 text-muted text-warning small mt-2">Registry database</dt>'
                + '<dd class="col-sm-8 font-monospace text-break small mt-2 text-muted">'
                + escapeHtmlSettings(String(data.registry_database || ''))
                + ' <span class="text-muted">(PGDATABASE — registry only, separate from graph)</span></dd>'
            );
        } catch (e) {
            msgEl.className = 'small mb-2 text-danger';
            msgEl.innerHTML = '<i class="bi bi-x-circle me-1"></i>' + escapeHtmlSettings(e.message || 'Network error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    function setGraphDbTabLoading(loading) {
        // Only the Lakebase section shows a spinner (ts-global/Global has no spinner).
        const lkBanner = document.getElementById('lakebaseSectionBanner');
        const lkPanel  = document.getElementById('lakebaseGraphPanel');
        if (lkBanner) {
            lkBanner.classList.toggle('d-none', !loading);
            lkBanner.classList.toggle('d-flex', loading);
        }
        // Hide lakebase panel during load; applyGraphDbEnginePanels() restores it after
        if (loading && lkPanel) lkPanel.style.display = 'none';
    }

    /** Reload engine + JSON from server so the tab matches persisted settings after every visit. */
    async function refreshGraphDbTabFromServer() {
        const sel = document.getElementById('graphEngineSelect');
        const ta  = document.getElementById('graphEngineConfig');
        if (!sel || !ta) return;
        try {
            const [engResp, cfgResp] = await Promise.all([
                fetch('/settings/graph-engine',        { credentials: 'same-origin' }),
                fetch('/settings/graph-engine-config', { credentials: 'same-origin' }),
            ]);
            const engData = engResp.ok ? await engResp.json() : {};
            const cfgData = cfgResp.ok ? await cfgResp.json() : {};
            if (cfgData.success) {
                ta.value = JSON.stringify(cfgData.graph_engine_config || {}, null, 2);
            }
            const rawEng = engData.graph_engine;
            if (engData.success && rawEng && typeof rawEng === 'string') {
                const allowed = Array.isArray(engData.allowed_engines) ? engData.allowed_engines : [];
                if (allowed.length === 0 && rawEng === 'lakebase') {
                    sel.value = rawEng;
                } else if (allowed.indexOf(rawEng) >= 0) {
                    sel.value = rawEng;
                } else {
                    sel.value = 'lakebase';
                }
            }
            applyLakebaseFormFromConfigTextarea();
            if (sel.value === 'lakebase') {
                // auto-load the cascading chain if a project is already configured
                await loadLakebaseProjects();
                // guarantee the 4 fields always show the saved registry config,
                // even when the cascade couldn't list/match a stale project
                prefillLakebaseConnectionFromConfig();
                await loadLakebaseGraphHealth();
                // restore UC catalog/schema dropdowns when managed_synced was persisted
                const syncModeEl = document.getElementById('lakebaseSyncMode');
                if (syncModeEl && syncModeEl.value === 'managed_synced') {
                    await loadUcCatalogsForGraphEngine();
                }
            }
        } catch (e) {
            console.log('Graph DB tab refresh failed', e);
        } finally {
            applyGraphDbEnginePanels();
        }
    }

    document.getElementById('graphEngineSelect')?.addEventListener('change', async function () {
        applyGraphDbEnginePanels();
        if (this.value === 'lakebase') {
            applyLakebaseFormFromConfigTextarea();
            await loadLakebaseProjects();
            prefillLakebaseConnectionFromConfig();
            await loadLakebaseGraphHealth();
            const syncModeEl = document.getElementById('lakebaseSyncMode');
            if (syncModeEl && syncModeEl.value === 'managed_synced') {
                await loadUcCatalogsForGraphEngine();
            }
        }
    });

    // cascading project → branch → database → schema
    document.getElementById('btnLoadLakebaseProjects')?.addEventListener('click', () => loadLakebaseProjects());
    document.getElementById('lakebaseProject')?.addEventListener('change', async function () {
        const branchSel = document.getElementById('lakebaseBranch');
        const dbSel     = document.getElementById('lakebaseGraphDb');
        const schSel    = document.getElementById('lakebaseGraphSchema');
        _setSelectLoading(branchSel, '(select a project first)');
        _setSelectLoading(dbSel,     '(select a branch first)');
        _setSelectLoading(schSel,    '(select a database first)');
        mergeLakebasePanelIntoConfigTextarea();
        if (this.value) await loadLakebaseBranches(this.value, '', '');
    });
    document.getElementById('lakebaseBranch')?.addEventListener('change', async function () {
        const dbSel  = document.getElementById('lakebaseGraphDb');
        const schSel = document.getElementById('lakebaseGraphSchema');
        _setSelectLoading(dbSel,  '(select a branch first)');
        _setSelectLoading(schSel, '(select a database first)');
        mergeLakebasePanelIntoConfigTextarea();
        if (this.value) await loadLakebasePgDatabases(this.value, '');
    });
    document.getElementById('lakebaseGraphDb')?.addEventListener('change', async function () {
        const schSel = document.getElementById('lakebaseGraphSchema');
        _setSelectLoading(schSel, '(select a database first)');
        mergeLakebasePanelIntoConfigTextarea();
        if (this.value) {
            const bp = document.getElementById('lakebaseBranch')?.value || '';
            await loadLakebasePgSchemas(this.value, _getCurrentSchemaValue(), bp);
        }
    });
    document.getElementById('lakebaseGraphSchema')?.addEventListener('change', function () {
        mergeLakebasePanelIntoConfigTextarea();
        const ucSchDisplay = document.getElementById('lakebaseUcSchemaDisplay');
        if (ucSchDisplay) ucSchDisplay.value = this.value || '';
    });

    // managed-sync options
    document.getElementById('lakebaseSyncMode')?.addEventListener('change', function () {
        toggleLakebaseManagedSyncPanel();
        updateLakebaseSyncModeHelp();
        mergeLakebasePanelIntoConfigTextarea();
    });
    document.getElementById('lakebaseSyncTableMode')?.addEventListener('change', mergeLakebasePanelIntoConfigTextarea);
    document.getElementById('lakebaseSyncTimeout')?.addEventListener('input',  mergeLakebasePanelIntoConfigTextarea);
    document.getElementById('lakebaseSyncTimeout')?.addEventListener('change', mergeLakebasePanelIntoConfigTextarea);

    // UC catalog change
    document.getElementById('btnLoadUcCatalogs')?.addEventListener('click', () => loadUcCatalogsForGraphEngine());
    document.getElementById('lakebaseUcCatalog')?.addEventListener('change', mergeLakebasePanelIntoConfigTextarea);

    document.getElementById('btnRefreshLakebaseGraphHealth')?.addEventListener('click', () => loadLakebaseGraphHealth());

    // ── Lakebase objects (schemas / tables / views) ──────────────────────────
    async function loadLakebaseObjects() {
        const btn    = document.getElementById('btnLoadLakebaseObjects');
        const result = document.getElementById('lakebaseObjectsResult');
        const dbSel  = document.getElementById('lakebaseGraphDb');
        if (!result) return;

        // Always query the BOUND Lakebase host (where GraphDBFactory writes data).
        // The branch_path from the Connection form refers to the provisioner target
        // project — not the actual connection host — so it must NOT be forwarded here.
        const database   = dbSel?.value   || '';
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Loading…';
        }
        result.innerHTML = '';

        try {
            const params = new URLSearchParams();
            if (database) params.set('database', database);
            const url = '/settings/graph-engine/lakebase-objects'
                + (params.toString() ? '?' + params.toString() : '');
            const resp = await fetch(url, { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            if (!data.success) {
                result.innerHTML = '<div class="alert alert-warning small py-2 mt-2">'
                    + escapeHtmlSettings(data.message || 'Failed to load objects') + '</div>';
                return;
            }

            const cu = data.current_user || '';
            const regSchema = data.registry_schema || 'ontobricks_registry';
            const schemas = (data.schemas || []).filter(o => o.name   !== regSchema);
            const tables  = (data.tables  || []).filter(o => o.schema !== regSchema);
            const views   = (data.views   || []).filter(o => o.schema !== regSchema);

            if (schemas.length === 0 && tables.length === 0 && views.length === 0) {
                result.innerHTML = '<p class="small text-muted mt-2">No objects owned by you in this database.</p>';
                return;
            }

            // ── helpers ─────────────────────────────────────────────────────
            function mkDropBtn(kind, schema, name) {
                return '<button type="button" class="btn btn-outline-danger btn-sm py-0 px-2 lk-drop-obj-btn"'
                    + ' data-lk-kind="'   + escapeHtmlSettings(kind)   + '"'
                    + ' data-lk-schema="' + escapeHtmlSettings(schema) + '"'
                    + ' data-lk-name="'   + escapeHtmlSettings(name)   + '"'
                    + ' title="Drop ' + escapeHtmlSettings(kind) + '">'
                    + '<i class="bi bi-trash3"></i></button>';
            }

            // Strip _sync / __app suffix to get the common base name shared by all
            // three objects belonging to a graph version (view, sync table, companion).
            // Tables: "{base}_sync" and "{base}__app"  →  base = "{domain}_v{version}"
            // Views:  "{base}"                          →  base = "{domain}_v{version}"
            function objectBase(name, kind) {
                if (kind === 'table') {
                    if (name.endsWith('_sync')) return name.slice(0, -5);
                    if (name.endsWith('__app')) return name.slice(0, -5);
                }
                return name;
            }

            function kindBadge(kind) {
                const map = { view: 'bg-info-subtle text-info-emphasis', table: 'bg-primary-subtle text-primary-emphasis', schema: 'bg-secondary-subtle text-secondary-emphasis' };
                return '<span class="badge border ' + (map[kind] || 'bg-secondary-subtle text-secondary-emphasis') + '">'
                    + kind.charAt(0).toUpperCase() + kind.slice(1) + '</span>';
            }

            function mkObjectRow(kind, schemaName, name) {
                return '<tr>'
                    + '<td>' + kindBadge(kind) + '</td>'
                    + '<td class="font-monospace small">' + escapeHtmlSettings(name) + '</td>'
                    + '<td class="text-end">' + mkDropBtn(kind, schemaName, name) + '</td>'
                    + '</tr>';
            }

            // ── group tables + views by base (= domain label) ────────────────
            // Store in the module-level registry so onclick handlers can look
            // up items by key without embedding JSON in HTML attributes
            // (embedded JSON with " quotes breaks onclick="..." delimiters).
            _lkDomainRegistry = {};   // reset for this load
            _lkUCRegistry = {};

            [...tables.map(o => ({ kind: 'table', schemaName: o.schema, name: o.name })),
             ...views.map(o => ({ kind: 'view',  schemaName: o.schema, name: o.name }))]
            .forEach(o => {
                const base = objectBase(o.name, o.kind);
                if (!_lkDomainRegistry[base]) {
                    _lkDomainRegistry[base] = { base, schema: o.schemaName, items: [] };
                }
                _lkDomainRegistry[base].items.push(o);
            });

            // ── render ───────────────────────────────────────────────────────
            let html = '<p class="small text-muted mt-2 mb-3">Connected as: <code>'
                + escapeHtmlSettings(cu) + '</code>.'
                + ' <span><i class="bi bi-eye-slash me-1"></i>Registry schema'
                + ' (<code>' + escapeHtmlSettings(regSchema) + '</code>) hidden.</span></p>';

            // Domain groups — custom collapse cards, one per domain, collapsed by default
            const domainKeys = Object.keys(_lkDomainRegistry).sort();
            if (domainKeys.length > 0) {
                html += '<div class="lk-domain-cards">';
                domainKeys.forEach((key, idx) => {
                    const grp = _lkDomainRegistry[key];
                    // views first (drop order: views before tables)
                    const sorted = [...grp.items].sort((a, b) => {
                        if (a.kind === b.kind) return 0;
                        return a.kind === 'view' ? -1 : 1;
                    });
                    // Store sorted order back so dropDomainObjects picks it up
                    grp.sortedItems = sorted;
                    const collapseId = 'lkDomainCollapse_' + idx;

                    html += '<div class="lk-domain-card">';

                    // ── header ────────────────────────────────────────────
                    html += '<div class="lk-domain-header">';
                    html += '<button class="lk-domain-toggle" type="button"'
                        + ' data-bs-toggle="collapse" data-bs-target="#' + collapseId + '"'
                        + ' aria-expanded="false" aria-controls="' + collapseId + '">';
                    html += '<i class="bi bi-chevron-right lk-chevron"></i>';
                    html += '<i class="bi bi-folder2 text-muted" style="font-size:.85rem"></i>';
                    html += '<span class="lk-domain-name">' + escapeHtmlSettings(key) + '</span>';
                    html += '<span class="badge bg-secondary-subtle text-secondary-emphasis border lk-domain-count">'
                        + grp.items.length + '</span>';
                    html += '</button>';
                    html += '<button type="button"'
                        + ' class="btn btn-sm btn-outline-danger lk-domain-delete-btn lk-drop-domain-btn"'
                        + ' data-lk-domain="' + escapeHtmlSettings(key) + '"'
                        + ' title="Delete all objects for this domain">'
                        + '<i class="bi bi-trash3 me-1"></i>Delete</button>';
                    html += '</div>';

                    // ── body ──────────────────────────────────────────────
                    html += '<div id="' + collapseId + '" class="collapse lk-domain-body">';
                    html += '<table class="table table-sm mb-0"><thead class="table-light"><tr>'
                        + '<th style="width:90px">Type</th><th>Name</th>'
                        + '<th class="text-end" style="width:90px">Action</th>'
                        + '</tr></thead><tbody>';
                    sorted.forEach(o => {
                        html += mkObjectRow(o.kind, o.schemaName, o.name);
                    });
                    html += '</tbody></table>';
                    // Placeholder filled by loadLakebaseSyncObjects() after the main load
                    html += '<div class="lk-sync-slot" data-lk-base="' + escapeHtmlSettings(key) + '"></div>';
                    html += '</div>';

                    html += '</div>'; // /.lk-domain-card
                });
                html += '</div>'; // /.lk-domain-cards
            }


            result.innerHTML = html;

            // Wire buttons after DOM is ready — avoids JSON in HTML attributes
            result.querySelectorAll('.lk-drop-domain-btn').forEach(btn => {
                btn.addEventListener('click', function () {
                    dropDomainObjects(this.dataset.lkDomain);
                });
            });
            result.querySelectorAll('.lk-drop-obj-btn').forEach(btn => {
                btn.addEventListener('click', function () {
                    dropLakebaseObject(
                        this.dataset.lkKind,
                        this.dataset.lkSchema,
                        this.dataset.lkName,
                        database,
                        '',
                    );
                });
            });

            // Toggle .lk-open on the card for chevron rotation + header style
            result.querySelectorAll('.lk-domain-card').forEach(card => {
                const collapseEl = card.querySelector('.collapse');
                if (!collapseEl) return;
                collapseEl.addEventListener('show.bs.collapse', () => card.classList.add('lk-open'));
                collapseEl.addEventListener('hide.bs.collapse', () => card.classList.remove('lk-open'));
            });

            // Best-effort: load UC/Lakeflow sync objects and inject into each domain slot
            loadLakebaseSyncObjects(database, '');
        } catch (e) {
            result.innerHTML = '<div class="alert alert-danger small py-2 mt-2">'
                + escapeHtmlSettings(e.message || 'Network error') + '</div>';
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '<i class="bi bi-arrow-clockwise me-1"></i> Load objects';
            }
        }
    }

    function _showDropSpinner(result, msg) {
        if (result) {
            result.innerHTML = '<div class="d-flex align-items-center gap-2 py-3 text-muted small">'
                + '<span class="spinner-border spinner-border-sm" aria-hidden="true"></span>'
                + '<span>' + escapeHtmlSettings(msg) + '</span></div>';
        }
    }

    async function _execDrop(kind, schema, name, database, branchPath) {
        const label = kind === 'schema' ? '"' + name + '"' : '"' + schema + '"."' + name + '"';
        const result = document.getElementById('lakebaseObjectsResult');
        _showDropSpinner(result, 'Dropping ' + kind + ' ' + label + '…');
        try {
            const resp = await fetch('/settings/graph-engine/lakebase-drop-object', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ kind, schema, name, database: database || '' }),
            });
            let data = {};
            try { data = await resp.json(); } catch (_) {}
            if (data.success) {
                showNotification('Dropped ' + kind + ' ' + label, 'success');
                await loadLakebaseObjects();
            } else {
                const msg = data.detail || data.message || ('HTTP ' + resp.status);
                if (result) {
                    result.insertAdjacentHTML('afterbegin',
                        '<div class="alert alert-danger small py-2 mb-2">'
                        + escapeHtmlSettings(msg) + '</div>');
                }
                showNotification('Drop failed: ' + msg, 'danger');
            }
        } catch (e) {
            showNotification('Drop error: ' + (e.message || 'Network error'), 'danger');
        }
    }

    function dropLakebaseObject(kind, schema, name, database, branchPath) {
        const label = kind === 'schema' ? '"' + name + '"' : '"' + schema + '"."' + name + '"';
        const cascade = kind === 'schema' ? '<br><small class="text-muted">This will also drop all tables and views inside it (CASCADE).</small>' : '';
        const modalEl = document.getElementById('lkDropConfirmModal');
        const bodyEl  = document.getElementById('lkDropConfirmModalBody');
        const confirmBtn = document.getElementById('lkDropConfirmBtn');
        if (!modalEl || !bodyEl || !confirmBtn) {
            // Fallback for contexts where the modal wasn't injected yet
            if (window.confirm('Drop ' + kind + ' ' + label + (kind === 'schema' ? ' CASCADE?' : '?'))) {
                _execDrop(kind, schema, name, database, branchPath);
            }
            return;
        }
        bodyEl.innerHTML = 'Drop <strong>' + kind + '</strong> <code>' + escapeHtmlSettings(label) + '</code>?' + cascade;
        // Remove any previous listener to avoid stacking
        const newBtn = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newBtn, confirmBtn);
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        newBtn.addEventListener('click', function () {
            modal.hide();
            _execDrop(kind, schema, name, database, branchPath);
        });
        modal.show();
    }

    // ── Permissions tab ──────────────────────────────────────────────────────

    function _lkPermEsc(s) {
        return String(s ?? '')
            .replace(/&/g, '&amp;').replace(/</g, '&lt;')
            .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    function _lkPermBanner(cls, html) {
        const el = document.getElementById('lkPermBanner');
        if (!el) return;
        el.className = 'alert ' + cls + ' py-2 px-3 small mb-3';
        el.innerHTML = html;
    }

    async function _lkPermGrantEmail(email) {
        if (!email) {
            _lkPermBanner('alert-warning', 'Please select a user first.');
            return;
        }
        _lkPermBanner('alert-info',
            '<span class="spinner-border spinner-border-sm me-2" role="status"></span>Granting superuser to <strong>' + _lkPermEsc(email) + '</strong>…');
        try {
            const resp = await fetch('/settings/graph-engine/lakebase-grant-superuser', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({user_email: email}),
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) throw new Error(data.detail || data.message || 'Failed');
            _lkPermBanner('alert-success',
                '<i class="bi bi-check-circle me-1"></i>' + _lkPermEsc(data.message || 'Done'));
            await loadLakebasePermissions();
        } catch (e) {
            _lkPermBanner('alert-danger', 'Grant failed: ' + _lkPermEsc(e.message));
        }
    }

    async function loadLakebasePermissions() {
        const loading   = document.getElementById('lkPermLoading');
        const tableWrap = document.getElementById('lkPermTableWrap');
        const tbody     = document.getElementById('lkPermTbody');
        const empty     = document.getElementById('lkPermEmpty');
        const selUser   = document.getElementById('lkPermUserSelect');
        if (!loading) return;

        const bannerEl = document.getElementById('lkPermBanner');
        if (bannerEl) bannerEl.className = 'alert d-none py-2 px-3 small mb-3';
        loading.classList.remove('d-none');
        tableWrap.classList.add('d-none');

        let data;
        try {
            const resp = await fetch('/settings/graph-engine/lakebase-pg-roles');
            data = await resp.json();
            if (!resp.ok || !data.success) throw new Error(data.detail || data.message || 'Failed');
        } catch (e) {
            loading.classList.add('d-none');
            _lkPermBanner('alert-danger', 'Could not load permissions: ' + _lkPermEsc(e.message));
            return;
        }

        // Build email→role lookup
        const roleMap = {};
        (data.roles || []).forEach(r => { roleMap[r.email.toLowerCase()] = r; });

        // Merge: app_users + any Postgres roles not in app_users
        const appUsers = data.app_users || [];
        const appEmails = new Set(appUsers.map(u => u.email.toLowerCase()));
        const extraRoles = (data.roles || []).filter(r => !appEmails.has(r.email.toLowerCase()));

        const allRows = [
            ...appUsers.map(u => ({email: u.email, display: u.display_name, fromApp: true})),
            ...extraRoles.map(r => ({email: r.email, display: r.email, fromApp: false})),
        ];

        // Populate dropdown
        if (selUser) {
            const prevVal = selUser.value;
            selUser.innerHTML = '<option value="">— select a user —</option>';
            allRows.forEach(row => {
                const opt = document.createElement('option');
                opt.value = row.email;
                opt.textContent = row.display + (row.display !== row.email ? ' (' + row.email + ')' : '');
                selUser.appendChild(opt);
            });
            if (prevVal) selUser.value = prevVal;
        }

        // Render table
        tbody.innerHTML = '';
        empty.classList.toggle('d-none', allRows.length > 0);
        allRows.forEach(row => {
            const em   = row.email.toLowerCase();
            const role = roleMap[em];
            const hasRole      = Boolean(role);
            const hasSuperuser = hasRole && role.has_superuser;

            const tr = document.createElement('tr');

            // User cell
            const tdUser = document.createElement('td');
            tdUser.className = 'align-middle';
            tdUser.innerHTML = row.display !== row.email
                ? '<span class="fw-semibold">' + _lkPermEsc(row.display) + '</span>'
                  + ' <span class="text-muted small">' + _lkPermEsc(row.email) + '</span>'
                : '<span class="font-monospace small">' + _lkPermEsc(row.email) + '</span>';
            tr.appendChild(tdUser);

            // Role cell
            const tdRole = document.createElement('td');
            tdRole.className = 'text-center align-middle';
            tdRole.innerHTML = hasRole
                ? '<span class="badge bg-success-subtle text-success-emphasis">yes</span>'
                : '<span class="badge bg-secondary-subtle text-secondary-emphasis">none</span>';
            tr.appendChild(tdRole);

            // Superuser cell
            const tdSu = document.createElement('td');
            tdSu.className = 'text-center align-middle';
            tdSu.innerHTML = hasSuperuser
                ? '<span class="badge bg-primary-subtle text-primary-emphasis"><i class="bi bi-shield-fill-check me-1"></i>superuser</span>'
                : '<span class="badge bg-warning-subtle text-warning-emphasis">no</span>';
            tr.appendChild(tdSu);

            // Action cell
            const tdBtn = document.createElement('td');
            tdBtn.className = 'text-end align-middle';
            const btn = document.createElement('button');
            btn.className = 'btn btn-xs btn-outline-primary py-0 px-2';
            btn.disabled = hasSuperuser;
            btn.dataset.email = row.email;
            btn.innerHTML = '<i class="bi bi-shield-plus me-1"></i>Grant';
            btn.addEventListener('click', function () {
                _lkPermGrantEmail(this.dataset.email);
            });
            tdBtn.appendChild(btn);
            tr.appendChild(tdBtn);

            tbody.appendChild(tr);
        });

        loading.classList.add('d-none');
        tableWrap.classList.remove('d-none');
    }

    // Wire Permissions tab listeners once
    (function () {
        const tabBtn     = document.getElementById('lktab-perms');
        const grantBtn   = document.getElementById('btnLkPermGrant');
        const refreshBtn = document.getElementById('btnLkPermRefresh');
        let loaded = false;

        if (tabBtn) {
            tabBtn.addEventListener('shown.bs.tab', function () {
                if (!loaded) { loaded = true; loadLakebasePermissions(); }
            });
        }
        if (grantBtn) {
            grantBtn.addEventListener('click', function () {
                const sel = document.getElementById('lkPermUserSelect');
                _lkPermGrantEmail(sel ? sel.value.trim() : '');
            });
        }
        if (refreshBtn) {
            refreshBtn.addEventListener('click', function () { loadLakebasePermissions(); });
        }
    }());

    /** Drop all objects for a domain (views first, then tables, then UC/Lakeflow sync objects).
     *  Takes only the registry key — items are looked up from _lkDomainRegistry
     *  to avoid embedding JSON in HTML onclick attributes. */
    function dropDomainObjects(domainKey) {
        const entry = _lkDomainRegistry[domainKey];
        if (!entry) {
            showNotification('Domain not found: ' + domainKey, 'danger');
            return;
        }
        const { schema, sortedItems: items } = entry;
        const ucItems = _lkUCRegistry[domainKey] || [];
        const database   = document.getElementById('lakebaseGraphDb')?.value  || '';
        const branchPath = document.getElementById('lakebaseBranch')?.value   || '';
        const count = items.length + ucItems.length;

        const pgListHtml = items.map(o =>
            '<li class="font-monospace small">' + escapeHtmlSettings(o.kind) + ': '
            + escapeHtmlSettings(o.name) + '</li>'
        ).join('');
        const ucListHtml = ucItems.map(u =>
            '<li class="font-monospace small">'
            + (u.is_sync ? 'sync (Lakeflow): ' : 'delta: ')
            + escapeHtmlSettings(u.full_name) + '</li>'
        ).join('');
        const listHtml = pgListHtml + (ucListHtml
            ? '<li class="small text-muted mt-1 fw-semibold" style="list-style:none;margin-left:-1rem">Unity Catalog</li>'
              + ucListHtml
            : '');

        const bodyContent = 'Drop all <strong>' + count + ' object' + (count !== 1 ? 's' : '')
            + '</strong> for domain <code>' + escapeHtmlSettings(domainKey) + '</code>?'
            + '<ul class="mt-2 mb-0 ps-3">' + listHtml + '</ul>';

        const modalEl  = document.getElementById('lkDropConfirmModal');
        const bodyEl   = document.getElementById('lkDropConfirmModalBody');
        const confirmBtn = document.getElementById('lkDropConfirmBtn');

        if (!modalEl || !bodyEl || !confirmBtn) {
            if (window.confirm('Drop all ' + count + ' objects for domain ' + domainKey + '?')) {
                _execDropAll(items, schema, database, branchPath, ucItems);
            }
            return;
        }

        bodyEl.innerHTML = bodyContent;
        const newBtn = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newBtn, confirmBtn);
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        newBtn.addEventListener('click', function () {
            modal.hide();
            _execDropAll(items, schema, database, branchPath, ucItems);
        });
        modal.show();
    }

    /** Execute sequential drops: Postgres objects first, then UC/Lakeflow sync objects. */
    async function _execDropAll(items, schema, database, branchPath, ucItems = []) {
        const result = document.getElementById('lakebaseObjectsResult');
        const errors = [];
        const total = items.length + ucItems.length;
        _showDropSpinner(result, 'Deleting ' + total + ' object' + (total !== 1 ? 's' : '') + '…');

        // ── Postgres objects ─────────────────────────────────────────────
        for (let i = 0; i < items.length; i++) {
            const o = items[i];
            _showDropSpinner(result, 'Dropping ' + o.kind + ' ' + o.name + ' (' + (i + 1) + '/' + total + ')…');
            try {
                const resp = await fetch('/settings/graph-engine/lakebase-drop-object', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin',
                    body: JSON.stringify({ kind: o.kind, schema, name: o.name, database: database || '' }),
                });
                let data = {};
                try { data = await resp.json(); } catch (_) { /* non-JSON body */ }
                if (!data.success) {
                    const detail = data.detail || data.message || (resp.ok ? 'server returned failure' : 'HTTP ' + resp.status);
                    errors.push(o.kind + ' ' + o.name + ': ' + detail);
                }
            } catch (e) {
                errors.push(o.kind + ' ' + o.name + ': ' + (e.message || 'network error'));
            }
        }

        // ── UC / Lakeflow sync objects ────────────────────────────────────
        for (let j = 0; j < ucItems.length; j++) {
            const u = ucItems[j];
            const label = (u.is_sync ? 'sync' : 'delta') + ' ' + u.full_name;
            _showDropSpinner(result, 'Dropping ' + label + ' (' + (items.length + j + 1) + '/' + total + ')…');
            try {
                const resp = await fetch('/settings/graph-engine/drop-uc-object', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin',
                    body: JSON.stringify({ full_name: u.full_name, is_sync: u.is_sync }),
                });
                let data = {};
                try { data = await resp.json(); } catch (_) { /* non-JSON body */ }
                if (!data.success) {
                    const detail = data.detail || data.message || (resp.ok ? 'server returned failure' : 'HTTP ' + resp.status);
                    errors.push(label + ': ' + detail);
                }
            } catch (e) {
                errors.push(label + ': ' + (e.message || 'network error'));
            }
        }

        if (errors.length) {
            showNotification('Drops failed:\n' + errors.join('\n'), 'danger');
            if (result) {
                result.innerHTML = '<div class="alert alert-danger small py-2 mb-2"><strong>Drop errors:</strong><ul class="mb-0 mt-1 ps-3">'
                    + errors.map(e => '<li>' + escapeHtmlSettings(e) + '</li>').join('')
                    + '</ul></div>';
            }
        } else {
            showNotification('All domain objects dropped', 'success');
        }
        _showDropSpinner(result, 'Reloading objects…');
        await loadLakebaseObjects();
    }


    /** Fetch UC/Lakeflow synced-table objects and inject into each domain's sync slot. */
    async function loadLakebaseSyncObjects(database, branchPath) {
        const slots = document.querySelectorAll('.lk-sync-slot');
        if (!slots.length) return;

        // Show a spinner in each slot while loading
        slots.forEach(slot => {
            slot.innerHTML = '<div class="lk-sync-loading d-flex align-items-center gap-2 px-3 py-2 border-top">'
                + '<span class="spinner-border spinner-border-sm text-muted" aria-hidden="true"></span>'
                + '<span class="small text-muted">Loading sync objects…</span></div>';
        });

        function stateBadge(state) {
            const map = {
                ONLINE: 'bg-success-subtle text-success-emphasis',
                ONLINE_NO_PENDING_UPDATE: 'bg-success-subtle text-success-emphasis',
                PROVISIONING: 'bg-info-subtle text-info-emphasis',
                PROVISIONING_INITIAL_SNAPSHOT: 'bg-info-subtle text-info-emphasis',
                PROVISIONING_PIPELINE_RESOURCES: 'bg-info-subtle text-info-emphasis',
                ONLINE_TRIGGERED_UPDATE: 'bg-info-subtle text-info-emphasis',
                ONLINE_CONTINUOUS_UPDATE: 'bg-info-subtle text-info-emphasis',
                FAILED: 'bg-danger-subtle text-danger-emphasis',
                OFFLINE_FAILED: 'bg-danger-subtle text-danger-emphasis',
                TABLED_OFFLINE: 'bg-danger-subtle text-danger-emphasis',
                ERROR: 'bg-danger-subtle text-danger-emphasis',
                NOT_FOUND: 'bg-secondary-subtle text-secondary-emphasis',
                TIMEOUT: 'bg-warning-subtle text-warning-emphasis',
                UNKNOWN: 'bg-secondary-subtle text-secondary-emphasis',
            };
            const cls = map[state] || 'bg-secondary-subtle text-secondary-emphasis';
            return '<span class="badge border ' + cls + ' lk-sync-state-badge">'
                + escapeHtmlSettings(state || '—') + '</span>';
        }

        try {
            const params = new URLSearchParams();
            if (database)   params.set('database',    database);
            if (branchPath) params.set('branch_path', branchPath);
            const url = '/settings/graph-engine/lakebase-sync-objects'
                + (params.toString() ? '?' + params.toString() : '');
            const resp = await fetch(url, { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};

            if (!data.success || !data.uc_tables?.length) {
                slots.forEach(slot => { slot.innerHTML = ''; });
                return;
            }

            // Group UC tables by domain base name:
            //   "domain_v1_sync"  → base "domain_v1"  (Lakeflow synced table)
            //   "domain_v1"       → base "domain_v1"  (Delta source table/view)
            const byBase = {};
            (data.uc_tables || []).forEach(t => {
                const base = t.name.endsWith('_sync') ? t.name.slice(0, -5) : t.name;
                if (!byBase[base]) byBase[base] = [];
                byBase[base].push(t);
            });
            // Publish to module-level registry so dropDomainObjects can include them.
            _lkUCRegistry = byBase;

            const ucLabel = data.uc_catalog && data.uc_schema
                ? data.uc_catalog + '.' + data.uc_schema : '';

            slots.forEach(slot => {
                const base = slot.dataset.lkBase || '';
                const tables = byBase[base];
                if (!tables || !tables.length) {
                    slot.innerHTML = '';
                    return;
                }

                let h = '<div class="lk-sync-section border-top">';
                h += '<div class="lk-sync-header px-3 py-1 d-flex align-items-center gap-2">'
                    + '<span class="small text-muted fw-semibold" style="letter-spacing:.04em;font-size:.72rem;text-transform:uppercase">'
                    + '<i class="bi bi-table me-1"></i>Unity Catalog</span>';
                if (ucLabel) {
                    h += '<span class="badge bg-light border text-muted font-monospace" style="font-size:.68rem">'
                        + escapeHtmlSettings(ucLabel) + '</span>';
                }
                h += '</div>';
                h += '<table class="table table-sm mb-0 lk-sync-table"><tbody>';

                function mkUCDropBtn(fullName, isSync) {
                    return '<button type="button" class="btn btn-outline-danger btn-sm py-0 px-1 lk-drop-uc-btn"'
                        + ' data-lk-full-name="' + escapeHtmlSettings(fullName) + '"'
                        + ' data-lk-is-sync="' + (isSync ? '1' : '0') + '"'
                        + ' title="Drop ' + escapeHtmlSettings(fullName) + '">'
                        + '<i class="bi bi-trash" style="font-size:.75rem"></i></button>';
                }

                tables.forEach(t => {
                    if (t.is_sync) {
                        // Lakeflow synced-table registration row
                        const pipelineLink = t.pipeline_id
                            ? ' <a href="#" class="lk-sync-pipeline-link small text-muted ms-1"'
                              + ' data-lk-pipeline-id="' + escapeHtmlSettings(t.pipeline_id) + '"'
                              + ' title="Copy pipeline ID: ' + escapeHtmlSettings(t.pipeline_id) + '">'
                              + '<i class="bi bi-clipboard" style="font-size:.7rem"></i></a>'
                            : '';
                        const errorTip = t.error
                            ? ' <span class="text-danger ms-1" title="' + escapeHtmlSettings(t.error) + '">'
                              + '<i class="bi bi-exclamation-circle" style="font-size:.75rem"></i></span>'
                            : '';
                        h += '<tr>'
                            + '<td style="width:90px"><span class="badge border bg-warning-subtle text-warning-emphasis lk-sync-badge">sync</span></td>'
                            + '<td class="font-monospace lk-sync-uc-cell">'
                            + escapeHtmlSettings(t.full_name) + errorTip + '</td>'
                            + '<td class="text-end" style="width:120px">'
                            + (t.state ? stateBadge(t.state) : '') + pipelineLink
                            + ' ' + mkUCDropBtn(t.full_name, true) + '</td>'
                            + '</tr>';
                        // Lakeflow source table sub-row
                        if (t.source_table) {
                            h += '<tr class="lk-sync-source-row">'
                                + '<td></td>'
                                + '<td class="font-monospace lk-sync-uc-cell text-muted" colspan="2">'
                                + '<i class="bi bi-arrow-return-right me-1 text-muted" style="font-size:.7rem"></i>'
                                + 'source: ' + escapeHtmlSettings(t.source_table) + '</td>'
                                + '</tr>';
                        }
                    } else {
                        // Delta table / view row
                        const typeBadge = (t.table_type || '').toLowerCase() === 'view'
                            ? '<span class="badge border bg-info-subtle text-info-emphasis lk-sync-badge">view</span>'
                            : '<span class="badge border bg-primary-subtle text-primary-emphasis lk-sync-badge">delta</span>';
                        h += '<tr>'
                            + '<td style="width:90px">' + typeBadge + '</td>'
                            + '<td class="font-monospace lk-sync-uc-cell text-muted">'
                            + escapeHtmlSettings(t.full_name) + '</td>'
                            + '<td class="text-end" style="width:120px">'
                            + mkUCDropBtn(t.full_name, false) + '</td>'
                            + '</tr>';
                    }
                });

                h += '</tbody></table></div>';
                slot.innerHTML = h;

                slot.querySelectorAll('.lk-sync-pipeline-link').forEach(a => {
                    a.addEventListener('click', function (e) {
                        e.preventDefault();
                        const pid = this.dataset.lkPipelineId || '';
                        if (pid && navigator.clipboard) {
                            navigator.clipboard.writeText(pid).then(() => {
                                showNotification('Pipeline ID copied: ' + pid, 'info', 2000);
                            });
                        } else if (pid) {
                            showNotification('Pipeline ID: ' + pid, 'info', 3000);
                        }
                    });
                });

                slot.querySelectorAll('.lk-drop-uc-btn').forEach(btn => {
                    btn.addEventListener('click', function () {
                        dropUCObject(
                            this.dataset.lkFullName,
                            this.dataset.lkIsSync === '1',
                        );
                    });
                });
            });
        } catch (e) {
            slots.forEach(slot => { slot.innerHTML = ''; });
        }
    }

    /** Ask for confirmation, then DROP a Unity Catalog table or Lakeflow synced-table. */
    function dropUCObject(fullName, isSync) {
        const kindLabel = isSync ? 'Lakeflow sync table' : 'UC table';
        const warn = isSync
            ? '<br><small class="text-muted">This will also remove the Lakeflow pipeline registration.</small>'
            : '';
        const modalEl  = document.getElementById('lkDropConfirmModal');
        const bodyEl   = document.getElementById('lkDropConfirmModalBody');
        const confirmBtn = document.getElementById('lkDropConfirmBtn');
        if (!modalEl || !bodyEl || !confirmBtn) { return; }

        bodyEl.innerHTML = 'Are you sure you want to drop the ' + kindLabel
            + ' <strong>' + escapeHtmlSettings(fullName) + '</strong>?' + warn;

        const fresh = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(fresh, confirmBtn);
        fresh.addEventListener('click', async function () {
            bootstrap.Modal.getInstance(modalEl)?.hide();
            try {
                const resp = await fetch('/settings/graph-engine/drop-uc-object', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin',
                    body: JSON.stringify({ full_name: fullName, is_sync: isSync }),
                });
                const result = await resp.json();
                if (result.success) {
                    showNotification('Dropped: ' + fullName, 'success', 3000);
                    loadLakebaseObjects();
                } else {
                    showNotification('Error: ' + (result.message || result.detail || 'Unknown error'), 'error', 5000);
                }
            } catch (err) {
                showNotification('Request failed: ' + err.message, 'error', 5000);
            }
        });

        bootstrap.Modal.getOrCreateInstance(modalEl).show();
    }

    document.getElementById('btnLoadLakebaseObjects')?.addEventListener('click', loadLakebaseObjects);

    // ── Provision new graph DB from scratch ──────────────────────────────────
    const PROVISION_TASK_KEY = 'ontobricks_lakebase_provision_task';

    function updateProvProgress(percent, text) {
        const bar = document.getElementById('provProgressBar');
        const status = document.getElementById('provStatusText');
        if (bar) {
            const pct = Math.max(0, Math.min(100, percent || 0));
            bar.style.width = pct + '%';
            bar.textContent = pct + '%';
        }
        if (status && text) status.textContent = text;
    }

    function renderProvStepLog(task) {
        const list = document.getElementById('provStepLog');
        if (!list || !task || !Array.isArray(task.steps)) return;
        const icon = (s) => {
            if (s === 'completed') return '<i class="bi bi-check-circle-fill text-success me-2"></i>';
            if (s === 'running')   return '<span class="spinner-border spinner-border-sm text-primary me-2"></span>';
            if (s === 'failed')    return '<i class="bi bi-x-circle-fill text-danger me-2"></i>';
            if (s === 'skipped')   return '<i class="bi bi-dash-circle text-muted me-2"></i>';
            return '<i class="bi bi-circle text-muted me-2"></i>';
        };
        const rows = task.steps.map(s => {
            // Surface the live message under the running step and the error
            // under the failed step so each step has a visible log line.
            let detail = '';
            if (s.status === 'running' && task.message) {
                detail = '<div class="small text-muted ms-4">' +
                    escapeHtmlSettings(task.message) + '</div>';
            } else if (s.status === 'failed' && task.error) {
                detail = '<div class="small text-danger ms-4">' +
                    escapeHtmlSettings(task.error) + '</div>';
            }
            return '<li class="list-group-item bg-transparent px-0 py-1">' +
                '<div class="d-flex align-items-center">' +
                icon(s.status) + '<span>' +
                escapeHtmlSettings(s.description || s.name) + '</span></div>' +
                detail + '</li>';
        });
        list.innerHTML = rows.join('');
    }

    function _provDone() {
        const btn = document.getElementById('btnProvisionLakebaseGraph');
        if (btn) btn.disabled = false;
    }

    async function monitorProvisionTask(taskId) {
        const pollInterval = 1500;
        const area = document.getElementById('provProgressArea');
        if (area) area.classList.remove('d-none');
        while (true) {
            try {
                await new Promise(r => setTimeout(r, pollInterval));
                const resp = await fetch('/tasks/' + encodeURIComponent(taskId), { credentials: 'same-origin' });
                const data = await resp.json();
                if (!data.success) throw new Error('Task not found');
                const task = data.task;
                updateProvProgress(task.progress || 0, task.message || '');
                renderProvStepLog(task);

                if (task.status === 'completed') {
                    sessionStorage.removeItem(PROVISION_TASK_KEY);
                    const warnings = (task.result && task.result.warnings) || [];
                    if (warnings.length) {
                        showNotification('Graph DB created with ' + warnings.length +
                            ' warning(s): ' + warnings.join(' | '), 'warning', 8000);
                    } else {
                        showNotification('Lakebase graph DB created successfully!', 'success', 4000);
                    }
                    _provDone();
                    // Refresh the connection pickers so the new project shows up.
                    if (typeof loadLakebaseProjects === 'function') loadLakebaseProjects();
                    if (typeof refreshTasks === 'function') refreshTasks();
                    break;
                } else if (task.status === 'failed') {
                    sessionStorage.removeItem(PROVISION_TASK_KEY);
                    showNotification('Provisioning failed: ' + (task.error || 'Unknown error'), 'error', 8000);
                    _provDone();
                    break;
                } else if (task.status === 'cancelled') {
                    sessionStorage.removeItem(PROVISION_TASK_KEY);
                    showNotification('Provisioning was cancelled', 'warning');
                    _provDone();
                    break;
                }
            } catch (err) {
                sessionStorage.removeItem(PROVISION_TASK_KEY);
                showNotification('Provisioning monitoring failed: ' + (err.message || 'unknown'), 'error');
                _provDone();
                break;
            }
        }
    }

    // Lowercase + restrict to [a-z0-9_-]; mirrors the backend normaliser so
    // the value the operator sees matches what gets created.
    function normalizeProvName(raw) {
        return (raw || '').trim().toLowerCase()
            .replace(/[^a-z0-9_-]+/g, '_')
            .replace(/^[-_]+|[-_]+$/g, '');
    }

    // Per-keystroke variant: 1:1 char replacement (preserves caret position)
    // and no edge trimming so the operator can still type a leading "_".
    function normalizeProvNameLive(raw) {
        return (raw || '').toLowerCase().replace(/[^a-z0-9_-]/g, '_');
    }

    async function provisionLakebaseGraph() {
        const btn = document.getElementById('btnProvisionLakebaseGraph');
        const name = normalizeProvName(document.getElementById('provInstanceName')?.value);
        const database = normalizeProvName(document.getElementById('provDatabase')?.value);
        if (!name || !database) {
            showNotification('Instance name and Postgres database are required.', 'warning');
            return;
        }
        const payload = {
            name: name,
            capacity: document.getElementById('provCapacity')?.value || 'CU_2',
            branch: normalizeProvName(document.getElementById('provBranch')?.value) || 'production',
            database: database,
            schema: normalizeProvName(document.getElementById('provSchema')?.value) || 'ontobricks_graph',
            mcp_app_name: (document.getElementById('provMcpAppName')?.value || '').trim(),
            grant_uc_catalog: !!document.getElementById('provGrantUcCatalog')?.checked,
        };
        if (btn) btn.disabled = true;
        const list = document.getElementById('provStepLog');
        if (list) list.innerHTML = '';
        updateProvProgress(0, 'Starting…');
        const area = document.getElementById('provProgressArea');
        if (area) area.classList.remove('d-none');

        try {
            const resp = await fetch('/settings/graph-engine/lakebase-provision', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify(payload),
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                showNotification('Error: ' + (data.message || data.detail || 'Failed to start provisioning'), 'error', 6000);
                _provDone();
                return;
            }
            sessionStorage.setItem(PROVISION_TASK_KEY, data.task_id);
            monitorProvisionTask(data.task_id);
        } catch (err) {
            showNotification('Error: ' + err.message, 'error', 6000);
            _provDone();
        }
    }

    document.getElementById('btnProvisionLakebaseGraph')?.addEventListener('click', provisionLakebaseGraph);

    // Live-normalise the name fields on every keystroke so the operator always
    // sees a value that matches what the backend will create (lowercase,
    // [a-z0-9_-] only). The caret is restored since the replacement is 1:1.
    ['provInstanceName', 'provDatabase', 'provSchema'].forEach((id) => {
        document.getElementById(id)?.addEventListener('input', (e) => {
            const el = e.target;
            const start = el.selectionStart;
            const end = el.selectionEnd;
            const next = normalizeProvNameLive(el.value);
            if (next !== el.value) {
                el.value = next;
                try { el.setSelectionRange(start, end); } catch (_) { /* ignore */ }
            }
        });
    });

    // Resume a provisioning task across reloads (reopen the modal so the
    // live progress is visible again).
    (function _resumeProvisionTask() {
        const taskId = sessionStorage.getItem(PROVISION_TASK_KEY);
        if (taskId) {
            const btn = document.getElementById('btnProvisionLakebaseGraph');
            if (btn) btn.disabled = true;
            const modalEl = document.getElementById('lakebaseProvisionModal');
            if (modalEl && window.bootstrap) {
                bootstrap.Modal.getOrCreateInstance(modalEl).show();
            }
            monitorProvisionTask(taskId);
        }
    })();

    _initSchemaToggle();

    /** Persist graph engine and JSON config (used by global Save). */
    async function saveGraphDbSettings(errors) {
        const sel = document.getElementById('graphEngineSelect');
        const ta = document.getElementById('graphEngineConfig');
        const errDiv = document.getElementById('graphEngineConfigError');
        if (!sel || !ta) return;

        if (errDiv) errDiv.style.display = 'none';

        try {
            const resp = await fetch('/settings/graph-engine', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ graph_engine: sel.value }),
            });
            const result = await resp.json();
            if (!result.success) {
                errors.push('Graph DB engine: ' + (result.message || 'Unknown error'));
                return;
            }

            if (sel.value === 'lakebase') {
                mergeLakebasePanelIntoConfigTextarea();
            }

            let parsed;
            try {
                parsed = JSON.parse(ta.value || '{}');
            } catch (parseErr) {
                errors.push('Graph DB config: invalid JSON (' + parseErr.message + ')');
                if (errDiv) {
                    errDiv.textContent = 'Invalid JSON: ' + parseErr.message;
                    errDiv.style.display = 'block';
                }
                return;
            }
            if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
                errors.push('Graph DB config: must be a JSON object');
                if (errDiv) {
                    errDiv.textContent = 'Configuration must be a JSON object (not an array or primitive)';
                    errDiv.style.display = 'block';
                }
                return;
            }

            const cfgResp = await fetch('/settings/graph-engine-config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ graph_engine_config: parsed }),
            });
            const cfgJson = await cfgResp.json();
            if (!cfgJson.success) {
                errors.push('Graph DB config: ' + (cfgJson.message || 'Unknown error'));
                return;
            }
            ta.value = JSON.stringify(cfgJson.graph_engine_config || parsed, null, 2);
            applyGraphDbEnginePanels();
            if (sel.value === 'lakebase') loadLakebaseGraphHealth();
        } catch (e) {
            errors.push('Graph DB: ' + e.message);
        }
    }

    // =====================================================================
    //  TRIPLE STORE SECTIONS – lazy-load on first visit to ts-global or lakebase
    // =====================================================================

    document.addEventListener('sidebarSectionChanged', async (e) => {
        const s = e.detail?.section;
        if ((s === 'ts-global' || s === 'lakebase') && !graphDbLoaded) {
            graphDbLoaded = true;
            setGraphDbTabLoading(true);
            try {
                await refreshGraphDbTabFromServer();
            } finally {
                setGraphDbTabLoading(false);
            }
        }
    });

    // =====================================================================
    //  GLOBAL SAVE BUTTON – warehouse, global prefs, CloudFetch, Graph DB
    // =====================================================================

    document.querySelectorAll('.btn-save-settings').forEach(saveBtn => saveBtn.addEventListener('click', async function () {
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

        // 3b. Save edit-lock lease TTL (UI in minutes → API in seconds; 0 disables)
        const lockTtlInput = document.getElementById('editLockTtlMin');
        if (lockTtlInput) {
            const mins = parseInt(lockTtlInput.value, 10);
            if (!isNaN(mins) && mins >= 0) {
                try {
                    const resp = await fetch('/settings/save-edit-lock-ttl', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        credentials: 'same-origin',
                        body: JSON.stringify({ edit_lock_ttl_s: mins * 60 })
                    });
                    const r = await resp.json();
                    if (!r.success) errors.push('Edit lock lease: ' + r.message);
                } catch (e) { errors.push('Edit lock lease: ' + e.message); }
            }
        }

        // 4. Graph DB engine + JSON config (same tab; top Save only)
        await saveGraphDbSettings(errors);

        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-check-circle me-1"></i> Save';

        if (errors.length > 0) {
            showNotification('Some settings failed to save:\n' + errors.join('\n'), 'error');
        } else {
            showNotification('All settings saved', 'success', 2000);
        }
    }));
});
