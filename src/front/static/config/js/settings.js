/**
 * OntoBricks - settings.js
 * Settings page JavaScript – sidebar layout; global Save persists all sections including triple store
 */

document.addEventListener('DOMContentLoaded', function () {

    let currentWarehouseId = null;
    let currentDeltaWarehouseId = null;
    let effectiveDeltaWarehouseId = '';
    let warehouseLocked = false;
    // graphDbLoaded → the triple-store backend value is loaded (all the Back End
    // sub-page needs). graphEngineConfigLoaded → the graph engine + JSON config
    // textarea are loaded (needed by a Lakebase Save and by the heavy cascade).
    // graphDbHeavyLoaded → the remote Lakebase/Delta cascade + health have been
    // fetched (deferred until the Lakebase/Delta sections are opened).
    let graphDbLoaded = false;
    let graphEngineConfigLoaded = false;
    let graphDbHeavyLoaded = false;
    // Whether the analytics-job checkbox reflects the stored value yet. The
    // Save handler posts that checkbox from *every* section's Save button, and
    // its markup default is unchecked, so without this flag a hydration that
    // failed or had not resolved yet would make the next Save silently write
    // "off" over an admin's "on" — with no error and nobody touching the box.
    let analyticsJobHydrated = false;
    // Registry rebuilt on every loadLakebaseObjects call; keyed by domain base name.
    // Avoids embedding JSON in onclick HTML attributes (double quotes break the attribute).
    let _lkDomainRegistry = {};
    // UC/Lakeflow objects keyed by domain base name; populated by loadLakebaseSyncObjects.
    let _lkUCRegistry = {};
    // UC analytics tables keyed by domain base name; populated by loadLakebaseAnalyticsObjects.
    let _lkAnalyticsRegistry = {};
    // Analytics groups matching no domain, keyed by slug; shown as the orphan card.
    let _lkOrphanRegistry = {};
    // Delta UC objects keyed by domain base name (triplestore_<safe>_V<n>).
    let _dtDomainRegistry = {};
    // Analytics groups whose slug matches no domain, keyed by that slug.
    let _dtOrphanRegistry = {};

    function escapeHtmlSettings(str) { return escapeHtml(str); }

    // The graph backend *selection* moved to a mandatory per-domain choice
    // (Domain Information -> Knowledge Graph tab). The Settings Graph DB pages
    // now only configure the engine *connections* (Lakebase / Neo4j / Delta).

    loadCurrentConfig();
    loadBaseUri();
    loadCurrentDefaultEmoji();

    loadRegistryCacheTtl();
    loadEditLockTtl();
    loadAnalyticsJobEnabled();
    loadNavbarLogo();
    // Preload the Delta warehouse selection + registry location so the Delta
    // panel reflects the saved SQL warehouse. Also preload graph_engine_config
    // so Neo4j / Lakebase connection forms hydrate from the registry before
    // the first Save (avoids wiping typed Neo4j URI/user on save-time load).
    loadDeltaWarehouseState()
        .then(() => { graphDbLoaded = true; })
        .catch((e) => console.log('Graph DB preload failed', e));
    loadGraphEngineConfig()
        .catch((e) => console.log('Graph engine config preload failed', e));

    // Ensure the Lakebase / Delta configuration panels are visible on load.
    applyGraphDbEnginePanels();

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

    function warehouseNameFromSelect(select, warehouseId) {
        if (!select || !warehouseId) return '';
        const opt = Array.from(select.options).find((o) => o.value === warehouseId);
        if (!opt) return '';
        return opt.textContent
            .replace(/\s*\(running\)\s*$/i, '')
            .replace(/\s*\(saved\)\s*$/i, '')
            .trim();
    }

    function resolveDeltaWarehouseDisplayName(warehouseId) {
        if (!warehouseId) return '';
        return (
            warehouseNameFromSelect(document.getElementById('deltaWarehouseSelect'), warehouseId)
            || warehouseNameFromSelect(document.getElementById('settingsWarehouseSelect'), warehouseId)
            || warehouseId
        );
    }

    function setDeltaWarehouseStatus() {
        const effectiveEl = document.getElementById('deltaEffectiveWarehouse');
        if (!effectiveEl) return;

        const savedId = (currentDeltaWarehouseId || '').trim();
        if (savedId) {
            const name = resolveDeltaWarehouseDisplayName(savedId);
            effectiveEl.textContent =
                'Current SQL Warehouse used for Lakehouse queries: ' + name;
            return;
        }

        const fallbackId = (currentWarehouseId || '').trim();
        if (fallbackId) {
            const name = resolveDeltaWarehouseDisplayName(fallbackId);
            effectiveEl.textContent =
                'Current SQL Warehouse used for Lakehouse queries: ' + name
                + ' (same as global warehouse)';
            return;
        }

        effectiveEl.textContent = 'No SQL warehouse configured.';
    }

    function setDeltaWarehouseLoading(loading) {
        const loadingEl = document.getElementById('deltaWarehouseLoading');
        const controls = document.getElementById('deltaWarehouseControls');
        const select = document.getElementById('deltaWarehouseSelect');
        const refreshBtn = document.getElementById('btnRefreshDeltaWarehouses');
        const applyBtn = document.getElementById('btnApplyDeltaWarehouse');
        if (loadingEl) loadingEl.classList.toggle('d-none', !loading);
        if (controls) controls.classList.toggle('d-none', loading);
        if (select) select.setAttribute('aria-busy', loading ? 'true' : 'false');
        if (refreshBtn) refreshBtn.disabled = loading;
        if (applyBtn) applyBtn.disabled = loading;
    }

    async function loadDeltaWarehouseSelect(preselectId, effectiveId) {
        const select = document.getElementById('deltaWarehouseSelect');
        if (!select) return;
        setDeltaWarehouseLoading(true);

        try {
            const response = await fetch('/settings/warehouses', { credentials: 'same-origin' });
            const data = await response.json();

            select.innerHTML = '<option value="">— same as global warehouse —</option>';

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
                const hasOpt = Array.from(select.options).some((o) => o.value === preselectId);
                if (!hasOpt) {
                    const wh = (data.warehouses || []).find((w) => w.id === preselectId);
                    const savedOpt = document.createElement('option');
                    savedOpt.value = preselectId;
                    savedOpt.textContent = wh ? wh.name : preselectId + ' (saved)';
                    select.appendChild(savedOpt);
                }
                select.value = preselectId;
            } else {
                select.value = '';
            }
            setDeltaWarehouseStatus();
        } catch (error) {
            console.error('Error loading Delta warehouses:', error);
            select.innerHTML = '<option value="">Error loading warehouses</option>';
        } finally {
            setDeltaWarehouseLoading(false);
        }
    }

    document.getElementById('btnRefreshDeltaWarehouses')?.addEventListener(
        'click',
        () => loadDeltaWarehouseSelect(currentDeltaWarehouseId)
    );

    async function saveDeltaWarehouseSelection(errors) {
        const select = document.getElementById('deltaWarehouseSelect');
        if (!select) return false;
        const warehouseId = select.value || '';
        try {
            const resp = await fetch('/settings/select-delta-warehouse', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ warehouse_id: warehouseId }),
            });
            const result = await resp.json();
            if (!resp.ok || !result.success) {
                const msg = result.message || result.detail || 'Failed to save Lakehouse warehouse';
                if (errors) errors.push('Lakehouse warehouse: ' + msg);
                return false;
            }
            currentDeltaWarehouseId = result.delta_warehouse_id || '';
            setDeltaWarehouseStatus();
            return true;
        } catch (e) {
            if (errors) errors.push('Lakehouse warehouse: ' + e.message);
            return false;
        }
    }

    document.getElementById('btnApplyDeltaWarehouse')?.addEventListener('click', async function () {
        const btn = this;
        const select = document.getElementById('deltaWarehouseSelect');
        if (!select) return;
        btn.disabled = true;
        const errors = [];
        const ok = await saveDeltaWarehouseSelection(errors);
        btn.disabled = false;
        if (ok) {
            showNotification(
                currentDeltaWarehouseId
                    ? 'Lakehouse SQL Warehouse saved to registry'
                    : 'Lakehouse warehouse cleared — using global warehouse',
                'success',
                2500
            );
        } else if (errors.length) {
            showNotification(errors[0], 'error');
        }
    });

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

    async function loadAnalyticsJobEnabled() {
        const input = document.getElementById('analyticsJobEnabled');
        if (!input) return;
        const note = document.getElementById('analyticsJobEnabledSource');
        // Inert until the stored value arrives, so the unchecked markup default
        // is never mistaken for a setting the user can act on.
        analyticsJobHydrated = false;
        input.disabled = true;
        try {
            const resp = await fetch('/settings/analytics-job-enabled', { credentials: 'same-origin' });
            const result = await resp.json();
            if (!result.success) return;
            input.checked = !!result.analytics_job_enabled;
            analyticsJobHydrated = true;
            input.disabled = false;
            // Say where the value came from: an unconfigured toggle tracks the
            // deployment default, and a bare checkbox would imply someone chose it.
            if (note) {
                note.innerHTML = result.source === 'admin'
                    ? '<i class="bi bi-person-check me-1"></i>Set by an admin.'
                    : '<i class="bi bi-gear me-1"></i>Not configured — following the deployment '
                      + 'default (<code>ONTOBRICKS_ANALYTICS_JOB_ENABLED</code> = '
                      + (result.env_default ? 'true' : 'false') + '). Saving here overrides it.';
            }
        } catch (error) {
            console.log('Could not read the analytics job setting', error);
        } finally {
            // A failed read leaves the box disabled and says so, rather than
            // showing a plausible-looking "off" the next Save would persist.
            if (!analyticsJobHydrated && note) {
                note.innerHTML = '<i class="bi bi-exclamation-triangle me-1"></i>'
                    + 'Could not read the current setting, so it cannot be changed '
                    + 'from this page. Reload to try again.';
            }
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
                showNotification('Default entity icon updated to ' + emoji, 'success', 2000);
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

    /** Ensure Lakebase / Delta configuration panels are visible after load. */
    function applyGraphDbEnginePanels() {
        const lkPanel = document.getElementById('lakebaseGraphPanel');
        const dtPanel = document.getElementById('deltaGraphPanel');
        if (lkPanel) lkPanel.style.display = 'block';
        if (dtPanel) dtPanel.style.display = 'block';
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

    /** Keys that belong exclusively to the Neo4j Settings panel (legacy flat). */
    const _NEO4J_FLAT_KEYS = new Set([
        'uri', 'auth_method', 'encrypted', 'username', 'password',
        'secret_scope', 'secret_key', 'neo4j_database',
    ]);

    /**
     * Normalise ``#graphEngineConfig`` JSON to ``{lakebase:{}, neo4j:{}}``.
     * Migrates legacy flat blobs so Lakebase and Neo4j never share keys.
     */
    function normalizeEngineConfigRoot(raw) {
        let o = raw;
        if (typeof o !== 'object' || o === null || Array.isArray(o)) o = {};
        if (o.lakebase || o.neo4j || o.lakehouse) {
            const lakebase = (typeof o.lakebase === 'object' && o.lakebase && !Array.isArray(o.lakebase))
                ? Object.assign({}, o.lakebase) : {};
            const neo4j = (typeof o.neo4j === 'object' && o.neo4j && !Array.isArray(o.neo4j))
                ? Object.assign({}, o.neo4j) : {};
            const lakehouse = (typeof o.lakehouse === 'object' && o.lakehouse && !Array.isArray(o.lakehouse))
                ? Object.assign({}, o.lakehouse) : {};
            if (neo4j.neo4j_database && !neo4j.database) {
                neo4j.database = neo4j.neo4j_database;
            }
            delete neo4j.neo4j_database;
            if (o.warehouse_id && !lakehouse.warehouse_id) {
                lakehouse.warehouse_id = o.warehouse_id;
            }
            return { lakebase: lakebase, neo4j: neo4j, lakehouse: lakehouse };
        }
        const lakebase = {};
        const neo4j = {};
        const lakehouse = {};
        Object.keys(o).forEach(function (k) {
            if (k === 'lakebase' || k === 'neo4j' || k === 'lakehouse' || k === 'database') return;
            if (k === 'warehouse_id') {
                if (o[k]) lakehouse.warehouse_id = o[k];
                return;
            }
            if (k === 'neo4j_database') {
                if (o[k]) neo4j.database = o[k];
                return;
            }
            if (_NEO4J_FLAT_KEYS.has(k)) {
                neo4j[k] = o[k];
                return;
            }
            lakebase[k] = o[k];
        });
        const db = String(o.database || '').trim();
        const hasNeo = !!(o.uri || o.neo4j_database);
        if (db) {
            if (hasNeo && db.toLowerCase() === 'neo4j') {
                if (!neo4j.database) neo4j.database = db;
            } else {
                lakebase.database = db;
            }
        }
        return { lakebase: lakebase, neo4j: neo4j, lakehouse: lakehouse };
    }

    function readEngineConfigRoot() {
        const ta = document.getElementById('graphEngineConfig');
        let raw = {};
        try { raw = JSON.parse(ta?.value || '{}'); } catch (_) { raw = {}; }
        return normalizeEngineConfigRoot(raw);
    }

    function writeEngineConfigRoot(root) {
        const ta = document.getElementById('graphEngineConfig');
        if (!ta) return;
        const normalized = normalizeEngineConfigRoot(root || {});
        ta.value = JSON.stringify({
            lakebase: normalized.lakebase || {},
            neo4j: normalized.neo4j || {},
            lakehouse: normalized.lakehouse || {},
        }, null, 2);
    }

    /** Merge Lakehouse warehouse picker into ``graph_engine_config.lakehouse``. */
    function mergeLakehousePanelIntoConfigTextarea() {
        if (!document.getElementById('graphEngineConfig')) return;
        const root = readEngineConfigRoot();
        const sel = document.getElementById('deltaWarehouseSelect');
        const lh = root.lakehouse || {};
        lh.warehouse_id = (sel ? sel.value : '') || currentDeltaWarehouseId || '';
        root.lakehouse = lh;
        writeEngineConfigRoot(root);
    }

    /** Merge Lakebase form fields + optional managed-sync options into the JSON textarea. */
    function mergeLakebasePanelIntoConfigTextarea() {
        const dbSel      = document.getElementById('lakebaseGraphDb');
        const projSel    = document.getElementById('lakebaseProject');
        const branchSel  = document.getElementById('lakebaseBranch');
        const syncModeEl = document.getElementById('lakebaseSyncMode');
        if (!dbSel) return;
        const root = readEngineConfigRoot();
        const o = root.lakebase || {};

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
        root.lakebase = o;
        writeEngineConfigRoot(root);
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
            cfgCat = (readEngineConfigRoot().lakebase || {}).sync_uc_catalog || '';
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
        const o = (readEngineConfigRoot().lakebase || {});
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
        const syncModeEl = document.getElementById('lakebaseSyncMode');
        if (!document.getElementById('graphEngineConfig')) return;
        const o = (readEngineConfigRoot().lakebase || {});

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

    // Cache: avoid re-fetching the scope list on every form hydration —
    // refreshed explicitly via the "refresh" buttons or a hard page reload.
    let neo4jSecretScopesLoaded = false;
    let neo4jSecretKeysScope = null;

    /** Populate a <select> with string options, preserving/selecting `selected` if present. */
    function _populateSelectOptions(selectEl, options, placeholder, selected) {
        if (!selectEl) return;
        const frag = document.createDocumentFragment();
        const ph = document.createElement('option');
        ph.value = '';
        ph.textContent = placeholder;
        frag.appendChild(ph);
        let found = false;
        options.forEach(opt => {
            const el = document.createElement('option');
            el.value = opt;
            el.textContent = opt;
            if (selected && opt === selected) found = true;
            frag.appendChild(el);
        });
        // Persisted value no longer visible (e.g. permission revoked) — keep
        // it selectable so Save doesn't silently drop a working config.
        if (selected && !found) {
            const el = document.createElement('option');
            el.value = selected;
            el.textContent = selected + ' (not visible to current identity)';
            frag.appendChild(el);
        }
        selectEl.innerHTML = '';
        selectEl.appendChild(frag);
        selectEl.value = selected || '';
    }

    /** Fetch + populate the Neo4j secret-scope dropdown, then cascade into keys. */
    async function loadNeo4jSecretScopes(forceRefresh) {
        const sel = document.getElementById('neo4jSecretScope');
        if (!sel) return;
        if (neo4jSecretScopesLoaded && !forceRefresh) return;
        const persisted = (readEngineConfigRoot().neo4j || {}).secret_scope || sel.value || '';
        try {
            const resp = await fetch('/settings/graph-engine/neo4j-secret-scopes', { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            _populateSelectOptions(sel, data.scopes || [], '— Select a scope —', persisted);
            neo4jSecretScopesLoaded = true;
        } catch (e) {
            console.log('Neo4j secret scopes load failed', e);
        }
        if (sel.value) await loadNeo4jSecretKeys(sel.value, forceRefresh);
    }

    /** Fetch + populate the Neo4j secret-key dropdown for the given scope. */
    async function loadNeo4jSecretKeys(scope, forceRefresh) {
        const sel = document.getElementById('neo4jSecretKey');
        const refreshBtn = document.getElementById('btnRefreshNeo4jSecretKeys');
        if (!sel) return;
        if (!scope) {
            sel.disabled = true;
            if (refreshBtn) refreshBtn.disabled = true;
            _populateSelectOptions(sel, [], '— Select a scope first —', '');
            neo4jSecretKeysScope = null;
            return;
        }
        if (neo4jSecretKeysScope === scope && !forceRefresh) return;
        const persisted = scope === ((readEngineConfigRoot().neo4j || {}).secret_scope || '')
            ? ((readEngineConfigRoot().neo4j || {}).secret_key || '')
            : '';
        try {
            const resp = await fetch(
                '/settings/graph-engine/neo4j-secret-keys?scope=' + encodeURIComponent(scope),
                { credentials: 'same-origin' }
            );
            const data = resp.ok ? await resp.json() : {};
            sel.disabled = false;
            if (refreshBtn) refreshBtn.disabled = false;
            _populateSelectOptions(sel, data.keys || [], '— Select a secret —', persisted);
            neo4jSecretKeysScope = scope;
        } catch (e) {
            console.log('Neo4j secret keys load failed', e);
        }
    }

    /**
     * Hydrate the Neo4j Settings form from ``#graphEngineConfig.neo4j``.
     * Called after every config load so URI / user / database survive a refresh.
     */
    function applyNeo4jFormFromConfigTextarea() {
        if (!document.getElementById('graphEngineConfig')) return;
        const o = (readEngineConfigRoot().neo4j || {});

        function _set(id, val) {
            const el = document.getElementById(id);
            if (el) el.value = val == null ? '' : String(val);
        }

        _set('neo4jUri', o.uri || '');
        _set('neo4jDatabase', o.database || o.neo4j_database || 'neo4j');
        _set('neo4jUsername', o.username || '');
        const enc = document.getElementById('neo4jEncrypted');
        if (enc) enc.checked = o.encrypted !== false;
        // Scope/key dropdowns are populated live from the Secrets API —
        // fire-and-forget, they self-select the persisted values once loaded.
        loadNeo4jSecretScopes(false);
    }

    async function loadLakebaseGraphHealth() {
        const msgEl = document.getElementById('lakebaseGraphHealthMessage');
        const dl = document.getElementById('lakebaseGraphHealthDl');
        const btn = document.getElementById('btnRefreshLakebaseGraphHealth');
        if (!msgEl || !dl) return;

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

    // Spinner scoped to the Lakebase + Delta sections (deferred heavy load).
    function setGraphDbHeavyLoading(loading) {
        const lkBanner = document.getElementById('lakebaseSectionBanner');
        const lkPanel  = document.getElementById('lakebaseGraphPanel');
        const dtBanner = document.getElementById('deltaSectionBanner');
        const dtPanel  = document.getElementById('deltaGraphPanel');
        [lkBanner, dtBanner].forEach(function (banner) {
            if (!banner) return;
            banner.classList.toggle('d-none', !loading);
            banner.classList.toggle('d-flex', loading);
        });
        if (loading) {
            if (lkPanel) lkPanel.style.display = 'none';
            if (dtPanel) dtPanel.style.display = 'none';
        } else {
            applyGraphDbEnginePanels();
        }
    }

    function applyDeltaRegistryLocation(data) {
        const regLoc = document.getElementById('deltaRegistryLocation');
        if (!regLoc || !data) return;
        regLoc.textContent = data.storage_location
            || (data.registry_catalog && data.registry_schema
                ? data.registry_catalog + '.' + data.registry_schema
                : '(not configured — set Registry catalog & schema)');
    }

    // Delta panel load: fetch the saved Delta SQL-warehouse selection + registry
    // location. Single GET, so its spinner clears fast. The graph engine config
    // and the Lakebase/Delta remote cascade are deferred (see
    // loadGraphEngineConfig / loadGraphDbHeavyFromServer).
    async function loadDeltaWarehouseState() {
        try {
            const resp = await fetch('/settings/delta-warehouse', { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            currentDeltaWarehouseId = data.delta_warehouse_id || '';
            effectiveDeltaWarehouseId = data.effective_delta_warehouse_id || '';
            applyDeltaRegistryLocation(data);
        } catch (e) {
            console.log('Delta warehouse state load failed', e);
        }
    }

    // Graph engine JSON config load. Needed by a Lakebase global Save (the
    // config textarea) and as a prerequisite for the heavy cascade. Local-only
    // form mirroring runs here so saved values are pre-selected.
    async function loadGraphEngineConfig() {
        const ta  = document.getElementById('graphEngineConfig');
        try {
            const cfgResp = ta
                ? await fetch('/settings/graph-engine-config', { credentials: 'same-origin' })
                : null;
            const cfgData = cfgResp && cfgResp.ok ? await cfgResp.json() : {};
            if (ta && cfgData.success) {
                writeEngineConfigRoot(
                    normalizeEngineConfigRoot(cfgData.graph_engine_config || {})
                );
                const lhWid = (readEngineConfigRoot().lakehouse || {}).warehouse_id || '';
                if (lhWid) currentDeltaWarehouseId = lhWid;
            }
            applyLakebaseFormFromConfigTextarea();
            applyNeo4jFormFromConfigTextarea();
            prefillLakebaseConnectionFromConfig();
            graphEngineConfigLoaded = true;
        } catch (e) {
            console.log('Graph engine config load failed', e);
        }
    }

    // Heavy load: the remote Lakebase/Delta cascade — Delta warehouse listing,
    // Lakebase project→branch→db→schema pickers, the Lakebase health probe and
    // (managed_synced only) the UC catalog listing. Deferred until the user
    // actually opens the Lakebase or Delta sidebar section.
    async function loadGraphDbHeavyFromServer() {
        try {
            if (!graphEngineConfigLoaded) await loadGraphEngineConfig();
            await loadDeltaWarehouseSelect(
                currentDeltaWarehouseId,
                effectiveDeltaWarehouseId
            );
            await loadLakebaseProjects();
            prefillLakebaseConnectionFromConfig();
            await loadLakebaseGraphHealth();
            const syncModeEl = document.getElementById('lakebaseSyncMode');
            if (syncModeEl && syncModeEl.value === 'managed_synced') {
                await loadUcCatalogsForGraphEngine();
            }
        } catch (e) {
            console.log('Graph DB heavy refresh failed', e);
        } finally {
            applyGraphDbEnginePanels();
        }
    }

    async function loadDeltaTripleStoreHealth(options) {
        const opts = options || {};
        const healthPanel = opts.healthPanel !== false;
        const out = document.getElementById('deltaHealthResult');
        const btn = document.getElementById('btnDeltaTripleStoreHealth');
        const regLoc = document.getElementById('deltaRegistryLocation');
        if (!out && !regLoc) return;
        if (btn && healthPanel) btn.disabled = true;
        if (healthPanel && out) {
            out.innerHTML = '<span class="text-muted"><span class="spinner-border spinner-border-sm me-1"></span>Checking…</span>';
        }
        try {
            const resp = await fetch('/settings/triple-store/databricks-health', { credentials: 'same-origin' });
            const data = await resp.json();

            if (regLoc && !regLoc.textContent.replace(/[—\s]/g, '')) {
                applyDeltaRegistryLocation(data);
            }

            if (!healthPanel || !out) return;

            if (!data.registry_configured) {
                out.innerHTML =
                    '<div class="alert alert-warning mb-0 py-2">' +
                    '<i class="bi bi-exclamation-triangle me-1"></i>' +
                    'Registry catalog/schema is not configured. Go to <strong>Settings → Registry</strong> first.' +
                    '</div>';
                return;
            }
            if (!data.warehouse_configured) {
                out.innerHTML =
                    '<div class="alert alert-warning mb-0 py-2">' +
                    '<i class="bi bi-exclamation-triangle me-1"></i>' +
                    'SQL Warehouse is not configured. Select one on the <strong>SQL Warehouse</strong> tab ' +
                    'or set the global warehouse under <strong>Settings → Databricks</strong>.' +
                    '</div>';
                return;
            }

            const dt = data.data_table || {};
            const viewErr = (data.view && data.view.error) ? data.view.error : '';
            const dataErr = dt.error ? dt.error : '';
            let html = '<dl class="row mb-0">';
            if (data.warehouse_id) {
                html += '<dt class="col-sm-3">Warehouse</dt><dd class="col-sm-9 font-monospace">' +
                    escapeHtmlSettings(data.warehouse_id) + '</dd>';
            }
            if (data.view_fqn) {
                html += '<dt class="col-sm-3">R2RML VIEW</dt><dd class="col-sm-9 font-monospace">' +
                    escapeHtmlSettings(data.view_fqn) + '</dd>';
            }
            if (data.data_table_fqn) {
                html += '<dt class="col-sm-3">Data TABLE</dt><dd class="col-sm-9 font-monospace">' +
                    escapeHtmlSettings(data.data_table_fqn) + '</dd>';
            }
            if (data.inferred_table_fqn) {
                html += '<dt class="col-sm-3">Inferred TABLE</dt><dd class="col-sm-9 font-monospace">' +
                    escapeHtmlSettings(data.inferred_table_fqn) + '</dd>';
            }
            if (data.data_table_fqn) {
                const exists = dt.exists ? 'yes' : 'no';
                const count = dt.count != null ? dt.count : '—';
                html += '<dt class="col-sm-3">Data table</dt><dd class="col-sm-9">exists: ' +
                    escapeHtmlSettings(exists) + ' · triples: <strong>' + escapeHtmlSettings(String(count)) +
                    '</strong></dd>';
            }
            html += '</dl>';
            if (!data.active_domain) {
                html += '<p class="text-muted mt-2 mb-0">Open a domain to see resolved FQNs and row counts.</p>';
            } else if (viewErr || dataErr) {
                html += '<p class="text-warning mt-2 mb-0">' +
                    escapeHtmlSettings(viewErr || dataErr) + '</p>';
            } else if (!data.data_table_fqn) {
                html += '<p class="text-muted mt-2 mb-0">Could not derive table names for the active domain.</p>';
            }
            out.innerHTML = html;
        } catch (e) {
            if (healthPanel && out) {
                out.innerHTML = '<span class="text-danger">' + escapeHtmlSettings(e.message || 'Error') + '</span>';
            }
        } finally {
            if (btn && healthPanel) btn.disabled = false;
        }
    }

    document.getElementById('btnDeltaTripleStoreHealth')?.addEventListener('click', function () {
        loadDeltaTripleStoreHealth();
    });

    // Lazy-load Delta health when the Health tab is first opened
    (function () {
        const tabBtn = document.getElementById('dttab-health');
        let loaded = false;
        if (tabBtn) {
            tabBtn.addEventListener('shown.bs.tab', function () {
                if (!loaded) {
                    loaded = true;
                    loadDeltaTripleStoreHealth();
                }
            });
        }
    }());

    // ── Delta objects (UC tables / views in Registry schema) ───────────────
    async function loadDeltaObjects() {
        const btn = document.getElementById('btnLoadDeltaObjects');
        const result = document.getElementById('deltaObjectsResult');
        const regLoc = document.getElementById('deltaRegistryLocation');
        if (!result) return;

        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Loading…';
        }
        result.innerHTML = '';

        try {
            const resp = await fetch('/settings/triple-store/databricks-objects', { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            if (!data.success) {
                result.innerHTML = '<div class="alert alert-warning small py-2 mt-2">'
                    + escapeHtmlSettings(data.message || data.detail || 'Failed to load objects') + '</div>';
                return;
            }

            applyDeltaRegistryLocation(data);

            if (!data.registry_configured) {
                result.innerHTML = '<div class="alert alert-warning small py-2 mt-2">'
                    + escapeHtmlSettings(data.message || 'Registry catalog/schema is not configured.') + '</div>';
                return;
            }

            const domains = data.domains || [];
            const orphans = data.orphans || [];
            const analyticsLocation = data.analytics_location || '';
            const analyticsByKey = {};
            (data.analytics || []).forEach(function (grp) {
                analyticsByKey[grp.key] = grp.items || [];
            });
            _dtDomainRegistry = {};
            _dtOrphanRegistry = {};

            const analyticsWarning = data.analytics_message
                ? '<div class="alert alert-warning small py-2 mt-2">'
                  + escapeHtmlSettings(data.analytics_message) + '</div>'
                : '';

            if (domains.length === 0 && orphans.length === 0) {
                result.innerHTML = analyticsWarning
                    + '<p class="small text-muted mt-2">No triple-store objects found in this schema.</p>';
                return;
            }

            function mkDropBtn(fullName, kind) {
                return '<button type="button" class="btn btn-outline-danger btn-sm py-0 px-2 dt-drop-obj-btn"'
                    + ' data-dt-full-name="' + escapeHtmlSettings(fullName) + '"'
                    + ' data-dt-kind="' + escapeHtmlSettings(kind) + '"'
                    + ' title="Drop ' + escapeHtmlSettings(kind) + '">'
                    + '<i class="bi bi-trash3"></i></button>';
            }

            function kindBadge(kind) {
                const map = {
                    view: 'bg-info-subtle text-info-emphasis',
                    table: 'bg-primary-subtle text-primary-emphasis',
                };
                return '<span class="badge border ' + (map[kind] || 'bg-secondary-subtle text-secondary-emphasis') + '">'
                    + kind.charAt(0).toUpperCase() + kind.slice(1) + '</span>';
            }

            function mkObjectRow(kind, name, fullName) {
                return '<tr>'
                    + '<td>' + kindBadge(kind) + '</td>'
                    + '<td class="font-monospace small">' + escapeHtmlSettings(name) + '</td>'
                    + '<td class="text-end">' + mkDropBtn(fullName, kind) + '</td>'
                    + '</tr>';
            }

            /** Scratch tables from a failed run read differently from real output. */
            function analyticsBadge(name) {
                const isWork = /_work(_|$)/.test(name);
                const cls = isWork
                    ? 'bg-warning-subtle text-warning-emphasis'
                    : 'bg-primary-subtle text-primary-emphasis';
                return '<span class="badge border ' + cls + ' lk-sync-badge">'
                    + (isWork ? 'work' : 'metrics') + '</span>';
            }

            function analyticsBlock(items, useFullName) {
                if (!items.length) return '';
                let h = '<div class="lk-sync-section border-top">';
                h += '<div class="lk-sync-header px-3 py-1 d-flex align-items-center gap-2">'
                    + '<span class="small text-muted fw-semibold" style="letter-spacing:.04em;font-size:.72rem;text-transform:uppercase">'
                    + '<i class="bi bi-graph-up me-1"></i>Analytics</span>';
                if (analyticsLocation) {
                    h += '<span class="badge bg-light border text-muted font-monospace" style="font-size:.68rem">'
                        + escapeHtmlSettings(analyticsLocation) + '</span>';
                }
                h += '</div><table class="table table-sm mb-0 lk-sync-table"><tbody>';
                items.forEach(function (o) {
                    h += '<tr>'
                        + '<td style="width:90px">' + analyticsBadge(o.name) + '</td>'
                        + '<td class="font-monospace lk-sync-uc-cell text-muted">'
                        + escapeHtmlSettings(useFullName ? o.full_name : o.name) + '</td>'
                        + '<td class="text-end" style="width:90px">'
                        + mkDropBtn(o.full_name, o.kind) + '</td>'
                        + '</tr>';
                });
                h += '</tbody></table></div>';
                return h;
            }

            let html = analyticsWarning + '<div class="lk-domain-cards">';
            domains.forEach(function (grp, idx) {
                const key = grp.base;
                const analyticsItems = analyticsByKey[grp.key] || [];
                _dtDomainRegistry[key] = {
                    base: key,
                    sortedItems: grp.items || [],
                    analyticsItems: analyticsItems,
                };
                const collapseId = 'dtDomainCollapse_' + idx;

                html += '<div class="lk-domain-card">';
                html += '<div class="lk-domain-header">';
                html += '<button class="lk-domain-toggle" type="button"'
                    + ' data-bs-toggle="collapse" data-bs-target="#' + collapseId + '"'
                    + ' aria-expanded="false" aria-controls="' + collapseId + '">';
                html += '<i class="bi bi-chevron-right lk-chevron"></i>';
                html += '<i class="bi bi-box text-muted" style="font-size:.85rem"></i>';
                html += '<span class="lk-domain-name">' + escapeHtmlSettings(key) + '</span>';
                html += '<span class="badge bg-secondary-subtle text-secondary-emphasis border lk-domain-count">'
                    + ((grp.items || []).length + analyticsItems.length) + '</span>';
                html += '</button>';
                html += '<button type="button"'
                    + ' class="btn btn-sm btn-outline-danger lk-domain-delete-btn dt-drop-domain-btn"'
                    + ' data-dt-domain="' + escapeHtmlSettings(key) + '"'
                    + ' title="Delete all objects for this domain">'
                    + '<i class="bi bi-trash3 me-1"></i>Delete</button>';
                html += '</div>';
                html += '<div id="' + collapseId + '" class="collapse lk-domain-body">';
                html += '<table class="table table-sm mb-0"><thead class="table-light"><tr>'
                    + '<th style="width:90px">Type</th><th>Name</th>'
                    + '<th class="text-end" style="width:90px">Action</th>'
                    + '</tr></thead><tbody>';
                (grp.items || []).forEach(function (o) {
                    html += mkObjectRow(o.kind, o.name, o.full_name);
                });
                html += '</tbody></table>';
                html += analyticsBlock(analyticsItems, false);
                html += '</div></div>';
            });

            orphans.forEach(function (grp, idx) {
                _dtOrphanRegistry[grp.key] = { base: grp.base, sortedItems: grp.items || [] };
                const collapseId = 'dtOrphanCollapse_' + idx;

                html += '<div class="lk-domain-card">';
                html += '<div class="lk-domain-header">';
                html += '<button class="lk-domain-toggle" type="button"'
                    + ' data-bs-toggle="collapse" data-bs-target="#' + collapseId + '"'
                    + ' aria-expanded="false" aria-controls="' + collapseId + '">';
                html += '<i class="bi bi-chevron-right lk-chevron"></i>';
                html += '<i class="bi bi-exclamation-triangle text-warning" style="font-size:.85rem"></i>';
                html += '<span class="lk-domain-name">' + escapeHtmlSettings(grp.base) + '</span>';
                html += '<span class="badge bg-warning-subtle text-warning-emphasis border lk-domain-count">'
                    + (grp.items || []).length + '</span>';
                html += '</button>';
                html += '<button type="button"'
                    + ' class="btn btn-sm btn-outline-danger lk-domain-delete-btn dt-drop-orphan-btn"'
                    + ' data-dt-orphan="' + escapeHtmlSettings(grp.key) + '"'
                    + ' title="Delete these analytics tables">'
                    + '<i class="bi bi-trash3 me-1"></i>Delete</button>';
                html += '</div>';
                html += '<div id="' + collapseId + '" class="collapse lk-domain-body">';
                html += '<p class="small text-muted px-3 pt-2 mb-0">Analytics tables with no matching '
                    + 'triple-store objects — left over from a renamed or deleted domain version.</p>';
                html += analyticsBlock(grp.items || [], true);
                html += '</div></div>';
            });
            html += '</div>';

            result.innerHTML = html;

            result.querySelectorAll('.dt-drop-domain-btn').forEach(function (el) {
                el.addEventListener('click', function () {
                    dropDeltaDomainObjects(this.dataset.dtDomain);
                });
            });
            result.querySelectorAll('.dt-drop-orphan-btn').forEach(function (el) {
                el.addEventListener('click', function () {
                    dropDeltaOrphanObjects(this.dataset.dtOrphan);
                });
            });
            result.querySelectorAll('.dt-drop-obj-btn').forEach(function (el) {
                el.addEventListener('click', function () {
                    dropDeltaObject(this.dataset.dtFullName, this.dataset.dtKind);
                });
            });
            result.querySelectorAll('.lk-domain-card').forEach(function (card) {
                const collapseEl = card.querySelector('.collapse');
                if (!collapseEl) return;
                collapseEl.addEventListener('show.bs.collapse', function () { card.classList.add('lk-open'); });
                collapseEl.addEventListener('hide.bs.collapse', function () { card.classList.remove('lk-open'); });
            });
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

    async function _execDropDelta(fullName) {
        const result = document.getElementById('deltaObjectsResult');
        _showDropSpinner(result, 'Dropping ' + fullName + '…');
        try {
            const resp = await fetch('/settings/graph-engine/drop-uc-object', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ full_name: fullName, is_sync: false }),
            });
            let data = {};
            try { data = await resp.json(); } catch (_) {}
            if (data.success) {
                showNotification('Dropped ' + fullName, 'success');
                await loadDeltaObjects();
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

    function dropDeltaObject(fullName, kind) {
        const modalEl = document.getElementById('lkDropConfirmModal');
        const bodyEl = document.getElementById('lkDropConfirmModalBody');
        const confirmBtn = document.getElementById('lkDropConfirmBtn');
        if (!modalEl || !bodyEl || !confirmBtn) {
            if (window.confirm('Drop ' + kind + ' ' + fullName + '?')) {
                _execDropDelta(fullName);
            }
            return;
        }
        bodyEl.innerHTML = 'Drop <strong>' + escapeHtmlSettings(kind) + '</strong> <code>'
            + escapeHtmlSettings(fullName) + '</code>?';
        const newBtn = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newBtn, confirmBtn);
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        newBtn.addEventListener('click', function () {
            modal.hide();
            _execDropDelta(fullName);
        });
        modal.show();
    }

    async function _execDropAllDelta(items) {
        const result = document.getElementById('deltaObjectsResult');
        const errors = [];
        const total = items.length;
        _showDropSpinner(result, 'Deleting ' + total + ' object' + (total !== 1 ? 's' : '') + '…');

        for (let i = 0; i < items.length; i++) {
            const o = items[i];
            const label = o.kind + ' ' + o.name;
            _showDropSpinner(result, 'Dropping ' + label + ' (' + (i + 1) + '/' + total + ')…');
            try {
                const resp = await fetch('/settings/graph-engine/drop-uc-object', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin',
                    body: JSON.stringify({ full_name: o.full_name, is_sync: false }),
                });
                let data = {};
                try { data = await resp.json(); } catch (_) {}
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
                    + errors.map(function (err) { return '<li>' + escapeHtmlSettings(err) + '</li>'; }).join('')
                    + '</ul></div>';
            }
            return;
        }
        showNotification('All objects dropped', 'success');
        await loadDeltaObjects();
    }

    function dropDeltaDomainObjects(domainKey) {
        const entry = _dtDomainRegistry[domainKey];
        if (!entry) {
            showNotification('Domain not found: ' + domainKey, 'danger');
            return;
        }
        // Analytics tables come last: the triple-store drop order (views before
        // tables) is the one that matters, and these have no dependants.
        const items = (entry.sortedItems || []).concat(entry.analyticsItems || []);
        _confirmDropDelta(domainKey, items);
    }

    function dropDeltaOrphanObjects(orphanKey) {
        const entry = _dtOrphanRegistry[orphanKey];
        if (!entry) {
            showNotification('Analytics group not found: ' + orphanKey, 'danger');
            return;
        }
        _confirmDropDelta(entry.base, entry.sortedItems || []);
    }

    function _confirmDropDelta(label, items) {
        const count = items.length;
        const listHtml = items.map(function (o) {
            return '<li class="font-monospace small">' + escapeHtmlSettings(o.kind) + ': '
                + escapeHtmlSettings(o.name) + '</li>';
        }).join('');
        const bodyContent = 'Drop all <strong>' + count + ' object' + (count !== 1 ? 's' : '')
            + '</strong> for <code>' + escapeHtmlSettings(label) + '</code>?'
            + '<ul class="mt-2 mb-0 ps-3">' + listHtml + '</ul>';

        const modalEl = document.getElementById('lkDropConfirmModal');
        const bodyEl = document.getElementById('lkDropConfirmModalBody');
        const confirmBtn = document.getElementById('lkDropConfirmBtn');
        if (!modalEl || !bodyEl || !confirmBtn) {
            if (window.confirm('Drop all ' + count + ' objects for ' + label + '?')) {
                _execDropAllDelta(items);
            }
            return;
        }
        bodyEl.innerHTML = bodyContent;
        const newBtn = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newBtn, confirmBtn);
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        newBtn.addEventListener('click', function () {
            modal.hide();
            _execDropAllDelta(items);
        });
        modal.show();
    }

    document.getElementById('btnLoadDeltaObjects')?.addEventListener('click', loadDeltaObjects);

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
                    html += '<i class="bi bi-box text-muted" style="font-size:.85rem"></i>';
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
                    // Placeholders filled after the main load by loadLakebaseSyncObjects()
                    // and loadLakebaseAnalyticsObjects()
                    html += '<div class="lk-sync-slot" data-lk-base="' + escapeHtmlSettings(key) + '"></div>';
                    html += '<div class="lk-analytics-slot" data-lk-base="' + escapeHtmlSettings(key) + '"></div>';
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
            loadLakebaseAnalyticsObjects();
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
        // Analytics tables drop through the same UC endpoint as the sync objects,
        // and come last because nothing depends on them.
        const ucItems = (_lkUCRegistry[domainKey] || []).concat(
            (_lkAnalyticsRegistry[domainKey] || []).map(function (o) {
                return { full_name: o.full_name, is_sync: false };
            })
        );
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

    /** Postgres card base "<Domain>_V<n>" → the "<domain>_<n>" analytics slug. */
    function lkDomainMatchKey(base) {
        const m = /^(.+)_V([^_]+)$/i.exec(base || '');
        return m ? (m[1] + '_' + m[2]).toLowerCase() : '';
    }

    function _lkAnalyticsBlock(items, location, useFullName) {
        function badge(name) {
            const isWork = /_work(_|$)/.test(name);
            const cls = isWork
                ? 'bg-warning-subtle text-warning-emphasis'
                : 'bg-primary-subtle text-primary-emphasis';
            return '<span class="badge border ' + cls + ' lk-sync-badge">'
                + (isWork ? 'work' : 'metrics') + '</span>';
        }

        let h = '<div class="lk-sync-section border-top">';
        h += '<div class="lk-sync-header px-3 py-1 d-flex align-items-center gap-2">'
            + '<span class="small text-muted fw-semibold" style="letter-spacing:.04em;font-size:.72rem;text-transform:uppercase">'
            + '<i class="bi bi-graph-up me-1"></i>Analytics (Unity Catalog)</span>';
        if (location) {
            h += '<span class="badge bg-light border text-muted font-monospace" style="font-size:.68rem">'
                + escapeHtmlSettings(location) + '</span>';
        }
        h += '</div><table class="table table-sm mb-0 lk-sync-table"><tbody>';
        items.forEach(function (o) {
            h += '<tr>'
                + '<td style="width:90px">' + badge(o.name) + '</td>'
                + '<td class="font-monospace lk-sync-uc-cell text-muted">'
                + escapeHtmlSettings(useFullName ? o.full_name : o.name) + '</td>'
                + '<td class="text-end" style="width:120px">'
                + '<button type="button" class="btn btn-outline-danger btn-sm py-0 px-1 lk-drop-uc-btn"'
                + ' data-lk-full-name="' + escapeHtmlSettings(o.full_name) + '"'
                + ' data-lk-is-sync="0"'
                + ' title="Drop ' + escapeHtmlSettings(o.full_name) + '">'
                + '<i class="bi bi-trash" style="font-size:.75rem"></i></button>'
                + '</td></tr>';
        });
        h += '</tbody></table></div>';
        return h;
    }

    function _wireUCDropButtons(root) {
        root.querySelectorAll('.lk-drop-uc-btn').forEach(function (btn) {
            btn.addEventListener('click', function () {
                dropUCObject(this.dataset.lkFullName, this.dataset.lkIsSync === '1');
            });
        });
    }

    /** Fetch UC analytics tables and inject them into each domain's analytics slot.
     *
     *  Orphans come from the server, which matches analytics against the UC
     *  triple-store objects every engine's Build writes. Matching them against
     *  the Postgres cards on this tab instead would flag every other backend's
     *  analytics as orphaned. */
    async function loadLakebaseAnalyticsObjects() {
        const slots = document.querySelectorAll('.lk-analytics-slot');
        const result = document.getElementById('lakebaseObjectsResult');
        if (!slots.length || !result) return;

        _lkAnalyticsRegistry = {};
        _lkOrphanRegistry = {};

        try {
            const resp = await fetch('/settings/triple-store/databricks-objects',
                                     { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            if (!data.success) {
                slots.forEach(function (slot) { slot.innerHTML = ''; });
                return;
            }

            const location = data.analytics_location || '';
            const byKey = {};
            (data.analytics || []).forEach(function (grp) { byKey[grp.key] = grp.items || []; });

            slots.forEach(function (slot) {
                const base = slot.dataset.lkBase || '';
                const items = byKey[lkDomainMatchKey(base)] || [];
                if (!items.length) {
                    slot.innerHTML = '';
                    return;
                }
                _lkAnalyticsRegistry[base] = items;
                slot.innerHTML = _lkAnalyticsBlock(items, location, false);
                _wireUCDropButtons(slot);
            });

            const orphans = data.orphans || [];
            const cards = result.querySelector('.lk-domain-cards');
            if (!orphans.length || !cards) return;

            let html = '';
            orphans.forEach(function (grp, idx) {
                _lkOrphanRegistry[grp.key] = { base: grp.base, items: grp.items || [] };
                const collapseId = 'lkOrphanCollapse_' + idx;
                html += '<div class="lk-domain-card lk-orphan-card">';
                html += '<div class="lk-domain-header">';
                html += '<button class="lk-domain-toggle" type="button"'
                    + ' data-bs-toggle="collapse" data-bs-target="#' + collapseId + '"'
                    + ' aria-expanded="false" aria-controls="' + collapseId + '">';
                html += '<i class="bi bi-chevron-right lk-chevron"></i>';
                html += '<i class="bi bi-exclamation-triangle text-warning" style="font-size:.85rem"></i>';
                html += '<span class="lk-domain-name">' + escapeHtmlSettings(grp.base) + '</span>';
                html += '<span class="badge bg-warning-subtle text-warning-emphasis border lk-domain-count">'
                    + (grp.items || []).length + '</span>';
                html += '</button>';
                html += '<button type="button"'
                    + ' class="btn btn-sm btn-outline-danger lk-domain-delete-btn lk-drop-orphan-btn"'
                    + ' data-lk-orphan="' + escapeHtmlSettings(grp.key) + '"'
                    + ' title="Delete these analytics tables">'
                    + '<i class="bi bi-trash3 me-1"></i>Delete</button>';
                html += '</div>';
                html += '<div id="' + collapseId + '" class="collapse lk-domain-body">';
                html += '<p class="small text-muted px-3 pt-2 mb-0">Analytics tables with no matching '
                    + 'triple-store objects — left over from a renamed or deleted domain version.</p>';
                html += _lkAnalyticsBlock(grp.items || [], location, true);
                html += '</div></div>';
            });
            cards.insertAdjacentHTML('beforeend', html);

            // Wire only the orphan cards — the domain cards already own their
            // handlers, and re-wiring their drop buttons would double-fire them.
            cards.querySelectorAll('.lk-orphan-card').forEach(function (card) {
                card.querySelectorAll('.lk-drop-orphan-btn').forEach(function (btn) {
                    btn.addEventListener('click', function () {
                        dropLakebaseOrphanObjects(this.dataset.lkOrphan);
                    });
                });
                _wireUCDropButtons(card);
                const collapseEl = card.querySelector('.collapse');
                if (!collapseEl) return;
                collapseEl.addEventListener('show.bs.collapse', () => card.classList.add('lk-open'));
                collapseEl.addEventListener('hide.bs.collapse', () => card.classList.remove('lk-open'));
            });
        } catch (e) {
            slots.forEach(function (slot) { slot.innerHTML = ''; });
        }
    }

    /** Confirm, then drop every analytics table of an orphaned group. */
    function dropLakebaseOrphanObjects(orphanKey) {
        const entry = _lkOrphanRegistry[orphanKey];
        if (!entry) {
            showNotification('Analytics group not found: ' + orphanKey, 'danger');
            return;
        }
        const ucItems = (entry.items || []).map(function (o) {
            return { full_name: o.full_name, is_sync: false };
        });
        const count = ucItems.length;
        const listHtml = ucItems.map(function (u) {
            return '<li class="font-monospace small">' + escapeHtmlSettings(u.full_name) + '</li>';
        }).join('');
        const bodyContent = 'Drop all <strong>' + count + ' analytics table'
            + (count !== 1 ? 's' : '') + '</strong> for <code>'
            + escapeHtmlSettings(entry.base) + '</code>?'
            + '<ul class="mt-2 mb-0 ps-3">' + listHtml + '</ul>';

        const modalEl = document.getElementById('lkDropConfirmModal');
        const bodyEl = document.getElementById('lkDropConfirmModalBody');
        const confirmBtn = document.getElementById('lkDropConfirmBtn');
        if (!modalEl || !bodyEl || !confirmBtn) {
            if (window.confirm('Drop all ' + count + ' analytics tables for ' + entry.base + '?')) {
                _execDropAll([], '', '', '', ucItems);
            }
            return;
        }
        bodyEl.innerHTML = bodyContent;
        const newBtn = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newBtn, confirmBtn);
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        newBtn.addEventListener('click', function () {
            modal.hide();
            _execDropAll([], '', '', '', ucItems);
        });
        modal.show();
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

    /** Persist the graph engine *connection* JSON config + Delta warehouse
     *  selection (used by global Save). The backend *selection* is per-domain
     *  now (Domain Information -> Knowledge Graph tab), so nothing is chosen
     *  here — only the Lakebase / Neo4j / Delta connection settings. */
    async function saveGraphDbSettings(errors) {
        const ta = document.getElementById('graphEngineConfig');
        const errDiv = document.getElementById('graphEngineConfigError');
        if (errDiv) errDiv.style.display = 'none';

        try {
            // Persist the Delta SQL-warehouse selection via its dedicated endpoint.
            await saveDeltaWarehouseSelection(errors);

            if (!ta) {
                applyGraphDbEnginePanels();
                return;
            }

            // Fold connection panels into the textarea before POST. Always merge
            // Neo4j (form is hydrated by loadGraphEngineConfig). Lakebase panel
            // merge stays gated on the heavy load so empty Lakebase pickers
            // cannot blank project/branch/schema on a Neo4j-only Save.
            mergeNeo4jPanelIntoConfigTextarea();
            mergeLakehousePanelIntoConfigTextarea();
            if (graphDbHeavyLoaded) {
                mergeLakebasePanelIntoConfigTextarea();
            }

            let parsed;
            try {
                parsed = normalizeEngineConfigRoot(JSON.parse(ta.value || '{}'));
            } catch (parseErr) {
                errors.push('Graph DB config: invalid JSON (' + parseErr.message + ')');
                if (errDiv) {
                    errDiv.textContent = 'Invalid JSON: ' + parseErr.message;
                    errDiv.style.display = 'block';
                }
                return;
            }
            writeEngineConfigRoot(parsed);

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
            writeEngineConfigRoot(cfgJson.graph_engine_config || parsed);
            applyLakebaseFormFromConfigTextarea();
            applyNeo4jFormFromConfigTextarea();
            applyGraphDbEnginePanels();
            if (graphDbHeavyLoaded) loadLakebaseGraphHealth();
        } catch (e) {
            errors.push('Graph DB: ' + e.message);
        }
    }

    // =====================================================================
    //  TRIPLE STORE SECTIONS – lazy-load on first visit to lakebase or delta
    // =====================================================================

    document.addEventListener('sidebarSectionChanged', async (e) => {
        const s = e.detail?.section;
        if (s !== 'lakebase' && s !== 'delta' && s !== 'neo4j') return;

        // Neo4j only needs the registry graph_engine_config (URI / user / …).
        if (s === 'neo4j') {
            if (!graphEngineConfigLoaded) {
                const banner = document.getElementById('neo4jSectionBanner');
                if (banner) {
                    banner.classList.remove('d-none');
                    banner.classList.add('d-flex');
                }
                try {
                    await loadGraphEngineConfig();
                } finally {
                    if (banner) {
                        banner.classList.add('d-none');
                        banner.classList.remove('d-flex');
                    }
                }
            } else {
                applyNeo4jFormFromConfigTextarea();
            }
            return;
        }

        // Ensure the Delta warehouse state is present (covers a deep-link
        // race where the section activates before the page-load preload resolves).
        if (!graphDbLoaded) {
            try {
                await loadDeltaWarehouseState();
                graphDbLoaded = true;
            } catch (e) {
                console.log('Delta warehouse state load failed', e);
            }
        }

        // The heavy Lakebase/Delta data is only needed by those two sections;
        // load it lazily the first time either is opened.
        if (s === 'lakebase' || s === 'delta') {
            if (!graphDbHeavyLoaded) {
                graphDbHeavyLoaded = true;
                setGraphDbHeavyLoading(true);
                try {
                    await loadGraphDbHeavyFromServer();
                } finally {
                    setGraphDbHeavyLoading(false);
                }
            } else if (s === 'lakebase') {
                // Revisit → refresh the Lakebase health probe only.
                await loadLakebaseGraphHealth();
            }
        }
    });

    // =====================================================================
    //  GLOBAL SAVE BUTTON – warehouse, global prefs, CloudFetch, Graph DB
    // =====================================================================

    // ── Neo4j engine config — form ↔ textarea ───────────────────────────────
    //
    // Mirrors the Lakebase merge/toggle pattern. When the active engine is
    // "neo4j" (Settings > Back end > Neo4j), this function
    // reads the Neo4j config form fields from #neo4j-section and serialises
    // them into graph_engine_config.neo4j (fully separate from lakebase).
    function mergeNeo4jPanelIntoConfigTextarea() {
        if (!document.getElementById('graphEngineConfig')) return;
        const root = readEngineConfigRoot();
        const o = root.neo4j || {};

        const uri      = (document.getElementById('neo4jUri')?.value || '').trim();
        const database = (document.getElementById('neo4jDatabase')?.value || '').trim();
        const user     = (document.getElementById('neo4jUsername')?.value || '').trim();
        const scope    = (document.getElementById('neo4jSecretScope')?.value || '').trim();
        const key      = (document.getElementById('neo4jSecretKey')?.value || '').trim();
        const encrypted = !!document.getElementById('neo4jEncrypted')?.checked;

        if (uri) o.uri = uri; else delete o.uri;
        o.database = database || 'neo4j';
        delete o.neo4j_database;
        // The password is always resolved live from a Databricks secret —
        // no other auth method is offered from the UI, and clear-text
        // passwords are never serialised.
        o.auth_method = 'databricks_secret';
        o.encrypted = encrypted;
        if (user) o.username = user; else delete o.username;
        if (scope) o.secret_scope = scope; else delete o.secret_scope;
        if (key) o.secret_key = key; else delete o.secret_key;
        delete o.password;

        root.neo4j = o;
        writeEngineConfigRoot(root);
    }

    // Wire up Neo4j form field listeners — keep the textarea in sync as the
    // user edits the panel, so the save flow always serialises fresh values.
    [
        'neo4jUri', 'neo4jDatabase',
        'neo4jUsername', 'neo4jSecretScope', 'neo4jSecretKey',
        'neo4jEncrypted',
    ].forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        el.addEventListener('input',  mergeNeo4jPanelIntoConfigTextarea);
        el.addEventListener('change', mergeNeo4jPanelIntoConfigTextarea);
    });
    // Scope change cascades into a fresh key list for that scope (the
    // previously selected key almost certainly doesn't exist in the new one).
    document.getElementById('neo4jSecretScope')?.addEventListener('change', (e) => {
        loadNeo4jSecretKeys(e.target.value, false).then(mergeNeo4jPanelIntoConfigTextarea);
    });
    document.getElementById('btnRefreshNeo4jSecretScopes')?.addEventListener('click', () => {
        loadNeo4jSecretScopes(true);
    });
    document.getElementById('btnRefreshNeo4jSecretKeys')?.addEventListener('click', () => {
        const scope = document.getElementById('neo4jSecretScope')?.value || '';
        if (scope) loadNeo4jSecretKeys(scope, true);
    });

    // Test-connection button — POSTs to /settings/graph-engine/neo4j-test which
    // runs a Bolt protocol handshake (driver.verify_connectivity()) using the
    // persisted engine_config + NEO4J_PASSWORD env var. No Cypher is executed.
    document.getElementById('btnTestNeo4jConnection')?.addEventListener('click', async function () {
        const btn = this;
        const result = document.getElementById('neo4jTestResult');
        if (!result) return;
        const origHtml = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Testing…';
        result.className = 'alert alert-info mt-3 small';
        result.classList.remove('d-none');
        result.textContent = 'Sending Bolt handshake…';
        try {
            // Save the current panel state first so the test uses the values
            // currently in the form, not just what was persisted.
            mergeNeo4jPanelIntoConfigTextarea();
            const ta = document.getElementById('graphEngineConfig');
            let parsed = {};
            try { parsed = normalizeEngineConfigRoot(JSON.parse(ta?.value || '{}')); } catch (_) { parsed = { lakebase: {}, neo4j: {} }; }
            writeEngineConfigRoot(parsed);
            await fetch('/settings/graph-engine-config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ graph_engine_config: parsed }),
            });
            const resp = await fetch('/settings/graph-engine/neo4j-test', {
                method: 'POST',
                credentials: 'same-origin',
            });
            const j = await resp.json();
            if (j.ok) {
                result.className = 'alert alert-success mt-3 small';
                const probe = j.cypher_probe
                    ? ' · <code>RETURN 1 AS probe</code> echoed ' +
                      j.cypher_probe.rows + ' row(s) — Cypher path live.'
                    : '';
                result.innerHTML =
                    '<i class="bi bi-check-circle me-1"></i>' +
                    '<strong>Connected</strong> to <code>' + j.uri + '</code> ' +
                    '(database <code>' + j.database + '</code>) in ' + j.latency_ms + ' ms · ' +
                    'credentials from <em>' + j.credentials_source + '</em>.' + probe;
            } else {
                result.className = 'alert alert-danger mt-3 small';
                const cat = j.category ? ' <span class="badge bg-danger-subtle text-danger-emphasis border ms-1">' + j.category + '</span>' : '';
                result.innerHTML =
                    '<i class="bi bi-x-circle me-1"></i>' +
                    '<strong>Test failed</strong>' + cat + ': ' +
                    (j.error || j.message || 'Unknown error');
            }
        } catch (e) {
            result.className = 'alert alert-danger mt-3 small';
            result.textContent = 'Test failed: ' + (e.message || e);
        } finally {
            btn.disabled = false;
            btn.innerHTML = origHtml;
        }
    });

    // =====================================================================
    //  NEO4J ADMIN — Objects (list + drop graphs) & Health (Bolt probe), P5
    //  Mirrors the Lakebase Objects / Health tabs. Reuses the shared
    //  #lkDropConfirmModal drop-confirmation modal.
    // =====================================================================

    // Graphs listed by the last loadNeo4jLabels() call, keyed by marker label,
    // so the per-row Drop handler looks up the item without embedding it in the
    // DOM (matches the _lkDomainRegistry pattern for Lakebase objects).
    let _neo4jLabelRegistry = {};

    async function loadNeo4jLabels() {
        const btn    = document.getElementById('btnLoadNeo4jLabels');
        const result = document.getElementById('neo4jLabelsResult');
        if (!result) return;

        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Loading…';
        }
        result.innerHTML = '';
        _neo4jLabelRegistry = {};

        try {
            const resp = await fetch('/settings/graph-engine/neo4j-labels', { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            if (!data.success) {
                result.innerHTML = '<div class="alert alert-warning small py-2 mt-2">'
                    + escapeHtmlSettings(data.detail || data.message || 'Failed to load graphs') + '</div>';
                return;
            }

            const graphs = data.graphs || [];
            const database = data.database || '';
            if (graphs.length === 0) {
                result.innerHTML = '<p class="small text-muted mt-2">No OntoBricks graphs found in database <code>'
                    + escapeHtmlSettings(database) + '</code>.</p>';
                return;
            }

            let html = '<p class="small text-muted mt-2 mb-3">Database: <code>'
                + escapeHtmlSettings(database) + '</code> · '
                + graphs.length + ' graph' + (graphs.length === 1 ? '' : 's') + '.</p>';
            html += '<table class="table table-sm table-hover ob-table mb-0">'
                + '<thead class="table-light"><tr>'
                + '<th>Graph</th>'
                + '<th class="text-end">Nodes</th>'
                + '<th class="text-end">Relationships</th>'
                + '<th class="text-end">Action</th>'
                + '</tr></thead><tbody>';
            graphs.forEach(g => {
                _neo4jLabelRegistry[g.label] = g;
                html += '<tr>'
                    + '<td class="font-monospace small"><i class="bi bi-bezier2 me-1 text-primary"></i>'
                    + escapeHtmlSettings(g.label) + '</td>'
                    + '<td class="text-end">' + Number(g.nodes || 0).toLocaleString() + '</td>'
                    + '<td class="text-end">' + Number(g.edges || 0).toLocaleString() + '</td>'
                    + '<td class="text-end">'
                    + '<button type="button" class="btn btn-outline-danger btn-sm py-0 px-2 n4-drop-graph-btn"'
                    + ' data-n4-label="' + escapeHtmlSettings(g.label) + '" title="Drop graph">'
                    + '<i class="bi bi-trash3"></i></button>'
                    + '</td></tr>';
            });
            html += '</tbody></table>';
            result.innerHTML = html;

            result.querySelectorAll('.n4-drop-graph-btn').forEach(b => {
                b.addEventListener('click', () => dropNeo4jLabel(b.getAttribute('data-n4-label')));
            });
        } catch (e) {
            result.innerHTML = '<div class="alert alert-danger small py-2 mt-2">'
                + escapeHtmlSettings(e.message || 'Network error') + '</div>';
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '<i class="bi bi-arrow-clockwise me-1"></i> Load graphs';
            }
        }
    }

    function dropNeo4jLabel(label) {
        const g = _neo4jLabelRegistry[label] || { label: label, nodes: 0, edges: 0 };
        const modalEl    = document.getElementById('lkDropConfirmModal');
        const bodyEl     = document.getElementById('lkDropConfirmModalBody');
        const confirmBtn = document.getElementById('lkDropConfirmBtn');
        const detail = '<br><small class="text-muted">Deletes all '
            + Number(g.nodes || 0).toLocaleString() + ' node(s), '
            + Number(g.edges || 0).toLocaleString() + ' relationship(s), the uniqueness '
            + 'constraint and the schema map.</small>';
        if (!modalEl || !bodyEl || !confirmBtn) {
            if (window.confirm('Drop graph "' + label + '"? This deletes all its nodes and relationships.')) {
                _execDropNeo4jLabel(label);
            }
            return;
        }
        bodyEl.innerHTML = 'Drop graph <code>' + escapeHtmlSettings(label) + '</code>?' + detail;
        // Replace the button to clear any previously-stacked listener.
        const newBtn = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newBtn, confirmBtn);
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        newBtn.addEventListener('click', function () {
            modal.hide();
            _execDropNeo4jLabel(label);
        });
        modal.show();
    }

    async function _execDropNeo4jLabel(label) {
        const result = document.getElementById('neo4jLabelsResult');
        if (result) {
            result.insertAdjacentHTML('afterbegin',
                '<div class="alert alert-info small py-2 mb-2" id="n4DropSpinner">'
                + '<span class="spinner-border spinner-border-sm me-1"></span> Dropping <code>'
                + escapeHtmlSettings(label) + '</code>…</div>');
        }
        try {
            const resp = await fetch('/settings/graph-engine/neo4j-drop-label', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ label: label }),
            });
            let data = {};
            try { data = await resp.json(); } catch (_) {}
            if (data.success) {
                if (typeof showNotification === 'function') {
                    showNotification('Dropped graph ' + (data.dropped || label), 'success');
                }
                await loadNeo4jLabels();
            } else {
                const msg = data.detail || data.message || ('HTTP ' + resp.status);
                if (result) {
                    const sp = document.getElementById('n4DropSpinner');
                    if (sp) sp.remove();
                    result.insertAdjacentHTML('afterbegin',
                        '<div class="alert alert-danger small py-2 mb-2">' + escapeHtmlSettings(msg) + '</div>');
                }
            }
        } catch (e) {
            const sp = document.getElementById('n4DropSpinner');
            if (sp) sp.remove();
            if (result) {
                result.insertAdjacentHTML('afterbegin',
                    '<div class="alert alert-danger small py-2 mb-2">' + escapeHtmlSettings(e.message || 'Network error') + '</div>');
            }
        }
    }

    async function loadNeo4jHealth() {
        const msgEl = document.getElementById('neo4jHealthMessage');
        const dl    = document.getElementById('neo4jHealthDl');
        const btn   = document.getElementById('btnRefreshNeo4jHealth');
        if (!msgEl || !dl) return;

        if (btn) btn.disabled = true;
        dl.innerHTML = '';
        msgEl.style.display = '';
        msgEl.className = 'small mb-2 text-muted';
        msgEl.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Checking Neo4j…';

        function row(label, value) {
            return '<dt class="col-sm-4 text-muted">' + escapeHtmlSettings(label) + '</dt>'
                + '<dd class="col-sm-8 font-monospace text-break">' + value + '</dd>';
        }

        try {
            const resp = await fetch('/settings/graph-engine/neo4j-health', { credentials: 'same-origin' });
            const data = resp.ok ? await resp.json() : {};
            if (!data.success) {
                msgEl.className = 'small mb-2 text-warning';
                msgEl.innerHTML = '<i class="bi bi-exclamation-triangle me-1"></i>'
                    + escapeHtmlSettings(data.detail || data.message || 'Health check failed');
                return;
            }
            msgEl.className = 'small mb-2 ' + (data.ok ? 'text-success' : 'text-danger');
            msgEl.innerHTML = '<i class="bi bi-' + (data.ok ? 'check-circle' : 'x-circle') + ' me-1"></i>'
                + (data.ok ? 'Bolt handshake OK' : escapeHtmlSettings(data.error || 'Unreachable'));
            dl.innerHTML = (
                row('Bolt URI', escapeHtmlSettings(String(data.uri || '')))
                + row('Database', escapeHtmlSettings(String(data.database || '')))
                + row('Latency', escapeHtmlSettings(String(data.latency_ms != null ? data.latency_ms + ' ms' : '')))
                + (data.ok ? '' : row('Error', escapeHtmlSettings(String(data.error || ''))))
            );
        } catch (e) {
            msgEl.className = 'small mb-2 text-danger';
            msgEl.innerHTML = '<i class="bi bi-x-circle me-1"></i>' + escapeHtmlSettings(e.message || 'Network error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    document.getElementById('btnLoadNeo4jLabels')?.addEventListener('click', loadNeo4jLabels);
    document.getElementById('btnRefreshNeo4jHealth')?.addEventListener('click', loadNeo4jHealth);

    // Lazy-load each admin tab the first time it is shown (parity with Lakebase).
    document.getElementById('n4tab-objects')?.addEventListener('shown.bs.tab', function () {
        if (!_neo4jLabelRegistry || Object.keys(_neo4jLabelRegistry).length === 0) {
            loadNeo4jLabels();
        }
    });
    document.getElementById('n4tab-health')?.addEventListener('shown.bs.tab', function () {
        loadNeo4jHealth();
    });

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

        // 3c. Save the Databricks graph-analytics job toggle. An explicit "off"
        // is sent as readily as an "on", because unchecking has to persist a
        // value that overrides the env-var default — but only once the checkbox
        // is known to hold the stored state. Posting an unhydrated box would
        // write its unchecked default over an admin's "on", and this handler is
        // shared by every section's Save button, so that would happen on a save
        // having nothing to do with analytics.
        const analyticsJobInput = document.getElementById('analyticsJobEnabled');
        if (analyticsJobInput && !analyticsJobHydrated) {
            errors.push(
                'Analytics job: current value could not be read, so it was left '
                + 'unchanged. Reload the page and try again.'
            );
        }
        if (analyticsJobInput && analyticsJobHydrated) {
            try {
                const resp = await fetch('/settings/save-analytics-job-enabled', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin',
                    body: JSON.stringify({ analytics_job_enabled: analyticsJobInput.checked })
                });
                const r = await resp.json();
                if (!r.success) errors.push('Analytics job: ' + r.message);
                else loadAnalyticsJobEnabled();
            } catch (e) { errors.push('Analytics job: ' + e.message); }
        }

        // 4. Graph DB connection config + Delta warehouse (same tab; top Save only)
        if (!graphDbLoaded) {
            try {
                await loadDeltaWarehouseState();
                graphDbLoaded = true;
            } catch (e) {
                console.log('Graph DB refresh before save failed', e);
            }
        }
        // Ensure the graph engine JSON config is loaded first so the merge/save
        // cannot blank out the saved connection config.
        if (!graphEngineConfigLoaded) {
            try {
                await loadGraphEngineConfig();
            } catch (e) {
                console.log('Graph engine config load before save failed', e);
            }
        }
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
