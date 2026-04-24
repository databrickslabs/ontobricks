/**
 * OntoBricks - registry.js
 * Registry page JavaScript – domain browsing and registry configuration
 */

document.addEventListener('DOMContentLoaded', function () {

    let registryConfigured = false;
    let registryCfg = { catalog: '', schema: '', volume: 'OntoBricksRegistry', configured: false };
    let registryLocked = false;

    loadRegistryConfig();

    // =====================================================================
    //  REGISTRY CONFIG
    // =====================================================================

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
            if (label) {
                label.innerHTML = '<i class="bi bi-x-circle text-danger"></i> <span class="text-danger">Error loading config</span>';
            }
        }
    }

    function updateRegistryLabel() {
        const label = document.getElementById('registryLocationLabel');
        const initBtn = document.getElementById('btnInitRegistry');

        if (registryCfg.catalog && registryCfg.schema) {
            const path = registryCfg.catalog + '.' + registryCfg.schema + '.' + (registryCfg.volume || 'OntoBricksRegistry');
            label.innerHTML = '<i class="bi bi-archive text-success me-1"></i> <strong>' + escapeHtml(path) + '</strong>';
            if (initBtn) initBtn.style.display = registryCfg.configured ? 'none' : '';
        } else {
            label.innerHTML = '<i class="bi bi-exclamation-triangle text-warning me-1"></i> <span class="text-muted">Not configured</span>';
            if (initBtn) initBtn.style.display = 'none';
        }
    }

    function updateRegistryStatus(cfg) {
        const div = document.getElementById('registryStatus');
        const configDiv = document.getElementById('registryConfigStatus');
        registryConfigured = !!cfg.configured;

        if (cfg.configured) {
            if (div) div.style.display = 'none';
            if (configDiv) configDiv.style.display = 'none';
            loadRegistryDomains();
        } else if (cfg.catalog && cfg.schema) {
            const msg = registryLocked
                ? 'Registry volume is set via Databricks App resource but not yet initialized. Click <strong>Initialize</strong> to set up the registry.'
                : 'Registry location set but not initialized yet. Click <strong>Initialize</strong> to create the volume.';
            const alertHtml = '<div class="alert alert-warning small mb-0">' +
                '<i class="bi bi-exclamation-triangle me-1"></i> ' + msg + '</div>';
            if (div) { div.style.display = 'block'; div.innerHTML = alertHtml; }
            if (configDiv) { configDiv.style.display = 'block'; configDiv.innerHTML = alertHtml; }
            const section = document.getElementById('registryDomainsSection');
            if (section) section.style.display = 'none';
        } else {
            const alertHtml = '<div class="alert alert-warning small mb-0">' +
                '<i class="bi bi-exclamation-triangle me-1"></i> Registry not configured. Go to <strong>Configuration</strong> to select a catalog, schema and volume.</div>';
            if (div) { div.style.display = 'block'; div.innerHTML = alertHtml; }
            if (configDiv) { configDiv.style.display = 'block'; configDiv.innerHTML = alertHtml; }
            const section = document.getElementById('registryDomainsSection');
            if (section) section.style.display = 'none';
        }
    }

    // --- Change modal ---

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

    // --- Helpers ---

    function _shortDate(iso) {
        if (!iso) return '';
        try {
            const d = new Date(iso);
            if (isNaN(d.getTime())) return '';
            const now = new Date();
            const pad = n => String(n).padStart(2, '0');
            const date = d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate());
            const time = pad(d.getHours()) + ':' + pad(d.getMinutes());
            if (d.toDateString() === now.toDateString()) return 'Today ' + time;
            const yesterday = new Date(now);
            yesterday.setDate(yesterday.getDate() - 1);
            if (d.toDateString() === yesterday.toDateString()) return 'Yesterday ' + time;
            return date + ' ' + time;
        } catch (_) { return ''; }
    }

    function _formatVersionDates(lastUpdate, lastBuild) {
        if (!lastUpdate && !lastBuild) return '';
        let html = '<div class="registry-version-dates text-muted">';
        if (lastUpdate) {
            html += '<div><i class="bi bi-pencil-square me-1"></i><span class="registry-date-label">Updated:</span> ' + escapeHtml(_shortDate(lastUpdate)) + '</div>';
        }
        if (lastBuild) {
            html += '<div><i class="bi bi-hammer me-1"></i><span class="registry-date-label">Built:</span> ' + escapeHtml(_shortDate(lastBuild)) + '</div>';
        }
        html += '</div>';
        return html;
    }

    // --- Registry domain list ---

    async function loadRegistryDomains() {
        const section = document.getElementById('registryDomainsSection');
        const listDiv = document.getElementById('registryDomainsList');
        if (!section || !listDiv) return;

        section.style.display = 'flex';
        listDiv.innerHTML = '<div class="text-center text-muted small py-3">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading domains...</div>';

        try {
            const [data, vsData] = await Promise.all([
                fetch('/settings/registry/domains', { credentials: 'same-origin' }).then(r => r.json()),
                fetchOnce('/domain/version-status')
            ]);
            const currentFolder = (vsData.success && (vsData.domain_folder || vsData.project_folder))
                ? (vsData.domain_folder || vsData.project_folder) : null;
            const currentVersion = (vsData.success && vsData.version) ? vsData.version : null;

            if (!data.success) {
                listDiv.innerHTML = '<div class="text-muted small py-3"><i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                    (data.message || 'Could not load domains') + '</div>';
                return;
            }

            const rows = data.domains || data.projects || [];
            if (!rows.length) {
                listDiv.innerHTML = '<div class="text-muted small py-3 text-center">' +
                    '<i class="bi bi-folder"></i> No domains in registry yet</div>';
                return;
            }

            let html = '<div class="table-responsive registry-domain-table-wrapper">' +
                '<table class="table table-sm table-hover align-middle mb-0 registry-domain-table">' +
                '<thead><tr>' +
                    '<th class="ps-3" style="width:20%;">Name</th>' +
                    '<th style="width:30%;">URI</th>' +
                    '<th>Description</th>' +
                    '<th class="text-center" style="width:5rem;">Versions</th>' +
                    '<th class="text-end pe-3" style="width:3rem;"></th>' +
                '</tr></thead><tbody>';

            rows.forEach((d, idx) => {
                const desc = d.description
                    ? escapeHtml(d.description)
                    : '<span class="fst-italic text-muted">—</span>';
                const uri = d.base_uri
                    ? '<span class="font-monospace small">' + escapeHtml(d.base_uri) + '</span>'
                    : '<span class="fst-italic text-muted">—</span>';
                const versions = d.versions || [];
                const vCount = versions.length;
                const hasVersions = vCount > 0;
                const activeVer = versions.find(v => typeof v === 'object' && v.active);
                const rowId = 'reg-versions-' + idx;
                const isCurrent = currentFolder && d.name === currentFolder;
                const nameLabel = escapeHtml(d.name) +
                    (isCurrent ? ' <span class="badge bg-primary-subtle text-primary border ms-1" style="font-size:0.65rem;">current</span>' : '');
                const deleteBtn = isCurrent
                    ? '<button type="button" class="btn btn-sm border-0 text-muted admin-only-nav" style="display:none" disabled title="Cannot delete the currently loaded domain">' +
                          '<i class="bi bi-trash"></i></button>'
                    : '<button type="button" class="btn btn-sm btn-outline-danger border-0 registry-delete-btn admin-only-nav" style="display:none" ' +
                          'data-domain="' + escapeHtml(d.name) + '" title="Delete domain and all versions">' +
                          '<i class="bi bi-trash"></i></button>';
                const versionsBadge = activeVer
                    ? '<span class="badge bg-secondary">' + vCount + '</span> ' +
                      '<span class="badge bg-success-subtle text-success border-success" style="font-size:0.65rem;" title="Active: v' + escapeHtml(activeVer.version) + '">' +
                          '<i class="bi bi-broadcast"></i> v' + escapeHtml(activeVer.version) +
                      '</span>'
                    : '<span class="badge bg-secondary">' + vCount + '</span>';
                html += '<tr class="registry-domain-row" data-target="' + rowId + '" style="cursor:pointer;">' +
                    '<td class="ps-3 fw-semibold text-nowrap">' +
                        '<i class="bi bi-chevron-right me-1 text-muted registry-chevron" style="font-size:0.7rem;transition:transform 0.15s;"></i>' +
                        '<i class="bi bi-folder2 me-1 text-primary"></i>' +
                        nameLabel +
                    '</td>' +
                    '<td class="text-muted text-truncate">' + uri + '</td>' +
                    '<td class="text-muted text-truncate">' + desc + '</td>' +
                    '<td class="text-center">' + versionsBadge + '</td>' +
                    '<td class="text-end pe-3">' + deleteBtn + '</td>' +
                '</tr>';
                if (hasVersions) {
                    html += '<tr id="' + rowId + '" class="registry-version-panel" style="display:none;">' +
                        '<td colspan="5" class="px-0 py-0">' +
                        '<div class="registry-version-list">';
                    d.versions.forEach(v => {
                        const ver = typeof v === 'object' ? v.version : v;
                        const isActive = typeof v === 'object' && v.active;
                        const lastUpdate = (typeof v === 'object' && v.last_update) ? v.last_update : '';
                        const lastBuild = (typeof v === 'object' && v.last_build) ? v.last_build : '';
                        const isLoaded = currentFolder === d.name && currentVersion === ver;
                        const activeLabel = isActive
                            ? '<span class="badge bg-success-subtle text-success border-success" style="font-size:.65rem;"><i class="bi bi-broadcast me-1"></i>Active</span>'
                            : '';
                        const loadedLabel = isLoaded
                            ? '<span class="badge bg-primary-subtle text-primary border" style="font-size:.65rem;"><i class="bi bi-check-circle me-1"></i>Loaded</span>'
                            : '';
                        const datesHtml = _formatVersionDates(lastUpdate, lastBuild);
                        const activeBtn = isActive
                            ? '<button type="button" class="btn btn-sm btn-success registry-active-version-btn" disabled title="This version is Active">' +
                                  '<i class="bi bi-broadcast me-1"></i>Active</button>'
                            : '<button type="button" class="btn btn-sm btn-outline-success registry-active-version-btn" ' +
                                  'data-domain="' + escapeHtml(d.name) + '" data-version="' + escapeHtml(ver) + '" title="Set as Active version">' +
                                  '<i class="bi bi-broadcast me-1"></i>Set Active</button>';
                        const loadBtn = isLoaded
                            ? ''
                            : '<button type="button" class="btn btn-sm btn-outline-primary registry-load-version-btn" ' +
                                  'data-domain="' + escapeHtml(d.name) + '" data-version="' + escapeHtml(ver) + '" title="Load this version">' +
                                  '<i class="bi bi-box-arrow-in-down me-1"></i>Load</button>';
                        const deleteBtn = isLoaded
                            ? ''
                            : '<button type="button" class="btn btn-sm btn-outline-danger border-0 registry-delete-version-btn admin-only-nav" style="display:none" ' +
                                  'data-domain="' + escapeHtml(d.name) + '" data-version="' + escapeHtml(ver) + '" ' +
                                  'title="Delete version v' + escapeHtml(ver) + '">' +
                                  '<i class="bi bi-trash"></i></button>';
                        html += '<div class="registry-version-row d-flex align-items-center gap-2 px-4 py-2' + (isLoaded ? ' registry-version-loaded' : '') + '">' +
                            '<span class="badge ' + (isLoaded ? 'bg-primary' : 'bg-secondary') + ' registry-version-num">v' + escapeHtml(ver) + '</span>' +
                            '<div class="d-flex align-items-center gap-2">' + activeLabel + loadedLabel + '</div>' +
                            datesHtml +
                            '<span class="flex-grow-1"></span>' +
                            '<div class="d-flex align-items-center gap-1">' + activeBtn + loadBtn + deleteBtn + '</div>' +
                        '</div>';
                    });
                    html += '</div></td></tr>';
                }
            });

            html += '</tbody></table></div>';
            listDiv.innerHTML = html;

            listDiv.querySelectorAll('.registry-domain-row').forEach(row => {
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
                    deleteRegistryDomain(btn.dataset.domain);
                });
            });

            listDiv.querySelectorAll('.registry-delete-version-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    deleteRegistryVersion(btn.dataset.domain, btn.dataset.version);
                });
            });

            // Reveal the .admin-only-nav delete controls only for admins
            // (no-op for non-admins; buttons stay hidden via inline style).
            if (typeof showAdminNavItems === 'function') {
                showAdminNavItems();
            }

            listDiv.querySelectorAll('.registry-load-version-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    loadRegistryDomainVersion(btn.dataset.domain, btn.dataset.version);
                });
            });

            listDiv.querySelectorAll('.registry-active-version-btn:not([disabled])').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    setRegistryVersionActive(btn.dataset.domain, btn.dataset.version);
                });
            });

        } catch (e) {
            console.error('Error loading registry domains:', e);
            listDiv.innerHTML = '<div class="text-danger small py-3">' +
                '<i class="bi bi-x-circle me-1"></i> Error loading domains</div>';
        }
    }

    async function deleteRegistryDomain(domainName) {
        const confirmed = await showConfirmDialog({
            title: 'Delete Domain',
            message: 'Delete domain "' + domainName + '" and all its versions from the registry? This cannot be undone.',
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash'
        });
        if (!confirmed) return;

        try {
            const resp = await fetch('/settings/registry/domains/' + encodeURIComponent(domainName), {
                method: 'DELETE',
                credentials: 'same-origin'
            });
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message, 'success');
                loadRegistryDomains();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (e) {
            showNotification('Error deleting domain: ' + e.message, 'error');
        }
    }

    async function deleteRegistryVersion(domainName, version) {
        const confirmed = await showConfirmDialog({
            title: 'Delete Version',
            message: 'Delete version v' + version + ' from domain "' + domainName + '"? This cannot be undone.',
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash'
        });
        if (!confirmed) return;

        try {
            const resp = await fetch(
                '/settings/registry/domains/' + encodeURIComponent(domainName) + '/versions/' + encodeURIComponent(version),
                { method: 'DELETE', credentials: 'same-origin' }
            );
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message, 'success');
                loadRegistryDomains();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (e) {
            showNotification('Error deleting version: ' + e.message, 'error');
        }
    }

    async function loadRegistryDomainVersion(domainName, version) {
        const confirmed = await showConfirmDialog({
            title: 'Load Domain',
            message: 'Load <strong>' + escapeHtml(domainName) + '</strong> version <strong>v' + escapeHtml(version) + '</strong>? Any unsaved changes to the current domain will be lost.',
            confirmText: 'Load',
            confirmClass: 'btn-primary',
            icon: 'box-arrow-in-down'
        });
        if (!confirmed) return;

        try {
            showNotification('Loading ' + domainName + ' v' + version + '…', 'info', 5000);
            const resp = await fetch('/domain/load-from-uc', {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ domain: domainName, version: version })
            });
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message || 'Domain loaded!', 'success');
                if (typeof fetchCachedInvalidate === 'function') fetchCachedInvalidate('/navbar/state');
                setTimeout(() => window.location.reload(), 800);
            } else {
                showNotification('Error: ' + (data.message || 'Failed to load domain'), 'error');
            }
        } catch (e) {
            showNotification('Error loading domain: ' + e.message, 'error');
        }
    }

    async function setRegistryVersionActive(domainName, version) {
        const confirmed = await showConfirmDialog({
            title: 'Set Active Version',
            message: 'Set <strong>v' + escapeHtml(version) + '</strong> of <strong>' + escapeHtml(domainName) + '</strong> as the Active version? Any previously active version will be deactivated.',
            confirmText: 'Set Active',
            confirmClass: 'btn-success',
            icon: 'broadcast'
        });
        if (!confirmed) return;

        try {
            const resp = await fetch(
                '/settings/registry/domains/' + encodeURIComponent(domainName) + '/versions/' + encodeURIComponent(version) + '/active',
                {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ enabled: true })
                }
            );
            const data = await resp.json();
            if (data.success) {
                showNotification('v' + version + ' is now Active for ' + domainName, 'success');
                loadRegistryDomains();
            } else {
                showNotification('Error: ' + (data.message || 'Failed to set active'), 'error');
            }
        } catch (e) {
            showNotification('Error: ' + e.message, 'error');
        }
    }

    document.getElementById('btnRefreshDomains')?.addEventListener('click', () => loadRegistryDomains());

    // =====================================================================
    //  BRIDGES
    // =====================================================================

    const D3_CDN = 'https://d3js.org/d3.v7.min.js';
    const BRIDGES_VIEW_KEY = 'ontobricks-bridges-view';
    const NODE_PALETTE = [
        '#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f',
        '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac'
    ];

    let bridgesLoaded = false;
    let bridgesData = null;

    function _ensureD3() {
        if (typeof d3 !== 'undefined') return Promise.resolve();
        return new Promise((resolve, reject) => {
            const s = document.createElement('script');
            s.src = D3_CDN;
            s.onload = resolve;
            s.onerror = () => reject(new Error('Failed to load D3.js'));
            document.head.appendChild(s);
        });
    }

    // --- View toggle ---

    function _getBridgesView() {
        try { return sessionStorage.getItem(BRIDGES_VIEW_KEY) || 'graph'; } catch (_) { return 'graph'; }
    }

    function _setBridgesView(v) {
        try { sessionStorage.setItem(BRIDGES_VIEW_KEY, v); } catch (_) { /* ignore */ }
    }

    function _applyBridgesView(view) {
        const graphC = document.getElementById('bridgesGraphContainer');
        const tableC = document.getElementById('bridgesContent');
        const toggle = document.getElementById('bridgesViewToggle');
        if (!graphC || !tableC) return;

        if (view === 'graph') {
            graphC.style.display = '';
            tableC.style.display = 'none';
        } else {
            graphC.style.display = 'none';
            tableC.style.display = '';
        }
        if (toggle) {
            toggle.querySelectorAll('[data-view]').forEach(btn => {
                btn.classList.toggle('active', btn.dataset.view === view);
            });
        }
        _setBridgesView(view);
    }

    document.getElementById('bridgesViewToggle')?.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-view]');
        if (!btn) return;
        _applyBridgesView(btn.dataset.view);
    });

    // --- Bridges load triggers ---

    document.addEventListener('sidebarSectionChanged', (e) => {
        if (e.detail?.section === 'bridges' && !bridgesLoaded) {
            loadRegistryBridges();
        }
    });

    const urlSection = new URLSearchParams(window.location.search).get('section');
    if (urlSection === 'bridges') {
        loadRegistryBridges();
    }

    document.getElementById('btnRefreshBridges')?.addEventListener('click', () => {
        bridgesLoaded = false;
        bridgesData = null;
        loadRegistryBridges();
    });

    // --- Build entity-level graph model grouped by domain ---

    function _buildGraphModel(domains) {
        const domainMap = {};
        const entityMap = {};
        const links = [];

        domains.forEach(d => {
            if (!domainMap[d.name]) {
                domainMap[d.name] = { id: d.name, baseUri: d.base_uri || '', entities: new Set() };
            }
            (d.bridges || []).forEach(b => {
                const tgt = b.target_domain;
                if (!tgt) return;
                if (!domainMap[tgt]) {
                    domainMap[tgt] = { id: tgt, baseUri: '', entities: new Set() };
                }

                const srcKey = d.name + '::' + b.source_class;
                const tgtKey = tgt + '::' + b.target_class_name;

                domainMap[d.name].entities.add(b.source_class);
                domainMap[tgt].entities.add(b.target_class_name);

                if (!entityMap[srcKey]) {
                    entityMap[srcKey] = {
                        id: srcKey, name: b.source_class, domain: d.name,
                        emoji: b.source_emoji || '📦'
                    };
                }
                if (!entityMap[tgtKey]) {
                    entityMap[tgtKey] = {
                        id: tgtKey, name: b.target_class_name, domain: tgt,
                        emoji: '📦'
                    };
                }

                links.push({
                    sourceId: srcKey, targetId: tgtKey,
                    sourceDomain: d.name, targetDomain: tgt,
                    label: b.label || ''
                });
            });
        });

        const domainGroups = Object.values(domainMap)
            .filter(d => d.entities.size > 0)
            .map(d => ({ ...d, entities: Array.from(d.entities) }));

        return { domainGroups, entities: entityMap, links };
    }

    // --- Render static diagram with entities inside domain bubbles ---

    function _renderBridgesGraph(graphData) {
        const container = document.getElementById('bridgesGraph');
        if (!container) return;
        container.innerHTML = '';

        const rect = container.getBoundingClientRect();
        const width = rect.width || container.clientWidth || 900;
        const height = rect.height || container.clientHeight || 600;
        const cx = width / 2;
        const cy = height / 2;

        const svg = d3.select(container)
            .append('svg')
            .attr('width', '100%')
            .attr('height', '100%')
            .attr('viewBox', '0 0 ' + width + ' ' + height)
            .attr('preserveAspectRatio', 'xMidYMid meet');

        const defs = svg.append('defs');

        defs.append('marker')
            .attr('id', 'bridges-arrowhead')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 10).attr('refY', 0)
            .attr('markerWidth', 7).attr('markerHeight', 7)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', 'var(--bs-primary, #0d6efd)')
            .attr('fill-opacity', 0.55);

        NODE_PALETTE.forEach((color, i) => {
            const grad = defs.append('radialGradient')
                .attr('id', 'domain-grad-' + i)
                .attr('cx', '35%').attr('cy', '35%').attr('r', '65%');
            grad.append('stop').attr('offset', '0%').attr('stop-color', color).attr('stop-opacity', 0.12);
            grad.append('stop').attr('offset', '100%').attr('stop-color', color).attr('stop-opacity', 0.04);
        });

        const g = svg.append('g');

        const zoom = d3.zoom()
            .scaleExtent([0.3, 4])
            .on('zoom', (event) => g.attr('transform', event.transform));
        svg.call(zoom);

        const groups = graphData.domainGroups;
        const numGroups = groups.length;
        if (numGroups === 0) return;

        const maxEntities = Math.max(...groups.map(g => g.entities.length));
        const domainRadius = Math.max(60, 28 + maxEntities * 16);
        const orbitRadius = numGroups === 1 ? 0
            : Math.max(domainRadius * 2.2, Math.min(width, height) * 0.32);

        const entityRadius = 18;
        const entityPositions = {};

        groups.forEach((domain, i) => {
            const angle = numGroups === 1 ? 0 : (2 * Math.PI * i / numGroups) - Math.PI / 2;
            const dx = cx + Math.cos(angle) * orbitRadius;
            const dy = cy + Math.sin(angle) * orbitRadius;
            domain._x = dx;
            domain._y = dy;
            domain._r = domainRadius;

            const colorIdx = Math.abs(_hashStr(domain.id)) % NODE_PALETTE.length;
            domain._color = NODE_PALETTE[colorIdx];
            domain._colorIdx = colorIdx;

            const entCount = domain.entities.length;
            const innerRadius = domainRadius * 0.55;
            domain.entities.forEach((entName, j) => {
                const eAngle = entCount === 1 ? 0 : (2 * Math.PI * j / entCount) - Math.PI / 2;
                const ex = dx + Math.cos(eAngle) * innerRadius;
                const ey = dy + Math.sin(eAngle) * innerRadius;
                const key = domain.id + '::' + entName;
                entityPositions[key] = { x: ex, y: ey, domain: domain.id };
            });
        });

        // Domain group circles
        const domainG = g.append('g').attr('class', 'bridges-domains-layer');
        groups.forEach(domain => {
            const dg = domainG.append('g')
                .attr('class', 'bridges-domain-group')
                .attr('transform', 'translate(' + domain._x + ',' + domain._y + ')');

            dg.append('circle')
                .attr('r', domain._r)
                .attr('fill', 'url(#domain-grad-' + domain._colorIdx + ')')
                .attr('stroke', domain._color)
                .attr('stroke-width', 2)
                .attr('stroke-opacity', 0.35)
                .attr('stroke-dasharray', '6 3');

            dg.append('text')
                .attr('class', 'bridges-domain-label')
                .attr('y', -domain._r - 10)
                .text(domain.id);

            dg.append('text')
                .attr('class', 'bridges-domain-badge')
                .attr('y', -domain._r - 10)
                .attr('dy', '1.1em')
                .text(domain.entities.length + ' entit' + (domain.entities.length !== 1 ? 'ies' : 'y'));
        });

        // Edge layer (behind entity nodes)
        const edgeG = g.append('g').attr('class', 'bridges-edges-layer');

        const edgeLabelIndexMap = {};
        graphData.links.forEach(link => {
            const pairKey = [link.sourceId, link.targetId].sort().join('|||');
            if (!edgeLabelIndexMap[pairKey]) edgeLabelIndexMap[pairKey] = 0;
            link._pairIdx = edgeLabelIndexMap[pairKey]++;
        });

        graphData.links.forEach(link => {
            const src = entityPositions[link.sourceId];
            const tgt = entityPositions[link.targetId];
            if (!src || !tgt) return;

            const dx = tgt.x - src.x;
            const dy = tgt.y - src.y;
            const dist = Math.sqrt(dx * dx + dy * dy) || 1;
            const ux = dx / dist, uy = dy / dist;

            const startX = src.x + ux * (entityRadius + 2);
            const startY = src.y + uy * (entityRadius + 2);
            const endX = tgt.x - ux * (entityRadius + 10);
            const endY = tgt.y - uy * (entityRadius + 10);

            const curvature = 30 + (link._pairIdx || 0) * 20;
            const midX = (startX + endX) / 2 - uy * curvature;
            const midY = (startY + endY) / 2 + ux * curvature;
            const pathD = 'M' + startX + ',' + startY + ' Q' + midX + ',' + midY + ' ' + endX + ',' + endY;

            edgeG.append('path')
                .attr('class', 'bridges-graph-edge')
                .attr('d', pathD)
                .attr('stroke-width', 1.8)
                .attr('marker-end', 'url(#bridges-arrowhead)');

            const labelX = (startX + 2 * midX + endX) / 4;
            const labelY = (startY + 2 * midY + endY) / 4;
            const labelText = link.label || '';

            if (labelText) {
                const bg = edgeG.append('rect')
                    .attr('class', 'bridges-edge-label-bg')
                    .attr('rx', 3).attr('ry', 3);

                const lbl = edgeG.append('text')
                    .attr('class', 'bridges-edge-label')
                    .attr('x', labelX).attr('y', labelY)
                    .text(labelText);

                const bbox = lbl.node().getBBox();
                bg.attr('x', bbox.x - 4).attr('y', bbox.y - 1)
                  .attr('width', bbox.width + 8).attr('height', bbox.height + 2);
            }
        });

        // Entity nodes layer
        const entityG = g.append('g').attr('class', 'bridges-entities-layer');
        Object.keys(entityPositions).forEach(key => {
            const pos = entityPositions[key];
            const ent = graphData.entities[key];
            if (!ent) return;

            const domain = groups.find(g => g.id === ent.domain);
            const fillColor = domain ? domain._color : '#999';

            const eg = entityG.append('g')
                .attr('class', 'bridges-entity-node')
                .attr('transform', 'translate(' + pos.x + ',' + pos.y + ')');

            eg.append('circle')
                .attr('r', entityRadius)
                .attr('fill', '#fff')
                .attr('stroke', fillColor)
                .attr('stroke-width', 2.5);

            eg.append('text')
                .attr('class', 'bridges-entity-emoji')
                .attr('dy', '0.35em')
                .text(ent.emoji);

            eg.append('text')
                .attr('class', 'bridges-entity-label')
                .attr('y', entityRadius + 13)
                .text(ent.name);
        });

        // Fit the diagram into the viewport
        requestAnimationFrame(() => {
            const bounds = g.node().getBBox();
            if (bounds.width > 0 && bounds.height > 0) {
                const pad = 50;
                const scale = Math.min(
                    (width - pad * 2) / bounds.width,
                    (height - pad * 2) / bounds.height,
                    1.3
                );
                const tx = width / 2 - (bounds.x + bounds.width / 2) * scale;
                const ty = height / 2 - (bounds.y + bounds.height / 2) * scale;
                svg.call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
            }
        });

        // Hover: highlight connected entities and edges
        entityG.selectAll('.bridges-entity-node')
            .on('mouseenter', function (event) {
                const thisKey = _entityKeyFromPos(this, entityPositions);
                if (!thisKey) return;
                const connected = new Set([thisKey]);
                graphData.links.forEach(l => {
                    if (l.sourceId === thisKey) connected.add(l.targetId);
                    if (l.targetId === thisKey) connected.add(l.sourceId);
                });
                entityG.selectAll('.bridges-entity-node')
                    .classed('dimmed', function () {
                        return !connected.has(_entityKeyFromPos(this, entityPositions));
                    });
                edgeG.selectAll('.bridges-graph-edge')
                    .classed('highlighted', function (d, i) {
                        const link = graphData.links[i];
                        return link && (link.sourceId === thisKey || link.targetId === thisKey);
                    });
            })
            .on('mouseleave', function () {
                entityG.selectAll('.bridges-entity-node').classed('dimmed', false);
                edgeG.selectAll('.bridges-graph-edge').classed('highlighted', false);
            });
    }

    function _hashStr(s) {
        let h = 0;
        for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
        return h;
    }

    function _entityKeyFromPos(el, positions) {
        const t = d3.select(el).attr('transform');
        const m = t && t.match(/translate\(([\d.e+-]+),([\d.e+-]+)\)/);
        if (!m) return null;
        const px = parseFloat(m[1]), py = parseFloat(m[2]);
        for (const [key, pos] of Object.entries(positions)) {
            if (Math.abs(pos.x - px) < 0.5 && Math.abs(pos.y - py) < 0.5) return key;
        }
        return null;
    }

    // --- Main load function ---

    async function loadRegistryBridges() {
        const content = document.getElementById('bridgesContent');
        const status = document.getElementById('bridgesStatus');
        const graphContainer = document.getElementById('bridgesGraphContainer');
        const toggle = document.getElementById('bridgesViewToggle');
        if (!content) return;

        content.innerHTML = '<div class="text-center text-muted small py-3">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading bridges...</div>';
        if (status) status.style.display = 'none';
        if (graphContainer) graphContainer.style.display = 'none';
        if (toggle) toggle.style.display = 'none';

        try {
            const resp = await fetch('/settings/registry/bridges', { credentials: 'same-origin' });
            const data = await resp.json();

            if (!data.success) {
                content.innerHTML = '<div class="text-muted small py-3"><i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                    escapeHtml(data.message || 'Could not load bridges') + '</div>';
                return;
            }

            const domains = data.domains || [];
            bridgesData = domains;
            const domainsWithBridges = domains.filter(d => (d.bridges || []).length > 0);
            const totalBridges = domains.reduce((sum, d) => sum + (d.bridges || []).length, 0);

            if (totalBridges === 0) {
                content.innerHTML = '<div class="text-muted small py-3 text-center">' +
                    '<i class="bi bi-signpost-split me-1"></i> No bridges defined in any domain</div>';
                bridgesLoaded = true;
                return;
            }

            // Show view toggle
            if (toggle) toggle.style.display = '';

            // Populate shared summary bar (visible in both views)
            const summaryEl = document.getElementById('bridgesSummary');
            if (summaryEl) {
                summaryEl.innerHTML = '<div class="bridges-summary-bar">' +
                    '<div class="summary-item"><i class="bi bi-signpost-split text-primary"></i> ' +
                        '<span class="summary-value">' + totalBridges + '</span> bridge' + (totalBridges !== 1 ? 's' : '') + '</div>' +
                    '<div class="summary-item"><i class="bi bi-folder2 text-secondary"></i> ' +
                        '<span class="summary-value">' + domainsWithBridges.length + '</span> domain' + (domainsWithBridges.length !== 1 ? 's' : '') +
                        ' with bridges</div>' +
                    '<div class="summary-item"><i class="bi bi-globe text-secondary"></i> ' +
                        '<span class="summary-value">' + domains.length + '</span> total domain' + (domains.length !== 1 ? 's' : '') + '</div>' +
                '</div>';
                summaryEl.style.display = '';
            }

            // Build table HTML
            let html = '';

            domains.forEach((d, idx) => {
                const bridges = d.bridges || [];
                const hasBridges = bridges.length > 0;
                const cardId = 'bridges-card-' + idx;

                html += '<div class="bridges-domain-card">' +
                    '<div class="bridges-domain-header" data-bs-toggle="collapse" data-bs-target="#' + cardId + '">' +
                        '<div class="d-flex align-items-center gap-2">' +
                            '<i class="bi bi-chevron-right text-muted bridges-chevron" style="font-size:0.7rem;transition:transform 0.15s;"></i>' +
                            '<i class="bi bi-folder2 text-primary"></i>' +
                            '<span class="domain-name">' + escapeHtml(d.name) + '</span>';

                if (d.base_uri) {
                    html += '<span class="font-monospace text-muted small ms-2">' + escapeHtml(d.base_uri) + '</span>';
                }

                html += '</div>' +
                    '<span class="badge ' + (hasBridges ? 'bg-primary' : 'bg-secondary') + ' bridge-count">' +
                        bridges.length + ' bridge' + (bridges.length !== 1 ? 's' : '') +
                    '</span>' +
                '</div>';

                html += '<div id="' + cardId + '" class="collapse bridges-domain-body">';

                if (!hasBridges) {
                    html += '<div class="bridge-no-bridges">No bridges defined</div>';
                } else {
                    html += '<table class="table table-sm table-hover bridges-table">' +
                        '<thead><tr>' +
                            '<th>Source Class</th>' +
                            '<th style="width:3rem;"></th>' +
                            '<th>Target Domain</th>' +
                            '<th>Target Class</th>' +
                            '<th>Label</th>' +
                        '</tr></thead><tbody>';

                    bridges.forEach(b => {
                        const srcEmoji = b.source_emoji || '📦';
                        const label = b.label
                            ? escapeHtml(b.label)
                            : '<span class="text-muted fst-italic">—</span>';
                        html += '<tr>' +
                            '<td><span class="me-1">' + srcEmoji + '</span> ' + escapeHtml(b.source_class) + '</td>' +
                            '<td class="text-center bridge-arrow"><i class="bi bi-arrow-right"></i></td>' +
                            '<td><i class="bi bi-folder2 text-secondary me-1"></i>' + escapeHtml(b.target_domain) + '</td>' +
                            '<td>' + escapeHtml(b.target_class_name) + '</td>' +
                            '<td>' + label + '</td>' +
                        '</tr>';
                    });

                    html += '</tbody></table>';
                }

                html += '</div></div>';
            });

            content.innerHTML = html;
            bridgesLoaded = true;

            content.querySelectorAll('.bridges-domain-header').forEach(header => {
                const target = document.querySelector(header.dataset.bsTarget);
                if (!target) return;
                const chevron = header.querySelector('.bridges-chevron');
                target.addEventListener('show.bs.collapse', () => {
                    if (chevron) chevron.style.transform = 'rotate(90deg)';
                });
                target.addEventListener('hide.bs.collapse', () => {
                    if (chevron) chevron.style.transform = '';
                });
            });

            // Render graph -- show container first so it gets laid out, then render
            const graphModel = _buildGraphModel(domains);
            try {
                await _ensureD3();
                if (graphContainer) graphContainer.style.display = '';
                _applyBridgesView(_getBridgesView());
                // Defer render to next frame so the flex layout has computed dimensions
                requestAnimationFrame(() => {
                    requestAnimationFrame(() => {
                        _renderBridgesGraph(graphModel);
                    });
                });
            } catch (e) {
                console.warn('D3 graph not available, falling back to table view:', e);
                _applyBridgesView('table');
                if (toggle) toggle.style.display = 'none';
            }

        } catch (e) {
            console.error('Error loading bridges:', e);
            content.innerHTML = '<div class="text-danger small py-3">' +
                '<i class="bi bi-x-circle me-1"></i> Error loading bridges</div>';
        }
    }

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
});
