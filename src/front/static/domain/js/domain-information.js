/**
 * OntoBricks - domain-information.js
 * Extracted from domain templates per code_instructions.txt
 */

let currentDomainFolder = null;

// The hidden select remains the single persistence-compatible value source.
// Native radio cards mirror it and dispatch its existing change pipeline.
function syncGraphBackendCards() {
    const backendSelect = document.getElementById('domainGraphBackend');
    if (!backendSelect) return;
    document.querySelectorAll('.domain-backend-option').forEach(option => {
        option.checked = option.value === backendSelect.value;
    });
}

function syncGraphBackendCardEditability() {
    const backendSelect = document.getElementById('domainGraphBackend');
    if (!backendSelect) return;
    const readOnly = [
        'read-only-version',
        'role-viewer',
        'read-only-locked'
    ].some(className => document.body.classList.contains(className));
    document.querySelectorAll('.domain-backend-option').forEach(option => {
        option.disabled = readOnly || backendSelect.disabled;
    });
}

function initGraphBackendCards() {
    const backendSelect = document.getElementById('domainGraphBackend');
    if (!backendSelect) return;
    document.querySelectorAll('.domain-backend-option').forEach(option => {
        option.addEventListener('change', () => {
            if (!option.checked) return;
            backendSelect.value = option.value;
            backendSelect.dispatchEvent(new Event('change', { bubbles: true }));
        });
    });
    syncGraphBackendCards();
    syncGraphBackendCardEditability();
    new MutationObserver(syncGraphBackendCardEditability).observe(document.body, {
        attributes: true,
        attributeFilter: ['class']
    });
}

// Show the Neo4j connection selector only when the backend is Neo4j.
function toggleNeo4jDatabaseSection() {
    const backend = (document.getElementById('domainGraphBackend') || {}).value;
    const section = document.getElementById('neo4jDatabaseSection');
    if (!section) return;
    section.classList.toggle('d-none', backend !== 'neo4j');
}

/** Reveal the selector and fill it from Settings when the backend is Neo4j. */
function syncNeo4jConnectionSection() {
    toggleNeo4jDatabaseSection();
    const backend = (document.getElementById('domainGraphBackend') || {}).value;
    if (backend === 'neo4j') loadNeo4jDatabases();
}

// Show the materialization picker only for Lakehouse: it is the one backend
// whose ..._data relation can be a view rather than a copy.
function toggleLakehouseMaterializationSection() {
    const backend = (document.getElementById('domainGraphBackend') || {}).value;
    const section = document.getElementById('lakehouseMaterializationSection');
    if (!section) return;
    section.classList.toggle('d-none', backend !== 'databricks');
}

function toggleSearchCacheSection() {
    const backend = (document.getElementById('domainGraphBackend') || {}).value;
    const section = document.getElementById('searchCacheSection');
    if (!section) return;
    const show = backend === 'databricks' || backend === 'lakebase';
    section.classList.toggle('d-none', !show);
}

function refreshSearchCacheStatusLine(state) {
    const line = document.getElementById('searchCacheStatusLine');
    const enabledEl = document.getElementById('domainGraphCacheEnabled');
    if (!line || !enabledEl) return;
    const enabled = enabledEl.checked;
    const resolved = state || enabledEl.dataset.cacheState || 'pending_refresh';
    if (!enabled) {
        line.textContent = 'Off — live reads';
        return;
    }
    if (resolved === 'ready') {
        line.textContent = 'On — using cached indexes';
        return;
    }
    line.textContent = 'On — rebuild or Refresh cache required (live reads until then)';
}

// A "No Backend" domain is ontology-only: every graph MCP tool is unchecked
// and locked, and the explanatory notice is revealed. Switching back to a real
// backend re-enables (and re-checks) the graph tools to their default state.
function applyGraphlessConstraints() {
    const backend = (document.getElementById('domainGraphBackend') || {}).value;
    const graphless = backend === 'none';
    syncGraphBackendCards();

    const notice = document.getElementById('noBackendNotice');
    if (notice) notice.classList.toggle('d-none', !graphless);

    [
        'graphBackendMigrationNotice',
        'dualKnowledgeGraphSection',
        'tripleStoreGatewaySection',
        'graphAccessInfoSection'
    ].forEach(sectionId => {
        const section = document.getElementById(sectionId);
        if (section) section.classList.toggle('d-none', graphless);
    });

    document.querySelectorAll('.js-mcp-tool[data-requires-graph="true"]').forEach(el => {
        el.disabled = graphless;
        el.checked = !graphless;
    });
    if (typeof syncMcpSelectAll === 'function') syncMcpSelectAll();

    // Show only the access/permissions detail for the selected backend.
    document.querySelectorAll('.graph-access-detail').forEach(el => {
        el.classList.toggle('d-none', el.dataset.backend !== backend);
    });
}

// Populate the Neo4j connection dropdown from Settings named connections.
// In-flight guard: the section toggle fires on init and on every backend
// change, so without it a single reveal would issue several identical fetches.
let _neo4jConnectionsLoading = null;

async function loadNeo4jDatabases(forceRefresh) {
    if (_neo4jConnectionsLoading && !forceRefresh) return _neo4jConnectionsLoading;
    _neo4jConnectionsLoading = _loadNeo4jConnectionOptions();
    try {
        return await _neo4jConnectionsLoading;
    } finally {
        _neo4jConnectionsLoading = null;
    }
}

async function _loadNeo4jConnectionOptions() {
    const select = document.getElementById('domainNeo4jDatabase');
    const help = document.getElementById('neo4jDatabaseHelp');
    if (!select) return;
    if (help) help.innerHTML = '<span class="text-muted">Loading connections…</span>';
    try {
        const resp = await fetch('/settings/graph-engine/neo4j-connections', { credentials: 'same-origin' });
        const data = await resp.json();
        if (!data || !data.success) throw new Error((data && data.error) || 'request failed');
        const connections = data.connections || [];
        const names = connections.map(c => (c && c.name) ? String(c.name) : '').filter(Boolean);
        // Read the target selection only now: /domain/info may have supplied
        // the persisted name while this request was in flight. A pick the user
        // just made outranks the last persisted value.
        const saved = select.value || select.dataset.savedValue || '';
        select.innerHTML = '';
        select.add(new Option('— Select a Neo4j connection —', '', true, !saved));
        select.options[0].disabled = true;
        names.forEach(n => select.add(new Option(n, n)));
        if (saved && names.indexOf(saved) === -1) {
            select.add(new Option(saved + ' (missing in Settings)', saved));
        }
        if (saved) select.value = saved;
        if (help) {
            help.innerHTML = names.length
                ? '<span class="text-success"><i class="bi bi-check-circle me-1"></i>' + names.length + ' connection(s) in Settings → Neo4j</span>'
                : '<span class="text-warning"><i class="bi bi-exclamation-triangle me-1"></i>No Neo4j connections yet — add one under Settings → Neo4j.</span>';
        }
    } catch (e) {
        if (help) help.innerHTML = '<span class="text-danger"><i class="bi bi-exclamation-triangle me-1"></i>' + (e.message || 'Could not list connections') + '</span>';
    }
}

function setSelectedLlmEndpoint(endpointName, endpointKind = '') {
    const nameInput = document.getElementById('domainLlmEndpoint');
    const kindInput = document.getElementById('domainLlmEndpointKind');
    const display = document.getElementById('domainLlmEndpointDisplay');
    if (!nameInput || !kindInput || !display) return;
    nameInput.value = endpointName || '';
    kindInput.value = endpointKind || '';
    display.value = endpointName || '';
}

async function browseLlmEndpoints() {
    const nameInput = document.getElementById('domainLlmEndpoint');
    const kindInput = document.getElementById('domainLlmEndpointKind');
    if (!nameInput || !kindInput || typeof openLlmEndpointPicker !== 'function') return;
    const selected = await openLlmEndpointPicker({
        name: nameInput.value,
        kind: kindInput.value
    });
    if (!selected) return;
    setSelectedLlmEndpoint(selected.name, selected.kind);
    nameInput.dataset.userEdited = '1';
    nameInput.dispatchEvent(new Event('change', { bubbles: true }));
    kindInput.dispatchEvent(new Event('change', { bubbles: true }));
}

// Create a new version
async function createNewVersion() {
    try {
        const confirmed = await showConfirmDialog({
            title: 'Create New Version',
            message: 'This will copy the current version and increment the version number. Continue?',
            confirmText: 'Create Version',
            confirmClass: 'btn-primary',
            icon: 'plus-circle'
        });
        if (!confirmed) return;
        
        showNotification('Creating new version...', 'info', 2000);
        
        const response = await fetch('/domain/create-version', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(`Version ${data.new_version} created successfully!`, 'success');
            // Add new version to dropdown and select it
            const versionSelect = document.getElementById('domainVersionSelect');
            const newOption = document.createElement('option');
            newOption.value = data.new_version;
            newOption.textContent = `v${data.new_version}`;
            // Insert at the beginning (latest first)
            versionSelect.insertBefore(newOption, versionSelect.firstChild);
            versionSelect.value = data.new_version;
            if (typeof invalidateDomainCaches === 'function') invalidateDomainCaches();
            // Reload page to refresh status
            window.location.reload();
        } else {
            showNotification('Error: ' + data.message, 'error');
        }
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

// Update the version display label and hidden input
function populateVersionDropdown(versions, currentVersion) {
    const versionHidden = document.getElementById('domainVersionSelect');
    const versionDisplay = document.getElementById('domainVersionDisplay');

    if (versionHidden) versionHidden.value = currentVersion;
    if (versionDisplay) versionDisplay.value = `v${currentVersion}`;
}

// Readiness of an incremental / microbatch domain: what the mode would run as and
// which slot-pipeline sources lack row tracking (GET /dtwin/incremental/readiness).
// A topic-fed realtime pipeline lists its sources unprobed (row_tracking null).
function _readinessUntracked(report) {
    return (report.sources || []).filter(s => s.row_tracking === false).map(s => s.table);
}

function _readinessLocalName(uri) {
    const text = String(uri || '');
    return text.slice(Math.max(text.lastIndexOf('#'), text.lastIndexOf('/')) + 1) || text;
}

// Per-mapping freshness class: row-local mappings feed the slot pipeline (and so
// microbatch); the others stay on _data's batch branch. The batch ones and why are the
// line's tooltip. Hidden when the mappings do not compile (counts null).
function _renderReadinessMappings(report) {
    const line = document.getElementById('materializationReadinessMappings');
    if (!line) return;
    const show = report.slot_mappings !== null && report.slot_mappings !== undefined;
    line.textContent = show
        ? `Mappings: ${report.slot_mappings} row-local (microbatch-eligible), ${report.batch_mappings} batch-incremental`
        : '';
    line.title = (report.mappings || [])
        .filter(m => m.kind === 'batch')
        .map(m => `${_readinessLocalName(m.class_uri || m.predicate)}: ${m.reason}`)
        .join('\n');
    line.classList.toggle('d-none', !show);
}

// Lakehouse engine only (lakehouse_rt null elsewhere): the query warehouse and the RT toggle.
function _renderReadinessLakehouseRT(report) {
    const line = document.getElementById('materializationReadinessLakehouseRT');
    if (!line) return;
    const rt = report.lakehouse_rt;
    line.textContent = rt
        ? `Lakehouse//RT: query warehouse ${rt.query_warehouse_configured ? 'set' : 'not set'}`
          + ` · RT reads ${rt.use_sea ? 'on' : 'off'}`
          + (rt.fast_path === 'slots_live' ? ' · fast path (live slots)' : '')
        : '';
    line.classList.toggle('d-none', !rt);
}

function renderMaterializationReadiness(report) {
    const box = document.getElementById('materializationReadiness');
    const text = document.getElementById('materializationReadinessText');
    const list = document.getElementById('materializationReadinessSources');
    const button = document.getElementById('enableRowTrackingBtn');
    if (!box || !text || !list || !button) return;
    const decision = report.decision || {};
    const how = decision.merge_fallback ? ' by MERGE' : '';
    const reason = decision.reason ? ` (${decision.reason})` : '';
    const problem = report.compile_error ? ` — mappings do not compile: ${report.compile_error}` : '';
    text.textContent = `${report.mode_requested} runs as ${decision.effective}${how}${reason}${problem}`;
    const untracked = _readinessUntracked(report);
    list.replaceChildren(...untracked.map(table => {
        const item = document.createElement('li');
        const icon = document.createElement('i');
        icon.className = 'bi bi-exclamation-triangle text-warning me-1';
        item.append(icon, `${table} — row tracking off`);
        return item;
    }));
    list.classList.toggle('d-none', untracked.length === 0);
    button.classList.toggle('d-none', untracked.length === 0);
    const realtimeLine = document.getElementById('materializationReadinessRealtime');
    if (realtimeLine) {
        const rt = report.realtime || {};
        const showRealtime = report.mode_requested === 'realtime';
        realtimeLine.textContent = showRealtime
            ? `Real-time: ${rt.enabled ? 'enabled' : 'disabled on this deployment (ONTOBRICKS_REALTIME_ENABLED)'}`
              + (rt.backend_eligible ? '' : ' · needs the Lakebase backend')
              + ` · topic ${rt.source_configured ? `${rt.kind} ${rt.topic}` : 'not configured'}`
              + (rt.experimental ? ' (experimental source)' : '')
              + (report.row_tracking_required === false ? ' · slot pipeline reads the topic (no row tracking needed)' : '')
            : '';
        realtimeLine.classList.toggle('d-none', !showRealtime);
    }
    _renderReadinessMappings(report);
    _renderReadinessLakehouseRT(report);
    button.dataset.tables = JSON.stringify(untracked);
    box.classList.remove('d-none');
}

async function loadMaterializationReadiness() {
    const modeEl = document.getElementById('domainMaterializationMode');
    const box = document.getElementById('materializationReadiness');
    if (!modeEl || !box) return;
    if ((modeEl.dataset.savedValue || modeEl.value) === 'full') {
        box.classList.add('d-none');
        return;
    }
    try {
        const resp = await fetch('/dtwin/incremental/readiness', { credentials: 'same-origin' });
        if (!resp.ok) return;
        renderMaterializationReadiness(await resp.json());
    } catch (err) {
        console.warn('Readiness check failed', err);
    }
}

async function enableRowTracking() {
    const button = document.getElementById('enableRowTrackingBtn');
    const result = document.getElementById('enableRowTrackingResult');
    if (!button || !result) return;
    const tables = JSON.parse(button.dataset.tables || '[]');
    button.disabled = true;
    try {
        const resp = await fetch('/dtwin/incremental/enable-row-tracking', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tables }),
        });
        const data = await resp.json();
        if (!resp.ok) {
            throw new Error(data.message || data.detail || `HTTP ${resp.status}`);
        }
        const rows = (data.tables || []);
        const failed = rows.filter(r => !r.success);
        result.textContent = failed.length
            ? `Row tracking could not be enabled on ${failed.map(r => r.table).join(', ')}: ${failed[0].error || 'error'}`
            : `Row tracking enabled on ${rows.length} table(s).`;
        result.classList.remove('d-none');
    } catch (err) {
        result.textContent = `Row tracking could not be enabled: ${err.message || err}`;
        result.classList.remove('d-none');
    } finally {
        button.disabled = false;
        loadMaterializationReadiness();
    }
}

/** Show the real-time source fields only for the ``realtime`` mode. */
function toggleRealtimeSourceSection() {
    const modeEl = document.getElementById('domainMaterializationMode');
    const section = document.getElementById('realtimeSourceSection');
    if (!modeEl || !section) return;
    section.classList.toggle('d-none', modeEl.value !== 'realtime');
}

// No-op: version selector has been replaced by a read-only label
function enableVersionSelector() {}

// Update UI based on version status (latest = editable, older = read-only)
function updateVersionStatusUI(isActive, version, hasRegistry) {
    const domainNameInput = document.getElementById('domainName');
    const domainNameHint = document.getElementById('domainNameHint');

    const editableFields = document.querySelectorAll('.domain-editable:not(#domainName)');
    const baseUriToggle = document.getElementById('baseUriCustomToggle');

    const versionDisplay = document.getElementById('domainVersionDisplay');
    const versionHidden = document.getElementById('domainVersionSelect');
    if (versionDisplay) versionDisplay.value = `v${version}`;
    if (versionHidden) versionHidden.value = version;

    if (hasRegistry && domainNameInput) {
        domainNameInput.disabled = true;
        domainNameInput.readOnly = true;
        domainNameInput.style.backgroundColor = '#e9ecef';
        if (domainNameHint) {
            domainNameHint.innerHTML = '<i class="bi bi-lock"></i> Locked (used as folder name in the registry)';
        }
    }
    
    if (isActive) {
        editableFields.forEach(el => {
            el.disabled = false;
            if (el.tagName === 'BUTTON') {
                el.classList.remove('disabled');
            }
        });
        if (baseUriToggle) baseUriToggle.disabled = false;
    } else {
        editableFields.forEach(el => {
            el.disabled = true;
            if (el.tagName === 'BUTTON') {
                el.classList.add('disabled');
            }
        });
        if (baseUriToggle) baseUriToggle.disabled = true;
        
        if (domainNameInput) {
            domainNameInput.disabled = true;
        }
    }
    
    if (typeof applyBaseUriMode === 'function') {
        applyBaseUriMode();
    }
}

// Fetch and update version status on page load
document.addEventListener('DOMContentLoaded', async function() {
    try {
        refreshDtNamesFromForm();

        // Re-derive the DT names whenever the Triple Store tab is shown
        // (panel is initially hidden) and whenever the user commits a
        // change to the domain name (on blur / Enter, NOT per-keystroke).
        const tsTab = document.getElementById('tab-triplestore');
        if (tsTab) {
            tsTab.addEventListener('shown.bs.tab', refreshDtNamesFromForm);
        }
        const nameEl = document.getElementById('domainName');
        if (nameEl) {
            nameEl.addEventListener('change', refreshDtNamesFromForm);
            nameEl.addEventListener('blur', refreshDtNamesFromForm);
            // Duplicate-name guard runs on blur as well as the
            // existing debounced ``input`` hook in domain.js, so the
            // user gets immediate feedback when committing the field.
            // The check itself, the inline ``invalid-feedback`` hint,
            // and the ``is-invalid`` class are owned by domain.js.
            if (typeof checkDomainNameAvailability === 'function') {
                nameEl.addEventListener('blur', () => checkDomainNameAvailability(nameEl));
            }
        }
        const versionEl = document.getElementById('domainVersionSelect');
        if (versionEl) {
            versionEl.addEventListener('change', refreshDtNamesFromForm);
        }
        const graphBackendEl = document.getElementById('domainGraphBackend');
        const neo4jDbElInit = document.getElementById('domainNeo4jDatabase');
        // The template already renders persisted values server-side. Mark
        // controls dirty on first interaction so the `/domain/info` refresh
        // below cannot overwrite a newer browser selection.
        if (graphBackendEl) {
            graphBackendEl.addEventListener('change', refreshDtNamesFromForm);
            graphBackendEl.addEventListener('change', syncNeo4jConnectionSection);
            graphBackendEl.addEventListener('change', toggleLakehouseMaterializationSection);
            graphBackendEl.addEventListener('change', toggleSearchCacheSection);
            graphBackendEl.addEventListener('change', applyGraphlessConstraints);
            graphBackendEl.addEventListener('change', () => {
                graphBackendEl.dataset.userEdited = '1';
            });
        }
        if (neo4jDbElInit) {
            neo4jDbElInit.addEventListener('change', () => {
                neo4jDbElInit.dataset.userEdited = '1';
            });
        }
        const materializationElInit = document.getElementById('domainLakehouseMaterialization');
        if (materializationElInit) {
            materializationElInit.addEventListener('change', () => {
                materializationElInit.dataset.userEdited = '1';
            });
        }
        const materializationModeElInit = document.getElementById('domainMaterializationMode');
        if (materializationModeElInit) {
            materializationModeElInit.addEventListener('change', () => {
                materializationModeElInit.dataset.userEdited = '1';
            });
        }
        const cacheElInit = document.getElementById('domainGraphCacheEnabled');
        if (cacheElInit) {
            cacheElInit.addEventListener('change', () => {
                cacheElInit.dataset.userEdited = '1';
                refreshSearchCacheStatusLine();
            });
        }
        const enableRowTrackingBtn = document.getElementById('enableRowTrackingBtn');
        if (enableRowTrackingBtn) {
            enableRowTrackingBtn.addEventListener('click', enableRowTracking);
        }
        const materializationModeSelect = document.getElementById('domainMaterializationMode');
        if (materializationModeSelect) {
            materializationModeSelect.addEventListener('change', toggleRealtimeSourceSection);
        }
        toggleRealtimeSourceSection();
        loadMaterializationReadiness();
        initGraphBackendCards();
        syncNeo4jConnectionSection();
        toggleLakehouseMaterializationSection();
        toggleSearchCacheSection();
        applyGraphlessConstraints();
        const refreshDbBtn = document.getElementById('btnRefreshNeo4jDatabases');
        if (refreshDbBtn) {
            refreshDbBtn.addEventListener('click', () => loadNeo4jDatabases(true));
        }
        const llmBrowse = document.getElementById('domainLlmBrowse');
        if (llmBrowse) {
            llmBrowse.addEventListener('click', browseLlmEndpoints);
        }

        const [statusData, infoData] = await Promise.all([
            fetchOnce('/domain/version-status').catch(() => null),
            fetchOnce('/domain/info').catch(() => null)
        ]);

        if (statusData && statusData.success) {
            // Editability depends only on lifecycle status (DRAFT), not on
            // whether this is the latest version. Older DRAFT versions edit.
            const editable = (statusData.status || 'DRAFT') === 'DRAFT';
            updateVersionStatusUI(editable, statusData.version, statusData.has_registry);
            syncGraphBackendCardEditability();
            populateVersionDropdown(statusData.available_versions, statusData.version);
            const sf = statusData.domain_folder || statusData.project_folder;
            if (sf) {
                currentDomainFolder = sf;
            }
        }

        if (infoData && infoData.success) {
            const inf = infoData.domain_folder || infoData.project_folder;
            if (inf && !currentDomainFolder) {
                currentDomainFolder = inf;
            }
            const llmNameInput = document.getElementById('domainLlmEndpoint');
            if (llmNameInput && !llmNameInput.dataset.userEdited
                    && infoData.info && infoData.info.llm_endpoint) {
                setSelectedLlmEndpoint(
                    infoData.info.llm_endpoint,
                    infoData.info.llm_endpoint_kind || ''
                );
            }
            const graphBackendEl = document.getElementById('domainGraphBackend');
            // Skip once the user has touched the field: this fetch reflects
            // whatever was persisted *before* the page loaded, so applying it
            // after an edit would silently revert the user's pick (see the
            // dirty-flag wiring above).
            if (graphBackendEl && !graphBackendEl.dataset.userEdited
                    && infoData.info && infoData.info.graph_backend) {
                graphBackendEl.value = infoData.info.graph_backend;
                syncGraphBackendCards();
            }
            const neo4jDbEl = document.getElementById('domainNeo4jDatabase');
            if (neo4jDbEl && !neo4jDbEl.dataset.userEdited
                    && infoData.info && infoData.info.neo4j_connection) {
                const saved = infoData.info.neo4j_connection;
                if (![...neo4jDbEl.options].some(o => o.value === saved)) {
                    neo4jDbEl.add(new Option(saved, saved, true, true));
                }
                neo4jDbEl.value = saved;
                neo4jDbEl.dataset.savedValue = saved;
            }
            const materializationEl = document.getElementById('domainLakehouseMaterialization');
            if (materializationEl && !materializationEl.dataset.userEdited
                    && infoData.info && infoData.info.lakehouse_materialization) {
                materializationEl.value = infoData.info.lakehouse_materialization;
                materializationEl.dataset.savedValue = infoData.info.lakehouse_materialization;
            }
            const materializationModeEl = document.getElementById('domainMaterializationMode');
            if (materializationModeEl && !materializationModeEl.dataset.userEdited
                    && infoData.info && infoData.info.materialization_mode) {
                materializationModeEl.value = infoData.info.materialization_mode;
                materializationModeEl.dataset.savedValue = infoData.info.materialization_mode;
            }
            // Runs after the saved value is known, so the freshly fetched
            // option list keeps it selected. Safe even when the user already
            // edited the field: it reads the select's *current* value first
            // (see `_loadNeo4jConnectionOptions`) and only falls back to
            // `dataset.savedValue` when the select has nothing of its own.
            const cacheEl = document.getElementById('domainGraphCacheEnabled');
            if (cacheEl && !cacheEl.dataset.userEdited && infoData.info) {
                if (typeof infoData.info.graph_cache_enabled === 'boolean') {
                    cacheEl.checked = infoData.info.graph_cache_enabled;
                    cacheEl.dataset.savedValue = infoData.info.graph_cache_enabled ? 'true' : 'false';
                }
                if (infoData.info.graph_cache_state) {
                    cacheEl.dataset.cacheState = infoData.info.graph_cache_state;
                }
            }
            syncNeo4jConnectionSection();
            toggleLakehouseMaterializationSection();
            toggleSearchCacheSection();
            refreshSearchCacheStatusLine();
            applyGraphlessConstraints();
        }

        // The DT panel reads catalog/schema from the dropdown rendering of
        // version-status; rerun once after status is applied so the FQNs
        // line up with the real registry config.
        refreshDtNamesFromForm();
        
        // Re-enable version selector after a short delay to override any global disabling
        setTimeout(enableVersionSelector, 100);
    } catch (e) {
        console.log('Could not fetch domain status:', e);
    } finally {
        // Hand-off from navbar.js domainNew(): hide the overlay once the
        // initial info round-trips have resolved.
        try {
            if (sessionStorage.getItem('ob_creating_new_domain') === '1') {
                sessionStorage.removeItem('ob_creating_new_domain');
                if (typeof hideDomainLoading === 'function') hideDomainLoading();
            }
        } catch (e) {}
    }
});


/**
 * Lowercase slug with non ``[a-z0-9_]`` replaced by ``_`` — matches
 * the triple-store / snapshot *table* naming helpers in the backend.
 * The **registry folder** slug uses ``sanitize_domain_folder`` instead
 * (non-alphanumerics stripped, not replaced); with CamelCase-only
 * domain names the two coincide; they can diverge if validation ever
 * loosens.
 */
function _safeDomainSlug(name) {
    return (name || '').toLowerCase().replace(/[^a-z0-9_]/g, '_');
}

/**
 * Best-effort split of an existing FQN ("catalog.schema.table") into
 * its catalog/schema parts so we can keep the registry prefix while
 * rewriting the table portion. Returns ``null`` when the value is
 * empty or not a 3-part dotted name (i.e. registry not configured).
 */
function _splitFqnPrefix(fqn) {
    const parts = (fqn || '').split('.');
    if (parts.length === 3 && parts[0] && parts[1]) {
        return { catalog: parts[0], schema: parts[1] };
    }
    return null;
}

/**
 * Recompute the Triple-Store Gateway FQN from the current domain name +
 * version inputs. Mirrors the backend naming rules so the user sees
 * what UC objects *will be* called once they save the domain —
 * without round-tripping to the server. Bound to the domain-name
 * ``change``/``blur`` event so it runs once per committed name
 * change, not on every keystroke.
 */
function refreshDtNamesFromForm() {
    const nameEl = document.getElementById('domainName');
    const versionEl = document.getElementById('domainVersionSelect');
    const name = nameEl ? nameEl.value.trim() : '';
    const safe = _safeDomainSlug(name);
    const version = versionEl ? versionEl.value.trim() : '1';
    const v = version || '1';

    const tsEl = document.getElementById('domainTriplestoreFullName');
    if (tsEl) {
        const prefix = _splitFqnPrefix(tsEl.value);
        const tsName = 'triplestore_' + safe + '_V' + v;
        tsEl.value = prefix ? prefix.catalog + '.' + prefix.schema + '.' + tsName : (safe ? tsName : '');
    }
}


/* ──────────────────────────────────────────────────────────────────────────
   Current-domain KPI / health band (moved here from the Home page).
   Populates the #kpi* tiles rendered at the top of Domain → General:
   entities, relationships, mappings, quality, status, version.
   Self-contained (IIFE) to avoid leaking generic helper names as globals.
   ────────────────────────────────────────────────────────────────────────── */
(function () {
    function esc(text) {
        return (typeof window.escapeHtml === 'function')
            ? window.escapeHtml(text)
            : String(text == null ? '' : text);
    }

    function setTile(valueId, tileId, value, active) {
        const valueEl = document.getElementById(valueId);
        const tileEl = document.getElementById(tileId);
        if (valueEl) valueEl.textContent = value != null ? value : '-';
        if (tileEl) tileEl.className = 'ob-kpi-tile ' + (active ? 'tile-success' : 'tile-muted');
    }

    function statusBadge(status) {
        const map = {
            'DRAFT': 'bg-warning-subtle text-dark border-warning',
            'IN-REVIEW': 'bg-info-subtle text-dark border-info',
            'PUBLISHED': 'bg-success-subtle text-dark border-success',
        };
        const key = (status || 'DRAFT').toUpperCase();
        const cls = map[key] || map['DRAFT'];
        const label = key === 'IN-REVIEW'
            ? 'In Review'
            : (key.charAt(0) + key.slice(1).toLowerCase());
        return '<span class="badge border ' + cls + '">' + esc(label) + '</span>';
    }

    function renderQuality(score) {
        const valueEl = document.getElementById('kpiQuality');
        const tileEl = document.getElementById('kpiQualityTile');
        if (!valueEl || !tileEl) return;
        if (score == null) {
            valueEl.textContent = '-';
            tileEl.className = 'ob-kpi-tile tile-muted';
            return;
        }
        valueEl.textContent = score;
        let variant = 'tile-danger';
        if (score >= 80) variant = 'tile-success';
        else if (score >= 50) variant = 'tile-warning';
        tileEl.className = 'ob-kpi-tile ' + variant;
    }

    function renderStatus(status) {
        const valueEl = document.getElementById('kpiStatus');
        const tileEl = document.getElementById('kpiStatusTile');
        if (!valueEl || !tileEl) return;
        valueEl.innerHTML = statusBadge(status);
        tileEl.className = 'ob-kpi-tile tile-muted';
    }

    function renderVersion(version) {
        const valueEl = document.getElementById('kpiVersion');
        const tileEl = document.getElementById('kpiVersionTile');
        if (!valueEl || !tileEl) return;
        valueEl.textContent = 'v' + version;
        tileEl.className = 'ob-kpi-tile tile-muted';
    }

    async function loadDomainKpis() {
        // Only run when the KPI band is on the page.
        if (!document.getElementById('domainKpiPanel')) return;
        try {
            // Reuse fetchOnce so /domain/info is shared with the page's
            // main version-status loader (single network round-trip).
            const [info, session] = await Promise.all([
                fetchOnce('/domain/info').catch(() => ({})),
                fetchOnce('/session-status').catch(() => ({})),
            ]);

            const stats = (info && info.stats) || {};
            const meta = (info && info.info) || {};

            const entityCount = stats.entities != null
                ? stats.entities
                : (session.class_count || 0);
            const relationshipCount = stats.relationships != null
                ? stats.relationships
                : (session.property_count || 0);
            const mappingCount = (stats.entity_mappings || 0) + (stats.relationship_mappings || 0)
                || ((session.entities || 0) + (session.relationships || 0));

            setTile('kpiEntities', 'kpiEntitiesTile', entityCount, entityCount > 0);
            setTile('kpiRelationships', 'kpiRelationshipsTile', relationshipCount, relationshipCount > 0);
            setTile('kpiMappings', 'kpiMappingsTile', mappingCount, mappingCount > 0);
            renderQuality(info ? info.precision_score : null);
            renderStatus(meta.status || 'DRAFT');
            renderVersion(meta.version || session.version || '1');
        } catch (error) {
            console.error('Error loading domain KPIs:', error);
        }
    }

    document.addEventListener('DOMContentLoaded', loadDomainKpis);
})();
