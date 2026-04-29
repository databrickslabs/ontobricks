/**
 * OntoBricks - query-sync.js
 * Triple Store Sync: generates all triples and writes them to a UC table.
 */

const SYNC_TASK_KEY = 'ontobricks_sync_task';

/** Whether ontology + assignments are both ready */
let syncIsReady = false;

/** Whether a sync task is currently running */
let syncIsRunning = false;

/** Whether the triple store table has data */
let tripleStoreHasData = false;

/**
 * Fetch all Information-page data in a single round-trip and distribute
 * results to the individual rendering functions.
 *
 * The caller is responsible for showing/hiding the loading overlay so that
 * it covers the full render cycle (including post-fetch DOM updates).
 *
 * Returns the parsed payload so callers can act on it (e.g. domain info).
 */
async function loadSyncInfo() {
    try {
        var resp = await fetch('/dtwin/sync/info', { credentials: 'same-origin' });
        var payload = await resp.json();

        // --- Readiness ---
        _applyReadiness(payload.readiness || {});

        // --- Triplestore status ---
        var tsStatus = payload.triplestore_status || {};
        tripleStoreHasData = !!(tsStatus.success && tsStatus.has_data);
        console.log('[Sync] Consolidated triplestore status -> hasData:', tripleStoreHasData);
        renderTripleStoreStatus(tsStatus);

        // --- Rebuild warning (timestamps and/or session change flags from readiness) ---
        _updateSyncRebuildWarningFromPayload(payload);

        updateDataMenus();
        updateInsightsTab();
        return payload;
    } catch (e) {
        console.error('[Sync] Error loading consolidated sync info:', e);
        return null;
    }
}

/**
 * Show the top rebuild banner when timestamps indicate drift or when the
 * session reports ontology / mapping edits since load (readiness flags).
 */
function _updateSyncRebuildWarningFromPayload(payload) {
    var warning = document.getElementById('syncRebuildWarning');
    if (!warning) return;
    var ch = (payload && payload.changes) || {};
    var r = (payload && payload.readiness) || {};
    var show = !!(ch.needs_rebuild || r.ontology_changed || r.assignment_changed);
    if (show) {
        warning.classList.remove('d-none');
        warning.classList.add('d-flex');
    } else {
        warning.classList.remove('d-flex');
        warning.classList.add('d-none');
    }
}

/**
 * Apply readiness data obtained from the consolidated endpoint.
 */
function _applyReadiness(data) {
    var ontologyReady = !!data.ontology_valid;
    var assignmentReady = !!data.mapping_valid;
    var ontologyChanged = !!data.ontology_changed;
    var assignmentChanged = !!data.assignment_changed;
    var ontologyStats = data.ontology_stats || {};
    var assignmentStats = data.mapping_stats || {};

    var entities = ontologyStats.classes || 0;
    var attributes = ontologyStats.attributes || 0;
    var relationships = ontologyStats.object_properties || ontologyStats.properties || 0;
    var mappedEntities = assignmentStats.entities || 0;
    var mappedRels = assignmentStats.relationships || 0;

    _setSyncTile('syncTileEntities', entities, entities > 0 ? 'success' : 'muted');
    _setSyncTile('syncTileAttributes', attributes, attributes > 0 ? 'success' : 'muted');
    _setSyncTile('syncTileRelationships', relationships, relationships > 0 ? 'success' : 'muted');
    _setSyncTile('syncTileMappedEntities', mappedEntities,
        assignmentReady ? 'success' : mappedEntities > 0 ? 'warning' : 'danger');
    _setSyncTile('syncTileMappedRels', mappedRels,
        assignmentReady ? 'success' : mappedRels > 0 ? 'warning' : 'danger');

    syncIsReady = ontologyReady && assignmentReady;

    var alertEl = document.getElementById('syncNotReadyAlert');
    if (alertEl) {
        if (syncIsReady) alertEl.classList.add('d-none');
        else alertEl.classList.remove('d-none');
    }

    var syncBtn = document.getElementById('syncStartBtn');
    var loadBtn = document.getElementById('syncLoadBtn');
    if (syncBtn) syncBtn.disabled = !syncIsReady;
    if (loadBtn) loadBtn.disabled = !syncIsReady;

    var loadingEl = document.getElementById('syncReadinessLoading');
    var contentEl = document.getElementById('syncReadinessContent');
    if (loadingEl) loadingEl.classList.add('d-none');
    if (contentEl) contentEl.classList.remove('d-none');

    var staleRow = document.getElementById('syncReadinessStaleIndicators');
    var ontBadge = document.getElementById('syncBadgeOntologyChanged');
    var mapBadge = document.getElementById('syncBadgeAssignmentChanged');
    if (ontBadge) {
        if (ontologyChanged) ontBadge.classList.remove('d-none');
        else ontBadge.classList.add('d-none');
    }
    if (mapBadge) {
        if (assignmentChanged) mapBadge.classList.remove('d-none');
        else mapBadge.classList.add('d-none');
    }
    if (staleRow) {
        if (ontologyChanged || assignmentChanged) staleRow.classList.remove('d-none');
        else staleRow.classList.add('d-none');
    }
}

function _setSyncTile(tileId, value, state) {
    var tile = document.getElementById(tileId);
    if (!tile) return;
    tile.className = 'ob-kpi-tile' + (state ? ' tile-' + state : '');
    var valEl = document.getElementById(tileId + 'Value');
    if (valEl) valEl.textContent = value;
}

/**
 * Apply DT-existence data obtained from the consolidated endpoint.
 * Mirrors _loadDtExistence() rendering without the separate fetch.
 */
function _applyDtExistence(data) {
    function _badge(flag, okText, failText, unknownText) {
        if (flag === true)
            return '<span class="badge bg-success bg-opacity-10 text-success border border-success"><i class="bi bi-check-circle-fill me-1"></i>' + okText + '</span>';
        if (flag === false)
            return '<span class="badge bg-danger text-white border border-danger"><i class="bi bi-x-circle-fill me-1"></i>' + failText + '</span>';
        return '<span class="badge bg-secondary bg-opacity-10 text-secondary border"><i class="bi bi-dash-circle me-1"></i>' + (unknownText || 'N/A') + '</span>';
    }

    var viewEl = document.getElementById('dtExistView');
    if (viewEl) {
        viewEl.innerHTML = _badge(data.view_exists, 'Exists', 'Not found', 'Not configured');
        if (data.view_check_error) viewEl.title = data.view_check_error;
        else if (data.view_table) viewEl.title = 'Queried: ' + data.view_table;
        else viewEl.title = '';
    }

    var zcCard = document.getElementById('dtZeroCopyCard');
    if (zcCard) {
        if (data.view_exists === true)
            zcCard.className = 'border rounded p-3 h-100 border-success';
        else if (data.view_exists === false)
            zcCard.className = 'border rounded p-3 h-100 border-danger';
        else
            zcCard.className = 'border rounded p-3 h-100';
    }

    // Graph DB card: status badges, local path + card border
    var localEl = document.getElementById('dtExistLocal');
    if (localEl) localEl.innerHTML = _badge(data.local_lbug_exists, 'Loaded', 'Not loaded', 'N/A');

    var localPathEl = document.getElementById('dtLocalPath');
    if (localPathEl) localPathEl.textContent = data.local_lbug_path || '';

    var regEl = document.getElementById('dtExistRegistry');
    if (regEl) {
        regEl.innerHTML = _badge(data.registry_lbug_exists, 'Archived', 'Not archived', 'Not configured');
        if (data.registry_check_error) regEl.title = data.registry_check_error;
        else if (data.registry_lbug_path) regEl.title = 'Archive: ' + data.registry_lbug_path;
        else regEl.title = '';
    }

    var reloadBtn = document.getElementById('dtReloadFromRegistry');
    if (reloadBtn) {
        if (data.registry_lbug_exists === true) reloadBtn.classList.remove('d-none');
        else reloadBtn.classList.add('d-none');
    }

    var graphCard = document.getElementById('dtGraphCard');
    if (graphCard) {
        if (data.local_lbug_exists === true)
            graphCard.className = 'border rounded p-3 h-100 border-success';
        else if (data.local_lbug_exists === false)
            graphCard.className = 'border rounded p-3 h-100 border-danger';
        else
            graphCard.className = 'border rounded p-3 h-100';
    }

    // Snapshot info (inside Triple-Store card)
    var snapshotArea = document.getElementById('dtSnapshotArea');
    var snapshotNameEl = document.getElementById('dtSnapshotName');
    var snapshotBadgeEl = document.getElementById('dtExistSnapshot');
    var dropBtn = document.getElementById('dtDropSnapshot');
    if (snapshotArea && data.snapshot_table) {
        snapshotArea.classList.remove('d-none');
        if (snapshotNameEl) snapshotNameEl.textContent = data.snapshot_table;
        if (snapshotBadgeEl) snapshotBadgeEl.innerHTML = _badge(data.snapshot_exists, 'Exists', 'Not created', 'N/A');
        if (dropBtn) {
            if (data.snapshot_exists === true) dropBtn.classList.remove('d-none');
            else dropBtn.classList.add('d-none');
        }
    } else if (snapshotArea) {
        snapshotArea.classList.add('d-none');
    }

    // Global info: last update & last built
    var lastUpdateEl = document.getElementById('dtLastUpdate');
    if (lastUpdateEl) {
        if (data.last_update) {
            try {
                var dtu = new Date(data.last_update);
                var fmtU = dtu.toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
                lastUpdateEl.innerHTML = '<i class="bi bi-info-circle-fill text-info me-1"></i>' + fmtU;
                lastUpdateEl.className = '';
            } catch (_) { lastUpdateEl.textContent = data.last_update; lastUpdateEl.className = ''; }
        } else {
            lastUpdateEl.innerHTML = '<span class="text-muted">No changes yet</span>';
        }
    }

    var lastBuiltEl = document.getElementById('dtLastBuilt');
    if (lastBuiltEl) {
        if (data.last_built) {
            try {
                var dt = new Date(data.last_built);
                var formatted = dt.toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
                lastBuiltEl.innerHTML = '<i class="bi bi-check-circle-fill text-success me-1"></i>' + formatted;
                lastBuiltEl.className = '';
            } catch (_) { lastBuiltEl.textContent = data.last_built; lastBuiltEl.className = ''; }
        } else {
            lastBuiltEl.innerHTML = '<span class="text-muted">Never built</span>';
        }
    }
}

/**
 * Guard: set to true once the sync section has been fully loaded.
 * Prevents the 300ms-delayed nav click in query.js from re-running the
 * whole initialization a second time.  Reset to false when the user
 * navigates AWAY from the sync section (MutationObserver below).
 */
let _syncSectionLoaded = false;

/**
 * Initialize the sync section when it becomes visible.
 * Uses the consolidated /dtwin/sync/info endpoint for a single round-trip.
 * The loading overlay stays visible until ALL data is fetched AND rendered,
 * then drops to reveal the fully-populated page in one shot.
 */
async function initSyncSection() {
    if (_syncSectionLoaded) return;
    _syncSectionLoaded = true;

    var overlay = document.getElementById('syncLoadingOverlay');
    if (overlay) overlay.classList.remove('d-none');

    try {
        var payload = await loadSyncInfo();

        var cfg = window.__TRIPLESTORE_CONFIG || {};

        const di = payload && (payload.domain_info || payload.project_info);
        if (di && di.success && di.info) {
            cfg = {
                view_table: di.info.view_table || '',
                graph_name: di.info.graph_name || '',
                cache: {}
            };
            window.__TRIPLESTORE_CONFIG = cfg;
        }

        var tableEl = document.getElementById('syncTriplestoreTable');
        if (tableEl) {
            tableEl.value = cfg.view_table || '';
        }

        var viewNameEl = document.getElementById('dtViewName');
        if (viewNameEl) {
            viewNameEl.textContent = cfg.view_table || 'Not configured';
        }

        var globalInfo = document.getElementById('dtGlobalInfo');
        if (globalInfo) globalInfo.classList.remove('d-none');

        var columnsArea = document.getElementById('dtColumnsArea');
        if (columnsArea) columnsArea.classList.remove('d-none');

        if (payload && payload.dt_existence) {
            _applyDtExistence(payload.dt_existence);
        }

        var targetLabel = document.getElementById('syncTargetLabel');
        if (targetLabel) {
            targetLabel.innerHTML = '<i class="bi bi-lightning-charge me-1"></i>Digital Twin';
        }

        var ladybugSyncInfo = document.getElementById('ladybugSyncInfo');
        if (ladybugSyncInfo) {
            ladybugSyncInfo.classList.remove('d-none');
        }

        if (typeof refreshNavbarIndicators === 'function') refreshNavbarIndicators();

        await checkAndResumeSyncTask();
    } finally {
        if (overlay) overlay.classList.add('d-none');
    }
}

/**
 * Check whether the ontology or assignments have changed since the last
 * Digital Twin build. Shows a warning banner when a rebuild is recommended.
 */
async function checkConfigChanges() {
    var warning = document.getElementById('syncRebuildWarning');
    if (!warning) return;
    try {
        var resp = await fetch('/dtwin/sync/changes', { credentials: 'same-origin' });
        var data = await resp.json();
        if (data.needs_rebuild) {
            warning.classList.remove('d-none');
            warning.classList.add('d-flex');
        } else {
            warning.classList.remove('d-flex');
            warning.classList.add('d-none');
        }
    } catch (e) {
        console.warn('[Sync] Failed to check config changes:', e);
        warning.classList.remove('d-flex');
        warning.classList.add('d-none');
    }
}


/**
 * Check if the triple store has data.
 * When ``refresh`` is true the backend always hits the DB and updates the
 * session cache; the loading overlay is shown while the request is in flight.
 */
async function checkTripleStoreStatus(refresh) {
    var url = '/dtwin/sync/status';
    if (refresh) url += '?refresh=true';

    var overlay = document.getElementById('syncLoadingOverlay');
    var showedOverlay = false;
    if (refresh && overlay) { overlay.classList.remove('d-none'); showedOverlay = true; }

    try {
        const response = await fetch(url, { credentials: 'same-origin' });
        const data = await response.json();
        tripleStoreHasData = !!(data.success && data.has_data);
        console.log('[Sync] Triple store status:', data, '-> hasData:', tripleStoreHasData);
        renderTripleStoreStatus(data);
    } catch (e) {
        console.error('[Sync] Error checking triple store status:', e);
        tripleStoreHasData = false;
        renderTripleStoreStatus(null);
    } finally {
        if (showedOverlay && overlay) overlay.classList.add('d-none');
    }
    updateDataMenus();
    updateInsightsTab();

    if (refresh) {
        window._obInsights.loaded = false;
        var insightsTab = document.getElementById('sync-tab-insights');
        if (insightsTab && insightsTab.classList.contains('active')) {
            loadInsights();
        }
        _loadDtExistence();
    }
}

/**
 * Display the triple store table status in the Sync page.
 * Shows row count when data exists, or a warning when empty / missing.
 */
function renderTripleStoreStatus(data) {
    const area = document.getElementById('tripleStoreStatusArea');
    if (!area) return;

    area.classList.remove('d-none');

    if (!data || !data.success) {
        // Error checking status (e.g. Databricks not configured)
        const msg = (data && data.message) ? data.message : 'Unable to check triple store status';
        area.innerHTML =
            '<div class="alert alert-warning small mb-0 py-1 px-2">' +
            '<i class="bi bi-exclamation-triangle me-1"></i>' + msg +
            '</div>';
        return;
    }

    if (data.has_data) {
        const count = (data.count || 0).toLocaleString();
        area.innerHTML =
            '<div class="small mb-0 py-1 px-2">' +
            '<i class="bi bi-check-circle text-success me-1"></i>Graph contains <strong>' + count + '</strong> triples' +
            '</div>';
    } else {
        const reason = data.reason || '';
        let msg;
        if (reason.toLowerCase().includes('does not exist')) {
            msg = 'Digital Twin not built yet. Click <strong>Build</strong> to create the VIEW and graph.';
        } else if (reason.toLowerCase().includes('not configured')) {
            msg = 'Digital Twin is not configured. Set it in <a href="/domain/#information">Domain Settings</a>.';
        } else {
            msg = 'Graph is empty. Run <strong>Synchronize</strong> to generate triples.';
        }
        area.innerHTML =
            '<div class="alert alert-warning small mb-0 py-1 px-2">' +
            '<i class="bi bi-exclamation-triangle me-1"></i>' + msg +
            '</div>';
    }
}

/**
 * Enable or disable the Data section sidebar menus (Quality, Triples, Knowledge Graph).
 *
 * Menus are enabled ONLY when ALL conditions are met:
 *   1. Ontology + assignments are ready (syncIsReady)
 *   2. No sync task is currently running (!syncIsRunning)
 *   3. The triple store has data (tripleStoreHasData)
 */
function updateDataMenus() {
    const canAccess = syncIsReady && !syncIsRunning && tripleStoreHasData;
    console.log('[Sync] updateDataMenus: syncIsReady=' + syncIsReady +
                ', syncIsRunning=' + syncIsRunning +
                ', tripleStoreHasData=' + tripleStoreHasData +
                ' -> canAccess=' + canAccess);

    document.querySelectorAll('.sync-requires-ready').forEach(link => {
        if (canAccess) {
            link.classList.remove('disabled');
            link.style.pointerEvents = '';
            link.style.opacity = '';
            link.removeAttribute('title');
        } else {
            link.classList.add('disabled');
            link.style.pointerEvents = 'none';
            link.style.opacity = '0.45';

            // Set a tooltip explaining why the menu is disabled
            if (syncIsRunning) {
                link.setAttribute('title', 'Synchronization in progress…');
            } else if (!syncIsReady) {
                link.setAttribute('title', 'Ontology and mapping assignments must be configured first');
            } else if (!tripleStoreHasData) {
                link.setAttribute('title', 'Triple store is empty — run Sync first');
            }
        }
    });
}

/**
 * Check sessionStorage for an in-progress sync task and resume monitoring.
 */
async function checkAndResumeSyncTask() {
    const taskId = sessionStorage.getItem(SYNC_TASK_KEY);
    if (!taskId) return;

    try {
        const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
        const data = await response.json();
        if (!data.success) {
            sessionStorage.removeItem(SYNC_TASK_KEY);
            return;
        }
        const task = data.task;
        if (task.status === 'completed') {
            sessionStorage.removeItem(SYNC_TASK_KEY);
            showSyncResult(task.result);
            // Sync just finished — mark data as available and hide rebuild warning
            tripleStoreHasData = true;
            syncIsRunning = false;
            updateDataMenus();
            var rebuildWarning = document.getElementById('syncRebuildWarning');
            if (rebuildWarning) { rebuildWarning.classList.remove('d-flex'); rebuildWarning.classList.add('d-none'); }
        } else if (task.status === 'running' || task.status === 'pending') {
            // Resume monitoring — menus stay disabled
            syncIsRunning = true;
            updateDataMenus();
            showSyncProgress();
            monitorSyncTask(taskId);
        } else {
            sessionStorage.removeItem(SYNC_TASK_KEY);
        }
    } catch (e) {
        sessionStorage.removeItem(SYNC_TASK_KEY);
    }
}

/**
 * Show a confirmation modal before building the Digital Twin.
 * Always displayed so the domain is saved with the latest changes.
 * Resolves to 'save' (user confirms) or 'cancel'.
 */
function _showSaveBeforeBuildDialog() {
    return new Promise(resolve => {
        const modalId = 'saveBeforeBuildModal';
        const existing = document.getElementById(modalId);
        if (existing) existing.remove();

        const html = `
            <div class="modal fade" id="${modalId}" tabindex="-1" data-bs-backdrop="static">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header bg-primary text-white">
                            <h5 class="modal-title">
                                <i class="bi bi-cloud-upload me-2"></i>Save &amp; Build
                            </h5>
                            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            <p>The domain will be saved to the registry before building the Digital Twin.</p>
                            <p class="mb-0 text-muted">
                                This ensures the triple store, GraphQL API, and other services
                                use the latest ontology and mapping configuration.
                            </p>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal"
                                    id="${modalId}_cancel">Cancel</button>
                            <button type="button" class="btn btn-primary"
                                    id="${modalId}_save">
                                <i class="bi bi-cloud-upload me-1"></i>Save &amp; Build
                            </button>
                        </div>
                    </div>
                </div>
            </div>`;

        document.body.insertAdjacentHTML('beforeend', html);
        const modalEl = document.getElementById(modalId);
        const modal = new bootstrap.Modal(modalEl);

        let resolved = false;
        function done(result) {
            if (resolved) return;
            resolved = true;
            modal.hide();
            resolve(result);
        }

        document.getElementById(modalId + '_cancel').addEventListener('click', () => done('cancel'));
        document.getElementById(modalId + '_save').addEventListener('click', () => done('save'));
        modalEl.addEventListener('hidden.bs.modal', () => {
            done('cancel');
            modalEl.remove();
        });

        modal.show();
    });
}

/**
 * Start the synchronization process.
 * Always prompts the user to save the domain first so the registry
 * contains the latest ontology and mapping configuration.
 */
async function startTripleStoreSync() {
    const tableEl = document.getElementById('syncTriplestoreTable');
    const triplestoreTable = tableEl ? tableEl.value.trim() : '';

    if (!triplestoreTable) {
        showNotification('Triple store table not configured. Please set it in Domain Settings.', 'warning');
        return;
    }

    const choice = await _showSaveBeforeBuildDialog();
    if (choice === 'cancel') return;

    try {
        if (typeof saveDomainInfoBeforeSave === 'function') {
            await saveDomainInfoBeforeSave();
        }
        await doDomainSave();
    } catch (err) {
        showNotification('Save failed: ' + err.message, 'error');
        return;
    }

    var buildModeEl = document.querySelector('input[name="buildMode"]:checked');
    var buildMode = buildModeEl ? buildModeEl.value : 'incremental';

    // Disable button and menus
    const btn = document.getElementById('syncStartBtn');
    if (btn) btn.disabled = true;
    var resultCard = document.getElementById('syncResultCard');
    if (resultCard) resultCard.classList.add('d-none');

    // Mark sync as running — disable data menus
    syncIsRunning = true;
    updateDataMenus();

    showSyncProgress();
    updateSyncProgress(0, 'Starting synchronization...');

    try {
        const response = await fetch('/dtwin/sync/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                build_mode: buildMode
            }),
            credentials: 'same-origin'
        });

        const data = await response.json();

        if (!data.success) {
            hideSyncProgress();
            syncIsRunning = false;
            updateDataMenus();
            if (btn) btn.disabled = !syncIsReady;
            showNotification('Error: ' + (data.message || 'Failed to start sync'), 'error');
            return;
        }

        // Store task ID and start monitoring
        sessionStorage.setItem(SYNC_TASK_KEY, data.task_id);
        _loadDtExistence();
        monitorSyncTask(data.task_id);

    } catch (error) {
        hideSyncProgress();
        syncIsRunning = false;
        updateDataMenus();
        if (btn) btn.disabled = !syncIsReady;
        showNotification('Error: ' + error.message, 'error');
    }
}

/**
 * Monitor the sync task until completion.
 */
async function monitorSyncTask(taskId) {
    const pollInterval = 1500;

    while (true) {
        try {
            await new Promise(r => setTimeout(r, pollInterval));

            const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
            const data = await response.json();

            if (!data.success) {
                throw new Error('Task not found');
            }

            const task = data.task;
            updateSyncProgress(task.progress || 0, task.current_step_description || task.message || '');

            if (task.status === 'completed') {
                sessionStorage.removeItem(SYNC_TASK_KEY);
                hideSyncProgress();
                showSyncResult(task.result);
                showNotification('Triple store synchronized successfully!', 'success');

                // Build completed — hide the rebuild warning
                var rebuildWarning = document.getElementById('syncRebuildWarning');
                if (rebuildWarning) { rebuildWarning.classList.remove('d-flex'); rebuildWarning.classList.add('d-none'); }

                // Sync finished — data is now available, re-enable menus
                syncIsRunning = false;
                tripleStoreHasData = true;
                updateDataMenus();
                updateInsightsTab();

                // Invalidate cached insights so they reload on next section click
                window._obInsights.loaded = false;

                // Refresh the table status display with the new row count (force DB)
                checkTripleStoreStatus(true);

                // Refresh artefact existence flags
                _loadDtExistence();

                // Refresh navbar Digital Twin indicator
                if (typeof refreshDigitalTwinStatus === 'function') refreshDigitalTwinStatus();

                if (typeof refreshTasks === 'function') refreshTasks();
                break;
            } else if (task.status === 'failed') {
                sessionStorage.removeItem(SYNC_TASK_KEY);
                hideSyncProgress();
                showNotification('Sync failed: ' + (task.error || 'Unknown error'), 'error');

                // Sync failed — re-enable menus based on data state
                syncIsRunning = false;
                updateDataMenus();
                break;
            } else if (task.status === 'cancelled') {
                sessionStorage.removeItem(SYNC_TASK_KEY);
                hideSyncProgress();
                showNotification('Sync was cancelled', 'warning');

                syncIsRunning = false;
                updateDataMenus();
                break;
            }
        } catch (error) {
            console.error('[Sync] Monitoring error:', error);
            sessionStorage.removeItem(SYNC_TASK_KEY);
            hideSyncProgress();
            showNotification('Error monitoring sync task', 'error');

            syncIsRunning = false;
            updateDataMenus();
            break;
        }
    }

    const btn = document.getElementById('syncStartBtn');
    if (btn) btn.disabled = !syncIsReady;
}

/**
 * Show progress area.
 */
function showSyncProgress() {
    const area = document.getElementById('syncProgressArea');
    if (area) area.classList.remove('d-none');
}

/**
 * Hide progress area.
 */
function hideSyncProgress() {
    const area = document.getElementById('syncProgressArea');
    if (area) area.classList.add('d-none');
}

/**
 * Update progress bar and step text.
 */
function updateSyncProgress(percent, stepText) {
    const bar = document.getElementById('syncProgressBar');
    const step = document.getElementById('syncProgressStep');
    const status = document.getElementById('syncStatusText');
    if (bar) {
        bar.style.width = percent + '%';
        bar.textContent = percent + '%';
    }
    if (step) step.textContent = stepText;
    if (status) status.textContent = stepText;
}

/**
 * Display sync result card.
 */
function showSyncResult(result) {
    const card = document.getElementById('syncResultCard');
    const content = document.getElementById('syncResultContent');
    if (!card || !content || !result) return;

    const tripleCount = result.triple_count || 0;
    const viewTable = result.view_table || '';
    const graphName = result.graph_name || '';
    const duration = result.duration_seconds ? result.duration_seconds.toFixed(1) : '?';
    const buildMode = result.build_mode || 'full';
    const diff = result.diff || null;
    const skippedReason = result.skipped_reason || null;

    var modeLabel = 'Full rebuild';
    var modeIcon = 'bi-arrow-repeat';
    var modeColor = 'text-primary';
    if (buildMode === 'incremental') {
        modeLabel = 'Incremental';
        modeIcon = 'bi-lightning-charge';
        modeColor = 'text-success';
    } else if (buildMode === 'skipped') {
        modeLabel = 'Skipped';
        modeIcon = 'bi-skip-forward';
        modeColor = 'text-muted';
    }

    var mainMetric = tripleCount.toLocaleString();
    var mainLabel = 'Total triples';
    if (diff) {
        mainMetric = '+' + (diff.added || 0).toLocaleString() + ' / -' + (diff.removed || 0).toLocaleString();
        mainLabel = 'Changes applied';
    }
    if (skippedReason) {
        mainMetric = '<i class="bi bi-skip-forward"></i>';
        mainLabel = skippedReason;
    }

    content.innerHTML =
        '<div class="row g-3">' +
        '  <div class="col-md-4">' +
        '    <div class="border rounded p-2 text-center">' +
        '      <div class="fs-4 fw-bold text-primary">' + mainMetric + '</div>' +
        '      <small class="text-muted">' + mainLabel + '</small>' +
        '    </div>' +
        '  </div>' +
        '  <div class="col-md-4">' +
        '    <div class="border rounded p-2 text-center">' +
        '      <div class="fs-6 fw-bold text-secondary">' + duration + 's</div>' +
        '      <small class="text-muted">Duration</small>' +
        '    </div>' +
        '  </div>' +
        '  <div class="col-md-4">' +
        '    <div class="border rounded p-2 text-center">' +
        '      <div class="fs-6 fw-bold ' + modeColor + '"><i class="bi ' + modeIcon + ' me-1"></i>' + modeLabel + '</div>' +
        '      <small class="text-muted">Build mode</small>' +
        '    </div>' +
        '  </div>' +
        '</div>' +
        '<div class="mt-2">' +
        '  <small class="text-muted">View: <code>' + viewTable + '</code> | Graph: <code>' + graphName + '</code></small>' +
        '</div>';

    card.classList.remove('d-none');
}

/**
 * Load triples from the triple store table.
 * @param {Object} [options]
 * @param {boolean} [options.navigate=true]  - Switch to visualization after loading
 * @param {boolean} [options.silent=false]   - Suppress notifications
 */
async function loadTripleStore(options = {}) {
    const navigate = options.navigate !== false;
    const silent = options.silent === true;

    // Try to read from the sync input first, then fall back to domain settings
    const tableEl = document.getElementById('syncTriplestoreTable');
    let triplestoreTable = tableEl ? tableEl.value.trim() : '';

    if (!triplestoreTable) {
        var cfg = window.__TRIPLESTORE_CONFIG || {};
        if (cfg.graph_name) {
            triplestoreTable = cfg.graph_name;
        }
    }

    if (!triplestoreTable) {
        if (!silent) showNotification('Triple store table not configured. Please set it in Domain Settings.', 'warning');
        return;
    }

    // Disable sync load button
    const syncBtn = document.getElementById('syncLoadBtn');
    const loadingHtml = '<span class="spinner-border spinner-border-sm me-1"></span>Loading...';
    if (syncBtn) { syncBtn.disabled = true; syncBtn.innerHTML = loadingHtml; }

    try {
        const response = await fetch('/dtwin/sync/load', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({}),
            credentials: 'same-origin'
        });

        const data = await response.json();

        if (!data.success) {
            if (!silent) showNotification('Error: ' + (data.message || 'Failed to load triple store'), 'error');
            return;
        }

        const count = data.results ? data.results.length : 0;

        // Update triple store data flag
        tripleStoreHasData = count > 0;
        updateDataMenus();

        // Populate global query state (same as after a query execution)
        if (typeof queryResults !== 'undefined') {
            queryResults = data.results;
        }
        if (typeof generatedSql !== 'undefined') {
            generatedSql = data.generated_sql || `SELECT * FROM ${triplestoreTable}`;
        }

        // Update badges
        const badge = document.getElementById('resultCountBadge');
        if (badge) badge.textContent = count;
        const resultCount = document.getElementById('resultCount');
        if (resultCount) resultCount.textContent = count + ' results';

        // Display results in the Results section
        if (typeof displayResults === 'function') {
            displayResults({
                success: true,
                results: data.results,
                columns: data.columns || ['subject', 'predicate', 'object'],
                count: count,
                generated_sql: data.generated_sql || ''
            });
        }

        // Flag for visualization to rebuild graph
        if (typeof graphJustBuilt !== 'undefined') {
            graphJustBuilt = true;
        }

        if (!silent) showNotification(`Loaded ${count} triples from triple store`, 'success');

        // Navigate to Knowledge Graph only when explicitly requested
        if (navigate && typeof SidebarNav !== 'undefined' && SidebarNav.switchTo) {
            SidebarNav.switchTo('sigmagraph');
        }

    } catch (error) {
        if (!silent) showNotification('Error: ' + error.message, 'error');
    } finally {
        if (syncBtn) {
            syncBtn.disabled = !syncIsReady;
            syncBtn.innerHTML = '<i class="bi bi-box-arrow-in-right me-1"></i>Load Triple Store';
        }
    }
}

/**
 * Update the standalone Insight section visibility based on triple store data.
 */
function updateInsightsTab() {
    const noData = document.getElementById('insightNoData');
    if (noData) {
        if (tripleStoreHasData) {
            noData.classList.add('d-none');
        } else {
            noData.classList.remove('d-none');
        }
    }
}

/** Shared insight state — also referenced by query.js */
window._obInsights = window._obInsights || { loaded: false };

/**
 * Fetch and render triple store insights (standalone section on /dtwin/).
 */
async function loadInsights() {
    const loading = document.getElementById('insightSectionLoading');
    const content = document.getElementById('insightSectionContent');
    const errorEl = document.getElementById('insightSectionError');
    const noData  = document.getElementById('insightNoData');
    if (loading) loading.classList.remove('d-none');
    if (content) content.classList.add('d-none');
    if (errorEl) errorEl.classList.add('d-none');
    if (noData)  noData.classList.add('d-none');

    try {
        const resp = await fetch('/dtwin/sync/stats', { credentials: 'same-origin' });
        const data = await resp.json();
        if (!data.success) {
            if (noData) noData.classList.remove('d-none');
            if (errorEl) {
                errorEl.innerHTML = '<div class="alert alert-warning small mb-0 py-1 px-2">' +
                    '<i class="bi bi-exclamation-triangle me-1"></i>' + (data.message || 'Failed to load insights') +
                    '</div>';
                errorEl.classList.remove('d-none');
            }
            return;
        }
        renderInsights(data);
        window._obInsights.loaded = true;
        if (content) content.classList.remove('d-none');
    } catch (e) {
        console.error('[Insights] Error:', e);
        if (errorEl) {
            errorEl.innerHTML = '<div class="alert alert-danger small mb-0 py-1 px-2">' +
                '<i class="bi bi-x-circle me-1"></i>Error loading insights: ' + e.message + '</div>';
            errorEl.classList.remove('d-none');
        }
    } finally {
        if (loading) loading.classList.add('d-none');
    }
}

const INSIGHTS_PAGE_SIZE = 10;

let _insightsEntityPage = 0;
let _insightsEntityData = [];
let _insightsEntityTotal = 0;

let _insightsRelPage = 0;
let _insightsRelData = [];
let _insightsRelMax = 1;

/**
 * Build the insights UI from the stats data.
 */
function renderInsights(data) {
    const summaryRow = document.getElementById('insightsSummaryRow');
    if (summaryRow) {
        const cards = [
            { label: 'Total Triples', value: (data.total_triples || 0).toLocaleString(), icon: 'bi-stack', color: 'primary' },
            { label: 'Distinct Subjects', value: (data.distinct_subjects || 0).toLocaleString(), icon: 'bi-node-plus', color: 'success' },
            { label: 'Entity Types', value: (data.entity_types || []).length.toLocaleString(), icon: 'bi-diagram-3', color: 'info' },
            { label: 'Predicates', value: (data.distinct_predicates || 0).toLocaleString(), icon: 'bi-signpost-split', color: 'warning' },
            { label: 'Type Assertions', value: (data.type_assertion_count || 0).toLocaleString(), icon: 'bi-tag', color: 'secondary' },
            { label: 'Labels', value: (data.label_count || 0).toLocaleString(), icon: 'bi-fonts', color: 'dark' },
        ];
        summaryRow.innerHTML = cards.map(c =>
            '<div class="col-md-4 col-lg-2">' +
            '  <div class="ob-kpi-tile">' +
            '    <div class="ob-kpi-tile-icon text-' + c.color + '"><i class="bi ' + c.icon + '"></i></div>' +
            '    <div class="ob-kpi-tile-value">' + c.value + '</div>' +
            '    <div class="ob-kpi-tile-label">' + c.label + '</div>' +
            '  </div>' +
            '</div>'
        ).join('');
    }

    _insightsEntityData = data.entity_types || [];
    _insightsEntityTotal = _insightsEntityData.reduce((s, t) => s + t.count, 0);
    _insightsEntityPage = 0;
    _renderEntityPage();

    var allPreds = data.top_predicates || [];
    _insightsRelData = allPreds.filter(function (p) { return p.kind === 'relationship'; });
    _insightsRelMax = _insightsRelData.length > 0 ? _insightsRelData[0].count : 1;
    _insightsRelPage = 0;
    _renderRelPage();
}

function _renderEntityPage() {
    const div = document.getElementById('insightsEntityTypes');
    if (!div) return;
    const items = _insightsEntityData;
    if (items.length === 0) {
        div.innerHTML = '<span class="text-muted">No entity types found (no rdf:type assertions).</span>';
        return;
    }
    const start = _insightsEntityPage * INSIGHTS_PAGE_SIZE;
    const page = items.slice(start, start + INSIGHTS_PAGE_SIZE);
    const totalPages = Math.ceil(items.length / INSIGHTS_PAGE_SIZE);

    let html = '<table class="table table-sm table-hover mb-0"><thead><tr>' +
        '<th>Type</th><th class="text-end" style="width:80px;">Count</th>' +
        '<th style="width:140px;">Distribution</th></tr></thead><tbody>';
    page.forEach(t => {
        const label = _shortUri(t.uri);
        const pct = _insightsEntityTotal > 0 ? ((t.count / _insightsEntityTotal) * 100).toFixed(1) : 0;
        html += '<tr><td title="' + _escHtml(t.uri) + '"><code>' + _escHtml(label) + '</code></td>' +
            '<td class="text-end fw-semibold">' + t.count.toLocaleString() + '</td>' +
            '<td><div class="progress" style="height:14px;">' +
            '<div class="progress-bar bg-info" style="width:' + pct + '%"></div></div>' +
            '<small class="text-muted">' + pct + '%</small></td></tr>';
    });
    html += '</tbody></table>';
    html += _buildPager('entity', _insightsEntityPage, totalPages, items.length);
    div.innerHTML = html;
}

function _renderRelPage() {
    _renderPredTable('insightsRelationships', _insightsRelData, _insightsRelPage,
                     _insightsRelMax, 'rel', 'bg-primary', 'No relationships found.');
}

function _renderPredTable(divId, items, currentPage, maxCount, section, barClass, emptyMsg) {
    const div = document.getElementById(divId);
    if (!div) return;
    if (items.length === 0) {
        div.innerHTML = '<span class="text-muted">' + emptyMsg + '</span>';
        return;
    }
    const start = currentPage * INSIGHTS_PAGE_SIZE;
    const page = items.slice(start, start + INSIGHTS_PAGE_SIZE);
    const totalPages = Math.ceil(items.length / INSIGHTS_PAGE_SIZE);

    let html = '<table class="table table-sm table-hover mb-0"><thead><tr>' +
        '<th>Predicate</th><th class="text-end" style="width:80px;">Count</th>' +
        '<th style="width:140px;">Relative</th></tr></thead><tbody>';
    page.forEach(function (p) {
        const label = _shortUri(p.uri);
        const pct = maxCount > 0 ? ((p.count / maxCount) * 100).toFixed(1) : 0;
        html += '<tr><td title="' + _escHtml(p.uri) + '"><code>' + _escHtml(label) + '</code></td>' +
            '<td class="text-end fw-semibold">' + p.count.toLocaleString() + '</td>' +
            '<td><div class="progress" style="height:14px;">' +
            '<div class="progress-bar ' + barClass + '" style="width:' + pct + '%"></div></div></td></tr>';
    });
    html += '</tbody></table>';
    html += _buildPager(section, currentPage, totalPages, items.length);
    div.innerHTML = html;
}

function _buildPager(section, currentPage, totalPages, totalItems) {
    if (totalPages <= 1) return '';
    const start = currentPage * INSIGHTS_PAGE_SIZE + 1;
    const end = Math.min((currentPage + 1) * INSIGHTS_PAGE_SIZE, totalItems);
    let html = '<div class="d-flex justify-content-between align-items-center mt-2">' +
        '<small class="text-muted">' + start + '–' + end + ' of ' + totalItems + '</small>' +
        '<nav><ul class="pagination pagination-sm mb-0">';
    html += '<li class="page-item ' + (currentPage === 0 ? 'disabled' : '') + '">' +
        '<a class="page-link" href="#" onclick="_insightsGoTo(\'' + section + '\',' + (currentPage - 1) + ');return false;">&laquo;</a></li>';
    for (let i = 0; i < totalPages; i++) {
        html += '<li class="page-item ' + (i === currentPage ? 'active' : '') + '">' +
            '<a class="page-link" href="#" onclick="_insightsGoTo(\'' + section + '\',' + i + ');return false;">' + (i + 1) + '</a></li>';
    }
    html += '<li class="page-item ' + (currentPage >= totalPages - 1 ? 'disabled' : '') + '">' +
        '<a class="page-link" href="#" onclick="_insightsGoTo(\'' + section + '\',' + (currentPage + 1) + ');return false;">&raquo;</a></li>';
    html += '</ul></nav></div>';
    return html;
}

function _insightsGoTo(section, page) {
    if (section === 'entity') {
        const maxPage = Math.ceil(_insightsEntityData.length / INSIGHTS_PAGE_SIZE) - 1;
        _insightsEntityPage = Math.max(0, Math.min(page, maxPage));
        _renderEntityPage();
    } else if (section === 'rel') {
        const maxPage = Math.ceil(_insightsRelData.length / INSIGHTS_PAGE_SIZE) - 1;
        _insightsRelPage = Math.max(0, Math.min(page, maxPage));
        _renderRelPage();
    }
}

/** Shorten a full URI to a readable local name. */
function _shortUri(uri) {
    if (!uri) return '(unknown)';
    const hash = uri.lastIndexOf('#');
    if (hash >= 0) return uri.substring(hash + 1);
    const slash = uri.lastIndexOf('/');
    if (slash >= 0) return uri.substring(slash + 1);
    return uri;
}

/** Escape HTML special characters. */
function _escHtml(str) {
    const el = document.createElement('span');
    el.textContent = str;
    return el.innerHTML;
}

/**
 * Fetch artefact existence flags and render check / cross icons
 * next to each Digital Twin line.
 */
async function _loadDtExistence() {
    try {
        var resp = await fetch('/dtwin/sync/dt-existence', { credentials: 'same-origin' });
        var data = await resp.json();
        _applyDtExistence(data);
    } catch (e) {
        console.warn('[Sync] Could not load DT existence flags', e);
    }
}

/**
 * Download the Graph DB archive from the registry volume and restore it locally.
 */
async function reloadGraphFromRegistry() {
    var btn = document.getElementById('dtReloadFromRegistry');
    if (!btn) return;
    var origHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Reloading...';

    try {
        var resp = await fetch('/dtwin/sync/reload-from-registry', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        var data = await resp.json();
        if (data.success) {
            showNotification('Graph reloaded from registry', 'success');
            _loadDtExistence();
        } else {
            showNotification('Reload failed: ' + (data.message || 'Unknown error'), 'error');
        }
    } catch (e) {
        showNotification('Reload failed: ' + e.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = origHtml;
    }
}

/**
 * Drop the incremental snapshot table to force a full rebuild on next build.
 */
async function dropSnapshotTable() {
    var btn = document.getElementById('dtDropSnapshot');
    if (!btn) return;
    var origHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Dropping...';

    try {
        var resp = await fetch('/dtwin/sync/drop-snapshot', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        var data = await resp.json();
        if (data.success) {
            showNotification('Snapshot dropped — next build will be a full rebuild', 'success');
            _loadDtExistence();
        } else {
            showNotification('Drop failed: ' + (data.message || 'Unknown error'), 'error');
        }
    } catch (e) {
        showNotification('Drop failed: ' + e.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = origHtml;
    }
}

// Initialize on page load with a single consolidated API call
document.addEventListener('DOMContentLoaded', async function() {
    // Wire the standalone Insight section refresh button
    const insightRefreshBtn = document.getElementById('insightRefreshBtn');
    if (insightRefreshBtn) {
        insightRefreshBtn.addEventListener('click', function () {
            window._obInsights.loaded = false;
            loadInsights();
        });
    }

    const syncRoot = document.getElementById('sync-section');
    if (syncRoot) {
        syncRoot.addEventListener('click', function (e) {
            var btn = e.target.closest('[data-sync-action]');
            if (!btn || !syncRoot.contains(btn)) return;
            var act = btn.getAttribute('data-sync-action');
            if (act === 'refresh-triple-store') checkTripleStoreStatus(true);
            else if (act === 'start-triple-store-sync') startTripleStoreSync();
            else if (act === 'drop-snapshot-table') dropSnapshotTable();
            else if (act === 'reload-graph-from-registry') reloadGraphFromRegistry();
        });
    }

    // Re-load when the sync section becomes visible after navigating away and back
    const syncSection = document.getElementById('sync-section');
    if (syncSection) {
        const observer = new MutationObserver(() => {
            if (syncSection.classList.contains('active')) {
                initSyncSection();
            } else {
                _syncSectionLoaded = false;
            }
        });
        observer.observe(syncSection, { attributes: true, attributeFilter: ['class'] });

        if (syncSection.classList.contains('active')) {
            initSyncSection();
        }
    }
});
