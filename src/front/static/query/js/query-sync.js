/**
 * OntoBricks - query-sync.js
 * Triple Store Sync: generates all triples and writes them to a UC table.
 */

const ADJ_REFRESH_TASK_KEY = 'ontobricks_adjacency_refresh_task';

(function _bootstrapTriplestoreConfig() {
    if (window.__TRIPLESTORE_CONFIG) return;
    var el = document.getElementById('triplestore-config');
    if (!el) return;
    try { window.__TRIPLESTORE_CONFIG = JSON.parse(el.textContent || '{}'); }
    catch (_) { window.__TRIPLESTORE_CONFIG = {}; }
})();

function _syncTripleStoreBackend() {
    try {
        const el = document.getElementById('triplestore-config');
        if (!el) return 'lakebase';
        return JSON.parse(el.textContent || '{}').triple_store_backend || 'lakebase';
    } catch (_) {
        return 'lakebase';
    }
}

/** Whether ontology + assignments are both ready */
let syncIsReady = false;

let adjacencyRefreshRunning = false;

/** Whether the triple store table has data */
let tripleStoreHasData = false;

/**
 * Whether the last status probe could not reach the graph engine (`has_data`
 * came back null). A timeout is not evidence of a missing graph, so the badge
 * must say "status unavailable" rather than send the user off to rebuild a
 * graph they already have.
 */
let tripleStoreStatusUnknown = false;

/**
 * Whether `/dtwin/sync/info` served the graph status as a "pending" skeleton
 * (cache miss) so the page could paint without waiting on the ~4-8s COUNT(*)
 * probe. While true, the KG badges show a "checking" spinner and the real
 * state is confirmed by a background `checkTripleStoreStatus(true)` call.
 */
let tripleStoreStatusPending = false;


/**
 * Fetch all Knowledge Graph Overview data in a single round-trip and distribute
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

        // Cache miss: status was served as a non-blocking "pending" skeleton to
        // keep the ~4-8s COUNT(*) probe off the first paint. Show a "checking"
        // state and confirm the live count in the background — do NOT decide
        // hasData / unknown yet, or the badge would flash "Graph not built".
        // The rest of loadSyncInfo (rebuild warning, dt_existence, menus) still
        // runs below; only the status verdict is deferred.
        var statusPending = !!(payload.triplestore_status_pending || tsStatus.pending);
        tripleStoreStatusPending = statusPending;
        if (statusPending) {
            console.log('[Sync] Triplestore status pending — confirming in background');
            renderTripleStoreStatus({ success: true, has_data: null, pending: true });
            setTimeout(function () { checkTripleStoreStatus(true); }, 50);
        } else {
            tripleStoreStatusUnknown = !!(tsStatus.success && tsStatus.has_data === null);
            tripleStoreHasData = !!(tsStatus.success && tsStatus.has_data);
            console.log('[Sync] Consolidated triplestore status -> hasData:', tripleStoreHasData,
                tripleStoreStatusUnknown ? '(probe inconclusive)' : '');
            renderTripleStoreStatus(tsStatus);
        }

        // Guard against cache-staleness inconsistency: the shared cache timestamp
        // can make an old dt_existence entry (graph_has_data=true) appear fresh
        // even after a new status write sets has_data=false.  When the two signals
        // disagree (both ultimately check the same Postgres table), trust the
        // triplestore_status — it is a more targeted, recent check.
        //
        // Only a *definite* false may do this. A null has_data means the probe
        // never reached the engine, which is no reason to erase a graph that
        // dt_existence found; that asymmetry is what showed "Graph not built" on
        // Analytics while the Build page listed the graph as present.
        if (tsStatus.has_data === false && payload.dt_existence) {
            if (payload.dt_existence.graph_has_data) {
                console.warn(
                    '[Sync] dt_existence says Loaded but triplestore_status says no data ' +
                    '(cache staleness). Overriding graph_has_data to false.'
                );
                payload.dt_existence.graph_has_data = false;
            }
        }

        updateDataMenus();
        updateInsightsTab();
        return payload;
    } catch (e) {
        console.error('[Sync] Error loading consolidated sync info:', e);
        return null;
    }
}

/**
 * Apply readiness data obtained from the consolidated endpoint.
 */
function _applyReadiness(data) {
    syncIsReady = !!data.mapping_valid;
}

/** Compact badge spinner used while Build-page artefact probes are in flight. */
function _archSpinnerBadge(label) {
    return '<span class="badge bg-light text-muted border" style="font-size:.65rem;" role="status">' +
        '<span class="spinner-border spinner-border-sm me-1" style="width:.65rem;height:.65rem;border-width:.12em;" aria-hidden="true"></span>' +
        _escHtml(label || 'Loading') + '</span>';
}

/** Compact name-line spinner for unresolved Triple Store / Sync / Graph DB FQNs. */
function _archSpinnerName() {
    return '<span class="spinner-border spinner-border-sm text-muted" role="status" aria-label="Loading" ' +
        'style="width:.7rem;height:.7rem;border-width:.12em;"></span>';
}

/**
 * Toggle inline retrieval spinners on the Lakebase architecture cards.
 * Used before the live ``/dtwin/sync/dt-existence`` probe so badges/names do
 * not flash "Unable to check" / "—" while the probe is still running.
 */
function _setArchRetrievalLoading(on) {
    if (!on) return;
    var badgeIds = ['dtExistView', 'dtLakebaseSyncedUcExists', 'dtLakebaseTableExists'];
    for (var i = 0; i < badgeIds.length; i++) {
        var el = document.getElementById(badgeIds[i]);
        if (el) el.innerHTML = _archSpinnerBadge('Loading');
    }
    var nameIds = ['dtViewName', 'dtLakebaseSyncedUc', 'dtLakebaseFullName'];
    for (var j = 0; j < nameIds.length; j++) {
        var nameEl = document.getElementById(nameIds[j]);
        if (!nameEl) continue;
        var cur = (nameEl.textContent || '').trim();
        if (!cur || cur === '—' || cur === 'N/A' || cur === 'Not configured' || nameEl.querySelector('.spinner-border')) {
            nameEl.innerHTML = _archSpinnerName();
        }
    }
}

/**
 * Build page: labels and options depend on the per-domain Graph DB engine.
 */
function _setBackendBrandIcon(element, backend) {
    if (!element) return;
    element.classList.remove(
        'ob-icon-postgresql',
        'ob-icon-lakehouse',
        'ob-icon-databricks',
        'ob-icon-neo4j',
        'd-none'
    );
    element.classList.add(_backendBrandIconClass(backend));
}

function _applyBuildGraphEngineUi(dtExist) {
    var dt = dtExist || {};
    var cfg = window.__TRIPLESTORE_CONFIG || {};
    // The Original Graph page logo is always Lakehouse (applyTripleStoreBackendPanels).
    var eng = dt.graph_engine || cfg.graph_engine || 'lakebase';
    var pending = dt.pending === true;
    cfg.graph_engine = eng;
    window.__TRIPLESTORE_CONFIG = cfg;

    // (footnote moved into card)
    var fnLk = document.getElementById('graphEngineFootnoteLakebase');
    if (fnLk) fnLk.classList.remove('d-none');

    var title = document.getElementById('dtGraphBackendTitle');
    var labels = {
        'lakebase': 'Graph DB (Lakebase)',
        'databricks': 'Graph DB (Lakehouse)',
        'delta': 'Graph DB (Lakehouse)',
        'neo4j': 'Graph DB (Neo4j)'
    };
    if (title) {
        title.textContent = labels[eng] || 'Graph DB Digital Twin';
    }
    // Toggle the post-Triple-Store cards (Sync + Graph DB) based on engine.
    // The `dtLakebaseDetails` container wraps both the Sync card and the
    // Graph DB card. On Lakebase, both show. On Neo4j, the Sync card is
    // hidden (no UC-synced table on the Neo4j path) but the Graph DB card
    // remains visible with engine-aware label and metadata.
    function _renderEngineUi(activeEng) {
        var container = document.getElementById('dtLakebaseDetails');
        var titleEl   = document.getElementById('dtGraphBackendTitle');
        var graphIcon = document.getElementById('dtGraphBackendIcon');
        var syncRow   = document.getElementById('dtLakebaseSyncedUcRow');
        var boltRow   = document.getElementById('dtNeo4jBoltCard');
        var lkBuild   = document.getElementById('dtLakebaseBuildNote');
        var graphFn   = document.getElementById('dtLakebaseFullName');
        if (container) container.classList.remove('d-none');
        if (titleEl)   titleEl.textContent = labels[activeEng] || ('Graph DB (' + activeEng + ')');
        _setBackendBrandIcon(graphIcon, activeEng);
        if (activeEng === 'neo4j') {
            // Show the Bolt writer card and hide Lakebase-only details.
            if (syncRow) syncRow.classList.add('d-none');
            if (boltRow) boltRow.classList.remove('d-none');
            if (lkBuild) lkBuild.classList.add('d-none');
            // Neo4j Graph DB card: show the marker label (or FQN display) and a
            // Neo4j-specific build note. No Postgres/UC concepts apply here.
            if (graphFn) {
                var neoName = dt.graph_display
                    || dt.neo4j_marker_label
                    || cfg.graph_name
                    || 'Knowledge Graph';
                if (dt.neo4j_graph_exists == null && dt.pending) {
                    graphFn.innerHTML = _archSpinnerName();
                } else {
                    graphFn.textContent = neoName;
                }
            }
            var neoBuild = document.getElementById('dtNeo4jBuildNote');
            if (neoBuild) {
                neoBuild.classList.remove('d-none');
                var neoDbEl = document.getElementById('fnNeo4jDatabase');
                var neoLblEl = document.getElementById('fnNeo4jLabel');
                if (neoDbEl) neoDbEl.textContent = dt.neo4j_database || cfg.neo4j_database || 'neo4j';
                if (neoLblEl) neoLblEl.textContent = dt.neo4j_marker_label || cfg.graph_name || '…';
            }
        } else {
            if (syncRow) syncRow.classList.remove('d-none');
            if (boltRow) boltRow.classList.add('d-none');
            if (lkBuild) lkBuild.classList.remove('d-none');
            var neoBuild2 = document.getElementById('dtNeo4jBuildNote');
            if (neoBuild2) neoBuild2.classList.add('d-none');
        }
    }
    _renderEngineUi(eng);
    var sub = document.getElementById('dtGraphStorageSubtitle');
    var primaryRow = document.getElementById('dtGraphPrimaryRow');
    if (sub) sub.classList.add('d-none');
    if (primaryRow) primaryRow.classList.add('d-none');
    var regRow = document.getElementById('dtRegistryArchiveRow');
    if (regRow) regRow.classList.add('d-none');

    if (eng === 'lakebase') {
        var lkDb  = document.getElementById('dtLakebaseDatabase');
        var lkSch = document.getElementById('dtLakebaseSchema');
        var lkTbl = document.getElementById('dtLakebaseTable');
        var lkUc    = document.getElementById('dtLakebaseSyncedUc');
        if (lkDb)  lkDb.textContent  = dt.lakebase_database || '—';
        if (lkSch) lkSch.textContent = dt.lakebase_schema   || '—';
        if (lkTbl) lkTbl.textContent = dt.lakebase_table    || '—';
        var lkFullName = document.getElementById('dtLakebaseFullName');
        var db = dt.lakebase_database || '', sch = dt.lakebase_schema || '', tbl = dt.lakebase_table || '';
        var fullName = '';
        if (db && sch && tbl) fullName = db + '.' + sch + '.' + tbl;
        else if (sch && tbl) fullName = sch + '.' + tbl;
        else fullName = db || sch || tbl || '';
        if (lkFullName) {
            if (fullName) lkFullName.textContent = fullName;
            else if (pending) lkFullName.innerHTML = _archSpinnerName();
            else lkFullName.textContent = '—';
        }
        var hasUcName = !!(dt.lakebase_synced_uc);
        if (lkUc) {
            if (hasUcName) lkUc.textContent = dt.lakebase_synced_uc;
            else if (pending) lkUc.innerHTML = _archSpinnerName();
            else lkUc.textContent = '—';
        }

        // existence badges for table and UC sync
        var tblExistsEl = document.getElementById('dtLakebaseTableExists');
        if (tblExistsEl) {
            if (dt.lakebase_table_exists === true) {
                tblExistsEl.innerHTML = '<span class="badge bg-success-subtle text-success-emphasis border border-success-subtle" style="font-size:.65rem;"><i class="bi bi-check-circle-fill text-success me-1"></i>Exists</span>';
            } else if (dt.lakebase_table_exists === false) {
                tblExistsEl.innerHTML = '<span class="badge bg-secondary-subtle text-secondary-emphasis border" style="font-size:.65rem;"><i class="bi bi-dash-circle me-1"></i>Not found</span>';
            } else if (pending) {
                tblExistsEl.innerHTML = _archSpinnerBadge('Loading');
            } else {
                tblExistsEl.innerHTML = '<span class="badge bg-warning-subtle text-warning-emphasis border border-warning-subtle" style="font-size:.65rem;"><i class="bi bi-question-circle me-1"></i>Unable to check</span>';
                var sp = tblExistsEl.querySelector('span');
                if (sp) sp.title = dt.lakebase_check_error
                    ? String(dt.lakebase_check_error)
                    : 'Could not reach Lakebase Postgres to verify the triple table.';
            }
        }
        var ucExistsEl = document.getElementById('dtLakebaseSyncedUcExists');
        if (ucExistsEl) {
            if (dt.lakebase_synced_uc_exists === true) {
                ucExistsEl.innerHTML = '<span class="badge bg-success-subtle text-success-emphasis border border-success-subtle" style="font-size:.65rem;"><i class="bi bi-check-circle-fill text-success me-1"></i>Exists</span>';
            } else if (dt.lakebase_synced_uc_exists === false) {
                ucExistsEl.innerHTML = '<span class="badge bg-secondary-subtle text-secondary-emphasis border" style="font-size:.65rem;"><i class="bi bi-dash-circle me-1"></i>Not found</span>';
            } else if (pending) {
                ucExistsEl.innerHTML = _archSpinnerBadge('Loading');
            } else if (hasUcName) {
                ucExistsEl.innerHTML = '<span class="badge bg-warning-subtle text-warning-emphasis border border-warning-subtle" style="font-size:.65rem;" title="Could not verify whether the UC sync table exists."><i class="bi bi-question-circle me-1"></i>Unable to check</span>';
            } else {
                ucExistsEl.innerHTML = '<span class="badge bg-secondary-subtle text-secondary-emphasis border" style="font-size:.65rem;"><i class="bi bi-dash-circle me-1"></i>Not found</span>';
            }
        }

        // in-card build note (replaces footnote below the card)
        var buildNote = document.getElementById('dtLakebaseBuildNote');
        if (buildNote) {
            var fn2Db  = document.getElementById('fnLkDatabase2');
            var fn2Sch = document.getElementById('fnLkSchema2');
            var fn2Tbl = document.getElementById('fnLkTable2');
            var fn2Uc  = document.getElementById('fnLkSyncedUc2');
            var fn2Sync = document.getElementById('fnLkSyncNote');
            if (fn2Db)  fn2Db.textContent  = dt.lakebase_database || '…';
            if (fn2Sch) fn2Sch.textContent = dt.lakebase_schema   || '…';
            if (fn2Tbl) fn2Tbl.textContent = dt.lakebase_table    || '…';
            if (fn2Sync) fn2Sync.classList.toggle('d-none', !hasUcName);
            if (fn2Uc && hasUcName) fn2Uc.textContent = dt.lakebase_synced_uc;
            buildNote.style.display = '';
        }
    }
}

function _applyDtExistence(data) {
    function _badge(flag, okText, failText, unknownText) {
        var s = 'style="font-size:.65rem;"';
        if (flag === true)
            return '<span class="badge bg-success-subtle text-success-emphasis border border-success-subtle" ' + s + '><i class="bi bi-check-circle-fill text-success me-1"></i>' + okText + '</span>';
        if (flag === false)
            return '<span class="badge bg-secondary-subtle text-secondary-emphasis border" ' + s + '><i class="bi bi-dash-circle me-1"></i>' + failText + '</span>';
        return '<span class="badge bg-secondary-subtle text-secondary-emphasis border" ' + s + '><i class="bi bi-dash-circle me-1"></i>' + (unknownText || 'N/A') + '</span>';
    }

    var pending = data && data.pending === true;
    var viewIsNeo4j = (data.graph_engine === 'neo4j')
        || ((window.__TRIPLESTORE_CONFIG || {}).graph_engine === 'neo4j');
    var viewEl = document.getElementById('dtExistView');
    if (viewEl) {
        if (viewIsNeo4j) {
            // The SQL VIEW is not part of the Neo4j build path (triples go
            // straight to the graph over Bolt) — show N/A, never a stuck spinner.
            viewEl.innerHTML = _badge(null, 'Exists', 'Not found', 'N/A');
            viewEl.title = 'Not used on the Neo4j path — the graph is written directly over Bolt.';
        } else if (pending || (data.view_exists == null && !data.view_check_error && data.view_table)) {
            viewEl.innerHTML = _archSpinnerBadge('Loading');
            viewEl.title = '';
        } else {
            viewEl.innerHTML = _badge(data.view_exists, 'Exists', 'Not found', 'Not configured');
            if (data.view_check_error) viewEl.title = data.view_check_error;
            else if (data.view_table) viewEl.title = 'Queried: ' + data.view_table;
            else viewEl.title = '';
        }
    }

    var viewNameEl = document.getElementById('dtViewName');
    if (viewNameEl) {
        if (data.view_table) viewNameEl.textContent = data.view_table;
        else if (pending) viewNameEl.innerHTML = _archSpinnerName();
        else viewNameEl.textContent = 'Not configured';
    }

    var zcCard = document.getElementById('dtZeroCopyCard');
    if (zcCard) {
        zcCard.classList.remove('border-success', 'border-danger');
        if (data.view_exists === true) zcCard.classList.add('border-success');
        else if (data.view_exists === false) zcCard.classList.add('border-danger');
    }

    var isNeo4j = (data.graph_engine === 'neo4j')
        || ((window.__TRIPLESTORE_CONFIG || {}).graph_engine === 'neo4j');
    // Graph DB existence flag is engine-specific: Neo4j marker-label presence
    // vs Lakebase Postgres table presence.
    var graphExists = isNeo4j ? data.neo4j_graph_exists : data.lakebase_table_exists;

    var graphCard = document.getElementById('dtGraphCard');
    if (graphCard) {
        graphCard.classList.remove('border-success', 'border-danger');
        if (graphExists === true) graphCard.classList.add('border-success');
        else if (graphExists === false) graphCard.classList.add('border-danger');
    }

    // Neo4j: paint the Graph DB card's existence badge (Lakebase paints its own
    // #dtLakebaseTableExists inside _applyBuildGraphEngineUi).
    if (isNeo4j) {
        var neoBadge = document.getElementById('dtLakebaseTableExists');
        if (neoBadge) {
            if (pending && graphExists == null && !data.neo4j_check_error) {
                neoBadge.innerHTML = _archSpinnerBadge('Loading');
            } else if (graphExists === true) {
                neoBadge.innerHTML = _badge(true, 'Exists', 'Not found', 'N/A');
            } else if (graphExists === false) {
                neoBadge.innerHTML = _badge(false, 'Exists', 'Not built', 'N/A');
            } else {
                neoBadge.innerHTML = '<span class="badge bg-warning bg-opacity-10 text-warning border border-warning" style="font-size:.65rem;"><i class="bi bi-question-circle me-1"></i>Unable to check</span>';
                var nsp = neoBadge.querySelector('span');
                if (nsp) nsp.title = data.neo4j_check_error
                    ? String(data.neo4j_check_error)
                    : 'Could not reach Neo4j to verify the graph.';
            }
        }
    }

    // Global info: last update & last built
    var lastUpdateEl = document.getElementById('dtLastUpdate');
    if (lastUpdateEl) {
        if (data.last_update) {
            try {
                var fmtU = formatDateTime(data.last_update);
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
                var formatted = formatDateTime(data.last_built);
                lastBuiltEl.innerHTML = '<i class="bi bi-check-circle-fill text-success me-1"></i>' + formatted;
                lastBuiltEl.className = '';
            } catch (_) { lastBuiltEl.textContent = data.last_built; lastBuiltEl.className = ''; }
        } else {
            lastBuiltEl.innerHTML = '<span class="text-muted">Never built</span>';
        }
    }
}

/**
 * Paint Graph cache status on the Build tab (Activated / State / Last refresh).
 * Accepts either domain_info.info fields or the richer graph_cache block from
 * /dtwin/sync/stats.
 */
/** Paint the Activated / State / Cache updated cells of one Cache pane (*prefix* orig | cert). */
function renderBuildGraphCacheStatus(source, prefix) {
    var activatedEl = document.getElementById(prefix + 'CacheActivated');
    var stateEl = document.getElementById(prefix + 'CacheState');
    var refreshedEl = document.getElementById(prefix + 'CacheRefreshed');
    if (!activatedEl || !stateEl || !refreshedEl) return;

    var cache = source || {};
    // domain_info uses graph_cache_*; stats uses graph_cache.{enabled,state,refreshed_at}
    var supported = cache.supported;
    if (supported === undefined) {
        var backend = String(cache.graph_backend || cache.backend || '').toLowerCase();
        supported = !backend || backend === 'lakebase' || backend === 'databricks';
    }
    var enabled = cache.enabled;
    if (enabled === undefined) enabled = cache.graph_cache_enabled;
    var state = cache.state || cache.graph_cache_state || '';
    // Explicit refresh stamp, else the Build that created the companions.
    var refreshedAt = cache.refreshed_at
        || cache.graph_cache_refreshed_at
        || cache.last_built
        || cache.last_build
        || '';

    if (!supported) {
        activatedEl.innerHTML = '<span class="text-muted">Not available</span>';
        stateEl.innerHTML = '<span class="text-muted">—</span>';
        refreshedEl.innerHTML = '<span class="text-muted">—</span>';
        return;
    }

    if (enabled) {
        activatedEl.innerHTML =
            '<span class="badge bg-success bg-opacity-10 text-success border border-success">' +
            '<i class="bi bi-check-circle me-1"></i>On</span>';
    } else {
        activatedEl.innerHTML =
            '<span class="badge bg-secondary bg-opacity-10 text-secondary border border-secondary">' +
            '<i class="bi bi-slash-circle me-1"></i>Off</span>';
    }

    var stateKey = String(state || '').toLowerCase();
    if (!enabled || stateKey === 'disabled') {
        stateEl.innerHTML = '<span class="text-muted">Disabled</span>';
    } else if (stateKey === 'ready') {
        stateEl.innerHTML =
            '<span class="badge bg-success bg-opacity-10 text-success border border-success">' +
            '<i class="bi bi-check-circle me-1"></i>Ready</span>';
    } else {
        stateEl.innerHTML =
            '<span class="badge bg-warning bg-opacity-10 text-warning border border-warning">' +
            '<i class="bi bi-exclamation-triangle me-1"></i>Refresh required</span>';
    }

    if (refreshedAt) {
        try {
            refreshedEl.innerHTML =
                '<i class="bi bi-check-circle-fill text-success me-1"></i>' +
                formatDateTime(refreshedAt);
            refreshedEl.className = 'ms-1';
        } catch (_) {
            refreshedEl.textContent = refreshedAt;
            refreshedEl.className = 'ms-1';
        }
    } else {
        // No dedicated stamp and no Build yet — keep a dash.
        refreshedEl.innerHTML = '<span class="text-muted">—</span>';
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
    if (_syncTripleStoreBackend() === 'databricks') return;
    if (_syncSectionLoaded) return;
    _syncSectionLoaded = true;

    var overlay = document.getElementById('syncLoadingOverlay');
    if (overlay) overlay.classList.remove('d-none');

    try {
        _applyBuildGraphEngineUi(window.__TRIPLESTORE_CONFIG || {});

        var payload = await loadSyncInfo();

        var cfg = window.__TRIPLESTORE_CONFIG || {};

        const di = payload && (payload.domain_info || payload.project_info);
        if (di && di.success && di.info) {
            // Prefer the authoritative per-domain backend from /domain/info
            // (``graph_backend``: lakebase | databricks | neo4j) over whatever the
            // pre-existence seed left in cfg — otherwise a Neo4j domain paints as
            // Lakebase until (and unless) dt_existence later corrects it.
            const backend = di.info.graph_backend || di.info.graph_engine || '';
            const prevEng = backend || cfg.graph_engine || 'lakebase';
            cfg = {
                view_table: di.info.view_table || '',
                graph_name: di.info.graph_name || '',
                graph_engine: prevEng,
                neo4j_database: di.info.neo4j_database || '',
                cache: {},
            };
            window.__TRIPLESTORE_CONFIG = cfg;
            var cacheSource = Object.assign({}, di.info);
            if (payload && payload.dt_existence && payload.dt_existence.last_built) {
                cacheSource.last_built = payload.dt_existence.last_built;
            }
            renderBuildGraphCacheStatus(cacheSource, 'cert');
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
            _applyBuildGraphEngineUi(payload.dt_existence);
            _applyDtExistence(payload.dt_existence);

            // Existence is served cache-first (or as a pending skeleton) for an
            // instant paint. Confirm the live Lakebase/UC state in the
            // background so a cold SQL-warehouse / Lakebase wake-up never blocks
            // the page. Fire-and-forget: it silently updates the badges in place.
            if (payload.dt_existence_pending && typeof _loadDtExistence === 'function') {
                setTimeout(function () { _loadDtExistence(); }, 50);
            }
        }

        var targetLabel = document.getElementById('syncTargetLabel');
        if (targetLabel) {
            targetLabel.innerHTML = '<i class="bi bi-lightning-charge me-1"></i>Graph';
        }

        if (typeof refreshNavbarIndicators === 'function') refreshNavbarIndicators();

        await checkAndResumeAdjacencyRefreshTask();
    } finally {
        if (overlay) overlay.classList.add('d-none');
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
        // The live probe has answered — clear the "pending" (background-confirm)
        // state set by loadSyncInfo's cache-miss skeleton.
        tripleStoreStatusPending = false;
        tripleStoreStatusUnknown = !!(data.success && data.has_data === null);
        tripleStoreHasData = !!(data.success && data.has_data);
        console.log('[Sync] Triple store status:', data, '-> hasData:', tripleStoreHasData);
        renderTripleStoreStatus(data);
    } catch (e) {
        console.error('[Sync] Error checking triple store status:', e);
        // The request itself failed, so the graph's state is equally unknown.
        tripleStoreStatusPending = false;
        tripleStoreStatusUnknown = true;
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

    // Pending: status served as a non-blocking skeleton; the live count is
    // being confirmed in the background. Show a spinner instead of a verdict.
    if (data && data.pending) {
        area.innerHTML =
            '<div class="small mb-0 py-1 px-2 text-muted">' +
            '<span class="spinner-border spinner-border-sm me-1" style="width:.8rem;height:.8rem;"></span>' +
            'Checking graph status…</div>';
        return;
    }

    if (!data || !data.success) {
        // Error checking status (e.g. Databricks not configured)
        const msg = (data && data.message) ? data.message : 'Unable to check triple store status';
        area.innerHTML =
            '<div class="alert alert-warning small mb-0 py-1 px-2">' +
            '<i class="bi bi-exclamation-triangle me-1"></i>' + msg +
            '</div>';
        return;
    }

    if (data.has_data === null) {
        // The probe never reached the engine, so neither "not built" nor "empty"
        // is a claim we can make — say so instead of guessing at a cause.
        const detail = data.graph_check_error ? ' (' + data.graph_check_error + ')' : '';
        area.innerHTML =
            '<div class="alert alert-warning small mb-0 py-1 px-2">' +
            '<i class="bi bi-question-circle me-1"></i>' +
            'Could not check the graph status' + detail +
            ' — a connection problem, not a missing graph.' +
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
            msg = 'Certified Graph not built yet. Run <strong>Build</strong> from the <a href="/dtwin/?section=sync">Original Graph</a> page.';
        } else if (reason.toLowerCase().includes('not configured')) {
            msg = 'Knowledge Graph is not configured. Set it in <a href="/domain/#information">Context Settings</a>.';
        } else {
            msg = 'Certified Graph is empty. Run <strong>Build</strong> from the <a href="/dtwin/?section=sync">Original Graph</a> page.';
        }
        area.innerHTML =
            '<div class="alert alert-warning small mb-0 py-1 px-2">' +
            '<i class="bi bi-exclamation-triangle me-1"></i>' + msg +
            '</div>';
    }
}

/** Certified Graph cache refresh: its own Delta companions (Lakehouse) or the Lakebase ones. */
function _syncAdjacencyRefreshVisible(backend) {
    return backend === 'lakebase' || backend === 'databricks';
}

/** Whether the Certified Graph has data to index (panel probe, or a successful build). */
function _certifiedHasData() {
    return tripleStoreHasData || window.__certifiedBuilt === true;
}

function _updateSyncAdjacencyActionState() {
    var backend = String(_syncTripleStoreBackend() || 'lakebase').toLowerCase();
    var visible = _syncAdjacencyRefreshVisible(backend);
    var disabled = !visible
        || !_certifiedHasData()
        || adjacencyRefreshRunning;
    var btn = document.getElementById('syncAdjacencyRefreshBtn');
    if (btn) {
        btn.classList.toggle('d-none', !visible);
        btn.disabled = disabled;
    }
    // Header kebab item (Lakebase): keep enabled/disabled in sync; never hide —
    // the parent action group is already backend-gated.
    document.querySelectorAll(
        '#sync-lakebase-actions .js-build-menu-refresh-cache'
    ).forEach(function (item) {
        item.disabled = disabled;
        item.classList.toggle('disabled', disabled);
    });
}

/**
 * Enable or disable the Data section sidebar menus (Quality, Triples, Graph Viewer).
 *
 * Menus are enabled ONLY when ALL conditions are met:
 *   1. Ontology + assignments are ready (syncIsReady)
 *   2. No cache refresh is running (!adjacencyRefreshRunning)
 *   3. The triple store has data (tripleStoreHasData)
 */
function updateDataMenus() {
    const canAccess = syncIsReady && !adjacencyRefreshRunning && tripleStoreHasData;
    console.log('[Sync] updateDataMenus: syncIsReady=' + syncIsReady +
                ', adjacencyRefreshRunning=' + adjacencyRefreshRunning +
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
            if (adjacencyRefreshRunning) {
                link.setAttribute('title', 'Adjacency refresh in progress…');
            } else if (!syncIsReady) {
                link.setAttribute('title', 'Ontology and mapping assignments must be configured first');
            } else if (tripleStoreStatusPending) {
                link.setAttribute('title', 'Checking graph status…');
            } else if (!tripleStoreHasData) {
                link.setAttribute('title', 'Triple store is empty — run Sync first');
            }
        }
    });

    updateKgReadyIndicators();
    _updateSyncAdjacencyActionState();
}

/**
 * Populate every Knowledge Graph readiness indicator (the `[data-kg-status]`
 * spans injected next to each sub-page title via `_kg_status_indicator.html`)
 * and the blocking gate card for Graph / Intelligence pages that need a
 * built graph.
 *
 * Badge states, driven by the shared triple-store status:
 *   - building  -> a "Building…" badge while a sync/build task is running;
 *   - ready     -> a green "Graph ready" badge once the graph has data;
 *   - not built -> a message + a "Go to Build" button that jumps to the
 *                  Build sub-page.
 *
 * When the graph is definitely not built, each KG sub-page also shows a
 * blocking empty-state card and hides its body content (Explorer, Query,
 * Chat, Data Quality, Business rules, and Intelligence menus).
 */
/**
 * Friendly label for the triple-store backend / graph engine in use, from the
 * injected triplestore-config: "Lakehouse" (Delta), "Neo4j", or "Lakebase".
 */
function _kgBackendKey() {
    var cfg = window.__TRIPLESTORE_CONFIG || {};
    var backend = String(cfg.triple_store_backend || 'lakebase').toLowerCase();
    var engine = String(cfg.graph_engine || 'lakebase').toLowerCase();
    if (backend === 'databricks' || engine === 'delta') return 'databricks';
    if (backend === 'neo4j' || engine === 'neo4j') return 'neo4j';
    return 'lakebase';
}

function _backendBrandIconClass(backend) {
    var key = String(backend || 'lakebase').toLowerCase();
    if (key === 'databricks' || key === 'delta' || key === 'lakehouse') {
        return 'ob-icon-lakehouse';
    }
    if (key === 'neo4j') return 'ob-icon-neo4j';
    return 'ob-icon-postgresql';
}

function _kgBackendLabel() {
    var backend = _kgBackendKey();
    if (backend === 'databricks') return 'Lakehouse';
    if (backend === 'neo4j') return 'Neo4j';
    return 'Lakebase';
}

function _kgBackendIconMarkup() {
    return '<i class="ob-brand-icon ' + _backendBrandIconClass(_kgBackendKey())
        + ' ms-1 me-1" aria-hidden="true"></i>';
}

function updateKgReadyIndicators() {
    var els = document.querySelectorAll('[data-kg-status]');
    if (!els.length) return;

    var backend = _kgBackendLabel();
    var html;
    if (tripleStoreStatusPending) {
        // Status probe deferred off the first paint; confirming in background.
        html = '<span class="badge bg-secondary-subtle text-secondary-emphasis border fw-medium" '
            + 'title="Confirming the graph status in the background…">'
            + '<span class="spinner-border spinner-border-sm me-1 kg-status-spinner"></span>Checking…</span>';
    } else if (tripleStoreHasData) {
        html = '<span class="badge bg-success-subtle text-success-emphasis border border-success-subtle fw-medium" '
            + 'title="The Knowledge Graph is built and ready to use (backend: ' + backend + ').">'
            + '<i class="bi bi-check-circle-fill text-success me-1"></i>Graph ready'
            + '<span class="ms-1">·' + _kgBackendIconMarkup() + backend + '</span></span>';
    } else if (tripleStoreStatusUnknown) {
        // The probe could not reach the engine, so we do not know either way.
        // Offering "Go to Build" here would invite a needless rebuild.
        html = '<span class="badge bg-warning-subtle text-warning-emphasis border border-warning-subtle fw-medium" '
            + 'title="Could not reach the ' + backend + ' backend to check the graph. '
            + 'This is a connection problem, not a missing graph — retry in a moment.">'
            + '<i class="bi bi-question-circle me-1"></i>Status unavailable'
            + '<span class="ms-1">·' + _kgBackendIconMarkup() + backend + '</span></span>';
    } else {
        html = '<span class="kg-not-built d-inline-flex align-items-center gap-2">'
            + '<span class="badge bg-secondary-subtle text-secondary-emphasis border fw-medium">'
            + '<i class="bi bi-slash-circle me-1"></i>Graph not built</span>'
            + '<button type="button" class="btn btn-sm btn-outline-secondary py-0 px-2 kg-go-build-btn">'
            + '<i class="bi bi-hammer me-1"></i>Go to Build</button>'
            + '</span>';
    }

    els.forEach(function(el) { el.innerHTML = html; });
    updateKgGates();
}

/**
 * Whether KG pages that need triples should block interaction.
 * Only a definite empty graph blocks — building / checking / unreachable
 * keep the page usable (the title badge already covers those states).
 */
function _kgShouldBlockPage() {
    return !tripleStoreStatusPending
        && !tripleStoreHasData
        && !tripleStoreStatusUnknown;
}

function _kgGateRoot(badge) {
    return badge.closest('.content-section') || badge.closest('.sidebar-section');
}

function _ensureKgGate(header) {
    var el = header.nextElementSibling;
    while (el) {
        if (el.hasAttribute && el.hasAttribute('data-kg-gate')) return el;
        el = el.nextElementSibling;
    }
    var gate = document.createElement('div');
    gate.setAttribute('data-kg-gate', '');
    gate.className = 'card kg-gate-card mb-3 d-none';
    gate.setAttribute('role', 'status');
    gate.setAttribute('aria-live', 'polite');
    header.insertAdjacentElement('afterend', gate);
    return gate;
}

function _kgGateBodyHtml() {
    return ''
        + '<div class="card-body text-center py-5 px-4">'
        +   '<i class="bi bi-exclamation-triangle text-warning kg-gate-icon d-block mb-3" aria-hidden="true"></i>'
        +   '<h5 class="fw-semibold mb-2">Knowledge Graph not ready</h5>'
        +   '<p class="text-muted mb-3 kg-gate-copy">'
        +     'This page needs a built Knowledge Graph. Run <strong>Build</strong> to '
        +     'materialise mapped triples, then return here.'
        +   '</p>'
        +   '<button type="button" class="btn btn-sm btn-primary kg-go-build-btn">'
        +     '<i class="bi bi-hammer me-1"></i>Go to Build'
        +   '</button>'
        + '</div>';
}

/**
 * Show / hide the blocking empty-state card on every KG sub-page that
 * carries a readiness badge. Body siblings after the section header are
 * hidden while blocked; Bootstrap modals stay in the DOM (not hidden) so
 * they keep working once the graph is ready.
 */
function updateKgGates() {
    var blocked = _kgShouldBlockPage();
    var seen = [];
    document.querySelectorAll('[data-kg-status]').forEach(function(badge) {
        var root = _kgGateRoot(badge);
        if (!root || seen.indexOf(root) !== -1) return;
        seen.push(root);

        var header = root.querySelector('.section-header');
        if (!header) return;

        var gate = _ensureKgGate(header);
        root.classList.toggle('kg-gate-blocked', blocked);
        if (blocked) {
            gate.innerHTML = _kgGateBodyHtml();
            gate.classList.remove('d-none');
        } else {
            gate.classList.add('d-none');
            gate.innerHTML = '';
        }

        var sib = gate.nextElementSibling;
        while (sib) {
            // Keep Bootstrap modals mounted so they can open once the graph
            // is ready (hiding an ancestor with d-none breaks show()).
            if (!_kgSiblingIsModalHost(sib)) {
                // Dedicated class — do not reuse Bootstrap `d-none`, which
                // page scripts already toggle for their own empty/loading panes.
                sib.classList.toggle('kg-gate-hidden', blocked);
                if (blocked) sib.setAttribute('aria-hidden', 'true');
                else sib.removeAttribute('aria-hidden');
            }
            sib = sib.nextElementSibling;
        }
    });
}

function _kgSiblingIsModalHost(sib) {
    if (!sib || !sib.classList) return false;
    if (sib.classList.contains('modal')) return true;
    return !!(sib.children
        && sib.children.length === 1
        && sib.children[0].classList
        && sib.children[0].classList.contains('modal'));
}

// One-time delegated handler: any "Go to Build" button jumps to the Build
// sub-page by triggering its sidebar nav link (data-section="sync").
if (!window.__kgGoBuildWired) {
    window.__kgGoBuildWired = true;
    document.addEventListener('click', function(e) {
        var btn = e.target.closest && e.target.closest('.kg-go-build-btn');
        if (!btn) return;
        e.preventDefault();
        var link = document.querySelector('[data-section="sync"]');
        if (link) {
            link.click();
            return;
        }
        window.location.href = '/dtwin/?section=sync';
    });
}

async function checkAndResumeAdjacencyRefreshTask() {
    const taskId = sessionStorage.getItem(ADJ_REFRESH_TASK_KEY);
    if (!taskId) return;
    try {
        const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
        const data = await response.json();
        if (!data.success || !data.task) {
            sessionStorage.removeItem(ADJ_REFRESH_TASK_KEY);
            return;
        }
        const task = data.task;
        if (task.status === 'running' || task.status === 'pending') {
            adjacencyRefreshRunning = true;
            showSyncProgress();
            updateSyncProgress(task.progress || 0, task.current_step_description || task.message || '');
            updateDataMenus();
            monitorAdjacencyRefreshTask(taskId);
            return;
        }
    } catch (_) {
        // best effort resume only
    }
    sessionStorage.removeItem(ADJ_REFRESH_TASK_KEY);
}

async function startSyncAdjacencyRefresh() {
    const backend = String(_syncTripleStoreBackend() || 'lakebase').toLowerCase();
    if (!_syncAdjacencyRefreshVisible(backend)) {
        return;
    }
    if (!_certifiedHasData() || adjacencyRefreshRunning) {
        return;
    }

    adjacencyRefreshRunning = true;
    updateDataMenus();
    showSyncProgress();
    updateSyncProgress(0, 'Starting adjacency refresh...');

    try {
        const response = await fetch('/dtwin/adjacency/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ source: 'certified' }),
            credentials: 'same-origin',
        });
        const data = await response.json();
        if (!response.ok || !data.success || !data.task_id) {
            throw new Error(data.message || data.detail || 'Adjacency refresh failed to start');
        }
        sessionStorage.setItem(ADJ_REFRESH_TASK_KEY, data.task_id);
        if (typeof refreshTasks === 'function') refreshTasks();
        monitorAdjacencyRefreshTask(data.task_id);
    } catch (error) {
        adjacencyRefreshRunning = false;
        hideSyncProgress();
        updateDataMenus();
        showNotification('Adjacency refresh failed to start: ' + (error.message || error), 'error');
    }
}

async function monitorAdjacencyRefreshTask(taskId) {
    const pollInterval = 1500;
    while (true) {
        try {
            await new Promise(r => setTimeout(r, pollInterval));
            const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
            const data = await response.json();
            if (!data.success || !data.task) throw new Error('Task not found');
            const task = data.task;
            updateSyncProgress(task.progress || 0, task.current_step_description || task.message || '');
            if (task.status === 'completed') {
                sessionStorage.removeItem(ADJ_REFRESH_TASK_KEY);
                adjacencyRefreshRunning = false;
                hideSyncProgress();
                showNotification('Adjacency refreshed successfully!', 'success');
                checkTripleStoreStatus(true);
                _loadDtExistence();
                if (typeof refreshDigitalTwinStatus === 'function') refreshDigitalTwinStatus();
                if (typeof refreshTasks === 'function') refreshTasks();
                if (window.CertifiedCachePane) window.CertifiedCachePane.load();
                updateDataMenus();
                break;
            }
            if (task.status === 'failed') {
                sessionStorage.removeItem(ADJ_REFRESH_TASK_KEY);
                adjacencyRefreshRunning = false;
                hideSyncProgress();
                showNotification(
                    'Adjacency refresh failed: ' + (task.error || task.message || 'Unknown error'),
                    'error'
                );
                updateDataMenus();
                break;
            }
            if (task.status === 'cancelled') {
                sessionStorage.removeItem(ADJ_REFRESH_TASK_KEY);
                adjacencyRefreshRunning = false;
                hideSyncProgress();
                showNotification('Adjacency refresh was cancelled', 'warning');
                updateDataMenus();
                break;
            }
        } catch (error) {
            sessionStorage.removeItem(ADJ_REFRESH_TASK_KEY);
            adjacencyRefreshRunning = false;
            hideSyncProgress();
            showNotification('Adjacency refresh monitoring failed: ' + (error.message || error), 'error');
            updateDataMenus();
            break;
        }
    }
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
        if (!silent) showNotification('Triple store table not configured. Please set it in Context Settings.', 'warning');
        return;
    }

    // Disable sync load button
    const syncBtn = document.getElementById('syncLoadBtn');
    const loadingHtml = '<span class="spinner-border spinner-border-sm me-1"></span>Loading...';
    if (syncBtn) { syncBtn.disabled = true; syncBtn.innerHTML = loadingHtml; }

    const source = (typeof SigmaGraph !== 'undefined' && typeof SigmaGraph.graphSource === 'function')
        ? SigmaGraph.graphSource()
        : { includeInferred: true, simulationId: '' };
    const includeInferred = source.includeInferred !== false;
    const simulationId = source.simulationId || '';

    try {
        const response = await fetch('/dtwin/sync/load', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                include_inferred: includeInferred,
                ...(simulationId ? { simulation_id: simulationId } : {}),
            }),
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

        // Flag for visualization to rebuild graph
        if (typeof graphJustBuilt !== 'undefined') {
            graphJustBuilt = true;
        }

        if (!silent) showNotification(`Loaded ${count} triples from triple store`, 'success');

        // Navigate to Graph Viewer only when explicitly requested
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
function _insightKpiHtml(cards, colClass) {
    var cols = colClass || 'col-md-4 col-lg-2';
    return (cards || []).map(function (c) {
        var hint = c.hint
            ? '<div class="small text-muted">' + c.hint + '</div>'
            : '';
        return '<div class="' + cols + '">' +
            '  <div class="ob-kpi-tile">' +
            '    <div class="ob-kpi-tile-icon text-' + (c.color || 'secondary') + '"><i class="bi ' + c.icon + '"></i></div>' +
            '    <div class="ob-kpi-tile-value">' + c.value + '</div>' +
            '    <div class="ob-kpi-tile-label">' + c.label + '</div>' +
            hint +
            '  </div>' +
            '</div>';
    }).join('');
}

function _insightCacheStateLabel(cache) {
    if (!cache || !cache.supported) {
        return 'Search cache is only available for Lakehouse and Lakebase.';
    }
    var refreshed = '';
    if (cache.refreshed_at) {
        try {
            refreshed = ' Last refresh: ' + formatDateTime(cache.refreshed_at) + '.';
        } catch (_) {
            refreshed = ' Last refresh: ' + cache.refreshed_at + '.';
        }
    }
    if (!cache.enabled) {
        return 'Off — live reads (adjacency, properties, and entity-search companions are not used).' + refreshed;
    }
    if (cache.state === 'ready') {
        return 'On — using cached indexes (adjacency in/out, properties, entity search).' + refreshed;
    }
    return 'On — rebuild or Refresh cache required (live reads until then).' + refreshed;
}

function _companionCount(companions, key) {
    var items = companions || [];
    for (var i = 0; i < items.length; i++) {
        if (items[i].key === key) return items[i];
    }
    return null;
}

function _companionValue(item) {
    if (!item) return '—';
    if (!item.ready) return 'Missing';
    if (item.row_count == null) return '—';
    return Number(item.row_count).toLocaleString();
}

function _renderInsightsCache(data) {
    var block = document.getElementById('insightsCacheBlock');
    var status = document.getElementById('insightsCacheStatus');
    var row = document.getElementById('insightsCacheRow');
    var cache = data.graph_cache || {};
    renderBuildGraphCacheStatus(cache, 'orig');
    if (status) status.textContent = _insightCacheStateLabel(cache);
    if (!row) return;
    if (!cache.supported) {
        if (block) block.classList.remove('d-none');
        row.innerHTML = '';
        return;
    }
    var companions = cache.companions || [];
    var specs = [
        { key: 'adj_out', label: 'Adjacency out', icon: 'bi-box-arrow-right', color: 'primary' },
        { key: 'adj_in', label: 'Adjacency in', icon: 'bi-box-arrow-in-left', color: 'info' },
        { key: 'props', label: 'Properties', icon: 'bi-list-ul', color: 'success' },
        { key: 'entity_search', label: 'Entity search', icon: 'bi-search', color: 'warning' },
        { key: 'entity_search_asserted', label: 'Entity search (asserted)', icon: 'bi-funnel', color: 'secondary' },
    ];
    var cards = specs.map(function (spec) {
        var item = _companionCount(companions, spec.key);
        return {
            label: spec.label,
            value: _companionValue(item),
            icon: spec.icon,
            color: spec.color,
            hint: item && item.table ? item.table : '',
        };
    });
    row.innerHTML = _insightKpiHtml(cards, 'col-md-4 col-lg');
}

function _renderInsightsMaterialized(data) {
    var row = document.getElementById('insightsMaterializedRow');
    if (!row) return;
    var intel = data.intelligence || {};
    var communities = intel.communities || {};
    var inferred = intel.inferred_triples;
    if (inferred == null) inferred = data.inferred_triples;
    var inferredLabel = inferred == null ? '—' : Number(inferred).toLocaleString();
    var clusterCount = communities.ready
        ? Number(communities.cluster_count || 0).toLocaleString()
        : 'None';
    var memberCount = communities.ready
        ? Number(communities.node_count || 0).toLocaleString()
        : '—';
    var algo = communities.algorithm || '';
    row.innerHTML = _insightKpiHtml([
        {
            label: 'Inferred triples',
            value: inferredLabel,
            icon: 'bi-lightbulb',
            color: 'warning',
            hint: 'Reasoning / IF-THEN writes',
        },
        {
            label: 'Communities',
            value: clusterCount,
            icon: 'bi-people',
            color: 'info',
            hint: algo ? algo.replace(/_/g, ' ') : 'Intelligence → Communities',
        },
        {
            label: 'Community members',
            value: memberCount,
            icon: 'bi-diagram-3',
            color: 'primary',
            hint: 'Nodes assigned to a cluster',
        },
    ], 'col-md-4');
}

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
        summaryRow.innerHTML = _insightKpiHtml(cards);
    }
    _renderInsightsCache(data);
    _renderInsightsMaterialized(data);

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
 * next to each Knowledge Graph line.
 */
/**
 * Let the force-refreshed existence probe correct the readiness badge.
 *
 * `/dtwin/sync/info` serves `triplestore_status` from a five-minute session
 * cache, whereas `/dtwin/sync/dt-existence` is fetched with force_refresh and so
 * carries the current truth. Without this the two never met: the Build page
 * showed the graph as present from this payload while every KG sub-page kept
 * rendering "Graph not built" from the cached status.
 *
 * Only an affirmative live result promotes the badge — a probe that came back
 * unknown leaves the existing state alone.
 */
function _reconcileReadinessWithLiveProbe(data) {
    if (!data || data.pending === true) return;
    if (data.graph_has_data !== true) return;
    if (tripleStoreHasData && !tripleStoreStatusUnknown) return;

    console.warn(
        '[Sync] Live dt_existence probe found graph data but the cached ' +
        'triplestore_status did not. Promoting readiness to built.'
    );
    tripleStoreHasData = true;
    tripleStoreStatusUnknown = false;
    updateDataMenus();
    updateInsightsTab();
}

async function _loadDtExistence() {
    _setArchRetrievalLoading(true);
    try {
        var resp = await fetch('/dtwin/sync/dt-existence', { credentials: 'same-origin' });
        var data = await resp.json();
        _applyBuildGraphEngineUi(data);
        _applyDtExistence(data);
        _reconcileReadinessWithLiveProbe(data);
    } catch (e) {
        console.warn('[Sync] Could not load DT existence flags', e);
        // Clear spinners so the cards never stay stuck in Loading.
        _applyBuildGraphEngineUi({
            pending: false,
            lakebase_table_exists: null,
            lakebase_synced_uc_exists: null,
            lakebase_check_error: String(e && e.message ? e.message : e),
            graph_engine: (window.__TRIPLESTORE_CONFIG || {}).graph_engine || 'lakebase',
        });
        _applyDtExistence({
            pending: false,
            view_exists: null,
            view_check_error: 'Could not refresh artefact status',
        });
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

    // The backend panel lives in the Certified Graph section (dtwin) or the
    // legacy Build section (domain page).
    const syncSection = document.getElementById('certified-section')
        || document.getElementById('sync-section');
    if (syncSection) {
        syncSection.addEventListener('click', function (e) {
            var btn = e.target.closest('[data-sync-action]');
            if (!btn || !syncSection.contains(btn)) return;
            if (btn.getAttribute('data-sync-action') === 'refresh-adjacency-only') {
                startSyncAdjacencyRefresh();
            }
        });
    }

    // Re-load when the section becomes visible after navigating away and back
    if (syncSection) {
        const observer = new MutationObserver(() => {
            if (syncSection.classList.contains('active')) initSyncSection();
            else _syncSectionLoaded = false;
        });
        observer.observe(syncSection, { attributes: true, attributeFilter: ['class'] });
        if (syncSection.classList.contains('active')) initSyncSection();
    } else if (document.querySelector('[data-kg-status]')) {
        // Intelligence (and any other page with the Graph-ready badge) has no
        // Build section, so probe status and fill the indicators directly.
        tripleStoreStatusPending = true;
        updateKgReadyIndicators();
        checkTripleStoreStatus();
    }
});
