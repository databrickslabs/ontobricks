/**
 * OntoBricks - ontology-map.js
 * Extracted from ontology templates per code_instructions.txt
 */

// ONTOLOGY MAP - D3.js Force-Directed Graph
// =====================================================

let ontologyMapSvg = null;
let ontologyMapSimulation = null;
let ontologyMapZoom = null;
let mapAutoSaveTimeout = null;
const MAP_GRID_STORAGE_KEY = 'mapGridVisible';
let mapConnectionMode = null; // { sourceEntity: {...}, lineElement: <line>, type: 'relationship'|'inheritance' }
let _mapInitGeneration = 0;   // guards against concurrent initOntologyMap() calls
let ontologyMapLinks  = [];   // live reference to link data (set by initOntologyMap)
let ontologyMapNodes  = [];   // live reference to node data (set by initOntologyMap)
let _mapHighlightNeighborhood = null;  // set by initOntologyMap, used by focusMapEntity
let _mapClearHighlights       = null;  // set by initOntologyMap, used by focusMapEntity
let mapSelectedEntityNames    = new Set();
let mapSelectMode             = false;
let _mapCanvasClickSuppressUntil = 0;  // performance.now() deadline; 0 = not armed

// Studio Business View filter (null = All)
let _studioActiveView = null;
let _studioViewLayout = null;
let _studioViewWired = false;
let _studioCollapsedHydrated = false;  // All-view collapsedGroups loaded from map-layout once

// Ontology Groups collapse/expand on the Studio canvas
let _studioGroupsDefs = [];
let _studioCollapsedGroups = new Set();
let _studioMemberToGroup = {};  // entityName → groupName
// Live entity positions (name → {x,y,fx,fy}). Survives collapse/expand rebuilds
// so members reappear at the exact coordinates they had before collapse.
let _studioEntityPositions = new Map();

/**
 * Rebuild #studioViewSelect from GET /domain/design-views.
 */
async function refreshStudioViewSelect() {
    const select = document.getElementById('studioViewSelect');
    const deleteBtn = document.getElementById('studioDeleteViewBtn');
    if (!select) return;

    let views = [];
    try {
        const resp = await fetch('/domain/design-views', { credentials: 'same-origin' });
        if (resp.ok) {
            const data = await resp.json();
            views = data.views || [];
        }
    } catch (err) {
        console.warn('[Studio] Could not load design views:', err);
    }

    const current = _studioActiveView || '__all__';
    select.innerHTML = '';
    const allOpt = document.createElement('option');
    allOpt.value = '__all__';
    allOpt.textContent = 'All';
    select.appendChild(allOpt);

    views.forEach(name => {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = name;
        select.appendChild(opt);
    });

    select.value = views.includes(current) ? current : '__all__';
    if (deleteBtn) {
        deleteBtn.disabled = !(_studioActiveView && views.includes(_studioActiveView));
    }
}

/**
 * Switch the Studio canvas to a Business View (or All) and re-render.
 * @param {string|null} viewNameOrAll
 */
async function applyStudioView(viewNameOrAll) {
    if (!viewNameOrAll || viewNameOrAll === '__all__') {
        _studioActiveView = null;
        _studioViewLayout = null;
        // Restore collapsed groups from the All map layout when available.
        try {
            const layout = await loadMapLayout();
            const known = new Set(_studioGroupsDefs.map(g => g.name));
            if (Array.isArray(layout?.collapsedGroups)) {
                _studioCollapsedGroups = new Set(
                    layout.collapsedGroups.filter(name => !known.size || known.has(name))
                );
            }
            _studioCollapsedHydrated = true;
        } catch (_) { /* keep current collapsed set */ }
        await initOntologyMap();
        await refreshStudioViewSelect();
        return;
    }

    try {
        const switchResp = await fetch('/domain/design-views/switch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: viewNameOrAll }),
            credentials: 'same-origin',
        });
        if (!switchResp.ok) {
            const err = await switchResp.json().catch(() => ({}));
            throw new Error(err.detail || `Switch view failed (${switchResp.status})`);
        }

        const curResp = await fetch('/domain/design-views/current', { credentials: 'same-origin' });
        if (!curResp.ok) {
            throw new Error(`Get current view failed (${curResp.status})`);
        }
        const curData = await curResp.json();
        _studioViewLayout = curData.layout || {};
        _studioActiveView = viewNameOrAll;

        const collapsed = _studioViewLayout?.visibility?.collapsedGroups;
        if (Array.isArray(collapsed)) {
            _studioCollapsedGroups = new Set(collapsed);
        }

        await initOntologyMap();
        await refreshStudioViewSelect();
    } catch (err) {
        console.error('[Studio] applyStudioView failed:', err);
        if (typeof showNotification === 'function') {
            showNotification(`Could not switch view: ${err.message}`, 'danger');
        }
    }
}

/**
 * Wire the Studio view dropdown and delete button once.
 */
function wireStudioViewControls() {
    if (_studioViewWired) return;
    const select = document.getElementById('studioViewSelect');
    const deleteBtn = document.getElementById('studioDeleteViewBtn');
    if (!select) return;
    _studioViewWired = true;

    select.addEventListener('change', () => {
        applyStudioView(select.value);
    });

    if (deleteBtn) {
        deleteBtn.addEventListener('click', async () => {
            if (!_studioActiveView) return;
            const viewName = _studioActiveView;
            const confirmed = typeof showConfirmDialog === 'function'
                ? await showConfirmDialog({
                    title: 'Delete Business View',
                    message: `Delete Business View "${viewName}"? This cannot be undone.`,
                    confirmText: 'Delete',
                    confirmClass: 'btn-danger',
                })
                : window.confirm(`Delete Business View "${viewName}"?`);
            if (!confirmed) return;

            try {
                const resp = await fetch('/domain/design-views/delete', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name: viewName }),
                    credentials: 'same-origin',
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Delete failed (${resp.status})`);
                }
                await applyStudioView('__all__');
            } catch (err) {
                console.error('[Studio] delete view failed:', err);
                if (typeof showNotification === 'function') {
                    showNotification(`Could not delete view: ${err.message}`, 'danger');
                }
            }
        });
    }
}

/**
 * Flush current canvas node positions before an ontology save.
 */
async function flushStudioLayoutBeforeSave() {
    if (ontologyMapNodes && ontologyMapNodes.length) {
        await saveMapLayout(ontologyMapNodes);
    }
}
window.flushStudioLayoutBeforeSave = flushStudioLayoutBeforeSave;

/**
 * Load ontology groups and rebuild the member → group map.
 */
async function loadStudioGroups() {
    try {
        const resp = await fetch('/ontology/groups/list', { credentials: 'same-origin' });
        if (!resp.ok) return;
        const data = await resp.json();
        _studioGroupsDefs = data.groups || [];
        _studioMemberToGroup = {};
        const known = new Set();
        _studioGroupsDefs.forEach(g => {
            known.add(g.name);
            (g.members || []).forEach(m => {
                _studioMemberToGroup[m] = g.name;
            });
        });
        // Drop collapsed entries for groups that no longer exist.
        _studioCollapsedGroups = new Set(
            [..._studioCollapsedGroups].filter(name => known.has(name))
        );
    } catch (err) {
        console.warn('[Studio] Could not load groups:', err);
    }
}

/**
 * Snapshot non-group node positions into the in-memory cache.
 * Call before any collapse/expand so members keep their exact coords.
 * @param {Array} [nodes]
 */
function _snapshotStudioEntityPositions(nodes) {
    (nodes || ontologyMapNodes || []).forEach(node => {
        if (!node || node.isGroup || !node.name) return;
        const x = node.fx != null ? node.fx : node.x;
        const y = node.fy != null ? node.fy : node.y;
        if (x == null || y == null) return;
        _studioEntityPositions.set(node.name, {
            x: x,
            y: y,
            fx: node.fx != null ? node.fx : x,
            fy: node.fy != null ? node.fy : y,
        });
    });
}

/**
 * Apply cached (or saved) positions onto a freshly built entity node.
 * @param {Object} node
 * @param {Object|null} savedPos  from map-layout / BV layout
 * @returns {Object}
 */
/**
 * @returns {boolean} true when a concrete cached/saved position was applied
 */
function _applyStudioEntityPosition(node, savedPos) {
    const cached = _studioEntityPositions.get(node.name);
    if (cached) {
        node.x = cached.x;
        node.y = cached.y;
        node.fx = cached.fx != null ? cached.fx : cached.x;
        node.fy = cached.fy != null ? cached.fy : cached.y;
        return true;
    }
    if (savedPos && savedPos.x != null && savedPos.y != null) {
        node.x = savedPos.x;
        node.y = savedPos.y;
        node.fx = savedPos.fx != null ? savedPos.fx : savedPos.x;
        node.fy = savedPos.fy != null ? savedPos.fy : savedPos.y;
        _studioEntityPositions.set(node.name, {
            x: node.x, y: node.y, fx: node.fx, fy: node.fy,
        });
        return true;
    }
    return false;
}

/**
 * When a group super-node is dragged, keep the anchor member's cached
 * position in sync so the circle stays coherent across collapse cycles.
 * @param {Object} groupNode
 */
function _syncGroupAnchorPosition(groupNode) {
    if (!groupNode || !groupNode.isGroup || !groupNode.groupName) return;
    const g = _studioGroupsDefs.find(d => d.name === groupNode.groupName);
    if (!g || !g.members || !g.members.length) return;
    const anchorName = g.members.find(m => _studioEntityPositions.has(m)) || g.members[0];
    if (!anchorName) return;
    const x = groupNode.fx != null ? groupNode.fx : groupNode.x;
    const y = groupNode.fy != null ? groupNode.fy : groupNode.y;
    if (x == null || y == null) return;
    _studioEntityPositions.set(anchorName, { x: x, y: y, fx: x, fy: y });
}

async function collapseStudioGroup(name) {
    if (!name) return;
    _snapshotStudioEntityPositions(ontologyMapNodes);
    _studioCollapsedGroups.add(name);
    await persistStudioCollapsedGroups();
    await initOntologyMap();
}

async function expandStudioGroup(name) {
    if (!name) return;
    // Snapshot first: non-group nodes keep their live coords; collapsed members
    // already live in `_studioEntityPositions` from the last collapse/drag.
    _snapshotStudioEntityPositions(ontologyMapNodes);
    _studioCollapsedGroups.delete(name);
    await persistStudioCollapsedGroups();
    await initOntologyMap();
}

window.refreshStudioGroups = async function () {
    _snapshotStudioEntityPositions(ontologyMapNodes);
    await loadStudioGroups();
    await initOntologyMap();
};

/**
 * Persist collapsed group names: into the active BV visibility, or the All map layout.
 */
async function persistStudioCollapsedGroups() {
    try {
        // Prefer the position cache so collapsed members (absent from the live
        // graph) are still written back into the saved layout.
        const positions = {};
        _studioEntityPositions.forEach((pos, name) => {
            positions[name] = {
                x: pos.x,
                y: pos.y,
                fx: pos.fx != null ? pos.fx : pos.x,
                fy: pos.fy != null ? pos.fy : pos.y,
            };
        });
        // Overlay any currently visible non-group nodes (most recent drag).
        (ontologyMapNodes || []).forEach(node => {
            if (!node || node.isGroup || !node.name) return;
            const x = node.fx != null ? node.fx : node.x;
            const y = node.fy != null ? node.fy : node.y;
            if (x == null || y == null) return;
            positions[node.name] = { x: x, y: y, fx: x, fy: y };
        });

        if (_studioActiveView && _studioViewLayout) {
            const byPos = positions;
            const entities = (_studioViewLayout.entities || []).map(e => {
                const p = byPos[e.name];
                if (!p) return e;
                return { ...e, x: p.x, y: p.y };
            });
            // Ensure every cached entity is present for restore on expand.
            Object.keys(byPos).forEach(name => {
                if (entities.some(e => e.name === name)) return;
                entities.push({
                    id: undefined,
                    name: name,
                    x: byPos[name].x,
                    y: byPos[name].y,
                    properties: null,
                    color: null,
                });
            });
            const visibility = {
                ...(_studioViewLayout.visibility || {}),
                collapsedGroups: Array.from(_studioCollapsedGroups),
            };
            _studioViewLayout = { ..._studioViewLayout, entities, visibility };
            await fetch('/domain/design-views/save-current', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(_studioViewLayout),
                credentials: 'same-origin',
            });
            return;
        }

        await fetch('/domain/map-layout', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                positions: positions,
                collapsedGroups: Array.from(_studioCollapsedGroups),
            }),
            credentials: 'same-origin',
        });
    } catch (err) {
        console.warn('[Studio] Could not persist collapsed groups:', err);
    }
}

/**
 * Save current node positions into the active Business View layout.
 * @param {Array} nodes
 */
async function _saveStudioViewLayoutFromNodes(nodes) {
    if (!_studioViewLayout) return;
    _snapshotStudioEntityPositions(nodes);
    const byName = new Map(
        (nodes || []).filter(n => !n.isGroup).map(n => [n.name, n])
    );
    const entities = (_studioViewLayout.entities || []).map(e => {
        const n = byName.get(e.name);
        if (n) {
            const x = n.fx != null ? n.fx : n.x;
            const y = n.fy != null ? n.fy : n.y;
            return { ...e, x: x, y: y };
        }
        const cached = _studioEntityPositions.get(e.name);
        if (cached) return { ...e, x: cached.x, y: cached.y };
        return e;
    });
    const visibility = {
        ...(_studioViewLayout.visibility || {}),
        collapsedGroups: Array.from(_studioCollapsedGroups),
    };
    const layout = { ..._studioViewLayout, entities, visibility };
    _studioViewLayout = layout;
    await fetch('/domain/design-views/save-current', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(layout),
        credentials: 'same-origin',
    });
}

/**
 * True for Ctrl/Cmd gestures and while the Studio Select toolbar is on.
 * @param {Event} event
 * @returns {boolean}
 */
function _isMapSelectGesture(event) {
    return !!(mapSelectMode || (event && (event.ctrlKey || event.metaKey)));
}

/**
 * A Ctrl/Cmd marquee ends on ``pointerup``; the browser then dispatches a
 * ``click`` on the canvas, which would clear the selection the marquee just
 * made. Arm a one-shot, short-lived guard so that click is ignored.
 */
function _suppressNextMapCanvasClick() {
    _mapCanvasClickSuppressUntil = performance.now() + 400;
}

/**
 * @returns {boolean} true (once) when the next canvas click must be ignored.
 */
function _consumeMapCanvasClickSuppression() {
    const armed = _mapCanvasClickSuppressUntil > 0
        && performance.now() <= _mapCanvasClickSuppressUntil;
    _mapCanvasClickSuppressUntil = 0;
    return armed;
}

/**
 * Names currently selected (live read, never a cached copy).
 * @returns {string[]}
 */
function _liveMapSelectionNames() {
    return Array.from(mapSelectedEntityNames);
}

/**
 * Apply a new entity selection set and sync node classes, neighbourhood
 * dimming, and the single-entity panel.
 * @param {Iterable<string>} names
 * @param {{openPanel?: boolean, closePanel?: boolean}} [options]
 */
function _setMapSelection(names, options) {
    const openPanel = !(options && options.openPanel === false);
    const closePanel = !(options && options.closePanel === false);
    const previousSize = mapSelectedEntityNames.size;
    mapSelectedEntityNames = new Set(names || []);
    // A context menu captured the previous selection; never leave it open.
    hideMapContextMenu();
    d3.selectAll('.map-node').classed('selected', d => d && mapSelectedEntityNames.has(d.name));

    if (mapSelectedEntityNames.size === 1) {
        const name = mapSelectedEntityNames.values().next().value;
        if (_mapHighlightNeighborhood) _mapHighlightNeighborhood(name);
        if (openPanel && typeof editClassByName === 'function') {
            editClassByName(name);
        }
        return;
    }

    if (_mapClearHighlights) _mapClearHighlights();
    if (closePanel && previousSize !== 0 && typeof guardedCloseSharedPanel === 'function') {
        guardedCloseSharedPanel();
    }
}

function _toggleMapEntitySelection(name) {
    const next = new Set(mapSelectedEntityNames);
    if (next.has(name)) next.delete(name);
    else next.add(name);
    _setMapSelection(next, { openPanel: false });
}

function _clearMapSelection(options) {
    _setMapSelection([], { openPanel: false, closePanel: options && options.closePanel });
}

function handleMapSelectionKeyDown(event) {
    if (event.key !== 'Escape') return;
    // Escape belongs to an open Bootstrap modal, or to another Ontology
    // section, not to the Studio canvas. Bootstrap's own (element-level)
    // keydown handler runs first and already drops `.show` while dismissing,
    // so also look at where the key came from and at `body.modal-open`
    // (which stays set until the hide transition ends).
    if (event.target && event.target.closest && event.target.closest('.modal')) return;
    if (document.body.classList.contains('modal-open')) return;
    if (document.querySelector('.modal.show')) return;
    if (!document.querySelector('#map-section.active')) return;
    hideMapContextMenu();
    if (mapConnectionMode) return;
    if (mapSelectedEntityNames.size <= 1) return;
    _clearMapSelection();
}

document.addEventListener('keydown', handleMapSelectionKeyDown);

/**
 * Resolve the name from a D3 link endpoint that may be either a plain string
 * (before simulation) or a node object (after forceLink resolution).
 * @param {string|Object} endpoint - link.source or link.target
 * @returns {string}
 */
function _resolveLinkEndpoint(endpoint) {
    return (endpoint && typeof endpoint === 'object') ? endpoint.name : endpoint;
}

function _classHasExternalConfig(cls) {
    const hasDatasets = (Array.isArray(cls.datasets) && cls.datasets.length)
        || !!cls.dataset;
    return !!(cls.dashboard || hasDatasets || (cls.actions || []).length || (cls.bridges || []).length);
}

function _appendMapExternalBadge(nodeSelection) {
    nodeSelection.append('circle')
        .attr('class', 'map-node-external-badge-bg')
        .attr('cx', 16)
        .attr('cy', -16)
        .attr('r', 9)
        .attr('role', 'img')
        .attr('aria-label', 'Has external configuration');

    nodeSelection.append('text')
        .attr('class', 'map-node-external-badge-icon')
        .attr('x', 16)
        .attr('y', -16)
        .attr('aria-hidden', 'true')
        .text('\uf46d');
}

/**
 * Resolve the current brand primary from CSS custom properties.
 */
function _getBrandPrimaryColor() {
    const value = getComputedStyle(document.documentElement)
        .getPropertyValue('--db-primary')
        .trim();
    return value || '#4F46E5';
}

/**
 * Show/hide loading overlay for ontology model
 */
function showOntologyMapLoading(show) {
    const loadingEl = document.getElementById('ontologyMapLoading');
    if (loadingEl) {
        loadingEl.style.display = show ? 'flex' : 'none';
    }
}

/**
 * Load saved map layout from domain session
 */
async function loadMapLayout() {
    try {
        const response = await fetch('/domain/map-layout', { credentials: 'same-origin' });
        const data = await response.json();
        if (data.success && data.layout) {
            return data.layout;
        }
    } catch (error) {
        console.log('[Map] Could not load map layout:', error);
    }
    return null;
}

/**
 * Save map layout to domain session (All view) or to the active Business View.
 */
async function saveMapLayout(nodes) {
    try {
        _snapshotStudioEntityPositions(nodes);
        if (_studioActiveView) {
            await _saveStudioViewLayoutFromNodes(nodes);
            console.log('[Map] Business View layout saved');
            return;
        }
        // Merge cache + live nodes so collapsed members keep their coords.
        const positions = {};
        _studioEntityPositions.forEach((pos, name) => {
            positions[name] = {
                x: pos.x,
                y: pos.y,
                fx: pos.fx != null ? pos.fx : pos.x,
                fy: pos.fy != null ? pos.fy : pos.y,
            };
        });
        (nodes || []).forEach(node => {
            if (!node || node.isGroup) return;
            const x = node.fx != null ? node.fx : node.x;
            const y = node.fy != null ? node.fy : node.y;
            if (x == null || y == null) return;
            positions[node.id || node.name] = { x: x, y: y, fx: x, fy: y };
        });

        await fetch('/domain/map-layout', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                positions,
                collapsedGroups: Array.from(_studioCollapsedGroups),
            }),
            credentials: 'same-origin'
        });
        console.log('[Map] Layout saved');
    } catch (error) {
        console.log('[Map] Could not save map layout:', error);
    }
}

/**
 * Schedule auto-save for map layout (debounced)
 */
function scheduleMapAutoSave(nodes) {
    if (mapAutoSaveTimeout) {
        clearTimeout(mapAutoSaveTimeout);
    }
    mapAutoSaveTimeout = setTimeout(() => {
        saveMapLayout(nodes);
    }, 500);
}

/**
 * Whether the Designer canvas dotted grid is shown (default: on, like OntoViz).
 * @returns {boolean}
 */
function isMapGridVisible() {
    return sessionStorage.getItem(MAP_GRID_STORAGE_KEY) !== 'false';
}

/**
 * Apply persisted grid visibility to the canvas and toggle button.
 */
function applyMapGridVisibility() {
    const container = document.getElementById('ontology-map-container');
    const btn = document.getElementById('mapToggleGrid');
    const visible = isMapGridVisible();
    if (container) {
        container.classList.toggle('map-grid-visible', visible);
    }
    if (btn) {
        btn.classList.toggle('active', visible);
        btn.setAttribute('aria-pressed', visible ? 'true' : 'false');
        btn.title = visible ? 'Hide dot grid' : 'Show dot grid';
    }
}

/**
 * Bind the header grid toggle once. Safe to call from every map init
 * (including the empty-ontology early return).
 */
function initMapGridToggle() {
    const btn = document.getElementById('mapToggleGrid');
    applyMapGridVisibility();
    if (!btn || btn.dataset.bound === '1') return;
    btn.dataset.bound = '1';
    btn.onclick = () => {
        sessionStorage.setItem(MAP_GRID_STORAGE_KEY, String(!isMapGridVisible()));
        applyMapGridVisibility();
    };
}

/**
 * Sync the Select toolbar button and canvas cursor with ``mapSelectMode``.
 */
function applyMapSelectMode() {
    const container = document.getElementById('ontology-map-container');
    const btn = document.getElementById('mapToggleSelect');
    if (container) {
        container.classList.toggle('map-select-mode', mapSelectMode);
    }
    if (btn) {
        btn.classList.toggle('active', mapSelectMode);
        btn.setAttribute('aria-pressed', mapSelectMode ? 'true' : 'false');
        btn.title = mapSelectMode
            ? 'Exit select mode (plain click opens the panel)'
            : 'Select entities (or hold Ctrl/Cmd)';
        btn.setAttribute('aria-label', btn.title);
    }
}

/**
 * Bind the header Select toggle once. Safe to call from every map init.
 */
function initMapSelectToggle() {
    const btn = document.getElementById('mapToggleSelect');
    applyMapSelectMode();
    if (!btn || btn.dataset.bound === '1') return;
    btn.dataset.bound = '1';
    btn.onclick = () => {
        mapSelectMode = !mapSelectMode;
        applyMapSelectMode();
    };
}

/**
 * Initialize the Ontology Designer visualization
 */
async function initOntologyMap() {
    // Increment generation counter to cancel any previous in-flight init
    const thisGeneration = ++_mapInitGeneration;
    // Rebuilds must not double-save/close a panel the caller just opened.
    _clearMapSelection({ closePanel: false });

    initMapGridToggle();
    initMapSelectToggle();
    wireStudioViewControls();
    showOntologyMapLoading(true);
    
    const container = document.getElementById('ontology-map-container');
    if (!container) {
        console.error('Map container not found');
        showOntologyMapLoading(false);
        return;
    }
    
    try {

    // Clear previous map content (SVGs, legends, empty messages) but keep the loading overlay
    container.querySelectorAll(':scope > svg').forEach(el => el.remove());
    container.querySelectorAll('.map-legend').forEach(el => el.remove());
    container.querySelectorAll('.map-empty-message').forEach(el => el.remove());

    // Stop any running simulation to avoid lingering tick callbacks
    if (ontologyMapSimulation) {
        ontologyMapSimulation.stop();
        ontologyMapSimulation = null;
    }

    // Wait for ontology data to be fully loaded before checking
    if (typeof window.waitForOntologyLoaded === 'function') {
        await window.waitForOntologyLoaded();
    }

    // If a newer init was triggered while we were waiting, abort
    if (thisGeneration !== _mapInitGeneration) {
        console.log('[Map] Superseded by newer init call, aborting');
        return;
    }

    await loadStudioGroups();
    // Refresh the view dropdown without awaiting apply (options only).
    refreshStudioViewSelect();

    // If a newer init was triggered while we were loading groups, abort
    if (thisGeneration !== _mapInitGeneration) {
        console.log('[Map] Superseded by newer init call, aborting');
        return;
    }

    // Get ontology data
    const classes = OntologyState?.config?.classes || [];
    const properties = OntologyState?.config?.properties || [];

    if (classes.length === 0) {
        container.innerHTML += `
            <div class="map-empty-message text-center text-muted py-5">
                <i class="bi bi-diagram-3 fs-1 d-block mb-3 opacity-50"></i>
                <p class="mb-0">No entities defined yet.</p>
                <p class="small">Add entities in the <strong>Entities</strong> section to see them here.</p>
            </div>
        `;
        showOntologyMapLoading(false);
        return;
    }

    // If a previous icon-assignment task is still running, resume monitoring
    // so the user sees the result land without clicking the button again.
    if (typeof checkAndResumeIconsTask === 'function') {
        checkAndResumeIconsTask();
    }

    // Load saved layout
    const savedLayout = await loadMapLayout();

    // Hydrate collapsed groups from the All map layout once (avoid races with
    // in-memory collapse/expand that re-enter initOntologyMap).
    if (!_studioActiveView && !_studioCollapsedHydrated
            && Array.isArray(savedLayout?.collapsedGroups)) {
        const known = new Set(_studioGroupsDefs.map(g => g.name));
        _studioCollapsedGroups = new Set(
            savedLayout.collapsedGroups.filter(name => known.has(name))
        );
        _studioCollapsedHydrated = true;
    }

    // If a newer init was triggered while we were loading layout, abort
    if (thisGeneration !== _mapInitGeneration) {
        console.log('[Map] Superseded by newer init call, aborting');
        return;
    }

    // Build nodes from classes. Prefer the in-memory position cache (survives
    // collapse/expand) over the persisted map layout, then fall back to a grid.
    let nodes = classes.map((cls, idx) => {
        const savedPos = savedLayout?.positions?.[cls.name];
        const fallbackX = 100 + (idx % 5) * 150;
        const fallbackY = 100 + Math.floor(idx / 5) * 120;
        const node = {
            id: cls.name,
            name: cls.name,
            label: cls.label || cls.name,
            icon: cls.emoji || OntologyState.defaultClassEmoji || '📦',
            // True when the backing class has a Dashboard, Dataset, Actions, or
            // Bridges configured (entity panel's Links / Behaviour / Model tabs).
            hasExternal: _classHasExternalConfig(cls),
            parent: cls.parent,
            x: fallbackX,
            y: fallbackY,
            fx: null,
            fy: null,
        };
        _applyStudioEntityPosition(node, savedPos || null);
        return node;
    });

    // Build links from properties (relationships) and inheritance
    let links = [];

    // Create lookup maps for robust matching (handles case mismatches)
    const validNodeIds = new Set(nodes.map(n => n.id));
    const nodeNameLower = new Map();  // lowercase → actual name
    nodes.forEach(n => nodeNameLower.set(n.id.toLowerCase(), n.id));

    function resolveNodeId(name) {
        if (!name) return undefined;
        if (validNodeIds.has(name)) return name;
        // URI extraction fallback
        if (name.includes('#') || (name.includes('/') && name.includes(':'))) {
            const local = name.split('#').pop().split('/').pop();
            if (local && validNodeIds.has(local)) return local;
        }
        // Case-insensitive fallback
        return nodeNameLower.get(name.toLowerCase());
    }
    
    // Primitive XSD/OWL data types — properties whose range is one of these are
    // data-type attributes, not object relationships. Skip them silently; they
    // have no target node on the graph.
    const PRIMITIVE_TYPES = new Set([
        'string', 'integer', 'int', 'long', 'float', 'double', 'decimal',
        'boolean', 'date', 'datetime', 'time', 'duration',
        'anyuri', 'literal', 'plainliteral', 'langstring',
        'xsd:string', 'xsd:integer', 'xsd:int', 'xsd:long', 'xsd:float',
        'xsd:double', 'xsd:decimal', 'xsd:boolean', 'xsd:date',
        'xsd:datetime', 'xsd:time', 'xsd:duration', 'xsd:anyuri',
        'rdfs:literal',
    ]);

    // Add relationship links (only if both domain and range exist as classes)
    let mapSkipped = 0;
    properties.forEach(prop => {
        if (prop.domain && prop.range) {
            if (PRIMITIVE_TYPES.has((prop.range || '').toLowerCase())) return;
            const source = resolveNodeId(prop.domain);
            const target = resolveNodeId(prop.range);
            if (source && target) {
                links.push({
                    source: source,
                    target: target,
                    name: prop.name,
                    label: prop.label || prop.name,
                    type: 'relationship',
                    direction: prop.direction || 'forward'
                });
            } else {
                mapSkipped++;
                console.warn(`[Map] Skipping property "${prop.name}": domain="${prop.domain}" (${source ? 'found' : 'NOT FOUND'}), range="${prop.range}" (${target ? 'found' : 'NOT FOUND'})`);
            }
        }
    });
    if (mapSkipped > 0) {
        console.warn(`[Map] ${mapSkipped}/${properties.length} properties skipped — available entities: [${Array.from(validNodeIds).join(', ')}]`);
    }

    // Add inheritance links (only if parent exists as a class)
    classes.forEach(cls => {
        if (cls.parent) {
            const parentId = resolveNodeId(cls.parent);
            if (parentId) {
                links.push({
                    source: parentId,
                    target: cls.name,
                    name: 'inherits',
                    type: 'inheritance'
                });
            } else {
                console.warn(`[Map] Skipping inheritance for "${cls.name}": parent "${cls.parent}" not found in classes`);
            }
        }
    });
    
    classes.forEach(cls => {
        const rels = (typeof outgoingRelationsForClass === 'function')
            ? outgoingRelationsForClass(classes, properties, cls.name)
            : [];
        rels.forEach(rel => {
            if (!rel.inherited) return;
            if (PRIMITIVE_TYPES.has((rel.range || '').toLowerCase())) return;
            const source = resolveNodeId(cls.name);
            const target = resolveNodeId(rel.range);
            if (!source || !target) return;
            links.push({
                source: source,
                target: target,
                name: rel.name,
                label: rel.label || rel.name,
                type: 'relationship',
                direction: rel.direction || 'forward',
                inherited: true,
                inheritedFrom: rel.inheritedFrom || '',
            });
        });
    });

    console.log(`[Map] Built ${links.length} valid links from ${properties.length} properties`);

    // ── Business View filter ───────────────────────────────────────────────
    if (_studioActiveView && _studioViewLayout) {
        const visibility = _studioViewLayout.visibility || {};
        const hidden = new Set(visibility.hiddenEntities || []);
        const posByName = new Map();
        (_studioViewLayout.entities || []).forEach(e => {
            if (e && e.name) posByName.set(e.name, e);
        });

        nodes = nodes.filter(n => !hidden.has(n.name)).map(n => {
            // Cache wins (exact pre-collapse coords); else BV layout entities.
            const e = posByName.get(n.name);
            const saved = (e && e.x != null && e.y != null)
                ? { x: e.x, y: e.y, fx: e.x, fy: e.y }
                : null;
            const next = { ...n };
            _applyStudioEntityPosition(next, saved);
            return next;
        });

        const remainingIds = new Set(nodes.map(n => n.id));
        const hiddenRels = visibility.hiddenRelationships || [];
        const hiddenInhs = visibility.hiddenInheritances || [];

        links = links.filter(l => {
            if (!remainingIds.has(l.source) || !remainingIds.has(l.target)) return false;
            if (l.type === 'inheritance') {
                return !hiddenInhs.some(h =>
                    (h.source === l.source && h.target === l.target)
                    || (typeof h === 'string' && h === `${l.source}|${l.target}`)
                );
            }
            return !hiddenRels.some(h => {
                if (typeof h === 'string') return h === l.name;
                return h.name === l.name && h.source === l.source && h.target === l.target;
            });
        });
    }

    // ── Ontology Groups collapse ───────────────────────────────────────────
    if (_studioGroupsDefs.length && _studioCollapsedGroups.size) {
        const nodeByName = new Map(nodes.map(n => [n.name, n]));
        const collapsedMemberNames = new Set();
        const groupNodes = [];

        _studioGroupsDefs.forEach(g => {
            if (!_studioCollapsedGroups.has(g.name)) return;
            const visibleMembers = (g.members || [])
                .map(m => nodeByName.get(m))
                .filter(Boolean);
            if (!visibleMembers.length) return;

            visibleMembers.forEach(m => collapsedMemberNames.add(m.name));
            // Prefer the first member that still has a cached position (stable
            // across collapse cycles); fall back to members[0] order.
            const anchorName = (g.members || []).find(m => nodeByName.has(m))
                || visibleMembers[0].name;
            const anchor = nodeByName.get(anchorName) || visibleMembers[0];
            const ax = anchor.fx != null ? anchor.fx : anchor.x;
            const ay = anchor.fy != null ? anchor.fy : anchor.y;
            groupNodes.push({
                id: '__group__' + g.name,
                name: g.name,
                label: (g.icon || '📁') + ' ' + (g.label || g.name),
                icon: g.icon || '📁',
                isGroup: true,
                groupName: g.name,
                groupColor: g.color || '#4A90D9',
                hasExternal: false,
                x: ax,
                y: ay,
                fx: ax,
                fy: ay,
            });
        });

        if (collapsedMemberNames.size) {
            nodes = nodes.filter(n => !collapsedMemberNames.has(n.name)).concat(groupNodes);

            const memberToGroupId = {};
            collapsedMemberNames.forEach(memberName => {
                const gName = _studioMemberToGroup[memberName];
                if (gName) memberToGroupId[memberName] = '__group__' + gName;
            });

            const seenLinkKeys = new Set();
            links = links.map(l => {
                const source = memberToGroupId[l.source] || l.source;
                const target = memberToGroupId[l.target] || l.target;
                return { ...l, source, target };
            }).filter(l => {
                // Drop edges that collapsed onto the same group super-node.
                if (l.source === l.target && String(l.source).startsWith('__group__')) {
                    return false;
                }
                const key = [l.type, l.name || '', l.source, l.target].join('|');
                if (seenLinkKeys.has(key)) return false;
                seenLinkKeys.add(key);
                return true;
            });
        }
    }

    // Expose to module scope so context-menu handlers can access them
    ontologyMapNodes = nodes;
    ontologyMapLinks = links;

    // Expose highlight helpers so module-level functions (focusMapEntity, etc.) can call them
    _mapHighlightNeighborhood = highlightNeighborhood;
    _mapClearHighlights       = clearHighlights;

    // Count links between same pairs for offsetting
    const linkCountMap = new Map();
    links.forEach(link => {
        const key = [link.source, link.target].sort().join('|');
        linkCountMap.set(key, (linkCountMap.get(key) || 0) + 1);
    });
    
    const linkIndexMap = new Map();
    links.forEach(link => {
        const key = [link.source, link.target].sort().join('|');
        const count = linkCountMap.get(key);
        const idx = linkIndexMap.get(key) || 0;
        link.linkCount = count;
        link.linkIndex = idx;
        linkIndexMap.set(key, idx + 1);
    });

    // Create SVG
    const width = container.clientWidth;
    const height = container.clientHeight;

    const svg = d3.select(container)
        .append('svg')
        .attr('width', width)
        .attr('height', height);

    ontologyMapSvg = svg;   // expose for focusMapEntity / zoom helpers

    // Add zoom behavior
    ontologyMapZoom = d3.zoom()
        .scaleExtent([0.3, 3])
        .on('zoom', (event) => {
            g.attr('transform', event.transform);
        });

    ontologyMapZoom.filter((event) => {
        if (event.type === 'wheel') return true;
        if (event.button) return false;
        if (_isMapSelectGesture(event)) return false;
        return true;
    });

    svg.call(ontologyMapZoom);

    // Main group for transforms
    const g = svg.append('g');

    // Define arrow markers
    const defs = svg.append('defs');
    
    // Arrow for relationships
    defs.append('marker')
        .attr('id', 'map-arrow')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 28)
        .attr('refY', 0)
        .attr('markerWidth', 6)
        .attr('markerHeight', 6)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', '#6c757d');
    
    // Arrow for self-loops
    defs.append('marker')
        .attr('id', 'map-arrow-self')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 10)
        .attr('refY', 0)
        .attr('markerWidth', 5)
        .attr('markerHeight', 5)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', '#495057');
    
    // Arrow for reverse-direction relationships (placed at path start, points back toward source)
    defs.append('marker')
        .attr('id', 'map-arrow-start')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 28)
        .attr('refY', 0)
        .attr('markerWidth', 6)
        .attr('markerHeight', 6)
        .attr('orient', 'auto-start-reverse')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', '#6c757d');

    // Arrow for inheritance (hollow)
    defs.append('marker')
        .attr('id', 'map-arrow-inheritance')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 28)
        .attr('refY', 0)
        .attr('markerWidth', 8)
        .attr('markerHeight', 8)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5Z')
        .attr('fill', 'white')
        .attr('stroke', '#adb5bd')
        .attr('stroke-width', 1);

    // Create force simulation (static when saved layout exists)
    ontologyMapSimulation = d3.forceSimulation(nodes)
        .force('link', d3.forceLink(links).id(d => d.id).distance(180));
    
    // Only add layout forces if no saved layout
    if (!savedLayout) {
        ontologyMapSimulation
            .force('charge', d3.forceManyBody().strength(-400))
            .force('collision', d3.forceCollide().radius(60))
            .force('center', d3.forceCenter(width / 2, height / 2));
    } else {
        // Static mode - no animation
        ontologyMapSimulation
            .alphaDecay(1)
            .velocityDecay(1);
    }

    // Drag handlers
    let mapDragStartPositions = new Map();

    function dragStarted(event, d) {
        d.fx = d.x;
        d.fy = d.y;
        mapDragStartPositions = new Map();
        if (mapSelectedEntityNames.size > 1 && mapSelectedEntityNames.has(d.name)) {
            ontologyMapNodes.forEach(node => {
                if (mapSelectedEntityNames.has(node.name)) {
                    mapDragStartPositions.set(node.name, { x: node.x, y: node.y });
                }
            });
        }
    }

    function dragged(event, d) {
        if (mapDragStartPositions.size > 1) {
            const origin = mapDragStartPositions.get(d.name);
            const deltaX = event.x - origin.x;
            const deltaY = event.y - origin.y;
            ontologyMapNodes.forEach(node => {
                const start = mapDragStartPositions.get(node.name);
                if (!start) return;
                node.fx = start.x + deltaX;
                node.fy = start.y + deltaY;
            });
        } else {
            d.fx = event.x;
            d.fy = event.y;
        }
        ontologyMapSimulation.alpha(0.01).restart();
    }

    function dragEnded(event, d) {
        mapDragStartPositions = new Map();
        if (d && d.isGroup) {
            _syncGroupAnchorPosition(d);
        } else {
            _snapshotStudioEntityPositions(ontologyMapNodes);
        }
        scheduleMapAutoSave(nodes);
    }

    // Separate self-loops from regular links
    const selfLoopLinks = links.filter(l => {
        const sourceId = typeof l.source === 'object' ? l.source.id : l.source;
        const targetId = typeof l.target === 'object' ? l.target.id : l.target;
        return sourceId === targetId;
    });
    const regularLinks = links.filter(l => !selfLoopLinks.includes(l));
    
    // Count and index self-loops per node for offsetting
    const selfLoopCountMap = new Map();
    selfLoopLinks.forEach(link => {
        const nodeId = typeof link.source === 'object' ? link.source.id : link.source;
        const count = selfLoopCountMap.get(nodeId) || 0;
        link.selfLoopIndex = count;
        selfLoopCountMap.set(nodeId, count + 1);
    });
    // Store total count in each link
    selfLoopLinks.forEach(link => {
        const nodeId = typeof link.source === 'object' ? link.source.id : link.source;
        link.selfLoopCount = selfLoopCountMap.get(nodeId);
    });

    // Draw regular relationship links
    const relationshipLinkElements = g.append('g')
        .selectAll('path')
        .data(regularLinks.filter(l => l.type === 'relationship'))
        .enter()
        .append('path')
        .attr('class', d => `map-link${d.direction === 'reverse' ? ' reverse' : ''}${d.inherited ? ' inherited' : ''}`);

    // Draw inheritance links
    const inheritanceLinkElements = g.append('g')
        .selectAll('path')
        .data(regularLinks.filter(l => l.type === 'inheritance'))
        .enter()
        .append('path')
        .attr('class', 'map-link inheritance');

    // Draw self-loop links
    const selfLoopElements = g.append('g')
        .selectAll('path')
        .data(selfLoopLinks)
        .enter()
        .append('path')
        .attr('class', d => `map-link self-loop${d.inherited ? ' inherited' : ''}`);

    // Draw clickable hitareas at link midpoints (for relationships only)
    const linkHitareas = g.append('g')
        .selectAll('circle')
        .data(links.filter(l => l.type === 'relationship'))
        .enter()
        .append('circle')
        .attr('class', 'map-link-hitarea')
        .attr('r', 8)
        .attr('fill', '#e9ecef')
        .attr('stroke', '#999')
        .attr('stroke-width', 1.5)
        .on('click', function(event, d) {
            event.stopPropagation();

            // Clear entity selection, then highlight both endpoints
            _clearMapSelection({ closePanel: false });
            highlightLink(d);

            // Show floating delete button near the hitarea (only in edit mode)
            if (window.isActiveVersion !== false) {
                showMapRelationshipActions(d, this);
            }

            // Open relationship edit panel (using shared panel)
            if (typeof editPropertyByName === 'function') {
                editPropertyByName(d.name);
            }
        })
        .on('contextmenu', function(event, d) {
            event.preventDefault();
            event.stopPropagation();

            if (window.isActiveVersion === false) return;

            // Clear entity selection, then highlight both endpoints
            _clearMapSelection({ closePanel: false });
            highlightLink(d);

            showMapRelationshipContextMenu(event, d, container);
        });

    // Draw relationship labels
    const linkLabels = g.append('g')
        .selectAll('text')
        .data(links.filter(l => l.type === 'relationship'))
        .enter()
        .append('text')
        .attr('class', 'map-link-label')
        .text(d => d.label || d.name);

    // Track if we're dragging to prevent click after drag
    let isDragging = false;
    
    // Draw nodes (icon only for entities; filled circle for collapsed groups)
    const nodeElements = g.append('g')
        .selectAll('g')
        .data(nodes)
        .enter()
        .append('g')
        .attr('class', d => d.isGroup ? 'map-node map-group-node' : 'map-node')
        .call(d3.drag()
            .on('start', function(event, d) {
                isDragging = false;
                dragStarted(event, d);
            })
            .on('drag', function(event, d) {
                isDragging = true;
                dragged(event, d);
            })
            .on('end', dragEnded));

    const entityNodeElements = nodeElements.filter(d => !d.isGroup);
    const groupNodeElements = nodeElements.filter(d => d.isGroup);

    // Invisible hitarea circle for better click detection
    entityNodeElements.append('circle')
        .attr('class', 'map-node-hitarea')
        .attr('r', 25);

    // Emoji icon (centered via CSS dominant-baseline: central)
    entityNodeElements.append('text')
        .attr('class', 'map-node-icon')
        .text(d => d.icon);

    // Node label below the icon
    entityNodeElements.append('text')
        .attr('class', 'map-node-label')
        .attr('dy', 35)
        .text(d => d.label || d.name);

    // Collapsed ontology-group super-nodes
    groupNodeElements.append('circle')
        .attr('class', 'map-group-circle')
        .attr('r', 28)
        .attr('fill', d => d.groupColor || '#4A90D9');

    groupNodeElements.append('text')
        .attr('class', 'map-node-icon')
        .text(d => d.icon);

    groupNodeElements.append('text')
        .attr('class', 'map-node-label')
        .attr('dy', 42)
        .text(d => d.label || d.name);

    // Collapse toggle (+) for entities that belong to an expanded group
    entityNodeElements
        .filter(d => {
            const gName = _studioMemberToGroup[d.name];
            return gName && !_studioCollapsedGroups.has(gName);
        })
        .each(function (d) {
            const gName = _studioMemberToGroup[d.name];
            const toggle = d3.select(this).append('g')
                .attr('class', 'studio-group-toggle')
                .attr('transform', 'translate(-22, -22)')
                .style('cursor', 'pointer');
            toggle.append('circle')
                .attr('r', 7)
                .attr('class', 'studio-group-toggle-bg');
            toggle.append('text')
                .attr('class', 'studio-group-toggle-icon')
                .attr('text-anchor', 'middle')
                .attr('dominant-baseline', 'central')
                .text('+');
            toggle.on('click', function (event) {
                event.preventDefault();
                event.stopPropagation();
                collapseStudioGroup(gName);
            });
        });

    // Expand toggle (+) on collapsed group super-nodes
    groupNodeElements.each(function (d) {
        const toggle = d3.select(this).append('g')
            .attr('class', 'studio-group-toggle')
            .attr('transform', 'translate(-22, -22)')
            .style('cursor', 'pointer');
        toggle.append('circle')
            .attr('r', 7)
            .attr('class', 'studio-group-toggle-bg');
        toggle.append('text')
            .attr('class', 'studio-group-toggle-icon')
            .attr('text-anchor', 'middle')
            .attr('dominant-baseline', 'central')
            .text('+');
        toggle.on('click', function (event) {
            event.preventDefault();
            event.stopPropagation();
            expandStudioGroup(d.groupName);
        });
    });

    // Small badge overlay signalling the class has Dashboard/Dataset/Actions/
    // Bridges configured (entity panel's Links / Behaviour / Model tabs). Purely visual — no
    // tooltip, no click handler — so it's rendered only for matching nodes
    // and marked pointer-events: none in CSS.
    const externalBadgeNodes = entityNodeElements.filter(d => d.hasExternal);

    _appendMapExternalBadge(externalBadgeNodes);

    // Tooltip on hover
    nodeElements.append('title')
        .text(d => d.label || d.name);

    // ── Neighborhood highlight helpers ──────────────────────────────────
    function highlightNeighborhood(selectedName) {
        // Resolve source/target: D3 forceLink replaces ids with node objects
        const resolveName = n => (typeof n === 'object' ? n.name : n);

        const connectedLinkSet = new Set();
        const neighborSet      = new Set();

        // Inspect ALL link arrays (regular + self-loops)
        const allLinks = [
            ...regularLinks,
            ...selfLoopLinks,
        ];
        allLinks.forEach(l => {
            const s = resolveName(l.source);
            const t = resolveName(l.target);
            if (s === selectedName || t === selectedName) {
                connectedLinkSet.add(l);
                if (s !== selectedName) neighborSet.add(s);
                if (t !== selectedName) neighborSet.add(t);
            }
        });

        // Nodes
        d3.selectAll('.map-node')
            .classed('dimmed',   d => d.name !== selectedName && !neighborSet.has(d.name))
            .classed('neighbor', d => neighborSet.has(d.name));

        // Links (path elements)
        d3.selectAll('.map-link')
            .classed('highlighted', d => connectedLinkSet.has(d))
            .classed('dimmed',      d => !connectedLinkSet.has(d));

        // Hitarea dots
        d3.selectAll('.map-link-hitarea')
            .classed('highlighted', d => connectedLinkSet.has(d))
            .classed('dimmed',      d => !connectedLinkSet.has(d));

        // Labels
        d3.selectAll('.map-link-label')
            .classed('highlighted', d => connectedLinkSet.has(d))
            .classed('dimmed',      d => !connectedLinkSet.has(d));
    }

    function clearHighlights() {
        d3.selectAll('.map-node').classed('dimmed', false).classed('neighbor', false);
        d3.selectAll('.map-link').classed('highlighted', false).classed('dimmed', false);
        d3.selectAll('.map-link-hitarea').classed('highlighted', false).classed('dimmed', false);
        d3.selectAll('.map-link-label').classed('highlighted', false).classed('dimmed', false);
    }

    function highlightLink(linkDatum) {
        const resolveName = n => (typeof n === 'object' ? n.name : n);
        const srcName = resolveName(linkDatum.source);
        const tgtName = resolveName(linkDatum.target);
        const involvedNames = new Set([srcName, tgtName]);

        d3.selectAll('.map-node')
            .classed('dimmed',   d => !involvedNames.has(d.name))
            .classed('neighbor', d => involvedNames.has(d.name));

        d3.selectAll('.map-link')
            .classed('highlighted', d => d === linkDatum)
            .classed('dimmed',      d => d !== linkDatum);

        d3.selectAll('.map-link-hitarea')
            .classed('highlighted', d => d === linkDatum)
            .classed('dimmed',      d => d !== linkDatum);

        d3.selectAll('.map-link-label')
            .classed('highlighted', d => d === linkDatum)
            .classed('dimmed',      d => d !== linkDatum);
    }

    // Click to edit entity (only if not dragging)
    nodeElements.on('click', function(event, d) {
        // Group super-nodes are anchors only — no entity panel / connections.
        if (d.isGroup) {
            event.stopPropagation();
            if (isDragging) {
                isDragging = false;
            }
            return;
        }

        // Handle connection mode - create relationship or inheritance
        if (mapConnectionMode) {
            event.preventDefault();
            event.stopPropagation();
            
            if (d.name !== mapConnectionMode.sourceEntity.name) {
                if (mapConnectionMode.type === 'inheritance') {
                    // Create inheritance - source inherits from target (d is the parent)
                    createInheritanceFromMap(mapConnectionMode.sourceEntity, d);
                } else {
                    // Create relationship between source and target
                    createRelationshipFromMap(mapConnectionMode.sourceEntity, d);
                }
            }
            endMapConnectionMode();
            return;
        }
        
        // Ignore click if we were dragging
        if (isDragging) {
            isDragging = false;
            return;
        }
        
        event.stopPropagation();
        
        // Hide context menu if open
        hideMapContextMenu();
        hideMapRelationshipActions();
        
        // Clear relationship selection
        d3.selectAll('.map-link-hitarea')
            .attr('fill', '#e9ecef')
            .attr('stroke', '#999')
            .attr('stroke-width', 1.5);

        if (_isMapSelectGesture(event)) {
            _toggleMapEntitySelection(d.name);
            return;
        }

        _setMapSelection([d.name]);
    });
    
    // Right-click context menu for entities (suppressed in view mode).
    // macOS Control-click emits contextmenu with no click; toggle then.
    nodeElements.on('contextmenu', function(event, d) {
        event.preventDefault();
        event.stopPropagation();

        if (d.isGroup) return;

        if (event.ctrlKey || event.metaKey) {
            _toggleMapEntitySelection(d.name);
            return;
        }

        if (window.isActiveVersion === false) return;

        if (!mapSelectedEntityNames.has(d.name)) {
            _setMapSelection([d.name], { openPanel: false });
        }

        if (mapSelectedEntityNames.size > 1) {
            showMapMultiSelectContextMenu(event, Array.from(mapSelectedEntityNames), container);
            return;
        }

        showMapContextMenu(event, d, container);
    });
    
    // Clicking the empty canvas drops the selection. Entity and relationship
    // clicks stop propagation, so only background clicks reach this handler.
    svg.on('click', function() {
        // The click that trails a Ctrl/Cmd marquee pointerup must not undo it.
        if (_consumeMapCanvasClickSuppression()) return;
        hideMapContextMenu();
        hideMapRelationshipActions();
        d3.selectAll('.map-link-hitarea')
            .attr('fill', '#e9ecef')
            .attr('stroke', '#999')
            .attr('stroke-width', 1.5);
        _clearMapSelection({ closePanel: false });

        // Guarded: this is now the only way out of the panel, so a pending
        // edit must be flushed rather than dropped.
        if (typeof guardedCloseSharedPanel === 'function') {
            guardedCloseSharedPanel();
        }
    });

    function startMapMarquee(event) {
        if (mapConnectionMode) return;
        if (!_isMapSelectGesture(event)) return;
        if (event.button !== 0) return;
        if (event.target.tagName !== 'svg') return;

        event.preventDefault();
        event.stopPropagation();
        const fromSelectMode = mapSelectMode;

        const svgNode = svg.node();
        const pointerToGraph = (evt) => d3.zoomTransform(svgNode).invert(d3.pointer(evt, svgNode));
        const [startX, startY] = pointerToGraph(event);
        const marquee = g.append('rect')
            .attr('class', 'map-selection-marquee')
            .attr('x', startX)
            .attr('y', startY)
            .attr('width', 0)
            .attr('height', 0);
        let cancelled = false;

        // Remove listeners and the rubber band only; never touch the selection.
        function cleanup() {
            window.removeEventListener('pointermove', onMove);
            window.removeEventListener('pointerup', onUp);
            window.removeEventListener('pointercancel', onCancel);
            window.removeEventListener('blur', onCancel);
            svgNode.removeEventListener('pointerleave', onCancel);
            marquee.remove();
        }

        function onCancel() {
            cleanup();
        }

        function onMove(evt) {
            if (!fromSelectMode && !_isMapSelectGesture(evt)) {
                // Modifier released mid-drag: abandon the marquee but keep
                // listening so the trailing pointerup/click is still absorbed.
                cancelled = true;
                marquee.attr('width', 0).attr('height', 0);
                return;
            }
            if (cancelled) return;
            const [curX, curY] = pointerToGraph(evt);
            const x = Math.min(startX, curX);
            const y = Math.min(startY, curY);
            marquee
                .attr('x', x)
                .attr('y', y)
                .attr('width', Math.abs(curX - startX))
                .attr('height', Math.abs(curY - startY));
        }

        function onUp(evt) {
            const [endX, endY] = pointerToGraph(evt);
            const minX = Math.min(startX, endX);
            const maxX = Math.max(startX, endX);
            const minY = Math.min(startY, endY);
            const maxY = Math.max(startY, endY);
            cleanup();
            // The browser dispatches a click on the canvas right after this
            // pointerup; it must neither clear a fresh selection nor undo a
            // no-op marquee.
            _suppressNextMapCanvasClick();
            if (cancelled || (!fromSelectMode && !_isMapSelectGesture(evt))) return;
            const namesInside = ontologyMapNodes
                .filter(n => !n.isGroup
                    && n.x >= minX && n.x <= maxX && n.y >= minY && n.y <= maxY)
                .map(n => n.name);
            if (namesInside.length) {
                _setMapSelection(namesInside);
            }
        }

        window.addEventListener('pointermove', onMove);
        window.addEventListener('pointerup', onUp);
        window.addEventListener('pointercancel', onCancel);
        window.addEventListener('blur', onCancel);
        svgNode.addEventListener('pointerleave', onCancel);
    }

    svg.on('pointerdown', function(event) {
        // A new gesture starts: any leftover guard from a previous marquee
        // (e.g. macOS Control-click never produces a trailing click) must go.
        _mapCanvasClickSuppressUntil = 0;
        startMapMarquee(event);
    });
    
    svg.on('contextmenu', function(event) {
        // Control-drag starts the marquee on pointerdown; suppress the
        // synthesised contextmenu so the Canvas menu does not cancel it.
        if (event.ctrlKey || event.metaKey) {
            event.preventDefault();
            return;
        }
        // Only show canvas context menu if clicking on background (not on a node)
        if (event.target.tagName === 'svg' || event.target.closest('g.map-node') === null) {
            event.preventDefault();
            hideMapContextMenu();
            if (window.isActiveVersion !== false) {
                showMapCanvasContextMenu(event, container, nodes);
            }
        }
    });

    // Update positions on simulation tick
    ontologyMapSimulation.on('tick', () => {
        // Regular relationship links (curved paths when multiple between same entities)
        relationshipLinkElements.attr('d', d => {
            const sx = d.source.x, sy = d.source.y;
            const tx = d.target.x, ty = d.target.y;
            
            // Calculate curve offset for multiple links between same entities
            if (d.linkCount > 1) {
                // Calculate offset: distribute links evenly around the center
                const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                
                // Calculate midpoint and perpendicular offset
                const mx = (sx + tx) / 2;
                const my = (sy + ty) / 2;
                
                // Perpendicular direction — normalize to canonical (sorted) order so that
                // opposite-direction links between the same entity pair are offset to
                // opposite sides of the midpoint rather than both curving the same way.
                const rawDx = tx - sx;
                const rawDy = ty - sy;
                const canonFwd = d.source.id <= d.target.id;
                const dx = canonFwd ? rawDx : -rawDx;
                const dy = canonFwd ? rawDy : -rawDy;
                const len = Math.sqrt(dx * dx + dy * dy) || 1;
                const px = -dy / len;
                const py = dx / len;
                
                // Control point with offset
                const cx = mx + px * offset;
                const cy = my + py * offset;
                
                return `M${sx},${sy} Q${cx},${cy} ${tx},${ty}`;
            }
            
            // Single link - straight line
            return `M${sx},${sy} L${tx},${ty}`;
        });

        // Inheritance links (straight lines)
        inheritanceLinkElements.attr('d', d => {
            const sx = d.source.x, sy = d.source.y;
            const tx = d.target.x, ty = d.target.y;
            return `M${sx},${sy} L${tx},${ty}`;
        });

        // Self-loop links - position at different angles around the node
        selfLoopElements.attr('d', d => {
            const node = typeof d.source === 'object' ? d.source : nodes.find(n => n.id === d.source);
            if (!node) return '';
            
            const x = node.x;
            const y = node.y;
            const r = 35; // Loop radius
            const loopSize = 40; // Size of the loop
            
            // Calculate angle based on self-loop index
            // Distribute loops around the node: right (0°), top-right (45°), top (-90°), etc.
            const baseAngle = -45; // Start from top-right
            const angleStep = 90; // 90 degrees apart
            const angle = (baseAngle + d.selfLoopIndex * angleStep) * Math.PI / 180;
            
            // Calculate start and end points on the node edge
            const nodeRadius = 25;
            const startAngle = angle - 0.3;
            const endAngle = angle + 0.3;
            
            const startX = x + Math.cos(startAngle) * nodeRadius;
            const startY = y + Math.sin(startAngle) * nodeRadius;
            const endX = x + Math.cos(endAngle) * nodeRadius;
            const endY = y + Math.sin(endAngle) * nodeRadius;
            
            // Control points for the loop curve
            const ctrlDist = loopSize + r;
            const ctrlX = x + Math.cos(angle) * ctrlDist;
            const ctrlY = y + Math.sin(angle) * ctrlDist;
            
            // Two control points for smooth loop
            const ctrl1X = x + Math.cos(startAngle) * ctrlDist;
            const ctrl1Y = y + Math.sin(startAngle) * ctrlDist;
            const ctrl2X = x + Math.cos(endAngle) * ctrlDist;
            const ctrl2Y = y + Math.sin(endAngle) * ctrlDist;
            
            return `M${startX},${startY} C${ctrl1X},${ctrl1Y} ${ctrl2X},${ctrl2Y} ${endX},${endY}`;
        });

        // Link hitareas at midpoint
        linkHitareas.attr('cx', d => {
            const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
            const targetId = typeof d.target === 'object' ? d.target.id : d.target;
            const sx = typeof d.source === 'object' ? d.source.x : nodes.find(n => n.id === d.source)?.x || 0;
            const tx = typeof d.target === 'object' ? d.target.x : nodes.find(n => n.id === d.target)?.x || 0;
            
            // For self-loops, position based on angle
            if (sourceId === targetId) {
                const baseAngle = -45;
                const angleStep = 90;
                const angle = (baseAngle + (d.selfLoopIndex || 0) * angleStep) * Math.PI / 180;
                const dist = 55;
                return sx + Math.cos(angle) * dist;
            }
            
            // For curved links
            if (d.linkCount > 1) {
                const mx = (sx + tx) / 2;
                const rawDx = tx - sx;
                const rawDy = (typeof d.target === 'object' ? d.target.y : nodes.find(n => n.id === d.target)?.y || 0) - 
                           (typeof d.source === 'object' ? d.source.y : nodes.find(n => n.id === d.source)?.y || 0);
                const canonFwd = d.source.id <= d.target.id;
                const dx = canonFwd ? rawDx : -rawDx;
                const dy = canonFwd ? rawDy : -rawDy;
                const len = Math.sqrt(dx * dx + dy * dy) || 1;
                // Use offset*0.5: hitarea sits at the Bezier midpoint (t=0.5),
                // not at the control point (which is 2× further from the straight midpoint).
                const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                return mx + (-dy / len) * offset * 0.5;
            }
            
            return (sx + tx) / 2;
        })
        .attr('cy', d => {
            const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
            const targetId = typeof d.target === 'object' ? d.target.id : d.target;
            const sy = typeof d.source === 'object' ? d.source.y : nodes.find(n => n.id === d.source)?.y || 0;
            const ty = typeof d.target === 'object' ? d.target.y : nodes.find(n => n.id === d.target)?.y || 0;
            
            // For self-loops, position based on angle
            if (sourceId === targetId) {
                const baseAngle = -45;
                const angleStep = 90;
                const angle = (baseAngle + (d.selfLoopIndex || 0) * angleStep) * Math.PI / 180;
                const dist = 55;
                return sy + Math.sin(angle) * dist;
            }
            
            // For curved links
            if (d.linkCount > 1) {
                const my = (sy + ty) / 2;
                const rawDx = (typeof d.target === 'object' ? d.target.x : nodes.find(n => n.id === d.target)?.x || 0) - 
                           (typeof d.source === 'object' ? d.source.x : nodes.find(n => n.id === d.source)?.x || 0);
                const rawDy = ty - sy;
                const canonFwd = d.source.id <= d.target.id;
                const dx = canonFwd ? rawDx : -rawDx;
                const dy = canonFwd ? rawDy : -rawDy;
                const len = Math.sqrt(dx * dx + dy * dy) || 1;
                // Use offset*0.5: Bezier midpoint (t=0.5) is halfway between straight midpoint
                // and control point, so half the perpendicular displacement.
                const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                return my + (dx / len) * offset * 0.5;
            }
            
            return (sy + ty) / 2;
        });

        // Link labels at midpoint (offset for curved links)
        linkLabels.each(function(d) {
            const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
            const targetId = typeof d.target === 'object' ? d.target.id : d.target;
            const sx = typeof d.source === 'object' ? d.source.x : 0;
            const sy = typeof d.source === 'object' ? d.source.y : 0;
            const tx = typeof d.target === 'object' ? d.target.x : 0;
            const ty = typeof d.target === 'object' ? d.target.y : 0;
            
            let mx, my;
            
            // For self-loops - position based on angle
            if (sourceId === targetId) {
                const node = typeof d.source === 'object' ? d.source : nodes.find(n => n.id === d.source);
                const baseAngle = -45;
                const angleStep = 90;
                const angle = (baseAngle + (d.selfLoopIndex || 0) * angleStep) * Math.PI / 180;
                const dist = 75; // Further out for label
                mx = node.x + Math.cos(angle) * dist;
                my = node.y + Math.sin(angle) * dist;
                d3.select(this).classed('self-loop-label', true);
            } else if (d.linkCount > 1) {
                // Curved link — normalize direction to canonical (sorted) order so that
                // opposite-direction links offset to opposite sides of the midpoint.
                mx = (sx + tx) / 2;
                my = (sy + ty) / 2;
                const rawDx = tx - sx;
                const rawDy = ty - sy;
                const canonFwd = d.source.id <= d.target.id;
                const dx = canonFwd ? rawDx : -rawDx;
                const dy = canonFwd ? rawDy : -rawDy;
                const len = Math.sqrt(dx * dx + dy * dy) || 1;
                const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                // Use offset*0.5: label sits at the Bezier midpoint (t=0.5), not at
                // the control point (which is 2× further from the straight midpoint).
                mx += (-dy / len) * offset * 0.5;
                my += (dx / len) * offset * 0.5;
            } else {
                // Straight link
                mx = (sx + tx) / 2;
                my = (sy + ty) / 2 - 8;
            }
            
            d3.select(this)
                .attr('x', mx)
                .attr('y', my);
        });

        // Node positions
        nodeElements.attr('transform', d => `translate(${d.x},${d.y})`);
    });

    // Add legend
    const legend = document.createElement('div');
    legend.className = 'map-legend';
    legend.innerHTML = `
        <div class="map-legend-title">Legend</div>
        <div class="map-legend-item">
            <div class="map-legend-line"></div>
            <span>Relationship</span>
        </div>
        <div class="map-legend-item">
            <div class="map-legend-line inherited"></div>
            <span>Inherited relation</span>
        </div>
        <div class="map-legend-item">
            <div class="map-legend-line inheritance"></div>
            <span>Inheritance</span>
        </div>
    `;
    container.appendChild(legend);

    // Bind zoom controls
    document.getElementById('mapZoomIn')?.addEventListener('click', () => {
        svg.transition().duration(300).call(ontologyMapZoom.scaleBy, 1.3);
    });

    document.getElementById('mapZoomOut')?.addEventListener('click', () => {
        svg.transition().duration(300).call(ontologyMapZoom.scaleBy, 0.7);
    });

    document.getElementById('mapResetView')?.addEventListener('click', () => {
        svg.transition().duration(500).call(ontologyMapZoom.transform, d3.zoomIdentity);
    });
    
    // Auto-map icons button (use onclick to avoid stacking listeners on map refresh)
    const autoIconsBtn = document.getElementById('mapAutoAssignIcons');
    if (autoIconsBtn) autoIconsBtn.onclick = () => autoAssignEntityIcons();

    // Toggle inheritance links visibility
    const toggleInheritanceBtn = document.getElementById('mapToggleInheritance');
    if (toggleInheritanceBtn) {
        // Restore persisted state (default: shown)
        let inheritanceVisible = sessionStorage.getItem('mapInheritanceVisible') !== 'false';
        const applyInheritanceVisibility = () => {
            inheritanceLinkElements.style('display', inheritanceVisible ? null : 'none');
            toggleInheritanceBtn.classList.toggle('active', inheritanceVisible);
            toggleInheritanceBtn.title = inheritanceVisible
                ? 'Hide inheritance links'
                : 'Show inheritance links';
            // Keep legend item in sync
            const legendInheritance = legend.querySelector('.map-legend-item:last-child');
            if (legendInheritance) legendInheritance.style.display = inheritanceVisible ? '' : 'none';
        };
        applyInheritanceVisibility();
        toggleInheritanceBtn.onclick = () => {
            inheritanceVisible = !inheritanceVisible;
            sessionStorage.setItem('mapInheritanceVisible', inheritanceVisible);
            applyInheritanceVisibility();
        };
    }

    // Discussion button — opens the whole-ontology thread with a picker to
    // optionally re-tag the comment to a specific entity/relationship.
    const discussBtn = document.getElementById('mapDiscuss');
    if (discussBtn) discussBtn.onclick = () => openOntologyDiscussion();

    // Reset layout button - clears saved positions and re-runs simulation
    document.getElementById('mapResetLayout')?.addEventListener('click', async () => {
        // Clear fixed positions
        nodes.forEach(node => {
            node.fx = null;
            node.fy = null;
        });
        
        // Re-enable animation and forces for auto-arrange
        ontologyMapSimulation
            .alphaDecay(0.0228) // Default decay
            .velocityDecay(0.4) // Default velocity decay
            .force('charge', d3.forceManyBody().strength(-400))
            .force('collision', d3.forceCollide().radius(60))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .alpha(1)
            .restart();
        
        // After simulation settles, fix positions and save
        setTimeout(() => {
            nodes.forEach(node => {
                node.fx = node.x;
                node.fy = node.y;
            });
            ontologyMapSimulation
                .alphaDecay(1)
                .velocityDecay(1);
            scheduleMapAutoSave(nodes);
        }, 3000);
        
        // Clear saved layout initially
        await saveMapLayout([]);
        
        if (typeof showNotification === 'function') {
            showNotification('Layout reset - nodes will auto-arrange', 'info', 2000);
        }
    });
    
    // Wire search popup to the search button
    initMapSearch(nodes);

    } catch (error) {
        console.error('Error initializing ontology model:', error);
    } finally {
        // Always hide loading after a short delay
        setTimeout(() => {
            showOntologyMapLoading(false);
        }, 500);
    }
}

/**
 * Initialise the entity search popup.
 * Called at the end of initOntologyMap once nodes are available.
 */
function initMapSearch(nodes) {
    const btn   = document.getElementById('mapSearchBtn');
    const popup = document.getElementById('mapSearchPopup');
    const sel   = document.getElementById('mapSearchSelect');
    const goBtn = document.getElementById('mapSearchGo');
    const close = document.getElementById('mapSearchClose');
    if (!btn || !popup || !sel || !goBtn) return;

    // Re-populate the dropdown every time the popup opens
    function openPopup() {
        sel.innerHTML = '<option value="" disabled selected>Select an entity…</option>';
        const sorted = [...nodes].sort((a, b) => (a.label || a.name).localeCompare(b.label || b.name));
        sorted.forEach(n => {
            const opt = document.createElement('option');
            opt.value = n.name;
            opt.textContent = `${n.icon || '📦'} ${n.label || n.name}`;
            sel.appendChild(opt);
        });
        popup.classList.remove('map-search-popup--hidden');
        setTimeout(() => sel.focus(), 50);
    }

    function closePopup() {
        popup.classList.add('map-search-popup--hidden');
    }

    // Toggle on button click
    btn.onclick = (e) => {
        e.stopPropagation();
        popup.classList.contains('map-search-popup--hidden') ? openPopup() : closePopup();
    };

    if (close) close.onclick = closePopup;

    // Enter key in select triggers focus
    sel.addEventListener('keydown', (e) => { if (e.key === 'Enter') goBtn.click(); });

    goBtn.onclick = () => {
        const name = sel.value;
        if (!name) return;
        focusMapEntity(name);
        closePopup();
    };

    // Dismiss on outside click
    document.addEventListener('click', (e) => {
        if (!popup.classList.contains('map-search-popup--hidden') && !popup.contains(e.target) && e.target !== btn) {
            closePopup();
        }
    });
}

/**
 * Centre the canvas on a named entity and trigger its neighbourhood highlight —
 * identical to clicking the node directly.
 */
function focusMapEntity(name) {
    const node = ontologyMapNodes.find(n => n.name === name);
    if (!node) return;

    // Same visual effect as clicking the node
    if (_mapClearHighlights)       _mapClearHighlights();
    if (_mapHighlightNeighborhood) _mapHighlightNeighborhood(name);

    // Pan + zoom to centre the node
    if (ontologyMapZoom && ontologyMapSvg) {
        const svgEl = ontologyMapSvg.node();
        const rect  = svgEl ? svgEl.getBoundingClientRect() : null;
        const w = (rect && rect.width  > 0) ? rect.width  : (svgEl ? +svgEl.getAttribute('width')  : 800);
        const h = (rect && rect.height > 0) ? rect.height : (svgEl ? +svgEl.getAttribute('height') : 600);
        const scale = 1.4;
        const tx = w / 2 - node.x * scale;
        const ty = h / 2 - node.y * scale;
        ontologyMapSvg.transition()
            .duration(600)
            .call(ontologyMapZoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
    }
}


/**
 * Create, switch to and populate a Business View (design-view) from a set of
 * Studio entities, then stay on Studio with that view selected. Shared by the
 * single-entity (1-hop) and multi-select (selected-only) entry points.
 *
 * Everything outside `entityNames` is hidden in the saved view; the relationship
 * and inheritance arrays are exactly `relLinks` / `inhLinks`.
 *
 * @param {Object}   spec
 * @param {string}   spec.baseName     Preferred name; a numeric suffix is added if taken.
 * @param {string[]} spec.entityNames  Entities in layout order.
 * @param {Array<{name: string, source: string, target: string}>} spec.relLinks
 * @param {Array<{source: string, target: string}>} spec.inhLinks
 * @param {function(number, number, number, number): {x: number, y: number}} spec.positionFor
 *        Placement for an entity, called as (index, cx, cy, radius): the
 *        entity's index in `entityNames`, the circle centre, and its radius.
 * @param {string}   spec.logLabel     Caller name for diagnostics.
 */
async function _createMapBusinessView(spec) {
    const { baseName, entityNames, relLinks, inhLinks, positionFor, logLabel } = spec;
    try {
        // ── 1. Unique view name ─────────────────────────────────────────────
        let viewName = baseName;
        let existingViews = [];
        try {
            const viewsResp = await fetch('/domain/design-views');
            if (viewsResp.ok) {
                const viewsData = await viewsResp.json();
                existingViews = viewsData.views || [];
            }
        } catch (_) { /* ignore – proceed with attempted name */ }

        if (existingViews.includes(viewName)) {
            let suffix = 1;
            while (existingViews.includes(`${viewName}_${suffix}`)) suffix++;
            viewName = `${viewName}_${suffix}`;
        }

        // ── 2. Entities with circular layout ────────────────────────────────
        const cx = 450, cy = 280, radius = 230;
        const visibleNames = new Set(entityNames);

        const entityIdMap = new Map();
        entityNames.forEach((name, i) => {
            entityIdMap.set(name, `ent_${Date.now()}_${i}`);
        });

        const entities = entityNames.map((name, i) => {
            const { x, y } = positionFor(i, cx, cy, radius);
            // Try to get properties from the node data already loaded in the map
            const nodeData = ontologyMapNodes.find(n => n.name === name && !n.isGroup);
            return {
                id: entityIdMap.get(name),
                name,
                x,
                y,
                properties: nodeData ? (nodeData.dataProperties || null) : null,
                color: null,
            };
        });

        // ── 3. Relationship and inheritance arrays ──────────────────────────
        const relationships = relLinks
            .filter(l => entityIdMap.has(l.source) && entityIdMap.has(l.target))
            .map((l, i) => ({
                id: `rel_${Date.now()}_${i}`,
                name: l.name,
                label: l.name,
                sourceEntityId: entityIdMap.get(l.source),
                targetEntityId: entityIdMap.get(l.target),
                sourceAnchor: 'right',
                targetAnchor: 'left',
            }));

        const inheritances = inhLinks
            .filter(l => entityIdMap.has(l.source) && entityIdMap.has(l.target))
            .map((l, i) => ({
                id: `inh_${Date.now()}_${i}`,
                sourceEntityId: entityIdMap.get(l.source),
                targetEntityId: entityIdMap.get(l.target),
                direction: 'forward',
            }));

        // ── 4. Visibility — hide everything outside the chosen entities ─────
        // Use the full ontology class list so collapsed group members are still
        // recorded as hidden when creating a view from a filtered canvas.
        const allClassNames = (OntologyState?.config?.classes || []).map(c => c.name);
        const hiddenEntities = allClassNames.filter(name => !visibleNames.has(name));

        const hiddenRelationships = [];
        const hiddenInheritances = [];
        ontologyMapLinks.forEach(link => {
            const s = _resolveLinkEndpoint(link.source);
            const t = _resolveLinkEndpoint(link.target);
            if (String(s).startsWith('__group__') || String(t).startsWith('__group__')) return;
            if (!visibleNames.has(s) || !visibleNames.has(t)) {
                if (link.type === 'inheritance') {
                    hiddenInheritances.push({ source: s, target: t });
                } else {
                    hiddenRelationships.push({ name: link.name, source: s, target: t });
                }
            }
        });

        const layoutData = {
            entities,
            relationships,
            inheritances,
            visibility: {
                hiddenEntities,
                hiddenRelationships,
                hiddenInheritances,
                collapsedGroups: Array.from(_studioCollapsedGroups),
            },
        };

        // ── 5. Create → switch → save ────────────────────────────────────────
        const createResp = await fetch('/domain/design-views/create', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: viewName }),
        });
        if (!createResp.ok) {
            const err = await createResp.json().catch(() => ({}));
            throw new Error(err.detail || `Create view failed (${createResp.status})`);
        }

        const switchResp = await fetch('/domain/design-views/switch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: viewName }),
        });
        if (!switchResp.ok) {
            const err = await switchResp.json().catch(() => ({}));
            throw new Error(err.detail || `Switch view failed (${switchResp.status})`);
        }

        const saveResp = await fetch('/domain/design-views/save-current', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(layoutData),
        });
        if (!saveResp.ok) {
            const err = await saveResp.json().catch(() => ({}));
            throw new Error(err.detail || `Save view failed (${saveResp.status})`);
        }

        // ── 6. Stay on Studio and filter the canvas to the new view ────────
        _studioActiveView = viewName;
        _studioViewLayout = layoutData;
        await refreshStudioViewSelect();
        await applyStudioView(viewName);

        console.log(`[Map] Business view "${viewName}" created with ${entities.length} entities.`);
    } catch (err) {
        console.error(`[Map] ${logLabel} failed:`, err);
        if (typeof showNotification === 'function') {
            showNotification(`Could not create Business View: ${err.message}`, 'danger');
        }
    }
}

/**
 * Create a Business View (design-view) from a single entity — 1-hop neighbourhood.
 * The view is named "Auto_<entityName>", with numeric suffixes if the name already exists.
 */
async function createBusinessViewFromEntity(entityData) {
    const selectedName = entityData.name;
    const neighbourNames = new Set();
    const relLinks = [];    // {name, source, target}  – ObjectProperty / datatype
    const inhLinks = [];    // {source, target}         – inheritance

    ontologyMapLinks.forEach(link => {
        const srcName = _resolveLinkEndpoint(link.source);
        const tgtName = _resolveLinkEndpoint(link.target);
        if (srcName !== selectedName && tgtName !== selectedName) return;
        const neighbourName = srcName === selectedName ? tgtName : srcName;
        neighbourNames.add(neighbourName);
        if (link.type === 'inheritance') {
            inhLinks.push({ source: srcName, target: tgtName });
        } else {
            relLinks.push({ name: link.name, source: srcName, target: tgtName });
        }
    });

    await _createMapBusinessView({
        baseName: `Auto_${selectedName}`,
        entityNames: [selectedName, ...Array.from(neighbourNames)],
        relLinks,
        inhLinks,
        // Selected entity at the centre, neighbours evenly spaced on the circle.
        positionFor: (i, cx, cy, radius) => {
            if (i === 0) return { x: cx, y: cy };
            const angle = (2 * Math.PI / neighbourNames.size) * (i - 1);
            return {
                x: Math.round(cx + radius * Math.cos(angle)),
                y: Math.round(cy + radius * Math.sin(angle)),
            };
        },
        logLabel: 'createBusinessViewFromEntity',
    });
}

/**
 * Create a Business View from the current multi-selection.
 * Contains exactly the selected entities plus object-property / inheritance
 * links whose both endpoints are selected. Named Auto_Selection[_N].
 * @param {string[]} names
 */
async function createBusinessViewFromSelection(names) {
    if (!names || !names.length) return;
    const visibleNames = new Set(names);
    const relLinks = [];
    const inhLinks = [];

    ontologyMapLinks.forEach(link => {
        const source = _resolveLinkEndpoint(link.source);
        const target = _resolveLinkEndpoint(link.target);
        if (!(visibleNames.has(source) && visibleNames.has(target))) return;
        if (link.type === 'inheritance') {
            inhLinks.push({ source, target });
        } else {
            relLinks.push({ name: link.name, source, target });
        }
    });

    const entityNames = Array.from(visibleNames);
    await _createMapBusinessView({
        baseName: 'Auto_Selection',
        entityNames,
        relLinks,
        inhLinks,
        // Every selected entity on one circle (a lone entity sits at angle 0).
        positionFor: (i, cx, cy, radius) => {
            const angle = entityNames.length === 1
                ? 0
                : (2 * Math.PI / entityNames.length) * i;
            return {
                x: Math.round(cx + radius * Math.cos(angle)),
                y: Math.round(cy + radius * Math.sin(angle)),
            };
        },
        logLabel: 'createBusinessViewFromSelection',
    });
}


function showMapMultiSelectContextMenu(event, names, container) {
    hideMapContextMenu();

    const menu = document.createElement('div');
    menu.id = 'mapContextMenu';
    menu.className = 'map-context-menu';
    menu.innerHTML = `
        <div class="map-context-header">
            <span class="map-context-icon"><i class="bi bi-collection"></i></span>
            <span class="map-context-title">${names.length} entities</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item" data-action="create-business-view">
            <i class="bi bi-layout-text-window"></i>
            <span>Create Business View</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item map-context-danger" data-action="delete">
            <i class="bi bi-trash"></i>
            <span>Delete ${names.length} entities</span>
        </div>
    `;

    const containerRect = container.getBoundingClientRect();
    let x = event.clientX - containerRect.left;
    let y = event.clientY - containerRect.top;

    menu.style.left = x + 'px';
    menu.style.top = y + 'px';
    container.appendChild(menu);

    const menuRect = menu.getBoundingClientRect();
    if (menuRect.right > containerRect.right) {
        menu.style.left = (x - menuRect.width) + 'px';
    }
    if (menuRect.bottom > containerRect.bottom) {
        menu.style.top = (y - menuRect.height) + 'px';
    }

    // Defense in depth: the menu closes on every selection change, but act on
    // the live selection rather than the names captured when it opened.
    menu.querySelector('[data-action="create-business-view"]').addEventListener('click', async () => {
        hideMapContextMenu();
        const live = _liveMapSelectionNames();
        if (live.length < 2) return;
        await createBusinessViewFromSelection(live);
    });

    menu.querySelector('[data-action="delete"]').addEventListener('click', async () => {
        hideMapContextMenu();
        const live = _liveMapSelectionNames();
        if (live.length < 2) return;
        await deleteEntitiesFromMap(live);
    });

    setTimeout(() => {
        document.addEventListener('click', hideMapContextMenuOnClickOutside);
        document.addEventListener('contextmenu', hideMapContextMenuOnClickOutside);
    }, 0);
}


function showMapContextMenu(event, entityData, container) {
    // Remove existing menu
    hideMapContextMenu();
    
    // Create context menu
    const menu = document.createElement('div');
    menu.id = 'mapContextMenu';
    menu.className = 'map-context-menu';
    menu.innerHTML = `
        <div class="map-context-header">
            <span class="map-context-icon">${entityData.icon || '📦'}</span>
            <span class="map-context-title">${entityData.name}</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item" data-action="open-tab-details">
            <i class="bi bi-info-circle"></i>
            <span>General</span>
        </div>
        <div class="map-context-item" data-action="open-tab-attributes">
            <i class="bi bi-tags"></i>
            <span>Attributes</span>
        </div>
        <div class="map-context-item" data-action="open-tab-model">
            <i class="bi bi-diagram-2"></i>
            <span>Model</span>
        </div>
        <div class="map-context-item" data-action="open-tab-behaviour">
            <i class="bi bi-lightning"></i>
            <span>Behaviour</span>
        </div>
        <div class="map-context-item" data-action="open-tab-links">
            <i class="bi bi-link-45deg"></i>
            <span>Links</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item" data-action="create-relationship">
            <i class="bi bi-arrow-right"></i>
            <span>Create Relationship</span>
        </div>
        <div class="map-context-item" data-action="inherit-from">
            <i class="bi bi-diagram-3"></i>
            <span>Inherit from...</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item" data-action="create-business-view">
            <i class="bi bi-layout-text-window"></i>
            <span>Create Business View</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item map-context-danger" data-action="delete">
            <i class="bi bi-trash"></i>
            <span>Delete Entity</span>
        </div>
    `;
    
    // Position the menu
    const containerRect = container.getBoundingClientRect();
    let x = event.clientX - containerRect.left;
    let y = event.clientY - containerRect.top;
    
    // Ensure menu stays within container bounds
    menu.style.left = x + 'px';
    menu.style.top = y + 'px';
    
    container.appendChild(menu);
    
    // Adjust if menu goes off-screen
    const menuRect = menu.getBoundingClientRect();
    if (menuRect.right > containerRect.right) {
        menu.style.left = (x - menuRect.width) + 'px';
    }
    if (menuRect.bottom > containerRect.bottom) {
        menu.style.top = (y - menuRect.height) + 'px';
    }
    
    // Handle menu item clicks
    ['details', 'attributes', 'model', 'behaviour', 'links'].forEach(tab => {
        menu.querySelector(`[data-action="open-tab-${tab}"]`).addEventListener('click', () => {
            hideMapContextMenu();
            if (typeof editClassByName === 'function') {
                editClassByName(entityData.name, tab);
            }
        });
    });

    menu.querySelector('[data-action="delete"]').addEventListener('click', async () => {
        hideMapContextMenu();
        await deleteEntityFromMap(entityData.name);
    });
    
    menu.querySelector('[data-action="create-relationship"]').addEventListener('click', () => {
        hideMapContextMenu();
        startMapConnectionMode(entityData, container, 'relationship');
    });
    
    menu.querySelector('[data-action="inherit-from"]').addEventListener('click', () => {
        hideMapContextMenu();
        startMapConnectionMode(entityData, container, 'inheritance');
    });

    menu.querySelector('[data-action="create-business-view"]').addEventListener('click', async () => {
        hideMapContextMenu();
        await createBusinessViewFromEntity(entityData);
    });
    
    // Close menu when clicking outside
    setTimeout(() => {
        document.addEventListener('click', hideMapContextMenuOnClickOutside);
        document.addEventListener('contextmenu', hideMapContextMenuOnClickOutside);
    }, 0);
}

/**
 * Show a right-click context menu for a relationship link, offering
 * shortcuts straight to each tab of its edit panel (General, Constraints).
 */
function showMapRelationshipContextMenu(event, linkData, container) {
    // Remove existing menu
    hideMapContextMenu();

    const menu = document.createElement('div');
    menu.id = 'mapContextMenu';
    menu.className = 'map-context-menu';
    menu.innerHTML = `
        <div class="map-context-header">
            <span class="map-context-icon"><i class="bi bi-arrow-left-right"></i></span>
            <span class="map-context-title">${linkData.label || linkData.name}</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item" data-action="open-tab-details">
            <i class="bi bi-info-circle"></i>
            <span>General</span>
        </div>
        <div class="map-context-item" data-action="open-tab-constraints">
            <i class="bi bi-sliders"></i>
            <span>Constraints</span>
        </div>
    `;

    // Position the menu
    const containerRect = container.getBoundingClientRect();
    let x = event.clientX - containerRect.left;
    let y = event.clientY - containerRect.top;

    menu.style.left = x + 'px';
    menu.style.top = y + 'px';

    container.appendChild(menu);

    // Adjust if menu goes off-screen
    const menuRect = menu.getBoundingClientRect();
    if (menuRect.right > containerRect.right) {
        menu.style.left = (x - menuRect.width) + 'px';
    }
    if (menuRect.bottom > containerRect.bottom) {
        menu.style.top = (y - menuRect.height) + 'px';
    }

    // Handle menu item clicks
    ['details', 'constraints'].forEach(tab => {
        menu.querySelector(`[data-action="open-tab-${tab}"]`).addEventListener('click', () => {
            hideMapContextMenu();
            if (typeof editPropertyByName === 'function') {
                editPropertyByName(linkData.name, tab);
            }
        });
    });

    // Close menu when clicking outside
    setTimeout(() => {
        document.addEventListener('click', hideMapContextMenuOnClickOutside);
        document.addEventListener('contextmenu', hideMapContextMenuOnClickOutside);
    }, 0);
}

/**
 * Hide the map context menu
 */
function hideMapContextMenu() {
    const menu = document.getElementById('mapContextMenu');
    if (menu) {
        menu.remove();
    }
    document.removeEventListener('click', hideMapContextMenuOnClickOutside);
    document.removeEventListener('contextmenu', hideMapContextMenuOnClickOutside);
}

/**
 * Handler to hide context menu when clicking outside
 */
function hideMapContextMenuOnClickOutside(event) {
    const menu = document.getElementById('mapContextMenu');
    if (menu && !menu.contains(event.target)) {
        hideMapContextMenu();
    }
}

/**
 * Show canvas context menu (right-click on empty area)
 */
function showMapCanvasContextMenu(event, container, nodes) {
    // Remove existing menu
    hideMapContextMenu();
    
    // Create context menu
    const menu = document.createElement('div');
    menu.id = 'mapContextMenu';
    menu.className = 'map-context-menu';
    menu.innerHTML = `
        <div class="map-context-header">
            <span class="map-context-icon">🗺️</span>
            <span class="map-context-title">Canvas</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item" data-action="create-entity">
            <i class="bi bi-plus-lg text-primary"></i>
            <span>Create Entity</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item" data-action="zoom-in">
            <i class="bi bi-zoom-in text-secondary"></i>
            <span>Zoom In</span>
        </div>
        <div class="map-context-item" data-action="zoom-out">
            <i class="bi bi-zoom-out text-secondary"></i>
            <span>Zoom Out</span>
        </div>
        <div class="map-context-divider"></div>
        <div class="map-context-item" data-action="auto-layout">
            <i class="bi bi-grid-3x3 text-primary"></i>
            <span>Auto-Layout</span>
        </div>
    `;
    
    // Position the menu
    const containerRect = container.getBoundingClientRect();
    let x = event.clientX - containerRect.left;
    let y = event.clientY - containerRect.top;
    
    menu.style.left = x + 'px';
    menu.style.top = y + 'px';
    
    container.appendChild(menu);
    
    // Adjust if menu goes off-screen
    const menuRect = menu.getBoundingClientRect();
    if (menuRect.right > containerRect.right) {
        menu.style.left = (x - menuRect.width) + 'px';
    }
    if (menuRect.bottom > containerRect.bottom) {
        menu.style.top = (y - menuRect.height) + 'px';
    }
    
    // Handle menu item clicks
    menu.querySelector('[data-action="create-entity"]')?.addEventListener('click', () => {
        hideMapContextMenu();
        // Use the entity edit panel to create a new entity
        if (typeof openEntityPanel === 'function') {
            openEntityPanel({ onSave: () => { initOntologyMap(); } });
        }
    });
    
    menu.querySelector('[data-action="zoom-in"]')?.addEventListener('click', () => {
        hideMapContextMenu();
        if (ontologyMapZoom) {
            const svg = d3.select(container).select('svg');
            svg.transition().duration(300).call(ontologyMapZoom.scaleBy, 1.3);
        }
    });
    
    menu.querySelector('[data-action="zoom-out"]')?.addEventListener('click', () => {
        hideMapContextMenu();
        if (ontologyMapZoom) {
            const svg = d3.select(container).select('svg');
            svg.transition().duration(300).call(ontologyMapZoom.scaleBy, 0.7);
        }
    });
    
    menu.querySelector('[data-action="auto-layout"]')?.addEventListener('click', () => {
        hideMapContextMenu();
        // Trigger the reset layout button click
        document.getElementById('mapResetLayout')?.click();
    });
    
    // Close menu when clicking outside
    setTimeout(() => {
        document.addEventListener('click', hideMapContextMenuOnClickOutside);
        document.addEventListener('contextmenu', hideMapContextMenuOnClickOutside);
    }, 0);
}

/**
 * Delete an entity from the ontology
 */
async function deleteEntityFromMap(entityName) {
    // Show confirmation dialog
    const confirmed = await showConfirmDialog({
        title: 'Delete Entity',
        message: `Are you sure you want to delete the entity "${entityName}"? This will also remove any relationships connected to it.`,
        confirmText: 'Delete',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
    
    if (!confirmed) return;
    
    try {
        // Find and remove the entity from OntologyState
        if (typeof OntologyState !== 'undefined' && OntologyState.config) {
            const classIndex = OntologyState.config.classes.findIndex(c => c.name === entityName);
            if (classIndex >= 0) {
                // Remove the class
                OntologyState.config.classes.splice(classIndex, 1);
                
                // Remove any relationships (properties) connected to this entity
                OntologyState.config.properties = (OntologyState.config.properties || []).filter(p => 
                    p.domain !== entityName && p.range !== entityName
                );
                
                // Remove parent references in child classes
                OntologyState.config.classes.forEach(c => {
                    if (c.parent === entityName) {
                        delete c.parent;
                    }
                });
                
                // Save to session
                if (typeof saveConfigToSession === 'function') {
                    await saveConfigToSession();
                }
                
                // Refresh the map
                initOntologyMap();
                
                // Update other UI components if available
                if (typeof updateClassesList === 'function') {
                    updateClassesList();
                }
                if (typeof updatePropertiesList === 'function') {
                    updatePropertiesList();
                }
                
                showNotification(`Entity "${entityName}" deleted`, 'success');
            } else {
                showNotification('Entity not found', 'warning');
            }
        }
    } catch (error) {
        console.error('Error deleting entity:', error);
        showNotification('Error deleting entity: ' + error.message, 'error');
    }
}

/**
 * Delete several entities after a single confirmation.
 * @param {string[]} names
 */
async function deleteEntitiesFromMap(names) {
    if (!names || !names.length) return;
    if (typeof showConfirmDialog !== 'function') return;

    const confirmed = await showConfirmDialog({
        title: 'Delete entities',
        message: `Delete ${names.length} entities? Relationships connected to them will also be removed.`,
        confirmText: 'Delete',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });

    if (!confirmed) return;

    try {
        if (typeof OntologyState === 'undefined' || !OntologyState.config) return;

        const namesSet = new Set(names);
        const classesBefore = (OntologyState.config.classes || []).length;
        OntologyState.config.classes = (OntologyState.config.classes || []).filter(
            c => !namesSet.has(c.name)
        );
        const removedCount = classesBefore - OntologyState.config.classes.length;
        if (removedCount === 0) {
            if (typeof showNotification === 'function') {
                showNotification('Entities not found', 'warning');
            }
            return;
        }
        OntologyState.config.properties = (OntologyState.config.properties || []).filter(
            p => !namesSet.has(p.domain) && !namesSet.has(p.range)
        );
        OntologyState.config.classes.forEach(c => {
            if (namesSet.has(c.parent)) {
                delete c.parent;
            }
        });

        if (typeof saveConfigToSession === 'function') {
            await saveConfigToSession();
        }

        initOntologyMap();

        if (typeof updateClassesList === 'function') {
            updateClassesList();
        }
        if (typeof updatePropertiesList === 'function') {
            updatePropertiesList();
        }

        if (typeof showNotification === 'function') {
            showNotification(
                removedCount === 1 ? '1 entity deleted' : `${removedCount} entities deleted`,
                'success'
            );
        }
    } catch (error) {
        console.error('Error deleting entities:', error);
        if (typeof showNotification === 'function') {
            showNotification('Error deleting entities: ' + error.message, 'error');
        }
    }
}

/**
 * Show a floating delete button next to a selected relationship hitarea
 */
function showMapRelationshipActions(linkData, hitareaElement) {
    hideMapRelationshipActions();

    const container = document.getElementById('ontology-map-container');
    if (!container) return;

    const containerRect = container.getBoundingClientRect();
    const hitRect = hitareaElement.getBoundingClientRect();

    const btn = document.createElement('button');
    btn.id = 'mapRelActionBtn';
    btn.className = 'btn btn-sm btn-outline-danger';
    btn.title = 'Delete relationship';
    btn.innerHTML = '<i class="bi bi-trash"></i>';
    btn.style.cssText = 'position:absolute;z-index:20;padding:2px 6px;font-size:12px;';
    btn.style.left = (hitRect.left - containerRect.left + hitRect.width / 2 + 12) + 'px';
    btn.style.top = (hitRect.top - containerRect.top + hitRect.height / 2 - 12) + 'px';

    btn.addEventListener('click', async (e) => {
        e.stopPropagation();
        hideMapRelationshipActions();
        await deleteRelationshipFromMap(linkData.name);
    });

    container.appendChild(btn);
}

function hideMapRelationshipActions() {
    document.getElementById('mapRelActionBtn')?.remove();
}

async function deleteRelationshipFromMap(propertyName) {
    const confirmed = await showConfirmDialog({
        title: 'Delete Relationship',
        message: `Are you sure you want to delete the relationship "${propertyName}"?`,
        confirmText: 'Delete',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });

    if (!confirmed) return;

    try {
        if (typeof OntologyState !== 'undefined' && OntologyState.config) {
            const idx = (OntologyState.config.properties || []).findIndex(p => p.name === propertyName);
            if (idx >= 0) {
                OntologyState.config.properties.splice(idx, 1);

                if (typeof saveConfigToSession === 'function') {
                    await saveConfigToSession();
                }

                initOntologyMap();

                if (typeof updatePropertiesList === 'function') {
                    updatePropertiesList();
                }

                showNotification(`Relationship "${propertyName}" deleted`, 'success');
            } else {
                showNotification('Relationship not found', 'warning');
            }
        }
    } catch (error) {
        console.error('Error deleting relationship:', error);
        showNotification('Error deleting relationship: ' + error.message, 'error');
    }
}

/**
 * Start connection mode to create a new relationship or inheritance
 * @param {Object} sourceEntity - The source entity data
 * @param {Element} container - The map container
 * @param {string} type - 'relationship' or 'inheritance'
 */
function startMapConnectionMode(sourceEntity, container, type = 'relationship') {
    // Use the map SVG specifically. NOTE: `container` (#ontology-map-container)
    // also holds the loading-spinner SVG, so `container.querySelector('svg')`
    // would wrongly return the spinner. `ontologyMapSvg` is the graph SVG; fall
    // back to the first *direct-child* svg of the container (the map is appended
    // directly, the spinner is nested inside the loading overlay).
    const svg = (ontologyMapSvg && ontologyMapSvg.node())
        || container.querySelector(':scope > svg');
    if (!svg) return;

    // Get the main group (g element with transform)
    const mainGroup = svg.querySelector('g');
    if (!mainGroup) return;
    
    // Create a temporary line for visual feedback. Inline stroke attributes
    // are a belt-and-suspenders fallback so the thread is visible even if the
    // CSS class fails to apply for any reason.
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('class', type === 'inheritance' ? 'map-connection-line map-inheritance-line' : 'map-connection-line');
    line.setAttribute('stroke', type === 'inheritance' ? '#6f42c1' : _getBrandPrimaryColor());
    line.setAttribute('stroke-width', '3');
    line.setAttribute('stroke-dasharray', type === 'inheritance' ? '5,5' : '8,4');
    line.setAttribute('stroke-linecap', 'round');
    line.setAttribute('pointer-events', 'none');
    line.setAttribute('x1', sourceEntity.x);
    line.setAttribute('y1', sourceEntity.y);
    line.setAttribute('x2', sourceEntity.x);
    line.setAttribute('y2', sourceEntity.y);
    mainGroup.appendChild(line);
    
    // Store connection state
    mapConnectionMode = {
        sourceEntity: sourceEntity,
        lineElement: line,
        container: container,
        svg: svg,
        mainGroup: mainGroup,
        type: type
    };
    
    // Add connection mode class to container for styling
    container.classList.add('map-connecting');
    if (type === 'inheritance') {
        container.classList.add('map-connecting-inheritance');
    }
    
    // Show instruction tooltip
    const tooltipMessage = type === 'inheritance' 
        ? `Click on a parent entity. "${sourceEntity.name}" will inherit from it. Press Escape to cancel.`
        : 'Click on another entity to create a relationship. Press Escape to cancel.';
    showMapConnectionTooltip(container, tooltipMessage);
    
    // Add mouse move listener to update line position
    svg.addEventListener('mousemove', handleMapConnectionMouseMove);
    
    // Add click listener to complete connection
    svg.addEventListener('click', handleMapConnectionClick);
    
    // Add escape key listener to cancel
    document.addEventListener('keydown', handleMapConnectionKeyDown);
    
    // Highlight source entity
    d3.selectAll('.map-node').classed('connection-source', false);
    d3.selectAll('.map-node').classed('inheritance-source', false);
    if (type === 'inheritance') {
        d3.selectAll('.map-node').filter(d => d.name === sourceEntity.name).classed('inheritance-source', true);
    } else {
        d3.selectAll('.map-node').filter(d => d.name === sourceEntity.name).classed('connection-source', true);
    }
}

/**
 * Handle mouse move during connection mode
 */
function handleMapConnectionMouseMove(event) {
    if (!mapConnectionMode) return;

    const mainGroup = mapConnectionMode.mainGroup;
    const line = mapConnectionMode.lineElement;

    // d3.pointer maps the event into the local coordinate space of mainGroup,
    // correctly accounting for the zoom/pan transform on the group (and any
    // svg viewBox scaling) — so the thread tracks the cursor at any zoom level.
    const [mouseX, mouseY] = d3.pointer(event, mainGroup);

    // Update line end position
    line.setAttribute('x2', mouseX);
    line.setAttribute('y2', mouseY);
    
    // Highlight potential target nodes
    const isInheritance = mapConnectionMode.type === 'inheritance';
    d3.selectAll('.map-node').classed('connection-target', false);
    d3.selectAll('.map-node').classed('inheritance-target', false);
    d3.selectAll('.map-node').each(function(d) {
        const node = d3.select(this);
        const nodeX = d.x;
        const nodeY = d.y;
        const distance = Math.sqrt(Math.pow(mouseX - nodeX, 2) + Math.pow(mouseY - nodeY, 2));
        
        if (distance < 40 && d.name !== mapConnectionMode.sourceEntity.name) {
            if (isInheritance) {
                node.classed('inheritance-target', true);
            } else {
                node.classed('connection-target', true);
            }
        }
    });
}

/**
 * Handle click during connection mode (clicking on empty space cancels)
 */
function handleMapConnectionClick(event) {
    if (!mapConnectionMode) return;
    
    // Check if clicked on a node - if so, the node click handler will handle it
    const clickedNode = event.target.closest('.map-node');
    if (clickedNode) {
        return; // Let the node click handler process this
    }
    
    // Clicked on empty space - cancel connection mode
    endMapConnectionMode();
    showNotification('Connection cancelled', 'info', 1500);
}

/**
 * Handle key press during connection mode
 */
function handleMapConnectionKeyDown(event) {
    if (event.key === 'Escape') {
        endMapConnectionMode();
        showNotification('Connection cancelled', 'info', 1500);
    }
}

/**
 * End connection mode and clean up
 */
function endMapConnectionMode() {
    if (!mapConnectionMode) return;
    
    // Remove the temporary line
    if (mapConnectionMode.lineElement) {
        mapConnectionMode.lineElement.remove();
    }
    
    // Remove event listeners
    if (mapConnectionMode.svg) {
        mapConnectionMode.svg.removeEventListener('mousemove', handleMapConnectionMouseMove);
        mapConnectionMode.svg.removeEventListener('click', handleMapConnectionClick);
    }
    document.removeEventListener('keydown', handleMapConnectionKeyDown);
    
    // Remove styling classes
    if (mapConnectionMode.container) {
        mapConnectionMode.container.classList.remove('map-connecting');
        mapConnectionMode.container.classList.remove('map-connecting-inheritance');
    }
    d3.selectAll('.map-node').classed('connection-source', false);
    d3.selectAll('.map-node').classed('connection-target', false);
    d3.selectAll('.map-node').classed('inheritance-source', false);
    d3.selectAll('.map-node').classed('inheritance-target', false);
    
    // Hide tooltip
    hideMapConnectionTooltip();
    
    // Clear connection state
    mapConnectionMode = null;
}

/**
 * Show tooltip during connection mode
 */
function showMapConnectionTooltip(container, message) {
    hideMapConnectionTooltip();
    
    const tooltip = document.createElement('div');
    tooltip.id = 'mapConnectionTooltip';
    tooltip.className = 'map-connection-tooltip';
    tooltip.innerHTML = `<i class="bi bi-info-circle me-2"></i>${message}`;
    container.appendChild(tooltip);
}

/**
 * Hide connection tooltip
 */
function hideMapConnectionTooltip() {
    const tooltip = document.getElementById('mapConnectionTooltip');
    if (tooltip) {
        tooltip.remove();
    }
}

/**
 * Create a new relationship between two entities
 */
async function createRelationshipFromMap(sourceEntity, targetEntity) {
    // Show dialog to get the relationship's ID (name) and Label
    const result = await showMapRelationshipDialog(sourceEntity, targetEntity);
    
    if (!result) return; // Cancelled
    
    const relationshipName = result.name;
    const relationshipLabel = result.label || relationshipName;
    
    try {
        if (typeof OntologyState !== 'undefined' && OntologyState.config) {
            // Property IDs are unique across the whole ontology (defensive
            // re-check — the dialog already blocks duplicates before this point).
            const existingProp = OntologyState.config.properties.find(p => p.name === relationshipName);
            
            if (existingProp) {
                showNotification(`A relationship with ID "${relationshipName}" already exists`, 'warning');
                return;
            }
            
            // Create new property/relationship
            const newProperty = {
                name: relationshipName,
                localName: relationshipName,
                label: relationshipLabel,
                type: 'ObjectProperty',
                domain: sourceEntity.name,
                range: targetEntity.name,
                direction: 'forward'
            };
            
            OntologyState.config.properties = OntologyState.config.properties || [];
            OntologyState.config.properties.push(newProperty);
            
            // Save to session
            if (typeof saveConfigToSession === 'function') {
                await saveConfigToSession();
            }
            
            // Refresh the map
            initOntologyMap();
            
            // Update other UI components if available
            if (typeof updatePropertiesList === 'function') {
                updatePropertiesList();
            }
            
            showNotification(`Relationship "${relationshipName}" created`, 'success');
        }
    } catch (error) {
        console.error('Error creating relationship:', error);
        showNotification('Error creating relationship: ' + error.message, 'error');
    }
}

/**
 * Create an inheritance relationship (child inherits from parent)
 * @param {Object} childEntity - The child entity (will inherit)
 * @param {Object} parentEntity - The parent entity
 */
async function createInheritanceFromMap(childEntity, parentEntity) {
    // Show confirmation dialog
    const confirmed = await showConfirmDialog({
        title: 'Create Inheritance',
        message: `Make "${childEntity.name}" inherit from "${parentEntity.name}"?`,
        confirmText: 'Create Inheritance',
        confirmClass: 'btn-primary',
        icon: 'diagram-3'
    });
    
    if (!confirmed) return;
    
    try {
        if (typeof OntologyState !== 'undefined' && OntologyState.config) {
            // Find the child class
            const childClass = OntologyState.config.classes.find(c => c.name === childEntity.name);
            
            if (!childClass) {
                showNotification('Child entity not found', 'warning');
                return;
            }
            
            // Check if already has this parent
            if (childClass.parent === parentEntity.name) {
                showNotification(`"${childEntity.name}" already inherits from "${parentEntity.name}"`, 'warning');
                return;
            }
            
            // Check for circular inheritance
            let currentParent = parentEntity.name;
            while (currentParent) {
                if (currentParent === childEntity.name) {
                    showNotification('Cannot create circular inheritance', 'error');
                    return;
                }
                const parentClass = OntologyState.config.classes.find(c => c.name === currentParent);
                currentParent = parentClass ? parentClass.parent : null;
            }
            
            // Set the parent
            childClass.parent = parentEntity.name;
            
            // Save to session
            if (typeof saveConfigToSession === 'function') {
                await saveConfigToSession();
            }
            
            // Refresh the map
            initOntologyMap();
            
            // Update other UI components if available
            if (typeof updateClassesList === 'function') {
                updateClassesList();
            }
            
            showNotification(`"${childEntity.name}" now inherits from "${parentEntity.name}"`, 'success');
        }
    } catch (error) {
        console.error('Error creating inheritance:', error);
        showNotification('Error creating inheritance: ' + error.message, 'error');
    }
}

// =====================================================
// AUTO-MAP ICONS (via LLM, async background task)
// =====================================================

// Session-storage key for persisting a running icon task across navigation.
const ICONS_TASK_KEY = 'ontobricks_icons_task';

// Module-level guard so the monitor loop is started at most once per task.
let _iconsCurrentTaskId = null;

/**
 * Open the ontology designer discussion. Anchors to the whole ontology
 * diagram (domain/'ontology'); each comment can optionally be tagged with
 * one or more classes/relationships via the compose-box tag picker.
 */
function openOntologyDiscussion() {
    if (!window.OntoComments) return;
    const cfg = (typeof OntologyState !== 'undefined' && OntologyState.config) || {};
    OntoComments.openForSelection(
        'domain', 'ontology', 'Whole ontology diagram',
        OntoComments.taggableFromOntology(cfg)
    );
}

/**
 * Restore button state (used after completion, failure, or resume).
 */
function _restoreAutoAssignIconsButton() {
    const btn = document.getElementById('mapAutoAssignIcons');
    if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-emoji-smile"></i>';
    }
}

/**
 * Apply an {entityName: emoji} map to OntologyState.config.classes, then
 * save and refresh the map. Returns the number of icons actually applied.
 */
async function _applyIconsToOntologyState(iconMap) {
    const classes = OntologyState?.config?.classes || [];
    let assignedCount = 0;
    for (const cls of classes) {
        const emoji = iconMap ? iconMap[cls.name] : undefined;
        if (emoji) {
            cls.emoji = emoji;
            assignedCount++;
        }
    }
    if (assignedCount > 0) {
        if (typeof saveConfigToSession === 'function') {
            await saveConfigToSession();
        }
        initOntologyMap();
        if (typeof updateClassesList === 'function') {
            updateClassesList();
        }
    }
    return assignedCount;
}

/**
 * Poll /tasks/{taskId} until the icon task terminates, then apply the
 * result or surface an error. Safe to call after a page reload because
 * it reads the latest task state on the first poll.
 */
async function _monitorIconsTask(taskId) {
    if (_iconsCurrentTaskId === taskId) {
        // Already monitoring — avoid stacking loops after a resume.
        return;
    }
    _iconsCurrentTaskId = taskId;

    const pollInterval = 1500;
    try {
        while (true) {
            await new Promise(r => setTimeout(r, pollInterval));

            let task;
            try {
                const resp = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
                const data = await resp.json();
                if (!data.success || !data.task) {
                    throw new Error('Task not found');
                }
                task = data.task;
            } catch (err) {
                console.error('[Map] Icons task polling error:', err);
                if (typeof showNotification === 'function') {
                    showNotification('Error monitoring icon task: ' + err.message, 'error', 5000);
                }
                sessionStorage.removeItem(ICONS_TASK_KEY);
                _restoreAutoAssignIconsButton();
                return;
            }

            if (task.status === 'running' || task.status === 'pending') {
                continue;
            }

            sessionStorage.removeItem(ICONS_TASK_KEY);

            if (task.status === 'completed') {
                const iconMap = (task.result && task.result.icons) || {};
                const missing = (task.result && task.result.missing) || [];
                const applied = await _applyIconsToOntologyState(iconMap);

                if (typeof showNotification === 'function') {
                    if (applied === 0) {
                        showNotification('LLM did not return any icons', 'warning', 3000);
                    } else {
                        showNotification(
                            `Mapped icons to ${applied} ${applied === 1 ? 'entity' : 'entities'}`,
                            'success',
                            3000,
                        );
                    }
                    if (missing.length > 0) {
                        showNotification(
                            `No icon for ${missing.length} ${missing.length === 1 ? 'entity' : 'entities'}`,
                            'warning',
                            4000,
                        );
                    }
                }
                if (typeof refreshTasks === 'function') refreshTasks();
                return;
            }

            if (task.status === 'failed') {
                if (typeof showNotification === 'function') {
                    showNotification(
                        'Icon assignment failed: ' + (task.error || task.message || 'Unknown error'),
                        'error',
                        5000,
                    );
                }
                if (typeof refreshTasks === 'function') refreshTasks();
                return;
            }

            if (task.status === 'cancelled') {
                if (typeof showNotification === 'function') {
                    showNotification('Icon assignment was cancelled', 'warning', 3000);
                }
                if (typeof refreshTasks === 'function') refreshTasks();
                return;
            }

            // Unknown terminal status — bail out.
            console.warn('[Map] Icons task unexpected status:', task.status);
            return;
        }
    } finally {
        _iconsCurrentTaskId = null;
        _restoreAutoAssignIconsButton();
    }
}

/**
 * Resume monitoring a previously-started icon task after a page reload or
 * view switch. Called from initOntologyMap().
 */
async function checkAndResumeIconsTask() {
    const savedTaskId = sessionStorage.getItem(ICONS_TASK_KEY);
    if (!savedTaskId) return;
    try {
        const resp = await fetch(`/tasks/${savedTaskId}`, { credentials: 'same-origin' });
        const data = await resp.json();
        if (!data.success || !data.task) {
            sessionStorage.removeItem(ICONS_TASK_KEY);
            return;
        }
        const task = data.task;
        if (task.status === 'running' || task.status === 'pending') {
            console.log('[Map] Resuming icons task monitoring:', savedTaskId);
            const btn = document.getElementById('mapAutoAssignIcons');
            if (btn) {
                btn.disabled = true;
                btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
            }
            _monitorIconsTask(savedTaskId);
        } else if (task.status === 'completed') {
            const iconMap = (task.result && task.result.icons) || {};
            sessionStorage.removeItem(ICONS_TASK_KEY);
            await _applyIconsToOntologyState(iconMap);
        } else {
            sessionStorage.removeItem(ICONS_TASK_KEY);
        }
    } catch (err) {
        console.error('[Map] Resume icons task error:', err);
        sessionStorage.removeItem(ICONS_TASK_KEY);
    }
}

/**
 * Auto-map icons to entities that still have the default icon.
 * When every entity already has a custom icon, ask the user to confirm a
 * full remap before starting the same background task.
 */
async function autoAssignEntityIcons() {
    if (window.isActiveVersion === false) return;
    const classes = OntologyState?.config?.classes;
    if (!classes || classes.length === 0) {
        if (typeof showNotification === 'function') {
            showNotification('No entities to assign icons to', 'info', 2000);
        }
        return;
    }

    // A task is already running — don't launch another.
    if (sessionStorage.getItem(ICONS_TASK_KEY)) {
        if (typeof showNotification === 'function') {
            showNotification('Icon assignment already in progress…', 'info', 2500);
        }
        return;
    }

    const defaultEmoji = OntologyState.defaultClassEmoji || '📦';
    let candidates = classes.filter(cls => (cls.emoji || defaultEmoji) === defaultEmoji);
    if (candidates.length === 0) {
        if (typeof showConfirmDialog !== 'function') {
            if (typeof showNotification === 'function') {
                showNotification('All entities already have custom icons', 'info', 2000);
            }
            return;
        }
        const confirmed = await showConfirmDialog({
            title: 'Remap entity icons?',
            message: 'All entities already have custom icons. Remapping will replace every current icon.',
            confirmText: 'Remap all',
            cancelText: 'Cancel',
            confirmClass: 'btn-primary',
            icon: 'emoji-smile',
        });
        if (!confirmed) return;
        candidates = classes;
    }

    const entityNames = candidates.map(cls => cls.name);

    const btn = document.getElementById('mapAutoAssignIcons');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
    }

    try {
        const response = await fetch('/ontology/auto-assign-icons', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entity_names: entityNames }),
            credentials: 'same-origin',
        });
        const result = await response.json();

        if (!result.success || !result.task_id) {
            if (typeof showNotification === 'function') {
                showNotification(result.message || 'Failed to start icon task', 'error', 5000);
            }
            _restoreAutoAssignIconsButton();
            return;
        }

        sessionStorage.setItem(ICONS_TASK_KEY, result.task_id);
        if (typeof showNotification === 'function') {
            showNotification(
                `Assigning icons for ${entityNames.length} ${entityNames.length === 1 ? 'entity' : 'entities'} in the background…`,
                'info',
                4000,
            );
        }
        if (typeof refreshTasks === 'function') refreshTasks();

        _monitorIconsTask(result.task_id);

    } catch (error) {
        console.error('[Map] Auto-map icons start error:', error);
        if (typeof showNotification === 'function') {
            showNotification('Error starting icon task: ' + error.message, 'error', 5000);
        }
        _restoreAutoAssignIconsButton();
    }
}


/**
 * Show dialog to get relationship name
 */
function showMapRelationshipDialog(sourceEntity, targetEntity) {
    return new Promise((resolve) => {
        // Remove existing modal
        const existingModal = document.getElementById('mapRelationshipModal');
        if (existingModal) existingModal.remove();
        
        const modalHtml = `
            <div class="modal fade" id="mapRelationshipModal" tabindex="-1">
                <div class="modal-dialog modal-dialog-centered">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">
                                <i class="bi bi-arrow-right me-2"></i>Create Relationship
                            </h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            <div class="d-flex align-items-center justify-content-center mb-4 gap-2">
                                <div class="text-center">
                                    <div class="h3 mb-1">${sourceEntity.icon || '📦'}</div>
                                    <div class="small fw-semibold">${sourceEntity.name}</div>
                                </div>
                                <div class="px-3">
                                    <i class="bi bi-arrow-right fs-4 text-muted"></i>
                                </div>
                                <div class="text-center">
                                    <div class="h3 mb-1">${targetEntity.icon || '📦'}</div>
                                    <div class="small fw-semibold">${targetEntity.name}</div>
                                </div>
                            </div>
                            <div class="mb-3">
                                <label class="form-label" for="mapRelationshipLabel">Label</label>
                                <input type="text" class="form-control" id="mapRelationshipLabel"
                                       placeholder="e.g., Has Customer, Belongs To" autofocus>
                            </div>
                            <div class="mb-3">
                                <label class="form-label" for="mapRelationshipName">ID <span class="text-danger">*</span></label>
                                <input type="text" class="form-control" id="mapRelationshipName"
                                       placeholder="e.g., hasCustomer, belongsTo">
                                <div class="invalid-feedback" id="mapRelationshipNameError">This ID already exists in the ontology.</div>
                                <div class="form-text">Unique identifier, camelCase convention. Mirrors the Label until edited directly.</div>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                            <button type="button" class="btn btn-primary" id="mapRelationshipCreate">
                                <i class="bi bi-plus-lg me-1"></i>Create
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        const modalEl = document.getElementById('mapRelationshipModal');
        const modal = new bootstrap.Modal(modalEl);
        const labelInput = document.getElementById('mapRelationshipLabel');
        const nameInput = document.getElementById('mapRelationshipName');
        const nameError = document.getElementById('mapRelationshipNameError');
        
        let resolved = false;
        let idManuallyEdited = false;
        
        const isDuplicateId = (id) => {
            const props = (typeof OntologyState !== 'undefined' && OntologyState.config && OntologyState.config.properties) || [];
            return props.some(p => p.name === id);
        };
        
        // Validates the ID field and reflects the result in the UI.
        // Returns true when the ID is non-empty and not already used.
        const validateName = () => {
            const id = nameInput.value.trim();
            if (!id) {
                nameInput.classList.add('is-invalid');
                nameError.textContent = 'ID is required.';
                return false;
            }
            if (isDuplicateId(id)) {
                nameInput.classList.add('is-invalid');
                nameError.textContent = 'This ID already exists in the ontology.';
                return false;
            }
            nameInput.classList.remove('is-invalid');
            return true;
        };
        
        // Mirror the Label into the ID (camelCase) until the user edits the ID directly.
        labelInput.addEventListener('input', () => {
            if (idManuallyEdited) return;
            nameInput.value = typeof columnToCamelCase === 'function'
                ? columnToCamelCase(labelInput.value)
                : labelInput.value;
            validateName();
        });
        
        nameInput.addEventListener('input', () => {
            idManuallyEdited = true;
            validateName();
        });
        
        // Handle create button
        document.getElementById('mapRelationshipCreate').addEventListener('click', () => {
            if (!validateName()) return;
            resolved = true;
            modal.hide();
            resolve({ name: nameInput.value.trim(), label: labelInput.value.trim() });
        });
        
        // Handle enter key in either input
        [labelInput, nameInput].forEach(input => {
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    document.getElementById('mapRelationshipCreate').click();
                }
            });
        });
        
        // Handle modal close
        modalEl.addEventListener('hidden.bs.modal', () => {
            modalEl.remove();
            if (!resolved) {
                resolve(null);
            }
        }, { once: true });
        
        // Bootstrap's own focus-trap grabs focus once the modal's shown
        // transition completes, so focus the Label field only after that
        // (a synchronous .focus() right after .show() gets overridden).
        modalEl.addEventListener('shown.bs.modal', () => {
            labelInput.focus();
        }, { once: true });
        
        modal.show();
    });
}

/**
 * Targeted in-place update of a single relationship link's direction in the D3 map.
 * Updates the live `ontologyMapLinks` data and toggles the `reverse` CSS class on the
 * corresponding SVG path element so the arrowhead flips immediately.
 * Called by `saveSharedRelationship` via window.refreshMapLinkDirection.
 * @param {string} name - Relationship name (matched by link.name)
 * @param {string} direction - 'forward' | 'reverse'
 */
function refreshMapLinkDirection(name, direction) {
    if (!ontologyMapSimulation) return;
    const isReverse = direction === 'reverse';
    // Update the live data so the next simulation tick renders the correct direction
    ontologyMapLinks.forEach(link => {
        if (link.name === name) link.direction = direction;
    });
    // Update the DOM element class immediately (CSS handles the marker swap)
    if (typeof d3 !== 'undefined' && ontologyMapSvg) {
        ontologyMapSvg.selectAll('.map-link').each(function(d) {
            if (d && d.name === name) {
                d3.select(this).classed('reverse', isReverse);
            }
        });
    }
}
window.refreshMapLinkDirection = refreshMapLinkDirection;

/**
 * Refresh visible metadata for one existing entity without rebuilding the map.
 * Structural changes (create, rename, delete, or inheritance) still require
 * initOntologyMap().
 * @param {string} name - Entity name
 * @returns {boolean} true when an existing map node was updated
 */
function refreshMapNodeFromConfig(name) {
    if (!ontologyMapSimulation || typeof d3 === 'undefined') return false;

    const classData = (OntologyState?.config?.classes || []).find(cls => cls.name === name);
    const node = ontologyMapNodes.find(candidate => candidate.name === name);
    if (!classData || !node) return false;

    node.label = classData.label || classData.name;
    node.icon = classData.emoji || OntologyState.defaultClassEmoji || '📦';
    node.hasExternal = _classHasExternalConfig(classData);

    const nodeSelection = d3.selectAll('.map-node').filter(d => d && d.name === name);
    if (nodeSelection.empty()) return false;

    nodeSelection.select('.map-node-icon').text(node.icon);
    nodeSelection.select('.map-node-label').text(node.label || node.name);
    nodeSelection.select('title').text(node.label || node.name);
    nodeSelection.selectAll('.map-node-external-badge-bg').remove();
    nodeSelection.selectAll('.map-node-external-badge-icon').remove();
    if (node.hasExternal) _appendMapExternalBadge(nodeSelection);

    return true;
}
window.refreshMapNodeFromConfig = refreshMapNodeFromConfig;

/**
 * Refresh direction and label for one relationship whose endpoints and name
 * did not change.
 * @param {string} name - Relationship name
 * @returns {boolean} true when an existing map link was updated
 */
function refreshMapRelationshipFromConfig(name) {
    if (!ontologyMapSimulation || !ontologyMapSvg || typeof d3 === 'undefined') return false;

    const property = (OntologyState?.config?.properties || []).find(prop => prop.name === name);
    const link = ontologyMapLinks.find(candidate => candidate.name === name);
    if (!property || !link) return false;

    link.label = property.label || property.name;
    refreshMapLinkDirection(name, property.direction);

    const labelSelection = ontologyMapSvg.selectAll('.map-link-label').filter(d => d && d.name === name);
    if (labelSelection.empty()) return false;
    labelSelection.text(link.label);

    return true;
}
window.refreshMapRelationshipFromConfig = refreshMapRelationshipFromConfig;
