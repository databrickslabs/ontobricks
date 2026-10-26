/**
 * OntoBricks - query.js
 * Main query page entry point: global state, initialization, and shared utilities.
 *
 * The remaining logic is split across:
 *   query-loaders.js        – Ontology/mapping loaders, entity type validation
 *   query-execute.js        – Query execution, results grid, filtering, grouping
 *   query-d3graph.js        – D3.js graph build, render, visual filters, resize
 *   query-entity-details.js – Entity/relationship detail panel, mapping lookup
 *   query-dashboard.js      – Dashboard modal + dataset row preview modal
 *   query-sync.js           – Triple store sync, readiness checks
 *   query-sigmagraph.js     – Sigma.js graph viewer
 *   query-quality.js        – Quality checks
 *   query-api.js            – API documentation helpers
 *   query-graphql.js        – Embedded GraphiQL playground
 */

// =====================================================
// QUERY PAGE - State & Configuration
// =====================================================

// Bootstrap triplestore config from JSON script tag (keeps JS out of HTML)
(function() {
    var el = document.getElementById('triplestore-config');
    if (el) {
        try { window.__TRIPLESTORE_CONFIG = JSON.parse(el.textContent); }
        catch (e) { window.__TRIPLESTORE_CONFIG = {}; }
    }
})();

// Enable full-width layout for this page
document.body.classList.add('full-width-layout');

// D3.js graph state (exposed globally for query-sigmagraph.js)
var d3Simulation = null;
var d3Svg = null;
var d3Zoom = null;
var d3NodesData = [];
var d3LinksData = [];

// Flag to prevent double graph building after query execution
let graphJustBuilt = false;

// Store last query results for entity details
let lastQueryResults = null;

// Store entity mappings (class -> label column mapping)
let entityMappings = {};

// Store ontology classes (for dashboard and other class metadata)
let ontologyClasses = {};
let ontologyProperties = {};

// Ontology icon map (class name/URI -> emoji)
let taxonomyIcons = {};

// Track all relationship types for filtering
let allRelationshipTypes = new Set();

// =====================================================
// DISCUSSION
// =====================================================

// Cache the ontology-derived tag vocabulary for the Knowledge Graph discussion.
let _twinTaggable = null;

/**
 * Open the Knowledge Graph discussion. Anchors to the whole twin
 * (domain/'digital-twin'); each comment can optionally be tagged with one or
 * more ontology classes/relationships via the compose-box tag picker. The tag
 * vocabulary is lazily fetched from the loaded ontology and cached.
 */
async function openTwinDiscussion() {
    if (!window.OntoComments) return;
    if (_twinTaggable === null) {
        _twinTaggable = [];
        try {
            const resp = await fetch('/ontology/load', { credentials: 'same-origin' });
            const data = await resp.json();
            const cfg = (data && data.success && data.config) ? data.config : {};
            _twinTaggable = window.OntoComments.taggableFromOntology(cfg);
        } catch (e) {
            console.log('Twin discussion: could not load ontology tags:', e.message);
        }
    }
    window.OntoComments.openForSelection(
        'domain', 'digital-twin', 'Knowledge Graph', _twinTaggable
    );
}
window.openTwinDiscussion = openTwinDiscussion;

// =====================================================
// INITIALIZATION
// =====================================================

// Configure sidebar navigation
window.SIDEBAR_NAV_MANUAL_INIT = true;
const _QUERY_SHELL_ACTION_MAP = {
    'ontology': 'OntologyViewer.open',
    'discussion': 'openTwinDiscussion'
};

function _resolveQueryShellAction(path) {
    const parts = String(path || '').split('.');
    let context = window;
    for (let i = 0; i < parts.length - 1; i++) {
        if (!context) return { fn: null, context: null };
        context = context[parts[i]];
    }
    const key = parts[parts.length - 1];
    return {
        fn: context && typeof context[key] === 'function' ? context[key] : null,
        context: context || null
    };
}

function _bindQueryShellActions() {
    if (window.__obQueryShellActionsBound) return;
    window.__obQueryShellActionsBound = true;

    document.addEventListener('click', function(event) {
        const trigger = event.target.closest('[data-query-action]');
        if (!trigger) return;

        const action = trigger.getAttribute('data-query-action');
        const targetPath = _QUERY_SHELL_ACTION_MAP[action];
        if (!targetPath) return;

        event.preventDefault();
        const resolved = _resolveQueryShellAction(targetPath);
        if (resolved.fn) {
            resolved.fn.call(resolved.context);
        }
    });
}

document.addEventListener('DOMContentLoaded', function() {
    const urlParams = new URLSearchParams(window.location.search);
    const initialSection = urlParams.get('section');
    const initialQueryTab = urlParams.get('tab');
    const focusEntityUri = urlParams.get('focus');
    const bridgeDomain = urlParams.get('domain') || urlParams.get('project');

    _bindQueryShellActions();
    _bindQueryLanguageTabs();
    _initQueryPage(initialSection, focusEntityUri, bridgeDomain, initialQueryTab);
});

function _bindQueryLanguageTabs() {
    if (window.__obQueryLanguageTabsBound) return;
    window.__obQueryLanguageTabsBound = true;

    document.getElementById('queryGraphqlTab')?.addEventListener(
        'shown.bs.tab',
        function () {
            if (typeof GraphQLPlayground !== 'undefined') {
                GraphQLPlayground.init();
            }
        }
    );
    document.getElementById('querySparqlTab')?.addEventListener(
        'shown.bs.tab',
        function () {
            if (typeof SPARQLPlayground !== 'undefined') {
                SPARQLPlayground.init();
            }
        }
    );
}

function getActiveQueryTabName() {
    const sparqlTab = document.getElementById('querySparqlTab');
    if (sparqlTab && sparqlTab.classList.contains('active')) {
        return 'sparql';
    }
    const graphqlTab = document.getElementById('queryGraphqlTab');
    if (graphqlTab && graphqlTab.classList.contains('active')) {
        return 'graphql';
    }
    return 'graphql';
}

function initQueryPlayground(tabName) {
    const requestedTab = tabName === 'sparql' || tabName === 'graphql' ? tabName : null;
    if (requestedTab) {
        const tabId = requestedTab === 'sparql' ? 'querySparqlTab' : 'queryGraphqlTab';
        const tab = document.getElementById(tabId);
        if (tab && typeof bootstrap !== 'undefined' && bootstrap.Tab) {
            bootstrap.Tab.getOrCreateInstance(tab).show();
        }
    }

    const activeTabName = getActiveQueryTabName();
    if (activeTabName === 'sparql') {
        if (typeof SPARQLPlayground !== 'undefined') {
            SPARQLPlayground.init();
        }
    } else if (typeof GraphQLPlayground !== 'undefined') {
        GraphQLPlayground.init();
    }
}

async function _initQueryPage(initialSection, focusEntityUri, bridgeDomain, initialQueryTab) {
    if (bridgeDomain) {
        await _switchDomainForBridge(bridgeDomain, focusEntityUri);
        return;
    }

    var pendingFocus = focusEntityUri;
    var pendingQueryTab = initialQueryTab;

    SidebarNav.init({
        onSectionChange: async function(section, targetSection) {
            if (section === 'sigmagraph') {
                if (typeof SigmaGraph !== 'undefined') {
                    setTimeout(function () {
                        SigmaGraph.init(pendingFocus || undefined);
                        pendingFocus = null;
                    }, 100);
                }
            }
            if (section === 'graphql') {
                setTimeout(function () {
                    initQueryPlayground(pendingQueryTab);
                    pendingQueryTab = null;
                }, 100);
            }
            if (section === 'dataquality') {
                if (typeof DQExecModule !== 'undefined') {
                    DQExecModule.init();
                }
            }
            if (section === 'insight') {
                if (typeof loadInsights === 'function' && !(window._obInsights || {}).loaded) {
                    loadInsights();
                }
            }
            if (section === 'analytics') {
                if (typeof window.analyticsLoadTypes === 'function') {
                    window.analyticsLoadTypes();
                }
            }
        }
    });
    
    loadOntologyIcons();
    loadEntityMappings();

    if (typeof loadSyncInfo === 'function') {
        loadSyncInfo();
    }

    const targetSection = initialSection || 'sigmagraph';
    const link = document.querySelector(`[data-section="${targetSection}"]`);
    if (link) {
        if (focusEntityUri && targetSection === 'sigmagraph') {
            link.click();
        } else {
            setTimeout(() => link.click(), 300);
        }
    }
}

async function _switchDomainForBridge(domainName, focusUri) {
    if (typeof showDomainLoading === 'function') showDomainLoading('Loading ' + domainName + '...');
    try {
        const resp = await fetch('/domain/load-from-uc', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain: domainName })
        });
        const data = await resp.json();
        if (data.success) {
            // Bust name/version caches before navigation — same contract as
            // Registry load / Graph Switcher (see navbar.js invalidateDomainCaches).
            if (typeof invalidateDomainCaches === 'function') {
                invalidateDomainCaches();
            } else if (typeof fetchCachedInvalidate === 'function') {
                fetchCachedInvalidate('/navbar/state');
            }
            console.log('[Bridge] Switched to domain:', domainName);
            var target = '/dtwin/?section=sigmagraph';
            if (focusUri) target += '&focus=' + encodeURIComponent(focusUri);
            window.location.replace(target);
        } else {
            if (typeof hideDomainLoading === 'function') hideDomainLoading();
            console.warn('[Bridge] Failed to switch domain:', data.message || data);
        }
    } catch (err) {
        if (typeof hideDomainLoading === 'function') hideDomainLoading();
        console.error('[Bridge] Error switching domain:', err);
    }
}

// =====================================================
// UTILITY FUNCTIONS
// =====================================================

function _applyFocusEntityWhenReady(uri, retries) {
    retries = (retries === undefined) ? 20 : retries;
    if (typeof SigmaGraph !== 'undefined' && SigmaGraph.focusEntityByUri) {
        SigmaGraph.focusEntityByUri(uri);
        return;
    }
    if (retries > 0) {
        setTimeout(function () { _applyFocusEntityWhenReady(uri, retries - 1); }, 500);
    }
}

// escapeHtml is provided globally by utils.js
