/**
 * OntoBricks - query.js
 * Main query page entry point: global state, initialization, and shared utilities.
 *
 * The remaining logic is split across:
 *   query-loaders.js        – Ontology/mapping loaders, entity type validation
 *   query-execute.js        – Query execution, results grid, filtering, grouping
 *   query-d3graph.js        – D3.js graph build, render, visual filters, resize
 *   query-entity-details.js – Entity/relationship detail panel, mapping lookup
 *   query-dashboard.js      – Dashboard modal (URL builder, iframe)
 *   query-sync.js           – Triple store sync, readiness checks
 *   query-sigmagraph.js     – Sigma.js knowledge graph
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

let queryResults = null;
let generatedSql = null;

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

// Ontology icon map (class name/URI -> emoji)
let taxonomyIcons = {};

// Track all relationship types for filtering
let allRelationshipTypes = new Set();

// =====================================================
// INITIALIZATION
// =====================================================

// Configure sidebar navigation
window.SIDEBAR_NAV_MANUAL_INIT = true;
document.addEventListener('DOMContentLoaded', function() {
    const urlParams = new URLSearchParams(window.location.search);
    const initialSection = urlParams.get('section');
    const focusEntityUri = urlParams.get('focus');
    const bridgeDomain = urlParams.get('domain') || urlParams.get('project');

    _initQueryPage(initialSection, focusEntityUri, bridgeDomain);
});

async function _initQueryPage(initialSection, focusEntityUri, bridgeDomain) {
    if (bridgeDomain) {
        await _switchDomainForBridge(bridgeDomain, focusEntityUri);
        return;
    }

    var pendingFocus = focusEntityUri;

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
                if (typeof GraphQLPlayground !== 'undefined') {
                    setTimeout(function () { GraphQLPlayground.init(); }, 100);
                }
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
            if (typeof fetchCachedInvalidate === 'function') fetchCachedInvalidate('/navbar/state');
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

function copyGeneratedSql() {
    if (!generatedSql) return;
    navigator.clipboard.writeText(generatedSql).then(() => {
        const btn = event.target.closest('button');
        const original = btn.innerHTML;
        btn.innerHTML = '<i class="bi bi-check"></i> Copied!';
        setTimeout(() => btn.innerHTML = original, 1500);
    });
}

function downloadResults() {
    if (!queryResults || queryResults.length === 0) return;
    
    const columns = Object.keys(queryResults[0]);
    let csv = columns.join(',') + '\n';
    
    for (const row of queryResults) {
        csv += columns.map(col => {
            const val = row[col] || '';
            if (val.includes(',') || val.includes('"') || val.includes('\n')) {
                return '"' + val.replace(/"/g, '""') + '"';
            }
            return val;
        }).join(',') + '\n';
    }
    
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'sparql_results.csv';
    a.click();
    URL.revokeObjectURL(url);
}

// escapeHtml is provided globally by utils.js
