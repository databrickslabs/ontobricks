/**
 * OntoBricks - query-loaders.js
 * Ontology and mapping loaders, entity type validation.
 * Extracted from query.js per code_instructions.txt
 */

// =====================================================
// ENTITY TYPE VALIDATION
// =====================================================

function isValidEntityType(type) {
    if (!type) return false;
    var excludedTypes = ['Source', 'Target', 'Basic', 'Literal', 'BlankNode', 'Unknown',
                         'Resource', 'Property', 'Statement', 'List', 'Container'];
    if (excludedTypes.indexOf(type) >= 0) return false;
    var typeLower = type.toLowerCase();
    var typeLocalName = type.split('#').pop().split('/').pop().toLowerCase();
    if (typeof SearchBuilder !== 'undefined' && SearchBuilder.ontologyClasses && SearchBuilder.ontologyClasses.length > 0) {
        for (var i = 0; i < SearchBuilder.ontologyClasses.length; i++) {
            var cls = SearchBuilder.ontologyClasses[i];
            var clsName = (cls.name || cls.localName || '').toLowerCase();
            var clsLabel = (cls.label || '').toLowerCase();
            var clsUri = (cls.uri || '').toLowerCase();
            if (clsName === typeLower || clsName === typeLocalName ||
                clsLabel === typeLower || clsLabel === typeLocalName ||
                clsUri.endsWith('#' + typeLocalName) || clsUri.endsWith('/' + typeLocalName)) {
                return true;
            }
        }
        return false;
    }
    if (typeof entityMappings !== 'undefined' && Object.keys(entityMappings).length > 0) {
        if (entityMappings[typeLower] || entityMappings[typeLocalName]) return true;
        for (var key in entityMappings) {
            if (key.indexOf(typeLocalName) >= 0 || typeLocalName.indexOf(key) >= 0) return true;
        }
        return false;
    }
    return false;
}

/**
 * Display cached graph data (legacy, retained for compatibility).
 * Graph data is cached in memory when a query is executed.
 */
async function displayCachedVisualization() {
    const loadingMsg = document.getElementById('graphLoading');
    const noGraphMsg = document.getElementById('noGraphMessage');
    const svgElement = document.getElementById('graphSvg');
    
    // If graph is already displayed, no need to rebuild or fit again
    if (d3NodesData.length > 0 && svgElement && svgElement.style.opacity === '1') {
        console.log('[Viz] Graph already displayed, no action needed');
        return;
    }
    
    // Check if we have cached results to build graph from
    if (!lastQueryResults || !lastQueryResults.results || lastQueryResults.results.length === 0) {
        if (noGraphMsg) noGraphMsg.style.display = 'block';
        if (loadingMsg) loadingMsg.style.display = 'none';
        if (svgElement) svgElement.style.opacity = '1';
        return;
    }
    
    // Show loading and build graph
    if (noGraphMsg) noGraphMsg.style.display = 'none';
    if (loadingMsg) loadingMsg.style.display = 'block';
    if (svgElement) svgElement.style.opacity = '0';
    
    // Clear any existing graph data for fresh start
    d3NodesData = [];
    d3LinksData = [];
    d3.select('#graphSvg').selectAll('*').remove();
    
    // Build graph from cached results
    if (typeof buildGraph === 'function') {
        await buildGraph(lastQueryResults.results, lastQueryResults.columns);
    }
    
    if (d3NodesData.length === 0) {
        // No data - hide loading and show no graph message
        if (loadingMsg) loadingMsg.style.display = 'none';
        if (svgElement) svgElement.style.opacity = '1';
        if (noGraphMsg) noGraphMsg.style.display = 'block';
    }
}

// =====================================================
// QUERY FUNCTIONS
// =====================================================

async function loadOntologyIcons() {
    try {
        const response = await fetch('/ontology/get-loaded-ontology');
        const data = await response.json();
        
        if (data.success && data.ontology && data.ontology.classes) {
            taxonomyIcons = {};
            for (const cls of data.ontology.classes) {
                const emoji = cls.emoji || '📦';
                
                // Store by name (various forms)
                if (cls.name) {
                    taxonomyIcons[cls.name.toLowerCase()] = emoji;
                    // Also store without spaces/underscores
                    taxonomyIcons[cls.name.toLowerCase().replace(/[\s_-]/g, '')] = emoji;
                }
                
                // Store by localName
                if (cls.localName) {
                    taxonomyIcons[cls.localName.toLowerCase()] = emoji;
                }
                
                // Store by URI
                if (cls.uri) {
                    taxonomyIcons[cls.uri.toLowerCase()] = emoji;
                    // Also extract and store the local part of the URI
                    const localPart = cls.uri.split('#').pop().split('/').pop();
                    if (localPart) {
                        taxonomyIcons[localPart.toLowerCase()] = emoji;
                    }
                }
            }
            console.log('Loaded ontology icons:', Object.keys(taxonomyIcons).length, 'keys for', data.ontology.classes.length, 'classes');
        }
    } catch (error) {
        console.log('No ontology icons loaded:', error.message);
    }
}

async function loadOntologyClasses() {
    try {
        const response = await fetch('/ontology/load');
        const data = await response.json();
        
        ontologyClasses = {};
        
        if (data && data.success && data.config && data.config.classes) {
            for (const cls of data.config.classes) {
                const classInfo = {
                    name: cls.name || '',
                    label: cls.label || cls.name || '',
                    emoji: cls.emoji || '📦',
                    dashboard: cls.dashboard || null,
                    dashboardParams: cls.dashboardParams || {},
                    bridges: cls.bridges || [],
                    description: cls.description || cls.comment || '',
                    dataProperties: cls.dataProperties || []
                };
                
                // Store by name (lowercase for lookup)
                if (cls.name) {
                    ontologyClasses[cls.name.toLowerCase()] = classInfo;
                }
                // Also store by URI if available
                if (cls.uri) {
                    ontologyClasses[cls.uri.toLowerCase()] = classInfo;
                    // Also by local part of URI
                    const localPart = cls.uri.split('#').pop().split('/').pop();
                    if (localPart) {
                        ontologyClasses[localPart.toLowerCase()] = classInfo;
                    }
                }
            }
            console.log('Loaded ontology classes:', Object.keys(ontologyClasses).length, 'keys for', data.config.classes.length, 'classes');
        }
    } catch (error) {
        console.log('No ontology classes loaded:', error.message);
    }
}

async function loadEntityMappings() {
    try {
        // First load ontology classes to get dashboard info
        await loadOntologyClasses();
        
        console.log('[Mappings] Fetching from /mapping/load...');
        const response = await fetch('/mapping/load');
        const data = await response.json();
        
        console.log('[Mappings] Raw API response:', data);
        
        entityMappings = {};
        
        // API returns data in config.entities
        const mappingsData = data?.config?.entities || data?.entities || [];
        
        if (mappingsData && mappingsData.length > 0) {
            console.log('[Mappings] Found', mappingsData.length, 'entities');
            
            for (const mapping of mappingsData) {
                const classLabel = mapping.ontology_class_label || '';
                const classUri = mapping.ontology_class || '';
                const labelColumn = mapping.label_column || null;
                const idColumn = mapping.id_column || null;
                const attributeMappings = mapping.attribute_mappings || {};
                
                console.log('[Mappings] Processing mapping:', { classLabel, classUri, labelColumn, idColumn });
                
                // Look up ontology class info (for dashboard, emoji, etc.)
                const classInfo = findOntologyClass(classLabel) || findOntologyClass(classUri);
                console.log('[Mappings] Class lookup for', classLabel, ':', classInfo ? 'found' : 'not found');
                if (classInfo) {
                    console.log('[Mappings] Class dashboard:', classInfo.dashboard);
                    console.log('[Mappings] Class dashboardParams:', classInfo.dashboardParams);
                }
                
                // Store mapping by various keys for flexible lookup
                const mappingInfo = {
                    labelColumn: labelColumn,
                    idColumn: idColumn,
                    className: classLabel,
                    classUri: classUri,
                    attributeMappings: attributeMappings,
                    sqlQuery: mapping.sql_query || null,
                    dashboard: classInfo?.dashboard || null,
                    dashboardParams: classInfo?.dashboardParams || {},
                    bridges: classInfo?.bridges || [],
                    emoji: classInfo?.emoji || '📦',
                    description: classInfo?.description || '',
                    dataProperties: classInfo?.dataProperties || []
                };
                
                if (classLabel) {
                    entityMappings[classLabel.toLowerCase()] = mappingInfo;
                    console.log('[Mappings] Added key:', classLabel.toLowerCase());
                }
                if (classUri) {
                    entityMappings[classUri.toLowerCase()] = mappingInfo;
                    // Also by local part of URI
                    const localPart = classUri.split('#').pop().split('/').pop();
                    if (localPart) {
                        entityMappings[localPart.toLowerCase()] = mappingInfo;
                        console.log('[Mappings] Added URI key:', localPart.toLowerCase());
                    }
                }
            }
            console.log('[Mappings] Loaded entity mappings:', Object.keys(entityMappings).length, 'keys for', mappingsData.length, 'mappings');
            console.log('[Mappings] Entity mappings keys:', Object.keys(entityMappings));
        } else {
            console.log('[Mappings] No entities in response. data:', data);
            console.log('[Mappings] Tried: data.config.entities =', data?.config?.entities);
            console.log('[Mappings] Tried: data.entities =', data?.entities);
        }
    } catch (error) {
        console.error('[Mappings] Error loading entity mappings:', error);
    }
}

/**
 * Find ontology class by name or URI
 */
function findOntologyClass(classRef) {
    if (!classRef) return null;
    
    const refLower = classRef.toLowerCase();
    
    // Direct match
    if (ontologyClasses[refLower]) return ontologyClasses[refLower];
    
    // Try local part of URI
    const localPart = refLower.split('#').pop().split('/').pop();
    if (ontologyClasses[localPart]) return ontologyClasses[localPart];
    
    // Partial match
    for (const [key, info] of Object.entries(ontologyClasses)) {
        if (key.includes(localPart) || localPart.includes(key)) {
            return info;
        }
    }
    
    return null;
}
