/**
 * OntoBricks - query-loaders.js
 * Ontology and mapping loaders, entity type validation.
 * Extracted from query.js per code_instructions.txt
 */

// =====================================================
// ENTITY TYPE VALIDATION
// =====================================================

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
        if (typeof ontologyProperties !== 'undefined') ontologyProperties = {};
        
        if (data && data.success && data.config) {
            // Business rules: keep references to existing, enabled SWRL rules.
            const swrlByName = {};
            let hasSwrl = false;
            try {
                const swrlResp = await fetch('/ontology/swrl/list');
                const swrlData = await swrlResp.json();
                if (swrlData && Array.isArray(swrlData.rules)) {
                    hasSwrl = true;
                    swrlData.rules.forEach(r => { if (r && r.name) swrlByName[r.name] = r; });
                }
            } catch (e) {
                console.log('No SWRL rules loaded:', e.message);
            }
            // SHACL shapes: only their count per class is needed up front, to
            // gate the "Check SHACL facets" menu item without a round-trip.
            let shaclShapes = [];
            try {
                const shaclResp = await fetch('/ontology/dataquality/list', { credentials: 'same-origin' });
                const shaclData = await shaclResp.json();
                if (shaclData && Array.isArray(shaclData.shapes)) shaclShapes = shaclData.shapes;
            } catch (e) {
                console.log('No SHACL shapes loaded:', e.message);
            }
            const allClasses = data.config.classes || [];
            // Load classes
            for (const cls of allClasses) {
                const classInfo = {
                    name: cls.name || '',
                    label: cls.label || cls.name || '',
                    emoji: cls.emoji || '📦',
                    dashboard: cls.dashboard || null,
                    dashboardParams: cls.dashboardParams || {},
                    bridges: cls.bridges || [],
                    dataset: cls.dataset || null,
                    actions: cls.actions || [],
                    businessRules: (cls.business_rules || [])
                        .filter(ref => ref && ref.name && (!hasSwrl
                            || (swrlByName[ref.name] && swrlByName[ref.name].enabled !== false)))
                        .map(ref => ({ name: ref.name, description: (swrlByName[ref.name] || {}).description || '' })),
                    virtualAttributes: cls.virtualAttributes || [],
                    shaclShapeCount: (typeof shaclShapeCountFor === 'function')
                        ? shaclShapeCountFor(cls.name, allClasses, shaclShapes) : 0,
                    description: cls.description || cls.comment || '',
                    dataProperties: cls.dataProperties || []
                };
                if (cls.name) ontologyClasses[cls.name.toLowerCase()] = classInfo;
                if (cls.uri) {
                    ontologyClasses[cls.uri.toLowerCase()] = classInfo;
                    const localPart = cls.uri.split('#').pop().split('/').pop();
                    if (localPart) ontologyClasses[localPart.toLowerCase()] = classInfo;
                }
            }
            console.log('Loaded ontology classes:', Object.keys(ontologyClasses).length, 'keys for', (data.config.classes || []).length, 'classes');

            // Load properties (for label lookup in the graph viewer)
            for (const prop of (data.config.properties || [])) {
                const propInfo = {
                    name: prop.name || '',
                    label: prop.label || prop.name || ''
                };
                if (prop.name) ontologyProperties[prop.name.toLowerCase()] = propInfo;
                if (prop.uri) {
                    ontologyProperties[prop.uri.toLowerCase()] = propInfo;
                    const localPart = prop.uri.split('#').pop().split('/').pop();
                    if (localPart) ontologyProperties[localPart.toLowerCase()] = propInfo;
                }
            }
            console.log('Loaded ontology properties:', Object.keys(ontologyProperties).length, 'keys for', (data.config.properties || []).length, 'properties', '| sample:', Object.keys(ontologyProperties).slice(0, 5));
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
                    dataset: classInfo?.dataset || null,
                    actions: classInfo?.actions || [],
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
    if (ontologyClasses[refLower]) return ontologyClasses[refLower];
    const localPart = refLower.split('#').pop().split('/').pop();
    if (ontologyClasses[localPart]) return ontologyClasses[localPart];
    for (const [key, info] of Object.entries(ontologyClasses)) {
        if (key.includes(localPart) || localPart.includes(key)) return info;
    }
    return null;
}

function findOntologyProperty(propRef) {
    if (!propRef) return null;
    try {
        const refLower = propRef.toLowerCase();
        if (ontologyProperties[refLower]) return ontologyProperties[refLower];
        const localPart = refLower.split('#').pop().split('/').pop();
        if (localPart && localPart !== refLower && ontologyProperties[localPart]) return ontologyProperties[localPart];
    } catch (_) {}
    return null;
}
