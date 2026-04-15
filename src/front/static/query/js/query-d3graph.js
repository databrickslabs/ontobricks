/**
 * OntoBricks - query-d3graph.js
 * D3.js graph visualization: build, render, visual filters, and resize handling.
 * Extracted from query.js per code_instructions.txt
 */

// =====================================================
// D3.js GRAPH VISUALIZATION
// =====================================================

// Track if entity limit was exceeded
let entityLimitExceeded = false;

async function buildGraph(results, columns) {
    const noGraphMsg = document.getElementById('noGraphMessage');
    const loadingMsg = document.getElementById('graphLoading');
    const svgElement = document.getElementById('graphSvg');
    const tooManyMsg = document.getElementById('tooManyEntitiesMessage');
    
    // Hide all messages first
    if (noGraphMsg) noGraphMsg.style.display = 'none';
    if (tooManyMsg) tooManyMsg.style.display = 'none';
    
    // Hide SVG and show loading animation FIRST
    if (svgElement) svgElement.style.opacity = '0';
    if (loadingMsg) loadingMsg.style.display = 'block';
    d3.select('#graphSvg').selectAll('*').remove();
    
    // Small delay to allow the loading animation to render
    await new Promise(resolve => setTimeout(resolve, 50));
    
    // Ensure entity mappings are loaded
    if (Object.keys(entityMappings).length === 0) {
        console.log('Entity mappings not loaded, loading now...');
        await loadEntityMappings();
    }
    
    // Update container height first
    updateQueryGraphHeight();
    
    if (!results || results.length === 0) {
        if (loadingMsg) loadingMsg.style.display = 'none';
        if (noGraphMsg) noGraphMsg.style.display = 'block';
        d3NodesData = [];
        d3LinksData = [];
        entityLimitExceeded = false;
        return;
    }
    
    const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
    const RDFS_LABEL = 'http://www.w3.org/2000/01/rdf-schema#label';
    
    // Detect query format: triple format (s/p/o) or direct columns
    const colLower = columns.map(c => c.toLowerCase());
    const isTripleFormat = colLower.includes('predicate') || colLower.includes('p');
    
    // Helper to get row value case-insensitively
    function getRowValue(row, colName) {
        if (!row || !colName) return null;
        // Try exact match first
        if (row[colName] !== undefined) return row[colName];
        // Try case-insensitive match
        const colLower = colName.toLowerCase();
        for (const key of Object.keys(row)) {
            if (key.toLowerCase() === colLower) return row[key];
        }
        return null;
    }
    
    // Find column names (case-insensitive)
    const subjectCol = columns.find(c => c.toLowerCase() === 'subject' || c.toLowerCase() === 's') || columns[0];
    const predicateCol = columns.find(c => c.toLowerCase() === 'predicate' || c.toLowerCase() === 'p');
    const objectCol = columns.find(c => c.toLowerCase() === 'object' || c.toLowerCase() === 'o');
    
    // Direct column format detection
    const labelCol = columns.find(c => c.toLowerCase() === 'label' || c.toLowerCase().includes('label') || c.toLowerCase() === 'name');
    const typeCol = columns.find(c => c.toLowerCase() === 'type' || c.toLowerCase().includes('type'));
    
    console.log('=== GRAPH BUILD DEBUG ===');
    console.log('Query format:', isTripleFormat ? 'triple (s/p/o)' : 'direct columns');
    console.log('Columns:', columns);
    console.log('Detected: subject=', subjectCol, ', predicate=', predicateCol, ', object=', objectCol);
    console.log('Detected: label=', labelCol, ', type=', typeCol);
    
    // Debug: log sample rows
    if (results.length > 0) {
        console.log('Sample row keys:', Object.keys(results[0]));
        console.log('Sample row:', results[0]);
        if (labelCol) {
            console.log('Label column value in sample:', results[0][labelCol], '| via getRowValue:', getRowValue(results[0], labelCol));
        }
    }
    
    // Debug: log all unique predicates if triple format
    if (isTripleFormat && predicateCol) {
        const uniquePredicates = new Set(results.map(r => getRowValue(r, predicateCol)).filter(p => p));
        console.log('Unique predicates in results:', Array.from(uniquePredicates));
    }
    
    const entities = new Map();
    const relationships = [];
    const typeURIs = new Set(); // Track URIs that are types/classes, not instances
    const allSubjects = new Set(); // Track all URIs that appear as subjects (have real triples)
    
    if (isTripleFormat) {
        // =========== TRIPLE FORMAT (s/p/o) ===========
        
        // Pre-pass: Identify type URIs and collect all subjects
        for (const row of results) {
            const subject = getRowValue(row, subjectCol) || '';
            const predicate = predicateCol ? (getRowValue(row, predicateCol) || '') : '';
            const object = objectCol ? (getRowValue(row, objectCol) || '') : '';
            
            if (subject) {
                allSubjects.add(subject);
            }
            
            const isTypePredicate = predicate && (predicate === RDF_TYPE || 
                                   predicate.endsWith('#type') || 
                                   predicate.endsWith('/type'));
            
            if (isTypePredicate && object && (object.startsWith('http://') || object.startsWith('https://'))) {
                typeURIs.add(object);
            }
        }
        console.log('Identified type URIs (not entities):', Array.from(typeURIs));
        console.log('Distinct subjects in data:', allSubjects.size);
        
        // Debug: Log first few raw rows to understand data format
        console.log('=== RAW DATA SAMPLE (first 5 rows) ===');
        for (let i = 0; i < Math.min(5, results.length); i++) {
            const row = results[i];
            console.log(`Row ${i}: subject="${getRowValue(row, subjectCol)}", predicate="${predicateCol ? getRowValue(row, predicateCol) : 'N/A'}", object="${objectCol ? getRowValue(row, objectCol) : 'N/A'}"`);
        }
        
        // Pass 1: Create all entities (from subjects and URI objects that are NOT types)
        for (const row of results) {
            const subject = getRowValue(row, subjectCol) || '';
            const predicate = predicateCol ? (getRowValue(row, predicateCol) || '') : '';
            const object = objectCol ? (getRowValue(row, objectCol) || '') : '';
            
            // Add subject as entity (subjects are always instances)
            if (subject && !entities.has(subject) && !typeURIs.has(subject)) {
                entities.set(subject, {
                    id: subject,
                    label: null,
                    type: null,
                    instanceId: extractInstanceId(subject),
                    hasRealLabel: false,
                    attributes: {}
                });
            }
            
            // Add object as entity if it's a URI or a short ID reference (and not a type URI)
            // IMPORTANT: Only add if the object also appears as a subject somewhere in the data,
            // meaning it has its own triples in the triple store. This prevents ghost entities
            // (URIs that are relationship targets but have no data of their own).
            const isObjectUri = object && (object.startsWith('http://') || object.startsWith('https://'));
            const isObjectShortId = object && !isObjectUri && (
                /^[A-Za-z]+[0-9]+$/.test(object) ||
                /^[A-Za-z]+_[0-9]+$/.test(object)
            );
            
            if ((isObjectUri || isObjectShortId) && !entities.has(object) && !typeURIs.has(object)) {
                const predLower = (predicate || '').toLowerCase();
                const predLocalName = extractEntityLabel(predicate).toLowerCase();
                const isLiteralPredicate = predLower.includes('label') || predLower.includes('name') || 
                                           predLower.includes('type') || predLower.includes('email') ||
                                           predLower.includes('date') || predLower.includes('phone') ||
                                           predLocalName.endsWith('id') || predLocalName.endsWith('_id') ||
                                           predLocalName === 'id' || predLocalName === 'identifier';
                
                // Only create an entity node if the object appears as a subject in the data
                const objectHasTriples = allSubjects.has(object);
                
                if (!isLiteralPredicate && objectHasTriples) {
                    entities.set(object, {
                        id: object,
                        label: null,
                        type: null,
                        instanceId: isObjectUri ? extractInstanceId(object) : object,
                        hasRealLabel: false,
                        attributes: {}
                    });
                } else if (!isLiteralPredicate && !objectHasTriples) {
                    console.log(`[Graph] Skipping ghost entity (no triples as subject): ${object.slice(-40)}`);
                }
            }
        }
        
        // DEBUG: Log entities created from relationship objects (Pass 1)
        console.log('=== PASS 1 COMPLETE: Entities created ===');
        const entitiesFromRelationships = [];
        entities.forEach((entity, uri) => {
            if (!entity.hasRealLabel && !entity.type) {
                entitiesFromRelationships.push(uri);
            }
        });
        console.log(`Total entities: ${entities.size}, entities from relationship objects (no attrs yet): ${entitiesFromRelationships.length}`);
        if (entitiesFromRelationships.length > 0) {
            console.log('Sample entities from relationships:', entitiesFromRelationships.slice(0, 5));
        }
        
        // Pass 2: Capture type, label, and attributes
        for (const row of results) {
            const subject = getRowValue(row, subjectCol) || '';
            const predicate = predicateCol ? (getRowValue(row, predicateCol) || '') : '';
            const object = objectCol ? (getRowValue(row, objectCol) || '') : '';
            
            if (!subject || !predicate) continue;
            
            // DEBUG: Check if this subject exists as an entity
            if (!entities.has(subject) && !typeURIs.has(subject)) {
                // This subject doesn't match any entity - log for debugging
                const instanceId = extractInstanceId(subject);
                if (instanceId.match(/^P\d+$/) || instanceId.match(/^\d+$/)) {
                    console.log(`[DEBUG] Triple subject not in entities: ${subject}`);
                }
            }
            
            // Capture rdf:type (be strict - only match actual type predicates, not attributes ending in "type")
            const predicateLocalName = extractEntityLabel(predicate).toLowerCase();
            const isTypePredicate = predicate === RDF_TYPE || 
                                   predicate.endsWith('#type') || 
                                   predicate.endsWith('/type') ||
                                   predicateLocalName === 'type' ||
                                   predicateLocalName === 'rdf:type' ||
                                   predicateLocalName === 'a';  // SPARQL shorthand
            if (isTypePredicate && entities.has(subject)) {
                const entity = entities.get(subject);
                const extractedType = extractEntityLabel(object);
                console.log(`Setting entity type: ${subject.substring(subject.length-20)} -> ${extractedType} (from ${object})`);
                entity.type = extractedType;
                entity.typeUri = object;  // Store full URI for filtering
            }
            
            // Capture rdfs:label (be selective to avoid picking up wrong attributes)
            const predicateLower = predicate.toLowerCase();
            const predicateLocal = extractEntityLabel(predicate).toLowerCase();
            const isLabelPredicate = predicate === RDFS_LABEL || 
                                    predicate.endsWith('#label') || 
                                    predicate.endsWith('/label') ||
                                    predicateLocal === 'label' ||
                                    predicateLocal === 'name' ||
                                    predicateLocal === 'fullname' ||
                                    predicateLocal === 'full_name' ||
                                    predicateLocal === 'displayname';
            if (isLabelPredicate && object && !object.startsWith('http') && entities.has(subject)) {
                const entity = entities.get(subject);
                // Only overwrite if we don't have a real label yet
                if (!entity.hasRealLabel) {
                    entity.label = object;
                    entity.hasRealLabel = true;
                    console.log('Triple format: Found label for', subject, 'via predicate', predicate, ':', object);
                }
            }
            
            // Store all literal attributes
            if (object && !object.startsWith('http') && entities.has(subject)) {
                const entity = entities.get(subject);
                const predicateName = extractEntityLabel(predicate);
                entity.attributes[predicateName] = object;
            }
        }
        
        // DEBUG: Log entities still without labels after Pass 2
        console.log('=== PASS 2 COMPLETE: Attribute assignment ===');
        const entitiesWithoutLabels = [];
        const entitiesWithoutTypes = [];
        entities.forEach((entity, uri) => {
            if (!entity.hasRealLabel) {
                entitiesWithoutLabels.push({ uri: uri.slice(-30), type: entity.type, attrs: Object.keys(entity.attributes) });
            }
            if (!entity.type) {
                entitiesWithoutTypes.push(uri.slice(-30));
            }
        });
        console.log(`Entities without labels: ${entitiesWithoutLabels.length}/${entities.size}`);
        if (entitiesWithoutLabels.length > 0 && entitiesWithoutLabels.length <= 10) {
            console.log('Entities without labels:', entitiesWithoutLabels);
        } else if (entitiesWithoutLabels.length > 10) {
            console.log('Sample entities without labels:', entitiesWithoutLabels.slice(0, 5));
        }
        console.log(`Entities without types: ${entitiesWithoutTypes.length}/${entities.size}`);
        
        // Pass 2b: Bridge attributes for subclass entities
        // If an entity created from a relationship (e.g., .../Person/M001) has no attributes,
        // try to find another entity with the same ID but different class (e.g., .../Manager/M001)
        // and copy attributes from it. This handles subclass relationships (Manager extends Person).
        entities.forEach((entity, uri) => {
            // Skip entities that already have attributes
            if (entity.hasRealLabel || Object.keys(entity.attributes).length > 0) {
                return;
            }
            
            // Extract the instance ID from this entity's URI
            const instanceId = extractInstanceId(uri);
            if (!instanceId) return;
            
            // Look for another entity with the same instance ID but different class
            for (const [otherUri, otherEntity] of entities) {
                if (otherUri === uri) continue;  // Skip self
                
                const otherId = extractInstanceId(otherUri);
                if (otherId !== instanceId) continue;  // Different ID
                
                // Found a match! Check if it has attributes
                if (otherEntity.hasRealLabel || Object.keys(otherEntity.attributes).length > 0) {
                    console.log(`[Bridge] Copying attributes from ${otherUri.slice(-30)} to ${uri.slice(-30)}`);
                    
                    // Copy attributes (but not type - keep inferred type)
                    if (otherEntity.label && !entity.hasRealLabel) {
                        entity.label = otherEntity.label;
                        entity.hasRealLabel = true;
                    }
                    // Copy all other attributes
                    for (const [attrKey, attrValue] of Object.entries(otherEntity.attributes)) {
                        if (!(attrKey in entity.attributes)) {
                            entity.attributes[attrKey] = attrValue;
                        }
                    }
                    break;  // Stop after first match
                }
            }
        });
        
        // Pass 3: Build relationships
        // Also track relationship attribute triples (where subject matches a relationship subject but object is a literal)
        const relationshipAttributes = new Map();  // Key: subject, Value: {predicate: value, ...}
        
        // Debug: Count relationship candidates
        let skippedType = 0, skippedLabel = 0, skippedLiteral = 0, createdRel = 0;
        
        for (const row of results) {
            const subject = getRowValue(row, subjectCol) || '';
            const predicate = predicateCol ? (getRowValue(row, predicateCol) || '') : '';
            const object = objectCol ? (getRowValue(row, objectCol) || '') : '';
            
            if (!subject || !predicate || !object) continue;
            if (predicate === RDF_TYPE || predicate.endsWith('#type') || predicate.endsWith('/type')) {
                skippedType++;
                continue;
            }
            if (predicate === RDFS_LABEL || predicate.endsWith('#label') || predicate.endsWith('/label')) {
                skippedLabel++;
                continue;
            }
            
            // Detect if object is a URI or a short ID reference (entity identifier)
            const isUriObject = object.startsWith('http://') || object.startsWith('https://');
            const isShortIdObject = !isUriObject && (
                // Pattern 1: Letters followed by numbers (CUST00189, INT000054, CON00004)
                /^[A-Za-z]+[0-9]+$/.test(object) ||
                // Pattern 2: Letters, underscore, numbers (CUST_00189)
                /^[A-Za-z]+_[0-9]+$/.test(object) ||
                // Pattern 3: The object exists as a known entity (created from subjects)
                entities.has(object)
            );
            
            // Check if this is a literal object (not a URI and not a short ID reference)
            if (!isUriObject && !isShortIdObject) {
                // This could be a relationship attribute - store it for later association
                // Predicate format: RelationshipName_AttributeName (e.g., WorkWith_CollaborationType)
                const fullPredName = extractEntityLabel(predicate);
                
                // Check if this is a relationship attribute (contains underscore with relationship name)
                // Store both the relationship name and attribute name for proper matching
                if (fullPredName.includes('_')) {
                    const parts = fullPredName.split('_');
                    const relName = parts[0];  // e.g., "WorkWith"
                    const attrName = parts.slice(1).join('_');  // e.g., "CollaborationType"
                    
                    // Store with a key that includes the relationship name for proper matching
                    // Key format: subject|relName -> {attrName: value}
                    const key = `${subject}|${relName}`;
                    if (!relationshipAttributes.has(key)) {
                        relationshipAttributes.set(key, {});
                    }
                    relationshipAttributes.get(key)[attrName] = object;
                    
                    // Debug: Special logging for CollaborationType
                    if (attrName.toLowerCase().includes('collaboration')) {
                        console.log(`[DEBUG CollaborationType] Found: subject=${subject}, relName=${relName}, attr=${attrName}, value=${object}`);
                    }
                } else {
                    // Legacy format without relationship name prefix
                    if (!relationshipAttributes.has(subject)) {
                        relationshipAttributes.set(subject, {});
                    }
                    relationshipAttributes.get(subject)[fullPredName] = object;
                }
                continue;
            }
            
            // Debug: Log first few relationship candidates
            if (relationships.length < 5) {
                console.log(`[Graph Debug] Relationship candidate: subject=${subject}, predicate=${extractEntityLabel(predicate)}, object=${object}`);
                console.log(`[Graph Debug]   isUriObject=${isUriObject}, isShortIdObject=${isShortIdObject}`);
            }
            
            if (isUriObject || isShortIdObject) {
                const predName = extractEntityLabel(predicate);
                
                // Skip predicates that are clearly data properties (identifiers)
                const predNameLower = predName.toLowerCase();
                if (predNameLower.endsWith('id') || predNameLower.endsWith('_id') ||
                    predNameLower === 'id' || predNameLower === 'identifier') {
                    skippedLiteral++;
                    continue;
                }
                
                // Skip self-referencing relationships (object is the entity's own ID)
                if (entities.has(subject)) {
                    const subjectEntity = entities.get(subject);
                    if (subjectEntity.instanceId && subjectEntity.instanceId === object) {
                        skippedLiteral++;
                        continue;
                    }
                }
                if (subject === object) {
                    skippedLiteral++;
                    continue;
                }
                
                // For short ID objects, ensure the target entity exists
                // Only create if the short ID appears as a subject (has real triples)
                if (isShortIdObject && !entities.has(object)) {
                    if (allSubjects.has(object)) {
                        entities.set(object, {
                            id: object,
                            label: null,
                            type: null,
                            instanceId: object,
                            hasRealLabel: false,
                            attributes: {}
                        });
                        console.log(`[Graph] Created entity from short ID relationship target: ${object}`);
                    } else {
                        console.log(`[Graph] Skipping short ID ghost entity (no triples): ${object}`);
                    }
                }
                
                // Skip relationship if either endpoint doesn't exist as an entity
                if (!entities.has(subject) || !entities.has(object)) {
                    skippedLiteral++;
                    continue;
                }
                
                // Collect any additional columns from this row as relationship attributes
                const relAttributes = {};
                
                // First, check if there's a JSON 'attributes' column from the backend
                const attrCol = columns.find(c => c.toLowerCase() === 'attributes');
                if (attrCol) {
                    const attrJson = getRowValue(row, attrCol);
                    if (attrJson && attrJson !== 'null') {
                        try {
                            const parsed = typeof attrJson === 'string' ? JSON.parse(attrJson) : attrJson;
                            Object.assign(relAttributes, parsed);
                            console.log(`[Triple] Parsed attributes JSON for ${predName}:`, parsed);
                        } catch (e) {
                            console.warn(`[Triple] Failed to parse attributes JSON:`, attrJson, e);
                        }
                    }
                }
                
                // Also capture any other additional columns
                for (const col of columns) {
                    const colLower = col.toLowerCase();
                    // Skip the standard triple columns and the attributes column
                    if (colLower === 'subject' || colLower === 's' || 
                        colLower === 'predicate' || colLower === 'p' ||
                        colLower === 'object' || colLower === 'o' ||
                        colLower === 'attributes') continue;
                    const val = getRowValue(row, col);
                    if (val !== null && val !== undefined && val !== '') {
                        relAttributes[col] = val;
                    }
                }
                
                // Debug: log relationship attributes if any
                if (Object.keys(relAttributes).length > 0) {
                    console.log(`[Triple] Relationship ${predName} has attributes:`, relAttributes);
                }
                
                relationships.push({
                    source: subject,
                    target: object,
                    predicate: predName,
                    predicateUri: predicate,  // Store full URI for filtering
                    attributes: relAttributes  // Store any additional row data
                });
                createdRel++;
            } else {
                skippedLiteral++;
                // Debug: Log first few skipped literals
                if (skippedLiteral <= 5) {
                    console.log(`[Graph Debug] Skipped literal: subject=${subject}, predicate=${extractEntityLabel(predicate)}, object=${object}`);
                }
            }
        }
        
        // Debug: Log relationship creation summary
        console.log('=== RELATIONSHIP CREATION SUMMARY ===');
        console.log(`Total rows processed: ${results.length}`);
        console.log(`Skipped (type predicates): ${skippedType}`);
        console.log(`Skipped (label predicates): ${skippedLabel}`);
        console.log(`Skipped (literals/non-entity objects): ${skippedLiteral}`);
        console.log(`Relationships created: ${createdRel}`);
        console.log(`Final relationships array length: ${relationships.length}`);
        
        // Pass 3b: Associate collected literal attributes with relationships
        // Match by both source entity AND relationship type
        // Key format in relationshipAttributes: "subject|relName" -> {attrName: value}
        
        for (const rel of relationships) {
            // Build the lookup key: source|relationshipName
            const key = `${rel.source}|${rel.predicate}`;
            const subjectAttrs = relationshipAttributes.get(key);
            
            if (subjectAttrs && Object.keys(subjectAttrs).length > 0) {
                if (!rel.attributes) rel.attributes = {};
                for (const [attrKey, attrValue] of Object.entries(subjectAttrs)) {
                    rel.attributes[attrKey] = attrValue;
                    console.log(`[Attr] Added attribute to relationship ${rel.predicate}: ${attrKey} = ${attrValue}`);
                }
            }
        }
        
        // Debug: Log final relationship attributes
        console.log('[DEBUG] Final relationships with attributes:', 
            relationships.filter(r => r.attributes && Object.keys(r.attributes).length > 0)
                .map(r => ({predicate: r.predicate, attrs: r.attributes})));
        
    } else {
        // =========== DETECT MULTI-ENTITY FORMAT ===========
        // Check if we have multiple entity columns (e.g., department, person)
        // Entity columns are those without _label, _type suffix and have URI values
        const entityColumns = [];
        const labelColumns = {};
        
        for (const col of columns) {
            const colLower = col.toLowerCase();
            if (colLower.endsWith('_label') || colLower === 'label') {
                // This is a label column - find its parent entity
                const parentCol = col.replace(/_label$/i, '').replace(/^label$/i, '');
                if (parentCol) labelColumns[parentCol.toLowerCase()] = col;
            } else if (!colLower.endsWith('_type') && colLower !== 'type') {
                // Check if this column has URI values (making it an entity column)
                const sampleValue = results[0] ? getRowValue(results[0], col) : null;
                if (sampleValue && typeof sampleValue === 'string' && 
                    (sampleValue.startsWith('http://') || sampleValue.startsWith('https://'))) {
                    entityColumns.push(col);
                }
            }
        }
        
        const isMultiEntityFormat = entityColumns.length > 1;
        console.log('Detected entity columns:', entityColumns);
        console.log('Label columns mapping:', labelColumns);
        console.log('Is multi-entity format:', isMultiEntityFormat);
        
        if (isMultiEntityFormat) {
            // =========== MULTI-ENTITY FORMAT ===========
            // Each row represents a relationship between multiple entities
            console.log('Processing multi-entity format with', results.length, 'rows');
            
            for (const row of results) {
                // Add each entity from this row
                for (const entityCol of entityColumns) {
                    const entityUri = getRowValue(row, entityCol);
                    if (!entityUri) continue;
                    
                    if (!entities.has(entityUri)) {
                        const entity = {
                            id: entityUri,
                            label: null,
                            type: entityCol, // Use column name as type
                            instanceId: extractInstanceId(entityUri),
                            hasRealLabel: false,
                            attributes: {}
                        };
                        
                        // Get label from corresponding label column
                        const labelColName = labelColumns[entityCol.toLowerCase()] || `${entityCol}_label`;
                        const labelValue = getRowValue(row, labelColName);
                        if (labelValue !== null && labelValue !== undefined && labelValue !== '') {
                            entity.label = String(labelValue);
                            entity.hasRealLabel = true;
                        }
                        
                        entities.set(entityUri, entity);
                    }
                }
                
                // Collect non-entity columns as relationship attributes
                const rowAttributes = {};
                for (const col of columns) {
                    // Skip entity columns and their labels
                    if (entityColumns.includes(col)) continue;
                    const colLower = col.toLowerCase();
                    // Only skip label columns (not _type columns which may contain actual attribute values)
                    if (colLower.endsWith('_label')) continue;
                    // Skip columns that look like entity type descriptors (entity_name_type pattern)
                    // but keep columns like collaboration_type, payment_type, etc.
                    const isEntityTypeCol = entityColumns.some(ec => colLower === ec.toLowerCase() + '_type');
                    if (isEntityTypeCol) continue;
                    
                    const val = getRowValue(row, col);
                    if (val !== null && val !== undefined && val !== '') {
                        // Include both strings and other types (numbers, dates, etc.)
                        const valStr = String(val);
                        if (!valStr.startsWith('http://') && !valStr.startsWith('https://')) {
                            rowAttributes[col] = val;
                        }
                    }
                }
                
                // Debug: log attributes found
                if (Object.keys(rowAttributes).length > 0) {
                    console.log('Row attributes found:', rowAttributes);
                }
                
                // Create relationships between entities in the same row
                // The relationship exists because they're in the same row (from the JOIN)
                for (let i = 0; i < entityColumns.length; i++) {
                    for (let j = i + 1; j < entityColumns.length; j++) {
                        const entity1Uri = getRowValue(row, entityColumns[i]);
                        const entity2Uri = getRowValue(row, entityColumns[j]);
                        
                        if (entity1Uri && entity2Uri) {
                            // Determine relationship direction based on column order
                            const rel = {
                                source: entity1Uri,
                                target: entity2Uri,
                                predicate: `${entityColumns[i]} → ${entityColumns[j]}`,
                                attributes: { ...rowAttributes }  // Include row attributes
                            };
                            console.log('Creating relationship with attributes:', rel.predicate, rel.attributes);
                            relationships.push(rel);
                        }
                    }
                }
            }
        } else {
            // =========== SINGLE ENTITY DIRECT COLUMN FORMAT ===========
            // Each row represents an entity with its properties as columns
            console.log('Processing direct column format with', results.length, 'rows');
            
            for (const row of results) {
                const subject = getRowValue(row, subjectCol) || '';
                if (!subject) continue;
                
                if (!entities.has(subject)) {
                    const entity = {
                        id: subject,
                        label: null,
                        type: null,
                        instanceId: extractInstanceId(subject),
                        hasRealLabel: false,
                        attributes: {}
                    };
                    
                    // Get type from type column
                    const typeValue = getRowValue(row, typeCol);
                    if (typeValue) {
                        entity.type = extractEntityLabel(typeValue);
                    }
                    
                    // Get label from label column
                    const labelValue = getRowValue(row, labelCol);
                    if (labelValue !== null && labelValue !== undefined && labelValue !== '') {
                        entity.label = String(labelValue);
                        entity.hasRealLabel = true;
                        console.log('Direct format: Found label for', subject, ':', labelValue, '| Column:', labelCol);
                    } else {
                        console.log('Direct format: No label found for', subject, '| labelCol:', labelCol, '| value:', labelValue);
                    }
                    
                    // Store all other columns as attributes (including label, type, etc.)
                    for (const col of columns) {
                        if (col === subjectCol) continue;
                        const colValue = getRowValue(row, col);
                        // Store any non-null, non-undefined value (convert to string if needed)
                        if (colValue !== null && colValue !== undefined) {
                            const strValue = String(colValue);
                            // Only skip URIs - keep all other values including labels
                            if (!strValue.startsWith('http://') && !strValue.startsWith('https://')) {
                                entity.attributes[col] = strValue;
                            }
                        }
                    }
                    
                    entities.set(subject, entity);
                } else {
                    // Update existing entity with any new info
                    const entity = entities.get(subject);
                    const labelValue = getRowValue(row, labelCol);
                    if (labelValue !== null && labelValue !== undefined && !entity.hasRealLabel) {
                        entity.label = String(labelValue);
                        entity.hasRealLabel = true;
                        console.log('Direct format: Updated label for', subject, ':', labelValue);
                    }
                    for (const col of columns) {
                        if (col === subjectCol) continue;
                        const colValue = getRowValue(row, col);
                        if (colValue !== null && colValue !== undefined) {
                            const strValue = String(colValue);
                            if (!strValue.startsWith('http://') && !strValue.startsWith('https://')) {
                                entity.attributes[col] = strValue;
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Pass: Try to find label from mapping's label column (for entities without labels)
    entities.forEach((entity, key) => {
        if (entity.hasRealLabel) {
            console.log('Entity', key.substring(key.length-20), 'already has label:', entity.label);
            return;
        }
        
        const typeLower = (entity.type || '').toLowerCase();
        const mapping = entityMappings[typeLower] || findMappingByType(entity.type);
        
        console.log('Looking for label mapping for entity type:', entity.type, '| mapping found:', !!mapping);
        
        if (mapping && mapping.labelColumn && entity.attributes) {
            const labelColName = mapping.labelColumn;
            const labelColNameLower = labelColName.toLowerCase();
            
            console.log('Mapping labelColumn:', labelColName, '| Entity attributes:', Object.keys(entity.attributes));
            
            for (const [attrName, attrValue] of Object.entries(entity.attributes)) {
                if (attrName.toLowerCase() === labelColNameLower || 
                    attrName.toLowerCase().endsWith(labelColNameLower) ||
                    attrName.toLowerCase().includes(labelColNameLower) ||
                    labelColNameLower.includes(attrName.toLowerCase())) {
                    entity.label = attrValue;
                    entity.hasRealLabel = true;
                    console.log('Found label from mapping for', key.substring(key.length-20), ':', attrValue);
                    break;
                }
            }
        }
        
        // If still no label, try the 'label' attribute directly
        if (!entity.hasRealLabel && entity.attributes) {
            const labelAttr = entity.attributes['label'] || entity.attributes['Label'] || entity.attributes['name'] || entity.attributes['Name'];
            if (labelAttr) {
                entity.label = labelAttr;
                entity.hasRealLabel = true;
                console.log('Found label from attribute for', key.substring(key.length-20), ':', labelAttr);
            }
        }
    });
    
    // Pass 4: Finalize entities - set default labels and infer types
    entities.forEach((entity, key) => {
        // Infer type from URI if not set
        if (!entity.type) {
            entity.type = inferTypeFromUri(entity.id);
        }
        
        // Also store the full type URI for filtering
        entity.typeUri = entity.typeUri || null;
        
        // Set default label from URI if not set
        if (!entity.label) {
            entity.label = extractEntityLabel(entity.id);
        }
    });
    
    // Apply Visual Search filters if defined (from SearchBuilder.queryFilters)
    let filteredRelationships = relationships;
    const hasSearchFilters = typeof SearchBuilder !== 'undefined' && SearchBuilder.queryFilters;
    const selectedEntityTypes = hasSearchFilters ? (SearchBuilder.queryFilters.entityTypes || []) : [];
    const selectedEntityTypeNames = hasSearchFilters ? (SearchBuilder.queryFilters.entityTypeNames || []) : [];
    const selectedRelationships = hasSearchFilters ? (SearchBuilder.queryFilters.relationships || []) : [];
    
    // Build sets for matching (case-insensitive)
    const selectedTypeUris = new Set(selectedEntityTypes.map(u => u.toLowerCase()));
    const selectedTypeNames = new Set(selectedEntityTypeNames);  // Already lowercase
    
    // Helper function to check if entity type matches selection
    function entityTypeMatches(entity) {
        if (selectedEntityTypes.length === 0) return true;
        
        // Check by type URI (exact match)
        if (entity.typeUri && selectedTypeUris.has(entity.typeUri.toLowerCase())) {
            return true;
        }
        
        // Check by type name (extracted label) - most reliable
        const typeName = entity.type ? entity.type.toLowerCase() : '';
        if (selectedTypeNames.has(typeName)) {
            return true;
        }
        
        // Also check if type URI ends with any of the selected names
        if (entity.typeUri) {
            const typeUriLower = entity.typeUri.toLowerCase();
            for (const name of selectedTypeNames) {
                if (typeUriLower.endsWith('#' + name) || typeUriLower.endsWith('/' + name)) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    if (hasSearchFilters && (selectedEntityTypes.length > 0 || selectedRelationships.length > 0)) {
        console.log('=== APPLYING VISUAL SEARCH FILTERS ===');
        console.log('Selected entity types:', selectedEntityTypes);
        console.log('Selected type names:', Array.from(selectedTypeNames));
        console.log('Selected relationships:', selectedRelationships.map(r => r.uri));
        
        // Debug: Show sample entity types
        let sampleCount = 0;
        entities.forEach((entity, id) => {
            if (sampleCount < 5) {
                console.log(`Entity sample: type="${entity.type}", typeUri="${entity.typeUri}"`);
                sampleCount++;
            }
        });
        
        // Filter relationships by selected relationship types (if any selected)
        if (selectedRelationships.length > 0) {
            const selectedRelUris = new Set(selectedRelationships.map(r => r.uri.toLowerCase()));
            const selectedRelNames = new Set(selectedRelationships.map(r => r.name.toLowerCase()));
            
            filteredRelationships = relationships.filter(rel => {
                const predUri = (rel.predicateUri || '').toLowerCase();
                const predName = rel.predicate ? rel.predicate.toLowerCase() : '';
                
                // Check exact URI match
                if (selectedRelUris.has(predUri)) return true;
                
                // Check name match
                if (selectedRelNames.has(predName)) return true;
                
                // Check if predicate URI ends with the relationship name
                for (const relName of selectedRelNames) {
                    if (predUri.endsWith('#' + relName) || predUri.endsWith('/' + relName)) {
                        return true;
                    }
                }
                
                return false;
            });
            console.log('Relationships after filter:', filteredRelationships.length, 'of', relationships.length);
            
            // Debug: Show filtered relationship predicates
            const filteredPreds = new Set(filteredRelationships.map(r => r.predicate));
            console.log('Filtered relationship predicates:', Array.from(filteredPreds));
        }
    }
    
    // Build set of entity IDs involved in filtered relationships
    const relationshipEntityIds = new Set();
    filteredRelationships.forEach(r => {
        relationshipEntityIds.add(r.source);
        relationshipEntityIds.add(r.target);
    });
    
    // Filter entities:
    // - If entity types selected: keep entities of those types OR involved in selected relationships
    // - Always filter out empty/invalid entities
    const filteredEntities = Array.from(entities.values()).filter(entity => {
        // If Visual Search filters are active with entity types, apply type filter
        if (hasSearchFilters && selectedEntityTypes.length > 0) {
            const typeMatches = entityTypeMatches(entity);
            const inRelationship = relationshipEntityIds.has(entity.id);
            
            // Must match type OR be involved in a selected relationship
            if (!typeMatches && !inRelationship) {
                return false;
            }
        }
        
        // Keep if it has a real label
        if (entity.hasRealLabel && entity.label) return true;
        
        // Keep if it has meaningful attributes (not just type)
        const attrCount = Object.keys(entity.attributes || {}).filter(k => 
            k.toLowerCase() !== 'type' && k.toLowerCase() !== 'label'
        ).length;
        if (attrCount > 0) return true;
        
        // Keep if it's involved in relationships
        if (relationshipEntityIds.has(entity.id)) return true;
        
        // Keep if it has a type (it's a real instance)
        if (entity.type) return true;
        
        // Filter out - likely a type URI or empty entity
        console.log('Filtering out empty entity:', entity.id);
        return false;
    });
    
    // Use filtered relationships
    const finalRelationships = filteredRelationships;
    
    console.log('=== FILTER RESULTS ===');
    console.log('Final entities:', filteredEntities.length);
    console.log('Final relationships:', finalRelationships.length);
    
    d3NodesData = filteredEntities;
    d3LinksData = finalRelationships;
    
    allRelationshipTypes = new Set(d3LinksData.map(l => l.predicate));
    
    // Check entity limit before rendering
    const entityLimitSelect = document.getElementById('entityLimit');
    const entityLimit = entityLimitSelect ? parseInt(entityLimitSelect.value) : 100;
    // Note: tooManyMsg, loadingMsg, svgElement already declared at top of function
    
    if (entityLimit > 0 && d3NodesData.length > entityLimit) {
        console.log(`[Graph] Entity limit exceeded: ${d3NodesData.length} > ${entityLimit}`);
        entityLimitExceeded = true;
        
        // Update the message with counts
        const entityCountDisplay = document.getElementById('entityCountDisplay');
        const entityLimitDisplay = document.getElementById('entityLimitDisplay');
        if (entityCountDisplay) entityCountDisplay.textContent = d3NodesData.length;
        if (entityLimitDisplay) entityLimitDisplay.textContent = entityLimit;
        
        // Show the "too many entities" message
        if (loadingMsg) loadingMsg.style.display = 'none';
        if (svgElement) svgElement.style.opacity = '1';
        if (tooManyMsg) tooManyMsg.style.display = 'block';
        
        // Populate search entity types so user can search
        if (typeof populateSearchEntityTypes === 'function') {
            populateSearchEntityTypes();
        }
        
        // Hide graph stats
        const graphStats = document.getElementById('graphStats');
        if (graphStats) graphStats.style.display = 'none';
        
        return;
    }
    
    entityLimitExceeded = false;
    if (tooManyMsg) tooManyMsg.style.display = 'none';
    
    // Reset visual filters to include all types when new data loads
    visualFilterEntityTypes = new Set(d3NodesData.map(n => n.type).filter(t => t));
    visualFilterRelationships = new Set(d3LinksData.map(l => l.predicate).filter(p => p));
    populateVisualFilters();
    
    var graphStatsEl = document.getElementById('graphStats');
    if (graphStatsEl) graphStatsEl.style.display = 'block';
    
    // Debug: Show what labels are being used
    console.log('=== FINAL GRAPH DATA ===');
    console.log('D3 Graph:', d3NodesData.length, 'nodes (filtered from', entities.size, '),', d3LinksData.length, 'links');
    console.log('Entity mappings available:', Object.keys(entityMappings));
    
    renderD3Graph(true);  // Hide while rendering since loading message is shown
}


function extractEntityLabel(uri) {
    if (!uri) return '';
    if (uri.includes('#')) {
        const parts = uri.split('#');
        const localName = parts[parts.length - 1];
        if (localName.includes('/')) return localName.split('/')[0];
        return localName;
    }
    if (uri.includes('/')) {
        const parts = uri.split('/');
        return parts[parts.length - 1] || parts[parts.length - 2];
    }
    return uri;
}

function inferTypeFromUri(uri) {
    if (!uri) return null;
    
    // Try pattern: http://base#Type/id or http://base/OntologyName/Type/id
    // This is common in R2RML generated URIs
    // The type is the SECOND-TO-LAST part (just before the ID)
    
    let localPart = uri;
    if (uri.includes('#')) {
        localPart = uri.split('#').pop();
    } else if (uri.includes('/')) {
        // Get the path after the domain
        const match = uri.match(/https?:\/\/[^\/]+\/(.+)/);
        if (match) localPart = match[1];
    }
    
    // If localPart contains a slash, find the type (second-to-last meaningful part)
    // Pattern: OntologyName/ClassName/ID -> we want ClassName
    if (localPart && localPart.includes('/')) {
        const parts = localPart.split('/').filter(p => p);
        
        // Work backwards: the last part is likely the ID, the one before it is the type
        // Skip parts that look like IDs (numeric, UUID-like, or codes like PR005)
        for (let i = parts.length - 1; i >= 0; i--) {
            const part = parts[i];
            
            // Check if this looks like an ID
            const looksLikeId = /^[0-9]+$/.test(part) ||  // Pure numeric
                               /^[a-f0-9-]{32,}$/i.test(part) ||  // UUID-like
                               /^[A-Z]{1,3}[0-9]+$/i.test(part) ||  // Code like PR005, D001
                               /^[a-z]+[0-9]+$/i.test(part);  // id123, dept5
            
            if (!looksLikeId) {
                // This is likely the type/class name
                return part;
            }
        }
    }
    
    return null;
}

function extractInstanceId(uri) {
    if (!uri) return '';
    
    let localPart = uri;
    
    // Handle URIs with #
    if (uri.includes('#')) {
        localPart = uri.split('#').pop();
    }
    
    // Get the last segment (typically the ID)
    if (localPart.includes('/')) {
        const parts = localPart.split('/');
        // The last non-empty part is typically the instance ID
        for (let i = parts.length - 1; i >= 0; i--) {
            if (parts[i]) {
                return parts[i];
            }
        }
    }
    
    return localPart || uri.split('/').pop();
}

function getEntityIcon(entity) {
    const type = (entity.type || '').toLowerCase();
    const label = (entity.label || '').toLowerCase();
    const id = (entity.id || '').toLowerCase();
    
    // Direct match with type
    if (type && taxonomyIcons[type]) {
        return taxonomyIcons[type];
    }
    
    // Try to match the type name (without URI prefix)
    if (type) {
        const typeName = type.split('#').pop().split('/').pop();
        if (taxonomyIcons[typeName]) {
            return taxonomyIcons[typeName];
        }
        
        // Try partial match on type name
        for (const [key, emoji] of Object.entries(taxonomyIcons)) {
            const keyName = key.split('#').pop().split('/').pop();
            if (typeName === keyName || typeName.includes(keyName) || keyName.includes(typeName)) {
                return emoji;
            }
        }
    }
    
    // Try to infer from entity ID
    const inferredType = inferTypeFromUri(entity.id);
    if (inferredType) {
        const inferredTypeLower = inferredType.toLowerCase();
        if (taxonomyIcons[inferredTypeLower]) {
            return taxonomyIcons[inferredTypeLower];
        }
        
        // Try partial match on inferred type
        for (const [key, emoji] of Object.entries(taxonomyIcons)) {
            const keyName = key.split('#').pop().split('/').pop();
            if (inferredTypeLower === keyName || inferredTypeLower.includes(keyName) || keyName.includes(inferredTypeLower)) {
                return emoji;
            }
        }
    }
    
    // Fallback: Try matching on any taxonomy icon key
    for (const [key, emoji] of Object.entries(taxonomyIcons)) {
        const keyName = key.split('#').pop().split('/').pop();
        if (type.includes(keyName) || id.includes(keyName) || label.includes(keyName)) {
            return emoji;
        }
    }
    
    // Generic fallbacks based on common patterns
    const searchText = `${type} ${label} ${id}`;
    if (searchText.includes('person') || searchText.includes('employee') || searchText.includes('user')) return '👤';
    if (searchText.includes('department') || searchText.includes('organization') || searchText.includes('org')) return '🏢';
    if (searchText.includes('domain')) return '📋';
    if (searchText.includes('document') || searchText.includes('file')) return '📄';
    if (searchText.includes('location') || searchText.includes('place')) return '📍';
    if (searchText.includes('event') || searchText.includes('meeting')) return '📅';
    if (searchText.includes('product')) return '📦';
    if (searchText.includes('collaboration')) return '🤝';
    
    return '🔷';
}

function getDisplayLabel(node) {
    // Priority 0: Check if we have a 'label' in attributes directly (from query column)
    if (node.attributes) {
        const directLabel = node.attributes['label'] || node.attributes['Label'] || 
                           node.attributes['name'] || node.attributes['Name'];
        if (directLabel && directLabel.trim() !== '') {
            return directLabel;
        }
    }
    
    // Priority 1: Real label from rdfs:label or mapping's label column
    // Note: Check for non-empty string specifically
    if (node.hasRealLabel && node.label !== null && node.label !== undefined && node.label !== '') {
        return node.label;
    }
    
    // Priority 2: Try to find the label column from entity mapping
    if (node.type && node.attributes) {
        const typeLower = node.type.toLowerCase();
        const mapping = entityMappings[typeLower] || findMappingByType(node.type);
        
        if (mapping && mapping.labelColumn) {
            const labelColName = mapping.labelColumn.toLowerCase();
            for (const [key, value] of Object.entries(node.attributes)) {
                // Fix: ensure value exists AND matches key pattern (operator precedence issue)
                const keyLower = key.toLowerCase();
                const keyMatches = keyLower === labelColName || 
                                   keyLower.includes(labelColName) ||
                                   labelColName.includes(keyLower);
                if (value && keyMatches && String(value).trim() !== '') {
                    return value;
                }
            }
        }
    }
    
    // Priority 3: Try to find a good label from attributes
    if (node.attributes && Object.keys(node.attributes).length > 0) {
        // Common label-like attribute names (expanded list, normalized)
        const labelAttrs = ['name', 'label', 'title', 'displayname', 'display_name', 
                           'fullname', 'full_name', 'firstname', 'first_name',
                           'lastname', 'last_name', 'description', 'text'];
        
        // Normalize function for fuzzy matching
        const normalize = s => s.toLowerCase().replace(/[_-]/g, '');
        
        for (const attrName of labelAttrs) {
            const attrNormalized = normalize(attrName);
            for (const [key, value] of Object.entries(node.attributes)) {
                const keyNormalized = normalize(key);
                // Match normalized versions (full_name matches FullName matches fullname)
                if ((keyNormalized === attrNormalized || keyNormalized.endsWith(attrNormalized) || keyNormalized.includes(attrNormalized)) && 
                    value && String(value).trim() !== '') {
                    return value;
                }
            }
        }
        
        // If there's only one non-ID, non-type attribute, use it
        const attrValues = Object.entries(node.attributes).filter(([k, v]) => {
            const kLower = k.toLowerCase();
            return !kLower.includes('id') && !kLower.includes('uri') && 
                   !kLower.includes('type') && v && typeof v === 'string' && String(v).trim() !== '';
        });
        if (attrValues.length === 1) {
            return attrValues[0][1];
        }
        
        // Use first string attribute that's not an ID
        if (attrValues.length > 0) {
            return attrValues[0][1];
        }
    }
    
    // Priority 4: Instance ID (if it's readable, not just a number)
    if (node.instanceId && node.instanceId !== 'Unknown') {
        if (isNaN(parseInt(node.instanceId))) {
            return node.instanceId;
        }
        // If it's a number and we have a type, show "Type #ID"
        if (node.type) {
            return `${node.type} #${node.instanceId}`;
        }
        return `#${node.instanceId}`;
    }
    
    // Priority 5: Type alone
    if (node.type) {
        return node.type;
    }
    
    // Priority 6: Label extracted from URI
    if (node.label && node.label.trim() !== '') {
        return node.label;
    }
    
    return 'Unknown';
}

// Flag to prevent recursive render calls
let isRenderingGraph = false;

function renderD3Graph(hideWhileRendering = false) {
    // Prevent recursive calls
    if (isRenderingGraph) {
        console.log('[Graph] Skipping render - already in progress');
        return;
    }
    isRenderingGraph = true;
    
    const svgElement = document.getElementById('graphSvg');
    const loadingMsg = document.getElementById('graphLoading');
    
    // Only hide SVG during initial load (when loading message is shown)
    if (hideWhileRendering && svgElement) {
        svgElement.style.opacity = '0';
    }
    
    // Ensure container has proper height
    updateQueryGraphHeight();
    
    const container = document.getElementById('graphContainer');
    if (!container) {
        isRenderingGraph = false;
        return;
    }
    const width = container.offsetWidth;
    const height = container.offsetHeight;
    
    const { nodes: filteredNodes, links: filteredLinks } = getFilteredData();
    
    if (filteredNodes.length === 0) {
        if (svgElement) svgElement.style.opacity = '1';
        if (loadingMsg) loadingMsg.style.display = 'none';
        var noGraphMsg = document.getElementById('noGraphMessage');
        if (noGraphMsg) noGraphMsg.style.display = 'block';
        isRenderingGraph = false;
        return;
    }
    
    var noGraphMsgEl = document.getElementById('noGraphMessage');
    if (noGraphMsgEl) noGraphMsgEl.style.display = 'none';
    
    // Clear existing SVG content
    d3.select('#graphSvg').selectAll('*').remove();
    
    const showLabels = document.getElementById('showLabels').checked;
    const showRelLabels = document.getElementById('showRelLabels').checked;
    
    d3Svg = d3.select('#graphSvg')
        .attr('width', width)
        .attr('height', height);
    
    const g = d3Svg.append('g').attr('class', 'graph-group');
    
    d3Zoom = d3.zoom()
        .scaleExtent([0.2, 4])
        .on('zoom', (event) => {
            g.attr('transform', event.transform);
        });
    
    d3Svg.call(d3Zoom);
    
    d3Svg.append('defs').append('marker')
        .attr('id', 'd3-arrow')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 20)
        .attr('refY', 0)
        .attr('markerWidth', 8)
        .attr('markerHeight', 8)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('class', 'd3-arrow');
    
    const simNodes = filteredNodes.map(d => ({...d}));
    const simLinks = filteredLinks.map(d => ({
        source: d.source,
        target: d.target,
        predicate: d.predicate,
        attributes: d.attributes || {}  // Include relationship attributes!
    }));
    
    d3Simulation = d3.forceSimulation(simNodes)
        .force('link', d3.forceLink(simLinks)
            .id(d => d.id)
            .distance(150)
            .strength(0.5))
        .force('charge', d3.forceManyBody()
            .strength(-600)
            .distanceMax(400))
        .force('center', d3.forceCenter(width / 2, height / 2))
        .force('collision', d3.forceCollide().radius(50))
        .force('x', d3.forceX(width / 2).strength(0.05))
        .force('y', d3.forceY(height / 2).strength(0.05))
        .stop();  // Stop automatic animation
    
    // Compute layout synchronously (run simulation ticks without animation)
    const numTicks = Math.ceil(Math.log(d3Simulation.alphaMin()) / Math.log(1 - d3Simulation.alphaDecay()));
    for (let i = 0; i < numTicks; i++) {
        d3Simulation.tick();
    }
    
    const link = g.append('g')
        .attr('class', 'links')
        .selectAll('line')
        .data(simLinks)
        .enter().append('line')
        .attr('class', 'd3-link')
        .attr('marker-end', 'url(#d3-arrow)')
        .style('cursor', 'pointer')
        .on('click', function(event, d) {
            event.stopPropagation();
            showRelationshipDetails(d);
            // Clear node selections
            d3.selectAll('.d3-node').classed('selected', false);
            d3.selectAll('.d3-node-hitarea').attr('stroke', 'none');
            // Highlight the clicked link
            d3.selectAll('.d3-link').classed('selected', false);
            d3.select(this).classed('selected', true);
            // Reset all hitareas to default, then highlight the associated one
            d3.selectAll('.d3-link-hitarea')
                .classed('selected', false)
                .attr('fill', '#e9ecef')
                .attr('stroke', '#999')
                .attr('stroke-width', 1.5);
            d3.selectAll('.d3-link-hitarea').filter(hitarea => hitarea === d)
                .classed('selected', true)
                .attr('fill', '#a5b4fc')
                .attr('stroke', '#6366F1')
                .attr('stroke-width', 2.5);
        });
    
    const linkLabel = g.append('g')
        .attr('class', 'link-labels')
        .selectAll('text')
        .data(simLinks)
        .enter().append('text')
        .attr('class', 'd3-link-label')
        .style('display', showRelLabels ? 'block' : 'none')
        .style('cursor', 'pointer')
        .text(d => d.predicate || '')
        .on('click', function(event, d) {
            event.stopPropagation();
            showRelationshipDetails(d);
            // Clear node selections
            d3.selectAll('.d3-node').classed('selected', false);
            d3.selectAll('.d3-node-hitarea').attr('stroke', 'none');
            // Highlight the associated link
            d3.selectAll('.d3-link').classed('selected', false);
            d3.selectAll('.d3-link').filter(link => link === d).classed('selected', true);
            // Reset all hitareas to default, then highlight the associated one
            d3.selectAll('.d3-link-hitarea')
                .classed('selected', false)
                .attr('fill', '#e9ecef')
                .attr('stroke', '#999')
                .attr('stroke-width', 1.5);
            d3.selectAll('.d3-link-hitarea').filter(hitarea => hitarea === d)
                .classed('selected', true)
                .attr('fill', '#a5b4fc')
                .attr('stroke', '#6366F1')
                .attr('stroke-width', 2.5);
        });
    
    // Add clickable circles at the midpoint of each link for easier clicking
    const linkHitarea = g.append('g')
        .attr('class', 'link-hitareas')
        .selectAll('circle')
        .data(simLinks)
        .enter().append('circle')
        .attr('class', 'd3-link-hitarea')
        .attr('r', 8)
        .attr('fill', '#e9ecef')
        .attr('stroke', '#999')
        .attr('stroke-width', 1.5)
        .style('cursor', 'pointer')
        .on('click', function(event, d) {
            event.stopPropagation();
            showRelationshipDetails(d);
            // Clear node selections
            d3.selectAll('.d3-node').classed('selected', false);
            d3.selectAll('.d3-node-hitarea').attr('stroke', 'none');
            // Highlight the associated link
            d3.selectAll('.d3-link').classed('selected', false);
            d3.selectAll('.d3-link').filter(link => link === d).classed('selected', true);
            // Reset all hitareas to default style, then highlight this one
            d3.selectAll('.d3-link-hitarea')
                .classed('selected', false)
                .attr('fill', '#e9ecef')
                .attr('stroke', '#999')
                .attr('stroke-width', 1.5);
            d3.select(this)
                .classed('selected', true)
                .attr('fill', '#a5b4fc')
                .attr('stroke', '#6366F1')
                .attr('stroke-width', 2.5);
        })
        .on('mouseenter', function(event, d) {
            // Highlight the circle on hover
            d3.select(this)
                .attr('fill', '#c7d2fe')
                .attr('stroke', '#6366F1')
                .attr('stroke-width', 2);
            // Highlight the associated link on hover
            d3.selectAll('.d3-link').filter(link => link === d)
                .attr('stroke', '#6366F1')
                .attr('stroke-opacity', 0.9)
                .attr('stroke-width', 3);
        })
        .on('mouseleave', function(event, d) {
            // Reset circle style if not selected
            if (!d3.select(this).classed('selected')) {
                d3.select(this)
                    .attr('fill', '#e9ecef')
                    .attr('stroke', '#999')
                    .attr('stroke-width', 1.5);
            }
            // Reset link style if not selected
            d3.selectAll('.d3-link').filter(link => link === d)
                .filter(function() { return !d3.select(this).classed('selected'); })
                .attr('stroke', '#999')
                .attr('stroke-opacity', 0.6)
                .attr('stroke-width', 1.5);
        });
    
    const node = g.append('g')
        .attr('class', 'nodes')
        .selectAll('g')
        .data(simNodes)
        .enter().append('g')
        .attr('class', 'd3-node')
        .style('cursor', 'grab')
        .call(d3.drag()
            .on('start', dragStarted)
            .on('drag', dragged)
            .on('end', dragEnded));
    
    node.append('circle')
        .attr('class', 'd3-node-hitarea')
        .attr('r', 25)
        .attr('fill', 'transparent')
        .attr('stroke', 'none');
    
    node.append('text')
        .attr('class', 'd3-node-icon')
        .attr('dy', '0.35em')
        .text(d => getEntityIcon(d));
    
    node.append('text')
        .attr('class', 'd3-node-label')
        .attr('dy', 35)
        .style('display', showLabels ? 'block' : 'none')
        .text(d => getDisplayLabel(d));
    
    node.append('title')
        .text(d => `${d.id}\nType: ${d.type || 'Unknown'}\nLabel: ${d.label || 'N/A'}`);
    
    // Render positions immediately (layout was computed synchronously)
    link
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y);
    
    linkLabel
        .attr('x', d => (d.source.x + d.target.x) / 2)
        .attr('y', d => (d.source.y + d.target.y) / 2 - 8);
    
    // Position the link hitarea circles at the midpoint
    linkHitarea
        .attr('cx', d => (d.source.x + d.target.x) / 2)
        .attr('cy', d => (d.source.y + d.target.y) / 2);
    
    node.attr('transform', d => `translate(${d.x},${d.y})`);
    
    // Auto-fit graph after layout is complete (always fit to view)
    console.log('[Graph] Layout computed, fitting graph to view');
    setTimeout(() => {
        if (d3NodesData.length > 0) {
            fitD3Graph();
        }
    }, 50);  // Small delay to ensure DOM is ready
    
    function dragStarted(event, d) {
        d.fx = d.x;
        d.fy = d.y;
        d.pinned = true;
        d3.select(this).classed('pinned', true).style('cursor', 'grabbing');
    }
    
    function dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
        d.x = event.x;
        d.y = event.y;
        // Update position immediately without animation
        d3.select(this).attr('transform', `translate(${d.x},${d.y})`);
        // Update connected links
        d3.selectAll('.d3-link')
            .attr('x1', l => l.source.x)
            .attr('y1', l => l.source.y)
            .attr('x2', l => l.target.x)
            .attr('y2', l => l.target.y);
        d3.selectAll('.d3-link-label')
            .attr('x', l => (l.source.x + l.target.x) / 2)
            .attr('y', l => (l.source.y + l.target.y) / 2);
        d3.selectAll('.d3-link-hitarea')
            .attr('cx', l => (l.source.x + l.target.x) / 2)
            .attr('cy', l => (l.source.y + l.target.y) / 2);
    }
    
    function dragEnded(event, d) {
        d3.select(this).style('cursor', 'grab');
    }
    
    node.on('dblclick', function(event, d) {
        event.stopPropagation();
        d.fx = null;
        d.fy = null;
        d.pinned = false;
        d3.select(this).classed('pinned', false);
        // No animation - node stays in place when unpinned
    });
    
    node.on('click', function(event, d) {
        event.stopPropagation();
        
        // Always show entity details
        showEntityDetails(d);
        
        // Clear all selections and reset all hitarea strokes
        d3.selectAll('.d3-node').classed('selected', false).classed('pinned', false);
        d3.selectAll('.d3-node-hitarea').attr('stroke', 'none').attr('fill', 'transparent');
        d3.selectAll('.d3-link').classed('selected', false);
        d3.selectAll('.d3-link-hitarea')
            .classed('selected', false)
            .attr('fill', '#e9ecef')
            .attr('stroke', '#999')
            .attr('stroke-width', 1.5);
        
        // Highlight the clicked node
        d3.select(this).classed('selected', true);
        d3.select(this).select('.d3-node-hitarea').attr('stroke', '#0d6efd').attr('stroke-width', 3);
    });
    
    // Right-click context menu (Expand / Collapse)
    node.on('contextmenu', function(event, d) {
        if (typeof showContextMenu === 'function') {
            showContextMenu(event, d);
        }
    });
    
    // Click on background to deselect
    d3Svg.on('click', function(event) {
        if (event.target === this || event.target.tagName === 'svg') {
            d3.selectAll('.d3-node').classed('selected', false).classed('pinned', false);
            d3.selectAll('.d3-node-hitarea').attr('stroke', 'none').attr('fill', 'transparent');
            d3.selectAll('.d3-link').classed('selected', false);
            d3.selectAll('.d3-link-hitarea')
                .classed('selected', false)
                .attr('fill', '#e9ecef')
                .attr('stroke', '#999')
                .attr('stroke-width', 1.5);
            clearEntityDetails();
        }
    });
    
    // Reset render flag
    isRenderingGraph = false;
}

function resetD3Graph() {
    if (d3Simulation && d3NodesData.length > 0) {
        // Reset pinned state
        d3NodesData.forEach(d => {
            d.fx = null;
            d.fy = null;
            d.pinned = false;
        });
        d3.selectAll('.d3-node').classed('pinned', false);
        
        // Recompute layout synchronously
        d3Simulation.alpha(1);
        const numTicks = Math.ceil(Math.log(d3Simulation.alphaMin()) / Math.log(1 - d3Simulation.alphaDecay()));
        for (let i = 0; i < numTicks; i++) {
            d3Simulation.tick();
        }
        
        // Update all positions immediately
        d3.selectAll('.d3-node').attr('transform', d => `translate(${d.x},${d.y})`);
        d3.selectAll('.d3-link')
            .attr('x1', d => d.source.x)
            .attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x)
            .attr('y2', d => d.target.y);
        d3.selectAll('.d3-link-label')
            .attr('x', d => (d.source.x + d.target.x) / 2)
            .attr('y', d => (d.source.y + d.target.y) / 2);
        d3.selectAll('.d3-link-hitarea')
            .attr('cx', d => (d.source.x + d.target.x) / 2)
            .attr('cy', d => (d.source.y + d.target.y) / 2);
    }
    if (d3Svg && d3Zoom) {
        d3Svg.transition().duration(500).call(d3Zoom.transform, d3.zoomIdentity);
    }
}

async function refreshVisualization() {
    // Show loading state
    const loadingMsg = document.getElementById('graphLoading');
    const noGraphMsg = document.getElementById('noGraphMessage');
    const svgElement = document.getElementById('graphSvg');
    
    if (noGraphMsg) noGraphMsg.style.display = 'none';
    if (loadingMsg) loadingMsg.style.display = 'block';
    if (svgElement) svgElement.style.opacity = '0';
    
    // Clear existing graph data
    d3NodesData = [];
    d3LinksData = [];
    d3.select('#graphSvg').selectAll('*').remove();
    
    // Rebuild graph from cached results
    if (lastQueryResults && lastQueryResults.results && lastQueryResults.results.length > 0) {
        if (typeof buildGraph === 'function') {
            await buildGraph(lastQueryResults.results, lastQueryResults.columns);
        }
    }
    
    // If no data, show no graph message
    if (d3NodesData.length === 0) {
        if (loadingMsg) loadingMsg.style.display = 'none';
        if (svgElement) svgElement.style.opacity = '1';
        if (noGraphMsg) noGraphMsg.style.display = 'block';
    }
}

function unpinAllNodes() {
    if (d3Simulation) {
        d3NodesData.forEach(d => {
            d.fx = null;
            d.fy = null;
            d.pinned = false;
        });
        d3.selectAll('.d3-node').classed('pinned', false);
        // No animation - nodes stay in place when unpinned
    }
}

function zoomD3Graph(factor) {
    if (d3Svg && d3Zoom) {
        d3Svg.transition().duration(300).call(d3Zoom.scaleBy, factor);
    }
}

// Flag to prevent recursive fitD3Graph calls
let isFittingGraph = false;

function fitD3Graph() {
    // Prevent recursive calls
    if (isFittingGraph) return;
    if (!d3Svg || !d3Zoom || d3NodesData.length === 0) return;
    
    isFittingGraph = true;
    
    const container = document.getElementById('graphContainer');
    const svgElement = document.getElementById('graphSvg');
    const loadingMsg = document.getElementById('graphLoading');
    
    if (!container) {
        isFittingGraph = false;
        return;
    }
    
    const width = container.offsetWidth;
    const height = container.offsetHeight;
    
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    d3NodesData.forEach(d => {
        if (d.x !== undefined && d.y !== undefined) {
            minX = Math.min(minX, d.x);
            maxX = Math.max(maxX, d.x);
            minY = Math.min(minY, d.y);
            maxY = Math.max(maxY, d.y);
        }
    });
    
    if (minX === Infinity) {
        // No valid positions yet, show SVG anyway and hide loading
        if (svgElement) svgElement.style.opacity = '1';
        if (loadingMsg) loadingMsg.style.display = 'none';
        isFittingGraph = false;
        return;
    }
    
    const padding = 80;
    const graphWidth = maxX - minX + padding * 2;
    const graphHeight = maxY - minY + padding * 2;
    
    const scale = Math.min(
        width / graphWidth,
        height / graphHeight,
        2
    ) * 0.9;
    
    const centerX = (minX + maxX) / 2;
    const centerY = (minY + maxY) / 2;
    const translateX = width / 2 - centerX * scale;
    const translateY = height / 2 - centerY * scale;
    
    // Apply transform immediately (no animation for initial display)
    d3Svg.call(
        d3Zoom.transform,
        d3.zoomIdentity.translate(translateX, translateY).scale(scale)
    );
    
    // Hide loading and show the graph
    if (loadingMsg) loadingMsg.style.display = 'none';
    if (svgElement) svgElement.style.opacity = '1';
    
    // Reset flag after a short delay
    setTimeout(() => { isFittingGraph = false; }, 100);
}

function applyGraphFilters() {
    renderD3Graph();
}

// Visual Filter State
let visualFilterEntityTypes = new Set();
let visualFilterRelationships = new Set();

function toggleVisualFiltersPanel() {
    const panel = document.getElementById('visualFiltersPanel');
    const btn = document.getElementById('toggleVisualFiltersBtn');
    if (!panel) return;

    const isVisible = panel.style.display !== 'none';
    panel.style.display = isVisible ? 'none' : 'block';

    if (btn) {
        btn.classList.toggle('active', !isVisible);
    }

    if (!isVisible) {
        populateVisualFilters();
    }
}

function toggleFilterPane() {
    const pane = document.getElementById('filterPane');
    const handle = document.getElementById('filterResizeHandle');
    const btn = document.getElementById('toggleFilterPaneBtn');
    if (!pane) return;

    const isVisible = pane.style.display !== 'none';
    pane.style.display = isVisible ? 'none' : 'flex';
    if (handle) handle.style.display = isVisible ? 'none' : '';
    if (btn) btn.classList.toggle('active', !isVisible);

    if (!isVisible) {
        initGraphSearch();
    }
}

function populateVisualFilters() {
    // Populate entity type filters
    const entityTypeContainer = document.getElementById('entityTypeFilters');
    const relationshipContainer = document.getElementById('relationshipFilters');
    
    if (!entityTypeContainer || !relationshipContainer) return;
    
    // Collect unique entity types with counts
    const entityTypeCounts = {};
    d3NodesData.forEach(node => {
        const type = node.type || 'Unknown';
        entityTypeCounts[type] = (entityTypeCounts[type] || 0) + 1;
    });
    
    // Collect unique relationship types with counts
    const relTypeCounts = {};
    d3LinksData.forEach(link => {
        const predicate = link.predicate || 'Unknown';
        relTypeCounts[predicate] = (relTypeCounts[predicate] || 0) + 1;
    });
    
    // Initialize filters if empty (select all by default)
    if (visualFilterEntityTypes.size === 0) {
        Object.keys(entityTypeCounts).forEach(type => visualFilterEntityTypes.add(type));
    }
    if (visualFilterRelationships.size === 0) {
        Object.keys(relTypeCounts).forEach(rel => visualFilterRelationships.add(rel));
    }
    
    // Render entity type chips
    if (Object.keys(entityTypeCounts).length === 0) {
        entityTypeContainer.innerHTML = '<span class="text-muted small">No entities loaded</span>';
    } else {
        entityTypeContainer.innerHTML = Object.entries(entityTypeCounts)
            .sort((a, b) => b[1] - a[1])
            .map(([type, count]) => {
                const isActive = visualFilterEntityTypes.has(type);
                const icon = getEntityIconByType(type);
                const displayName = extractLocalName(type);
                return `
                    <div class="filter-chip ${isActive ? 'active' : ''}" 
                         onclick="toggleVisualEntityFilter('${escapeHtml(type)}')"
                         title="${type}">
                        <span class="chip-icon">${icon}</span>
                        <span>${escapeHtml(displayName)}</span>
                        <span class="chip-count">${count}</span>
                    </div>
                `;
            }).join('');
    }
    
    // Render relationship chips
    if (Object.keys(relTypeCounts).length === 0) {
        relationshipContainer.innerHTML = '<span class="text-muted small">No relationships loaded</span>';
    } else {
        relationshipContainer.innerHTML = Object.entries(relTypeCounts)
            .sort((a, b) => b[1] - a[1])
            .map(([rel, count]) => {
                const isActive = visualFilterRelationships.has(rel);
                const displayName = extractLocalName(rel);
                return `
                    <div class="filter-chip ${isActive ? 'active' : ''}" 
                         onclick="toggleVisualRelationshipFilter('${escapeHtml(rel)}')"
                         title="${rel}">
                        <span class="chip-icon">🔗</span>
                        <span>${escapeHtml(displayName)}</span>
                        <span class="chip-count">${count}</span>
                    </div>
                `;
            }).join('');
    }
}

// Build a map of relationship -> {sourceTypes, targetTypes} from the actual data
function getRelationshipEntityTypes() {
    const relEntityTypes = {};
    
    d3LinksData.forEach(link => {
        const predicate = link.predicate;
        if (!predicate) return;
        
        // Get source and target node types
        const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
        const targetId = typeof link.target === 'object' ? link.target.id : link.target;
        
        const sourceNode = d3NodesData.find(n => n.id === sourceId);
        const targetNode = d3NodesData.find(n => n.id === targetId);
        
        const sourceType = sourceNode?.type;
        const targetType = targetNode?.type;
        
        if (!relEntityTypes[predicate]) {
            relEntityTypes[predicate] = { sourceTypes: new Set(), targetTypes: new Set() };
        }
        
        if (sourceType) relEntityTypes[predicate].sourceTypes.add(sourceType);
        if (targetType) relEntityTypes[predicate].targetTypes.add(targetType);
    });
    
    return relEntityTypes;
}

function toggleVisualEntityFilter(type) {
    const wasSelected = visualFilterEntityTypes.has(type);
    
    if (wasSelected) {
        // Unselecting entity type
        visualFilterEntityTypes.delete(type);
        
        // Remove relationships that involve this entity type
        const relEntityTypes = getRelationshipEntityTypes();
        for (const [predicate, types] of Object.entries(relEntityTypes)) {
            if (types.sourceTypes.has(type) || types.targetTypes.has(type)) {
                visualFilterRelationships.delete(predicate);
            }
        }
    } else {
        // Selecting entity type
        visualFilterEntityTypes.add(type);
        
        // Add relationships where both source and target types are now selected
        const relEntityTypes = getRelationshipEntityTypes();
        for (const [predicate, types] of Object.entries(relEntityTypes)) {
            // Check if all source types and all target types are selected
            const allSourcesSelected = [...types.sourceTypes].every(t => visualFilterEntityTypes.has(t));
            const allTargetsSelected = [...types.targetTypes].every(t => visualFilterEntityTypes.has(t));
            
            if (allSourcesSelected && allTargetsSelected) {
                visualFilterRelationships.add(predicate);
            }
        }
    }
    
    populateVisualFilters();
    applyGraphFilters();
}

function toggleVisualRelationshipFilter(rel) {
    if (visualFilterRelationships.has(rel)) {
        visualFilterRelationships.delete(rel);
    } else {
        visualFilterRelationships.add(rel);
    }
    populateVisualFilters();
    applyGraphFilters();
}

function selectAllVisualFilters() {
    // Select all entity types
    d3NodesData.forEach(node => {
        if (node.type) visualFilterEntityTypes.add(node.type);
    });
    // Select all relationships
    d3LinksData.forEach(link => {
        if (link.predicate) visualFilterRelationships.add(link.predicate);
    });
    populateVisualFilters();
    applyGraphFilters();
}

function clearAllVisualFilters() {
    visualFilterEntityTypes.clear();
    visualFilterRelationships.clear();
    populateVisualFilters();
    applyGraphFilters();
}

function getEntityIconByType(type) {
    // Try to find a matching icon from taxonomyIcons
    if (!type) return '📦';
    
    const typeLower = type.toLowerCase();
    
    // Direct match
    if (taxonomyIcons[typeLower]) {
        return taxonomyIcons[typeLower];
    }
    
    // Try local name
    const localName = extractLocalName(type).toLowerCase();
    if (taxonomyIcons[localName]) {
        return taxonomyIcons[localName];
    }
    
    // Try partial match
    for (const [key, emoji] of Object.entries(taxonomyIcons)) {
        const keyName = extractLocalName(key).toLowerCase();
        if (localName === keyName || localName.includes(keyName) || keyName.includes(localName)) {
            return emoji;
        }
    }
    
    return '📦';
}

function getFilteredData() {
    const hideOrphans = document.getElementById('hideOrphans').checked;
    
    // Apply visual filters for entity types
    let filteredNodes = d3NodesData;
    if (visualFilterEntityTypes.size > 0) {
        filteredNodes = d3NodesData.filter(node => visualFilterEntityTypes.has(node.type));
    }
    
    // Apply visual filters for relationships
    let filteredLinks = d3LinksData;
    if (visualFilterRelationships.size > 0) {
        filteredLinks = d3LinksData.filter(link => visualFilterRelationships.has(link.predicate));
    }
    
    // Filter links to only include those where both source and target are in filtered nodes
    const filteredNodeIds = new Set(filteredNodes.map(n => n.id));
    filteredLinks = filteredLinks.filter(link => {
        const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
        const targetId = typeof link.target === 'object' ? link.target.id : link.target;
        return filteredNodeIds.has(sourceId) && filteredNodeIds.has(targetId);
    });
    
    const connectedNodes = new Set();
    filteredLinks.forEach(link => {
        connectedNodes.add(typeof link.source === 'object' ? link.source.id : link.source);
        connectedNodes.add(typeof link.target === 'object' ? link.target.id : link.target);
    });
    
    if (hideOrphans) {
        filteredNodes = filteredNodes.filter(node => connectedNodes.has(node.id));
    }
    
    const orphanCount = d3NodesData.length - connectedNodes.size;
    document.getElementById('nodeCount').textContent = `${filteredNodes.length} entities`;
    document.getElementById('linkCount').textContent = `${filteredLinks.length} relationships`;
    document.getElementById('orphanCount').textContent = orphanCount > 0 ? `(${orphanCount} orphans${hideOrphans ? ' hidden' : ''})` : '';
    
    return { nodes: filteredNodes, links: filteredLinks };
}



// Height is now handled by CSS flexbox - this just triggers a re-render
function updateQueryGraphHeight() {
    // CSS handles the height via flexbox, no manual calculation needed
}

// Check entity limit when user changes the dropdown
function checkEntityLimit() {
    if (!d3NodesData || d3NodesData.length === 0) return;
    
    const entityLimitSelect = document.getElementById('entityLimit');
    const entityLimit = entityLimitSelect ? parseInt(entityLimitSelect.value) : 100;
    const tooManyMsg = document.getElementById('tooManyEntitiesMessage');
    const svgElement = document.getElementById('graphSvg');
    const graphStats = document.getElementById('graphStats');
    
    console.log(`[Graph] Checking entity limit: ${d3NodesData.length} entities, limit: ${entityLimit}`);
    
    if (entityLimit > 0 && d3NodesData.length > entityLimit) {
        // Still exceeds limit - show message
        entityLimitExceeded = true;
        
        const entityCountDisplay = document.getElementById('entityCountDisplay');
        const entityLimitDisplay = document.getElementById('entityLimitDisplay');
        if (entityCountDisplay) entityCountDisplay.textContent = d3NodesData.length;
        if (entityLimitDisplay) entityLimitDisplay.textContent = entityLimit;
        
        if (tooManyMsg) tooManyMsg.style.display = 'block';
        if (svgElement) svgElement.style.opacity = '0';
        if (graphStats) graphStats.style.display = 'none';
        
        d3.select('#graphSvg').selectAll('*').remove();
    } else {
        // Under limit - render the graph
        entityLimitExceeded = false;
        if (tooManyMsg) tooManyMsg.style.display = 'none';
        
        // Render the graph
        renderD3Graph(true);
    }
}

// Rerender on window resize (debounced with longer delay)
let resizeTimeout = null;
let lastResizeWidth = 0;
let lastResizeHeight = 0;
window.addEventListener('resize', () => {
    if (resizeTimeout) clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
        const container = document.getElementById('graphContainer');
        if (!container) return;
        
        // Only re-render if size changed significantly (more than 50px)
        const newWidth = container.offsetWidth;
        const newHeight = container.offsetHeight;
        const widthDiff = Math.abs(newWidth - lastResizeWidth);
        const heightDiff = Math.abs(newHeight - lastResizeHeight);
        
        if (widthDiff > 50 || heightDiff > 50) {
            lastResizeWidth = newWidth;
            lastResizeHeight = newHeight;
            updateQueryGraphHeight();
            if (d3NodesData.length > 0 && !isRenderingGraph) {
                renderD3Graph();
            }
        }
    }, 300);
});
