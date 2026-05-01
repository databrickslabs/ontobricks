/**
 * OntoBricks - Mapping Design JavaScript
 * D3.js Force-Directed Graph for Visual Mapping Designer
 * Extracted from _mapping_design.html per code_instructions.txt
 */


// MAPPING DESIGN - D3.js Force-Directed Graph
// =====================================================

let mappingMapInitialized = false;
let mappingMapSvg = null;
let mappingMapSimulation = null;
let mappingMapZoom = null;
let mappingMapNodes = [];

/**
 * Show/hide loading overlay for mapping designer
 */
function showMappingDesignerLoading(show) {
    const loadingEl = document.getElementById('mappingDesignerLoading');
    if (loadingEl) {
        loadingEl.style.display = show ? 'flex' : 'none';
    }
}

/**
 * Load saved map layout from the Ontology Designer
 */
async function loadMapLayout() {
    try {
        const response = await fetch('/domain/map-layout', { credentials: 'same-origin' });
        const data = await response.json();
        if (data.success && data.layout) {
            return data.layout;
        }
    } catch (error) {
        console.log('[MappingMap] Could not load map layout:', error);
    }
    return null;
}

/**
 * Initialize the mapping designer (D3.js Designer)
 */
async function initMappingDesigner() {
    showMappingDesignerLoading(true);
    
    // Wait for MappingState initialization (ontology + mappings fetched)
    if (!MappingState.initialized) {
        let attempts = 0;
        while (!MappingState.initialized && attempts < 50) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
    }
    
    const container = document.getElementById('mapping-map-container');
    if (!container) {
        console.error('Mapping Designer: Container not found');
        showMappingDesignerLoading(false);
        return;
    }
    
    try {
        // Clear previous map content (keep the loading overlay and its spinner SVG)
        const existingSvg = container.querySelector(':scope > svg');
        if (existingSvg) existingSvg.remove();
        const existingLegend = container.querySelector('.mapping-map-legend');
        if (existingLegend) existingLegend.remove();
        const existingEmpty = container.querySelector('.map-empty-message');
        if (existingEmpty) existingEmpty.remove();
        
        // Get ontology data from MappingState; if missing, try fetching it once
        if (!MappingState.loadedOntology) {
            try {
                const resp = await fetch('/ontology/get-loaded-ontology', { credentials: 'same-origin' });
                const data = await resp.json();
                if (data.success && data.ontology) {
                    MappingState.loadedOntology = data.ontology;
                    console.log('Mapping Designer: Fetched ontology on-demand —', data.ontology.classes?.length, 'classes');
                }
            } catch (e) {
                console.warn('Mapping Designer: On-demand ontology fetch failed:', e);
            }
        }
        if (!MappingState.loadedOntology) {
            container.innerHTML += `
                <div class="map-empty-message text-center text-muted py-5">
                    <i class="bi bi-diagram-3 fs-1 d-block mb-3 opacity-50"></i>
                    <p class="mb-0">No ontology loaded.</p>
                    <p class="small">Load an ontology first to see the mapping design.</p>
                </div>
            `;
            showMappingDesignerLoading(false);
            mappingMapInitialized = true;
            return;
        }
        
        const classes = MappingState.loadedOntology.classes || [];
        const allProperties = MappingState.loadedOntology.properties || [];
        
        // Filter to only show ObjectProperties (relationships), not DatatypeProperties (attributes)
        const properties = allProperties.filter(prop => {
            if (prop.type) {
                return prop.type === 'ObjectProperty' || prop.type === 'owl:ObjectProperty';
            }
            if (prop.range) {
                const range = prop.range.toLowerCase();
                if (range.startsWith('xsd:') || range.includes('string') || range.includes('integer') || 
                    range.includes('decimal') || range.includes('date') || range.includes('boolean') ||
                    range.includes('float') || range.includes('double') || range.includes('time')) {
                    return false;
                }
            }
            return true;
        });
        
        if (classes.length === 0) {
            container.innerHTML += `
                <div class="map-empty-message text-center text-muted py-5">
                    <i class="bi bi-diagram-3 fs-1 d-block mb-3 opacity-50"></i>
                    <p class="mb-0">No entities defined in the ontology.</p>
                </div>
            `;
            showMappingDesignerLoading(false);
            mappingMapInitialized = true;
            return;
        }
        
        // Load saved layout from Ontology Designer
        const savedLayout = await loadMapLayout();
        
        // Get mapping status (only count entries that have a SQL query as truly assigned)
        const mappedClassUris = new Set(
            MappingState.config.entities.filter(m => m.sql_query).map(m => m.ontology_class || m.class_uri)
        );
        const mappedPropertyUris = new Set(
            MappingState.config.relationships.filter(m => m.sql_query).map(m => m.property)
        );
        
        // Build lookup: class URI -> entity mapping (for attribute checks)
        const mappingByClassUri = {};
        MappingState.config.entities.forEach(m => {
            const uri = m.ontology_class || m.class_uri;
            if (uri) mappingByClassUri[uri] = m;
        });
        
        // Build nodes from classes with saved positions
        const nodes = classes.map((cls, idx) => {
            const savedPos = savedLayout?.positions?.[cls.name || cls.localName];
            const x = savedPos?.x ?? (100 + (idx % 5) * 150);
            const y = savedPos?.y ?? (100 + Math.floor(idx / 5) * 120);
            
            const isExcluded = !!cls.excluded;
            const isMapped = mappedClassUris.has(cls.uri);
            
            // Check if attributes are fully assigned
            let mappingStatus = isExcluded ? 'excluded' : (isMapped ? 'mapped' : 'unmapped');
            if (!isExcluded && isMapped) {
                const dataProps = cls.dataProperties || [];
                if (dataProps.length > 0) {
                    const em = mappingByClassUri[cls.uri] || {};
                    const attrMap = em.attribute_mappings || {};
                    const hasAllAttrs = dataProps.every(dp => {
                        const name = dp.name || dp.localName || '';
                        return name && attrMap[name];
                    });
                    if (!hasAllAttrs) {
                        mappingStatus = 'partial';
                    }
                }
            }
            
            return {
                id: cls.name || cls.localName,
                uri: cls.uri,
                name: cls.name || cls.localName,
                label: cls.label || cls.name || cls.localName,
                icon: cls.emoji || '📦',
                parent: cls.parent,
                mapped: isMapped,
                excluded: isExcluded,
                mappingStatus: mappingStatus,
                x: x,
                y: y,
                // Fix positions - no animation
                fx: x,
                fy: y
            };
        });
        
        mappingMapNodes = nodes;
        
        // Build a set of excluded entity names for relationship exclusion
        const excludedEntityNames = new Set(
            nodes.filter(n => n.excluded).map(n => n.id)
        );
        
        // Build links from properties (relationships) and inheritance
        const links = [];
        const validNodeIds = new Set(nodes.map(n => n.id));
        
        // Add relationship links (only if both domain and range exist as nodes)
        properties.forEach(prop => {
            if (prop.domain && prop.range) {
                if (!validNodeIds.has(prop.domain) || !validNodeIds.has(prop.range)) {
                    console.log(`[MappingMap] Skipping property "${prop.name}": domain="${prop.domain}" or range="${prop.range}" not found in nodes`);
                    return;
                }
                const isExcluded = !!prop.excluded
                    || excludedEntityNames.has(prop.domain)
                    || excludedEntityNames.has(prop.range);
                links.push({
                    source: prop.domain,
                    target: prop.range,
                    name: prop.name || prop.localName,
                    uri: prop.uri,
                    type: 'relationship',
                    direction: prop.direction || 'forward',
                    mapped: mappedPropertyUris.has(prop.uri),
                    excluded: isExcluded
                });
            }
        });
        
        // Add inheritance links (only if parent exists as a node)
        classes.forEach(cls => {
            if (cls.parent) {
                const childId = cls.name || cls.localName;
                if (!validNodeIds.has(cls.parent)) {
                    console.log(`[MappingMap] Skipping inheritance for "${childId}": parent "${cls.parent}" not found in nodes`);
                    return;
                }
                const isExcluded = !!cls.excluded || excludedEntityNames.has(cls.parent);
                links.push({
                    source: cls.parent,
                    target: childId,
                    name: 'inherits',
                    type: 'inheritance',
                    mapped: true,
                    excluded: isExcluded
                });
            }
        });
        
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
        
        mappingMapSvg = svg;
        
        // Add zoom behavior
        mappingMapZoom = d3.zoom()
            .scaleExtent([0.3, 3])
            .on('zoom', (event) => {
                g.attr('transform', event.transform);
            });
        
        svg.call(mappingMapZoom);
        
        // Main group for transforms
        const g = svg.append('g');
        
        // Define arrow markers
        const defs = svg.append('defs');
        
        // Arrow for mapped relationships (green)
        defs.append('marker')
            .attr('id', 'mapping-arrow-mapped')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 28)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#198754');
        
        // Arrow for unmapped relationships (red)
        defs.append('marker')
            .attr('id', 'mapping-arrow-unmapped')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 28)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#dc3545');
        
        // Arrow for self-loops mapped
        defs.append('marker')
            .attr('id', 'mapping-arrow-self-mapped')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 10)
            .attr('refY', 0)
            .attr('markerWidth', 5)
            .attr('markerHeight', 5)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#198754');
        
        // Arrow for self-loops unmapped
        defs.append('marker')
            .attr('id', 'mapping-arrow-self-unmapped')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 10)
            .attr('refY', 0)
            .attr('markerWidth', 5)
            .attr('markerHeight', 5)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#dc3545');
        
        // Arrow for excluded relationships (grey)
        defs.append('marker')
            .attr('id', 'mapping-arrow-excluded')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 28)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#ced4da');

        // Arrow for inheritance (hollow)
        defs.append('marker')
            .attr('id', 'mapping-arrow-inheritance')
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
        
        // Create force simulation (static - no animation)
        mappingMapSimulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(links).id(d => d.id).distance(180))
            .alphaDecay(1) // Instant decay - no animation
            .velocityDecay(1); // No velocity
        
        // Only add layout forces if no saved layout
        if (!savedLayout) {
            mappingMapSimulation
                .force('charge', d3.forceManyBody().strength(-400))
                .force('collision', d3.forceCollide().radius(60))
                .force('center', d3.forceCenter(width / 2, height / 2));
        }
        
        // Drag handlers - static mode, just update position
        function dragStarted(event, d) {
            d.fx = d.x;
            d.fy = d.y;
        }
        
        function dragged(event, d) {
            d.fx = event.x;
            d.fy = event.y;
            // Immediately update the tick
            mappingMapSimulation.alpha(0.01).restart();
        }
        
        function dragEnded(event, d) {
            // Keep fixed position
        }
        
        // Separate self-loops from regular links
        const selfLoopLinks = links.filter(l => {
            const sourceId = typeof l.source === 'object' ? l.source.id : l.source;
            const targetId = typeof l.target === 'object' ? l.target.id : l.target;
            return sourceId === targetId;
        });
        const regularLinks = links.filter(l => !selfLoopLinks.includes(l));
        
        // Count and index self-loops per node
        const selfLoopCountMap = new Map();
        selfLoopLinks.forEach(link => {
            const nodeId = typeof link.source === 'object' ? link.source.id : link.source;
            const count = selfLoopCountMap.get(nodeId) || 0;
            link.selfLoopIndex = count;
            selfLoopCountMap.set(nodeId, count + 1);
        });
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
            .attr('class', d => `mapping-map-link ${d.excluded ? 'excluded' : (d.mapped ? 'mapped' : 'unmapped')}`);
        
        // Draw inheritance links
        const inheritanceLinkElements = g.append('g')
            .selectAll('path')
            .data(regularLinks.filter(l => l.type === 'inheritance'))
            .enter()
            .append('path')
            .attr('class', d => `mapping-map-link inheritance${d.excluded ? ' excluded' : ''}`);
        
        // Draw self-loop links
        const selfLoopElements = g.append('g')
            .selectAll('path')
            .data(selfLoopLinks)
            .enter()
            .append('path')
            .attr('class', d => {
                if (d.excluded) return 'mapping-map-link self-loop excluded';
                return `mapping-map-link self-loop ${d.mapped ? 'mapped' : 'unmapped'}`;
            })
            .attr('marker-end', d => {
                if (d.excluded) return 'url(#mapping-arrow-excluded)';
                return d.mapped ? 'url(#mapping-arrow-self-mapped)' : 'url(#mapping-arrow-self-unmapped)';
            });
        
        // Draw clickable hitareas at link midpoints (for relationships only)
        const linkHitareas = g.append('g')
            .selectAll('circle')
            .data(links.filter(l => l.type === 'relationship'))
            .enter()
            .append('circle')
            .attr('class', d => `mapping-map-link-hitarea ${d.excluded ? 'excluded' : (d.mapped ? 'mapped' : 'unmapped')}`)
            .attr('r', 8)
            .attr('stroke-width', 1.5)
            .on('click', function(event, d) {
                event.stopPropagation();
                
                // Highlight clicked hitarea
                d3.selectAll('.mapping-map-link-hitarea').classed('selected', false);
                d3.select(this).classed('selected', true);
                
                // Clear entity selection
                d3.selectAll('.mapping-map-node').classed('selected', false);
                
                // Open relationship mapping panel
                openRelationshipMappingFromDesign(d);
            })
            .on('contextmenu', function(event, d) {
                event.preventDefault();
                event.stopPropagation();

                if (window.isActiveVersion === false) return;
                
                // Highlight clicked hitarea
                d3.selectAll('.mapping-map-link-hitarea').classed('selected', false);
                d3.select(this).classed('selected', true);
                
                // Clear entity selection
                d3.selectAll('.mapping-map-node').classed('selected', false);
                
                // Show context menu for relationship
                showMappingMapContextMenu(event, d, 'relationship', container);
            });
        
        // Draw relationship labels
        const linkLabels = g.append('g')
            .selectAll('text')
            .data(links.filter(l => l.type === 'relationship'))
            .enter()
            .append('text')
            .attr('class', d => `mapping-map-link-label ${d.excluded ? 'excluded' : (d.mapped ? 'mapped' : 'unmapped')}`)
            .text(d => d.name);
        
        // Track if we're dragging
        let isDragging = false;
        
        // Draw nodes
        const nodeElements = g.append('g')
            .selectAll('g')
            .data(nodes)
            .enter()
            .append('g')
            .attr('class', d => `mapping-map-node ${d.excluded ? 'excluded' : (d.mappingStatus || (d.mapped ? 'mapped' : 'unmapped'))}`)
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
        
        // Invisible hitarea circle
        nodeElements.append('circle')
            .attr('class', 'mapping-map-node-hitarea')
            .attr('r', 25);
        
        // Emoji icon (centered via CSS dominant-baseline: central)
        nodeElements.append('text')
            .attr('class', 'mapping-map-node-icon')
            .text(d => d.icon);
        
        // Node label
        nodeElements.append('text')
            .attr('class', 'mapping-map-node-label')
            .attr('dy', 35)
            .text(d => d.name);
        
        // Tooltip
        const statusLabels = { mapped: 'Mapped', unmapped: 'Not Mapped', partial: 'Attributes Missing', excluded: 'Excluded' };
        nodeElements.append('title')
            .text(d => `${d.name} (${statusLabels[d.mappingStatus] || 'Not Mapped'})`);
        
        // Click to open mapping panel
        nodeElements.on('click', function(event, d) {
            if (isDragging) {
                isDragging = false;
                return;
            }
            
            event.stopPropagation();
            
            // Clear relationship selection
            d3.selectAll('.mapping-map-link-hitarea').classed('selected', false);
            
            // Highlight selected node
            d3.selectAll('.mapping-map-node').classed('selected', false);
            d3.select(this).classed('selected', true);
            
            // Open entity mapping panel
            openEntityMappingFromDesign(d);
        });
        
        // Right-click context menu for entities (suppressed in view mode)
        nodeElements.on('contextmenu', function(event, d) {
            event.preventDefault();
            event.stopPropagation();

            if (window.isActiveVersion === false) return;
            
            // Highlight selected node
            d3.selectAll('.mapping-map-node').classed('selected', false);
            d3.select(this).classed('selected', true);
            
            // Show context menu
            showMappingMapContextMenu(event, d, 'entity', container);
        });
        
        // Hide context menu and close panel when clicking on SVG background
        svg.on('click', function() {
            hideMappingMapContextMenu();
            d3.selectAll('.mapping-map-node').classed('selected', false);
            d3.selectAll('.mapping-map-link-hitarea').classed('selected', false);
            closeMappingPanel();
        });
        
        svg.on('contextmenu', function(event) {
            // Only show canvas context menu if clicking on background (not on a node or link)
            if (event.target.tagName === 'svg' || 
                (event.target.closest('g.mapping-map-node') === null && 
                 !event.target.classList.contains('mapping-map-link-hitarea'))) {
                event.preventDefault();
                hideMappingMapContextMenu();
                if (window.isActiveVersion !== false) {
                    showMappingMapCanvasContextMenu(event, container);
                }
            }
        });
        
        // Update positions on simulation tick
        mappingMapSimulation.on('tick', () => {
            // Regular relationship links
            relationshipLinkElements.attr('d', d => {
                const sx = d.source.x, sy = d.source.y;
                const tx = d.target.x, ty = d.target.y;
                
                if (d.linkCount > 1) {
                    const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                    const mx = (sx + tx) / 2;
                    const my = (sy + ty) / 2;
                    const dx = tx - sx;
                    const dy = ty - sy;
                    const len = Math.sqrt(dx * dx + dy * dy) || 1;
                    const px = -dy / len;
                    const py = dx / len;
                    const cx = mx + px * offset;
                    const cy = my + py * offset;
                    return `M${sx},${sy} Q${cx},${cy} ${tx},${ty}`;
                }
                
                return `M${sx},${sy} L${tx},${ty}`;
            });
            
            // Inheritance links
            inheritanceLinkElements.attr('d', d => {
                const sx = d.source.x, sy = d.source.y;
                const tx = d.target.x, ty = d.target.y;
                return `M${sx},${sy} L${tx},${ty}`;
            });
            
            // Self-loop links
            selfLoopElements.attr('d', d => {
                const node = typeof d.source === 'object' ? d.source : nodes.find(n => n.id === d.source);
                if (!node) return '';
                
                const x = node.x;
                const y = node.y;
                const r = 35;
                const loopSize = 40;
                
                const baseAngle = -45;
                const angleStep = 90;
                const angle = (baseAngle + d.selfLoopIndex * angleStep) * Math.PI / 180;
                
                const nodeRadius = 25;
                const startAngle = angle - 0.3;
                const endAngle = angle + 0.3;
                
                const startX = x + Math.cos(startAngle) * nodeRadius;
                const startY = y + Math.sin(startAngle) * nodeRadius;
                const endX = x + Math.cos(endAngle) * nodeRadius;
                const endY = y + Math.sin(endAngle) * nodeRadius;
                
                const ctrlDist = loopSize + r;
                const ctrl1X = x + Math.cos(startAngle) * ctrlDist;
                const ctrl1Y = y + Math.sin(startAngle) * ctrlDist;
                const ctrl2X = x + Math.cos(endAngle) * ctrlDist;
                const ctrl2Y = y + Math.sin(endAngle) * ctrlDist;
                
                return `M${startX},${startY} C${ctrl1X},${ctrl1Y} ${ctrl2X},${ctrl2Y} ${endX},${endY}`;
            });
            
            // Link hitareas
            linkHitareas.attr('cx', d => {
                const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
                const targetId = typeof d.target === 'object' ? d.target.id : d.target;
                const sx = typeof d.source === 'object' ? d.source.x : nodes.find(n => n.id === d.source)?.x || 0;
                const tx = typeof d.target === 'object' ? d.target.x : nodes.find(n => n.id === d.target)?.x || 0;
                
                if (sourceId === targetId) {
                    const baseAngle = -45;
                    const angleStep = 90;
                    const angle = (baseAngle + (d.selfLoopIndex || 0) * angleStep) * Math.PI / 180;
                    return sx + Math.cos(angle) * 55;
                }
                
                if (d.linkCount > 1) {
                    const mx = (sx + tx) / 2;
                    const dx = tx - sx;
                    const dy = (typeof d.target === 'object' ? d.target.y : nodes.find(n => n.id === d.target)?.y || 0) - 
                               (typeof d.source === 'object' ? d.source.y : nodes.find(n => n.id === d.source)?.y || 0);
                    const len = Math.sqrt(dx * dx + dy * dy) || 1;
                    const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                    return mx + (-dy / len) * offset;
                }
                
                return (sx + tx) / 2;
            })
            .attr('cy', d => {
                const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
                const targetId = typeof d.target === 'object' ? d.target.id : d.target;
                const sy = typeof d.source === 'object' ? d.source.y : nodes.find(n => n.id === d.source)?.y || 0;
                const ty = typeof d.target === 'object' ? d.target.y : nodes.find(n => n.id === d.target)?.y || 0;
                
                if (sourceId === targetId) {
                    const baseAngle = -45;
                    const angleStep = 90;
                    const angle = (baseAngle + (d.selfLoopIndex || 0) * angleStep) * Math.PI / 180;
                    return sy + Math.sin(angle) * 55;
                }
                
                if (d.linkCount > 1) {
                    const my = (sy + ty) / 2;
                    const dx = (typeof d.target === 'object' ? d.target.x : nodes.find(n => n.id === d.target)?.x || 0) - 
                               (typeof d.source === 'object' ? d.source.x : nodes.find(n => n.id === d.source)?.x || 0);
                    const dy = ty - sy;
                    const len = Math.sqrt(dx * dx + dy * dy) || 1;
                    const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                    return my + (dx / len) * offset;
                }
                
                return (sy + ty) / 2;
            });
            
            // Link labels
            linkLabels.each(function(d) {
                const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
                const targetId = typeof d.target === 'object' ? d.target.id : d.target;
                const sx = typeof d.source === 'object' ? d.source.x : 0;
                const sy = typeof d.source === 'object' ? d.source.y : 0;
                const tx = typeof d.target === 'object' ? d.target.x : 0;
                const ty = typeof d.target === 'object' ? d.target.y : 0;
                
                let mx, my;
                
                if (sourceId === targetId) {
                    const node = typeof d.source === 'object' ? d.source : nodes.find(n => n.id === d.source);
                    const baseAngle = -45;
                    const angleStep = 90;
                    const angle = (baseAngle + (d.selfLoopIndex || 0) * angleStep) * Math.PI / 180;
                    mx = node.x + Math.cos(angle) * 75;
                    my = node.y + Math.sin(angle) * 75;
                    d3.select(this).classed('self-loop-label', true);
                } else if (d.linkCount > 1) {
                    mx = (sx + tx) / 2;
                    my = (sy + ty) / 2;
                    const dx = tx - sx;
                    const dy = ty - sy;
                    const len = Math.sqrt(dx * dx + dy * dy) || 1;
                    const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                    mx += (-dy / len) * offset;
                    my += (dx / len) * offset;
                } else {
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
        legend.className = 'mapping-map-legend';
        legend.innerHTML = `
            <div class="mapping-map-legend-title">Legend</div>
            <div class="mapping-map-legend-item">
                <div class="mapping-map-legend-line mapped"></div>
                <span>Mapped</span>
            </div>
            <div class="mapping-map-legend-item">
                <div class="mapping-map-legend-line partial"></div>
                <span>Attributes Missing</span>
            </div>
            <div class="mapping-map-legend-item">
                <div class="mapping-map-legend-line unmapped"></div>
                <span>Not Mapped</span>
            </div>
            <div class="mapping-map-legend-item">
                <div class="mapping-map-legend-line inheritance"></div>
                <span>Inheritance</span>
            </div>
            <div class="mapping-map-legend-item">
                <div class="mapping-map-legend-line excluded"></div>
                <span>Excluded</span>
            </div>
        `;
        container.appendChild(legend);
        
        mappingMapInitialized = true;
        
        // Bind zoom controls
        document.getElementById('mappingMapZoomIn')?.addEventListener('click', () => {
            svg.transition().duration(300).call(mappingMapZoom.scaleBy, 1.3);
        });
        
        document.getElementById('mappingMapZoomOut')?.addEventListener('click', () => {
            svg.transition().duration(300).call(mappingMapZoom.scaleBy, 0.7);
        });
        
        document.getElementById('mappingMapResetView')?.addEventListener('click', () => {
            svg.transition().duration(500).call(mappingMapZoom.transform, d3.zoomIdentity);
        });
        
        document.getElementById('mappingMapResetLayout')?.addEventListener('click', async () => {
            // Reload saved layout from Ontology Designer
            const layout = await loadMapLayout();
            if (layout && layout.positions) {
                nodes.forEach((node, idx) => {
                    const savedPos = layout.positions[node.id];
                    if (savedPos) {
                        node.x = savedPos.x;
                        node.y = savedPos.y;
                        node.fx = savedPos.x;
                        node.fy = savedPos.y;
                    } else {
                        // Default grid position
                        node.x = 100 + (idx % 5) * 150;
                        node.y = 100 + Math.floor(idx / 5) * 120;
                        node.fx = node.x;
                        node.fy = node.y;
                    }
                });
                mappingMapSimulation.alpha(0.01).restart();
                
                if (typeof showNotification === 'function') {
                    showNotification('Layout restored from Ontology Designer', 'info', 2000);
                }
            } else {
                if (typeof showNotification === 'function') {
                    showNotification('No saved layout found', 'warning', 2000);
                }
            }
        });
        
    } catch (error) {
        console.error('Error initializing mapping map:', error);
    } finally {
        setTimeout(() => {
            showMappingDesignerLoading(false);
        }, 500);
    }
}

/**
 * Refresh the mapping design view (update mapping status)
 */
function refreshMappingDesign() {
    initMappingDesigner();
}

// Alias for compatibility
function loadOntologyIntoMappingDesigner() {
    refreshMappingDesign();
}

// ==========================================================================
// MAPPING RIGHT PANEL MANAGEMENT
// ==========================================================================

let currentPanelType = null; // 'entity' or 'relationship'
let currentPanelUri = null;

/**
 * Open the right panel for entity mapping
 */
function openEntityMappingFromDesign(entity) {
    if (!MappingState.loadedOntology || !MappingState.loadedOntology.classes) {
        showNotification('No ontology loaded', 'warning');
        return;
    }
    
    const ontologyClass = MappingState.loadedOntology.classes.find(c => 
        c.name === entity.name || c.localName === entity.name || c.uri === entity.uri
    );
    
    if (!ontologyClass) {
        showNotification(`Class "${entity.name}" not found in ontology`, 'warning');
        return;
    }
    
    currentPanelType = 'entity';
    currentPanelUri = ontologyClass.uri;
    
    const panelTitle = document.getElementById('panelTitle');
    panelTitle.innerHTML = `<i class="bi bi-box"></i> <span id="panelItemName">${ontologyClass.label || ontologyClass.name}</span>`;
    
    loadEntityPanelContent(ontologyClass.uri, ontologyClass.label || ontologyClass.name);
    openMappingPanel();
}

/**
 * Open the right panel for relationship mapping
 */
function openRelationshipMappingFromDesign(rel) {
    if (!MappingState.loadedOntology || !MappingState.loadedOntology.properties) {
        showNotification('No ontology loaded', 'warning');
        return;
    }
    
    const ontologyProperty = MappingState.loadedOntology.properties.find(p => 
        p.name === rel.name || p.localName === rel.name || p.uri === rel.uri
    );
    
    if (!ontologyProperty) {
        showNotification(`Relationship "${rel.name}" not found in ontology`, 'warning');
        return;
    }
    
    currentPanelType = 'relationship';
    currentPanelUri = ontologyProperty.uri;
    
    const panelTitle = document.getElementById('panelTitle');
    panelTitle.innerHTML = `<i class="bi bi-arrow-left-right"></i> <span id="panelItemName">${ontologyProperty.label || ontologyProperty.name}</span>`;
    
    loadRelationshipPanelContent(ontologyProperty);
    openMappingPanel();
}

/**
 * Open the mapping panel
 */
function openMappingPanel() {
    const container = document.getElementById('mappingDesignerContainer');
    container.classList.add('panel-open');
    
    // Resize SVG after transition completes
    setTimeout(resizeMapSvg, 250);
}

/**
 * Close the mapping panel
 */
function closeMappingPanel() {
    const container = document.getElementById('mappingDesignerContainer');
    if (container) container.classList.remove('panel-open');
    currentPanelType = null;
    currentPanelUri = null;
    
    // Invalidate any in-flight entity panel queries
    if (EntityPanelState._autoLoadTimer) {
        clearTimeout(EntityPanelState._autoLoadTimer);
        EntityPanelState._autoLoadTimer = null;
    }
    EntityPanelState._generation++;
    
    const panelBody = document.getElementById('panelBody');
    if (panelBody) panelBody.innerHTML = '';
    const saveBtn = document.getElementById('savePanelBtn');
    if (saveBtn) saveBtn.disabled = true;
    
    // Clear selections on map
    d3.selectAll('.mapping-map-node').classed('selected', false);
    d3.selectAll('.mapping-map-link-hitarea').classed('selected', false);
    
    // Resize SVG after transition completes
    setTimeout(resizeMapSvg, 250);
}

/**
 * Resize the map SVG to fit its container
 */
function resizeMapSvg() {
    const container = document.getElementById('mapping-map-container');
    if (!container || !mappingMapSvg) return;
    
    const width = container.clientWidth;
    const height = container.clientHeight;
    
    mappingMapSvg
        .attr('width', width)
        .attr('height', height);
    
    // Update simulation center if active
    if (mappingMapSimulation) {
        const centerForce = mappingMapSimulation.force('center');
        if (centerForce) {
            centerForce.x(width / 2).y(height / 2);
        }
    }
}

/**
 * Load entity panel content
 * @param {string} classUri - The class URI
 * @param {string} className - The class name
 * @param {HTMLElement} targetPanelBody - Optional target panel body element (defaults to 'panelBody')
 */
function loadEntityPanelContent(classUri, className, targetPanelBody = null) {
    const panelBody = targetPanelBody || document.getElementById('panelBody');
    const existingMapping = MappingState.config.entities.find(m => m.ontology_class === classUri);
    const classInfo = MappingState.loadedOntology?.classes?.find(c => c.uri === classUri);
    
    const attributes = classInfo?.dataProperties || [];
    
    const epAttrMap = existingMapping?.attribute_mappings || {};
    const epHasSql = !!(existingMapping?.sql_query);
    const epHasId = !!(existingMapping?.id_column);
    const epHasLabel = !!(existingMapping?.label_column);
    const epMappedAttrCount = attributes.filter(a => {
        const n = a.name || a.localName || '';
        return n && epAttrMap[n];
    }).length;
    const epStatusIcon = (ok) => ok
        ? '<i class="bi bi-check-circle-fill text-success"></i>'
        : '<i class="bi bi-x-circle-fill text-danger"></i>';
    const epOntologyRows = attributes.map((a, i) => {
        const name = a.name || a.localName || '';
        const assigned = !!(name && epAttrMap[name]);
        return `<tr><td class="text-muted" style="width:28px;">${i + 1}</td><td>${name}</td><td class="text-center">${epStatusIcon(assigned)}</td></tr>`;
    }).join('');

    panelBody.innerHTML = `
        <input type="hidden" id="panelEntityClass" value="${classUri}" />
        
        <ul class="nav nav-tabs" id="entityPanelTabs" role="tablist" style="gap: 2px;">
            <li class="nav-item" role="presentation">
                <button class="nav-link active" id="ep-status-tab" data-bs-toggle="tab" data-bs-target="#ep-status-pane" type="button">
                    <i class="bi bi-clipboard-check"></i> Status
                </button>
            </li>
            <li class="nav-item" role="presentation">
                <button class="nav-link" id="ep-sql-tab" data-bs-toggle="tab" data-bs-target="#ep-sql-pane" type="button">
                    <i class="bi bi-code-square"></i> SQL
                </button>
            </li>
            <li class="nav-item" role="presentation">
                <button class="nav-link" id="ep-mapping-tab" data-bs-toggle="tab" data-bs-target="#ep-mapping-pane" type="button" ${!existingMapping ? 'disabled' : ''}>
                    <i class="bi bi-diagram-3"></i> Mapping
                </button>
            </li>
        </ul>
        
        <div class="tab-content">
            <div class="tab-pane fade show active" id="ep-status-pane" role="tabpanel">
                <div class="small mb-2">
                    <span class="fw-semibold">${className}</span>
                    ${classInfo?.parent ? '<span class="text-muted ms-1">inherits <span class="fst-italic">' + classInfo.parent + '</span></span>' : ''}
                    <a href="/ontology/?section=entities&select=${encodeURIComponent(className)}" class="ms-2 small" title="View ${className} in Ontology Editor"><i class="bi bi-box-arrow-up-right"></i> Ontology</a>
                    ${classInfo?.comment ? '<p class="text-muted mt-1 mb-0" style="font-size:0.78rem;">' + classInfo.comment + '</p>' : ''}
                </div>
                <div class="form-check mb-2">
                    <input class="form-check-input" type="checkbox" id="epExcludeCheck" ${classInfo?.excluded ? '' : 'checked'}>
                    <label class="form-check-label small" for="epExcludeCheck">Include in mapping <span class="text-muted" style="font-size:0.72rem;">(uncheck to exclude this entity and its relationships from mapping, validation & R2RML generation)</span></label>
                </div>
                <div class="row g-2">
                    <div class="${attributes.length > 0 ? 'col-6' : 'col-12'}">
                        <div class="border rounded mb-2">
                            <table class="table table-sm mb-0" style="font-size:0.8rem;">
                                <tbody>
                                    <tr><td>${epStatusIcon(epHasSql)}</td><td>SQL Query</td><td class="text-muted small">${epHasSql ? 'Defined' : 'Not defined'}</td></tr>
                                    <tr><td>${epStatusIcon(epHasId)}</td><td>ID column</td><td class="text-muted small">${epHasId ? existingMapping.id_column : 'Not assigned'}</td></tr>
                                    <tr><td>${epStatusIcon(epHasLabel)}</td><td>Label column</td><td class="text-muted small">${epHasLabel ? existingMapping.label_column : 'Not assigned'}</td></tr>
                                    ${attributes.length > 0 ? '<tr><td>' + epStatusIcon(epMappedAttrCount === attributes.length) + '</td><td>Attributes</td><td class="text-muted small">' + epMappedAttrCount + ' / ' + attributes.length + ' assigned</td></tr>' : ''}
                                </tbody>
                            </table>
                        </div>
                        <div class="text-muted small">
                            ${existingMapping
                                ? '<i class="bi bi-check-circle text-success"></i> This entity has a mapping'
                                : '<i class="bi bi-info-circle"></i> No mapping yet — use Auto-Map or the SQL tab'}
                        </div>
                    </div>
                    ${attributes.length > 0
                        ? '<div class="col-6"><div class="border rounded" style="max-height: 200px; overflow-y: auto;"><table class="table table-sm table-striped mb-0" style="font-size:0.78rem;"><thead class="table-light sticky-top"><tr><th style="width:28px;">#</th><th>Attribute</th><th class="text-center" style="width:70px;">Mapped</th></tr></thead><tbody>' + epOntologyRows + '</tbody></table></div></div>'
                        : ''}
                </div>
            </div>

            <div class="tab-pane fade" id="ep-sql-pane" role="tabpanel">
                <div class="d-flex justify-content-between align-items-center mb-1">
                    <label class="form-label mb-0">SQL Query <span class="text-danger">*</span></label>
                    <button type="button" class="btn btn-sm btn-outline-primary py-0 px-2" id="epSqlRunBtn" onclick="runEntityPanelQuery()" title="Run query">
                        <i class="bi bi-play-fill me-1"></i><span style="font-size:0.75rem;">Run</span>
                    </button>
                </div>
                <textarea class="form-control form-control-sm font-monospace" id="epSqlQuery" placeholder="SELECT id, name FROM catalog.schema.table" style="resize: none;">${existingMapping?.sql_query || ''}</textarea>
                <div class="d-flex justify-content-end align-items-center mt-2">
                    <span id="epQueryStatus" class="text-muted small"></span>
                </div>
            </div>
            
            <div class="tab-pane fade" id="ep-mapping-pane" role="tabpanel">
                <div id="epMappingLoading" class="text-center py-4" style="display:${existingMapping ? 'flex' : 'none'}; flex-direction: column; align-items: center; justify-content: center;">
                    <div class="ob-loading-spinner">
                        <svg class="ob-spinner-svg" viewBox="0 0 80 80" fill="none">
                            <g class="ob-ring">
                                <g stroke="#CBD5E1" stroke-width="1.2" opacity="0.5">
                                    <line x1="40" y1="10" x2="61" y2="19"/><line x1="61" y1="19" x2="70" y2="40"/>
                                    <line x1="70" y1="40" x2="61" y2="61"/><line x1="61" y1="61" x2="40" y2="70"/>
                                    <line x1="40" y1="70" x2="19" y2="61"/><line x1="19" y1="61" x2="10" y2="40"/>
                                    <line x1="10" y1="40" x2="19" y2="19"/><line x1="19" y1="19" x2="40" y2="10"/>
                                </g>
                                <circle cx="40" cy="10" r="5" fill="#FF3621"/><circle cx="61" cy="19" r="5" fill="#6366F1"/>
                                <circle cx="70" cy="40" r="5" fill="#4ECDC4"/><circle cx="61" cy="61" r="5" fill="#F59E0B"/>
                                <circle cx="40" cy="70" r="5" fill="#FF3621"/><circle cx="19" cy="61" r="5" fill="#6366F1"/>
                                <circle cx="10" cy="40" r="5" fill="#4ECDC4"/><circle cx="19" cy="19" r="5" fill="#F59E0B"/>
                            </g>
                            <g transform="translate(40,40)">
                                <g class="ob-center">
                                    <path d="M0-12 L10-6 L0 0 L-10-6Z" fill="#FF3621"/>
                                    <path d="M0-5 L10 1 L0 7 L-10 1Z" fill="#FF3621" opacity="0.85"/>
                                    <path d="M0 2 L10 8 L0 14 L-10 8Z" fill="#FF3621" opacity="0.7"/>
                                </g>
                            </g>
                        </svg>
                        <span class="ob-spinner-label" style="font-size:0.8rem;">Loading mapping data...</span>
                    </div>
                </div>
                <div id="epMappingGrid" style="display:none; flex-direction:column; flex:1; min-height:0;">
                    <div class="d-flex justify-content-between align-items-center mb-2 flex-shrink-0">
                        <div class="small text-muted"><i class="bi bi-info-circle"></i> Click column headers to assign mappings</div>
                        <div class="d-flex align-items-center gap-2">
                            <label class="small text-muted mb-0 text-nowrap">Limit:</label>
                            <input type="number" class="form-control form-control-sm" id="epPreviewLimit" value="10" min="1" max="1000" style="width: 55px;">
                            <button type="button" class="btn btn-outline-dark btn-sm" id="epRunQueryBtn"><i class="bi bi-arrow-clockwise"></i> Refresh</button>
                        </div>
                    </div>
                    <div class="table-responsive border rounded mb-2" style="flex:1; min-height:0; overflow:auto;">
                        <table class="table table-sm table-striped mb-0" id="epResultsTable">
                            <thead class="table-light sticky-top"><tr id="epResultsHeader"></tr></thead>
                            <tbody id="epResultsBody"></tbody>
                        </table>
                    </div>
                    <div id="epMappingStatus" class="small text-danger flex-shrink-0"><i class="bi bi-x-circle"></i> ID required</div>
                </div>
            </div>
        </div>
    `;
    
    initEntityPanel(classUri, className, existingMapping, classInfo);
}

/**
 * Load relationship panel content
 * @param {Object} ontologyProperty - The ontology property object
 * @param {HTMLElement} targetPanelBody - Optional target panel body element (defaults to 'panelBody')
 */
function loadRelationshipPanelContent(ontologyProperty, targetPanelBody = null) {
    const panelBody = targetPanelBody || document.getElementById('panelBody');
    const existingMapping = MappingState.config.relationships.find(m => m.property === ontologyProperty.uri);
    
    const domainUri = ontologyProperty.domain;
    const rangeUri = ontologyProperty.range;
    const direction = ontologyProperty.direction || 'forward';
    const sourceUri = direction === 'reverse' ? rangeUri : domainUri;
    const targetUri = direction === 'reverse' ? domainUri : rangeUri;
    
    const getLocalName = (uri) => uri ? uri.split('#').pop().split('/').pop() : '';
    const sourceName = getLocalName(sourceUri);
    const targetName = getLocalName(targetUri);
    
    const relName = ontologyProperty.label || ontologyProperty.name || '';
    const relComment = ontologyProperty.comment || '';
    const hasSql = !!(existingMapping?.sql_query);
    const hasSrcId = !!(existingMapping?.source_id_column);
    const hasTgtId = !!(existingMapping?.target_id_column);
    const relAttrMap = existingMapping?.attribute_mappings || {};
    const relAttributes = ontologyProperty?.properties || [];
    const mappedAttrCount = relAttributes.filter(a => {
        const n = a.name || a.localName || '';
        return n && relAttrMap[n];
    }).length;

    const allClasses = MappingState.loadedOntology?.classes || [];
    const excludedClassNames = new Set(allClasses.filter(c => c.excluded).map(c => c.name || c.localName || ''));
    const isParentExcluded = excludedClassNames.has(ontologyProperty.domain) || excludedClassNames.has(ontologyProperty.range);
    const isRelExcluded = !!ontologyProperty.excluded || isParentExcluded;
    const rpExcludeDisabled = isParentExcluded ? 'disabled' : '';
    const rpExcludeTitle = isParentExcluded
        ? 'Excluded because a connected entity is excluded'
        : (isRelExcluded ? 'Include in mapping' : 'Exclude from mapping');

    const statusIcon = (ok) => ok
        ? '<i class="bi bi-check-circle-fill text-success"></i>'
        : '<i class="bi bi-x-circle-fill text-danger"></i>';

    const rpStatusRows = `
        <tr><td>${statusIcon(hasSql)}</td><td>SQL Query</td><td class="text-muted small">${hasSql ? 'Defined' : 'Not defined'}</td></tr>
        <tr><td>${statusIcon(hasSrcId)}</td><td>Source ID column</td><td class="text-muted small">${hasSrcId ? existingMapping.source_id_column : 'Not assigned'}</td></tr>
        <tr><td>${statusIcon(hasTgtId)}</td><td>Target ID column</td><td class="text-muted small">${hasTgtId ? existingMapping.target_id_column : 'Not assigned'}</td></tr>
        ${relAttributes.length > 0 ? `<tr><td>${statusIcon(mappedAttrCount === relAttributes.length)}</td><td>Attributes</td><td class="text-muted small">${mappedAttrCount} / ${relAttributes.length} assigned</td></tr>` : ''}
    `;

    const rpOntologyRows = relAttributes.map((a, i) => {
        const name = a.name || a.localName || '';
        const assigned = !!(name && relAttrMap[name]);
        return `<tr><td class="text-muted" style="width:28px;">${i + 1}</td><td>${name}</td><td class="text-center">${statusIcon(assigned)}</td></tr>`;
    }).join('');

    panelBody.innerHTML = `
        <input type="hidden" id="panelPropertyUri" value="${ontologyProperty.uri}" />
        
        <div class="d-flex align-items-center justify-content-center gap-2 py-2 mb-2 bg-light rounded">
            <span class="badge bg-primary small">${sourceName}</span>
            <i class="bi bi-arrow-right text-muted"></i>
            <span class="badge bg-success small">${targetName}</span>
        </div>
        
        <ul class="nav nav-tabs" id="relPanelTabs" role="tablist" style="gap: 2px;">
            <li class="nav-item" role="presentation">
                <button class="nav-link active" id="rp-status-tab" data-bs-toggle="tab" data-bs-target="#rp-status-pane" type="button">
                    <i class="bi bi-clipboard-check"></i> Status
                </button>
            </li>
            <li class="nav-item" role="presentation">
                <button class="nav-link" id="rp-sql-tab" data-bs-toggle="tab" data-bs-target="#rp-sql-pane" type="button">
                    <i class="bi bi-code-square"></i> SQL
                </button>
            </li>
            <li class="nav-item" role="presentation">
                <button class="nav-link" id="rp-mapping-tab" data-bs-toggle="tab" data-bs-target="#rp-mapping-pane" type="button" ${!existingMapping ? 'disabled' : ''}>
                    <i class="bi bi-diagram-3"></i> Mapping
                </button>
            </li>
        </ul>
        
        <div class="tab-content">
            <div class="tab-pane fade show active" id="rp-status-pane" role="tabpanel">
                <div class="small mb-2">
                    <span class="fw-semibold">${relName}</span>
                    <span class="text-muted ms-1">(${sourceName} &rarr; ${targetName})</span>
                    <a href="/ontology/?section=relationships&select=${encodeURIComponent(relName)}" class="ms-2 small" title="View ${relName} in Ontology Editor"><i class="bi bi-box-arrow-up-right"></i> Ontology</a>
                    ${relComment ? '<p class="text-muted mt-1 mb-0" style="font-size:0.78rem;">' + relComment + '</p>' : ''}
                </div>
                <div class="form-check mb-2">
                    <input class="form-check-input" type="checkbox" id="rpExcludeCheck" ${isRelExcluded ? '' : 'checked'} ${rpExcludeDisabled} title="${rpExcludeTitle}">
                    <label class="form-check-label small" for="rpExcludeCheck">Include in mapping <span class="text-muted" style="font-size:0.72rem;">(${isParentExcluded ? 'inherited from excluded connected entity' : 'uncheck to exclude this relationship from mapping, validation & R2RML generation'})</span></label>
                </div>
                <div class="row g-2">
                    <div class="${relAttributes.length > 0 ? 'col-6' : 'col-12'}">
                        <div class="border rounded mb-2">
                            <table class="table table-sm mb-0" style="font-size:0.8rem;">
                                <tbody>${rpStatusRows}</tbody>
                            </table>
                        </div>
                        <div class="text-muted small">
                            ${existingMapping
                                ? '<i class="bi bi-check-circle text-success"></i> This relationship has a mapping'
                                : '<i class="bi bi-info-circle"></i> No mapping yet — use Auto-Map or the SQL tab'}
                        </div>
                    </div>
                    ${relAttributes.length > 0
                        ? '<div class="col-6"><div class="border rounded" style="max-height: 200px; overflow-y: auto;"><table class="table table-sm table-striped mb-0" style="font-size:0.78rem;"><thead class="table-light sticky-top"><tr><th style="width:28px;">#</th><th>Attribute</th><th class="text-center" style="width:70px;">Mapped</th></tr></thead><tbody>' + rpOntologyRows + '</tbody></table></div></div>'
                        : ''}
                </div>
            </div>

            <div class="tab-pane fade" id="rp-sql-pane" role="tabpanel">
                <div class="d-flex justify-content-between align-items-center mb-1">
                    <label class="form-label mb-0">SQL Query <span class="text-danger">*</span></label>
                    <button type="button" class="btn btn-sm btn-outline-primary py-0 px-2" id="rpSqlRunBtn" onclick="runRelPanelQuery()" title="Run query">
                        <i class="bi bi-play-fill me-1"></i><span style="font-size:0.75rem;">Run</span>
                    </button>
                </div>
                <textarea class="form-control form-control-sm font-monospace" id="rpSqlQuery" placeholder="SELECT source_id, target_id FROM ..." style="resize: none;">${existingMapping?.sql_query || ''}</textarea>
                <div class="d-flex justify-content-end align-items-center mt-2">
                    <span id="rpQueryStatus" class="text-muted small"></span>
                </div>
            </div>
            
            <div class="tab-pane fade" id="rp-mapping-pane" role="tabpanel">
                <div id="rpMappingLoading" class="text-center py-4" style="display:${existingMapping ? 'flex' : 'none'}; flex-direction: column; align-items: center; justify-content: center;">
                    <div class="ob-loading-spinner">
                        <svg class="ob-spinner-svg" viewBox="0 0 80 80" fill="none">
                            <g class="ob-ring">
                                <g stroke="#CBD5E1" stroke-width="1.2" opacity="0.5">
                                    <line x1="40" y1="10" x2="61" y2="19"/><line x1="61" y1="19" x2="70" y2="40"/>
                                    <line x1="70" y1="40" x2="61" y2="61"/><line x1="61" y1="61" x2="40" y2="70"/>
                                    <line x1="40" y1="70" x2="19" y2="61"/><line x1="19" y1="61" x2="10" y2="40"/>
                                    <line x1="10" y1="40" x2="19" y2="19"/><line x1="19" y1="19" x2="40" y2="10"/>
                                </g>
                                <circle cx="40" cy="10" r="5" fill="#FF3621"/><circle cx="61" cy="19" r="5" fill="#6366F1"/>
                                <circle cx="70" cy="40" r="5" fill="#4ECDC4"/><circle cx="61" cy="61" r="5" fill="#F59E0B"/>
                                <circle cx="40" cy="70" r="5" fill="#FF3621"/><circle cx="19" cy="61" r="5" fill="#6366F1"/>
                                <circle cx="10" cy="40" r="5" fill="#4ECDC4"/><circle cx="19" cy="19" r="5" fill="#F59E0B"/>
                            </g>
                            <g transform="translate(40,40)">
                                <g class="ob-center">
                                    <path d="M0-12 L10-6 L0 0 L-10-6Z" fill="#FF3621"/>
                                    <path d="M0-5 L10 1 L0 7 L-10 1Z" fill="#FF3621" opacity="0.85"/>
                                    <path d="M0 2 L10 8 L0 14 L-10 8Z" fill="#FF3621" opacity="0.7"/>
                                </g>
                            </g>
                        </svg>
                        <span class="ob-spinner-label" style="font-size:0.8rem;">Loading mapping data...</span>
                    </div>
                </div>
                <div id="rpMappingGrid" style="display:none; flex-direction:column; flex:1; min-height:0;">
                    <div class="d-flex justify-content-between align-items-center mb-2 flex-shrink-0">
                        <div class="small text-muted"><i class="bi bi-info-circle"></i> Click column headers to assign mappings</div>
                        <div class="d-flex align-items-center gap-2">
                            <label class="small text-muted mb-0 text-nowrap">Limit:</label>
                            <input type="number" class="form-control form-control-sm" id="rpPreviewLimit" value="10" min="1" max="1000" style="width: 55px;">
                            <button type="button" class="btn btn-outline-dark btn-sm" id="rpRunQueryBtn"><i class="bi bi-arrow-clockwise"></i> Refresh</button>
                        </div>
                    </div>
                    <div class="table-responsive border rounded mb-2" style="flex:1; min-height:0; overflow:auto;">
                        <table class="table table-sm table-striped mb-0" id="rpResultsTable">
                            <thead class="table-light sticky-top"><tr id="rpResultsHeader"></tr></thead>
                            <tbody id="rpResultsBody"></tbody>
                        </table>
                    </div>
                    <div id="rpMappingStatus" class="small text-danger flex-shrink-0"><i class="bi bi-x-circle"></i> Source & Target required</div>
                </div>
            </div>
        </div>
    `;
    
    initRelationshipPanel(ontologyProperty, existingMapping);
}

// ==========================================================================
// ENTITY PANEL INITIALIZATION
// ==========================================================================

const EntityPanelState = {
    columns: [],
    rows: [],
    idColumn: null,
    labelColumn: null,
    attributeMappings: {},
    attributes: [],
    _generation: 0,
    _autoLoadTimer: null
};

function initEntityPanel(classUri, className, existingMapping, classInfo) {
    // Cancel any pending autoLoad from a previous entity to prevent race conditions
    if (EntityPanelState._autoLoadTimer) {
        clearTimeout(EntityPanelState._autoLoadTimer);
        EntityPanelState._autoLoadTimer = null;
    }
    // Increment generation so in-flight fetches from a previous entity are discarded
    EntityPanelState._generation++;

    EntityPanelState.columns = [];
    EntityPanelState.rows = [];
    EntityPanelState.idColumn = existingMapping?.id_column || null;
    EntityPanelState.labelColumn = existingMapping?.label_column || null;
    EntityPanelState.attributeMappings = existingMapping?.attribute_mappings ? {...existingMapping.attribute_mappings} : {};
    EntityPanelState.attributes = classInfo?.dataProperties || [];
    
    updateEntityPanelSaveBtn();
    
    document.getElementById('epRunQueryBtn')?.addEventListener('click', runEntityPanelQuery);
    
    // Exclude checkbox
    const epExcludeCb = document.getElementById('epExcludeCheck');
    if (epExcludeCb) {
        epExcludeCb.addEventListener('change', function() {
            toggleEntityExclusion(classUri, !this.checked, 'entity');
        });
    }
    
    // Auto-load query data in background when there is an existing mapping with SQL
    if (existingMapping?.sql_query) {
        EntityPanelState._autoLoadTimer = setTimeout(() => {
            EntityPanelState._autoLoadTimer = null;
            runEntityPanelQuery({ autoLoad: true });
        }, 100);
    }
}

async function runEntityPanelQuery(options = {}) {
    const sqlEl = document.getElementById('epSqlQuery');
    if (!sqlEl) return;
    const sql = sqlEl.value.trim();
    if (!sql) {
        showNotification('Please enter a SQL query', 'warning');
        return;
    }
    
    // Capture the generation at call time so we can detect stale responses
    const capturedGeneration = EntityPanelState._generation;
    
    const previewLimit = parseInt(document.getElementById('epPreviewLimit')?.value) || 10;
    const btn = document.getElementById('epRunQueryBtn');
    const statusEl = document.getElementById('epQueryStatus');
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Refreshing...'; }
    if (statusEl) statusEl.textContent = 'Executing...';
    
    try {
        const response = await fetch('/mapping/test-query', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({query: sql, limit: previewLimit}),
            credentials: 'same-origin'
        });
        const result = await response.json();
        
        // Discard stale response if the user switched to a different entity
        if (currentPanelType !== 'entity' || capturedGeneration !== EntityPanelState._generation) return;
        
        if (result.success) {
            EntityPanelState.columns = result.columns;
            EntityPanelState.rows = result.rows || [];
            
            if (!EntityPanelState.idColumn || !result.columns.includes(EntityPanelState.idColumn)) {
                EntityPanelState.idColumn = null;
            }
            if (EntityPanelState.labelColumn && !result.columns.includes(EntityPanelState.labelColumn)) {
                EntityPanelState.labelColumn = null;
            }
            
            autoMapEntityColumns(result.columns);
            renderEntityPanelGrid();
            const epSummary = document.getElementById('epMappingSummary');
            if (epSummary) epSummary.style.display = 'none';
            const epLoading = document.getElementById('epMappingLoading');
            if (epLoading) epLoading.style.display = 'none';
            const epGrid = document.getElementById('epMappingGrid');
            if (epGrid) epGrid.style.display = 'flex';
            
            const epTab = document.getElementById('ep-mapping-tab');
            if (epTab) {
                epTab.disabled = false;
                if (!options.autoLoad) bootstrap.Tab.getOrCreateInstance(epTab).show();
            }
            
            if (statusEl) statusEl.innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> ' + result.row_count + ' rows</span>';
        } else {
            const epLoading = document.getElementById('epMappingLoading');
            if (epLoading) epLoading.style.display = 'none';
            if (statusEl) statusEl.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> Error</span>';
            const reason = [result.message, result.detail].filter(Boolean).join(' — ');
            showNotification('Query failed: ' + (reason || 'Unknown error'), 'error');
        }
    } catch (error) {
        if (capturedGeneration !== EntityPanelState._generation) return;
        const epLoading = document.getElementById('epMappingLoading');
        if (epLoading) epLoading.style.display = 'none';
        if (statusEl) statusEl.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> Error</span>';
        showNotification('Error: ' + error.message, 'error');
    } finally {
        if (capturedGeneration === EntityPanelState._generation && btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Refresh';
        }
    }
}

function renderEntityPanelGrid() {
    const headerRow = document.getElementById('epResultsHeader');
    const tbody = document.getElementById('epResultsBody');
    if (!headerRow || !tbody) return;
    
    headerRow.innerHTML = EntityPanelState.columns.map(col => {
        let badge = '';
        if (EntityPanelState.idColumn === col) {
            badge = '<span class="badge bg-primary">ID</span>';
        } else if (EntityPanelState.labelColumn === col) {
            badge = '<span class="badge bg-info">Label</span>';
        } else {
            const attr = Object.entries(EntityPanelState.attributeMappings).find(([a, c]) => c === col);
            if (attr) badge = `<span class="badge bg-secondary">${attr[0]}</span>`;
            else badge = '<span class="badge bg-light text-muted border">Map</span>';
        }
        return `<th data-col="${col}" style="cursor:pointer;">${col}<br>${badge}</th>`;
    }).join('');
    
    tbody.innerHTML = EntityPanelState.rows.map(row => 
        '<tr>' + EntityPanelState.columns.map(col => `<td>${row[col] ?? '<em class="text-muted">null</em>'}</td>`).join('') + '</tr>'
    ).join('') || '<tr><td colspan="' + EntityPanelState.columns.length + '" class="text-center text-muted">No data</td></tr>';
    
    headerRow.querySelectorAll('th').forEach(th => {
        th.addEventListener('click', () => showEntityColumnMenu(th, th.dataset.col));
    });
    
    updateEntityPanelSaveBtn();
}

function showEntityColumnMenu(th, column) {
    document.querySelectorAll('.panel-col-menu').forEach(m => m.remove());
    
    const menu = document.createElement('div');
    menu.className = 'panel-col-menu dropdown-menu show';
    menu.style.cssText = 'position:fixed;z-index:9999;';
    
    const attrOptions = EntityPanelState.attributes.map(attr => {
        const name = attr.name || attr.localName;
        const isSel = EntityPanelState.attributeMappings[name] === column;
        return `<a class="dropdown-item ${isSel ? 'active' : ''}" data-action="attr" data-attr="${name}"><i class="bi bi-list"></i> ${name}</a>`;
    }).join('');
    
    menu.innerHTML = `
        <a class="dropdown-item ${EntityPanelState.idColumn === column ? 'active' : ''}" data-action="id"><i class="bi bi-key text-primary"></i> Set as ID</a>
        <a class="dropdown-item ${EntityPanelState.labelColumn === column ? 'active' : ''}" data-action="label"><i class="bi bi-tag text-info"></i> Set as Label</a>
        <hr class="my-1">
        ${attrOptions || '<span class="dropdown-item text-muted">No attributes</span>'}
        <hr class="my-1">
        <a class="dropdown-item text-danger" data-action="clear"><i class="bi bi-x-circle"></i> Clear</a>
    `;
    
    const rect = th.getBoundingClientRect();
    menu.style.top = (rect.bottom + 2) + 'px';
    menu.style.left = rect.left + 'px';
    document.body.appendChild(menu);
    
    menu.querySelectorAll('.dropdown-item').forEach(item => {
        item.addEventListener('click', () => {
            const action = item.dataset.action;
            if (EntityPanelState.idColumn === column) EntityPanelState.idColumn = null;
            if (EntityPanelState.labelColumn === column) EntityPanelState.labelColumn = null;
            Object.keys(EntityPanelState.attributeMappings).forEach(a => {
                if (EntityPanelState.attributeMappings[a] === column) delete EntityPanelState.attributeMappings[a];
            });
            
            if (action === 'id') EntityPanelState.idColumn = column;
            else if (action === 'label') EntityPanelState.labelColumn = column;
            else if (action === 'attr') EntityPanelState.attributeMappings[item.dataset.attr] = column;
            
            menu.remove();
            renderEntityPanelGrid();
        });
    });
    
    setTimeout(() => {
        document.addEventListener('click', function closeMenu(e) {
            if (!menu.contains(e.target)) {
                menu.remove();
                document.removeEventListener('click', closeMenu);
            }
        });
    }, 10);
}

function autoMapEntityColumns(columns) {
    const lowerColumns = columns.map(c => c.toLowerCase());
    
    if (!EntityPanelState.idColumn) {
        const idCandidates = ['id', 'identifier', '_id', 'pk', 'key', 'entity_id'];
        for (const candidate of idCandidates) {
            const idx = lowerColumns.findIndex(c => c === candidate || c.endsWith('_id') || c.endsWith('_pk'));
            if (idx !== -1) {
                EntityPanelState.idColumn = columns[idx];
                break;
            }
        }
    }
    
    if (!EntityPanelState.labelColumn) {
        const labelCandidates = ['label', 'name', 'title', 'description', 'display_name'];
        for (const candidate of labelCandidates) {
            const idx = lowerColumns.findIndex(c => c === candidate || c.endsWith('_name') || c.endsWith('_label'));
            if (idx !== -1 && columns[idx] !== EntityPanelState.idColumn) {
                EntityPanelState.labelColumn = columns[idx];
                break;
            }
        }
    }
    
    if (EntityPanelState.attributes && EntityPanelState.attributes.length > 0) {
        for (const attr of EntityPanelState.attributes) {
            const attrName = (attr.name || attr.localName || attr).toLowerCase();
            if (EntityPanelState.attributeMappings[attr.name || attr.localName || attr]) continue;
            
            const idx = lowerColumns.findIndex(c => 
                c === attrName || 
                c === attrName.replace(/_/g, '') ||
                c.replace(/_/g, '') === attrName ||
                c.includes(attrName) ||
                attrName.includes(c)
            );
            
            if (idx !== -1 && 
                columns[idx] !== EntityPanelState.idColumn && 
                columns[idx] !== EntityPanelState.labelColumn) {
                EntityPanelState.attributeMappings[attr.name || attr.localName || attr] = columns[idx];
            }
        }
    }
}

function updateEntityPanelSaveBtn() {
    const saveBtn = document.getElementById('savePanelBtn');
    const statusEl = document.getElementById('epMappingStatus');
    
    if (window.isActiveVersion === false) {
        if (saveBtn) saveBtn.disabled = true;
        return;
    }

    if (EntityPanelState.idColumn) {
        saveBtn.disabled = false;
        if (statusEl) statusEl.innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> ID: ' + EntityPanelState.idColumn + '</span>';
    } else {
        saveBtn.disabled = true;
        if (statusEl) statusEl.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> ID required</span>';
    }
}

// ==========================================================================
// RELATIONSHIP PANEL INITIALIZATION
// ==========================================================================

const RelPanelState = {
    columns: [],
    rows: [],
    sourceIdColumn: null,
    targetIdColumn: null,
    attributeMappings: {},
    attributes: []
};

function initRelationshipPanel(ontologyProperty, existingMapping) {
    RelPanelState.columns = [];
    RelPanelState.rows = [];
    RelPanelState.sourceIdColumn = existingMapping?.source_id_column || null;
    RelPanelState.targetIdColumn = existingMapping?.target_id_column || null;
    RelPanelState.attributeMappings = existingMapping?.attribute_mappings ? {...existingMapping.attribute_mappings} : {};
    RelPanelState.attributes = ontologyProperty?.properties || [];
    
    updateRelPanelSaveBtn();
    
    document.getElementById('rpRunQueryBtn')?.addEventListener('click', runRelPanelQuery);
    
    // Exclude checkbox
    const rpExcludeCb = document.getElementById('rpExcludeCheck');
    if (rpExcludeCb) {
        rpExcludeCb.addEventListener('change', function() {
            toggleEntityExclusion(ontologyProperty.uri, !this.checked, 'relationship');
        });
    }
    
    // Auto-load query data in background when there is an existing mapping with SQL
    if (existingMapping?.sql_query) {
        setTimeout(() => {
            runRelPanelQuery({ autoLoad: true });
        }, 100);
    }
}

async function runRelPanelQuery(options = {}) {
    const sqlEl = document.getElementById('rpSqlQuery');
    if (!sqlEl) return;
    const sql = sqlEl.value.trim();
    if (!sql) {
        showNotification('Please enter a SQL query', 'warning');
        return;
    }
    
    const previewLimit = parseInt(document.getElementById('rpPreviewLimit')?.value) || 10;
    const btn = document.getElementById('rpRunQueryBtn');
    const statusEl = document.getElementById('rpQueryStatus');
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Refreshing...'; }
    if (statusEl) statusEl.textContent = 'Executing...';
    
    try {
        const response = await fetch('/mapping/test-query', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({query: sql, limit: previewLimit}),
            credentials: 'same-origin'
        });
        const result = await response.json();
        
        if (currentPanelType !== 'relationship') return;
        
        if (result.success) {
            RelPanelState.columns = result.columns;
            RelPanelState.rows = result.rows || [];
            
            autoMapRelColumns(result.columns);
            renderRelPanelGrid();
            const rpSummary = document.getElementById('rpMappingSummary');
            if (rpSummary) rpSummary.style.display = 'none';
            const rpLoading = document.getElementById('rpMappingLoading');
            if (rpLoading) rpLoading.style.display = 'none';
            const rpGrid = document.getElementById('rpMappingGrid');
            if (rpGrid) rpGrid.style.display = 'flex';
            
            const rpTab = document.getElementById('rp-mapping-tab');
            if (rpTab) {
                rpTab.disabled = false;
                if (!options.autoLoad) bootstrap.Tab.getOrCreateInstance(rpTab).show();
            }
            
            if (statusEl) statusEl.innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> ' + result.row_count + ' rows</span>';
        } else {
            const rpLoading = document.getElementById('rpMappingLoading');
            if (rpLoading) rpLoading.style.display = 'none';
            if (statusEl) statusEl.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> Error</span>';
            const reason = [result.message, result.detail].filter(Boolean).join(' — ');
            showNotification('Query failed: ' + (reason || 'Unknown error'), 'error');
        }
    } catch (error) {
        const rpLoading = document.getElementById('rpMappingLoading');
        if (rpLoading) rpLoading.style.display = 'none';
        if (statusEl) statusEl.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> Error</span>';
        showNotification('Error: ' + error.message, 'error');
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Refresh'; }
    }
}

function renderRelPanelGrid() {
    const headerRow = document.getElementById('rpResultsHeader');
    const tbody = document.getElementById('rpResultsBody');
    if (!headerRow || !tbody) return;
    
    headerRow.innerHTML = RelPanelState.columns.map(col => {
        let badge = '';
        if (RelPanelState.sourceIdColumn === col) {
            badge = '<span class="badge bg-primary">Source</span>';
        } else if (RelPanelState.targetIdColumn === col) {
            badge = '<span class="badge bg-success">Target</span>';
        } else {
            const attr = Object.entries(RelPanelState.attributeMappings).find(([a, c]) => c === col);
            if (attr) badge = `<span class="badge bg-secondary">${attr[0]}</span>`;
            else badge = '<span class="badge bg-light text-muted border">Map</span>';
        }
        return `<th data-col="${col}" style="cursor:pointer;">${col}<br>${badge}</th>`;
    }).join('');
    
    tbody.innerHTML = RelPanelState.rows.map(row => 
        '<tr>' + RelPanelState.columns.map(col => `<td>${row[col] ?? '<em class="text-muted">null</em>'}</td>`).join('') + '</tr>'
    ).join('') || '<tr><td colspan="' + RelPanelState.columns.length + '" class="text-center text-muted">No data</td></tr>';
    
    headerRow.querySelectorAll('th').forEach(th => {
        th.addEventListener('click', () => showRelColumnMenu(th, th.dataset.col));
    });
    
    updateRelPanelSaveBtn();
}

function showRelColumnMenu(th, column) {
    document.querySelectorAll('.panel-col-menu').forEach(m => m.remove());
    
    const menu = document.createElement('div');
    menu.className = 'panel-col-menu dropdown-menu show';
    menu.style.cssText = 'position:fixed;z-index:9999;';
    
    const attrOptions = RelPanelState.attributes.map(attr => {
        const name = attr.name || attr.localName;
        const isSel = RelPanelState.attributeMappings[name] === column;
        return `<a class="dropdown-item ${isSel ? 'active' : ''}" data-action="attr" data-attr="${name}"><i class="bi bi-list"></i> ${name}</a>`;
    }).join('');
    
    menu.innerHTML = `
        <a class="dropdown-item ${RelPanelState.sourceIdColumn === column ? 'active' : ''}" data-action="source"><i class="bi bi-box-arrow-right text-primary"></i> Set as Source ID</a>
        <a class="dropdown-item ${RelPanelState.targetIdColumn === column ? 'active' : ''}" data-action="target"><i class="bi bi-box-arrow-in-right text-success"></i> Set as Target ID</a>
        <hr class="my-1">
        ${attrOptions || '<span class="dropdown-item text-muted">No attributes</span>'}
        <hr class="my-1">
        <a class="dropdown-item text-danger" data-action="clear"><i class="bi bi-x-circle"></i> Clear</a>
    `;
    
    const rect = th.getBoundingClientRect();
    menu.style.top = (rect.bottom + 2) + 'px';
    menu.style.left = rect.left + 'px';
    document.body.appendChild(menu);
    
    menu.querySelectorAll('.dropdown-item').forEach(item => {
        item.addEventListener('click', () => {
            const action = item.dataset.action;
            if (RelPanelState.sourceIdColumn === column) RelPanelState.sourceIdColumn = null;
            if (RelPanelState.targetIdColumn === column) RelPanelState.targetIdColumn = null;
            Object.keys(RelPanelState.attributeMappings).forEach(a => {
                if (RelPanelState.attributeMappings[a] === column) delete RelPanelState.attributeMappings[a];
            });
            
            if (action === 'source') RelPanelState.sourceIdColumn = column;
            else if (action === 'target') RelPanelState.targetIdColumn = column;
            else if (action === 'attr') RelPanelState.attributeMappings[item.dataset.attr] = column;
            
            menu.remove();
            renderRelPanelGrid();
        });
    });
    
    setTimeout(() => {
        document.addEventListener('click', function closeMenu(e) {
            if (!menu.contains(e.target)) {
                menu.remove();
                document.removeEventListener('click', closeMenu);
            }
        });
    }, 10);
}

function autoMapRelColumns(columns) {
    const lowerColumns = columns.map(c => c.toLowerCase());
    
    if (!RelPanelState.sourceIdColumn) {
        const sourceCandidates = ['source_id', 'source', 'from_id', 'from', 'src_id', 'parent_id'];
        for (const candidate of sourceCandidates) {
            const idx = lowerColumns.findIndex(c => c === candidate || c.startsWith('source') || c.startsWith('from'));
            if (idx !== -1) {
                RelPanelState.sourceIdColumn = columns[idx];
                break;
            }
        }
    }
    
    if (!RelPanelState.targetIdColumn) {
        const targetCandidates = ['target_id', 'target', 'to_id', 'to', 'dest_id', 'child_id'];
        for (const candidate of targetCandidates) {
            const idx = lowerColumns.findIndex(c => c === candidate || c.startsWith('target') || c.startsWith('to_'));
            if (idx !== -1 && columns[idx] !== RelPanelState.sourceIdColumn) {
                RelPanelState.targetIdColumn = columns[idx];
                break;
            }
        }
    }
    
    if (RelPanelState.attributes && RelPanelState.attributes.length > 0) {
        for (const attr of RelPanelState.attributes) {
            const attrName = (attr.name || attr.localName || attr).toLowerCase();
            if (RelPanelState.attributeMappings[attr.name || attr.localName || attr]) continue;
            
            const idx = lowerColumns.findIndex(c => 
                c === attrName || 
                c === attrName.replace(/_/g, '') ||
                c.replace(/_/g, '') === attrName ||
                c.includes(attrName) ||
                attrName.includes(c)
            );
            
            if (idx !== -1 && 
                columns[idx] !== RelPanelState.sourceIdColumn && 
                columns[idx] !== RelPanelState.targetIdColumn) {
                RelPanelState.attributeMappings[attr.name || attr.localName || attr] = columns[idx];
            }
        }
    }
}

function updateRelPanelSaveBtn() {
    const saveBtn = document.getElementById('savePanelBtn');
    const statusEl = document.getElementById('rpMappingStatus');
    
    if (window.isActiveVersion === false) {
        if (saveBtn) saveBtn.disabled = true;
        return;
    }

    if (RelPanelState.sourceIdColumn && RelPanelState.targetIdColumn) {
        saveBtn.disabled = false;
        if (statusEl) statusEl.innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> Source: ' + RelPanelState.sourceIdColumn + ' | Target: ' + RelPanelState.targetIdColumn + '</span>';
    } else {
        saveBtn.disabled = true;
        if (statusEl) statusEl.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> Source & Target required</span>';
    }
}

// ==========================================================================
// PANEL SAVE AND CLOSE HANDLERS
// ==========================================================================


function savePanelMapping() {
    if (currentPanelType === 'entity') {
        saveEntityPanelMapping();
    } else if (currentPanelType === 'relationship') {
        saveRelPanelMapping();
    }
}

function saveEntityPanelMapping() {
    const classUri = document.getElementById('panelEntityClass')?.value;
    const sqlQueryRaw = document.getElementById('epSqlQuery')?.value?.trim();
    const sqlQuery = stripLimitClause(sqlQueryRaw);
    
    if (!sqlQuery || !EntityPanelState.idColumn) {
        showNotification('Please complete the mapping', 'warning');
        return;
    }
    
    const classInfo = MappingState.loadedOntology?.classes?.find(c => c.uri === classUri);
    const classLabel = classInfo ? (classInfo.label || classInfo.name) : 'Unknown';
    
    const existingIndex = MappingState.config.entities.findIndex(m => m.ontology_class === classUri);
    
    const newMapping = {
        ontology_class: classUri,
        ontology_class_label: classLabel,
        sql_query: sqlQuery,
        id_column: EntityPanelState.idColumn,
        label_column: EntityPanelState.labelColumn,
        attribute_mappings: {...EntityPanelState.attributeMappings}
    };
    
    if (existingIndex >= 0) {
        MappingState.config.entities[existingIndex] = newMapping;
        showNotification(`Mapping for "${classLabel}" updated`, 'success', 2000);
    } else {
        MappingState.config.entities.push(newMapping);
        showNotification(`Mapping created for "${classLabel}"`, 'success', 2000);
    }
    
    autoSaveMappings();
    closeMappingPanel();
    refreshMappingDesign();
}

function saveRelPanelMapping() {
    const propertyUri = document.getElementById('panelPropertyUri')?.value;
    const sqlQueryRaw = document.getElementById('rpSqlQuery')?.value?.trim();
    const sqlQuery = stripLimitClause(sqlQueryRaw);
    
    if (!sqlQuery || !RelPanelState.sourceIdColumn || !RelPanelState.targetIdColumn) {
        showNotification('Please complete the mapping', 'warning');
        return;
    }
    
    const propertyInfo = MappingState.loadedOntology?.properties?.find(p => p.uri === propertyUri);
    const propertyLabel = propertyInfo ? (propertyInfo.label || propertyInfo.name) : 'Unknown';
    
    const sourceEntityId = propertyInfo?.sourceEntityId || propertyInfo?.domain || propertyInfo?.source || '';
    const targetEntityId = propertyInfo?.targetEntityId || propertyInfo?.range || propertyInfo?.target || '';
    
    const sourceClass = MappingState.loadedOntology?.classes?.find(c => 
        c.id === sourceEntityId || c.uri === sourceEntityId || c.name === sourceEntityId || c.localName === sourceEntityId
    );
    const targetClass = MappingState.loadedOntology?.classes?.find(c => 
        c.id === targetEntityId || c.uri === targetEntityId || c.name === targetEntityId || c.localName === targetEntityId
    );
    
    const sourceClassLabel = sourceClass ? (sourceClass.label || sourceClass.name || sourceClass.localName || '') : '';
    const targetClassLabel = targetClass ? (targetClass.label || targetClass.name || targetClass.localName || '') : '';
    const sourceClassUri = sourceClass?.uri || '';
    const targetClassUri = targetClass?.uri || '';
    
    const existingIndex = MappingState.config.relationships.findIndex(m => m.property === propertyUri);
    
    const newMapping = {
        property: propertyUri,
        property_label: propertyLabel,
        sql_query: sqlQuery,
        source_id_column: RelPanelState.sourceIdColumn,
        target_id_column: RelPanelState.targetIdColumn,
        source_class: sourceClassUri,
        source_class_label: sourceClassLabel,
        target_class: targetClassUri,
        target_class_label: targetClassLabel,
        attribute_mappings: {...RelPanelState.attributeMappings},
        direction: propertyInfo?.direction || 'forward'
    };
    
    if (existingIndex >= 0) {
        MappingState.config.relationships[existingIndex] = newMapping;
        showNotification(`Mapping for "${propertyLabel}" updated`, 'success', 2000);
    } else {
        MappingState.config.relationships.push(newMapping);
        showNotification(`Mapping created for "${propertyLabel}"`, 'success', 2000);
    }
    
    autoSaveMappings();
    closeMappingPanel();
    refreshMappingDesign();
}

// ==========================================================================
// RESET PANEL FUNCTIONALITY
// ==========================================================================

/**
 * Reset the panel: remove the mapping entirely (unmap the entity/relationship)
 */
function resetPanel() {
    if (currentPanelType === 'entity') {
        resetEntityPanel();
    } else if (currentPanelType === 'relationship') {
        resetRelPanel();
    }
}

/**
 * Reset Entity panel - removes the mapping entirely
 */
function resetEntityPanel() {
    const classUri = document.getElementById('panelEntityClass')?.value;
    if (!classUri) return;
    
    // Remove the mapping from state
    const existingIndex = MappingState.config.entities.findIndex(m => m.ontology_class === classUri);
    if (existingIndex >= 0) {
        const classInfo = MappingState.loadedOntology?.classes?.find(c => c.uri === classUri);
        const className = classInfo ? (classInfo.label || classInfo.name) : 'Entity';
        
        MappingState.config.entities.splice(existingIndex, 1);
        showNotification(`Mapping for "${className}" removed`, 'success', 2000);
        
        // Save and refresh
        autoSaveMappings();
    }
    
    // Clear SQL
    const sqlEl = document.getElementById('epSqlQuery');
    if (sqlEl) sqlEl.value = '';
    
    // Cancel pending autoLoad and invalidate in-flight queries
    if (EntityPanelState._autoLoadTimer) {
        clearTimeout(EntityPanelState._autoLoadTimer);
        EntityPanelState._autoLoadTimer = null;
    }
    EntityPanelState._generation++;

    // Reset state
    EntityPanelState.columns = [];
    EntityPanelState.rows = [];
    EntityPanelState.idColumn = null;
    EntityPanelState.labelColumn = null;
    EntityPanelState.attributeMappings = {};
    
    // Clear results table
    const headerEl = document.getElementById('epResultsHeader');
    const bodyEl = document.getElementById('epResultsBody');
    if (headerEl) headerEl.innerHTML = '';
    if (bodyEl) bodyEl.innerHTML = '';
    
    // Reset mapping summary
    const summaryIdEl = document.getElementById('epSummaryId');
    const summaryLabelEl = document.getElementById('epSummaryLabel');
    const summaryAttrsEl = document.getElementById('epSummaryAttrs');
    const summaryAttrListEl = document.getElementById('epSummaryAttrList');
    if (summaryIdEl) summaryIdEl.textContent = 'Not set';
    if (summaryLabelEl) summaryLabelEl.textContent = 'Not set';
    if (summaryAttrsEl) summaryAttrsEl.style.display = 'none';
    if (summaryAttrListEl) summaryAttrListEl.innerHTML = '';
    
    // Reset status
    const statusEl = document.getElementById('epMappingStatus');
    if (statusEl) statusEl.innerHTML = '<i class="bi bi-x-circle"></i> ID required';
    
    // Show summary view, hide grid
    const summaryView = document.getElementById('epMappingSummary');
    const gridView = document.getElementById('epMappingGrid');
    if (summaryView) summaryView.style.display = 'block';
    if (gridView) gridView.style.display = 'none';
    
    // Disable mapping tab
    const mappingTab = document.getElementById('ep-mapping-tab');
    if (mappingTab) mappingTab.disabled = true;
    
    // Switch to status tab
    const statusTab = document.getElementById('ep-status-tab');
    if (statusTab) bootstrap.Tab.getOrCreateInstance(statusTab).show();
    
    // Disable save button
    updateEntityPanelSaveBtn();
    
    // Close panel and refresh the map
    closeMappingPanel();
    refreshMappingDesign();
}

/**
 * Reset Relationship panel - removes the mapping entirely
 */
function resetRelPanel() {
    const propertyUri = document.getElementById('panelPropertyUri')?.value;
    if (!propertyUri) return;
    
    // Remove the mapping from state
    const existingIndex = MappingState.config.relationships.findIndex(m => m.property === propertyUri);
    if (existingIndex >= 0) {
        const propertyInfo = MappingState.loadedOntology?.properties?.find(p => p.uri === propertyUri);
        const propertyName = propertyInfo ? (propertyInfo.label || propertyInfo.name) : 'Relationship';
        
        MappingState.config.relationships.splice(existingIndex, 1);
        showNotification(`Mapping for "${propertyName}" removed`, 'success', 2000);
        
        // Save and refresh
        autoSaveMappings();
    }
    
    // Clear SQL
    const sqlEl = document.getElementById('rpSqlQuery');
    if (sqlEl) sqlEl.value = '';
    
    // Reset state
    RelPanelState.columns = [];
    RelPanelState.rows = [];
    RelPanelState.sourceIdColumn = null;
    RelPanelState.targetIdColumn = null;
    RelPanelState.attributeMappings = {};
    
    // Clear results table
    const headerEl = document.getElementById('rpResultsHeader');
    const bodyEl = document.getElementById('rpResultsBody');
    if (headerEl) headerEl.innerHTML = '';
    if (bodyEl) bodyEl.innerHTML = '';
    
    // Reset mapping summary
    const summarySourceEl = document.getElementById('rpSummarySource');
    const summaryTargetEl = document.getElementById('rpSummaryTarget');
    if (summarySourceEl) summarySourceEl.textContent = 'Not set';
    if (summaryTargetEl) summaryTargetEl.textContent = 'Not set';
    
    // Reset status
    const statusEl = document.getElementById('rpMappingStatus');
    if (statusEl) statusEl.innerHTML = '<i class="bi bi-x-circle"></i> Source & Target required';
    
    // Show summary view, hide grid
    const summaryView = document.getElementById('rpMappingSummary');
    const gridView = document.getElementById('rpMappingGrid');
    if (summaryView) summaryView.style.display = 'block';
    if (gridView) gridView.style.display = 'none';
    
    // Disable mapping tab
    const mappingTab = document.getElementById('rp-mapping-tab');
    if (mappingTab) mappingTab.disabled = true;
    
    // Switch to status tab
    const statusTab = document.getElementById('rp-status-tab');
    if (statusTab) bootstrap.Tab.getOrCreateInstance(statusTab).show();
    
    // Disable save button
    updateRelPanelSaveBtn();
    
    // Close panel and refresh the map
    closeMappingPanel();
    refreshMappingDesign();
}

// ==========================================================================
// AUTO-MAP FUNCTIONALITY  (async agent — fire-and-forget, URI-keyed saves)
// ==========================================================================

/**
 * Build the item payload for the single auto-map agent endpoint.
 */
function _buildAgentEntityItem(uri) {
    const lookupUri = uri || currentPanelUri;
    const classInfo = MappingState.loadedOntology?.classes?.find(c => c.uri === lookupUri);
    if (!classInfo) return null;
    const attributes = (classInfo.dataProperties || []).map(a => a.name || a.localName || a);
    return {
        uri: classInfo.uri,
        name: classInfo.label || classInfo.name || classInfo.localName,
        attributes: attributes
    };
}

function _buildAgentRelItem(uri) {
    const lookupUri = uri || currentPanelUri;
    const propInfo = MappingState.loadedOntology?.properties?.find(p => p.uri === lookupUri);
    if (!propInfo) return null;
    return {
        uri: propInfo.uri,
        name: propInfo.label || propInfo.name || propInfo.localName,
        domain: propInfo.domain,
        range: propInfo.range,
        direction: propInfo.direction || 'forward'
    };
}

const _SINGLE_POLL_MS = 1500;

/**
 * Start a single-item auto-map task and return the task_id (or null on error).
 */
async function _startSingleAutoAssign(itemType, item) {
    const response = await fetch('/mapping/auto-assign/single', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ type: itemType, item })
    });
    const data = await response.json();
    if (!data.success) {
        showNotification('Auto-map failed: ' + (data.error || 'Unknown error'), 'error');
        return null;
    }
    return data.task_id;
}

/**
 * Poll a task until it completes and return the task result (or null).
 */
async function _pollSingleTask(taskId) {
    while (true) {
        await new Promise(r => setTimeout(r, _SINGLE_POLL_MS));
        const resp = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
        const data = await resp.json();
        if (!data.success) throw new Error('Task not found');
        const task = data.task;
        if (task.status === 'completed') return task.result;
        if (task.status === 'failed') {
            showNotification('Auto-map failed: ' + (task.error || 'Unknown error'), 'error');
            return null;
        }
        if (task.status === 'cancelled') {
            showNotification('Auto-map was cancelled', 'warning');
            return null;
        }
    }
}

/**
 * Save an entity mapping result directly to MappingState.config (URI-keyed).
 * Does NOT depend on the panel — safe to call regardless of what is currently displayed.
 */
function _saveEntityAgentResult(targetUri, itemName, m) {
    const classInfo = MappingState.loadedOntology?.classes?.find(c => c.uri === targetUri);
    const classLabel = classInfo ? (classInfo.label || classInfo.name) : itemName;
    const mapping = {
        ontology_class: m.ontology_class || targetUri,
        ontology_class_label: m.class_name || classLabel,
        sql_query: m.sql_query,
        id_column: m.id_column,
        label_column: m.label_column,
        attribute_mappings: m.attribute_mappings || {}
    };
    const idx = MappingState.config.entities.findIndex(x => x.ontology_class === targetUri);
    if (idx >= 0) MappingState.config.entities[idx] = mapping;
    else MappingState.config.entities.push(mapping);
}

/**
 * Save a relationship mapping result directly to MappingState.config (URI-keyed).
 */
function _saveRelAgentResult(targetUri, itemName, m) {
    const propInfo = MappingState.loadedOntology?.properties?.find(p => p.uri === targetUri);
    const propLabel = propInfo ? (propInfo.label || propInfo.name) : itemName;
    const mapping = {
        property: m.property || targetUri,
        property_label: m.property_label || m.property_name || propLabel,
        sql_query: m.sql_query,
        source_id_column: m.source_id_column,
        target_id_column: m.target_id_column,
        source_class: m.source_class || '',
        source_class_label: m.source_class_label || '',
        target_class: m.target_class || '',
        target_class_label: m.target_class_label || '',
        attribute_mappings: m.attribute_mappings || {},
        direction: m.direction || propInfo?.direction || 'forward'
    };
    const idx = MappingState.config.relationships.findIndex(x => x.property === targetUri);
    if (idx >= 0) MappingState.config.relationships[idx] = mapping;
    else MappingState.config.relationships.push(mapping);
}

/**
 * Poll a task, save the result by URI, persist, and refresh the design map.
 * Runs in the background (not awaited by caller).
 */
async function _pollAndSaveResult(taskId, itemType, targetUri, itemName) {
    try {
        const result = await _pollSingleTask(taskId);
        if (!result || !result.mapping) return;

        if (itemType === 'entity') {
            _saveEntityAgentResult(targetUri, itemName, result.mapping);
        } else {
            _saveRelAgentResult(targetUri, itemName, result.mapping);
        }

        autoSaveMappings();
        refreshMappingDesign();

        showNotification(
            `Auto-mapped "${itemName}"` +
            (result.iterations ? ` (${result.iterations} iterations)` : ''),
            'success', 4000
        );

        if (currentPanelUri === targetUri) {
            closeMappingPanel();
        }
    } catch (error) {
        console.error('[Auto-Map] Poll/save error for ' + itemName + ':', error);
        showNotification(`Auto-map failed for "${itemName}": ` + error.message, 'error');
    }
}

/**
 * Auto-Map: Launch the auto-mapping agent and return immediately.
 * The result is polled and saved in the background — the user can keep working.
 */
async function autoMapPanel() {
    const targetUri = currentPanelUri;
    const targetType = currentPanelType;
    if (!targetUri || !targetType) return;

    const item = (targetType === 'entity')
        ? _buildAgentEntityItem(targetUri)
        : _buildAgentRelItem(targetUri);
    if (!item) { showNotification('Item not found in ontology', 'warning'); return; }

    const btn = document.getElementById('autoMapPanelBtn');
    const spinner = btn?.querySelector('.spinner-border');
    spinner?.classList.remove('d-none');
    if (btn) btn.disabled = true;

    try {
        const taskId = await _startSingleAutoAssign(targetType, item);
        if (!taskId) return;
        showNotification(`Agent is working on "${item.name}"…`, 'info', 15000);
        _pollAndSaveResult(taskId, targetType, targetUri, item.name);
    } catch (error) {
        console.error('[Auto-Map] Error:', error);
        showNotification('Auto-Map failed: ' + error.message, 'error');
    } finally {
        spinner?.classList.add('d-none');
        if (btn) btn.disabled = (window.isActiveVersion === false);
    }
}

// ==========================================================================
// MAPPING MAP CONTEXT MENU
// ==========================================================================

/**
 * Show context menu for entity or relationship in the mapping map
 */
function showMappingMapContextMenu(event, itemData, type, container) {
    // Remove existing menu
    hideMappingMapContextMenu();
    
    // Check if item is assigned
    const isAssigned = type === 'entity'
        ? MappingState.config.entities.some(m => m.ontology_class === itemData.uri && m.sql_query)
        : MappingState.config.relationships.some(m => m.property === itemData.uri && m.sql_query);
    
    const isExcluded = !!itemData.excluded;
    
    // Determine icon based on type
    const icon = type === 'entity' ? (itemData.icon || '📦') : '🔗';
    const displayName = itemData.label || itemData.name;
    
    // Create context menu
    const menu = document.createElement('div');
    menu.id = 'mappingMapContextMenu';
    menu.className = 'mapping-map-context-menu';
    menu.innerHTML = `
        <div class="mapping-map-context-header">
            <span class="mapping-map-context-icon">${icon}</span>
            <span class="mapping-map-context-title">${displayName}</span>
            ${isExcluded ? '<span class="badge bg-secondary ms-1" style="font-size:10px">Excluded</span>' : ''}
        </div>
        <div class="mapping-map-context-divider"></div>
        ${!isExcluded ? `
        <div class="mapping-map-context-item" data-action="auto-assign">
            <i class="bi bi-lightning-charge text-primary"></i>
            <span>Auto-Map</span>
        </div>
        ${isAssigned ? `
        <div class="mapping-map-context-item mapping-map-context-danger" data-action="unassign">
            <i class="bi bi-arrow-counterclockwise"></i>
            <span>Unmap</span>
        </div>
        ` : ''}
        <div class="mapping-map-context-divider"></div>
        <div class="mapping-map-context-item mapping-map-context-warning" data-action="exclude">
            <i class="bi bi-eye-slash"></i>
            <span>Exclude from Mapping</span>
        </div>
        ` : `
        <div class="mapping-map-context-item" data-action="include">
            <i class="bi bi-eye text-success"></i>
            <span>Include in Mapping</span>
        </div>
        `}
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
    
    // Handle menu item clicks based on type
    const autoAssignItem = menu.querySelector('[data-action="auto-assign"]');
    if (autoAssignItem) {
        autoAssignItem.addEventListener('click', async () => {
            hideMappingMapContextMenu();
            if (type === 'entity') {
                await contextMenuAutoAssignEntity(itemData);
            } else {
                await contextMenuAutoAssignRelationship(itemData);
            }
        });
    }
    
    const unassignItem = menu.querySelector('[data-action="unassign"]');
    if (unassignItem) {
        unassignItem.addEventListener('click', () => {
            hideMappingMapContextMenu();
            if (type === 'entity') {
                contextMenuUnassignEntity(itemData);
            } else {
                contextMenuUnassignRelationship(itemData);
            }
        });
    }
    
    const excludeItem = menu.querySelector('[data-action="exclude"]');
    if (excludeItem) {
        excludeItem.addEventListener('click', () => {
            hideMappingMapContextMenu();
            toggleEntityExclusion(itemData.uri, true, type);
        });
    }
    
    const includeItem = menu.querySelector('[data-action="include"]');
    if (includeItem) {
        includeItem.addEventListener('click', () => {
            hideMappingMapContextMenu();
            toggleEntityExclusion(itemData.uri, false, type);
        });
    }
    
    // Close menu when clicking outside
    setTimeout(() => {
        document.addEventListener('click', hideMappingMapContextMenuOnClickOutside);
        document.addEventListener('contextmenu', hideMappingMapContextMenuOnClickOutside);
    }, 0);
}

/**
 * Hide the mapping map context menu
 */
function hideMappingMapContextMenu() {
    const menu = document.getElementById('mappingMapContextMenu');
    if (menu) {
        menu.remove();
    }
    document.removeEventListener('click', hideMappingMapContextMenuOnClickOutside);
    document.removeEventListener('contextmenu', hideMappingMapContextMenuOnClickOutside);
}

/**
 * Handler to hide context menu when clicking outside
 */
function hideMappingMapContextMenuOnClickOutside(event) {
    const menu = document.getElementById('mappingMapContextMenu');
    if (menu && !menu.contains(event.target)) {
        hideMappingMapContextMenu();
    }
}

/**
 * Show canvas context menu (right-click on empty area)
 */
function showMappingMapCanvasContextMenu(event, container) {
    // Remove existing menu
    hideMappingMapContextMenu();
    
    // Create context menu
    const menu = document.createElement('div');
    menu.id = 'mappingMapContextMenu';
    menu.className = 'mapping-map-context-menu';
    menu.innerHTML = `
        <div class="mapping-map-context-header">
            <span class="mapping-map-context-icon">🗺️</span>
            <span class="mapping-map-context-title">Canvas</span>
        </div>
        <div class="mapping-map-context-divider"></div>
        <div class="mapping-map-context-item" data-action="zoom-in">
            <i class="bi bi-zoom-in text-secondary"></i>
            <span>Zoom In</span>
        </div>
        <div class="mapping-map-context-item" data-action="zoom-out">
            <i class="bi bi-zoom-out text-secondary"></i>
            <span>Zoom Out</span>
        </div>
        <div class="mapping-map-context-divider"></div>
        <div class="mapping-map-context-item" data-action="auto-layout">
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
    menu.querySelector('[data-action="zoom-in"]')?.addEventListener('click', () => {
        hideMappingMapContextMenu();
        if (mappingMapZoom) {
            const svg = d3.select(container).select('svg');
            svg.transition().duration(300).call(mappingMapZoom.scaleBy, 1.3);
        }
    });
    
    menu.querySelector('[data-action="zoom-out"]')?.addEventListener('click', () => {
        hideMappingMapContextMenu();
        if (mappingMapZoom) {
            const svg = d3.select(container).select('svg');
            svg.transition().duration(300).call(mappingMapZoom.scaleBy, 0.7);
        }
    });
    
    menu.querySelector('[data-action="auto-layout"]')?.addEventListener('click', () => {
        hideMappingMapContextMenu();
        // Trigger the reset layout button click
        document.getElementById('mappingMapResetLayout')?.click();
    });
    
    // Close menu when clicking outside
    setTimeout(() => {
        document.addEventListener('click', hideMappingMapContextMenuOnClickOutside);
        document.addEventListener('contextmenu', hideMappingMapContextMenuOnClickOutside);
    }, 0);
}

/**
 * Context menu action: Auto-Map Entity (uses the auto-mapping agent)
 */
async function contextMenuAutoAssignEntity(entityData) {
    const uri = entityData.uri;
    const label = entityData.label || entityData.name;
    const item = _buildAgentEntityItem(uri);
    if (!item) { showNotification('Entity not found in ontology', 'warning'); return; }

    try {
        const taskId = await _startSingleAutoAssign('entity', item);
        if (!taskId) return;
        showNotification(`Agent is auto-mapping "${label}"…`, 'info', 15000);
        _pollAndSaveResult(taskId, 'entity', uri, label);
    } catch (error) {
        console.error('[Context Auto-Assign] Error:', error);
        showNotification('Auto-Map failed: ' + error.message, 'error');
    }
}

/**
 * Context menu action: Unmap Entity
 */
function contextMenuUnassignEntity(entityData) {
    const uri = entityData.uri;
    const displayName = entityData.label || entityData.name;
    
    const existingIndex = MappingState.config.entities.findIndex(m => m.ontology_class === uri);
    if (existingIndex >= 0) {
        MappingState.config.entities.splice(existingIndex, 1);
        showNotification(`Mapping for "${displayName}" removed`, 'success', 2000);
        autoSaveMappings();
        refreshMappingDesign();
    } else {
        showNotification(`No mapping to remove for "${displayName}"`, 'info', 1500);
    }
}

/**
 * Context menu action: Auto-Map Relationship (uses the auto-mapping agent)
 */
async function contextMenuAutoAssignRelationship(relData) {
    const uri = relData.uri;
    const label = relData.label || relData.name;
    const item = _buildAgentRelItem(uri);
    if (!item) { showNotification('Relationship not found in ontology', 'warning'); return; }

    try {
        const taskId = await _startSingleAutoAssign('relationship', item);
        if (!taskId) return;
        showNotification(`Agent is auto-mapping "${label}"…`, 'info', 15000);
        _pollAndSaveResult(taskId, 'relationship', uri, label);
    } catch (error) {
        console.error('[Context Auto-Assign Relationship] Error:', error);
        showNotification('Auto-Map failed: ' + error.message, 'error');
    }
}

/**
 * Context menu action: Unmap Relationship
 */
function contextMenuUnassignRelationship(relData) {
    const uri = relData.uri;
    const displayName = relData.label || relData.name;
    
    const existingIndex = MappingState.config.relationships.findIndex(m => m.property === uri);
    if (existingIndex >= 0) {
        MappingState.config.relationships.splice(existingIndex, 1);
        showNotification(`Mapping for "${displayName}" removed`, 'success', 2000);
        autoSaveMappings();
        refreshMappingDesign();
    } else {
        showNotification(`No mapping to remove for "${displayName}"`, 'info', 1500);
    }
}

/**
 * Toggle exclusion of an entity or relationship via backend and refresh the map.
 */
async function toggleEntityExclusion(uri, excluded, itemType) {
    try {
        const response = await fetch('/mapping/exclude', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                uris: [uri],
                excluded: excluded,
                item_type: itemType
            }),
            credentials: 'same-origin'
        });
        const result = await response.json();
        if (!result.success) {
            showNotification('Error toggling exclusion: ' + (result.message || ''), 'error');
            return;
        }

        // Update the local mapping entry
        const entries = itemType === 'entity'
            ? (MappingState.config.entities || [])
            : (MappingState.config.relationships || []);
        const key = itemType === 'entity' ? 'ontology_class' : 'property';
        let entry = entries.find(m => m[key] === uri);
        if (entry) {
            if (excluded) { entry.excluded = true; } else { delete entry.excluded; }
        } else if (excluded) {
            entries.push({ [key]: uri, excluded: true });
        }

        // Stamp excluded flag onto ontology object so UI reads work
        const collection = itemType === 'entity'
            ? (MappingState.loadedOntology?.classes || [])
            : (MappingState.loadedOntology?.properties || []);
        const item = collection.find(c => c.uri === uri);
        if (item) item.excluded = excluded;

        showNotification(
            excluded ? `"${item?.label || item?.name || uri}" excluded from mapping` : `"${item?.label || item?.name || uri}" included in mapping`,
            'success', 2000
        );

        refreshMappingDesign();
        if (typeof updateMappingCompletionStatus === 'function') updateMappingCompletionStatus();
    } catch (error) {
        showNotification('Error: ' + error.message, 'error');
    }
}

// Expose globally so the status tab can call it
window.toggleEntityExclusion = toggleEntityExclusion;

// Initialize panel close/save buttons
document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('closePanelBtn')?.addEventListener('click', closeMappingPanel);
    document.getElementById('cancelPanelBtn')?.addEventListener('click', closeMappingPanel);
    document.getElementById('savePanelBtn')?.addEventListener('click', savePanelMapping);
    document.getElementById('autoMapPanelBtn')?.addEventListener('click', autoMapPanel);
    document.getElementById('resetPanelBtn')?.addEventListener('click', resetPanel);
    
    const panel = document.getElementById('mappingRightPanel');
    if (panel) {
        const eventsToBlock = ['click', 'mousedown', 'mouseup', 'dblclick', 'pointerdown', 'pointerup', 'focusin', 'focusout'];
        eventsToBlock.forEach(eventType => {
            panel.addEventListener(eventType, function(e) {
                e.stopPropagation();
            });
        });
    }
});

// Expose functions globally
window.initMappingDesigner = initMappingDesigner;
window.loadOntologyIntoMappingDesigner = loadOntologyIntoMappingDesigner;
window.refreshMappingDesign = refreshMappingDesign;
window.resizeMapSvg = resizeMapSvg;

// Handle window resize
window.addEventListener('resize', function() {
    if (mappingMapInitialized) {
        resizeMapSvg();
    }
});
