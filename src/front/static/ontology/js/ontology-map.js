/**
 * OntoBricks - ontology-map.js
 * Extracted from ontology templates per code_instructions.txt
 */

// ONTOLOGY MAP - D3.js Force-Directed Graph
// =====================================================

let ontologyMapInitialized = false;
let ontologyMapSvg = null;
let ontologyMapSimulation = null;
let ontologyMapZoom = null;
let mapAutoSaveTimeout = null;
let mapConnectionMode = null; // { sourceEntity: {...}, lineElement: <line>, type: 'relationship'|'inheritance' }
let _mapInitGeneration = 0;   // guards against concurrent initOntologyMap() calls

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
 * Save map layout to domain session
 */
async function saveMapLayout(nodes) {
    try {
        const positions = {};
        nodes.forEach(node => {
            positions[node.id] = {
                x: node.x,
                y: node.y,
                fx: node.fx,
                fy: node.fy
            };
        });
        
        await fetch('/domain/map-layout', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ positions }),
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
 * Initialize the Ontology Designer visualization
 */
async function initOntologyMap() {
    // Increment generation counter to cancel any previous in-flight init
    const thisGeneration = ++_mapInitGeneration;

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
        ontologyMapInitialized = true;
        return;
    }

    // If a previous icon-assignment task is still running, resume monitoring
    // so the user sees the result land without clicking the button again.
    if (typeof checkAndResumeIconsTask === 'function') {
        checkAndResumeIconsTask();
    }

    // Load saved layout
    const savedLayout = await loadMapLayout();

    // If a newer init was triggered while we were loading layout, abort
    if (thisGeneration !== _mapInitGeneration) {
        console.log('[Map] Superseded by newer init call, aborting');
        return;
    }

    // Build nodes from classes
    const nodes = classes.map((cls, idx) => {
        const savedPos = savedLayout?.positions?.[cls.name];
        const x = savedPos?.x ?? (100 + (idx % 5) * 150);
        const y = savedPos?.y ?? (100 + Math.floor(idx / 5) * 120);
        return {
            id: cls.name,
            name: cls.name,
            label: cls.label || cls.name,
            icon: cls.emoji || OntologyState.defaultClassEmoji || '📦',
            parent: cls.parent,
            // Use saved position if available, fix positions to prevent animation
            x: x,
            y: y,
            fx: savedPos ? x : null,
            fy: savedPos ? y : null
        };
    });

    // Build links from properties (relationships) and inheritance
    const links = [];
    
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
    
    // Add relationship links (only if both domain and range exist as classes)
    let mapSkipped = 0;
    properties.forEach(prop => {
        if (prop.domain && prop.range) {
            const source = resolveNodeId(prop.domain);
            const target = resolveNodeId(prop.range);
            if (source && target) {
                links.push({
                    source: source,
                    target: target,
                    name: prop.name,
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
    
    console.log(`[Map] Built ${links.length} valid links from ${properties.length} properties`);

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

    // Add zoom behavior
    ontologyMapZoom = d3.zoom()
        .scaleExtent([0.3, 3])
        .on('zoom', (event) => {
            g.attr('transform', event.transform);
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
    function dragStarted(event, d) {
        d.fx = d.x;
        d.fy = d.y;
    }

    function dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
        // Immediately update positions
        ontologyMapSimulation.alpha(0.01).restart();
    }

    function dragEnded(event, d) {
        // Keep fixed position and save
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
        .attr('class', 'map-link');

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
        .attr('class', 'map-link self-loop');

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
            
            // Highlight clicked hitarea
            d3.selectAll('.map-link-hitarea')
                .attr('fill', '#e9ecef')
                .attr('stroke', '#999')
                .attr('stroke-width', 1.5);
            d3.select(this)
                .attr('fill', '#dee2e6')
                .attr('stroke', '#495057')
                .attr('stroke-width', 2.5);
            
            // Clear entity selection
            d3.selectAll('.map-node').classed('selected', false);
            
            // Show floating delete button near the hitarea (only in edit mode)
            if (window.isActiveVersion !== false) {
                showMapRelationshipActions(d, this);
            }
            
            // Open relationship edit panel (using shared panel)
            if (typeof editPropertyByName === 'function') {
                editPropertyByName(d.name);
            }
        });

    // Draw relationship labels
    const linkLabels = g.append('g')
        .selectAll('text')
        .data(links.filter(l => l.type === 'relationship'))
        .enter()
        .append('text')
        .attr('class', 'map-link-label')
        .text(d => d.name);

    // Track if we're dragging to prevent click after drag
    let isDragging = false;
    
    // Draw nodes (icon only, no circles - matching Query visualization)
    const nodeElements = g.append('g')
        .selectAll('g')
        .data(nodes)
        .enter()
        .append('g')
        .attr('class', 'map-node')
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

    // Invisible hitarea circle for better click detection
    nodeElements.append('circle')
        .attr('class', 'map-node-hitarea')
        .attr('r', 25);

    // Emoji icon (centered via CSS dominant-baseline: central)
    nodeElements.append('text')
        .attr('class', 'map-node-icon')
        .text(d => d.icon);

    // Node label below the icon
    nodeElements.append('text')
        .attr('class', 'map-node-label')
        .attr('dy', 35)
        .text(d => d.name);

    // Tooltip on hover
    nodeElements.append('title')
        .text(d => d.name);

    // Click to edit entity (only if not dragging)
    nodeElements.on('click', function(event, d) {
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
        
        // Highlight selected node
        d3.selectAll('.map-node').classed('selected', false);
        d3.select(this).classed('selected', true);
        
        // Open entity edit panel (using shared panel)
        if (typeof editClassByName === 'function') {
            editClassByName(d.name);
        }
    });
    
    // Right-click context menu for entities (suppressed in view mode)
    nodeElements.on('contextmenu', function(event, d) {
        event.preventDefault();
        event.stopPropagation();

        if (window.isActiveVersion === false) return;
        
        // Highlight selected node
        d3.selectAll('.map-node').classed('selected', false);
        d3.select(this).classed('selected', true);
        
        // Show context menu
        showMapContextMenu(event, d, container);
    });
    
    // Hide context menu when clicking on SVG background
    svg.on('click', function() {
        hideMapContextMenu();
        hideMapRelationshipActions();
        d3.selectAll('.map-node').classed('selected', false);
        d3.selectAll('.map-link-hitarea')
            .attr('fill', '#e9ecef')
            .attr('stroke', '#999')
            .attr('stroke-width', 1.5);
    });
    
    svg.on('contextmenu', function(event) {
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
                
                // Perpendicular direction
                const dx = tx - sx;
                const dy = ty - sy;
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
                const dx = (typeof d.target === 'object' ? d.target.x : nodes.find(n => n.id === d.target)?.x || 0) - 
                           (typeof d.source === 'object' ? d.source.x : nodes.find(n => n.id === d.source)?.x || 0);
                const dy = ty - sy;
                const len = Math.sqrt(dx * dx + dy * dy) || 1;
                const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                return my + (dx / len) * offset;
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
                // Curved link
                mx = (sx + tx) / 2;
                my = (sy + ty) / 2;
                const dx = tx - sx;
                const dy = ty - sy;
                const len = Math.sqrt(dx * dx + dy * dy) || 1;
                const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                mx += (-dy / len) * offset;
                my += (dx / len) * offset;
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
            <div class="map-legend-line inheritance"></div>
            <span>Inheritance</span>
        </div>
    `;
    container.appendChild(legend);

    ontologyMapInitialized = true;

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
 * Show context menu for entity in the map
 */
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
        <div class="map-context-item" data-action="create-relationship">
            <i class="bi bi-arrow-right"></i>
            <span>Create Relationship</span>
        </div>
        <div class="map-context-item" data-action="inherit-from">
            <i class="bi bi-diagram-3"></i>
            <span>Inherit from...</span>
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
    // Get the SVG element
    const svg = container.querySelector('svg');
    if (!svg) return;
    
    // Get the main group (g element with transform)
    const mainGroup = svg.querySelector('g');
    if (!mainGroup) return;
    
    // Create a temporary line for visual feedback
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('class', type === 'inheritance' ? 'map-connection-line map-inheritance-line' : 'map-connection-line');
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
    
    const svg = mapConnectionMode.svg;
    const mainGroup = mapConnectionMode.mainGroup;
    const line = mapConnectionMode.lineElement;
    
    // Get current transform of main group
    const transform = mainGroup.getAttribute('transform');
    let translateX = 0, translateY = 0, scale = 1;
    
    if (transform) {
        const translateMatch = transform.match(/translate\(([^,]+),\s*([^)]+)\)/);
        if (translateMatch) {
            translateX = parseFloat(translateMatch[1]);
            translateY = parseFloat(translateMatch[2]);
        }
        const scaleMatch = transform.match(/scale\(([^)]+)\)/);
        if (scaleMatch) {
            scale = parseFloat(scaleMatch[1]);
        }
    }
    
    // Calculate mouse position in SVG coordinates
    const rect = svg.getBoundingClientRect();
    const mouseX = (event.clientX - rect.left - translateX) / scale;
    const mouseY = (event.clientY - rect.top - translateY) / scale;
    
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
    // Show dialog to get relationship name
    const relationshipName = await showMapRelationshipDialog(sourceEntity, targetEntity);
    
    if (!relationshipName) return; // Cancelled
    
    try {
        if (typeof OntologyState !== 'undefined' && OntologyState.config) {
            // Check if relationship already exists
            const existingProp = OntologyState.config.properties.find(p => 
                p.name === relationshipName && p.domain === sourceEntity.name && p.range === targetEntity.name
            );
            
            if (existingProp) {
                showNotification(`Relationship "${relationshipName}" already exists`, 'warning');
                return;
            }
            
            // Create new property/relationship
            const newProperty = {
                name: relationshipName,
                localName: relationshipName,
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
 * Auto-map icons to all entities that still have the default icon.
 * Starts the icon agent as a background task, persists task_id to
 * sessionStorage, and polls /tasks/{id} for the result.
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

    const defaultEmoji = OntologyState.defaultClassEmoji || '📦';
    const candidates = classes.filter(cls => (cls.emoji || defaultEmoji) === defaultEmoji);
    if (candidates.length === 0) {
        if (typeof showNotification === 'function') {
            showNotification('All entities already have custom icons', 'info', 2000);
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
                                <label class="form-label">Relationship Name</label>
                                <input type="text" class="form-control" id="mapRelationshipName" 
                                       placeholder="e.g., hasRelation, belongsTo, contains" autofocus>
                                <div class="form-text">Use camelCase naming convention (e.g., hasCustomer, belongsTo)</div>
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
        const nameInput = document.getElementById('mapRelationshipName');
        
        let resolved = false;
        
        // Handle create button
        document.getElementById('mapRelationshipCreate').addEventListener('click', () => {
            const name = nameInput.value.trim();
            if (!name) {
                nameInput.classList.add('is-invalid');
                return;
            }
            resolved = true;
            modal.hide();
            resolve(name);
        });
        
        // Handle enter key in input
        nameInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                document.getElementById('mapRelationshipCreate').click();
            }
            nameInput.classList.remove('is-invalid');
        });
        
        // Handle modal close
        modalEl.addEventListener('hidden.bs.modal', () => {
            modalEl.remove();
            if (!resolved) {
                resolve(null);
            }
        }, { once: true });
        
        modal.show();
        nameInput.focus();
    });
}
