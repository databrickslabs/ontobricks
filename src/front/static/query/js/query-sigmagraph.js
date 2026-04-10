/**
 * OntoBricks - query-sigmagraph.js
 * Sigma.js + Graphology graph view for the Digital Twin section.
 * Reuses the same data (lastQueryResults / d3NodesData / d3LinksData) built by query.js.
 */

// Resize handle for sigma.js details panel
document.addEventListener('DOMContentLoaded', function () {
    (function () {
        var handle = document.getElementById('sgResizeHandle');
        var panel = document.getElementById('sgDetailsPanel');
        var layout = document.getElementById('sgLayout');
        if (!handle || !panel || !layout) return;
        var resizing = false, startX, startW;
        handle.addEventListener('mousedown', function (e) {
            resizing = true; startX = e.clientX; startW = panel.offsetWidth;
            handle.classList.add('active');
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            e.preventDefault();
        });
        document.addEventListener('mousemove', function (e) {
            if (!resizing) return;
            panel.style.width = Math.max(200, Math.min(600, startW + (startX - e.clientX))) + 'px';
        });
        document.addEventListener('mouseup', function () {
            if (resizing) { resizing = false; handle.classList.remove('active'); document.body.style.cursor = ''; document.body.style.userSelect = ''; }
        });
    })();
});

var SigmaGraph = (function () {
    var _renderer = null;
    var _graph = null;
    var _hoveredNode = null;
    var _selectedNode = null;
    var _visibleTypes = new Set();
    var _visibleEdgeTypes = new Set();
    var _searchMatched = null;   // null = no search active; Set of directly matched node IDs
    var _searchNeighbors = null; // Set of neighbor node IDs of matched nodes
    var _highlightedSeeds = null; // Set of node IDs to visually emphasize (ring effect)
    var _pendingHighlightTerm = null; // term to auto-highlight after next filter execution
    var _graphFilterActive = false;
    var _initialized = false;
    var _cachedStats = null;
    var _libsRequested = false;

    var _GRAPH_LIB_URLS = [
        'https://d3js.org/d3.v7.min.js',
        'https://cdnjs.cloudflare.com/ajax/libs/graphology/0.26.0/graphology.umd.min.js',
        'https://cdn.jsdelivr.net/npm/graphology-library@0.8.0/dist/graphology-library.min.js',
        'https://cdnjs.cloudflare.com/ajax/libs/sigma.js/3.0.2/sigma.min.js'
    ];

    function _loadGraphLibs() {
        if (_libsRequested) return;
        _libsRequested = true;
        _GRAPH_LIB_URLS.forEach(function (src) {
            var s = document.createElement('script');
            s.src = src;
            document.head.appendChild(s);
        });
    }

    function _waitForGraphLibs(maxMs) {
        return new Promise(function (resolve) {
            if (typeof Sigma !== 'undefined' && typeof graphology !== 'undefined') { resolve(true); return; }
            var t0 = Date.now();
            var iv = setInterval(function () {
                if (typeof Sigma !== 'undefined' && typeof graphology !== 'undefined') { clearInterval(iv); resolve(true); }
                else if (Date.now() - t0 > maxMs) { clearInterval(iv); resolve(false); }
            }, 80);
        });
    }

    var TYPE_COLORS = [
        '#FF3621', '#6366F1', '#4ECDC4', '#F59E0B', '#EC4899',
        '#10B981', '#8B5CF6', '#F97316', '#06B6D4', '#EF4444',
        '#84CC16', '#14B8A6', '#A855F7', '#E11D48', '#0EA5E9'
    ];
    var _typeColorMap = {};

    function _colorForType(type) {
        if (!type) return '#6c757d';
        if (_typeColorMap[type]) return _typeColorMap[type];
        var idx = Object.keys(_typeColorMap).length % TYPE_COLORS.length;
        _typeColorMap[type] = TYPE_COLORS[idx];
        return _typeColorMap[type];
    }

    function _iconForType(type) {
        if (typeof getEntityIconByType === 'function') return getEntityIconByType(type);
        if (typeof taxonomyIcons !== 'undefined' && type) {
            var t = type.toLowerCase();
            if (taxonomyIcons[t]) return taxonomyIcons[t];
            var local = _extractLocalName(type).toLowerCase();
            if (taxonomyIcons[local]) return taxonomyIcons[local];
        }
        return '📦';
    }

    function _extractLocalName(uri) { return extractLocalName(uri); }

    // -----------------------------------------------------------
    // Build graphology graph from the data already parsed by query.js
    // -----------------------------------------------------------
    function _buildGraph(filterIds) {
        var GraphClass;
        if (typeof graphology !== 'undefined') {
            GraphClass = graphology.Graph || (typeof graphology === 'function' ? graphology : null);
        }
        if (!GraphClass) { console.error('[SigmaGraph] graphology.Graph not found'); return null; }

        var graph;
        try {
            graph = new GraphClass({ multi: true, type: 'directed' });
        } catch (err) {
            console.error('[SigmaGraph] Graph constructor error:', err);
            return null;
        }

        var nodes = (typeof d3NodesData !== 'undefined' && d3NodesData) ? d3NodesData : [];
        var links = (typeof d3LinksData !== 'undefined' && d3LinksData) ? d3LinksData : [];

        console.log('[SigmaGraph] _buildGraph: d3NodesData=' + nodes.length + ', d3LinksData=' + links.length);
        if (nodes.length === 0) return null;

        var hideOrphans = document.getElementById('sgHideOrphans')?.checked || false;
        var connectedIds = new Set();
        if (hideOrphans) {
            links.forEach(function (l) {
                var s = typeof l.source === 'object' ? l.source.id : l.source;
                var t = typeof l.target === 'object' ? l.target.id : l.target;
                connectedIds.add(s);
                connectedIds.add(t);
            });
        }

        _typeColorMap = {};
        _visibleTypes = new Set();
        _visibleEdgeTypes = new Set();
        var addedNodes = new Set();

        nodes.forEach(function (n) {
            if (!n || !n.id) return;
            if (filterIds && !filterIds.has(n.id)) return;
            if (hideOrphans && !connectedIds.has(n.id)) return;
            if (addedNodes.has(n.id)) return;
            var entityType = n.type || 'Unknown';
            _visibleTypes.add(entityType);
            var icon = _iconForType(entityType);
            var rawLabel = n.label || _extractLocalName(n.id);
            try {
                graph.addNode(n.id, {
                    label: icon + ' ' + rawLabel,
                    entityType: entityType,
                    icon: icon,
                    color: _colorForType(entityType),
                    size: 6,
                    _data: n
                });
                addedNodes.add(n.id);
            } catch (_) {}
        });

        links.forEach(function (l) {
            var s = typeof l.source === 'object' ? l.source.id : l.source;
            var t = typeof l.target === 'object' ? l.target.id : l.target;
            if (!graph.hasNode(s) || !graph.hasNode(t)) return;
            var pred = l.predicate || '';
            _visibleEdgeTypes.add(pred);
            var isInferred = !!(l.provenance || l.inferred);
            var edgeColor = isInferred ? '#4ECDC4' : '#bbb';
            var edgeSize = isInferred ? 2.5 : 1.5;
            try {
                graph.addEdge(s, t, {
                    label: isInferred ? pred + ' [inferred]' : pred,
                    size: edgeSize,
                    color: edgeColor,
                    type: isInferred ? 'dashed' : undefined,
                    _data: l,
                    _inferred: isInferred,
                    _provenance: l.provenance || ''
                });
            } catch (_) {}
        });

        console.log('[SigmaGraph] graphology result: ' + graph.order + ' nodes, ' + graph.size + ' edges');
        return graph;
    }

    // -----------------------------------------------------------
    // ForceAtlas2 layout
    // -----------------------------------------------------------
    function _applyLayout(graph) {
        if (!graph || graph.order === 0) return;

        // Random initial positions
        graph.forEachNode(function (node) {
            graph.setNodeAttribute(node, 'x', Math.random() * 1000);
            graph.setNodeAttribute(node, 'y', Math.random() * 1000);
        });

        var fa2 = (typeof graphologyLibrary !== 'undefined' && graphologyLibrary.layoutForceAtlas2)
            ? graphologyLibrary.layoutForceAtlas2
            : (typeof ForceAtlas2 !== 'undefined' ? ForceAtlas2 : null);

        if (fa2 && fa2.assign) {
            fa2.assign(graph, {
                iterations: 100,
                settings: {
                    gravity: 1,
                    scalingRatio: 10,
                    barnesHutOptimize: graph.order > 500,
                    strongGravityMode: true,
                    slowDown: 5
                }
            });
        }
    }

    // -----------------------------------------------------------
    // Render / Re-render
    // -----------------------------------------------------------
    function _hideLoading() {
        var loading = document.getElementById('sgLoading');
        if (loading) loading.style.display = 'none';
    }

    function _render(filterIds) {
        var container = document.getElementById('sgContainer');
        var loading = document.getElementById('sgLoading');
        if (!container) { console.warn('[SigmaGraph] container #sgContainer not found'); _hideLoading(); return; }
        _hideEmptyState();

        var SigmaModule = (typeof Sigma !== 'undefined') ? Sigma : null;
        if (!SigmaModule) { console.error('[SigmaGraph] Sigma library not loaded'); _hideLoading(); return; }
        var SigmaClass = (typeof SigmaModule === 'function') ? SigmaModule : (SigmaModule.Sigma || SigmaModule.default || null);
        if (!SigmaClass) { console.error('[SigmaGraph] Could not find Sigma constructor in', Object.keys(SigmaModule)); _hideLoading(); return; }

        if (_renderer) {
            try { _renderer.kill(); } catch (_) {}
            _renderer = null;
        }

        _graph = _buildGraph(filterIds);
        if (!_graph || _graph.order === 0) {
            console.warn('[SigmaGraph] graph is empty (0 nodes)');
            _hideLoading();
            return;
        }

        console.log('[SigmaGraph] graph built:', _graph.order, 'nodes,', _graph.size, 'edges');

        _applyLayout(_graph);

        if (loading) loading.style.display = 'none';

        // Ensure container has actual dimensions
        var rect = container.getBoundingClientRect();
        console.log('[SigmaGraph] container size:', rect.width, 'x', rect.height);
        if (rect.width < 10 || rect.height < 10) {
            console.warn('[SigmaGraph] container too small, deferring render');
            setTimeout(function () { _render(filterIds); }, 300);
            return;
        }

        var sigmaSettings = {
            renderLabels: document.getElementById('sgShowLabels')?.checked !== false,
            renderEdgeLabels: document.getElementById('sgShowEdgeLabels')?.checked !== false,
            labelSize: 12,
            labelColor: { color: '#333' },
            edgeLabelSize: 10,
            edgeLabelColor: { color: '#666' },
            nodeReducer: _nodeReducer,
            edgeReducer: _edgeReducer,
            enableEdgeEvents: true,
            allowInvalidContainer: true
        };

        // Register arrow edge program if available (sigma v3: Sigma.rendering.EdgeArrowProgram)
        var rendering = SigmaModule.rendering || {};
        if (rendering.EdgeArrowProgram) {
            sigmaSettings.defaultEdgeType = 'arrow';
            sigmaSettings.edgeProgramClasses = { arrow: rendering.EdgeArrowProgram };
        }

        try {
            _renderer = new SigmaClass(_graph, container, sigmaSettings);
        } catch (err) {
            console.error('[SigmaGraph] Sigma constructor error:', err);
            _hideLoading();
            return;
        }

        _renderer.on('clickNode', function (e) {
            _searchMatched = null;
            _searchNeighbors = null;
            _selectedNode = e.node;
            _hoveredNode = null;
            _showNodeDetails(e.node);
            _renderer.refresh();
        });

        _renderer.on('clickStage', function () {
            _searchMatched = null;
            _searchNeighbors = null;
            _selectedNode = null;
            _hoveredNode = null;
            _showPlaceholder();
            _renderer.refresh();
        });

        _renderer.on('enterNode', function (e) {
            if (!_selectedNode) {
                _hoveredNode = e.node;
                _renderer.refresh();
            }
        });

        _renderer.on('leaveNode', function () {
            if (!_selectedNode) {
                _hoveredNode = null;
                _renderer.refresh();
            }
        });

        _renderer.on('clickEdge', function (e) {
            _selectedNode = null;
            _hoveredNode = null;
            _showEdgeDetails(e.edge);
            _renderer.refresh();
        });

        _updateStats();
        _populateTypes();
        _populateSearchTypes();
        console.log('[SigmaGraph] render complete');
    }

    // -----------------------------------------------------------
    // Camera helpers
    // -----------------------------------------------------------
    function _focusCameraOnNodes(nodeSet) {
        if (!_renderer || !_graph || !nodeSet || nodeSet.size === 0) return;

        // Camera state uses framedGraph coordinates (normalized 0-1 space).
        // Convert: graph attributes -> viewport pixels -> framedGraph.
        function toFramedGraph(graphPos) {
            var vp = _renderer.graphToViewport(graphPos);
            return _renderer.viewportToFramedGraph(vp);
        }

        var fgPositions = [];
        nodeSet.forEach(function (n) {
            var attrs = _graph.getNodeAttributes(n);
            if (attrs && attrs.x !== undefined && attrs.y !== undefined) {
                fgPositions.push(toFramedGraph({ x: attrs.x, y: attrs.y }));
            }
        });
        if (fgPositions.length === 0) return;

        var cam = _renderer.getCamera();

        if (fgPositions.length === 1) {
            cam.animate({ x: fgPositions[0].x, y: fgPositions[0].y, ratio: 0.08 }, { duration: 400 });
            return;
        }

        var minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
        fgPositions.forEach(function (p) {
            if (p.x < minX) minX = p.x;
            if (p.x > maxX) maxX = p.x;
            if (p.y < minY) minY = p.y;
            if (p.y > maxY) maxY = p.y;
        });

        var centerX = (minX + maxX) / 2;
        var centerY = (minY + maxY) / 2;
        var spanX = (maxX - minX) || 0.001;
        var spanY = (maxY - minY) || 0.001;
        var ratio = Math.max(spanX, spanY) * 1.5;
        ratio = Math.min(1, Math.max(0.02, ratio));

        cam.animate({ x: centerX, y: centerY, ratio: ratio }, { duration: 400 });
    }

    // -----------------------------------------------------------
    // Node / Edge reducers (highlighting, filtering)
    // -----------------------------------------------------------
    function _nodeReducer(node, data) {
        var res = Object.assign({}, data);

        // Type filter
        if (_visibleTypes.size > 0 && !_visibleTypes.has(data.entityType)) {
            res.hidden = true;
            return res;
        }

        if (_highlightedSeeds && _highlightedSeeds.has(node)) {
            res.highlighted = true;
            res.size = (data.size || 6) * 1.5;
            res.borderColor = '#FF3621';
            res.borderSize = 3;
            res.labelSize = 16;
            res.labelWeight = 'bold';
            res.forceLabel = true;
            res.zIndex = 10;
        }

        // Search filter: matched nodes get highlighted, neighbors stay visible, rest dimmed
        if (_searchMatched !== null) {
            if (_searchMatched.has(node)) {
                res.highlighted = true;
                res.size = data.size * 1.6;
            } else if (_searchNeighbors && _searchNeighbors.has(node)) {
                res.highlighted = true;
            } else {
                res.color = '#e0e0e0';
                res.label = '';
                res.size = 3;
            }
            return res;
        }

        // Determine the focus node (selected takes priority, then hovered)
        var focusNode = _selectedNode || _hoveredNode;

        if (focusNode && _graph) {
            if (node === focusNode) {
                res.highlighted = true;
                res.size = data.size * 1.6;
            } else if (_graph.hasEdge(focusNode, node) || _graph.hasEdge(node, focusNode)) {
                res.highlighted = true;
            } else {
                res.color = '#e0e0e0';
                res.label = '';
            }
        }

        return res;
    }

    function _edgeReducer(edge, data) {
        var res = Object.assign({}, data);
        var pred = data.label || '';

        // Edge type filter
        if (_visibleEdgeTypes.size > 0 && pred && !_visibleEdgeTypes.has(pred)) {
            res.hidden = true;
            return res;
        }

        // Search: show edges connected to matched nodes, dim the rest
        if (_searchMatched !== null) {
            var src = _graph.source(edge);
            var tgt = _graph.target(edge);
            var srcVisible = _searchMatched.has(src) || (_searchNeighbors && _searchNeighbors.has(src));
            var tgtVisible = _searchMatched.has(tgt) || (_searchNeighbors && _searchNeighbors.has(tgt));
            var srcIsMatch = _searchMatched.has(src);
            var tgtIsMatch = _searchMatched.has(tgt);
            if ((srcIsMatch || tgtIsMatch) && srcVisible && tgtVisible) {
                res.color = '#333';
                res.size = 2;
            } else {
                res.color = '#f0f0f0';
                res.label = '';
            }
            return res;
        }

        // Focus highlighting (selected or hovered node)
        var focusNode = _selectedNode || _hoveredNode;
        if (focusNode && _graph) {
            var source = _graph.source(edge);
            var target = _graph.target(edge);
            if (source !== focusNode && target !== focusNode) {
                res.color = '#f0f0f0';
                res.label = '';
            } else {
                res.color = '#333';
                res.size = 2.5;
            }
        }

        return res;
    }

    // -----------------------------------------------------------
    // Details Panel
    // -----------------------------------------------------------
    function _showPlaceholder() {
        var el = document.getElementById('sgDetailsContent');
        if (!el) return;
        el.innerHTML = '<div class="entity-details-placeholder"><i class="bi bi-cursor"></i><p class="small mb-0">Click on an entity or<br>relationship to view details</p></div>';
    }

    async function _showNodeDetails(nodeId) {
        var el = document.getElementById('sgDetailsContent');
        if (!el || !_graph) return;
        var attrs = _graph.getNodeAttributes(nodeId);
        var entity = attrs._data || {};

        if (typeof entityMappings !== 'undefined' && Object.keys(entityMappings).length === 0 && typeof loadEntityMappings === 'function') {
            await loadEntityMappings();
        }

        var esc = (typeof escapeHtml === 'function') ? escapeHtml : _esc;
        var truncUri = (typeof truncateUri === 'function') ? truncateUri : function (u) { return u; };
        var icon = (typeof getEntityIcon === 'function') ? getEntityIcon(entity) : (attrs.icon || '📦');
        var displayLabel = (typeof getDisplayLabel === 'function') ? getDisplayLabel(entity) : (entity.label || _extractLocalName(nodeId));

        var typeLower = (entity.type || '').toLowerCase();
        var entityMapping = null;
        if (typeof entityMappings !== 'undefined') {
            entityMapping = entityMappings[typeLower] || (typeof findMappingByType === 'function' ? findMappingByType(entity.type) : null);
        }
        if (!entityMapping && entity.typeUri && typeof findMappingByType === 'function') entityMapping = findMappingByType(entity.typeUri);
        if (!entityMapping && entity.id && typeof findMappingByUri === 'function') entityMapping = findMappingByUri(entity.id);

        var classInfo = null;
        var ontologyTypeName = 'Unknown';

        if (entityMapping) {
            if (entityMapping.className) {
                ontologyTypeName = entityMapping.className;
                if (typeof findOntologyClass === 'function') classInfo = findOntologyClass(entityMapping.className) || findOntologyClass(entityMapping.classUri);
            } else if (entityMapping.classUri) {
                var uriParts = entityMapping.classUri.split(/[#\/]/);
                ontologyTypeName = uriParts[uriParts.length - 1] || 'Unknown';
                if (typeof findOntologyClass === 'function') classInfo = findOntologyClass(entityMapping.classUri);
            }
        } else if (entity.typeUri) {
            if (typeof findOntologyClass === 'function') classInfo = findOntologyClass(entity.typeUri);
            if (classInfo) { ontologyTypeName = classInfo.name; }
            else { ontologyTypeName = entity.typeUri.split('#').pop().split('/').pop() || entity.type || 'Unknown'; }
        } else if (entity.id) {
            var extractedClass = (typeof extractClassFromUri === 'function') ? extractClassFromUri(entity.id) : null;
            if (extractedClass) {
                if (typeof findOntologyClass === 'function') classInfo = findOntologyClass(extractedClass);
                ontologyTypeName = classInfo ? classInfo.name : extractedClass;
            }
        }
        if (ontologyTypeName === 'Unknown' && entity.type && typeof findOntologyClass === 'function') {
            classInfo = findOntologyClass(entity.type);
            if (classInfo) ontologyTypeName = classInfo.name;
        }

        var ontologyTypeEmoji = (classInfo && classInfo.emoji) || (entityMapping && entityMapping.emoji) || icon;

        var allAttributes = {};
        var normalizeAttr = function (s) { return s.toLowerCase().replace(/[_-]/g, ''); };
        var validAttributeNames = {};
        if (entityMapping) {
            if (entityMapping.idColumn) validAttributeNames[entityMapping.idColumn.toLowerCase()] = true;
            if (entityMapping.labelColumn) validAttributeNames[entityMapping.labelColumn.toLowerCase()] = true;
            if (entityMapping.attributeMappings) {
                Object.entries(entityMapping.attributeMappings).forEach(function (kv) {
                    validAttributeNames[kv[0].toLowerCase()] = true;
                    validAttributeNames[kv[1].toLowerCase()] = true;
                });
            }
            validAttributeNames['label'] = true;
            validAttributeNames['name'] = true;
        }
        var hasValidNames = Object.keys(validAttributeNames).length > 0;
        var validNormalized = {};
        Object.keys(validAttributeNames).forEach(function (v) { validNormalized[normalizeAttr(v)] = true; });

        function isValidAttr(key) {
            if (!entityMapping || !hasValidNames) return true;
            var kl = key.toLowerCase(); var kn = normalizeAttr(key);
            if (validAttributeNames[kl] || validNormalized[kn]) return true;
            return Object.keys(validAttributeNames).some(function (v) { return kl.indexOf(v) >= 0 || v.indexOf(kl) >= 0; }) ||
                   Object.keys(validNormalized).some(function (v) { return kn.indexOf(v) >= 0 || v.indexOf(kn) >= 0; });
        }

        if (entity.attributes) {
            Object.entries(entity.attributes).forEach(function (kv) {
                if (kv[1] && isValidAttr(kv[0])) allAttributes[kv[0]] = kv[1];
            });
        }

        if (typeof getEntityAttributes === 'function') {
            var queryAttrs = getEntityAttributes(entity.id);
            queryAttrs.forEach(function (attr) {
                if (attr.value && !allAttributes[attr.predicate] && isValidAttr(attr.predicate)) allAttributes[attr.predicate] = attr.value;
            });
        }

        var specialAttrNames = { id: true, label: true, name: true, dashboard: true };
        var actualIdValue = entity.instanceId;
        var actualLabelValue = entity.label;
        var dashboardUrl = (entityMapping && entityMapping.dashboard) || (classInfo && classInfo.dashboard) || null;
        var dashboardParams = (entityMapping && entityMapping.dashboardParams) || (classInfo && classInfo.dashboardParams) || {};

        if (entityMapping) {
            if (entityMapping.idColumn) {
                specialAttrNames[entityMapping.idColumn.toLowerCase()] = true;
                specialAttrNames[normalizeAttr(entityMapping.idColumn)] = true;
                actualIdValue = _findAttrValue(allAttributes, entityMapping.idColumn) || entity.instanceId;
            }
            if (entityMapping.labelColumn) {
                specialAttrNames[entityMapping.labelColumn.toLowerCase()] = true;
                specialAttrNames[normalizeAttr(entityMapping.labelColumn)] = true;
                actualLabelValue = _findAttrValue(allAttributes, entityMapping.labelColumn) || entity.label;
            }
        }

        var html = '<div class="entity-detail-header">' +
            '<span class="entity-detail-icon">' + ontologyTypeEmoji + '</span>' +
            '<div class="entity-detail-title">' +
            '<h6>' + esc(displayLabel) + '</h6>' +
            '<small title="' + esc(entity.id) + '">' + esc(truncUri(entity.id)) + '</small>' +
            '</div></div>';

        html += '<div class="entity-detail-section">' +
            '<h6><i class="bi bi-card-list"></i> Entity Info</h6>' +
            '<div class="entity-detail-item"><span class="detail-key"><i class="bi bi-box text-primary"></i> Type</span>' +
            '<span class="detail-value">' + esc(ontologyTypeName) + '</span></div>' +
            '<div class="entity-detail-item"><span class="detail-key"><i class="bi bi-key-fill text-warning"></i> ID</span>' +
            '<span class="detail-value">' + esc(actualIdValue || 'N/A') + '</span></div></div>';

        var customAttrs = {};
        if (entityMapping && entityMapping.attributeMappings) {
            Object.entries(entityMapping.attributeMappings).forEach(function (kv) {
                var attrName = kv[0], columnName = kv[1];
                if (specialAttrNames[attrName.toLowerCase()] || specialAttrNames[columnName.toLowerCase()] ||
                    specialAttrNames[normalizeAttr(attrName)] || specialAttrNames[normalizeAttr(columnName)]) return;
                var val = _findAttrValue(allAttributes, columnName) || _findAttrValue(allAttributes, attrName);
                if (val) customAttrs[attrName] = val;
            });
        } else {
            Object.entries(allAttributes).forEach(function (kv) {
                var kl = kv[0].toLowerCase(), kn = normalizeAttr(kv[0]);
                if (!specialAttrNames[kl] && !specialAttrNames[kn]) customAttrs[kv[0]] = kv[1];
            });
        }

        if (Object.keys(customAttrs).length > 0) {
            html += '<div class="entity-detail-section"><h6><i class="bi bi-tags"></i> Attributes</h6>';
            Object.entries(customAttrs).forEach(function (kv) {
                html += '<div class="entity-detail-item"><span class="detail-key"><i class="bi bi-card-text text-secondary"></i> ' + esc(kv[0]) + '</span>' +
                    '<span class="detail-value">' + esc(kv[1]) + '</span></div>';
            });
            html += '</div>';
        } else {
            html += '<div class="entity-detail-section"><h6><i class="bi bi-tags"></i> Attributes</h6>' +
                '<p class="small text-muted mb-0">No custom attributes found for this entity.</p></div>';
        }

        if (dashboardUrl && typeof buildDashboardUrl === 'function') {
            var paramValues = {};
            Object.entries(dashboardParams).forEach(function (kv) {
                var paramKeyword = kv[0], mapping = kv[1];
                var attrName = (typeof mapping === 'object') ? mapping.attribute : mapping;
                var pageId = (typeof mapping === 'object') ? (mapping.pageId || '') : '';
                var widgetId = (typeof mapping === 'object') ? (mapping.widgetId || '') : '';
                var value = (attrName === '__ID__') ? actualIdValue : _findAttrValue(allAttributes, attrName);
                if (value) paramValues[paramKeyword] = { value: value, pageId: pageId, widgetId: widgetId };
            });
            var dashUrl = buildDashboardUrl(dashboardUrl, actualIdValue, paramValues);
            html += '<div class="entity-detail-section"><h6><i class="bi bi-speedometer2"></i> Dashboard</h6>' +
                '<div class="entity-detail-item"><button onclick="openDashboardModal(\'' + esc(dashUrl) + '\', \'' + esc(ontologyTypeName) + '\', \'' + esc(actualIdValue || '') + '\')" ' +
                'class="btn btn-sm btn-outline-info w-100" title="Open dashboard"><i class="bi bi-speedometer2 me-1"></i>View Dashboard</button></div></div>';
        }

        var outgoingRels = (typeof d3LinksData !== 'undefined' && d3LinksData) ? d3LinksData.filter(function (l) {
            return (typeof l.source === 'object' ? l.source.id : l.source) === entity.id;
        }) : [];
        var incomingRels = (typeof d3LinksData !== 'undefined' && d3LinksData) ? d3LinksData.filter(function (l) {
            return (typeof l.target === 'object' ? l.target.id : l.target) === entity.id;
        }) : [];

        if (outgoingRels.length > 0) {
            html += '<div class="entity-detail-section"><h6><i class="bi bi-arrow-right-circle"></i> Outgoing (' + outgoingRels.length + ')</h6>';
            outgoingRels.forEach(function (rel) {
                var targetId = typeof rel.target === 'object' ? rel.target.id : rel.target;
                var targetNode = d3NodesData.find(function (n) { return n.id === targetId; });
                var targetLabel = targetNode ? ((typeof getDisplayLabel === 'function') ? getDisplayLabel(targetNode) : (targetNode.label || '')) : _extractLocalName(targetId);
                var targetIcon = targetNode ? ((typeof getEntityIcon === 'function') ? getEntityIcon(targetNode) : '🔷') : '🔷';
                html += '<div class="entity-relationship-item">' +
                    '<span class="rel-direction">→</span> ' +
                    '<span class="rel-predicate">' + esc(rel.predicate) + '</span> ' +
                    '<span class="rel-direction">→</span> ' +
                    '<span class="rel-target" onclick="SigmaGraph.selectEntity(\'' + esc(targetId) + '\')">' + targetIcon + ' ' + esc(targetLabel) + '</span></div>';
            });
            html += '</div>';
        }

        if (incomingRels.length > 0) {
            html += '<div class="entity-detail-section"><h6><i class="bi bi-arrow-left-circle"></i> Incoming (' + incomingRels.length + ')</h6>';
            incomingRels.forEach(function (rel) {
                var sourceId = typeof rel.source === 'object' ? rel.source.id : rel.source;
                var sourceNode = d3NodesData.find(function (n) { return n.id === sourceId; });
                var sourceLabel = sourceNode ? ((typeof getDisplayLabel === 'function') ? getDisplayLabel(sourceNode) : (sourceNode.label || '')) : _extractLocalName(sourceId);
                var sourceIcon = sourceNode ? ((typeof getEntityIcon === 'function') ? getEntityIcon(sourceNode) : '🔷') : '🔷';
                html += '<div class="entity-relationship-item">' +
                    '<span class="rel-target" onclick="SigmaGraph.selectEntity(\'' + esc(sourceId) + '\')">' + sourceIcon + ' ' + esc(sourceLabel) + '</span> ' +
                    '<span class="rel-direction">→</span> ' +
                    '<span class="rel-predicate">' + esc(rel.predicate) + '</span> ' +
                    '<span class="rel-direction">→</span></div>';
            });
            html += '</div>';
        }

        html += '<div class="entity-detail-section"><h6><i class="bi bi-link-45deg"></i> Full URI</h6>' +
            '<div class="small text-muted" style="word-break: break-all;">' + esc(entity.id) + '</div></div>';

        el.innerHTML = html;
    }

    function _showEdgeDetails(edgeId) {
        var el = document.getElementById('sgDetailsContent');
        if (!el || !_graph) return;
        var attrs = _graph.getEdgeAttributes(edgeId);
        var data = attrs._data || {};
        var source = _graph.source(edgeId);
        var target = _graph.target(edgeId);

        var esc = (typeof escapeHtml === 'function') ? escapeHtml : _esc;

        var sourceNode = (typeof d3NodesData !== 'undefined') ? d3NodesData.find(function (n) { return n.id === source; }) : null;
        var targetNode = (typeof d3NodesData !== 'undefined') ? d3NodesData.find(function (n) { return n.id === target; }) : null;

        var predicateUri = data.predicate || attrs.label || '';
        var predicateLabel = predicateUri.indexOf('#') >= 0 ? predicateUri.split('#').pop() :
            predicateUri.indexOf('/') >= 0 ? predicateUri.split('/').pop() : predicateUri;

        var sourceIcon = sourceNode ? ((typeof getEntityIcon === 'function') ? getEntityIcon(sourceNode) : '📦') : '📦';
        var targetIcon = targetNode ? ((typeof getEntityIcon === 'function') ? getEntityIcon(targetNode) : '📦') : '📦';
        var sourceLabel = sourceNode ? ((typeof getDisplayLabel === 'function') ? getDisplayLabel(sourceNode) : (sourceNode.label || 'Unknown')) : 'Unknown';
        var targetLabel = targetNode ? ((typeof getDisplayLabel === 'function') ? getDisplayLabel(targetNode) : (targetNode.label || 'Unknown')) : 'Unknown';

        var html = '<div class="entity-detail-header">' +
            '<span class="entity-detail-icon">🔗</span>' +
            '<div class="entity-detail-title"><h6>' + esc(predicateLabel) + '</h6><small>Relationship</small></div></div>';

        html += '<div class="entity-detail-section"><h6><i class="bi bi-card-list"></i> Relationship Info</h6>' +
            '<div class="entity-detail-item"><span class="detail-key">Name</span><span class="detail-value">' + esc(predicateLabel) + '</span></div>' +
            '<div class="entity-detail-item"><span class="detail-key">URI</span><span class="detail-value small" style="word-break: break-all;">' + esc(predicateUri) + '</span></div></div>';

        if (typeof relationshipMappings !== 'undefined' && relationshipMappings) {
            var predLower = predicateLabel.toLowerCase();
            var relMapping = relationshipMappings[predLower] ||
                Object.values(relationshipMappings).find(function (m) { return m.predicate && m.predicate.toLowerCase().indexOf(predLower) >= 0; });
            if (relMapping) {
                var mappingAttrs = [];
                if (relMapping.sourceTable) mappingAttrs.push({ key: 'Source Table', value: relMapping.sourceTable });
                if (relMapping.targetTable) mappingAttrs.push({ key: 'Target Table', value: relMapping.targetTable });
                if (relMapping.joinColumn) mappingAttrs.push({ key: 'Join Column', value: relMapping.joinColumn });
                if (relMapping.sourceColumn) mappingAttrs.push({ key: 'Source Column', value: relMapping.sourceColumn });
                if (relMapping.targetColumn) mappingAttrs.push({ key: 'Target Column', value: relMapping.targetColumn });
                if (mappingAttrs.length > 0) {
                    html += '<div class="entity-detail-section"><h6><i class="bi bi-database"></i> Mapping Info</h6>';
                    mappingAttrs.forEach(function (attr) {
                        html += '<div class="entity-detail-item"><span class="detail-key">' + esc(attr.key) + '</span><span class="detail-value">' + esc(attr.value) + '</span></div>';
                    });
                    html += '</div>';
                }
            }
        }

        html += '<div class="entity-detail-section"><h6><i class="bi bi-box-arrow-right"></i> Source Entity</h6>' +
            '<div class="entity-relationship-item" style="cursor:pointer;" onclick="SigmaGraph.selectEntity(\'' + esc(source) + '\')">' +
            '<span class="me-2">' + sourceIcon + '</span><span class="rel-target">' + esc(sourceLabel) + '</span></div></div>';

        html += '<div class="entity-detail-section"><h6><i class="bi bi-box-arrow-in-right"></i> Target Entity</h6>' +
            '<div class="entity-relationship-item" style="cursor:pointer;" onclick="SigmaGraph.selectEntity(\'' + esc(target) + '\')">' +
            '<span class="me-2">' + targetIcon + '</span><span class="rel-target">' + esc(targetLabel) + '</span></div></div>';

        html += '<div class="entity-detail-section"><h6><i class="bi bi-diagram-3"></i> Triple Pattern</h6>' +
            '<div class="p-2 bg-light rounded small"><div class="d-flex align-items-center justify-content-between flex-wrap gap-1">' +
            '<span class="badge bg-success">' + sourceIcon + ' ' + esc(sourceLabel) + '</span>' +
            ' <i class="bi bi-arrow-right text-muted"></i> ' +
            '<span class="badge bg-primary">' + esc(predicateLabel) + '</span>' +
            ' <i class="bi bi-arrow-right text-muted"></i> ' +
            '<span class="badge bg-info">' + targetIcon + ' ' + esc(targetLabel) + '</span>' +
            '</div></div></div>';

        el.innerHTML = html;
    }

    function _findAttrValue(attrs, columnName) {
        if (typeof findAttributeValue === 'function') {
            var map = new Map(Object.entries(attrs));
            return findAttributeValue(map, columnName);
        }
        if (attrs[columnName]) return attrs[columnName];
        var lower = columnName.toLowerCase();
        for (var k in attrs) { if (k.toLowerCase() === lower) return attrs[k]; }
        return null;
    }

    function _esc(t) {
        if (t == null) return '';
        var d = document.createElement('div');
        d.textContent = String(t);
        return d.innerHTML;
    }

    // -----------------------------------------------------------
    // Stats & Types panels
    // -----------------------------------------------------------
    function _updateStats() {
        if (!_graph) return;
        var nodeEl = document.getElementById('sgNodeCount');
        var edgeEl = document.getElementById('sgEdgeCount');
        var statsEl = document.getElementById('sgStats');
        if (nodeEl) nodeEl.textContent = _graph.order + ' entities';
        if (edgeEl) edgeEl.textContent = _graph.size + ' relationships';
        if (statsEl) statsEl.style.display = 'flex';
    }

    function _populateTypes() {
        var entityCont = document.getElementById('sgEntityTypeFilters');
        var relCont = document.getElementById('sgRelTypeFilters');
        if (!entityCont || !relCont || !_graph) return;

        // Entity types
        var types = {};
        _graph.forEachNode(function (n, attrs) {
            var t = attrs.entityType || 'Unknown';
            types[t] = (types[t] || 0) + 1;
        });
        var eHtml = '';
        Object.keys(types).sort().forEach(function (t) {
            var active = _visibleTypes.has(t);
            var color = _colorForType(t);
            var icon = _iconForType(t);
            eHtml += '<button class="btn btn-sm me-1 mb-1 ' + (active ? '' : 'btn-outline-secondary') + '" '
                + 'style="' + (active ? 'background:' + color + ';color:#fff;border-color:' + color : '') + '" '
                + 'onclick="SigmaGraph.toggleType(\'' + _esc(t) + '\')">'
                + icon + ' ' + _esc(t) + ' <span class="badge bg-light text-dark">' + types[t] + '</span></button>';
        });
        entityCont.innerHTML = eHtml || '<span class="text-muted small">No entities loaded</span>';

        // Edge types
        var rels = {};
        _graph.forEachEdge(function (e, attrs) {
            var p = attrs.label || 'unknown';
            rels[p] = (rels[p] || 0) + 1;
        });
        var rHtml = '';
        Object.keys(rels).sort().forEach(function (r) {
            var active = _visibleEdgeTypes.has(r);
            rHtml += '<button class="btn btn-sm me-1 mb-1 ' + (active ? 'btn-outline-info' : 'btn-outline-secondary') + '" '
                + 'onclick="SigmaGraph.toggleEdgeType(\'' + _esc(r) + '\')">'
                + _esc(r) + ' <span class="badge bg-light text-dark">' + rels[r] + '</span></button>';
        });
        relCont.innerHTML = rHtml || '<span class="text-muted small">No relationships loaded</span>';
    }

    function _populateSearchTypes() {
        var sel = document.getElementById('sgSearchEntityType');
        if (!sel || !_graph) return;
        var types = new Set();
        _graph.forEachNode(function (n, a) { if (a.entityType) types.add(a.entityType); });
        var html = '<option value="">All types</option>';
        Array.from(types).sort().forEach(function (t) {
            html += '<option value="' + _esc(t) + '">' + _esc(t) + '</option>';
        });
        sel.innerHTML = html;
    }

    async function _populateFilterEntityTypes() {
        var sel = document.getElementById('sgFilterEntityType');
        if (!sel) return;

        // If the graph filter is active and we have loaded nodes, use those
        if (_graphFilterActive) {
            var nodes = (typeof d3NodesData !== 'undefined' && d3NodesData) ? d3NodesData : [];
            if (nodes.length > 0) {
                var types = {};
                nodes.forEach(function (n) { var t = n.type || 'Unknown'; types[t] = (types[t] || 0) + 1; });
                var html = '<option value="">All types</option>';
                Object.keys(types).sort().forEach(function (t) {
                    html += '<option value="' + _esc(t) + '">' + _esc(t) + ' (' + types[t] + ')</option>';
                });
                sel.innerHTML = html;
                return;
            }
        }

        // Always query the triple store live (with refresh=true to bypass server cache)
        sel.innerHTML = '<option value="">Loading types...</option>';
        try {
            var resp = await fetch('/dtwin/sync/stats?refresh=true', { credentials: 'same-origin' });
            var stats = await resp.json();
            if (stats.success && stats.entity_types) {
                _cachedStats = stats.entity_types;
                _renderStatsDropdown(sel, _cachedStats);
            } else {
                sel.innerHTML = '<option value="">All types</option>';
            }
        } catch (err) {
            console.warn('[SigmaGraph] Failed to load stats for entity types:', err);
            sel.innerHTML = '<option value="">All types</option>';
        }
    }

    function _renderStatsDropdown(sel, entityTypes) {
        var html = '<option value="">All types</option>';
        entityTypes.forEach(function (et) {
            var uri = et.uri || '';
            var shortName = uri.indexOf('#') >= 0 ? uri.split('#').pop() :
                            uri.indexOf('/') >= 0 ? uri.split('/').pop() : uri;
            html += '<option value="' + _esc(uri) + '">' + _esc(shortName) + ' (' + et.count + ')</option>';
        });
        sel.innerHTML = html;
    }

    function _extractShortId(uri) {
        if (!uri) return null;
        if (!uri.startsWith('http')) return uri;
        var parts = uri.split('/');
        return parts[parts.length - 1];
    }

    async function _executeGraphFilter() {
        var entityType = (document.getElementById('sgFilterEntityType')?.value || '').trim();
        var matchType = document.getElementById('sgFilterMatchType')?.value || 'contains';
        var searchValue = (document.getElementById('sgFilterValue')?.value || '').trim();
        var includeRels = true;
        var maxDepth = parseInt(document.getElementById('sgFilterDepth')?.value || '3');

        if (!searchValue && !entityType) return;

        var info = document.getElementById('sgGraphFilterInfo');
        var text = document.getElementById('sgGraphFilterInfoText');
        if (info && text) { info.classList.remove('d-none'); text.textContent = 'Querying triple store...'; }

        var loading = document.getElementById('sgLoading');
        if (loading) loading.style.display = 'flex';
        _hideEmptyState();

        try {
            var resp = await fetch('/dtwin/sync/filter', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    entity_type: entityType,
                    field: 'any',
                    match_type: matchType,
                    value: searchValue,
                    include_rels: includeRels,
                    depth: maxDepth
                }),
                credentials: 'same-origin'
            });
            var data = await resp.json();

            if (!data.success) {
                _hideLoading();
                if (info && text) { info.classList.remove('d-none'); text.textContent = data.message || 'Filter query failed.'; }
                return;
            }

            if (!data.results || data.results.length === 0) {
                _hideLoading();
                if (info && text) { info.classList.remove('d-none'); text.textContent = data.message || 'No entities found matching your search criteria.'; }
                return;
            }

            lastQueryResults = { results: data.results, columns: data.columns };

            var libsOk = await _waitForGraphLibs(10000);
            if (!libsOk) {
                _hideLoading();
                if (info && text) { info.classList.remove('d-none'); text.textContent = 'Graph libraries failed to load. Check your network and reload.'; }
                return;
            }

            if (typeof buildGraph === 'function') {
                await buildGraph(data.results, data.columns);
            }

            _graphFilterActive = true;
            _searchMatched = null;
            _searchNeighbors = null;
            _selectedNode = null;
            _hoveredNode = null;
            if (!_pendingHighlightTerm && searchValue) {
                _pendingHighlightTerm = searchValue;
            }
            _render();

            setTimeout(function () { _applyPendingHighlight(); }, 200);

            var initialCount = data.initial_count || 0;
            var expandedCount = data.expanded_count || 0;
            var relatedCount = expandedCount - initialCount;
            if (info && text) {
                info.classList.remove('d-none');
                if (includeRels && relatedCount > 0) {
                    text.textContent = 'Found ' + expandedCount + ' entities (' + initialCount + ' matched, ' + relatedCount + ' related at ' + maxDepth + ' level' + (maxDepth > 1 ? 's' : '') + ').';
                } else {
                    text.textContent = 'Found ' + initialCount + ' entities (' + data.count + ' triples).';
                }
            }
            var clearBtn = document.getElementById('sgClearGraphFilterBtn');
            if (clearBtn) clearBtn.style.display = 'inline-block';

        } catch (err) {
            console.error('[SigmaGraph] _executeGraphFilter error:', err);
            _hideLoading();
            if (info && text) { info.classList.remove('d-none'); text.textContent = 'Error: ' + err.message; }
        }
    }

    function _clearGraphFilter() {
        _graphFilterActive = false;
        _searchMatched = null;
        _searchNeighbors = null;
        _highlightedSeeds = null;
        _pendingHighlightTerm = null;
        _selectedNode = null;
        _hoveredNode = null;

        if (_renderer) {
            try { _renderer.kill(); } catch (_) {}
            _renderer = null;
        }
        _graph = null;
        d3NodesData = [];
        d3LinksData = [];

        var info = document.getElementById('sgGraphFilterInfo');
        if (info) info.classList.add('d-none');
        var clearBtn = document.getElementById('sgClearGraphFilterBtn');
        if (clearBtn) clearBtn.style.display = 'none';
        var val = document.getElementById('sgFilterValue');
        if (val) val.value = '';
        var sel = document.getElementById('sgFilterEntityType');
        if (sel) sel.value = '';

        var statsEl = document.getElementById('sgStats');
        if (statsEl) statsEl.style.display = 'none';

        _showPlaceholder();
        _showEmptyState();
    }

    // -----------------------------------------------------------
    // Public API
    // -----------------------------------------------------------
    function init() {
        console.log('[SigmaGraph] init called');
        _loadGraphLibs();

        // Always start fresh: discard any stale d3 data so the graph
        // queries the triple store directly via the search filter.
        if (!_graphFilterActive) {
            d3NodesData = [];
            d3LinksData = [];
            if (_renderer) {
                try { _renderer.kill(); } catch (_) {}
                _renderer = null;
            }
            _graph = null;
            _showEmptyState();
        } else {
            _render();
        }
        _initialized = true;
    }

    function _hasData() {
        return typeof d3NodesData !== 'undefined' && d3NodesData && d3NodesData.length > 0;
    }

    function _showEmptyState() {
        _hideLoading();
        var container = document.getElementById('sgContainer');
        if (container) {
            var placeholder = document.getElementById('sgEmptyState');
            if (!placeholder) {
                placeholder = document.createElement('div');
                placeholder.id = 'sgEmptyState';
                placeholder.className = 'position-absolute top-50 start-50 translate-middle text-center';
                placeholder.innerHTML =
                    '<div class="text-muted">' +
                    '<i class="bi bi-funnel" style="font-size:2.5rem;"></i>' +
                    '<p class="mt-2 mb-1 fw-semibold">No data loaded</p>' +
                    '<p class="small">Use search to load entities from the triple store.</p>' +
                    '</div>';
                container.appendChild(placeholder);
            }
            placeholder.style.display = '';
        }
        var gp = document.getElementById('sgGraphFilterPane');
        if (gp && gp.style.display === 'none') {
            SigmaGraph.toggleGraphFilterPane();
        }
        _populateFilterEntityTypes();
    }

    function _applyPendingHighlight() {
        if (!_pendingHighlightTerm || !_graph) {
            _highlightedSeeds = null;
            return;
        }
        var term = _pendingHighlightTerm.toLowerCase();
        _pendingHighlightTerm = null;
        var matched = new Set();
        _graph.forEachNode(function (nodeId, attrs) {
            var label = (attrs.label || '').toLowerCase();
            var localId = nodeId.toLowerCase();
            var frag = localId.indexOf('#') >= 0
                ? localId.substring(localId.lastIndexOf('#') + 1)
                : localId.substring(localId.lastIndexOf('/') + 1);
            if (label === term || frag === term || label.indexOf(term) >= 0 || frag.indexOf(term) >= 0) {
                matched.add(nodeId);
            }
        });
        _highlightedSeeds = matched.size > 0 ? matched : null;
        if (_renderer) {
            _renderer.refresh();
        }
    }

    function _hideEmptyState() {
        var el = document.getElementById('sgEmptyState');
        if (el) el.style.display = 'none';
    }

    return {
        init: init,
        reload: function () { if (_hasData()) { _render(); } else { _showEmptyState(); } },
        refresh: function () { _render(); },

        selectEntity: function (entityId) {
            if (!_graph || !_graph.hasNode(entityId)) return;
            _searchMatched = null;
            _searchNeighbors = null;
            _selectedNode = entityId;
            _hoveredNode = null;
            _showNodeDetails(entityId);
            if (_renderer) {
                _renderer.refresh();
                _focusCameraOnNodes(new Set([entityId]));
            }
        },

        zoomIn: function () { if (_renderer) { var c = _renderer.getCamera(); c.animatedZoom({ duration: 200 }); } },
        zoomOut: function () { if (_renderer) { var c = _renderer.getCamera(); c.animatedUnzoom({ duration: 200 }); } },
        fitToView: function () { if (_renderer) { var c = _renderer.getCamera(); c.animatedReset({ duration: 300 }); } },
        resetLayout: function () { if (_graph) { _applyLayout(_graph); if (_renderer) _renderer.refresh(); } },

        toggleLabels: function () { if (_renderer) _renderer.setSetting('renderLabels', document.getElementById('sgShowLabels')?.checked !== false); },
        toggleEdgeLabels: function () { if (_renderer) _renderer.setSetting('renderEdgeLabels', document.getElementById('sgShowEdgeLabels')?.checked !== false); },

        toggleTypesPanel: function () {
            var p = document.getElementById('sgTypesPanel');
            var b = document.getElementById('sgToggleTypesBtn');
            if (!p) return;
            var vis = p.style.display !== 'none';
            p.style.display = vis ? 'none' : 'block';
            if (b) b.classList.toggle('active', !vis);
            if (!vis) _populateTypes();
        },

        toggleFilterPane: function () {
            var p = document.getElementById('sgFilterPane');
            var b = document.getElementById('sgToggleFilterBtn');
            if (!p) return;
            var vis = p.style.display !== 'none';
            if (!vis) {
                var gp = document.getElementById('sgGraphFilterPane');
                var gb = document.getElementById('sgToggleGraphFilterBtn');
                if (gp && gp.style.display !== 'none') { gp.style.display = 'none'; if (gb) gb.classList.remove('active'); }
            }
            p.style.display = vis ? 'none' : 'flex';
            if (b) b.classList.toggle('active', !vis);
        },

        toggleType: function (type) {
            if (_visibleTypes.has(type)) _visibleTypes.delete(type);
            else _visibleTypes.add(type);
            _populateTypes();
            if (_renderer) _renderer.refresh();
        },

        toggleEdgeType: function (type) {
            if (_visibleEdgeTypes.has(type)) _visibleEdgeTypes.delete(type);
            else _visibleEdgeTypes.add(type);
            _populateTypes();
            if (_renderer) _renderer.refresh();
        },

        selectAllTypes: function () {
            if (!_graph) return;
            _graph.forEachNode(function (n, a) { _visibleTypes.add(a.entityType || 'Unknown'); });
            _graph.forEachEdge(function (e, a) { _visibleEdgeTypes.add(a.label || ''); });
            _populateTypes();
            if (_renderer) _renderer.refresh();
        },

        clearAllTypes: function () {
            _visibleTypes.clear();
            _visibleEdgeTypes.clear();
            _populateTypes();
            if (_renderer) _renderer.refresh();
        },

        applySearch: function () {
            if (!_graph) return;
            var typeFilter = document.getElementById('sgSearchEntityType')?.value || '';
            var query = (document.getElementById('sgSearchValue')?.value || '').toLowerCase().trim();

            if (!typeFilter && !query) {
                SigmaGraph.clearSearch();
                return;
            }

            // Find directly matched nodes
            _searchMatched = new Set();
            _searchNeighbors = new Set();
            _graph.forEachNode(function (node, attrs) {
                var matchType = !typeFilter || attrs.entityType === typeFilter;
                var rawLabel = (attrs._data && attrs._data.label) ? attrs._data.label : (attrs.label || '');
                var matchQuery = !query || rawLabel.toLowerCase().includes(query);
                if (matchType && matchQuery) _searchMatched.add(node);
            });

            // Expand to neighbors of matched nodes
            _searchMatched.forEach(function (node) {
                _graph.forEachNeighbor(node, function (neighbor) {
                    if (!_searchMatched.has(neighbor)) _searchNeighbors.add(neighbor);
                });
            });

            var info = document.getElementById('sgSearchInfo');
            var infoText = document.getElementById('sgSearchInfoText');
            if (info && infoText) {
                info.classList.remove('d-none');
                infoText.textContent = 'Found ' + _searchMatched.size + ' entit' + (_searchMatched.size === 1 ? 'y' : 'ies');
            }
            var clearBtn = document.getElementById('sgClearSearchBtn');
            if (clearBtn) clearBtn.style.display = 'inline-block';

            // Clear click/hover selection so search focus takes over
            _selectedNode = null;
            _hoveredNode = null;

            if (_renderer) {
                _renderer.refresh();
                var allVisible = new Set(_searchMatched);
                if (_searchNeighbors) _searchNeighbors.forEach(function (n) { allVisible.add(n); });
                _focusCameraOnNodes(allVisible);
            }
        },

        clearSearch: function () {
            _searchMatched = null;
            _searchNeighbors = null;
            _selectedNode = null;
            _hoveredNode = null;
            var info = document.getElementById('sgSearchInfo');
            if (info) info.classList.add('d-none');
            var clearBtn = document.getElementById('sgClearSearchBtn');
            if (clearBtn) clearBtn.style.display = 'none';
            var val = document.getElementById('sgSearchValue');
            if (val) val.value = '';
            var sel = document.getElementById('sgSearchEntityType');
            if (sel) sel.value = '';
            if (_renderer) {
                _renderer.getCamera().animatedReset({ duration: 300 });
                _renderer.refresh();
            }
        },

        toggleGraphFilterPane: function () {
            var p = document.getElementById('sgGraphFilterPane');
            var b = document.getElementById('sgToggleGraphFilterBtn');
            if (!p) return;
            var vis = p.style.display !== 'none';
            if (!vis) {
                var fp = document.getElementById('sgFilterPane');
                var fb = document.getElementById('sgToggleFilterBtn');
                if (fp && fp.style.display !== 'none') { fp.style.display = 'none'; if (fb) fb.classList.remove('active'); }
            }
            p.style.display = vis ? 'none' : 'flex';
            if (b) b.classList.toggle('active', !vis);
            if (!vis) _populateFilterEntityTypes();
        },

        populateFilterEntityTypes: function () { _populateFilterEntityTypes(); },
        executeGraphFilter: function () { _executeGraphFilter(); },
        clearGraphFilter: function () { _clearGraphFilter(); },
        setHighlightTerm: function (term) { _pendingHighlightTerm = term || null; },
        clearHighlight: function () { _highlightedSeeds = null; _pendingHighlightTerm = null; if (_renderer) _renderer.refresh(); },

        loadInferredTriples: async function () {
            try {
                var resp = await fetch('/dtwin/reasoning/inferred');
                var data = await resp.json();
                if (!data.success) return;
                var reasoning = data.reasoning || {};
                var inferred = reasoning.inferred_triples || [];
                if (inferred.length === 0) return;
                var count = 0;
                for (var i = 0; i < inferred.length; i++) {
                    var t = inferred[i];
                    if (!t.subject || t.subject === '(batch)') continue;
                    if (typeof d3LinksData !== 'undefined' && d3LinksData) {
                        d3LinksData.push({
                            source: t.subject,
                            target: t.object,
                            predicate: t.predicate,
                            provenance: t.provenance || 'inferred',
                            inferred: true
                        });
                        count++;
                    }
                }
                if (count > 0 && typeof showNotification === 'function') {
                    showNotification('Added ' + count + ' inferred triples to graph.', 'info');
                    SigmaGraph.refresh();
                }
            } catch (e) { console.error('loadInferredTriples error:', e); }
        },

        toggleInferred: function () {
            if (!_graph) return;
            var show = document.getElementById('sgShowInferred')?.checked !== false;
            _graph.forEachEdge(function (edge, attrs) {
                if (attrs._inferred) {
                    _graph.setEdgeAttribute(edge, 'hidden', !show);
                }
            });
            if (_renderer) _renderer.refresh();
        }
    };
})();
