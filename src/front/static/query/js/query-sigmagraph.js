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
    var _pendingFocusLoad = false;

    // --- Group expand/collapse state ---
    var _groupsDefs = [];          // group definitions from /dtwin/groups
    var _collapsedGroups = new Set(); // group names currently collapsed
    var _groupMemberMap = {};      // entityType → group name (for quick lookup)
    var _groupsLoaded = false;
    var _expandedInstanceMembers = new Set(); // node IDs individually expanded from a super-node

    // --- Data cluster state ---
    var _clusterAssignments = null;  // node ID → cluster integer, or null
    var _clusterCount = 0;
    var _clusterColorByActive = false;
    var _clusterResolution = 1.0;
    var _collapsedClusters = new Set();
    var _clusterCollapseActive = false;

    function _clearExpandedForGroup(groupName) {
        if (_expandedInstanceMembers.size === 0) return;
        var nodes = (typeof d3NodesData !== 'undefined' && d3NodesData) ? d3NodesData : [];
        var nodeTypeMap = {};
        nodes.forEach(function (n) { if (n && n.id) nodeTypeMap[n.id] = n.type || ''; });
        var toRemove = [];
        _expandedInstanceMembers.forEach(function (nid) {
            var nType = nodeTypeMap[nid] || '';
            if (_groupMemberMap[nType] === groupName) toRemove.push(nid);
        });
        toRemove.forEach(function (nid) { _expandedInstanceMembers.delete(nid); });
    }

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
    // Group helpers
    // -----------------------------------------------------------
    function _loadGroups() {
        return fetch('/dtwin/groups')
            .then(function (r) { return r.json(); })
            .then(function (data) {
                if (!data.success) return;
                _groupsDefs = data.groups || [];
                _groupMemberMap = {};
                _groupsDefs.forEach(function (g) {
                    (g.memberUris || []).forEach(function (uri) { _groupMemberMap[uri] = g.name; });
                    (g.members || []).forEach(function (m) { _groupMemberMap[m] = g.name; });
                });
                if (!_groupsLoaded) {
                    _groupsDefs.forEach(function (g) { _collapsedGroups.add(g.name); });
                    _groupsLoaded = true;
                }
                _populateGroupsPanel();
            })
            .catch(function (err) { console.warn('[SigmaGraph] groups load error:', err); });
    }

    function _nodeGroupName(nodeData) {
        if (!nodeData) return null;
        var id = nodeData.id || '';
        var type = nodeData.type || nodeData.entityType || '';
        return _groupMemberMap[id] || _groupMemberMap[type] || _groupMemberMap[_extractLocalName(id)] || _groupMemberMap[_extractLocalName(type)] || null;
    }

    function _populateGroupsPanel() {
        var container = document.getElementById('sgGroupsChips');
        if (!container) return;
        if (!_groupsDefs || _groupsDefs.length === 0) {
            container.innerHTML = '<span class="text-muted small">No groups defined.</span>';
            return;
        }
        var html = '';
        _groupsDefs.forEach(function (g) {
            var name = g.name;
            var collapsed = _collapsedGroups.has(name);
            var color = g.color || '#4A90D9';
            var count = (g.members || []).length;
            var iconCls = collapsed ? 'bi-arrows-angle-expand' : 'bi-arrows-angle-contract';
            html += '<button class="btn btn-sm me-1 mb-1' + (collapsed ? ' btn-outline-secondary' : ' btn-primary') + '" ' +
                'onclick="SigmaGraph.toggleGroup(\'' + name.replace(/'/g, "\\'") + '\')" ' +
                'title="' + (collapsed ? 'Expand' : 'Collapse') + ' group: ' + name + '">' +
                '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:' + color + ';margin-right:4px;vertical-align:middle;"></span>' +
                (g.icon ? g.icon + ' ' : '') +
                (g.label || name) + ' (' + count + ') ' +
                '<i class="bi ' + iconCls + '"></i></button>';
        });
        container.innerHTML = html;
    }

    // -----------------------------------------------------------
    // Data cluster helpers
    // -----------------------------------------------------------

    var CLUSTER_COLORS = [
        '#E63946', '#457B9D', '#2A9D8F', '#E9C46A', '#F4A261',
        '#264653', '#A8DADC', '#8338EC', '#FF006E', '#3A86FF',
        '#06D6A0', '#FFD166', '#118AB2', '#EF476F', '#073B4C',
        '#9B5DE5', '#F15BB5', '#00BBF9', '#00F5D4', '#FEE440'
    ];

    function _clusterColor(clusterId) {
        return CLUSTER_COLORS[clusterId % CLUSTER_COLORS.length];
    }

    function _detectClusters(resolution) {
        if (!_graph || _graph.order === 0) return;
        if (typeof graphologyLibrary === 'undefined' || !graphologyLibrary.communitiesLouvain) {
            console.warn('[SigmaGraph] communitiesLouvain not available');
            return;
        }

        var louvain = graphologyLibrary.communitiesLouvain;
        var undirected;
        try {
            var GraphClass = graphology.Graph || graphology;
            undirected = new GraphClass({ multi: false, type: 'undirected' });
            _graph.forEachNode(function (node, attrs) {
                if (!attrs._isGroup) undirected.addNode(node);
            });
            _graph.forEachEdge(function (edge, attrs, source, target) {
                if (source === target) return;
                var sa = _graph.getNodeAttributes(source);
                var ta = _graph.getNodeAttributes(target);
                if (sa._isGroup || ta._isGroup) return;
                if (!undirected.hasEdge(source, target)) {
                    undirected.addEdge(source, target);
                }
            });
        } catch (e) {
            console.error('[SigmaGraph] Error building undirected projection:', e);
            return;
        }

        if (undirected.order === 0) return;

        try {
            _clusterAssignments = louvain.assign(undirected, {
                nodeCommunityAttribute: '_cluster',
                resolution: resolution || 1.0
            });

            _clusterAssignments = {};
            var maxCluster = -1;
            undirected.forEachNode(function (node, attrs) {
                var c = attrs._cluster;
                if (c === undefined) c = 0;
                _clusterAssignments[node] = c;
                if (c > maxCluster) maxCluster = c;
            });
            _clusterCount = maxCluster + 1;

            _graph.forEachNode(function (node) {
                if (_clusterAssignments[node] !== undefined) {
                    _graph.setNodeAttribute(node, '_cluster', _clusterAssignments[node]);
                }
            });
        } catch (e) {
            console.error('[SigmaGraph] Louvain detection failed:', e);
            _clusterAssignments = null;
            _clusterCount = 0;
        }

        _populateClustersPanel();
    }

    function _clearClusters() {
        _clusterAssignments = null;
        _clusterCount = 0;
        _clusterColorByActive = false;
        _collapsedClusters.clear();
        _clusterCollapseActive = false;
        if (_graph) {
            _graph.forEachNode(function (node) {
                _graph.removeNodeAttribute(node, '_cluster');
            });
        }
        _populateClustersPanel();
        if (_renderer) _renderer.refresh();
    }

    function _getClusterStats() {
        if (!_clusterAssignments) return [];
        var stats = {};
        for (var nodeId in _clusterAssignments) {
            var c = _clusterAssignments[nodeId];
            if (!stats[c]) stats[c] = { id: c, size: 0, types: {} };
            stats[c].size++;
            if (_graph && _graph.hasNode(nodeId)) {
                var t = _graph.getNodeAttributes(nodeId).entityType || 'Unknown';
                stats[c].types[t] = (stats[c].types[t] || 0) + 1;
            }
        }
        var list = [];
        for (var k in stats) list.push(stats[k]);
        list.sort(function (a, b) { return b.size - a.size; });
        return list;
    }

    function _populateClustersPanel() {
        var container = document.getElementById('sgClusterList');
        if (!container) return;

        var stats = _getClusterStats();
        if (stats.length === 0) {
            container.innerHTML = '<span class="text-muted small">No clusters detected yet.</span>';
            var countEl = document.getElementById('sgClusterCount');
            if (countEl) countEl.textContent = '';
            return;
        }

        var countEl = document.getElementById('sgClusterCount');
        if (countEl) countEl.textContent = stats.length + ' clusters found';

        var html = '';
        stats.forEach(function (cl) {
            var color = _clusterColor(cl.id);
            var collapsed = _collapsedClusters.has(cl.id);
            var topTypes = Object.keys(cl.types).sort(function (a, b) { return cl.types[b] - cl.types[a]; }).slice(0, 3);
            var typeStr = topTypes.map(function (t) {
                var name = t.indexOf('#') >= 0 ? t.split('#').pop() : (t.indexOf('/') >= 0 ? t.split('/').pop() : t);
                return name + '(' + cl.types[t] + ')';
            }).join(', ');

            html += '<div class="sg-cluster-chip d-flex align-items-center gap-1 mb-1">' +
                '<span class="sg-cluster-dot" style="background:' + color + '"></span>' +
                '<span class="small flex-grow-1">' +
                '<strong>Cluster #' + cl.id + '</strong> <span class="text-muted">(' + cl.size + ')</span>' +
                '<br><span class="text-muted" style="font-size:.7rem">' + typeStr + '</span>' +
                '</span>' +
                '<button class="btn btn-sm py-0 px-1 ' + (collapsed ? 'btn-outline-secondary' : 'btn-outline-primary') + '" ' +
                'onclick="SigmaGraph.toggleClusterCollapse(' + cl.id + ')" title="' + (collapsed ? 'Expand' : 'Collapse') + '">' +
                '<i class="bi bi-' + (collapsed ? 'arrows-angle-expand' : 'arrows-angle-contract') + '"></i>' +
                '</button>' +
                '</div>';
        });
        container.innerHTML = html;
    }

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

        // --- Union-Find helpers for connected-component detection ---
        var _ufParent = {};
        function _ufFind(x) {
            if (_ufParent[x] === undefined) _ufParent[x] = x;
            while (_ufParent[x] !== x) {
                _ufParent[x] = _ufParent[_ufParent[x]];
                x = _ufParent[x];
            }
            return x;
        }
        function _ufUnion(a, b) {
            var ra = _ufFind(a), rb = _ufFind(b);
            if (ra !== rb) _ufParent[ra] = rb;
        }

        // Build type-based lookup: class name/URI -> group name
        // Groups contain class names, but graph nodes are entity instances whose
        // `type` field holds the class name. We match on type, not on node id.
        var _collapsedTypeToGroup = {};
        var _collapsedInstanceToSuper = {};
        var _superNodeIds = new Set();

        if (_groupsDefs.length > 0) {
            _groupsDefs.forEach(function (g) {
                if (!_collapsedGroups.has(g.name)) return;
                (g.memberUris || []).forEach(function (uri) { _collapsedTypeToGroup[uri] = g.name; });
                (g.members || []).forEach(function (m) { _collapsedTypeToGroup[m] = g.name; });
            });
        }

        function _groupForType(nodeType, nodeTypeUri) {
            if (!nodeType && !nodeTypeUri) return null;
            return _collapsedTypeToGroup[nodeType] || _collapsedTypeToGroup[nodeTypeUri] ||
                   _collapsedTypeToGroup[_extractLocalName(nodeTypeUri || '')] || null;
        }

        function _resolveNodeId(nodeId) {
            if (_collapsedInstanceToSuper[nodeId]) return _collapsedInstanceToSuper[nodeId];
            return nodeId;
        }

        // --- Pre-scan: collect group member instances and run union-find ---
        var _groupInstances = {};   // groupName -> { nodeId: node, ... }
        var _instanceGroup = {};    // nodeId -> groupName

        nodes.forEach(function (n) {
            if (!n || !n.id) return;
            if (filterIds && !filterIds.has(n.id)) return;
            if (hideOrphans && !connectedIds.has(n.id)) return;
            var gName = _groupForType(n.type || '', n.typeUri || '');
            if (gName && !_expandedInstanceMembers.has(n.id)) {
                if (!_groupInstances[gName]) _groupInstances[gName] = {};
                _groupInstances[gName][n.id] = n;
                _instanceGroup[n.id] = gName;
            }
        });

        links.forEach(function (l) {
            var s = typeof l.source === 'object' ? l.source.id : l.source;
            var t = typeof l.target === 'object' ? l.target.id : l.target;
            var gs = _instanceGroup[s], gt = _instanceGroup[t];
            if (gs && gs === gt) _ufUnion(s, t);
        });

        // --- Build connected components and create one super-node per cluster ---
        var _groupDefs = {};
        _groupsDefs.forEach(function (g) { _groupDefs[g.name] = g; });

        for (var gName in _groupInstances) {
            var instances = _groupInstances[gName];
            var components = {};
            for (var nid in instances) {
                var root = _ufFind(nid);
                if (!components[root]) components[root] = [];
                components[root].push(nid);
            }
            var g = _groupDefs[gName];
            var color = (g && g.color) || '#4A90D9';
            var groupIcon = (g && g.icon) || '📁';
            var groupLabel = (g && g.label) || gName;
            var compIdx = 0;
            var compKeys = Object.keys(components);
            var multiComponent = compKeys.length > 1;
            compKeys.forEach(function (root) {
                var memberIds = components[root];
                var superNodeId = '__group__' + gName + '__' + compIdx;
                compIdx++;
                _superNodeIds.add(superNodeId);
                var instanceCount = memberIds.length;
                var label = groupIcon + ' ' + groupLabel +
                    (multiComponent ? ' #' + compIdx : '') +
                    ' (' + instanceCount + ')';
                try {
                    graph.addNode(superNodeId, {
                        label: label,
                        entityType: '__group__',
                        icon: groupIcon,
                        color: color,
                        size: Math.min(14 + instanceCount, 24),
                        _isGroup: true,
                        _groupName: gName,
                        _memberIds: memberIds,
                        _data: { id: superNodeId, label: groupLabel, type: '__group__' }
                    });
                    addedNodes.add(superNodeId);
                    _visibleTypes.add('__group__');
                } catch (_) {}
                memberIds.forEach(function (mid) {
                    _collapsedInstanceToSuper[mid] = superNodeId;
                });
            });
        }

        // --- Cluster collapse: build super-nodes for collapsed clusters ---
        var _clusterInstanceToSuper = {};
        if (_clusterCollapseActive && _clusterAssignments && _collapsedClusters.size > 0) {
            var _clusterMembers = {};
            nodes.forEach(function (n) {
                if (!n || !n.id) return;
                if (filterIds && !filterIds.has(n.id)) return;
                if (hideOrphans && !connectedIds.has(n.id)) return;
                if (_instanceGroup[n.id]) return;
                var cid = _clusterAssignments[n.id];
                if (cid !== undefined && _collapsedClusters.has(cid)) {
                    if (!_clusterMembers[cid]) _clusterMembers[cid] = [];
                    _clusterMembers[cid].push(n.id);
                }
            });

            for (var cid in _clusterMembers) {
                var memberIds = _clusterMembers[cid];
                var cidInt = parseInt(cid);
                var color = _clusterColor(cidInt);
                var superNodeId = '__cluster__' + cid;
                _superNodeIds.add(superNodeId);
                var label = '🔵 Cluster #' + cid + ' (' + memberIds.length + ')';
                try {
                    graph.addNode(superNodeId, {
                        label: label,
                        entityType: '__cluster__',
                        color: color,
                        size: Math.min(14 + memberIds.length, 24),
                        _isClusterNode: true,
                        _clusterId: cidInt,
                        _memberIds: memberIds,
                        _data: { id: superNodeId, label: 'Cluster #' + cid, type: '__cluster__' }
                    });
                    addedNodes.add(superNodeId);
                    _visibleTypes.add('__cluster__');
                } catch (_) {}
                memberIds.forEach(function (mid) {
                    _clusterInstanceToSuper[mid] = superNodeId;
                    _collapsedInstanceToSuper[mid] = superNodeId;
                });
            }
        }

        // --- Add non-group nodes ---
        nodes.forEach(function (n) {
            if (!n || !n.id) return;
            if (filterIds && !filterIds.has(n.id)) return;
            if (hideOrphans && !connectedIds.has(n.id)) return;
            if (_instanceGroup[n.id]) return;
            if (_clusterInstanceToSuper[n.id]) return;

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

        var _addedEdgeKeys = new Set();
        links.forEach(function (l) {
            var s = typeof l.source === 'object' ? l.source.id : l.source;
            var t = typeof l.target === 'object' ? l.target.id : l.target;

            // Redirect edges to super-nodes for collapsed groups
            s = _resolveNodeId(s);
            t = _resolveNodeId(t);

            if (!graph.hasNode(s) || !graph.hasNode(t)) return;
            if (s === t) return; // skip self-loops caused by collapsing

            var pred = l.predicate || '';
            _visibleEdgeTypes.add(pred);
            var isInferred = !!(l.provenance || l.inferred);
            var edgeColor = isInferred ? '#4ECDC4' : '#bbb';
            var edgeSize = isInferred ? 2.5 : 1.5;

            // Deduplicate edges to/from super-nodes
            var edgeKey = s + '|' + t + '|' + pred;
            if (_superNodeIds.has(s) || _superNodeIds.has(t)) {
                if (_addedEdgeKeys.has(edgeKey)) return;
                _addedEdgeKeys.add(edgeKey);
            }

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
    var _savedPositions = null;

    function _savePositions() {
        if (!_graph) { _savedPositions = null; return; }
        var pos = {};
        _graph.forEachNode(function (node, attrs) {
            if (attrs.x !== undefined && attrs.y !== undefined) {
                pos[node] = { x: attrs.x, y: attrs.y };
            }
        });
        _savedPositions = pos;
    }

    function _applyLayout(graph, isGroupToggle) {
        if (!graph || graph.order === 0) return;

        var fa2 = (typeof graphologyLibrary !== 'undefined' && graphologyLibrary.layoutForceAtlas2)
            ? graphologyLibrary.layoutForceAtlas2
            : (typeof ForceAtlas2 !== 'undefined' ? ForceAtlas2 : null);

        if (isGroupToggle && _savedPositions) {
            var groupCentroids = {};
            for (var sn in _savedPositions) {
                if (sn.indexOf('__group__') !== 0) continue;
                var parts = sn.split('__');
                var gn = parts.length >= 4 ? parts[2] : '';
                if (!gn) continue;
                if (!groupCentroids[gn]) groupCentroids[gn] = { x: 0, y: 0, c: 0 };
                groupCentroids[gn].x += _savedPositions[sn].x;
                groupCentroids[gn].y += _savedPositions[sn].y;
                groupCentroids[gn].c++;
            }
            for (var gn in groupCentroids) {
                groupCentroids[gn].x /= groupCentroids[gn].c;
                groupCentroids[gn].y /= groupCentroids[gn].c;
            }

            var newNodes = [];
            graph.forEachNode(function (node, attrs) {
                var saved = _savedPositions[node];
                if (saved) {
                    graph.setNodeAttribute(node, 'x', saved.x);
                    graph.setNodeAttribute(node, 'y', saved.y);
                } else {
                    newNodes.push(node);
                }
            });

            if (newNodes.length > 0) {
                newNodes.forEach(function (node) {
                    var attrs = graph.getNodeAttributes(node);
                    var refX = 0, refY = 0, refCount = 0;

                    if (attrs._isGroup && attrs._memberIds) {
                        attrs._memberIds.forEach(function (mid) {
                            var p = _savedPositions[mid];
                            if (p) { refX += p.x; refY += p.y; refCount++; }
                        });
                    }

                    if (refCount === 0) {
                        graph.forEachNeighbor(node, function (neighbor) {
                            var na = graph.getNodeAttributes(neighbor);
                            if (na.x !== undefined && na.y !== undefined) {
                                refX += na.x; refY += na.y; refCount++;
                            }
                        });
                    }

                    if (refCount === 0) {
                        var nodeGroup = _groupMemberMap[attrs.entityType] || null;
                        if (nodeGroup && groupCentroids[nodeGroup]) {
                            refX = groupCentroids[nodeGroup].x;
                            refY = groupCentroids[nodeGroup].y;
                            refCount = 1;
                        } else if (attrs._isGroup && attrs._groupName && groupCentroids[attrs._groupName]) {
                            refX = groupCentroids[attrs._groupName].x;
                            refY = groupCentroids[attrs._groupName].y;
                            refCount = 1;
                        }
                    }

                    if (refCount > 0) {
                        var jitter = 40;
                        graph.setNodeAttribute(node, 'x', refX / refCount + (Math.random() - 0.5) * jitter);
                        graph.setNodeAttribute(node, 'y', refY / refCount + (Math.random() - 0.5) * jitter);
                    } else {
                        graph.setNodeAttribute(node, 'x', Math.random() * 1000);
                        graph.setNodeAttribute(node, 'y', Math.random() * 1000);
                    }
                });

                if (fa2 && fa2.assign) {
                    fa2.assign(graph, {
                        iterations: 30,
                        settings: {
                            gravity: 1,
                            scalingRatio: 10,
                            barnesHutOptimize: graph.order > 500,
                            strongGravityMode: true,
                            slowDown: 10
                        }
                    });
                }
            }
            _savedPositions = null;
            return;
        }

        graph.forEachNode(function (node) {
            graph.setNodeAttribute(node, 'x', Math.random() * 1000);
            graph.setNodeAttribute(node, 'y', Math.random() * 1000);
        });

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

    function _render(filterIds, isGroupToggle) {
        var container = document.getElementById('sgContainer');
        var loading = document.getElementById('sgLoading');
        if (!container) { console.warn('[SigmaGraph] container #sgContainer not found'); _hideLoading(); return; }
        _hideEmptyState();

        // Ensure groups are loaded before first render
        if (!_groupsLoaded) {
            _loadGroups().then(function () { _render(filterIds); });
            return;
        }

        var SigmaModule = (typeof Sigma !== 'undefined') ? Sigma : null;
        if (!SigmaModule) { console.error('[SigmaGraph] Sigma library not loaded'); _hideLoading(); return; }
        var SigmaClass = (typeof SigmaModule === 'function') ? SigmaModule : (SigmaModule.Sigma || SigmaModule.default || null);
        if (!SigmaClass) { console.error('[SigmaGraph] Could not find Sigma constructor in', Object.keys(SigmaModule)); _hideLoading(); return; }

        var savedCamera = null;
        if (isGroupToggle) {
            _savePositions();
            if (_renderer) {
                try { savedCamera = _renderer.getCamera().getState(); } catch (_) {}
            }
        }

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

        _applyLayout(_graph, isGroupToggle);

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
            _switchToTab('sgTabDetails');
            _showNodeDetails(e.node);
            _renderer.refresh();
        });

        _renderer.on('doubleClickNode', function (e) {
            var attrs = _graph.getNodeAttributes(e.node);
            if (attrs && attrs._isGroup && attrs._groupName) {
                SigmaGraph.expandInstance(e.node);
            } else if (_expandedInstanceMembers.has(e.node)) {
                SigmaGraph.collapseInstance(e.node);
            } else {
                var groupName = _nodeGroupName(attrs && attrs._data) ||
                    _groupMemberMap[attrs && attrs.entityType] || null;
                if (groupName && !_collapsedGroups.has(groupName)) {
                    SigmaGraph.toggleGroup(groupName);
                }
            }
            if (e.preventSigmaDefault) e.preventSigmaDefault();
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
            _switchToTab('sgTabDetails');
            _showEdgeDetails(e.edge);
            _renderer.refresh();
        });

        _updateStats();
        _populateTypes();
        _populateGroupsPanel();

        if (savedCamera && _renderer) {
            try { _renderer.getCamera().setState(savedCamera); } catch (_) {}
        }

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

        // Group super-nodes: always show label, larger, with border ring
        if (data._isGroup) {
            res.forceLabel = true;
            res.labelSize = 14;
            res.labelWeight = 'bold';
            res.zIndex = 5;
            res.borderColor = data.color || '#4A90D9';
            res.borderSize = 3;
        }

        // Cluster super-nodes
        if (data._isClusterNode) {
            res.forceLabel = true;
            res.labelSize = 14;
            res.labelWeight = 'bold';
            res.zIndex = 5;
            res.borderColor = data.color || '#999';
            res.borderSize = 3;
        }

        // Color by cluster mode
        if (_clusterColorByActive && _clusterAssignments && !data._isGroup && !data._isClusterNode) {
            var cid = _clusterAssignments[node];
            if (cid !== undefined) {
                res.color = _clusterColor(cid);
            }
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
    function _switchToTab(tabId) {
        var tab = document.getElementById(tabId);
        if (tab && typeof bootstrap !== 'undefined') {
            bootstrap.Tab.getOrCreateInstance(tab).show();
        }
    }

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

        // --- Group super-node: show group info + member list ---
        if (attrs._isGroup) {
            var esc = (typeof escapeHtml === 'function') ? escapeHtml : _esc;
            var gIcon = attrs.icon || '📁';
            var gLabel = (attrs._data && attrs._data.label) || attrs._groupName || '';
            var gColor = attrs.color || '#4A90D9';
            var memberIds = attrs._memberIds || [];
            var nodes = (typeof d3NodesData !== 'undefined' && d3NodesData) ? d3NodesData : [];
            var nodeMap = {};
            nodes.forEach(function (n) { if (n && n.id) nodeMap[n.id] = n; });

            var html = '<div class="entity-detail-header">' +
                '<span class="entity-detail-icon" style="font-size:1.6rem;">' + gIcon + '</span>' +
                '<div class="entity-detail-title">' +
                '<h6>' + esc(gLabel) + '</h6>' +
                '<small class="text-muted">Group &mdash; ' + memberIds.length + ' entities</small>' +
                '</div></div>';

            html += '<div class="entity-detail-section">' +
                '<h6><i class="bi bi-collection" style="color:' + esc(gColor) + '"></i> Included Entities</h6>';
            if (memberIds.length === 0) {
                html += '<p class="small text-muted mb-0">No entities in this cluster.</p>';
            } else {
                memberIds.forEach(function (mid) {
                    var n = nodeMap[mid];
                    var mLabel = n ? ((typeof getDisplayLabel === 'function') ? getDisplayLabel(n) : (n.label || _extractLocalName(mid))) : _extractLocalName(mid);
                    var mIcon = n ? ((typeof getEntityIcon === 'function') ? getEntityIcon(n) : '📦') : '📦';
                    var mType = n ? (n.type || '') : '';
                    html += '<div class="entity-relationship-item">' +
                        mIcon + ' <strong>' + esc(mLabel) + '</strong>' +
                        (mType ? ' <span class="text-muted small">(' + esc(mType) + ')</span>' : '') +
                        '</div>';
                });
            }
            html += '</div>';

            var escapedNodeId = esc(nodeId).replace(/'/g, "\\'");
            html += '<div class="entity-detail-section">' +
                '<button class="btn btn-sm btn-outline-warning w-100" onclick="SigmaGraph.expandInstance(\'' + escapedNodeId + '\')">' +
                '<i class="bi bi-arrows-expand me-1"></i>Expand this cluster</button></div>';

            el.innerHTML = html;
            return;
        }

        // --- Cluster super-node: show cluster info + member list ---
        if (attrs._isClusterNode) {
            var esc = (typeof escapeHtml === 'function') ? escapeHtml : _esc;
            var clusterId = attrs._clusterId;
            var clColor = attrs.color || _clusterColor(clusterId);
            var memberIds = attrs._memberIds || [];
            var nodes = (typeof d3NodesData !== 'undefined' && d3NodesData) ? d3NodesData : [];
            var nodeMap = {};
            nodes.forEach(function (n) { if (n && n.id) nodeMap[n.id] = n; });

            var html = '<div class="entity-detail-header">' +
                '<span class="entity-detail-icon" style="font-size:1.6rem;">🔵</span>' +
                '<div class="entity-detail-title">' +
                '<h6>Cluster #' + clusterId + '</h6>' +
                '<small class="text-muted">Data cluster &mdash; ' + memberIds.length + ' entities</small>' +
                '</div></div>';

            html += '<div class="entity-detail-section">' +
                '<h6><i class="bi bi-people" style="color:' + esc(clColor) + '"></i> Members (' + memberIds.length + ')</h6>' +
                '<div style="max-height:300px; overflow-y:auto;">';
            memberIds.forEach(function (mid) {
                var n = nodeMap[mid];
                var mLabel = n ? ((typeof getDisplayLabel === 'function') ? getDisplayLabel(n) : (n.label || _extractLocalName(mid))) : _extractLocalName(mid);
                var mIcon = n ? ((typeof getEntityIcon === 'function') ? getEntityIcon(n) : '📦') : '📦';
                var mType = n ? (n.type || '') : '';
                var mTypeName = mType ? (mType.indexOf('#') >= 0 ? mType.split('#').pop() : (mType.indexOf('/') >= 0 ? mType.split('/').pop() : mType)) : '';
                html += '<div class="entity-relationship-item">' +
                    mIcon + ' <strong>' + esc(mLabel) + '</strong>' +
                    (mTypeName ? ' <span class="text-muted small">(' + esc(mTypeName) + ')</span>' : '') +
                    '</div>';
            });
            html += '</div></div>';

            var escapedNodeId = esc(nodeId).replace(/'/g, "\\'");
            html += '<div class="entity-detail-section">' +
                '<button class="btn btn-sm btn-outline-warning w-100" onclick="SigmaGraph.expandCluster(' + clusterId + ')">' +
                '<i class="bi bi-arrows-expand me-1"></i>Expand this cluster</button></div>';

            el.innerHTML = html;
            return;
        }

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
            '<span class="detail-value">' + esc(actualIdValue || 'N/A') + '</span></div>';
        if (_clusterAssignments && _clusterAssignments[nodeId] !== undefined) {
            var _cid = _clusterAssignments[nodeId];
            html += '<div class="entity-detail-item"><span class="detail-key"><i class="bi bi-bezier2 text-success"></i> Cluster</span>' +
                '<span class="detail-value"><span class="sg-cluster-dot" style="background:' + _clusterColor(_cid) + ';width:10px;height:10px;display:inline-block;border-radius:50%;vertical-align:middle;margin-right:4px;"></span>' +
                'Cluster #' + _cid + '</span></div>';
        }
        html += '</div>';

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

        // Cross-domain bridges
        var bridges = (entityMapping && entityMapping.bridges) || (classInfo && classInfo.bridges) || [];
        if (bridges.length > 0) {
            html += '<div class="entity-detail-section"><h6><i class="bi bi-signpost-2"></i> Bridges (' + bridges.length + ')</h6>';
            bridges.forEach(function (bridge) {
                var tgtDom = bridge.target_domain || bridge.target_project || '';
                var targetEntityUri = actualIdValue
                    ? (bridge.target_class_uri || '') + '#' + actualIdValue
                    : (bridge.target_class_uri || '');
                var resolveUrl = '/resolve?uri=' + encodeURIComponent(targetEntityUri) +
                    '&domain=' + encodeURIComponent(tgtDom);
                var tooltip = bridge.label || ('Navigate to ' + (bridge.target_class_name || '') + ' in ' + tgtDom);
                html += '<div class="entity-detail-item">' +
                    '<a href="' + esc(resolveUrl) + '" class="btn btn-sm btn-outline-primary w-100 text-start" title="' + esc(tooltip) + '">' +
                    '<i class="bi bi-signpost-2 me-1"></i>' +
                    '<span class="fw-semibold">' + esc(bridge.target_class_name || '') + '</span>' +
                    '<small class="text-muted ms-1"><i class="bi bi-folder2-open ms-1 me-1"></i>' + esc(tgtDom) + '</small>' +
                    '<i class="bi bi-box-arrow-up-right ms-auto float-end mt-1"></i>' +
                    '</a></div>';
            });
            html += '</div>';
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
        if (statsEl) {
            statsEl.classList.remove('d-none');
            statsEl.classList.add('d-flex');
        }
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

    // -- Seed preview modal state ---------------------------------------
    var _seedData = [];
    var _seedSearchTerm = '';

    var _TYPE_COLORS = [
        '#6366F1', '#EC4899', '#14B8A6', '#F59E0B', '#EF4444',
        '#3B82F6', '#8B5CF6', '#10B981', '#F97316', '#06B6D4',
    ];
    var _typeColorMap = {};
    var _typeColorIdx = 0;

    function _typeColor(typeName) {
        if (!_typeColorMap[typeName]) {
            _typeColorMap[typeName] = _TYPE_COLORS[_typeColorIdx % _TYPE_COLORS.length];
            _typeColorIdx++;
        }
        return _typeColorMap[typeName];
    }

    function _updateSeedSelectedCount() {
        var checks = document.querySelectorAll('#sgSeedTableBody .sg-seed-check');
        var checked = 0;
        checks.forEach(function (c) { if (c.checked) checked++; });
        var el = document.getElementById('sgSeedSelectedCount');
        if (el) el.textContent = checked;
        var btn = document.getElementById('sgSeedExploreBtn');
        if (btn) btn.disabled = (checked === 0);
    }

    function _renderSeedTable() {
        var tbody = document.getElementById('sgSeedTableBody');
        if (!tbody) return;
        var filter = _seedSearchTerm.toLowerCase();
        var html = '';
        for (var i = 0; i < _seedData.length; i++) {
            var s = _seedData[i];
            if (filter && s.label.toLowerCase().indexOf(filter) < 0 &&
                s.type.toLowerCase().indexOf(filter) < 0 &&
                s.uri.toLowerCase().indexOf(filter) < 0) {
                continue;
            }
            var color = _typeColor(s.type);
            var shortUri = s.uri.length > 60 ? '...' + s.uri.slice(-57) : s.uri;
            html += '<tr style="cursor:pointer;">'
                + '<td><input class="form-check-input sg-seed-check" type="checkbox" data-uri="' + _esc(s.uri) + '" onchange="SigmaGraph.updateSeedSelection()"></td>'
                + '<td><span class="badge sg-type-badge" style="background:' + color + ';">' + _esc(s.type) + '</span></td>'
                + '<td class="small">' + _esc(s.label) + '</td>'
                + '<td class="small text-muted text-truncate" style="max-width:200px;" title="' + _esc(s.uri) + '">' + _esc(shortUri) + '</td>'
                + '</tr>';
        }
        tbody.innerHTML = html || '<tr><td colspan="4" class="text-center text-muted small py-3">No matches</td></tr>';

        if (!tbody._rowClickBound) {
            tbody.addEventListener('click', function (e) {
                if (e.target.tagName === 'INPUT') return;
                var tr = e.target.closest('tr');
                if (!tr) return;
                var cb = tr.querySelector('.sg-seed-check');
                if (cb) {
                    cb.checked = !cb.checked;
                    _updateSeedSelectedCount();
                }
            });
            tbody._rowClickBound = true;
        }

        _updateSeedSelectedCount();
    }

    function _toggleSeedSelectAll() {
        var allCb = document.getElementById('sgSeedSelectAll');
        var checked = allCb ? allCb.checked : true;
        document.querySelectorAll('#sgSeedTableBody .sg-seed-check').forEach(function (c) {
            c.checked = checked;
        });
        _updateSeedSelectedCount();
    }

    function _filterSeedTable() {
        _seedSearchTerm = (document.getElementById('sgSeedQuickFilter')?.value || '').trim();
        _renderSeedTable();
    }

    function _getSelectedSeedUris() {
        var uris = [];
        document.querySelectorAll('#sgSeedTableBody .sg-seed-check:checked').forEach(function (c) {
            uris.push(c.getAttribute('data-uri'));
        });
        return uris;
    }

    // -- Phase 1: preview search ----------------------------------------
    async function _executeGraphSearch() {
        var entityType = (document.getElementById('sgFilterEntityType')?.value || '').trim();
        var matchType = document.getElementById('sgFilterMatchType')?.value || 'contains';
        var searchValue = (document.getElementById('sgFilterValue')?.value || '').trim();

        if (!searchValue && !entityType) return;

        var info = document.getElementById('sgGraphFilterInfo');
        var text = document.getElementById('sgGraphFilterInfoText');
        if (info && text) { info.classList.remove('d-none'); text.textContent = 'Searching...'; }

        try {
            var resp = await fetch('/dtwin/sync/filter', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    phase: 'preview',
                    entity_type: entityType,
                    field: 'any',
                    match_type: matchType,
                    value: searchValue,
                }),
                credentials: 'same-origin'
            });
            var data = await resp.json();

            if (!data.success) {
                if (info && text) { info.classList.remove('d-none'); text.textContent = data.message || 'Search failed.'; }
                return;
            }

            var seeds = data.seeds || [];
            if (seeds.length === 0) {
                if (info && text) { info.classList.remove('d-none'); text.textContent = data.message || 'No entities found.'; }
                return;
            }

            // Single result: skip the selection modal and render directly
            if (seeds.length === 1) {
                if (info && text) { info.classList.remove('d-none'); text.textContent = '1 entity found — loading graph...'; }
                try {
                    await _expandAndRenderGraph([seeds[0].uri]);
                } catch (err) {
                    console.error('[SigmaGraph] single-seed expand error:', err);
                    if (info && text) { info.classList.remove('d-none'); text.textContent = 'Error: ' + err.message; }
                }
                return;
            }

            _seedData = seeds;
            _seedSearchTerm = '';
            _typeColorMap = {};
            _typeColorIdx = 0;
            var quickFilter = document.getElementById('sgSeedQuickFilter');
            if (quickFilter) quickFilter.value = '';
            var selectAll = document.getElementById('sgSeedSelectAll');
            if (selectAll) selectAll.checked = false;

            var countEl = document.getElementById('sgSeedCount');
            if (countEl) countEl.textContent = seeds.length;

            var cappedWarn = document.getElementById('sgSeedCappedWarning');
            var totalEl = document.getElementById('sgSeedTotalCount');
            if (data.capped && cappedWarn) {
                if (totalEl) totalEl.textContent = data.total;
                cappedWarn.classList.remove('d-none');
            } else if (cappedWarn) {
                cappedWarn.classList.add('d-none');
            }

            _renderSeedTable();

            var modalEl = document.getElementById('sgSeedPreviewModal');
            if (modalEl) {
                var modal = bootstrap.Modal.getOrCreateInstance(modalEl);
                modal.show();
            }

            if (info && text) {
                info.classList.remove('d-none');
                text.textContent = seeds.length + ' entities found' + (data.capped ? ' (showing first 500 of ' + data.total + ')' : '') + '. Select entities to explore.';
            }

        } catch (err) {
            console.error('[SigmaGraph] _executeGraphSearch error:', err);
            if (info && text) { info.classList.remove('d-none'); text.textContent = 'Error: ' + err.message; }
        }
    }

    // -- Shared expand + render pipeline ----------------------------------
    async function _expandAndRenderGraph(uris, opts) {
        opts = opts || {};
        var maxDepth = parseInt(document.getElementById('sgFilterDepth')?.value || '3');
        var maxEntities = parseInt(document.getElementById('sgMaxEntities')?.value || '5000');
        var highlightTerm = opts.highlightTerm || (document.getElementById('sgFilterValue')?.value || '').trim();

        var info = document.getElementById('sgGraphFilterInfo');
        var text = document.getElementById('sgGraphFilterInfoText');
        if (info && text) { info.classList.remove('d-none'); text.textContent = 'Expanding ' + uris.length + ' entities...'; }

        var loading = document.getElementById('sgLoading');
        if (loading) loading.style.display = 'flex';
        _hideEmptyState();

        var resp = await fetch('/dtwin/sync/filter', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                phase: 'expand',
                selected_uris: uris,
                include_rels: true,
                depth: maxDepth,
                max_entities: maxEntities,
            }),
            credentials: 'same-origin'
        });
        var data = await resp.json();

        if (!data.success) {
            _hideLoading();
            if (info && text) { info.classList.remove('d-none'); text.textContent = data.message || 'Expansion failed.'; }
            return null;
        }

        if (!data.results || data.results.length === 0) {
            _hideLoading();
            if (info && text) { info.classList.remove('d-none'); text.textContent = 'No triples found for the selected entities.'; }
            return null;
        }

        lastQueryResults = { results: data.results, columns: data.columns };

        var libsOk = await _waitForGraphLibs(10000);
        if (!libsOk) {
            _hideLoading();
            if (info && text) { info.classList.remove('d-none'); text.textContent = 'Graph libraries failed to load. Check your network and reload.'; }
            return null;
        }

        if (typeof buildGraph === 'function') {
            await buildGraph(data.results, data.columns);
        }

        _graphFilterActive = true;
        _searchMatched = null;
        _searchNeighbors = null;
        _selectedNode = null;
        _hoveredNode = null;
        if (highlightTerm) _pendingHighlightTerm = highlightTerm;
        _render();

        setTimeout(function () { _applyPendingHighlight(); }, 200);

        var initialCount = data.initial_count || 0;
        var expandedCount = data.expanded_count || 0;
        var relatedCount = expandedCount - initialCount;
        if (info && text) {
            info.classList.remove('d-none');
            var msg;
            if (relatedCount > 0) {
                msg = expandedCount + ' entities (' + initialCount + ' selected, ' + relatedCount + ' related at ' + maxDepth + ' level' + (maxDepth > 1 ? 's' : '') + ').';
            } else {
                msg = initialCount + ' entities (' + (data.count || 0) + ' triples).';
            }
            if (data.capped) {
                msg += ' Results capped at ' + maxEntities.toLocaleString() + ' entities.';
            }
            text.textContent = msg;
        }
        var clearBtn = document.getElementById('sgClearGraphFilterBtn');
        if (clearBtn) {
            clearBtn.classList.remove('d-none');
            clearBtn.classList.add('d-inline-block');
        }

        return data;
    }

    // -- Phase 2: expand selected seeds ---------------------------------
    async function _expandSelectedSeeds() {
        var selectedUris = _getSelectedSeedUris();
        if (selectedUris.length === 0) return;

        var modalEl = document.getElementById('sgSeedPreviewModal');
        if (modalEl) {
            var modal = bootstrap.Modal.getInstance(modalEl);
            if (modal) modal.hide();
        }

        try {
            await _expandAndRenderGraph(selectedUris);
        } catch (err) {
            console.error('[SigmaGraph] _expandSelectedSeeds error:', err);
            _hideLoading();
            var info = document.getElementById('sgGraphFilterInfo');
            var text = document.getElementById('sgGraphFilterInfoText');
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
        if (clearBtn) {
            clearBtn.classList.add('d-none');
            clearBtn.classList.remove('d-inline-block');
        }
        var val = document.getElementById('sgFilterValue');
        if (val) val.value = '';
        var sel = document.getElementById('sgFilterEntityType');
        if (sel) sel.value = '';

        var statsEl = document.getElementById('sgStats');
        if (statsEl) {
            statsEl.classList.add('d-none');
            statsEl.classList.remove('d-flex');
        }

        _showPlaceholder();
        _showEmptyState();
    }

    // -----------------------------------------------------------
    // Public API
    // -----------------------------------------------------------
    function init(focusUri) {
        console.log('[SigmaGraph] init called, focusUri:', focusUri || 'none');
        _pendingFocusLoad = !!focusUri;
        _loadGraphLibs();

        if (!_graphFilterActive) {
            d3NodesData = [];
            d3LinksData = [];
            if (_renderer) {
                try { _renderer.kill(); } catch (_) {}
                _renderer = null;
            }
            _graph = null;
            if (focusUri) {
                _hideEmptyState();
                var loading = document.getElementById('sgLoading');
                if (loading) loading.style.display = 'flex';
            } else {
                _showEmptyState();
            }
        } else {
            _render();
        }
        _initialized = true;

        if (focusUri) {
            _waitForGraphLibs(10000).then(function () {
                SigmaGraph.focusEntityByUri(focusUri).finally(function () {
                    _pendingFocusLoad = false;
                });
            });
        }
    }

    function _hasData() {
        return typeof d3NodesData !== 'undefined' && d3NodesData && d3NodesData.length > 0;
    }

    function _showEmptyState() {
        if (_pendingFocusLoad) return;
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
                    '<i class="bi bi-diagram-3 " style="font-size:2.5rem;"></i>' +
                    '<p class="mt-2 mb-1 fw-semibold">Knowledge Graph</p>' +
                    '<p class="small">Use the filter panel to search and explore entities.</p>' +
                    '</div>';
                container.appendChild(placeholder);
            }
            placeholder.style.display = '';
        }
        _switchToTab('sgTabFilter');
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
        refresh: function (isGroupToggle) { _render(undefined, isGroupToggle); },

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

        focusEntityByUri: async function (uri) {
            if (!uri) return false;

            var localName = uri;
            if (uri.indexOf('#') !== -1) localName = uri.split('#').pop();
            else if (uri.indexOf('/') !== -1) {
                var parts = uri.split('/');
                localName = parts[parts.length - 1] || parts[parts.length - 2] || '';
            }
            if (!localName) return false;

            var filterInput = document.getElementById('sgFilterValue');
            if (filterInput) filterInput.value = localName;
            var matchSel = document.getElementById('sgFilterMatchType');
            if (matchSel) matchSel.value = 'contains';

            var info = document.getElementById('sgGraphFilterInfo');
            var text = document.getElementById('sgGraphFilterInfoText');
            if (info && text) { info.classList.remove('d-none'); text.textContent = 'Searching...'; }

            try {
                var previewResp = await fetch('/dtwin/sync/filter', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        phase: 'preview',
                        entity_type: '',
                        field: 'any',
                        match_type: 'contains',
                        value: localName,
                    }),
                    credentials: 'same-origin'
                });
                var previewData = await previewResp.json();

                if (!previewData.success || !(previewData.seeds || []).length) {
                    if (info && text) { info.classList.remove('d-none'); text.textContent = previewData.message || 'No entities found.'; }
                    return false;
                }

                var seedUris = previewData.seeds.map(function (s) { return s.uri; });
                var data = await _expandAndRenderGraph(seedUris, { highlightTerm: localName });
                if (!data) return false;

                if (_graph) {
                    var found = null;
                    if (_graph.hasNode(uri)) {
                        found = uri;
                    } else {
                        _graph.forEachNode(function (nodeId, attrs) {
                            if (found) return;
                            var nodeUri = (attrs._data && attrs._data.uri) ? attrs._data.uri : '';
                            if (nodeUri === uri) { found = nodeId; return; }
                            var label = (attrs._data && attrs._data.label) ? attrs._data.label : (attrs.label || '');
                            if (label.toLowerCase() === localName.toLowerCase() || nodeId.toLowerCase() === localName.toLowerCase()) {
                                found = nodeId;
                            }
                        });
                    }
                    if (found) {
                        SigmaGraph.selectEntity(found);
                    }
                }
                return true;
            } catch (err) {
                console.error('[SigmaGraph] focusEntityByUri error:', err);
                _hideLoading();
                if (info && text) { info.classList.remove('d-none'); text.textContent = 'Error: ' + err.message; }
                return false;
            }
        },

        zoomIn: function () { if (_renderer) { var c = _renderer.getCamera(); c.animatedZoom({ duration: 200 }); } },
        zoomOut: function () { if (_renderer) { var c = _renderer.getCamera(); c.animatedUnzoom({ duration: 200 }); } },
        fitToView: function () { if (_renderer) { var c = _renderer.getCamera(); c.animatedReset({ duration: 300 }); } },
        resetLayout: function () { if (_graph) { _applyLayout(_graph); if (_renderer) _renderer.refresh(); } },

        toggleLabels: function () { if (_renderer) _renderer.setSetting('renderLabels', document.getElementById('sgShowLabels')?.checked !== false); },
        toggleEdgeLabels: function () { if (_renderer) _renderer.setSetting('renderEdgeLabels', document.getElementById('sgShowEdgeLabels')?.checked !== false); },

        toggleTypesPanel: function () {
            _switchToTab('sgTabView');
            _populateTypes();
        },

        toggleGroupsPanel: function () {
            _switchToTab('sgTabView');
            _loadGroups();
        },

        toggleFilterPane: function () {
            _switchToTab('sgTabFind');
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
            var typeFilter = '';
            var query = (document.getElementById('sgSearchValue')?.value || '').toLowerCase().trim();

            if (!query) {
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
            if (clearBtn) {
                clearBtn.classList.remove('d-none');
                clearBtn.classList.add('d-inline-block');
            }

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
            if (clearBtn) {
                clearBtn.classList.add('d-none');
                clearBtn.classList.remove('d-inline-block');
            }
            var val = document.getElementById('sgSearchValue');
            if (val) val.value = '';
            if (_renderer) {
                _renderer.getCamera().animatedReset({ duration: 300 });
                _renderer.refresh();
            }
        },

        toggleGraphFilterPane: function () {
            _switchToTab('sgTabFilter');
            _populateFilterEntityTypes();
        },

        populateFilterEntityTypes: function () { _populateFilterEntityTypes(); },
        executeGraphFilter: function () { _executeGraphSearch(); },
        executeGraphSearch: function () { _executeGraphSearch(); },
        updateFilterDepthLabel: function () {
            var depthEl = document.getElementById('sgFilterDepth');
            var labelEl = document.getElementById('sgFilterDepthValue');
            if (labelEl && depthEl) labelEl.textContent = depthEl.value;
        },
        expandSelectedSeeds: function () { _expandSelectedSeeds(); },
        toggleSeedSelectAll: function () { _toggleSeedSelectAll(); },
        filterSeedTable: function () { _filterSeedTable(); },
        updateSeedSelection: function () { _updateSeedSelectedCount(); },
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
        },

        // --- Group expand / collapse ---

        expandInstance: function (superNodeId) {
            if (!_graph || !_graph.hasNode(superNodeId)) return;
            var attrs = _graph.getNodeAttributes(superNodeId);
            if (!attrs || !attrs._memberIds) return;
            attrs._memberIds.forEach(function (mid) {
                _expandedInstanceMembers.add(mid);
            });
            _selectedNode = null;
            _showPlaceholder();
            SigmaGraph.refresh(true);
        },

        collapseInstance: function (nodeId) {
            if (!_graph || !_graph.hasNode(nodeId)) return;
            var attrs = _graph.getNodeAttributes(nodeId);
            var groupName = _groupMemberMap[attrs && attrs.entityType] || null;
            if (!groupName) return;

            var component = new Set([nodeId]);
            var queue = [nodeId];
            while (queue.length > 0) {
                var current = queue.shift();
                _graph.forEachNeighbor(current, function (neighbor) {
                    if (component.has(neighbor)) return;
                    var na = _graph.getNodeAttributes(neighbor);
                    var nGroup = _groupMemberMap[na && na.entityType] || null;
                    if (nGroup === groupName && _expandedInstanceMembers.has(neighbor)) {
                        component.add(neighbor);
                        queue.push(neighbor);
                    }
                });
            }

            component.forEach(function (mid) {
                _expandedInstanceMembers.delete(mid);
            });
            _selectedNode = null;
            _showPlaceholder();
            SigmaGraph.refresh(true);
        },

        toggleGroup: function (groupName) {
            if (_collapsedGroups.has(groupName)) {
                _collapsedGroups.delete(groupName);
            } else {
                _collapsedGroups.add(groupName);
            }
            _clearExpandedForGroup(groupName);
            _selectedNode = null;
            _showPlaceholder();
            _populateGroupsPanel();
            SigmaGraph.refresh(true);
        },

        collapseAllGroups: function () {
            _groupsDefs.forEach(function (g) { _collapsedGroups.add(g.name); });
            _expandedInstanceMembers.clear();
            _populateGroupsPanel();
            SigmaGraph.refresh(true);
        },

        expandAllGroups: function () {
            _collapsedGroups.clear();
            _expandedInstanceMembers.clear();
            _populateGroupsPanel();
            SigmaGraph.refresh(true);
        },

        reloadGroups: function () {
            _loadGroups().then(function () {
                if (_hasData()) SigmaGraph.refresh();
            });
        },

        // --- Data cluster detection ---

        detectClusters: function (resolution) {
            if (!_graph || _graph.order === 0) {
                if (typeof showNotification === 'function') showNotification('No graph data loaded.', 'warning');
                return;
            }
            _clusterResolution = resolution || parseFloat(document.getElementById('sgClusterResolution')?.value) || 1.0;
            _detectClusters(_clusterResolution);
            if (_clusterAssignments && _clusterCount > 0) {
                if (typeof showNotification === 'function') {
                    showNotification('Detected ' + _clusterCount + ' clusters (resolution ' + _clusterResolution.toFixed(2) + ')', 'info');
                }
                if (_renderer) _renderer.refresh();
            } else {
                if (typeof showNotification === 'function') showNotification('No clusters detected.', 'warning');
            }
        },

        clearClusters: function () {
            _clearClusters();
            var colorCheck = document.getElementById('sgClusterColorBy');
            if (colorCheck) colorCheck.checked = false;
            if (typeof showNotification === 'function') showNotification('Clusters cleared.', 'info');
        },

        toggleClusterColorBy: function () {
            var checked = document.getElementById('sgClusterColorBy')?.checked || false;
            _clusterColorByActive = checked;
            if (_renderer) _renderer.refresh();
        },

        toggleClusterCollapse: function (clusterId) {
            if (_collapsedClusters.has(clusterId)) {
                _collapsedClusters.delete(clusterId);
            } else {
                _collapsedClusters.add(clusterId);
            }
            _clusterCollapseActive = _collapsedClusters.size > 0;
            _populateClustersPanel();
            SigmaGraph.refresh(true);
        },

        collapseAllClusters: function () {
            if (!_clusterAssignments) return;
            for (var nodeId in _clusterAssignments) {
                _collapsedClusters.add(_clusterAssignments[nodeId]);
            }
            _clusterCollapseActive = true;
            _populateClustersPanel();
            SigmaGraph.refresh(true);
        },

        expandAllClusters: function () {
            _collapsedClusters.clear();
            _clusterCollapseActive = false;
            _populateClustersPanel();
            SigmaGraph.refresh(true);
        },

        expandCluster: function (clusterId) {
            _collapsedClusters.delete(clusterId);
            _clusterCollapseActive = _collapsedClusters.size > 0;
            _selectedNode = null;
            _showPlaceholder();
            _populateClustersPanel();
            SigmaGraph.refresh(true);
        },

        detectClustersBackend: async function () {
            if (typeof showNotification === 'function') showNotification('Detecting clusters on full graph...', 'info');
            var algorithm = document.getElementById('sgClusterAlgorithm')?.value || 'louvain';
            var resolution = parseFloat(document.getElementById('sgClusterResolution')?.value) || 1.0;
            try {
                var resp = await fetch('/dtwin/clusters/detect', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'same-origin',
                    body: JSON.stringify({ algorithm: algorithm, resolution: resolution })
                });
                var data = await resp.json();
                if (!data.success) {
                    if (typeof showNotification === 'function') showNotification(data.message || 'Detection failed', 'danger');
                    return;
                }
                var clusters = data.clusters || [];
                _clusterAssignments = {};
                _clusterCount = clusters.length;
                clusters.forEach(function (cl) {
                    (cl.members || []).forEach(function (uri) {
                        _clusterAssignments[uri] = cl.id;
                    });
                });
                if (_graph) {
                    _graph.forEachNode(function (node) {
                        if (_clusterAssignments[node] !== undefined) {
                            _graph.setNodeAttribute(node, '_cluster', _clusterAssignments[node]);
                        }
                    });
                }
                _populateClustersPanel();
                if (_renderer) _renderer.refresh();
                var stats = data.stats || {};
                if (typeof showNotification === 'function') {
                    showNotification(
                        'Backend: ' + _clusterCount + ' clusters detected (' +
                        (stats.node_count || '?') + ' nodes, ' +
                        (stats.elapsed_ms || '?') + 'ms)',
                        'success'
                    );
                }
            } catch (e) {
                console.error('[SigmaGraph] Backend cluster detection error:', e);
                if (typeof showNotification === 'function') showNotification('Backend detection error: ' + e.message, 'danger');
            }
        },

        updateClusterResolutionLabel: function () {
            var slider = document.getElementById('sgClusterResolution');
            var label = document.getElementById('sgClusterResolutionValue');
            if (slider && label) label.textContent = parseFloat(slider.value).toFixed(2);
        },

        openGraphSwitcher: function () { _openGraphSwitcherModal(); }
    };
})();

// =====================================================
// GRAPH SWITCHER MODAL (global scope for onclick access)
// =====================================================

async function _openGraphSwitcherModal() {
    var modalId = 'graphSwitcherModal';
    var existing = document.getElementById(modalId);
    if (existing) existing.remove();

    var currentGraph = (window.__TRIPLESTORE_CONFIG && window.__TRIPLESTORE_CONFIG.graph_name) || '';
    var esc = (typeof escapeHtml === 'function') ? escapeHtml : function (s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; };

    var html = '<div class="modal fade" id="' + modalId + '" tabindex="-1" data-bs-backdrop="static">' +
        '<div class="modal-dialog modal-lg modal-dialog-centered" style="max-height:50vh;">' +
        '<div class="modal-content" style="max-height:50vh;">' +
        '<div class="modal-header flex-shrink-0">' +
        '<h5 class="modal-title"><i class="bi bi-arrow-left-right me-2"></i>Switch Domain</h5>' +
        '<button type="button" class="btn-close" data-bs-dismiss="modal"></button>' +
        '</div>' +
        '<div class="modal-body" style="overflow-y:auto;">' +
        '<div class="mb-3 p-2 bg-light rounded border">' +
        '<small class="text-muted">Current graph:</small> ' +
        '<code class="text-primary fw-semibold">' + esc(currentGraph) + '</code>' +
        '</div>' +
        '<div id="graphSwitcherLoading" class="text-center py-4">' +
        '<div class="spinner-border spinner-border-sm text-primary"></div>' +
        '<span class="ms-2">Loading registry domains...</span>' +
        '</div>' +
        '<div id="graphSwitcherContent" style="display:none;">' +
        '<div id="graphSwitcherList"></div>' +
        '</div>' +
        '</div></div></div></div>';

    document.body.insertAdjacentHTML('beforeend', html);
    var modal = new bootstrap.Modal(document.getElementById(modalId));
    modal.show();

    try {
        var resp = await fetch('/settings/registry/domains', { credentials: 'same-origin' });
        var data = await resp.json();

        document.getElementById('graphSwitcherLoading').style.display = 'none';
        document.getElementById('graphSwitcherContent').style.display = '';

        var list = document.getElementById('graphSwitcherList');
        var rows = data.domains || data.projects || [];
        if (!data.success || !rows.length) {
            list.innerHTML = '<div class="text-muted text-center p-3">No domains found in the registry</div>';
            return;
        }

        var domainsHtml = '';
        rows.forEach(function (p) {
            var versions = p.versions || [];
            if (versions.length === 0) return;

            var versionsHtml = versions.map(function (v) {
                var ver = (typeof v === 'object' && v !== null) ? v.version : v;
                var isActive = (typeof v === 'object' && v !== null) && v.active;
                var graphName = p.name + '_V' + ver;
                var isCurrent = graphName === currentGraph;
                var btnClass = isCurrent ? 'btn-primary disabled' : (isActive ? 'btn-outline-success' : 'btn-outline-primary');
                var badge = isCurrent ? ' <span class="badge bg-success ms-1">current</span>'
                    : (isActive ? ' <span class="badge bg-success-subtle text-success ms-1" style="font-size:.6rem">Active</span>' : '');
                return '<button type="button" class="btn btn-sm ' + btnClass + ' me-1 mb-1" ' +
                    'onclick="_graphSwitcherSelect(\'' + esc(p.name) + '\', \'' + esc(ver) + '\')" ' +
                    'title="Graph: ' + esc(graphName) + '">' +
                    'v' + esc(ver) + badge + '</button>';
            }).join('');

            domainsHtml += '<div class="border rounded p-2 mb-2">' +
                '<div class="d-flex align-items-center gap-2">' +
                '<i class="bi bi-folder2-open text-primary"></i>' +
                '<span class="fw-semibold">' + esc(p.name) + '</span>' +
                '</div>' +
                (p.description ? '<div class="ms-4 mb-1"><small class="text-muted" style="font-size:.8rem">' + esc(p.description) + '</small></div>' : '') +
                (p.base_uri ? '<div class="ms-4 mb-1"><small class="text-muted"><i class="bi bi-link-45deg me-1"></i>URI: <code>' + esc(p.base_uri) + '</code></small></div>' : '') +
                '<div class="ms-4">' + versionsHtml + '</div>' +
                '</div>';
        });

        list.innerHTML = domainsHtml || '<div class="text-muted text-center p-3">No domains with versions found</div>';
    } catch (err) {
        console.error('[GraphSwitcher] Error:', err);
        document.getElementById('graphSwitcherLoading').innerHTML =
            '<div class="text-danger"><i class="bi bi-exclamation-triangle"></i> Failed to load domains</div>';
    }
}

function _closeGraphSwitcherModal() {
    var el = document.getElementById('graphSwitcherModal');
    if (el) {
        var m = bootstrap.Modal.getInstance(el);
        if (m) m.hide();
        el.addEventListener('hidden.bs.modal', function () { el.remove(); }, { once: true });
    }
}

async function _graphSwitcherSelect(domainName, version) {
    var esc = (typeof escapeHtml === 'function') ? escapeHtml : function (s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; };
    var list = document.getElementById('graphSwitcherList');
    if (list) {
        list.innerHTML = '<div class="text-center py-4">' +
            '<div class="spinner-border spinner-border-sm text-primary"></div>' +
            '<span class="ms-2">Loading <strong>' + esc(domainName) + '</strong> v' + esc(version) + '...</span>' +
            '</div>';
    }

    try {
        var resp = await fetch('/domain/load-from-uc', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain: domainName, version: version })
        });
        var data = await resp.json();
        if (data.success) {
            if (typeof fetchCachedInvalidate === 'function') fetchCachedInvalidate('/navbar/state');
            _closeGraphSwitcherModal();
            window.location.href = '/dtwin/?section=sigmagraph';
        } else {
            if (list) list.innerHTML = '<div class="text-danger p-3"><i class="bi bi-exclamation-triangle me-1"></i>' +
                esc(data.message || 'Failed to load domain') + '</div>';
        }
    } catch (err) {
        console.error('[GraphSwitcher] Load error:', err);
        if (list) list.innerHTML = '<div class="text-danger p-3"><i class="bi bi-exclamation-triangle me-1"></i>Failed to load domain</div>';
    }
}

/**
 * Replaces inline onclick/onchange/oninput handlers from _query_sigmagraph.html
 * (section + seed preview modal, which is rendered outside #sigmagraph-section).
 */
document.addEventListener('DOMContentLoaded', function () {
    function _sgUiContains(node) {
        if (!node) return false;
        var sec = document.getElementById('sigmagraph-section');
        var seedModal = document.getElementById('sgSeedPreviewModal');
        return (sec && sec.contains(node)) || (seedModal && seedModal.contains(node));
    }

    document.addEventListener('click', function (e) {
        var btn = e.target.closest('[data-sg-action]');
        if (!btn || !_sgUiContains(btn)) return;
        var act = btn.getAttribute('data-sg-action');
        if (act === 'openOntologyViewer') {
            if (typeof OntologyViewer !== 'undefined' && typeof OntologyViewer.open === 'function') {
                OntologyViewer.open();
            }
            return;
        }
        if (typeof SigmaGraph !== 'undefined' && typeof SigmaGraph[act] === 'function') {
            SigmaGraph[act]();
        }
    });

    document.addEventListener('change', function (e) {
        var el = e.target;
        if (!_sgUiContains(el)) return;
        var m = el.getAttribute('data-sg-change');
        if (m && typeof SigmaGraph !== 'undefined' && typeof SigmaGraph[m] === 'function') {
            SigmaGraph[m]();
        }
    });

    document.addEventListener('input', function (e) {
        var el = e.target;
        if (!_sgUiContains(el)) return;
        var m = el.getAttribute('data-sg-input');
        if (m && typeof SigmaGraph !== 'undefined' && typeof SigmaGraph[m] === 'function') {
            SigmaGraph[m]();
        }
    });

    document.addEventListener('keydown', function (e) {
        if (e.key !== 'Enter') return;
        var el = e.target;
        if (!_sgUiContains(el) || !el || el.tagName !== 'INPUT') return;
        var m = el.getAttribute('data-sg-keyenter');
        if (m && typeof SigmaGraph !== 'undefined' && typeof SigmaGraph[m] === 'function') {
            SigmaGraph[m]();
        }
    });
});
