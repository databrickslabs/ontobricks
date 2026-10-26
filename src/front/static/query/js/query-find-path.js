/**
 * OntoBricks - query-find-path.js
 * Shortest-path highlight on the loaded Explorer graph (undirected BFS).
 */

var ExplorerFindPath = (function () {
    var SUGGEST_LIMIT = 20;

    function _notify(message, type) {
        if (typeof showNotification === 'function') {
            showNotification(message, type);
        }
    }

    function _esc(value) {
        if (typeof escapeHtml === 'function') return escapeHtml(value);
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function _localName(uri) {
        if (typeof extractLocalName === 'function') return extractLocalName(uri);
        var text = String(uri || '');
        if (text.indexOf('#') !== -1) return text.split('#').pop();
        var parts = text.split('/');
        return parts[parts.length - 1] || text;
    }

    function _isInstanceNode(attrs) {
        return !!(attrs && !attrs._isGroup && !attrs._isClusterNode);
    }

    function resolveDisplayedNodeId(graph, id) {
        if (!graph || !id) return null;
        if (graph.hasNode(id)) return id;
        var found = null;
        graph.forEachNode(function (nid, attrs) {
            if (found || !attrs || !attrs._memberIds) return;
            if (attrs._memberIds.indexOf(id) !== -1) found = nid;
        });
        return found;
    }

    function forEachUndirectedNeighbor(graph, nodeId, callback) {
        var seen = Object.create(null);
        function visit(neighbor) {
            if (!neighbor || neighbor === nodeId || seen[neighbor]) return;
            seen[neighbor] = true;
            callback(neighbor);
        }
        if (typeof graph.forEachOutboundNeighbor === 'function') {
            graph.forEachOutboundNeighbor(nodeId, visit);
            graph.forEachInboundNeighbor(nodeId, visit);
        } else {
            graph.forEachNeighbor(nodeId, visit);
        }
    }

    function undirectedEdgeKey(a, b) {
        return a < b ? a + '\0' + b : b + '\0' + a;
    }

    function searchLoadedNodes(graph, query, limit) {
        var cap = limit || SUGGEST_LIMIT;
        var needle = (query || '').toLowerCase().trim();
        var hits = [];
        if (!graph || !needle) return hits;
        graph.forEachNode(function (id, attrs) {
            if (hits.length >= cap) return;
            if (!_isInstanceNode(attrs)) return;
            var label = (attrs._data && attrs._data.label) ? attrs._data.label : (attrs.label || _localName(id));
            var type = attrs.entityType || '';
            var hay = (label + ' ' + type + ' ' + id + ' ' + _localName(id)).toLowerCase();
            if (hay.indexOf(needle) !== -1) {
                hits.push({ id: id, label: label, type: type });
            }
        });
        return hits;
    }

    function findUndirectedShortestPaths(graph, sourceId, targetId) {
        var empty = { nodes: [], edges: [], hopCount: 0, pathCount: 0 };
        if (!graph || !sourceId || !targetId) return empty;
        sourceId = resolveDisplayedNodeId(graph, sourceId);
        targetId = resolveDisplayedNodeId(graph, targetId);
        if (!sourceId || !targetId) return empty;
        if (sourceId === targetId) {
            return { nodes: [sourceId], edges: [], hopCount: 0, pathCount: 1 };
        }

        var dist = Object.create(null);
        var parents = Object.create(null);
        dist[sourceId] = 0;
        parents[sourceId] = [];
        var queue = [sourceId];
        var foundDist = null;
        var qIndex = 0;

        while (qIndex < queue.length) {
            var u = queue[qIndex++];
            var du = dist[u];
            if (foundDist !== null && du >= foundDist) continue;
            forEachUndirectedNeighbor(graph, u, function (v) {
                if (v === u) return;
                if (dist[v] === undefined) {
                    dist[v] = du + 1;
                    parents[v] = [u];
                    queue.push(v);
                    if (v === targetId) foundDist = dist[v];
                } else if (dist[v] === du + 1) {
                    if (parents[v].indexOf(u) === -1) parents[v].push(u);
                }
            });
        }

        if (foundDist === null) return empty;

        var nodeSet = {};
        var edgeKeys = {};
        var edges = [];

        function walk(node) {
            nodeSet[node] = true;
            (parents[node] || []).forEach(function (parent) {
                var key = undirectedEdgeKey(parent, node);
                if (!edgeKeys[key]) {
                    edgeKeys[key] = true;
                    edges.push({ source: parent, target: node });
                }
                walk(parent);
            });
        }
        walk(targetId);

        var pathMemo = Object.create(null);
        function countPaths(node) {
            if (node === sourceId) return 1;
            if (pathMemo[node] !== undefined) return pathMemo[node];
            var total = 0;
            (parents[node] || []).forEach(function (parent) {
                total += countPaths(parent);
            });
            pathMemo[node] = total;
            return total;
        }

        return {
            nodes: Object.keys(nodeSet),
            edges: edges,
            hopCount: foundDist,
            pathCount: countPaths(targetId)
        };
    }

    function _graph() {
        return (typeof SigmaGraph !== 'undefined' && SigmaGraph.getGraph)
            ? SigmaGraph.getGraph()
            : null;
    }

    function _side(which) {
        return {
            input: document.getElementById(which === 'source' ? 'sgFindPathSource' : 'sgFindPathTarget'),
            hidden: document.getElementById(which === 'source' ? 'sgFindPathSourceId' : 'sgFindPathTargetId'),
            list: document.getElementById(which === 'source' ? 'sgFindPathSourceList' : 'sgFindPathTargetList'),
            picked: document.getElementById(which === 'source' ? 'sgFindPathSourcePicked' : 'sgFindPathTargetPicked')
        };
    }

    function _hideSuggest(which) {
        var side = _side(which);
        if (side.list) {
            side.list.classList.add('d-none');
            side.list.innerHTML = '';
        }
        if (side.input) side.input.setAttribute('aria-expanded', 'false');
    }

    function _pick(which, node) {
        var side = _side(which);
        if (side.hidden) side.hidden.value = node.id;
        if (side.input) {
            side.input.value = '';
            side.input.placeholder = 'Search loaded entities…';
        }
        _hideSuggest(which);
        if (side.picked) {
            side.picked.classList.remove('d-none');
            side.picked.innerHTML =
                '<span class="sg-find-path-picked-label">' +
                _esc(node.label) +
                (node.type ? ' <span class="sg-find-path-suggest-type d-inline">(' + _esc(node.type) + ')</span>' : '') +
                '</span>' +
                '<button type="button" class="btn-close" data-find-path-clear="' + which + '" aria-label="Clear"></button>';
        }
    }

    function _clearPick(which) {
        var side = _side(which);
        if (side.hidden) side.hidden.value = '';
        if (side.input) side.input.value = '';
        if (side.picked) {
            side.picked.classList.add('d-none');
            side.picked.innerHTML = '';
        }
        _hideSuggest(which);
    }

    function _nodePayload(graph, nodeId) {
        var id = resolveDisplayedNodeId(graph, nodeId);
        if (!id) return null;
        var attrs = graph.getNodeAttributes(id);
        var label = (attrs._data && attrs._data.label) ? attrs._data.label : (attrs.label || _localName(id));
        return { id: id, label: label, type: attrs.entityType || '' };
    }

    function _renderSuggest(which, hits) {
        var side = _side(which);
        if (!side.list) return;
        if (!hits.length) {
            _hideSuggest(which);
            return;
        }
        side.list.innerHTML = hits.map(function (hit, index) {
            return '<button type="button" class="sg-find-path-suggest-item' + (index === 0 ? ' active' : '') + '" role="option" data-find-path-pick="' + which + '" data-node-id="' + _esc(hit.id) + '">' +
                '<span>' + _esc(hit.label) + '</span>' +
                (hit.type ? '<span class="sg-find-path-suggest-type">' + _esc(hit.type) + '</span>' : '') +
                '</button>';
        }).join('');
        side.list.classList.remove('d-none');
        if (side.input) side.input.setAttribute('aria-expanded', 'true');
    }

    function _suggest(which) {
        var side = _side(which);
        var graph = _graph();
        if (!graph || !side.input) {
            _hideSuggest(which);
            return;
        }
        _renderSuggest(which, searchLoadedNodes(graph, side.input.value, SUGGEST_LIMIT));
    }

    function _firstHit(which) {
        var side = _side(which);
        var btn = side.list && side.list.querySelector('[data-find-path-pick]');
        if (!btn) return null;
        return _nodePayload(_graph(), btn.getAttribute('data-node-id'));
    }

    function open(opts) {
        var graph = _graph();
        if (!graph || graph.order === 0) {
            _notify('Load a graph before finding a path.', 'warning');
            return;
        }
        if (typeof opts === 'string') {
            opts = { selectedId: opts, role: 'from' };
        }
        opts = opts || {};
        var role = opts.role === 'to' ? 'to' : 'from';
        _clearPick('source');
        _clearPick('target');

        var selectedId = opts.selectedId
            || ((typeof SigmaGraph !== 'undefined' && SigmaGraph.getSelectedNodeId)
                ? SigmaGraph.getSelectedNodeId()
                : null);
        var pairWithFilter = Object.prototype.hasOwnProperty.call(opts, 'selectedId');
        var filterId = (pairWithFilter && typeof SigmaGraph !== 'undefined' && SigmaGraph.getFilterAnchorNodeId)
            ? SigmaGraph.getFilterAnchorNodeId(selectedId)
            : null;
        var selected = selectedId ? _nodePayload(graph, selectedId) : null;
        var filterEntity = filterId ? _nodePayload(graph, filterId) : null;

        if (role === 'to') {
            _runDirect(filterId, selectedId);
            return;
        }
        if (selected) _pick('source', selected);
        if (filterEntity) _pick('target', filterEntity);

        var modalEl = document.getElementById('sgFindPathModal');
        if (!modalEl || typeof bootstrap === 'undefined') return;
        bootstrap.Modal.getOrCreateInstance(modalEl).show();
        window.setTimeout(function () {
            var sourceFilled = !!(document.getElementById('sgFindPathSourceId') || {}).value;
            var destFilled = !!(document.getElementById('sgFindPathTargetId') || {}).value;
            var focusId = !sourceFilled ? 'sgFindPathSource' : (!destFilled ? 'sgFindPathTarget' : 'sgFindPathApplyBtn');
            var input = document.getElementById(focusId);
            if (input) input.focus();
        }, 150);
    }

    function _runDirect(sourceId, targetId) {
        var graph = _graph();
        if (!graph || graph.order === 0) {
            _notify('Load a graph before finding a path.', 'warning');
            return;
        }
        sourceId = resolveDisplayedNodeId(graph, sourceId);
        targetId = resolveDisplayedNodeId(graph, targetId);
        if (!sourceId) {
            _notify('No Filter search entity to use as source.', 'warning');
            return;
        }
        if (!targetId) {
            _notify('Pick a destination on the loaded graph.', 'warning');
            return;
        }
        if (sourceId === targetId) {
            _notify('Source and destination must be different entities.', 'warning');
            return;
        }
        var result = findUndirectedShortestPaths(graph, sourceId, targetId);
        if (!result.nodes.length) {
            _notify('No path on the loaded graph.', 'warning');
            return;
        }
        if (typeof SigmaGraph !== 'undefined' && typeof SigmaGraph.applyPathHighlight === 'function') {
            SigmaGraph.applyPathHighlight(result);
        }
    }

    function apply() {
        var sourceId = (document.getElementById('sgFindPathSourceId') || {}).value || '';
        var targetId = (document.getElementById('sgFindPathTargetId') || {}).value || '';
        if (!sourceId || !targetId) {
            _notify('Pick a source and a destination from the loaded graph.', 'warning');
            return;
        }
        _runDirect(sourceId, targetId);
        var modalEl = document.getElementById('sgFindPathModal');
        if (modalEl && typeof bootstrap !== 'undefined') {
            var modal = bootstrap.Modal.getInstance(modalEl);
            if (modal) modal.hide();
        }
    }

    function _bind() {
        ['source', 'target'].forEach(function (which) {
            var side = _side(which);
            if (!side.input) return;
            side.input.addEventListener('input', function () {
                _suggest(which);
            });
            side.input.addEventListener('keydown', function (e) {
                if (e.key !== 'Enter') return;
                e.preventDefault();
                e.stopPropagation();
                var hit = _firstHit(which);
                if (hit) _pick(which, hit);
            });
        });

        document.addEventListener('click', function (e) {
            var pickBtn = e.target.closest('[data-find-path-pick]');
            if (pickBtn) {
                var which = pickBtn.getAttribute('data-find-path-pick');
                var node = _nodePayload(_graph(), pickBtn.getAttribute('data-node-id'));
                if (node) _pick(which, node);
                return;
            }
            var clearBtn = e.target.closest('[data-find-path-clear]');
            if (clearBtn) {
                _clearPick(clearBtn.getAttribute('data-find-path-clear'));
                return;
            }
            if (!e.target.closest('.sg-find-path-picker')) {
                _hideSuggest('source');
                _hideSuggest('target');
            }
        });
    }

    document.addEventListener('DOMContentLoaded', _bind);

    return {
        searchLoadedNodes: searchLoadedNodes,
        findUndirectedShortestPaths: findUndirectedShortestPaths,
        resolveDisplayedNodeId: resolveDisplayedNodeId,
        undirectedEdgeKey: undirectedEdgeKey,
        open: open,
        apply: apply
    };
})();
