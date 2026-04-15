/**
 * OntoBricks - query-ontology-viewer.js
 * Read-only D3.js ontology model viewer modal for the Digital Twin page.
 *
 * Provides OntologyViewer.open() which fetches the ontology classes,
 * properties, and saved map layout, then renders a frozen (non-editable)
 * force-directed graph inside a fullscreen Bootstrap modal.
 * Only pan and zoom are allowed.
 */

const OntologyViewer = (() => {
    const MODAL_ID = 'ontologyViewerModal';
    const CONTAINER_ID = 'ontologyViewerContainer';
    const D3_CDN = 'https://d3js.org/d3.v7.min.js';

    let _modal = null;
    let _zoom = null;
    let _svg = null;

    function _ensureD3() {
        if (typeof d3 !== 'undefined') return Promise.resolve();
        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = D3_CDN;
            script.onload = resolve;
            script.onerror = () => reject(new Error('Failed to load D3.js'));
            document.head.appendChild(script);
        });
    }

    function _ensureModal() {
        if (document.getElementById(MODAL_ID)) return;
        const html = `
        <div class="modal fade" id="${MODAL_ID}" tabindex="-1" aria-labelledby="${MODAL_ID}Label" aria-hidden="true">
            <div class="modal-dialog modal-fullscreen">
                <div class="modal-content">
                    <div class="modal-header py-2">
                        <h5 class="modal-title" id="${MODAL_ID}Label">
                            <i class="bi bi-diagram-3 me-2"></i>Ontology Model
                            <span id="ontologyViewerName" class="text-muted small ms-2"></span>
                        </h5>
                        <div class="d-flex align-items-center gap-2">
                            <button class="btn btn-sm btn-outline-secondary" onclick="OntologyViewer.zoomIn()" title="Zoom in">
                                <i class="bi bi-zoom-in"></i>
                            </button>
                            <button class="btn btn-sm btn-outline-secondary" onclick="OntologyViewer.zoomOut()" title="Zoom out">
                                <i class="bi bi-zoom-out"></i>
                            </button>
                            <button class="btn btn-sm btn-outline-secondary" onclick="OntologyViewer.fitToView()" title="Fit to view">
                                <i class="bi bi-arrows-fullscreen"></i> Fit
                            </button>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                    </div>
                    <div class="modal-body p-0" style="overflow: hidden; background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);">
                        <div id="${CONTAINER_ID}" style="width: 100%; height: 100%; position: relative;"></div>
                    </div>
                </div>
            </div>
        </div>`;
        document.body.insertAdjacentHTML('beforeend', html);
    }

    async function _fetchOntology() {
        const resp = await fetch('/ontology/load', { credentials: 'same-origin' });
        const data = await resp.json();
        return data.success ? data.config : null;
    }

    async function _fetchMapLayout() {
        try {
            const resp = await fetch('/domain/map-layout', { credentials: 'same-origin' });
            const data = await resp.json();
            return (data.success && data.layout) ? data.layout : null;
        } catch { return null; }
    }

    function _buildGraph(container, config, savedLayout) {
        const classes = config.classes || [];
        const properties = config.properties || [];
        const width = container.clientWidth || container.parentElement.clientWidth || window.innerWidth;
        const height = container.clientHeight || container.parentElement.clientHeight || (window.innerHeight - 60);

        const nodes = classes.map((cls, idx) => {
            const saved = savedLayout?.positions?.[cls.name];
            return {
                id: cls.name,
                label: cls.label || cls.name,
                icon: cls.emoji || '📦',
                parent: cls.parent,
                x: saved?.x ?? (100 + (idx % 5) * 150),
                y: saved?.y ?? (100 + Math.floor(idx / 5) * 120),
                fx: saved ? (saved.x ?? saved.fx) : null,
                fy: saved ? (saved.y ?? saved.fy) : null
            };
        });

        const validIds = new Set(nodes.map(n => n.id));
        const links = [];

        properties.forEach(prop => {
            if (prop.domain && prop.range && validIds.has(prop.domain) && validIds.has(prop.range)) {
                links.push({ source: prop.domain, target: prop.range, name: prop.name, type: 'relationship', direction: prop.direction || 'forward' });
            }
        });

        classes.forEach(cls => {
            if (cls.parent && validIds.has(cls.parent)) {
                links.push({ source: cls.parent, target: cls.name, name: 'inherits', type: 'inheritance' });
            }
        });

        const linkCountMap = new Map();
        links.forEach(l => {
            const key = [l.source, l.target].sort().join('|');
            linkCountMap.set(key, (linkCountMap.get(key) || 0) + 1);
        });
        const linkIndexMap = new Map();
        links.forEach(l => {
            const key = [l.source, l.target].sort().join('|');
            const idx = linkIndexMap.get(key) || 0;
            l.linkCount = linkCountMap.get(key);
            l.linkIndex = idx;
            linkIndexMap.set(key, idx + 1);
        });

        const selfLoopLinks = links.filter(l => l.source === l.target);
        const regularLinks = links.filter(l => l.source !== l.target);

        const selfLoopCountMap = new Map();
        selfLoopLinks.forEach(l => {
            const count = selfLoopCountMap.get(l.source) || 0;
            l.selfLoopIndex = count;
            selfLoopCountMap.set(l.source, count + 1);
        });
        selfLoopLinks.forEach(l => { l.selfLoopCount = selfLoopCountMap.get(l.source); });

        _svg = d3.select(container).append('svg').attr('width', width).attr('height', height);

        _zoom = d3.zoom().scaleExtent([0.2, 4]).on('zoom', (e) => g.attr('transform', e.transform));
        _svg.call(_zoom);

        const g = _svg.append('g');
        const defs = _svg.append('defs');

        defs.append('marker').attr('id', 'ov-arrow').attr('viewBox', '0 -5 10 10')
            .attr('refX', 28).attr('refY', 0).attr('markerWidth', 6).attr('markerHeight', 6).attr('orient', 'auto')
            .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#6c757d');

        defs.append('marker').attr('id', 'ov-arrow-self').attr('viewBox', '0 -5 10 10')
            .attr('refX', 10).attr('refY', 0).attr('markerWidth', 5).attr('markerHeight', 5).attr('orient', 'auto')
            .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#495057');

        defs.append('marker').attr('id', 'ov-arrow-inh').attr('viewBox', '0 -5 10 10')
            .attr('refX', 28).attr('refY', 0).attr('markerWidth', 8).attr('markerHeight', 8).attr('orient', 'auto')
            .append('path').attr('d', 'M0,-5L10,0L0,5Z').attr('fill', 'white').attr('stroke', '#adb5bd').attr('stroke-width', 1);

        const simulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(links).id(d => d.id).distance(180));

        if (!savedLayout) {
            simulation
                .force('charge', d3.forceManyBody().strength(-400))
                .force('collision', d3.forceCollide().radius(60))
                .force('center', d3.forceCenter(width / 2, height / 2));
        } else {
            simulation.alphaDecay(1).velocityDecay(1);
        }

        const relLinks = g.append('g').selectAll('path')
            .data(regularLinks.filter(l => l.type === 'relationship'))
            .enter().append('path').attr('class', 'map-link')
            .style('marker-end', 'url(#ov-arrow)');

        const inhLinks = g.append('g').selectAll('path')
            .data(regularLinks.filter(l => l.type === 'inheritance'))
            .enter().append('path').attr('class', 'map-link inheritance')
            .style('marker-end', 'url(#ov-arrow-inh)');

        const selfLoops = g.append('g').selectAll('path')
            .data(selfLoopLinks).enter().append('path').attr('class', 'map-link self-loop')
            .style('marker-end', 'url(#ov-arrow-self)');

        const linkLabels = g.append('g').selectAll('text')
            .data(links.filter(l => l.type === 'relationship'))
            .enter().append('text').attr('class', 'map-link-label').text(d => d.name);

        const nodeEls = g.append('g').selectAll('g').data(nodes).enter().append('g')
            .attr('class', 'map-node').style('cursor', 'default');

        nodeEls.append('circle').attr('class', 'map-node-hitarea').attr('r', 25);
        nodeEls.append('text').attr('class', 'map-node-icon').text(d => d.icon);
        nodeEls.append('text').attr('class', 'map-node-label').attr('dy', 35).text(d => d.label);
        nodeEls.append('title').text(d => d.id);

        simulation.on('tick', () => {
            relLinks.attr('d', d => {
                const sx = d.source.x, sy = d.source.y, tx = d.target.x, ty = d.target.y;
                if (d.linkCount > 1) {
                    const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                    const mx = (sx + tx) / 2, my = (sy + ty) / 2;
                    const dx = tx - sx, dy = ty - sy, len = Math.sqrt(dx * dx + dy * dy) || 1;
                    return `M${sx},${sy} Q${mx + (-dy / len) * offset},${my + (dx / len) * offset} ${tx},${ty}`;
                }
                return `M${sx},${sy} L${tx},${ty}`;
            });

            inhLinks.attr('d', d => `M${d.source.x},${d.source.y} L${d.target.x},${d.target.y}`);

            selfLoops.attr('d', d => {
                const node = d.source;
                const baseAngle = -45, angleStep = 90;
                const angle = (baseAngle + (d.selfLoopIndex || 0) * angleStep) * Math.PI / 180;
                const nr = 25, loopSize = 40, ctrlDist = loopSize + 35;
                const sa = angle - 0.3, ea = angle + 0.3;
                return `M${node.x + Math.cos(sa) * nr},${node.y + Math.sin(sa) * nr} C${node.x + Math.cos(sa) * ctrlDist},${node.y + Math.sin(sa) * ctrlDist} ${node.x + Math.cos(ea) * ctrlDist},${node.y + Math.sin(ea) * ctrlDist} ${node.x + Math.cos(ea) * nr},${node.y + Math.sin(ea) * nr}`;
            });

            linkLabels.each(function (d) {
                const sx = d.source.x, sy = d.source.y, tx = d.target.x, ty = d.target.y;
                let mx, my;
                if (d.source.id === d.target.id) {
                    const baseAngle = -45, angleStep = 90;
                    const angle = (baseAngle + (d.selfLoopIndex || 0) * angleStep) * Math.PI / 180;
                    mx = d.source.x + Math.cos(angle) * 75;
                    my = d.source.y + Math.sin(angle) * 75;
                } else if (d.linkCount > 1) {
                    mx = (sx + tx) / 2; my = (sy + ty) / 2;
                    const dx = tx - sx, dy = ty - sy, len = Math.sqrt(dx * dx + dy * dy) || 1;
                    const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                    mx += (-dy / len) * offset; my += (dx / len) * offset;
                } else {
                    mx = (sx + tx) / 2; my = (sy + ty) / 2 - 8;
                }
                d3.select(this).attr('x', mx).attr('y', my);
            });

            nodeEls.attr('transform', d => `translate(${d.x},${d.y})`);
        });

        const legend = document.createElement('div');
        legend.className = 'map-legend';
        legend.innerHTML = `
            <div class="map-legend-title">Legend</div>
            <div class="map-legend-item"><div class="map-legend-line"></div><span>Relationship</span></div>
            <div class="map-legend-item"><div class="map-legend-line inheritance"></div><span>Inheritance</span></div>`;
        container.appendChild(legend);

        return { simulation, nodes, svg: _svg };
    }

    function _fitToView() {
        if (!_svg || !_zoom) return;
        const g = _svg.select('g');
        const bounds = g.node().getBBox();
        if (bounds.width === 0 || bounds.height === 0) return;

        const container = document.getElementById(CONTAINER_ID);
        const w = container.clientWidth, h = container.clientHeight;
        const padding = 60;
        const scale = Math.min((w - padding * 2) / bounds.width, (h - padding * 2) / bounds.height, 2);
        const tx = w / 2 - scale * (bounds.x + bounds.width / 2);
        const ty = h / 2 - scale * (bounds.y + bounds.height / 2);

        _svg.transition().duration(500).call(_zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
    }

    async function open() {
        _ensureModal();

        const container = document.getElementById(CONTAINER_ID);
        container.innerHTML = `
            <div class="d-flex justify-content-center align-items-center h-100">
                <div class="text-center text-muted">
                    <div class="spinner-border spinner-border-sm text-primary mb-3" role="status"></div>
                    <div>Loading ontology model...</div>
                </div>
            </div>`;

        const modalEl = document.getElementById(MODAL_ID);
        _modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        _modal.show();

        try {
            const [, config, savedLayout] = await Promise.all([
                _ensureD3(),
                _fetchOntology(),
                _fetchMapLayout()
            ]);

            const nameEl = document.getElementById('ontologyViewerName');
            if (nameEl && config) {
                nameEl.textContent = config.name ? `— ${config.name}` : '';
            }

            if (!config || !config.classes || config.classes.length === 0) {
                container.innerHTML = `
                    <div class="d-flex justify-content-center align-items-center h-100">
                        <div class="text-center text-muted">
                            <i class="bi bi-diagram-3 fs-1 d-block mb-2"></i>
                            <p>No ontology defined yet.<br>
                            Create entities in the <strong>Ontology</strong> section first.</p>
                        </div>
                    </div>`;
                return;
            }

            // Wait for the modal fade-in to complete so the container has real dimensions
            await new Promise(resolve => {
                if (container.clientWidth > 0 && container.clientHeight > 0) {
                    resolve();
                } else {
                    modalEl.addEventListener('shown.bs.modal', resolve, { once: true });
                }
            });

            container.innerHTML = '';
            const { simulation } = _buildGraph(container, config, savedLayout);

            if (!savedLayout) {
                simulation.on('end', () => _fitToView());
                setTimeout(() => _fitToView(), 2000);
            } else {
                setTimeout(() => _fitToView(), 200);
            }

        } catch (err) {
            console.error('[OntologyViewer] Error:', err);
            container.innerHTML = `
                <div class="d-flex justify-content-center align-items-center h-100">
                    <div class="text-center text-danger">
                        <i class="bi bi-exclamation-triangle fs-1 d-block mb-2"></i>
                        <p>Failed to load ontology model.<br><small>${err.message}</small></p>
                    </div>
                </div>`;
        }
    }

    function fitToView() { _fitToView(); }

    function zoomIn() {
        if (_svg && _zoom) _svg.transition().duration(300).call(_zoom.scaleBy, 1.4);
    }

    function zoomOut() {
        if (_svg && _zoom) _svg.transition().duration(300).call(_zoom.scaleBy, 0.7);
    }

    return { open, fitToView, zoomIn, zoomOut };
})();

window.OntologyViewer = OntologyViewer;
