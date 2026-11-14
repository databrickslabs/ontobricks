/**
 * Simulation Compare — overlays a ready sandbox on the current live graph
 * (inferred or asserted only), restricted to the scenario scope. Nodes and
 * edges are colour-coded unchanged / added (sandbox only) / removed (live only).
 */
const SimulationCompare = {
    scenarios: [],
    data: null,
    graph: null,
    renderer: null,
    /** PNG data URL captured right after a visible Sigma refresh. */
    lastGraphPng: '',
    _libsPromise: null,
    _requestSeq: 0,
    _lastId: '',

    LIB_URLS: [
        'https://cdnjs.cloudflare.com/ajax/libs/graphology/0.26.0/graphology.umd.min.js',
        'https://cdn.jsdelivr.net/npm/graphology-library@0.8.0/dist/graphology-library.min.js',
        'https://cdnjs.cloudflare.com/ajax/libs/sigma.js/3.0.2/sigma.min.js',
    ],

    init() {
        const run = document.getElementById('simCompareRunBtn');
        if (!run) return;
        run.addEventListener('click', () => this.run());
        const scenario = document.getElementById('simCompareScenario');
        if (scenario) {
            scenario.addEventListener('change', () => {
                this._paintExplorerLink();
                this._toggleRun();
            });
        }
        document.addEventListener('sidebarSectionChanged', (e) => {
            if (e.detail && e.detail.section === 'compare') this.onShow();
        });
        const params = new URLSearchParams(window.location.search);
        if ((params.get('section') || '') === 'compare') {
            setTimeout(() => this.onShow({ autoRun: !!params.get('simulation') }), 0);
        }
    },

    // -- Scenario picker -----------------------------------------------------

    async onShow({ autoRun = false } = {}) {
        const select = document.getElementById('simCompareScenario');
        if (!select) return;
        try {
            const data = await SimulationPage.api('/simulation/list');
            this.scenarios = (data.simulations || []).filter((s) => s.status === 'ready');
        } catch (err) {
            this.scenarios = [];
            this._showState('idle');
            showNotification(`Could not load scenarios: ${err.message}`, 'error');
        }
        const params = new URLSearchParams(window.location.search);
        const wanted = (params.get('simulation') || '').trim() || this._lastId || select.value;
        this._paintPicker(wanted);
        const source = document.getElementById('simCompareSource');
        if (source && params.get('inferred') === '0') source.value = 'asserted';
        if (!this.scenarios.length) {
            this._showState('empty');
        } else if (autoRun && select.value) {
            this.run();
        } else if (!this.data) {
            this._showState('idle');
        }
    },

    _paintPicker(wanted) {
        const select = document.getElementById('simCompareScenario');
        if (!select) return;
        if (!this.scenarios.length) {
            select.innerHTML = '<option value="">No ready scenario</option>';
            select.disabled = true;
            this._toggleRun();
            this._paintExplorerLink();
            return;
        }
        select.innerHTML = this.scenarios.map((s) => {
            const scope = s.spec && s.spec.scope ? s.spec.scope : {};
            const focus = scope.focus_label || SimulationPage.shortUri(scope.focus);
            const suffix = focus ? ` — ${focus}` : '';
            return `<option value="${SimulationPage.esc(s.id)}">${SimulationPage.esc((s.label || s.id) + suffix)}</option>`;
        }).join('');
        select.disabled = false;
        const known = this.scenarios.some((s) => s.id === wanted);
        select.value = known ? wanted : this.scenarios[0].id;
        this._toggleRun();
        this._paintExplorerLink();
    },

    _selectedId() {
        const select = document.getElementById('simCompareScenario');
        return select && !select.disabled ? select.value : '';
    },

    _includeInferred() {
        const source = document.getElementById('simCompareSource');
        return !source || source.value !== 'asserted';
    },

    _toggleRun() {
        const run = document.getElementById('simCompareRunBtn');
        if (run) run.disabled = !this._selectedId();
        // Evaluate tab shares the scenario picker: keep its Ask button in sync.
        if (window.SimulationEvaluate) SimulationEvaluate.refresh();
    },

    _paintExplorerLink() {
        const link = document.getElementById('simCompareExplorerBtn');
        if (!link) return;
        const id = this._selectedId();
        link.classList.toggle('d-none', !id);
        link.href = id
            ? `/dtwin/?section=sigmagraph&simulation=${encodeURIComponent(id)}`
            : '/dtwin/?section=sigmagraph';
    },

    _syncUrl(id, inferred) {
        const url = new URL(window.location.href);
        url.hash = '';
        url.searchParams.set('section', 'compare');
        url.searchParams.set('simulation', id);
        url.searchParams.set('inferred', inferred ? '1' : '0');
        if (window.history && window.history.replaceState) {
            window.history.replaceState({ section: 'compare', simulation: id }, '', url);
        }
    },

    // -- Run -----------------------------------------------------------------

    _showState(state) {
        const map = {
            empty: 'simCompareEmpty',
            idle: 'simCompareIdle',
            loading: 'simCompareLoading',
            body: 'simCompareBody',
        };
        Object.entries(map).forEach(([key, id]) => {
            const el = document.getElementById(id);
            if (el) el.classList.toggle('d-none', key !== state);
        });
        const isBody = state === 'body';
        const canvas = document.getElementById('simCompareCanvas');
        if (canvas) canvas.classList.toggle('d-none', !isBody);
        const footer = document.querySelector('.sim-compare-canvas-footer');
        if (footer) footer.classList.toggle('d-none', !isBody);
    },

    async run() {
        const id = this._selectedId();
        if (!id) return;
        const inferred = this._includeInferred();
        const seq = ++this._requestSeq;
        const run = document.getElementById('simCompareRunBtn');
        if (run) run.disabled = true;
        this._lastId = id;
        this._syncUrl(id, inferred);
        this._teardown();
        this._showState('loading');
        try {
            const qs = new URLSearchParams({ include_inferred: inferred ? 'true' : 'false' });
            const [data] = await Promise.all([
                SimulationPage.api(`/simulation/${encodeURIComponent(id)}/compare?${qs}`),
                this._ensureLibs().catch(() => false),
            ]);
            if (seq !== this._requestSeq) return;
            this.data = data;
            this._showState('body');
            this._renderSummary(data);
            await this._renderGraph(data);
        } catch (err) {
            if (seq !== this._requestSeq) return;
            this.data = null;
            this._teardown();
            this._showState('idle');
            showNotification(`Could not compare: ${err.message}`, 'error');
        } finally {
            if (seq === this._requestSeq) this._toggleRun();
        }
    },

    // -- Summary ---------------------------------------------------------------

    _renderSummary(data) {
        const summary = data.summary || {};
        document.querySelectorAll('#simCompareKpis [data-kpi]').forEach((el) => {
            const v = summary[el.dataset.kpi];
            el.textContent = v == null ? '–' : Number(v).toLocaleString();
        });
        const cap = document.getElementById('simCompareCapNote');
        if (cap) {
            cap.textContent = summary.capped
                ? `Showing ${summary.node_count} of ${summary.node_total} entities and ${summary.edge_count} of ${summary.edge_total} links (changes first)`
                : `${summary.node_count || 0} entities, ${summary.edge_count || 0} links`;
        }
        const note = document.getElementById('simCompareNote');
        if (note) {
            const asserted = data.include_inferred === false;
            const parts = [];
            if (!(data.nodes || []).length) parts.push('Nothing to draw: the sandbox and the live graph have no triple in this scope.');
            if (asserted) parts.push('Compared with asserted triples only: inferences already present in the sandbox appear as added.');
            note.textContent = parts.join(' ');
            note.classList.toggle('d-none', !parts.length);
        }
        const err = document.getElementById('simCompareError');
        if (err) err.classList.add('d-none');
        this._resetDetails();
    },

    // -- Graph -------------------------------------------------------------------

    _ensureLibs() {
        if (typeof Sigma !== 'undefined' && typeof graphology !== 'undefined') {
            return Promise.resolve(true);
        }
        if (this._libsPromise) return this._libsPromise;
        const load = (src) => new Promise((resolve, reject) => {
            const existing = Array.from(document.scripts).find((s) => s.src === src);
            if (existing && existing.dataset.loaded === '1') { resolve(); return; }
            const el = existing || document.createElement('script');
            el.addEventListener('load', () => { el.dataset.loaded = '1'; resolve(); });
            el.addEventListener('error', () => reject(new Error(`Could not load ${src}`)));
            if (!existing) {
                el.src = src;
                document.head.appendChild(el);
            }
        });
        this._libsPromise = Promise.all(this.LIB_URLS.map(load)).then(() => true).catch((err) => {
            this._libsPromise = null;
            throw err;
        });
        return this._libsPromise;
    },

    _tokens() {
        const style = getComputedStyle(document.documentElement);
        const token = (name, fallback) => (style.getPropertyValue(name) || '').trim() || fallback;
        return {
            added: token('--db-status-success', '#0E9F6E'),
            removed: token('--db-status-danger', '#DC2626'),
            changed: token('--db-status-warning', '#D97706'),
            unchanged: token('--db-text-muted', '#6F6B65'),
            edgeUnchanged: token('--db-border', '#ECEAE6'),
            label: token('--db-text', '#191817'),
            labelSecondary: token('--db-text-secondary', '#6B6864'),
        };
    },

    _nodeColor(node, colors) {
        if (node.presence === 'sandbox_only') return colors.added;
        if (node.presence === 'live_only') return colors.removed;
        return node.changed ? colors.changed : colors.unchanged;
    },

    _teardown() {
        if (this._resizeObserver) {
            this._resizeObserver.disconnect();
            this._resizeObserver = null;
        }
        if (this.renderer) {
            try { this.renderer.kill(); } catch (_) { /* ignore */ }
        }
        this.renderer = null;
        this.graph = null;
        this.lastGraphPng = '';
    },

    /**
     * Composite Sigma's layered canvases into a PNG. Sigma's WebGL layers use
     * ``preserveDrawingBuffer: false``, so the read must follow a synchronous
     * ``refresh()`` in the same turn. A hidden container renders at 1×1
     * (``allowInvalidContainer``), so a hidden pane keeps the last snapshot.
     */
    _snapshotGraphPng() {
        const container = document.getElementById('simCompareCanvas');
        if (!this.renderer || !container
            || container.offsetWidth < 2 || container.offsetHeight < 2) {
            return this.lastGraphPng;
        }
        this.renderer.refresh();
        const canvases = Array.from(container.querySelectorAll('canvas'))
            .filter((c) => c.width > 1 && c.height > 1);
        if (!canvases.length) return this.lastGraphPng;
        const width = Math.max(...canvases.map((c) => c.width));
        const height = Math.max(...canvases.map((c) => c.height));
        const out = document.createElement('canvas');
        out.width = width;
        out.height = height;
        const ctx = out.getContext('2d');
        if (!ctx) return this.lastGraphPng;
        const fill = getComputedStyle(document.documentElement)
            .getPropertyValue('--db-canvas-warm').trim() || '#FAFAF9';
        ctx.fillStyle = fill;
        ctx.fillRect(0, 0, width, height);
        canvases.forEach((c) => ctx.drawImage(c, 0, 0));
        try {
            this.lastGraphPng = out.toDataURL('image/png');
        } catch (_) { /* keep previous snapshot */ }
        return this.lastGraphPng;
    },

    _buildGraph(data, colors) {
        const GraphClass = (typeof graphology !== 'undefined') ? (graphology.Graph || graphology) : null;
        if (!GraphClass) throw new Error('Graph library is not available');
        const graph = new GraphClass({ multi: true, type: 'directed' });
        (data.nodes || []).forEach((n) => {
            graph.addNode(n.id, {
                label: n.label || n.id,
                size: n.changed ? 9 : 6,
                color: this._nodeColor(n, colors),
                x: Math.random() * 100,
                y: Math.random() * 100,
                _node: n,
            });
        });
        (data.edges || []).forEach((e) => {
            if (!graph.hasNode(e.source) || !graph.hasNode(e.target)) return;
            const delta = e.status !== 'unchanged';
            graph.addEdgeWithKey(e.id, e.source, e.target, {
                label: e.label,
                size: delta ? 3 : 1.2,
                color: e.status === 'added' ? colors.added
                    : e.status === 'removed' ? colors.removed : colors.edgeUnchanged,
                _edge: e,
            });
        });
        return graph;
    },

    _layout(graph) {
        if (!graph.order) return;
        const fa2 = (typeof graphologyLibrary !== 'undefined' && graphologyLibrary.layoutForceAtlas2)
            ? graphologyLibrary.layoutForceAtlas2 : null;
        if (fa2 && fa2.assign) {
            fa2.assign(graph, {
                iterations: 120,
                settings: {
                    gravity: 1,
                    scalingRatio: 10,
                    barnesHutOptimize: graph.order > 500,
                    strongGravityMode: true,
                    slowDown: 5,
                },
            });
            return;
        }
        const total = graph.order;
        let i = 0;
        graph.forEachNode((node) => {
            const angle = (2 * Math.PI * i) / total;
            graph.setNodeAttribute(node, 'x', Math.cos(angle) * 100);
            graph.setNodeAttribute(node, 'y', Math.sin(angle) * 100);
            i += 1;
        });
    },

    async _renderGraph(data) {
        this._teardown();
        const container = document.getElementById('simCompareCanvas');
        if (!container) return;
        container.innerHTML = '';
        if (!(data.nodes || []).length) return;
        try {
            await this._ensureLibs();
        } catch (err) {
            this._showError(`Graph libraries could not be loaded: ${err.message}`);
            return;
        }
        const SigmaModule = typeof Sigma !== 'undefined' ? Sigma : null;
        const SigmaClass = typeof SigmaModule === 'function'
            ? SigmaModule : (SigmaModule && (SigmaModule.Sigma || SigmaModule.default));
        if (!SigmaClass) {
            this._showError('Graph renderer is not available.');
            return;
        }
        const colors = this._tokens();
        const graph = this._buildGraph(data, colors);
        this._layout(graph);
        const settings = {
            renderLabels: true,
            renderEdgeLabels: graph.size <= 120,
            labelSize: 12,
            labelColor: { color: colors.label },
            edgeLabelSize: 10,
            edgeLabelColor: { color: colors.labelSecondary },
            enableEdgeEvents: true,
            allowInvalidContainer: true,
        };
        const rendering = (SigmaModule && SigmaModule.rendering) || {};
        if (rendering.EdgeArrowProgram) {
            settings.defaultEdgeType = 'arrow';
            settings.edgeProgramClasses = { arrow: rendering.EdgeArrowProgram };
        }
        try {
            this.renderer = new SigmaClass(graph, container, settings);
        } catch (err) {
            this._showError(`Could not draw the graph: ${err.message}`);
            return;
        }
        this.graph = graph;
        this.renderer.on('clickNode', ({ node }) => this._showNode(graph.getNodeAttribute(node, '_node')));
        this.renderer.on('clickEdge', ({ edge }) => this._showEdge(graph.getEdgeAttribute(edge, '_edge')));
        this.renderer.on('clickStage', () => this._resetDetails());
        requestAnimationFrame(() => {
            if (!this.renderer) return;
            try {
                this.renderer.resize();
                this.renderer.refresh();
                this._snapshotGraphPng();
            } catch (_) { /* ignore */ }
        });
        if (window.ResizeObserver && !this._resizeObserver) {
            this._resizeObserver = new ResizeObserver(() => {
                if (this.renderer) {
                    try {
                        this.renderer.resize();
                        this.renderer.refresh();
                        this._snapshotGraphPng();
                    } catch (_) { /* ignore */ }
                }
            });
            this._resizeObserver.observe(container);
        }
    },

    _showError(message) {
        const err = document.getElementById('simCompareError');
        if (!err) return;
        err.textContent = message;
        err.classList.remove('d-none');
    },

    // -- Details -------------------------------------------------------------------

    _resetDetails() {
        const box = document.getElementById('simCompareDetails');
        if (!box) return;
        box.innerHTML = `
            <p class="text-muted small mb-3">Select a node or an edge in the graph to view attributes, inferences, and rules.</p>
            <div class="border-top pt-2">
                <div class="fw-semibold small mb-2"><i class="bi bi-palette me-1"></i>Color guide</div>
                <div class="mb-2">
                    <div class="small fw-semibold text-muted mb-1">Nodes (Entities)</div>
                    <ul class="list-unstyled small mb-0 ps-1">
                        <li class="mb-1"><span class="sim-compare-swatch sim-compare-swatch-unchanged"></span><strong>Grey:</strong> Unchanged</li>
                        <li class="mb-1"><span class="sim-compare-swatch sim-compare-swatch-changed"></span><strong>Amber:</strong> Modified entity</li>
                        <li class="mb-1"><span class="sim-compare-swatch sim-compare-swatch-added"></span><strong>Green:</strong> Sandbox only (added)</li>
                        <li class="mb-1"><span class="sim-compare-swatch sim-compare-swatch-removed"></span><strong>Red:</strong> Live only (removed)</li>
                    </ul>
                </div>
                <div>
                    <div class="small fw-semibold text-muted mb-1">Links (Relationships)</div>
                    <ul class="list-unstyled small mb-0 ps-1">
                        <li class="mb-1"><span class="sim-compare-swatch sim-compare-swatch-unchanged"></span><strong>Grey:</strong> Unchanged link</li>
                        <li class="mb-1"><span class="sim-compare-swatch sim-compare-swatch-added"></span><strong>Green:</strong> Added link</li>
                        <li class="mb-1"><span class="sim-compare-swatch sim-compare-swatch-removed"></span><strong>Red:</strong> Removed link</li>
                    </ul>
                </div>
            </div>`;
    },

    _statusBadge(status) {
        const map = { added: 'bg-success', removed: 'bg-danger', unchanged: 'bg-secondary' };
        return `<span class="badge ${map[status] || 'bg-secondary'}">${SimulationPage.esc(status)}</span>`;
    },

    _showNode(node) {
        const box = document.getElementById('simCompareDetails');
        if (!box || !node) return;
        const esc = (v) => SimulationPage.esc(v);
        const presence = { shared: 'Live and sandbox', sandbox_only: 'Sandbox only', live_only: 'Live only' };
        const changes = (node.changes || []).map((c) => `<li class="mb-1">
            ${this._statusBadge(c.status)}
            <span class="fw-semibold">${esc(c.predicate_label)}</span>
            <span class="text-break">${esc(SimulationPage.termLabel(c.object))}</span>
        </li>`).join('');
        box.innerHTML = `
            <div class="fw-semibold text-break">${esc(node.label)}</div>
            <div class="small text-muted text-break mb-2">${esc(node.id)}</div>
            <dl class="row small mb-2">
                <dt class="col-4">Type</dt><dd class="col-8">${esc(node.type || '–')}</dd>
                <dt class="col-4">Present in</dt><dd class="col-8">${esc(presence[node.presence] || node.presence)}</dd>
                <dt class="col-4">Added</dt><dd class="col-8">${Number(node.added || 0).toLocaleString()}</dd>
                <dt class="col-4">Removed</dt><dd class="col-8">${Number(node.removed || 0).toLocaleString()}</dd>
            </dl>
            ${changes ? `<div class="small fw-semibold mb-1">Attribute changes</div><ul class="list-unstyled small mb-0">${changes}</ul>` : ''}`;
    },

    _showEdge(edge) {
        const box = document.getElementById('simCompareDetails');
        if (!box || !edge) return;
        const esc = (v) => SimulationPage.esc(v);
        const origin = edge.origin ? `<dt class="col-4">Origin</dt><dd class="col-8">${esc(edge.origin)}</dd>` : '';
        const prov = edge.provenance ? `<dt class="col-4">Why</dt><dd class="col-8 text-break">${esc(edge.provenance)}</dd>` : '';
        box.innerHTML = `
            <div class="fw-semibold">${esc(edge.label)} ${this._statusBadge(edge.status)}</div>
            <div class="small text-muted text-break mb-2">${esc(edge.predicate)}</div>
            <dl class="row small mb-0">
                <dt class="col-4">From</dt><dd class="col-8 text-break">${esc(SimulationPage.shortUri(edge.source))}</dd>
                <dt class="col-4">To</dt><dd class="col-8 text-break">${esc(SimulationPage.shortUri(edge.target))}</dd>
                ${origin}${prov}
            </dl>`;
    },
};

window.SimulationCompare = SimulationCompare;
document.addEventListener('DOMContentLoaded', () => SimulationCompare.init());
