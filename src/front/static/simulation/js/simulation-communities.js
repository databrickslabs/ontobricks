/**
 * Intelligence → Community: detect a partition, then materialize it.
 * Preview uses a light Sigma explorer (same CDN libs as Compare).
 */
const SimulationCommunities = {
    preview: null,
    current: null,
    entityMeta: {},
    ontologyClasses: null,
    graph: null,
    renderer: null,
    selectedCluster: null,
    _busy: false,
    _libsPromise: null,
    _resizeObserver: null,
    _nodeBase: null,

    LIB_URLS: [
        'https://cdnjs.cloudflare.com/ajax/libs/graphology/0.26.0/graphology.umd.min.js',
        'https://cdn.jsdelivr.net/npm/graphology-library@0.8.0/dist/graphology-library.min.js',
        'https://cdnjs.cloudflare.com/ajax/libs/sigma.js/3.0.2/sigma.min.js',
    ],

    CLUSTER_COLORS: [
        '#E63946', '#457B9D', '#2A9D8F', '#E9C46A', '#F4A261',
        '#264653', '#A8DADC', '#8338EC', '#FF006E', '#3A86FF',
        '#06D6A0', '#FFD166', '#118AB2', '#EF476F', '#073B4C',
        '#9B5DE5', '#F15BB5', '#00BBF9', '#00F5D4', '#FEE440',
    ],

    init() {
        const on = (id, fn) => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('click', fn);
        };
        on('commDetectBtn', () => this.detect());
        on('commMaterializeBtn', () => this.materialize());
        on('commClearBtn', () => this.clear());
        on('commRefreshBtn', () => this.loadCurrent());
        const slider = document.getElementById('commResolution');
        if (slider) {
            slider.addEventListener('input', () => this._syncResolutionLabel());
        }
        this._syncResolutionLabel();
        this._applyEditGate();
        document.addEventListener('sidebarSectionChanged', (e) => {
            if (e.detail && e.detail.section === 'communities') {
                this.loadCurrent();
                this._resizePreview();
            }
        });
        const section = document.getElementById('communities-section');
        if (section && section.classList.contains('active')) {
            this.loadCurrent();
        }
        window.addEventListener('resize', () => this._resizePreview());
        const explorer = document.getElementById('commExplorerBtn');
        if (explorer && !explorer.classList.contains('disabled')) {
            explorer.classList.add('disabled');
            explorer.setAttribute('aria-disabled', 'true');
        }
    },

    _canEdit() {
        return !(window.OB && typeof window.OB.canEditOntology === 'function'
            && !window.OB.canEditOntology());
    },

    _applyEditGate() {
        const editable = this._canEdit();
        ['commDetectBtn', 'commMaterializeBtn', 'commClearBtn'].forEach((id) => {
            const el = document.getElementById(id);
            if (el && id !== 'commMaterializeBtn') el.disabled = !editable;
        });
        this._syncMaterializeEnabled();
    },

    _syncResolutionLabel() {
        const slider = document.getElementById('commResolution');
        const label = document.getElementById('commResolutionValue');
        if (slider && label) label.textContent = parseFloat(slider.value).toFixed(2);
    },

    _syncMaterializeEnabled() {
        const btn = document.getElementById('commMaterializeBtn');
        if (!btn) return;
        const hasPreview = !!(this.preview && (this.preview.clusters || []).length);
        btn.disabled = !this._canEdit() || !hasPreview || this._busy;
    },

    _notify(msg, kind) {
        if (typeof showNotification === 'function') showNotification(msg, kind);
    },

    _esc(s) {
        return String(s ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    },

    async _json(url, opts) {
        const resp = await fetch(url, Object.assign({ credentials: 'same-origin' }, opts || {}));
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || data.success === false) {
            throw new Error(data.detail || data.message || resp.statusText || 'Request failed');
        }
        return data;
    },

    _sizeStats(clusters) {
        const sizes = (clusters || [])
            .map((cl) => Number(cl.size != null ? cl.size : (cl.members || []).length) || 0)
            .filter((n) => n >= 0);
        if (!sizes.length) {
            return { count: 0, min: null, max: null, avg: null, median: null };
        }
        const sorted = sizes.slice().sort((a, b) => a - b);
        const sum = sorted.reduce((acc, n) => acc + n, 0);
        const mid = Math.floor(sorted.length / 2);
        const median = sorted.length % 2
            ? sorted[mid]
            : (sorted[mid - 1] + sorted[mid]) / 2;
        return {
            count: sorted.length,
            min: sorted[0],
            max: sorted[sorted.length - 1],
            avg: sum / sorted.length,
            median,
        };
    },

    _fmtNum(value, digits) {
        if (value == null || Number.isNaN(value)) return '–';
        const n = Number(value);
        if (digits != null) return n.toFixed(digits);
        return Number.isInteger(n) ? String(n) : n.toFixed(1);
    },

    _renderStats(elId, { clusters, stats, graph } = {}) {
        const el = document.getElementById(elId);
        if (!el) return;
        const sizes = this._sizeStats(clusters);
        if (!sizes.count) {
            el.classList.add('d-none');
            el.innerHTML = '';
            return;
        }
        const s = stats || {};
        const tiles = [
            { label: 'Clusters', value: this._fmtNum(sizes.count) },
            { label: 'Nodes', value: this._fmtNum(s.node_count) },
            { label: 'Edges', value: this._fmtNum(s.edge_count) },
            { label: 'Size min', value: this._fmtNum(sizes.min) },
            { label: 'Size max', value: this._fmtNum(sizes.max) },
            { label: 'Size avg', value: this._fmtNum(sizes.avg, 1) },
            { label: 'Size median', value: this._fmtNum(sizes.median, 1) },
            {
                label: 'Modularity',
                value: s.modularity != null ? Number(s.modularity).toFixed(3) : '–',
            },
        ];
        const meta = [
            s.algorithm || '',
            s.resolution != null ? `resolution ${Number(s.resolution).toFixed(2)}` : '',
            s.elapsed_ms != null ? `${s.elapsed_ms} ms` : '',
            graph && graph.capped
                ? `preview ${ (graph.nodes || []).length } of ${graph.node_total} nodes`
                : '',
        ].filter(Boolean).join(' · ');
        el.classList.remove('d-none');
        el.innerHTML = `
            <div class="row g-2 mb-2">
                ${tiles.map((t) => `
                    <div class="col-6 col-md-3 col-xl">
                        <div class="sim-kpi-tile h-100">
                            <div class="sim-kpi-value">${this._esc(t.value)}</div>
                            <div class="sim-kpi-label">${this._esc(t.label)}</div>
                        </div>
                    </div>`).join('')}
            </div>
            ${meta ? `<div class="small text-muted mb-2">${this._esc(meta)}</div>` : ''}`;
    },

    _table(clusters) {
        if (!clusters || !clusters.length) {
            return '<p class="text-muted small mb-0">No clusters.</p>';
        }
        const rows = clusters.map((cl) => {
            const members = cl.members || [];
            const sample = members.slice(0, 3).map((m) => this._esc(m.split(/[#/]/).pop())).join(', ');
            const more = members.length > 3 ? ` +${members.length - 3}` : '';
            return `<tr>
                <td>${this._esc(cl.id)}</td>
                <td class="text-end">${this._esc(cl.size || members.length)}</td>
                <td class="small text-muted">${sample}${more}</td>
            </tr>`;
        }).join('');
        return `<table class="table table-sm sim-table mb-0">
            <thead><tr><th>Id</th><th class="text-end">Size</th><th>Members (sample)</th></tr></thead>
            <tbody>${rows}</tbody>
        </table>`;
    },

    _clusterColor(id) {
        return this.CLUSTER_COLORS[Math.abs(Number(id) || 0) % this.CLUSTER_COLORS.length];
    },

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

    _teardownPreview() {
        if (this._resizeObserver) {
            this._resizeObserver.disconnect();
            this._resizeObserver = null;
        }
        if (this.renderer) {
            try { this.renderer.kill(); } catch (_) { /* ignore */ }
        }
        this.renderer = null;
        this.graph = null;
        this.selectedCluster = null;
        this._nodeBase = null;
        const canvas = document.getElementById('commPreviewCanvas');
        if (canvas) canvas.innerHTML = '';
        this._resetClusterDetails();
    },

    _resetClusterDetails() {
        const box = document.getElementById('commPreviewDetails');
        if (!box) return;
        box.innerHTML = '<p class="text-muted small mb-0">Click a node to list the entities in its cluster.</p>';
    },

    _shortLabel(uri) {
        const s = String(uri || '');
        const tail = s.split(/[#/]/).pop();
        return tail || s;
    },

    async _ensureOntologyClasses() {
        if (Array.isArray(this.ontologyClasses)) return this.ontologyClasses;
        try {
            const data = (window.SimulationPage && typeof SimulationPage.api === 'function')
                ? await SimulationPage.api('/ontology/get-loaded-ontology')
                : await this._json('/ontology/get-loaded-ontology');
            this.ontologyClasses = ((data && data.ontology) || {}).classes || [];
        } catch (_) {
            this.ontologyClasses = [];
        }
        return this.ontologyClasses;
    },

    _findOntologyClass(typeUri, typeLabel) {
        const classes = this.ontologyClasses || [];
        if (!classes.length) return null;
        const uri = String(typeUri || '');
        const local = String(typeLabel || this._shortLabel(uri) || '').toLowerCase();
        return classes.find((cls) => {
            const classUri = String(cls.uri || cls.id || '');
            const name = String(cls.name || cls.label || '').toLowerCase();
            return (uri && (classUri === uri || classUri.endsWith(uri) || uri.endsWith(classUri)))
                || (local && (name === local || name.endsWith(local) || local.endsWith(name)));
        }) || null;
    },

    _typeIcon(typeUri, typeLabel) {
        const cls = this._findOntologyClass(typeUri, typeLabel);
        return (cls && (cls.emoji || cls.icon)) || '📦';
    },

    _memberInfo(uri) {
        const meta = (this.entityMeta && this.entityMeta[uri]) || {};
        const label = meta.label || this._shortLabel(uri);
        const typeUri = meta.type || '';
        const typeLabel = meta.type_label || this._shortLabel(typeUri) || 'Entity';
        return {
            uri,
            label,
            typeUri,
            typeLabel,
            icon: this._typeIcon(typeUri, typeLabel),
        };
    },

    _clusterById(clusterId) {
        const clusters = (this.preview && this.preview.clusters) || [];
        return clusters.find((cl) => Number(cl.id) === Number(clusterId)) || null;
    },

    _applyClusterHighlight(clusterId) {
        if (!this.graph || !this._nodeBase) return;
        const active = clusterId == null ? null : Number(clusterId);
        this.graph.forEachNode((node) => {
            const base = this._nodeBase[node];
            if (!base) return;
            const inCluster = active == null || Number(base.cluster) === active;
            this.graph.setNodeAttribute(node, 'color', inCluster ? base.color : '#D6D3D1');
            this.graph.setNodeAttribute(node, 'size', inCluster ? (active == null ? base.size : base.size + 2) : Math.max(2, base.size - 2));
            this.graph.setNodeAttribute(node, 'zIndex', inCluster && active != null ? 1 : 0);
        });
        if (this.renderer) {
            try { this.renderer.refresh(); } catch (_) { /* ignore */ }
        }
    },

    _showClusterDetails(clusterId, focusUri) {
        const box = document.getElementById('commPreviewDetails');
        if (!box) return;
        const cluster = this._clusterById(clusterId);
        if (!cluster) {
            box.innerHTML = `<p class="text-muted small mb-0">Cluster #${this._esc(clusterId)} has no members in the preview.</p>`;
            return;
        }
        const members = cluster.members || [];
        const size = cluster.size || members.length;
        const color = this._clusterColor(cluster.id);
        const infos = members.map((uri) => this._memberInfo(uri));
        infos.sort((a, b) => {
            const byType = String(a.typeLabel).localeCompare(String(b.typeLabel));
            if (byType) return byType;
            return String(a.label).localeCompare(String(b.label));
        });
        const rows = infos.map((info) => {
            const focused = focusUri && info.uri === focusUri ? ' is-focused' : '';
            return `<li class="comm-member${focused}" title="${this._esc(info.uri)}">
                <span class="comm-member-icon" aria-hidden="true">${this._esc(info.icon)}</span>
                <span class="comm-member-text">
                    <span class="comm-member-label">${this._esc(info.label)}</span>
                    <span class="comm-member-type">${this._esc(info.typeLabel)}</span>
                </span>
            </li>`;
        }).join('');
        box.innerHTML = `
            <div class="d-flex align-items-center gap-2 mb-2">
                <span class="comm-cluster-swatch" style="background:${color}"></span>
                <div>
                    <div class="fw-semibold">Cluster #${this._esc(cluster.id)}</div>
                    <div class="small text-muted">${this._esc(size)} entities</div>
                </div>
            </div>
            <ul class="comm-member-list list-unstyled mb-0">${rows || '<li class="text-muted small">No members.</li>'}</ul>`;
        if (focusUri) {
            const focusedEl = box.querySelector('.comm-member.is-focused');
            if (focusedEl && typeof focusedEl.scrollIntoView === 'function') {
                focusedEl.scrollIntoView({ block: 'nearest' });
            }
        }
    },

    async _selectCluster(clusterId, focusUri) {
        this.selectedCluster = clusterId;
        this._applyClusterHighlight(clusterId);
        await this._ensureOntologyClasses();
        this._showClusterDetails(clusterId, focusUri);
    },

    _resizePreview() {
        if (!this.renderer) return;
        try {
            this.renderer.resize();
            this.renderer.refresh();
        } catch (_) { /* ignore */ }
    },

    _layout(graph) {
        if (!graph.order) return;
        const fa2 = (typeof graphologyLibrary !== 'undefined' && graphologyLibrary.layoutForceAtlas2)
            ? graphologyLibrary.layoutForceAtlas2 : null;
        if (fa2 && fa2.assign) {
            fa2.assign(graph, {
                iterations: Math.min(150, 40 + Math.floor(graph.order / 10)),
                settings: {
                    gravity: 1,
                    scalingRatio: 10,
                    barnesHutOptimize: graph.order > 400,
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

    _buildGraphology(data) {
        const GraphClass = (typeof graphology !== 'undefined') ? (graphology.Graph || graphology) : null;
        if (!GraphClass) throw new Error('Graph library is not available');
        const graph = new GraphClass({ multi: false, type: 'undirected' });
        const style = getComputedStyle(document.documentElement);
        const edgeColor = (style.getPropertyValue('--db-border') || '').trim() || '#ECEAE6';
        (data.nodes || []).forEach((n) => {
            graph.addNode(n.id, {
                label: n.label || n.id,
                size: 6,
                color: this._clusterColor(n.cluster),
                x: Math.random() * 100,
                y: Math.random() * 100,
                cluster: n.cluster,
            });
        });
        (data.edges || []).forEach((e, idx) => {
            if (!graph.hasNode(e.source) || !graph.hasNode(e.target)) return;
            const key = e.id || `e-${idx}`;
            if (graph.hasEdge(key) || graph.hasEdge(e.source, e.target)) return;
            graph.addEdgeWithKey(key, e.source, e.target, {
                size: 1,
                color: edgeColor,
            });
        });
        return graph;
    },

    async _renderPreviewGraph(data) {
        this._teardownPreview();
        const empty = document.getElementById('commPreviewEmpty');
        const layout = document.getElementById('commPreviewLayout');
        const canvas = document.getElementById('commPreviewCanvas');
        if (!canvas) return;
        const nodes = (data && data.nodes) || [];
        if (!nodes.length) {
            if (empty) empty.classList.remove('d-none');
            if (layout) layout.classList.add('d-none');
            return;
        }
        if (empty) empty.classList.add('d-none');
        if (layout) layout.classList.remove('d-none');
        try {
            await this._ensureLibs();
        } catch (err) {
            canvas.innerHTML = `<p class="text-danger small m-3">${this._esc(err.message)}</p>`;
            return;
        }
        const SigmaModule = typeof Sigma !== 'undefined' ? Sigma : null;
        const SigmaClass = typeof SigmaModule === 'function'
            ? SigmaModule : (SigmaModule && (SigmaModule.Sigma || SigmaModule.default));
        if (!SigmaClass) {
            canvas.innerHTML = '<p class="text-danger small m-3">Graph renderer is not available.</p>';
            return;
        }
        const labelColor = (getComputedStyle(document.documentElement)
            .getPropertyValue('--db-text') || '').trim() || '#191817';
        let graph;
        try {
            graph = this._buildGraphology(data);
            this._layout(graph);
        } catch (err) {
            canvas.innerHTML = `<p class="text-danger small m-3">${this._esc(err.message)}</p>`;
            return;
        }
        this._nodeBase = {};
        graph.forEachNode((node, attrs) => {
            this._nodeBase[node] = {
                color: attrs.color,
                size: attrs.size,
                cluster: attrs.cluster,
            };
        });
        const settings = {
            renderLabels: graph.order <= 250,
            labelSize: 11,
            labelColor: { color: labelColor },
            allowInvalidContainer: true,
            zIndex: true,
        };
        try {
            this.renderer = new SigmaClass(graph, canvas, settings);
            this.graph = graph;
        } catch (err) {
            canvas.innerHTML = `<p class="text-danger small m-3">${this._esc(err.message)}</p>`;
            return;
        }
        this.renderer.on('clickNode', ({ node }) => {
            const clusterId = graph.getNodeAttribute(node, 'cluster');
            this._selectCluster(clusterId, node);
        });
        this.renderer.on('clickStage', () => {
            this.selectedCluster = null;
            this._applyClusterHighlight(null);
            this._resetClusterDetails();
        });
        requestAnimationFrame(() => this._resizePreview());
        if (window.ResizeObserver && !this._resizeObserver) {
            this._resizeObserver = new ResizeObserver(() => this._resizePreview());
            this._resizeObserver.observe(canvas);
        }
    },

    _erasePreview() {
        this.preview = null;
        this.entityMeta = {};
        this.selectedCluster = null;
        this._nodeBase = null;
        this._teardownPreview();
        const statsEl = document.getElementById('commPreviewStats');
        if (statsEl) {
            statsEl.classList.add('d-none');
            statsEl.innerHTML = '';
        }
        const layoutEl = document.getElementById('commPreviewLayout');
        if (layoutEl) layoutEl.classList.add('d-none');
        const emptyEl = document.getElementById('commPreviewEmpty');
        if (emptyEl) emptyEl.classList.remove('d-none');
        const loadingEl = document.getElementById('commPreviewLoading');
        if (loadingEl) loadingEl.classList.add('d-none');
        this._syncMaterializeEnabled();
    },

    _showPreviewLoading(loading) {
        const loadingEl = document.getElementById('commPreviewLoading');
        const emptyEl = document.getElementById('commPreviewEmpty');
        const layoutEl = document.getElementById('commPreviewLayout');
        const statsEl = document.getElementById('commPreviewStats');
        if (loading) {
            if (loadingEl) loadingEl.classList.remove('d-none');
            if (emptyEl) emptyEl.classList.add('d-none');
            if (layoutEl) layoutEl.classList.add('d-none');
            if (statsEl) statsEl.classList.add('d-none');
        } else {
            if (loadingEl) loadingEl.classList.add('d-none');
        }
    },

    async detect() {
        if (this._busy || !this._canEdit()) return;
        this._busy = true;
        this._syncMaterializeEnabled();
        const btn = document.getElementById('commDetectBtn');
        if (btn) btn.disabled = true;
        this._showPreviewLoading(true);
        try {
            const algorithm = document.getElementById('commAlgorithm')?.value || 'louvain';
            const resolution = parseFloat(document.getElementById('commResolution')?.value) || 1.0;
            this._notify('Detecting communities…', 'info');
            const data = await this._json('/dtwin/clusters/detect', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ algorithm, resolution }),
            });
            this.preview = {
                clusters: data.clusters || [],
                stats: Object.assign({}, data.stats || {}, { algorithm, resolution }),
            };
            this.entityMeta = data.entity_meta || {};
            await this._ensureOntologyClasses();
            const graph = data.graph || { nodes: [], edges: [] };
            this._showPreviewLoading(false);
            this._renderStats('commPreviewStats', {
                clusters: this.preview.clusters,
                stats: this.preview.stats,
                graph,
            });
            await this._renderPreviewGraph(graph);
            this._notify(
                `${(this.preview.clusters || []).length} clusters in preview — materialize to publish.`,
                'success'
            );
        } catch (err) {
            this._showPreviewLoading(false);
            const emptyEl = document.getElementById('commPreviewEmpty');
            if (emptyEl) emptyEl.classList.remove('d-none');
            this._notify(err.message || 'Detection failed', 'danger');
        } finally {
            this._busy = false;
            if (btn) btn.disabled = !this._canEdit();
            this._syncMaterializeEnabled();
        }
    },

    async materialize() {
        if (this._busy || !this._canEdit() || !this.preview) return;
        this._busy = true;
        this._syncMaterializeEnabled();
        try {
            const data = await this._json('/dtwin/clusters/materialize', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(this.preview),
            });
            this._notify(
                `Materialized ${data.cluster_count || 0} clusters (${data.inserted || 0} triples).`,
                'success'
            );
            await this.loadCurrent();
        } catch (err) {
            this._notify(err.message || 'Materialize failed', 'danger');
        } finally {
            this._busy = false;
            this._syncMaterializeEnabled();
        }
    },

    async clear() {
        if (this._busy || !this._canEdit()) return;
        this._busy = true;
        try {
            await this._json('/dtwin/clusters/current', { method: 'DELETE' });
            this._notify('Materialized clusters removed.', 'info');
            this._erasePreview();
            await this.loadCurrent();
        } catch (err) {
            this._notify(err.message || 'Clear failed', 'danger');
        } finally {
            this._busy = false;
            this._syncMaterializeEnabled();
        }
    },

    async loadCurrent() {
        try {
            const data = await this._json('/dtwin/clusters/current');
            this.current = data;
            const clusters = data.clusters || [];
            this._renderStats('commCurrentStats', {
                clusters,
                stats: Object.assign({}, data.stats || {}, {
                    cluster_count: clusters.length,
                    node_count: (data.stats && data.stats.node_count != null)
                        ? data.stats.node_count
                        : clusters.reduce((n, cl) => n + (cl.size || (cl.members || []).length), 0),
                }),
            });
            const body = document.getElementById('commCurrentBody');
            if (body) {
                body.innerHTML = clusters.length
                    ? this._table(clusters)
                    : '<p class="text-muted small mb-0">No materialized clusters yet.</p>';
            }
            const explorer = document.getElementById('commExplorerBtn');
            if (explorer) {
                explorer.classList.toggle('disabled', !clusters.length);
                explorer.setAttribute('aria-disabled', clusters.length ? 'false' : 'true');
                explorer.title = clusters.length
                    ? 'Open Graph Explorer to colour and collapse clusters'
                    : 'Materialize a partition before opening Explorer';
            }
        } catch (err) {
            this._renderStats('commCurrentStats', { clusters: [] });
            const body = document.getElementById('commCurrentBody');
            if (body) {
                body.innerHTML = `<p class="text-muted small mb-0">${this._esc(err.message)}</p>`;
            }
        }
    },
};

document.addEventListener('DOMContentLoaded', () => SimulationCommunities.init());
window.SimulationCommunities = SimulationCommunities;
