/**
 * Intelligence → Discovery → Communities: detect a partition on the Databricks job, then
 * materialize it. Both run as background tasks; every read is bounded (top
 * clusters, a capped preview, members paged on demand). The preview uses a
 * light Sigma explorer (same CDN libs as Compare).
 */
const IntelligenceCommunities = {
    latest: null,
    current: null,
    blockedReason: '',
    runPageUrl: '',
    ontologyClasses: null,
    graph: null,
    renderer: null,
    selectedCluster: null,
    _members: null,
    _busy: false,
    _libsPromise: null,
    _resizeObserver: null,
    _nodeBase: null,

    TOP_CLUSTERS: 50,
    MEMBERS_PAGE: 100,

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
        on('commRefreshBtn', () => this.refresh());
        const details = document.getElementById('commPreviewDetails');
        if (details) {
            details.addEventListener('click', (e) => {
                if (e.target.closest('[data-comm-more]')) this._loadMoreMembers();
            });
        }
        const current = document.getElementById('commCurrentBody');
        if (current) {
            current.addEventListener('click', (e) => {
                const more = e.target.closest('[data-comm-current-more]');
                if (more) {
                    this._loadCurrentMembers(Number(more.dataset.commCurrentMore), true);
                    return;
                }
                const row = e.target.closest('[data-comm-cluster]');
                if (row) this._toggleCurrentMembers(Number(row.dataset.commCluster));
            });
        }
        this._applyEditGate();
        document.addEventListener('sidebarSectionChanged', (e) => {
            if (e.detail && e.detail.section === 'communities') {
                this.refresh();
                this._resizePreview();
            }
        });
        const section = document.getElementById('communities-section');
        if (section && section.classList.contains('active')) {
            this.refresh();
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
        const clearBtn = document.getElementById('commClearBtn');
        if (clearBtn) clearBtn.disabled = !editable;
        this._syncDetectEnabled();
        this._syncMaterializeEnabled();
    },

    _syncDetectEnabled() {
        const btn = document.getElementById('commDetectBtn');
        if (!btn) return;
        btn.disabled = !this._canEdit() || this._busy || !!this.blockedReason;
        btn.title = this.blockedReason || 'Run community detection on Databricks';
    },

    _syncMaterializeEnabled() {
        const btn = document.getElementById('commMaterializeBtn');
        if (!btn) return;
        const hasRun = !!(this.latest && this.latest.stats);
        btn.disabled = !this._canEdit() || !hasRun || this._busy;
    },

    _setBusy(busy) {
        this._busy = busy;
        this._syncDetectEnabled();
        this._syncMaterializeEnabled();
    },

    /** Mirror navbar Save: swap the button icon for a spinner while the action runs. */
    _setBtnSpinner(btnId, busy) {
        const btn = document.getElementById(btnId);
        if (!btn) return;
        btn.classList.toggle('is-busy', busy);
        btn.setAttribute('aria-busy', busy ? 'true' : 'false');
        btn.querySelector('i.bi')?.classList.toggle('d-none', busy);
        btn.querySelector('.ob-btn-spinner')?.remove();
        if (busy) {
            btn.insertAdjacentHTML(
                'afterbegin',
                '<span class="spinner-border spinner-border-sm ob-btn-spinner" role="status" aria-hidden="true"></span>'
            );
        }
    },

    _renderJobStatus() {
        const el = document.getElementById('commJobStatus');
        if (!el) return;
        if (!this.blockedReason) {
            el.classList.add('d-none');
            el.textContent = '';
            return;
        }
        el.classList.remove('d-none');
        el.textContent = this.blockedReason;
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

    _fmtNum(value, digits) {
        if (value == null || Number.isNaN(Number(value))) return '–';
        const n = Number(value);
        if (digits != null) return n.toFixed(digits);
        return Number.isInteger(n) ? n.toLocaleString() : n.toFixed(1);
    },

    _fmtWhen(iso) {
        if (!iso) return '';
        const d = new Date(iso);
        return Number.isNaN(d.getTime()) ? '' : d.toLocaleString();
    },

    /** Same scale as analytics / task tracker (``formatTaskSeconds``). */
    _fmtElapsed(ms) {
        if (ms == null || Number.isNaN(Number(ms))) return '';
        const n = Number(ms);
        if (window.formatTaskSeconds) return window.formatTaskSeconds(n / 1000);
        if (n < 1000) return `${Math.round(n)} ms`;
        const s = n / 1000;
        if (s < 60) return `${s < 10 ? s.toFixed(1) : Math.round(s)}s`;
        const m = Math.floor(s / 60);
        const rem = Math.round(s % 60);
        return `${m}m ${rem}s`;
    },

    _renderStats(elId, stats, meta) {
        const el = document.getElementById(elId);
        if (!el) return;
        const s = stats || {};
        if (!s.cluster_count) {
            el.classList.add('d-none');
            el.innerHTML = '';
            return;
        }
        const tiles = [
            { label: 'Clusters', value: s.cluster_count },
            { label: 'Nodes', value: s.node_count },
            { label: 'Edges', value: s.edge_count },
            { label: 'Size min', value: s.size_min },
            { label: 'Size max', value: s.size_max },
            { label: 'Size avg', value: s.size_avg, digits: 1 },
            { label: 'Size median', value: s.size_median, digits: 1 },
            { label: 'Modularity', value: s.modularity, digits: 3 },
        ].filter((t) => t.value != null);
        el.classList.remove('d-none');
        el.innerHTML = `
            <div class="row g-2 mb-2">
                ${tiles.map((t) => `
                    <div class="col-6 col-md-3 col-xl">
                        <div class="sim-kpi-tile h-100">
                            <div class="sim-kpi-value">${this._esc(this._fmtNum(t.value, t.digits))}</div>
                            <div class="sim-kpi-label">${this._esc(t.label)}</div>
                        </div>
                    </div>`).join('')}
            </div>
            ${meta ? `<div class="small text-muted mb-2">${meta}</div>` : ''}`;
    },

    _runMeta(stats, graph) {
        const s = stats || {};
        const parts = [];
        if (s.algorithm) parts.push(this._esc(s.algorithm.replace(/_/g, ' ')));
        if (s.iterations != null) {
            parts.push(this._esc(`${s.iterations} rounds${s.converged === false ? ' (not converged)' : ''}`));
        }
        const duration = this._fmtElapsed(s.elapsed_ms);
        if (duration) parts.push(this._esc(`duration ${duration}`));
        if (s.class_filter && s.class_filter.length) {
            parts.push(this._esc(`scope: ${s.class_filter.map((u) => this._shortLabel(u)).join(', ')}`));
        }
        const when = this._fmtWhen(s.computed_at);
        if (when) parts.push(this._esc(`computed ${when}`));
        if (graph && graph.capped) {
            parts.push(this._esc(`preview ${(graph.nodes || []).length.toLocaleString()} of ${Number(graph.node_total || 0).toLocaleString()} nodes`));
        }
        if (this.runPageUrl || s.run_page_url) {
            const url = this.runPageUrl || s.run_page_url;
            parts.push(`<a href="${this._esc(url)}" target="_blank" rel="noopener">Databricks run <i class="bi bi-box-arrow-up-right"></i></a>`);
        }
        return parts.join(' · ');
    },

    _table(clusters, total) {
        if (!clusters || !clusters.length) {
            return '<p class="text-muted small mb-0">No clusters.</p>';
        }
        const rows = clusters.map((cl) => `
            <tr class="comm-current-row" data-comm-cluster="${this._esc(cl.id)}" role="button" tabindex="0"
                title="Show the entities in this cluster">
                <td>${this._esc(cl.id)}</td>
                <td class="text-end">${this._esc(this._fmtNum(cl.size))}</td>
                <td class="small text-muted text-truncate">${this._esc(cl.label || '')}</td>
            </tr>
            <tr class="comm-current-members d-none" data-comm-members-for="${this._esc(cl.id)}">
                <td colspan="3"></td>
            </tr>`).join('');
        const more = total > clusters.length
            ? `<p class="small text-muted mt-2 mb-0">Top ${clusters.length} of ${Number(total).toLocaleString()} clusters.</p>`
            : '';
        return `<table class="table table-sm sim-table mb-0">
            <thead><tr><th>Id</th><th class="text-end">Size</th><th>Seed</th></tr></thead>
            <tbody>${rows}</tbody>
        </table>${more}`;
    },

    _memberItems(members) {
        return members.map((m) => {
            const info = this._memberInfo(m);
            return `<li class="comm-member" title="${this._esc(info.uri)}">
                <span class="comm-member-icon" aria-hidden="true">${this._esc(info.icon)}</span>
                <span class="comm-member-text">
                    <span class="comm-member-label">${this._esc(info.label)}</span>
                    <span class="comm-member-type">${this._esc(info.typeLabel)}</span>
                </span>
            </li>`;
        }).join('');
    },

    async _toggleCurrentMembers(clusterId) {
        const row = document.querySelector(`[data-comm-members-for="${clusterId}"]`);
        if (!row) return;
        const open = row.classList.toggle('d-none') === false;
        if (open && !row.dataset.loaded) await this._loadCurrentMembers(clusterId, false);
    },

    async _loadCurrentMembers(clusterId, append) {
        const row = document.querySelector(`[data-comm-members-for="${clusterId}"]`);
        if (!row) return;
        const cell = row.querySelector('td');
        const offset = append ? Number(row.dataset.loaded || 0) : 0;
        try {
            await this._ensureOntologyClasses();
            const page = await this._json(
                `/dtwin/clusters/${clusterId}/members?source=materialized&offset=${offset}&limit=${this.MEMBERS_PAGE}`
            );
            const members = page.members || [];
            row.dataset.loaded = String(offset + members.length);
            let list = cell.querySelector('.comm-member-list');
            if (!list) {
                cell.innerHTML = '<ul class="comm-member-list list-unstyled mb-0"></ul>';
                list = cell.querySelector('.comm-member-list');
            }
            list.insertAdjacentHTML('beforeend', this._memberItems(members));
            const moreBtn = cell.querySelector('[data-comm-current-more]');
            if (moreBtn) moreBtn.remove();
            if (page.has_more) {
                cell.insertAdjacentHTML('beforeend',
                    `<button type="button" class="btn btn-sm btn-link px-0" data-comm-current-more="${clusterId}">Load more</button>`);
            }
        } catch (err) {
            cell.innerHTML = `<p class="text-muted small mb-0">${this._esc(err.message)}</p>`;
        }
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
        this._members = null;
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

    _memberInfo(member) {
        const m = typeof member === 'string' ? { uri: member } : (member || {});
        const typeUri = m.type || '';
        const typeLabel = m.type_label || this._shortLabel(typeUri) || 'Entity';
        return {
            uri: m.uri,
            label: m.label || this._shortLabel(m.uri),
            typeUri,
            typeLabel,
            icon: this._typeIcon(typeUri, typeLabel),
        };
    },

    _clusterById(clusterId) {
        const clusters = (this.latest && this.latest.clusters) || [];
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

    async _showClusterDetails(clusterId) {
        const box = document.getElementById('commPreviewDetails');
        if (!box) return;
        const known = this._clusterById(clusterId);
        const color = this._clusterColor(clusterId);
        box.innerHTML = `
            <div class="d-flex align-items-center gap-2 mb-2">
                <span class="comm-cluster-swatch" style="background:${color}"></span>
                <div>
                    <div class="fw-semibold">Cluster #${this._esc(clusterId)}</div>
                    <div class="small text-muted" data-comm-size>${known ? this._esc(this._fmtNum(known.size)) + ' entities' : ''}</div>
                </div>
            </div>
            <ul class="comm-member-list list-unstyled mb-0"></ul>
            <div class="comm-member-more"></div>`;
        this._members = { clusterId, loaded: 0 };
        await this._loadMoreMembers();
    },

    async _loadMoreMembers() {
        const box = document.getElementById('commPreviewDetails');
        const state = this._members;
        if (!box || !state) return;
        const list = box.querySelector('.comm-member-list');
        const more = box.querySelector('.comm-member-more');
        if (more) more.innerHTML = '<span class="small text-muted">Loading…</span>';
        try {
            const page = await this._json(
                `/dtwin/clusters/${state.clusterId}/members?offset=${state.loaded}&limit=${this.MEMBERS_PAGE}`
            );
            if (this._members !== state) return;
            const members = page.members || [];
            state.loaded += members.length;
            if (list) list.insertAdjacentHTML('beforeend', this._memberItems(members));
            const sizeEl = box.querySelector('[data-comm-size]');
            if (sizeEl && page.size != null) {
                sizeEl.textContent = `${this._fmtNum(page.size)} entities`;
            }
            if (more) {
                more.innerHTML = page.has_more
                    ? `<button type="button" class="btn btn-sm btn-link px-0" data-comm-more>Load more (${this._fmtNum(state.loaded)} of ${this._fmtNum(page.size)})</button>`
                    : '';
            }
            if (!state.loaded && list) list.innerHTML = '<li class="text-muted small">No members.</li>';
        } catch (err) {
            if (more) more.innerHTML = `<span class="small text-danger">${this._esc(err.message)}</span>`;
        }
    },

    async _selectCluster(clusterId) {
        this.selectedCluster = clusterId;
        this._applyClusterHighlight(clusterId);
        await this._ensureOntologyClasses();
        await this._showClusterDetails(clusterId);
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
            this._selectCluster(graph.getNodeAttribute(node, 'cluster'));
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
        this.latest = null;
        this.runPageUrl = '';
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
        this._showPreviewLoading(false);
        this._syncMaterializeEnabled();
    },

    _showPreviewLoading(loading, message, title) {
        const loadingEl = document.getElementById('commPreviewLoading');
        const emptyEl = document.getElementById('commPreviewEmpty');
        const layoutEl = document.getElementById('commPreviewLayout');
        const statsEl = document.getElementById('commPreviewStats');
        const msgEl = document.getElementById('commPreviewLoadingMsg');
        const titleEl = document.getElementById('commPreviewLoadingTitle');
        if (titleEl && title) titleEl.textContent = title;
        if (msgEl && message) msgEl.textContent = message;
        if (loading) {
            if (loadingEl) loadingEl.classList.remove('d-none');
            if (emptyEl) emptyEl.classList.add('d-none');
            if (layoutEl) layoutEl.classList.add('d-none');
            if (statsEl) statsEl.classList.add('d-none');
        } else if (loadingEl) {
            loadingEl.classList.add('d-none');
        }
    },

    _showCurrentLoading(loading) {
        const loadingEl = document.getElementById('commCurrentLoading');
        const body = document.getElementById('commCurrentBody');
        const statsEl = document.getElementById('commCurrentStats');
        if (loading) {
            if (loadingEl) loadingEl.classList.remove('d-none');
            if (body) body.classList.add('d-none');
            if (statsEl) statsEl.classList.add('d-none');
        } else {
            if (loadingEl) loadingEl.classList.add('d-none');
            if (body) body.classList.remove('d-none');
        }
    },

    async _populateScope() {
        const select = document.getElementById('commClassFilter');
        if (!select || select.dataset.ready) return;
        const classes = await this._ensureOntologyClasses();
        const options = classes
            .map((cls) => ({ uri: cls.uri || cls.id || '', name: cls.label || cls.name || this._shortLabel(cls.uri) }))
            .filter((c) => c.uri)
            .sort((a, b) => String(a.name).localeCompare(String(b.name)));
        select.insertAdjacentHTML('beforeend', options.map(
            (c) => `<option value="${this._esc(c.uri)}">${this._esc(c.name)}</option>`
        ).join(''));
        select.dataset.ready = '1';
    },

    async _findRunningTask(types) {
        try {
            const data = await this._json('/tasks/?include_completed=false');
            return (data.tasks || []).find((t) => types.includes(t.task_type)
                && (t.status === 'running' || t.status === 'pending')) || null;
        } catch (_) {
            return null;
        }
    },

    async refresh() {
        this._populateScope();
        await Promise.all([this.loadLatest(), this.loadCurrent()]);
        if (this._busy) return;
        const running = await this._findRunningTask(['community_detection', 'community_materialize']);
        if (!running) return;
        if (running.task_type === 'community_detection') {
            await this._awaitDetection(running.id);
        } else {
            await this._awaitMaterialize(running.id);
        }
    },

    async loadLatest(opts) {
        const quiet = !!(opts && opts.quiet);
        if (!quiet) {
            this._showPreviewLoading(
                true,
                'Fetching the last detection run…',
                'Loading preview…'
            );
        }
        try {
            const data = await this._json(`/dtwin/clusters/latest?top=${this.TOP_CLUSTERS}`);
            this.blockedReason = data.job_available === false ? (data.blocked_reason || '') : '';
            this._renderJobStatus();
            this._syncDetectEnabled();
            if (!data.stats) {
                this._erasePreview();
                return;
            }
            this.latest = { clusters: data.clusters || [], stats: data.stats };
            await this._ensureOntologyClasses();
            const graph = data.graph || { nodes: [], edges: [] };
            this._showPreviewLoading(false);
            this._renderStats('commPreviewStats', data.stats, this._runMeta(data.stats, graph));
            await this._renderPreviewGraph(graph);
        } catch (err) {
            this._showPreviewLoading(false);
            this._notify(err.message || 'Could not load the last detection run', 'danger');
        } finally {
            this._syncMaterializeEnabled();
        }
    },

    async _awaitDetection(taskId) {
        this._setBusy(true);
        this._setBtnSpinner('commDetectBtn', true);
        this._showPreviewLoading(
            true,
            'Running label propagation on Databricks…',
            'Detecting communities…'
        );
        try {
            const task = await waitForTask(taskId, (t) => {
                if (t && t.message) {
                    this._showPreviewLoading(true, t.message, 'Detecting communities…');
                }
            });
            this.runPageUrl = (task.result && task.result.run_page_url) || '';
            await this.loadLatest({ quiet: true });
            const stats = (this.latest && this.latest.stats) || {};
            this._notify(
                `${this._fmtNum(stats.cluster_count || 0)} communities detected — materialize to publish.`,
                'success'
            );
        } catch (err) {
            this._showPreviewLoading(false);
            await this.loadLatest({ quiet: true });
            this._notify(err.message || 'Detection failed', 'danger');
        } finally {
            this._setBtnSpinner('commDetectBtn', false);
            this._setBusy(false);
        }
    },

    async detect() {
        if (this._busy || !this._canEdit() || this.blockedReason) return;
        const scope = document.getElementById('commClassFilter')?.value || '';
        this._setBtnSpinner('commDetectBtn', true);
        this._setBusy(true);
        try {
            const data = await this._json('/dtwin/clusters/detect', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(scope ? { class_filter: [scope] } : {}),
            });
            if (typeof refreshTasks === 'function') refreshTasks();
            this._notify('Community detection started on Databricks.', 'info');
            // _awaitDetection owns busy/spinner teardown.
            await this._awaitDetection(data.task_id);
        } catch (err) {
            this._setBtnSpinner('commDetectBtn', false);
            this._setBusy(false);
            this._notify(err.message || 'Detection failed', 'danger');
        }
    },

    async _awaitMaterialize(taskId) {
        this._setBusy(true);
        this._setBtnSpinner('commMaterializeBtn', true);
        this._showCurrentLoading(true);
        try {
            const task = await waitForTask(taskId);
            this._notify(task.message || 'Communities materialized.', 'success');
        } catch (err) {
            this._notify(err.message || 'Materialize failed', 'danger');
        } finally {
            this._setBtnSpinner('commMaterializeBtn', false);
            this._setBusy(false);
            await this.loadCurrent({ quiet: true });
        }
    },

    async materialize() {
        if (this._busy || !this._canEdit() || !(this.latest && this.latest.stats)) return;
        this._setBtnSpinner('commMaterializeBtn', true);
        this._setBusy(true);
        try {
            const data = await this._json('/dtwin/clusters/materialize', { method: 'POST' });
            if (typeof refreshTasks === 'function') refreshTasks();
            this._notify('Writing communities into the graph…', 'info');
            // _awaitMaterialize owns busy/spinner teardown.
            await this._awaitMaterialize(data.task_id);
        } catch (err) {
            this._setBtnSpinner('commMaterializeBtn', false);
            this._setBusy(false);
            this._notify(err.message || 'Materialize failed', 'danger');
        }
    },

    async clear() {
        if (this._busy || !this._canEdit()) return;
        this._setBusy(true);
        try {
            await this._json('/dtwin/clusters/current', { method: 'DELETE' });
            this._notify('Materialized clusters removed.', 'info');
            await this.loadCurrent();
        } catch (err) {
            this._notify(err.message || 'Clear failed', 'danger');
        } finally {
            this._setBusy(false);
        }
    },

    async loadCurrent(opts) {
        const quiet = !!(opts && opts.quiet);
        if (!quiet) this._showCurrentLoading(true);
        try {
            const data = await this._json(`/dtwin/clusters/current?top=${this.TOP_CLUSTERS}`);
            this.current = data;
            const clusters = data.clusters || [];
            const stats = data.stats || {};
            this._renderStats('commCurrentStats', stats, '');
            const body = document.getElementById('commCurrentBody');
            if (body) {
                body.innerHTML = clusters.length
                    ? this._table(clusters, stats.cluster_count || clusters.length)
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
            this._renderStats('commCurrentStats', null, '');
            const body = document.getElementById('commCurrentBody');
            if (body) {
                body.innerHTML = `<p class="text-muted small mb-0">${this._esc(err.message)}</p>`;
            }
        } finally {
            this._showCurrentLoading(false);
        }
    },
};

document.addEventListener('DOMContentLoaded', () => IntelligenceCommunities.init());
window.IntelligenceCommunities = IntelligenceCommunities;
