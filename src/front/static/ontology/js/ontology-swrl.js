/**
 * OntoBricks — ontology-swrl.js
 * SWRL Visual Graph Editor + rules list
 *
 * Replaces the 4-step wizard with a fullscreen D3 ontology graph
 * where the user clicks entities/relationships and assigns them
 * to IF (blue) or THEN (red) via a contextual menu.
 */

window.SwrlModule = {

    // ── Persistent state ─────────────────────────────────
    rules: [],
    editingIndex: -1,
    classes: [],
    properties: [],
    _rawClasses: [],
    _rawProperties: [],

    // ── Graph editor state ───────────────────────────────
    _svg: null,
    _zoom: null,
    _simulation: null,
    _graphNodes: [],
    _graphLinks: [],
    _modal: null,
    _readOnly: false,

    // Selection state
    ifNodes: new Set(),
    thenNodes: new Set(),
    ifLinks: new Set(),
    thenLinks: new Set(),
    nodeVars: new Map(),

    // Context menu state
    _ctxTarget: null,
    _ctxType: null,

    ATOM_RE: /([A-Za-z_][\w.]*)\(([^)]+)\)/g,
    VAR_NAMES: ['?x', '?y', '?z', '?w', '?v1', '?v2', '?v3', '?v4', '?v5'],
    rawMode: false,

    // ── Initialisation ───────────────────────────────────

    init() {
        this.loadRules();
        this.loadOntologyItems();
    },

    async loadRules() {
        const spinner = document.getElementById('swrlRulesSpinner');
        const list = document.getElementById('swrlRulesList');
        if (spinner) spinner.style.display = '';
        if (list) list.style.display = 'none';
        try {
            const r = await fetch('/ontology/swrl/list');
            const d = await r.json();
            if (d.success) {
                this.rules = d.rules || [];
                this.renderRulesList();
            }
        } catch (e) {
            console.error('Error loading SWRL rules:', e);
        } finally {
            if (spinner) spinner.style.display = 'none';
            if (list) list.style.display = '';
        }
    },

    async loadOntologyItems() {
        try {
            const r = await fetch('/ontology/get-loaded-ontology');
            const d = await r.json();
            if (d.success && d.ontology) {
                this._rawClasses = d.ontology.classes || [];
                this._rawProperties = d.ontology.properties || [];
                this.classes = this._rawClasses.map(c => c.name || c.uri);
                this.properties = this._rawProperties.map(p => p.name || p.uri);
            }
        } catch (e) {
            console.log('Could not load ontology items:', e);
        }
    },

    _classEmoji(name) {
        const cls = (this._rawClasses || []).find(c => (c.name || c.uri) === name);
        if (cls && cls.emoji) return cls.emoji;
        return (typeof OntologyState !== 'undefined' && OntologyState.defaultClassEmoji)
            ? OntologyState.defaultClassEmoji : '📦';
    },

    // ── Rules list ───────────────────────────────────────

    renderRulesList() {
        const container = document.getElementById('swrlRulesList');
        const noMsg = document.getElementById('noSwrlRulesMessage');

        if (this.rules.length === 0) {
            noMsg.style.display = 'block';
            return;
        }
        noMsg.style.display = 'none';
        const canEdit = window.isActiveVersion !== false;

        let html = '';
        this.rules.forEach((rule, i) => {
            const enabled = rule.enabled !== false;
            const disabledBadge = enabled ? '' : '<span class="badge bg-secondary me-1" style="font-size:.6rem">disabled</span>';
            const toggleIcon = enabled ? 'bi-toggle-on text-success' : 'bi-toggle-off text-danger';
            const toggleTitle = enabled ? 'Disable' : 'Enable';
            const cardOpacity = enabled ? '' : ' style="opacity:0.55"';

            const actions = canEdit ? `
                <div class="btn-group btn-group-sm ontology-edit-btn">
                    <button class="btn btn-outline-secondary" onclick="SwrlModule.toggleEnabled(${i})" title="${toggleTitle}">
                        <i class="bi ${toggleIcon}"></i>
                    </button>
                    <button class="btn btn-outline-secondary" onclick="SwrlModule.editRule(${i})" title="Edit">
                        <i class="bi bi-pencil"></i>
                    </button>
                    <button class="btn btn-outline-danger" onclick="SwrlModule.deleteRule(${i})" title="Delete">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>` : `
                <button class="btn btn-sm btn-outline-secondary" onclick="SwrlModule.viewRule(${i})" title="View">
                    <i class="bi bi-eye"></i> View
                </button>`;

            html += `
                <div class="card mb-2 swrl-rule-card"${cardOpacity}>
                    <div class="card-body py-2 px-3">
                        <div class="d-flex justify-content-between align-items-start">
                            <div class="flex-grow-1">
                                <strong>${disabledBadge}${this._esc(rule.name)}</strong>
                                ${rule.description ? `<small class="text-muted d-block">${this._esc(rule.description)}</small>` : ''}
                                <div class="mt-1">
                                    <code class="small">${this._esc(rule.antecedent)}</code>
                                    <span class="mx-2">&rarr;</span>
                                    <code class="small">${this._esc(rule.consequent)}</code>
                                </div>
                            </div>
                            ${actions}
                        </div>
                    </div>
                </div>`;
        });
        container.innerHTML = html + noMsg.outerHTML;
    },

    async toggleEnabled(index) {
        if (!this.rules[index]) return;
        const rule = { ...this.rules[index], enabled: !this.rules[index].enabled };
        try {
            const r = await fetch('/ontology/swrl/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rule, index })
            });
            const d = await r.json();
            if (d.success) {
                this.rules = d.rules || [];
                this.renderRulesList();
                if (typeof OntologyState !== 'undefined' && OntologyState.config) {
                    OntologyState.config.swrl_rules = this.rules;
                }
                if (typeof BusinessRulesModule !== 'undefined') BusinessRulesModule._refreshAllBadges();
            }
        } catch (e) {
            console.error('Error toggling SWRL rule:', e);
        }
    },

    // ── Editor open / close ──────────────────────────────

    _resetEditor() {
        this.editingIndex = -1;
        this.ifNodes = new Set();
        this.thenNodes = new Set();
        this.ifLinks = new Set();
        this.thenLinks = new Set();
        this.nodeVars = new Map();
        this.rawMode = false;
        this._ctxTarget = null;
        this._ctxType = null;
        this._readOnly = false;

        const name = document.getElementById('swrlRuleName');
        const desc = document.getElementById('swrlRuleDescription');
        if (name) name.value = '';
        if (desc) desc.value = '';
        const rawToggle = document.getElementById('swrlRawToggle');
        if (rawToggle) rawToggle.checked = false;
        const rawEditor = document.getElementById('swrlRawEditor');
        if (rawEditor) rawEditor.style.display = 'none';

        this._hideContextMenu();
    },

    async _openEditor(title) {
        this._hideContextMenu();
        const modalEl = document.getElementById('swrlGraphEditorModal');
        document.getElementById('swrlEditorTitle').innerHTML = `<i class="bi bi-lightning me-2"></i>${this._esc(title)}`;
        this._modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        this._modal.show();

        const container = document.getElementById('swrlGraphContainer');
        container.querySelectorAll('svg, .swrl-legend').forEach(el => el.remove());
        container.insertAdjacentHTML('afterbegin',
            '<div class="d-flex justify-content-center align-items-center h-100 swrl-graph-loading">' +
            '<div class="text-center text-muted"><div class="spinner-border spinner-border-sm text-primary mb-3"></div>' +
            '<div>Loading ontology graph...</div></div></div>');

        try {
            await this._ensureD3();
            const config = await this._getOntologyConfig();
            const layout = await this._fetchMapLayout();

            container.querySelector('.swrl-graph-loading')?.remove();

            if (!config || !config.classes || config.classes.length === 0) {
                container.insertAdjacentHTML('afterbegin',
                    '<div class="d-flex justify-content-center align-items-center h-100">' +
                    '<div class="text-center text-muted"><i class="bi bi-diagram-3 fs-1 d-block mb-2"></i>' +
                    '<p>No ontology defined yet.<br>Create entities first.</p></div></div>');
                return;
            }

            await new Promise(resolve => {
                if (container.clientWidth > 0 && container.clientHeight > 0) resolve();
                else modalEl.addEventListener('shown.bs.modal', resolve, { once: true });
            });

            this._buildGraph(container, config, layout);

            if (layout) {
                setTimeout(() => this.fitToView(), 200);
            } else {
                this._simulation.on('end', () => this.fitToView());
                setTimeout(() => this.fitToView(), 2000);
            }
        } catch (err) {
            console.error('[SwrlModule] Graph error:', err);
            container.querySelector('.swrl-graph-loading')?.remove();
            container.insertAdjacentHTML('afterbegin',
                `<div class="d-flex justify-content-center align-items-center h-100">` +
                `<div class="text-center text-danger"><i class="bi bi-exclamation-triangle fs-1 d-block mb-2"></i>` +
                `<p>Failed to load graph.<br><small>${this._esc(err.message)}</small></p></div></div>`);
        }

        this._updateRulePane();
    },

    addRule() {
        this._resetEditor();
        this._openEditor('Add SWRL Rule');
    },

    editRule(index) {
        this._resetEditor();
        this.editingIndex = index;
        const rule = this.rules[index];
        document.getElementById('swrlRuleName').value = rule.name || '';
        document.getElementById('swrlRuleDescription').value = rule.description || '';
        this._openEditor('Edit SWRL Rule').then(() => {
            this._prefillFromRule(rule);
        });
    },

    viewRule(index) {
        this._resetEditor();
        this._readOnly = true;
        this.editingIndex = index;
        const rule = this.rules[index];
        document.getElementById('swrlRuleName').value = rule.name || '';
        document.getElementById('swrlRuleDescription').value = rule.description || '';
        this._openEditor('View SWRL Rule').then(() => {
            this._prefillFromRule(rule);
            document.querySelectorAll('#swrlRulePane input, #swrlRulePane textarea').forEach(el => { el.disabled = true; });
            document.getElementById('swrlSaveBtn').style.display = 'none';
        });
    },

    // ── D3 Helpers ───────────────────────────────────────

    _ensureD3() {
        if (typeof d3 !== 'undefined') return Promise.resolve();
        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = 'https://d3js.org/d3.v7.min.js';
            script.onload = resolve;
            script.onerror = () => reject(new Error('Failed to load D3.js'));
            document.head.appendChild(script);
        });
    },

    async _getOntologyConfig() {
        if (typeof OntologyState !== 'undefined' && OntologyState.config &&
            OntologyState.config.classes && OntologyState.config.classes.length > 0) {
            return OntologyState.config;
        }
        const resp = await fetch('/ontology/load', { credentials: 'same-origin' });
        const data = await resp.json();
        return data.success ? data.config : null;
    },

    async _fetchMapLayout() {
        try {
            const resp = await fetch('/project/map-layout', { credentials: 'same-origin' });
            const data = await resp.json();
            return (data.success && data.layout) ? data.layout : null;
        } catch { return null; }
    },

    // ── Graph construction ───────────────────────────────

    _buildGraph(container, config, savedLayout) {
        const classes = config.classes || [];
        const properties = config.properties || [];
        const width = container.clientWidth || window.innerWidth * 0.7;
        const height = container.clientHeight || window.innerHeight - 60;

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
        this._graphNodes = nodes;

        const validIds = new Set(nodes.map(n => n.id));
        const links = [];

        properties.forEach(prop => {
            if (prop.domain && prop.range && validIds.has(prop.domain) && validIds.has(prop.range)) {
                links.push({
                    source: prop.domain, target: prop.range,
                    name: prop.name, type: 'relationship',
                    direction: prop.direction || 'forward',
                    linkId: `rel__${prop.name}__${prop.domain}__${prop.range}`
                });
            }
        });

        classes.forEach(cls => {
            if (cls.parent && validIds.has(cls.parent)) {
                links.push({
                    source: cls.parent, target: cls.name,
                    name: 'inherits', type: 'inheritance',
                    linkId: `inh__${cls.parent}__${cls.name}`
                });
            }
        });
        this._graphLinks = links;

        // Multi-edge indexing
        const linkCountMap = new Map();
        links.forEach(l => {
            const key = [typeof l.source === 'object' ? l.source.id : l.source,
                         typeof l.target === 'object' ? l.target.id : l.target].sort().join('|');
            linkCountMap.set(key, (linkCountMap.get(key) || 0) + 1);
        });
        const linkIndexMap = new Map();
        links.forEach(l => {
            const key = [typeof l.source === 'object' ? l.source.id : l.source,
                         typeof l.target === 'object' ? l.target.id : l.target].sort().join('|');
            const idx = linkIndexMap.get(key) || 0;
            l.linkCount = linkCountMap.get(key);
            l.linkIndex = idx;
            linkIndexMap.set(key, idx + 1);
        });

        const selfLoopLinks = links.filter(l => {
            const s = typeof l.source === 'object' ? l.source.id : l.source;
            const t = typeof l.target === 'object' ? l.target.id : l.target;
            return s === t;
        });
        const regularLinks = links.filter(l => {
            const s = typeof l.source === 'object' ? l.source.id : l.source;
            const t = typeof l.target === 'object' ? l.target.id : l.target;
            return s !== t;
        });

        const selfLoopCountMap = new Map();
        selfLoopLinks.forEach(l => {
            const sid = typeof l.source === 'object' ? l.source.id : l.source;
            const count = selfLoopCountMap.get(sid) || 0;
            l.selfLoopIndex = count;
            selfLoopCountMap.set(sid, count + 1);
        });
        selfLoopLinks.forEach(l => {
            const sid = typeof l.source === 'object' ? l.source.id : l.source;
            l.selfLoopCount = selfLoopCountMap.get(sid);
        });

        this._svg = d3.select(container).append('svg').attr('width', width).attr('height', height);
        this._zoom = d3.zoom().scaleExtent([0.2, 4]).on('zoom', (e) => g.attr('transform', e.transform));
        this._svg.call(this._zoom);
        this._svg.on('click', () => this._hideContextMenu());

        const g = this._svg.append('g');
        const defs = this._svg.append('defs');

        defs.append('marker').attr('id', 'swrl-arrow').attr('viewBox', '0 -5 10 10')
            .attr('refX', 28).attr('refY', 0).attr('markerWidth', 6).attr('markerHeight', 6).attr('orient', 'auto')
            .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#6c757d');

        defs.append('marker').attr('id', 'swrl-arrow-self').attr('viewBox', '0 -5 10 10')
            .attr('refX', 10).attr('refY', 0).attr('markerWidth', 5).attr('markerHeight', 5).attr('orient', 'auto')
            .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#495057');

        defs.append('marker').attr('id', 'swrl-arrow-inh').attr('viewBox', '0 -5 10 10')
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
        this._simulation = simulation;

        // Relationship paths
        const relLinks = g.append('g').attr('class', 'swrl-rel-links').selectAll('path')
            .data(regularLinks.filter(l => l.type === 'relationship'))
            .enter().append('path').attr('class', 'map-link')
            .attr('data-link-id', d => d.linkId)
            .style('marker-end', 'url(#swrl-arrow)');

        // Relationship hitareas (invisible wider targets)
        const relHitareas = g.append('g').attr('class', 'swrl-rel-hitareas').selectAll('path')
            .data(regularLinks.filter(l => l.type === 'relationship'))
            .enter().append('path').attr('class', 'swrl-link-hitarea')
            .attr('data-link-id', d => d.linkId)
            .on('click', (event, d) => { event.stopPropagation(); this._onLinkClick(event, d); });

        // Inheritance paths
        const inhLinks = g.append('g').selectAll('path')
            .data(regularLinks.filter(l => l.type === 'inheritance'))
            .enter().append('path').attr('class', 'map-link inheritance')
            .style('marker-end', 'url(#swrl-arrow-inh)');

        // Self-loop paths
        const selfLoops = g.append('g').selectAll('path')
            .data(selfLoopLinks).enter().append('path')
            .attr('class', d => d.type === 'relationship' ? 'map-link self-loop' : 'map-link self-loop inheritance')
            .attr('data-link-id', d => d.linkId)
            .style('marker-end', d => d.type === 'relationship' ? 'url(#swrl-arrow-self)' : 'url(#swrl-arrow-inh)');

        // Self-loop hitareas
        const selfHitareas = g.append('g').selectAll('path')
            .data(selfLoopLinks.filter(l => l.type === 'relationship'))
            .enter().append('path').attr('class', 'swrl-link-hitarea')
            .attr('data-link-id', d => d.linkId)
            .on('click', (event, d) => { event.stopPropagation(); this._onLinkClick(event, d); });

        // Link labels
        const linkLabels = g.append('g').selectAll('text')
            .data(links.filter(l => l.type === 'relationship'))
            .enter().append('text').attr('class', 'swrl-link-label').text(d => d.name);

        // Nodes
        const nodeEls = g.append('g').selectAll('g').data(nodes).enter().append('g')
            .attr('class', 'map-node')
            .attr('data-node-id', d => d.id)
            .on('click', (event, d) => { event.stopPropagation(); this._onNodeClick(event, d); });

        nodeEls.append('circle').attr('class', 'map-node-hitarea').attr('r', 25);
        nodeEls.append('text').attr('class', 'map-node-icon').text(d => d.icon);
        nodeEls.append('text').attr('class', 'map-node-label').attr('dy', 35).text(d => d.label);
        nodeEls.append('text').attr('class', 'swrl-var-label').attr('dy', -30).text('');
        nodeEls.append('title').text(d => d.id);

        // Tick handler
        simulation.on('tick', () => {
            const pathFn = (d) => {
                const sx = d.source.x, sy = d.source.y, tx = d.target.x, ty = d.target.y;
                if (d.linkCount > 1) {
                    const offset = (d.linkIndex - (d.linkCount - 1) / 2) * 40;
                    const mx = (sx + tx) / 2, my = (sy + ty) / 2;
                    const dx = tx - sx, dy = ty - sy, len = Math.sqrt(dx * dx + dy * dy) || 1;
                    return `M${sx},${sy} Q${mx + (-dy / len) * offset},${my + (dx / len) * offset} ${tx},${ty}`;
                }
                return `M${sx},${sy} L${tx},${ty}`;
            };

            relLinks.attr('d', pathFn);
            relHitareas.attr('d', pathFn);
            inhLinks.attr('d', d => `M${d.source.x},${d.source.y} L${d.target.x},${d.target.y}`);

            const selfFn = (d) => {
                const node = d.source;
                const baseAngle = -45, angleStep = 90;
                const angle = (baseAngle + (d.selfLoopIndex || 0) * angleStep) * Math.PI / 180;
                const nr = 25, loopSize = 40, ctrlDist = loopSize + 35;
                const sa = angle - 0.3, ea = angle + 0.3;
                return `M${node.x + Math.cos(sa) * nr},${node.y + Math.sin(sa) * nr} ` +
                       `C${node.x + Math.cos(sa) * ctrlDist},${node.y + Math.sin(sa) * ctrlDist} ` +
                       `${node.x + Math.cos(ea) * ctrlDist},${node.y + Math.sin(ea) * ctrlDist} ` +
                       `${node.x + Math.cos(ea) * nr},${node.y + Math.sin(ea) * nr}`;
            };
            selfLoops.attr('d', selfFn);
            selfHitareas.attr('d', selfFn);

            linkLabels.each(function (d) {
                const sx = d.source.x, sy = d.source.y, tx = d.target.x, ty = d.target.y;
                let mx, my;
                if (d.source.id === d.target.id) {
                    const angle = (-45 + (d.selfLoopIndex || 0) * 90) * Math.PI / 180;
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

        // Legend
        const legend = document.createElement('div');
        legend.className = 'swrl-legend';
        legend.innerHTML =
            '<div class="swrl-legend-item"><div class="swrl-legend-swatch" style="background:#6c757d"></div><span>Relationship</span></div>' +
            '<div class="swrl-legend-item"><div class="swrl-legend-swatch" style="background:#adb5bd;border:1px dashed #adb5bd"></div><span>Inheritance</span></div>' +
            '<div class="swrl-legend-item"><div class="swrl-legend-swatch" style="background:#0d6efd"></div><span>IF (condition)</span></div>' +
            '<div class="swrl-legend-item"><div class="swrl-legend-swatch" style="background:#dc3545"></div><span>THEN (conclusion)</span></div>';
        container.appendChild(legend);
    },

    // ── Zoom controls ────────────────────────────────────

    fitToView() {
        if (!this._svg || !this._zoom) return;
        const g = this._svg.select('g');
        const bounds = g.node().getBBox();
        if (bounds.width === 0 || bounds.height === 0) return;
        const container = document.getElementById('swrlGraphContainer');
        const w = container.clientWidth, h = container.clientHeight;
        const padding = 60;
        const scale = Math.min((w - padding * 2) / bounds.width, (h - padding * 2) / bounds.height, 2);
        const tx = w / 2 - scale * (bounds.x + bounds.width / 2);
        const ty = h / 2 - scale * (bounds.y + bounds.height / 2);
        this._svg.transition().duration(500).call(this._zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
    },

    zoomIn() {
        if (this._svg && this._zoom) this._svg.transition().duration(300).call(this._zoom.scaleBy, 1.4);
    },

    zoomOut() {
        if (this._svg && this._zoom) this._svg.transition().duration(300).call(this._zoom.scaleBy, 0.7);
    },

    // ── Click handlers ───────────────────────────────────

    _onNodeClick(event, d) {
        if (this._readOnly) return;
        this._ctxTarget = d.id;
        this._ctxType = 'node';
        this._showContextMenu(event, d.id);
    },

    _onLinkClick(event, d) {
        if (this._readOnly) return;
        this._ctxTarget = d.linkId;
        this._ctxType = 'link';
        this._showContextMenu(event, d.linkId);
    },

    // ── Context menu ─────────────────────────────────────

    _showContextMenu(event, elementId) {
        const menu = document.getElementById('swrlContextMenu');
        const isSelected = this.ifNodes.has(elementId) || this.thenNodes.has(elementId) ||
                          this.ifLinks.has(elementId) || this.thenLinks.has(elementId);

        menu.querySelector('.swrl-ctx-if').style.display = isSelected ? 'none' : '';
        menu.querySelector('.swrl-ctx-then').style.display = isSelected ? 'none' : '';
        menu.querySelector('.swrl-ctx-remove').style.display = isSelected ? '' : 'none';

        menu.style.display = '';
        menu.style.left = event.clientX + 'px';
        menu.style.top = event.clientY + 'px';

        const rect = menu.getBoundingClientRect();
        if (rect.right > window.innerWidth) menu.style.left = (window.innerWidth - rect.width - 8) + 'px';
        if (rect.bottom > window.innerHeight) menu.style.top = (window.innerHeight - rect.height - 8) + 'px';
    },

    _hideContextMenu() {
        const menu = document.getElementById('swrlContextMenu');
        if (menu) menu.style.display = 'none';
    },

    ctxAddToIf() {
        this._hideContextMenu();
        if (this._ctxType === 'node') this._selectNode(this._ctxTarget, 'if');
        else if (this._ctxType === 'link') this._selectLink(this._ctxTarget, 'if');
        this._applyVisualSelection();
        this._updateRulePane();
    },

    ctxAddToThen() {
        this._hideContextMenu();
        if (this._ctxType === 'node') this._selectNode(this._ctxTarget, 'then');
        else if (this._ctxType === 'link') this._selectLink(this._ctxTarget, 'then');
        this._applyVisualSelection();
        this._updateRulePane();
    },

    ctxRemove() {
        this._hideContextMenu();
        if (this._ctxType === 'node') this._removeNode(this._ctxTarget);
        else if (this._ctxType === 'link') this._removeLink(this._ctxTarget);
        this._applyVisualSelection();
        this._updateRulePane();
    },

    // ── Selection logic ──────────────────────────────────

    _nextFreeVar() {
        const used = new Set(this.nodeVars.values());
        for (const v of this.VAR_NAMES) {
            if (!used.has(v)) return v;
        }
        return `?v${this.nodeVars.size + 1}`;
    },

    _ensureNodeVar(nodeId) {
        if (!this.nodeVars.has(nodeId)) {
            this.nodeVars.set(nodeId, this._nextFreeVar());
        }
        return this.nodeVars.get(nodeId);
    },

    _selectNode(nodeId, side) {
        if (side === 'if') {
            this.thenNodes.delete(nodeId);
            this.ifNodes.add(nodeId);
        } else {
            this.ifNodes.delete(nodeId);
            this.thenNodes.add(nodeId);
        }
        this._ensureNodeVar(nodeId);
    },

    _selectLink(linkId, side) {
        const link = this._graphLinks.find(l => l.linkId === linkId);
        if (!link) return;

        if (side === 'if') {
            this.thenLinks.delete(linkId);
            this.ifLinks.add(linkId);
        } else {
            this.ifLinks.delete(linkId);
            this.thenLinks.add(linkId);
        }

        const srcId = typeof link.source === 'object' ? link.source.id : link.source;
        const tgtId = typeof link.target === 'object' ? link.target.id : link.target;

        if (!this.ifNodes.has(srcId) && !this.thenNodes.has(srcId)) {
            if (side === 'if') this.ifNodes.add(srcId); else this.thenNodes.add(srcId);
        }
        if (!this.ifNodes.has(tgtId) && !this.thenNodes.has(tgtId)) {
            if (side === 'if') this.ifNodes.add(tgtId); else this.thenNodes.add(tgtId);
        }
        this._ensureNodeVar(srcId);
        this._ensureNodeVar(tgtId);
    },

    _removeNode(nodeId) {
        this.ifNodes.delete(nodeId);
        this.thenNodes.delete(nodeId);

        const orphanedLinks = this._graphLinks.filter(l => {
            const sid = typeof l.source === 'object' ? l.source.id : l.source;
            const tid = typeof l.target === 'object' ? l.target.id : l.target;
            return (sid === nodeId || tid === nodeId) && (this.ifLinks.has(l.linkId) || this.thenLinks.has(l.linkId));
        });
        orphanedLinks.forEach(l => {
            this.ifLinks.delete(l.linkId);
            this.thenLinks.delete(l.linkId);
        });

        const stillReferenced = new Set();
        for (const lid of [...this.ifLinks, ...this.thenLinks]) {
            const l = this._graphLinks.find(x => x.linkId === lid);
            if (l) {
                stillReferenced.add(typeof l.source === 'object' ? l.source.id : l.source);
                stillReferenced.add(typeof l.target === 'object' ? l.target.id : l.target);
            }
        }
        if (!stillReferenced.has(nodeId) && !this.ifNodes.has(nodeId) && !this.thenNodes.has(nodeId)) {
            this.nodeVars.delete(nodeId);
        }
    },

    _removeLink(linkId) {
        this.ifLinks.delete(linkId);
        this.thenLinks.delete(linkId);

        const link = this._graphLinks.find(l => l.linkId === linkId);
        if (!link) return;

        const srcId = typeof link.source === 'object' ? link.source.id : link.source;
        const tgtId = typeof link.target === 'object' ? link.target.id : link.target;

        [srcId, tgtId].forEach(nid => {
            const hasOtherLinks = this._graphLinks.some(l => {
                if (l.linkId === linkId) return false;
                const s = typeof l.source === 'object' ? l.source.id : l.source;
                const t = typeof l.target === 'object' ? l.target.id : l.target;
                return (s === nid || t === nid) && (this.ifLinks.has(l.linkId) || this.thenLinks.has(l.linkId));
            });
            if (!hasOtherLinks && !this.ifNodes.has(nid) && !this.thenNodes.has(nid)) {
                this.nodeVars.delete(nid);
            }
        });
    },

    // ── Visual highlight application ─────────────────────

    _applyVisualSelection() {
        if (!this._svg) return;

        this._svg.selectAll('.map-node').each(function(d) {
            const el = d3.select(this);
            el.classed('swrl-node-if', SwrlModule.ifNodes.has(d.id));
            el.classed('swrl-node-then', SwrlModule.thenNodes.has(d.id));

            const varLabel = el.select('.swrl-var-label');
            const v = SwrlModule.nodeVars.get(d.id);
            varLabel.text(v || '');
        });

        this._svg.selectAll('[data-link-id]').each(function(d) {
            if (!d || !d.linkId) return;
            const el = d3.select(this);
            if (el.classed('swrl-link-hitarea')) return;
            el.classed('swrl-link-if', SwrlModule.ifLinks.has(d.linkId));
            el.classed('swrl-link-then', SwrlModule.thenLinks.has(d.linkId));
        });
    },

    // ── Rule pane update ─────────────────────────────────

    _updateRulePane() {
        const atoms = this._buildAtomsFromSelection();
        const ifAtoms = atoms.ifAtoms;
        const thenAtoms = atoms.thenAtoms;

        // IF section
        const ifContainer = document.getElementById('swrlIfAtoms');
        const ifEmpty = document.getElementById('swrlIfEmpty');
        const ifCount = document.getElementById('swrlIfCount');
        if (ifContainer) {
            if (ifAtoms.length === 0) {
                ifContainer.innerHTML = '';
                if (ifEmpty) { ifEmpty.style.display = ''; ifContainer.appendChild(ifEmpty); }
            } else {
                let html = '';
                ifAtoms.forEach((a, i) => {
                    const display = a.kind === 'class'
                        ? `${this._esc(a.name)}(${this._esc(a.args[0])})`
                        : `${this._esc(a.name)}(${this._esc(a.args[0])}, ${this._esc(a.args[1])})`;
                    html += `<div class="swrl-atom-badge if-atom">` +
                        `<span class="swrl-atom-type">${a.kind === 'class' ? 'CLS' : 'REL'}</span>` +
                        `<span>${display}</span>` +
                        (this._readOnly ? '' : `<span class="swrl-atom-remove" onclick="SwrlModule._removeAtomByIndex('if',${i})" title="Remove"><i class="bi bi-x-lg"></i></span>`) +
                        `</div>`;
                });
                ifContainer.innerHTML = html;
            }
            if (ifCount) ifCount.textContent = ifAtoms.length;
        }

        // THEN section
        const thenContainer = document.getElementById('swrlThenAtoms');
        const thenEmpty = document.getElementById('swrlThenEmpty');
        const thenCount = document.getElementById('swrlThenCount');
        if (thenContainer) {
            if (thenAtoms.length === 0) {
                thenContainer.innerHTML = '';
                if (thenEmpty) { thenEmpty.style.display = ''; thenContainer.appendChild(thenEmpty); }
            } else {
                let html = '';
                thenAtoms.forEach((a, i) => {
                    const display = a.kind === 'class'
                        ? `${this._esc(a.name)}(${this._esc(a.args[0])})`
                        : `${this._esc(a.name)}(${this._esc(a.args[0])}, ${this._esc(a.args[1])})`;
                    html += `<div class="swrl-atom-badge then-atom">` +
                        `<span class="swrl-atom-type">${a.kind === 'class' ? 'CLS' : 'REL'}</span>` +
                        `<span>${display}</span>` +
                        (this._readOnly ? '' : `<span class="swrl-atom-remove" onclick="SwrlModule._removeAtomByIndex('then',${i})" title="Remove"><i class="bi bi-x-lg"></i></span>`) +
                        `</div>`;
                });
                thenContainer.innerHTML = html;
            }
            if (thenCount) thenCount.textContent = thenAtoms.length;
        }

        // SWRL preview
        const antStr = this._buildSwrlString(ifAtoms);
        const conStr = this._buildSwrlString(thenAtoms);
        const preview = document.getElementById('swrlPreview');
        if (preview) {
            if (!antStr && !conStr) {
                preview.innerHTML = '<span class="text-muted fst-italic">Select elements to build the rule</span>';
            } else {
                preview.innerHTML = `${this._esc(antStr)} <span class="swrl-arrow">&rarr;</span> ${this._esc(conStr)}`;
            }
        }

        // Raw editor sync
        const rawAnt = document.getElementById('swrlRawAntecedent');
        const rawCon = document.getElementById('swrlRawConsequent');
        if (rawAnt) rawAnt.value = antStr;
        if (rawCon) rawCon.value = conStr;
    },

    _buildAtomsFromSelection() {
        const ifAtoms = [];
        const thenAtoms = [];

        for (const nodeId of this.ifNodes) {
            const v = this.nodeVars.get(nodeId) || '?';
            ifAtoms.push({ kind: 'class', name: nodeId, args: [v] });
        }
        for (const linkId of this.ifLinks) {
            const link = this._graphLinks.find(l => l.linkId === linkId);
            if (!link) continue;
            const srcId = typeof link.source === 'object' ? link.source.id : link.source;
            const tgtId = typeof link.target === 'object' ? link.target.id : link.target;
            const sv = this.nodeVars.get(srcId) || '?';
            const tv = this.nodeVars.get(tgtId) || '?';
            ifAtoms.push({ kind: 'property', name: link.name, args: [sv, tv] });
        }

        for (const nodeId of this.thenNodes) {
            const v = this.nodeVars.get(nodeId) || '?';
            thenAtoms.push({ kind: 'class', name: nodeId, args: [v] });
        }
        for (const linkId of this.thenLinks) {
            const link = this._graphLinks.find(l => l.linkId === linkId);
            if (!link) continue;
            const srcId = typeof link.source === 'object' ? link.source.id : link.source;
            const tgtId = typeof link.target === 'object' ? link.target.id : link.target;
            const sv = this.nodeVars.get(srcId) || '?';
            const tv = this.nodeVars.get(tgtId) || '?';
            thenAtoms.push({ kind: 'property', name: link.name, args: [sv, tv] });
        }

        return { ifAtoms, thenAtoms };
    },

    _removeAtomByIndex(side, index) {
        const atoms = this._buildAtomsFromSelection();
        const list = side === 'if' ? atoms.ifAtoms : atoms.thenAtoms;
        if (index < 0 || index >= list.length) return;

        const atom = list[index];
        if (atom.kind === 'class') {
            const nodeId = atom.name;
            if (side === 'if') this._removeNode(nodeId); else { this.thenNodes.delete(nodeId); this._cleanupOrphanedVars(); }
        } else {
            const link = this._graphLinks.find(l => l.name === atom.name);
            if (link) {
                if (side === 'if') this._removeLink(link.linkId); else { this.thenLinks.delete(link.linkId); this._cleanupOrphanedVars(); }
            }
        }
        this._applyVisualSelection();
        this._updateRulePane();
    },

    _cleanupOrphanedVars() {
        const referenced = new Set();
        for (const nid of [...this.ifNodes, ...this.thenNodes]) referenced.add(nid);
        for (const lid of [...this.ifLinks, ...this.thenLinks]) {
            const l = this._graphLinks.find(x => x.linkId === lid);
            if (l) {
                referenced.add(typeof l.source === 'object' ? l.source.id : l.source);
                referenced.add(typeof l.target === 'object' ? l.target.id : l.target);
            }
        }
        for (const nid of this.nodeVars.keys()) {
            if (!referenced.has(nid)) this.nodeVars.delete(nid);
        }
    },

    // ── SWRL string utilities ────────────────────────────

    _buildSwrlString(atoms) {
        return atoms.map(a => `${a.name}(${a.args.join(', ')})`).join(' \u2227 ');
    },

    _parseSwrlString(str) {
        if (!str) return [];
        const atoms = [];
        const re = new RegExp(this.ATOM_RE.source, 'g');
        let m;
        while ((m = re.exec(str)) !== null) {
            const name = m[1];
            const args = m[2].split(',').map(s => s.trim());
            atoms.push({ kind: args.length === 1 ? 'class' : 'property', name, args });
        }
        return atoms;
    },

    // ── Pre-fill from existing rule ──────────────────────

    _prefillFromRule(rule) {
        if (!rule) return;
        const ifAtoms = this._parseSwrlString(rule.antecedent || '');
        const thenAtoms = this._parseSwrlString(rule.consequent || '');

        const varToNodeId = new Map();

        const processClassAtom = (atom, side) => {
            const nodeId = atom.name;
            const varName = atom.args[0];
            if (!this._graphNodes.find(n => n.id === nodeId)) return;
            if (side === 'if') this.ifNodes.add(nodeId); else this.thenNodes.add(nodeId);
            this.nodeVars.set(nodeId, varName);
            varToNodeId.set(varName, nodeId);
        };

        const processPropertyAtom = (atom, side) => {
            const link = this._graphLinks.find(l => l.name === atom.name && l.type === 'relationship');
            if (!link) return;

            const srcVar = atom.args[0];
            const tgtVar = atom.args[1];
            const srcId = typeof link.source === 'object' ? link.source.id : link.source;
            const tgtId = typeof link.target === 'object' ? link.target.id : link.target;

            if (side === 'if') this.ifLinks.add(link.linkId); else this.thenLinks.add(link.linkId);

            if (!this.nodeVars.has(srcId)) this.nodeVars.set(srcId, srcVar);
            if (!this.nodeVars.has(tgtId)) this.nodeVars.set(tgtId, tgtVar);

            if (!this.ifNodes.has(srcId) && !this.thenNodes.has(srcId)) {
                if (side === 'if') this.ifNodes.add(srcId); else this.thenNodes.add(srcId);
            }
            if (!this.ifNodes.has(tgtId) && !this.thenNodes.has(tgtId)) {
                if (side === 'if') this.ifNodes.add(tgtId); else this.thenNodes.add(tgtId);
            }

            varToNodeId.set(srcVar, srcId);
            varToNodeId.set(tgtVar, tgtId);
        };

        ifAtoms.forEach(a => { if (a.kind === 'class') processClassAtom(a, 'if'); });
        ifAtoms.forEach(a => { if (a.kind === 'property') processPropertyAtom(a, 'if'); });
        thenAtoms.forEach(a => { if (a.kind === 'class') processClassAtom(a, 'then'); });
        thenAtoms.forEach(a => { if (a.kind === 'property') processPropertyAtom(a, 'then'); });

        this._applyVisualSelection();
        this._updateRulePane();
    },

    // ── Raw mode toggle ──────────────────────────────────

    toggleRawMode() {
        const checked = document.getElementById('swrlRawToggle')?.checked;
        this.rawMode = !!checked;
        const editor = document.getElementById('swrlRawEditor');
        if (editor) editor.style.display = this.rawMode ? '' : 'none';
    },

    // ── Save & Delete ────────────────────────────────────

    async saveRule() {
        let antecedent, consequent;

        if (this.rawMode) {
            antecedent = (document.getElementById('swrlRawAntecedent')?.value || '').trim();
            consequent = (document.getElementById('swrlRawConsequent')?.value || '').trim();
        } else {
            const atoms = this._buildAtomsFromSelection();
            antecedent = this._buildSwrlString(atoms.ifAtoms);
            consequent = this._buildSwrlString(atoms.thenAtoms);
        }

        const rule = {
            name: document.getElementById('swrlRuleName').value.trim(),
            description: document.getElementById('swrlRuleDescription').value.trim(),
            antecedent,
            consequent,
            enabled: true
        };
        if (this.editingIndex >= 0 && this.rules[this.editingIndex]) {
            rule.enabled = this.rules[this.editingIndex].enabled !== false;
        }

        if (!rule.name || !rule.antecedent || !rule.consequent) {
            if (typeof showNotification === 'function')
                showNotification('Rule name, IF conditions, and THEN conclusions are all required.', 'warning');
            return;
        }

        try {
            const r = await fetch('/ontology/swrl/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rule, index: this.editingIndex })
            });
            const d = await r.json();

            if (d.success) {
                bootstrap.Modal.getInstance(document.getElementById('swrlGraphEditorModal'))?.hide();
                this.rules = d.rules || [];
                this.renderRulesList();
                if (typeof OntologyState !== 'undefined' && OntologyState.config) {
                    OntologyState.config.swrl_rules = this.rules;
                }
                if (typeof autoGenerateOwl === 'function') autoGenerateOwl();
                if (typeof BusinessRulesModule !== 'undefined') BusinessRulesModule._refreshAllBadges();
            } else {
                if (typeof showNotification === 'function')
                    showNotification('Error saving rule: ' + d.message, 'error');
            }
        } catch (e) {
            if (typeof showNotification === 'function')
                showNotification('Error: ' + e.message, 'error');
        }
    },

    async deleteRule(index) {
        const confirmed = await showConfirmDialog({
            title: 'Delete SWRL Rule',
            message: 'Are you sure you want to delete this SWRL rule?',
            confirmText: 'Delete',
            confirmClass: 'btn-danger',
            icon: 'trash'
        });
        if (!confirmed) return;

        try {
            const r = await fetch('/ontology/swrl/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ index })
            });
            const d = await r.json();

            if (d.success) {
                this.rules = d.rules || [];
                this.renderRulesList();
                if (typeof OntologyState !== 'undefined' && OntologyState.config) {
                    OntologyState.config.swrl_rules = this.rules;
                }
                if (typeof autoGenerateOwl === 'function') autoGenerateOwl();
                if (typeof BusinessRulesModule !== 'undefined') BusinessRulesModule._refreshAllBadges();
            } else {
                if (typeof showNotification === 'function')
                    showNotification('Error deleting rule: ' + d.message, 'error');
            }
        } catch (e) {
            if (typeof showNotification === 'function')
                showNotification('Error: ' + e.message, 'error');
        }
    },

    _esc(s) { return typeof escapeHtml === 'function' ? escapeHtml(s || '') : (s || ''); }
};

// ── Bootstrap wiring ─────────────────────────────────────

document.addEventListener('DOMContentLoaded', function () {
    if (document.getElementById('swrl-section')?.classList.contains('active')) {
        SwrlModule.init();
    }

    const modal = document.getElementById('swrlGraphEditorModal');
    if (modal) {
        modal.addEventListener('hidden.bs.modal', function () {
            modal.querySelectorAll('input, textarea').forEach(el => { el.disabled = false; });
            document.getElementById('swrlSaveBtn').style.display = '';
            SwrlModule._resetEditor();
            if (SwrlModule._simulation) { SwrlModule._simulation.stop(); SwrlModule._simulation = null; }
            SwrlModule._svg = null;
            SwrlModule._zoom = null;
        });
    }

    document.addEventListener('click', function (e) {
        const menu = document.getElementById('swrlContextMenu');
        if (menu && menu.style.display !== 'none' && !menu.contains(e.target)) {
            SwrlModule._hideContextMenu();
        }
    });
});
