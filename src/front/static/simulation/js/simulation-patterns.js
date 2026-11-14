/**
 * Intelligence → Patterns: discover egocentric neighbourhood signatures on
 * Databricks, then save named matches to the graph.
 */
const SimulationPatterns = {
    latest: null,
    blockedReason: '',
    selectedId: null,
    ontologyClasses: null,
    _members: null,
    _busy: false,
    _saveModal: null,

    MEMBERS_PAGE: 100,

    init() {
        const on = (id, fn) => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('click', fn);
        };
        on('patDetectBtn', () => this.detect());
        on('patRefreshBtn', () => this.refreshDiscover());
        on('patSaveBtn', () => this.openSaveModal());
        on('patSaveConfirmBtn', () => this.confirmSave());
        on('patSavedRefreshBtn', () => this.refreshSaved());

        const list = document.getElementById('patPatternList');
        if (list) {
            list.addEventListener('click', (e) => {
                const row = e.target.closest('[data-pat-id]');
                if (row) this.selectPattern(row.dataset.patId);
            });
        }
        const details = document.getElementById('patDetails');
        if (details) {
            details.addEventListener('click', (e) => {
                if (e.target.closest('[data-pat-more]')) this._loadMoreMembers();
            });
        }
        const saved = document.getElementById('patSavedList');
        if (saved) {
            saved.addEventListener('click', (e) => {
                const del = e.target.closest('[data-pat-delete]');
                if (del) {
                    this.deleteSaved(del.dataset.patDelete);
                    return;
                }
                const more = e.target.closest('[data-pat-saved-more]');
                if (more) {
                    this._loadSavedMembers(more.dataset.patSavedMore, true);
                    return;
                }
                const row = e.target.closest('[data-pat-saved]');
                if (row) this._toggleSavedMembers(row.dataset.patSaved);
            });
        }

        const modalEl = document.getElementById('patSaveModal');
        if (modalEl && window.bootstrap) {
            this._saveModal = bootstrap.Modal.getOrCreateInstance(modalEl);
        }

        document.addEventListener('sidebarSectionChanged', (e) => {
            const section = e.detail && e.detail.section;
            if (section === 'patterns') this.refreshDiscover();
            if (section === 'patterns-saved') this.refreshSaved();
        });
        if (document.getElementById('patterns-section')?.classList.contains('active')) {
            this.refreshDiscover();
        }
        if (document.getElementById('patterns-saved-section')?.classList.contains('active')) {
            this.refreshSaved();
        }
    },

    _canEdit() {
        return !(window.OB && typeof window.OB.canEditOntology === 'function'
            && !window.OB.canEditOntology());
    },

    _esc(s) {
        return (window.SimulationPage && SimulationPage.esc)
            ? SimulationPage.esc(s)
            : String(s || '').replace(/[&<>"']/g, (c) => ({
                '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
            }[c]));
    },

    _fmtNum(n) {
        return Number(n || 0).toLocaleString();
    },

    _notify(msg, type) {
        if (typeof showToast === 'function') showToast(msg, type || 'info');
        else if (window.OB && typeof OB.notify === 'function') OB.notify(msg, type);
    },

    async _json(url, opts) {
        const resp = await fetch(url, opts);
        let data = {};
        try { data = await resp.json(); } catch (_) { /* empty */ }
        if (!resp.ok) {
            const msg = data.message || data.error || data.detail || resp.statusText;
            throw new Error(msg || 'Request failed');
        }
        return data;
    },

    _setBusy(on) {
        this._busy = !!on;
        this._syncButtons();
    },

    _setBtnSpinner(id, on) {
        const btn = document.getElementById(id);
        if (!btn) return;
        btn.classList.toggle('is-busy', !!on);
        if (on) {
            if (!btn.dataset.origHtml) btn.dataset.origHtml = btn.innerHTML;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm ob-btn-spinner" role="status" aria-hidden="true"></span>';
            btn.disabled = true;
        } else if (btn.dataset.origHtml) {
            btn.innerHTML = btn.dataset.origHtml;
            delete btn.dataset.origHtml;
            this._syncButtons();
        }
    },

    _syncButtons() {
        const detect = document.getElementById('patDetectBtn');
        if (detect && !detect.classList.contains('is-busy')) {
            detect.disabled = !this._canEdit() || this._busy || !!this.blockedReason
                || !this._selectedClass();
        }
        const save = document.getElementById('patSaveBtn');
        if (save && !save.classList.contains('is-busy')) {
            save.disabled = !this._canEdit() || this._busy || !this.selectedId
                || !(this.latest && this.latest.stats);
        }
    },

    _selectedClass() {
        const el = document.getElementById('patClassFilter');
        return el ? (el.value || '').trim() : '';
    },

    _num(id, fallback) {
        const el = document.getElementById(id);
        const n = el ? Number(el.value) : NaN;
        return Number.isFinite(n) ? n : fallback;
    },

    async _loadClasses() {
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

    async _populateScope() {
        const sel = document.getElementById('patClassFilter');
        if (!sel) return;
        const prev = sel.value;
        const classes = await this._loadClasses();
        const opts = ['<option value="">Select an entity type…</option>'];
        classes.forEach((cls) => {
            const uri = cls.uri || cls.id || '';
            const label = cls.label || cls.name || uri;
            const icon = cls.emoji || '📦';
            if (!uri) return;
            opts.push(`<option value="${this._esc(uri)}">${this._esc(icon)} ${this._esc(label)}</option>`);
        });
        sel.innerHTML = opts.join('');
        if (prev) sel.value = prev;
        if (!sel.dataset.patWired) {
            sel.dataset.patWired = '1';
            sel.addEventListener('change', () => this._syncButtons());
        }
        this._syncButtons();
    },

    _showLoading(on, msg, title) {
        const box = document.getElementById('patPreviewLoading');
        const empty = document.getElementById('patPreviewEmpty');
        const body = document.getElementById('patPreviewBody');
        if (box) box.classList.toggle('d-none', !on);
        if (on) {
            if (empty) empty.classList.add('d-none');
            if (body) body.classList.add('d-none');
            const t = document.getElementById('patPreviewLoadingTitle');
            const m = document.getElementById('patPreviewLoadingMsg');
            if (t && title) t.textContent = title;
            if (m && msg) m.textContent = msg;
        }
    },

    // Signature detail the job settled on (finest level that explained enough anchors).
    _detailLabel(level) {
        return {
            full: 'Exact',
            neighbour: 'Neighbours (1 hop)',
            own: 'Own attributes',
            structure: 'Structure only',
        }[level] || level || '—';
    },

    _kpiTile(label, value) {
        return `<div class="col">`
            + `<div class="sim-kpi-tile h-100">`
            + `<div class="sim-kpi-value">${this._esc(String(value ?? '—'))}</div>`
            + `<div class="sim-kpi-label">${this._esc(label)}</div>`
            + `</div></div>`;
    },

    _renderStats(stats) {
        const el = document.getElementById('patPreviewStats');
        if (!el) return;
        if (!stats) {
            el.classList.add('d-none');
            el.innerHTML = '';
            return;
        }
        el.classList.remove('d-none');
        el.innerHTML = `<div class="row g-2 pat-kpi-row">`
            + this._kpiTile('Patterns', this._fmtNum(stats.pattern_count))
            + this._kpiTile('Anchors', this._fmtNum(stats.anchor_count))
            + this._kpiTile('Hops', this._fmtNum(stats.hop_count))
            + this._kpiTile('Bins', this._fmtNum(stats.bins))
            + this._kpiTile('Min support', this._fmtNum(stats.min_support))
            + this._kpiTile('Detail', this._detailLabel(stats.detail_level))
            + this._kpiTile('Refined', stats.refined ? 'yes' : 'no')
            + `</div>`;
    },

    _renderList() {
        const list = document.getElementById('patPatternList');
        const empty = document.getElementById('patPreviewEmpty');
        const body = document.getElementById('patPreviewBody');
        const patterns = (this.latest && this.latest.patterns) || [];
        if (!patterns.length) {
            if (empty) empty.classList.remove('d-none');
            if (body) body.classList.add('d-none');
            return;
        }
        if (empty) empty.classList.add('d-none');
        if (body) body.classList.remove('d-none');
        if (!list) return;
        list.innerHTML = patterns.map((p) => {
            const active = p.id === this.selectedId ? ' active' : '';
            return `<button type="button" class="pat-pattern-row${active}" data-pat-id="${this._esc(p.id)}">`
                + `<span class="fw-medium">${this._esc(p.label || p.id.slice(0, 8))}</span>`
                + `<span class="text-muted small">support ${this._fmtNum(p.support)}</span>`
                + `</button>`;
        }).join('');
    },

    async selectPattern(patternId) {
        this.selectedId = patternId;
        this._renderList();
        this._syncButtons();
        const details = document.getElementById('patDetails');
        if (!details) return;
        details.innerHTML = '<p class="text-muted small mb-0">Loading…</p>';
        try {
            const [shape, members] = await Promise.all([
                this._json(`/dtwin/patterns/${encodeURIComponent(patternId)}/shape`),
                this._json(`/dtwin/patterns/${encodeURIComponent(patternId)}/members?offset=0&limit=${this.MEMBERS_PAGE}`),
            ]);
            this._members = members;
            this._selectedShape = shape;
            details.innerHTML = this._detailsHtml(shape, members);
        } catch (err) {
            details.innerHTML = `<p class="text-danger small mb-0">${this._esc(err.message || 'Failed')}</p>`;
        }
    },

    _detailsHtml(shape, members) {
        const diagram = shape.diagram
            ? this._diagramSvg(shape.diagram)
            : '<p class="text-muted small mb-0">Re-run <strong>Detect</strong> to draw this pattern.</p>';
        const nodes = (shape.diagram && shape.diagram.nodes) || [];
        const edges = (shape.diagram && shape.diagram.edges) || [];
        const title = shape.seed_label || (shape.pattern_id || '').slice(0, 8) || 'Pattern';
        const rows = (members.members || []).map((m) => (
            `<li class="pat-member-row">`
            + `<span class="fw-medium">${this._esc(m.label || m.uri)}</span>`
            + `<span class="badge rounded-pill text-bg-light border">${this._esc(m.type_label || '—')}</span>`
            + `</li>`
        )).join('');
        const more = members.has_more
            ? '<button type="button" class="btn btn-sm btn-outline-secondary mt-2" data-pat-more>Load more</button>'
            : '';
        return `
            <div class="pat-detail-head">
                <div class="d-flex align-items-baseline justify-content-between gap-2 flex-wrap mb-2">
                    <h6 class="mb-0">${this._esc(title)}</h6>
                    <span class="badge rounded-pill text-bg-primary-subtle border border-primary-subtle">
                        ${this._fmtNum(members.total)} members
                    </span>
                </div>
                <div class="row g-2 pat-kpi-row mb-3">
                    ${this._kpiTile('Members', this._fmtNum(shape.support || members.total))}
                    ${this._kpiTile('Hops', this._fmtNum(shape.hop_count))}
                    ${this._kpiTile('Types', this._fmtNum(nodes.length))}
                    ${this._kpiTile('Relations', this._fmtNum(edges.length))}
                    ${this._kpiTile('Detail', this._detailLabel((this.latest && this.latest.stats || {}).detail_level))}
                    ${this._kpiTile('Refined', shape.refined ? 'yes' : 'no')}
                </div>
            </div>
            <div class="pat-diagram mb-3">${diagram}</div>
            <div class="pat-members">
                <h6 class="mb-2">Matching entities</h6>
                <ul class="pat-member-list mb-0">${rows || '<li class="text-muted">None</li>'}</ul>
                ${more}
            </div>`;
    },

    DIAGRAM: { colW: 280, nodeW: 150, headH: 34, attrH: 14, gapY: 22, pad: 16, top: 26 },

    _classIcon(typeUri, typeLabel) {
        const classes = this.ontologyClasses || [];
        const low = String(typeLabel || '').toLowerCase();
        const cls = classes.find((c) => (c.uri || c.id) === typeUri)
            || classes.find((c) => String(c.name || c.label || '').toLowerCase() === low);
        return (cls && cls.emoji) || '📦';
    },

    _trim(text, max) {
        const s = String(text || '');
        return s.length > max ? `${s.slice(0, max - 1)}…` : s;
    },

    // Columns by hop, nodes stacked in each column and centred vertically.
    _layoutDiagram(diagram) {
        const D = this.DIAGRAM;
        const cols = {};
        diagram.nodes.forEach((n) => { (cols[n.hop] = cols[n.hop] || []).push(n); });
        const heightOf = (n) => D.headH + D.attrH * (n.attributes || []).length;
        const colH = (list) => list.reduce((h, n) => h + heightOf(n), 0) + D.gapY * (list.length - 1);
        const maxH = Math.max(...Object.values(cols).map(colH));
        const pos = {};
        Object.entries(cols).forEach(([hop, list]) => {
            let y = D.top + (maxH - colH(list)) / 2;
            list.forEach((n) => {
                pos[n.id] = { x: D.pad + Number(hop) * D.colW, y, h: heightOf(n) };
                y += heightOf(n) + D.gapY;
            });
        });
        const maxHop = Math.max(...Object.keys(cols).map(Number));
        return {
            pos,
            maxHop,
            width: D.pad * 2 + maxHop * D.colW + D.nodeW,
            height: D.top + maxH + D.pad,
        };
    },

    _diagramEdge(e, pos, incoming, incomingTotal) {
        const D = this.DIAGRAM;
        const a = pos[e.source];
        const b = pos[e.target];
        if (!a || !b) return '';
        const slot = incoming[e.target] = (incoming[e.target] || 0) + 1;
        const nIn = incomingTotal[e.target] || 1;
        const dy = (slot - (nIn + 1) / 2) * 12;
        const x1 = a.x + D.nodeW;
        const y1 = a.y + a.h / 2;
        const x2 = b.x;
        const y2 = b.y + b.h / 2 + dy;
        const mx = (x1 + x2) / 2;
        const path = `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
        const dashed = e.share < 1 ? ' stroke-dasharray="4 3"' : '';
        // Arrow follows the RDF edge: "in" means the neighbour points at its parent.
        const marker = e.direction === 'in' ? 'marker-start="url(#patArrowIn)"' : 'marker-end="url(#patArrow)"';
        const extra = [
            e.avg > 1 ? `×${e.avg}` : '',
            e.share < 1 ? `${Math.round(e.share * 100)}%` : '',
        ].filter(Boolean).join(' · ');
        const label = this._trim(e.via_label, 16) + (extra ? ` ${extra}` : '');
        return `<path d="${path}" class="pat-dg-edge"${dashed} ${marker}><title>${this._esc(e.via)}</title></path>`
            + `<text x="${x2 - 8}" y="${y2 - 4}" class="pat-dg-edge-label" text-anchor="end">${this._esc(label)}</text>`;
    },

    _diagramNode(n, p) {
        const D = this.DIAGRAM;
        const icon = this._classIcon(n.type, n.type_label);
        const cls = n.hop === 0 ? 'pat-dg-node pat-dg-anchor' : 'pat-dg-node';
        const attrs = (n.attributes || []).map((at, i) => (
            `<text x="${p.x + 10}" y="${p.y + D.headH + 4 + i * D.attrH}" class="pat-dg-attr">`
            + `${this._esc(this._trim(`${at.prop}: ${at.value}`, 26))}</text>`
        )).join('');
        return `<g><rect x="${p.x}" y="${p.y}" width="${D.nodeW}" height="${p.h}" rx="8" class="${cls}"><title>${this._esc(n.type)}</title></rect>`
            + `<text x="${p.x + 10}" y="${p.y + 21}" class="pat-dg-type">${this._esc(icon)} ${this._esc(this._trim(n.type_label, 18))}</text>`
            + `${attrs}</g>`;
    },

    _diagramSvg(diagram) {
        if (!diagram || !(diagram.nodes || []).length) return '';
        const D = this.DIAGRAM;
        const L = this._layoutDiagram(diagram);
        const heads = Array.from({ length: L.maxHop + 1 }, (_, h) => (
            `<text x="${D.pad + h * D.colW + D.nodeW / 2}" y="14" class="pat-dg-head" text-anchor="middle">`
            + `${h === 0 ? 'Entity' : `${h} hop${h > 1 ? 's' : ''}`}</text>`
        )).join('');
        const incomingTotal = {};
        (diagram.edges || []).forEach((e) => {
            incomingTotal[e.target] = (incomingTotal[e.target] || 0) + 1;
        });
        const incoming = {};
        const edges = (diagram.edges || []).map((e) => this._diagramEdge(e, L.pos, incoming, incomingTotal)).join('');
        const nodes = diagram.nodes.map((n) => this._diagramNode(n, L.pos[n.id])).join('');
        const note = diagram.truncated ? '<p class="text-muted small mb-0">Showing the most common relationships only.</p>' : '';
        return `<div class="pat-diagram-scroll"><svg class="pat-diagram-svg" width="100%" height="${L.height}" `
            + `viewBox="0 0 ${L.width} ${L.height}" preserveAspectRatio="xMidYMid meet" role="img" aria-label="Pattern diagram">`
            + '<defs>'
            + '<marker id="patArrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L10,5 L0,10 z" class="pat-dg-arrow"/></marker>'
            + '<marker id="patArrowIn" viewBox="0 0 10 10" refX="0" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M10,0 L0,5 L10,10 z" class="pat-dg-arrow"/></marker>'
            + '</defs>'
            + `${heads}${edges}${nodes}</svg></div>`
            + '<p class="text-muted small mb-0 mt-1">Dashed: not every member has it · ×n: average per member · hover for full URIs.</p>'
            + note;
    },

    async _loadMoreMembers() {
        if (!this.selectedId || !this._members) return;
        const offset = (this._members.offset || 0) + (this._members.members || []).length;
        try {
            const page = await this._json(
                `/dtwin/patterns/${encodeURIComponent(this.selectedId)}/members?offset=${offset}&limit=${this.MEMBERS_PAGE}`
            );
            this._members = {
                ...this._members,
                members: (this._members.members || []).concat(page.members || []),
                has_more: !!page.has_more,
                offset: 0,
            };
            const details = document.getElementById('patDetails');
            if (!details) return;
            const rows = this._members.members.map((m) => (
                `<li class="pat-member-row">`
                + `<span class="fw-medium">${this._esc(m.label || m.uri)}</span>`
                + `<span class="badge rounded-pill text-bg-light border">${this._esc(m.type_label || '—')}</span>`
                + `</li>`
            )).join('');
            const list = details.querySelector('.pat-member-list');
            if (list) list.innerHTML = rows || '<li class="text-muted">None</li>';
            const members = details.querySelector('.pat-members');
            const btn = details.querySelector('[data-pat-more]');
            if (!this._members.has_more) {
                if (btn) btn.remove();
            } else if (!btn && members) {
                members.insertAdjacentHTML(
                    'beforeend',
                    '<button type="button" class="btn btn-sm btn-outline-secondary mt-2" data-pat-more>Load more</button>'
                );
            }
        } catch (err) {
            this._notify(err.message || 'Failed to load members', 'danger');
        }
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

    async refreshDiscover() {
        await this._populateScope();
        await this.loadLatest();
        if (this._busy) return;
        const running = await this._findRunningTask(['pattern_detection', 'pattern_materialize']);
        if (!running) return;
        if (running.task_type === 'pattern_detection') await this._awaitDetection(running.id);
        else await this._awaitMaterialize(running.id);
    },

    async loadLatest() {
        this._showLoading(true, 'Fetching the last discovery run…', 'Loading…');
        try {
            const data = await this._json('/dtwin/patterns/latest');
            this.latest = data;
            this.blockedReason = data.job_available ? '' : (data.blocked_reason || 'Pattern job unavailable');
            const status = document.getElementById('patJobStatus');
            if (status) {
                status.classList.toggle('d-none', !this.blockedReason);
                status.textContent = this.blockedReason;
            }
            this._renderStats(data.stats);
            if (this.selectedId && !(data.patterns || []).some((p) => p.id === this.selectedId)) {
                this.selectedId = null;
            }
            this._renderList();
            this._syncButtons();
        } catch (err) {
            this._notify(err.message || 'Failed to load patterns', 'danger');
        } finally {
            this._showLoading(false);
            if (!(this.latest && this.latest.stats)) {
                document.getElementById('patPreviewEmpty')?.classList.remove('d-none');
            } else {
                this._renderList();
            }
        }
    },

    async _awaitDetection(taskId) {
        this._setBtnSpinner('patDetectBtn', true);
        this._setBusy(true);
        this._showLoading(true, 'Detecting patterns on Databricks…', 'Detecting…');
        try {
            const task = await waitForTask(taskId, (t) => {
                if (t && t.message) this._showLoading(true, t.message, 'Detecting…');
            });
            await this.loadLatest();
            const stats = (this.latest && this.latest.stats) || {};
            this._notify(
                task.message || `${this._fmtNum(stats.pattern_count || 0)} patterns detected.`,
                'success'
            );
        } catch (err) {
            await this.loadLatest();
            this._notify(err.message || 'Detection failed', 'danger');
        } finally {
            this._setBtnSpinner('patDetectBtn', false);
            this._setBusy(false);
            this._showLoading(false);
        }
    },

    async detect() {
        if (this._busy || !this._canEdit()) return;
        const classFilter = this._selectedClass();
        if (!classFilter) {
            this._notify('Select an anchor entity type first.', 'warning');
            return;
        }
        this._setBtnSpinner('patDetectBtn', true);
        this._setBusy(true);
        try {
            const data = await this._json('/dtwin/patterns/detect', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    class_filter: classFilter,
                    hops: this._num('patHops', 3),
                    bins: this._num('patBins', 8),
                    min_support: this._num('patMinSupport', 3),
                    refine_top: this._num('patRefineTop', 0),
                }),
            });
            if (typeof refreshTasks === 'function') refreshTasks();
            this._notify('Pattern detection started on Databricks.', 'info');
            await this._awaitDetection(data.task_id);
        } catch (err) {
            this._setBtnSpinner('patDetectBtn', false);
            this._setBusy(false);
            this._notify(err.message || 'Detection failed', 'danger');
        }
    },

    openSaveModal() {
        if (!this.selectedId) return;
        const input = document.getElementById('patSaveLabel');
        const patterns = (this.latest && this.latest.patterns) || [];
        const current = patterns.find((p) => p.id === this.selectedId);
        if (input) input.value = (current && current.label) || '';
        if (this._saveModal) this._saveModal.show();
    },

    async confirmSave() {
        const input = document.getElementById('patSaveLabel');
        const label = (input && input.value || '').trim();
        if (!label || !this.selectedId) {
            this._notify('Enter a name for the pattern.', 'warning');
            return;
        }
        if (this._saveModal) this._saveModal.hide();
        await this._materialize(this.selectedId, label);
    },

    async _awaitMaterialize(taskId) {
        this._setBtnSpinner('patSaveBtn', true);
        this._setBusy(true);
        try {
            const task = await waitForTask(taskId);
            this._notify(task.message || 'Pattern saved.', 'success');
        } catch (err) {
            this._notify(err.message || 'Save failed', 'danger');
        } finally {
            this._setBtnSpinner('patSaveBtn', false);
            this._setBusy(false);
        }
    },

    async _materialize(patternId, label) {
        if (this._busy || !this._canEdit()) return;
        this._setBtnSpinner('patSaveBtn', true);
        this._setBusy(true);
        try {
            const data = await this._json('/dtwin/patterns/materialize', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pattern_id: patternId, label }),
            });
            if (typeof refreshTasks === 'function') refreshTasks();
            await this._awaitMaterialize(data.task_id);
        } catch (err) {
            this._setBtnSpinner('patSaveBtn', false);
            this._setBusy(false);
            this._notify(err.message || 'Save failed', 'danger');
        }
    },

    async refreshSaved() {
        const loading = document.getElementById('patSavedLoading');
        const empty = document.getElementById('patSavedEmpty');
        const list = document.getElementById('patSavedList');
        if (loading) loading.classList.remove('d-none');
        if (empty) empty.classList.add('d-none');
        if (list) list.classList.add('d-none');
        try {
            const data = await this._json('/dtwin/patterns/saved');
            const patterns = data.patterns || [];
            if (loading) loading.classList.add('d-none');
            if (!patterns.length) {
                if (empty) empty.classList.remove('d-none');
                return;
            }
            if (list) {
                list.classList.remove('d-none');
                list.innerHTML = patterns.map((p) => (
                    `<div class="pat-saved-row" data-pat-saved="${this._esc(p.id)}">`
                    + `<div class="d-flex justify-content-between align-items-center gap-2">`
                    + `<div><span class="fw-medium">${this._esc(p.label)}</span>`
                    + `<span class="text-muted small ms-2">${this._fmtNum(p.size)} members</span></div>`
                    + `<button type="button" class="btn btn-sm btn-outline-danger" data-pat-delete="${this._esc(p.id)}">`
                    + `<i class="bi bi-trash"></i></button></div>`
                    + `<div class="pat-saved-members d-none" id="patSavedMembers-${this._esc(p.id)}"></div>`
                    + `</div>`
                )).join('');
            }
        } catch (err) {
            if (loading) loading.classList.add('d-none');
            if (empty) {
                empty.classList.remove('d-none');
                empty.innerHTML = `<p class="text-danger mb-0">${this._esc(err.message || 'Failed')}</p>`;
            }
        }
    },

    async _toggleSavedMembers(savedId) {
        const box = document.getElementById(`patSavedMembers-${savedId}`);
        if (!box) return;
        if (!box.classList.contains('d-none')) {
            box.classList.add('d-none');
            return;
        }
        box.classList.remove('d-none');
        await this._loadSavedMembers(savedId, false);
    },

    async _loadSavedMembers(savedId, append) {
        const box = document.getElementById(`patSavedMembers-${savedId}`);
        if (!box) return;
        const offset = append ? box.querySelectorAll('li').length : 0;
        try {
            const data = await this._json(
                `/dtwin/patterns/saved/${encodeURIComponent(savedId)}/members?offset=${offset}&limit=${this.MEMBERS_PAGE}`
            );
            const rows = (data.members || []).map((m) => (
                `<li>${this._esc(m.label || m.uri)}</li>`
            )).join('');
            if (!append) {
                box.innerHTML = `<ul class="mb-1">${rows}</ul>`;
            } else {
                const ul = box.querySelector('ul');
                if (ul) ul.insertAdjacentHTML('beforeend', rows);
            }
            box.querySelector('[data-pat-saved-more]')?.remove();
            if (data.has_more) {
                box.insertAdjacentHTML(
                    'beforeend',
                    `<button type="button" class="btn btn-sm btn-outline-secondary" data-pat-saved-more="${this._esc(savedId)}">Load more</button>`
                );
            }
        } catch (err) {
            this._notify(err.message || 'Failed to load members', 'danger');
        }
    },

    async deleteSaved(savedId) {
        if (!this._canEdit()) return;
        const go = (typeof showDeleteConfirm === 'function')
            ? await showDeleteConfirm('Delete this saved pattern?')
            : true;
        if (!go) return;
        try {
            await this._json(`/dtwin/patterns/saved/${encodeURIComponent(savedId)}`, {
                method: 'DELETE',
            });
            this._notify('Saved pattern deleted.', 'success');
            await this.refreshSaved();
        } catch (err) {
            this._notify(err.message || 'Delete failed', 'danger');
        }
    },
};

document.addEventListener('DOMContentLoaded', () => SimulationPatterns.init());
