/**
 * Intelligence → Discovery → Patterns: Scenarios-style list (saved snapshots) + details
 * pane (Detect results or a saved snapshot), like Workbench.
 */
const IntelligencePatterns = {
    latest: null,
    blockedReason: '',
    selectedId: null,
    _pendingPatternId: null,
    _pendingSaved: null,
    _viewingSavedId: null,
    _viewingSaved: null,
    ontologyClasses: null,
    _members: null,
    _busy: false,
    _detectModal: null,
    _openingDetails: false,

    MEMBERS_PAGE: 100,
    LAST_KEY: 'ontobricks-patterns-last',
    DISCOVER_TITLE: 'Pattern discovery',
    DISCOVER_SUBTITLE: 'Discover recurring neighbourhood shapes for a chosen entity type on Databricks. '
        + 'Each Detect run is saved automatically so you can reopen it later.',

    init() {
        const on = (id, fn) => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('click', fn);
        };
        on('patDetectBtn', () => this.openDetectModal());
        on('patBackBtn', () => this.showList({ replace: true }));
        on('patRefreshBtn', () => {
            if (this._viewingSavedId && this._viewingSaved) {
                this._pendingSaved = this._viewingSaved;
                this._showPendingSaved();
                return;
            }
            this._clearSavedView();
            this.selectedId = null;
            this.refreshDiscover();
        });
        on('patSavedRefreshBtn', () => this.refreshSaved());
        const detectForm = document.getElementById('patDetectForm');
        if (detectForm) {
            detectForm.addEventListener('submit', (e) => {
                e.preventDefault();
                this.detect();
            });
        }

        const details = document.getElementById('patDetails');
        if (details) {
            details.addEventListener('click', (e) => {
                if (e.target.closest('[data-pat-more]')) {
                    this._loadMoreMembers();
                    return;
                }
                const opt = e.target.closest('[data-pat-id]');
                if (opt && opt.dataset.patId) this.selectPattern(opt.dataset.patId);
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
                const open = e.target.closest('[data-pat-open]');
                if (open) {
                    e.preventDefault();
                    this.openSaved(open.closest('[data-pat-saved]') || open);
                    return;
                }
                const row = e.target.closest('[data-pat-saved]');
                if (row) this.openSaved(row);
            });
            saved.addEventListener('keydown', (e) => {
                if (e.key !== 'Enter' && e.key !== ' ') return;
                if (e.target.closest('[data-pat-delete]')) return;
                const row = e.target.closest('[data-pat-saved]');
                if (!row) return;
                e.preventDefault();
                this.openSaved(row);
            });
        }

        const detectEl = document.getElementById('patDetectModal');
        if (detectEl && window.bootstrap) {
            this._detectModal = bootstrap.Modal.getOrCreateInstance(detectEl);
        }

        const patternsLink = document.querySelector('.sidebar-nav .nav-link[data-section="patterns"]');
        if (patternsLink) {
            patternsLink.addEventListener('click', () => this.showList({ replace: true }));
        }
        window.addEventListener('popstate', () => this.applyPatternView({ fromPop: true }));
        setTimeout(() => this.applyPatternView({ replace: true }), 0);
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

    _fmtDate(iso) {
        if (!iso) return '–';
        const d = new Date(iso);
        return Number.isNaN(d.getTime()) ? String(iso) : d.toLocaleString();
    },

    _plural(n, one, many) {
        return `${this._fmtNum(n)} ${Number(n) === 1 ? one : many}`;
    },

    _savedCardHtml(p) {
        const id = this._esc(p.id || '');
        const patternId = this._esc(p.pattern_id || '');
        const label = p.label || p.id || 'Pattern';
        const hops = Number(p.hop_count) || 0;
        const types = Number(p.type_count) || 0;
        const relations = Number(p.relation_count) || 0;
        const found = Number(p.pattern_count) || 0;
        const members = Number(p.size) || Number(p.support) || 0;
        return `
        <div class="ob-rule-card pat-saved-card mb-2" role="button" tabindex="0"
             data-pat-saved="${id}"
             data-pat-pattern-id="${patternId}"
             data-pat-label="${this._esc(label)}"
             data-pat-hops="${hops}"
             data-pat-types="${types}"
             data-pat-relations="${relations}"
             data-pat-found="${found}"
             data-pat-size="${members}"
             title="Open">
            <div class="d-flex align-items-start justify-content-between">
                <div class="flex-grow-1 min-w-0">
                    <div class="d-flex align-items-center gap-2 mb-1 flex-wrap">
                        <span class="badge bg-success">saved</span>
                        <span class="text-muted small">${this._esc(this._plural(hops, 'hop', 'hops'))}</span>
                    </div>
                    <div class="small fw-medium">
                        <a href="/intelligence/?section=patterns&amp;saved=${id}"
                           class="text-decoration-none text-body"
                           data-pat-open="${id}">${this._esc(label)}</a>
                    </div>
                    <div class="text-muted small mt-1">
                        ${this._esc(this._plural(found, 'pattern', 'patterns'))} found
                        <span class="mx-1">·</span>
                        Saved ${this._esc(this._fmtDate(p.saved_at))}
                    </div>
                </div>
                <div class="btn-group btn-group-sm ms-2 flex-shrink-0">
                    <button type="button" class="btn btn-outline-secondary btn-sm"
                            data-pat-open="${id}" title="Open">
                        <i class="bi bi-pencil"></i>
                    </button>
                    <button type="button" class="btn btn-outline-danger btn-sm"
                            data-pat-delete="${id}" title="Delete saved pattern">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </div>
        </div>`;
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
            detect.disabled = !this._canEdit() || this._busy;
        }
        const run = document.getElementById('patDetectRunBtn');
        if (run && !run.classList.contains('is-busy')) {
            run.disabled = !this._canEdit() || this._busy || !!this.blockedReason
                || !this._selectedClass();
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

    /** Re-show the details preview after a loading overlay hid body + empty. */
    _restoreDiscoverPreview() {
        this._showDetailsPane();
        document.getElementById('patPreviewLoading')?.classList.add('d-none');
        if (this._viewingSavedId) {
            document.getElementById('patPreviewEmpty')?.classList.add('d-none');
            document.getElementById('patPreviewBody')?.classList.remove('d-none');
            return;
        }
        if (this.latest && ((this.latest.patterns || []).length || this.latest.stats)) {
            this._renderList();
            return;
        }
        document.getElementById('patPreviewBody')?.classList.add('d-none');
        document.getElementById('patPreviewEmpty')?.classList.remove('d-none');
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
            + `<div class="sim-kpi-tile pat-kpi-compact">`
            + `<span class="sim-kpi-value">${this._esc(String(value ?? '—'))}</span>`
            + `<span class="sim-kpi-label">${this._esc(label)}</span>`
            + `</div></div>`;
    },

    _patternOf(patternId) {
        const patterns = (this.latest && this.latest.patterns) || [];
        return patterns.find((p) => p.id === patternId) || null;
    },

    _supportOf(patternId) {
        const hit = this._patternOf(patternId);
        return hit ? hit.support : null;
    },

    _patternMenuHtml(selectedId) {
        const patterns = (this.latest && this.latest.patterns) || [];
        return patterns.map((p) => {
            const label = p.label || (p.id || '').slice(0, 8);
            const active = p.id === selectedId ? ' active' : '';
            return `<li><button type="button" class="dropdown-item pat-pattern-option${active}" `
                + `data-pat-id="${this._esc(p.id)}">`
                + `<span class="pat-pattern-option-label">${this._esc(label)}</span>`
                + `<span class="badge pat-support-badge">${this._esc(this._fmtNum(p.support))}</span>`
                + `</button></li>`;
        }).join('');
    },

    _renderList() {
        const empty = document.getElementById('patPreviewEmpty');
        const body = document.getElementById('patPreviewBody');
        const patterns = (this.latest && this.latest.patterns) || [];
        const pendingSaved = this._pendingSaved;
        const pending = this._pendingPatternId;
        this._pendingPatternId = null;
        if (pendingSaved) {
            if (empty) empty.classList.add('d-none');
            if (body) body.classList.remove('d-none');
            this._showPendingSaved();
            return;
        }
        if (this._viewingSavedId) {
            // A saved snapshot is open: a (re)load of the latest run must not replace it.
            if (empty) empty.classList.add('d-none');
            if (body) body.classList.remove('d-none');
            return;
        }
        if (pending) this.selectedId = pending;
        if (!patterns.length) {
            if (pending) {
                if (empty) empty.classList.add('d-none');
                if (body) body.classList.remove('d-none');
                this.selectPattern(pending, { sync: true });
                return;
            }
            if (empty) empty.classList.remove('d-none');
            if (body) body.classList.add('d-none');
            this.selectedId = null;
            return;
        }
        if (empty) empty.classList.add('d-none');
        if (body) body.classList.remove('d-none');
        if (!pending && (!this.selectedId || !patterns.some((p) => p.id === this.selectedId))) {
            this.selectedId = patterns[0].id;
        }
        if (this.selectedId) this.selectPattern(this.selectedId, { sync: true });
    },

    async selectPattern(patternId, { sync = true } = {}) {
        if (!patternId) return;
        const savedId = this._viewingSavedId;
        this.selectedId = patternId;
        if (!savedId) {
            this._clearSavedView();
            this.rememberPattern(patternId);
            if (sync) this.syncPatternUrl({ pattern: patternId });
        } else if (sync) {
            this.syncPatternUrl({ saved: savedId, pattern: patternId });
        }
        this._syncButtons();
        const details = document.getElementById('patDetails');
        if (!details) return;
        details.innerHTML = this._toolbarHtml(patternId)
            + '<div id="patDetailBody"><p class="text-muted small mb-0">Loading…</p></div>';
        try {
            const shapeUrl = savedId
                ? `/dtwin/patterns/saved/${encodeURIComponent(savedId)}/shape?pattern_id=${encodeURIComponent(patternId)}`
                : `/dtwin/patterns/${encodeURIComponent(patternId)}/shape`;
            const membersUrl = savedId
                ? `/dtwin/patterns/saved/${encodeURIComponent(savedId)}/members?pattern_id=${encodeURIComponent(patternId)}&offset=0&limit=${this.MEMBERS_PAGE}`
                : `/dtwin/patterns/${encodeURIComponent(patternId)}/members?offset=0&limit=${this.MEMBERS_PAGE}`;
            const [shape, members] = await Promise.all([
                this._json(shapeUrl),
                this._json(membersUrl),
            ]);
            if (this.selectedId !== patternId) return;
            if (savedId) {
                shape._fromSaved = true;
                if (this._viewingSaved && this._viewingSaved.label) {
                    shape._runLabel = this._viewingSaved.label;
                }
            }
            this._members = members;
            this._selectedShape = shape;
            details.innerHTML = this._detailsHtml(shape, members);
        } catch (err) {
            if (this.selectedId !== patternId) return;
            details.innerHTML = this._toolbarHtml(patternId)
                + `<div id="patDetailBody"><p class="text-danger small mb-0">${this._esc(err.message || 'Failed')}</p></div>`;
        }
    },

    showList({ replace = false } = {}) {
        this._clearSavedView();
        this._toggleDetails(false);
        this.syncPatternUrl({}, { replace });
        this.refreshSaved();
    },

    _toggleDetails(on) {
        const list = document.getElementById('patPatternsList');
        const details = document.getElementById('patPatternsDetails');
        if (list) list.classList.toggle('d-none', on);
        if (details) details.classList.toggle('d-none', !on);
    },

    _showDetailsPane() {
        this._openingDetails = true;
        if (typeof SidebarNav !== 'undefined' && SidebarNav.getActiveSection() !== 'patterns') {
            SidebarNav._showSection('patterns');
        }
        this._toggleDetails(true);
        this._openingDetails = false;
    },

    rememberPattern(id) {
        if (!id) return;
        try { sessionStorage.setItem(this.LAST_KEY, id); } catch (_) { /* ignore */ }
    },

    syncPatternUrl({ pattern, saved } = {}, { replace = false } = {}) {
        const url = new URL(window.location.href);
        url.hash = '';
        url.searchParams.set('section', 'patterns');
        const setOrDel = (key, value) => {
            const text = String(value || '').trim();
            if (text) url.searchParams.set(key, text);
            else url.searchParams.delete(key);
        };
        setOrDel('pattern', pattern);
        setOrDel('saved', saved);
        const state = {
            section: 'patterns',
            pattern: pattern || null,
            saved: saved || null,
        };
        const method = replace ? 'replaceState' : 'pushState';
        if (window.history && window.history[method]) window.history[method](state, '', url);
    },

    applyPatternView({ replace = false, fromPop = false } = {}) {
        const params = new URLSearchParams(window.location.search);
        const rawSection = (params.get('section') || '').trim();
        if (rawSection && rawSection !== 'patterns') return;
        const savedId = (params.get('saved') || '').trim();
        const patternId = (params.get('pattern') || '').trim();
        const last = (() => {
            try { return sessionStorage.getItem(this.LAST_KEY) || ''; } catch (_) { return ''; }
        })();

        if (savedId) {
            this.openSavedById(savedId, {
                sync: rawSection !== 'patterns' && !fromPop,
                replace: true,
            });
            return;
        }
        if (patternId) {
            this._openLiveDetails(patternId, {
                sync: rawSection !== 'patterns' && !fromPop,
                replace: true,
            });
            return;
        }
        if (rawSection === 'patterns' && last && fromPop === false && replace) {
            // Fresh landing on Patterns with no query → list (not last pattern).
        }
        this._toggleDetails(false);
        this.refreshSaved();
        if (rawSection === 'patterns') {
            this.syncPatternUrl({}, { replace: true });
        }
    },

    async _openLiveDetails(patternId, { sync = false, replace = false } = {}) {
        this._clearSavedView();
        this._setDiscoverHeading(null);
        this._showDetailsPane();
        this.selectedId = patternId;
        if (sync) this.syncPatternUrl({ pattern: patternId }, { replace });
        await this.refreshDiscover();
        if (patternId) await this.selectPattern(patternId, { sync: false });
    },

    _clearSavedView() {
        this._viewingSavedId = null;
        this._viewingSaved = null;
        this._setDiscoverHeading(null);
    },

    _setDiscoverHeading(saved) {
        const title = document.getElementById('patDiscoverTitleText');
        const subtitle = document.getElementById('patDiscoverSubtitle');
        if (title) {
            title.textContent = saved && saved.label
                ? saved.label
                : this.DISCOVER_TITLE;
        }
        if (subtitle) {
            subtitle.textContent = saved
                ? 'Saved Detect run — all patterns, diagrams and members from when it was saved.'
                : this.DISCOVER_SUBTITLE;
        }
    },

    _toolbarHtml(selectedId, kpisHtml = '', opts = {}) {
        const current = this._patternOf(selectedId);
        const label = opts.label
            || (current && (current.label || current.id.slice(0, 8)))
            || 'Pattern';
        const support = opts.support != null
            ? opts.support
            : (current ? current.support : null);
        const badge = `<span class="badge pat-support-badge" id="patSupportBadge">${this._esc(this._fmtNum(support))}</span>`;
        const savedBadge = this._viewingSavedId
            ? '<span class="badge bg-success">saved</span>'
            : '';
        // Always use the Detect picker — for a saved run, `latest.patterns` is
        // the snapshotted catalog so every pattern (diagram + members) is reachable.
        return `
            <div class="pat-detail-toolbar mb-3">
                <div class="dropdown pat-pattern-picker">
                    <button type="button" class="pat-pattern-toggle" id="patPatternSelect"
                            data-bs-toggle="dropdown" aria-expanded="false"
                            title="Select a pattern">
                        ${savedBadge}
                        <span class="pat-pattern-label">${this._esc(label)}</span>
                        ${badge}
                        <i class="bi bi-chevron-down pat-pattern-caret" aria-hidden="true"></i>
                    </button>
                    <ul class="dropdown-menu pat-pattern-menu" aria-labelledby="patPatternSelect">
                        ${this._patternMenuHtml(selectedId)}
                    </ul>
                </div>
                <div class="row g-2 pat-kpi-row pat-detail-kpis" id="patDetailKpis">${kpisHtml}</div>
            </div>`;
    },

    _explorerHref(uri) {
        if (!uri) return '';
        return `/dtwin/?section=sigmagraph&focus=${encodeURIComponent(uri)}`;
    },

    _memberRow(m) {
        const label = m.label || m.uri || '—';
        const type = m.type_label || '—';
        const href = this._explorerHref(m.uri);
        const name = href
            ? `<a class="pat-member-link" href="${this._esc(href)}" title="Open in Graph Explorer">${this._esc(label)}</a>`
            : `<span class="fw-medium">${this._esc(label)}</span>`;
        return `<li class="pat-member-row">`
            + `${name}`
            + `<span class="badge rounded-pill text-bg-light border">${this._esc(type)}</span>`
            + `</li>`;
    },

    _detailsHtml(shape, members) {
        const fromSaved = !!(shape && shape._fromSaved);
        const diagram = shape.diagram
            ? this._diagramSvg(shape.diagram)
            : (fromSaved
                ? '<p class="text-muted small mb-0">No diagram in this snapshot'
                    + ' — Save again from a Detect run that includes the neighbourhood diagram.</p>'
                : '<p class="text-muted small mb-0">Re-run <strong>Detect</strong> to draw this pattern.</p>');
        const nodes = (shape.diagram && shape.diagram.nodes) || [];
        const edges = (shape.diagram && shape.diagram.edges) || [];
        // Prefer snapshotted KPI columns when present; else derive from diagram.
        const typeCount = Number(shape._typeCount) || nodes.length || 0;
        const relationCount = Number(shape._relationCount) || edges.length || 0;
        const rows = (members.members || []).map((m) => this._memberRow(m)).join('');
        const more = members.has_more
            ? '<button type="button" class="btn btn-sm btn-outline-secondary mt-2" data-pat-more>Load more</button>'
            : '';
        const kpis = this._kpiTile('Hops', this._fmtNum(shape.hop_count))
            + this._kpiTile('Types', this._fmtNum(typeCount))
            + this._kpiTile('Relations', this._fmtNum(relationCount));
        const current = this._patternOf(shape.pattern_id || this.selectedId);
        return `
            ${this._toolbarHtml(shape.pattern_id || this.selectedId, kpis, {
                label: (current && current.label) || shape.seed_label || shape._savedLabel,
                support: shape.support != null ? shape.support : (current && current.support),
            })}
            <div id="patDetailBody">
                <div class="pat-diagram mb-3">${diagram}</div>
                <div class="pat-members">
                    <h6 class="mb-2">Matching entities</h6>
                    <ul class="pat-member-list mb-0">${rows || '<li class="text-muted">None</li>'}</ul>
                    ${more}
                </div>
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
        if ((!this.selectedId && !this._viewingSavedId) || !this._members) return;
        const offset = (this._members.offset || 0) + (this._members.members || []).length;
        try {
            const url = this._viewingSavedId
                ? `/dtwin/patterns/saved/${encodeURIComponent(this._viewingSavedId)}/members`
                    + `?pattern_id=${encodeURIComponent(this.selectedId || '')}`
                    + `&offset=${offset}&limit=${this.MEMBERS_PAGE}`
                : `/dtwin/patterns/${encodeURIComponent(this.selectedId)}/members?offset=${offset}&limit=${this.MEMBERS_PAGE}`;
            const page = await this._json(url);
            this._members = {
                ...this._members,
                members: (this._members.members || []).concat(page.members || []),
                has_more: !!page.has_more,
                offset: 0,
            };
            const details = document.getElementById('patDetails');
            if (!details) return;
            const rows = this._members.members.map((m) => this._memberRow(m)).join('');
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
        const running = await this._findRunningTask(['pattern_detection', 'pattern_save']);
        if (!running) return;
        if (running.task_type === 'pattern_detection') {
            await this._awaitDetection(running.id);
            return;
        }
        const savedId = await this._awaitSave(running.id);
        if (savedId) await this.openSavedById(savedId, { sync: true, replace: true });
        else this._restoreDiscoverPreview();
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
            if (this._pendingSaved) {
                this.selectedId = this._pendingSaved.pattern_id || this._pendingSaved.id;
            } else if (this._pendingPatternId) {
                this.selectedId = this._pendingPatternId;
            } else if (this._viewingSavedId) {
                // keep the saved snapshot selected
            } else if (this.selectedId && !(data.patterns || []).some((p) => p.id === this.selectedId)) {
                this.selectedId = null;
            }
            this._syncButtons();
        } catch (err) {
            this._notify(err.message || 'Failed to load patterns', 'danger');
        } finally {
            this._showLoading(false);
            if (this._pendingSaved || this._pendingPatternId || this._viewingSavedId
                    || (this.latest && ((this.latest.patterns || []).length || this.latest.stats))) {
                this._renderList();
            } else {
                document.getElementById('patPreviewEmpty')?.classList.remove('d-none');
            }
        }
    },

    async _awaitDetection(taskId) {
        this._setBtnSpinner('patDetectBtn', true);
        this._setBusy(true);
        this._showLoading(true, 'Detecting patterns on Databricks…', 'Detecting…');
        let ok = false;
        try {
            const task = await waitForTask(taskId, (t) => {
                if (t && t.message) this._showLoading(true, t.message, 'Detecting…');
            });
            this._clearSavedView();
            await this.loadLatest();
            const stats = (this.latest && this.latest.stats) || {};
            this._notify(
                task.message || `${this._fmtNum(stats.pattern_count || 0)} patterns detected.`,
                'success'
            );
            ok = true;
        } catch (err) {
            await this.loadLatest();
            this._notify(err.message || 'Detection failed', 'danger');
        } finally {
            this._setBtnSpinner('patDetectBtn', false);
            this._setBusy(false);
            this._showLoading(false);
        }
        if (ok) await this._autoSaveAfterDetect();
    },

    _entityLabelForSave() {
        const stats = (this.latest && this.latest.stats) || {};
        const uri = (stats.class_filter && stats.class_filter[0])
            || stats.anchor_type
            || this._selectedClass()
            || '';
        if (!uri) return 'Pattern';
        const classes = this.ontologyClasses || [];
        const hit = classes.find((c) => (c.uri || c.id) === uri);
        if (hit && (hit.label || hit.name)) return hit.label || hit.name;
        const local = String(uri).split(/[#/]/).filter(Boolean).pop();
        return local || 'Pattern';
    },

    _autoSaveLabel() {
        const entity = this._entityLabelForSave();
        const d = new Date();
        const pad = (n) => String(n).padStart(2, '0');
        const stamp = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} `
            + `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
        return `${entity} — ${stamp}`;
    },

    async _autoSaveAfterDetect() {
        if (!this._canEdit()) {
            this._restoreDiscoverPreview();
            return;
        }
        const patterns = (this.latest && this.latest.patterns) || [];
        const focus = this.selectedId || (patterns[0] && patterns[0].id);
        if (!focus || !patterns.length) {
            this._restoreDiscoverPreview();
            return;
        }
        const savedId = await this._save(focus, this._autoSaveLabel());
        if (savedId) {
            // Open the snapshot so the user lands on diagrams + picker, not a blank overlay.
            await this.openSavedById(savedId, { sync: true, replace: true });
            return;
        }
        this._restoreDiscoverPreview();
    },

    async openDetectModal() {
        if (this._busy || !this._canEdit()) return;
        await this._populateScope();
        this._syncButtons();
        if (this._detectModal) this._detectModal.show();
    },

    async detect() {
        if (this._busy || !this._canEdit()) return;
        const classFilter = this._selectedClass();
        if (!classFilter) {
            this._notify('Select an entity type first.', 'warning');
            return;
        }
        if (this._detectModal) this._detectModal.hide();
        this._clearSavedView();
        this._setDiscoverHeading(null);
        this._showDetailsPane();
        this.syncPatternUrl({}, { replace: false });
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
            // Auto-save opens the new snapshot (URL + details); do not overwrite with pattern=.
            await this._awaitDetection(data.task_id);
        } catch (err) {
            this._setBtnSpinner('patDetectBtn', false);
            this._setBusy(false);
            this._notify(err.message || 'Detection failed', 'danger');
        }
    },

    async _awaitSave(taskId, label) {
        this._setBusy(true);
        this._showLoading(true, 'Saving Detect run…', 'Saving…');
        try {
            const task = await waitForTask(taskId);
            const name = label || 'Detect run';
            const savedId = task && task.result && task.result.saved_id;
            this._notify(task.message || `Saved as ${name}.`, 'success');
            await this.refreshSaved();
            return savedId || null;
        } catch (err) {
            this._notify(err.message || 'Save failed', 'danger');
            return null;
        } finally {
            this._setBusy(false);
            this._showLoading(false);
        }
    },

    async _save(patternId, label) {
        if (this._busy || !this._canEdit()) return null;
        const name = (label || '').trim() || this._autoSaveLabel();
        this._setBusy(true);
        this._showLoading(true, 'Saving Detect run…', 'Saving…');
        try {
            const data = await this._json('/dtwin/patterns/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pattern_id: patternId, label: name }),
            });
            if (typeof refreshTasks === 'function') refreshTasks();
            return await this._awaitSave(data.task_id, name);
        } catch (err) {
            this._setBusy(false);
            this._showLoading(false);
            this._notify(err.message || 'Save failed', 'danger');
            return null;
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
                list.innerHTML = patterns.map((p) => this._savedCardHtml(p)).join('');
            }
        } catch (err) {
            if (loading) loading.classList.add('d-none');
            if (empty) {
                empty.classList.remove('d-none');
                empty.innerHTML = `<p class="text-danger mb-0">${this._esc(err.message || 'Failed')}</p>`;
            }
        }
    },

    _savedFromCard(el) {
        const card = el && el.closest ? (el.closest('[data-pat-saved]') || el) : el;
        if (!card || !card.dataset) return null;
        const id = card.dataset.patSaved || '';
        if (!id) return null;
        return {
            id,
            pattern_id: card.dataset.patPatternId || '',
            label: card.dataset.patLabel || id,
            hop_count: Number(card.dataset.patHops) || 0,
            type_count: Number(card.dataset.patTypes) || 0,
            relation_count: Number(card.dataset.patRelations) || 0,
            pattern_count: Number(card.dataset.patFound) || 0,
            size: Number(card.dataset.patSize) || 0,
        };
    },

    openSaved(el, { sync = true, replace = false } = {}) {
        const saved = this._savedFromCard(el);
        if (!saved) {
            this._notify('Could not open this saved pattern.', 'warning');
            return;
        }
        return this._openSavedDetails(saved, { sync, replace });
    },

    async openSavedById(savedId, { sync = false, replace = false } = {}) {
        if (!savedId) return;
        let saved = null;
        const card = document.querySelector(`[data-pat-saved="${CSS.escape(savedId)}"]`);
        if (card) saved = this._savedFromCard(card);
        if (!saved) {
            try {
                const data = await this._json('/dtwin/patterns/saved');
                const hit = (data.patterns || []).find((p) => p.id === savedId);
                if (hit) {
                    saved = {
                        id: hit.id,
                        pattern_id: hit.pattern_id || '',
                        label: hit.label || hit.id,
                        hop_count: Number(hit.hop_count) || 0,
                        type_count: Number(hit.type_count) || 0,
                        relation_count: Number(hit.relation_count) || 0,
                        pattern_count: Number(hit.pattern_count) || 0,
                        size: Number(hit.size) || 0,
                    };
                }
            } catch (_) { /* fall through */ }
        }
        if (!saved) {
            this._notify('Saved pattern not found.', 'warning');
            this.showList({ replace: true });
            return;
        }
        return this._openSavedDetails(saved, { sync, replace });
    },

    async _openSavedDetails(saved, { sync = true, replace = false } = {}) {
        this._pendingSaved = saved;
        this.selectedId = saved.pattern_id || saved.id;
        this._showDetailsPane();
        if (sync) this.syncPatternUrl({ saved: saved.id }, { replace });
        // Snapshot only: saved KPIs + members. Do not pull `/latest` values.
        await this._showPendingSaved();
    },

    async _showPendingSaved() {
        const saved = this._pendingSaved;
        this._pendingSaved = null;
        if (!saved) return;
        this._viewingSavedId = saved.id;
        this._viewingSaved = saved;
        this._setDiscoverHeading(saved);
        this._syncButtons();
        const empty = document.getElementById('patPreviewEmpty');
        const body = document.getElementById('patPreviewBody');
        if (empty) empty.classList.add('d-none');
        if (body) body.classList.remove('d-none');
        const details = document.getElementById('patDetails');
        if (details) {
            details.innerHTML = '<p class="text-muted small mb-0">Loading saved Detect run…</p>';
        }
        try {
            // Restore the full Detect catalog from the snapshot (not live /latest).
            const catalog = await this._json(
                `/dtwin/patterns/saved/${encodeURIComponent(saved.id)}/patterns`
            );
            if (this._viewingSavedId !== saved.id) return;
            const patterns = catalog.patterns || [];
            this.latest = {
                ...(this.latest || {}),
                patterns,
                job_available: true,
            };
            const focus = saved.pattern_id
                || catalog.focus_pattern_id
                || (patterns[0] && patterns[0].id)
                || '';
            this.selectedId = focus;
            if (!focus) {
                if (details) {
                    details.innerHTML = '<p class="text-muted small mb-0">This saved run has no patterns.</p>';
                }
                return;
            }
            await this.selectPattern(focus, { sync: false });
        } catch (err) {
            if (this._viewingSavedId !== saved.id) return;
            if (details) {
                details.innerHTML = `<p class="text-danger small mb-0">${this._esc(err.message || 'Failed')}</p>`;
            }
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
            if (this._viewingSavedId === savedId) {
                this.showList({ replace: true });
            } else {
                await this.refreshSaved();
            }
        } catch (err) {
            this._notify(err.message || 'Delete failed', 'danger');
        }
    },
};

document.addEventListener('DOMContentLoaded', () => IntelligencePatterns.init());
