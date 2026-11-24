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
    _membersModal: null,
    _openingDetails: false,

    MEMBERS_PAGE: 100,
    LAST_KEY: 'ontobricks-patterns-last',
    DISCOVER_TITLE: 'Pattern discovery',
    DISCOVER_SUBTITLE: 'Discover recurring neighbourhood shapes for a chosen entity type on Databricks. '
        + 'Each Detect run is saved automatically so you can reopen it later.',

    init() {
        this._bindDiagramHover();
        this._bindDiagramTooltips();
        const on = (id, fn) => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('click', fn);
        };
        on('patDetectBtn', () => this.openDetectModal());
        on('patSavedDiscussBtn', () => openOntologyDiscussion());
        on('patDiscussBtn', () => openOntologyDiscussion());
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
                if (e.target.closest('[data-pat-members]')) {
                    this.openMembersModal();
                    return;
                }
                if (e.target.closest('[data-pat-certify]')) {
                    this.toggleCertified();
                    return;
                }
                if (e.target.closest('[data-pat-chart-toggle]')) {
                    e.preventDefault();
                    this._toggleChart();
                    return;
                }
                const grp = e.target.closest('[data-pat-group]');
                if (grp && grp.dataset.patGroup) {
                    this.selectGroup(grp.dataset.patGroup);
                    return;
                }
                const opt = e.target.closest('[data-pat-id]');
                if (opt && opt.dataset.patId) this.selectPattern(opt.dataset.patId);
            });
            details.addEventListener('change', (e) => {
                const sw = e.target.closest('[data-pat-group-toggle]');
                if (sw) this.toggleGrouping(sw.checked);
            });
            details.addEventListener('keydown', (e) => {
                if (e.key !== 'Enter' && e.key !== ' ') return;
                const bar = e.target.closest('.pat-chart-bar[data-pat-id], .pat-chart-bar[data-pat-group], tr[data-pat-id]');
                if (!bar) return;
                e.preventDefault();
                if (bar.dataset.patGroup) this.selectGroup(bar.dataset.patGroup);
                else this.selectPattern(bar.dataset.patId);
            });
        }
        const membersBody = document.getElementById('patMembersModalBody');
        if (membersBody) {
            membersBody.addEventListener('click', (e) => {
                if (e.target.closest('[data-pat-more]')) this._loadMoreMembers();
            });
        }
        const saved = document.getElementById('patSavedList');
        if (saved) {
            saved.addEventListener('click', (e) => {
                const del = e.target.closest('[data-pat-delete]');
                if (del) {
                    this.deleteSaved(del.dataset.patDelete, del);
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
            if (bootstrap.Tooltip) {
                detectEl.querySelectorAll('[data-bs-toggle="tooltip"]').forEach((el) => {
                    bootstrap.Tooltip.getOrCreateInstance(el);
                });
            }
        }
        const membersEl = document.getElementById('patMembersModal');
        if (membersEl && window.bootstrap) {
            this._membersModal = bootstrap.Modal.getOrCreateInstance(membersEl);
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
        return formatDateTime(iso);
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
        // Same surface as Communities / the rest of the app. An earlier
        // helper name was never defined globally, so Detect failures (and
        // every other notify) were silent and looked like a no-op click.
        if (typeof showNotification === 'function') {
            showNotification(msg, type || 'info');
        }
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
            const icon = cls.emoji || cls.icon || '📦';
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

    _kpiTile(label, value, title = '') {
        const tip = title ? ` title="${this._esc(title)}"` : '';
        return `<div class="col">`
            + `<div class="sim-kpi-tile pat-kpi-compact"${tip}>`
            + `<span class="sim-kpi-value">${this._esc(String(value ?? '—'))}</span>`
            + `<span class="sim-kpi-label">${this._esc(label)}</span>`
            + `</div></div>`;
    },

    // Total patterns in the run: saved snapshots carry it per row; live runs
    // report it in the summary stats; fall back to the (possibly capped) list.
    _patternCount() {
        const patterns = (this.latest && this.latest.patterns) || [];
        const first = patterns[0];
        if (first && first.pattern_count != null) return Number(first.pattern_count) || 0;
        const stats = (this.latest && this.latest.stats) || {};
        if (stats.pattern_count != null) return Number(stats.pattern_count) || 0;
        return patterns.length;
    },

    // Total number of entities of the type in the run (the Coverage
    // denominator). Unknown (runs saved before it was persisted) → "—".
    _entityCount() {
        const stats = (this.latest && this.latest.stats) || {};
        const universe = Number(stats.population_count) || 0;
        return universe ? this._fmtNum(universe) : '—';
    },

    // Share of the entity universe (all entities of the type) that follow a
    // pattern = covered_count / population_count. Needs the run's total
    // population; unknown (runs saved before it was persisted) → "—".
    _coveragePct() {
        const stats = (this.latest && this.latest.stats) || {};
        const universe = Number(stats.population_count) || 0;
        if (!universe) return '—';
        const covered = stats.covered_count != null
            ? Number(stats.covered_count)
            : ((this.latest.patterns || []).reduce((s, p) => s + (Number(p.support) || 0), 0));
        return `${Math.min(100, Math.round((covered / universe) * 100))}%`;
    },

    _patternOf(patternId) {
        const patterns = (this.latest && this.latest.patterns) || [];
        return patterns.find((p) => p.id === patternId) || null;
    },

    // Stable ordinal name for the picker ("Pattern #1", #2, …). The seed
    // entities stay reachable through the "Matching entities" button.
    _patternDisplayName(patternId) {
        const patterns = (this.latest && this.latest.patterns) || [];
        const idx = patterns.findIndex((p) => p.id === patternId);
        return idx >= 0 ? `Pattern #${idx + 1}` : '';
    },

    _supportOf(patternId) {
        const hit = this._patternOf(patternId);
        return hit ? hit.support : null;
    },

    // Certification is a per-snapshot flag, so it only exists on a saved run.
    _certifiable() {
        return !!this._viewingSavedId;
    },

    _isCertified(patternId) {
        const hit = this._patternOf(patternId);
        return !!(hit && hit.certified);
    },

    // Share of the entities of the type that follow a certified pattern.
    // Denominator = the run's whole population (runs saved before it was
    // persisted fall back to the entities covered by a pattern); everything
    // else — uncertified patterns and entities outside any pattern — counts
    // as not certified.
    _certStats() {
        const patterns = (this.latest && this.latest.patterns) || [];
        const stats = (this.latest && this.latest.stats) || {};
        const covered = patterns.reduce((s, p) => s + (Number(p.support) || 0), 0);
        const total = Number(stats.population_count) || covered;
        const certified = Math.min(total, patterns.reduce(
            (s, p) => s + (p.certified ? (Number(p.support) || 0) : 0), 0,
        ));
        const certifiedPatterns = patterns.filter((p) => p.certified).length;
        return {
            total,
            certified,
            certifiedPatterns,
            patternCount: patterns.length,
            pct: total ? Math.round((certified / total) * 100) : 0,
        };
    },

    // Compact donut next to "Matching entities": green = certified,
    // red = not certified, plus the percentage.
    _certGaugeHtml() {
        if (!this._certifiable()) return '';
        const st = this._certStats();
        const tip = st.total
            ? `${this._fmtNum(st.certified)} of ${this._fmtNum(st.total)} entities `
                + `(${st.pct}%) follow a certified pattern; the rest is not certified. `
                + `${this._fmtNum(st.certifiedPatterns)} of ${this._fmtNum(st.patternCount)} `
                + 'patterns certified.'
            : 'No entities in this run.';
        const C = 2 * Math.PI * 11;
        const green = (st.pct / 100) * C;
        return `<div class="pat-cert-gauge" id="patCertGauge" role="img" title="${this._esc(tip)}"`
            + ` aria-label="${this._esc(`${st.pct}% certified. ${tip}`)}">`
            + '<svg class="pat-cert-gauge-svg" viewBox="0 0 28 28" width="24" height="24" aria-hidden="true">'
            + '<circle class="pat-cert-gauge-bad" cx="14" cy="14" r="11"/>'
            + (green > 0
                ? `<circle class="pat-cert-gauge-ok" cx="14" cy="14" r="11"`
                    + ` stroke-dasharray="${green.toFixed(2)} ${C.toFixed(2)}"`
                    + ' transform="rotate(-90 14 14)"/>'
                : '')
            + '</svg>'
            + `<span class="pat-cert-gauge-pct">${st.pct}%</span>`
            + '<span class="pat-cert-gauge-label">certified</span>'
            + '</div>';
    },

    _certifyBtnHtml(selectedId) {
        if (!this._certifiable()) return '';
        const on = this._isCertified(selectedId);
        const disabled = this._canEdit() ? '' : ' disabled';
        return `<button type="button" class="btn btn-sm btn-outline-secondary pat-cert-btn${on ? ' is-certified' : ''}"`
            + ` data-pat-certify aria-pressed="${on ? 'true' : 'false'}"${disabled}`
            + ` title="${on ? 'Certified — click to remove the certification' : 'Not certified — click to certify this pattern'}">`
            + `<i class="bi ${on ? 'bi-patch-check-fill' : 'bi-patch-check'} me-1" aria-hidden="true"></i>`
            + `${on ? 'Certified' : 'Certify'}</button>`;
    },

    async toggleCertified() {
        const savedId = this._viewingSavedId;
        const patternId = this.selectedId;
        if (!savedId || !patternId || !this._canEdit()) return;
        const hit = this._patternOf(patternId);
        if (!hit) return;
        const next = !hit.certified;
        const btn = document.querySelector('#patDetails [data-pat-certify]');
        if (btn && btn.classList.contains('is-busy')) return;
        this._setCertifyBusy(btn, true);
        try {
            await this._json(
                `/dtwin/patterns/saved/${encodeURIComponent(savedId)}/certification`,
                {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ pattern_id: patternId, certified: next }),
                },
            );
            hit.certified = next;
            await this._reloadSavedCatalog(savedId);
            if (this._viewingSavedId !== savedId) return;
            // Rebuilds the toolbar (fresh Certify/Certified button) and the
            // "Matching entities per pattern" chart from the reloaded catalog.
            this._refreshCertUi();
        } catch (err) {
            this._setCertifyBusy(btn, false);
            this._notify(err.message || 'Could not update the certification', 'danger');
        }
    },

    // Spinner inside the Certify button while the request runs (same markup
    // as the Save / Detect buttons, see _setBtnSpinner).
    _setCertifyBusy(btn, on) {
        if (!btn) return;
        btn.classList.toggle('is-busy', !!on);
        if (on) {
            if (!btn.dataset.origHtml) btn.dataset.origHtml = btn.innerHTML;
            // Keep the toolbar from shifting while the label is swapped out.
            btn.style.minWidth = `${btn.offsetWidth}px`;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm ob-btn-spinner" role="status" aria-hidden="true"></span>';
            btn.setAttribute('aria-busy', 'true');
            btn.disabled = true;
        } else {
            if (btn.dataset.origHtml) {
                btn.innerHTML = btn.dataset.origHtml;
                delete btn.dataset.origHtml;
            }
            btn.style.minWidth = '';
            btn.removeAttribute('aria-busy');
            btn.disabled = !this._canEdit();
        }
    },

    // Re-read the saved run's pattern catalog so the chart reflects the
    // server's certification state. Best-effort: on failure the local toggle
    // already applied is kept.
    async _reloadSavedCatalog(savedId) {
        try {
            const catalog = await this._json(
                `/dtwin/patterns/saved/${encodeURIComponent(savedId)}/patterns`
            );
            if (this._viewingSavedId !== savedId || !Array.isArray(catalog.patterns)) return;
            this.latest = { ...(this.latest || {}), patterns: catalog.patterns };
        } catch (err) {
            console.warn('[Patterns] Could not reload the saved catalog after certification:', err);
        }
    },

    // Re-render only the certification-aware bits (toolbar, picker, chart),
    // leaving the diagram and its scroll position untouched.
    _refreshCertUi() {
        const details = document.getElementById('patDetails');
        if (!details || !this.selectedId) return;
        const toolbar = details.querySelector('.pat-detail-toolbar');
        if (toolbar) {
            const kpis = details.querySelector('#patDetailKpis');
            const wrap = document.createElement('div');
            wrap.innerHTML = this._toolbarHtml(this.selectedId, kpis ? kpis.innerHTML : '');
            toolbar.replaceWith(wrap.firstElementChild);
        }
        const chart = details.querySelector('.pat-chart-card');
        if (chart) {
            const wrap = document.createElement('div');
            wrap.innerHTML = this._patternsChartHtml(this.selectedId);
            if (wrap.firstElementChild) chart.replaceWith(wrap.firstElementChild);
        }
    },

    _patternMenuHtml(selectedId) {
        const patterns = (this.latest && this.latest.patterns) || [];
        return patterns.map((p, i) => {
            const label = `Pattern #${i + 1}`;
            const active = p.id === selectedId ? ' active' : '';
            const cert = this._certifiable() && p.certified
                ? '<i class="bi bi-patch-check-fill pat-cert-icon ms-1" title="Certified" aria-label="Certified"></i>'
                : '';
            return `<li><button type="button" class="dropdown-item pat-pattern-option${active}" `
                + `data-pat-id="${this._esc(p.id)}">`
                + `<span class="pat-pattern-option-label">${this._esc(label)}${cert}</span>`
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
        this._chartGrouped = false;
        this._groupSel = null;
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
        this._members = null;
        details.innerHTML = this._toolbarHtml(patternId)
            + `<div id="patDetailBody">${this._loadingHtml('Loading…')}</div>`;
        try {
            const shapeUrl = savedId
                ? `/dtwin/patterns/saved/${encodeURIComponent(savedId)}/shape?pattern_id=${encodeURIComponent(patternId)}`
                : `/dtwin/patterns/${encodeURIComponent(patternId)}/shape`;
            const [shape] = await Promise.all([
                this._json(shapeUrl),
                this._loadClasses(),
            ]);
            if (this.selectedId !== patternId) return;
            if (savedId) {
                shape._fromSaved = true;
                if (this._viewingSaved && this._viewingSaved.label) {
                    shape._runLabel = this._viewingSaved.label;
                }
            }
            this._selectedShape = shape;
            details.innerHTML = this._detailsHtml(shape);
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
                ? 'Saved Detect run — pattern catalog, cardinalities and diagrams from when it was saved.'
                : this.DISCOVER_SUBTITLE;
        }
    },

    _toolbarHtml(selectedId, kpisHtml = '', opts = {}) {
        const current = this._patternOf(selectedId);
        const label = this._patternDisplayName(selectedId)
            || opts.label
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
        // the snapshotted catalog so every pattern (diagram + cardinality) is reachable.
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
                <div class="pat-kpi-row pat-detail-kpis" id="patDetailKpis">${kpisHtml}</div>
                ${this._certifyBtnHtml(selectedId)}
                <button type="button" class="btn btn-sm btn-outline-secondary pat-members-btn"
                        data-pat-members title="Show entities that match this pattern">
                    <i class="bi bi-people me-1" aria-hidden="true"></i>Matching entities
                </button>
                ${this._certGaugeHtml()}
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

    _detailsHtml(shape) {
        const fromSaved = !!(shape && shape._fromSaved);
        const diagram = shape.diagram
            ? this._diagramSvg(shape.diagram)
            : (fromSaved
                ? '<p class="text-muted small mb-0">No diagram in this snapshot'
                    + ' — Save again from a Detect run that includes the neighbourhood diagram.</p>'
                : '<p class="text-muted small mb-0">Re-run <strong>Detect</strong> to draw this pattern.</p>');
        const kpis = this._kpiTile('Hops', this._fmtNum(shape.hop_count))
            + this._kpiTile('Entities', this._entityCount(),
                'Total entities of this type in the run')
            + this._kpiTile('Patterns', this._fmtNum(this._patternCount()),
                'Patterns detected in this run')
            + this._kpiTile('Coverage', this._coveragePct(),
                'Share of entities of this type that follow at least one pattern');
        const current = this._patternOf(shape.pattern_id || this.selectedId);
        return `
            ${this._toolbarHtml(shape.pattern_id || this.selectedId, kpis, {
                label: (current && current.label) || shape.seed_label || shape._savedLabel,
                support: shape.support != null ? shape.support : (current && current.support),
            })}
            ${this._patternsChartHtml(shape.pattern_id || this.selectedId)}
            <div id="patDetailBody" class="pat-detail-body">
                <div class="pat-diagram">${diagram}</div>
            </div>`;
    },

    // Bar chart above the diagram: X = each pattern, Y = matching entities
    // (the support count shown in the picker). Click a bar to open it.
    // Collapsible via the top-right link; state is kept across re-renders.
    _patternsChartHtml(selectedId) {
        const patterns = (this.latest && this.latest.patterns) || [];
        if (!patterns.length) return '';
        const collapsed = !!this._chartCollapsed;
        const byCert = this._certifiable();
        const grouped = byCert && !!this._chartGrouped;
        const universe = Number((this.latest && this.latest.stats || {}).population_count) || 0;
        // One bar per pattern, or (grouped) one bar per certification state.
        const items = grouped
            ? ['certified', 'uncertified'].map((kind) => {
                const g = this._groupStats(kind);
                return {
                    attr: `data-pat-group="${kind}"`,
                    value: g.entities,
                    label: kind === 'certified' ? 'Certified' : 'Not certified',
                    sub: `${this._fmtNum(g.patterns.length)} pattern${g.patterns.length === 1 ? '' : 's'}`,
                    active: this._groupSel === kind,
                    cert: kind === 'certified',
                    aria: `${kind === 'certified' ? 'Certified' : 'Not certified'} patterns`
                        + ` (${this._fmtNum(g.patterns.length)})`,
                };
            })
            : patterns.map((p, i) => ({
                attr: `data-pat-id="${this._esc(p.id)}"`,
                value: Number(p.support) || 0,
                label: `#${i + 1}`,
                active: p.id === selectedId,
                cert: !!p.certified,
                aria: `Pattern #${i + 1}`,
            }));
        // Entities of the type that follow no pattern: always the last bar.
        // Patterns partition the anchors, so the rest is population - covered.
        const covered = patterns.reduce((s, p) => s + (Number(p.support) || 0), 0);
        items.push({
            outside: true,
            value: universe ? Math.max(0, universe - covered) : 0,
            label: 'Outside',
            sub: 'not in a pattern',
            active: false,
            cert: false,
            aria: 'Not in any pattern',
        });
        const max = Math.max(1, ...items.map((it) => it.value));
        const SLOT = grouped ? 120 : 54;
        const BAR = grouped ? 56 : 30;
        const OUT_SLOT = 120;     // wide enough for the "not in a pattern" caption
        const PLOT = 120;
        const TOP = 30;          // room for the value + (percentage) above bars
        const LEFT = 4;
        const BASE = TOP + PLOT;
        const H = BASE + 38;     // room for the label and sub-label under each bar
        let cursor = LEFT;
        const bars = items.map((it) => {
            const slot = it.outside ? OUT_SLOT : SLOT;
            const barW = BAR;
            const slotLeft = cursor;
            cursor += slot;
            const x = slotLeft + (slot - barW) / 2;
            const cx = x + barW / 2;
            const v = it.value;
            const h = max > 0 ? Math.round((v / max) * PLOT) : 0;
            const y = BASE - h;
            const active = !it.outside && it.active ? ' pat-chart-bar--active' : '';
            // Saved runs colour each pattern bar by certification: green / red.
            const certClass = byCert && !it.outside
                ? (it.cert ? ' pat-chart-bar--certified' : ' pat-chart-bar--uncertified')
                : '';
            const certAria = byCert && !grouped && !it.outside
                ? (it.cert ? ', certified' : ', not certified')
                : '';
            // Share of all entities of the type this bar accounts for.
            const known = !!universe || !it.outside;
            const pct = universe ? Math.round((v / universe) * 100) : null;
            const valueText = known ? this._fmtNum(v) : '—';
            const aria = known
                ? `${it.aria}: ${this._fmtNum(v)} entities`
                    + (pct != null ? ` (${pct}%)` : '') + certAria
                : `${it.aria}: population unknown`;
            const pctText = pct != null
                ? `<text class="pat-chart-bar-pct" x="${cx}" y="${y - 4}" text-anchor="middle">(${pct}%)</text>`
                : '';
            const valueY = pct != null ? y - 15 : y - 4;
            const sub = it.sub
                ? `<text class="pat-chart-bar-label" x="${cx}" y="${BASE + 30}" text-anchor="middle">${this._esc(it.sub)}</text>`
                : '';
            // The outside bar is informational: no data-pat-id, no role=button.
            const attrs = it.outside
                ? `role="img" aria-label="${this._esc(aria)}"`
                : `${it.attr} role="button" tabindex="0" aria-label="${this._esc(aria)}"`;
            const separator = it.outside
                ? `<line class="pat-chart-sep" x1="${slotLeft}" y1="${TOP - 6}" x2="${slotLeft}" y2="${BASE + 34}"></line>`
                : '';
            return separator
                + `<g class="pat-chart-bar${active}${certClass}${it.outside ? ' pat-chart-bar--outside' : ''}" ${attrs}>`
                + `<title>${this._esc(aria)}</title>`
                + `<rect class="pat-chart-bar-rect" x="${x}" y="${y}" width="${barW}" height="${Math.max(h, 1)}" rx="3"></rect>`
                + `<text class="pat-chart-bar-value" x="${cx}" y="${valueY}" text-anchor="middle">${this._esc(valueText)}</text>`
                + pctText
                + `<text class="pat-chart-bar-label" x="${cx}" y="${BASE + 16}" text-anchor="middle">${this._esc(it.label)}</text>`
                + sub
                + `</g>`;
        }).join('');
        const W = Math.max(cursor + 8, 220);
        const groupSwitch = byCert
            ? `<div class="form-check form-switch pat-chart-group-switch">
                        <input class="form-check-input" type="checkbox" role="switch" id="patChartGroupSwitch"
                               data-pat-group-toggle${grouped ? ' checked' : ''}>
                        <label class="form-check-label small" for="patChartGroupSwitch">Group by certification</label>
                    </div>`
            : '';
        return `
            <div class="pat-chart-card mb-3${collapsed ? ' is-collapsed' : ''}">
                <div class="pat-chart-head">
                    <span class="pat-chart-title">Matching entities per pattern</span>
                    ${byCert ? `<span class="pat-chart-legend" aria-hidden="true">
                        <span class="pat-chart-legend-item"><i class="pat-chart-swatch pat-chart-swatch--ok"></i>Certified</span>
                        <span class="pat-chart-legend-item"><i class="pat-chart-swatch pat-chart-swatch--bad"></i>Not certified</span>
                    </span>` : ''}
                    ${groupSwitch}
                    <a href="#" class="pat-chart-toggle small" data-pat-chart-toggle role="button"
                       aria-expanded="${collapsed ? 'false' : 'true'}">
                        <i class="bi ${collapsed ? 'bi-chevron-down' : 'bi-chevron-up'} me-1" aria-hidden="true"></i>${collapsed ? 'Expand' : 'Collapse'}
                    </a>
                </div>
                <div class="pat-chart-body"${collapsed ? ' hidden' : ''}>
                    <div class="pat-chart-plot">
                        <span class="pat-chart-axis-title pat-chart-ylabel">Matching entities</span>
                        <svg class="pat-chart-svg" viewBox="0 0 ${W} ${H}" width="100%" height="${H}"
                             preserveAspectRatio="xMidYMid meet" role="img"
                             aria-label="Bar chart of matching entities per ${grouped ? 'certification group' : 'pattern'}">
                            <line class="pat-chart-axis" x1="${LEFT}" y1="${BASE}" x2="${W}" y2="${BASE}"></line>
                            ${bars}
                        </svg>
                    </div>
                    <div class="pat-chart-axis-title pat-chart-xlabel">${grouped ? 'Certification' : 'Patterns'}</div>
                </div>
            </div>`;
    },

    // ---- Grouping by certification (chart switch) -------------------------

    // Patterns of one certification state, strongest first, with their total.
    _groupStats(kind) {
        const all = (this.latest && this.latest.patterns) || [];
        const want = kind === 'certified';
        const patterns = all
            .map((p, i) => ({ ...p, _ordinal: i + 1 }))
            .filter((p) => !!p.certified === want)
            .sort((a, b) => (Number(b.support) || 0) - (Number(a.support) || 0));
        const entities = patterns.reduce((s, p) => s + (Number(p.support) || 0), 0);
        const st = this._certStats();
        return {
            patterns,
            entities,
            total: st.total,
            pct: st.total ? Math.round((entities / st.total) * 100) : 0,
        };
    },

    toggleGrouping(on) {
        if (!this._certifiable()) return;
        this._chartGrouped = !!on;
        if (on) {
            this._groupSel = this._groupSel
                || (this._groupStats('certified').patterns.length ? 'certified' : 'uncertified');
            this._renderGroupView();
        } else {
            this._groupSel = null;
            if (this.selectedId) this.selectPattern(this.selectedId, { sync: false });
        }
    },

    selectGroup(kind) {
        if (kind !== 'certified' && kind !== 'uncertified') return;
        this._chartGrouped = true;
        this._groupSel = kind;
        this._renderGroupView();
    },

    _renderGroupView() {
        const details = document.getElementById('patDetails');
        if (!details) return;
        const kind = this._groupSel || 'certified';
        this._members = null;
        details.innerHTML = this._groupToolbarHtml(kind)
            + this._patternsChartHtml(this.selectedId)
            + `<div id="patDetailBody" class="pat-detail-body">${this._groupBodyHtml(kind)}</div>`;
    },

    _groupToolbarHtml(kind) {
        const g = this._groupStats(kind);
        const ok = kind === 'certified';
        const title = ok ? 'Certified patterns' : 'Not certified patterns';
        const kpis = this._kpiTile('Patterns', this._fmtNum(g.patterns.length),
                `${title} in this run`)
            + this._kpiTile('Entities', this._fmtNum(g.entities),
                `Entities that follow one of the ${title.toLowerCase()}`)
            + this._kpiTile('Share', g.total ? `${g.pct}%` : '—',
                'Share of the entities of this type in the run');
        return `
            <div class="pat-detail-toolbar mb-3">
                <div class="pat-pattern-picker">
                    <div class="pat-pattern-toggle pat-pattern-static">
                        <span class="badge bg-success">saved</span>
                        <i class="bi ${ok ? 'bi-patch-check-fill pat-cert-icon' : 'bi-patch-exclamation pat-uncert-icon'}" aria-hidden="true"></i>
                        <span class="pat-pattern-label">${this._esc(title)}</span>
                        <span class="badge pat-support-badge">${this._esc(this._fmtNum(g.entities))}</span>
                    </div>
                </div>
                <div class="pat-kpi-row pat-detail-kpis" id="patDetailKpis">${kpis}</div>
                <button type="button" class="btn btn-sm btn-outline-secondary pat-members-btn"
                        data-pat-members title="Show entities that match these patterns">
                    <i class="bi bi-people me-1" aria-hidden="true"></i>Matching entities
                </button>
                ${this._certGaugeHtml()}
            </div>`;
    },

    // Detail canvas for a group: every pattern it holds with its values.
    _groupBodyHtml(kind) {
        const g = this._groupStats(kind);
        const ok = kind === 'certified';
        const word = ok ? 'certified' : 'not certified';
        if (!g.patterns.length) {
            return `<p class="text-muted small mb-0">${ok
                ? 'No pattern is certified yet. Open a pattern and use <strong>Certify</strong>.'
                : 'Every pattern of this run is certified.'}</p>`;
        }
        const CAP = 100;
        const max = Math.max(1, ...g.patterns.map((p) => Number(p.support) || 0));
        const rows = g.patterns.slice(0, CAP).map((p) => {
            const v = Number(p.support) || 0;
            const share = g.total ? Math.round((v / g.total) * 100) : null;
            return `<tr data-pat-id="${this._esc(p.id)}" role="button" tabindex="0" title="Open this pattern">`
                + `<td class="fw-medium">Pattern #${p._ordinal}`
                + `<span class="text-muted small ms-2">${this._esc(p.label || '')}</span></td>`
                + `<td class="text-end">${this._esc(this._fmtNum(v))}</td>`
                + `<td class="text-end">${share != null ? `${share}%` : '—'}</td>`
                + `<td class="pat-group-meter-cell"><span class="pat-group-meter pat-group-meter--${ok ? 'ok' : 'bad'}"`
                + ` style="width:${Math.max(2, Math.round((v / max) * 100))}%"></span></td>`
                + `</tr>`;
        }).join('');
        const more = g.patterns.length > CAP
            ? `<p class="text-muted small mt-2 mb-0">Showing the ${CAP} strongest of ${this._fmtNum(g.patterns.length)} patterns.</p>`
            : '';
        return `<div class="pat-group-panel">
            <p class="text-muted small mb-2">
                ${this._esc(this._fmtNum(g.patterns.length))} ${word} pattern${g.patterns.length === 1 ? '' : 's'} together match
                <strong>${this._esc(this._fmtNum(g.entities))}</strong> entities${g.total ? ` (${g.pct}% of ${this._esc(this._fmtNum(g.total))})` : ''}.
                Click a pattern to open it.
            </p>
            <div class="table-responsive">
                <table class="table table-sm align-middle mb-0 pat-group-table">
                    <thead><tr><th>Pattern</th><th class="text-end">Entities</th><th class="text-end">Share</th><th></th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
            ${more}
        </div>`;
    },

    _toggleChart() {
        this._chartCollapsed = !this._chartCollapsed;
        const card = document.querySelector('#patDetails .pat-chart-card');
        if (!card) return;
        const collapsed = this._chartCollapsed;
        card.classList.toggle('is-collapsed', collapsed);
        const body = card.querySelector('.pat-chart-body');
        if (body) body.toggleAttribute('hidden', collapsed);
        const link = card.querySelector('[data-pat-chart-toggle]');
        if (link) {
            link.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
            link.innerHTML = `<i class="bi ${collapsed ? 'bi-chevron-down' : 'bi-chevron-up'} me-1" aria-hidden="true"></i>`
                + (collapsed ? 'Expand' : 'Collapse');
        }
    },

    // Shared inline loader: the usual spinner image + a message.
    _loadingHtml(msg) {
        return '<div class="pat-inline-loading text-center text-muted py-4">'
            + '<div class="spinner-border text-primary mb-2" role="status" aria-hidden="true"></div>'
            + `<p class="small mb-0">${this._esc(msg || 'Loading…')}</p>`
            + '</div>';
    },

    _membersLoadingHtml() {
        return this._loadingHtml('Loading matching entities…');
    },

    _membersListHtml(members) {
        const rows = (members.members || []).map((m) => this._memberRow(m)).join('');
        const more = members.has_more
            ? '<button type="button" class="btn btn-sm btn-outline-secondary mt-3" data-pat-more>Load more</button>'
            : '';
        const count = (members.members || []).length;
        const totalHint = members.has_more
            ? `${count}+ matching entities`
            : `${count} matching entit${count === 1 ? 'y' : 'ies'}`;
        return `<p class="text-muted small mb-2" id="patMembersCount">${this._esc(totalHint)}</p>`
            + `<ul class="pat-member-list mb-0">${rows || '<li class="text-muted">None</li>'}</ul>`
            + more;
    },

    // What the members modal lists: one pattern, or a whole certification group.
    _membersScope() {
        return this._groupSel ? `group:${this._groupSel}` : (this.selectedId || '');
    },

    _membersUrl({ offset = 0 } = {}) {
        const patternId = this.selectedId || '';
        const savedId = this._viewingSavedId;
        if (savedId && this._groupSel) {
            return `/dtwin/patterns/saved/${encodeURIComponent(savedId)}/members`
                + `?certified=${this._groupSel === 'certified'}`
                + `&offset=${offset}&limit=${this.MEMBERS_PAGE}`;
        }
        if (savedId) {
            return `/dtwin/patterns/saved/${encodeURIComponent(savedId)}/members`
                + `?pattern_id=${encodeURIComponent(patternId)}`
                + `&offset=${offset}&limit=${this.MEMBERS_PAGE}`;
        }
        return `/dtwin/patterns/${encodeURIComponent(patternId)}/members`
            + `?offset=${offset}&limit=${this.MEMBERS_PAGE}`;
    },

    async openMembersModal() {
        if (!this.selectedId && !this._viewingSavedId) return;
        const body = document.getElementById('patMembersModalBody');
        if (!body) return;
        if (this._membersModal) this._membersModal.show();
        body.innerHTML = this._membersLoadingHtml();
        try {
            if (!this._members || this._members._patternId !== this._membersScope()
                || this._members._savedId !== (this._viewingSavedId || null)) {
                const members = await this._json(this._membersUrl({ offset: 0 }));
                this._members = {
                    ...members,
                    _patternId: this._membersScope(),
                    _savedId: this._viewingSavedId || null,
                };
            }
            this._renderMembersModal();
        } catch (err) {
            body.innerHTML = `<p class="text-danger small mb-0">${this._esc(err.message || 'Failed')}</p>`;
        }
    },

    _renderMembersModal() {
        const body = document.getElementById('patMembersModalBody');
        if (!body || !this._members) return;
        body.innerHTML = this._membersListHtml(this._members);
    },

    // edgeSlot: vertical pitch for parallel edges into the same node (name + badge).
    // labelPad: extra SVG top/bottom so mid-curve labels are not clipped.
    DIAGRAM: {
        colW: 340, nodeW: 150, headH: 34, attrH: 14,
        gapY: 36, pad: 20, top: 28, edgeSlot: 44, labelPad: 28,
    },

    _shortLabel(uri) {
        const s = String(uri || '');
        const tail = s.split(/[#/]/).pop();
        return tail || s;
    },

    // Same flexible URI/label match as communities — graph type URIs often
    // differ slightly from ontology class URIs (suffix / local-name only).
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

    _classIcon(typeUri, typeLabel) {
        const cls = this._findOntologyClass(typeUri, typeLabel);
        return (cls && (cls.emoji || cls.icon)) || '📦';
    },

    _trim(text, max) {
        const s = String(text || '');
        return s.length > max ? `${s.slice(0, max - 1)}…` : s;
    },

    // Columns by hop. Each node keeps a vertical band wide enough for its
    // incoming edge labels (name + badge) so parallel relationships do not overlap.
    _layoutDiagram(diagram) {
        const D = this.DIAGRAM;
        const fanIn = {};
        (diagram.edges || []).forEach((e) => {
            fanIn[e.target] = (fanIn[e.target] || 0) + 1;
        });
        const cols = {};
        diagram.nodes.forEach((n) => { (cols[n.hop] = cols[n.hop] || []).push(n); });
        const visualH = (n) => D.headH + D.attrH * (n.attributes || []).length;
        const bandH = (n) => Math.max(visualH(n), Math.max(1, fanIn[n.id] || 0) * D.edgeSlot);
        const colH = (list) => list.reduce((h, n) => h + bandH(n), 0) + D.gapY * (list.length - 1);
        const maxH = Math.max(0, ...Object.values(cols).map(colH));
        const pos = {};
        Object.entries(cols).forEach(([hop, list]) => {
            let y = D.top + D.labelPad + (maxH - colH(list)) / 2;
            list.forEach((n) => {
                const vh = visualH(n);
                const band = bandH(n);
                pos[n.id] = {
                    x: D.pad + Number(hop) * D.colW,
                    y: y + (band - vh) / 2,
                    h: vh,
                    band,
                };
                y += band + D.gapY;
            });
        });
        const maxHop = Math.max(0, ...Object.keys(cols).map(Number));
        return {
            pos,
            maxHop,
            width: D.pad * 2 + maxHop * D.colW + D.nodeW,
            height: D.top + D.labelPad + maxH + D.labelPad + D.pad,
        };
    },

    /**
     * Y on the left/right face of a node box for the i-th of n edges.
     * Always stays inside the visible rect (never floats in the band gap).
     */
    _edgeAttachY(p, index, total) {
        const n = Math.max(1, total || 1);
        const i = Math.min(Math.max(1, index || 1), n);
        if (n === 1) return p.y + p.h / 2;
        const m = Math.min(8, p.h / 4);
        return p.y + m + ((i - 1) / (n - 1)) * (p.h - 2 * m);
    },

    _diagramEdge(e, pos, incoming, incomingTotal, outgoing, outgoingTotal) {
        const D = this.DIAGRAM;
        const a = pos[e.source];
        const b = pos[e.target];
        if (!a || !b) return '';
        const inSlot = incoming[e.target] = (incoming[e.target] || 0) + 1;
        const nIn = incomingTotal[e.target] || 1;
        const outSlot = outgoing[e.source] = (outgoing[e.source] || 0) + 1;
        const nOut = outgoingTotal[e.source] || 1;
        // Endpoints sit on the entity boxes. Lane offset only bends the curve
        // and separates labels — it must not pull the arrow off the node.
        const x1 = a.x + D.nodeW;
        const y1 = this._edgeAttachY(a, outSlot, nOut);
        const x2 = b.x;
        const y2 = this._edgeAttachY(b, inSlot, nIn);
        const lane = (inSlot - (nIn + 1) / 2) * D.edgeSlot;
        const mx = (x1 + x2) / 2;
        const my = (y1 + y2) / 2 + lane * 0.45;
        const path = `M${x1},${y1} C${mx},${y1 + lane * 0.35} ${mx},${y2 + lane * 0.35} ${x2},${y2}`;
        const dashed = e.share < 1 ? ' stroke-dasharray="4 3"' : '';
        // Every edge is walked subject -> object, so the arrow always points at the child.
        const marker = 'marker-end="url(#patArrow)"';
        const relName = e.via_label || this._shortLabel(e.via) || 'rel';
        const name = this._esc(relName);
        const viaTip = `Relationship “${relName}”. Full URI: ${e.via || relName}`;
        const chipW = (text) => Math.max(22, String(text).length * 6.2 + 10);
        const chips = [];
        if (Number(e.avg) > 0) {
            let avgTip = `Average neighbours of this type per matching entity (×${e.avg}).`;
            if (e.min != null && e.max != null) {
                avgTip += ` Range ${this._fmtNum(e.min)}–${this._fmtNum(e.max)} per entity`;
                if (e.count != null) {
                    avgTip += `, across ${this._fmtNum(e.count)} matching `
                        + `entit${Number(e.count) === 1 ? 'y' : 'ies'}`;
                }
                avgTip += '.';
            }
            chips.push({ text: `×${e.avg}`, kind: 'avg', tip: avgTip });
        }
        if (e.share < 1) {
            const pct = Math.round(e.share * 100);
            chips.push({
                text: `${pct}%`,
                kind: 'share',
                tip: `Share of matching entities that have this relationship (${pct}%). `
                    + 'Dashed edges mean not every member has it.',
            });
        }
        const gap = 4;
        const rowW = chips.reduce((s, c) => s + chipW(c.text), 0) + gap * Math.max(0, chips.length - 1);
        let bx = mx - rowW / 2;
        const badges = chips.map((chip) => {
            const w = chipW(chip.text);
            const g = `<g class="pat-dg-edge-badge pat-dg-edge-badge-${chip.kind}" `
                + `transform="translate(${bx},${my + 4})" data-pat-tip="${this._esc(chip.tip)}">`
                + `<rect width="${w}" height="14" rx="7" class="pat-dg-edge-badge-bg"/>`
                + `<text x="${w / 2}" y="10.5" text-anchor="middle" class="pat-dg-edge-badge-txt">${this._esc(chip.text)}</text>`
                + `</g>`;
            bx += w + gap;
            return g;
        }).join('');
        const ends = `data-source="${this._esc(e.source)}" data-target="${this._esc(e.target)}"`;
        // No SVG <title>: the floating tip owns the explanation (avoids a double tooltip).
        return `<path d="${path}" class="pat-dg-edge"${dashed} ${marker} ${ends} `
            + `data-pat-tip="${this._esc(viaTip)}"></path>`
            + `<g class="pat-dg-edge-label" ${ends} data-pat-tip="${this._esc(viaTip)}">`
            + `<text x="${mx}" y="${my - 6}" text-anchor="middle" class="pat-dg-edge-name" `
            + `data-pat-tip="${this._esc(viaTip)}">${name}</text>`
            + `${badges}</g>`;
    },

    _diagramNode(n, p) {
        const D = this.DIAGRAM;
        const icon = this._classIcon(n.type, n.type_label);
        const cls = n.hop === 0 ? 'pat-dg-node pat-dg-anchor' : 'pat-dg-node';
        const attrs = (n.attributes || []).map((at, i) => (
            `<text x="${p.x + 10}" y="${p.y + D.headH + 4 + i * D.attrH}" class="pat-dg-attr">`
            + `${this._esc(this._trim(`${at.prop}: ${at.value}`, 26))}</text>`
        )).join('');
        return `<g class="pat-dg-node-g" data-node-id="${this._esc(n.id)}">`
            + `<rect x="${p.x}" y="${p.y}" width="${D.nodeW}" height="${p.h}" rx="8" class="${cls}"><title>${this._esc(n.type)}</title></rect>`
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
        // Dotted vertical guides through each hop column centre (Entity, 1 hop, …).
        // No gutter lines in the relationship space between columns.
        const laneTop = 18;
        const laneBottom = L.height - Math.max(8, D.pad / 2);
        const lanes = Array.from({ length: L.maxHop + 1 }, (_, h) => {
            const x = D.pad + h * D.colW + D.nodeW / 2;
            return `<line x1="${x}" y1="${laneTop}" x2="${x}" y2="${laneBottom}" class="pat-dg-lane"/>`;
        }).join('');
        const incomingTotal = {};
        const outgoingTotal = {};
        (diagram.edges || []).forEach((e) => {
            incomingTotal[e.target] = (incomingTotal[e.target] || 0) + 1;
            outgoingTotal[e.source] = (outgoingTotal[e.source] || 0) + 1;
        });
        const incoming = {};
        const outgoing = {};
        const edges = (diagram.edges || []).map((e) => (
            this._diagramEdge(e, L.pos, incoming, incomingTotal, outgoing, outgoingTotal)
        )).join('');
        const nodes = diagram.nodes.map((n) => this._diagramNode(n, L.pos[n.id])).join('');
        const note = diagram.truncated ? '<p class="text-muted small mb-0">Showing the most common relationships only.</p>' : '';
        return `<div class="pat-diagram-scroll"><svg class="pat-diagram-svg" width="100%" height="${L.height}" `
            + `viewBox="0 0 ${L.width} ${L.height}" preserveAspectRatio="xMidYMid meet" role="img" aria-label="Pattern diagram">`
            + '<defs>'
            + '<marker id="patArrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L10,5 L0,10 z" class="pat-dg-arrow"/></marker>'
            + '</defs>'
            + `${lanes}${heads}${edges}${nodes}</svg></div>`
            + '<p class="text-muted small mb-0 mt-1">Dashed: not every member has it · badge ×n: average per member · '
            + 'hover a relationship or badge for details · hover a node to trace its related paths.</p>'
            + note;
    },

    /**
     * Hover highlight: pointing at a diagram node lights up its lineage —
     * ancestors toward the entity (hop 0), descendants toward further hops,
     * and every relationship on those paths. Unrelated branches dim.
     * (Undirected BFS would light the whole diagram: every pattern graph is
     * one component rooted at hop 0.) Delegated on document; classes only.
     */
    _bindDiagramHover() {
        document.addEventListener('mouseover', (e) => {
            const node = e.target.closest?.('.pat-dg-node-g');
            if (node) this._highlightDiagram(node);
        });
        document.addEventListener('mouseout', (e) => {
            const node = e.target.closest?.('.pat-dg-node-g');
            if (!node) return;
            // Still inside a node (child element) or entering another one:
            // that mouseover handles the highlight, so keep the current state.
            if (e.relatedTarget?.closest?.('.pat-dg-node-g')) return;
            this._clearDiagramHighlight(node.closest('.pat-diagram-svg'));
        });
    },

    /**
     * Relationship / badge tooltips: delegated so they survive re-renders.
     * Prefer the nearest element that carries `data-pat-tip` (badge beats
     * label group so ×n / % get their own explanations).
     */
    _bindDiagramTooltips() {
        const tipSel = '[data-pat-tip]';
        document.addEventListener('mouseover', (e) => {
            const host = e.target.closest?.(tipSel);
            if (!host || !host.closest?.('.pat-diagram-svg')) return;
            this._showEdgeTip(host, e);
        });
        document.addEventListener('mousemove', (e) => {
            if (!this._edgeTipEl || this._edgeTipEl.hidden) return;
            if (!e.target.closest?.(tipSel)) return;
            this._placeEdgeTip(e);
        });
        document.addEventListener('mouseout', (e) => {
            const host = e.target.closest?.(tipSel);
            if (!host || !host.closest?.('.pat-diagram-svg')) return;
            if (e.relatedTarget?.closest?.(tipSel)) return;
            this._hideEdgeTip();
        });
    },

    _ensureEdgeTip() {
        if (this._edgeTipEl) return this._edgeTipEl;
        const tip = document.createElement('div');
        tip.id = 'patDgTip';
        tip.className = 'pat-dg-tip';
        tip.setAttribute('role', 'tooltip');
        tip.hidden = true;
        document.body.appendChild(tip);
        this._edgeTipEl = tip;
        return tip;
    },

    _showEdgeTip(host, e) {
        const text = this._attr(host, 'data-pat-tip');
        if (!text) return;
        const tip = this._ensureEdgeTip();
        tip.textContent = text;
        tip.hidden = false;
        this._placeEdgeTip(e);
    },

    _placeEdgeTip(e) {
        const tip = this._edgeTipEl;
        if (!tip || tip.hidden) return;
        const pad = 12;
        const tw = tip.offsetWidth || 200;
        const th = tip.offsetHeight || 40;
        let left = e.clientX + pad;
        let top = e.clientY + pad;
        if (left + tw > window.innerWidth - 8) left = e.clientX - tw - pad;
        if (top + th > window.innerHeight - 8) top = e.clientY - th - pad;
        tip.style.left = `${Math.max(8, left)}px`;
        tip.style.top = `${Math.max(8, top)}px`;
    },

    _hideEdgeTip() {
        if (this._edgeTipEl) this._edgeTipEl.hidden = true;
    },

    _attr(el, name) {
        return (el && el.getAttribute && el.getAttribute(name)) || '';
    },

    _highlightDiagram(node) {
        const svg = node.closest('.pat-diagram-svg');
        const id = this._attr(node, 'data-node-id');
        if (!svg || !id || this._attr(svg, 'data-hover-node') === id) return;
        svg.setAttribute('data-hover-node', id);

        // Edges always run parent (hop N-1) → child (hop N). Walk both ways
        // from the hovered node so related hops light up; skip side branches.
        const down = new Map();
        const up = new Map();
        const link = (map, a, b) => {
            if (!a || !b) return;
            if (!map.has(a)) map.set(a, []);
            map.get(a).push(b);
        };
        svg.querySelectorAll('.pat-dg-edge').forEach((p) => {
            const s = this._attr(p, 'data-source');
            const t = this._attr(p, 'data-target');
            link(down, s, t);
            link(up, t, s);
        });

        const reached = new Set([id]);
        const walk = (start, adj) => {
            const stack = [start];
            while (stack.length) {
                for (const next of adj.get(stack.pop()) || []) {
                    if (!reached.has(next)) {
                        reached.add(next);
                        stack.push(next);
                    }
                }
            }
        };
        walk(id, up);
        walk(id, down);

        svg.classList.add('is-hovering');
        svg.querySelectorAll('.pat-dg-node-g').forEach((g) => {
            const nid = this._attr(g, 'data-node-id');
            g.classList.toggle('is-on', reached.has(nid));
            g.classList.toggle('is-focus', nid === id);
        });
        // An edge is on the related path only when both ends are in the
        // lineage AND it actually links them as parent→child in the walk.
        svg.querySelectorAll('.pat-dg-edge, .pat-dg-edge-label').forEach((el) => {
            const s = this._attr(el, 'data-source');
            const t = this._attr(el, 'data-target');
            const onPath = reached.has(s) && reached.has(t)
                && (down.get(s) || []).includes(t);
            el.classList.toggle('is-on', onPath);
        });
    },

    _clearDiagramHighlight(svg) {
        if (!svg) return;
        svg.removeAttribute('data-hover-node');
        svg.classList.remove('is-hovering');
        svg.querySelectorAll('.is-on, .is-focus').forEach((el) => el.classList.remove('is-on', 'is-focus'));
    },

    async _loadMoreMembers() {
        if ((!this.selectedId && !this._viewingSavedId) || !this._members) return;
        const offset = (this._members.offset || 0) + (this._members.members || []).length;
        const btn = document.querySelector('#patMembersModalBody [data-pat-more]');
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>Loading…';
        }
        try {
            const page = await this._json(this._membersUrl({ offset }));
            this._members = {
                ...this._members,
                members: (this._members.members || []).concat(page.members || []),
                has_more: !!page.has_more,
                offset: 0,
            };
            this._renderMembersModal();
        } catch (err) {
            this._notify(err.message || 'Failed to load members', 'danger');
            if (btn) {
                btn.disabled = false;
                btn.textContent = 'Load more';
            }
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
        // Detect now saves in the same task; still resume a legacy in-flight save.
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
        try {
            const task = await waitForTask(taskId, (t) => {
                if (!t || !t.message) return;
                const saving = /sav/i.test(t.message);
                this._showLoading(
                    true,
                    t.message,
                    saving ? 'Saving…' : 'Detecting…'
                );
            });
            this._clearSavedView();
            await this.loadLatest();
            const stats = (this.latest && this.latest.stats) || {};
            this._notify(
                task.message || `${this._fmtNum(stats.pattern_count || 0)} patterns detected.`,
                'success'
            );
            const savedId = task && task.result && task.result.saved_id;
            await this.refreshSaved();
            if (savedId) {
                // The run's universe/coverage is not stored in the saved
                // snapshot — carry the just-computed stats into the saved view.
                this._freshRunStats = (this.latest && this.latest.stats) || null;
                await this.openSavedById(savedId, { sync: true, replace: true });
            } else {
                this._restoreDiscoverPreview();
            }
        } catch (err) {
            await this.loadLatest();
            this._notify(err.message || 'Detection failed', 'danger');
            this._restoreDiscoverPreview();
        } finally {
            this._setBtnSpinner('patDetectBtn', false);
            this._setBusy(false);
            this._showLoading(false);
        }
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
            // One task: Detect + catalog save; opens the new snapshot on success.
            await this._awaitDetection(data.task_id);
        } catch (err) {
            this._setBtnSpinner('patDetectBtn', false);
            this._setBusy(false);
            this._notify(err.message || 'Detection failed', 'danger');
        }
    },

    async _awaitSave(taskId, label) {
        // Resume helper for a legacy in-flight `pattern_save` task only.
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
        // Snapshot only: saved catalog + diagrams. Do not pull `/latest` values.
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
            details.innerHTML = this._loadingHtml('Loading saved Detect run…');
        }
        try {
            // Restore the full Detect catalog from the snapshot (not live /latest).
            const catalog = await this._json(
                `/dtwin/patterns/saved/${encodeURIComponent(saved.id)}/patterns`
            );
            if (this._viewingSavedId !== saved.id) return;
            const patterns = catalog.patterns || [];
            // Prefer the snapshot's own universe (runs saved since
            // population_count was persisted); otherwise fall back to the
            // just-computed run stats; else Coverage shows "—".
            const snapStats = (catalog.stats && Number(catalog.stats.population_count))
                ? catalog.stats
                : null;
            this.latest = {
                ...(this.latest || {}),
                patterns,
                stats: snapStats || this._freshRunStats || null,
                job_available: true,
            };
            this._freshRunStats = null;
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

    async deleteSaved(savedId, triggerBtn = null) {
        if (!this._canEdit()) return;
        const btn = triggerBtn || document.querySelector(`[data-pat-delete="${CSS.escape(savedId)}"]`);
        if (btn && btn.classList.contains('is-busy')) return;
        const go = (typeof showDeleteConfirm === 'function')
            ? await showDeleteConfirm('Delete this saved pattern?')
            : true;
        if (!go) return;
        const card = btn ? btn.closest('.pat-saved-card') : null;
        const origHtml = btn ? btn.innerHTML : '';
        if (btn) {
            btn.disabled = true;
            btn.classList.add('is-busy');
            btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>';
            btn.setAttribute('title', 'Deleting…');
            btn.setAttribute('aria-label', 'Deleting…');
        }
        if (card) {
            card.classList.add('is-deleting');
        }
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
            if (btn) {
                btn.disabled = false;
                btn.classList.remove('is-busy');
                btn.innerHTML = origHtml || '<i class="bi bi-trash"></i>';
                btn.setAttribute('title', 'Delete saved pattern');
                btn.removeAttribute('aria-label');
            }
            if (card) {
                card.classList.remove('is-deleting');
            }
            this._notify(err.message || 'Delete failed', 'danger');
        }
    },
};

document.addEventListener('DOMContentLoaded', () => IntelligencePatterns.init());
