/**
 * Simulation Results — KPIs and paged added / removed triples of the
 * last run of the scenario open in the workbench.
 */
const SimulationResults = {
    selectedId: null,
    kind: 'added',
    page: 1,
    PAGE_SIZE: 50,
    _searchTimer: null,

    init() {
        const refresh = document.getElementById('simResultsRefreshBtn');
        if (refresh) refresh.addEventListener('click', () => this.load());
        document.querySelectorAll('#simResultsTabs [data-kind]').forEach((btn) => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('#simResultsTabs [data-kind]').forEach((b) => {
                    b.classList.toggle('active', b === btn);
                    b.setAttribute('aria-selected', b === btn ? 'true' : 'false');
                });
                this.kind = btn.dataset.kind;
                this.page = 1;
                this.load();
            });
        });
        const search = document.getElementById('simResultsSearch');
        if (search) {
            search.addEventListener('input', () => {
                clearTimeout(this._searchTimer);
                this._searchTimer = setTimeout(() => {
                    this.page = 1;
                    this.load();
                }, 300);
            });
        }
        const prev = document.getElementById('simResultsPrev');
        const next = document.getElementById('simResultsNext');
        if (prev) prev.addEventListener('click', () => { if (this.page > 1) { this.page -= 1; this.load(); } });
        if (next) next.addEventListener('click', () => { this.page += 1; this.load(); });
    },

    onShow() {
        const id = (window.SimulationWorkbench && SimulationWorkbench.currentId) || this.selectedId;
        if (id) this.select(id);
        else this._toggle(false);
    },

    select(id) {
        this.selectedId = id || null;
        this.page = 1;
        this.load();
    },

    _paintBadge(summary) {
        const badge = document.getElementById('simWbResultBadge');
        if (!badge) return;
        const added = summary && summary.added_count;
        const removed = summary && summary.removed_count;
        if (added == null && removed == null) {
            badge.textContent = '';
            return;
        }
        badge.textContent = `+${added || 0} / −${removed || 0}`;
    },

    _toggle(hasResult) {
        const empty = document.getElementById('simResultsEmpty');
        const body = document.getElementById('simResultsBody');
        if (empty) empty.classList.toggle('d-none', hasResult);
        if (body) body.classList.toggle('d-none', !hasResult);
        const explorer = document.getElementById('simResultsExplorerBtn');
        if (explorer) {
            explorer.classList.toggle('d-none', !this.selectedId);
            explorer.href = this.selectedId
                ? `/dtwin/?section=sigmagraph&simulation=${encodeURIComponent(this.selectedId)}`
                : '/dtwin/?section=sigmagraph';
        }
        if (!hasResult) this._paintBadge(null);
    },

    async load() {
        if (!this.selectedId) {
            this._toggle(false);
            return;
        }
        const search = (document.getElementById('simResultsSearch') || {}).value || '';
        const qs = new URLSearchParams({
            kind: this.kind,
            page: String(this.page),
            page_size: String(this.PAGE_SIZE),
            search,
        });
        if (window.SimulationWorkbench) SimulationWorkbench.setTabBusy('results', true);
        try {
            const sim = (SimulationPage.scenarios || []).find((s) => s.id === this.selectedId) || {};
            const data = await SimulationPage.api(`/simulation/${encodeURIComponent(this.selectedId)}/diff?${qs}`);
            this._toggle(true);
            this._renderSummary(data.summary || {}, sim);
            this._renderRows(data);
        } catch (err) {
            this._toggle(false);
            showNotification(`Could not load results: ${err.message}`, 'error');
        } finally {
            if (window.SimulationWorkbench) SimulationWorkbench.setTabBusy('results', false);
        }
    },

    _renderSummary(summary, sim) {
        document.querySelectorAll('#simResultsKpis [data-kpi]').forEach((el) => {
            const v = summary[el.dataset.kpi];
            el.textContent = v == null ? '–' : Number(v).toLocaleString();
        });
        this._paintBadge(summary);
        const added = document.getElementById('simTabBadgeAdded');
        const removed = document.getElementById('simTabBadgeRemoved');
        if (added) added.textContent = summary.added_count != null ? summary.added_count : '';
        if (removed) removed.textContent = summary.removed_count != null ? summary.removed_count : '';

        const errBox = document.getElementById('simResultsError');
        if (errBox) {
            const failed = sim.status === 'failed' && sim.error;
            errBox.classList.toggle('d-none', !failed);
            errBox.textContent = failed ? `Last run failed: ${sim.error}` : '';
        }
        const warnBox = document.getElementById('simResultsWarnings');
        const phaseErrors = summary.phase_errors || {};
        const keys = Object.keys(phaseErrors);
        if (warnBox) {
            warnBox.classList.toggle('d-none', !keys.length);
            warnBox.innerHTML = keys.map((k) => `<div><strong>${SimulationPage.esc(k)}</strong>: ${SimulationPage.esc(phaseErrors[k])}</div>`).join('');
        }
        const cap = document.getElementById('simResultsCapNote');
        if (cap) cap.textContent = summary.capped ? `Showing the first ${summary.row_cap} rows per list` : '';
    },

    explorerHref(uri) {
        if (!this.selectedId || !SimulationPage.isUri(uri)) return '';
        return '/dtwin/?section=sigmagraph'
            + '&simulation=' + encodeURIComponent(this.selectedId)
            + '&focus=' + encodeURIComponent(uri);
    },

    explorerCell(uri) {
        const raw = String(uri || '');
        const href = this.explorerHref(raw);
        if (!href) return SimulationPage.uriCell(raw);
        const shown = SimulationPage.termLabel(raw);
        return `<a class="sim-uri sim-uri-link" href="${SimulationPage.esc(href)}" title="${SimulationPage.esc(raw)}">${SimulationPage.esc(shown)}</a>`;
    },

    _renderRows(data) {
        const body = document.getElementById('simResultsRows');
        const info = document.getElementById('simResultsPageInfo');
        const rows = data.rows || [];
        const total = data.total || 0;
        const pages = Math.max(1, Math.ceil(total / (data.page_size || this.PAGE_SIZE)));
        if (this.page > pages) this.page = pages;
        if (body) {
            body.innerHTML = rows.length
                ? rows.map((r) => {
                    const origin = r.origin === 'inferred'
                        ? `<span class="badge sim-origin-inferred" title="${SimulationPage.esc(r.provenance || '')}">inferred</span>`
                        : '<span class="badge sim-origin-patch">your fact</span>';
                    return `<tr>
                        <td>${this.explorerCell(r.subject)}</td>
                        <td>${SimulationPage.uriCell(r.predicate)}</td>
                        <td>${this.explorerCell(r.object)}</td>
                        <td>${origin}</td>
                    </tr>`;
                }).join('')
                : '<tr><td colspan="4" class="text-center text-muted py-3">Nothing here.</td></tr>';
        }
        if (info) info.textContent = `${total.toLocaleString()} rows · page ${data.page || 1} / ${pages}`;
        const prev = document.getElementById('simResultsPrev');
        const next = document.getElementById('simResultsNext');
        if (prev) prev.disabled = (data.page || 1) <= 1;
        if (next) next.disabled = (data.page || 1) >= pages;
    },
};

window.SimulationResults = SimulationResults;
document.addEventListener('DOMContentLoaded', () => SimulationResults.init());
