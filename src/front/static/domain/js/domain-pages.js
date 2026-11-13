// =====================================================
// CONTEXT → PAGES — Genie Pages search → selection for Generate
// =====================================================

const PagesManager = {
    /** Last Databricks list page (raw, before text filter). */
    searchPool: [],
    /** Hits matching the current query. */
    searchHits: [],
    nextToken: null,
    lastQuery: '',
    domains: [],
    domainLabels: {},
    selected: new Map(), // name → snapshot
    dirty: false,
    loading: false,
    domainsLoaded: false,

    init() {
        if (!document.getElementById('pagesSelectedList')) return;

        const searchBtn = document.getElementById('pagesSearchBtn');
        const searchInput = document.getElementById('pagesSearchInput');
        const moreBtn = document.getElementById('pagesSearchMoreBtn');
        const saveBtn = document.getElementById('pagesSaveSelectionBtn');
        const clearBtn = document.getElementById('pagesClearSelectionBtn');

        if (searchBtn) {
            searchBtn.addEventListener('click', (e) => {
                e.preventDefault();
                this.runSearch(false);
            });
        }
        if (searchInput) {
            searchInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    this.runSearch(false);
                }
            });
        }
        if (moreBtn) moreBtn.addEventListener('click', () => this.runSearch(true));
        if (saveBtn) saveBtn.addEventListener('click', () => this.saveSelection());
        if (clearBtn) clearBtn.addEventListener('click', () => this.clearSelection());

        document.addEventListener('click', (e) => {
            const previewBtn = e.target.closest('[data-pages-preview]');
            if (previewBtn) {
                e.preventDefault();
                this.preview(previewBtn.getAttribute('data-pages-preview'));
                return;
            }
            const addBtn = e.target.closest('[data-pages-add]');
            if (addBtn) {
                e.preventDefault();
                const name = addBtn.getAttribute('data-pages-add');
                const page = this.searchHits.find((p) => p.name === name)
                    || this.searchPool.find((p) => p.name === name);
                this.toggleSelect(name, true, page);
                this.renderSearchHits();
                return;
            }
            const removeBtn = e.target.closest('[data-pages-remove]');
            if (removeBtn) {
                e.preventDefault();
                this.toggleSelect(removeBtn.getAttribute('data-pages-remove'), false);
                this.renderSearchHits();
            }
        });

        // Resolve Databricks Domain labels first so tables never flash
        // raw ``domains/{uuid}`` resource names.
        this.ensureDomains().finally(() => this.loadSelection());
    },

    async loadSelection() {
        this._showSelectedLoading();
        try {
            const resp = await fetch('/domain/pages/selection', { credentials: 'same-origin' });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.message || data.detail || 'Failed to load selection');
            }
            this.selected = new Map();
            (data.pages || []).forEach((p) => {
                if (p && p.name) this.selected.set(p.name, p);
            });
            this.dirty = false;
            this.renderSelected();
        } catch (err) {
            this._searchStatus('Could not load saved selection: ' + err.message, true);
            const list = document.getElementById('pagesSelectedList');
            if (list) {
                list.innerHTML =
                    '<tr><td colspan="5" class="text-danger small px-3 py-3">' +
                    this._esc(err.message || 'Failed to load selection') +
                    '</td></tr>';
            }
        }
    },

    _showSelectedLoading() {
        const list = document.getElementById('pagesSelectedList');
        const count = document.getElementById('pagesSelectedCount');
        if (count) count.textContent = '…';
        if (!list) return;
        list.innerHTML =
            '<tr>' +
            '<td colspan="5" class="text-muted small px-3 py-3">' +
            '<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>' +
            'Loading selected pages…' +
            '</td>' +
            '</tr>';
    },

    async ensureDomains() {
        if (this.domainsLoaded) return;
        try {
            const params = new URLSearchParams({
                page_size: '1',
                include_domains: 'true',
            });
            const resp = await fetch('/domain/pages/list?' + params.toString(), {
                credentials: 'same-origin',
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.message || data.detail || 'Could not load Databricks Domains');
            }
            if (Array.isArray(data.domains)) {
                this._applyDomains(data.domains);
            }
            if (data.domains_error) {
                this._searchStatus('Databricks Domains unavailable: ' + data.domains_error, true);
            }
        } catch (err) {
            this._searchStatus(err.message || 'Could not load Databricks Domains', true);
        }
    },

    _applyDomains(domains) {
        this.domains = domains || [];
        this.domainLabels = {};
        this.domains.forEach((d) => {
            const resource = d.name || '';
            const label = (d.label || '').trim();
            if (resource) {
                this.domainLabels[resource] = label || resource;
            }
        });
        this.domainsLoaded = true;
        const sel = document.getElementById('pagesDomainFilter');
        if (sel) {
            const current = sel.value;
            const opts = ['<option value="">All domains</option>'];
            this.domains.forEach((d) => {
                opts.push(
                    '<option value="' + this._escAttr(d.name) + '">' +
                    this._esc(this._domainLabel(d.name)) +
                    '</option>'
                );
            });
            sel.innerHTML = opts.join('');
            if (current && [...sel.options].some((o) => o.value === current)) {
                sel.value = current;
            }
        }
        this.renderSelected();
        this.renderSearchHits();
    },

    /** Human-readable Databricks Domain name (tag key), never the raw resource id. */
    _domainLabel(domainResource) {
        const name = domainResource || '';
        if (!name) return '—';
        const mapped = (this.domainLabels[name] || '').trim();
        if (mapped && !mapped.startsWith('domains/')) {
            return mapped;
        }
        // Last resort: strip ``domains/`` rather than show the full resource.
        if (name.startsWith('domains/')) {
            return name.slice('domains/'.length) || '—';
        }
        return name;
    },

    buildFilter() {
        const parts = [];
        const domain = (document.getElementById('pagesDomainFilter') || {}).value || '';
        const state = (document.getElementById('pagesStateFilter') || {}).value;
        if (domain) parts.push('domain = "' + domain.replace(/"/g, '') + '"');
        if (state === 'PUBLISHED' || state === 'DRAFT') {
            parts.push('state = "' + state + '"');
        }
        return parts.join(' AND ');
    },

    _matchQuery(page, q) {
        if (!q) return false;
        const hay = (
            (page.display_name || '') +
            ' ' +
            (page.description || '') +
            ' ' +
            ((page.synonyms || []).join(' '))
        ).toLowerCase();
        return hay.includes(q);
    },

    async runSearch(append) {
        const input = document.getElementById('pagesSearchInput');
        const q = ((input && input.value) || '').trim().toLowerCase();
        if (!q) {
            this._searchStatus('Enter a keyword to search Genie Pages.', true);
            this._hideResults();
            return;
        }
        if (this.loading) return;

        if (!append) {
            this.searchPool = [];
            this.searchHits = [];
            this.nextToken = null;
            this.lastQuery = q;
        } else if (q !== this.lastQuery) {
            // Query changed since last run — restart.
            return this.runSearch(false);
        }

        this.loading = true;
        this._searchStatus(append ? 'Searching for more matches…' : 'Searching Genie Pages…');
        this._showResults();

        try {
            await this.ensureDomains();
            // Pull pages from Databricks (domain/state filter), match query client-side.
            // Keep paging until we have hits or run out of pages (cap rounds).
            let rounds = 0;
            const maxRounds = append ? 1 : 5;
            do {
                const params = new URLSearchParams();
                params.set('page_size', '50');
                const filter = this.buildFilter();
                if (filter) params.set('filter', filter);
                if (this.nextToken) params.set('page_token', this.nextToken);
                if (!this.domainsLoaded) params.set('include_domains', 'true');

                const resp = await fetch('/domain/pages/list?' + params.toString(), {
                    credentials: 'same-origin',
                });
                const data = await resp.json();
                if (!resp.ok || !data.success) {
                    throw new Error(data.message || data.detail || 'Search failed');
                }
                if (Array.isArray(data.domains) && data.domains.length) {
                    this._applyDomains(data.domains);
                }
                const batch = data.pages || [];
                this.searchPool = this.searchPool.concat(batch);
                this.nextToken = data.next_page_token || null;
                rounds += 1;

                const newHits = batch.filter((p) => this._matchQuery(p, this.lastQuery));
                if (newHits.length) {
                    this.searchHits = this.searchHits.concat(newHits);
                }
                // Stop early once we have some hits on first search, or after one more page on append.
                if (this.searchHits.length && (!append || rounds >= 1)) break;
                if (!this.nextToken) break;
            } while (rounds < maxRounds);

            this.renderSearchHits();
            if (!this.searchHits.length) {
                this._searchStatus(
                    'No Genie Pages match "' + this.lastQuery + '". Try another keyword or domain.',
                    false
                );
            } else {
                this._searchStatus(
                    this.searchHits.length +
                        ' match' +
                        (this.searchHits.length === 1 ? '' : 'es') +
                        (this.nextToken ? ' (more available)' : '')
                );
            }
        } catch (err) {
            this._searchStatus(err.message || 'Search failed', true);
            const body = document.getElementById('pagesSearchBody');
            if (body) {
                body.innerHTML =
                    '<tr><td colspan="4" class="text-danger small px-3 py-3">' +
                    this._esc(err.message || 'Search failed') +
                    '</td></tr>';
            }
        } finally {
            this.loading = false;
            const more = document.getElementById('pagesSearchMoreBtn');
            if (more) more.classList.toggle('d-none', !this.nextToken);
            const count = document.getElementById('pagesSearchCount');
            if (count) {
                count.textContent = this.searchHits.length
                    ? this.searchHits.length + ' shown'
                    : '';
            }
        }
    },

    renderSearchHits() {
        const body = document.getElementById('pagesSearchBody');
        const wrap = document.getElementById('pagesSearchResults');
        if (!body || !wrap) return;
        wrap.classList.remove('d-none');
        if (!this.searchHits.length) {
            body.innerHTML =
                '<tr><td colspan="4" class="text-muted small px-3 py-3">No matches.</td></tr>';
            return;
        }
        body.innerHTML = this.searchHits
            .map((p) => {
                const already = this.selected.has(p.name);
                const domainLabel = this._domainLabel(p.domain);
                const state = p.state || '';
                const badge =
                    state === 'PUBLISHED'
                        ? 'text-bg-success'
                        : state === 'DRAFT'
                          ? 'text-bg-warning'
                          : 'text-bg-secondary';
                const action = already
                    ? '<span class="badge text-bg-light text-muted">Added</span>'
                    : '<button type="button" class="btn btn-sm btn-outline-primary ontology-edit-btn" ' +
                      'data-pages-add="' +
                      this._escAttr(p.name) +
                      '"><i class="bi bi-plus-lg me-1"></i>Add</button>';
                return (
                    '<tr>' +
                    '<td class="ps-2">' +
                    '<a href="#" class="fw-semibold text-decoration-none pages-title-link" data-pages-preview="' +
                    this._escAttr(p.name) +
                    '" role="button" title="Preview ' +
                    this._escAttr(p.display_name || p.name) +
                    '">' +
                    this._esc(p.display_name || p.name) +
                    '</a>' +
                    '</td>' +
                    '<td class="small text-muted text-truncate" style="max-width:12rem;" title="' +
                    this._escAttr(domainLabel) +
                    '">' +
                    this._esc(domainLabel) +
                    '</td>' +
                    '<td><span class="badge ' +
                    badge +
                    '">' +
                    this._esc(state || '—') +
                    '</span></td>' +
                    '<td class="pe-3 text-end">' +
                    action +
                    '</td>' +
                    '</tr>'
                );
            })
            .join('');
    },

    _showResults() {
        const wrap = document.getElementById('pagesSearchResults');
        if (wrap) wrap.classList.remove('d-none');
    },

    _hideResults() {
        const wrap = document.getElementById('pagesSearchResults');
        if (wrap) wrap.classList.add('d-none');
        const body = document.getElementById('pagesSearchBody');
        if (body) body.innerHTML = '';
        const more = document.getElementById('pagesSearchMoreBtn');
        if (more) more.classList.add('d-none');
        const count = document.getElementById('pagesSearchCount');
        if (count) count.textContent = '';
    },

    toggleSelect(name, on, page) {
        if (!name) return;
        if (on) {
            const snap = page || this.selected.get(name) || { name: name };
            this.selected.set(name, {
                name: snap.name,
                domain: snap.domain || '',
                display_name: snap.display_name || snap.name,
                state: snap.state || 'PUBLISHED',
                description: snap.description || '',
                synonyms: snap.synonyms || [],
                update_time: snap.update_time || '',
            });
        } else {
            this.selected.delete(name);
        }
        this.dirty = true;
        this.renderSelected();
    },

    clearSelection() {
        this.selected.clear();
        this.dirty = true;
        this.renderSelected();
        this.renderSearchHits();
    },

    renderSelected() {
        const list = document.getElementById('pagesSelectedList');
        const count = document.getElementById('pagesSelectedCount');
        if (count) count.textContent = String(this.selected.size);
        if (!list) return;
        if (!this.selected.size) {
            list.innerHTML =
                '<tr><td colspan="5" class="text-muted small px-3 py-3">' +
                'No pages yet. Use the search above to find Genie Pages and add them.' +
                '</td></tr>';
            return;
        }
        const items = [...this.selected.values()].sort((a, b) =>
            (a.display_name || '').localeCompare(b.display_name || '')
        );
        list.innerHTML = items
            .map((p) => {
                const domainLabel = this._domainLabel(p.domain);
                const state = p.state || '';
                const badge =
                    state === 'PUBLISHED'
                        ? 'text-bg-success'
                        : state === 'DRAFT'
                          ? 'text-bg-warning'
                          : 'text-bg-secondary';
                return (
                    '<tr>' +
                    '<td class="ps-3">' +
                    '<a href="#" class="fw-semibold text-decoration-none pages-title-link" data-pages-preview="' +
                    this._escAttr(p.name) +
                    '" role="button" title="Preview ' +
                    this._escAttr(p.display_name || p.name) +
                    '">' +
                    this._esc(p.display_name || p.name) +
                    '</a>' +
                    '</td>' +
                    '<td class="small text-muted text-truncate" style="max-width:12rem;" title="' +
                    this._escAttr(domainLabel) +
                    '">' +
                    this._esc(domainLabel) +
                    '</td>' +
                    '<td><span class="badge ' +
                    badge +
                    '">' +
                    this._esc(state || '—') +
                    '</span></td>' +
                    '<td class="small text-muted">' +
                    this._esc(this._fmtTime(p.update_time)) +
                    '</td>' +
                    '<td class="pe-3 text-end">' +
                    '<button type="button" class="btn btn-sm btn-link text-danger py-0 ontology-edit-btn" ' +
                    'data-pages-remove="' +
                    this._escAttr(p.name) +
                    '" title="Remove">Remove</button>' +
                    '</td>' +
                    '</tr>'
                );
            })
            .join('');
    },

    async saveSelection() {
        if (window.isActiveVersion === false) return;
        const btn = document.getElementById('pagesSaveSelectionBtn');
        if (btn) {
            btn.disabled = true;
            btn.innerHTML =
                '<span class="spinner-border spinner-border-sm me-1"></span> Saving…';
        }
        try {
            const pages = [...this.selected.values()];
            const resp = await fetch('/domain/pages/selection', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ pages: pages }),
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.message || data.detail || 'Save failed');
            }
            this.selected = new Map();
            (data.pages || []).forEach((p) => {
                if (p && p.name) this.selected.set(p.name, p);
            });
            this.dirty = false;
            this.renderSelected();
            this.renderSearchHits();
            if (typeof showNotification === 'function') {
                showNotification(
                    data.message || 'Knowledge Pages selection saved',
                    'success'
                );
            }
            if (typeof setDomainDirty === 'function') {
                setDomainDirty('domain', true);
            }
        } catch (err) {
            if (typeof showNotification === 'function') {
                showNotification('Save failed: ' + err.message, 'error');
            }
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML =
                    '<i class="bi bi-cloud-upload me-1"></i> Save selection';
            }
        }
    },

    async preview(name) {
        const modalEl = document.getElementById('pagesPreviewModal');
        const body = document.getElementById('pagesPreviewBody');
        const title = document.getElementById('pagesPreviewModalLabel');
        if (!modalEl || !body) return;
        body.innerHTML = '<p class="text-muted small">Loading…</p>';
        if (title) title.textContent = 'Page preview';
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        modal.show();
        try {
            const resp = await fetch(
                '/domain/pages/preview?name=' + encodeURIComponent(name),
                { credentials: 'same-origin' }
            );
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.message || data.detail || 'Preview failed');
            }
            const p = data.page || {};
            if (title) title.textContent = p.display_name || p.name || 'Page preview';
            const synonyms = (p.synonyms || []).join(', ');
            body.innerHTML =
                '<p class="small mb-2"><span class="text-muted">Domain:</span> ' +
                this._esc(this._domainLabel(p.domain)) +
                ' &nbsp;·&nbsp; <span class="badge ' +
                (p.state === 'PUBLISHED' ? 'text-bg-success' : 'text-bg-warning') +
                '">' +
                this._esc(p.state || '') +
                '</span></p>' +
                (p.description
                    ? '<p class="mb-2">' + this._esc(p.description) + '</p>'
                    : '') +
                (synonyms
                    ? '<p class="small text-muted mb-3">Synonyms: ' +
                      this._esc(synonyms) +
                      '</p>'
                    : '') +
                '<div class="pages-preview-body">' +
                this._renderPageHtml(p.body) +
                '</div>';
        } catch (err) {
            body.innerHTML =
                '<p class="text-danger small">' + this._esc(err.message) + '</p>';
        }
    },

    /**
     * Convert Knowledge Page Markdown body to sanitised HTML for preview.
     * Prefer shared ``renderMarkdown`` (marked + DOMPurify from base.html).
     */
    _renderPageHtml(body) {
        const src = body == null ? '' : String(body);
        if (!src.trim()) {
            return '<p class="text-muted small mb-0">(empty body)</p>';
        }
        if (typeof window.renderMarkdown === 'function') {
            return window.renderMarkdown(src);
        }
        if (window.marked && typeof window.marked.parse === 'function'
                && window.DOMPurify && typeof window.DOMPurify.sanitize === 'function') {
            try {
                return window.DOMPurify.sanitize(
                    window.marked.parse(src, { breaks: true, gfm: true })
                );
            } catch (_) { /* fall through */ }
        }
        return '<div class="pages-preview-plain">' + this._esc(src) + '</div>';
    },

    _searchStatus(msg, isError) {
        const el = document.getElementById('pagesSearchStatus');
        if (!el) return;
        el.textContent = msg || '';
        el.classList.toggle('text-danger', !!isError);
        el.classList.toggle('text-muted', !isError);
    },

    _fmtTime(iso) {
        if (!iso) return '—';
        try {
            const d = new Date(iso);
            if (Number.isNaN(d.getTime())) return iso;
            return d.toLocaleDateString();
        } catch (_) {
            return iso;
        }
    },

    _esc(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    },

    _escAttr(s) {
        return this._esc(s).replace(/'/g, '&#39;');
    },
};

document.addEventListener('DOMContentLoaded', () => {
    PagesManager.init();
});
