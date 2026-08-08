/*
 * Settings → Automation → Runs (admin only).
 *
 * The cross-domain twin of Knowledge Graph → Management → Runs: one tab per
 * run kind, spanning every domain in the registry unless the Domain filter
 * narrows it, one page at a time.
 *
 * Backed by:
 *   - GET /settings/runs/build?domain=&limit=&offset=
 *   - GET /settings/runs/analytics?domain=&limit=&offset=
 *   - GET /settings/registry/domains  (the filter's options)
 *
 * All rendering comes from RunsRender (global/js/runs-render.js), shared with
 * domain-runs.js. This file owns the filter, the paging and the element ids.
 *
 * The two tabs are loaded and rendered independently, so one endpoint failing
 * leaves the other tab's table on screen. Both load on section entry, so
 * switching tabs never waits on a fetch.
 */

(function () {
    'use strict';

    const DEFAULT_PAGE_SIZE = 25;

    // One descriptor per tab. Everything that differs between build runs and
    // analytics runs is data here rather than a second copy of the loader.
    const TABS = {
        build: {
            url: '/settings/runs/build',
            label: 'build runs',
            handler: 'showSettingsRunDetails',
            ids: {
                loading: 'srBuildLoading',
                empty: 'srBuildEmpty',
                emptyScope: 'srBuildEmptyScope',
                error: 'srBuildError',
                errorMessage: 'srBuildErrorMessage',
                wrapper: 'srBuildTableWrapper',
                tbody: 'srBuildTableBody',
                pagination: 'srBuildPagination',
                pagingControls: 'srBuildPagingControls',
                pageInfo: 'srBuildPageInfo',
                pageSize: 'srBuildPageSize',
                prev: 'srBuildPrev',
                next: 'srBuildNext'
            },
            cells: function (run, idx) {
                return RunsRender.buildRunCells(run, idx, {
                    domain: true,
                    handler: 'showSettingsRunDetails'
                });
            }
        },
        analytics: {
            url: '/settings/runs/analytics',
            label: 'analytics runs',
            handler: 'showSettingsAnalyticsRunDetails',
            ids: {
                loading: 'srAnalyticsLoading',
                empty: 'srAnalyticsEmpty',
                emptyScope: 'srAnalyticsEmptyScope',
                error: 'srAnalyticsError',
                errorMessage: 'srAnalyticsErrorMessage',
                wrapper: 'srAnalyticsTableWrapper',
                tbody: 'srAnalyticsTableBody',
                pagination: 'srAnalyticsPagination',
                pagingControls: 'srAnalyticsPagingControls',
                pageInfo: 'srAnalyticsPageInfo',
                pageSize: 'srAnalyticsPageSize',
                prev: 'srAnalyticsPrev',
                next: 'srAnalyticsNext'
            },
            cells: function (run, idx) {
                return RunsRender.analyticsRunCells(run, idx, {
                    domain: true,
                    handler: 'showSettingsAnalyticsRunDetails'
                });
            }
        }
    };

    const state = {
        build: { rows: [], total: 0, limit: DEFAULT_PAGE_SIZE, offset: 0 },
        analytics: { rows: [], total: 0, limit: DEFAULT_PAGE_SIZE, offset: 0 }
    };

    let domainSel = null;

    function el(id) {
        return document.getElementById(id);
    }

    function selectedDomain() {
        return (domainSel && domainSel.value) || '';
    }

    // ── Rendering ────────────────────────────────────────────────────────

    function renderTable(kind) {
        const tab = TABS[kind];
        const st = state[kind];
        const tbody = el(tab.ids.tbody);
        if (!tbody) return;

        if (st.rows.length === 0) {
            el(tab.ids.wrapper).style.display = 'none';
            el(tab.ids.emptyScope).textContent = selectedDomain()
                ? 'for this domain'
                : '';
            el(tab.ids.empty).style.display = '';
            renderPagination(kind);
            return;
        }
        el(tab.ids.empty).style.display = 'none';
        tbody.innerHTML = '';
        st.rows.forEach(function (run, idx) {
            const row = document.createElement('tr');
            row.innerHTML = tab.cells(run, idx);
            tbody.appendChild(row);
        });
        el(tab.ids.wrapper).style.display = '';
        renderPagination(kind);
    }

    function renderPagination(kind) {
        const ids = TABS[kind].ids;
        const st = state[kind];
        const nav = el(ids.pagination);
        const controls = el(ids.pagingControls);
        if (!nav) return;

        // Toggled by class, not `style.display`: these are `d-flex` rows and
        // Bootstrap's `display: flex !important` beats an inline display.
        //
        // The footer follows the table — it is only meaningless when there is
        // nothing to page. The label and Prev/Next hide on their own once the
        // result fits one page, which keeps the Rows selector reachable; hiding
        // the whole footer would strand a tab that always fits one page with no
        // way to raise its page size.
        nav.classList.toggle('d-none', st.rows.length === 0);
        controls.classList.toggle('d-none', st.total <= st.limit && st.offset === 0);

        // A page can come back empty while total > 0 if rows were written or
        // pruned between the count and the page, so the label reads off the
        // rows in hand rather than assuming the offset landed on one.
        const first = st.rows.length === 0 ? 0 : st.offset + 1;
        const last = st.offset + st.rows.length;
        el(ids.pageInfo).textContent =
            'Showing ' + first + '–' + last + ' of ' + st.total;
        el(ids.prev).disabled = st.offset <= 0;
        el(ids.next).disabled = last >= st.total;
    }

    // ── Loading ──────────────────────────────────────────────────────────

    // Returns true iff the fetch produced a renderable result (an empty
    // registry is not a failure), false on any error path.
    async function loadTab(kind) {
        const tab = TABS[kind];
        const st = state[kind];
        const ids = tab.ids;
        if (!el(ids.loading)) return false;

        el(ids.loading).style.display = '';
        el(ids.empty).style.display = 'none';
        el(ids.error).style.display = 'none';
        el(ids.wrapper).style.display = 'none';
        // Only the now-stale label and Prev/Next go: taking the whole footer
        // down would make the Rows selector vanish and reappear on every
        // reload, which is visible whenever the registry is slow to answer.
        el(ids.pagingControls).classList.add('d-none');

        const params = new URLSearchParams({
            limit: String(st.limit),
            offset: String(st.offset)
        });
        const domain = selectedDomain();
        if (domain) params.set('domain', domain);

        try {
            const resp = await fetch(tab.url + '?' + params.toString(), {
                credentials: 'same-origin'
            });
            const data = await resp.json();
            el(ids.loading).style.display = 'none';

            if (!data.success) {
                el(ids.errorMessage).textContent =
                    data.message || 'Failed to load ' + tab.label;
                el(ids.error).style.display = '';
                // renderPagination() owns the footer's final state but does
                // not run on this path, and paging a table that is not on
                // screen is meaningless.
                el(ids.pagination).classList.add('d-none');
                return false;
            }
            st.rows = data.runs || [];
            st.total = Number(data.total) || 0;
            renderTable(kind);
            return true;
        } catch (err) {
            el(ids.loading).style.display = 'none';
            el(ids.errorMessage).textContent = err.message;
            el(ids.error).style.display = '';
            el(ids.pagination).classList.add('d-none');
            return false;
        }
    }

    // Sequential awaits rather than a combinator that settles on the first
    // rejection: each tab owns its error handling and neither may blank the
    // other's table.
    async function loadAll() {
        await loadTab('build');
        await loadTab('analytics');
    }

    function resetPaging() {
        state.build.offset = 0;
        state.analytics.offset = 0;
    }

    async function loadDomains() {
        if (!domainSel) return;
        try {
            const resp = await fetch('/settings/registry/domains', {
                credentials: 'same-origin'
            });
            const data = await resp.json();
            (data.domains || []).forEach(function (d) {
                const opt = document.createElement('option');
                opt.value = d.name;
                opt.textContent = d.name;
                domainSel.appendChild(opt);
            });
        } catch (err) {
            // The filter is an optional narrowing of an already-complete
            // view, so a failure here must not stop the tables from loading.
            console.error('[settings-runs] failed to list domains:', err);
        }
    }

    // ── Details modals ───────────────────────────────────────────────────

    function showDetails(kind, idx, bodyId, modalId) {
        const run = state[kind].rows[idx];
        if (!run) return;
        const body = el(bodyId);
        if (!body) {
            console.error('[settings-runs] #' + bodyId + ' not found — modal missing from page template.');
            return;
        }
        body.innerHTML = kind === 'build'
            ? RunsRender.buildRunDetailsHtml(run)
            : RunsRender.analyticsRunDetailsHtml(run);
        bootstrap.Modal.getOrCreateInstance(el(modalId)).show();
    }

    window.showSettingsRunDetails = function (idx) {
        showDetails('build', idx, 'srRunDetailsBody', 'srRunDetailsModal');
    };

    window.showSettingsAnalyticsRunDetails = function (idx) {
        showDetails(
            'analytics', idx,
            'srAnalyticsRunDetailsBody', 'srAnalyticsRunDetailsModal'
        );
    };

    // ── Wiring ───────────────────────────────────────────────────────────

    function wireTab(kind) {
        const ids = TABS[kind].ids;
        const st = state[kind];

        el(ids.pageSize)?.addEventListener('change', function (e) {
            st.limit = Number(e.target.value) || DEFAULT_PAGE_SIZE;
            // A page size change moves every row boundary, so the old offset
            // would land mid-page (or past the end) of the new pagination.
            st.offset = 0;
            loadTab(kind);
        });
        el(ids.prev)?.addEventListener('click', function () {
            st.offset = Math.max(0, st.offset - st.limit);
            loadTab(kind);
        });
        el(ids.next)?.addEventListener('click', function () {
            if (st.offset + st.limit >= st.total) return;
            st.offset += st.limit;
            loadTab(kind);
        });
    }

    document.addEventListener('DOMContentLoaded', function () {
        const section = el('runs-section');
        domainSel = el('settingsRunsDomain');
        if (!section || !domainSel) return;

        domainSel.addEventListener('change', function () {
            // Narrowing to a domain changes the row set entirely, so the
            // current offset may not exist in it any more.
            resetPaging();
            loadAll();
        });
        el('btnReloadSettingsRuns')?.addEventListener('click', loadAll);
        wireTab('build');
        wireTab('analytics');

        loadDomains();

        // Reload on every visit rather than latching once: an admin watching
        // builds land wants the current ledger, not the snapshot from their
        // first visit (same choice as the Locks panel).
        document.addEventListener('sidebarSectionChanged', function (e) {
            if (e.detail && e.detail.section === 'runs') {
                loadAll();
            }
        });

        if (section.classList.contains('active')) loadAll();
    });
})();
