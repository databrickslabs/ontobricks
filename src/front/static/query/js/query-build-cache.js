/**
 * Build page → Cache tab: Search-cache status, companion tables, Refresh cache.
 *
 * Status badges are painted by query-sync.js (`renderBuildGraphCacheStatus`).
 * This module loads `/dtwin/sync/stats`, fills the companion table, and
 * reloads whenever the Cache tab is shown.
 */
const BuildCacheModule = (() => {
    const ENDPOINT = '/dtwin/sync/stats';
    const STATE_IDS = [
        'buildCacheLoading',
        'buildCacheNotice',
        'buildCacheError',
        'buildCacheContent',
    ];
    const numberFormat = new Intl.NumberFormat();

    const el = (id) => document.getElementById(id);

    function showOnly(visibleId) {
        STATE_IDS.forEach((id) => {
            const node = el(id);
            if (node) node.classList.toggle('d-none', id !== visibleId);
        });
    }

    function setReloadBusy(busy) {
        const btn = el('buildCacheReloadBtn');
        if (!btn) return;
        btn.disabled = !!busy;
        const icon = btn.querySelector('i');
        if (!icon) return;
        if (busy) {
            icon.className = 'spinner-border spinner-border-sm me-1';
            icon.setAttribute('role', 'status');
        } else {
            icon.className = 'bi bi-arrow-counterclockwise me-1';
            icon.removeAttribute('role');
        }
    }

    function companionRows(companions) {
        const specs = [
            { key: 'adj_out', label: 'Adjacency out' },
            { key: 'adj_in', label: 'Adjacency in' },
            { key: 'props', label: 'Properties' },
            { key: 'entity_search', label: 'Entity search' },
            { key: 'entity_search_asserted', label: 'Entity search (asserted)' },
            { key: 'adj_out_asserted', label: 'Adjacency out (asserted)' },
            { key: 'adj_in_asserted', label: 'Adjacency in (asserted)' },
            { key: 'props_asserted', label: 'Properties (asserted)' },
        ];
        const byKey = {};
        (companions || []).forEach((item) => {
            if (item && item.key) byKey[item.key] = item;
        });
        return specs.map((spec) => {
            const item = byKey[spec.key] || {};
            return {
                label: item.label || spec.label,
                table: item.table || '—',
                ready: !!item.ready,
                rows: item.ready && item.row_count != null
                    ? numberFormat.format(Number(item.row_count))
                    : (item.ready ? '—' : 'Missing'),
            };
        });
    }

    function renderCompanions(cache) {
        const body = el('buildCacheCompanionsBody');
        const empty = el('buildCacheCompanionsEmpty');
        if (!body) return;
        body.textContent = '';
        const rows = companionRows(cache.companions);
        const anyReady = rows.some((row) => row.ready);
        rows.forEach((row) => {
            const tr = document.createElement('tr');
            const cells = [
                row.label,
                row.table,
                row.ready ? 'Ready' : 'Missing',
                row.rows,
            ];
            cells.forEach((value, index) => {
                const td = document.createElement('td');
                if (index === 3) td.className = 'text-end';
                if (index === 1) {
                    const code = document.createElement('code');
                    code.textContent = value;
                    td.appendChild(code);
                } else if (index === 2) {
                    const badge = document.createElement('span');
                    badge.className = row.ready
                        ? 'badge bg-success bg-opacity-10 text-success border border-success'
                        : 'badge bg-warning bg-opacity-10 text-warning border border-warning';
                    badge.textContent = value;
                    td.appendChild(badge);
                } else {
                    td.textContent = value;
                }
                tr.appendChild(td);
            });
            body.appendChild(tr);
        });
        if (empty) empty.classList.toggle('d-none', anyReady);
    }

    function showUnsupported(message) {
        const notice = el('buildCacheNoticeMessage');
        if (notice) {
            notice.textContent = message
                || 'Search cache is only available for Lakehouse and Lakebase.';
        }
        showOnly('buildCacheNotice');
    }

    function showError(message) {
        const node = el('buildCacheErrorMessage');
        if (node) node.textContent = message;
        showOnly('buildCacheError');
    }

    function render(data) {
        const cache = (data && data.graph_cache) || {};
        if (typeof renderBuildGraphCacheStatus === 'function') {
            renderBuildGraphCacheStatus(cache);
        }
        const statusLine = el('buildCacheStatusLine');
        if (statusLine && typeof _insightCacheStateLabel === 'function') {
            statusLine.textContent = _insightCacheStateLabel(cache);
        }
        if (!cache.supported) {
            showUnsupported();
            return;
        }
        renderCompanions(cache);
        showOnly('buildCacheContent');
    }

    async function load() {
        setReloadBusy(true);
        showOnly('buildCacheLoading');
        try {
            const response = await fetch(ENDPOINT, { method: 'GET', credentials: 'include' });
            const data = await response.json();
            if (!response.ok || data.success === false) {
                throw new Error(data.message || 'Could not load graph cache status');
            }
            render(data);
            if (typeof window._obInsights === 'object') {
                window._obInsights.loaded = true;
                window._obInsights.data = data;
            }
            if (typeof renderInsights === 'function') {
                try { renderInsights(data); } catch (_) { /* Overview may be absent */ }
            }
        } catch (error) {
            showError(error.message || 'Could not load graph cache status');
        } finally {
            setReloadBusy(false);
        }
    }

    function init() {
        const tabBtn = el('buildTabCacheBtn');
        if (tabBtn) tabBtn.addEventListener('shown.bs.tab', load);
        const reloadBtn = el('buildCacheReloadBtn');
        if (reloadBtn) reloadBtn.addEventListener('click', load);
    }

    return { init, load, render };
})();

window.BuildCacheModule = BuildCacheModule;
document.addEventListener('DOMContentLoaded', () => BuildCacheModule.init());
