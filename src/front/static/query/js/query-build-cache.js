/**
 * Cache tab of one graph: Search-cache status, companion tables, Refresh cache.
 *
 * The Original Graph page and the Certified Graph page each have one pane
 * (``_graph_cache_pane.html``, element ids prefixed ``orig`` / ``cert``) fed by
 * ``/dtwin/sync/stats`` for that graph. Status badges are painted by
 * query-sync.js (`renderBuildGraphCacheStatus`); the pane reloads whenever its
 * tab is shown.
 */
function GraphCachePane({ prefix, source, tabBtnId, feedsOverview }) {
    const ENDPOINT = '/dtwin/sync/stats?simulation_id=' + encodeURIComponent(source);
    const STATE_IDS = ['Loading', 'Notice', 'Error', 'Content'].map((s) => prefix + 'Cache' + s);
    const numberFormat = new Intl.NumberFormat();

    const el = (id) => document.getElementById(id);

    function showOnly(visibleId) {
        STATE_IDS.forEach((id) => {
            const node = el(id);
            if (node) node.classList.toggle('d-none', id !== visibleId);
        });
    }

    function setReloadBusy(busy) {
        const btn = el(prefix + 'CacheReloadBtn');
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
        const body = el(prefix + 'CacheCompanionsBody');
        const empty = el(prefix + 'CacheCompanionsEmpty');
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
        const notice = el(prefix + 'CacheNoticeMessage');
        if (notice) {
            notice.textContent = message
                || 'Search cache is only available for Lakehouse and Lakebase.';
        }
        showOnly(prefix + 'CacheNotice');
    }

    function showError(message) {
        const node = el(prefix + 'CacheErrorMessage');
        if (node) node.textContent = message;
        showOnly(prefix + 'CacheError');
    }

    function render(data) {
        const cache = (data && data.graph_cache) || {};
        if (typeof renderBuildGraphCacheStatus === 'function') {
            renderBuildGraphCacheStatus(cache, prefix);
        }
        const statusLine = el(prefix + 'CacheStatusLine');
        if (statusLine && typeof _insightCacheStateLabel === 'function') {
            statusLine.textContent = _insightCacheStateLabel(cache);
        }
        if (!cache.supported) {
            showUnsupported();
            return;
        }
        renderCompanions(cache);
        showOnly(prefix + 'CacheContent');
    }

    async function load() {
        setReloadBusy(true);
        showOnly(prefix + 'CacheLoading');
        try {
            const response = await fetch(ENDPOINT, { method: 'GET', credentials: 'include' });
            const data = await response.json();
            if (!response.ok || data.success === false) {
                throw new Error(data.message || 'Could not load graph cache status');
            }
            render(data);
            if (feedsOverview && typeof window._obInsights === 'object') {
                window._obInsights.loaded = true;
                window._obInsights.data = data;
            }
            if (feedsOverview && typeof renderInsights === 'function') {
                try { renderInsights(data); } catch (_) { /* Overview may be absent */ }
            }
        } catch (error) {
            showError(error.message || 'Could not load graph cache status');
        } finally {
            setReloadBusy(false);
        }
    }

    function init() {
        const tabBtn = el(tabBtnId);
        if (tabBtn) tabBtn.addEventListener('shown.bs.tab', load);
        const reloadBtn = el(prefix + 'CacheReloadBtn');
        if (reloadBtn) reloadBtn.addEventListener('click', load);
    }

    return { init, load, render };
}

// The Overview figures are the Original Graph's, so only that pane feeds them.
window.OriginalCachePane = GraphCachePane({
    prefix: 'orig', source: 'original', tabBtnId: 'buildTabCacheBtn', feedsOverview: true,
});
window.CertifiedCachePane = GraphCachePane({
    prefix: 'cert', source: 'certified', tabBtnId: 'certTabCacheBtn', feedsOverview: false,
});
document.addEventListener('DOMContentLoaded', () => {
    window.OriginalCachePane.init();
    window.CertifiedCachePane.init();
});
