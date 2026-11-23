/**
 * Build page → Objects tab: physical objects of the current context version.
 *
 * Loads `/dtwin/build/objects` (already scoped to the current version and to
 * the backend it uses) and renders one table per group. Every item carries a
 * `drop` descriptor naming the Settings drop endpoint and its payload, so this
 * module has no per-backend logic. Drop buttons only appear when the server
 * reports `can_drop` (app admins); the drop endpoints are admin-only anyway.
 */
const BuildObjectsModule = (() => {
    const ENDPOINT = '/dtwin/build/objects';
    const STATE_IDS = [
        'buildObjectsLoading',
        'buildObjectsNotice',
        'buildObjectsError',
        'buildObjectsContent',
    ];
    const DROP_ENDPOINTS = {
        uc: '/settings/graph-engine/drop-uc-object',
        lakebase: '/settings/graph-engine/lakebase-drop-object',
        neo4j: '/settings/graph-engine/neo4j-drop-label',
    };
    // Sort rank inside a usage bucket: views, then tables, then registrations.
    const KIND_RANK = { view: 0, table: 1, sync: 2, graph: 3, metrics: 4, work: 5 };

    let current = { groups: [], usages: [], can_drop: false };
    let lastData = null;
    let groupBy = 'usage';
    let busy = false;
    let listFilter = null;

    const el = (id) => document.getElementById(id);

    function showOnly(visibleId) {
        STATE_IDS.forEach((id) => {
            const node = el(id);
            if (node) node.classList.toggle('d-none', id !== visibleId);
        });
    }

    function setBusy(isBusy) {
        busy = isBusy;
        ['buildObjectsReloadBtn', 'buildObjectsDeleteAllBtn'].forEach((id) => {
            const btn = el(id);
            if (btn) btn.disabled = isBusy;
        });
        document.querySelectorAll('.js-build-object-drop').forEach((btn) => {
            btn.disabled = isBusy;
        });
    }

    function node(tag, className, text) {
        const element = document.createElement(tag);
        if (className) element.className = className;
        if (text != null) element.textContent = text;
        return element;
    }

    function kindLabel(kind) {
        return kind.charAt(0).toUpperCase() + kind.slice(1);
    }

    /** Work-list header row: sentence-case columns on a transparent band. */
    function tableHead(columns) {
        const thead = node('thead');
        const tr = node('tr');
        columns.forEach((label) => tr.appendChild(node('th', null, label)));
        thead.appendChild(tr);
        return thead;
    }

    function dropButton(item) {
        const btn = node('button', 'btn btn-outline-danger btn-sm py-0 px-2 js-build-object-drop');
        btn.type = 'button';
        btn.title = `Drop ${item.kind} ${item.name}`;
        btn.innerHTML = '<i class="bi bi-trash3"></i>';
        btn.addEventListener('click', () => confirmAndDrop([item], item.name));
        return btn;
    }

    /** One object row. `sourceTitle` is set only in the by-usage view, where rows
     *  from several sources share a table and need their origin shown. */
    function itemRow(item, canDrop, sourceTitle) {
        const tr = document.createElement('tr');
        const kindCell = node('td', null, kindLabel(item.kind));
        const nameCell = node('td', 'font-monospace', item.name);
        if (item.detail) {
            nameCell.appendChild(node('div', 'text-muted', item.detail));
        }
        if (item.error) {
            nameCell.appendChild(node('div', 'text-danger', item.error));
        }
        const cells = [kindCell, nameCell];
        if (sourceTitle) {
            cells.push(node('td', 'small text-muted', sourceTitle));
        }
        cells.push(node('td', 'small text-muted', item.state || ''));
        const actionCell = node('td', 'text-end');
        if (canDrop) actionCell.appendChild(dropButton(item));
        cells.push(actionCell);
        cells.forEach((td) => tr.appendChild(td));
        return tr;
    }

    function groupBlock(group, canDrop) {
        const block = node('div', 'mb-4');
        const header = node('div', 'd-flex align-items-center gap-2 mb-2');
        header.appendChild(node('h6', 'mb-0', group.title));
        if (group.location) {
            header.appendChild(node('span', 'text-muted small font-monospace', group.location));
        }
        block.appendChild(header);
        if (group.error) {
            block.appendChild(node('div', 'alert alert-warning small py-2 mb-2', group.error));
        }
        if (!group.items.length) {
            if (!group.error) block.appendChild(node('p', 'text-muted small mb-0', 'No objects.'));
            return block;
        }
        const wrap = node('div', 'table-responsive');
        const table = node('table', 'table table-hover align-middle ob-runs-table mb-0');
        table.appendChild(tableHead(['Kind', 'Name', 'State', '']));
        const tbody = document.createElement('tbody');
        group.items.forEach((item) => tbody.appendChild(itemRow(item, canDrop)));
        table.appendChild(tbody);
        wrap.appendChild(table);
        block.appendChild(wrap);
        return block;
    }

    function allItems() {
        return current.groups.flatMap((group) => group.items);
    }

    /** By-source view: one table per backend group, as the server returns them. */
    function renderBySource(canDrop) {
        const wrap = node('div', 'p-3');
        current.groups.forEach((group) => wrap.appendChild(groupBlock(group, canDrop)));
        return wrap;
    }

    function sortEntries(entries) {
        const rank = (kind) => (kind in KIND_RANK ? KIND_RANK[kind] : 9);
        return [...entries].sort((a, b) => (
            rank(a.item.kind) - rank(b.item.kind) || a.item.name.localeCompare(b.item.name)
        ));
    }

    function usageBlock(usage, entries, canDrop) {
        const block = node('div', 'mb-4');
        const header = node('div', 'd-flex align-items-center gap-2 mb-2');
        header.appendChild(node('h6', 'mb-0', usage.title));
        header.appendChild(node('span', 'ob-runs-count', String(entries.length)));
        block.appendChild(header);
        const wrap = node('div', 'table-responsive');
        const table = node('table', 'table table-hover align-middle ob-runs-table mb-0');
        table.appendChild(tableHead(['Kind', 'Name', 'Source', 'State', '']));
        const tbody = document.createElement('tbody');
        sortEntries(entries).forEach(({ item, source }) => {
            tbody.appendChild(itemRow(item, canDrop, source));
        });
        table.appendChild(tbody);
        wrap.appendChild(table);
        block.appendChild(wrap);
        return block;
    }

    /** By-usage view: objects from every source, bucketed by what they are for.
     *  Sections that failed to load stay visible on top, as in the source view. */
    function renderByUsage(canDrop) {
        const wrap = node('div', 'p-3');
        current.groups
            .filter((group) => group.error)
            .forEach((group) => wrap.appendChild(groupBlock(group, canDrop)));

        const buckets = new Map(current.usages.map((usage) => [usage.key, []]));
        current.groups.forEach((group) => {
            group.items.forEach((item) => {
                const bucket = buckets.get(item.usage);
                if (bucket) bucket.push({ item, source: group.title });
            });
        });
        current.usages.forEach((usage) => {
            const entries = buckets.get(usage.key);
            if (entries.length) wrap.appendChild(usageBlock(usage, entries, canDrop));
        });
        return wrap;
    }

    /** Re-apply the free-text filter; the row count follows the visible rows. */
    function applyFilter() {
        if (listFilter) listFilter.apply();
    }

    function clearContent() {
        el('buildObjectsContent').textContent = '';
        applyFilter();
    }

    function render(data) {
        lastData = data;
        current = {
            groups: data.groups || [],
            usages: data.usages || [],
            can_drop: !!data.can_drop,
        };
        const count = allItems().length;
        const hasProblem = current.groups.some((group) => group.error);
        const deleteAll = el('buildObjectsDeleteAllBtn');
        if (deleteAll) deleteAll.classList.toggle('d-none', !(current.can_drop && count));

        if (data.message || (!count && !hasProblem)) {
            el('buildObjectsNoticeMessage').textContent = data.message
                || 'Nothing built yet. Run Build to create the graph objects.';
            clearContent();
            showOnly('buildObjectsNotice');
            return;
        }
        const content = el('buildObjectsContent');
        content.textContent = '';
        content.appendChild(groupBy === 'usage'
            ? renderByUsage(current.can_drop)
            : renderBySource(current.can_drop));
        applyFilter();
        showOnly('buildObjectsContent');
    }

    async function load() {
        if (busy) return;
        setBusy(true);
        showOnly('buildObjectsLoading');
        try {
            const response = await fetch(ENDPOINT, { method: 'GET', credentials: 'include' });
            const data = await response.json();
            if (!response.ok || data.success === false) {
                throw new Error(data.message || data.detail || 'Could not load objects');
            }
            render(data);
        } catch (error) {
            el('buildObjectsErrorMessage').textContent = error.message || 'Could not load objects';
            clearContent();
            showOnly('buildObjectsError');
        } finally {
            setBusy(false);
        }
    }

    async function dropOne(item) {
        const drop = item.drop || {};
        const url = DROP_ENDPOINTS[drop.api];
        if (!url) throw new Error('Unsupported object type');
        const { api, ...payload } = drop;
        if (api === 'lakebase') Object.assign(payload, { database: '', branch_path: '' });
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify(payload),
        });
        let data = {};
        try { data = await response.json(); } catch (_) { /* non-JSON body */ }
        if (!response.ok || !data.success) {
            throw new Error(data.detail || data.message || `HTTP ${response.status}`);
        }
    }

    async function confirmAndDrop(items, label) {
        if (busy || !current.can_drop || !items.length) return;
        const list = items
            .map((item) => `<li class="font-monospace small">${escapeHtml(item.kind)}: ${escapeHtml(item.name)}</li>`)
            .join('');
        const confirmed = await showConfirmDialog({
            title: 'Drop objects?',
            messageHtml: `Drop ${items.length} object${items.length === 1 ? '' : 's'} for `
                + `<code>${escapeHtml(label)}</code>? This cannot be undone.`,
            detailHtml: `<ul class="mb-0 ps-3">${list}</ul>`,
            confirmText: 'Drop',
            confirmClass: 'btn-danger',
            icon: 'trash',
        });
        if (!confirmed) return;

        setBusy(true);
        const errors = [];
        for (const item of items) {
            try {
                await dropOne(item);
            } catch (error) {
                errors.push(`${item.kind} ${item.name}: ${error.message || 'failed'}`);
            }
        }
        busy = false;
        if (errors.length) {
            showNotification(`Drops failed:\n${errors.join('\n')}`, 'danger');
        } else {
            showNotification(items.length === 1 ? `Dropped ${items[0].name}` : 'Objects dropped', 'success');
        }
        if (typeof checkTripleStoreStatus === 'function') {
            try { checkTripleStoreStatus(true); } catch (_) { /* status card may be absent */ }
        }
        await load();
    }

    function init() {
        // The content wrapper holds one table per group/bucket; the filter
        // matches every row across them.
        listFilter = window.ObListFilter.bind(
            el('buildObjectsFilter'), el('buildObjectsContent'), el('buildObjectsCount'), 'object'
        );
        const tabBtn = el('certTabObjectsBtn');
        if (tabBtn) tabBtn.addEventListener('shown.bs.tab', load);
        const reloadBtn = el('buildObjectsReloadBtn');
        if (reloadBtn) reloadBtn.addEventListener('click', load);
        const deleteAll = el('buildObjectsDeleteAllBtn');
        if (deleteAll) {
            deleteAll.addEventListener('click', () => confirmAndDrop(allItems(), 'this context version'));
        }
        document.querySelectorAll('input[name="buildObjectsGroupBy"]').forEach((input) => {
            input.addEventListener('change', () => {
                if (!input.checked) return;
                groupBy = input.value;
                if (lastData) render(lastData);
            });
        });
    }

    return { init, load, render };
})();

window.BuildObjectsModule = BuildObjectsModule;
document.addEventListener('DOMContentLoaded', () => BuildObjectsModule.init());
