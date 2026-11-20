/**
 * Build page → Inferred tab: per-category breakdown of materialized inferred
 * triples (Reasoning / Cohorts / Communities).
 *
 * Uses the shared list style (.ob-filter-bar, .ob-runs-table, ObListFilter)
 * and the same loading / empty / error states as the Runs tab.
 * Reloads each time the tab is shown, and on Refresh.
 */
const InferredBreakdownModule = (() => {
    const ENDPOINT = '/dtwin/reasoning/inferred/breakdown';
    const numberFormat = new Intl.NumberFormat();
    const STATE_IDS = ['buildInferredLoading', 'buildInferredEmpty', 'buildInferredNotice',
        'buildInferredError', 'buildInferredTableWrapper'];
    let listFilter = null;

    const el = (id) => document.getElementById(id);

    function showOnly(visibleId) {
        STATE_IDS.forEach((id) => {
            const node = el(id);
            if (node) node.classList.toggle('d-none', id !== visibleId);
        });
    }

    function setRefreshBusy(busy) {
        const refreshBtn = el('buildInferredRefreshBtn');
        if (!refreshBtn) return;
        refreshBtn.disabled = !!busy;
        const icon = refreshBtn.querySelector('i');
        if (!icon) return;
        if (busy) {
            icon.className = 'spinner-border spinner-border-sm me-1';
            icon.setAttribute('role', 'status');
            icon.setAttribute('aria-hidden', 'true');
        } else {
            icon.className = 'bi bi-arrow-counterclockwise me-1';
            icon.removeAttribute('role');
            icon.setAttribute('aria-hidden', 'true');
        }
    }

    function formatCount(value) {
        return value === null || value === undefined ? '—' : numberFormat.format(value);
    }

    function renderRows(categories) {
        const body = el('buildInferredBody');
        body.textContent = '';
        categories.forEach((category) => {
            const row = document.createElement('tr');

            const label = document.createElement('td');
            label.textContent = category.label;
            row.appendChild(label);

            [category.triples, category.entities, category.groups].forEach((value) => {
                const cell = document.createElement('td');
                cell.className = 'text-end';
                cell.textContent = formatCount(value);
                row.appendChild(cell);
            });
            body.appendChild(row);
        });
        if (listFilter) listFilter.apply();
    }

    function showData(data) {
        if (!data.total) {
            showOnly('buildInferredEmpty');
            return;
        }
        renderRows(data.categories);
        el('buildInferredTotal').textContent = formatCount(data.total);
        showOnly('buildInferredTableWrapper');
    }

    function showError(message) {
        el('buildInferredErrorMessage').textContent = message;
        showOnly('buildInferredError');
    }

    async function load() {
        setRefreshBusy(true);
        showOnly('buildInferredLoading');
        try {
            const response = await fetch(ENDPOINT, { method: 'GET', credentials: 'include' });
            const data = await response.json();
            if (!response.ok || !data.success) {
                throw new Error(data.message || 'Could not load the inferred breakdown');
            }
            if (data.supported) {
                showData(data);
            } else {
                showOnly('buildInferredNotice');
            }
        } catch (error) {
            showError(error.message || 'Could not load the inferred breakdown');
        } finally {
            setRefreshBusy(false);
        }
    }

    function init() {
        const tbody = el('buildInferredBody');
        if (tbody) {
            listFilter = window.ObListFilter.bind(
                el('buildInferredFilter'), tbody, el('buildInferredCount'), 'row'
            );
        }
        const tabBtn = el('buildTabInferredBtn');
        if (tabBtn) tabBtn.addEventListener('shown.bs.tab', load);
        const refreshBtn = el('buildInferredRefreshBtn');
        if (refreshBtn) refreshBtn.addEventListener('click', load);
    }

    return { init, load };
})();

window.InferredBreakdownModule = InferredBreakdownModule;
document.addEventListener('DOMContentLoaded', () => InferredBreakdownModule.init());
