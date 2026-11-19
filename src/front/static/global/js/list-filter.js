/**
 * OntoBricks - list-filter.js
 * Shared free-text filter for flat work lists (.ob-runs-table).
 *
 * Markup: an <input type="search"> inside .ob-filter-bar, a <tbody> with
 * one <tr> per row, and an optional count element (.ob-runs-count).
 * Rows are matched on their visible text; the count reads "N of M runs"
 * while a filter is active.
 *
 *   const f = ObListFilter.bind(inputEl, tbodyEl, countEl, 'run');
 *   f.apply();   // call again after the rows are re-rendered
 */
window.ObListFilter = (function () {
    'use strict';

    function bind(input, tbody, countEl, noun) {
        if (!input || !tbody) return null;

        function apply() {
            const q = input.value.trim().toLowerCase();
            const rows = Array.from(tbody.querySelectorAll('tr'));
            let shown = 0;
            rows.forEach(function (tr) {
                const hit = !q || tr.textContent.toLowerCase().indexOf(q) !== -1;
                tr.style.display = hit ? '' : 'none';
                if (hit) shown++;
            });
            if (!countEl) return;
            const total = rows.length;
            if (!total) {
                countEl.textContent = '';
            } else if (shown === total) {
                countEl.textContent = total.toLocaleString() + ' ' + noun + (total === 1 ? '' : 's');
            } else {
                countEl.textContent = shown + ' of ' + total + ' ' + noun + 's';
            }
        }

        input.addEventListener('input', apply);
        return { apply: apply };
    }

    return { bind: bind };
})();
