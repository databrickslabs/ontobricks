/**
 * OntoBricks - settings-health.js
 *
 * Settings → Health tab.  Calls GET /health (the readiness probe in
 * shared/fastapi/health.py) and renders the result with KPI tiles and a
 * per-check table.  The probe runs ~8 backend checks against the
 * filesystem, Databricks auth, the SQL warehouse, the registry UC volume,
 * registry catalog/schema DDL and Lakebase, so we lazy-load on first tab
 * show (mirroring the Graph DB tab pattern) instead of every page load.
 */
document.addEventListener('DOMContentLoaded', function () {

    const STATUS_ICON = {
        ok:      '<i class="bi bi-check-circle-fill text-success"></i>',
        warning: '<i class="bi bi-exclamation-triangle-fill text-warning"></i>',
        error:   '<i class="bi bi-x-circle-fill text-danger"></i>'
    };

    const OVERALL_BADGE = {
        ok:      { cls: 'bg-success',           label: 'All systems go' },
        warning: { cls: 'bg-warning text-dark', label: 'Degraded' },
        error:   { cls: 'bg-danger',            label: 'Failing' }
    };

    let healthLoaded = false;

    // Lazy-load on first tab show so the eight backend probes do not run
    // for every visitor of the Settings page.
    const tabBtn = document.getElementById('tab-health');
    if (tabBtn) {
        tabBtn.addEventListener('shown.bs.tab', () => {
            if (!healthLoaded) loadHealth();
        });
    }

    const btnRefresh = document.getElementById('btnRefreshHealth');
    if (btnRefresh) {
        btnRefresh.addEventListener('click', loadHealth);
    }

    async function loadHealth() {
        const container = document.getElementById('healthChecksContainer');
        const overall = document.getElementById('healthOverallBadge');
        if (!container) return;

        container.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span>' +
            ' Running readiness probes…</div>';
        if (overall) {
            overall.className = 'badge bg-secondary ms-2';
            overall.textContent = 'Running…';
        }

        let data;
        try {
            const resp = await fetch('/health', { credentials: 'same-origin' });
            data = await resp.json();
        } catch (err) {
            console.error('Error loading /health:', err);
            container.innerHTML =
                '<div class="alert alert-danger small mb-0">' +
                'Network error while contacting /health: ' + escapeHtml(String(err)) +
                '</div>';
            if (overall) {
                overall.className = 'badge bg-danger ms-2';
                overall.textContent = 'Unreachable';
            }
            return;
        }

        healthLoaded = true;

        const summary = data.summary || { total: 0, ok: 0, warnings: 0, errors: 0 };
        document.getElementById('healthTotalCount').textContent = summary.total || 0;
        document.getElementById('healthOkCount').textContent = summary.ok || 0;
        document.getElementById('healthWarningCount').textContent = summary.warnings || 0;
        document.getElementById('healthErrorCount').textContent = summary.errors || 0;
        const tiles = document.getElementById('healthKpiTiles');
        if (tiles) tiles.classList.remove('d-none');

        if (overall) {
            const cfg = OVERALL_BADGE[data.status] || OVERALL_BADGE.warning;
            overall.className = 'badge ' + cfg.cls + ' ms-2';
            overall.textContent = cfg.label;
            overall.title = data.version
                ? 'OntoBricks ' + data.version
                : '';
        }

        const checks = Array.isArray(data.checks) ? data.checks : [];
        if (checks.length === 0) {
            container.innerHTML =
                '<div class="alert alert-warning small mb-0">' +
                'No checks were returned by /health — the readiness probe may not be configured.' +
                '</div>';
            return;
        }

        // Sort errors first, then warnings, then ok — saves the operator a scan.
        const order = { error: 0, warning: 1, ok: 2 };
        const sorted = checks.slice().sort((a, b) => {
            const sa = order[a.status] ?? 9;
            const sb = order[b.status] ?? 9;
            if (sa !== sb) return sa - sb;
            return (a.name || '').localeCompare(b.name || '');
        });

        const rows = sorted.map(c => {
            const icon = STATUS_ICON[c.status] || STATUS_ICON.warning;
            const detail = c.detail || '';
            const isLong = detail.length > 200;
            const detailHtml = isLong
                ? '<details><summary class="text-muted small">'
                    + escapeHtml(detail.substring(0, 200)) + '…</summary>'
                    + '<pre class="small mt-2 mb-0">' + escapeHtml(detail) + '</pre>'
                    + '</details>'
                : '<span>' + escapeHtml(detail) + '</span>';
            const dur = (typeof c.duration_ms === 'number')
                ? '<span class="badge bg-light text-muted border">' + c.duration_ms + ' ms</span>'
                : '';
            return '<tr class="diag-check-row diag-check-' + c.status + '">' +
                '<td class="diag-check-icon">' + icon + '</td>' +
                '<td class="diag-check-name">' +
                    '<div class="fw-semibold">' + escapeHtml(c.label || c.name || '') + '</div>' +
                    '<small class="text-muted font-monospace">' + escapeHtml(c.name || '') + '</small>' +
                '</td>' +
                '<td class="diag-check-detail">' + detailHtml + '</td>' +
                '<td class="text-end">' + dur + '</td>' +
                '</tr>';
        });

        container.innerHTML =
            '<div class="table-responsive">' +
            '<table class="table table-sm table-hover align-middle mb-0">' +
            '<thead class="table-light">' +
            '<tr>' +
            '<th style="width: 4%;"></th>' +
            '<th style="width: 28%;">Check</th>' +
            '<th>Detail</th>' +
            '<th style="width: 10%;" class="text-end">Time</th>' +
            '</tr>' +
            '</thead>' +
            '<tbody>' + rows.join('') + '</tbody>' +
            '</table>' +
            '</div>';
    }

    function escapeHtml(text) {
        if (text == null) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }
});
