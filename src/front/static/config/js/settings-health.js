/**
 * OntoBricks - settings-health.js
 *
 * Settings → Health section.  Manages two lazy-loaded tabs:
 *
 *   Databricks tab  — calls GET /health (readiness probes: filesystem, auth,
 *                     warehouse, CloudFetch, registry config, Lakebase);
 *                     renders a flat KPI + check table, same shape as before.
 *
 *   Diagnostics tab — calls GET /settings/diagnostics (admin-only, grouped
 *                     deep-dive: UC privileges, Lakebase registry tables,
 *                     Graph DB, Delta); renders collapsible group cards.
 */
document.addEventListener('DOMContentLoaded', function () {

    // ── Status helpers ───────────────────────────────────────────────────
    const STATUS_ICON = {
        ok:      '<i class="bi bi-check-circle-fill text-success"></i>',
        warning: '<i class="bi bi-exclamation-triangle-fill text-warning"></i>',
        error:   '<i class="bi bi-x-circle-fill text-danger"></i>'
    };

    const OVERALL_BADGE = {
        ok:      { cls: 'bg-success',            label: 'All systems go' },
        warning: { cls: 'bg-warning text-dark',  label: 'Degraded' },
        error:   { cls: 'bg-danger',             label: 'Failing' }
    };

    const GROUP_ICON = {
        uc_registry:       'bi-layers',
        lakebase_registry: 'bi-database',
        graphdb:           'bi-diagram-3',
        delta:             'bi-table'
    };

    // ── Refresh buttons — only way to trigger checks (no auto-run) ──────
    const btnRefresh = document.getElementById('btnRefreshHealth');
    if (btnRefresh) {
        btnRefresh.addEventListener('click', loadHealth);
    }

    const btnRefreshDiag = document.getElementById('btnRefreshDiagnostics');
    if (btnRefreshDiag) {
        btnRefreshDiag.addEventListener('click', loadDiagnostics);
    }

    // ════════════════════════════════════════════════════════════════════
    // TAB 1 — Databricks readiness probes (GET /health)
    // ════════════════════════════════════════════════════════════════════

    async function loadHealth() {
        const container = document.getElementById('healthChecksContainer');
        const overall   = document.getElementById('healthOverallBadge');
        if (!container) return;

        container.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span>' +
            ' Running readiness probes…</div>';
        if (overall) {
            overall.className   = 'badge bg-secondary ms-2';
            overall.textContent = 'Running…';
        }

        let data;
        try {
            const resp = await fetch('/health', { credentials: 'same-origin' });
            data = await resp.json();
        } catch (err) {
            container.innerHTML =
                '<div class="alert alert-danger small mb-0">' +
                'Network error while contacting /health: ' + escapeHtml(String(err)) +
                '</div>';
            if (overall) {
                overall.className   = 'badge bg-danger ms-2';
                overall.textContent = 'Unreachable';
            }
            return;
        }

        // KPI tiles
        const summary = data.summary || { total: 0, ok: 0, warnings: 0, errors: 0 };
        document.getElementById('healthTotalCount').textContent   = summary.total   || 0;
        document.getElementById('healthOkCount').textContent      = summary.ok      || 0;
        document.getElementById('healthWarningCount').textContent = summary.warnings || 0;
        document.getElementById('healthErrorCount').textContent   = summary.errors  || 0;
        const tiles = document.getElementById('healthKpiTiles');
        if (tiles) tiles.classList.remove('d-none');

        // Overall badge
        if (overall) {
            const cfg = OVERALL_BADGE[data.status] || OVERALL_BADGE.warning;
            overall.className   = 'badge ' + cfg.cls + ' ms-2';
            overall.textContent = cfg.label;
            overall.title       = data.version ? 'OntoBricks ' + data.version : '';
        }

        const checks = Array.isArray(data.checks) ? data.checks : [];
        if (checks.length === 0) {
            container.innerHTML =
                '<div class="alert alert-warning small mb-0">' +
                'No checks were returned by /health — the readiness probe may not be configured.' +
                '</div>';
            return;
        }

        // Sort: errors → warnings → ok
        const order  = { error: 0, warning: 1, ok: 2 };
        const sorted = checks.slice().sort((a, b) => {
            const sa = order[a.status] ?? 9;
            const sb = order[b.status] ?? 9;
            return sa !== sb ? sa - sb : (a.name || '').localeCompare(b.name || '');
        });

        const rows = sorted.map(c => renderCheckRow(c)).join('');

        container.innerHTML =
            '<div class="table-responsive">' +
            '<table class="table table-sm table-hover align-middle mb-0">' +
            '<thead class="table-light">' +
            '<tr>' +
            '<th style="width:4%;"></th>' +
            '<th style="width:28%;">Check</th>' +
            '<th>Detail</th>' +
            '<th style="width:10%;" class="text-end">Time</th>' +
            '</tr>' +
            '</thead>' +
            '<tbody>' + rows + '</tbody>' +
            '</table>' +
            '</div>';
    }

    // ════════════════════════════════════════════════════════════════════
    // TAB 2 — Diagnostics (GET /settings/diagnostics)
    // ════════════════════════════════════════════════════════════════════

    async function loadDiagnostics() {
        const container = document.getElementById('diagChecksContainer');
        const overall   = document.getElementById('diagOverallBadge');
        if (!container) return;

        container.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span>' +
            ' Running diagnostic checks…</div>';
        if (overall) {
            overall.className   = 'badge bg-secondary ms-2';
            overall.textContent = 'Running…';
        }

        let data;
        try {
            const resp = await fetch('/settings/diagnostics', { credentials: 'same-origin' });
            if (resp.status === 403) {
                container.innerHTML =
                    '<div class="alert alert-warning small mb-0">' +
                    '<i class="bi bi-shield-lock me-1"></i>' +
                    'Diagnostics are admin-only. You do not have permission to run them.' +
                    '</div>';
                if (overall) {
                    overall.className   = 'badge bg-secondary ms-2';
                    overall.textContent = 'Forbidden';
                }
                return;
            }
            data = await resp.json();
        } catch (err) {
            container.innerHTML =
                '<div class="alert alert-danger small mb-0">' +
                'Network error while running diagnostics: ' + escapeHtml(String(err)) +
                '</div>';
            if (overall) {
                overall.className   = 'badge bg-danger ms-2';
                overall.textContent = 'Unreachable';
            }
            return;
        }

        // KPI tiles
        const summary = data.summary || {};
        document.getElementById('diagTotalCount').textContent   = summary.total   || 0;
        document.getElementById('diagOkCount').textContent      = summary.ok      || 0;
        document.getElementById('diagWarningCount').textContent = summary.warnings || 0;
        document.getElementById('diagErrorCount').textContent   = summary.errors  || 0;
        const tiles = document.getElementById('diagKpiTiles');
        if (tiles) tiles.classList.remove('d-none');

        // Overall badge
        if (overall) {
            const cfg = OVERALL_BADGE[data.status] || OVERALL_BADGE.warning;
            overall.className   = 'badge ' + cfg.cls + ' ms-2';
            overall.textContent = cfg.label;
        }

        const groups = Array.isArray(data.groups) ? data.groups : [];
        if (groups.length === 0) {
            container.innerHTML =
                '<div class="alert alert-warning small mb-0">' +
                'No diagnostic groups were returned.' +
                '</div>';
            return;
        }

        container.innerHTML = groups.map(g => renderGroup(g)).join('');
    }

    // ── Render helpers ───────────────────────────────────────────────────

    function groupStatus(checks) {
        if (!Array.isArray(checks) || checks.length === 0) return 'ok';
        if (checks.some(c => c.status === 'error'))   return 'error';
        if (checks.some(c => c.status === 'warning')) return 'warning';
        return 'ok';
    }

    function renderGroup(group) {
        const checks    = group.checks || [];
        const gStatus   = groupStatus(checks);
        const gIcon     = STATUS_ICON[gStatus] || STATUS_ICON.warning;
        const biIcon    = GROUP_ICON[group.id] || 'bi-hdd-stack';
        const rows      = checks.map(c => renderCheckRow(c)).join('');
        const descHtml  = group.description
            ? '<div class="px-3 pt-3 pb-0">' +
              '<p class="small text-muted mb-0 fst-italic">' +
              escapeHtml(group.description) +
              '</p></div>'
            : '';

        const badgeClass = gStatus === 'ok'      ? 'bg-success'
                         : gStatus === 'warning' ? 'bg-warning text-dark'
                         : 'bg-danger';
        const passCount  = checks.filter(c => c.status === 'ok').length;

        return (
            '<div class="card mb-3 border">' +

              '<div class="card-header d-flex align-items-center gap-2 py-2">' +
                '<i class="bi ' + biIcon + ' text-muted"></i>' +
                '<strong class="small flex-grow-1">' + escapeHtml(group.title || group.id) + '</strong>' +
                '<span class="badge ' + badgeClass + '">' + passCount + '/' + checks.length + '</span>' +
                gIcon +
              '</div>' +

              '<div class="card-body p-0">' +
                descHtml +
                '<div class="table-responsive">' +
                  '<table class="table table-sm table-hover align-middle mb-0">' +
                    '<tbody>' + rows + '</tbody>' +
                  '</table>' +
                '</div>' +
              '</div>' +

            '</div>'
        );
    }

    function renderCheckRow(c) {
        const icon   = STATUS_ICON[c.status] || STATUS_ICON.warning;
        const detail = c.detail || '';
        const isLong = detail.length > 240;
        const detailHtml = isLong
            ? '<details><summary class="text-muted small">' +
              escapeHtml(detail.substring(0, 240)) + '…</summary>' +
              '<pre class="small mt-2 mb-0 text-wrap">' + escapeHtml(detail) + '</pre>' +
              '</details>'
            : '<span class="small">' + escapeHtml(detail) + '</span>';
        const dur = (typeof c.duration_ms === 'number')
            ? '<span class="badge bg-light text-muted border">' + c.duration_ms + '&nbsp;ms</span>'
            : '';

        return (
            '<tr class="diag-check-row diag-check-' + (c.status || 'warning') + '">' +
              '<td class="diag-check-icon" style="width:3%;">' + icon + '</td>' +
              '<td class="diag-check-name" style="width:28%;">' +
                '<div class="fw-semibold small">' + escapeHtml(c.label || c.name || '') + '</div>' +
                '<small class="text-muted font-monospace">' + escapeHtml(c.name || '') + '</small>' +
              '</td>' +
              '<td class="diag-check-detail">' + detailHtml + '</td>' +
              '<td class="text-end" style="width:9%;">' + dur + '</td>' +
            '</tr>'
        );
    }

    // ── Utilities ────────────────────────────────────────────────────────

    function escapeHtml(text) {
        if (text == null) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }
});
