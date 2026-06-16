/*
 * Registry → Build Analytics.
 *
 * Renders the per-domain Digital Twin build-run trace stored in the
 * registry ``build_runs`` table: summary cards plus a full runs table
 * (newest-first). The most recent successful run for the selected
 * (domain, version) is flagged as the "active" build.
 *
 * Backed by:
 *   - GET /settings/build-analytics/{domain}?version=
 *   - GET /settings/build-runs/{domain}?version=
 */

document.addEventListener('DOMContentLoaded', function () {
    const domainSel = document.getElementById('buildAnalyticsDomain');
    const versionSel = document.getElementById('buildAnalyticsVersion');
    const cards = document.getElementById('buildAnalyticsCards');
    const runsContainer = document.getElementById('buildAnalyticsRunsContainer');
    const refreshBtn = document.getElementById('btnRefreshBuildAnalytics');

    // The page only renders these elements on the Registry view.
    if (!domainSel || !runsContainer) return;

    function fmtDuration(seconds) {
        const s = Number(seconds || 0);
        if (s <= 0) return '-';
        if (s < 60) return s.toFixed(1) + 's';
        const m = Math.floor(s / 60);
        const rem = Math.round(s % 60);
        return m + 'm ' + rem + 's';
    }

    function fmtTs(iso) {
        if (!iso) return '-';
        const d = new Date(iso);
        if (isNaN(d.getTime())) return escapeHtml(iso);
        return d.toLocaleString();
    }

    function statusBadge(status) {
        const s = (status || '').toLowerCase();
        let cls = 'bg-secondary';
        if (s === 'success') cls = 'bg-success';
        else if (s === 'error') cls = 'bg-danger';
        else if (s === 'cancelled') cls = 'bg-warning text-dark';
        return '<span class="badge ' + cls + '">' + escapeHtml(status || 'unknown') + '</span>';
    }

    function kindBadge(kind) {
        const k = (kind || '').toLowerCase();
        let icon = 'bi-play-circle';
        if (k === 'api') icon = 'bi-braces';
        else if (k === 'scheduled') icon = 'bi-clock-history';
        return '<span class="text-muted"><i class="bi ' + icon + ' me-1"></i>' +
            escapeHtml(kind || '-') + '</span>';
    }

    async function loadDomains() {
        domainSel.innerHTML = '<option value="">Loading domains...</option>';
        try {
            const resp = await fetch('/settings/registry/domains', { credentials: 'same-origin' });
            const data = await resp.json();
            domainSel.innerHTML = '<option value="">Select a domain</option>';
            const rows = data.domains || [];
            if (data.success && rows.length) {
                rows.forEach(d => {
                    const opt = document.createElement('option');
                    opt.value = d.name;
                    opt.textContent = d.name;
                    domainSel.appendChild(opt);
                });
            }
        } catch (e) {
            domainSel.innerHTML = '<option value="">Error loading domains</option>';
        }
    }

    function populateVersions(perVersion) {
        const current = versionSel.value;
        versionSel.innerHTML = '<option value="">All versions</option>';
        (perVersion || []).forEach(pv => {
            const opt = document.createElement('option');
            opt.value = pv.version;
            opt.textContent = 'V' + pv.version + ' (' + pv.total_runs + ' build' +
                (pv.total_runs === 1 ? '' : 's') + ')';
            versionSel.appendChild(opt);
        });
        // Keep the previously selected version if it still exists.
        if (current && Array.from(versionSel.options).some(o => o.value === current)) {
            versionSel.value = current;
        }
    }

    function renderCards(a) {
        cards.style.display = '';
        document.getElementById('baTotalRuns').textContent = a.total_runs || 0;
        const rate = Math.round((a.success_rate || 0) * 100);
        document.getElementById('baSuccessRate').textContent = rate + '%';
        document.getElementById('baAvgDuration').textContent = fmtDuration(a.avg_duration_s);
        document.getElementById('baLastTriples').textContent =
            (a.last_triple_count || 0).toLocaleString();
    }

    function renderRuns(runs) {
        if (!runs || !runs.length) {
            runsContainer.innerHTML =
                '<div class="text-center text-muted small py-3">No builds recorded yet for this selection.</div>';
            return;
        }
        let activeMarked = false;
        let html = '<div class="table-responsive"><table class="table table-sm table-hover align-middle mb-0">' +
            '<thead><tr>' +
            '<th>Started</th><th>Version</th><th>Kind</th><th>Status</th>' +
            '<th class="text-end">Duration</th><th class="text-end">Triples</th>' +
            '<th class="text-end">Entities</th><th class="text-end">Rel.</th>' +
            '<th>Message</th>' +
            '</tr></thead><tbody>';

        runs.forEach(r => {
            const isActive = !activeMarked && (r.status === 'success');
            if (isActive) activeMarked = true;
            const note = r.status === 'error' ? (r.error || '') : (r.message || '');
            html += '<tr' + (isActive ? ' class="table-success"' : '') + '>' +
                '<td class="small">' + fmtTs(r.started_at) + '</td>' +
                '<td><span class="badge bg-light text-dark">V' + escapeHtml(r.version || '?') + '</span>' +
                    (isActive ? ' <span class="badge bg-success">active</span>' : '') + '</td>' +
                '<td class="small">' + kindBadge(r.build_kind) + '</td>' +
                '<td>' + statusBadge(r.status) + '</td>' +
                '<td class="text-end small">' + fmtDuration(r.duration_s) + '</td>' +
                '<td class="text-end small">' + Number(r.triple_count || 0).toLocaleString() + '</td>' +
                '<td class="text-end small">' + Number(r.entity_count || 0) + '</td>' +
                '<td class="text-end small">' + Number(r.relationship_count || 0) + '</td>' +
                '<td class="small text-muted text-truncate" style="max-width:240px" title="' +
                    escapeHtml(note) + '">' + escapeHtml(note) + '</td>' +
                '</tr>';
        });
        html += '</tbody></table></div>';
        runsContainer.innerHTML = html;
    }

    async function loadAnalytics() {
        const domain = domainSel.value;
        if (!domain) {
            cards.style.display = 'none';
            runsContainer.innerHTML =
                '<div class="text-center text-muted small py-3">Select a domain to view its build history.</div>';
            return;
        }
        const version = versionSel.value;
        const qs = version ? ('?version=' + encodeURIComponent(version)) : '';
        runsContainer.innerHTML =
            '<div class="text-center text-muted small py-3"><span class="spinner-border spinner-border-sm me-1"></span> Loading build history...</div>';
        try {
            const [aResp, rResp] = await Promise.all([
                fetch('/settings/build-analytics/' + encodeURIComponent(domain) + qs,
                    { credentials: 'same-origin' }).then(r => r.json()).catch(() => ({})),
                fetch('/settings/build-runs/' + encodeURIComponent(domain) + qs,
                    { credentials: 'same-origin' }).then(r => r.json()).catch(() => ({})),
            ]);

            const analytics = (aResp && aResp.success && aResp.analytics) || {};
            renderCards(analytics);
            populateVersions(analytics.per_version);
            renderRuns((rResp && rResp.success && rResp.runs) || []);
        } catch (e) {
            cards.style.display = 'none';
            runsContainer.innerHTML =
                '<div class="alert alert-danger small">Failed to load build analytics: ' +
                escapeHtml(e.message || String(e)) + '</div>';
        }
    }

    domainSel.addEventListener('change', () => {
        versionSel.value = '';
        loadAnalytics();
    });
    versionSel.addEventListener('change', loadAnalytics);
    refreshBtn?.addEventListener('click', loadAnalytics);

    loadDomains();
});
