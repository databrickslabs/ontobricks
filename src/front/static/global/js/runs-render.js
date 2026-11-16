/**
 * OntoBricks - runs-render.js
 * Shared rendering for the run-history pages:
 *
 *   - Knowledge Graph > Management > Runs  (domain-runs.js, one domain)
 *   - Settings > Automation > Runs         (settings-runs.js, every domain)
 *
 * Build and analytics share the same columns/details between the two pages;
 * Settings also has a Simulation tab. Only the fetching, the paging and the
 * element ids differ. Everything that turns a run object into HTML lives here
 * so the pages cannot drift.
 *
 * The row builders take an ``opts`` object rather than being duplicated per
 * page: ``domain: true`` inserts the Context cell that only the cross-domain
 * page needs, and ``handler`` names the global function its Details button
 * calls. The modal builders RETURN HTML instead of writing to the DOM, so each
 * page can inject into its own modal body.
 */

window.RunsRender = (function () {

    function esc(s) {
        if (typeof escapeHtml === 'function') return escapeHtml(s == null ? '' : String(s));
        return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
            return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
        });
    }

    function fmtTs(iso) {
        if (!iso) return '—';
        return esc(formatDateTime(iso));
    }

    function fmtDuration(secs) {
        const s = Number(secs) || 0;
        if (s < 1) return (s * 1000).toFixed(0) + ' ms';
        if (s < 60) return s.toFixed(1) + ' s';
        const m = Math.floor(s / 60);
        const r = Math.round(s % 60);
        return m + 'm ' + r + 's';
    }

    function fmtMillis(ms) {
        const n = Number(ms) || 0;
        if (n < 1000) return n + ' ms';
        return fmtDuration(n / 1000);
    }

    function buildStatusBadge(status) {
        const st = (status || '').toLowerCase();
        if (st === 'success') return '<span class="badge bg-success"><i class="bi bi-check-circle me-1"></i>Success</span>';
        if (st === 'error') return '<span class="badge bg-danger"><i class="bi bi-x-circle me-1"></i>Error</span>';
        if (st === 'cancelled') return '<span class="badge bg-warning text-dark"><i class="bi bi-slash-circle me-1"></i>Cancelled</span>';
        return '<span class="badge bg-secondary">' + esc(status || 'unknown') + '</span>';
    }

    // Analytics reports completed/failed, builds report success/error/
    // cancelled. Overloading one helper would grey out every analytics row.
    function analyticsStatusBadge(status) {
        const st = (status || '').toLowerCase();
        if (st === 'completed') return '<span class="badge bg-success"><i class="bi bi-check-circle me-1"></i>Completed</span>';
        if (st === 'failed') return '<span class="badge bg-danger"><i class="bi bi-x-circle me-1"></i>Failed</span>';
        return '<span class="badge bg-secondary">' + esc(status || 'unknown') + '</span>';
    }

    // Simulation run rows use success/error (from Runner); scenario rows use
    // draft/running/ready/failed. One badge covers both so the history tab
    // and any future reuse stay consistent.
    function simulationStatusBadge(status) {
        const st = (status || '').toLowerCase();
        if (st === 'success' || st === 'ready') {
            return '<span class="badge bg-success"><i class="bi bi-check-circle me-1"></i>'
                + esc(st === 'ready' ? 'Ready' : 'Success') + '</span>';
        }
        if (st === 'error' || st === 'failed') {
            return '<span class="badge bg-danger"><i class="bi bi-x-circle me-1"></i>'
                + esc(st === 'failed' ? 'Failed' : 'Error') + '</span>';
        }
        if (st === 'running') {
            return '<span class="badge bg-info text-dark"><i class="bi bi-hourglass-split me-1"></i>Running</span>';
        }
        return '<span class="badge bg-secondary">' + esc(status || 'unknown') + '</span>';
    }

    function kindBadge(kind) {
        const k = (kind || '').toLowerCase();
        const map = {
            session: ['bg-primary', 'bi-person-workspace', 'Session'],
            api: ['bg-info text-dark', 'bi-hdd-network', 'API'],
            scheduled: ['bg-dark', 'bi-clock', 'Scheduled']
        };
        const cfg = map[k] || ['bg-secondary', 'bi-question-circle', kind || '—'];
        return '<span class="badge ' + cfg[0] + '"><i class="bi ' + cfg[1] + ' me-1"></i>' + esc(cfg[2]) + '</span>';
    }

    function localName(uri) {
        const s = String(uri == null ? '' : uri);
        const cut = Math.max(s.lastIndexOf('#'), s.lastIndexOf('/'));
        return cut >= 0 ? s.slice(cut + 1) : s;
    }

    function analyticsScope(classFilter) {
        const list = classFilter || [];
        if (!list.length) return '<span class="text-muted">All types</span>';
        const first = esc(localName(list[0]));
        if (list.length === 1) return first;
        return first + ' <span class="text-muted">+' + (list.length - 1) + '</span>';
    }

    function versionBadge(version) {
        return '<span class="badge bg-secondary">v' + esc(version || '?') + '</span>';
    }

    function domainCell(run) {
        return '<td class="small fw-semibold">' + esc(run.domain || '—') + '</td>';
    }

    function detailsCell(handler, idx, title) {
        return '<td class="text-center">'
            + '<button class="btn btn-sm btn-outline-primary" onclick="' + handler + '(' + idx + ')" title="' + title + '">'
            + '<i class="bi bi-eye"></i></button></td>';
    }

    function num(v) {
        return esc((Number(v) || 0).toLocaleString());
    }

    // opts: { domain: bool, handler: string }
    function buildRunCells(run, idx, opts) {
        const o = opts || {};
        return '<td class="text-end text-muted small">' + esc(run.id || (idx + 1)) + '</td>'
            + (o.domain ? domainCell(run) : '')
            + '<td class="small">' + fmtTs(run.started_at) + '</td>'
            + '<td class="text-center">' + versionBadge(run.version) + '</td>'
            + '<td class="text-center">' + buildStatusBadge(run.status) + '</td>'
            + '<td class="text-end">' + num(run.triple_count) + '</td>'
            + detailsCell(o.handler || 'showRunDetails', idx, 'View run details');
    }

    // A failed run stores zeros for every metric. Printing them would read as
    // a graph with no nodes rather than a run that never produced numbers, so
    // failed rows dash their metric cells out.
    function analyticsRunCells(run, idx, opts) {
        const o = opts || {};
        const failed = (run.status || '').toLowerCase() === 'failed';
        const dash = '<span class="text-muted">&mdash;</span>';

        return '<td class="small">' + fmtTs(run.computed_at) + '</td>'
            + (o.domain ? domainCell(run) : '')
            + '<td class="small">' + analyticsScope(run.class_filter) + '</td>'
            + '<td class="text-center">' + versionBadge(run.version) + '</td>'
            + '<td class="text-center">' + analyticsStatusBadge(run.status) + '</td>'
            + '<td class="text-end">' + (failed ? dash : num(run.node_count)) + '</td>'
            + '<td class="text-end">' + (failed ? dash : num(run.edge_count)) + '</td>'
            + '<td class="text-end">' + (failed ? dash : num(run.connected_components)) + '</td>'
            + '<td class="text-end">' + (failed ? dash : esc((Number(run.avg_degree) || 0).toFixed(2))) + '</td>'
            + '<td class="text-end font-monospace">' + (failed ? dash : esc((Number(run.density) || 0).toFixed(6))) + '</td>'
            + '<td class="text-end">' + esc(fmtMillis(run.duration_ms)) + '</td>'
            +             detailsCell(
                o.handler || 'showAnalyticsRunDetails', idx,
                'View analytics run details'
            );
    }

    // opts: { domain: bool, handler: string }
    function simulationRunCells(run, idx, opts) {
        const o = opts || {};
        const outcome = run.error || run.message || '';
        const outcomeCls = run.error ? 'text-danger' : '';
        const scenario = run.label || run.simulation_id || '—';
        return '<td class="small">' + fmtTs(run.started_at) + '</td>'
            + (o.domain ? domainCell(run) : '')
            + '<td class="text-center">' + versionBadge(run.version) + '</td>'
            + '<td class="small fw-semibold">' + esc(scenario) + '</td>'
            + '<td class="text-center">' + simulationStatusBadge(run.status) + '</td>'
            + '<td class="small ' + outcomeCls + '">' + esc(outcome) + '</td>'
            + '<td class="text-end small">' + esc(fmtDuration(run.duration_s)) + '</td>'
            + detailsCell(
                o.handler || 'showSettingsSimulationRunDetails', idx,
                'View simulation run details'
            );
    }

    // Community and Pattern runs both record completed / failed rows, so they
    // share the analytics status badge and the same dashed-out metric cells.
    function patternScope(anchorType) {
        if (!anchorType) return '<span class="text-muted">All types</span>';
        return '<span title="' + esc(anchorType) + '">' + esc(localName(anchorType)) + '</span>';
    }

    function yesNo(flag) {
        return flag
            ? '<span class="badge bg-success">Yes</span>'
            : '<span class="badge bg-warning text-dark">No</span>';
    }

    // opts: { domain: bool, handler: string }
    function communityRunCells(run, idx, opts) {
        const o = opts || {};
        const failed = (run.status || '').toLowerCase() === 'failed';
        const dash = '<span class="text-muted">&mdash;</span>';

        return '<td class="small">' + fmtTs(run.computed_at) + '</td>'
            + (o.domain ? domainCell(run) : '')
            + '<td class="small">' + analyticsScope(run.class_filter) + '</td>'
            + '<td class="text-center">' + versionBadge(run.version) + '</td>'
            + '<td class="text-center">' + analyticsStatusBadge(run.status) + '</td>'
            + '<td class="text-end">' + (failed ? dash : num(run.node_count)) + '</td>'
            + '<td class="text-end">' + (failed ? dash : num(run.cluster_count)) + '</td>'
            + '<td class="text-center">' + (failed ? dash : yesNo(run.converged)) + '</td>'
            + '<td class="text-end">' + esc(fmtMillis(run.duration_ms)) + '</td>'
            + detailsCell(
                o.handler || 'showSettingsCommunityRunDetails', idx,
                'View community run details'
            );
    }

    // opts: { domain: bool, handler: string }
    function patternRunCells(run, idx, opts) {
        const o = opts || {};
        const failed = (run.status || '').toLowerCase() === 'failed';
        const dash = '<span class="text-muted">&mdash;</span>';

        return '<td class="small">' + fmtTs(run.computed_at) + '</td>'
            + (o.domain ? domainCell(run) : '')
            + '<td class="small">' + patternScope(run.anchor_type) + '</td>'
            + '<td class="text-center">' + versionBadge(run.version) + '</td>'
            + '<td class="text-center">' + analyticsStatusBadge(run.status) + '</td>'
            + '<td class="text-end">' + (failed ? dash : num(run.anchor_count)) + '</td>'
            + '<td class="text-end">' + (failed ? dash : num(run.pattern_count)) + '</td>'
            + '<td class="text-end">' + (failed ? dash : num(run.hop_count)) + '</td>'
            + '<td class="text-end">' + (failed ? dash : num(run.min_support)) + '</td>'
            + '<td class="text-end">' + esc(fmtMillis(run.duration_ms)) + '</td>'
            + detailsCell(
                o.handler || 'showSettingsPatternRunDetails', idx,
                'View pattern run details'
            );
    }

    // Link to the Databricks job run, when the task recorded one.
    function jobRunLink(url) {
        if (!url) return '—';
        return '<a href="' + esc(url) + '" target="_blank" rel="noopener">Open job run'
            + ' <i class="bi bi-box-arrow-up-right ms-1"></i></a>';
    }

    function communityRunDetailsHtml(run) {
        const failed = (run.status || '').toLowerCase() === 'failed';
        const dash = '<span class="text-muted">&mdash;</span>';

        let html = '<div class="row g-2 mb-3">';
        html += kv('Status', analyticsStatusBadge(run.status));
        if (run.domain) html += kv('Context', esc(run.domain));
        html += kv('Version', versionBadge(run.version));
        html += kv('When', fmtTs(run.computed_at));
        html += kv('Duration', esc(fmtMillis(run.duration_ms)));
        html += '</div>';

        if (run.error) {
            html += '<div class="alert alert-danger small mb-3"><i class="bi bi-exclamation-octagon me-1"></i>'
                + esc(run.error) + '</div>';
        }

        const scope = run.class_filter || [];
        html += '<h6 class="mt-2 mb-2"><i class="bi bi-diagram-3 me-1"></i>Scope</h6>';
        if (!scope.length) {
            html += '<p class="text-muted small mb-3">All entity types.</p>';
        } else {
            html += '<ul class="small mb-3">' + scope.map(function (uri) {
                return '<li><span class="fw-semibold">' + esc(localName(uri)) + '</span> '
                    + '<span class="text-muted font-monospace" style="font-size:0.75rem">' + esc(uri) + '</span></li>';
            }).join('') + '</ul>';
        }

        html += '<h6 class="mt-2 mb-2"><i class="bi bi-bar-chart me-1"></i>Communities</h6>';
        html += '<div class="row g-2 mb-3">';
        html += kv('Nodes', failed ? dash : num(run.node_count));
        html += kv('Communities', failed ? dash : num(run.cluster_count));
        html += kv('Converged', failed ? dash : yesNo(run.converged));
        html += kv('Job Run', jobRunLink(run.run_page_url));
        html += kv('Task ID', '<span class="font-monospace small">' + esc(run.task_id || '—') + '</span>');
        html += '</div>';

        return html;
    }

    function patternRunDetailsHtml(run) {
        const failed = (run.status || '').toLowerCase() === 'failed';
        const dash = '<span class="text-muted">&mdash;</span>';

        let html = '<div class="row g-2 mb-3">';
        html += kv('Status', analyticsStatusBadge(run.status));
        if (run.domain) html += kv('Context', esc(run.domain));
        html += kv('Version', versionBadge(run.version));
        html += kv('When', fmtTs(run.computed_at));
        html += kv('Duration', esc(fmtMillis(run.duration_ms)));
        html += '</div>';

        if (run.error) {
            html += '<div class="alert alert-danger small mb-3"><i class="bi bi-exclamation-octagon me-1"></i>'
                + esc(run.error) + '</div>';
        }

        html += '<h6 class="mt-2 mb-2"><i class="bi bi-puzzle me-1"></i>Parameters</h6>';
        html += '<div class="row g-2 mb-3">';
        html += kv('Anchor Type', failed && !run.anchor_type ? dash : patternScope(run.anchor_type));
        html += kv('Hops', esc(run.hop_count || 0));
        html += kv('Min Support', esc(run.min_support || 0));
        html += kv('Detail Level', esc(run.detail_level || '—'));
        html += '</div>';

        html += '<h6 class="mt-2 mb-2"><i class="bi bi-bar-chart me-1"></i>Result</h6>';
        html += '<div class="row g-2 mb-3">';
        html += kv('Anchors', failed ? dash : num(run.anchor_count));
        html += kv('Patterns', failed ? dash : num(run.pattern_count));
        html += kv('Job Run', jobRunLink(run.run_page_url));
        html += kv('Task ID', '<span class="font-monospace small">' + esc(run.task_id || '—') + '</span>');
        html += '</div>';

        return html;
    }

    function kv(label, value) {
        return '<div class="col-sm-6 mb-2">'
            + '<div class="text-muted small">' + esc(label) + '</div>'
            + '<div class="fw-semibold text-break" style="overflow-wrap:anywhere;word-break:break-word;">' + value + '</div>'
            + '</div>';
    }

    function statsTable(obj) {
        const keys = Object.keys(obj || {});
        if (keys.length === 0) return '<p class="text-muted small mb-0">No data.</p>';
        let rows = '';
        keys.forEach(function (k) {
            const label = k.replace(/_/g, ' ').replace(/\b\w/g, function (c) { return c.toUpperCase(); });
            rows += '<tr><td class="small text-muted">' + esc(label) + '</td>'
                + '<td class="text-end fw-semibold small">' + esc(obj[k]) + '</td></tr>';
        });
        return '<table class="table table-sm mb-0"><tbody>' + rows + '</tbody></table>';
    }

    function phaseTable(phases) {
        const keys = Object.keys(phases || {});
        if (keys.length === 0) return '<p class="text-muted small mb-0">No phase timings recorded.</p>';
        let rows = '';
        keys.forEach(function (k) {
            rows += '<tr><td class="small">' + esc(k) + '</td>'
                + '<td class="text-end small">' + fmtDuration(phases[k]) + '</td></tr>';
        });
        return '<table class="table table-sm mb-0">'
            + '<thead class="table-light"><tr><th class="small">Step</th><th class="text-end small">Duration</th></tr></thead>'
            + '<tbody>' + rows + '</tbody></table>';
    }

    // The Context line only appears when the row carries one, so per-domain
    // rows (which have no ``domain`` key) render exactly as they always did.
    function buildRunDetailsHtml(run) {
        const stats = run.stats || {};
        let html = '<div class="row g-2 mb-3">';
        html += kv('Run ID', esc(run.id || '—'));
        html += kv('Status', buildStatusBadge(run.status));
        if (run.domain) html += kv('Context', esc(run.domain));
        html += kv('Version', versionBadge(run.version));
        html += kv('Trigger', kindBadge(run.build_kind));
        html += kv('Started', fmtTs(run.started_at));
        html += kv('Finished', fmtTs(run.finished_at));
        html += kv('Duration', fmtDuration(run.duration_s));
        html += kv('Graph Engine', esc(run.graph_engine || '—'));
        html += '</div>';

        if (run.message) {
            html += '<div class="alert alert-light border small mb-3"><i class="bi bi-chat-left-text me-1"></i>' + esc(run.message) + '</div>';
        }
        if (run.error) {
            html += '<div class="alert alert-danger small mb-3"><i class="bi bi-exclamation-octagon me-1"></i>' + esc(run.error) + '</div>';
        }

        html += '<h6 class="mt-2 mb-2"><i class="bi bi-hdd-stack me-1"></i>Build Output</h6>';
        html += '<div class="row g-2 mb-3">';
        html += kv('Triples', esc((run.triple_count || 0).toLocaleString()));
        html += kv('Entities', esc(run.entity_count || 0));
        html += kv('Relationships', esc(run.relationship_count || 0));
        html += kv('SQL Size', esc((run.sql_chars || 0).toLocaleString()) + ' chars');
        html += kv('Sync Mode', esc(run.sync_mode || '—'));
        html += kv('Graph Name', esc(run.graph_name || '—'));
        html += kv('View / Table', esc(run.view_table || '—'));
        html += kv('Task ID', '<span class="font-monospace small">' + esc(run.task_id || '—') + '</span>');
        html += '</div>';

        html += '<h6 class="mt-2 mb-2"><i class="bi bi-stopwatch me-1"></i>Phase Timings</h6>';
        html += phaseTable(run.phase_times);

        html += '<div class="row mt-3">';
        html += '<div class="col-md-6">'
            + '<h6 class="mb-2"><i class="bi bi-bezier2 me-1"></i>Ontology</h6>'
            + statsTable(stats.ontology) + '</div>';
        html += '<div class="col-md-6">'
            + '<h6 class="mb-2"><i class="bi bi-shuffle me-1"></i>Mapping</h6>'
            + statsTable(stats.mapping) + '</div>';
        html += '</div>';

        return html;
    }

    function analyticsRunDetailsHtml(run) {
        const failed = (run.status || '').toLowerCase() === 'failed';
        const scope = (run.class_filter || []);
        const dash = '<span class="text-muted">&mdash;</span>';

        let html = '<div class="row g-2 mb-3">';
        html += kv('Status', analyticsStatusBadge(run.status));
        if (run.domain) html += kv('Context', esc(run.domain));
        html += kv('Version', versionBadge(run.version));
        html += kv('When', fmtTs(run.computed_at));
        html += kv('Duration', esc(fmtMillis(run.duration_ms)));
        html += '</div>';

        if (run.error) {
            html += '<div class="alert alert-danger small mb-3"><i class="bi bi-exclamation-octagon me-1"></i>'
                + esc(run.error) + '</div>';
        }

        html += '<h6 class="mt-2 mb-2"><i class="bi bi-diagram-3 me-1"></i>Scope</h6>';
        if (!scope.length) {
            html += '<p class="text-muted small mb-3">All entity types.</p>';
        } else {
            html += '<ul class="small mb-3">' + scope.map(function (uri) {
                return '<li><span class="fw-semibold">' + esc(localName(uri)) + '</span> '
                    + '<span class="text-muted font-monospace" style="font-size:0.75rem">' + esc(uri) + '</span></li>';
            }).join('') + '</ul>';
        }

        html += '<h6 class="mt-2 mb-2"><i class="bi bi-bar-chart me-1"></i>Graph</h6>';
        html += '<div class="row g-2 mb-3">';
        html += kv('Nodes', failed ? dash : num(run.node_count));
        html += kv('Edges', failed ? dash : num(run.edge_count));
        html += kv('Connected Components', failed ? dash : num(run.connected_components));
        html += kv('Avg Degree', failed ? dash : esc((Number(run.avg_degree) || 0).toFixed(2)));
        html += kv('Density', failed ? dash : esc((Number(run.density) || 0).toFixed(6)));
        html += kv('Task ID', '<span class="font-monospace small">' + esc(run.task_id || '—') + '</span>');
        html += '</div>';

        return html;
    }

    function simulationRunDetailsHtml(run) {
        const scenario = run.label || run.simulation_id || '—';
        let html = '<div class="row g-2 mb-3">';
        html += kv('Status', simulationStatusBadge(run.status));
        if (run.domain) html += kv('Context', esc(run.domain));
        html += kv('Version', versionBadge(run.version));
        html += kv('Scenario', esc(scenario));
        html += kv('Simulation ID', '<span class="font-monospace small">' + esc(run.simulation_id || '—') + '</span>');
        html += kv('Started', fmtTs(run.started_at));
        html += kv('Finished', fmtTs(run.finished_at));
        html += kv('Duration', esc(fmtDuration(run.duration_s)));
        html += kv('By', esc(run.owner || '—'));
        html += kv('Task ID', '<span class="font-monospace small">' + esc(run.task_id || '—') + '</span>');
        html += '</div>';

        if (run.message) {
            html += '<div class="alert alert-light border small mb-3"><i class="bi bi-chat-left-text me-1"></i>'
                + esc(run.message) + '</div>';
        }
        if (run.error) {
            html += '<div class="alert alert-danger small mb-3"><i class="bi bi-exclamation-octagon me-1"></i>'
                + esc(run.error) + '</div>';
        }

        const stats = run.stats || {};
        if (Object.keys(stats).length) {
            html += '<h6 class="mt-2 mb-2"><i class="bi bi-bar-chart me-1"></i>Stats</h6>';
            html += statsTable(stats);
        }
        return html;
    }

    return {
        esc: esc,
        fmtTs: fmtTs,
        fmtDuration: fmtDuration,
        fmtMillis: fmtMillis,
        buildStatusBadge: buildStatusBadge,
        analyticsStatusBadge: analyticsStatusBadge,
        simulationStatusBadge: simulationStatusBadge,
        kindBadge: kindBadge,
        localName: localName,
        analyticsScope: analyticsScope,
        versionBadge: versionBadge,
        buildRunCells: buildRunCells,
        analyticsRunCells: analyticsRunCells,
        communityRunCells: communityRunCells,
        patternRunCells: patternRunCells,
        simulationRunCells: simulationRunCells,
        kv: kv,
        statsTable: statsTable,
        phaseTable: phaseTable,
        buildRunDetailsHtml: buildRunDetailsHtml,
        analyticsRunDetailsHtml: analyticsRunDetailsHtml,
        communityRunDetailsHtml: communityRunDetailsHtml,
        patternRunDetailsHtml: patternRunDetailsHtml,
        simulationRunDetailsHtml: simulationRunDetailsHtml
    };
})();
